"""Series stop between steps (2026-09-26): saved intent proofs, bounded check
retry and a durable stop record.  Synthetic accounts only; no SDK, secrets or
network."""
import asyncio
from dataclasses import replace
from decimal import Decimal
import hashlib
import json
import math
import time

import aiohttp
import pytest

from risex_spread_shadow.hood_handoff import operator_recovery as recovery
from risex_spread_shadow.hood_handoff import telegram_control as bot
from risex_spread_shadow.hood_handoff.contracts import ContractError, OrderPlan, OrderSnapshot, PreflightBlocked
from risex_spread_shadow.hood_handoff.journal import DurableJournal
from risex_spread_shadow.hood_handoff.keychain import KeychainAccessError
from risex_spread_shadow.hood_handoff.operator_recovery import HistoryBoundExceeded, WalletKeyMissing
from risex_spread_shadow.hood_handoff.random_cycle import (_AccountIdentityFailure, _RateLimitedAccount,
                                                           _RetryablePreparationFailure)
from risex_spread_shadow.hood_handoff.read_errors import ReadRateLimited
from test_hood_auto_close_before_run import close_slot, proof
from test_hood_handoff_random_cycle import AdvancingClock, cycle_config
from test_hood_operator_recovery import RecoveryClient, intent_journal
from test_hood_telegram_control import setup, update
from test_hood_telegram_series import cycle_slot

CHECKPOINTS = recovery.INTENT_CHECKPOINT_JOURNAL


def rows(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def second_intent(operator, client, clock, *, number=2, coi=778, status='filled'):
    """Another unresolved creation intent on account 11 with a seeded terminal order."""
    slot = operator / f'cycle-{number:03d}'
    slot.mkdir(mode=0o700)
    plan = OrderPlan(11, 7, 'SELL', Decimal('.2'), 20, Decimal('100.1'), 1001, 'LIMIT', 'POST_ONLY',
                     False, 1300000, coi)
    journal = DurableJournal(slot / 'opening.jsonl', clock=lambda: 1000)
    journal.acquire_attempt()
    try:
        journal.append('SOURCE_DISPATCH_INTENT', {'plan': plan.as_dict()})
        journal.append('SOURCE_DISPATCH_RESULT', {'accepted': True, 'order_id': f'old{coi}'})
    finally:
        journal.release_attempt()
    filled = Decimal('.2') if status == 'filled' else Decimal(0)
    client._save_order(OrderSnapshot(11, 7, f'old{coi}', coi, status, 'SELL', 'LIMIT', 'POST_ONLY', False,
                                     Decimal('.2'), Decimal('.2') - filled, filled, Decimal('100.1'),
                                     clock.now()))
    return slot


async def ready(client, operator, clock, *, require_flat=True):
    return (await recovery.inspect_current(cycle_config(operator / 'unused'), client, operator,
                                           require_flat=require_flat, clock=clock))[2]


# --- Saved exact proofs of old creation intents ------------------------------------------------

async def test_live_terminal_proof_is_saved_once_and_later_checks_do_not_look_up_again(tmp_path):
    clock = AdvancingClock()
    client = RecoveryClient(clock)
    slot = intent_journal(tmp_path)
    history = {path: path.read_bytes() for path in slot.iterdir()}
    first = await ready(client, tmp_path, clock)
    assert len(client.reads) == 1
    assert [row['client_order_index'] for row in first['resolved_now']] == [777]
    assert first['resolved_by_checkpoint'] == []
    saved = rows(tmp_path / CHECKPOINTS)
    assert [row['event'] for row in saved] == ['INTENT_RESOLUTION_CHECKPOINT']
    payload = saved[0]['payload']
    original = slot / 'opening.jsonl'
    # Independent identity: exact path and SHA-256 of the untouched history bytes.
    assert payload['original_path'] == str(original)
    assert payload['original_sha256'] == hashlib.sha256(original.read_bytes()).hexdigest()
    assert payload['proof_version'] == 1 and payload['intent_at'] == 1000
    assert payload['plan']['client_order_index'] == 777 and payload['order_id'] == 'old'
    assert payload['order']['status'] == 'canceled' and payload['order']['order_id'] == 'old'
    for require_flat in (False, True, True):
        again = await ready(client, tmp_path, clock, require_flat=require_flat)
        assert again['resolved_now'] == []
        assert [row['order_id'] for row in again['resolved_by_checkpoint']] == ['old']
    assert len(client.reads) == 1  # No new venue lookup for the proved old order.
    assert len(rows(tmp_path / CHECKPOINTS)) == 1
    assert {path: path.read_bytes() for path in slot.iterdir()} == history
    assert not (tmp_path / 'recovery-checks.jsonl').exists()
    assert not client.submissions


async def test_cycle_child_history_check_uses_the_same_saved_proof(tmp_path):
    clock = AdvancingClock()
    client = RecoveryClient(clock)
    intent_journal(tmp_path)
    config = cycle_config(tmp_path / 'unused')
    first = await recovery.resolve_prior(config, client, tmp_path, clock=clock)
    second = await recovery.resolve_prior(config, client, tmp_path, clock=clock)
    assert len(first['resolved_now']) == 1 and len(second['resolved_by_checkpoint']) == 1
    assert len(client.reads) == 1


async def test_changed_history_bytes_are_never_covered_by_an_old_proof(tmp_path):
    clock = AdvancingClock()
    client = RecoveryClient(clock)
    slot = intent_journal(tmp_path)
    await ready(client, tmp_path, clock)
    run_id = rows(slot / 'opening.jsonl')[0]['run_id']
    journal = DurableJournal(slot / 'opening.jsonl', run_id=run_id, clock=lambda: 1000)
    journal.acquire_attempt()
    try:
        journal.append('OPERATOR_NOTE', {'text': 'appended later'})
    finally:
        journal.release_attempt()
    again = await ready(client, tmp_path, clock)
    assert len(client.reads) == 2 and len(again['resolved_now']) == 1
    saved = rows(tmp_path / CHECKPOINTS)
    assert len(saved) == 2
    assert saved[1]['payload']['original_sha256'] == hashlib.sha256((slot / 'opening.jsonl').read_bytes()).hexdigest()
    assert saved[0]['payload']['original_sha256'] != saved[1]['payload']['original_sha256']


def rewrite_checkpoint(operator, mutate, *, at=1000):
    path = operator / CHECKPOINTS
    payload = rows(path)[0]['payload']
    mutate(payload)
    path.unlink()
    journal = DurableJournal(path, clock=lambda: at)
    journal.acquire_attempt()
    try:
        journal.append('INTENT_RESOLUTION_CHECKPOINT', payload)
    finally:
        journal.release_attempt()


@pytest.mark.parametrize('tamper', [
    'active_status', 'plan_price', 'order_price', 'order_id', 'expected_order_id', 'intent_at',
    'observed_before_intent', 'observed_after_proof', 'missing_order', 'order_account_market',
])
async def test_saved_proof_that_conflicts_with_history_blocks_without_lookup(tmp_path, tamper):
    clock = AdvancingClock()
    client = RecoveryClient(clock)
    intent_journal(tmp_path)
    await ready(client, tmp_path, clock)

    def mutate(payload):
        order = payload['order']
        if tamper == 'active_status':
            order['status'] = 'open'
        elif tamper == 'plan_price':
            payload['plan']['price'] = '100.2'
        elif tamper == 'order_price':
            order['price'] = '100.2'
        elif tamper == 'order_id':
            order['order_id'] = 'other'
        elif tamper == 'expected_order_id':
            payload['order_id'] = 'other'
        elif tamper == 'intent_at':
            payload['intent_at'] = 999.0
        elif tamper == 'observed_before_intent':
            order['observed_at'] = 999.0
        elif tamper == 'observed_after_proof':
            order['observed_at'] = 1000.5
        elif tamper == 'missing_order':
            payload['order'] = None
        elif tamper == 'order_account_market':
            order['market_id'] = 8
    rewrite_checkpoint(tmp_path, mutate)
    with pytest.raises(PreflightBlocked, match='intent checkpoint conflicts with immutable history'):
        await ready(client, tmp_path, clock)
    assert len(client.reads) == 1  # Only the original live proof; nothing is looked up to paper over it.
    assert not client.submissions


async def test_unknown_proof_version_is_ignored_and_proved_live_again(tmp_path):
    clock = AdvancingClock()
    client = RecoveryClient(clock)
    intent_journal(tmp_path)
    await ready(client, tmp_path, clock)
    rewrite_checkpoint(tmp_path, lambda payload: payload.update(proof_version=2))
    again = await ready(client, tmp_path, clock)
    assert len(client.reads) == 2 and len(again['resolved_now']) == 1
    assert [row['payload']['proof_version'] for row in rows(tmp_path / CHECKPOINTS)] == [2, 1]


@pytest.mark.parametrize('damage', ['foreign_event', 'symlink', 'directory', 'truncated', 'bad_plan'])
async def test_unsafe_or_malformed_proof_journal_blocks(tmp_path, damage):
    clock = AdvancingClock()
    client = RecoveryClient(clock)
    intent_journal(tmp_path)
    path = tmp_path / CHECKPOINTS
    if damage == 'foreign_event':
        path.write_text(json.dumps({'sequence': 1, 'run_id': 'x', 'event': 'CURRENT_STATE_VERIFIED',
                                    'at': 1000, 'payload': {}}) + '\n')
    elif damage == 'symlink':
        path.symlink_to(tmp_path / 'cycle-001' / 'opening.jsonl')
    elif damage == 'directory':
        path.mkdir()
    elif damage == 'truncated':
        path.write_text('{"sequence": 1')
    elif damage == 'bad_plan':
        path.write_text(json.dumps({'sequence': 1, 'run_id': 'x', 'event': 'INTENT_RESOLUTION_CHECKPOINT',
                                    'at': 1000, 'payload': {'proof_version': 1, 'plan': {}}}) + '\n')
    with pytest.raises((PreflightBlocked, ValueError)):
        await ready(client, tmp_path, clock)
    assert client.reads == []


async def test_proofs_completed_before_a_later_lookup_times_out_are_kept(tmp_path):
    clock = AdvancingClock()

    class Stalling(RecoveryClient):
        stall = True

        async def lookup_order(self, *args, **kwargs):
            if self.stall and str(kwargs.get('client_order_index')) == '778':
                self.reads.append(kwargs)
                raise asyncio.TimeoutError()
            return await super().lookup_order(*args, **kwargs)

    client = Stalling(clock)
    intent_journal(tmp_path)
    second_intent(tmp_path, client, clock)
    with pytest.raises(TimeoutError):
        await ready(client, tmp_path, clock)
    assert [str(read['client_order_index']) for read in client.reads] == ['777', '778']
    saved = rows(tmp_path / CHECKPOINTS)
    assert [row['payload']['plan']['client_order_index'] for row in saved] == [777]
    client.stall = False
    proof_now = await ready(client, tmp_path, clock)
    assert [str(read['client_order_index']) for read in client.reads] == ['777', '778', '778']
    assert [row['client_order_index'] for row in proof_now['resolved_by_checkpoint']] == [777]
    assert [row['client_order_index'] for row in proof_now['resolved_now']] == [778]


async def test_saving_a_partial_proof_never_replaces_the_original_failure(tmp_path, monkeypatch):
    clock = AdvancingClock()

    class Failing(RecoveryClient):
        async def lookup_order(self, *args, **kwargs):
            if str(kwargs.get('client_order_index')) == '778':
                raise asyncio.TimeoutError()
            return await super().lookup_order(*args, **kwargs)

    client = Failing(clock)
    intent_journal(tmp_path)
    second_intent(tmp_path, client, clock)

    def broken(*args, **kwargs):
        raise RuntimeError('synthetic disk failure')
    monkeypatch.setattr(recovery, '_append_intent_checkpoints', broken)
    with pytest.raises(TimeoutError):
        await ready(client, tmp_path, clock)


async def test_saved_proofs_do_not_count_toward_the_live_lookup_bound(tmp_path):
    clock = AdvancingClock()
    client = RecoveryClient(clock)
    intent_journal(tmp_path)
    second_intent(tmp_path, client, clock)
    second_intent(tmp_path, client, clock, number=3, coi=779)
    config = cycle_config(tmp_path / 'unused')
    assert config.max_poll_count == 2
    with pytest.raises(PreflightBlocked, match='too many unresolved historical intents'):
        await recovery.resolve_prior(config, client, tmp_path, clock=clock)
    assert client.reads == []
    wide = replace(config, max_poll_count=3)
    await recovery.resolve_prior(wide, client, tmp_path, clock=clock)
    assert len(client.reads) == 3
    proof_now = await recovery.resolve_prior(config, client, tmp_path, clock=clock)
    assert len(client.reads) == 3 and len(proof_now['resolved_by_checkpoint']) == 3


async def test_nonterminal_or_stale_live_reads_never_create_a_proof(tmp_path):
    clock = AdvancingClock()
    client = RecoveryClient(clock)
    intent_journal(tmp_path)
    order = client.orders[(11, 'old')]
    client._save_order(replace(order, status='open'))
    with pytest.raises(PreflightBlocked):
        await ready(client, tmp_path, clock)
    client._save_order(replace(order, observed_at=900))
    with pytest.raises(PreflightBlocked):
        await ready(client, tmp_path, clock)
    assert not (tmp_path / CHECKPOINTS).exists()


async def test_new_proofs_never_enter_the_shared_readiness_journal(tmp_path):
    clock = AdvancingClock()
    clock.value = time.time()  # check_recovery uses the system clock for freshness.

    class Client(RecoveryClient):
        async def aclose(self):
            pass
    client = Client(clock)
    intent_journal(tmp_path)

    class Keys:
        def __init__(self, *args, **kwargs):
            pass

        def close(self):
            pass
    config = cycle_config(tmp_path / 'unused', api_key_index=4)
    for require_flat in (False, True):
        await recovery.check_recovery(config, tmp_path, require_flat=require_flat,
                                      client_factory=lambda *a, **k: client, secret_factory=Keys)
    events = [row['event'] for row in rows(tmp_path / 'recovery-checks.jsonl')]
    # The accepted base reader only allows these two events in this file.
    assert set(events) == {'CURRENT_STATE_VERIFIED'} and len(events) == 2
    checks = rows(tmp_path / 'recovery-checks.jsonl')
    assert len(checks[0]['payload']['resolved_now']) == 1 and checks[0]['payload']['resolved_by_checkpoint'] == []
    assert checks[1]['payload']['resolved_now'] == [] and len(checks[1]['payload']['resolved_by_checkpoint']) == 1
    assert [row['event'] for row in rows(tmp_path / CHECKPOINTS)] == ['INTENT_RESOLUTION_CHECKPOINT']
    assert len(client.reads) == 1


# --- Bounded retry of a between-cycle check ---------------------------------------------------

def series(tmp_path, recover, *, sleeps, notices, close=None):
    launches = []

    async def launch(**options):
        number = c.store.data['active']['series_index'] if 'series_index' in c.store.data['active'] else 1
        launches.append(number)
        cycle_slot(tmp_path, len(launches))

    async def sleep(seconds):
        sleeps.append(seconds)

    async def no_close():
        pytest.fail('flat series must not close')
    c = setup(tmp_path, launch)
    c.recovery, c.close = recover, close or no_close
    c._series_sleep = sleep
    c._series_delay = lambda: 7
    c.queue_notice = notices.append
    return c, launches


@pytest.mark.parametrize('failure', [
    lambda: TimeoutError(), lambda: asyncio.TimeoutError(), lambda: aiohttp.ServerTimeoutError(),
    lambda: aiohttp.ClientConnectionError(), lambda: ConnectionResetError(), lambda: ReadRateLimited(0),
])
async def test_one_incomplete_read_between_steps_is_retried_and_the_series_continues(tmp_path, failure):
    reads = []

    async def recover(*, require_flat):
        reads.append(require_flat)
        if len(reads) == 3:
            raise failure()
        return proof('READY' if require_flat else 'CLOSE_READY')
    sleeps, notices = [], []
    c, launches = series(tmp_path, recover, sleeps=sleeps, notices=notices)
    await c.handle(update(text='/run 2'))
    await c.task
    assert launches == [1, 2]
    assert reads == [False, True, False, False, True]
    assert sleeps == [7, 10]
    retry = [n for n in notices if isinstance(n, str) and 'проверка счетов не завершилась' in n]
    assert len(retry) == 1 and 'попытка 2 из 3' in retry[0] and 'Цикл 2/2' in retry[0]
    assert c.store.data['active'] is None
    assert c.store.data['last']['series_completed'] == 2 and 'stop' not in c.store.data['last']


async def test_three_incomplete_reads_stop_with_a_durable_stage_record(tmp_path, capsys):
    reads = []

    async def recover(*, require_flat):
        reads.append(require_flat)
        if len(reads) >= 3:
            raise TimeoutError('SECRET-URL https://api.example/?token=abc')
        return proof('READY' if require_flat else 'CLOSE_READY')
    sleeps, notices = [], []
    c, launches = series(tmp_path, recover, sleeps=sleeps, notices=notices)
    await c.handle(update(text='/run 30'))
    await c.task
    assert launches == [1]
    assert reads == [False, True, False, False, False]
    assert sleeps == [7, 10, 20]
    last = c.store.data['last']
    assert last['status'] == 'BLOCKED' and last['series_completed'] == 1 and last['series_total'] == 30
    assert last['stop'] == {'stage': 'SERIES_CLOSE_READY', 'category': 'READ_TIMEOUT', 'error': 'TimeoutError',
                            'attempts': 3, 'elapsed_seconds': 0, 'at': 1000}
    assert c.store.data['active']['series_index'] == 2  # Durable barrier; never replayed.
    fresh = bot.Store(c.store.directory, 'binding')
    assert fresh.data['last']['stop'] == last['stop']
    stop = [n for n in notices if isinstance(n, str) and n.startswith('⛔')]
    assert stop == ['⛔ <b>Цикл 2/30</b> · остановлено (проверка позиций и старых ордеров перед циклом, '
                    'попыток: 3): ошибка READ_TIMEOUT. Серия не продолжается; /accounts — позиции.']
    err = capsys.readouterr().err
    assert 'Telegram run stopped:' in err and '"stage": "SERIES_CLOSE_READY"' in err
    assert err.count('Telegram run check retry:') == 2
    assert 'SECRET' not in err + c.store.path.read_text() + ''.join(n for n in notices if isinstance(n, str))


@pytest.mark.parametrize('failure,category', [
    (PreflightBlocked('positions remain; use /close before /run'), 'POSITIONS_REMAIN'),
    (PreflightBlocked('previous order is unresolved or still executable'), 'PREFLIGHT_REFUSED'),
    (PreflightBlocked('intent checkpoint conflicts with immutable history'), 'PREFLIGHT_REFUSED'),
    (HistoryBoundExceeded('historical journal count exceeded'), 'HISTORY_LIMIT'),
    (WalletKeyMissing('wallet 5 has no stored credential'), 'WALLET_KEY_MISSING'),
    (RuntimeError('configuration changed'), 'CHECK_FAILED'),
    (RuntimeError('another operator process is active'), 'CHECK_FAILED'),
    (RuntimeError('Lighter read rejected HTTP 503, code=None'), 'CHECK_FAILED'),
    (ContractError('exact order read incomplete'), 'CHECK_FAILED'),
    (KeychainAccessError('macOS Keychain access was denied or failed'), 'CHECK_FAILED'),
    (OSError('disk full'), 'CHECK_FAILED'),
])
async def test_refusals_and_local_failures_are_never_retried(tmp_path, failure, category):
    reads = []

    async def recover(*, require_flat):
        reads.append(require_flat)
        if len(reads) == 3:
            raise failure
        return proof('READY' if require_flat else 'CLOSE_READY')
    sleeps, notices = [], []
    c, launches = series(tmp_path, recover, sleeps=sleeps, notices=notices)
    await c.handle(update(text='/run 3'))
    await c.task
    assert launches == [1] and reads == [False, True, False] and sleeps == [7]
    assert c.store.data['last']['stop']['attempts'] == 1
    assert c.store.data['last']['stop']['category'] == category
    assert not [n for n in notices if isinstance(n, str) and 'проверка счетов не завершилась' in n]


async def test_ready_after_flat_close_ready_is_retried_separately(tmp_path):
    reads = []

    async def recover(*, require_flat):
        reads.append(require_flat)
        if len(reads) in (4, 5):
            raise aiohttp.ClientConnectionError()
        return proof('READY' if require_flat else 'CLOSE_READY')
    sleeps, notices = [], []
    c, launches = series(tmp_path, recover, sleeps=sleeps, notices=notices)
    await c.handle(update(text='/run 2'))
    await c.task
    assert launches == [1, 2]
    assert reads == [False, True, False, True, True, True]
    assert sleeps == [7, 10, 20]


async def test_post_close_ready_is_retried_and_the_close_is_never_repeated(tmp_path):
    reads, closes = [], []

    async def recover(*, require_flat):
        reads.append(require_flat)
        if len(reads) == 3:
            return proof('CLOSE_READY', source='0.00025')
        if len(reads) == 4:
            raise TimeoutError()
        return proof('READY' if require_flat else 'CLOSE_READY')

    async def close():
        closes.append(True)
        close_slot(tmp_path)
    sleeps, notices = [], []
    c, launches = series(tmp_path, recover, sleeps=sleeps, notices=notices, close=close)
    await c.handle(update(text='/run 2'))
    await c.task
    assert closes == [True] and launches == [1, 2]
    assert reads == [False, True, False, True, True]
    assert sleeps == [7, 10]
    assert c.store.data['last']['series_completed'] == 2


async def test_first_step_admission_keeps_its_single_check(tmp_path):
    reads = []

    async def recover(*, require_flat):
        reads.append(require_flat)
        raise TimeoutError()
    sleeps, notices = [], []
    c, launches = series(tmp_path, recover, sleeps=sleeps, notices=notices)
    await c.handle(update(text='/run 3'))
    await c.task
    assert launches == [] and reads == [False] and sleeps == []
    assert c.store.data['last_admission']['category'] == 'READ_TIMEOUT'
    assert c.store.data['active'] is None


async def test_cancel_during_retry_wait_never_launches_the_next_cycle(tmp_path):
    reads = []
    waiting = asyncio.Event()

    async def recover(*, require_flat):
        reads.append(require_flat)
        if len(reads) == 3:
            raise TimeoutError()
        return proof('READY' if require_flat else 'CLOSE_READY')
    sleeps, notices = [], []
    c, launches = series(tmp_path, recover, sleeps=sleeps, notices=notices)

    async def sleep(seconds):
        sleeps.append(seconds)
        if seconds == 10:
            waiting.set()
            await asyncio.Event().wait()
    c._series_sleep = sleep
    await c.handle(update(text='/run 2'))
    await waiting.wait()
    c.task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await c.task
    assert launches == [1] and reads == [False, True, False]
    assert bot.Store(c.store.directory, 'binding').data['active']['series_index'] == 2


async def test_rate_limit_retry_waits_for_retry_after_within_bound(tmp_path):
    reads = []

    async def recover(*, require_flat):
        reads.append(require_flat)
        if len(reads) == 3:
            raise ReadRateLimited(45.2)
        return proof('READY' if require_flat else 'CLOSE_READY')
    sleeps, notices = [], []
    c, launches = series(tmp_path, recover, sleeps=sleeps, notices=notices)
    await c.handle(update(text='/run 2'))
    await c.task
    assert launches == [1, 2] and sleeps == [7, 46]


async def test_stop_record_names_the_trading_stage_without_attempts(tmp_path):
    async def recover(*, require_flat):
        return proof('READY' if require_flat else 'CLOSE_READY')

    async def launch(**options):
        raise RuntimeError('configuration changed; restart controller after local review')
    c = setup(tmp_path, launch)
    notices = []
    c.recovery, c.queue_notice = recover, notices.append

    async def no_close():
        pytest.fail('flat run must not close')
    c.close = no_close
    await c.handle(update(text='/run 1'))
    await c.task
    stop = c.store.data['last']['stop']
    assert stop['stage'] == 'LAUNCH' and stop['category'] == 'CHECK_FAILED' and 'attempts' not in stop
    assert any(isinstance(n, str) and 'остановлено (выполнение цикла): ошибка CHECK_FAILED' in n for n in notices)


def test_transient_classification_follows_only_explicit_causes():
    def caused(outer, inner):
        try:
            raise outer from inner
        except BaseException as exc:
            return exc

    def contextual(outer, inner):
        try:
            try:
                raise inner
            except BaseException:
                raise outer
        except BaseException as exc:
            return exc
    transient = [
        TimeoutError(), asyncio.TimeoutError(), aiohttp.ServerTimeoutError(), aiohttp.ClientConnectionError(),
        aiohttp.ServerDisconnectedError(), aiohttp.ClientPayloadError(), ConnectionResetError(),
        ReadRateLimited(1), _RateLimitedAccount('wallet 5 account', 2.0),
        _RetryablePreparationFailure('recovery accounts recovery read deadline exceeded'),
        caused(_RetryablePreparationFailure('source account read was transiently unavailable'), TimeoutError()),
        caused(_AccountIdentityFailure('account snapshot read failed'), aiohttp.ServerDisconnectedError()),
    ]
    for exc in transient:
        assert bot.transient_check_failure(exc) is not None, exc
    refused = [
        PreflightBlocked('previous order is unresolved or still executable'),
        caused(PreflightBlocked('previous leverage transaction evidence is incomplete'), TimeoutError()),
        _AccountIdentityFailure('account snapshot read failed'),
        contextual(ContractError('exact order identity conflicts'), TimeoutError()),
        ContractError('inactive order pagination exceeded configured bound'),
        RuntimeError('journal attempt is already owned by another process'),
        KeychainAccessError('denied'), OSError('disk'), ValueError('x'),
    ]
    for exc in refused:
        assert bot.transient_check_failure(exc) is None, exc
    assert bot.transient_check_failure(ReadRateLimited(1)).endswith('(HTTP 429)')


def test_retry_delay_is_fixed_or_bounded_retry_after():
    assert [bot.check_retry_delay(TimeoutError(), attempt) for attempt in (1, 2)] == [10, 20]
    assert bot.check_retry_delay(ReadRateLimited(3), 1) == 10
    assert bot.check_retry_delay(ReadRateLimited(45.2), 1) == math.ceil(45.2)
    assert bot.check_retry_delay(ReadRateLimited(500), 2) == 60
    assert bot.check_retry_delay(_RateLimitedAccount('x', 31.0), 1) == 31
    assert bot.check_retry_delay(ReadRateLimited(float('inf')), 1) == 10
    assert bot.SERIES_CHECK_ATTEMPTS == 3


# --- End to end: controller series over the real history check ---------------------------------

async def test_series_proves_the_old_order_once_and_survives_one_stalled_check(tmp_path):
    clock = AdvancingClock()
    client = RecoveryClient(clock)
    intent_journal(tmp_path)
    checks = []

    async def recover(*, require_flat):
        checks.append(require_flat)
        if len(checks) == 3:
            raise TimeoutError()  # The stalled check before step 2.
        return await ready(client, tmp_path, clock, require_flat=require_flat)
    sleeps, notices = [], []
    launches = []

    async def launch(**options):
        launches.append(c.store.data['active'].get('series_index', 1))
        (tmp_path / f'cycle-{len(launches) + 1:03d}').mkdir(mode=0o700)
    c = setup(tmp_path, launch)
    c.recovery = recover

    async def sleep(seconds):
        sleeps.append(seconds)

    async def no_close():
        pytest.fail('flat series must not close')
    c.close, c._series_sleep, c._series_delay, c.queue_notice = no_close, sleep, lambda: 5, notices.append
    complete = {'status': 'COMPLETE', 'inventory': {'status': 'CONFIRMED_FLAT'},
                'order_state': {'unresolved_intents': [], 'unresolved_observed_orders': []}}
    c.report = lambda name: complete if name in ('cycle-002', 'cycle-003', 'cycle-004') else None
    await c.handle(update(text='/run 3'))
    await c.task
    assert launches == [1, 2, 3]
    assert checks == [False, True, False, False, True, False, True]
    assert len(client.reads) == 1  # One live lookup of the old order for seven checks.
    assert sleeps == [5, 10, 5]
    assert c.store.data['last']['series_completed'] == 3 and 'stop' not in c.store.data['last']
    assert len(rows(tmp_path / CHECKPOINTS)) == 1
    assert not client.submissions
