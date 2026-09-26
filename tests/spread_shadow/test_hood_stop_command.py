"""Owner /stop (2026-09-26): safe-point cycle stop, owner-ended hold evidence,
launcher refusal and the controller's stop procedure.  Synthetic accounts only;
no SDK, secrets or network."""
import asyncio
import json
import os

import pytest

from risex_spread_shadow.hood_handoff import cli as cli_module
from risex_spread_shadow.hood_handoff import random_cycle as rc
from risex_spread_shadow.hood_handoff import telegram_cards as cards
from risex_spread_shadow.hood_handoff import telegram_control as bot
from risex_spread_shadow.hood_handoff import telegram_messages as views
from risex_spread_shadow.hood_handoff.contracts import Outcome, PreflightBlocked
from risex_spread_shadow.hood_handoff.offline_report import load_saved_cycle_report
from risex_spread_shadow.hood_handoff.operator_control import (STOP_REQUEST_NAME, clear_stop_request,
                                                               stop_requested, write_stop_request)
from test_hood_auto_close_before_run import close_slot, proof
from test_hood_handoff_random_cycle import (AdvancingClock, CycleClient, FixedRng, GuardSequenceClient,
                                            cycle_config, run_random_cycle)
from test_hood_telegram_control import setup, update
from test_hood_telegram_series import cycle_slot


def rows(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def events(path):
    return [row['event'] for row in rows(path)]


class NoLeverageWrites(CycleClient):
    async def update_leverage_fraction(self, *args, **kwargs):
        raise AssertionError('no leverage setting may be sent after an observed /stop')


# --- Engine: safe points --------------------------------------------------------------------

async def test_stop_before_leverage_sends_nothing_and_ends_as_preflight(tmp_path):
    clock = AdvancingClock()
    client = NoLeverageWrites(clock)
    path = tmp_path / 'cycle-001'
    result = await run_random_cycle(cycle_config(path), client, clock=clock, rng=FixedRng(25, 20),
                                    stop_requested=lambda: True)
    assert result.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED and result.reason == rc.OWNER_STOP_REASON
    assert client.submissions == [] and client.cancellations == []
    names = events(path / 'cycle.jsonl')
    assert names[names.index('SELECTION_PROVED') + 1] == 'OWNER_STOP_OBSERVED'
    assert [r['payload'] for r in rows(path / 'cycle.jsonl') if r['event'] == 'OWNER_STOP_OBSERVED'] == [
        {'stage': 'BEFORE_LEVERAGE'}]
    assert 'FIRST_MUTATION_BOUNDARY' not in names and 'LEVERAGE_PLAN' not in names
    assert names[-2:] == ['CYCLE_COMPLETE', 'HTTP_READ_TIMINGS'] or names[-1] == 'CYCLE_COMPLETE'
    assert not (path / 'opening.jsonl').exists()


async def test_stop_after_preparation_is_observed_before_the_first_order(tmp_path):
    clock = AdvancingClock()
    client = CycleClient(clock)
    path = tmp_path / 'cycle-001'
    answers = iter([False, True])
    result = await run_random_cycle(cycle_config(path), client, clock=clock, rng=FixedRng(25, 20),
                                    stop_requested=lambda: next(answers))
    assert result.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED and result.reason == rc.OWNER_STOP_REASON
    assert client.submissions == []
    observed = [r['payload'] for r in rows(path / 'cycle.jsonl') if r['event'] == 'OWNER_STOP_OBSERVED']
    assert observed == [{'stage': 'BEFORE_FIRST_ORDER'}]
    assert 'FIRST_MUTATION_BOUNDARY' not in events(path / 'cycle.jsonl')


async def test_stop_during_hold_ends_it_at_once_and_closes_with_the_normal_pair(tmp_path):
    clock = AdvancingClock()
    client = CycleClient(clock)
    path = tmp_path / 'cycle-001'
    # No stop before any order; the stop exists once the opening pair was sent.
    result = await run_random_cycle(cycle_config(path), client, clock=clock, rng=FixedRng(25, 20),
                                    stop_requested=lambda: len(client.submissions) >= 2)
    assert result.outcome is Outcome.SUCCESS, result.as_dict()
    assert client.source_position == client.receiver_position == 0
    assert [(p.side, p.reduce_only) for p in client.submissions] == [
        ('SELL', False), ('BUY', False), ('BUY', True), ('SELL', True)]
    assert clock.sleeps == []  # The 20 s hold never slept.
    names = events(path / 'cycle.jsonl')
    assert names.index('HOLD_ANCHORED') < names.index('HOLD_ENDED_BY_OWNER_STOP') < names.index('CLOSING_PLAN_READY')
    stop_row = next(r for r in rows(path / 'cycle.jsonl') if r['event'] == 'HOLD_ENDED_BY_OWNER_STOP')
    assert stop_row['payload'] == {'hold_seconds': 20, 'held_seconds': 0.0}
    report = load_saved_cycle_report(path)
    assert report['status'] == 'COMPLETE' and report['inventory']['status'] == 'CONFIRMED_FLAT'
    assert not [i for i in report['issues'] if i.get('code') == 'INCOMPLETE_HOLD_EVIDENCE']


async def test_stop_mid_hold_polls_each_second_and_records_the_held_time(tmp_path):
    clock = AdvancingClock()
    client = CycleClient(clock)
    path = tmp_path / 'cycle-001'
    result = await run_random_cycle(cycle_config(path), client, clock=clock, rng=FixedRng(25, 20),
                                    stop_requested=lambda: len(clock.sleeps) >= 7)
    assert result.outcome is Outcome.SUCCESS
    assert clock.sleeps == [1.0] * 7
    stop_row = next(r for r in rows(path / 'cycle.jsonl') if r['event'] == 'HOLD_ENDED_BY_OWNER_STOP')
    assert stop_row['payload'] == {'hold_seconds': 20, 'held_seconds': 7.0}
    assert load_saved_cycle_report(path)['status'] == 'COMPLETE'


async def test_marker_file_written_during_hold_is_observed_by_the_engine(tmp_path):
    tmp_path.chmod(0o700)

    class MarkerClock(AdvancingClock):
        async def sleep(self, seconds):
            await super().sleep(seconds)
            if len(self.sleeps) == 3:
                write_stop_request(tmp_path, {'update_id': 5, 'requested_at': self.value})

    clock = MarkerClock()
    client = CycleClient(clock)
    path = tmp_path / 'cycle-001'
    result = await run_random_cycle(cycle_config(path), client, clock=clock, rng=FixedRng(25, 20),
                                    stop_requested=lambda: stop_requested(tmp_path))
    assert result.outcome is Outcome.SUCCESS and clock.sleeps == [1.0, 1.0, 1.0]
    assert 'HOLD_ENDED_BY_OWNER_STOP' in events(path / 'cycle.jsonl')


async def test_without_a_stop_hook_the_hold_is_unchanged(tmp_path):
    clock = AdvancingClock()
    client = CycleClient(clock)
    path = tmp_path / 'cycle-001'
    result = await run_random_cycle(cycle_config(path), client, clock=clock, rng=FixedRng(25, 20))
    assert result.outcome is Outcome.SUCCESS and clock.sleeps == [20]
    assert 'HOLD_ENDED_BY_OWNER_STOP' not in events(path / 'cycle.jsonl')
    # A hook that never stops keeps the full hold (in one-second polls).
    clock2 = AdvancingClock()
    client2 = CycleClient(clock2)
    path2 = tmp_path / 'cycle-002'
    assert (await run_random_cycle(cycle_config(path2), client2, clock=clock2, rng=FixedRng(25, 20),
                                   stop_requested=lambda: False)).outcome is Outcome.SUCCESS
    assert clock2.sleeps == [1.0] * 20
    assert 'HOLD_ENDED_BY_OWNER_STOP' not in events(path2 / 'cycle.jsonl')


async def test_a_failing_stop_hook_never_stops_or_breaks_a_cycle(tmp_path):
    clock = AdvancingClock()
    client = CycleClient(clock)

    def broken():
        raise OSError('synthetic stat failure')
    result = await run_random_cycle(cycle_config(tmp_path / 'cycle-001'), client, clock=clock,
                                    rng=FixedRng(25, 20), stop_requested=broken)
    assert result.outcome is Outcome.SUCCESS and clock.sleeps == [1.0] * 20


async def test_stop_before_a_retry_sends_no_further_opening_attempt(tmp_path):
    clock = AdvancingClock()
    client = GuardSequenceClient(clock, {3})  # First opening attempt is a safe zero-fill retry.
    path = tmp_path / 'cycle-001'
    result = await run_random_cycle(cycle_config(path), client, clock=clock, rng=FixedRng(20, 20),
                                    stop_requested=lambda: len(client.submissions) >= 1)
    limits = [p for p in client.submissions if p.order_type == 'LIMIT' and not p.reduce_only]
    markets = [p for p in client.submissions if p.order_type == 'MARKET']
    assert len(limits) == 1 and markets == []
    assert client.source_position == client.receiver_position == 0
    names = events(path / 'cycle.jsonl')
    assert [r['payload'] for r in rows(path / 'cycle.jsonl') if r['event'] == 'OWNER_STOP_OBSERVED'] == [
        {'stage': 'BEFORE_OPENING_RETRY'}]
    assert 'PAIR_ATTEMPT_RETRY' not in names and not (path / 'opening-attempt-002.jsonl').exists()
    assert result.outcome is not Outcome.SUCCESS


# --- Report: owner-ended hold evidence ---------------------------------------------------------

async def _stopped_cycle(tmp_path):
    clock = AdvancingClock()
    client = CycleClient(clock)
    path = tmp_path / 'cycle-001'
    await run_random_cycle(cycle_config(path), client, clock=clock, rng=FixedRng(25, 20),
                           stop_requested=lambda: len(clock.sleeps) >= 3)
    return path


def _rewrite(path, mutate):
    items = rows(path / 'cycle.jsonl')
    items = mutate(items)
    for sequence, item in enumerate(items, 1):
        item['sequence'] = sequence
    (path / 'cycle.jsonl').write_text(''.join(json.dumps(item) + '\n' for item in items))


@pytest.mark.parametrize('forgery', ['removed', 'other_hold', 'after_closing_plan', 'before_anchor'])
async def test_short_hold_needs_the_exact_persisted_owner_stop(tmp_path, forgery):
    path = await _stopped_cycle(tmp_path)
    assert load_saved_cycle_report(path)['status'] == 'COMPLETE'

    def mutate(items):
        stop = next(i for i in items if i['event'] == 'HOLD_ENDED_BY_OWNER_STOP')
        anchor = next(i for i in items if i['event'] == 'HOLD_ANCHORED')
        plan = next(i for i in items if i['event'] == 'CLOSING_PLAN_READY')
        if forgery == 'removed':
            items.remove(stop)
        elif forgery == 'other_hold':
            stop['payload']['hold_seconds'] = 21
        elif forgery == 'after_closing_plan':
            stop['at'] = plan['at'] + 1
        elif forgery == 'before_anchor':
            stop['at'] = anchor['at'] - 1
        return items
    _rewrite(path, mutate)
    report = load_saved_cycle_report(path)
    assert report['status'] == 'INCOMPLETE'
    assert [i for i in report['issues'] if i.get('code') == 'INCOMPLETE_HOLD_EVIDENCE']


# --- Launcher and marker ------------------------------------------------------------------------

async def test_launcher_claims_no_slot_while_a_stop_marker_exists(tmp_path, capsys):
    tmp_path.chmod(0o700)
    write_stop_request(tmp_path, {'update_id': 1, 'requested_at': 1000.0})
    code = await cli_module._run_simple_confirmed(None, {}, tmp_path / 'random-cycle.json', tmp_path)
    assert code == 2 and '/stop' in capsys.readouterr().out
    assert not [p for p in tmp_path.iterdir() if p.name.startswith('cycle-')]


def test_marker_is_owner_only_atomic_and_presence_is_the_request(tmp_path):
    tmp_path.chmod(0o700)
    assert not stop_requested(tmp_path) and clear_stop_request(tmp_path) is False
    write_stop_request(tmp_path, {'update_id': 7, 'requested_at': 1.5})
    marker = tmp_path / STOP_REQUEST_NAME
    assert stop_requested(tmp_path) and oct(marker.stat().st_mode & 0o777) == '0o600'
    assert json.loads(marker.read_text()) == {'requested_at': 1.5, 'update_id': 7}
    assert [p.name for p in tmp_path.iterdir()] == [STOP_REQUEST_NAME]  # No temporary left.
    assert clear_stop_request(tmp_path) is True and not stop_requested(tmp_path)
    marker.write_text('not json')  # Unparsed: any entry counts as a pending stop.
    assert stop_requested(tmp_path)
    marker.unlink()
    marker.symlink_to(tmp_path / 'missing')
    assert stop_requested(tmp_path)


def test_views_share_the_engine_stop_reason():
    assert cards.OWNER_STOP_REASON == rc.OWNER_STOP_REASON


def test_live_steps_render_owner_stop_events(tmp_path):
    slot = tmp_path / 'cycle-001'
    slot.mkdir()
    base = [{'event': 'CYCLE_STARTED', 'payload': {'binding': {}}},
            {'event': 'OWNER_STOP_OBSERVED', 'payload': {'stage': 'BEFORE_FIRST_ORDER'}},
            {'event': 'CYCLE_PREFLIGHT_BLOCKED', 'payload': {'reason': rc.OWNER_STOP_REASON}},
            {'event': 'OWNER_STOP_OBSERVED', 'payload': {'stage': 'BEFORE_OPENING_RETRY'}},
            {'event': 'HOLD_ENDED_BY_OWNER_STOP', 'payload': {'hold_seconds': 40, 'held_seconds': 12.0}}]
    (slot / 'cycle.jsonl').write_text(''.join(
        json.dumps({'sequence': n, 'run_id': 'r', 'at': 1000 + n, **item}) + '\n' for n, item in enumerate(base, 1)))
    texts = [step[3] for step in cards.cycle_steps(slot)]
    assert any('до первого ордера, ордера не отправлялись' in t for t in texts)
    assert any('новых попыток открытия не будет' in t for t in texts)
    assert any('удержание прервано' in t and '12' in t for t in texts)
    assert not any('цикл остановлен до ордеров' in t for t in texts)


# --- Controller -----------------------------------------------------------------------------------

def controller(tmp_path, launch=None, *, recover=None, close=None):
    c = setup(tmp_path, launch or (lambda **o: pytest.fail('no cycle may launch')))
    notices = []
    c.queue_notice = notices.append
    c.recovery = recover
    c.close = close
    return c, notices


def flat_recovery(calls):
    async def recover(*, require_flat):
        calls.append(require_flat)
        return proof('READY' if require_flat else 'CLOSE_READY')
    return recover


async def test_idle_stop_on_flat_accounts_proves_zero_and_sends_nothing(tmp_path):
    calls = []

    async def close():
        pytest.fail('flat accounts need no close')
    c, notices = controller(tmp_path, recover=flat_recovery(calls), close=close)
    await c.handle(update(1, '/stop'))
    assert '⏹ Команда /stop принята' in c.transport.messages[-1][1]
    await c.task
    assert calls == [False, True]
    record = c.store.data['last_stop']
    assert record['result'] == 'FLAT' and record['update_id'] == 1 and 'close_slot' not in record
    assert not stop_requested(tmp_path)
    assert any(isinstance(n, str) and n.startswith('<b>⏹ Остановлено</b>') for n in notices)
    assert 'Остановлено командой /stop' in c.summary()
    assert bot.Store(c.store.directory, 'binding').data['last_stop'] == record


async def test_idle_stop_with_positions_closes_once_then_proves_zero(tmp_path):
    calls, closes = [], []

    async def recover(*, require_flat):
        calls.append(require_flat)
        return proof('READY' if require_flat else 'CLOSE_READY', source='0' if closes else '0.00025')

    async def close():
        saved = json.loads(c.store.path.read_text())['active']
        assert saved['action'] == 'close' and saved['update_id'] == 1  # Durable close record first.
        closes.append(True)
        close_slot(tmp_path)
    c, notices = controller(tmp_path, recover=recover, close=close)
    await c.handle(update(1, '/stop'))
    await c.task
    assert closes == [True] and calls == [False, True]
    record = c.store.data['last_stop']
    assert record['result'] == 'FLAT' and record['close_slot'] == 'close-001'
    assert record['close_status'] == 'CONFIRMED_FLAT'
    assert c.store.data['active'] is None
    assert any(isinstance(n, str) and 'закрываю reduce-only' in n for n in notices)


async def test_stop_during_a_running_cycle_lets_it_close_then_proves_zero(tmp_path):
    calls, launches, seen = [], [], []
    entered, release = asyncio.Event(), asyncio.Event()

    async def launch(**options):
        launches.append(True)
        entered.set()
        await release.wait()
        seen.append(stop_requested(tmp_path))
        cycle_slot(tmp_path, 1)

    async def close():
        pytest.fail('the cycle closed its own positions')
    c, notices = controller(tmp_path, launch, recover=flat_recovery(calls), close=close)
    await c.handle(update(1, '/run'))
    await entered.wait()
    await c.handle(update(2, '/stop'))
    assert 'Идущий цикл не отправит новых ордеров' in c.transport.messages[-1][1]
    assert stop_requested(tmp_path)
    assert 'Выполняется /stop' in c.summary()
    release.set()
    await c.task
    assert launches == [True] and seen == [True]
    assert calls == [False, True, False, True]
    assert c.store.data['last']['status'] == 'FINISHED' and c.store.data['active'] is None
    assert c.store.data['last_stop']['result'] == 'FLAT'
    assert not stop_requested(tmp_path)


async def test_stop_during_a_series_cycle_starts_no_further_step(tmp_path):
    calls, launches = [], []
    entered, release = asyncio.Event(), asyncio.Event()

    async def launch(**options):
        launches.append(c.store.data['active']['series_index'])
        entered.set()
        await release.wait()
        cycle_slot(tmp_path, len(launches))
    c, notices = controller(tmp_path, launch, recover=flat_recovery(calls),
                            close=lambda: pytest.fail('no close'))
    await c.handle(update(1, '/run 3'))
    await entered.wait()
    await c.handle(update(2, '/stop'))
    release.set()
    await c.task
    assert launches == [1] and calls == [False, True, False, True]
    last = c.store.data['last']
    assert last['series_completed'] == 1 and last['series_total'] == 3 and last['status'] == 'FINISHED'
    assert c.store.data['active'] is None and c.store.data['last_stop']['result'] == 'FLAT'
    assert any(isinstance(n, dict) and n.get('series_total') == 3 for n in notices)  # Series report.


async def test_stop_during_the_pause_ends_it_without_a_check_or_launch(tmp_path):
    calls, launches = [], []
    paused = asyncio.Event()

    async def launch(**options):
        launches.append(True)
        cycle_slot(tmp_path, len(launches))
    c, notices = controller(tmp_path, launch, recover=flat_recovery(calls),
                            close=lambda: pytest.fail('no close'))

    async def pause(seconds):
        paused.set()
        await asyncio.Event().wait()  # Only /stop ends this wait.
    c._series_sleep = pause
    await c.handle(update(1, '/run 3'))
    await paused.wait()
    await c.handle(update(2, '/stop'))
    await c.task
    assert launches == [True]
    assert calls == [False, True, False, True]  # Admission, then only the stop's checks.
    last = c.store.data['last']
    assert last['status'] == 'NOT_LAUNCHED' and last['series_completed'] == 1
    assert c.store.data['active'] is None and c.store.data['last_stop']['result'] == 'FLAT'


async def test_stop_during_a_check_retry_wait_ends_the_series_without_retrying(tmp_path):
    calls, launches = [], []
    waiting = asyncio.Event()

    async def recover(*, require_flat):
        calls.append(require_flat)
        if len(calls) == 3:
            raise TimeoutError()
        return proof('READY' if require_flat else 'CLOSE_READY')

    async def launch(**options):
        launches.append(True)
        cycle_slot(tmp_path, len(launches))
    c, notices = controller(tmp_path, launch, recover=recover, close=lambda: pytest.fail('no close'))

    async def sleep(seconds):
        if seconds == 10:
            waiting.set()
            await asyncio.Event().wait()
    c._series_sleep = sleep
    await c.handle(update(1, '/run 3'))
    await waiting.wait()
    await c.handle(update(2, '/stop'))
    await c.task
    assert launches == [True] and calls == [False, True, False, False, True]
    assert c.store.data['last']['stop']['stage'] == 'SERIES_CLOSE_READY'
    assert c.store.data['active'] is None and c.store.data['last_stop']['result'] == 'FLAT'


async def test_stop_during_admission_never_launches(tmp_path):
    calls = []
    entered, release = asyncio.Event(), asyncio.Event()

    async def recover(*, require_flat):
        calls.append(require_flat)
        if len(calls) == 2:
            entered.set()
            await release.wait()
        return proof('READY' if require_flat else 'CLOSE_READY')
    c, notices = controller(tmp_path, recover=recover, close=lambda: pytest.fail('no close'))
    await c.handle(update(1, '/run'))
    await entered.wait()
    await c.handle(update(2, '/stop'))
    release.set()
    await c.task
    assert calls == [False, True, False, True]
    assert c.store.data['active'] is None and c.store.data['last']['status'] == 'NOT_LAUNCHED'
    assert c.store.data['last_stop']['result'] == 'FLAT'


async def test_unconfirmed_stop_close_is_reported_and_keeps_the_barrier(tmp_path, capsys):
    calls = []

    async def recover(*, require_flat):
        calls.append(require_flat)
        return proof('READY' if require_flat else 'CLOSE_READY', source='0.00025')

    async def close():
        close_slot(tmp_path, status='UNKNOWN')
    c, notices = controller(tmp_path, recover=recover, close=close)
    await c.handle(update(1, '/stop'))
    await c.task
    record = c.store.data['last_stop']
    assert record['result'] == 'NOT_PROVED' and record['stage'] == 'STOP_CLOSE'
    assert record['close_status'] == 'UNKNOWN' and calls == [False]
    assert c.store.data['active']['action'] == 'close'
    assert not stop_requested(tmp_path)
    assert any(isinstance(n, str) and 'нулевые позиции не подтверждены' in n for n in notices)
    assert 'Telegram stop not proved' in capsys.readouterr().err


async def test_refused_stop_check_is_reported_without_a_close(tmp_path):
    async def recover(*, require_flat):
        raise PreflightBlocked('previous order is unresolved or still executable')
    c, notices = controller(tmp_path, recover=recover, close=lambda: pytest.fail('no close'))
    await c.handle(update(1, '/stop'))
    await c.task
    record = c.store.data['last_stop']
    assert record['result'] == 'NOT_PROVED' and record['category'] == 'PREFLIGHT_REFUSED'
    assert record['stage'] == 'STOP_CLOSE_READY' and record['attempts'] == 1
    assert '⛔ Остановка /stop не подтвердила ноль' in c.summary()


async def test_stop_check_keeps_its_own_bounded_retries(tmp_path):
    calls, sleeps = [], []

    async def recover(*, require_flat):
        calls.append(require_flat)
        if len(calls) == 1:
            raise TimeoutError()
        return proof('READY' if require_flat else 'CLOSE_READY')
    c, notices = controller(tmp_path, recover=recover, close=lambda: pytest.fail('no close'))

    async def sleep(seconds):
        sleeps.append(seconds)
    c._series_sleep = sleep
    await c.handle(update(1, '/stop'))
    await c.task
    assert calls == [False, False, True] and sleeps == [10]
    assert c.store.data['last_stop']['result'] == 'FLAT'


async def test_duplicate_foreign_and_replayed_stop_do_nothing_more(tmp_path):
    calls = []
    entered, release = asyncio.Event(), asyncio.Event()

    async def recover(*, require_flat):
        calls.append(require_flat)
        if len(calls) == 1:
            entered.set()
            await release.wait()
        return proof('READY' if require_flat else 'CLOSE_READY')
    c, notices = controller(tmp_path, recover=recover, close=lambda: pytest.fail('no close'))
    foreign = update(1, '/stop')
    foreign['message']['from']['id'] = 43
    await c.handle(foreign)
    assert c.task is None and not stop_requested(tmp_path)
    await c.handle(update(2, '/stop'))
    await entered.wait()
    await c.handle(update(3, '/stop'))
    assert 'Остановка уже выполняется' in c.transport.messages[-1][1]
    release.set()
    await c.task
    assert calls == [False, True]
    await c.handle(update(3, '/stop'))  # Consumed update: never replayed.
    assert c.task.done() and calls == [False, True]


async def test_run_removes_a_leftover_marker_and_the_old_stop_result(tmp_path):
    calls, launches = [], []

    async def launch(**options):
        launches.append(stop_requested(tmp_path))
        cycle_slot(tmp_path, 1)
    c, notices = controller(tmp_path, launch, recover=flat_recovery(calls), close=lambda: pytest.fail('no close'))
    tmp_path.chmod(0o700)
    write_stop_request(tmp_path, {'update_id': 0, 'requested_at': 1.0})
    c.store.data['last_stop'] = {'result': 'FLAT', 'completed_at': 1.0}
    await c.handle(update(1, '/run'))
    await c.task
    assert launches == [False] and c.store.data['last_stop'] is None


async def test_run_is_refused_when_the_leftover_marker_cannot_be_removed(tmp_path):
    c, notices = controller(tmp_path, recover=flat_recovery([]), close=lambda: pytest.fail('no close'))
    marker = tmp_path / STOP_REQUEST_NAME
    marker.mkdir()
    (marker / 'x').write_text('')
    await c.handle(update(1, '/run'))
    assert c.task is None and 'Новая операция не начата' in c.transport.messages[-1][1]


async def test_stop_after_a_running_close_proves_zero(tmp_path):
    calls = []
    entered, release = asyncio.Event(), asyncio.Event()

    async def close():
        entered.set()
        await release.wait()
        close_slot(tmp_path)
    c, notices = controller(tmp_path, recover=flat_recovery(calls), close=close)
    await c.handle(update(1, '/close'))
    await entered.wait()
    await c.handle(update(2, '/stop'))
    release.set()
    await c.task
    assert calls == [False, False, True]
    assert c.store.data['last_stop']['result'] == 'FLAT' and c.store.data['active'] is None


async def test_stop_texts_are_fixed_and_escaped():
    flat = views.stop_result_message({'result': 'FLAT', 'close_slot': '<b>'}, 'все 4 кошельков: 0')
    assert '&lt;b&gt;' in flat and 'все 4 кошельков: 0' in flat
    failed = views.stop_result_message({'result': 'NOT_PROVED', 'reason': 'x' * 400})
    assert 'нулевые позиции не подтверждены' in failed and len(failed) < 600
    assert views.stop_status(None) == '' and 'Выполняется /stop' in views.stop_status(None, running=True)
    assert '/stop' in views.help_message()
