"""Failures of the first four-wallet runs (owner cycles 312 and 317).

cycle-312: the private stream died during the hold, the paired close stopped
before its source send and reduce-only recovery started, but the source
account's nonce stayed reserved by the unsent prepared source order.  With API
nonce management the venue keeps returning that same next nonce, so recovery
could not reserve a nonce for the source account and +0.00039 BTC stayed open.

cycle-317: slow venue reads let the pre-quote nonce reservation expire; order
preparation then failed before anything was sent, the engine called that
UNKNOWN, nothing was retried or recovered and both legs stayed open.

These tests run the real SDK nonce/preparation bookkeeping with a fake signer,
fake HTTP and an API nonce model (the next nonce advances only after a send).
"""
import asyncio
import json
from decimal import Decimal
from types import SimpleNamespace

import pytest

from risex_spread_shadow.hood_handoff import ContractError, Outcome, load_saved_cycle_report, run_random_cycle
from risex_spread_shadow.hood_handoff.contracts import (
    LegReconciliation, TradeReceipt, TransientStreamContractError,
)
from risex_spread_shadow.hood_handoff.engine import HandoffEngine
from risex_spread_shadow.hood_handoff.operator_view import position_line
from risex_spread_shadow.hood_handoff.telegram_cards import residual_details
from test_hood_handoff_random_cycle import AdvancingClock, FixedRng, cycle_config
from test_hood_handoff_sdk_interface import FakeHttp, FakeSigner, _constant_nonce_client
from test_hood_ticks_ack import AckClient
from test_hood_ws_confirmed import WsCycleClient

PREPARATION = 'order preparation failed before source exposure: '
STREAM = 'WS_ADMISSION_STOP: pre-source stream unavailable: contract_error'


class ApiNonceSigner(FakeSigner):
    """The venue's next nonce advances only after a transaction was sent."""

    def __init__(self, nonces, **kwargs):
        super().__init__(**kwargs)
        account = kwargs['account_index']

        class Manager:
            async def async_next_nonce(self, key):
                return key, nonces[account]

        self.nonce_manager = Manager()


def sdk_client(base=AckClient, *, stream_fail_from=None, stream_fail_until=None, transient=False,
               prepare_failure=None, slow_book_after_reservations=None):
    class Client(base):
        def __init__(self, clock):
            super().__init__(clock)
            self.nonces = {11: 100, 22: 200}
            self.adapter = _constant_nonce_client(prefix='four-wallet', http_factory=FakeHttp)
            self.adapter._signer_factory = lambda **kwargs: ApiNonceSigner(self.nonces, **kwargs)
            self.adapter._lighter = lambda: SimpleNamespace()
            self.admissions = 0
            self.reservations = 0
            self.reservation_errors = []
            self.prepare_calls = []
            self.sent = []
            self.slowed = False

        def begin_ws_admission(self):
            self.admissions += 1
            if (stream_fail_from is not None and self.admissions >= stream_fail_from
                    and (stream_fail_until is None or self.admissions <= stream_fail_until)):
                error = TransientStreamContractError if transient else ContractError
                raise error('synthetic stream refusal before source dispatch')
            return super().begin_ws_admission()

        async def reserve_order_nonce(self, index, *, deadline):
            self.reservations += 1
            try:
                return await self.adapter.reserve_order_nonce(index, deadline=deadline)
            except Exception as exc:
                self.reservation_errors.append(str(exc))
                raise

        async def invalidate_reserved_nonce(self, token):
            await self.adapter.invalidate_reserved_nonce(token)

        async def order_book(self, market_id):
            if (slow_book_after_reservations is not None and not self.slowed
                    and self.reservations >= slow_book_after_reservations):
                # cycle-317: the closing reads finish only after the pre-quote
                # nonce reservation (freshness from its start) has expired.
                self.slowed = True
                await asyncio.sleep(0.6)
            return await super().order_book(market_id)

        async def prepare_order(self, plan, *, reserved_nonce=None):
            self.prepare_calls.append(plan)
            if prepare_failure is not None:
                error = prepare_failure(plan, self.prepare_calls)
                if error is not None:
                    raise error
            return await self.adapter.prepare_order(plan, reserved_nonce=reserved_nonce)

        async def invalidate_prepared_order(self, prepared):
            await self.adapter.invalidate_prepared_order(prepared)

        async def submit_prepared_order(self, plan, prepared, *, deadline=None):
            receipt = await self.adapter.submit_prepared_order(plan, prepared, deadline=deadline)
            if not receipt.accepted:
                return receipt
            self.nonces[plan.account_index] += 1  # the venue executed this nonce
            self.sent.append((plan.account_index, plan.order_type, plan.reduce_only))
            return await self.submit_order(plan)

    return Client


async def run(tmp_path, client, mode='ack', **overrides):
    config = cycle_config(tmp_path / 'cycle', receiver_admission=mode, **overrides)
    try:
        result = await run_random_cycle(config, client, clock=client.clock, rng=FixedRng(20, 20))
    finally:
        await client.adapter.aclose()
    return config, result


def rows(config):
    return [json.loads(line) for line in config.journal_path.read_text().splitlines()]


def closing_limits(client):
    return [s for s in client.sent if s[1] == 'LIMIT' and s[2] is True]


def recovery_markets(client):
    return [s for s in client.sent if s[1] == 'MARKET' and s[2] is True]


@pytest.mark.asyncio
@pytest.mark.parametrize('base,mode', [(AckClient, 'ack'), (WsCycleClient, 'ws_confirmed')])
async def test_dead_stream_at_closing_recovers_both_accounts_with_real_nonces(tmp_path, base, mode):
    """cycle-312: the source account's nonce must be free for reduce-only recovery."""
    client = sdk_client(base, stream_fail_from=2)(AdvancingClock())
    config, result = await run(tmp_path, client, mode)
    assert result.closing.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED
    assert list(result.closing.unknown_reasons) == [STREAM]
    assert not closing_limits(client)
    assert not client.reservation_errors, client.reservation_errors
    assert sorted(a for a, _, _ in recovery_markets(client)) == [11, 22]
    assert result.inventory == 'CONFIRMED_FLAT', result.reason
    assert result.outcome is Outcome.PARTIAL
    assert client.source_position == client.receiver_position == 0
    assert not client.adapter._nonce_reservations  # nothing stays reserved
    assert not [r for r in rows(config) if r['event'] == 'FALLBACK_PREPARATION_FAILED']


@pytest.mark.asyncio
async def test_transient_stream_refusal_at_closing_retries_with_real_nonces(tmp_path):
    """The cycle-223 retry must be able to reserve the source nonce again."""
    client = sdk_client(stream_fail_from=2, stream_fail_until=2, transient=True)(AdvancingClock())
    config, result = await run(tmp_path, client)
    assert result.outcome is Outcome.SUCCESS, result.reason
    assert result.inventory == 'CONFIRMED_FLAT'
    assert not client.reservation_errors, client.reservation_errors
    assert result.closing.attempt_index == 2
    assert not result.fallbacks
    assert not client.adapter._nonce_reservations


@pytest.mark.asyncio
async def test_expired_reservation_at_closing_retries_and_pairs(tmp_path):
    """cycle-317: the reservation expired during slow reads; nothing was sent."""
    client = sdk_client(slow_book_after_reservations=4)(AdvancingClock())
    config, result = await run(tmp_path, client, freshness_seconds=0.5)
    saved = rows(config)
    completes = [r['payload'] for r in saved if r['event'] == 'CLOSING_COMPLETE']
    first = completes[0]['result']
    assert first['outcome'] == 'PARTIAL' and first['retryable_pair'] is True, first['unknown_reasons']
    assert first['unknown_reasons'] == [PREPARATION + 'timeout']
    retry = next(r['payload'] for r in saved if r['event'] == 'PAIR_ATTEMPT_RETRY'
                 and r['payload']['phase'] == 'PAIRED_CLOSING')
    assert retry['guard']['pre_dispatch'] == 'TRANSIENT'
    assert retry['stream_settle_seconds'] == config.poll_interval_seconds
    assert config.poll_interval_seconds in client.clock.sleeps
    assert result.outcome is Outcome.SUCCESS, result.reason
    assert result.inventory == 'CONFIRMED_FLAT'
    assert result.closing.attempt_index == 2
    assert len(closing_limits(client)) == 1 and not result.fallbacks
    report = load_saved_cycle_report(config.cycle_dir)
    assert report['status'] == 'COMPLETE', report['issues']
    assert report['inventory']['status'] == 'CONFIRMED_FLAT'
    assert not client.adapter._nonce_reservations


def _closing_limit(plan):
    return plan.order_type == 'LIMIT' and plan.reduce_only


@pytest.mark.asyncio
@pytest.mark.parametrize('phase', ['opening', 'closing'])
async def test_preparation_timeout_is_retried_not_unknown(tmp_path, phase):
    def fail(plan, calls):
        wanted = _closing_limit if phase == 'closing' else (lambda p: p.order_type == 'LIMIT' and not p.reduce_only)
        if wanted(plan) and sum(1 for p in calls if wanted(p)) == 1:
            return TimeoutError('synthetic slow signing before the final barrier')
        return None
    client = sdk_client(prepare_failure=fail)(AdvancingClock())
    config, result = await run(tmp_path, client)
    assert result.outcome is Outcome.SUCCESS, result.reason
    assert result.inventory == 'CONFIRMED_FLAT'
    completed = result.opening if phase == 'opening' else result.closing
    assert completed.attempt_index == 2
    retries = [r['payload'] for r in rows(config) if r['event'] == 'PAIR_ATTEMPT_RETRY']
    assert len(retries) == 1 and retries[0]['guard']['pre_dispatch'] == 'TRANSIENT'
    assert retries[0]['reason'] == PREPARATION + 'timeout'
    assert len(client.sent) == 4  # the failed attempt sent nothing
    assert load_saved_cycle_report(config.cycle_dir)['status'] == 'COMPLETE'


@pytest.mark.asyncio
async def test_persistent_preparation_refusal_goes_to_reduce_only_recovery(tmp_path):
    def fail(plan, calls):
        return ContractError('synthetic signer refusal') if _closing_limit(plan) else None
    client = sdk_client(prepare_failure=fail)(AdvancingClock())
    config, result = await run(tmp_path, client)
    assert result.closing.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED
    assert list(result.closing.unknown_reasons) == [PREPARATION + 'contract_error']
    assert not [r for r in rows(config) if r['event'] == 'PAIR_ATTEMPT_RETRY'
                and r['payload']['phase'] == 'PAIRED_CLOSING']
    assert not closing_limits(client)
    assert sorted(a for a, _, _ in recovery_markets(client)) == [11, 22]
    assert result.inventory == 'CONFIRMED_FLAT', result.reason
    assert client.source_position == client.receiver_position == 0
    report = load_saved_cycle_report(config.cycle_dir)
    assert report['status'] == 'COMPLETE', report['issues']
    assert report['inventory']['status'] == 'CONFIRMED_FLAT'


@pytest.mark.asyncio
async def test_persistent_opening_preparation_refusal_sends_nothing_and_is_not_unknown(tmp_path):
    def fail(plan, calls):
        return ContractError('synthetic signer refusal') if not plan.reduce_only else None
    client = sdk_client(prepare_failure=fail)(AdvancingClock())
    config, result = await run(tmp_path, client)
    assert not client.sent
    assert result.opening.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED
    assert result.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED, result.reason
    assert client.source_position == client.receiver_position == 0
    assert not client.adapter._nonce_reservations


@pytest.mark.asyncio
async def test_transient_preparation_exhaustion_is_bounded_and_ends_flat(tmp_path):
    def fail(plan, calls):
        return TimeoutError('synthetic persistent timeout') if _closing_limit(plan) else None
    client = sdk_client(prepare_failure=fail)(AdvancingClock())
    config, result = await run(tmp_path, client)
    saved = rows(config)
    closing_retries = [r for r in saved if r['event'] == 'PAIR_ATTEMPT_RETRY'
                       and r['payload']['phase'] == 'PAIRED_CLOSING']
    assert len(closing_retries) == 14  # 15 closing attempts in total
    assert any(r['event'] == 'PAIR_ATTEMPT_EXHAUSTED' and r['payload']['phase'] == 'PAIRED_CLOSING'
               for r in saved)
    assert not closing_limits(client)
    assert sorted(a for a, _, _ in recovery_markets(client)) == [11, 22]
    assert result.inventory == 'CONFIRMED_FLAT', result.reason
    assert not client.adapter._nonce_reservations
    report = load_saved_cycle_report(config.cycle_dir)
    assert report['status'] == 'COMPLETE', report['issues']


@pytest.mark.asyncio
async def test_sent_orders_keep_their_nonces_and_unsent_ones_are_released(tmp_path):
    """A veto after the source send: the source nonce stays used, the receiver's is freed."""
    client = sdk_client()(AdvancingClock())
    client.adverse = 'better'  # ACK liquidity veto after the opening LIMIT was sent
    config, result = await run(tmp_path, client)
    assert (11, 'LIMIT', False) in client.sent
    assert not any(a == 22 and t == 'MARKET' and not r for a, t, r in client.sent)
    assert not client.adapter._nonce_reservations
    blocked = client.adapter._blocked_nonces.get((11, 4), set())
    assert 100 in blocked  # the sent source nonce can never be reused
    assert client.nonces[22] == 200 + sum(1 for a, _, _ in client.sent if a == 22)


@pytest.mark.asyncio
async def test_release_never_touches_a_sent_preparation():
    adapter = _constant_nonce_client(prefix='release', http_factory=FakeHttp)
    nonces = {11: 100, 22: 200}
    adapter._signer_factory = lambda **kwargs: ApiNonceSigner(nonces, **kwargs)
    adapter._lighter = lambda: SimpleNamespace()
    from test_hood_handoff_sdk_interface import _sdk_plan
    try:
        engine = HandoffEngine(adapter)
        sent = await engine._prepare_order(_sdk_plan(account_index=11, client_order_index=1))
        unsent = await engine._prepare_order(_sdk_plan(account_index=22, client_order_index=2))
        assert adapter._consume_prepared_for_send(sent)
        await engine._release_unsent_orders()
        assert sent._state == 'CONSUMED' and unsent._state == 'INVALIDATED'
        assert not adapter._nonce_reservations
        assert 100 in adapter._blocked_nonces[(11, 4)]
        assert 200 not in adapter._blocked_nonces.get((22, 4), set())
        assert engine._attempt_prepared == []
    finally:
        await adapter.aclose()


def _leg(*, dispatched=False, order=None, trades=(), before=Decimal('0.2'), after=Decimal('0.2'),
         history=True, reasons=()):
    return LegReconciliation(account_index=11, order_id=None, trades=tuple(trades), position_before=before,
                             position_after=after, order=order, history_complete=history,
                             unknown_reasons=tuple(reasons), dispatched=dispatched)


@pytest.mark.parametrize('change', [None, 'source_dispatched', 'receiver_dispatched', 'trade', 'position',
                                    'history', 'leg_reason', 'extra_reason', 'failed', 'stream_reason',
                                    'order', 'no_reasons', 'quote_expired'])
def test_pre_dispatch_retry_requires_every_zero_mutation_fact(change):
    source, receiver = _leg(), _leg()
    reasons = [PREPARATION + 'timeout']
    guard = {'status': 'UNKNOWN', 'pre_dispatch': 'TRANSIENT'}
    if change == 'source_dispatched':
        source = _leg(dispatched=True)
    if change == 'receiver_dispatched':
        receiver = _leg(dispatched=True)
    if change == 'trade':
        source = _leg(trades=(TradeReceipt(trade_id='t', account_index=11, market_id=1, order_id='o',
                                           side='BUY', quantity=Decimal('0.1'), price=Decimal('100'),
                                           fee=Decimal(0), counterparty_account_index=22,
                                           observed_at=1.0),))
    if change == 'position':
        receiver = _leg(after=Decimal('0.1'))
    if change == 'history':
        source = _leg(history=False)
    if change == 'leg_reason':
        receiver = _leg(reasons=('unknown',))
    if change == 'extra_reason':
        reasons.append('source dispatch outcome unknown: timeout')
    if change == 'failed':
        guard['pre_dispatch'] = 'FAILED'
    if change == 'stream_reason':
        reasons = [STREAM]
    if change == 'order':
        source = _leg(order=object())
    if change == 'no_reasons':
        reasons = []
    if change == 'quote_expired':
        reasons.append('source quote latency budget expired before dispatch')
    plan = SimpleNamespace(source=None)
    allowed = HandoffEngine._retryable_pair_after_guard(plan, source, receiver, reasons, guard)
    assert allowed is (change in (None, 'quote_expired'))


@pytest.mark.parametrize('reasons,zero,expected', [
    ([PREPARATION + 'timeout'], True, Outcome.FAILED_PREFLIGHT_BLOCKED),
    ([PREPARATION + 'contract_error'], True, Outcome.FAILED_PREFLIGHT_BLOCKED),
    (['source quote latency budget expired before dispatch'], True, Outcome.FAILED_PREFLIGHT_BLOCKED),
    ([STREAM], True, Outcome.FAILED_PREFLIGHT_BLOCKED),
    ([PREPARATION + 'timeout'], False, Outcome.UNKNOWN),
    ([PREPARATION + 'timeout', 'source dispatch outcome unknown: timeout'], True, Outcome.UNKNOWN),
])
def test_pre_dispatch_stop_is_preflight_only_with_zero_mutation(reasons, zero, expected):
    source = _leg() if zero else _leg(dispatched=True)
    plan = SimpleNamespace(source=None, receiver=None, quantity=Decimal('0.2'))
    engine = HandoffEngine(SimpleNamespace())
    assert engine._classify(plan, source, _leg(), reasons) is expected


def test_card_never_claims_a_partly_closed_residual_as_closed():
    report = {
        'binding': {'source_account_index': 27331, 'receiver_account_index': 34019},
        'confirmed_fills': [{'phase': 'fallback', 'counterparty_account_index': 39}],
        'dispatched_actions': [{'phase': 'fallback'}],
        'inventory': {'status': 'UNKNOWN'},
    }
    assert residual_details(report) == 'остаток закрыт reduce-only не полностью: внешние участники'
    report['inventory']['status'] = 'CONFIRMED_FLAT'
    assert residual_details(report) == 'остаток закрыт reduce-only: внешние участники'
    assert position_line(34019, '0.00000', 'BTC') == 'Счёт 34019: позиции нет.'
    assert position_line(27331, '0.00039', 'BTC') == 'Счёт 27331: LONG 0.00039 BTC.'
    assert position_line(34019, '-0.00076', 'BTC') == 'Счёт 34019: SHORT 0.00076 BTC.'
