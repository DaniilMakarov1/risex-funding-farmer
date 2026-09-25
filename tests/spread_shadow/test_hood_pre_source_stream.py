"""Pre-source WS admission refusals (cycle-137 and cycle-223 class).

In both historical cycles the paired closing stopped before any order was sent
because the local stream refused admission; ACK mode then added a consequential
"ACK not proved" reason, which turned a proved zero-mutation attempt into
UNKNOWN, suppressed reduce-only recovery and left the hedged pair open.
"""
import json
from decimal import Decimal

import pytest

from risex_spread_shadow.hood_handoff import ContractError, Outcome, run_random_cycle
from risex_spread_shadow.hood_handoff.contracts import (
    LegReconciliation, OrderSnapshot, TradeReceipt, TransientStreamContractError,
)
from risex_spread_shadow.hood_handoff.engine import HandoffEngine
from test_hood_handoff_random_cycle import AdvancingClock, FixedRng, cycle_config
from test_hood_ticks_ack import AckClient
from test_hood_ws_confirmed import WsCycleClient, sdk_stream_client

REASON = 'WS_ADMISSION_STOP: pre-source stream unavailable: contract_error'


def _client_class(base, *, fail_from, fail_until=None, transient):
    class Client(base):
        admissions = 0

        def begin_ws_admission(self):
            self.admissions += 1
            if self.admissions >= fail_from and (fail_until is None or self.admissions <= fail_until):
                error = TransientStreamContractError if transient else ContractError
                raise error('WS admission observed an active order before source dispatch')
            return super().begin_ws_admission()
    return Client


def _rows(config):
    return [json.loads(line) for line in config.journal_path.read_text().splitlines()]


def _paired_sends(client, reduce_only):
    return [c for c in client.calls if c[0] == 'send' and c[1] == 'LIMIT' and c[3] is reduce_only]


@pytest.mark.asyncio
@pytest.mark.parametrize('base,mode', [(AckClient, 'ack'), (WsCycleClient, 'ws_confirmed')])
async def test_dead_stream_before_closing_source_is_zero_mutation_then_reduce_only_flat(tmp_path, base, mode):
    """cycle-137 shape: stream unusable at closing; nothing sent; must end flat."""
    clock = AdvancingClock()
    client = _client_class(base, fail_from=2, transient=False)(clock)
    config = cycle_config(tmp_path / 'cycle', receiver_admission=mode)
    result = await run_random_cycle(config, client, clock=clock, rng=FixedRng(20, 20))
    closing = result.closing
    assert closing is not None
    assert closing.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED, closing.unknown_reasons
    assert list(closing.unknown_reasons) == [REASON]  # no consequential ACK reason
    assert not closing.source.dispatched and not closing.receiver.dispatched
    assert 'PAIR_ATTEMPT_RETRY' not in [r['event'] for r in _rows(config) if r['payload'].get('phase') == 'PAIRED_CLOSING']
    assert not _paired_sends(client, True)  # no closing LIMIT was ever sent
    # Existing reduce-only recovery closes the proved opening inventory.
    assert result.fallbacks
    fallback_sends = [c for c in client.calls if c[0] == 'send' and c[1] == 'MARKET' and c[3] is True]
    assert len(fallback_sends) == len(result.fallbacks) == 2  # reduce-only MARKET per account
    assert result.inventory == 'CONFIRMED_FLAT', result.reason
    assert result.outcome is Outcome.PARTIAL
    assert client.source_position == client.receiver_position == 0


@pytest.mark.asyncio
@pytest.mark.parametrize('base,mode', [(AckClient, 'ack'), (WsCycleClient, 'ws_confirmed')])
@pytest.mark.parametrize('phase', ['opening', 'closing'])
async def test_transient_stream_refusal_retries_after_settle_and_pairs(tmp_path, base, mode, phase):
    """cycle-223 shape: stale active own order in the stream view; converges."""
    fail = 1 if phase == 'opening' else 2
    clock = AdvancingClock()
    client = _client_class(base, fail_from=fail, fail_until=fail, transient=True)(clock)
    config = cycle_config(tmp_path / 'cycle', receiver_admission=mode)
    result = await run_random_cycle(config, client, clock=clock, rng=FixedRng(20, 20))
    assert result.outcome is Outcome.SUCCESS, result.reason
    assert result.inventory == 'CONFIRMED_FLAT'
    rows = _rows(config)
    retries = [r['payload'] for r in rows if r['event'] == 'PAIR_ATTEMPT_RETRY']
    assert len(retries) == 1
    assert retries[0]['phase'] == ('PAIRED_OPENING' if phase == 'opening' else 'PAIRED_CLOSING')
    assert retries[0]['reason'] == REASON
    assert retries[0]['guard']['pre_source_stream'] == 'TRANSIENT'
    assert retries[0]['stream_settle_seconds'] == config.poll_interval_seconds
    assert config.poll_interval_seconds in clock.sleeps
    completed = result.opening if phase == 'opening' else result.closing
    assert completed.attempt_index == 2 and completed.outcome is Outcome.SUCCESS
    # The refused attempt sent nothing: one LIMIT + one MARKET per phase.
    assert len(client.submissions) == 4
    assert len({p.client_order_index for p in client.submissions}) == 4
    assert not result.fallbacks


@pytest.mark.asyncio
async def test_transient_refusal_exhaustion_is_bounded_and_ends_flat(tmp_path):
    clock = AdvancingClock()
    client = _client_class(AckClient, fail_from=2, transient=True)(clock)
    config = cycle_config(tmp_path / 'cycle', receiver_admission='ack')
    result = await run_random_cycle(config, client, clock=clock, rng=FixedRng(20, 20))
    rows = _rows(config)
    closing_retries = [r for r in rows if r['event'] == 'PAIR_ATTEMPT_RETRY' and r['payload']['phase'] == 'PAIRED_CLOSING']
    assert len(closing_retries) == 14  # 15 total closing attempts including the first
    assert any(r['event'] == 'PAIR_ATTEMPT_EXHAUSTED' and r['payload']['phase'] == 'PAIRED_CLOSING' for r in rows)
    assert client.admissions == 16  # 1 opening + 15 closing
    assert not _paired_sends(client, True)
    assert result.fallbacks
    assert all(c[1] == 'MARKET' for c in client.calls if c[0] == 'send' and c[3] is True)
    assert result.inventory == 'CONFIRMED_FLAT', result.reason
    assert client.source_position == client.receiver_position == 0


@pytest.mark.asyncio
async def test_ack_dead_stream_at_opening_is_preflight_not_unknown(tmp_path):
    clock = AdvancingClock()
    client = _client_class(AckClient, fail_from=1, transient=False)(clock)
    result = await run_random_cycle(cycle_config(tmp_path / 'cycle', receiver_admission='ack'),
                                    client, clock=clock, rng=FixedRng(20, 20))
    assert not client.submissions
    assert result.opening.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED
    assert list(result.opening.unknown_reasons) == [REASON]
    assert result.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED, result.reason
    assert client.source_position == client.receiver_position == 0


@pytest.mark.asyncio
async def test_ack_failed_source_send_still_reports_ack_not_proved(tmp_path):
    """The ACK reason is kept whenever a source send was actually attempted."""
    clock = AdvancingClock()
    client = AckClient(clock, 'negative_ack', False)
    result = await run_random_cycle(cycle_config(tmp_path / 'cycle', receiver_admission='ack'),
                                    client, clock=clock, rng=FixedRng(20, 20))
    reasons = [reason for attempt in [result.opening] for reason in attempt.unknown_reasons]
    assert 'ACK_ADMISSION_STOP: positive application ACK is not proved' in reasons


def _leg(*, dispatched=False, order=None, trades=(), before=Decimal('0.2'), after=Decimal('0.2'),
         history=True, reasons=()):
    return LegReconciliation(account_index=11, order_id=None, trades=tuple(trades), position_before=before,
                             position_after=after, order=order, history_complete=history,
                             unknown_reasons=tuple(reasons), dispatched=dispatched)


@pytest.mark.parametrize('change', [None, 'source_dispatched', 'receiver_dispatched', 'trade', 'position',
                                    'history', 'leg_reason', 'extra_reason', 'unavailable', 'order'])
def test_transient_retry_requires_every_zero_mutation_fact(change):
    source, receiver = _leg(), _leg()
    reasons = [REASON]
    guard = {'status': 'UNKNOWN', 'pre_source_stream': 'TRANSIENT'}
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
        reasons.append('ACK_ADMISSION_STOP: positive application ACK is not proved')
    if change == 'unavailable':
        guard['pre_source_stream'] = 'UNAVAILABLE'
    if change == 'order':
        source = _leg(order=object())
    from types import SimpleNamespace
    plan = SimpleNamespace(source=None)
    allowed = HandoffEngine._retryable_pair_after_guard(plan, source, receiver, reasons, guard)
    assert allowed is (change is None)


@pytest.mark.asyncio
@pytest.mark.parametrize('case,transient', [('stale_book', True), ('active_order', True),
                                           ('malformed', False), ('no_stream', False)])
async def test_sdk_marks_only_convergence_refusals_transient(case, transient):
    import time
    from test_hood_ws_reads import put_order, order
    c, s = await sdk_stream_client()
    if case == 'stale_book':
        s.reads.book_at = time.monotonic() - 1
    if case == 'active_order':
        await put_order(s, order())
    if case == 'malformed':
        s.observer.malformed += 1
    if case == 'no_stream':
        c._read_stream_state = None
    with pytest.raises(ContractError) as err:
        c.begin_ws_admission()
    assert isinstance(err.value, TransientStreamContractError) is transient
