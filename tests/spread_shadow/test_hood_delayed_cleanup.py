"""Cycle-158: accepted maker publication can lag an ACK admission veto."""
import asyncio
from dataclasses import replace
from decimal import Decimal

import pytest

from risex_spread_shadow.hood_handoff import ContractError, Direction, HandoffEngine, Outcome, TradeReceipt
from risex_spread_shadow.hood_handoff.journal import DurableJournal
from test_hood_ticks_ack import AckClient
from test_hood_handoff_random_cycle import AdvancingClock, FixedRng, cycle_config, run_random_cycle
from test_hood_ack_recovery import rows


class DelayedCleanup(AckClient):
    def __init__(self, clock, closing, mode='missing', fraction=Decimal('0')):
        super().__init__(clock, 'better', closing)
        self.mode = mode
        self.fraction = fraction
        self.cleanup_reads = {}
        self.injected = set()

    def external_fill(self, order):
        if order.order_id in self.injected or not self.fraction:
            return
        self.injected.add(order.order_id)
        quantity = order.initial_quantity * self.fraction
        self.source_position += quantity if order.side == 'BUY' else -quantity
        self._replace_order(order, filled_quantity=quantity,
                            remaining_quantity=order.initial_quantity-quantity,
                            status='filled' if self.fraction == 1 else 'open')
        self.trades[order.order_id] = (TradeReceipt(
            'outside-'+order.order_id, order.account_index, order.market_id, order.order_id,
            order.side, quantity, order.price, None, 999, self.clock.now(),
            counterparty_order_id='outside', client_order_index=order.client_order_index,
        ),)

    async def lookup_order(self, *args, **kwargs):
        value = await super().lookup_order(*args, **kwargs)
        if value is None or value.order_type != 'LIMIT' or value.reduce_only != self.fail_closing:
            return value
        count = self.cleanup_reads.get(value.order_id, 0) + 1
        self.cleanup_reads[value.order_id] = count
        if count == 1:
            if self.mode != 'during_cancel':
                self.external_fill(value)
            if self.mode == 'transport':
                raise TimeoutError('synthetic delayed read')
            if self.mode == 'stale':
                return replace(value, observed_at=self.clock.now()-100)
            if self.mode == 'conflict':
                return replace(value, client_order_index=value.client_order_index+1)
            if self.mode == 'contract':
                raise ContractError('synthetic identity conflict')
            return None
        if self.mode == 'never':
            return None
        return value

    async def cancel_order(self, account, market, order):
        if self.mode == 'during_cancel':
            self.external_fill(self.orders[(account, order)])
        return await super().cancel_order(account, market, order)


@pytest.mark.asyncio
@pytest.mark.parametrize('direction', list(Direction))
@pytest.mark.parametrize('closing', [False, True])
@pytest.mark.parametrize('mode,fraction', [
    ('missing', '0'), ('transport', '0'), ('stale', '0'),
    ('missing', '0.5'), ('missing', '1'), ('during_cancel', '0.5'), ('during_cancel', '1'),
])
async def test_delayed_maker_veto_cleans_up_exactly_once(tmp_path, direction, closing, mode, fraction):
    clock = AdvancingClock()
    client = DelayedCleanup(clock, closing, mode, Decimal(fraction))
    result = await run_random_cycle(cycle_config(tmp_path/'cycle', direction=direction,
                                   receiver_admission='ack'), client, clock=clock, rng=FixedRng(20,20))
    phase = result.closing if closing else result.opening
    assert phase is not None
    assert not phase.receiver.dispatched
    assert result.inventory == 'CONFIRMED_FLAT', result.reason
    assert client.source_position == client.receiver_position == Decimal('0')
    assert not any(order.active for order in client.orders.values())
    assert len(client.cancellations) == len(set(client.cancellations))
    if Decimal(fraction):
        assert phase.source.filled_quantity == Decimal('.20') * Decimal(fraction)
        assert result.fallbacks and all(p.reduce_only for p in client.fallback_plans)
        assert len([p for p in client.submissions if p.order_type == 'LIMIT' and p.reduce_only == closing]) == 1
    events = rows(tmp_path/'cycle'/('closing.jsonl' if closing else 'opening.jsonl'))
    assert any(e['event'] == 'ORDER_OBSERVED' and e['payload'].get('purpose') == 'source_cleanup'
               and e['payload']['poll'] == 2 for e in events)


@pytest.mark.asyncio
@pytest.mark.parametrize('closing', [False, True])
@pytest.mark.parametrize('mode', ['never', 'conflict', 'contract'])
async def test_unproved_maker_never_retries_or_closes_over_live_order(tmp_path, closing, mode):
    clock = AdvancingClock(); client = DelayedCleanup(clock, closing, mode)
    result = await run_random_cycle(cycle_config(tmp_path/'cycle', receiver_admission='ack'),
                                    client, clock=clock, rng=FixedRng(20,20))
    phase = result.closing if closing else result.opening
    assert phase is not None and not phase.receiver.dispatched
    assert result.outcome is Outcome.UNKNOWN
    assert not client.cancellations and not result.fallbacks
    assert len([p for p in client.submissions if p.order_type == 'LIMIT' and p.reduce_only == closing]) == 1


@pytest.mark.asyncio
async def test_cleanup_read_has_total_wall_deadline_and_no_mutation(tmp_path):
    clock = AdvancingClock(); client = DelayedCleanup(clock, False)
    entered = asyncio.Event(); cancelled = asyncio.Event()
    async def hung(*args, **kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()
    client.lookup_order = hung
    engine = HandoffEngine(client, clock=clock)
    engine._configured_order_timeout = .02
    engine._configured_request_timeout = 1
    engine._configured_poll_limit = 100
    # A plan is immaterial to a read that never returns.
    from types import SimpleNamespace
    plan = SimpleNamespace(account_index=11, market_id=7, client_order_index=123, side='BUY')
    journal = DurableJournal(tmp_path/'cleanup.jsonl')
    value = await asyncio.wait_for(engine._lookup_cleanup_order(plan, None, journal, journal.run_id), .5)
    assert value is None and entered.is_set() and cancelled.is_set()
    assert not client.submissions and not client.cancellations


@pytest.mark.asyncio
@pytest.mark.parametrize('bad_field,value', [
    ('account_index', 999), ('market_id', 999), ('client_order_index', 999),
    ('order_id', 'other'), ('reduce_only', True), ('price', Decimal('99')),
])
async def test_cleanup_identity_conflict_is_a_permanent_barrier(tmp_path, bad_field, value):
    from test_hood_cycle075_regressions import snapshot
    from risex_spread_shadow.hood_handoff import OrderPlan
    order = replace(snapshot(), market_id=7)
    plan = OrderPlan(account_index=11, market_id=7, side='BUY', quantity=Decimal('.20'),
                     price=Decimal('100'), order_type='LIMIT', time_in_force='POST_ONLY',
                     reduce_only=False, client_order_index=77, quantity_int=20, price_int=1000,
                     order_expiry_ms=9999999999999)
    calls = []
    class Client:
        async def lookup_order(self, *args, **kwargs):
            calls.append(kwargs)
            return replace(order, **{bad_field: value}) if len(calls) == 1 else order
    engine = HandoffEngine(Client())
    engine._configured_freshness = 10
    journal = DurableJournal(tmp_path/'cleanup.jsonl')
    with pytest.raises(ContractError, match='identity/parameters conflict'):
        await engine._lookup_cleanup_order(plan, '123', journal, journal.run_id)
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('limit,timeout,expected_calls', [(2, 10, 2), (100, .015, 2)])
async def test_missing_cleanup_has_count_and_clock_bounds(tmp_path, limit, timeout, expected_calls):
    from types import SimpleNamespace
    clock = AdvancingClock(); calls = []
    class Client:
        async def lookup_order(self, *args, **kwargs):
            calls.append(kwargs)
            return None
    engine = HandoffEngine(Client(), clock=clock)
    engine._configured_poll_limit = limit
    engine._configured_order_timeout = timeout
    engine._configured_poll_interval = .01
    plan = SimpleNamespace(account_index=11, market_id=7, client_order_index=123, side='BUY')
    journal = DurableJournal(tmp_path/'cleanup.jsonl')
    assert await engine._lookup_cleanup_order(plan, '123', journal, journal.run_id) is None
    assert len(calls) == expected_calls
    assert sum(clock.sleeps) <= timeout + 1e-9
    assert all(c == {'order_id': '123', 'client_order_index': 123} for c in calls)


@pytest.mark.asyncio
async def test_interrupted_cleanup_propagates_cancellation(tmp_path):
    from types import SimpleNamespace
    class Client:
        async def lookup_order(self, *args, **kwargs):
            raise asyncio.CancelledError()
    engine = HandoffEngine(Client())
    plan = SimpleNamespace(account_index=11, market_id=7, client_order_index=123, side='BUY')
    journal = DurableJournal(tmp_path/'cleanup.jsonl')
    with pytest.raises(asyncio.CancelledError):
        await engine._lookup_cleanup_order(plan, None, journal, journal.run_id)


@pytest.mark.asyncio
async def test_known_source_disappears_during_cancel_recheck_without_resend(tmp_path):
    from test_hood_cycle075_regressions import snapshot
    from risex_spread_shadow.hood_handoff import OrderPlan
    order = replace(snapshot(), market_id=7)
    plan = OrderPlan(account_index=11, market_id=7, side='BUY', quantity=Decimal('.20'),
                     price=Decimal('100'), order_type='LIMIT', time_in_force='POST_ONLY',
                     reduce_only=False, client_order_index=77, quantity_int=20, price_int=1000,
                     order_expiry_ms=9999999999999)
    class Client:
        reads = 0
        cancels = 0
        async def lookup_order(self, *args, **kwargs):
            self.reads += 1
            return None if self.reads == 1 else order
        async def cancel_order(self, account, market, order_id):
            assert (account, market, order_id) == (11, 7, '123')
            self.cancels += 1
            raise TimeoutError('cancel response lost')
    client = Client(); engine = HandoffEngine(client)
    engine._configured_freshness = 10
    engine._configured_request_timeout = 1
    engine._configured_poll_interval = .001
    journal = DurableJournal(tmp_path/'cleanup.jsonl'); errors = []
    try:
        for _ in range(2):
            await engine._cancel_if_safe(plan, order, journal, journal.run_id, errors,
                                         expected_order_id='123')
        assert client.reads == 2 and client.cancels == 1
        assert errors
        assert len([e for e in journal.events if e.event == 'CANCEL_DISPATCH_INTENT']) == 1
    finally:
        await engine._finish_cancel_preparation()
