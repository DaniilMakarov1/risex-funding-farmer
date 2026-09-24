"""Receipt reconciliation regressions derived from the cycle-138 journal."""
from dataclasses import replace
from decimal import Decimal

import pytest

from risex_spread_shadow.hood_handoff import HistoryPage, Outcome, run_handoff
from test_hood_handoff_engine import FakeClient, make_config


class Clock:
    value = 1000.0
    def now(self): return self.value
    async def sleep(self, seconds): self.value += seconds


class ClockAheadHistory(FakeClient):
    def __init__(self, clock, mode='exact'):
        super().__init__(source_fills=True)
        self.clock = clock
        self.mode = mode
        self.history_reads = 0

    async def list_trades(self, account_index, market_id, **kwargs):
        page = await super().list_trades(account_index, market_id, **kwargs)
        if account_index != self.source_account_index:
            return page
        self.history_reads += 1
        if self.history_reads > 1 and self.mode != 'never_catches_up':
            self.clock.value = 1002.1
        if self.history_reads > 1 and self.mode == 'disappears':
            return HistoryPage()
        changes = {'observed_at': 1002.0}
        if self.history_reads > 1:
            if self.mode == 'timestamp_changed': changes['observed_at'] = 1001.0
            if self.mode == 'identity_changed': changes['trade_id'] = 'other-trade'
            if self.mode == 'price_changed': changes['price'] = Decimal('99')
        return replace(page, trades=tuple(replace(t, **changes) for t in page.trades))


@pytest.mark.asyncio
async def test_same_receipt_reread_after_clock_catches_up_resolves_transient_error(tmp_path):
    clock = Clock()
    client = ClockAheadHistory(clock)
    result = await run_handoff(make_config(tmp_path/'result.jsonl', max_poll_count=4,
                              reconcile_timeout_seconds=5), client, clock=clock)
    assert result.outcome is Outcome.SUCCESS, result.unknown_reasons
    assert result.source.history_complete
    assert result.source.trades[0].observed_at == 1002.0
    assert result.source.filled_quantity == Decimal('.125')
    assert result.source.position_after == Decimal('.875')
    assert client.history_reads == 2
    assert len(client.submissions) == 2
    assert not client.cancellations


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['never_catches_up', 'timestamp_changed', 'identity_changed',
                                 'price_changed', 'disappears'])
async def test_future_receipt_requires_exact_valid_reread_within_bounds(tmp_path, mode):
    clock = Clock()
    client = ClockAheadHistory(clock, mode)
    result = await run_handoff(make_config(tmp_path/'result.jsonl', max_poll_count=4,
                              reconcile_timeout_seconds=5), client, clock=clock)
    assert result.outcome is Outcome.UNKNOWN
    assert not result.source.history_complete
    assert 'trade receipt is from the future' in result.source.unknown_reasons
    assert client.history_reads <= 4
    assert len(client.submissions) == 2
    assert not client.cancellations


@pytest.mark.asyncio
async def test_cycle_reconciles_clock_ahead_opening_receipt_then_closes_inventory(tmp_path):
    from test_hood_ticks_ack import AckClient
    from test_hood_handoff_random_cycle import AdvancingClock, FixedRng, cycle_config, run_random_cycle

    clock = AdvancingClock()
    class Client(AckClient):
        stamps = {}
        async def list_trades(self, account_index, market_id, **kwargs):
            page = await super().list_trades(account_index, market_id, **kwargs)
            if not page.trades:
                return page
            key = (account_index, kwargs.get('order_id'))
            if key not in self.stamps:
                self.stamps[key] = clock.now() + .2
            else:
                await clock.sleep(.3)
            return replace(page, trades=tuple(replace(t, observed_at=self.stamps[key]) for t in page.trades))
    client = Client(clock)
    result = await run_random_cycle(cycle_config(tmp_path/'cycle', receiver_admission='ack'),
                                   client, clock=clock, rng=FixedRng(20, 20))
    assert result.outcome is Outcome.SUCCESS, result.reason
    assert result.inventory == 'CONFIRMED_FLAT'
    assert result.remaining_source_position == result.remaining_receiver_position == 0
    assert len([c for c in client.calls if c[0] == 'send']) == 4


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['exact', 'future', 'changed', 'duplicate'])
async def test_residual_close_rereads_future_receipt_without_resending_order(tmp_path, mode):
    from test_hood_ack_recovery import LaggingFallback
    from test_hood_handoff_random_cycle import AdvancingClock, FixedRng, cycle_config, run_random_cycle
    clock = AdvancingClock()
    class Client(LaggingFallback):
        timestamp = None
        async def list_trades(self, a, m, **kwargs):
            page = await super().list_trades(a, m, **kwargs)
            if not self.fallback_plans or not page.trades:
                return page
            if self.timestamp is None:
                self.timestamp = clock.now() + (.2 if mode != 'future' else 100)
            else:
                await clock.sleep(.3)
            stamp = self.timestamp
            if mode == 'changed' and self.history_reads > 1:
                stamp -= .1
            trades = tuple(replace(t, observed_at=stamp) for t in page.trades)
            if mode == 'duplicate': trades = trades + trades
            return replace(page, trades=trades)
    client = Client(clock, 'time')
    result = await run_random_cycle(cycle_config(tmp_path/'cycle'), client,
                                   clock=clock, rng=FixedRng(20, 20))
    assert len(client.fallback_plans) == 1
    assert client.fallback_plans[0].reduce_only
    if mode == 'exact':
        assert result.inventory == 'CONFIRMED_FLAT', result.reason
        assert result.fallbacks[0].filled_quantity == Decimal('.20')
        assert result.fallbacks[0].position_after == 0
    else:
        assert result.outcome is Outcome.UNKNOWN
        assert result.inventory != 'CONFIRMED_FLAT'
