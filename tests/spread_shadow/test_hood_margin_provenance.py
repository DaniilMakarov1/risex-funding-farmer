"""The diagnostic delta must stay bound to the actual original observation."""
from dataclasses import replace
from decimal import Decimal as D
import json

import pytest

from risex_spread_shadow.hood_handoff.contracts import Outcome
from risex_spread_shadow.hood_handoff.series import DepthLevel, OrderBookSnapshot
from test_hood_handoff_random_cycle import AdvancingClock, FixedRng, cycle_config, run_random_cycle
from test_hood_hcr40 import LeverageClient


def _events(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


@pytest.mark.asyncio
@pytest.mark.parametrize('balance_shift', [D('-.01'), D('.10')])
async def test_setting_readback_does_not_relabel_original_budget_time(tmp_path, balance_shift):
    clock = AdvancingClock()

    class SettingShiftClient(LeverageClient):
        async def update_leverage_fraction(self, *args, **kwargs):
            receipt = await super().update_leverage_fraction(*args, **kwargs)
            await self.clock.sleep(.2)
            self.balances = {11: D('20') + balance_shift, 22: D('24') + balance_shift}
            return receipt

    client = SettingShiftClient(clock)
    slot = tmp_path / 'setting-change'
    result = await run_random_cycle(cycle_config(slot), client, clock=clock,
                                    rng=FixedRng(40, 20))
    rows = _events(slot / 'cycle.jsonl')
    plan = next(row for row in rows if row['event'] == 'LEVERAGE_PLAN')['payload']['initial_plan_observations']
    fresh = next(row for row in rows if row['event'] == 'FRESH_OPENING_MARGIN_BUDGET')['payload']['legs']
    assert [leg['role'] for leg in fresh] == ['source', 'receiver']
    for leg in fresh:
        original = plan[leg['role']]
        assert leg['initial_plan_provenance'] == 'COMPLETE'
        assert leg['initial_account_index'] == original['account_index']
        assert leg['initial_account_source_identity'] == original['account_source_identity']
        assert leg['initial_account_observed_at'] == original['account_observed_at']
        assert leg['initial_metadata_observed_at'] == original['metadata_observed_at']
        assert leg['initial_book_observed_at'] == original['book_observed_at']
        assert leg['preparation_reference_account_observed_at'] > leg['initial_account_observed_at']
        assert D(leg['initial_to_fresh']['available_balance']) == balance_shift
        assert D(leg['available_balance']) - D(original['available_balance']) == balance_shift
    if balance_shift < 0:
        assert [leg['status'] for leg in fresh] == ['INSUFFICIENT', 'INSUFFICIENT']
        # Owner policy 2026-09-25: a proved fresh shortfall reduces quantity at
        # the confirmed leverage instead of stopping; nothing is sent at 0.40.
        resize = [row['payload'] for row in rows if row['event'] == 'OPENING_QUANTITY_RECALCULATED']
        assert len(resize) == 1 and resize[0]['old_quantity'] == '0.40'
        smaller = D(resize[0]['new_quantity'])
        assert D('0') < smaller < D('0.40')
        assert result.outcome is Outcome.SUCCESS, result.reason
        assert client.submissions and all(plan.quantity == smaller for plan in client.submissions)
        assert len(client.settings) == 2
    else:
        assert [leg['status'] for leg in fresh] == ['ADMITTED', 'ADMITTED']
        assert client.submissions


@pytest.mark.asyncio
async def test_pair_retry_keeps_original_mark_and_book_times(tmp_path):
    clock = AdvancingClock()

    class RetryClient(LeverageClient):
        def __init__(self, clock):
            super().__init__(clock)
            self.book_calls = 0
            self.metadata_calls = 0

        async def update_leverage_fraction(self, *args, **kwargs):
            receipt = await super().update_leverage_fraction(*args, **kwargs)
            await self.clock.sleep(.2)
            self.balances = {11: D('20.2'), 22: D('24.2')}
            return receipt

        async def market_metadata(self, market_id):
            self.metadata_calls += 1
            await self.clock.sleep(.1)
            value = await super().market_metadata(market_id)
            return replace(value, mark_price=D('100.2') if self.metadata_calls >= 3 else D('100.1'))

        async def order_book(self, market_id):
            self.book_calls += 1
            if self.book_calls == 3:
                return self._book_with_source_level(OrderBookSnapshot(
                    market_id=7, symbol='BTC',
                    bids=(DepthLevel(D('99.0'), D('100'), 'foreign-bid', 999),),
                    asks=(DepthLevel(D('100.0'), D('1'), 'better-ask', 999),),
                    observed_at=self.clock.now(), market_type='perp', venue='robinhood'))
            return await super().order_book(market_id)

    client = RetryClient(clock)
    slot = tmp_path / 'pair-retry'
    await run_random_cycle(cycle_config(slot), client, clock=clock,
                           rng=FixedRng(40, 20))
    rows = _events(slot / 'cycle.jsonl')
    assert any(row['event'] == 'PAIR_ATTEMPT_RETRY' for row in rows)
    plan = next(row for row in rows if row['event'] == 'LEVERAGE_PLAN')['payload']['initial_plan_observations']
    fresh_events = [row['payload']['legs'] for row in rows if row['event'] == 'FRESH_OPENING_MARGIN_BUDGET']
    assert len(fresh_events) >= 2
    for role in ('source', 'receiver'):
        first = next(leg for leg in fresh_events[0] if leg['role'] == role)
        second = next(leg for leg in fresh_events[1] if leg['role'] == role)
        original = plan[role]
        for leg in (first, second):
            assert leg['initial_account_observed_at'] == original['account_observed_at']
            assert leg['initial_metadata_observed_at'] == original['metadata_observed_at']
            assert leg['initial_book_observed_at'] == original['book_observed_at']
            assert leg['initial_plan_provenance'] == 'COMPLETE'
        assert second['preparation_reference_metadata_observed_at'] > original['metadata_observed_at']
        assert second['preparation_reference_book_observed_at'] > original['book_observed_at']
        assert D(second['initial_to_fresh']['mark_price']) == D('.1')
        assert D(second['mark_price']) - D(original['mark_price']) == D('.1')
