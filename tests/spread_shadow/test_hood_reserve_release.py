"""Owner-selected reserve must reach the actual opening plan without implicit defaults."""
from decimal import Decimal as D
import json

import pytest

from risex_spread_shadow.hood_handoff import cli
from risex_spread_shadow.hood_handoff.contracts import Outcome
from test_hood_handoff_random_cycle import AdvancingClock, FixedRng, run_random_cycle
from test_hood_hcr40 import LeverageClient


POLICY = {"initial_quote": "0.10", "dispatch_quote": "0.02"}


def _value(tmp_path, reserve=POLICY):
    value = {
        "market_id": 7, "market_symbol": "BTC", "direction": "LONG",
        "source_account_index": 11, "receiver_account_index": 22,
        "cycle_dir": str(tmp_path / "cycle"), "api_key_index": 4,
    }
    if reserve is not None:
        value["margin_reserve"] = reserve
    return value


def _config(tmp_path):
    config = cli._random_cycle_config(_value(tmp_path), execute=True, plan_reviewed=True)
    cli._require_owner_opening_margin_reserve(config)
    return config


@pytest.mark.parametrize("reserve", [None,
    {"initial_quote": "0.09", "dispatch_quote": "0.02"},
    {"initial_quote": "0.10", "dispatch_quote": "0.01"}])
def test_owner_opening_refuses_missing_or_changed_reserve_before_launch(tmp_path, reserve):
    config = cli._random_cycle_config(_value(tmp_path, reserve), execute=True, plan_reviewed=True)
    with pytest.raises(SystemExit, match="initial_quote=0.10 and dispatch_quote=0.02"):
        cli._require_owner_opening_margin_reserve(config)


@pytest.mark.parametrize("reserve", [
    {"initial_quote": .10, "dispatch_quote": "0.02"},
    {"initial_quote": "0.10", "dispatch_quote": "0.02", "other": "1"},
    {"initial_quote": "NaN", "dispatch_quote": "0.02"},
    {"initial_quote": "0.01", "dispatch_quote": "0.02"},
])
def test_owner_opening_refuses_inexact_or_malformed_reserve(tmp_path, reserve):
    with pytest.raises(SystemExit, match="margin_reserve"):
        cli._random_cycle_config(_value(tmp_path, reserve), execute=True, plan_reviewed=True)


def test_owner_binding_has_exact_decimal_values_on_both_accounts(tmp_path):
    config = _config(tmp_path)
    assert config.margin_reserve.initial_quote == D("0.10")
    assert config.margin_reserve.dispatch_quote == D("0.02")
    assert config.binding()["margin_reserve"] == POLICY
    assert config.source_account_index == 11 and config.receiver_account_index == 22


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "reject", "unknown"])
async def test_parsed_owner_policy_reaches_leverage_and_fresh_budget(tmp_path, failure):
    config = _config(tmp_path)
    clock = AdvancingClock()
    client = LeverageClient(clock, fail=failure)
    result = await run_random_cycle(config, client, clock=clock, rng=FixedRng(40, 20))
    rows = [json.loads(line) for line in config.journal_path.read_text().splitlines()]
    start = next(row for row in rows if row["event"] == "CYCLE_STARTED")
    assert start["payload"]["binding"]["margin_reserve"] == POLICY
    plan = next(row for row in rows if row["event"] == "LEVERAGE_PLAN")["payload"]
    assert (plan["initial_reserve_quote"], plan["dispatch_reserve_quote"]) == ("0.10", "0.02")
    assert all(D(item['headroom']) >= D('0.10')
               for item in plan['initial_plan_observations'].values())
    assert result.selection.quantity == D("0.40")
    assert result.selection.hold_seconds == 20
    # Independent initial bound: N=.40*100.1=40.04, fee .0048048 / .014014,
    # receiver entry loss is zero at the original mark. Floor to integer bps.
    assert client.settings[0] == (11, 7, int((D('20') - D('.10') - D('.0048048'))
                                             * 10000 // D('40.04')), 0)
    if failure != 'unknown':
        assert client.settings[1] == (22, 7, int((D('24') - D('.10') - D('.014014'))
                                                 * 10000 // D('40.04')), 0)
    if failure is not None:
        assert not client.submissions
        assert result.outcome in (Outcome.FAILED_PREFLIGHT_BLOCKED, Outcome.UNKNOWN)
        return
    fresh = next(row for row in rows if row["event"] == "FRESH_OPENING_MARGIN_BUDGET")["payload"]
    assert fresh["dispatch_reserve_quote"] == "0.02"
    assert [leg["dispatch_reserve_quote"] for leg in fresh["legs"]] == ["0.02", "0.02"]
    assert [leg["status"] for leg in fresh["legs"]] == ["ADMITTED", "ADMITTED"]
    assert all(D(leg["headroom"]) >= D("0.02") for leg in fresh["legs"])
    assert result.outcome is Outcome.SUCCESS


@pytest.mark.asyncio
@pytest.mark.parametrize(('source_balance', 'expected_source'), [
    (D('5.0224024'), 'ADMITTED'), (D('5.02240239'), 'INSUFFICIENT'),
])
async def test_owner_dispatch_reserve_equal_boundary_and_one_tick_shortfall(
        tmp_path, source_balance, expected_source):
    from dataclasses import replace
    from risex_spread_shadow.hood_handoff.contracts import Direction, PreflightBlocked
    from risex_spread_shadow.hood_handoff.journal import DurableJournal
    from risex_spread_shadow.hood_handoff.random_cycle import (
        RandomCycleEngine, RandomCycleSelection, compute_quantity_bounds)
    from test_hood_handoff_random_cycle import book, metadata
    from test_hood_margin_reserve import _margin_account

    market = replace(metadata(), mark_price=D('100'), minimum_initial_margin_fraction=2500,
                     market_margin_mode=0)
    initial_source, initial_receiver = _margin_account(11, '20'), _margin_account(22, '20')
    fresh_source = _margin_account(11, str(source_balance))
    fresh_receiver = _margin_account(22, '5.0470070')

    class ReadOnlyClient:
        async def market_metadata(self, market_id): return market
        async def account_snapshot(self, account_index, market_id):
            return fresh_source if account_index == 11 else fresh_receiver
        async def order_book(self, market_id): return book()
        async def submit_order(self, *args, **kwargs):
            raise AssertionError('opening budget must not submit here')

    config = _config(tmp_path)
    bounds = compute_quantity_bounds(market, initial_source, initial_receiver, D('100.1'),
        receiver_bound=D('100.1'), direction=Direction.LONG, initial_reserve_quote=D('0.10'))
    selection = RandomCycleSelection(quantity=D('.20'), quantity_tick=20, hold_seconds=20,
        opening_source_price=D('100.1'), opening_receiver_bound=D('100.1'),
        bounds=bounds, metadata_observed_at=1000, book_observed_at=1000)
    engine = RandomCycleEngine(ReadOnlyClient(), clock=AdvancingClock())
    engine._leverage_fractions = {11: 2500, 22: 2500}
    journal = DurableJournal(config.journal_path, clock=lambda: 1000)
    if expected_source == 'INSUFFICIENT':
        with pytest.raises(PreflightBlocked, match='source selected quantity'):
            await engine._revalidate_open(config, selection, market, book(),
                                          initial_source, initial_receiver, journal=journal)
    else:
        result = await engine._revalidate_open(config, selection, market, book(),
                                               initial_source, initial_receiver, journal=journal)
        assert result[-1].quantity == D('.20')
    rows = [json.loads(line) for line in config.journal_path.read_text().splitlines()]
    legs = next(row['payload']['legs'] for row in rows if row['event'] == 'FRESH_OPENING_MARGIN_BUDGET')
    assert [leg['status'] for leg in legs] == [expected_source, 'ADMITTED']
    assert D(legs[0]['total_required']) == D('5.0024024')
    assert D(legs[1]['total_required']) == D('5.0270070')
    assert D(legs[0]['headroom']) == source_balance - D('5.0024024')
    assert D(legs[1]['headroom']) == D('0.02')
