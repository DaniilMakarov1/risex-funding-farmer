"""HCR-40 adverse coverage for occupied margin and setting reconciliation."""
from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
import json

import pytest

from risex_spread_shadow.hood_handoff.contracts import AccountMarginEvidence, MutationReceipt, Outcome, PreflightBlocked
from risex_spread_shadow.hood_handoff.operator_recovery import allocate_close_slot, close_positions, inspect_current
from risex_spread_shadow.hood_handoff.random_cycle import (
    minimal_sufficient_leverage_fraction, select_random_quantity,
)
from test_hood_handoff_random_cycle import (
    AdvancingClock, CycleClient, FixedRng, account, cycle_config, metadata, run_random_cycle,
)
from test_hood_operator_recovery import RecoveryClient


def test_minimal_fraction_uses_exact_free_balance_and_integer_venue_precision():
    assert minimal_sufficient_leverage_fraction(Decimal("20"), Decimal("40"), 200) == 5000
    assert minimal_sufficient_leverage_fraction(Decimal("24"), Decimal("40"), 200) == 6000
    assert minimal_sufficient_leverage_fraction(Decimal("20"), Decimal("20"), 200) == 10000
    assert minimal_sufficient_leverage_fraction(Decimal("20"), Decimal("40.01"), 200) == 4998
    with pytest.raises(PreflightBlocked, match="more than supported 4x"):
        minimal_sufficient_leverage_fraction(Decimal("20"), Decimal("80.01"), 200)
    with pytest.raises(PreflightBlocked, match="more than supported 4x"):
        minimal_sufficient_leverage_fraction(Decimal("20"), Decimal("40"), 6000)


def test_market_leverage_limit_caps_random_quantity_before_draw():
    from risex_spread_shadow.hood_handoff.random_cycle import compute_quantity_bounds
    market = replace(metadata(), minimum_initial_margin_fraction=6000)
    source = account(11, Decimal(0), available_balance=Decimal("20"))
    receiver = account(22, Decimal(0), available_balance=Decimal("24"))
    bounds = compute_quantity_bounds(market, source, receiver, Decimal("100"))
    assert bounds.upper_tick == 33  # floor(20 / .6 / 100 / .01)
    assert bounds.upper_quantity == Decimal("0.33")


def test_hold_range_is_exactly_twenty_through_one_eighty():
    from risex_spread_shadow.hood_handoff.random_cycle import compute_quantity_bounds
    bounds = compute_quantity_bounds(metadata(), account(11, Decimal(0)), account(22, Decimal(0)), Decimal("100.1"))
    assert select_random_quantity(bounds, FixedRng(10, 180))[-1] == 180
    with pytest.raises(Exception, match="must not exceed 180"):
        select_random_quantity(bounds, FixedRng(10), hold_seconds=181)


class LeverageClient(CycleClient):
    def __init__(self, clock, *, fail=None):
        super().__init__(clock)
        self.balances = {11: Decimal("20"), 22: Decimal("24")}
        self.fractions = {11: 4166, 22: 10000}
        self.settings = []
        self.fail = fail

    async def market_metadata(self, market_id):
        return replace(await super().market_metadata(market_id),
                       minimum_initial_margin_fraction=200, market_margin_mode=0)

    async def account_snapshot(self, account_index, market_id):
        old = await super().account_snapshot(account_index, market_id)
        balance = self.balances[account_index]
        fraction = self.fractions[account_index]
        evidence = AccountMarginEvidence.from_response(
            account_index=account_index, market_id=market_id,
            source_identity=old.source_identity, observed_at=self.clock.now(),
            selected_position={"market_id": market_id, "initial_margin_fraction": f"{Decimal(fraction) / 100:.2f}", "margin_mode": 0},
            account={"cross_asset_value": format(balance, "f")},
        )
        return replace(old, margin_available=balance, available_balance=balance,
                       margin_required=Decimal(0), margin_evidence=evidence)

    async def update_leverage_fraction(self, account_index, market_id, fraction, margin_mode=0):
        self.settings.append((account_index, market_id, fraction, margin_mode))
        if self.fail == "reject" and account_index == 22:
            return MutationReceipt(False, None, None, "venue rejected")
        if self.fail == "unknown":
            raise TimeoutError("ambiguous setting transport")
        if self.fail != "stale":
            self.fractions[account_index] = fraction
        return MutationReceipt(True, None, "synthetic-hash", None)


@pytest.mark.asyncio
async def test_quantity_first_then_minimal_fraction_for_unequal_accounts(tmp_path):
    clock = AdvancingClock(); client = LeverageClient(clock)
    result = await run_random_cycle(cycle_config(tmp_path / "cycle-001"), client,
                                    clock=clock, rng=FixedRng(40, 20))
    assert result.outcome is Outcome.SUCCESS, result.reason
    assert result.selection.quantity == Decimal("0.40")
    assert client.settings == [(11, 7, 4995, 0), (22, 7, 5994, 0)]
    assert len(client.submissions) > 0
    rows = [json.loads(line) for line in (tmp_path / "cycle-001" / "cycle.jsonl").read_text().splitlines()]
    assert [row["event"] for row in rows].count("LEVERAGE_UPDATE_CONFIRMED") == 2


@pytest.mark.asyncio
async def test_second_setting_rejection_stops_before_orders_without_rollback(tmp_path):
    clock = AdvancingClock(); client = LeverageClient(clock, fail="reject")
    result = await run_random_cycle(cycle_config(tmp_path / "cycle-001"), client,
                                    clock=clock, rng=FixedRng(40, 20))
    assert result.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED
    assert client.settings == [(11, 7, 4995, 0), (22, 7, 5994, 0)]
    assert client.fractions == {11: 4995, 22: 10000}
    assert not client.submissions


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["unknown", "stale"])
async def test_unknown_or_unproved_setting_blocks_next_run_but_allows_close(tmp_path, failure):
    clock = AdvancingClock(); client = LeverageClient(clock, fail=failure)
    result = await run_random_cycle(cycle_config(tmp_path / "cycle-001"), client,
                                    clock=clock, rng=FixedRng(40, 20))
    assert result.outcome is Outcome.UNKNOWN
    assert len(client.settings) == 1 and not client.submissions
    with pytest.raises(PreflightBlocked, match="leverage setting is unresolved"):
        await inspect_current(cycle_config(tmp_path / "cycle-002"), client, tmp_path,
                              require_flat=True, clock=clock)
    _, _, proof = await inspect_current(cycle_config(tmp_path / "cycle-002"), client, tmp_path,
                                        require_flat=False, clock=clock)
    assert proof["unresolved_leverage_settings"] == 1


class OccupiedMarginClient(RecoveryClient):
    def __init__(self, clock):
        super().__init__(clock, source="0.20")

    async def account_snapshot(self, account_index, market_id):
        observed = await super().account_snapshot(account_index, market_id)
        if account_index == 11:
            return replace(observed, margin_available=Decimal("1"),
                           available_balance=Decimal("1"), margin_required=Decimal("19"))
        return observed


@pytest.mark.asyncio
async def test_occupied_margin_allows_explicit_reduce_only_close_and_run_refuses_position(tmp_path):
    clock = AdvancingClock(); client = OccupiedMarginClient(clock)
    with pytest.raises(PreflightBlocked, match="positions remain; use /close"):
        await inspect_current(cycle_config(tmp_path / "new-cycle"), client, tmp_path,
                              require_flat=True, clock=clock)
    slot = allocate_close_slot(tmp_path)
    result = await close_positions(cycle_config(slot), client, tmp_path, slot, clock=clock)
    assert result["status"] == "CONFIRMED_FLAT", result
    assert len(client.submissions) == 1
    assert client.submissions[0].reduce_only is True
    assert client.submissions[0].quantity == Decimal("0.20")
