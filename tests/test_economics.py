from datetime import UTC, datetime
from decimal import Decimal

import pytest

from risex_farmer.economics import (
    applied_rate_complete,
    authoritative_funding_cash_usd,
    closed_net_pnl_usd,
    common_canonical_quantity_step,
    exact_quantity_vwap,
    funding_cash_usd,
    maker_price,
    minimum_order_eligible,
    pair_price_pnl_usd,
    planned_maker_net_pnl_usd,
    price_pnl_usd,
    recognized_funding_cash_usd,
    replace_funding_settlement,
    sized_canonical_quantity,
    spread_ticks,
    venue_fee_amount_usd,
)
from risex_farmer.models import (
    BookLevel,
    CanonicalMarket,
    ContractType,
    FundingSettlement,
    LifecycleState,
    LiquidityRole,
    MarketType,
    PositionSide,
    SettlementStatus,
    Side,
    Venue,
)


D = Decimal
NOW = datetime(2026, 1, 1, tzinfo=UTC)


def market(
    *, multiplier: str = "1", minimum_quantity: str = "0.01", minimum_notional: str = "10"
) -> CanonicalMarket:
    return CanonicalMarket(
        canonical_asset="BTC",
        venue=Venue.RISEX,
        venue_symbol="BTC-USD",
        market_type=MarketType.PERPETUAL,
        contract_type=ContractType.LINEAR,
        base_multiplier=D(multiplier),
        quote_asset="USD",
        settlement_asset="USD",
        tick_size_raw=D("0.1"),
        quantity_step_raw=D("0.001"),
        minimum_quantity_raw=D(minimum_quantity),
        minimum_notional_usd=D(minimum_notional),
        minimum_fee_notional_usd=None,
        is_active=True,
        is_rfq=False,
        is_off_hours=False,
    )


def settlement(status: SettlementStatus, cash: str | None = None) -> FundingSettlement:
    return FundingSettlement(
        venue=Venue.RISEX,
        canonical_market="BTC",
        settlement_at=NOW,
        status=status,
        cash_usd=None if cash is None else D(cash),
    )


def test_float_inputs_are_rejected() -> None:
    with pytest.raises(TypeError, match="Decimal"):
        maker_price(Side.BUY, 100.0, D("101"), D("1"))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("side", "ask", "expected"),
    [
        (Side.BUY, "101", "100"),
        (Side.SELL, "101", "101"),
        (Side.BUY, "102", "101"),
        (Side.SELL, "102", "101"),
        (Side.BUY, "103", "101"),
        (Side.SELL, "103", "102"),
    ],
)
def test_normal_maker_placement(side: Side, ask: str, expected: str) -> None:
    assert maker_price(side, D("100"), D(ask), D("1")) == D(expected)


def test_invalid_bbo_and_off_tick_are_rejected() -> None:
    with pytest.raises(ValueError, match="below"):
        spread_ticks(D("100"), D("100"), D("1"))
    with pytest.raises(ValueError, match="aligned"):
        spread_ticks(D("100.5"), D("102"), D("1"))


def test_common_canonical_step_handles_integer_and_fractional_multipliers() -> None:
    assert common_canonical_quantity_step(
        [(D("0.001"), D("1")), (D("0.001"), D("1000")), (D("0.001"), D("0.1"))]
    ) == D("1")
    assert common_canonical_quantity_step(
        [(D("0.01"), D("0.1")), (D("0.001"), D("1"))]
    ) == D("0.001")
    assert sized_canonical_quantity(D("500"), D("125"), D("0.3")) == D("3.9")


def test_minimum_quantity_and_notional_are_both_enforced() -> None:
    m = market(multiplier="1000", minimum_quantity="0.01", minimum_notional="100")
    assert not minimum_order_eligible(D("9"), D("20"), m)
    assert not minimum_order_eligible(D("10"), D("9"), m)
    assert minimum_order_eligible(D("10"), D("10"), m)


def test_exact_quantity_vwap_and_insufficient_depth() -> None:
    asks = (BookLevel(D("100"), D("1")), BookLevel(D("102"), D("2")))
    result = exact_quantity_vwap(Side.BUY, D("2"), (), asks)
    assert result.is_executable
    assert result.notional_usd == D("202")
    assert result.price == D("101")
    insufficient = exact_quantity_vwap(Side.BUY, D("4"), (), asks)
    assert not insufficient.is_executable
    assert insufficient.filled_quantity == D("3")
    assert insufficient.price is None


def test_nado_minimum_taker_fee_and_normal_fees() -> None:
    assert venue_fee_amount_usd(
        Venue.NADO, LiquidityRole.TAKER, D("50"), D("0.001"), D("100")
    ) == D("0.100")
    assert venue_fee_amount_usd(
        Venue.NADO, LiquidityRole.MAKER, D("50"), D("0.001"), D("100")
    ) == D("0.050")
    assert venue_fee_amount_usd(
        Venue.RISEX, LiquidityRole.TAKER, D("50"), D("0.001"), D("100")
    ) == D("0.050")


def test_long_and_short_pnl_signs() -> None:
    assert price_pnl_usd(PositionSide.LONG, D("2"), D("100"), D("110")) == D("20")
    assert price_pnl_usd(PositionSide.SHORT, D("2"), D("100"), D("110")) == D("-20")
    assert price_pnl_usd(PositionSide.LONG, D("2"), D("100"), D("90")) == D("-20")
    assert price_pnl_usd(PositionSide.SHORT, D("2"), D("100"), D("90")) == D("20")


def test_pair_execution_and_planned_net_are_derived_exactly() -> None:
    execution = pair_price_pnl_usd(D("2"), D("100"), D("105"), D("101"), D("104"))
    assert execution == D("4")
    assert planned_maker_net_pnl_usd(D("3"), execution, [D("0.5"), D("0.25")]) == D("6.25")


def test_funding_cash_multiplies_cash_per_base_exactly_once() -> None:
    assert funding_cash_usd(D("2"), D("3.50")) == D("7.00")


def test_estimate_is_replaced_by_applied_rate() -> None:
    estimated = replace_funding_settlement(
        settlement(SettlementStatus.PENDING), SettlementStatus.ESTIMATED, D("4")
    )
    applied = replace_funding_settlement(estimated, SettlementStatus.APPLIED_RATE, D("5"))
    assert authoritative_funding_cash_usd(applied) == D("5")
    assert recognized_funding_cash_usd([applied]) == D("5")


def test_unresolved_settlement_retains_an_existing_estimate() -> None:
    estimated = replace_funding_settlement(
        settlement(SettlementStatus.PENDING), SettlementStatus.ESTIMATED, D("4")
    )
    unresolved = replace_funding_settlement(estimated, SettlementStatus.UNRESOLVED, None)
    assert authoritative_funding_cash_usd(unresolved) == D("4")
    assert not applied_rate_complete([unresolved])


def test_applied_completeness_accepts_only_applied_or_deterministic_skip() -> None:
    skipped = settlement(SettlementStatus.SKIPPED_POSITION_NOT_OPEN)
    applied = settlement(SettlementStatus.APPLIED_RATE, "2")
    assert applied_rate_complete([skipped, applied])
    assert authoritative_funding_cash_usd(skipped) == D("0")
    assert not applied_rate_complete([settlement(SettlementStatus.ESTIMATED, "2"), applied])
    assert not applied_rate_complete([settlement(SettlementStatus.UNRESOLVED), applied])


def test_actual_closed_pnl_subtracts_each_fee_once() -> None:
    result = closed_net_pnl_usd(
        D("10"), D("-3"), D("5"), [D("1"), D("2"), D("3"), D("4")]
    )
    assert result == D("2")


def test_exactly_five_lifecycle_states() -> None:
    assert {state.value for state in LifecycleState} == {
        "FLAT",
        "ENTRY_MAKER_OPEN",
        "HOLDING",
        "EXITING_NORMAL",
        "EXITING_AGGRESSIVE",
    }
