from __future__ import annotations

from dataclasses import replace
from decimal import Decimal as D

import pytest

from risex_farmer.models import Venue

from risex_spread_shadow import (
    CausalUncertainty,
    CycleFillModel,
    CycleScenario,
    CycleTerminalState,
    CycleReason,
    Side,
    run_cycle,
    run_scv1_s1a,
    run_scv1_s1a_alternatives,
)

from tests.spread_shadow.test_cycle import _book, _market, _trade, _version


def _valid_entry_version(version_id: str = "scv1-valid"):
    version, source_books = _version(version_id, risex_asks=(("102", "10"),))
    return version, source_books


def _activation_books(*, asks: tuple[tuple[str, str], ...] = (("102", "10"),)):
    return (
        _book(
            Venue.RISEX,
            received=400_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=asks,
        ),
        _book(
            Venue.RISEX,
            received=900_000_000,
            revision=3,
            bids=(("99", "10"),),
            asks=asks,
        ),
    )


def test_scv1_runs_four_isolated_alternatives_and_distinguishes_touch() -> None:
    version, source_books = _valid_entry_version()
    risex_400, risex_900 = _activation_books()
    touch = _trade(
        "scv1-touch",
        received=1_100_000_000,
        quantity="0.60",
        price="101",
    )
    through = _trade(
        "scv1-through",
        received=1_200_000_000,
        quantity="0.60",
        price="102",
    )

    alternatives = run_scv1_s1a_alternatives(
        version,
        (risex_400, risex_900, touch, through),
        source_books=source_books,
        end_monotonic_ns=1_300_000_000,
    )

    assert len(alternatives.by_alternative) == 4
    assert len({id(item.result) for item in alternatives.by_alternative}) == 4
    strict = alternatives.select(CycleFillModel.TRADE_THROUGH_ONLY, CycleScenario.PRIMARY)
    touch_allowed = alternatives.select(CycleFillModel.TOUCH_ALLOWED, CycleScenario.PRIMARY)
    assert strict.result.entry_quantity == D("0.60")
    assert touch_allowed.result.entry_quantity == D("1.00")
    assert strict.result.entry_measurement is not None
    assert touch_allowed.result.entry_measurement is not None
    assert any(
        decision.reason == "TOUCH_IGNORED_BY_MODEL"
        for decision in strict.result.entry_measurement.decisions
    )
    assert any(
        decision.reason == "ELIGIBLE_TOUCH_ZERO_QUEUE"
        for decision in touch_allowed.result.entry_measurement.decisions
    )
    assert CycleReason.ENTRY_CAUSAL_UNCERTAINTY.value not in strict.result.reason_codes
    assert alternatives.contract_version == "SCV1-1.1-S1a"


def test_scv1_pre_activation_trade_is_ignored_without_a_touch_halt() -> None:
    version, source_books = _valid_entry_version("scv1-pre-activation")
    (risex_400, _) = _activation_books()
    pre_activation = _trade(
        "scv1-pre",
        received=450_000_000,
        quantity="1",
        price="102",
    )

    alternative = run_scv1_s1a(
        version,
        (risex_400, pre_activation),
        fill_model=CycleFillModel.TRADE_THROUGH_ONLY,
        source_books=source_books,
        end_monotonic_ns=600_000_000,
    )

    assert alternative.result.entry_quantity == D("0")
    assert alternative.result.status is CycleTerminalState.PENDING
    assert alternative.result.entry_measurement is not None
    assert alternative.result.entry_measurement.decisions[-1].reason == "ENTRY_BOUNDARY"
    assert CycleReason.ENTRY_CAUSAL_UNCERTAINTY.value not in alternative.result.reason_codes


def test_scv1_duplicate_volume_is_spent_once_inside_each_lane() -> None:
    version, source_books = _valid_entry_version("scv1-duplicate")
    (risex_400, _) = _activation_books()
    through = _trade(
        "scv1-duplicate-event",
        received=600_000_000,
        quantity="0.60",
        price="102",
    )

    alternative = run_scv1_s1a(
        version,
        (risex_400, through, through),
        fill_model=CycleFillModel.TOUCH_ALLOWED,
        source_books=source_books,
        end_monotonic_ns=700_000_000,
    )

    assert alternative.result.entry_quantity == D("0.60")
    assert alternative.result.entry_measurement is not None
    assert alternative.result.entry_measurement.duplicate_event_count == 1
    assert len(alternative.result.fills) == 1


def test_scv1_crossing_post_only_quote_has_no_maker_fill() -> None:
    version, source_books = _version("scv1-crossing")
    risex_400 = _book(
        Venue.RISEX,
        received=400_000_000,
        revision=2,
        bids=(("101", "10"),),
        asks=(("102", "10"),),
    )
    crossing_trade = _trade(
        "scv1-crossing-trade",
        received=600_000_000,
        quantity="1",
        price="102",
    )

    alternative = run_scv1_s1a(
        version,
        (risex_400, crossing_trade),
        fill_model=CycleFillModel.TOUCH_ALLOWED,
        source_books=source_books,
        end_monotonic_ns=700_000_000,
    )

    assert alternative.result.status is CycleTerminalState.ABORTED
    assert alternative.result.entry_quantity == D("0")
    assert not alternative.result.fills
    assert CycleReason.INVALID_ENTRY_QUOTE.value in alternative.result.reason_codes


@pytest.mark.parametrize("asks", ((('100', '10'),), (('101', '10'),)))
def test_scv1_non_crossing_sell_at_or_above_ask_remains_eligible(asks) -> None:
    version, source_books = _version("scv1-non-crossing-" + asks[0][0])
    activation_book = _book(
        Venue.RISEX,
        received=400_000_000,
        revision=2,
        bids=(("99", "10"),),
        asks=asks,
    )
    alternative = run_scv1_s1a(
        version,
        (activation_book, _trade("scv1-non-crossing-trade-" + asks[0][0], received=600_000_000, quantity="0.5", price="102")),
        fill_model=CycleFillModel.TOUCH_ALLOWED,
        source_books=source_books,
        end_monotonic_ns=700_000_000,
    )

    assert alternative.result.entry_quantity == D("0.5")
    assert alternative.result.status is CycleTerminalState.PENDING
    assert CycleReason.INVALID_ENTRY_QUOTE.value not in alternative.result.reason_codes


@pytest.mark.parametrize("model", tuple(CycleFillModel))
@pytest.mark.parametrize("price", ("101.5", "102.5"))
def test_scv1_off_grid_entry_price_is_uncertain_for_both_fill_models(model, price) -> None:
    version, source_books = _valid_entry_version("scv1-off-grid-" + model.value + "-" + price)
    (risex_400, _) = _activation_books()
    alternative = run_scv1_s1a(
        version,
        (risex_400, _trade("scv1-off-grid-trade-" + price, received=600_000_000, quantity="0.5", price=price)),
        fill_model=model,
        source_books=source_books,
        end_monotonic_ns=700_000_000,
    )

    assert alternative.result.status is CycleTerminalState.UNRESOLVED
    assert alternative.result.entry_quantity == D("0")
    assert not alternative.result.fills
    assert alternative.result.entry_measurement is not None
    decision = alternative.result.entry_measurement.decisions[-1]
    assert decision.classification == "UNCERTAIN"
    assert decision.reason == CausalUncertainty.INVALID_TRADE_PRICE_GRID.value


@pytest.mark.parametrize("model", tuple(CycleFillModel))
def test_scv1_off_grid_exit_price_is_uncertain_for_both_maker_sides(model) -> None:
    version, source_books = _valid_entry_version("scv1-off-grid-exit-" + model.value)
    events = list(_full_cycle_events())
    events[5] = _trade(
        "scv1-off-grid-exit-trade-" + model.value,
        received=1_600_000_300,
        quantity="1",
        price="97.5",
        aggressor=Side.SELL,
    )

    alternative = run_scv1_s1a(
        version,
        events,
        fill_model=model,
        source_books=source_books,
        end_monotonic_ns=2_300_000_000,
    )

    assert alternative.result.status is CycleTerminalState.UNRESOLVED
    assert alternative.result.exit_measurement is not None
    decision = alternative.result.exit_measurement.decisions[-1]
    assert decision.classification == "UNCERTAIN"
    assert decision.reason == CausalUncertainty.INVALID_TRADE_PRICE_GRID.value
    assert not any(fill.action_id == "exit-maker" for fill in alternative.result.fills)


def _custom_quantity_version(*, risex_minimum: str):
    risex_market = replace(
        _market(Venue.RISEX, "BTC/USDC", minimum_quantity=risex_minimum),
        quantity_step_raw=D("0.1"),
    )
    lighter_market = replace(
        _market(Venue.LIGHTER, "BTC", minimum_quantity="1"),
        quantity_step_raw=D("0.2"),
    )
    return _version(
        f"scv1-grid-{risex_minimum}",
        risex_asks=(("102", "10"),),
        risex_market=risex_market,
        lighter_market=lighter_market,
    )


def test_scv1_one_sided_close_uses_risex_grid_and_rejects_own_minimum() -> None:
    version, source_books = _custom_quantity_version(risex_minimum="0.1")
    risex_400, _ = _activation_books()
    lighter_1200 = _book(
        Venue.LIGHTER,
        received=1_200_000_000,
        revision=2,
        bids=(("99", "10"),),
        asks=(("100", "10"),),
    )
    risex_1200 = _book(
        Venue.RISEX,
        received=1_200_000_000,
        revision=3,
        bids=(("99", "10"),),
        asks=(("105", "10"),),
    )
    risex_1600 = _book(
        Venue.RISEX,
        received=1_600_000_000,
        revision=4,
        bids=(("99", "10"),),
        asks=(("105", "10"),),
    )
    partial = _trade(
        "scv1-subminimum-partial",
        received=500_000_200,
        quantity="0.50",
        price="102",
    )

    permitted = run_scv1_s1a(
        version,
        (risex_400, partial, lighter_1200, risex_1200, risex_1600),
        fill_model=CycleFillModel.TRADE_THROUGH_ONLY,
        source_books=source_books,
        end_monotonic_ns=2_200_000_000,
    )
    assert permitted.result.status is CycleTerminalState.FORCED
    assert permitted.result.is_flat
    assert permitted.result.entry_quantity == D("0.50")
    close = next(fill for fill in permitted.result.fills if fill.action_id == "unmatched-risex")
    assert close.quantity == D("0.50")
    assert close.price == D("105")

    rejected_version, rejected_source = _custom_quantity_version(risex_minimum="0.6")
    rejected = run_scv1_s1a(
        rejected_version,
        (risex_400, partial, lighter_1200, risex_1200, risex_1600),
        fill_model=CycleFillModel.TRADE_THROUGH_ONLY,
        source_books=rejected_source,
        end_monotonic_ns=2_200_000_000,
    )
    assert rejected.result.status is CycleTerminalState.UNRESOLVED
    assert rejected.result.positions.risex_signed_quantity == D("-0.50")
    assert not any(fill.action_id == "unmatched-risex" for fill in rejected.result.fills)
    assert CycleReason.MINIMUM_RESIDUE.value in rejected.result.reason_codes


def test_legacy_run_cycle_keeps_common_grid_and_accounting_contract() -> None:
    version, source_books = _custom_quantity_version(risex_minimum="0.1")
    risex_400, _ = _activation_books()
    partial = _trade(
        "legacy-common-grid-partial",
        received=500_000_200,
        quantity="0.50",
        price="102",
    )
    events = (
        risex_400,
        partial,
        _book(
            Venue.LIGHTER,
            received=1_200_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        ),
        _book(
            Venue.RISEX,
            received=1_200_000_000,
            revision=3,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        ),
        _book(
            Venue.RISEX,
            received=1_600_000_000,
            revision=4,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        ),
    )

    legacy = run_cycle(version, events, source_books=source_books, end_monotonic_ns=2_200_000_000)
    assert legacy.status is CycleTerminalState.UNRESOLVED
    assert legacy.positions.risex_signed_quantity == D("-0.10")
    assert next(fill for fill in legacy.fills if fill.action_id == "unmatched-risex").quantity == D("0.4")
    assert CycleReason.TERMINAL_NON_FLAT.value in legacy.reason_codes


def _repeating_vwap_version():
    risex_market = replace(
        _market(Venue.RISEX, "BTC/USDC"),
        quantity_step_raw=D("1"),
    )
    lighter_market = replace(
        _market(Venue.LIGHTER, "BTC"),
        quantity_step_raw=D("1"),
    )
    return _version(
        "scv1-repeating-vwap",
        risex_bids=(("32", "10"),),
        risex_asks=(("105", "10"),),
        lighter_bids=(("32", "10"),),
        lighter_asks=(("33", "1"), ("137", "2")),
        risex_market=risex_market,
        lighter_market=lighter_market,
    )


def test_scv1_level_notional_reaches_production_ledger_and_fees() -> None:
    version, source_books = _repeating_vwap_version()
    risex_400 = _book(
        Venue.RISEX,
        received=400_000_000,
        revision=2,
        bids=(("32", "10"),),
        asks=(("105", "10"),),
    )
    risex_900 = _book(
        Venue.RISEX,
        received=900_000_000,
        revision=3,
        bids=(("32", "10"),),
        asks=(("105", "10"),),
    )
    lighter_900 = _book(
        Venue.LIGHTER,
        received=900_000_000,
        revision=2,
        bids=(("32", "10"),),
        asks=(("33", "1"), ("137", "2")),
    )
    entry = _trade(
        "scv1-repeating-entry",
        received=500_000_200,
        quantity=str(version.quote.canonical_quantity),
        price=str(version.quote.maker_price + D("1")),
    )

    alternative = run_scv1_s1a(
        version,
        (risex_400, entry, risex_900, lighter_900),
        fill_model=CycleFillModel.TRADE_THROUGH_ONLY,
        source_books=source_books,
        end_monotonic_ns=1_100_000_000,
    )
    hedge = next(fill for fill in alternative.result.fills if fill.action_id == "entry-hedge")
    expected_notional = D("33") * D("1") + D("137") * D("2")
    assert hedge.quantity == D("3")
    assert hedge.notional_usd == expected_notional == D("307")
    assert hedge.price == expected_notional / D("3")
    assert hedge.quantity * hedge.price != hedge.notional_usd
    fee = next(item for item in alternative.result.fees if item.fill_id == hedge.fill_id)
    assert fee.notional_usd == expected_notional
    assert fee.amount_usd == D("0")
    cashflow = next(item for item in alternative.result.cashflows if item.fill_id == hedge.fill_id)
    assert cashflow.gross_cashflow_usd == -expected_notional
    assert alternative.result.ledger.turnover_usd == sum(
        (fill.notional_usd for fill in alternative.result.fills), D("0")
    )


def _full_cycle_events(*, close_bid: str = "99"):
    return (
        _book(
            Venue.RISEX,
            received=400_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("102", "10"),),
        ),
        _trade("scv1-cycle-entry", received=500_000_200, quantity="1", price="102"),
        _book(
            Venue.RISEX,
            received=900_000_200,
            revision=3,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        ),
        _book(
            Venue.LIGHTER,
            received=900_000_200,
            revision=2,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        ),
        _book(
            Venue.RISEX,
            received=1_200_000_300,
            revision=4,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        ),
        _trade(
            "scv1-cycle-exit",
            received=1_600_000_300,
            quantity="1",
            price="97",
            aggressor=Side.SELL,
        ),
        _book(
            Venue.RISEX,
            received=1_900_000_300,
            revision=5,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        ),
        _book(
            Venue.LIGHTER,
            received=1_900_000_300,
            revision=3,
            bids=((close_bid, "10"),),
            asks=((str(D(close_bid) + D("1")), "10"),),
        ),
    )


def test_scv1_accounting_has_real_positive_and_negative_paths() -> None:
    version, source_books = _valid_entry_version("scv1-positive")
    positive = run_scv1_s1a(
        version,
        _full_cycle_events(),
        fill_model=CycleFillModel.TRADE_THROUGH_ONLY,
        source_books=source_books,
        end_monotonic_ns=2_300_000_000,
    )
    negative_version, negative_source = _valid_entry_version("scv1-negative")
    negative = run_scv1_s1a(
        negative_version,
        _full_cycle_events(close_bid="90"),
        fill_model=CycleFillModel.TRADE_THROUGH_ONLY,
        source_books=negative_source,
        end_monotonic_ns=2_300_000_000,
    )

    assert positive.result.status is CycleTerminalState.NORMAL
    assert positive.result.is_flat
    assert positive.result.pnl_usd is not None and positive.result.pnl_usd > D("0")
    assert negative.result.status is CycleTerminalState.NORMAL
    assert negative.result.is_flat
    assert negative.result.pnl_usd is not None and negative.result.pnl_usd < D("0")
