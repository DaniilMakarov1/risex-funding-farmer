from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from risex_farmer.config import PAPER_CONFIG
from risex_farmer.models import (
    BookLevel,
    CanonicalMarket,
    ContractType,
    DataQuality,
    FundingAccrualMethod,
    FundingCashQuote,
    FundingQuality,
    MarketType,
    MarketVolume,
    OrderBook,
    RouteDirection,
    Side,
    StreamHealth,
    TradeEvidence,
    Venue,
)
from risex_farmer.scanner import (
    MarketObservation,
    NoTradeReason,
    activation_schedule,
    evaluate_route,
    planned_fee_split,
    scan_once,
    trade_precedes_cutoff,
)


D = Decimal
NOW = datetime(2027, 2, 1, 12, tzinfo=UTC)
TARGET = NOW + timedelta(minutes=5)


def market(
    venue: Venue,
    asset: str = "ABC",
    *,
    multiplier: str | None = "1",
    quantity_step: str = "0.001",
    minimum_quantity: str = "0.001",
    minimum_notional: str = "10",
    quote: str = "USDC",
    active: bool = True,
    market_type: MarketType = MarketType.PERPETUAL,
) -> CanonicalMarket:
    symbol = f"{asset}-{venue.value}"
    return CanonicalMarket(
        asset,
        venue,
        symbol,
        market_type,
        ContractType.LINEAR if market_type is MarketType.PERPETUAL else ContractType.OTHER,
        None if multiplier is None else D(multiplier),
        quote,
        quote,
        D("1"),
        D(quantity_step),
        D(minimum_quantity),
        D(minimum_notional),
        D("20") if venue is Venue.NADO else None,
        active,
        False,
        False,
    )


def observation(
    venue: Venue,
    asset: str = "ABC",
    *,
    liquidity: str = "1000000",
    multiplier: str | None = "1",
    quantity_step: str = "0.001",
    minimum_quantity: str = "0.001",
    minimum_notional: str = "10",
    quote: str = "USDC",
    active: bool = True,
    market_type: MarketType = MarketType.PERPETUAL,
    funding_observed_at: datetime = NOW,
    funding_eligible: bool = True,
    long_cash: str | None = "2",
    short_cash: str | None = "2",
    settlement_at: datetime = TARGET,
    depth: str = "10",
    healthy: bool = True,
) -> MarketObservation:
    normalized = market(
        venue,
        asset,
        multiplier=multiplier,
        quantity_step=quantity_step,
        minimum_quantity=minimum_quantity,
        minimum_notional=minimum_notional,
        quote=quote,
        active=active,
        market_type=market_type,
    )
    volume = MarketVolume(venue, normalized.venue_symbol, D(liquidity), NOW, "synthetic")
    book = OrderBook(
        venue,
        normalized.venue_symbol,
        (BookLevel(D("99"), D(depth)),),
        (BookLevel(D("101"), D(depth)),),
        NOW,
        1,
    )
    funding = FundingCashQuote(
        venue,
        normalized.venue_symbol,
        funding_observed_at,
        NOW,
        settlement_at,
        FundingQuality.PREDICTED,
        FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT,
        funding_eligible,
        None if long_cash is None else D(long_cash),
        None if short_cash is None else D(short_cash),
        "synthetic",
    )
    health = StreamHealth(
        NOW,
        NOW,
        healthy,
        healthy,
        healthy,
        DataQuality.COMPLETE if healthy else DataQuality.DEGRADED,
    )
    return MarketObservation(normalized, volume, book, funding, health)


def plan(
    *,
    risex: MarketObservation | None = None,
    hedge: MarketObservation | None = None,
    direction: RouteDirection = RouteDirection.LONG_RISEX_SHORT_HEDGE,
):
    return evaluate_route(
        risex or observation(Venue.RISEX),
        hedge or observation(Venue.EXTENDED),
        direction,
        NOW,
    )


def test_target_cycle_activation_startup_and_focused_cadence() -> None:
    route = plan(
        hedge=observation(
            Venue.EXTENDED, settlement_at=TARGET + timedelta(seconds=30)
        )
    )
    assert route.target_cycle is not None
    assert route.target_cycle.start_at == TARGET
    assert route.target_cycle.end_at == TARGET + timedelta(seconds=30)
    assert route.target_cycle.span_seconds == 30
    schedule = activation_schedule(route.target_cycle)
    assert schedule.activation_at == TARGET - timedelta(seconds=120)
    assert schedule.cutoff_at == TARGET - timedelta(seconds=5)
    assert not schedule.should_activate(schedule.activation_at - timedelta(microseconds=1))
    assert schedule.should_activate(schedule.activation_at)
    assert not schedule.should_activate(schedule.activation_at, already_activated=True)
    startup = TARGET - timedelta(seconds=87)
    assert schedule.is_startup_window(startup)
    assert schedule.should_activate(startup)
    assert not schedule.is_startup_window(schedule.activation_at)
    assert not schedule.is_startup_window(schedule.cutoff_at)
    assert schedule.next_focused_evaluation_at(startup) == startup + timedelta(seconds=10)
    assert schedule.next_focused_evaluation_at(TARGET - timedelta(seconds=14)) is None


def test_trade_cutoff_uses_exchange_time_strictly() -> None:
    cutoff = TARGET - timedelta(seconds=5)

    def trade(exchange_timestamp: datetime) -> TradeEvidence:
        return TradeEvidence(
            "synthetic-key",
            Venue.EXTENDED,
            "ABC-EXTENDED",
            exchange_timestamp,
            cutoff + timedelta(seconds=30),
            "synthetic-raw",
            D("1"),
            D("100"),
            Side.SELL,
            True,
        )

    assert trade_precedes_cutoff(trade(cutoff - timedelta(microseconds=1)), cutoff)
    assert not trade_precedes_cutoff(trade(cutoff), cutoff)
    assert not trade_precedes_cutoff(trade(cutoff + timedelta(microseconds=1)), cutoff)


@pytest.mark.asyncio
async def test_simultaneous_routes_use_one_timestamp_and_frozen_tie_break() -> None:
    config = replace(PAPER_CONFIG, nado_maker_fee_rate=D("0"))
    snapshot = await scan_once(
        (
            observation(Venue.RISEX),
            observation(Venue.NADO),
            observation(Venue.EXTENDED),
        ),
        NOW,
        config=config,
    )
    assert len(snapshot.ranked_routes) == 4
    assert all(route.logical_at == NOW for route in snapshot.evaluations)
    assert snapshot.winner is not None
    assert snapshot.winner.hedge_venue is Venue.EXTENDED
    assert snapshot.winner.direction is RouteDirection.LONG_RISEX_SHORT_HEDGE


def test_stale_funding_makes_pnl_unknown_and_blocks_entry() -> None:
    result = plan(
        hedge=observation(
            Venue.EXTENDED, funding_observed_at=NOW - timedelta(seconds=121)
        )
    )
    assert NoTradeReason.FUNDING_STALE in result.no_trade_reasons
    assert result.planned_maker_net_pnl_usd is None
    assert not result.entry_allowed


def test_unknown_multiplier_and_funding_eligibility_block_entry() -> None:
    multiplier_unknown = plan(risex=observation(Venue.RISEX, multiplier=None))
    assert NoTradeReason.PARITY_OR_MULTIPLIER_UNKNOWN in multiplier_unknown.no_trade_reasons
    assert not multiplier_unknown.entry_allowed

    funding_unknown = plan(
        hedge=observation(Venue.EXTENDED, funding_eligible=False, long_cash=None)
    )
    assert NoTradeReason.FUNDING_ELIGIBILITY_UNKNOWN in funding_unknown.no_trade_reasons
    assert not funding_unknown.entry_allowed


def test_insufficient_depth_minimums_and_zero_common_quantity_are_no_trade() -> None:
    shallow = plan(hedge=observation(Venue.EXTENDED, depth="1"))
    assert NoTradeReason.INSUFFICIENT_EXACT_DEPTH in shallow.no_trade_reasons

    below_minimum = plan(
        hedge=observation(Venue.EXTENDED, minimum_quantity="10")
    )
    assert NoTradeReason.MINIMUM_ORDER in below_minimum.no_trade_reasons

    risex_below_minimum = plan(
        risex=observation(Venue.RISEX, minimum_quantity="10")
    )
    assert NoTradeReason.MINIMUM_ORDER in risex_below_minimum.no_trade_reasons

    below_exit_notional = plan(
        risex=observation(Venue.RISEX, minimum_notional="500")
    )
    assert NoTradeReason.MINIMUM_ORDER in below_exit_notional.no_trade_reasons

    no_quantity = plan(
        hedge=observation(Venue.EXTENDED, quantity_step="10")
    )
    assert NoTradeReason.NO_COMMON_EXECUTABLE_QUANTITY in no_quantity.no_trade_reasons


def test_market_stablecoin_and_book_gates_fail_closed() -> None:
    spot = plan(hedge=observation(Venue.EXTENDED, market_type=MarketType.SPOT))
    assert NoTradeReason.MARKET_INELIGIBLE in spot.no_trade_reasons
    unsupported_quote = plan(hedge=observation(Venue.EXTENDED, quote="EUR"))
    assert NoTradeReason.STABLECOIN_PARITY_UNKNOWN in unsupported_quote.no_trade_reasons
    unhealthy = plan(hedge=observation(Venue.EXTENDED, healthy=False))
    assert NoTradeReason.BOOK_UNHEALTHY in unhealthy.no_trade_reasons
    stale_connection = observation(Venue.EXTENDED)
    assert stale_connection.health is not None
    stale_connection = replace(
        stale_connection,
        health=replace(
            stale_connection.health,
            last_connection_confirmation_at=NOW - timedelta(seconds=26),
        ),
    )
    stale = plan(hedge=stale_connection)
    assert NoTradeReason.BOOK_UNHEALTHY in stale.no_trade_reasons


def test_negative_net_is_no_trade_and_non_negative_is_allowed() -> None:
    negative = plan(
        risex=observation(Venue.RISEX, long_cash="0", short_cash="0"),
        hedge=observation(Venue.EXTENDED, long_cash="0", short_cash="0"),
    )
    assert negative.planned_maker_net_pnl_usd is not None
    assert negative.planned_maker_net_pnl_usd < 0
    assert negative.no_trade_reasons == (NoTradeReason.PLANNED_NET_PNL_NEGATIVE,)
    assert not negative.entry_allowed

    non_negative = plan()
    assert non_negative.planned_maker_net_pnl_usd is not None
    assert non_negative.planned_maker_net_pnl_usd >= 0
    assert non_negative.entry_allowed
    assert non_negative.target_cycle is not None
    assert non_negative.executable_unwind_net_pnl_usd is not None
    assert non_negative.planned_entry_execution_pnl_usd is not None
    assert non_negative.planned_exit_execution_pnl_usd is not None
    assert non_negative.planned_execution_pnl_usd == (
        non_negative.planned_entry_execution_pnl_usd
        + non_negative.planned_exit_execution_pnl_usd
    )
    fee_split = planned_fee_split(non_negative)
    assert fee_split is not None
    assert non_negative.planned_fees_usd == sum(fee_split, D("0"))


@pytest.mark.asyncio
async def test_top_five_assets_route_liquidity_and_twenty_route_cap() -> None:
    observations: list[MarketObservation] = []
    for index, asset in enumerate(("AAA", "BBB", "CCC", "DDD", "EEE", "FFF"), 1):
        liquidity = str(index * 1000)
        observations.extend(
            (
                observation(Venue.RISEX, asset, liquidity=liquidity),
                observation(Venue.EXTENDED, asset, liquidity=str(index * 2000)),
                observation(Venue.NADO, asset, liquidity=str(index * 3000)),
            )
        )
    snapshot = await scan_once(observations, NOW)
    assert snapshot.selected_assets == ("FFF", "EEE", "DDD", "CCC", "BBB")
    assert len(snapshot.ranked_routes) == 20
    assert {route.canonical_asset for route in snapshot.ranked_routes} == set(
        snapshot.selected_assets
    )
    fff = next(route for route in snapshot.ranked_routes if route.canonical_asset == "FFF")
    assert fff.route is not None
    assert fff.route.route_liquidity_usd == D("6000")
