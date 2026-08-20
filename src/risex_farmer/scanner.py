"""Deterministic paper route construction, economics, ranking, and scheduling."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Iterable

from .config import PAPER_CONFIG, PaperConfig
from .economics import (
    common_canonical_quantity_step,
    exact_quantity_vwap,
    funding_cash_usd,
    maker_price,
    minimum_order_eligible,
    pair_price_pnl_usd,
    planned_maker_net_pnl_usd,
    sized_canonical_quantity,
    venue_fee_amount_usd,
)
from .market_data import funding_is_fresh
from .models import (
    CanonicalMarket,
    ContractType,
    DataQuality,
    FundingAccrualMethod,
    FundingCashQuote,
    FundingEvent,
    FundingQuality,
    LiquidityRole,
    MarketType,
    MarketVolume,
    OrderBook,
    Route,
    RouteDirection,
    Side,
    StreamHealth,
    TargetFundingCycle,
    TradeEvidence,
    Venue,
)


STABLE_QUOTES = frozenset({"USD", "USDC", "USDT", "USDT0"})


class NoTradeReason:
    MARKET_INELIGIBLE = "MARKET_INELIGIBLE"
    PARITY_OR_MULTIPLIER_UNKNOWN = "PARITY_OR_MULTIPLIER_UNKNOWN"
    STABLECOIN_PARITY_UNKNOWN = "STABLECOIN_PARITY_UNKNOWN"
    VOLUME_UNKNOWN = "VOLUME_UNKNOWN"
    BOOK_UNHEALTHY = "BOOK_UNHEALTHY"
    INVALID_BBO = "INVALID_BBO"
    FUNDING_STALE = "FUNDING_STALE"
    FUNDING_ELIGIBILITY_UNKNOWN = "FUNDING_ELIGIBILITY_UNKNOWN"
    FUNDING_OPEN_TIME_MISMATCH = "FUNDING_OPEN_TIME_MISMATCH"
    TARGET_CYCLE_ELAPSED = "TARGET_CYCLE_ELAPSED"
    NO_COMMON_EXECUTABLE_QUANTITY = "NO_COMMON_EXECUTABLE_QUANTITY"
    MINIMUM_ORDER = "MINIMUM_ORDER"
    INSUFFICIENT_EXACT_DEPTH = "INSUFFICIENT_EXACT_DEPTH"
    PLANNED_NET_PNL_NEGATIVE = "PLANNED_NET_PNL_NEGATIVE"


@dataclass(frozen=True, slots=True)
class MarketObservation:
    market: CanonicalMarket
    volume: MarketVolume | None
    book: OrderBook | None
    funding: FundingCashQuote | None
    health: StreamHealth | None


@dataclass(frozen=True, slots=True)
class RoutePlan:
    canonical_asset: str
    risex_market: CanonicalMarket
    hedge_market: CanonicalMarket
    hedge_venue: Venue
    direction: RouteDirection
    logical_at: datetime
    route: Route | None
    target_cycle: TargetFundingCycle | None
    canonical_quantity: Decimal | None
    risex_entry_price: Decimal | None
    hedge_entry_price: Decimal | None
    risex_exit_price: Decimal | None
    hedge_exit_price: Decimal | None
    expected_target_cycle_funding_usd: Decimal | None
    planned_entry_execution_pnl_usd: Decimal | None
    planned_exit_execution_pnl_usd: Decimal | None
    planned_execution_pnl_usd: Decimal | None
    planned_fees_usd: Decimal | None
    planned_maker_net_pnl_usd: Decimal | None
    executable_unwind_net_pnl_usd: Decimal | None
    no_trade_reasons: tuple[str, ...]

    @property
    def entry_allowed(self) -> bool:
        return not self.no_trade_reasons and self.planned_maker_net_pnl_usd is not None

    @property
    def universe_eligible(self) -> bool:
        return self.planned_maker_net_pnl_usd is not None and all(
            reason == NoTradeReason.PLANNED_NET_PNL_NEGATIVE
            for reason in self.no_trade_reasons
        )


@dataclass(frozen=True, slots=True)
class ScanSnapshot:
    logical_at: datetime
    evaluations: tuple[RoutePlan, ...]
    selected_assets: tuple[str, ...]
    ranked_routes: tuple[RoutePlan, ...]
    winner: RoutePlan | None


@dataclass(frozen=True, slots=True)
class ActivationSchedule:
    activation_at: datetime
    cutoff_at: datetime

    def should_activate(self, now: datetime, *, already_activated: bool = False) -> bool:
        if already_activated:
            return False
        return self.activation_at <= now < self.cutoff_at

    def is_startup_window(self, now: datetime) -> bool:
        target_at = self.cutoff_at + timedelta(seconds=5)
        time_to_target = target_at - now
        return timedelta(seconds=5) < time_to_target < timedelta(seconds=120)

    def next_focused_evaluation_at(self, last_evaluated_at: datetime) -> datetime | None:
        next_at = last_evaluated_at + timedelta(seconds=10)
        return next_at if next_at < self.cutoff_at else None


def activation_schedule(cycle: TargetFundingCycle) -> ActivationSchedule:
    return ActivationSchedule(
        cycle.start_at - timedelta(seconds=120),
        cycle.start_at - timedelta(seconds=5),
    )


def trade_precedes_cutoff(trade: TradeEvidence, cutoff_at: datetime) -> bool:
    return trade.exchange_timestamp is not None and trade.exchange_timestamp < cutoff_at


def _market_is_eligible(market: CanonicalMarket) -> bool:
    return (
        market.market_type is MarketType.PERPETUAL
        and market.contract_type is ContractType.LINEAR
        and market.is_active
        and not market.is_rfq
        and not market.is_off_hours
    )


def _stablecoin_eligible(market: CanonicalMarket) -> bool:
    return market.quote_asset in STABLE_QUOTES and market.settlement_asset in STABLE_QUOTES


def _volume_matches(observation: MarketObservation) -> bool:
    market = observation.market
    return (
        observation.volume is not None
        and observation.volume.venue is market.venue
        and observation.volume.canonical_market == market.venue_symbol
    )


def _healthy(
    observation: MarketObservation,
    logical_at: datetime,
    *,
    max_silence_seconds: int,
) -> bool:
    market = observation.market
    health = observation.health
    return (
        observation.book is not None
        and observation.book.venue is market.venue
        and observation.book.canonical_market == market.venue_symbol
        and health is not None
        and health.data_quality is DataQuality.COMPLETE
        and health.stream_connected
        and health.book_initialized
        and health.book_sequence_valid
        and health.last_connection_confirmation_at is not None
        and logical_at >= health.last_connection_confirmation_at
        and logical_at - health.last_connection_confirmation_at
        <= timedelta(seconds=max_silence_seconds)
    )


def _best_prices(book: OrderBook) -> tuple[Decimal, Decimal]:
    if not book.bids or not book.asks:
        raise ValueError("book must contain bids and asks")
    return max(level.canonical_price for level in book.bids), min(
        level.canonical_price for level in book.asks
    )


def _fee_rate(config: PaperConfig, venue: Venue, role: LiquidityRole) -> Decimal:
    rates = {
        (Venue.RISEX, LiquidityRole.MAKER): config.risex_maker_fee_rate,
        (Venue.RISEX, LiquidityRole.TAKER): config.risex_taker_fee_rate,
        (Venue.EXTENDED, LiquidityRole.MAKER): config.extended_maker_fee_rate,
        (Venue.EXTENDED, LiquidityRole.TAKER): config.extended_taker_fee_rate,
        (Venue.NADO, LiquidityRole.MAKER): config.nado_maker_fee_rate,
        (Venue.NADO, LiquidityRole.TAKER): config.nado_taker_fee_rate,
    }
    return rates[(venue, role)]


def _fee(
    config: PaperConfig,
    market: CanonicalMarket,
    role: LiquidityRole,
    quantity: Decimal,
    price: Decimal,
) -> Decimal:
    return venue_fee_amount_usd(
        market.venue,
        role,
        quantity * price,
        _fee_rate(config, market.venue, role),
        market.minimum_fee_notional_usd,
    )


def _empty_plan(
    risex: MarketObservation,
    hedge: MarketObservation,
    direction: RouteDirection,
    logical_at: datetime,
    reasons: Iterable[str],
    *,
    route: Route | None = None,
) -> RoutePlan:
    return RoutePlan(
        canonical_asset=risex.market.canonical_asset,
        risex_market=risex.market,
        hedge_market=hedge.market,
        hedge_venue=hedge.market.venue,
        direction=direction,
        logical_at=logical_at,
        route=route,
        target_cycle=None,
        canonical_quantity=None,
        risex_entry_price=None,
        hedge_entry_price=None,
        risex_exit_price=None,
        hedge_exit_price=None,
        expected_target_cycle_funding_usd=None,
        planned_entry_execution_pnl_usd=None,
        planned_exit_execution_pnl_usd=None,
        planned_execution_pnl_usd=None,
        planned_fees_usd=None,
        planned_maker_net_pnl_usd=None,
        executable_unwind_net_pnl_usd=None,
        no_trade_reasons=tuple(dict.fromkeys(reasons)),
    )


def _funding_cash_for_direction(
    quote: FundingCashQuote, *, long_position: bool
) -> Decimal | None:
    return (
        quote.long_cash_per_canonical_base_usd
        if long_position
        else quote.short_cash_per_canonical_base_usd
    )


def _target_cycle(
    route: Route,
    direction: RouteDirection,
    quantity: Decimal,
    risex_quote: FundingCashQuote,
    hedge_quote: FundingCashQuote,
) -> tuple[TargetFundingCycle, Decimal]:
    risex_long = direction is RouteDirection.LONG_RISEX_SHORT_HEDGE
    risex_cash = _funding_cash_for_direction(risex_quote, long_position=risex_long)
    hedge_cash = _funding_cash_for_direction(hedge_quote, long_position=not risex_long)
    if risex_cash is None or hedge_cash is None:
        raise ValueError("funding cash must be known")
    risex_expected = funding_cash_usd(quantity, risex_cash)
    hedge_expected = funding_cash_usd(quantity, hedge_cash)
    risex_event = FundingEvent(
        Venue.RISEX,
        route.risex_market.venue_symbol,
        risex_quote.settlement_at,
        risex_expected,
        risex_quote.eligibility_known,
    )
    hedge_event = FundingEvent(
        route.hedge_venue,
        route.hedge_market.venue_symbol,
        hedge_quote.settlement_at,
        hedge_expected,
        hedge_quote.eligibility_known,
    )
    start_at = min(risex_event.settlement_at, hedge_event.settlement_at)
    end_at = max(risex_event.settlement_at, hedge_event.settlement_at)
    cycle_id = "|".join(
        (
            route.canonical_asset,
            direction.value,
            risex_event.settlement_at.isoformat(),
            route.hedge_venue.value,
            hedge_event.settlement_at.isoformat(),
        )
    )
    cycle = TargetFundingCycle(
        cycle_id,
        start_at,
        end_at,
        (end_at - start_at).days * 86_400 + (end_at - start_at).seconds,
        risex_event,
        hedge_event,
    )
    return cycle, risex_expected + hedge_expected


def evaluate_route(
    risex: MarketObservation,
    hedge: MarketObservation,
    direction: RouteDirection,
    logical_at: datetime,
    *,
    config: PaperConfig = PAPER_CONFIG,
) -> RoutePlan:
    if risex.market.venue is not Venue.RISEX or hedge.market.venue not in {
        Venue.EXTENDED,
        Venue.NADO,
    }:
        raise ValueError("route must be RISEx to Extended or Nado")
    reasons: list[str] = []
    markets = (risex.market, hedge.market)
    if not all(_market_is_eligible(market) for market in markets):
        reasons.append(NoTradeReason.MARKET_INELIGIBLE)
    if (
        risex.market.canonical_asset != hedge.market.canonical_asset
        or any(market.base_multiplier is None for market in markets)
        or any(
            market.base_multiplier is not None and market.base_multiplier <= 0
            for market in markets
        )
    ):
        reasons.append(NoTradeReason.PARITY_OR_MULTIPLIER_UNKNOWN)
    if not all(_stablecoin_eligible(market) for market in markets):
        reasons.append(NoTradeReason.STABLECOIN_PARITY_UNKNOWN)

    route_liquidity: Decimal | None = None
    if not _volume_matches(risex) or not _volume_matches(hedge):
        reasons.append(NoTradeReason.VOLUME_UNKNOWN)
    elif (
        risex.volume is None
        or hedge.volume is None
        or risex.volume.quote_volume_usd is None
        or hedge.volume.quote_volume_usd is None
        or risex.volume.quote_volume_usd < 0
        or hedge.volume.quote_volume_usd < 0
    ):
        reasons.append(NoTradeReason.VOLUME_UNKNOWN)
    else:
        route_liquidity = min(
            risex.volume.quote_volume_usd, hedge.volume.quote_volume_usd
        )

    if not _healthy(
        risex,
        logical_at,
        max_silence_seconds=config.max_market_stream_silence_seconds,
    ) or not _healthy(
        hedge,
        logical_at,
        max_silence_seconds=config.max_market_stream_silence_seconds,
    ):
        reasons.append(NoTradeReason.BOOK_UNHEALTHY)
    for observation in (risex, hedge):
        funding = observation.funding
        if (
            funding is None
            or funding.venue is not observation.market.venue
            or funding.canonical_market != observation.market.venue_symbol
        ):
            reasons.append(NoTradeReason.FUNDING_ELIGIBILITY_UNKNOWN)
    if reasons:
        return _empty_plan(risex, hedge, direction, logical_at, reasons)

    assert route_liquidity is not None
    assert risex.book is not None and hedge.book is not None
    assert risex.funding is not None and hedge.funding is not None
    route = Route(
        risex.market.canonical_asset,
        risex.market,
        hedge.market,
        hedge.market.venue,
        direction,
        route_liquidity,
    )
    try:
        risex_bid, risex_ask = _best_prices(risex.book)
        hedge_bid, hedge_ask = _best_prices(hedge.book)
        maker_price(Side.BUY, risex_bid, risex_ask, risex.market.tick_size_raw)
        maker_price(Side.SELL, risex_bid, risex_ask, risex.market.tick_size_raw)
        hedge_buy_maker = maker_price(
            Side.BUY, hedge_bid, hedge_ask, hedge.market.tick_size_raw
        )
        hedge_sell_maker = maker_price(
            Side.SELL, hedge_bid, hedge_ask, hedge.market.tick_size_raw
        )
    except ValueError:
        return _empty_plan(
            risex,
            hedge,
            direction,
            logical_at,
            (NoTradeReason.INVALID_BBO,),
            route=route,
        )

    quotes = (risex.funding, hedge.funding)
    if any(
        quote.quality is FundingQuality.UNKNOWN
        or quote.accrual_method is FundingAccrualMethod.UNKNOWN
        or not funding_is_fresh(
            quote.observed_at,
            logical_at,
            max_age_seconds=config.default_max_funding_data_age_seconds,
        )
        for quote in quotes
    ):
        reasons.append(NoTradeReason.FUNDING_STALE)
    if any(
        not quote.eligibility_known
        or quote.long_cash_per_canonical_base_usd is None
        or quote.short_cash_per_canonical_base_usd is None
        for quote in quotes
    ):
        reasons.append(NoTradeReason.FUNDING_ELIGIBILITY_UNKNOWN)
    if any(
        quote.assumed_or_actual_position_opened_at != logical_at for quote in quotes
    ):
        reasons.append(NoTradeReason.FUNDING_OPEN_TIME_MISMATCH)
    if min(quote.settlement_at for quote in quotes) <= logical_at:
        reasons.append(NoTradeReason.TARGET_CYCLE_ELAPSED)
    if reasons:
        return _empty_plan(
            risex, hedge, direction, logical_at, reasons, route=route
        )

    assert risex.market.base_multiplier is not None
    assert hedge.market.base_multiplier is not None
    try:
        common_step = common_canonical_quantity_step(
            (
                (risex.market.quantity_step_raw, risex.market.base_multiplier),
                (hedge.market.quantity_step_raw, hedge.market.base_multiplier),
            )
        )
    except ValueError:
        return _empty_plan(
            risex,
            hedge,
            direction,
            logical_at,
            (NoTradeReason.NO_COMMON_EXECUTABLE_QUANTITY,),
            route=route,
        )
    hedge_entry_price = (
        hedge_sell_maker
        if direction is RouteDirection.LONG_RISEX_SHORT_HEDGE
        else hedge_buy_maker
    )
    hedge_exit_price = (
        hedge_buy_maker
        if direction is RouteDirection.LONG_RISEX_SHORT_HEDGE
        else hedge_sell_maker
    )
    quantity = sized_canonical_quantity(
        config.target_notional_per_leg_usd, hedge_entry_price, common_step
    )
    if quantity <= 0:
        return _empty_plan(
            risex,
            hedge,
            direction,
            logical_at,
            (NoTradeReason.NO_COMMON_EXECUTABLE_QUANTITY,),
            route=route,
        )

    risex_entry_side = (
        Side.BUY
        if direction is RouteDirection.LONG_RISEX_SHORT_HEDGE
        else Side.SELL
    )
    risex_exit_side = Side.SELL if risex_entry_side is Side.BUY else Side.BUY
    try:
        risex_entry = exact_quantity_vwap(
            risex_entry_side, quantity, risex.book.bids, risex.book.asks
        )
        risex_exit = exact_quantity_vwap(
            risex_exit_side, quantity, risex.book.bids, risex.book.asks
        )
        hedge_buy = exact_quantity_vwap(
            Side.BUY, quantity, hedge.book.bids, hedge.book.asks
        )
        hedge_sell = exact_quantity_vwap(
            Side.SELL, quantity, hedge.book.bids, hedge.book.asks
        )
    except ValueError:
        return _empty_plan(
            risex,
            hedge,
            direction,
            logical_at,
            (NoTradeReason.INSUFFICIENT_EXACT_DEPTH,),
            route=route,
        )
    if not all(
        result.is_executable
        for result in (risex_entry, risex_exit, hedge_buy, hedge_sell)
    ):
        return _empty_plan(
            risex,
            hedge,
            direction,
            logical_at,
            (NoTradeReason.INSUFFICIENT_EXACT_DEPTH,),
            route=route,
        )
    assert risex_entry.price is not None and risex_exit.price is not None
    assert hedge_buy.price is not None and hedge_sell.price is not None
    if not (
        minimum_order_eligible(quantity, risex_entry.price, risex.market)
        and minimum_order_eligible(quantity, risex_exit.price, risex.market)
        and minimum_order_eligible(quantity, hedge_entry_price, hedge.market)
        and minimum_order_eligible(quantity, hedge_exit_price, hedge.market)
    ):
        return _empty_plan(
            risex,
            hedge,
            direction,
            logical_at,
            (NoTradeReason.MINIMUM_ORDER,),
            route=route,
        )

    cycle, expected_funding = _target_cycle(
        route, direction, quantity, risex.funding, hedge.funding
    )
    risex_entry_price = risex_entry.price
    risex_exit_price = risex_exit.price
    if direction is RouteDirection.LONG_RISEX_SHORT_HEDGE:
        entry_execution = quantity * (hedge_entry_price - risex_entry_price)
        exit_execution = quantity * (risex_exit_price - hedge_exit_price)
        execution = pair_price_pnl_usd(
            quantity,
            risex_entry_price,
            risex_exit_price,
            hedge_entry_price,
            hedge_exit_price,
        )
    else:
        entry_execution = quantity * (risex_entry_price - hedge_entry_price)
        exit_execution = quantity * (hedge_exit_price - risex_exit_price)
        execution = pair_price_pnl_usd(
            quantity,
            hedge_entry_price,
            hedge_exit_price,
            risex_entry_price,
            risex_exit_price,
        )
    planned_fees = sum(
        (
            _fee(config, risex.market, LiquidityRole.TAKER, quantity, risex_entry_price),
            _fee(config, hedge.market, LiquidityRole.MAKER, quantity, hedge_entry_price),
            _fee(config, risex.market, LiquidityRole.TAKER, quantity, risex_exit_price),
            _fee(config, hedge.market, LiquidityRole.MAKER, quantity, hedge_exit_price),
        ),
        Decimal("0"),
    )
    planned_net = planned_maker_net_pnl_usd(
        expected_funding, execution, (planned_fees,)
    )

    hedge_unwind_price = (
        hedge_buy.price
        if direction is RouteDirection.LONG_RISEX_SHORT_HEDGE
        else hedge_sell.price
    )
    if direction is RouteDirection.LONG_RISEX_SHORT_HEDGE:
        unwind_execution = pair_price_pnl_usd(
            quantity,
            risex_entry_price,
            risex_exit_price,
            hedge_entry_price,
            hedge_unwind_price,
        )
    else:
        unwind_execution = pair_price_pnl_usd(
            quantity,
            hedge_entry_price,
            hedge_unwind_price,
            risex_entry_price,
            risex_exit_price,
        )
    unwind_fees = sum(
        (
            _fee(config, risex.market, LiquidityRole.TAKER, quantity, risex_entry_price),
            _fee(config, hedge.market, LiquidityRole.MAKER, quantity, hedge_entry_price),
            _fee(config, risex.market, LiquidityRole.TAKER, quantity, risex_exit_price),
            _fee(config, hedge.market, LiquidityRole.TAKER, quantity, hedge_unwind_price),
        ),
        Decimal("0"),
    )
    unwind_net = unwind_execution - unwind_fees
    if planned_net < config.paper_entry_min_planned_net_pnl_usd:
        reasons.append(NoTradeReason.PLANNED_NET_PNL_NEGATIVE)
    if entry_execution + exit_execution != execution:
        raise AssertionError("planned execution components must equal total execution")
    return RoutePlan(
        canonical_asset=route.canonical_asset,
        risex_market=route.risex_market,
        hedge_market=route.hedge_market,
        hedge_venue=route.hedge_venue,
        direction=route.direction,
        logical_at=logical_at,
        route=route,
        target_cycle=cycle,
        canonical_quantity=quantity,
        risex_entry_price=risex_entry_price,
        hedge_entry_price=hedge_entry_price,
        risex_exit_price=risex_exit_price,
        hedge_exit_price=hedge_exit_price,
        expected_target_cycle_funding_usd=expected_funding,
        planned_entry_execution_pnl_usd=entry_execution,
        planned_exit_execution_pnl_usd=exit_execution,
        planned_execution_pnl_usd=execution,
        planned_fees_usd=planned_fees,
        planned_maker_net_pnl_usd=planned_net,
        executable_unwind_net_pnl_usd=unwind_net,
        no_trade_reasons=tuple(reasons),
    )


def _rank_key(plan: RoutePlan) -> tuple[object, ...]:
    assert plan.route is not None
    assert plan.target_cycle is not None
    assert plan.planned_maker_net_pnl_usd is not None
    return (
        -plan.planned_maker_net_pnl_usd,
        -plan.route.route_liquidity_usd,
        plan.target_cycle.start_at,
        plan.canonical_asset,
        plan.hedge_venue.value,
        plan.direction.value,
    )


async def scan_once(
    observations: Iterable[MarketObservation],
    logical_at: datetime,
    *,
    config: PaperConfig = PAPER_CONFIG,
) -> ScanSnapshot:
    observed = tuple(observations)
    risex_rows = tuple(row for row in observed if row.market.venue is Venue.RISEX)
    hedge_rows = tuple(
        row for row in observed if row.market.venue in {Venue.EXTENDED, Venue.NADO}
    )
    evaluations = tuple(
        evaluate_route(risex, hedge, direction, logical_at, config=config)
        for risex in risex_rows
        for hedge in hedge_rows
        if risex.market.canonical_asset == hedge.market.canonical_asset
        for direction in RouteDirection
    )
    universe = tuple(plan for plan in evaluations if plan.universe_eligible)
    asset_liquidity: dict[str, Decimal] = {}
    for plan in universe:
        assert plan.route is not None
        asset_liquidity[plan.canonical_asset] = max(
            asset_liquidity.get(plan.canonical_asset, Decimal("0")),
            plan.route.route_liquidity_usd,
        )
    selected_assets = tuple(
        asset
        for asset, _ in sorted(
            asset_liquidity.items(), key=lambda item: (-item[1], item[0])
        )[: config.top_markets]
    )
    ranked = tuple(
        sorted(
            (plan for plan in universe if plan.canonical_asset in selected_assets),
            key=_rank_key,
        )[:20]
    )
    winner = next((plan for plan in ranked if plan.entry_allowed), None)
    return ScanSnapshot(logical_at, evaluations, selected_assets, ranked, winner)
