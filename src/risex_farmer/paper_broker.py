"""Atomic in-memory paper maker entry and immediate RISEx hedge."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from .config import PAPER_CONFIG, PaperConfig
from .economics import (
    exact_quantity_vwap,
    is_tick_aligned,
    maker_price,
    pair_price_pnl_usd,
    venue_fee_amount_usd,
)
from .models import (
    CanonicalMarket,
    DataQuality,
    Fee,
    Fill,
    FundingCashQuote,
    FundingAccrualMethod,
    FundingEvent,
    FundingQuality,
    LifecycleState,
    LiquidityRole,
    RouteDirection,
    Side,
    StreamHealth,
    TargetFundingCycle,
    TradeEvidence,
    Venue,
)
from .scanner import (
    MarketObservation,
    NoTradeReason,
    RoutePlan,
    ScanSnapshot,
    activation_schedule,
    trade_precedes_cutoff,
)


class PaperOrderStatus(StrEnum):
    OPEN = "OPEN"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"


class PaperVersionStatus(StrEnum):
    OPEN = "OPEN"
    REPLACED = "REPLACED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"


class CancellationReason(StrEnum):
    CUTOFF = "PAPER_ORDER_CANCELLED_CUTOFF"
    DATA_STALE = "PAPER_ORDER_CANCELLED_DATA_STALE"
    RISEX_ENTRY_DEPTH_UNAVAILABLE = (
        "PAPER_ORDER_CANCELLED_RISEX_ENTRY_DEPTH_UNAVAILABLE"
    )
    ROUTE_INVALID = "PAPER_ORDER_CANCELLED_ROUTE_INVALID"
    PLANNED_NET_NEGATIVE = "PAPER_ORDER_CANCELLED_PLANNED_NET_NEGATIVE"
    PROCESS_RESTART = "PAPER_ORDER_CANCELLED_PROCESS_RESTART"


class VersionCloseReason(StrEnum):
    PRICE_CHANGED = "PAPER_ORDER_VERSION_REPLACED_PRICE_CHANGED"


class TradeProcessOutcome(StrEnum):
    IGNORED = "IGNORED"
    ACCUMULATED = "ACCUMULATED"
    OPENED = "OPENED"
    CANCELLED = "CANCELLED"


@dataclass(frozen=True, slots=True)
class LockedRouteKey:
    canonical_asset: str
    risex_market: str
    hedge_venue: Venue
    hedge_market: str
    direction: RouteDirection
    cycle_id: str


@dataclass(frozen=True, slots=True)
class PaperOrderVersion:
    version_id: str
    number: int
    limit_price: Decimal
    created_at: datetime
    last_checked_at: datetime
    cumulative_eligible_quantity: Decimal
    status: PaperVersionStatus = PaperVersionStatus.OPEN
    closed_at: datetime | None = None
    close_reason: str | None = None


@dataclass(frozen=True, slots=True)
class PaperEntryOrder:
    attempt_id: str
    order_id: str
    route_key: LockedRouteKey
    route_plan: RoutePlan
    venue: Venue
    canonical_market: str
    order_type: str
    post_only: bool
    side: Side
    canonical_quantity: Decimal
    cutoff_at: datetime
    created_at: datetime
    versions: tuple[PaperOrderVersion, ...]
    status: PaperOrderStatus = PaperOrderStatus.OPEN
    cancelled_at: datetime | None = None
    cancellation_reason: CancellationReason | None = None

    @property
    def active_version(self) -> PaperOrderVersion:
        return self.versions[-1]


@dataclass(frozen=True, slots=True)
class PaperPosition:
    position_id: str
    route_key: LockedRouteKey
    direction: RouteDirection
    canonical_quantity: Decimal
    hedge_maker_fill: Fill
    risex_taker_fill: Fill
    hedge_maker_fill_exchange_at: datetime
    hedge_maker_fill_received_at: datetime
    risex_taker_fill_at: datetime
    position_opened_at: datetime
    recomputed_funding_quotes: tuple[FundingCashQuote, FundingCashQuote]
    target_cycle: TargetFundingCycle | None
    remaining_target_funding_usd: Decimal | None
    planned_maker_exit_net_pnl_usd: Decimal | None
    planned_hold_to_target_net_pnl_usd: Decimal | None
    executable_unwind_net_pnl_usd: Decimal | None
    entry_executable_basis: Decimal


@dataclass(frozen=True, slots=True)
class PaperEntryState:
    lifecycle_state: LifecycleState = LifecycleState.FLAT
    locked_route: LockedRouteKey | None = None
    order: PaperEntryOrder | None = None
    position: PaperPosition | None = None
    processed_trade_keys: frozenset[str] = frozenset()


@dataclass(frozen=True, slots=True)
class TradeProcessResult:
    outcome: TradeProcessOutcome
    state: PaperEntryState
    detail: str | None = None


FundingRecomputer = Callable[
    [RoutePlan, datetime],
    Awaitable[tuple[FundingCashQuote, FundingCashQuote]],
]


def _route_key(plan: RoutePlan) -> LockedRouteKey:
    if plan.target_cycle is None:
        raise ValueError("route plan has no target cycle")
    return LockedRouteKey(
        plan.canonical_asset,
        plan.risex_market.venue_symbol,
        plan.hedge_venue,
        plan.hedge_market.venue_symbol,
        plan.direction,
        plan.target_cycle.cycle_id,
    )


def _entry_sides(direction: RouteDirection) -> tuple[Side, Side]:
    if direction is RouteDirection.LONG_RISEX_SHORT_HEDGE:
        return Side.BUY, Side.SELL
    return Side.SELL, Side.BUY


def _fee_rate(config: PaperConfig, venue: Venue, role: LiquidityRole) -> Decimal:
    return {
        (Venue.RISEX, LiquidityRole.MAKER): config.risex_maker_fee_rate,
        (Venue.RISEX, LiquidityRole.TAKER): config.risex_taker_fee_rate,
        (Venue.EXTENDED, LiquidityRole.MAKER): config.extended_maker_fee_rate,
        (Venue.EXTENDED, LiquidityRole.TAKER): config.extended_taker_fee_rate,
        (Venue.NADO, LiquidityRole.MAKER): config.nado_maker_fee_rate,
        (Venue.NADO, LiquidityRole.TAKER): config.nado_taker_fee_rate,
    }[(venue, role)]


def _fee_source(venue: Venue) -> str:
    return {
        Venue.RISEX: "SYSTEM_SPEC:RISEX_TIER3_CONFIG",
        Venue.EXTENDED: "SYSTEM_SPEC:EXTENDED_PUBLIC_FEE",
        Venue.NADO: "SYSTEM_SPEC:NADO_CONFIGURED_ASSUMPTION",
    }[venue]


def _fee(
    config: PaperConfig,
    market: CanonicalMarket,
    role: LiquidityRole,
    quantity: Decimal,
    price: Decimal,
    at: datetime,
) -> Fee:
    venue = market.venue
    notional = abs(quantity * price)
    minimum = (
        market.minimum_fee_notional_usd
        if venue is Venue.NADO and role is LiquidityRole.TAKER
        else None
    )
    fee_base = max(notional, minimum) if minimum is not None else notional
    rate = _fee_rate(config, venue, role)
    amount = venue_fee_amount_usd(
        venue,
        role,
        notional,
        rate,
        market.minimum_fee_notional_usd,
    )
    return Fee(venue, role, notional, fee_base, rate, amount, _fee_source(venue), at)


def _observation_is_fresh(
    observation: MarketObservation,
    now: datetime,
    config: PaperConfig,
) -> bool:
    health: StreamHealth | None = observation.health
    return (
        observation.book is not None
        and observation.book.venue is observation.market.venue
        and observation.book.canonical_market == observation.market.venue_symbol
        and health is not None
        and health.data_quality is DataQuality.COMPLETE
        and health.stream_connected
        and health.book_initialized
        and health.book_sequence_valid
        and health.last_connection_confirmation_at is not None
        and now >= health.last_connection_confirmation_at
        and now - health.last_connection_confirmation_at
        <= timedelta(seconds=config.max_market_stream_silence_seconds)
    )


def _exact_vwap(
    observation: MarketObservation,
    side: Side,
    quantity: Decimal,
) -> Decimal | None:
    if observation.book is None:
        return None
    try:
        result = exact_quantity_vwap(
            side, quantity, observation.book.bids, observation.book.asks
        )
    except (TypeError, ValueError):
        return None
    return result.price if result.is_executable else None


def _maker_price(
    observation: MarketObservation,
    side: Side,
) -> Decimal | None:
    if observation.book is None or not observation.book.bids or not observation.book.asks:
        return None
    bid = max(level.canonical_price for level in observation.book.bids)
    ask = min(level.canonical_price for level in observation.book.asks)
    try:
        return maker_price(side, bid, ask, observation.market.tick_size_raw)
    except (TypeError, ValueError):
        return None


def _opposite(side: Side) -> Side:
    return Side.SELL if side is Side.BUY else Side.BUY


def _pair_pnl(
    direction: RouteDirection,
    quantity: Decimal,
    risex_entry: Decimal,
    risex_exit: Decimal,
    hedge_entry: Decimal,
    hedge_exit: Decimal,
) -> Decimal:
    if direction is RouteDirection.LONG_RISEX_SHORT_HEDGE:
        return pair_price_pnl_usd(
            quantity, risex_entry, risex_exit, hedge_entry, hedge_exit
        )
    return pair_price_pnl_usd(
        quantity, hedge_entry, hedge_exit, risex_entry, risex_exit
    )


def _entry_basis(
    direction: RouteDirection, risex_entry: Decimal, hedge_entry: Decimal
) -> Decimal:
    if direction is RouteDirection.LONG_RISEX_SHORT_HEDGE:
        return hedge_entry / risex_entry - Decimal("1")
    return risex_entry / hedge_entry - Decimal("1")


def _recomputed_cycle_and_cash(
    plan: RoutePlan,
    quantity: Decimal,
    opened_at: datetime,
    quotes: tuple[FundingCashQuote, FundingCashQuote],
) -> tuple[TargetFundingCycle | None, Decimal | None]:
    risex_quote, hedge_quote = quotes
    if (
        risex_quote.venue is not Venue.RISEX
        or risex_quote.canonical_market != plan.risex_market.venue_symbol
        or hedge_quote.venue is not plan.hedge_venue
        or hedge_quote.canonical_market != plan.hedge_market.venue_symbol
        or risex_quote.assumed_or_actual_position_opened_at != opened_at
        or hedge_quote.assumed_or_actual_position_opened_at != opened_at
        or risex_quote.quality is FundingQuality.UNKNOWN
        or hedge_quote.quality is FundingQuality.UNKNOWN
        or risex_quote.accrual_method is FundingAccrualMethod.UNKNOWN
        or hedge_quote.accrual_method is FundingAccrualMethod.UNKNOWN
        or risex_quote.settlement_at <= opened_at
        or hedge_quote.settlement_at <= opened_at
        or not risex_quote.eligibility_known
        or not hedge_quote.eligibility_known
    ):
        return None, None
    risex_long = plan.direction is RouteDirection.LONG_RISEX_SHORT_HEDGE
    risex_cash = (
        risex_quote.long_cash_per_canonical_base_usd
        if risex_long
        else risex_quote.short_cash_per_canonical_base_usd
    )
    hedge_cash = (
        hedge_quote.short_cash_per_canonical_base_usd
        if risex_long
        else hedge_quote.long_cash_per_canonical_base_usd
    )
    if risex_cash is None or hedge_cash is None:
        return None, None
    risex_expected = quantity * risex_cash
    hedge_expected = quantity * hedge_cash
    risex_event = FundingEvent(
        Venue.RISEX,
        plan.risex_market.venue_symbol,
        risex_quote.settlement_at,
        risex_expected,
        True,
    )
    hedge_event = FundingEvent(
        plan.hedge_venue,
        plan.hedge_market.venue_symbol,
        hedge_quote.settlement_at,
        hedge_expected,
        True,
    )
    start_at = min(risex_event.settlement_at, hedge_event.settlement_at)
    end_at = max(risex_event.settlement_at, hedge_event.settlement_at)
    locked_cycle = plan.target_cycle
    if (
        locked_cycle is None
        or risex_event.settlement_at != locked_cycle.risex_event.settlement_at
        or hedge_event.settlement_at != locked_cycle.hedge_event.settlement_at
    ):
        return None, None
    cycle = TargetFundingCycle(
        locked_cycle.cycle_id,
        start_at,
        end_at,
        (end_at - start_at).days * 86_400 + (end_at - start_at).seconds,
        risex_event,
        hedge_event,
    )
    return cycle, risex_expected + hedge_expected


class PaperEntryBroker:
    """Serializes paper entry evidence and exposes only complete state transitions."""

    def __init__(self, *, config: PaperConfig = PAPER_CONFIG) -> None:
        self.config = config
        self._state = PaperEntryState()
        self._lock = asyncio.Lock()

    @classmethod
    def from_state(
        cls,
        state: PaperEntryState,
        *,
        config: PaperConfig = PAPER_CONFIG,
    ) -> PaperEntryBroker:
        broker = cls(config=config)
        broker._state = state
        return broker

    @property
    def state(self) -> PaperEntryState:
        return self._state

    async def activate(
        self,
        snapshot: ScanSnapshot,
        *,
        attempt_id: str,
        activated_at: datetime,
    ) -> PaperEntryState:
        async with self._lock:
            if self._state.lifecycle_state is not LifecycleState.FLAT:
                raise ValueError("paper entry activation requires FLAT state")
            if self._state.position is not None:
                raise ValueError("paper entry activation requires no position")
            if self._state.order is not None and self._state.order.status is PaperOrderStatus.OPEN:
                raise ValueError("paper entry activation requires no active order")
            plan = snapshot.winner
            if plan is None or not plan.entry_allowed:
                raise ValueError("scan snapshot has no eligible winner")
            if snapshot.logical_at != activated_at or plan.logical_at != activated_at:
                raise ValueError("activation must use the shared scan timestamp")
            if (
                plan.route is None
                or plan.target_cycle is None
                or plan.canonical_quantity is None
                or plan.hedge_entry_price is None
                or plan.risex_entry_price is None
            ):
                raise ValueError("winner is incomplete")
            schedule = activation_schedule(plan.target_cycle)
            if not schedule.should_activate(activated_at):
                raise ValueError("winner is outside its activation window")
            _, hedge_side = _entry_sides(plan.direction)
            order_id = f"{attempt_id}:entry"
            version = PaperOrderVersion(
                f"{order_id}:v1",
                1,
                plan.hedge_entry_price,
                activated_at,
                activated_at,
                Decimal("0"),
            )
            route_key = _route_key(plan)
            order = PaperEntryOrder(
                attempt_id,
                order_id,
                route_key,
                plan,
                plan.hedge_venue,
                plan.hedge_market.venue_symbol,
                "LIMIT",
                True,
                hedge_side,
                plan.canonical_quantity,
                schedule.cutoff_at,
                activated_at,
                (version,),
            )
            self._state = PaperEntryState(
                LifecycleState.ENTRY_MAKER_OPEN,
                route_key,
                order,
                None,
                self._state.processed_trade_keys,
            )
            return self._state

    def _cancel_locked(
        self, reason: CancellationReason, cancelled_at: datetime
    ) -> PaperEntryState:
        order = self._require_open_order()
        current = replace(
            order.active_version,
            status=PaperVersionStatus.CANCELLED,
            closed_at=cancelled_at,
            close_reason=reason.value,
        )
        order = replace(
            order,
            versions=order.versions[:-1] + (current,),
            status=PaperOrderStatus.CANCELLED,
            cancelled_at=cancelled_at,
            cancellation_reason=reason,
        )
        self._state = replace(
            self._state,
            lifecycle_state=LifecycleState.FLAT,
            locked_route=None,
            order=order,
        )
        return self._state

    def _require_open_order(self) -> PaperEntryOrder:
        order = self._state.order
        if (
            self._state.lifecycle_state is not LifecycleState.ENTRY_MAKER_OPEN
            or order is None
            or order.status is not PaperOrderStatus.OPEN
        ):
            raise ValueError("no open paper entry order")
        return order

    async def cancel_for_process_restart(
        self, *, restarted_at: datetime
    ) -> PaperEntryState:
        """Apply the PAPER-004 restart contract without reconstructing fills."""
        async with self._lock:
            self._require_open_order()
            return self._cancel_locked(
                CancellationReason.PROCESS_RESTART, restarted_at
            )

    async def refresh(
        self,
        refreshed_plan: RoutePlan | None,
        risex_observation: MarketObservation,
        *,
        evaluated_at: datetime,
    ) -> PaperEntryState:
        async with self._lock:
            order = self._require_open_order()
            if evaluated_at >= order.cutoff_at:
                return self._cancel_locked(CancellationReason.CUTOFF, evaluated_at)
            stale_reasons = {
                NoTradeReason.BOOK_UNHEALTHY,
                NoTradeReason.FUNDING_STALE,
            }
            if (
                refreshed_plan is not None
                and stale_reasons.intersection(refreshed_plan.no_trade_reasons)
            ) or not _observation_is_fresh(
                risex_observation, evaluated_at, self.config
            ):
                return self._cancel_locked(CancellationReason.DATA_STALE, evaluated_at)
            risex_side, _ = _entry_sides(order.route_plan.direction)
            if (
                _exact_vwap(
                    risex_observation, risex_side, order.canonical_quantity
                )
                is None
            ):
                return self._cancel_locked(
                    CancellationReason.RISEX_ENTRY_DEPTH_UNAVAILABLE,
                    evaluated_at,
                )
            if (
                refreshed_plan is None
                or refreshed_plan.target_cycle is None
                or _route_key(refreshed_plan) != order.route_key
                or refreshed_plan.canonical_quantity != order.canonical_quantity
                or refreshed_plan.hedge_entry_price is None
                or any(
                    reason != NoTradeReason.PLANNED_NET_PNL_NEGATIVE
                    for reason in refreshed_plan.no_trade_reasons
                )
            ):
                return self._cancel_locked(CancellationReason.ROUTE_INVALID, evaluated_at)
            if (
                refreshed_plan.planned_maker_net_pnl_usd is None
                or refreshed_plan.planned_maker_net_pnl_usd
                < self.config.paper_entry_min_planned_net_pnl_usd
            ):
                return self._cancel_locked(
                    CancellationReason.PLANNED_NET_NEGATIVE, evaluated_at
                )

            current = order.active_version
            if (
                evaluated_at - current.last_checked_at
                < timedelta(seconds=self.config.entry_order_reprice_seconds)
            ):
                return self._state
            if refreshed_plan.hedge_entry_price == current.limit_price:
                checked = replace(current, last_checked_at=evaluated_at)
                order = replace(
                    order,
                    route_plan=refreshed_plan,
                    versions=order.versions[:-1] + (checked,),
                )
            else:
                closed = replace(
                    current,
                    status=PaperVersionStatus.REPLACED,
                    closed_at=evaluated_at,
                    close_reason=VersionCloseReason.PRICE_CHANGED.value,
                )
                number = current.number + 1
                replacement = PaperOrderVersion(
                    f"{order.order_id}:v{number}",
                    number,
                    refreshed_plan.hedge_entry_price,
                    evaluated_at,
                    evaluated_at,
                    Decimal("0"),
                )
                order = replace(
                    order,
                    route_plan=refreshed_plan,
                    versions=order.versions[:-1] + (closed, replacement),
                )
            self._state = replace(self._state, order=order)
            return self._state

    def _trade_is_eligible(
        self,
        order: PaperEntryOrder,
        trade: TradeEvidence,
        observed_version_id: str,
    ) -> bool:
        version = order.active_version
        if observed_version_id != version.version_id:
            return False
        if trade.venue is not order.venue or trade.canonical_market != order.canonical_market:
            return False
        if trade.is_orderbook_match is not True or not trade_precedes_cutoff(
            trade, order.cutoff_at
        ):
            return False
        if trade.canonical_quantity <= 0 or not is_tick_aligned(
            trade.canonical_price, order.route_plan.hedge_market.tick_size_raw
        ):
            return False
        tick = order.route_plan.hedge_market.tick_size_raw
        if order.side is Side.BUY:
            return (
                trade.aggressor_side is Side.SELL
                and trade.canonical_price <= version.limit_price - tick
            )
        return (
            trade.aggressor_side is Side.BUY
            and trade.canonical_price >= version.limit_price + tick
        )

    async def process_trade(
        self,
        trade: TradeEvidence,
        *,
        observed_version_id: str,
        processed_at: datetime,
        risex_observation: MarketObservation,
        hedge_observation: MarketObservation,
        recompute_funding: FundingRecomputer,
    ) -> TradeProcessResult:
        async with self._lock:
            order = self._require_open_order()
            if trade.trade_event_key in self._state.processed_trade_keys:
                return TradeProcessResult(
                    TradeProcessOutcome.IGNORED, self._state, "DUPLICATE_EVENT_KEY"
                )
            self._state = replace(
                self._state,
                processed_trade_keys=self._state.processed_trade_keys
                | {trade.trade_event_key},
            )
            if not self._trade_is_eligible(order, trade, observed_version_id):
                return TradeProcessResult(
                    TradeProcessOutcome.IGNORED, self._state, "TRADE_INELIGIBLE"
                )
            current = order.active_version
            cumulative = current.cumulative_eligible_quantity + trade.canonical_quantity
            if cumulative < order.canonical_quantity:
                current = replace(
                    current, cumulative_eligible_quantity=cumulative
                )
                order = replace(
                    order, versions=order.versions[:-1] + (current,)
                )
                self._state = replace(self._state, order=order)
                return TradeProcessResult(TradeProcessOutcome.ACCUMULATED, self._state)

            if not _observation_is_fresh(
                risex_observation, processed_at, self.config
            ) or not _observation_is_fresh(
                hedge_observation, processed_at, self.config
            ):
                state = self._cancel_locked(CancellationReason.DATA_STALE, processed_at)
                return TradeProcessResult(TradeProcessOutcome.CANCELLED, state)
            risex_side, _ = _entry_sides(order.route_plan.direction)
            risex_entry_price = _exact_vwap(
                risex_observation, risex_side, order.canonical_quantity
            )
            if risex_entry_price is None:
                state = self._cancel_locked(
                    CancellationReason.RISEX_ENTRY_DEPTH_UNAVAILABLE, processed_at
                )
                return TradeProcessResult(TradeProcessOutcome.CANCELLED, state)

            quotes = await recompute_funding(order.route_plan, processed_at)
            assert trade.exchange_timestamp is not None
            maker_fee = _fee(
                self.config,
                order.route_plan.hedge_market,
                LiquidityRole.MAKER,
                order.canonical_quantity,
                current.limit_price,
                trade.exchange_timestamp,
            )
            risex_fee = _fee(
                self.config,
                order.route_plan.risex_market,
                LiquidityRole.TAKER,
                order.canonical_quantity,
                risex_entry_price,
                processed_at,
            )
            maker_fill = Fill(
                order.venue,
                order.canonical_market,
                order.side,
                order.canonical_quantity,
                current.limit_price,
                trade.exchange_timestamp,
                trade.received_at,
                maker_fee,
            )
            risex_fill = Fill(
                Venue.RISEX,
                order.route_plan.risex_market.venue_symbol,
                risex_side,
                order.canonical_quantity,
                risex_entry_price,
                processed_at,
                processed_at,
                risex_fee,
            )
            position, lifecycle = self._open_position(
                order,
                maker_fill,
                risex_fill,
                quotes,
                risex_observation,
                hedge_observation,
                processed_at,
            )
            filled_version = replace(
                current,
                cumulative_eligible_quantity=order.canonical_quantity,
                status=PaperVersionStatus.FILLED,
                closed_at=processed_at,
            )
            filled_order = replace(
                order,
                versions=order.versions[:-1] + (filled_version,),
                status=PaperOrderStatus.FILLED,
            )
            self._state = PaperEntryState(
                lifecycle,
                order.route_key,
                filled_order,
                position,
                self._state.processed_trade_keys,
            )
            return TradeProcessResult(TradeProcessOutcome.OPENED, self._state)

    def _open_position(
        self,
        order: PaperEntryOrder,
        maker_fill: Fill,
        risex_fill: Fill,
        quotes: tuple[FundingCashQuote, FundingCashQuote],
        risex_observation: MarketObservation,
        hedge_observation: MarketObservation,
        opened_at: datetime,
    ) -> tuple[PaperPosition, LifecycleState]:
        quantity = order.canonical_quantity
        risex_exit = _exact_vwap(
            risex_observation, _opposite(risex_fill.side), quantity
        )
        hedge_exit_side = _opposite(order.side)
        hedge_maker_exit = _maker_price(hedge_observation, hedge_exit_side)
        hedge_taker_exit = _exact_vwap(
            hedge_observation, hedge_exit_side, quantity
        )
        cycle, remaining_funding = _recomputed_cycle_and_cash(
            order.route_plan, quantity, opened_at, quotes
        )
        maker_exit_net: Decimal | None = None
        hold_to_target: Decimal | None = None
        unwind_net: Decimal | None = None
        entry_fees = maker_fill.fee.amount_usd + risex_fill.fee.amount_usd
        if risex_exit is not None and hedge_maker_exit is not None:
            exit_pnl = _pair_pnl(
                order.route_plan.direction,
                quantity,
                risex_fill.canonical_price,
                risex_exit,
                maker_fill.canonical_price,
                hedge_maker_exit,
            )
            planned_exit_fees = (
                _fee(
                    self.config,
                    order.route_plan.risex_market,
                    LiquidityRole.TAKER,
                    quantity,
                    risex_exit,
                    opened_at,
                ).amount_usd
                + _fee(
                    self.config,
                    order.route_plan.hedge_market,
                    LiquidityRole.MAKER,
                    quantity,
                    hedge_maker_exit,
                    opened_at,
                ).amount_usd
            )
            maker_exit_net = exit_pnl - entry_fees - planned_exit_fees
            if remaining_funding is not None:
                hold_to_target = maker_exit_net + remaining_funding
        if risex_exit is not None and hedge_taker_exit is not None:
            unwind_pnl = _pair_pnl(
                order.route_plan.direction,
                quantity,
                risex_fill.canonical_price,
                risex_exit,
                maker_fill.canonical_price,
                hedge_taker_exit,
            )
            unwind_fees = (
                _fee(
                    self.config,
                    order.route_plan.risex_market,
                    LiquidityRole.TAKER,
                    quantity,
                    risex_exit,
                    opened_at,
                ).amount_usd
                + _fee(
                    self.config,
                    order.route_plan.hedge_market,
                    LiquidityRole.TAKER,
                    quantity,
                    hedge_taker_exit,
                    opened_at,
                ).amount_usd
            )
            unwind_net = unwind_pnl - entry_fees - unwind_fees
        lifecycle = (
            LifecycleState.HOLDING
            if maker_exit_net is not None
            and hold_to_target is not None
            and hold_to_target > maker_exit_net
            else LifecycleState.EXITING_NORMAL
        )
        position = PaperPosition(
            f"{order.attempt_id}:position",
            order.route_key,
            order.route_plan.direction,
            quantity,
            maker_fill,
            risex_fill,
            maker_fill.exchange_at,
            maker_fill.receipt_at,
            opened_at,
            opened_at,
            quotes,
            cycle,
            remaining_funding,
            maker_exit_net,
            hold_to_target,
            unwind_net,
            _entry_basis(
                order.route_plan.direction,
                risex_fill.canonical_price,
                maker_fill.canonical_price,
            ),
        )
        return position, lifecycle
