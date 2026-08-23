"""Deterministic in-memory paper position lifecycle and PnL authority."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from .config import PAPER_CONFIG, PaperConfig
from .economics import (
    applied_rate_complete,
    authoritative_funding_cash_usd,
    is_tick_aligned,
    maker_price,
    recognized_funding_cash_usd,
    replace_funding_settlement,
    spread_ticks,
)
from .models import (
    BookExecutionCapture,
    CanonicalMarket,
    DataQuality,
    Fill,
    FillProvenance,
    FundingEvent,
    FundingSettlement,
    LifecycleState,
    LiquidityRole,
    MakerFillProvenance,
    RouteDirection,
    SettlementStatus,
    Side,
    TargetFundingCycle,
    TakerFillProvenance,
    TradeEvidence,
    Venue,
)
from .paper_broker import (
    CancellationReason,
    PaperEntryState,
    PaperOrderStatus,
    PaperPosition,
    PaperVersionStatus,
    _exact_vwap,
    _fee,
    _observation_is_fresh,
    _opposite,
    _pair_pnl,
    _taker_provenance,
)
from .scanner import MarketObservation


class ExitVersionStatus(StrEnum):
    OPEN = "OPEN"
    REPLACED = "REPLACED"
    CANCELLED = "CANCELLED"
    FILLED = "FILLED"


class ExitVersionReason(StrEnum):
    PRICE_CHANGED = "PAPER_EXIT_ORDER_VERSION_REPLACED_PRICE_CHANGED"
    AGGRESSIVE_TRANSITION = (
        "PAPER_EXIT_ORDER_VERSION_REPLACED_AGGRESSIVE_TRANSITION"
    )
    UNWIND_UNAVAILABLE = "PAPER_EXIT_ORDER_CANCELLED_UNWIND_UNAVAILABLE"
    DATA_GAP = "PAPER_EXIT_ORDER_CANCELLED_DATA_GAP"
    PROCESS_RESTART = "PAPER_EXIT_ORDER_CANCELLED_PROCESS_RESTART"
    HARD_BASIS = "PAPER_EXIT_ORDER_CANCELLED_HARD_BASIS"


class LifecycleEventType(StrEnum):
    GAP_STARTED = "MARKET_DATA_GAP_STARTED"
    GAP_ENDED = "MARKET_DATA_GAP_ENDED"
    UNWIND_QUOTE_UNAVAILABLE = "UNWIND_QUOTE_UNAVAILABLE"
    EXITING_NORMAL_STARTED = "EXITING_NORMAL_STARTED"
    EXITING_AGGRESSIVE_STARTED = "EXITING_AGGRESSIVE_STARTED"
    POSITION_CLOSED = "PAPER_POSITION_CLOSED"


class CloseReason(StrEnum):
    NORMAL_MAKER = "NORMAL_MAKER"
    AGGRESSIVE_MAKER = "AGGRESSIVE_MAKER"
    HARD_BASIS = "HARD_BASIS"


class ExitTradeOutcome(StrEnum):
    IGNORED = "IGNORED"
    ACCUMULATED = "ACCUMULATED"
    CLOSED = "CLOSED"
    SUSPENDED = "SUSPENDED"


@dataclass(frozen=True, slots=True)
class LifecycleEvent:
    event_type: LifecycleEventType
    occurred_at: datetime
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class DataGap:
    started_at: datetime
    ended_at: datetime | None
    overlapped_funding: bool
    overlapped_exit: bool

    @property
    def duration(self) -> timedelta | None:
        return None if self.ended_at is None else self.ended_at - self.started_at


@dataclass(frozen=True, slots=True)
class ExitOrderVersion:
    version_id: str
    number: int
    mode: LifecycleState
    limit_price: Decimal
    created_at: datetime
    last_checked_at: datetime
    cumulative_eligible_quantity: Decimal = Decimal("0")
    status: ExitVersionStatus = ExitVersionStatus.OPEN
    closed_at: datetime | None = None
    close_reason: ExitVersionReason | None = None


@dataclass(frozen=True, slots=True)
class PaperExitOrder:
    order_id: str
    venue: Venue
    canonical_market: str
    order_type: str
    post_only: bool
    side: Side
    canonical_quantity: Decimal
    versions: tuple[ExitOrderVersion, ...]

    @property
    def active_version(self) -> ExitOrderVersion | None:
        version = self.versions[-1]
        return version if version.status is ExitVersionStatus.OPEN else None


@dataclass(frozen=True, slots=True)
class PositionSample:
    sampled_at: datetime
    lifecycle_recognized_funding_usd: Decimal | None
    remaining_funding_usd: Decimal | None
    planned_maker_exit_net_pnl_usd: Decimal | None
    planned_hold_to_target_net_pnl_usd: Decimal | None
    executable_unwind_net_pnl_usd: Decimal | None
    current_executable_basis: Decimal | None
    adverse_basis_expansion: Decimal | None
    data_quality: DataQuality


@dataclass(frozen=True, slots=True)
class ClosedTrade:
    position_id: str
    close_reason: CloseReason
    closed_at: datetime
    hedge_exit_fill: Fill
    risex_exit_fill: Fill
    actual_pair_pnl_usd: Decimal
    actual_fees_usd: Decimal
    simulated_recognized_funding_usd: Decimal | None
    simulated_closed_net_pnl_usd: Decimal | None
    applied_rate_funding_usd: Decimal | None
    applied_rate_closed_net_pnl_usd: Decimal | None
    exiting_normal_started_at: datetime | None
    exit_wait: timedelta | None
    funding_while_exiting_usd: Decimal | None
    pair_pnl_change_while_exiting_usd: Decimal | None
    data_quality: DataQuality
    primary_metrics_valid: bool


@dataclass(frozen=True, slots=True)
class LifecycleSnapshot:
    lifecycle_state: LifecycleState
    position: PaperPosition | None
    risex_market: CanonicalMarket
    hedge_market: CanonicalMarket
    settlements: tuple[FundingSettlement, ...] = ()
    active_cycle: TargetFundingCycle | None = None
    exit_order: PaperExitOrder | None = None
    processed_trade_keys: frozenset[str] = frozenset()
    data_quality: DataQuality = DataQuality.COMPLETE
    gaps: tuple[DataGap, ...] = ()
    events: tuple[LifecycleEvent, ...] = ()
    samples: tuple[PositionSample, ...] = ()
    exiting_normal_started_at: datetime | None = None
    aggressive_started_at: datetime | None = None
    exit_start_pair_pnl_usd: Decimal | None = None
    unwind_quote_unavailable: bool = False
    closed_trade: ClosedTrade | None = None

    @property
    def gap_open(self) -> bool:
        return bool(self.gaps and self.gaps[-1].ended_at is None)

    @property
    def gap_count(self) -> int:
        return len(self.gaps)

    @property
    def maximum_gap_duration(self) -> timedelta:
        durations = tuple(gap.duration for gap in self.gaps if gap.duration is not None)
        return max(durations, default=timedelta(0))


@dataclass(frozen=True, slots=True)
class ExitTradeResult:
    outcome: ExitTradeOutcome
    snapshot: LifecycleSnapshot
    detail: str | None = None
    fill_provenance: tuple[tuple[str, FillProvenance], ...] = ()


def _settlement_sort_key(row: FundingSettlement) -> tuple[datetime, str, str]:
    return row.settlement_at, row.venue.value, row.canonical_market


def _cycle_events(cycle: TargetFundingCycle) -> tuple[FundingEvent, FundingEvent]:
    return cycle.risex_event, cycle.hedge_event


def _settlement_for_event(event: FundingEvent, opened_at: datetime) -> FundingSettlement:
    status = (
        SettlementStatus.SKIPPED_POSITION_NOT_OPEN
        if event.settlement_at <= opened_at
        else SettlementStatus.PENDING
    )
    return FundingSettlement(
        event.venue, event.canonical_market, event.settlement_at, status
    )


_SIMULATED_RESOLVED_STATUSES = frozenset(
    {
        SettlementStatus.ESTIMATED,
        SettlementStatus.APPLIED_RATE,
        SettlementStatus.SKIPPED_POSITION_NOT_OPEN,
        SettlementStatus.SKIPPED_POSITION_CLOSED,
    }
)


def _simulated_primary_complete(rows: tuple[FundingSettlement, ...]) -> bool:
    return bool(rows) and all(
        row.status in _SIMULATED_RESOLVED_STATUSES for row in rows
    )


def _aggressive_price(observation: MarketObservation, side: Side) -> Decimal | None:
    book = observation.book
    if book is None or not book.bids or not book.asks:
        return None
    bid = max(level.canonical_price for level in book.bids)
    ask = min(level.canonical_price for level in book.asks)
    tick = observation.market.tick_size_raw
    try:
        ticks = spread_ticks(bid, ask, tick)
    except (TypeError, ValueError):
        return None
    if ticks == 1:
        return maker_price(side, bid, ask, tick)
    return ask - tick if side is Side.BUY else bid + tick


def _exit_maker_price(
    observation: MarketObservation, side: Side, mode: LifecycleState
) -> Decimal | None:
    if mode is LifecycleState.EXITING_AGGRESSIVE:
        return _aggressive_price(observation, side)
    book = observation.book
    if book is None or not book.bids or not book.asks:
        return None
    bid = max(level.canonical_price for level in book.bids)
    ask = min(level.canonical_price for level in book.asks)
    try:
        return maker_price(side, bid, ask, observation.market.tick_size_raw)
    except (TypeError, ValueError):
        return None


def _current_basis(
    position: PaperPosition, risex_exit: Decimal, hedge_exit: Decimal
) -> Decimal:
    if position.direction is RouteDirection.LONG_RISEX_SHORT_HEDGE:
        return hedge_exit / risex_exit - Decimal("1")
    return risex_exit / hedge_exit - Decimal("1")


def _actual_pair_pnl(
    position: PaperPosition, risex_exit: Decimal, hedge_exit: Decimal
) -> Decimal:
    return _pair_pnl(
        position.direction,
        position.canonical_quantity,
        position.risex_taker_fill.canonical_price,
        risex_exit,
        position.hedge_maker_fill.canonical_price,
        hedge_exit,
    )


def restart_paper_entry_state(
    state: PaperEntryState, *, restarted_at: datetime
) -> PaperEntryState:
    """Apply restart rules for FLAT or ENTRY_MAKER_OPEN without fill recovery."""
    if state.lifecycle_state is LifecycleState.FLAT:
        return state
    if state.lifecycle_state is not LifecycleState.ENTRY_MAKER_OPEN:
        raise ValueError("open positions restart through LifecycleEngine")
    order = state.order
    if order is None or order.status is not PaperOrderStatus.OPEN:
        raise ValueError("ENTRY_MAKER_OPEN requires an open order")
    version = replace(
        order.active_version,
        status=PaperVersionStatus.CANCELLED,
        closed_at=restarted_at,
        close_reason=CancellationReason.PROCESS_RESTART.value,
    )
    order = replace(
        order,
        versions=order.versions[:-1] + (version,),
        status=PaperOrderStatus.CANCELLED,
        cancelled_at=restarted_at,
        cancellation_reason=CancellationReason.PROCESS_RESTART,
    )
    return replace(
        state,
        lifecycle_state=LifecycleState.FLAT,
        locked_route=None,
        order=order,
    )


class LifecycleEngine:
    """Own one paper position from full entry through an atomic full close."""

    def __init__(
        self,
        entry_state: PaperEntryState,
        *,
        config: PaperConfig = PAPER_CONFIG,
    ) -> None:
        if entry_state.lifecycle_state not in {
            LifecycleState.HOLDING,
            LifecycleState.EXITING_NORMAL,
            LifecycleState.EXITING_AGGRESSIVE,
        }:
            raise ValueError("LifecycleEngine requires a fully open position")
        position = entry_state.position
        order = entry_state.order
        if position is None or order is None:
            raise ValueError("open lifecycle state requires position and entry order")
        settlements: tuple[FundingSettlement, ...] = ()
        evidence_cycle = position.target_cycle or order.route_plan.target_cycle
        if evidence_cycle is not None:
            settlements = tuple(
                sorted(
                    (
                        _settlement_for_event(event, position.position_opened_at)
                        for event in _cycle_events(evidence_cycle)
                    ),
                    key=_settlement_sort_key,
                )
            )
        exiting_at = (
            position.position_opened_at
            if entry_state.lifecycle_state
            in {LifecycleState.EXITING_NORMAL, LifecycleState.EXITING_AGGRESSIVE}
            else None
        )
        aggressive_at = (
            position.position_opened_at
            if entry_state.lifecycle_state is LifecycleState.EXITING_AGGRESSIVE
            else None
        )
        self.config = config
        self._lock = asyncio.Lock()
        self._fill_provenance: tuple[tuple[str, FillProvenance], ...] = ()
        self._qualifying_trades: dict[str, tuple[TradeEvidence, ...]] = {}
        self._snapshot = LifecycleSnapshot(
            entry_state.lifecycle_state,
            position,
            order.route_plan.risex_market,
            order.route_plan.hedge_market,
            settlements,
            position.target_cycle,
            processed_trade_keys=entry_state.processed_trade_keys,
            exiting_normal_started_at=exiting_at,
            aggressive_started_at=aggressive_at,
        )

    @classmethod
    def from_snapshot(
        cls,
        snapshot: LifecycleSnapshot,
        *,
        config: PaperConfig = PAPER_CONFIG,
    ) -> LifecycleEngine:
        engine = cls.__new__(cls)
        engine.config = config
        engine._lock = asyncio.Lock()
        engine._snapshot = snapshot
        engine._fill_provenance = ()
        engine._qualifying_trades = {}
        return engine

    def detached(self) -> LifecycleEngine:
        candidate = self.from_snapshot(self._snapshot, config=self.config)
        candidate._qualifying_trades = dict(self._qualifying_trades)
        return candidate

    @property
    def snapshot(self) -> LifecycleSnapshot:
        return self._snapshot

    def publish_snapshot(self, snapshot: LifecycleSnapshot) -> None:
        """Publish an already-persisted candidate without replacing this owner."""
        self._snapshot = snapshot
        active = None if snapshot.exit_order is None else snapshot.exit_order.active_version
        active_id = None if active is None else active.version_id
        self._qualifying_trades = {
            key: value for key, value in self._qualifying_trades.items()
            if key == active_id
        }

    def publish_candidate(self, candidate: LifecycleEngine) -> None:
        self._snapshot = candidate.snapshot
        self._qualifying_trades = dict(candidate._qualifying_trades)

    @property
    def fill_provenance(self) -> tuple[tuple[str, FillProvenance], ...]:
        return self._fill_provenance

    def _position(self) -> PaperPosition:
        position = self._snapshot.position
        if position is None:
            raise ValueError("no open paper position")
        return position

    def _settlement_map(self) -> dict[tuple[Venue, str, datetime], FundingSettlement]:
        return {row.key: row for row in self._snapshot.settlements}

    def _set_settlements(
        self, rows: dict[tuple[Venue, str, datetime], FundingSettlement]
    ) -> None:
        self._snapshot = replace(
            self._snapshot,
            settlements=tuple(sorted(rows.values(), key=_settlement_sort_key)),
        )

    def _append_event(
        self, event_type: LifecycleEventType, at: datetime, detail: str | None = None
    ) -> None:
        if self._snapshot.events and at < self._snapshot.events[-1].occurred_at:
            raise ValueError("lifecycle event time cannot move backwards")
        self._snapshot = replace(
            self._snapshot,
            events=self._snapshot.events + (LifecycleEvent(event_type, at, detail),),
        )

    def _register_cycle(self, cycle: TargetFundingCycle) -> None:
        position = self._position()
        rows = self._settlement_map()
        for event in _cycle_events(cycle):
            candidate = _settlement_for_event(event, position.position_opened_at)
            existing = rows.get(candidate.key)
            if existing is not None and existing != candidate:
                raise ValueError("funding event conflicts with authoritative settlement")
            rows.setdefault(candidate.key, candidate)
        self._set_settlements(rows)
        self._snapshot = replace(self._snapshot, active_cycle=cycle)

    async def reconcile_settlement(
        self, update: FundingSettlement
    ) -> LifecycleSnapshot:
        async with self._lock:
            self._reconcile_locked(update)
            if self._snapshot.closed_trade is not None:
                self._recompute_closed_locked()
            return self._snapshot

    def _reconcile_locked(self, update: FundingSettlement) -> None:
        rows = self._settlement_map()
        current = rows.get(update.key)
        if current is None:
            raise ValueError("settlement key is not part of a registered cycle")
        if current == update:
            return
        if current.status is update.status:
            raise ValueError("conflicting duplicate settlement status")
        rows[update.key] = replace_funding_settlement(
            current, update.status, update.cash_usd
        )
        self._set_settlements(rows)

    async def mark_extended_history_unresolved(
        self, update: FundingSettlement
    ) -> LifecycleSnapshot:
        """Fail closed when official Extended applied history has no exact row."""
        async with self._lock:
            rows = self._settlement_map()
            current = rows.get(update.key)
            if (
                current is None
                or update.venue is not Venue.EXTENDED
                or update.status is not SettlementStatus.UNRESOLVED
                or update.cash_usd is not None
                or current.status not in {
                    SettlementStatus.PENDING, SettlementStatus.ESTIMATED
                }
            ):
                return self._snapshot
            rows[update.key] = update
            self._set_settlements(rows)
            return self._snapshot

    def _elapsed_settlements(self, at: datetime) -> tuple[FundingSettlement, ...]:
        return tuple(row for row in self._snapshot.settlements if row.settlement_at <= at)

    def _recognized_at(self, at: datetime) -> Decimal | None:
        return recognized_funding_cash_usd(self._elapsed_settlements(at))

    def _cycle_resolved(self, cycle: TargetFundingCycle, at: datetime) -> bool:
        rows = self._settlement_map()
        for event in _cycle_events(cycle):
            row = rows[event.venue, event.canonical_market, event.settlement_at]
            if row.settlement_at > at or row.status in {
                SettlementStatus.PENDING,
                SettlementStatus.UNRESOLVED,
            }:
                return False
            if authoritative_funding_cash_usd(row) is None:
                return False
        return True

    def _remaining_funding(self, at: datetime) -> Decimal | None:
        cycle = self._snapshot.active_cycle
        if cycle is None:
            return None
        total = Decimal("0")
        for event in _cycle_events(cycle):
            if event.settlement_at <= at:
                continue
            if not event.eligibility_known or event.expected_cash_usd is None:
                return None
            total += event.expected_cash_usd
        return total

    def _exit_prices(
        self,
        risex: MarketObservation,
        hedge: MarketObservation,
        mode: LifecycleState,
    ) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
        position = self._position()
        quantity = position.canonical_quantity
        risex_price = _exact_vwap(
            risex, _opposite(position.risex_taker_fill.side), quantity
        )
        hedge_side = _opposite(position.hedge_maker_fill.side)
        hedge_maker = _exit_maker_price(hedge, hedge_side, mode)
        hedge_taker = _exact_vwap(hedge, hedge_side, quantity)
        return risex_price, hedge_maker, hedge_taker

    def _planned_metrics(
        self,
        at: datetime,
        risex_price: Decimal,
        hedge_maker_price: Decimal | None,
        hedge_taker_price: Decimal,
    ) -> tuple[Decimal | None, Decimal | None, Decimal, Decimal | None]:
        position = self._position()
        recognized = self._recognized_at(at)
        entry_fees = (
            position.hedge_maker_fill.fee.amount_usd
            + position.risex_taker_fill.fee.amount_usd
        )
        maker_net: Decimal | None = None
        if hedge_maker_price is not None and recognized is not None:
            maker_pair = _actual_pair_pnl(position, risex_price, hedge_maker_price)
            maker_exit_fees = (
                _fee(
                    self.config,
                    self._snapshot.risex_market,
                    LiquidityRole.TAKER,
                    position.canonical_quantity,
                    risex_price,
                    at,
                ).amount_usd
                + _fee(
                    self.config,
                    self._snapshot.hedge_market,
                    LiquidityRole.MAKER,
                    position.canonical_quantity,
                    hedge_maker_price,
                    at,
                ).amount_usd
            )
            maker_net = recognized + maker_pair - entry_fees - maker_exit_fees
        unwind_pair = _actual_pair_pnl(position, risex_price, hedge_taker_price)
        unwind_exit_fees = (
            _fee(
                self.config,
                self._snapshot.risex_market,
                LiquidityRole.TAKER,
                position.canonical_quantity,
                risex_price,
                at,
            ).amount_usd
            + _fee(
                self.config,
                self._snapshot.hedge_market,
                LiquidityRole.TAKER,
                position.canonical_quantity,
                hedge_taker_price,
                at,
            ).amount_usd
        )
        unwind = unwind_pair - entry_fees - unwind_exit_fees
        remaining = self._remaining_funding(at)
        hold = (
            None
            if maker_net is None or remaining is None
            else maker_net + remaining
        )
        return maker_net, hold, unwind, recognized

    def _cancel_exit_version(
        self, reason: ExitVersionReason, at: datetime
    ) -> None:
        order = self._snapshot.exit_order
        if order is None or order.active_version is None:
            return
        active = order.active_version
        self._qualifying_trades.pop(active.version_id, None)
        causal_at = max(at, active.created_at, active.last_checked_at)
        version = replace(
            active,
            status=ExitVersionStatus.CANCELLED,
            closed_at=causal_at,
            close_reason=reason,
        )
        order = replace(order, versions=order.versions[:-1] + (version,))
        self._snapshot = replace(self._snapshot, exit_order=order)

    def _new_exit_version(
        self, price: Decimal, mode: LifecycleState, at: datetime
    ) -> None:
        position = self._position()
        order = self._snapshot.exit_order
        if order is None:
            order_id = f"{position.position_id}:exit"
            number = 1
            versions: tuple[ExitOrderVersion, ...] = ()
            order = PaperExitOrder(
                order_id,
                position.hedge_maker_fill.venue,
                position.hedge_maker_fill.canonical_market,
                "LIMIT",
                True,
                _opposite(position.hedge_maker_fill.side),
                position.canonical_quantity,
                (),
            )
        else:
            order_id = order.order_id
            number = order.versions[-1].number + 1
            versions = order.versions
        version = ExitOrderVersion(
            f"{order_id}:v{number}", number, mode, price, at, at
        )
        self._snapshot = replace(
            self._snapshot, exit_order=replace(order, versions=versions + (version,))
        )

    def _ensure_exit_version(
        self,
        at: datetime,
        hedge: MarketObservation,
        *,
        aggressive_transition: bool = False,
    ) -> None:
        mode = self._snapshot.lifecycle_state
        price = _exit_maker_price(
            hedge, _opposite(self._position().hedge_maker_fill.side), mode
        )
        if price is None:
            return
        order = self._snapshot.exit_order
        active = None if order is None else order.active_version
        if active is None:
            self._new_exit_version(price, mode, at)
            return
        if aggressive_transition:
            self._qualifying_trades.pop(active.version_id, None)
            closed = replace(
                active,
                status=ExitVersionStatus.REPLACED,
                closed_at=at,
                close_reason=ExitVersionReason.AGGRESSIVE_TRANSITION,
            )
            assert order is not None
            self._snapshot = replace(
                self._snapshot,
                exit_order=replace(
                    order, versions=order.versions[:-1] + (closed,)
                ),
            )
            self._new_exit_version(price, mode, at)
            return
        if (
            at - active.last_checked_at
            < timedelta(seconds=self.config.entry_order_reprice_seconds)
        ):
            return
        assert order is not None
        if active.limit_price == price:
            checked = replace(active, last_checked_at=at)
            self._snapshot = replace(
                self._snapshot,
                exit_order=replace(
                    order, versions=order.versions[:-1] + (checked,)
                ),
            )
            return
        closed = replace(
            active,
            status=ExitVersionStatus.REPLACED,
            closed_at=at,
            close_reason=ExitVersionReason.PRICE_CHANGED,
        )
        self._qualifying_trades.pop(active.version_id, None)
        self._snapshot = replace(
            self._snapshot,
            exit_order=replace(order, versions=order.versions[:-1] + (closed,)),
        )
        self._new_exit_version(price, mode, at)

    def _start_normal_exit(
        self,
        at: datetime,
        hedge: MarketObservation,
        maker_pair_pnl: Decimal | None,
    ) -> None:
        if self._snapshot.lifecycle_state is LifecycleState.HOLDING:
            self._snapshot = replace(
                self._snapshot,
                lifecycle_state=LifecycleState.EXITING_NORMAL,
                exiting_normal_started_at=at,
                exit_start_pair_pnl_usd=maker_pair_pnl,
            )
            self._append_event(LifecycleEventType.EXITING_NORMAL_STARTED, at)
        self._ensure_exit_version(at, hedge)

    def _start_gap(self, at: datetime) -> None:
        if self._snapshot.gap_open:
            return
        overlapped_exit = self._snapshot.lifecycle_state in {
            LifecycleState.EXITING_NORMAL,
            LifecycleState.EXITING_AGGRESSIVE,
        }
        self._cancel_exit_version(ExitVersionReason.DATA_GAP, at)
        gap = DataGap(at, None, False, overlapped_exit)
        self._snapshot = replace(
            self._snapshot,
            data_quality=DataQuality.DEGRADED,
            gaps=self._snapshot.gaps + (gap,),
        )
        self._append_event(LifecycleEventType.GAP_STARTED, at)

    async def start_gap(self, *, started_at: datetime) -> LifecycleSnapshot:
        async with self._lock:
            self._position()
            self._start_gap(started_at)
            return self._snapshot

    def _end_gap(self, at: datetime) -> None:
        if not self._snapshot.gap_open:
            raise ValueError("no open market-data gap")
        gap = self._snapshot.gaps[-1]
        overlapped_funding = any(
            gap.started_at <= row.settlement_at <= at
            for row in self._snapshot.settlements
        )
        closed = replace(
            gap, ended_at=at, overlapped_funding=overlapped_funding
        )
        self._snapshot = replace(
            self._snapshot, gaps=self._snapshot.gaps[:-1] + (closed,)
        )
        self._append_event(LifecycleEventType.GAP_ENDED, at)

    def _record_unavailable(self, at: datetime) -> None:
        if not self._snapshot.unwind_quote_unavailable:
            self._append_event(LifecycleEventType.UNWIND_QUOTE_UNAVAILABLE, at)
        self._snapshot = replace(
            self._snapshot,
            data_quality=DataQuality.DEGRADED,
            unwind_quote_unavailable=True,
        )

    def _append_sample(
        self,
        at: datetime,
        recognized: Decimal | None,
        remaining: Decimal | None,
        maker_net: Decimal | None,
        hold: Decimal | None,
        unwind: Decimal | None,
        basis: Decimal | None,
    ) -> None:
        adverse = (
            None
            if basis is None
            else basis - self._position().entry_executable_basis
        )
        sample = PositionSample(
            at,
            recognized,
            remaining,
            maker_net,
            hold,
            unwind,
            basis,
            adverse,
            self._snapshot.data_quality,
        )
        self._snapshot = replace(
            self._snapshot, samples=self._snapshot.samples + (sample,)
        )

    def _maybe_register_next_cycle(
        self, at: datetime, next_cycle: TargetFundingCycle | None
    ) -> None:
        active = self._snapshot.active_cycle
        if active is None or not self._cycle_resolved(active, at):
            return
        if next_cycle is None:
            return
        if next_cycle.cycle_id == active.cycle_id:
            raise ValueError("next funding cycle must have a new identity")
        if any(event.settlement_at <= active.end_at for event in _cycle_events(next_cycle)):
            raise ValueError("next funding cycle must follow the resolved cycle")
        self._register_cycle(next_cycle)

    async def evaluate(
        self,
        *,
        evaluated_at: datetime,
        risex_observation: MarketObservation,
        hedge_observation: MarketObservation,
        next_cycle: TargetFundingCycle | None = None,
        record_sample: bool = True,
        hard_basis_only: bool = False,
        risex_capture: BookExecutionCapture | None = None,
        hedge_capture: BookExecutionCapture | None = None,
    ) -> LifecycleSnapshot:
        async with self._lock:
            self._fill_provenance = ()
            return self._evaluate_locked(
                evaluated_at, risex_observation, hedge_observation, next_cycle,
                record_sample=record_sample,
                hard_basis_only=hard_basis_only,
                risex_capture=risex_capture,
                hedge_capture=hedge_capture,
            )

    def _evaluate_locked(
        self,
        at: datetime,
        risex: MarketObservation,
        hedge: MarketObservation,
        next_cycle: TargetFundingCycle | None,
        *,
        record_sample: bool = True,
        hard_basis_only: bool = False,
        risex_capture: BookExecutionCapture | None = None,
        hedge_capture: BookExecutionCapture | None = None,
    ) -> LifecycleSnapshot:
        position = self._position()
        if self._snapshot.gap_open:
            return self._snapshot
        if not _observation_is_fresh(
            risex, at, self.config
        ) or not _observation_is_fresh(hedge, at, self.config):
            self._start_gap(at)
            return self._snapshot
        risex_exit, hedge_maker, hedge_taker = self._exit_prices(
            risex, hedge, self._snapshot.lifecycle_state
        )
        if risex_exit is None or hedge_taker is None:
            if hard_basis_only:
                return self._snapshot
            self._record_unavailable(at)
            if risex_exit is None:
                self._cancel_exit_version(
                    ExitVersionReason.UNWIND_UNAVAILABLE, at
                )
            if record_sample:
                self._append_sample(
                    at, self._recognized_at(at), self._remaining_funding(at),
                    None, None, None, None
                )
            return self._snapshot
        self._snapshot = replace(self._snapshot, unwind_quote_unavailable=False)
        basis = _current_basis(position, risex_exit, hedge_taker)
        adverse = basis - position.entry_executable_basis
        threshold = (
            self.config.btc_eth_hard_basis_expansion_rate
            if position.route_key.canonical_asset.upper() in {"BTC", "ETH"}
            else self.config.other_top5_hard_basis_expansion_rate
        )
        if adverse >= threshold:
            risex_side = _opposite(position.risex_taker_fill.side)
            hedge_side = _opposite(position.hedge_maker_fill.side)
            risex_proof = _taker_provenance(
                risex_capture,
                risex_side,
                position.canonical_quantity,
                venue=self._snapshot.risex_market.venue,
                canonical_market=self._snapshot.risex_market.venue_symbol,
                config=self.config,
            )
            hedge_proof = _taker_provenance(
                hedge_capture,
                hedge_side,
                position.canonical_quantity,
                venue=self._snapshot.hedge_market.venue,
                canonical_market=self._snapshot.hedge_market.venue_symbol,
                config=self.config,
            )
            if risex_proof is None or hedge_proof is None:
                self._record_unavailable(at)
                return self._snapshot
            self._cancel_exit_version(ExitVersionReason.HARD_BASIS, at)
            hedge_fill = self._taker_fill(
                self._snapshot.hedge_market,
                hedge_side,
                hedge_proof.vwap_price,
                at,
            )
            risex_fill = self._taker_fill(
                self._snapshot.risex_market,
                risex_side,
                risex_proof.vwap_price,
                at,
            )
            self._close_locked(
                hedge_fill, risex_fill, CloseReason.HARD_BASIS, at
            )
            position_id = position.position_id
            self._fill_provenance = (
                (f"{position_id}:hedge-exit", hedge_proof),
                (f"{position_id}:risex-exit", risex_proof),
            )
            return self._snapshot

        if hard_basis_only:
            return self._snapshot

        self._maybe_register_next_cycle(at, next_cycle)
        maker_net, hold, unwind, recognized = self._planned_metrics(
            at, risex_exit, hedge_maker, hedge_taker
        )
        remaining = self._remaining_funding(at)
        if record_sample:
            self._append_sample(
                at, recognized, remaining, maker_net, hold, unwind, basis
            )
        if (
            self._snapshot.lifecycle_state
            in {LifecycleState.EXITING_NORMAL, LifecycleState.EXITING_AGGRESSIVE}
            and self._snapshot.exit_start_pair_pnl_usd is None
            and hedge_maker is not None
        ):
            self._snapshot = replace(
                self._snapshot,
                exit_start_pair_pnl_usd=_actual_pair_pnl(
                    position, risex_exit, hedge_maker
                ),
            )
        if self._snapshot.lifecycle_state is LifecycleState.HOLDING:
            if maker_net is not None and hold is not None and hold > maker_net:
                return self._snapshot
            baseline = (
                None
                if hedge_maker is None
                else _actual_pair_pnl(position, risex_exit, hedge_maker)
            )
            self._start_normal_exit(at, hedge, baseline)
            return self._snapshot

        if self._snapshot.lifecycle_state is LifecycleState.EXITING_NORMAL:
            started = self._snapshot.exiting_normal_started_at
            if started is None:
                raise AssertionError("normal exit requires a sticky start time")
            if (
                at - started
                >= timedelta(seconds=self.config.normal_exit_aggressive_after_seconds)
            ):
                self._snapshot = replace(
                    self._snapshot,
                    lifecycle_state=LifecycleState.EXITING_AGGRESSIVE,
                    aggressive_started_at=self._snapshot.aggressive_started_at or at,
                )
                self._append_event(LifecycleEventType.EXITING_AGGRESSIVE_STARTED, at)
                self._ensure_exit_version(
                    at, hedge, aggressive_transition=True
                )
                return self._snapshot
        self._ensure_exit_version(at, hedge)
        return self._snapshot

    async def recover(
        self,
        *,
        recovered_at: datetime,
        risex_observation: MarketObservation,
        hedge_observation: MarketObservation,
        next_cycle: TargetFundingCycle | None = None,
        settlement_updates: tuple[FundingSettlement, ...] = (),
    ) -> LifecycleSnapshot:
        async with self._lock:
            if not _observation_is_fresh(
                risex_observation, recovered_at, self.config
            ) or not _observation_is_fresh(
                hedge_observation, recovered_at, self.config
            ):
                raise ValueError("gap recovery requires healthy snapshots for both legs")
            self._end_gap(recovered_at)
            for update in settlement_updates:
                self._reconcile_locked(update)
            return self._evaluate_locked(
                recovered_at, risex_observation, hedge_observation, next_cycle
            )

    async def restart(
        self,
        *,
        last_known_at: datetime,
        recovered_at: datetime,
        risex_observation: MarketObservation,
        hedge_observation: MarketObservation,
        next_cycle: TargetFundingCycle | None = None,
        settlement_updates: tuple[FundingSettlement, ...] = (),
    ) -> LifecycleSnapshot:
        async with self._lock:
            self._position()
            self._cancel_exit_version(
                ExitVersionReason.PROCESS_RESTART, recovered_at
            )
            self._start_gap(last_known_at)
            if not _observation_is_fresh(
                risex_observation, recovered_at, self.config
            ) or not _observation_is_fresh(
                hedge_observation, recovered_at, self.config
            ):
                return self._snapshot
            self._end_gap(recovered_at)
            for update in settlement_updates:
                self._reconcile_locked(update)
            return self._evaluate_locked(
                recovered_at, risex_observation, hedge_observation, next_cycle
            )

    def _taker_fill(
        self,
        market: CanonicalMarket,
        side: Side,
        price: Decimal,
        at: datetime,
    ) -> Fill:
        position = self._position()
        fee = _fee(
            self.config,
            market,
            LiquidityRole.TAKER,
            position.canonical_quantity,
            price,
            at,
        )
        return Fill(
            market.venue,
            market.venue_symbol,
            side,
            position.canonical_quantity,
            price,
            at,
            at,
            fee,
        )

    def _trade_is_eligible(
        self,
        order: PaperExitOrder,
        trade: TradeEvidence,
        observed_version_id: str,
    ) -> bool:
        version = order.active_version
        if version is None or observed_version_id != version.version_id:
            return False
        if trade.venue is not order.venue or trade.canonical_market != order.canonical_market:
            return False
        if trade.is_orderbook_match is not True or trade.exchange_timestamp is None:
            return False
        tick = self._snapshot.hedge_market.tick_size_raw
        if trade.canonical_quantity <= 0 or not is_tick_aligned(
            trade.canonical_price, tick
        ):
            return False
        if order.side is Side.BUY:
            return (
                trade.aggressor_side is Side.SELL
                and trade.canonical_price <= version.limit_price - tick
            )
        return (
            trade.aggressor_side is Side.BUY
            and trade.canonical_price >= version.limit_price + tick
        )

    async def process_exit_trade(
        self,
        trade: TradeEvidence,
        *,
        observed_version_id: str,
        processed_at: datetime,
        risex_observation: MarketObservation,
        hedge_observation: MarketObservation,
        risex_capture: BookExecutionCapture | None = None,
    ) -> ExitTradeResult:
        async with self._lock:
            self._fill_provenance = ()
            self._position()
            order = self._snapshot.exit_order
            if order is None or order.active_version is None:
                return ExitTradeResult(
                    ExitTradeOutcome.IGNORED, self._snapshot, "NO_ACTIVE_EXIT_VERSION"
                )
            if observed_version_id != order.active_version.version_id:
                return ExitTradeResult(
                    ExitTradeOutcome.IGNORED, self._snapshot, "STALE_EXIT_VERSION"
                )
            if trade.trade_event_key in self._snapshot.processed_trade_keys:
                return ExitTradeResult(
                    ExitTradeOutcome.IGNORED, self._snapshot, "DUPLICATE_EVENT_KEY"
                )
            self._snapshot = replace(
                self._snapshot,
                processed_trade_keys=self._snapshot.processed_trade_keys
                | {trade.trade_event_key},
            )
            if not self._trade_is_eligible(order, trade, observed_version_id):
                return ExitTradeResult(
                    ExitTradeOutcome.IGNORED, self._snapshot, "TRADE_INELIGIBLE"
                )
            if (
                trade.exchange_timestamp is None
                or trade.exchange_timestamp > processed_at
                or trade.received_at > processed_at
            ):
                return ExitTradeResult(
                    ExitTradeOutcome.IGNORED, self._snapshot, "TRADE_FUTURE_DATED"
                )
            active = order.active_version
            assert active is not None
            qualifying_trades = self._qualifying_trades.get(
                active.version_id, ()
            ) + (trade,)
            self._qualifying_trades[active.version_id] = qualifying_trades
            cumulative = active.cumulative_eligible_quantity + trade.canonical_quantity
            if cumulative < order.canonical_quantity:
                active = replace(
                    active, cumulative_eligible_quantity=cumulative,
                )
                order = replace(
                    order, versions=order.versions[:-1] + (active,)
                )
                self._snapshot = replace(self._snapshot, exit_order=order)
                return ExitTradeResult(
                    ExitTradeOutcome.ACCUMULATED, self._snapshot
                )
            if not _observation_is_fresh(
                risex_observation, processed_at, self.config
            ) or not _observation_is_fresh(
                hedge_observation, processed_at, self.config
            ):
                self._start_gap(processed_at)
                return ExitTradeResult(
                    ExitTradeOutcome.SUSPENDED, self._snapshot, "DATA_GAP"
                )
            position = self._position()
            risex_side = _opposite(position.risex_taker_fill.side)
            taker_proof = _taker_provenance(
                risex_capture,
                risex_side,
                position.canonical_quantity,
                venue=self._snapshot.risex_market.venue,
                canonical_market=self._snapshot.risex_market.venue_symbol,
                config=self.config,
            )
            if taker_proof is None:
                self._record_unavailable(processed_at)
                self._cancel_exit_version(
                    ExitVersionReason.UNWIND_UNAVAILABLE, processed_at
                )
                return ExitTradeResult(
                    ExitTradeOutcome.SUSPENDED,
                    self._snapshot,
                    ExitVersionReason.UNWIND_UNAVAILABLE.value,
                )
            risex_price = taker_proof.vwap_price
            assert trade.exchange_timestamp is not None
            hedge_fee = _fee(
                self.config,
                self._snapshot.hedge_market,
                LiquidityRole.MAKER,
                position.canonical_quantity,
                active.limit_price,
                trade.exchange_timestamp,
            )
            hedge_fill = Fill(
                order.venue,
                order.canonical_market,
                order.side,
                position.canonical_quantity,
                active.limit_price,
                trade.exchange_timestamp,
                trade.received_at,
                hedge_fee,
            )
            risex_fill = self._taker_fill(
                self._snapshot.risex_market,
                _opposite(position.risex_taker_fill.side),
                risex_price,
                processed_at,
            )
            reason = (
                CloseReason.AGGRESSIVE_MAKER
                if active.mode is LifecycleState.EXITING_AGGRESSIVE
                else CloseReason.NORMAL_MAKER
            )
            filled = replace(
                active,
                cumulative_eligible_quantity=order.canonical_quantity,
                status=ExitVersionStatus.FILLED,
                closed_at=processed_at,
            )
            self._snapshot = replace(
                self._snapshot,
                exit_order=replace(
                    order, versions=order.versions[:-1] + (filled,)
                ),
            )
            self._close_locked(hedge_fill, risex_fill, reason, processed_at)
            maker_proof = MakerFillProvenance(
                order.venue, order.canonical_market, order.side, order.order_id,
                active.version_id, active.limit_price,
                self._snapshot.hedge_market.tick_size_raw, qualifying_trades,
                processed_at,
            )
            proofs = (
                (f"{position.position_id}:hedge-exit", maker_proof),
                (f"{position.position_id}:risex-exit", taker_proof),
            )
            self._fill_provenance = proofs
            return ExitTradeResult(
                ExitTradeOutcome.CLOSED, self._snapshot,
                fill_provenance=proofs,
            )

    def _mark_future_closed_skips(self, closed_at: datetime) -> None:
        rows = self._settlement_map()
        for key, row in tuple(rows.items()):
            if (
                row.status is SettlementStatus.PENDING
                and closed_at <= row.settlement_at
            ):
                rows[key] = replace_funding_settlement(
                    row, SettlementStatus.SKIPPED_POSITION_CLOSED, None
                )
        self._set_settlements(rows)

    def _close_locked(
        self,
        hedge_fill: Fill,
        risex_fill: Fill,
        reason: CloseReason,
        closed_at: datetime,
    ) -> None:
        position = self._position()
        self._mark_future_closed_skips(closed_at)
        pair_pnl = _actual_pair_pnl(
            position, risex_fill.canonical_price, hedge_fill.canonical_price
        )
        fees = sum(
            (
                position.hedge_maker_fill.fee.amount_usd,
                position.risex_taker_fill.fee.amount_usd,
                hedge_fill.fee.amount_usd,
                risex_fill.fee.amount_usd,
            ),
            Decimal("0"),
        )
        closed = ClosedTrade(
            position.position_id,
            reason,
            closed_at,
            hedge_fill,
            risex_fill,
            pair_pnl,
            fees,
            None,
            None,
            None,
            None,
            self._snapshot.exiting_normal_started_at,
            (
                None
                if self._snapshot.exiting_normal_started_at is None
                else closed_at - self._snapshot.exiting_normal_started_at
            ),
            None,
            (
                None
                if self._snapshot.exit_start_pair_pnl_usd is None
                else pair_pnl - self._snapshot.exit_start_pair_pnl_usd
            ),
            self._snapshot.data_quality,
            False,
        )
        self._snapshot = replace(
            self._snapshot,
            lifecycle_state=LifecycleState.FLAT,
            position=None,
            closed_trade=closed,
        )
        self._recompute_closed_locked()
        self._append_event(LifecycleEventType.POSITION_CLOSED, closed_at, reason.value)

    def _recompute_closed_locked(self) -> None:
        closed = self._snapshot.closed_trade
        if closed is None:
            return
        recognized = recognized_funding_cash_usd(self._snapshot.settlements)
        simulated = (
            None
            if recognized is None
            else recognized
            + closed.actual_pair_pnl_usd
            - closed.actual_fees_usd
        )
        required_rows = self._snapshot.settlements
        applied_funding = (
            recognized
            if required_rows and applied_rate_complete(required_rows)
            else None
        )
        applied_net = (
            None
            if applied_funding is None
            else applied_funding
            + closed.actual_pair_pnl_usd
            - closed.actual_fees_usd
        )
        funding_while_exiting: Decimal | None = Decimal("0")
        started = closed.exiting_normal_started_at
        if started is None:
            funding_while_exiting = None
        else:
            rows = tuple(
                row
                for row in self._snapshot.settlements
                if started <= row.settlement_at <= closed.closed_at
            )
            funding_while_exiting = recognized_funding_cash_usd(rows)
        self._snapshot = replace(
            self._snapshot,
            closed_trade=replace(
                closed,
                simulated_recognized_funding_usd=recognized,
                simulated_closed_net_pnl_usd=simulated,
                applied_rate_funding_usd=applied_funding,
                applied_rate_closed_net_pnl_usd=applied_net,
                funding_while_exiting_usd=funding_while_exiting,
                primary_metrics_valid=(
                    closed.data_quality is DataQuality.COMPLETE
                    and _simulated_primary_complete(required_rows)
                ),
            ),
        )
