"""Single-process public-data runtime for the existing paper domain path."""

from __future__ import annotations

import asyncio
import json
import signal
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Protocol

import aiohttp

from .config import PAPER_CONFIG, PaperConfig
from .exchanges.base import PublicAdapter, PublicDataUnavailable
from .exchanges.extended import ExtendedAdapter
from .exchanges.nado import NadoAdapter
from .exchanges.risex import RisexAdapter
from .lifecycle import LifecycleEngine, LifecycleSnapshot
from .market_data import MarketDataCoordinator
from .models import (
    BookDelta,
    ContractType,
    DataQuality,
    FundingCashQuote,
    FundingQuality,
    FundingSettlement,
    LifecycleState,
    MarketVolume,
    MarketType,
    OrderBook,
    RouteDirection,
    SettlementStatus,
    TradeEvidence,
    TargetFundingCycle,
    Venue,
)
from .notifications import (
    NotificationOutbox,
    NotificationPayload,
    format_telegram_money,
    full_scan_digest_payloads,
    utc_time,
)
from .paper_broker import PaperEntryBroker, PaperEntryState
from .scanner import (
    MarketObservation,
    RoutePlan,
    ScanSnapshot,
    activation_schedule,
    planned_fee_split,
    scan_once,
)
from .storage import PaperRepository


class Clock(Protocol):
    def now(self) -> datetime: ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


def _next_absolute_slot(
    scheduled_at: datetime, now: datetime, cadence_seconds: int
) -> datetime:
    """Advance beyond now without drifting or replaying missed periodic slots."""
    cadence = timedelta(seconds=cadence_seconds)
    if scheduled_at > now:
        return scheduled_at
    missed = int((now - scheduled_at) // cadence) + 1
    return scheduled_at + cadence * missed


def _public_session() -> aiohttp.ClientSession:
    return aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))


def _http_status(exc: BaseException) -> int | None:
    status = getattr(exc, "status", None)
    if status is None and exc.__cause__ is not None:
        status = getattr(exc.__cause__, "status", None)
    return status if isinstance(status, int) else None


@dataclass(slots=True)
class VenueReadiness:
    available: bool
    detail: str
    updated_at: datetime


def _assumption_flags() -> dict[str, bool]:
    return {
        "risex_contract_and_quantity_are_paper_assumptions": True,
        "risex_funding_eligibility_is_a_paper_assumption": True,
        "risex_next_rate_estimate_is_a_paper_assumption": True,
        "risex_assumed_funding_is_not_official_applied_funding": True,
    }


def _quote_for_open_time(
    quote: FundingCashQuote, opened_at: datetime
) -> FundingCashQuote:
    if quote.assumed_or_actual_position_opened_at == opened_at:
        return quote
    if quote.eligibility_known and opened_at >= quote.settlement_at:
        return replace(
            quote,
            assumed_or_actual_position_opened_at=opened_at,
            long_cash_per_canonical_base_usd=Decimal("0"),
            short_cash_per_canonical_base_usd=Decimal("0"),
        )
    return replace(quote, assumed_or_actual_position_opened_at=opened_at)


def _route_rank_key(plan: RoutePlan) -> tuple[object, ...]:
    assert plan.route is not None and plan.target_cycle is not None
    assert plan.planned_maker_net_pnl_usd is not None
    return (
        -plan.planned_maker_net_pnl_usd,
        -plan.route.route_liquidity_usd,
        plan.target_cycle.start_at,
        plan.canonical_asset,
        plan.hedge_venue.value,
        plan.direction.value,
    )


def _route_row(
    plan: RoutePlan,
    *,
    rank: int | None,
    observations: Mapping[tuple[Venue, str], MarketObservation],
    config: PaperConfig = PAPER_CONFIG,
) -> dict[str, object]:
    risex_quote = observations.get((Venue.RISEX, plan.risex_market.venue_symbol))
    hedge_quote = observations.get((plan.hedge_venue, plan.hedge_market.venue_symbol))
    risex_source = None if risex_quote is None or risex_quote.funding is None else risex_quote.funding.source
    hedge_source = None if hedge_quote is None or hedge_quote.funding is None else hedge_quote.funding.source

    def marker(observation: MarketObservation | None, source: str | None) -> str:
        if (
            observation is None
            or observation.funding is None
            or observation.funding.quality is FundingQuality.UNKNOWN
        ):
            return "UNKNOWN"
        return (
            "PAPER_ASSUMPTION"
            if (source or "").startswith("PAPER_ASSUMPTION")
            else "OFFICIAL"
        )

    contract_assumption = plan.risex_market.contract_type is ContractType.LINEAR
    eligibility_assumption = bool(
        risex_quote is not None and risex_quote.funding is not None
        and risex_quote.funding.quality is not FundingQuality.UNKNOWN
        and risex_quote.funding.eligibility_known
    )
    estimate_assumption = bool(
        risex_quote is not None and risex_quote.funding is not None
        and risex_quote.funding.quality is FundingQuality.ESTIMATED
    )
    risex_funding = None if plan.target_cycle is None else plan.target_cycle.risex_event.expected_cash_usd
    hedge_funding = None if plan.target_cycle is None else plan.target_cycle.hedge_event.expected_cash_usd
    fee_split = planned_fee_split(plan, config=config)
    return {
        "rank": rank,
        "route_key": f"{plan.canonical_asset}|{plan.hedge_venue.value}|{plan.direction.value}",
        "canonical_asset": plan.canonical_asset,
        "hedge_venue": plan.hedge_venue.value,
        "direction": plan.direction.value,
        "entry_allowed": plan.entry_allowed,
        "risex_contract_assumption_used": contract_assumption,
        "risex_funding_eligibility_assumption_used": eligibility_assumption,
        "risex_funding_estimate_assumption_used": estimate_assumption,
        "paper_assumption_used": contract_assumption or eligibility_assumption or estimate_assumption,
        "blockers": list(plan.no_trade_reasons),
        "route_liquidity_usd": (
            None if plan.route is None else str(plan.route.route_liquidity_usd)
        ),
        "target_cycle_start": (
            None if plan.target_cycle is None else plan.target_cycle.start_at.isoformat()
        ),
        "seconds_to_earliest_funding": (
            None if plan.target_cycle is None else str(
                Decimal(str((plan.target_cycle.start_at - plan.logical_at).total_seconds()))
            )
        ),
        "canonical_quantity": (
            None if plan.canonical_quantity is None else str(plan.canonical_quantity)
        ),
        "expected_funding_usd": (
            None
            if plan.expected_target_cycle_funding_usd is None
            else str(plan.expected_target_cycle_funding_usd)
        ),
        "risex_funding_usd": None if risex_funding is None else str(risex_funding),
        "hedge_funding_usd": None if hedge_funding is None else str(hedge_funding),
        "net_funding_usd": None if plan.expected_target_cycle_funding_usd is None else str(plan.expected_target_cycle_funding_usd),
        "risex_entry_liquidity_role": "TAKER",
        "risex_exact_q_entry_vwap_usd": None if plan.risex_entry_price is None else str(plan.risex_entry_price),
        "risex_exact_q_exit_vwap_usd": None if plan.risex_exit_price is None else str(plan.risex_exit_price),
        "hedge_entry_liquidity_role": "MAKER",
        "hedge_maker_entry_price_usd": None if plan.hedge_entry_price is None else str(plan.hedge_entry_price),
        "planned_hedge_exit_price_usd": None if plan.hedge_exit_price is None else str(plan.hedge_exit_price),
        "entry_execution_pnl_usd": (
            None
            if plan.planned_entry_execution_pnl_usd is None
            else str(plan.planned_entry_execution_pnl_usd)
        ),
        "exit_execution_pnl_usd": (
            None
            if plan.planned_exit_execution_pnl_usd is None
            else str(plan.planned_exit_execution_pnl_usd)
        ),
        "planned_entry_fees_usd": (
            None if fee_split is None else str(fee_split[0])
        ),
        "planned_exit_fees_usd": (
            None if fee_split is None else str(fee_split[1])
        ),
        "planned_fees_usd": (
            None if plan.planned_fees_usd is None else str(plan.planned_fees_usd)
        ),
        "planned_maker_net_pnl_usd": (
            None
            if plan.planned_maker_net_pnl_usd is None
            else str(plan.planned_maker_net_pnl_usd)
        ),
        "executable_unwind_net_pnl_usd": (
            None
            if plan.executable_unwind_net_pnl_usd is None
            else str(plan.executable_unwind_net_pnl_usd)
        ),
        "source_quality": {
            "risex_funding": {"source": risex_source, "marker": marker(risex_quote, risex_source)},
            "hedge_funding": {"source": hedge_source, "marker": marker(hedge_quote, hedge_source)},
            "risex_contract": "PAPER_ASSUMPTION" if plan.risex_market.contract_type is ContractType.LINEAR else "UNKNOWN",
        },
        "assumption_flags": _assumption_flags(),
    }


class PublicPaperRuntime:
    """Coordinates public transports without duplicating domain decisions."""

    def __init__(
        self,
        repository: PaperRepository,
        *,
        adapters: Mapping[Venue, PublicAdapter] | None = None,
        session_factory: Callable[[], aiohttp.ClientSession] = _public_session,
        clock: Clock | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        config: PaperConfig = PAPER_CONFIG,
        notifications: NotificationOutbox | None = None,
    ) -> None:
        self.repository = repository
        self.config = config
        self.clock = clock or SystemClock()
        self._sleep = sleep
        self._session_factory = session_factory
        self._session: aiohttp.ClientSession | None = None
        self.adapters = None if adapters is None else dict(adapters)
        self.coordinator = MarketDataCoordinator()
        self.readiness: dict[Venue, VenueReadiness] = {}
        self.markets: dict[Venue, tuple[Any, ...]] = {}
        self.volumes: dict[tuple[Venue, str], MarketVolume] = {}
        self.observations: dict[tuple[Venue, str], MarketObservation] = {}
        self.last_scan: ScanSnapshot | None = None
        self.broker: PaperEntryBroker | None = None
        self.lifecycle: LifecycleEngine | None = None
        self.next_full_scan_at: datetime | None = None
        self.next_focused_scan_at: datetime | None = None
        self.next_position_monitor_at: datetime | None = None
        self.accepting_entries = True
        self._stream_tasks: dict[tuple[Venue, str, str], asyncio.Task[None]] = {}
        self._refresh_task: asyncio.Task[None] | None = None
        self._recovery_tasks: dict[tuple[Venue, str], asyncio.Task[None]] = {}
        self._recovery_buffers: dict[tuple[Venue, str], list[BookDelta]] = {}
        self._pending_socket_episodes: dict[
            tuple[Venue, str, tuple[str, ...]], dict[str, object]
        ] = {}
        self._pending_watchdog_episodes: dict[
            tuple[Venue, str, tuple[str, ...]], dict[str, object]
        ] = {}
        self._socket_episode_number = 0
        self._stop_event: asyncio.Event | None = None
        self._attempt_number = 0
        self._nado_cumulative_funding: dict[tuple[str, str], object] = {}
        self._trade_stream_ready: set[tuple[Venue, str]] = set()
        self._live_book_ready: set[tuple[Venue, str]] = set()
        self._last_readiness_evidence_at: dict[Venue, datetime] = {}
        self._extended_trade_sequences: dict[str, int] = {}
        self._extended_confirmed_at: dict[tuple[str, str], datetime] = {}
        self._combined_symbols: dict[Venue, tuple[str, ...]] = {}
        self.next_health_check_at: datetime | None = None
        self.focused_cycle: TargetFundingCycle | None = None
        self.component_readiness: dict[Venue, dict[str, VenueReadiness]] = {}
        self._last_catalog_good_at: dict[Venue, datetime] = {}
        self._extended_universe_at: datetime | None = None
        self._extended_metadata_at: dict[str, datetime] = {}
        self._extended_universe_task: asyncio.Task[None] | None = None
        self.next_extended_catalog_at: datetime | None = None
        self.notifications = notifications
        self._notification_run_id: str | None = None
        self._stop_cause: str | None = None
        self._stop_requested_at: datetime | None = None
        self._background_fatal: BaseException | None = None

    async def __aenter__(self) -> PublicPaperRuntime:
        if self.adapters is None:
            self._session = self._session_factory()
            self.adapters = {
                Venue.RISEX: RisexAdapter(self._session),
                Venue.EXTENDED: ExtendedAdapter(self._session),
                Venue.NADO: NadoAdapter(self._session),
            }
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    def _record(
        self,
        event_type: str,
        *,
        at: datetime | None = None,
        venue: Venue | None = None,
        detail: dict[str, object] | None = None,
    ) -> None:
        self.repository.record_runtime_evidence(
            recorded_at=at or self.clock.now(),
            event_type=event_type,
            venue=None if venue is None else venue.value,
            detail=detail,
        )
        now = at or self.clock.now()
        if event_type in {
            "PUBLIC_SOCKET_DISCONNECTED", "PUBLIC_BOOK_RESYNC_REQUIRED",
            "PUBLIC_SNAPSHOT_RECOVERY_FAILED", "PUBLIC_STREAM_CONFIRMATION_STALE",
        }:
            self._notify_outage(
                event_type, degraded=True, venue=venue, detail=detail,
                event_id=f"data-loss:{venue.value if venue else 'PUBLIC'}:"
                f"{event_type}:{now.isoformat()}",
                kind="CRITICAL_DATA_LOSS", occurred_at=now,
                text=f"Critical public data loss: "
                f"{venue.value if venue else 'PUBLIC'} {event_type}",
            )
        elif event_type in {
            "PUBLIC_SOCKET_RECONNECTED", "PUBLIC_SNAPSHOT_RECOVERY_COMPLETED",
            "PUBLIC_STREAM_RESTARTED",
        }:
            episode = None if detail is None else detail.get("episode_id")
            recovery_id = episode or (
                f"{venue.value if venue else 'PUBLIC'}:{event_type}:{now.isoformat()}"
            )
            self._notify_outage(
                event_type, degraded=False, venue=venue, detail=detail,
                event_id=f"data-recovery:{recovery_id}", kind="DATA_RECOVERY",
                occurred_at=now,
                text=f"Public data recovered: "
                f"{venue.value if venue else 'PUBLIC'} {event_type}",
            )

    def _notify_outage(
        self,
        event_type: str,
        *,
        degraded: bool,
        venue: Venue | None,
        detail: dict[str, object] | None,
        event_id: str,
        kind: str,
        occurred_at: datetime,
        text: str,
    ) -> None:
        if self.notifications is None:
            return
        detail = detail or {}
        episode = detail.get("episode_id")
        stream_kind = detail.get("stream_kind", detail.get("stream", "book"))
        market = detail.get(
            "symbol", detail.get("market", detail.get("markets", "PUBLIC"))
        )
        if venue is Venue.EXTENDED and stream_kind == "book":
            # One Extended book socket outage also emits logical book-resync
            # evidence. Both describe the same notification state.
            identity = f"{venue.value}:{market}:book"
        elif episode is not None:
            identity = f"episode:{episode}"
        else:
            component = (
                stream_kind if "SOCKET" in event_type else "book"
            )
            identity = f"{venue.value if venue else 'PUBLIC'}:{market}:{component}"
        self.notifications.outage(
            identity,
            degraded=degraded,
            payload=NotificationPayload(event_id, kind, utc_time(occurred_at), text),
        )

    def _notify_event(
        self, event_id: str, kind: str, at: datetime, text: str, **fields: object
    ) -> None:
        if self.notifications is None:
            return
        self.notifications.event(NotificationPayload(
            event_id, kind, utc_time(at), text,
            ticker=fields.get("ticker"),  # type: ignore[arg-type]
            route=fields.get("route"),  # type: ignore[arg-type]
            final_pnl_usd=fields.get("final_pnl"),  # type: ignore[arg-type]
        ))

    def _notify_opportunity(self, snapshot: ScanSnapshot) -> None:
        if self.notifications is None:
            return
        plan = snapshot.winner
        at = snapshot.logical_at
        if plan is None or plan.target_cycle is None or plan.planned_maker_net_pnl_usd is None:
            self.notifications.opportunity(
                None,
                NotificationPayload(
                    f"opportunity:disappeared:{at.isoformat()}",
                    "OPPORTUNITY_DISAPPEARED", utc_time(at),
                    "Eligible funding opportunity disappeared",
                ),
            )
            return
        risex_side = "LONG" if plan.direction is RouteDirection.LONG_RISEX_SHORT_HEDGE else "SHORT"
        hedge_side = "SHORT" if risex_side == "LONG" else "LONG"
        route = f"RISEx {risex_side} / {plan.hedge_venue.value} {hedge_side}"
        pnl = plan.planned_maker_net_pnl_usd
        cents = str(pnl.quantize(Decimal("0.01")))
        state = (f"{plan.canonical_asset}:{route}", plan.target_cycle.cycle_id, cents)
        scan_utc = utc_time(at).isoformat()
        self.notifications.opportunity(
            state,
            NotificationPayload(
                f"opportunity:{state[0]}:{state[1]}:{state[2]}:{at.isoformat()}",
                "ELIGIBLE_OPPORTUNITY", utc_time(at),
                f"{plan.canonical_asset} | {route} | "
                f"Expected PnL: ${format_telegram_money(pnl)} | "
                f"Scan UTC: {scan_utc}",
                ticker=plan.canonical_asset, route=route,
                planned_maker_net_pnl_usd=pnl,
            ),
        )

    def _notify_lifecycle_transition(
        self, before: LifecycleSnapshot | None, after: LifecycleSnapshot, at: datetime
    ) -> None:
        position = after.position or (None if before is None else before.position)
        before_state = None if before is None else before.lifecycle_state
        if after.position is not None and (before is None or before.position is None):
            self._notify_event(
                f"position:{after.position.position_id}:opened", "POSITION_OPENED", at,
                f"Paper position opened: {after.position.position_id}",
            )
        exiting = {LifecycleState.EXITING_NORMAL, LifecycleState.EXITING_AGGRESSIVE}
        if after.lifecycle_state in exiting and before_state not in exiting and position is not None:
            self._notify_event(
                f"position:{position.position_id}:exit-started", "EXIT_STARTED", at,
                f"Paper exit started: {position.position_id}",
            )
        closed = after.closed_trade
        previous_closed = None if before is None else before.closed_trade
        if closed is not None and previous_closed is None:
            pnl = closed.simulated_closed_net_pnl_usd
            self._notify_event(
                f"position:{closed.position_id}:closed:{closed.closed_at.isoformat()}",
                "POSITION_CLOSED", closed.closed_at,
                f"Paper position closed: {closed.position_id}; "
                f"final PnL USD {format_telegram_money(pnl)}",
                final_pnl=pnl,
            )

    def _set_readiness(
        self, venue: Venue, available: bool, detail: str, at: datetime
    ) -> None:
        current = self.readiness.get(venue)
        value = VenueReadiness(available, detail, at)
        self.readiness[venue] = value
        self.repository.set_venue_readiness(
            venue=venue.value,
            updated_at=at,
            available=available,
            detail=detail,
        )
        last_recorded = self._last_readiness_evidence_at.get(venue)
        changed = current is None or (current.available, current.detail) != (
            available,
            detail,
        )
        if changed and (
            last_recorded is None or at - last_recorded >= timedelta(seconds=10)
        ):
            self._record(
                "VENUE_READINESS",
                at=at,
                venue=venue,
                detail={"available": available, "detail": detail},
            )
            self._last_readiness_evidence_at[venue] = at

    def _set_component_readiness(
        self, venue: Venue, component: str, available: bool, detail: str, at: datetime
    ) -> None:
        components = self.component_readiness.setdefault(venue, {})
        components[component] = VenueReadiness(available, detail, at)
        catalog = components.get("catalog")
        by_symbol: dict[str, list[VenueReadiness]] = {}
        for name, row in components.items():
            _, separator, symbol = name.partition(":")
            if separator:
                by_symbol.setdefault(symbol, []).append(row)
        symbol_ready = not by_symbol or any(
            all(row.available for row in rows) for rows in by_symbol.values()
        )
        available_now = (catalog is None or catalog.available) and symbol_ready
        failed = next((row for row in components.values() if not row.available), None)
        self._set_readiness(
            venue,
            available_now,
            detail if available_now or failed is None else failed.detail,
            at,
        )

    def _symbol_components_available(self, venue: Venue, symbol: str) -> bool:
        components = self.component_readiness.get(venue, {})
        catalog = components.get("catalog")
        if venue is not Venue.EXTENDED and catalog is not None and not catalog.available:
            return False
        return all(
            row.available
            for name, row in components.items()
            if name.endswith(f":{symbol}")
        )

    def _remove_obsolete_components(
        self, venue: Venue, relevant_symbols: set[str], at: datetime
    ) -> None:
        components = self.component_readiness.get(venue)
        if not components:
            return
        for name in tuple(components):
            _, separator, symbol = name.partition(":")
            if separator and symbol not in relevant_symbols:
                components.pop(name)
        catalog = components.get("catalog")
        by_symbol: dict[str, list[VenueReadiness]] = {}
        for name, row in components.items():
            _, separator, symbol = name.partition(":")
            if separator:
                by_symbol.setdefault(symbol, []).append(row)
        symbol_ready = not by_symbol or any(
            all(row.available for row in rows) for rows in by_symbol.values()
        )
        available = (catalog is None or catalog.available) and symbol_ready
        failed = next((row for row in components.values() if not row.available), None)
        self._set_readiness(
            venue,
            available,
            (
                "PUBLIC_COMPONENTS_RECONCILED"
                if available or failed is None else failed.detail
            ),
            at,
        )

    async def _catalog(self, venue: Venue, adapter: PublicAdapter) -> None:
        at = self.clock.now()
        if isinstance(adapter, ExtendedAdapter) and self._stop_event is not None:
            self._start_extended_universe_refresh()
            self._update_extended_catalog_readiness(at)
            return
        try:
            if isinstance(adapter, ExtendedAdapter):
                markets, volumes = await adapter.fetch_catalog()
            else:
                markets, volumes = await asyncio.gather(
                    adapter.fetch_markets(), adapter.fetch_volumes()  # type: ignore[attr-defined]
                )
            completed = self.clock.now()
            if isinstance(adapter, ExtendedAdapter):
                self._install_extended_catalog(markets, volumes, completed, full=True)
            else:
                self.markets[venue] = tuple(markets)
                for volume in volumes:
                    self.volumes[(venue, volume.canonical_market)] = volume
            self._last_catalog_good_at[venue] = completed
            self._set_component_readiness(
                venue, "catalog", True, "PUBLIC_REST_READY", completed
            )
            self._record(
                "PUBLIC_REQUEST_COMPLETED", at=completed, venue=venue,
                detail={
                    "component": "catalog",
                    "elapsed_seconds": str(Decimal(str(
                        (completed - at).total_seconds()
                    ))),
                },
            )
        except (
            aiohttp.ClientError, TimeoutError, OSError, PublicDataUnavailable,
            ValueError, KeyError, TypeError,
        ) as exc:
            self._set_component_readiness(
                venue, "catalog", False,
                f"PUBLIC_REST_UNAVAILABLE:{type(exc).__name__}", at,
            )
            self._record(
                "PUBLIC_REQUEST_FAILED", at=at, venue=venue,
                detail={
                    "component": "catalog",
                    "endpoint_class": "catalog",
                    "exception_class": type(exc).__name__,
                    "elapsed_ms": int(
                        (self.clock.now() - at).total_seconds() * 1000
                    ),
                    "http_status": _http_status(exc),
                    "retry_state": "NEXT_ABSOLUTE_FULL_SLOT",
                    "retry_backoff_seconds": self.config.normal_scan_seconds,
                    "last_good_at": (
                        None if venue not in self._last_catalog_good_at
                        else self._last_catalog_good_at[venue].isoformat()
                    ),
                },
            )

    def _install_extended_catalog(
        self, markets: tuple[Any, ...], volumes: tuple[MarketVolume, ...],
        at: datetime, *, full: bool,
    ) -> None:
        symbols = tuple(market.venue_symbol for market in markets)
        if not symbols or len(set(symbols)) != len(symbols):
            raise ValueError("Extended catalog is empty or duplicated")
        volume_map = {row.canonical_market: row for row in volumes}
        if len(volume_map) != len(volumes) or set(volume_map) != set(symbols):
            raise ValueError("Extended catalog volumes are incomplete")
        if full:
            self.markets[Venue.EXTENDED] = tuple(markets)
            self._extended_universe_at = at
            self._extended_metadata_at = {symbol: at for symbol in symbols}
        else:
            replacements = {market.venue_symbol: market for market in markets}
            current = tuple(self.markets.get(Venue.EXTENDED, ()))
            if not set(replacements) <= {market.venue_symbol for market in current}:
                raise ValueError("required Extended metadata is outside universe")
            self.markets[Venue.EXTENDED] = tuple(
                replacements.get(market.venue_symbol, market) for market in current
            )
            for symbol in symbols:
                self._extended_metadata_at[symbol] = at
        for symbol, volume in volume_map.items():
            self.volumes[Venue.EXTENDED, symbol] = volume

    def _update_extended_catalog_readiness(self, at: datetime) -> None:
        if self._extended_universe_at is None:
            available, detail = False, "CATALOG_UNAVAILABLE"
        elif at - self._extended_universe_at > timedelta(
            seconds=self.config.extended_universe_max_age_seconds
        ):
            available, detail = False, "CATALOG_STALE"
        else:
            available, detail = True, "PUBLIC_EXTENDED_CATALOG_READY"
        self._set_component_readiness(
            Venue.EXTENDED, "catalog", available, detail, at
        )

    async def _refresh_extended_universe(self) -> None:
        assert self.adapters is not None
        adapter = self.adapters.get(Venue.EXTENDED)
        if not isinstance(adapter, ExtendedAdapter):
            return
        try:
            markets, volumes = await adapter.fetch_catalog()
            completed = self.clock.now()
            self._install_extended_catalog(markets, volumes, completed, full=True)
            self._last_catalog_good_at[Venue.EXTENDED] = completed
            self._update_extended_catalog_readiness(completed)
            self._record(
                "PUBLIC_REQUEST_COMPLETED", at=completed, venue=Venue.EXTENDED,
                detail={
                    "component": "extended_universe",
                    "timeout_seconds": self.config.extended_catalog_timeout_seconds,
                },
            )
        except asyncio.CancelledError:
            raise
        except (
            aiohttp.ClientError, TimeoutError, OSError, PublicDataUnavailable,
            ValueError, KeyError, TypeError,
        ) as exc:
            self._update_extended_catalog_readiness(self.clock.now())
            self._record(
                "PUBLIC_REQUEST_FAILED", venue=Venue.EXTENDED,
                detail={
                    "component": "extended_universe", "endpoint_class": "catalog",
                    "exception_class": type(exc).__name__,
                    "timeout_seconds": self.config.extended_catalog_timeout_seconds,
                    "cache_state": (
                        "CACHED_LAST_GOOD" if self._extended_universe_at is not None
                        and self.clock.now() - self._extended_universe_at <= timedelta(
                            seconds=self.config.extended_universe_max_age_seconds
                        ) else "FAIL_CLOSED"
                    ),
                },
            )
        finally:
            self._extended_universe_task = None

    def _start_extended_universe_refresh(self) -> None:
        if self._extended_universe_task is not None and not self._extended_universe_task.done():
            return
        self._extended_universe_task = asyncio.create_task(
            self._refresh_extended_universe()
        )
        self._extended_universe_task.add_done_callback(self._background_task_done)

    async def _refresh_extended_required(self) -> None:
        assert self.adapters is not None
        adapter = self.adapters.get(Venue.EXTENDED)
        if not isinstance(adapter, ExtendedAdapter) or self._extended_universe_at is None:
            return
        symbols = tuple(sorted(self._required_extended_symbols()))
        if not symbols:
            return
        try:
            markets, volumes = await adapter.fetch_required_catalog(symbols)
            self._install_extended_catalog(
                markets, volumes, self.clock.now(), full=False
            )
            self._record(
                "PUBLIC_REQUEST_COMPLETED", venue=Venue.EXTENDED,
                detail={"component": "required_market_metadata", "symbols": list(symbols)},
            )
        except asyncio.CancelledError:
            raise
        except (
            aiohttp.ClientError, TimeoutError, OSError, PublicDataUnavailable,
            ValueError, KeyError, TypeError,
        ) as exc:
            self._record(
                "PUBLIC_REQUEST_FAILED", venue=Venue.EXTENDED,
                detail={
                    "component": "required_market_metadata",
                    "endpoint_class": "market_query", "exception_class": type(exc).__name__,
                    "cache_state": (
                        "CACHED_LAST_GOOD"
                        if all(
                            symbol in self._extended_metadata_at
                            and self.clock.now() - self._extended_metadata_at[symbol]
                            <= timedelta(seconds=self.config.extended_required_metadata_max_age_seconds)
                            for symbol in symbols
                        ) else "FAIL_CLOSED"
                    ),
                },
            )

    def _extended_market_with_cache_blocker(
        self, market: Any, at: datetime
    ) -> Any:
        blockers = list(market.evidence_blockers)
        if self._extended_universe_at is None:
            blockers.append("CATALOG_UNAVAILABLE")
        elif at - self._extended_universe_at > timedelta(
            seconds=self.config.extended_universe_max_age_seconds
        ):
            blockers.append("CATALOG_STALE")
        metadata_at = self._extended_metadata_at.get(market.venue_symbol)
        if metadata_at is None or at - metadata_at > timedelta(
            seconds=self.config.extended_required_metadata_max_age_seconds
        ):
            blockers.append("MARKET_METADATA_STALE")
        return replace(market, evidence_blockers=tuple(dict.fromkeys(blockers)))

    def _candidate_markets(self) -> tuple[Any, ...]:
        by_venue_asset: dict[tuple[Venue, str], Any] = {}
        for venue, rows in self.markets.items():
            for market in rows:
                key = (venue, market.canonical_asset)
                current = by_venue_asset.get(key)
                if current is None or market.venue_symbol < current.venue_symbol:
                    by_venue_asset[key] = market
        risex_assets = {
            asset
            for (venue, asset), market in by_venue_asset.items()
            if venue is Venue.RISEX
            and market.market_type is MarketType.PERPETUAL
            and market.is_active
        }
        hedge_assets = {
            asset
            for (venue, asset), market in by_venue_asset.items()
            if venue in {Venue.EXTENDED, Venue.NADO}
            and market.market_type is MarketType.PERPETUAL
            and market.contract_type is ContractType.LINEAR
            and market.is_active
        }
        assets = risex_assets & hedge_assets
        scores: dict[str, Decimal] = {}
        for asset in assets:
            risex = by_venue_asset[(Venue.RISEX, asset)]
            risex_volume = self.volumes.get((Venue.RISEX, risex.venue_symbol))
            values: list[Decimal] = []
            for venue in (Venue.EXTENDED, Venue.NADO):
                hedge = by_venue_asset.get((venue, asset))
                if hedge is None or risex_volume is None:
                    continue
                hedge_volume = self.volumes.get((venue, hedge.venue_symbol))
                if (
                    risex_volume.quote_volume_usd is not None
                    and hedge_volume is not None
                    and hedge_volume.quote_volume_usd is not None
                ):
                    values.append(
                        min(
                            risex_volume.quote_volume_usd,
                            hedge_volume.quote_volume_usd,
                        )
                    )
            scores[asset] = max(values, default=Decimal("0"))
        selected = [
            asset
            for asset, _ in sorted(scores.items(), key=lambda item: (-item[1], item[0]))[
                : self.config.top_markets
            ]
        ]
        runtime = self.repository.load_runtime()
        position = getattr(runtime, "position", None)
        if position is not None and position.route_key.canonical_asset not in selected:
            selected.append(position.route_key.canonical_asset)
        result = list(
            (
                self._extended_market_with_cache_blocker(
                    by_venue_asset[(venue, asset)], self.clock.now()
                )
                if (
                    venue is Venue.EXTENDED
                    and self.adapters is not None
                    and isinstance(self.adapters.get(Venue.EXTENDED), ExtendedAdapter)
                )
                else by_venue_asset[(venue, asset)]
            )
            for asset in selected
            for venue in (Venue.RISEX, Venue.EXTENDED, Venue.NADO)
            if (venue, asset) in by_venue_asset
        )
        if isinstance(runtime, LifecycleSnapshot):
            for market in (runtime.risex_market, runtime.hedge_market):
                if all(
                    candidate.venue is not market.venue
                    or candidate.venue_symbol != market.venue_symbol
                    for candidate in result
                ):
                    result.append(market)
        return tuple(result)

    async def _market_observation(
        self, market: Any, assumed_at: datetime, *, background: bool = False
    ) -> None:
        assert self.adapters is not None
        adapter = self.adapters[market.venue]
        key = (market.venue, market.venue_symbol)
        volume = self.volumes.get(key)
        existing = self.observations.get(key)
        request_started_at = self.clock.now()
        try:
            live_health = self.coordinator.stream(*key).health(self.clock.now())
            healthy_live = (
                background
                and key in self._trade_stream_ready
                and live_health.data_quality is DataQuality.COMPLETE
            )
            extended_ws_managed = (
                background
                and isinstance(adapter, ExtendedAdapter)
                and self._stop_event is not None
            )
            preserve_stream_book = healthy_live or extended_ws_managed
            if isinstance(adapter, RisexAdapter):
                # The immutable contract becomes eligible only after both public
                # book and recent-trade unit evidence are proven in this scan.
                if healthy_live and existing is not None:
                    book = self.coordinator.stream(*key).book()
                    market = existing.market
                else:
                    book = await adapter.fetch_book(market.venue_symbol)
                    market = await adapter.prime_recent_trade_evidence(market)
                funding = await adapter.fetch_funding_quote(
                    market, assumed_open_at=assumed_at
                )
            else:
                if preserve_stream_book:
                    book = self.coordinator.stream(*key).book()
                    funding = await adapter.fetch_funding_quote(
                        market, assumed_open_at=assumed_at
                    )
                else:
                    book, funding = await asyncio.gather(
                        adapter.fetch_book(market.venue_symbol),
                        adapter.fetch_funding_quote(market, assumed_open_at=assumed_at),
                    )
            logical_at = self.clock.now()
            funding = _quote_for_open_time(funding, logical_at)
            stream = self.coordinator.stream(market.venue, market.venue_symbol)
            if not preserve_stream_book:
                stream.connected(logical_at)
                stream.snapshot(book)
                stream.connection_confirmed(logical_at)
            self.observations[key] = MarketObservation(
                market, volume, stream.book(), funding, stream.health(logical_at)
            )
            ready_components = ["funding"]
            if not preserve_stream_book or live_health.data_quality is DataQuality.COMPLETE:
                ready_components.append("book")
            for component in ready_components:
                self._set_component_readiness(
                    market.venue, f"{component}:{market.venue_symbol}", True,
                    "PUBLIC_MARKET_READY", logical_at,
                )
        except Exception as exc:
            logical_at = self.clock.now()
            if background and existing is not None:
                failed_components = (
                    ("funding",) if preserve_stream_book else ("book", "funding")
                )
                for component in failed_components:
                    self._set_component_readiness(
                        market.venue, f"{component}:{market.venue_symbol}", False,
                        f"PUBLIC_MARKET_UNAVAILABLE:{type(exc).__name__}", logical_at,
                    )
                self._record(
                    "PUBLIC_REQUEST_FAILED", at=logical_at, venue=market.venue,
                    detail={
                        "component": f"book_or_funding:{market.venue_symbol}",
                        "endpoint_class": "market_observation",
                        "exception_class": type(exc).__name__,
                        "elapsed_ms": int(
                            (logical_at - request_started_at).total_seconds() * 1000
                        ),
                        "http_status": _http_status(exc),
                        "retry_state": "NEXT_ABSOLUTE_FULL_SLOT",
                        "retry_backoff_seconds": self.config.normal_scan_seconds,
                        "last_good_age_seconds": str(
                            Decimal(str((logical_at - existing.book.observed_at).total_seconds()))
                        ) if existing.book is not None else None,
                    },
                )
                return
            stream = self.coordinator.stream(market.venue, market.venue_symbol)
            stream.disconnected()
            self.observations[key] = MarketObservation(
                market,
                volume,
                None,
                adapter.unknown_funding_quote(
                    market, observed_at=logical_at, assumed_open_at=logical_at
                ),
                stream.health(logical_at),
            )
            for component in ("book", "funding"):
                self._set_component_readiness(
                    market.venue, f"{component}:{market.venue_symbol}", False,
                    f"PUBLIC_MARKET_UNAVAILABLE:{type(exc).__name__}", logical_at,
                )
            self._record(
                "PUBLIC_REQUEST_FAILED", at=logical_at, venue=market.venue,
                detail={
                    "component": f"book_or_funding:{market.venue_symbol}",
                    "endpoint_class": "market_observation",
                    "exception_class": type(exc).__name__,
                    "elapsed_ms": int(
                        (logical_at - request_started_at).total_seconds() * 1000
                    ),
                    "http_status": _http_status(exc),
                    "retry_state": "NEXT_ABSOLUTE_FULL_SLOT",
                    "retry_backoff_seconds": self.config.normal_scan_seconds,
                },
            )

    async def scan(
        self,
        *,
        refresh: bool = True,
        scan_kind: str = "INITIAL",
        scheduled_at: datetime | None = None,
    ) -> dict[str, object]:
        if self.adapters is None:
            raise RuntimeError("runtime must be entered before scanning")
        started_at = self.clock.now()
        scheduled_at = scheduled_at or started_at
        if refresh:
            await asyncio.gather(
                *(self._catalog(venue, adapter) for venue, adapter in self.adapters.items())
            )
            self.observations.clear()
            assumed_at = self.clock.now()
            candidates = self._candidate_markets()
            await asyncio.gather(
                *(self._market_observation(market, assumed_at) for market in candidates)
            )
        logical_at = self.clock.now()
        normalized: list[MarketObservation] = []
        for observation in self.observations.values():
            market = observation.market
            if (
                market.venue is Venue.EXTENDED
                and self.adapters is not None
                and isinstance(self.adapters.get(Venue.EXTENDED), ExtendedAdapter)
            ):
                market = self._extended_market_with_cache_blocker(
                    market, logical_at
                )
            funding = observation.funding
            if funding is not None:
                funding = _quote_for_open_time(funding, logical_at)
            health = self.coordinator.stream(
                observation.market.venue, observation.market.venue_symbol
            ).health(logical_at)
            if not self._symbol_components_available(
                observation.market.venue, observation.market.venue_symbol
            ) or (
                not refresh
                and (observation.market.venue, observation.market.venue_symbol)
                not in self._trade_stream_ready
            ):
                health = replace(
                    health, stream_connected=False, data_quality=DataQuality.DEGRADED
                )
            normalized.append(replace(
                observation, market=market, funding=funding, health=health
            ))
        snapshot = await scan_once(normalized, logical_at, config=self.config)
        persist_scan = (
            self.last_scan is None or self.last_scan.logical_at != logical_at
        )
        self.last_scan = snapshot
        if persist_scan:
            self.repository.save_decision(
                recorded_at=logical_at,
                scan_snapshot=snapshot,
                funding_quotes=tuple(
                    row.funding for row in normalized if row.funding is not None
                ),
            )
        completed_at = self.clock.now()
        self._record(
            "PUBLIC_SCAN",
            at=completed_at,
            detail={
                "evaluation_count": len(snapshot.evaluations),
                "eligible_count": sum(plan.entry_allowed for plan in snapshot.evaluations),
                "assumption_flags": _assumption_flags(),
                "scheduled_at": scheduled_at.isoformat(),
                "started_at": started_at.isoformat(),
                "completed_at": completed_at.isoformat(),
                "duration_ms": int(
                    (completed_at - started_at).total_seconds() * 1000
                ),
                "missed_deadline_ms": max(
                    0, int((started_at - scheduled_at).total_seconds() * 1000)
                ),
                "scan_kind": scan_kind,
                "observations_source": self._observations_source(refresh),
            },
        )
        rankable = [
            plan for plan in snapshot.evaluations
            if plan.route is not None and plan.target_cycle is not None
            and plan.planned_maker_net_pnl_usd is not None
        ]
        rankable.sort(key=_route_rank_key)
        ranks = {id(plan): index for index, plan in enumerate(rankable, 1)}
        blocked = sorted(
            (plan for plan in snapshot.evaluations if id(plan) not in ranks),
            key=lambda plan: (plan.canonical_asset, plan.hedge_venue.value, plan.direction.value),
        )
        rows = rankable + blocked
        route_rows = tuple(
            _route_row(
                plan, rank=ranks.get(id(plan)), observations=self.observations,
                config=self.config,
            )
            for plan in rows
        )
        if persist_scan:
            self.repository.save_public_route_rows(logical_at=logical_at, rows=route_rows)
            self._notify_opportunity(snapshot)
            if self.notifications is not None and scan_kind == "FULL":
                for payload in full_scan_digest_payloads(
                    scan_at=logical_at, opportunity=snapshot.winner is not None,
                    route_rows=route_rows,
                ):
                    self.notifications.event(payload)
        unavailable = {
            venue.value: state.detail
            for venue, state in self.readiness.items()
            if not state.available
        }

        return {
            "scan_at": logical_at.astimezone(UTC).isoformat(),
            "status": "OPPORTUNITY" if snapshot.winner is not None else "NO_TRADE",
            "reason": (
                None
                if snapshot.winner is not None
                else ("VENUE_SPECIFIC_BLOCKERS" if unavailable else "NO_ELIGIBLE_ROUTE")
            ),
            "eligible_count": sum(plan.entry_allowed for plan in snapshot.evaluations),
            "winner": None if snapshot.winner is None else snapshot.winner.canonical_asset,
            "routes": list(route_rows),
            "venue_readiness": {
                venue.value: {
                    "available": state.available,
                    "detail": state.detail,
                    "updated_at": state.updated_at.isoformat(),
                    "components": {
                        name: {
                            "available": component.available,
                            "detail": component.detail,
                            "updated_at": component.updated_at.isoformat(),
                        }
                        for name, component in self.component_readiness.get(venue, {}).items()
                    },
                    "last_good_catalog_at": (
                        None if venue not in self._last_catalog_good_at
                        else self._last_catalog_good_at[venue].isoformat()
                    ),
                    "last_good_catalog_age_seconds": (
                        None if venue not in self._last_catalog_good_at
                        else str(Decimal(str(
                            (logical_at - self._last_catalog_good_at[venue]).total_seconds()
                        )))
                    ),
                }
                for venue, state in self.readiness.items()
            },
            "assumption_flags": _assumption_flags(),
        }

    def _observations_source(self, refresh: bool) -> str:
        if refresh:
            return "REST_BOOTSTRAP"
        keys = set(self.observations)
        if keys and keys <= self._trade_stream_ready and keys <= self._live_book_ready:
            return "LIVE_STREAM"
        return "MIXED"

    async def _refresh_public_data(self) -> None:
        assert self.adapters is not None
        started = self.clock.now()
        self._record("PUBLIC_REFRESH_STARTED", at=started)
        try:
            await asyncio.gather(
                *(
                    self._catalog(venue, adapter)
                    for venue, adapter in self.adapters.items()
                    if venue is not Venue.EXTENDED
                )
            )
            await self._refresh_extended_required()
            self._update_extended_catalog_readiness(self.clock.now())
            assumed_at = self.clock.now()
            await asyncio.gather(*(
                self._market_observation(market, assumed_at, background=True)
                for market in self._candidate_markets()
            ))
            await self._reconcile_streams()
            completed = self.clock.now()
            self._record(
                "PUBLIC_REFRESH_COMPLETED", at=completed,
                detail={
                    "elapsed_seconds": str(
                        Decimal(str((completed - started).total_seconds()))
                    )
                },
            )
        except asyncio.CancelledError:
            raise

    def _start_public_refresh(self) -> None:
        if self._refresh_task is not None and not self._refresh_task.done():
            self._record("PUBLIC_REFRESH_COALESCED")
            return
        self._refresh_task = asyncio.create_task(self._refresh_public_data())
        self._refresh_task.add_done_callback(self._background_task_done)

    def _background_task_done(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        exception = task.exception()
        if exception is None or self._stop_cause == "RUNTIME_FATAL":
            return
        self._background_fatal = exception
        self._request_stop("RUNTIME_FATAL")
        self._record(
            "RUNTIME_FATAL", at=self._stop_requested_at,
            detail={"exception_class": type(exception).__name__},
        )

    def _observation(self, venue: Venue, symbol: str, at: datetime) -> MarketObservation:
        row = self.observations[(venue, symbol)]
        stream = self.coordinator.stream(venue, symbol)
        health = stream.health(at)
        if (
            not self._symbol_components_available(venue, symbol)
            or (venue, symbol) not in self._trade_stream_ready
        ):
            health = replace(
                health, stream_connected=False, data_quality=DataQuality.DEGRADED
            )
        return replace(row, book=stream.book(), health=health)

    def _route_observations(
        self, plan: RoutePlan, at: datetime
    ) -> tuple[MarketObservation, MarketObservation]:
        return self._market_pair_observations(
            plan.risex_market, plan.hedge_market, at
        )

    def _market_pair_observations(
        self, risex_market: Any, hedge_market: Any, at: datetime
    ) -> tuple[MarketObservation, MarketObservation]:
        return (
            self._observation(Venue.RISEX, risex_market.venue_symbol, at),
            self._observation(hedge_market.venue, hedge_market.venue_symbol, at),
        )

    async def _recompute_funding(
        self, plan: RoutePlan, opened_at: datetime
    ) -> tuple[FundingCashQuote, FundingCashQuote]:
        assert self.adapters is not None
        values = await asyncio.gather(
            self.adapters[Venue.RISEX].fetch_funding_quote(
                plan.risex_market, assumed_open_at=opened_at
            ),
            self.adapters[plan.hedge_venue].fetch_funding_quote(
                plan.hedge_market, assumed_open_at=opened_at
            ),
            return_exceptions=True,
        )
        quotes: list[FundingCashQuote] = []
        for market, value in zip((plan.risex_market, plan.hedge_market), values):
            if isinstance(value, BaseException):
                value = self.adapters[market.venue].unknown_funding_quote(
                    market, observed_at=opened_at, assumed_open_at=opened_at
                )
            quotes.append(_quote_for_open_time(value, opened_at))
        return quotes[0], quotes[1]

    async def _restore(self, at: datetime) -> None:
        runtime = self.repository.load_runtime()
        if isinstance(runtime, PaperEntryState) and runtime.lifecycle_state is LifecycleState.ENTRY_MAKER_OPEN:
            broker = PaperEntryBroker.from_state(runtime, config=self.config)
            restarted = await broker.cancel_for_process_restart(restarted_at=at)
            self.repository.save_decision(recorded_at=at, entry_state=restarted)
            self._record("ENTRY_CANCELLED_ON_RESTART", at=at)
            return
        if not isinstance(runtime, LifecycleSnapshot) or runtime.lifecycle_state is LifecycleState.FLAT:
            return
        engine = LifecycleEngine.from_snapshot(runtime, config=self.config)
        position = runtime.position
        assert position is not None
        risex, hedge = self._market_pair_observations(
            runtime.risex_market, runtime.hedge_market, at
        )
        assert self.adapters is not None
        last_known_at = self.repository.runtime_updated_at() or at
        history = await asyncio.gather(
            self.adapters[Venue.RISEX].fetch_applied_settlements(
                runtime.risex_market, since=last_known_at, until=at
            ),
            self.adapters[runtime.hedge_market.venue].fetch_applied_settlements(
                runtime.hedge_market, since=last_known_at, until=at
            ),
            return_exceptions=True,
        )
        required_keys = {row.key for row in runtime.settlements}
        settlement_updates = tuple(
            update
            for result in history
            if not isinstance(result, BaseException)
            for update in result
            if update.key in required_keys
        )
        await engine.restart(
            last_known_at=last_known_at,
            recovered_at=at,
            risex_observation=risex,
            hedge_observation=hedge,
            settlement_updates=settlement_updates,
        )
        self.lifecycle = engine
        self.repository.save_decision(recorded_at=at, lifecycle_snapshot=engine.snapshot)
        self._record("OPEN_POSITION_RESTORED", at=at)

    async def tick(self, at: datetime | None = None) -> None:
        now = at or self.clock.now()
        if (
            self.next_extended_catalog_at is not None
            and now >= self.next_extended_catalog_at
        ):
            scheduled_catalog = self.next_extended_catalog_at
            self._start_extended_universe_refresh()
            self.next_extended_catalog_at = _next_absolute_slot(
                scheduled_catalog, now,
                self.config.extended_universe_refresh_seconds,
            )
        if (
            self.adapters is not None
            and isinstance(self.adapters.get(Venue.EXTENDED), ExtendedAdapter)
        ):
            self._update_extended_catalog_readiness(now)
        maker_was_active = bool(
            self.broker is not None
            and self.broker.state.lifecycle_state is LifecycleState.ENTRY_MAKER_OPEN
        )
        if self.next_health_check_at is None or now >= self.next_health_check_at:
            await self._check_extended_health(now)
            for (venue, symbol), observation in tuple(self.observations.items()):
                if (
                    venue is Venue.EXTENDED
                    and self.adapters is not None
                    and isinstance(self.adapters.get(Venue.EXTENDED), ExtendedAdapter)
                ):
                    continue
                if (venue, symbol) not in self._trade_stream_ready:
                    continue
                health = self.coordinator.stream(venue, symbol).health(now)
                confirmation = health.last_connection_confirmation_at
                if confirmation is not None and now - confirmation > timedelta(seconds=25):
                    await self.mark_disconnected(venue, symbol, at=now, stream_kind="health")
            scheduled = self.next_health_check_at or now
            self.next_health_check_at = _next_absolute_slot(scheduled, now, 10)
        if maker_was_active and self.broker is None:
            return
        if self.last_scan is None:
            await self.scan(refresh=True, scan_kind="INITIAL", scheduled_at=now)
            now = self.last_scan.logical_at
            self.next_full_scan_at = now + timedelta(seconds=self.config.normal_scan_seconds)
        elif self.next_full_scan_at is None:
            self.next_full_scan_at = self.last_scan.logical_at + timedelta(
                seconds=self.config.normal_scan_seconds
            )
        elif now >= self.next_full_scan_at:
            scheduled = self.next_full_scan_at
            self._record(
                "PUBLIC_SCAN_DEADLINE", at=now,
                detail={
                    "kind": "full", "scheduled_at": scheduled.isoformat(),
                    "lateness_seconds": str(Decimal(str((now - scheduled).total_seconds()))),
                },
            )
            self._start_public_refresh()
            await self.scan(
                refresh=False, scan_kind="FULL", scheduled_at=scheduled
            )
            now = self.last_scan.logical_at
            self.next_full_scan_at = _next_absolute_slot(
                scheduled, now, self.config.normal_scan_seconds
            )
        if self.lifecycle is not None:
            if (
                self.next_position_monitor_at is None
                or now >= self.next_position_monitor_at
            ):
                risex_adapter = None if self.adapters is None else self.adapters.get(Venue.RISEX)
                position = self.lifecycle.snapshot.position
                if isinstance(risex_adapter, RisexAdapter) and position is not None:
                    unresolved = [
                        row.settlement_at for row in self.lifecycle.snapshot.settlements
                        if row.venue is Venue.RISEX
                        and row.status is not SettlementStatus.APPLIED_RATE
                        and row.settlement_at >= position.position_opened_at
                    ]
                    try:
                        if not unresolved:
                            quotes = ()
                        else:
                            since = max(position.position_opened_at, min(unresolved))
                            quotes = await risex_adapter.fetch_applied_funding_quotes(
                                self.lifecycle.snapshot.risex_market, since=since,
                                until=now, assumed_open_at=position.position_opened_at,
                            )
                        for quote in quotes:
                            await self._apply_funding_quote(quote)
                    except (aiohttp.ClientError, TimeoutError, ValueError, KeyError) as exc:
                        self._record(
                            "PUBLIC_FUNDING_HISTORY_UNAVAILABLE", at=now,
                            venue=Venue.RISEX, detail={"exception_class": type(exc).__name__},
                        )
                risex, hedge = self._market_pair_observations(
                    self.lifecycle.snapshot.risex_market,
                    self.lifecycle.snapshot.hedge_market,
                    now,
                )
                before = self.lifecycle.snapshot
                await self.lifecycle.evaluate(
                    evaluated_at=now,
                    risex_observation=risex,
                    hedge_observation=hedge,
                )
                self.repository.save_decision(
                    recorded_at=now, lifecycle_snapshot=self.lifecycle.snapshot
                )
                self._notify_lifecycle_transition(
                    before, self.lifecycle.snapshot, now
                )
                if self.lifecycle.snapshot.lifecycle_state is LifecycleState.FLAT:
                    self.lifecycle = None
                scheduled = self.next_position_monitor_at or now
                self.next_position_monitor_at = _next_absolute_slot(
                    scheduled, now, self.config.open_position_monitor_seconds
                )
            return
        if self.broker is not None and self.broker.state.lifecycle_state is LifecycleState.ENTRY_MAKER_OPEN:
            order = self.broker.state.order
            assert order is not None
            if now >= order.cutoff_at or self.next_focused_scan_at is None or now >= self.next_focused_scan_at:
                scheduled = self.next_focused_scan_at or now
                await self.scan(
                    refresh=False, scan_kind="FOCUSED", scheduled_at=scheduled
                )
                now = self.last_scan.logical_at
                refreshed = next(
                    (
                        plan
                        for plan in self.last_scan.evaluations
                        if plan.canonical_asset == order.route_key.canonical_asset
                        and plan.hedge_venue is order.route_key.hedge_venue
                        and plan.direction is order.route_key.direction
                    ),
                    None,
                )
                risex, _ = self._route_observations(order.route_plan, now)
                await self.broker.refresh(refreshed, risex, evaluated_at=now)
                self.repository.save_decision(
                    recorded_at=now, entry_state=self.broker.state
                )
                if self.broker.state.lifecycle_state is LifecycleState.FLAT:
                    self.broker = None
                self.next_focused_scan_at = _next_absolute_slot(
                    scheduled, now, self.config.focused_scan_seconds
                )
            return
        if not self.accepting_entries or self.last_scan is None:
            return
        expired_cycle = self.focused_cycle
        cycle = self._refresh_focused_cycle(now)
        if (
            cycle is None
            and expired_cycle is not None
            and now >= activation_schedule(expired_cycle).cutoff_at
        ):
            # The cutoff wake performs one fresh discovery scan. It does not
            # extend the expired cycle's focused cadence.
            await self.scan(
                refresh=False, scan_kind="RECOVERY",
                scheduled_at=activation_schedule(expired_cycle).cutoff_at,
            )
            now = self.last_scan.logical_at
            cycle = self._refresh_focused_cycle(now)
        if cycle is None:
            return
        schedule = activation_schedule(cycle)
        focused_start = cycle.start_at - timedelta(
            seconds=self.config.focused_window_seconds
        )
        if now >= focused_start and (
            self.next_focused_scan_at is None or now >= self.next_focused_scan_at
        ):
            scheduled = self.next_focused_scan_at or focused_start
            self._record(
                "PUBLIC_SCAN_DEADLINE", at=now,
                detail={
                    "kind": "focused", "scheduled_at": scheduled.isoformat(),
                    "lateness_seconds": str(Decimal(str(max(
                        0, (now - scheduled).total_seconds()
                    )))),
                },
            )
            await self.scan(
                refresh=False, scan_kind="FOCUSED", scheduled_at=scheduled
            )
            now = self.last_scan.logical_at
            self.next_focused_scan_at = _next_absolute_slot(
                scheduled, now, self.config.focused_scan_seconds
            )
            cycle = self._refresh_focused_cycle(now)
            if cycle is None:
                return
            schedule = activation_schedule(cycle)
        winner = self.last_scan.winner
        if (
            winner is not None
            and winner.target_cycle is not None
            and winner.target_cycle.start_at == cycle.start_at
            and schedule.should_activate(now)
        ):
            self._attempt_number += 1
            broker = PaperEntryBroker(config=self.config)
            await broker.activate(
                self.last_scan,
                attempt_id=f"public-{int(now.timestamp())}-{self._attempt_number}",
                activated_at=now,
            )
            self.broker = broker
            self.repository.save_decision(recorded_at=now, entry_state=broker.state)
            self._record("PAPER_ENTRY_ACTIVATED", at=now)
            order = broker.state.order
            assert order is not None
            self._notify_event(
                f"entry:{order.attempt_id}:activated", "ENTRY_ACTIVATED", now,
                f"Paper entry activated: {order.attempt_id}",
            )

    def _fresh_focus_candidates(
        self, snapshot: ScanSnapshot, now: datetime
    ) -> tuple[RoutePlan, ...]:
        """Usable scanner-universe routes whose cycle has not reached cutoff."""
        return tuple(
            plan
            for plan in snapshot.evaluations
            if plan.universe_eligible
            and plan.target_cycle is not None
            and activation_schedule(plan.target_cycle).cutoff_at > now
        )

    def _refresh_focused_cycle(self, now: datetime) -> TargetFundingCycle | None:
        preserved = self.focused_cycle
        if preserved is not None:
            cutoff = activation_schedule(preserved).cutoff_at
            if now >= cutoff:
                preserved = None
                self.focused_cycle = None
                self.next_focused_scan_at = None
        if self.last_scan is None:
            return preserved
        candidates = self._fresh_focus_candidates(self.last_scan, now)
        if not candidates:
            return preserved
        selected = min(
            candidates,
            key=lambda plan: (
                plan.target_cycle.start_at,  # type: ignore[union-attr]
                plan.target_cycle.end_at,  # type: ignore[union-attr]
                plan.target_cycle.cycle_id,  # type: ignore[union-attr]
            ),
        ).target_cycle
        assert selected is not None
        if preserved is None or selected.start_at != preserved.start_at:
            self.next_focused_scan_at = None
        self.focused_cycle = selected
        return selected

    async def deliver_trade(
        self,
        trade: TradeEvidence,
        *,
        observed_version_id: str | None = None,
        processed_at: datetime | None = None,
    ) -> None:
        at = processed_at or self.clock.now()
        if self.broker is not None and self.broker.state.lifecycle_state is LifecycleState.ENTRY_MAKER_OPEN:
            active_order = self.broker.state.order
            assert active_order is not None
            if (trade.venue, trade.canonical_market) != (
                active_order.venue,
                active_order.canonical_market,
            ):
                return
        elif self.lifecycle is not None and self.lifecycle.snapshot.exit_order is not None:
            active_order = self.lifecycle.snapshot.exit_order
            if (trade.venue, trade.canonical_market) != (
                active_order.venue,
                active_order.canonical_market,
            ):
                return
        else:
            return
        if self.broker is not None and self.broker.state.lifecycle_state is LifecycleState.ENTRY_MAKER_OPEN:
            plan = active_order.route_plan
            risex_observation = self.observations.get(
                (Venue.RISEX, plan.risex_market.venue_symbol)
            )
            quote = None if risex_observation is None else risex_observation.funding
            contract_used = plan.risex_market.contract_type is ContractType.LINEAR
            eligibility_used = bool(
                quote is not None and quote.quality is not FundingQuality.UNKNOWN
                and quote.eligibility_known
            )
            estimate_used = bool(
                quote is not None and quote.quality is FundingQuality.ESTIMATED
            )
            trade = replace(
                trade,
                risex_contract_assumption_used=contract_used,
                risex_funding_eligibility_assumption_used=eligibility_used,
                risex_funding_estimate_assumption_used=estimate_used,
                paper_assumption_used=contract_used or eligibility_used or estimate_used,
            )
        stream = self.coordinator.stream(trade.venue, trade.canonical_market)
        stream.connection_confirmed(at)
        if (
            (trade.venue, trade.canonical_market) not in self._trade_stream_ready
            or not stream.health(at).stream_connected
        ):
            self._record("TRADE_IGNORED_STREAM_UNHEALTHY", at=at, venue=trade.venue)
            return
        if self.broker is not None and self.broker.state.lifecycle_state is LifecycleState.ENTRY_MAKER_OPEN:
            order = self.broker.state.order
            assert order is not None
            version = observed_version_id or order.active_version.version_id
            risex, hedge = self._route_observations(order.route_plan, at)
            result = await self.broker.process_trade(
                trade,
                observed_version_id=version,
                processed_at=at,
                risex_observation=risex,
                hedge_observation=hedge,
                recompute_funding=self._recompute_funding,
            )
            self.repository.save_decision(
                recorded_at=at,
                trade_events=(trade,),
                entry_state=result.state,
            )
            if result.state.position is not None:
                self.lifecycle = LifecycleEngine(result.state, config=self.config)
                self.repository.save_decision(
                    recorded_at=at, lifecycle_snapshot=self.lifecycle.snapshot
                )
                self._notify_lifecycle_transition(None, self.lifecycle.snapshot, at)
                self.broker = None
                self.next_position_monitor_at = at + timedelta(
                    seconds=self.config.open_position_monitor_seconds
                )
            return
        if self.lifecycle is not None and self.lifecycle.snapshot.exit_order is not None:
            order = self.lifecycle.snapshot.exit_order
            active = order.active_version
            if active is None:
                return
            version = observed_version_id or active.version_id
            risex, hedge = self._market_pair_observations(
                self.lifecycle.snapshot.risex_market,
                self.lifecycle.snapshot.hedge_market,
                at,
            )
            before = self.lifecycle.snapshot
            await self.lifecycle.process_exit_trade(
                trade,
                observed_version_id=version,
                processed_at=at,
                risex_observation=risex,
                hedge_observation=hedge,
            )
            self.repository.save_decision(
                recorded_at=at,
                trade_events=(trade,),
                lifecycle_snapshot=self.lifecycle.snapshot,
            )
            self._notify_lifecycle_transition(before, self.lifecycle.snapshot, at)
            if self.lifecycle.snapshot.lifecycle_state is LifecycleState.FLAT:
                self.lifecycle = None

    def mark_trade_stream_connected(
        self, venue: Venue, symbol: str, *, at: datetime | None = None
    ) -> None:
        now = at or self.clock.now()
        if (
            venue is Venue.EXTENDED
            and self.adapters is not None
            and isinstance(self.adapters.get(Venue.EXTENDED), ExtendedAdapter)
        ):
            self._confirm_extended_stream(symbol, "trade", now, data_ready=True)
            return
        self._trade_stream_ready.add((venue, symbol))
        stream = self.coordinator.stream(venue, symbol)
        stream.connected(now)
        stream.connection_confirmed(now)
        self._set_component_readiness(
            venue, f"trade:{symbol}", True, "PUBLIC_TRADE_STREAM_READY", now
        )
        self._set_component_readiness(
            venue, f"connection_trade:{symbol}", True,
            "PUBLIC_STREAM_CONNECTED", now
        )

    async def deliver_settlement(self, settlement: FundingSettlement) -> None:
        self.repository.upsert_settlement(settlement)
        if self.lifecycle is not None and settlement.key in {
            row.key for row in self.lifecycle.snapshot.settlements
        }:
            before = next(
                row for row in self.lifecycle.snapshot.settlements
                if row.key == settlement.key
            )
            await self.lifecycle.reconcile_settlement(settlement)
            self.repository.save_decision(
                recorded_at=self.clock.now(), lifecycle_snapshot=self.lifecycle.snapshot
            )
            after = next(
                row for row in self.lifecycle.snapshot.settlements
                if row.key == settlement.key
            )
            if (before.status, before.cash_usd) != (after.status, after.cash_usd):
                at = self.clock.now()
                kind = (
                    "FUNDING_RECEIVED"
                    if before.status in {
                        SettlementStatus.PENDING, SettlementStatus.UNRESOLVED
                    }
                    else "FUNDING_RECONCILED"
                )
                self._notify_event(
                    f"funding:{after.venue.value}:{after.canonical_market}:"
                    f"{after.settlement_at.isoformat()}:{after.status.value}:{after.cash_usd}",
                    kind, at,
                    f"Funding {'received' if kind == 'FUNDING_RECEIVED' else 'reconciled'}: "
                    f"{after.venue.value} {after.canonical_market} "
                    f"{after.status.value} USD {format_telegram_money(after.cash_usd)}",
                )

    def _book_mid(self, venue: Venue, symbol: str) -> Decimal | None:
        book = self.coordinator.stream(venue, symbol).book()
        if book is None or not book.bids or not book.asks:
            return None
        bid = max(level.canonical_price for level in book.bids)
        ask = min(level.canonical_price for level in book.asks)
        return (bid + ask) / Decimal("2")

    async def _apply_extended_funding_record(
        self, settlement: FundingSettlement
    ) -> None:
        if settlement.venue is not Venue.EXTENDED or self.lifecycle is None:
            return
        required = {
            row.key: row for row in self.lifecycle.snapshot.settlements
        }
        current = required.get(settlement.key)
        if current is None or current.status in {
            SettlementStatus.UNRESOLVED,
            SettlementStatus.APPLIED_RATE,
            SettlementStatus.SKIPPED_POSITION_NOT_OPEN,
            SettlementStatus.SKIPPED_POSITION_CLOSED,
        }:
            return
        await self.deliver_settlement(settlement)

    async def _apply_funding_quote(self, quote: FundingCashQuote) -> None:
        key = (quote.venue, quote.canonical_market)
        self._set_component_readiness(
            quote.venue, f"funding:{quote.canonical_market}", True,
            "PUBLIC_FUNDING_STREAM_READY", self.clock.now(),
        )
        observation = self.observations.get(key)
        if observation is not None:
            self.observations[key] = replace(observation, funding=quote)
        if quote.quality is not FundingQuality.APPLIED_RATE or self.lifecycle is None:
            return
        position = self.lifecycle.snapshot.position
        if position is None:
            return
        risex_long = position.direction is RouteDirection.LONG_RISEX_SHORT_HEDGE
        venue_long = risex_long if quote.venue is Venue.RISEX else not risex_long
        cash_per_base = (
            quote.long_cash_per_canonical_base_usd
            if venue_long
            else quote.short_cash_per_canonical_base_usd
        )
        if cash_per_base is None:
            return
        settlement = FundingSettlement(
            quote.venue,
            quote.canonical_market,
            quote.settlement_at,
            SettlementStatus.APPLIED_RATE,
            position.canonical_quantity * cash_per_base,
        )
        if settlement.key in {row.key for row in self.lifecycle.snapshot.settlements}:
            await self.deliver_settlement(settlement)

    async def mark_disconnected(
        self, venue: Venue, symbol: str, *, at: datetime | None = None,
        stream_kind: str = "public", exception: BaseException | None = None,
    ) -> None:
        now = at or self.clock.now()
        if venue is Venue.EXTENDED and stream_kind in {"book", "trade", "funding"}:
            self._extended_confirmed_at.pop((symbol, stream_kind), None)
        invalidates_book = stream_kind in {"book", "combined", "health", "public"}
        invalidates_trade = stream_kind in {"trade", "combined", "health", "public"}
        if invalidates_trade:
            self._trade_stream_ready.discard((venue, symbol))
        if invalidates_book:
            self._live_book_ready.discard((venue, symbol))
            self.coordinator.stream(venue, symbol).disconnected()
        exception_name = "StreamGap" if exception is None else type(exception).__name__
        exception_detail = "" if exception is None else f":{str(exception)[:120]}"
        if stream_kind in {"combined", "public", "health"}:
            affected = ("book", "trade", "funding", "connection_combined")
        else:
            affected = (stream_kind, f"connection_{stream_kind}")
        for component in affected:
            self._set_component_readiness(
                venue, f"{component}:{symbol}", False,
                f"PUBLIC_STREAM_DISCONNECTED:{stream_kind}:{exception_name}{exception_detail}", now,
            )
        if stream_kind == "book":
            self._record(
                "PUBLIC_BOOK_RESYNC_REQUIRED",
                at=now, venue=venue,
                detail={"symbol": symbol, "stream": stream_kind},
            )
        if self.broker is not None and self.broker.state.lifecycle_state is LifecycleState.ENTRY_MAKER_OPEN:
            order = self.broker.state.order
            assert order is not None
            if (venue, symbol) in {
                (Venue.RISEX, order.route_plan.risex_market.venue_symbol),
                (order.route_plan.hedge_venue, order.route_plan.hedge_market.venue_symbol),
            }:
                risex, hedge = self._route_observations(order.route_plan, now)
                local = await scan_once((risex, hedge), now, config=self.config)
                refreshed = local.evaluations[0] if local.evaluations else None
                await self.broker.refresh(refreshed, risex, evaluated_at=now)
                self.repository.save_decision(recorded_at=now, entry_state=self.broker.state)
                self.last_scan = local
                if self.broker.state.lifecycle_state is LifecycleState.FLAT:
                    self.broker = None
        if self.lifecycle is not None:
            position = self.lifecycle.snapshot.position
            if position is not None and (venue, symbol) in {
                (Venue.RISEX, self.lifecycle.snapshot.risex_market.venue_symbol),
                (
                    self.lifecycle.snapshot.hedge_market.venue,
                    self.lifecycle.snapshot.hedge_market.venue_symbol,
                ),
            }:
                await self.lifecycle.start_gap(started_at=now)
                self.repository.save_decision(
                    recorded_at=now, lifecycle_snapshot=self.lifecycle.snapshot
                )

    def _confirm_extended_stream(
        self, symbol: str, kind: str, at: datetime, *, data_ready: bool
    ) -> None:
        self._extended_confirmed_at[symbol, kind] = at
        if kind == "book":
            self.coordinator.stream(Venue.EXTENDED, symbol).connected(at)
        self._set_component_readiness(
            Venue.EXTENDED, f"connection_{kind}:{symbol}", True,
            "PUBLIC_STREAM_CONFIRMED", at,
        )
        if not data_ready:
            return
        if kind == "trade":
            self._trade_stream_ready.add((Venue.EXTENDED, symbol))
        elif kind == "book":
            self._live_book_ready.add((Venue.EXTENDED, symbol))
        self._set_component_readiness(
            Venue.EXTENDED, f"{kind}:{symbol}", True,
            f"PUBLIC_{kind.upper()}_STREAM_READY", at,
        )

    async def _check_extended_health(self, at: datetime) -> None:
        stale = [
            (symbol, kind)
            for (symbol, kind), confirmed_at in self._extended_confirmed_at.items()
            if (Venue.EXTENDED, symbol, kind) in self._stream_tasks
            if at - confirmed_at > timedelta(seconds=25)
        ]
        for symbol, kind in sorted(stale):
            identity = (Venue.EXTENDED, kind, (symbol,))
            self._watchdog_stale(identity, at=at)
            await self.mark_disconnected(
                Venue.EXTENDED, symbol, at=at, stream_kind=kind,
                exception=TimeoutError("public socket confirmation stale"),
            )
            await self._restart_extended_stream(symbol, kind)

    def _watchdog_stale(
        self, identity: tuple[Venue, str, tuple[str, ...]], *, at: datetime
    ) -> None:
        if identity in self._pending_watchdog_episodes:
            return
        venue, stream_kind, markets = identity
        detail: dict[str, object] = {
            "episode_id": f"watchdog:{venue.value}:{stream_kind}:{markets[0]}:{at.isoformat()}",
            "stream_kind": stream_kind, "market": markets[0],
            "stale_at": at.isoformat(),
        }
        self._pending_watchdog_episodes[identity] = detail
        self._record(
            "PUBLIC_STREAM_CONFIRMATION_STALE", at=at, venue=venue, detail=detail
        )

    def _watchdog_restarted(
        self, identity: tuple[Venue, str, tuple[str, ...]], *, at: datetime
    ) -> None:
        detail = self._pending_watchdog_episodes.pop(identity, None)
        if detail is None:
            return
        self._record(
            "PUBLIC_STREAM_RESTARTED", at=at, venue=identity[0],
            detail={**detail, "restarted_at": at.isoformat()},
        )

    def _socket_disconnected(
        self,
        identity: tuple[Venue, str, tuple[str, ...]],
        *,
        at: datetime,
    ) -> None:
        if identity in self._pending_socket_episodes:
            return
        venue, stream_kind, markets = identity
        self._socket_episode_number += 1
        detail: dict[str, object] = {
            "episode_id": (
                f"{venue.value}:{stream_kind}:{at.astimezone(UTC).isoformat()}:"
                f"{self._socket_episode_number}"
            ),
            "stream_kind": stream_kind,
            "disconnected_at": at.isoformat(),
        }
        if stream_kind == "combined":
            detail["markets"] = list(markets)
        else:
            detail["market"] = markets[0]
        self._pending_socket_episodes[identity] = detail
        self._record(
            "PUBLIC_SOCKET_DISCONNECTED",
            at=at,
            venue=venue,
            detail=detail,
        )

    def _socket_reconnected(
        self,
        identity: tuple[Venue, str, tuple[str, ...]],
        *,
        at: datetime,
    ) -> None:
        detail = self._pending_socket_episodes.pop(identity, None)
        if detail is not None:
            reconnect_detail = {
                **detail,
                "reconnected_at": at.isoformat(),
            }
            self._record(
                "PUBLIC_SOCKET_RECONNECTED",
                at=at,
                venue=identity[0],
                detail=reconnect_detail,
            )

    async def recover_snapshot(self, book: OrderBook, *, at: datetime | None = None) -> None:
        now = at or self.clock.now()
        stream = self.coordinator.stream(book.venue, book.canonical_market)
        stream.connected(now)
        stream.snapshot(book)
        stream.connection_confirmed(now)
        self._live_book_ready.add((book.venue, book.canonical_market))
        self._set_component_readiness(
            book.venue, f"book:{book.canonical_market}", True,
            "PUBLIC_STREAM_RECOVERED", now,
        )
        self._set_component_readiness(
            book.venue, f"connection_book:{book.canonical_market}", True,
            "PUBLIC_STREAM_RECOVERED", now,
        )
        row = self.observations.get((book.venue, book.canonical_market))
        if row is not None:
            self.observations[(book.venue, book.canonical_market)] = replace(
                row, book=stream.book(), health=stream.health(now)
            )
        if self.lifecycle is not None and self.lifecycle.snapshot.gap_open:
            risex, hedge = self._market_pair_observations(
                self.lifecycle.snapshot.risex_market,
                self.lifecycle.snapshot.hedge_market,
                now,
            )
            if (
                risex.health is not None
                and hedge.health is not None
                and risex.health.data_quality is DataQuality.COMPLETE
                and hedge.health.data_quality is DataQuality.COMPLETE
            ):
                await self.lifecycle.recover(
                    recovered_at=now,
                    risex_observation=risex,
                    hedge_observation=hedge,
                )
                self.repository.save_decision(
                    recorded_at=now, lifecycle_snapshot=self.lifecycle.snapshot
                )
    async def apply_book_event(self, event: OrderBook | BookDelta) -> bool:
        now = self.clock.now()
        self.coordinator.stream(event.venue, event.canonical_market).connection_confirmed(now)
        key = (event.venue, event.canonical_market)
        if isinstance(event, OrderBook):
            if key in self._recovery_buffers:
                if event.venue is Venue.RISEX:
                    # The official resubscribe snapshot is an ordered WS
                    # boundary. Messages buffered before it belong to the old
                    # subscription and must not be replayed across that boundary.
                    buffered = len(self._recovery_buffers.pop(key))
                    await self.recover_snapshot(event, at=now)
                    self._record(
                        "PUBLIC_SNAPSHOT_RECOVERY_COMPLETED", at=now,
                        venue=event.venue,
                        detail={
                            "symbol": event.canonical_market,
                            "buffered": buffered,
                            "replayed": 0,
                            "source": "WS_RESUBSCRIBE_SNAPSHOT",
                        },
                    )
                    return True
                if event.venue is Venue.EXTENDED and event.sequence is None:
                    self._record(
                        "PUBLIC_REST_SNAPSHOT_IGNORED_FOR_WS_RESYNC",
                        at=now, venue=event.venue,
                        detail={"symbol": event.canonical_market},
                    )
                    return True
                try:
                    recovered, buffered, replayed = self._install_recovery_snapshot(event)
                except ValueError as exc:
                    self._recovery_buffers.pop(key, None)
                    await self.mark_disconnected(
                        event.venue, event.canonical_market, at=now,
                        stream_kind="book", exception=exc,
                    )
                    self._start_snapshot_recovery(event.venue, event.canonical_market)
                    return False
                await self.recover_snapshot(recovered, at=now)
                self._live_book_ready.add(key)
                self._record(
                    "PUBLIC_SNAPSHOT_RECOVERY_COMPLETED", at=now, venue=event.venue,
                    detail={
                        "symbol": event.canonical_market,
                        "buffered": buffered,
                        "replayed": replayed,
                        "source": "WS_SNAPSHOT",
                    },
                )
            else:
                await self.recover_snapshot(event, at=now)
                self._live_book_ready.add(key)
            return True
        if key in self._recovery_buffers:
            self._recovery_buffers[key].append(event)
            self._record(
                "PUBLIC_RECOVERY_DELTA_BUFFERED", at=now, venue=event.venue,
                detail={"symbol": event.canonical_market, "sequence": event.sequence},
            )
            return True
        stream = self.coordinator.stream(event.venue, event.canonical_market)
        if not stream.apply_delta(event):
            await self.mark_disconnected(event.venue, event.canonical_market, at=now, stream_kind="book")
            if event.venue is Venue.RISEX:
                return False
            self._start_snapshot_recovery(event.venue, event.canonical_market)
            return False
        else:
            self._live_book_ready.add(key)
            if self.lifecycle is not None:
                self.next_position_monitor_at = now
                await self.tick(now)
        return True

    async def _resubscribe_risex_orderbooks(
        self,
        ws: object,
        adapter: PublicAdapter,
        symbols: tuple[str, ...],
        *,
        triggering_symbol: str,
    ) -> None:
        now = self.clock.now()
        for symbol in symbols:
            if symbol != triggering_symbol:
                await self.mark_disconnected(
                    Venue.RISEX, symbol, at=now, stream_kind="book",
                    exception=ValueError("orderbook channel resubscribe"),
                )
            self._recovery_buffers[(Venue.RISEX, symbol)] = []
        market_ids = [adapter.market_id(symbol) for symbol in symbols]  # type: ignore[attr-defined]
        self._record(
            "PUBLIC_BOOK_RESYNC_STARTED", at=now, venue=Venue.RISEX,
            detail={"symbols": list(symbols), "source": "WS_RESUBSCRIBE"},
        )
        await ws.send_json(adapter.orderbook_unsubscription())  # type: ignore[attr-defined]
        await ws.send_json(adapter.orderbook_subscription(market_ids))  # type: ignore[attr-defined]

    def _start_snapshot_recovery(self, venue: Venue, symbol: str) -> None:
        key = (venue, symbol)
        if key in self._recovery_buffers:
            return
        self._recovery_buffers[key] = []
        existing = self._recovery_tasks.get(key)
        if existing is not None and not existing.done():
            return
        recovery = (
            self._restart_extended_book_stream(venue, symbol)
            if venue is Venue.EXTENDED
            else self._recover_snapshot_in_background(venue, symbol)
        )
        self._recovery_tasks[key] = asyncio.create_task(
            recovery
        )

    def _install_recovery_snapshot(
        self, snapshot: OrderBook
    ) -> tuple[OrderBook, int, int]:
        """Install and drain the current buffer without yielding to the receiver."""
        key = (snapshot.venue, snapshot.canonical_market)
        stream = self.coordinator.stream(*key)
        stream.connected(self.clock.now())
        stream.snapshot(snapshot)
        buffer = self._recovery_buffers[key]
        buffered = len(buffer)
        replayed = 0
        while buffer:
            delta = buffer.pop(0)
            if (
                snapshot.sequence is not None
                and delta.sequence is not None
                and delta.sequence <= snapshot.sequence
            ):
                continue
            if not stream.apply_delta(delta):
                raise ValueError("buffered book delta sequence gap")
            replayed += 1
        recovered = stream.book()
        if recovered is None:
            raise ValueError("snapshot recovery produced no book")
        self._recovery_buffers.pop(key, None)
        return recovered, buffered, replayed

    async def _restart_extended_book_stream(
        self, venue: Venue, symbol: str
    ) -> None:
        key = (venue, symbol)
        task_key = (Venue.EXTENDED, symbol, "book")
        self._record(
            "PUBLIC_BOOK_RESYNC_STARTED", venue=venue,
            detail={"symbol": symbol, "source": "WS_RECONNECT"},
        )
        try:
            current = self._stream_tasks.get(task_key)
            if current is not None and current is not asyncio.current_task():
                current.cancel()
                await asyncio.gather(current, return_exceptions=True)
            adapter = None if self.adapters is None else self.adapters.get(venue)
            if (
                isinstance(adapter, ExtendedAdapter)
                and self._stop_event is not None
                and not self._stop_event.is_set()
            ):
                self._stream_tasks[task_key] = asyncio.create_task(
                    self._extended_stream(adapter, symbol, "book")
                )
        finally:
            self._recovery_tasks.pop(key, None)

    def _start_extended_stream(self, symbol: str, kind: str) -> None:
        if (
            self._session is None
            or self.adapters is None
            or self._stop_event is None
            or self._stop_event.is_set()
        ):
            return
        adapter = self.adapters.get(Venue.EXTENDED)
        if not isinstance(adapter, ExtendedAdapter):
            return
        key = (Venue.EXTENDED, symbol, kind)
        current = self._stream_tasks.get(key)
        if current is not None and not current.done():
            return
        self._stream_tasks[key] = asyncio.create_task(
            self._extended_stream(adapter, symbol, kind)
        )

    async def _restart_extended_stream(self, symbol: str, kind: str) -> None:
        key = (Venue.EXTENDED, symbol, kind)
        current = self._stream_tasks.get(key)
        if current is not None and current is not asyncio.current_task():
            current.cancel()
            await asyncio.gather(current, return_exceptions=True)
            self._stream_tasks.pop(key, None)
        self._start_extended_stream(symbol, kind)

    async def _extended_heartbeat(self, ws: object) -> None:
        try:
            while self._stop_event is not None and not self._stop_event.is_set():
                await self._sleep(10)
                if self._stop_event.is_set():
                    return
                await ws.ping()  # type: ignore[attr-defined]
        except asyncio.CancelledError:
            raise
        except Exception:
            close = getattr(ws, "close", None)
            if close is not None:
                await close()
            raise

    async def _recover_snapshot_in_background(
        self, venue: Venue, symbol: str
    ) -> None:
        key = (venue, symbol)
        assert self.adapters is not None
        started = self.clock.now()
        self._record(
            "PUBLIC_SNAPSHOT_RECOVERY_STARTED", at=started, venue=venue,
            detail={"symbol": symbol},
        )
        try:
            snapshot = await self.adapters[venue].fetch_book(symbol)
            recovered, buffered, replayed = self._install_recovery_snapshot(snapshot)
            await self.recover_snapshot(recovered, at=self.clock.now())
            self._record(
                "PUBLIC_SNAPSHOT_RECOVERY_COMPLETED", venue=venue,
                detail={
                    "symbol": symbol,
                    "buffered": buffered,
                    "replayed": replayed,
                    "elapsed_seconds": str(Decimal(str(
                        (self.clock.now() - started).total_seconds()
                    ))),
                },
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._set_component_readiness(
                venue, f"stream:{symbol}:book", False,
                f"PUBLIC_SNAPSHOT_RECOVERY_FAILED:book:{type(exc).__name__}:{str(exc)[:120]}",
                self.clock.now(),
            )
            self._record(
                "PUBLIC_SNAPSHOT_RECOVERY_FAILED", venue=venue,
                detail={"symbol": symbol, "exception_class": type(exc).__name__},
            )
        finally:
            if key in self._recovery_buffers and venue is not Venue.EXTENDED:
                self._recovery_buffers.pop(key, None)
            self._recovery_tasks.pop(key, None)

    async def _extended_stream(
        self, adapter: ExtendedAdapter, symbol: str, kind: str
    ) -> None:
        assert self._session is not None
        socket_identity = (Venue.EXTENDED, kind, (symbol,))
        url = {
            "book": adapter.orderbook_stream_url(symbol),
            "trade": adapter.trades_stream_url(symbol),
            "funding": adapter.funding_stream_url(symbol),
        }[kind]
        delay = 1
        while self._stop_event is not None and not self._stop_event.is_set():
            session_established = False
            try:
                async with self._session.ws_connect(url, heartbeat=None, autoping=False) as ws:
                    session_established = True
                    self._confirm_extended_stream(
                        symbol, kind, self.clock.now(), data_ready=False
                    )
                    self._socket_reconnected(
                        socket_identity, at=self.clock.now()
                    )
                    self._watchdog_restarted(
                        socket_identity, at=self.clock.now()
                    )
                    delay = 1
                    ordinal = 0
                    heartbeat = asyncio.create_task(self._extended_heartbeat(ws))
                    try:
                        async for message in ws:
                            if message.type is aiohttp.WSMsgType.PING:
                                await ws.pong(message.data)
                                self._confirm_extended_stream(
                                    symbol, kind, self.clock.now(), data_ready=False
                                )
                                continue
                            if message.type is aiohttp.WSMsgType.PONG:
                                self._confirm_extended_stream(
                                    symbol, kind, self.clock.now(), data_ready=False
                                )
                                continue
                            if message.type is not aiohttp.WSMsgType.TEXT:
                                continue
                            payload = json.loads(message.data, parse_float=Decimal)
                            if kind == "book":
                                healthy = await self.apply_book_event(
                                    adapter.normalize_book_message(payload)
                                )
                                if healthy:
                                    self._confirm_extended_stream(
                                        symbol, kind, self.clock.now(), data_ready=True
                                    )
                            elif kind == "trade":
                                sequence, trades = adapter.normalize_trade_message(
                                    payload, received_at=self.clock.now(),
                                    session_id=str(id(ws)), starting_ordinal=ordinal,
                                )
                                previous = self._extended_trade_sequences.get(symbol)
                                if previous is not None and sequence != previous + 1:
                                    raise ValueError("Extended trade sequence gap")
                                self._extended_trade_sequences[symbol] = sequence
                                self._confirm_extended_stream(
                                    symbol, kind, self.clock.now(), data_ready=True
                                )
                                for trade in trades:
                                    ordinal += 1
                                    await self.deliver_trade(trade)
                            else:
                                row = self.observations.get((Venue.EXTENDED, symbol))
                                if row is not None:
                                    settlement = adapter.normalize_applied_funding_message(
                                        payload, row.market,
                                    )
                                    self._confirm_extended_stream(
                                        symbol, kind, self.clock.now(), data_ready=True
                                    )
                                    if settlement is not None:
                                        await self._apply_extended_funding_record(settlement)
                    finally:
                        heartbeat.cancel()
                        await asyncio.gather(heartbeat, return_exceptions=True)
                    if self._stop_event is None or not self._stop_event.is_set():
                        raise ConnectionError("public websocket closed")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self._stop_event is not None and self._stop_event.is_set():
                    break
                if session_established:
                    self._socket_disconnected(
                        socket_identity, at=self.clock.now()
                    )
                await self.mark_disconnected(Venue.EXTENDED, symbol, stream_kind=kind, exception=exc)
                await self._sleep(delay)
                delay = min(delay * 2, 30)

    async def _combined_stream(
        self, venue: Venue, adapter: PublicAdapter, symbols: tuple[str, ...]
    ) -> None:
        assert self._session is not None
        ordered_symbols = tuple(sorted(symbols))
        socket_identity = (venue, "combined", ordered_symbols)
        delay = 1
        if not symbols:
            return
        while self._stop_event is not None and not self._stop_event.is_set():
            session_established = False
            try:
                async with self._session.ws_connect(
                    adapter.ws_base, heartbeat=10, autoping=False, compress=15
                ) as ws:
                    session_established = True
                    if venue is Venue.RISEX:
                        ids = [adapter.market_id(symbol) for symbol in symbols]  # type: ignore[attr-defined]
                        await ws.send_json(adapter.orderbook_subscription(ids))  # type: ignore[attr-defined]
                        await ws.send_json(adapter.trades_subscription(ids))  # type: ignore[attr-defined]
                    else:
                        for symbol in symbols:
                            product = adapter.product_id(symbol)  # type: ignore[attr-defined]
                            for kind in ("book_depth", "trade", "funding_rate", "funding_payment"):
                                await ws.send_json(adapter.subscription(kind, product))  # type: ignore[attr-defined]
                    self._socket_reconnected(
                        socket_identity, at=self.clock.now()
                    )
                    for symbol in symbols:
                        self.coordinator.stream(venue, symbol).connected(self.clock.now())
                        self.mark_trade_stream_connected(venue, symbol)
                        for component in ("funding", "connection_combined"):
                            self._set_component_readiness(
                                venue, f"{component}:{symbol}", True,
                                "PUBLIC_STREAM_CONNECTED", self.clock.now(),
                            )
                        await self.recover_snapshot(await adapter.fetch_book(symbol))
                        for component in (
                            "book", "trade", "funding", "connection_combined"
                        ):
                            self._set_component_readiness(
                                venue, f"{component}:{symbol}", True,
                                "PUBLIC_STREAM_CONNECTED", self.clock.now(),
                            )
                    delay = 1
                    ordinal = 0
                    async for message in ws:
                        if message.type is aiohttp.WSMsgType.PING:
                            await ws.pong(message.data)
                            for symbol in symbols:
                                self.coordinator.stream(venue, symbol).connection_confirmed(self.clock.now())
                            continue
                        if message.type is aiohttp.WSMsgType.PONG:
                            for symbol in symbols:
                                self.coordinator.stream(venue, symbol).connection_confirmed(self.clock.now())
                            continue
                        if message.type is not aiohttp.WSMsgType.TEXT:
                            continue
                        payload = json.loads(message.data, parse_float=Decimal)
                        ordinal += 1
                        kind = str(payload.get("channel", payload.get("type", ""))).lower()
                        if venue is Venue.RISEX and str(payload.get("type", "")).lower() not in {"snapshot", "update"}:
                            for symbol in symbols:
                                self.coordinator.stream(venue, symbol).connection_confirmed(self.clock.now())
                            continue
                        if "orderbook" in kind or "book_depth" in kind:
                            event = adapter.normalize_book_message(payload)  # type: ignore[attr-defined]
                            healthy = await self.apply_book_event(event)
                            if venue is Venue.RISEX and not healthy:
                                await self._resubscribe_risex_orderbooks(
                                    ws, adapter, symbols,
                                    triggering_symbol=event.canonical_market,
                                )
                        elif "trade" in kind:
                            await self.deliver_trade(
                                adapter.normalize_trade(
                                    payload,
                                    received_at=self.clock.now(),
                                    session_id=str(id(ws)),
                                    ordinal=ordinal,
                                )
                            )
                        elif venue is Venue.NADO and "funding_rate" in kind:
                            product = int(payload["product_id"])
                            symbol = adapter.symbol_for_product(product)  # type: ignore[attr-defined]
                            row = self.observations.get((venue, symbol))
                            mid = self._book_mid(venue, symbol)
                            if row is not None:
                                quote = adapter.normalize_funding_rate_message(  # type: ignore[attr-defined]
                                    payload,
                                    row.market,
                                    index_price_x18=(
                                        None
                                        if mid is None
                                        else str(mid * Decimal("1000000000000000000"))
                                    ),
                                    assumed_open_at=self.clock.now(),
                                )
                                await self._apply_funding_quote(quote)
                        elif venue is Venue.NADO and "funding_payment" in kind:
                            product = int(payload["product_id"])
                            symbol = adapter.symbol_for_product(product)  # type: ignore[attr-defined]
                            row = self.observations.get((venue, symbol))
                            if row is not None:
                                previous_long = self._nado_cumulative_funding.get((symbol, "long"))
                                previous_short = self._nado_cumulative_funding.get((symbol, "short"))
                                quote = adapter.normalize_funding_payment_message(  # type: ignore[attr-defined]
                                    payload,
                                    row.market,
                                    previous_long_x18=previous_long,
                                    previous_short_x18=previous_short,
                                    assumed_open_at=self.clock.now(),
                                )
                                self._nado_cumulative_funding[(symbol, "long")] = payload.get("cumulative_funding_long_x18")
                                self._nado_cumulative_funding[(symbol, "short")] = payload.get("cumulative_funding_short_x18")
                                await self._apply_funding_quote(quote)
                    if self._stop_event is None or not self._stop_event.is_set():
                        raise ConnectionError("public websocket closed")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self._stop_event is not None and self._stop_event.is_set():
                    break
                if venue is Venue.RISEX:
                    for symbol in symbols:
                        self._recovery_buffers.pop((venue, symbol), None)
                if session_established:
                    self._socket_disconnected(
                        socket_identity, at=self.clock.now()
                    )
                for symbol in symbols:
                    await self.mark_disconnected(venue, symbol, stream_kind="combined", exception=exc)
                await self._sleep(delay)
                delay = min(delay * 2, 30)

    async def start_streams(self) -> None:
        if self._session is None or self.adapters is None:
            return
        self._stop_event = self._stop_event or asyncio.Event()
        await self._reconcile_streams()

    def _required_symbols(self, venue: Venue) -> set[str]:
        symbols = {
            market.venue_symbol for market in self._candidate_markets()
            if market.venue is venue
        }
        if self.broker is not None and self.broker.state.order is not None:
            plan = self.broker.state.order.route_plan
            market = plan.risex_market if venue is Venue.RISEX else plan.hedge_market
            if market.venue is venue:
                symbols.add(market.venue_symbol)
        if self.lifecycle is not None:
            market = (
                self.lifecycle.snapshot.risex_market
                if venue is Venue.RISEX else self.lifecycle.snapshot.hedge_market
            )
            if market.venue is venue:
                symbols.add(market.venue_symbol)
        return symbols

    async def _reconcile_streams(self) -> None:
        await self._reconcile_combined_streams()
        await self._reconcile_extended_streams()

    async def _reconcile_combined_streams(self) -> None:
        if self._session is None or self.adapters is None or self._stop_event is None:
            return
        for venue in (Venue.RISEX, Venue.NADO):
            adapter = self.adapters.get(venue)
            if adapter is None:
                continue
            wanted = tuple(sorted(self._required_symbols(venue)))
            current = self._combined_symbols.get(venue, ())
            key = (venue, "*", "combined")
            task = self._stream_tasks.get(key)
            if wanted == current and task is not None and not task.done():
                continue
            if task is not None:
                self._pending_socket_episodes.pop(
                    (venue, "combined", tuple(sorted(current))), None
                )
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                self._stream_tasks.pop(key, None)
            self._combined_symbols[venue] = wanted
            self._remove_obsolete_components(venue, set(wanted), self.clock.now())
            if wanted:
                self._stream_tasks[key] = asyncio.create_task(
                    self._combined_stream(venue, adapter, wanted)
                )
                self._record(
                    "PUBLIC_STREAM_RECONCILED", venue=venue,
                    detail={"symbols": list(wanted)},
                )

    def _required_extended_symbols(self) -> set[str]:
        return self._required_symbols(Venue.EXTENDED)

    async def _reconcile_extended_streams(self) -> None:
        if self._session is None or self.adapters is None or self._stop_event is None:
            return
        adapter = self.adapters.get(Venue.EXTENDED)
        if not isinstance(adapter, ExtendedAdapter):
            return
        wanted = {
            (Venue.EXTENDED, symbol, kind)
            for symbol in self._required_extended_symbols()
            for kind in ("book", "trade", "funding")
        }
        current = {
            key for key in self._stream_tasks if key[0] is Venue.EXTENDED
        }
        self._remove_obsolete_components(
            Venue.EXTENDED, {key[1] for key in wanted}, self.clock.now()
        )
        for key in sorted(wanted - current, key=lambda row: (row[1], row[2])):
            _, symbol, kind = key
            self._set_component_readiness(
                Venue.EXTENDED, f"{kind}:{symbol}", False,
                f"PUBLIC_{kind.upper()}_DATA_PENDING", self.clock.now(),
            )
            self._set_component_readiness(
                Venue.EXTENDED, f"connection_{kind}:{symbol}", False,
                f"PUBLIC_{kind.upper()}_CONNECTION_PENDING", self.clock.now(),
            )
            if kind == "book":
                self.coordinator.stream(Venue.EXTENDED, symbol).gap()
            self._start_extended_stream(symbol, kind)
            self._record(
                "PUBLIC_STREAM_ADDED", venue=Venue.EXTENDED,
                detail={"symbol": symbol, "stream": kind},
            )
        removed = current - wanted
        for key in removed:
            self._extended_confirmed_at.pop((key[1], key[2]), None)
            if key[2] == "trade":
                self._extended_trade_sequences.pop(key[1], None)
                self._trade_stream_ready.discard((Venue.EXTENDED, key[1]))
            self._pending_socket_episodes.pop(
                (Venue.EXTENDED, key[2], (key[1],)), None
            )
            self._stream_tasks[key].cancel()
        if removed:
            await asyncio.gather(
                *(self._stream_tasks[key] for key in removed),
                return_exceptions=True,
            )
            for key in removed:
                self._stream_tasks.pop(key, None)
                self._record(
                    "PUBLIC_STREAM_REMOVED", venue=Venue.EXTENDED,
                    detail={"symbol": key[1], "stream": key[2]},
                )

    async def _pause_or_stop(self, seconds: float) -> None:
        assert self._stop_event is not None
        sleep_task = asyncio.create_task(self._sleep(seconds))
        stop_task = asyncio.create_task(self._stop_event.wait())
        _, pending = await asyncio.wait(
            (sleep_task, stop_task), return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    def _next_wakeup_at(self, now: datetime) -> datetime:
        if self.broker is None:
            self._refresh_focused_cycle(now)
        deadlines = [value for value in (
            self.next_full_scan_at, self.next_focused_scan_at,
            self.next_position_monitor_at, self.next_health_check_at,
            self.next_extended_catalog_at,
        ) if value is not None and value > now]
        if self.broker is not None and self.broker.state.order is not None:
            order = self.broker.state.order
            deadlines.extend(value for value in (order.created_at, order.cutoff_at) if value > now)
        else:
            cycle = self.focused_cycle
            if cycle is not None:
                schedule = activation_schedule(cycle)
                focused = cycle.start_at - timedelta(
                    seconds=self.config.focused_window_seconds
                )
                deadlines.extend(
                    value
                    for value in (focused, schedule.activation_at, schedule.cutoff_at)
                    if value > now
                )
        return min(deadlines, default=now + timedelta(seconds=10))

    async def run(self, *, stop_event: asyncio.Event | None = None) -> dict[str, object]:
        self._stop_event = stop_event or asyncio.Event()
        started_at = self.clock.now()
        self._notification_run_id = f"paper-run:{started_at.isoformat()}"
        self._record("PAPER_RUN_STARTED", at=started_at)
        self._notify_event(
            f"{self._notification_run_id}:started", "RUNTIME_STARTED", started_at,
            "Paper runtime started",
        )
        loop = asyncio.get_running_loop()
        installed: list[signal.Signals] = []
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(
                    sig, self._request_stop, sig.name
                )
                installed.append(sig)
            except (NotImplementedError, RuntimeError):
                pass
        try:
            await self.scan()
            self.next_extended_catalog_at = started_at + timedelta(
                seconds=self.config.extended_universe_refresh_seconds
            )
            await self._restore(self.last_scan.logical_at)
            self.next_full_scan_at = self.last_scan.logical_at + timedelta(
                seconds=self.config.normal_scan_seconds
            )
            self.next_health_check_at = self.last_scan.logical_at + timedelta(seconds=10)
            await self.start_streams()
            ready_at = self.clock.now()
            self._record("PAPER_RUN_READY", at=ready_at)
            self._notify_event(
                f"{self._notification_run_id}:ready", "RUNTIME_READY", ready_at,
                "Paper runtime ready",
            )
            # Initial REST observations can predate completion of a slow
            # bootstrap. Seed the existing single-flight refresh now so the
            # first 120-second FULL scan does not inherit that bootstrap age.
            self._start_public_refresh()
            while not self._stop_event.is_set():
                await self.tick()
                now = self.clock.now()
                delay = max(0.0, (self._next_wakeup_at(now) - now).total_seconds())
                await self._pause_or_stop(delay)
            if self._background_fatal is not None:
                raise self._background_fatal
            if self._stop_cause is None:
                self._request_stop(
                    "STOP_EVENT" if stop_event is not None else "UNKNOWN_EXTERNAL_STOP"
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if self._stop_cause != "RUNTIME_FATAL":
                self._request_stop("RUNTIME_FATAL")
                self._record(
                    "RUNTIME_FATAL", at=self._stop_requested_at,
                    detail={"exception_class": type(exc).__name__},
                )
            raise
        finally:
            for sig in installed:
                loop.remove_signal_handler(sig)
            await self.shutdown()
        return {"status": "STOPPED_SAFE", "forced_close": False}

    def _request_stop(self, cause: str) -> None:
        if self._stop_cause is None:
            self._stop_cause = cause if cause in {
                "SIGINT", "SIGTERM", "STOP_EVENT", "RUNTIME_FATAL",
                "UNKNOWN_EXTERNAL_STOP",
            } else "UNKNOWN_EXTERNAL_STOP"
            self._stop_requested_at = self.clock.now()
        if self._stop_event is not None:
            self._stop_event.set()

    async def shutdown(self) -> None:
        if not self.accepting_entries:
            return
        self.accepting_entries = False
        at = self.clock.now()
        if self._stop_cause is None:
            self._request_stop("UNKNOWN_EXTERNAL_STOP")
        if self.broker is not None and self.broker.state.lifecycle_state is LifecycleState.ENTRY_MAKER_OPEN:
            state = await self.broker.cancel_for_process_restart(restarted_at=at)
            self.repository.save_decision(recorded_at=at, entry_state=state)
        self._record(
            (
                "RUNTIME_STOPPED_FATAL"
                if self._stop_cause == "RUNTIME_FATAL" else "STOPPED_SAFE"
            ),
            at=at,
            detail={
                "forced_close": False,
                "open_position_preserved": self.lifecycle is not None,
                "stop_cause": self._stop_cause,
                "stop_requested_at": (
                    None if self._stop_requested_at is None
                    else self._stop_requested_at.isoformat()
                ),
            },
        )
        if self._notification_run_id is not None and self._stop_cause != "RUNTIME_FATAL":
            self._notify_event(
                f"{self._notification_run_id}:stopped", "SAFE_STOP", at,
                "Paper runtime stopped safely",
            )
        if self._stop_event is not None:
            self._stop_event.set()
        owned = list(self._stream_tasks.values())
        if self._refresh_task is not None:
            owned.append(self._refresh_task)
        owned.extend(self._recovery_tasks.values())
        if self._extended_universe_task is not None:
            owned.append(self._extended_universe_task)
        for task in owned:
            task.cancel()
        if owned:
            await asyncio.gather(*owned, return_exceptions=True)
        self._stream_tasks.clear()
        self._recovery_tasks.clear()
        self._pending_socket_episodes.clear()
        self._pending_watchdog_episodes.clear()
        self._recovery_buffers.clear()
        self._refresh_task = None
        self._extended_universe_task = None

    async def close(self) -> None:
        if self.accepting_entries or self._stream_tasks:
            await self.shutdown()
        if self._session is not None and not self._session.closed:
            await self._session.close()


async def public_scan_once(
    repository: PaperRepository,
    *,
    adapters: Mapping[Venue, PublicAdapter] | None = None,
    session_factory: Callable[[], aiohttp.ClientSession] = _public_session,
    clock: Clock | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> dict[str, object]:
    async with PublicPaperRuntime(
        repository,
        adapters=adapters,
        session_factory=session_factory,
        clock=clock,
        sleep=sleep,
    ) as runtime:
        result = await runtime.scan()
        runtime.accepting_entries = False
        return result


async def public_paper_run(
    repository: PaperRepository,
    *,
    adapters: Mapping[Venue, PublicAdapter] | None = None,
    session_factory: Callable[[], aiohttp.ClientSession] = _public_session,
    clock: Clock | None = None,
    stop_event: asyncio.Event | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    notifications: NotificationOutbox | None = None,
) -> dict[str, object]:
    if notifications is not None:
        await notifications.start()
    try:
        async with PublicPaperRuntime(
            repository,
            adapters=adapters,
            session_factory=session_factory,
            clock=clock,
            sleep=sleep,
            notifications=notifications,
        ) as runtime:
            return await runtime.run(stop_event=stop_event)
    finally:
        if notifications is not None:
            await notifications.close()
