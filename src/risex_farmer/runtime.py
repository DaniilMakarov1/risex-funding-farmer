"""Single-process public-data runtime for the existing paper domain path."""

from __future__ import annotations

import asyncio
import json
import signal
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Protocol, TypeVar

import aiohttp

from .config import PAPER_CONFIG, PaperConfig
from .exchanges.base import PublicAdapter, PublicDataUnavailable
from .exchanges.extended import ExtendedAdapter
from .exchanges.nado import NadoAdapter
from .exchanges.risex import RisexAdapter
from .lifecycle import LifecycleEngine, LifecycleSnapshot
from .market_data import BookStream, MarketDataCoordinator
from .models import (
    BookExecutionCapture,
    BookDelta,
    ContractType,
    DataQuality,
    FillProvenance,
    FundingCashQuote,
    FundingQuality,
    FundingSettlement,
    LifecycleState,
    MarketVolume,
    MarketType,
    OrderBook,
    RouteDirection,
    SettlementStatus,
    StreamHealth,
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
    liquidity_bucket,
    planned_fee_split,
    scan_once,
)
from .storage import PaperRepository


class Clock(Protocol):
    def now(self) -> datetime: ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class StreamSessionId:
    value: int


@dataclass(frozen=True, slots=True)
class RecoveryEpisodeId:
    value: int


@dataclass(frozen=True, slots=True)
class RecoveryAttemptGeneration:
    value: int


@dataclass(slots=True)
class RecoveryEpisode:
    episode_id: RecoveryEpisodeId
    attempt_generation: RecoveryAttemptGeneration
    buffer: list[BookDelta]
    owned_stream_session_id: StreamSessionId
    task: asyncio.Task[None] | None = None
    attempts: int = 0
    overflows: int = 0
    terminal: str | None = None


@dataclass(frozen=True, slots=True)
class ExtendedHealthRecoveryRequest:
    symbol: str
    kind: str
    detected_at: datetime
    confirmed_at: datetime
    stream_session_id: StreamSessionId
    stream_task: asyncio.Task[None]


@dataclass(frozen=True, slots=True)
class RecoveryPublicationCandidate:
    recovered: OrderBook
    health: StreamHealth
    observation: MarketObservation | None
    components: tuple[tuple[str, VenueReadiness], ...]
    venue_readiness: VenueReadiness
    lifecycle_owner: LifecycleEngine | None
    lifecycle_candidate: LifecycleEngine | None
    lifecycle_before: LifecycleSnapshot | None
    lifecycle_after: LifecycleSnapshot | None
    execution_captures: tuple[BookExecutionCapture, ...]
    fill_provenance: tuple[tuple[str, FillProvenance], ...]
    buffer: tuple[BookDelta, ...]
    completion_detail: tuple[tuple[str, object], ...]


class _PublicSocketClosed(ConnectionError):
    def __init__(self, classification: str) -> None:
        super().__init__(f"public websocket transport closed: {classification}")
        self.classification = classification


_PublicResult = TypeVar("_PublicResult")
PUBLIC_REQUEST_TIMEOUT_SECONDS = 30
PUBLIC_REST_CONCURRENCY_PER_VENUE = 2
# This is only a cleanup bound.  It is deliberately below the product's
# 30-second safe-stop requirement and does not alter public-data freshness or
# request deadlines.
CANCELLATION_DRAIN_TIMEOUT_SECONDS = 1.0


def _volume_signature(rows: Mapping[str, MarketVolume]) -> dict[str, tuple[Decimal | None, str]]:
    return {
        symbol: (row.quote_volume_usd, row.source)
        for symbol, row in rows.items()
    }


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


async def _gather_owned(*work: Awaitable[Any]) -> tuple[Any, ...]:
    tasks = [asyncio.ensure_future(row) for row in work]
    gathered = asyncio.gather(*tasks)
    gathered.add_done_callback(_consume_future_result)
    try:
        # Shield the group so parent cancellation reaches this cleanup path
        # immediately instead of waiting for a cancellation-resistant child.
        return tuple(await asyncio.shield(gathered))
    except BaseException:
        if not gathered.done():
            gathered.cancel()
        await _cancel_tasks_bounded(tasks)
        raise


def _consume_future_result(future: asyncio.Future[Any]) -> None:
    if future.cancelled():
        return
    try:
        future.exception()
    except BaseException:
        return


async def _cancel_tasks_bounded(
    tasks: list[asyncio.Future[Any]] | set[asyncio.Future[Any]] | tuple[asyncio.Future[Any], ...],
    *,
    timeout_seconds: float = CANCELLATION_DRAIN_TIMEOUT_SECONDS,
) -> set[asyncio.Future[Any]]:
    current = asyncio.current_task()
    pending = {
        task for task in tasks
        if not task.done() and task is not current
    }
    for task in pending:
        task.cancel()
    if pending:
        done, pending = await asyncio.wait(
            pending, timeout=timeout_seconds
        )
        for task in done:
            _consume_future_result(task)
    return pending


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
        "liquidity_bucket": liquidity_bucket(
            None if plan.route is None else plan.route.route_liquidity_usd
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
        "bbo_spread_usd": (
            None if plan.bbo_spread_usd is None else str(plan.bbo_spread_usd)
        ),
        "taker_slippage_usd": (
            None if plan.taker_slippage_usd is None else str(plan.taker_slippage_usd)
        ),
        "quoted_spread_plus_exact_slippage_proxy_usd": (
            None
            if plan.bbo_spread_usd is None or plan.taker_slippage_usd is None
            else str(plan.bbo_spread_usd + plan.taker_slippage_usd)
        ),
        "freshness_age_seconds": (
            None
            if plan.freshness_age_seconds is None
            else str(plan.freshness_age_seconds)
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
        self._public_rest_slots = {
            venue: asyncio.Semaphore(PUBLIC_REST_CONCURRENCY_PER_VENUE)
            for venue in Venue
        }
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
        self._retired_stream_tasks: set[asyncio.Task[None]] = set()
        self._refresh_task: asyncio.Task[None] | None = None
        self._pending_full_scan_at: datetime | None = None
        self._pending_full_deadline_at: datetime | None = None
        self._catalog_generation = 0
        self._catalog_refresh_pending = False
        self._recoveries: dict[tuple[Venue, str], RecoveryEpisode] = {}
        self._retired_recovery_tasks: set[asyncio.Task[None]] = set()
        self._extended_health_recovery_task: asyncio.Task[None] | None = None
        self._extended_health_recovery_requests: dict[
            tuple[str, str], ExtendedHealthRecoveryRequest
        ] = {}
        self._recovery_episode_number = 0
        self._recovery_attempt_number = 0
        self._pending_socket_episodes: dict[
            tuple[Venue, str, tuple[str, ...]], dict[str, object]
        ] = {}
        self._pending_watchdog_episodes: dict[
            tuple[Venue, str, tuple[str, ...]], dict[str, object]
        ] = {}
        self._socket_episode_number = 0
        self._watchdog_episode_number = 0
        self._stop_event: asyncio.Event | None = None
        self._attempt_number = 0
        self._nado_cumulative_funding: dict[tuple[str, str], object] = {}
        self._trade_stream_ready: set[tuple[Venue, str]] = set()
        self._live_book_ready: set[tuple[Venue, str]] = set()
        self._last_readiness_evidence_at: dict[Venue, datetime] = {}
        self._extended_confirmed_at: dict[tuple[str, str], datetime] = {}
        self._stream_sessions: dict[
            tuple[Venue, str, str], StreamSessionId
        ] = {}
        self._stream_session_number = 0
        self._book_revisions: dict[tuple[Venue, str], int] = {}
        self._book_recovery_generations: dict[tuple[Venue, str], int] = {}
        self._book_checksums: dict[tuple[Venue, str], int | None] = {}
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
        self._tick_task: asyncio.Task[None] | None = None
        self._detached_tasks: set[asyncio.Future[Any]] = set()
        self._catalog_reconciliation_in_progress = False
        self._notification_run_id: str | None = None
        self._stop_cause: str | None = None
        self._requested_at: datetime | None = None
        self._background_fatal: BaseException | None = None
        self._position_event_lock = asyncio.Lock()
        self._shutdown_started = False
        self._startup_gate_satisfied = False

    async def _public_call(
        self,
        venue: Venue,
        operation: Callable[[], Awaitable[_PublicResult]],
    ) -> _PublicResult:
        """Bound both queueing and execution of public REST fallback."""
        async with asyncio.timeout(PUBLIC_REQUEST_TIMEOUT_SECONDS):
            async with self._public_rest_slots[venue]:
                return await operation()

    def _new_stream_session(
        self, key: tuple[Venue, str, str]
    ) -> StreamSessionId:
        self._stream_session_number += 1
        session_id = StreamSessionId(self._stream_session_number)
        self._stream_sessions[key] = session_id
        return session_id

    def _owns_stream_session(
        self, key: tuple[Venue, str, str], session_id: StreamSessionId
    ) -> bool:
        return self._stream_sessions.get(key) == session_id

    @staticmethod
    def _book_stream_key(venue: Venue, symbol: str) -> tuple[Venue, str, str]:
        return (
            (venue, symbol, "book")
            if venue is Venue.EXTENDED else (venue, "*", "combined")
        )

    def _book_session_value(self, venue: Venue, symbol: str) -> int:
        session = self._stream_sessions.get(self._book_stream_key(venue, symbol))
        return 0 if session is None else session.value

    def _book_generation_value(self, key: tuple[Venue, str]) -> int:
        recovery = self._recoveries.get(key)
        if recovery is not None and recovery.terminal is None:
            return recovery.attempt_generation.value
        return self._book_recovery_generations.get(key, 0)

    def _bump_book_revision(
        self,
        key: tuple[Venue, str],
        *,
        recovery_generation: int | None = None,
        checksum: int | None = None,
    ) -> None:
        self._book_revisions[key] = self._book_revisions.get(key, 0) + 1
        if recovery_generation is not None:
            self._book_recovery_generations[key] = recovery_generation
        self._book_checksums[key] = checksum

    def _execution_capture(
        self, observation: MarketObservation, at: datetime
    ) -> BookExecutionCapture | None:
        book = observation.book
        health = observation.health
        if book is None or health is None:
            return None
        key = (book.venue, book.canonical_market)
        return BookExecutionCapture(
            book=book,
            health=health,
            received_at=health.last_market_event_at or book.observed_at,
            decision_at=at,
            stream_session_id=self._book_session_value(*key),
            recovery_generation=self._book_generation_value(key),
            book_revision=self._book_revisions.get(key, 0),
            checksum=self._book_checksums.get(key),
        )

    def _captures_are_current(
        self, *captures: BookExecutionCapture | None
    ) -> bool:
        for capture in captures:
            if capture is None:
                return False
            key = (capture.book.venue, capture.book.canonical_market)
            if (
                self._book_session_value(*key) != capture.stream_session_id
                or self._book_generation_value(key) != capture.recovery_generation
                or self._book_revisions.get(key, 0) != capture.book_revision
            ):
                return False
        return True

    def _new_recovery_episode(
        self, key: tuple[Venue, str], session_id: StreamSessionId
    ) -> RecoveryEpisode:
        self._recovery_episode_number += 1
        self._recovery_attempt_number += 1
        episode = RecoveryEpisode(
            RecoveryEpisodeId(self._recovery_episode_number),
            RecoveryAttemptGeneration(self._recovery_attempt_number),
            [],
            session_id,
        )
        self._recoveries[key] = episode
        return episode

    def _owns_recovery(
        self,
        key: tuple[Venue, str],
        episode: RecoveryEpisode,
        generation: RecoveryAttemptGeneration,
    ) -> bool:
        venue, symbol = key
        stream_key = (
            (venue, symbol, "book")
            if venue is Venue.EXTENDED else (venue, "*", "combined")
        )
        return (
            self._recoveries.get(key) is episode
            and episode.terminal is None
            and episode.attempt_generation == generation
            and self._stream_sessions.get(stream_key)
            == episode.owned_stream_session_id
        )

    def _retire_recovery_task(self, task: asyncio.Task[None] | None) -> None:
        if task is None or task.done() or task is asyncio.current_task():
            return
        task.cancel()
        self._retired_recovery_tasks.add(task)
        task.add_done_callback(self._retired_recovery_tasks.discard)

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
        if self._shutdown_started and event_type not in {
            "STOPPED_SAFE", "RUNTIME_STOPPED_FATAL"
        }:
            return
        self.repository.record_runtime_evidence(
            recorded_at=at or self.clock.now(),
            event_type=event_type,
            venue=None if venue is None else venue.value,
            detail=detail,
        )
        now = at or self.clock.now()
        if event_type in {
            "PUBLIC_SOCKET_DISCONNECTED", "PUBLIC_SOCKET_RECONNECTED",
        }:
            self._notify_socket_outage_if_due(
                venue=venue, detail=detail, at=now,
            )
        # Physical churn and logical resync are persisted operational evidence;
        # only semantic failure or a socket episode beyond health opens outage.
        if event_type in {
            "PUBLIC_SNAPSHOT_RECOVERY_FAILED", "PUBLIC_STREAM_CONFIRMATION_STALE",
            "PUBLIC_SCAN_BLOCKED",
        }:
            if event_type == "PUBLIC_SCAN_BLOCKED":
                reason = "UNKNOWN"
                if detail is not None and detail.get("reason") is not None:
                    reason = str(detail["reason"])
                text = f"Critical public scan blocked: {reason}"
            else:
                text = (
                    f"Critical public data loss: "
                    f"{venue.value if venue else 'PUBLIC'} {event_type}"
                )
            self._notify_outage(
                event_type, degraded=True, venue=venue, detail=detail,
                event_id=f"data-loss:{venue.value if venue else 'PUBLIC'}:"
                f"{event_type}:{now.isoformat()}",
                kind="CRITICAL_DATA_LOSS", occurred_at=now,
                text=text,
            )
        elif event_type == "PUBLIC_SOCKET_RECONNECTED":
            if self._pending_socket_episodes_for_venue(venue):
                return
            episode = None if detail is None else detail.get("episode_id")
            recovery_id = episode or (
                f"{venue.value if venue else 'PUBLIC'}:{event_type}:{now.isoformat()}"
            )
            self._notify_outage(
                event_type, degraded=False, venue=venue, detail=detail,
                identity_override=self._socket_wave_identity(venue),
                event_id=f"data-recovery:{recovery_id}", kind="DATA_RECOVERY",
                occurred_at=now,
                text=f"Public data recovered: "
                f"{venue.value if venue else 'PUBLIC'} {event_type}",
            )
        elif event_type in {
            "PUBLIC_SNAPSHOT_RECOVERY_COMPLETED", "PUBLIC_STREAM_RESTARTED",
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

    def _notify_socket_outage_if_due(
        self,
        *,
        venue: Venue | None,
        detail: dict[str, object] | None,
        at: datetime,
    ) -> None:
        if detail is None or not self._socket_episode_is_due(detail, at):
            return
        episode = detail.get("episode_id") or detail.get("disconnected_at")
        recovery_id = (
            str(episode) if episode is not None else at.isoformat()
        )
        self._notify_outage(
            "PUBLIC_SOCKET_DISCONNECTED", degraded=True, venue=venue,
            detail=detail,
            identity_override=self._socket_wave_identity(venue),
            event_id=f"data-loss:{venue.value if venue else 'PUBLIC'}:"
            f"PUBLIC_SOCKET_DISCONNECTED:{recovery_id}",
            kind="CRITICAL_DATA_LOSS", occurred_at=at,
            text=f"Critical public data loss: "
            f"{venue.value if venue else 'PUBLIC'} PUBLIC_SOCKET_DISCONNECTED",
        )

    def _socket_episode_is_due(
        self, detail: dict[str, object], at: datetime
    ) -> bool:
        disconnected_at = detail.get("disconnected_at")
        if not isinstance(disconnected_at, str):
            return False
        try:
            started_at = datetime.fromisoformat(disconnected_at)
            age = at - started_at
        except (TypeError, ValueError):
            return False
        return age >= timedelta(
            seconds=self.config.max_market_stream_silence_seconds
        )

    def _notify_pending_socket_outages(self, at: datetime) -> None:
        due_by_venue: dict[Venue | None, list[dict[str, object]]] = {}
        for (venue, _stream_kind, _markets), detail in tuple(
            self._pending_socket_episodes.items()
        ):
            if self._socket_episode_is_due(detail, at):
                due_by_venue.setdefault(venue, []).append(detail)
        for venue, details in sorted(
            due_by_venue.items(),
            key=lambda item: item[0].value if item[0] else "PUBLIC",
        ):
            detail = min(
                details,
                key=lambda row: (
                    str(row.get("disconnected_at") or ""),
                    str(row.get("episode_id") or ""),
                ),
            )
            self._notify_socket_outage_if_due(
                venue=venue, detail=detail, at=at,
            )

    def _pending_socket_episodes_for_venue(self, venue: Venue | None) -> bool:
        return any(
            identity[0] is venue
            for identity in self._pending_socket_episodes
        )

    def _socket_wave_identity(self, venue: Venue | None) -> str:
        return f"socket-wave:{venue.value if venue else 'PUBLIC'}"

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
        identity_override: str | None = None,
    ) -> None:
        if self.notifications is None:
            return
        detail = detail or {}
        episode = detail.get("episode_id")
        stream_kind = detail.get("stream_kind", detail.get("stream", "book"))
        market = detail.get(
            "symbol", detail.get("market", detail.get("markets", "PUBLIC"))
        )
        if identity_override is not None:
            identity = identity_override
        elif event_type == "PUBLIC_SCAN_BLOCKED":
            semantic_episode = (
                detail.get("scheduled_at")
                or detail.get("deadline_at")
                or detail.get("reason")
                or "UNKNOWN"
            )
            identity = f"PUBLIC_SCAN_BLOCKED:{semantic_episode}"
        elif venue is Venue.EXTENDED and stream_kind == "book":
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
            if separator and not (
                venue is Venue.EXTENDED and name.startswith("applied_funding:")
            ):
                by_symbol.setdefault(symbol, []).append(row)
        symbol_ready = not by_symbol or any(
            all(row.available for row in rows) for rows in by_symbol.values()
        )
        available_now = (catalog is None or catalog.available) and symbol_ready
        failed = next((
            row for name, row in components.items()
            if not row.available and not (
                venue is Venue.EXTENDED and name.startswith("applied_funding:")
            )
        ), None)
        self._set_readiness(
            venue,
            available_now,
            detail if available_now or failed is None else failed.detail,
            at,
        )

    def _symbol_components_available(self, venue: Venue, symbol: str) -> bool:
        components = self.component_readiness.get(venue, {})
        book = components.get(f"book:{symbol}")
        return book is None or book.available

    def _extended_stream_components_available(
        self, symbol: str, kind: str
    ) -> bool:
        components = self.component_readiness.get(Venue.EXTENDED, {})
        return all(
            row.available
            for name, row in components.items()
            if name in {f"{kind}:{symbol}", f"connection_{kind}:{symbol}"}
        )

    def _extended_stream_connection_available(
        self, symbol: str, kind: str
    ) -> bool:
        row = self.component_readiness.get(Venue.EXTENDED, {}).get(
            f"connection_{kind}:{symbol}"
        )
        return row is not None and row.available

    def _extended_transport_gap_kinds(self, symbol: str) -> tuple[str, ...]:
        """Return Extended sockets already in a physical recovery episode.

        A background refresh must not queue a second REST book/funding fan-out
        behind a known transport outage.  The existing stream and component
        gates remain authoritative; this method only identifies an outage that
        has already been observed and recorded.
        """
        gap_kinds: list[str] = []
        components = self.component_readiness.get(Venue.EXTENDED, {})
        for kind in ("book", "trade", "funding"):
            identity = (Venue.EXTENDED, kind, (symbol,))
            if (
                identity in self._pending_socket_episodes
                or identity in self._pending_watchdog_episodes
            ):
                gap_kinds.append(kind)
                continue
            data_component = "applied_funding" if kind == "funding" else kind
            rows = (
                components.get(f"{data_component}:{symbol}"),
                components.get(f"connection_{kind}:{symbol}"),
            )
            if any(
                row is not None
                and row.detail.startswith(
                    (
                        "PUBLIC_STREAM_DISCONNECTED:",
                        "PUBLIC_STREAM_CONFIRMATION_STALE:",
                        "PUBLIC_STREAM_TRANSPORT_GAP",
                    )
                )
                for row in rows
            ):
                gap_kinds.append(kind)
        return tuple(gap_kinds)

    def _preserve_extended_transport_gap_observation(
        self,
        market: Any,
        volume: MarketVolume | None,
        existing: MarketObservation | None,
        gap_kinds: tuple[str, ...],
    ) -> None:
        """Retain only evidence that is still authoritative during a gap.

        A disconnected Extended stream is already a precise fail-closed
        condition.  Keep a healthy book only when the book socket itself is
        healthy; never install a REST snapshot or synthesize a fresh funding
        quote while the symbol is in transport recovery.
        """
        key = (Venue.EXTENDED, market.venue_symbol)
        at = self.clock.now()
        stream = self.coordinator.stream(*key)
        book = stream.book()
        funding = None if existing is None else existing.funding
        self.observations[key] = MarketObservation(
            market,
            volume,
            book,
            funding,
            stream.health(at),
            trade_stream_ready=key in self._trade_stream_ready,
            funding_stream_ready=self._extended_stream_connection_available(
                market.venue_symbol, "funding"
            ),
        )
        for kind in gap_kinds:
            data_component = "applied_funding" if kind == "funding" else kind
            self._set_component_readiness(
                Venue.EXTENDED,
                f"{data_component}:{market.venue_symbol}",
                False,
                "PUBLIC_STREAM_TRANSPORT_GAP",
                at,
            )
            self._set_component_readiness(
                Venue.EXTENDED,
                f"connection_{kind}:{market.venue_symbol}",
                False,
                "PUBLIC_STREAM_TRANSPORT_GAP",
                at,
            )
        self._record(
            "PUBLIC_MARKET_OBSERVATION_DEFERRED",
            at=at,
            venue=Venue.EXTENDED,
            detail={
                "symbol": market.venue_symbol,
                "reason": "EXTENDED_STREAM_TRANSPORT_GAP",
                "components": list(gap_kinds),
            },
        )

    def _remove_obsolete_components(
        self, venue: Venue, relevant_symbols: set[str], at: datetime
    ) -> None:
        components = self.component_readiness.get(venue)
        if not components:
            return
        removed = False
        for name in tuple(components):
            _, separator, symbol = name.partition(":")
            if separator and symbol not in relevant_symbols:
                components.pop(name)
                removed = True
        if not removed:
            return
        catalog = components.get("catalog")
        by_symbol: dict[str, list[VenueReadiness]] = {}
        for name, row in components.items():
            _, separator, symbol = name.partition(":")
            if separator and not (
                venue is Venue.EXTENDED and name.startswith("applied_funding:")
            ):
                by_symbol.setdefault(symbol, []).append(row)
        symbol_ready = not by_symbol or any(
            all(row.available for row in rows) for rows in by_symbol.values()
        )
        available = (catalog is None or catalog.available) and symbol_ready
        failed = next((
            row for name, row in components.items()
            if not row.available and not (
                venue is Venue.EXTENDED and name.startswith("applied_funding:")
            )
        ), None)
        self._set_readiness(
            venue,
            available,
            (
                "PUBLIC_COMPONENTS_RECONCILED"
                if available or failed is None else failed.detail
            ),
            at,
        )

    async def _catalog(
        self,
        venue: Venue,
        adapter: PublicAdapter,
        *,
        mark_catalog_change: bool = True,
    ) -> None:
        at = self.clock.now()
        try:
            if isinstance(adapter, ExtendedAdapter):
                markets, volumes = await adapter.fetch_catalog()
            else:
                markets, volumes = await _gather_owned(
                    adapter.fetch_markets(),  # type: ignore[attr-defined]
                    adapter.fetch_volumes(),  # type: ignore[attr-defined]
                )
            completed = self.clock.now()
            if isinstance(adapter, ExtendedAdapter):
                changed = self._install_extended_catalog(
                    markets, volumes, completed, full=True
                )
            else:
                previous_markets = self.markets.get(venue)
                current_symbols = {
                    volume.canonical_market for volume in volumes
                }
                previous_volumes = {
                    key[1]: value
                    for key, value in self.volumes.items()
                    if key[0] is venue
                }
                self.markets[venue] = tuple(markets)
                for key in tuple(self.volumes):
                    if key[0] is venue and key[1] not in current_symbols:
                        self.volumes.pop(key, None)
                for volume in volumes:
                    self.volumes[(venue, volume.canonical_market)] = volume
                changed = (
                    previous_markets != tuple(markets)
                    or _volume_signature(previous_volumes)
                    != _volume_signature({
                        volume.canonical_market: volume for volume in volumes
                    })
                )
            if changed and mark_catalog_change:
                self._catalog_generation += 1
                self._catalog_refresh_pending = True
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
    ) -> bool:
        symbols = tuple(market.venue_symbol for market in markets)
        if not symbols or len(set(symbols)) != len(symbols):
            raise ValueError("Extended catalog is empty or duplicated")
        volume_map = {row.canonical_market: row for row in volumes}
        if len(volume_map) != len(volumes) or set(volume_map) != set(symbols):
            raise ValueError("Extended catalog volumes are incomplete")
        if full:
            previous_markets = self.markets.get(Venue.EXTENDED)
            previous_volumes = {
                key[1]: value
                for key, value in self.volumes.items()
                if key[0] is Venue.EXTENDED
            }
            changed = (
                previous_markets != tuple(markets)
                or _volume_signature(previous_volumes)
                != _volume_signature(volume_map)
            )
            self.markets[Venue.EXTENDED] = tuple(markets)
            self._extended_universe_at = at
            self._extended_metadata_at = {symbol: at for symbol in symbols}
            for key in tuple(self.volumes):
                if key[0] is Venue.EXTENDED and key[1] not in volume_map:
                    self.volumes.pop(key, None)
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
            changed = False
        for symbol, volume in volume_map.items():
            self.volumes[Venue.EXTENDED, symbol] = volume
        return changed

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

    async def _refresh_extended_universe(
        self, *, include_universe: bool = True,
        include_normal_catalogs: bool = True,
    ) -> None:
        assert self.adapters is not None
        adapter = self.adapters.get(Venue.EXTENDED)
        async def refresh_universe_catalog() -> None:
            assert isinstance(adapter, ExtendedAdapter)
            try:
                markets, volumes = await asyncio.wait_for(
                    adapter.fetch_catalog(),
                    timeout=self.config.extended_universe_request_timeout_seconds,
                )
                completed = self.clock.now()
                changed = self._install_extended_catalog(
                    markets, volumes, completed, full=True
                )
                if changed:
                    self._catalog_generation += 1
                    self._catalog_refresh_pending = True
                self._last_catalog_good_at[Venue.EXTENDED] = completed
                self._update_extended_catalog_readiness(completed)
                self._record(
                    "PUBLIC_REQUEST_COMPLETED", at=completed,
                    venue=Venue.EXTENDED,
                    detail={
                        "component": "extended_universe",
                        "timeout_seconds": (
                            self.config.extended_universe_request_timeout_seconds
                        ),
                    },
                )
            except asyncio.CancelledError:
                raise
            except (
                aiohttp.ClientError, TimeoutError, OSError,
                PublicDataUnavailable, ValueError, KeyError, TypeError,
            ) as exc:
                self._update_extended_catalog_readiness(self.clock.now())
                self._record(
                    "PUBLIC_REQUEST_FAILED", venue=Venue.EXTENDED,
                    detail={
                        "component": "extended_universe",
                        "endpoint_class": "catalog",
                        "exception_class": type(exc).__name__,
                        "timeout_seconds": (
                            self.config.extended_universe_request_timeout_seconds
                        ),
                        "cache_state": (
                            "CACHED_LAST_GOOD"
                            if self._extended_universe_at is not None
                            and self.clock.now() - self._extended_universe_at
                            <= timedelta(
                                seconds=self.config.extended_universe_max_age_seconds
                            ) else "FAIL_CLOSED"
                        ),
                    },
                )

        try:
            work: list[Awaitable[None]] = [self._refresh_extended_required()]
            if include_normal_catalogs:
                work.extend(
                    self._catalog(venue, venue_adapter)
                    for venue, venue_adapter in self.adapters.items()
                    if venue is not Venue.EXTENDED
                )
            if include_universe and isinstance(adapter, ExtendedAdapter):
                work.append(refresh_universe_catalog())
            await _gather_owned(*work)
        finally:
            self._extended_universe_task = None

    def _start_background_catalog_refresh(
        self, *, include_extended_universe: bool,
        include_normal_catalogs: bool = True,
    ) -> None:
        if self._extended_universe_task is not None and not self._extended_universe_task.done():
            return
        self._extended_universe_task = asyncio.create_task(
            self._refresh_extended_universe(
                include_universe=include_extended_universe,
                include_normal_catalogs=include_normal_catalogs,
            )
        )
        self._extended_universe_task.add_done_callback(self._background_task_done)

    def _start_extended_universe_refresh(self) -> None:
        self._start_background_catalog_refresh(include_extended_universe=True)

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
                            <= timedelta(seconds=self.config.extended_required_markets_max_age_seconds)
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
            seconds=self.config.extended_required_markets_max_age_seconds
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
        selected = sorted(risex_assets & hedge_assets)
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
            if isinstance(adapter, ExtendedAdapter) and background:
                transport_gap = self._extended_transport_gap_kinds(
                    market.venue_symbol
                )
                if transport_gap:
                    self._preserve_extended_transport_gap_observation(
                        market, volume, existing, transport_gap
                    )
                    return
            live_health = self.coordinator.stream(*key).health(self.clock.now())
            live_stream_ready = (
                key in self._live_book_ready
                if isinstance(adapter, ExtendedAdapter)
                else key in self._trade_stream_ready
            )
            healthy_live = (
                background
                and live_stream_ready
                and live_health.data_quality is DataQuality.COMPLETE
            )
            preserve_stream_book = healthy_live
            if isinstance(adapter, RisexAdapter):
                # The immutable contract becomes eligible only after both public
                # book and recent-trade unit evidence are proven in this scan.
                if healthy_live and existing is not None:
                    book = self.coordinator.stream(*key).book()
                    market = existing.market
                else:
                    book = await self._public_call(
                        market.venue,
                        lambda: adapter.fetch_book(market.venue_symbol),
                    )
                    market = await self._public_call(
                        market.venue,
                        lambda: adapter.prime_recent_trade_evidence(market),
                    )
                funding = await self._public_call(
                    market.venue,
                    lambda: adapter.fetch_funding_quote(
                        market, assumed_open_at=assumed_at
                    ),
                )
            else:
                if preserve_stream_book:
                    book = self.coordinator.stream(*key).book()
                    funding = await self._public_call(
                        market.venue,
                        lambda: adapter.fetch_funding_quote(
                            market, assumed_open_at=assumed_at
                        ),
                    )
                else:
                    book, funding = await _gather_owned(
                        self._public_call(
                            market.venue,
                            lambda: adapter.fetch_book(market.venue_symbol),
                        ),
                        self._public_call(
                            market.venue,
                            lambda: adapter.fetch_funding_quote(
                                market, assumed_open_at=assumed_at
                            ),
                        ),
                    )
            logical_at = self.clock.now()
            funding = _quote_for_open_time(funding, logical_at)
            stream = self.coordinator.stream(market.venue, market.venue_symbol)
            if not preserve_stream_book:
                stream.connected(logical_at)
                stream.snapshot(book)
                stream.connection_confirmed(logical_at)
                self._bump_book_revision(
                    key,
                    checksum=(
                        stream.risex_checksum()
                        if market.venue is Venue.RISEX else None
                    ),
                )
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
        except (
            aiohttp.ClientError, TimeoutError, OSError, PublicDataUnavailable,
            ValueError, KeyError, TypeError,
        ) as exc:
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
            await _gather_owned(
                *(
                    self._catalog(
                        venue,
                        adapter,
                        mark_catalog_change=(scan_kind != "INITIAL"),
                    )
                    for venue, adapter in self.adapters.items()
                )
            )
            self.observations.clear()
            assumed_at = self.clock.now()
            candidates = self._candidate_markets()
            await _gather_owned(
                *(self._market_observation(market, assumed_at) for market in candidates)
            )
        logical_at = self.clock.now()
        captured = tuple(self.observations.values())
        normalized: list[MarketObservation] = []
        for observation in captured:
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
            book_components_available = self._symbol_components_available(
                observation.market.venue, observation.market.venue_symbol
            )
            if observation.market.venue is Venue.EXTENDED:
                book_components_available = self._extended_stream_components_available(
                    observation.market.venue_symbol, "book"
                )
            if not book_components_available:
                health = replace(
                    health, stream_connected=False, data_quality=DataQuality.DEGRADED
                )
            trade_stream_ready = refresh or (
                observation.market.venue, observation.market.venue_symbol
            ) in self._trade_stream_ready
            funding_stream_ready = True
            if observation.market.venue is Venue.EXTENDED:
                funding_stream_ready = refresh or self._extended_stream_connection_available(
                    observation.market.venue_symbol, "funding"
                )
            normalized.append(replace(
                observation, market=market, funding=funding, health=health,
                trade_stream_ready=trade_stream_ready,
                funding_stream_ready=funding_stream_ready,
            ))
        normalized_tuple = tuple(normalized)
        for observation in normalized_tuple:
            timestamps = (
                None if observation.book is None else observation.book.observed_at,
                None if observation.funding is None else observation.funding.observed_at,
                None if observation.health is None
                else observation.health.last_market_event_at,
                None if observation.health is None
                else observation.health.last_connection_confirmation_at,
            )
            if any(at is not None and at > logical_at for at in timestamps):
                raise RuntimeError("scan observation timestamp exceeds logical_at")
        normalized_by_market = {
            (row.market.venue, row.market.venue_symbol): row
            for row in normalized_tuple
        }
        captured_keys = set(normalized_by_market)
        observations_source = (
            "REST_BOOTSTRAP"
            if refresh else (
                "LIVE_STREAM"
                if captured_keys
                and captured_keys <= self._trade_stream_ready
                and captured_keys <= self._live_book_ready
                else "MIXED"
            )
        )
        snapshot = await scan_once(
            normalized_tuple, logical_at, config=self.config
        )
        persist_scan = (
            self.last_scan is None or self.last_scan.logical_at != logical_at
        )
        self.last_scan = snapshot
        if persist_scan:
            self.repository.save_decision(
                recorded_at=logical_at,
                scan_snapshot=snapshot,
                funding_quotes=tuple(
                    row.funding
                    for row in normalized_tuple if row.funding is not None
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
                "observations_source": observations_source,
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
                plan, rank=ranks.get(id(plan)),
                observations=normalized_by_market,
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

    async def _refresh_public_data(self) -> None:
        assert self.adapters is not None
        started = self.clock.now()
        catalog_generation = self._catalog_generation
        self._record("PUBLIC_REFRESH_STARTED", at=started)
        try:
            self._update_extended_catalog_readiness(self.clock.now())
            assumed_at = self.clock.now()
            candidates = self._candidate_markets()
            candidate_keys = {
                (market.venue, market.venue_symbol) for market in candidates
            }
            protected_keys: set[tuple[Venue, str]] = set()
            if self.broker is not None and self.broker.state.order is not None:
                order_plan = self.broker.state.order.route_plan
                protected_keys.update({
                    (Venue.RISEX, order_plan.risex_market.venue_symbol),
                    (order_plan.hedge_venue, order_plan.hedge_market.venue_symbol),
                })
            if self.lifecycle is not None:
                lifecycle_snapshot = self.lifecycle.snapshot
                protected_keys.update({
                    (Venue.RISEX, lifecycle_snapshot.risex_market.venue_symbol),
                    (
                        lifecycle_snapshot.hedge_market.venue,
                        lifecycle_snapshot.hedge_market.venue_symbol,
                    ),
                })
            for key in tuple(self.observations):
                if key not in candidate_keys | protected_keys:
                    self.observations.pop(key, None)
            await _gather_owned(*(
                self._market_observation(
                    market, assumed_at, background=True
                )
                for market in candidates
            ))
            self._catalog_reconciliation_in_progress = True
            try:
                await self._reconcile_streams()
            finally:
                self._catalog_reconciliation_in_progress = False
            completed = self.clock.now()
            if catalog_generation != self._catalog_generation:
                self._catalog_refresh_pending = True
                self._record(
                    "PUBLIC_REFRESH_SUPERSEDED", at=completed,
                    detail={
                        "started_at": started.isoformat(),
                        "completed_at": completed.isoformat(),
                        "catalog_generation_at_start": catalog_generation,
                        "catalog_generation_at_completion": self._catalog_generation,
                    },
                )
                return
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
        self._catalog_refresh_pending = False
        self._refresh_task = asyncio.create_task(self._refresh_public_data())
        self._refresh_task.add_done_callback(self._background_task_done)

    def _startup_gate_detail(self, at: datetime) -> dict[str, object]:
        assert self.adapters is not None
        catalog = {
            venue.value: (
                self.component_readiness.get(venue, {}).get("catalog")
            )
            for venue in Venue
        }
        candidates = {
            (market.venue, market.venue_symbol)
            for market in self._candidate_markets()
        }
        observed = set(self.observations)
        return {
            "catalog_ready": {
                venue: row is not None and row.available
                for venue, row in catalog.items()
            },
            "candidate_count": len(candidates),
            "observed_count": len(observed & candidates),
            "catalog_generation": self._catalog_generation,
            "catalog_refresh_pending": self._catalog_refresh_pending,
            "at": at.isoformat(),
        }

    def _startup_catalog_gate_ready(self) -> bool:
        if self.adapters is None or any(venue not in self.adapters for venue in Venue):
            return False
        return all(
            self.component_readiness.get(venue, {}).get("catalog") is not None
            and self.component_readiness[venue]["catalog"].available
            for venue in Venue
        )

    def _startup_stream_gate_ready(self) -> bool:
        if self._session is None or self.adapters is None or self._stop_event is None:
            return True
        for venue in (Venue.RISEX, Venue.NADO):
            wanted = tuple(sorted(self._required_symbols(venue)))
            if wanted and (
                self._combined_symbols.get(venue, ()) != wanted
                or (
                    task := self._stream_tasks.get((venue, "*", "combined"))
                ) is None
                or task.done()
            ):
                return False
        wanted_extended = {
            (Venue.EXTENDED, symbol, kind)
            for symbol in self._required_extended_symbols()
            for kind in ("book", "trade", "funding")
        }
        active_extended = {
            key
            for key, task in self._stream_tasks.items()
            if key[0] is Venue.EXTENDED and not task.done()
        }
        return active_extended == wanted_extended

    def _startup_evidence_gate_ready(self) -> bool:
        if self.last_scan is None or not self._startup_catalog_gate_ready():
            return False
        if self._catalog_refresh_pending or not self._startup_stream_gate_ready():
            return False
        candidates = {
            (market.venue, market.venue_symbol)
            for market in self._candidate_markets()
        }
        if not candidates <= set(self.observations):
            return False
        return True

    def _try_mark_startup_ready(self) -> bool:
        if self._startup_gate_satisfied or not self._startup_evidence_gate_ready():
            return self._startup_gate_satisfied
        ready_at = self.clock.now()
        self._startup_gate_satisfied = True
        self._record("PAPER_RUN_READY", at=ready_at)
        self._notify_event(
            f"{self._notification_run_id}:ready", "RUNTIME_READY", ready_at,
            "Paper runtime ready",
        )
        return True

    async def _fail_pending_full_scan(
        self, *, at: datetime, reason: str
    ) -> None:
        scheduled = self._pending_full_scan_at
        deadline = self._pending_full_deadline_at
        refresh = self._refresh_task
        if refresh is not None and not refresh.done():
            pending = await _cancel_tasks_bounded((refresh,))
            self._detach_tasks(pending)
        self._refresh_task = None
        self._pending_full_scan_at = None
        self._pending_full_deadline_at = None
        self._record(
            "PUBLIC_SCAN_BLOCKED",
            at=at,
            detail={
                "kind": "full",
                "reason": reason,
                "scheduled_at": None if scheduled is None else scheduled.isoformat(),
                "deadline_at": None if deadline is None else deadline.isoformat(),
                "completed": False,
                "catalog_generation": self._catalog_generation,
            },
        )

    def _background_task_done(self, task: asyncio.Task[None]) -> None:
        if self._shutdown_started:
            return
        if task.cancelled():
            return
        exception = task.exception()
        if exception is None or self._stop_cause == "RUNTIME_FATAL":
            return
        if task is self._refresh_task:
            self._refresh_task = None
            self._pending_full_scan_at = None
            self._pending_full_deadline_at = None
        self._background_fatal = exception
        self._request_stop("RUNTIME_FATAL")
        self._record(
            "RUNTIME_FATAL", at=self._requested_at,
            detail={"exception_class": type(exception).__name__},
        )

    def _retired_stream_task_done(self, task: asyncio.Task[None]) -> None:
        self._retired_stream_tasks.discard(task)
        self._background_task_done(task)

    def _observation(self, venue: Venue, symbol: str, at: datetime) -> MarketObservation:
        row = self.observations[(venue, symbol)]
        stream = self.coordinator.stream(venue, symbol)
        health = stream.health(at)
        book_components_available = self._symbol_components_available(venue, symbol)
        if venue is Venue.EXTENDED:
            book_components_available = self._extended_stream_components_available(
                symbol, "book"
            )
        if not book_components_available:
            health = replace(
                health, stream_connected=False, data_quality=DataQuality.DEGRADED
            )
        return replace(
            row, book=stream.book(), health=health,
            trade_stream_ready=(venue, symbol) in self._trade_stream_ready,
            funding_stream_ready=(
                venue is not Venue.EXTENDED
                or self._extended_stream_connection_available(symbol, "funding")
            ),
        )

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
        assert self.adapters is not None
        last_known_at = self.repository.runtime_updated_at() or at
        history_since = min(
            (
                row.settlement_at
                for row in runtime.settlements
                if row.status in {SettlementStatus.PENDING, SettlementStatus.UNRESOLVED}
            ),
            default=position.position_opened_at,
        )
        history_since = min(position.position_opened_at, history_since)
        history = await asyncio.gather(
            self.adapters[Venue.RISEX].fetch_applied_settlements(
                runtime.risex_market, since=history_since, until=at
            ),
            self.adapters[runtime.hedge_market.venue].fetch_applied_settlements(
                runtime.hedge_market, since=history_since, until=at
            ),
            return_exceptions=True,
        )
        required_keys = {row.key for row in runtime.settlements}
        settlement_updates: list[FundingSettlement] = [
            update
            for result in history
            if not isinstance(result, BaseException)
            for update in result
            if update.status == SettlementStatus.APPLIED_RATE
            and update.key in required_keys
        ]
        applied_keys = {update.key for update in settlement_updates}
        unresolved_updates: list[FundingSettlement] = []
        for venue in (Venue.RISEX, runtime.hedge_market.venue):
            for row in runtime.settlements:
                if row.venue is not venue or row.key not in required_keys:
                    continue
                if (
                    row.key not in applied_keys
                    and
                    venue is Venue.EXTENDED
                    and row.settlement_at <= at
                    and row.status in {
                        SettlementStatus.PENDING,
                        SettlementStatus.ESTIMATED,
                    }
                ):
                    unresolved_updates.append(replace(
                        row, status=SettlementStatus.UNRESOLVED, cash_usd=None,
                    ))
        risex, hedge = self._market_pair_observations(
            runtime.risex_market, runtime.hedge_market, at
        )
        risex_capture = self._execution_capture(risex, at)
        hedge_capture = self._execution_capture(hedge, at)
        await engine.restart(
            last_known_at=last_known_at,
            recovered_at=at,
            risex_observation=risex,
            hedge_observation=hedge,
            settlement_updates=tuple(settlement_updates),
            risex_capture=risex_capture,
            hedge_capture=hedge_capture,
        )
        # Settlement authority is independent of whether both execution books
        # were fresh enough for gap recovery.
        for update in settlement_updates:
            await engine.reconcile_settlement(update)
        for update in unresolved_updates:
            if update.key not in applied_keys:
                await engine.mark_extended_history_unresolved(update)
        if engine.fill_provenance and not self._captures_are_current(
            risex_capture, hedge_capture
        ):
            return
        self.repository.save_decision(
            recorded_at=at,
            lifecycle_snapshot=engine.snapshot,
            fill_provenance=engine.fill_provenance,
        )
        self.lifecycle = (
            None
            if engine.snapshot.lifecycle_state is LifecycleState.FLAT
            else engine
        )
        self._record("OPEN_POSITION_RESTORED", at=at)

    async def tick(self, at: datetime | None = None) -> None:
        now = at or self.clock.now()
        self._notify_pending_socket_outages(now)
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
                    session_id = self._stream_sessions.get(
                        (venue, "*", "combined")
                    )
                    if session_id is not None:
                        await self.mark_disconnected(
                            venue, symbol, at=now, stream_kind="health",
                            stream_session_id=session_id,
                        )
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
        elif (
            self._pending_full_scan_at is None
            and now >= self.next_full_scan_at
        ):
            scheduled = self.next_full_scan_at
            self._pending_full_scan_at = scheduled
            self._pending_full_deadline_at = now + timedelta(
                seconds=PUBLIC_REQUEST_TIMEOUT_SECONDS
            )
            self.next_full_scan_at = _next_absolute_slot(
                scheduled, now, self.config.normal_scan_seconds
            )
            self._record(
                "PUBLIC_SCAN_DEADLINE", at=now,
                detail={
                    "kind": "full", "scheduled_at": scheduled.isoformat(),
                    "lateness_seconds": str(Decimal(str((now - scheduled).total_seconds()))),
                },
            )
            self._start_public_refresh()
            self._start_background_catalog_refresh(
                include_extended_universe=False
            )
        refresh = self._refresh_task
        if self._pending_full_scan_at is not None:
            deadline = self._pending_full_deadline_at
            if deadline is not None and now >= deadline:
                await self._fail_pending_full_scan(
                    at=now, reason="PUBLIC_REFRESH_DEADLINE_EXCEEDED"
                )
            elif refresh is None:
                await self._fail_pending_full_scan(
                    at=now, reason="PUBLIC_REFRESH_OWNER_MISSING"
                )
            elif refresh.done():
                scheduled = self._pending_full_scan_at
                if refresh.cancelled() or refresh.exception() is not None:
                    await self._fail_pending_full_scan(
                        at=now, reason="PUBLIC_REFRESH_FAILED"
                    )
                elif self._catalog_refresh_pending:
                    self._refresh_task = None
                    self._start_public_refresh()
                else:
                    self._pending_full_scan_at = None
                    self._pending_full_deadline_at = None
                    self._refresh_task = None
                    await self.scan(
                        refresh=False, scan_kind="FULL", scheduled_at=scheduled
                    )
                    now = self.last_scan.logical_at
                    self.next_full_scan_at = _next_absolute_slot(
                        scheduled, now, self.config.normal_scan_seconds
                    )
        if (
            self._catalog_refresh_pending
            and self._pending_full_scan_at is None
            and (self._refresh_task is None or self._refresh_task.done())
        ):
            self._refresh_task = None
            self._start_public_refresh()
        self._try_mark_startup_ready()
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
                async with self._position_event_lock:
                    lifecycle = self.lifecycle
                    if lifecycle is None:
                        return
                    before = lifecycle.snapshot
                    risex, hedge = self._market_pair_observations(
                        before.risex_market, before.hedge_market, now
                    )
                    candidate = lifecycle.detached()
                    risex_capture = self._execution_capture(risex, now)
                    hedge_capture = self._execution_capture(hedge, now)
                    await candidate.evaluate(
                        evaluated_at=now,
                        risex_observation=risex,
                        hedge_observation=hedge,
                        risex_capture=risex_capture,
                        hedge_capture=hedge_capture,
                    )
                    if self.lifecycle is not lifecycle or lifecycle.snapshot is not before:
                        return
                    if candidate.fill_provenance and not self._captures_are_current(
                        risex_capture, hedge_capture
                    ):
                        return
                    self.repository.save_decision(
                        recorded_at=now,
                        lifecycle_snapshot=candidate.snapshot,
                        fill_provenance=candidate.fill_provenance,
                    )
                    lifecycle.publish_candidate(candidate)
                    after = candidate.snapshot
                    if after.lifecycle_state is LifecycleState.FLAT:
                        self.lifecycle = None
                self._notify_lifecycle_transition(before, after, now)
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
        exit_receipt_version: str | None = None
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
            active_version = active_order.active_version
            if active_version is None:
                return
            exit_receipt_version = observed_version_id or active_version.version_id
            if (
                observed_version_id is not None
                and observed_version_id != active_version.version_id
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
            broker = self.broker
            before = broker.state
            order = before.order
            assert order is not None
            version = observed_version_id or order.active_version.version_id
            risex, hedge = self._route_observations(order.route_plan, at)
            risex_capture = self._execution_capture(risex, at)
            candidate = broker.detached()
            result = await candidate.process_trade(
                trade,
                observed_version_id=version,
                processed_at=at,
                risex_observation=risex,
                hedge_observation=hedge,
                recompute_funding=self._recompute_funding,
                risex_capture=risex_capture,
            )
            if self.broker is not broker or broker.state is not before:
                return
            if result.fill_provenance and not self._captures_are_current(
                risex_capture
            ):
                return
            lifecycle_candidate = (
                LifecycleEngine(result.state, config=self.config)
                if result.state.position is not None else None
            )
            self.repository.save_decision(
                recorded_at=at,
                trade_events=(trade,),
                entry_state=result.state,
                lifecycle_snapshot=(
                    None if lifecycle_candidate is None
                    else lifecycle_candidate.snapshot
                ),
                fill_provenance=result.fill_provenance,
            )
            if result.state.position is not None:
                self.lifecycle = lifecycle_candidate
                self._notify_lifecycle_transition(None, self.lifecycle.snapshot, at)
                self.broker = None
                self.next_position_monitor_at = at + timedelta(
                    seconds=self.config.open_position_monitor_seconds
                )
            elif self.broker is broker and broker.state is before:
                broker.publish_candidate(candidate)
            return
        if self.lifecycle is not None and self.lifecycle.snapshot.exit_order is not None:
            lifecycle = self.lifecycle
            before = lifecycle.snapshot
            order = before.exit_order
            active = order.active_version
            if active is None:
                return
            assert exit_receipt_version is not None
            version = exit_receipt_version
            risex, hedge = self._market_pair_observations(
                before.risex_market,
                before.hedge_market,
                at,
            )
            risex_capture = self._execution_capture(risex, at)
            candidate = lifecycle.detached()
            result = await candidate.process_exit_trade(
                trade,
                observed_version_id=version,
                processed_at=at,
                risex_observation=risex,
                hedge_observation=hedge,
                risex_capture=risex_capture,
            )
            if result.detail == "STALE_EXIT_VERSION":
                return
            if result.fill_provenance and not self._captures_are_current(
                risex_capture
            ):
                return
            if self.lifecycle is not lifecycle or lifecycle.snapshot is not before:
                return
            self.repository.save_decision(
                recorded_at=at,
                trade_events=(trade,),
                lifecycle_snapshot=candidate.snapshot,
                fill_provenance=result.fill_provenance,
            )
            lifecycle.publish_candidate(candidate)
            self._notify_lifecycle_transition(before, candidate.snapshot, at)
            if candidate.snapshot.lifecycle_state is LifecycleState.FLAT:
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
            session_id = self._stream_sessions.get(
                (Venue.EXTENDED, symbol, "trade")
            )
            if session_id is not None:
                self._confirm_extended_stream(
                    symbol, "trade", now, data_ready=True,
                    stream_session_id=session_id,
                )
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
            self._notify_settlement_transition(before, after, self.clock.now())

    def _notify_settlement_transition(
        self,
        before: FundingSettlement,
        after: FundingSettlement,
        at: datetime,
    ) -> None:
        if (before.status, before.cash_usd) == (after.status, after.cash_usd):
            return
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

    @staticmethod
    def _funding_settlement_from_quote(
        quote: FundingCashQuote, snapshot: LifecycleSnapshot
    ) -> FundingSettlement | None:
        position = snapshot.position
        if quote.quality is not FundingQuality.APPLIED_RATE or position is None:
            return None
        risex_long = position.direction is RouteDirection.LONG_RISEX_SHORT_HEDGE
        venue_long = risex_long if quote.venue is Venue.RISEX else not risex_long
        cash_per_base = (
            quote.long_cash_per_canonical_base_usd
            if venue_long
            else quote.short_cash_per_canonical_base_usd
        )
        if cash_per_base is None:
            return None
        return FundingSettlement(
            quote.venue,
            quote.canonical_market,
            quote.settlement_at,
            SettlementStatus.APPLIED_RATE,
            position.canonical_quantity * cash_per_base,
        )

    async def _apply_nado_funding_quote(
        self,
        quote: FundingCashQuote,
        stream_session_id: StreamSessionId,
        cumulative_updates: tuple[tuple[tuple[str, str], object], ...],
    ) -> None:
        session_key = (Venue.NADO, "*", "combined")
        notification: tuple[
            FundingSettlement, FundingSettlement, datetime
        ] | None = None
        async with self._position_event_lock:
            if not self._owns_stream_session(session_key, stream_session_id):
                return
            commit_at = self.clock.now()
            key = (quote.venue, quote.canonical_market)
            lifecycle = self.lifecycle
            if lifecycle is not None:
                before_snapshot = lifecycle.snapshot
                settlement = self._funding_settlement_from_quote(
                    quote, before_snapshot
                )
                if settlement is not None and settlement.key in {
                    row.key for row in before_snapshot.settlements
                }:
                    before_settlement = next(
                        row for row in before_snapshot.settlements
                        if row.key == settlement.key
                    )
                    candidate = lifecycle.detached()
                    await candidate.reconcile_settlement(settlement)
                    if (
                        not self._owns_stream_session(
                            session_key, stream_session_id
                        )
                        or self.lifecycle is not lifecycle
                        or lifecycle.snapshot is not before_snapshot
                    ):
                        return
                    if candidate.snapshot != before_snapshot:
                        self.repository.save_decision(
                            recorded_at=commit_at,
                            lifecycle_snapshot=candidate.snapshot,
                        )
                        lifecycle.publish_candidate(candidate)
                        after_settlement = next(
                            row for row in candidate.snapshot.settlements
                            if row.key == settlement.key
                        )
                        notification = (
                            before_settlement, after_settlement, commit_at
                        )
            for cumulative_key, value in cumulative_updates:
                self._nado_cumulative_funding[cumulative_key] = value
            observation = self.observations.get(key)
            if observation is not None:
                self.observations[key] = replace(observation, funding=quote)
            self._set_component_readiness(
                quote.venue,
                f"funding:{quote.canonical_market}",
                True,
                "PUBLIC_FUNDING_STREAM_READY",
                commit_at,
            )
        if notification is not None:
            self._notify_settlement_transition(*notification)

    async def _apply_funding_quote(self, quote: FundingCashQuote) -> None:
        key = (quote.venue, quote.canonical_market)
        self._set_component_readiness(
            quote.venue, f"funding:{quote.canonical_market}", True,
            "PUBLIC_FUNDING_STREAM_READY", self.clock.now(),
        )
        observation = self.observations.get(key)
        if observation is not None:
            self.observations[key] = replace(observation, funding=quote)
        if self.lifecycle is None:
            return
        settlement = self._funding_settlement_from_quote(
            quote, self.lifecycle.snapshot
        )
        if settlement is None:
            return
        if settlement.key in {row.key for row in self.lifecycle.snapshot.settlements}:
            await self.deliver_settlement(settlement)

    async def mark_disconnected(
        self, venue: Venue, symbol: str, *, at: datetime | None = None,
        stream_kind: str = "public", exception: BaseException | None = None,
        stream_session_id: StreamSessionId,
    ) -> None:
        session_key = (
            (venue, "*", "combined")
            if venue is not Venue.EXTENDED or stream_kind == "combined"
            else (venue, symbol, stream_kind)
        )
        if not self._owns_stream_session(session_key, stream_session_id):
            return
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
        elif venue is Venue.EXTENDED and stream_kind == "funding":
            affected = ("applied_funding", "connection_funding")
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
        if self.lifecycle is not None and (invalidates_book or invalidates_trade):
            async with self._position_event_lock:
                if (
                    not self._owns_stream_session(session_key, stream_session_id)
                ):
                    return
                lifecycle = self.lifecycle
                if lifecycle is None:
                    return
                position = lifecycle.snapshot.position
                if position is not None and (venue, symbol) in {
                    (Venue.RISEX, lifecycle.snapshot.risex_market.venue_symbol),
                    (
                        lifecycle.snapshot.hedge_market.venue,
                        lifecycle.snapshot.hedge_market.venue_symbol,
                    ),
                }:
                    before = lifecycle.snapshot
                    causal_at = self.clock.now()
                    if before.events:
                        causal_at = max(causal_at, before.events[-1].occurred_at)
                    if before.exit_order is not None and before.exit_order.versions:
                        active = before.exit_order.versions[-1]
                        causal_at = max(
                            causal_at, active.created_at, active.last_checked_at
                        )
                    candidate = lifecycle.detached()
                    await candidate.start_gap(started_at=causal_at)
                    if (
                        self.lifecycle is not lifecycle
                        or lifecycle.snapshot is not before
                        or not self._owns_stream_session(
                            session_key, stream_session_id
                        )
                    ):
                        return
                    if candidate.snapshot != before:
                        self.repository.save_decision(
                            recorded_at=causal_at,
                            lifecycle_snapshot=candidate.snapshot,
                        )
                        lifecycle.publish_candidate(candidate)
                    else:
                        return

    def _confirm_extended_stream(
        self, symbol: str, kind: str, at: datetime, *, data_ready: bool,
        stream_session_id: StreamSessionId,
    ) -> None:
        if not self._owns_stream_session(
            (Venue.EXTENDED, symbol, kind), stream_session_id
        ):
            return
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
        data_component = "applied_funding" if kind == "funding" else kind
        self._set_component_readiness(
            Venue.EXTENDED, f"{data_component}:{symbol}", True,
            f"PUBLIC_{kind.upper()}_STREAM_READY", at,
        )

    async def _check_extended_health(self, at: datetime) -> None:
        stale = [
            (symbol, kind, confirmed_at,
             self._stream_sessions.get((Venue.EXTENDED, symbol, kind)),
             self._stream_tasks.get((Venue.EXTENDED, symbol, kind)))
            for (symbol, kind), confirmed_at in self._extended_confirmed_at.items()
            if (Venue.EXTENDED, symbol, kind) in self._stream_tasks
            if at - confirmed_at > timedelta(seconds=25)
        ]
        for symbol, kind, confirmed_at, session_id, task in sorted(
            stale, key=lambda row: (row[0], row[1])
        ):
            key = (Venue.EXTENDED, symbol, kind)
            if (
                self._stop_event is None or self._stop_event.is_set()
                or session_id is None
                or not self._owns_stream_session(key, session_id)
                or self._stream_tasks.get(key) is not task
                or self._extended_confirmed_at.get((symbol, kind)) != confirmed_at
            ):
                continue
            assert session_id is not None and task is not None
            self._queue_extended_health_recovery(
                ExtendedHealthRecoveryRequest(
                    symbol, kind, at, confirmed_at,
                    session_id, task,
                )
            )

    def _queue_extended_health_recovery(
        self, request: ExtendedHealthRecoveryRequest
    ) -> None:
        if self._stop_event is None or self._stop_event.is_set():
            return
        key = (request.symbol, request.kind)
        if key in self._extended_health_recovery_requests:
            return
        self._extended_health_recovery_requests[key] = request
        owner = self._extended_health_recovery_task
        if owner is not None and not owner.done():
            return
        owner = asyncio.create_task(self._run_extended_health_recoveries())
        self._extended_health_recovery_task = owner
        owner.add_done_callback(self._extended_health_recovery_done)

    def _extended_health_recovery_is_current(
        self, request: ExtendedHealthRecoveryRequest
    ) -> bool:
        if self._stop_event is None or self._stop_event.is_set():
            return False
        key = (Venue.EXTENDED, request.symbol, request.kind)
        return (
            self._stream_sessions.get(key) == request.stream_session_id
            and self._stream_tasks.get(key) is request.stream_task
            and self._extended_confirmed_at.get(
                (request.symbol, request.kind)
            ) == request.confirmed_at
        )

    async def _recover_extended_health(
        self, request: ExtendedHealthRecoveryRequest
    ) -> None:
        stream_key = (Venue.EXTENDED, request.symbol, request.kind)
        identity = (Venue.EXTENDED, request.kind, (request.symbol,))
        if not self._extended_health_recovery_is_current(request):
            return
        self._watchdog_stale(
            identity, at=request.detected_at,
            last_confirmation=request.confirmed_at,
        )
        watchdog_detail = self._pending_watchdog_episodes[identity]
        watchdog_episode_id = str(watchdog_detail["episode_id"])
        await self.mark_disconnected(
            Venue.EXTENDED, request.symbol,
            at=request.detected_at,
            stream_kind=request.kind,
            exception=TimeoutError("public socket confirmation stale"),
            stream_session_id=request.stream_session_id,
        )
        if (
            self._stop_event is None
            or self._stop_event.is_set()
            or not self._owns_stream_session(
                stream_key, request.stream_session_id
            )
            or self._stream_tasks.get(stream_key) is not request.stream_task
        ):
            self._clear_extended_health_watchdog(identity, watchdog_episode_id)
            return
        await self._restart_extended_stream(request.symbol, request.kind)

    def _clear_extended_health_watchdog(
        self,
        identity: tuple[Venue, str, tuple[str, ...]],
        episode_id: str,
    ) -> None:
        detail = self._pending_watchdog_episodes.get(identity)
        if (
            detail is not None
            and str(detail.get("episode_id")) == episode_id
        ):
            self._pending_watchdog_episodes.pop(identity, None)

    async def _run_extended_health_recoveries(self) -> None:
        while (
            self._extended_health_recovery_requests
            and self._stop_event is not None
            and not self._stop_event.is_set()
        ):
            batch = tuple(self._extended_health_recovery_requests.items())
            outcomes: list[object] = []
            for kind in ("book", "trade", "funding"):
                group = tuple(
                    (key, request)
                    for key, request in batch
                    if request.kind == kind
                )
                if not group:
                    continue
                outcomes.extend(await asyncio.gather(
                    *(
                        self._recover_extended_health(request)
                        for _, request in group
                    ),
                    return_exceptions=True,
                ))
            for key, request in batch:
                if self._extended_health_recovery_requests.get(key) == request:
                    self._extended_health_recovery_requests.pop(key, None)
            failure = next(
                (
                    outcome
                    for outcome in outcomes
                    if isinstance(outcome, BaseException)
                    and not isinstance(outcome, asyncio.CancelledError)
                ),
                None,
            )
            if failure is not None:
                raise failure

    def _extended_health_recovery_done(
        self, task: asyncio.Task[None]
    ) -> None:
        if task is self._extended_health_recovery_task:
            self._extended_health_recovery_task = None
        self._background_task_done(task)

    def _watchdog_stale(
        self, identity: tuple[Venue, str, tuple[str, ...]], *, at: datetime,
        last_confirmation: datetime,
    ) -> None:
        if identity in self._pending_watchdog_episodes:
            return
        venue, stream_kind, markets = identity
        self._watchdog_episode_number += 1
        detail: dict[str, object] = {
            "episode_id": (
                f"watchdog:{venue.value}:{stream_kind}:{markets[0]}:"
                f"{at.isoformat()}:{self._watchdog_episode_number}"
            ),
            "stream_identity": f"{venue.value}:{stream_kind}:{markets[0]}",
            "stream_kind": stream_kind, "market": markets[0],
            "last_confirmation": last_confirmation.isoformat(),
            "detected_at": at.isoformat(),
            "stale_age": str(Decimal(str((at - last_confirmation).total_seconds()))),
            "restart_reason": "CONFIRMATION_STALE",
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
        stream_session_id: StreamSessionId,
        cause: str = "SOCKET_CLOSED",
        exception_class: str | None = None,
        close_classification: str | None = "EOF",
    ) -> None:
        session_key = (
            (identity[0], "*", "combined")
            if identity[1] == "combined"
            else (identity[0], identity[2][0], identity[1])
        )
        if not self._owns_stream_session(session_key, stream_session_id):
            return
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
            "cause": cause,
            "disconnected_stream_session_id": stream_session_id.value,
        }
        if exception_class is not None:
            detail["exception_class"] = exception_class
        if close_classification is not None:
            detail["close_classification"] = close_classification
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
        stream_session_id: StreamSessionId,
    ) -> None:
        session_key = (
            (identity[0], "*", "combined")
            if identity[1] == "combined"
            else (identity[0], identity[2][0], identity[1])
        )
        if not self._owns_stream_session(session_key, stream_session_id):
            return
        detail = self._pending_socket_episodes.pop(identity, None)
        if detail is not None:
            reconnect_detail = {
                **detail,
                "reconnected_at": at.isoformat(),
                "reconnected_stream_session_id": stream_session_id.value,
            }
            self._record(
                "PUBLIC_SOCKET_RECONNECTED",
                at=at,
                venue=identity[0],
                detail=reconnect_detail,
            )

    def _record_transport_disconnect(
        self,
        identity: tuple[Venue, str, tuple[str, ...]],
        *,
        at: datetime,
        exception: BaseException,
        stream_session_id: StreamSessionId,
    ) -> None:
        if isinstance(exception, _PublicSocketClosed):
            self._socket_disconnected(
                identity, at=at, cause="SOCKET_CLOSED",
                close_classification=exception.classification,
                stream_session_id=stream_session_id,
            )
        elif isinstance(
            exception, (aiohttp.ClientConnectionError, ConnectionError, OSError)
        ):
            self._socket_disconnected(
                identity, at=at, cause="TRANSPORT_EXCEPTION",
                exception_class=type(exception).__name__, close_classification=None,
                stream_session_id=stream_session_id,
            )

    async def _recover_snapshot_locked(
        self, book: OrderBook, *, at: datetime
    ) -> None:
        now = at
        stream = self.coordinator.stream(book.venue, book.canonical_market)
        stream.connected(now)
        stream.snapshot(book)
        stream.connection_confirmed(now)
        self._bump_book_revision(
            (book.venue, book.canonical_market),
            checksum=(stream.risex_checksum() if book.venue is Venue.RISEX else None),
        )
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
        lifecycle = self.lifecycle
        if lifecycle is None or not lifecycle.snapshot.gap_open:
            return
        snapshot = lifecycle.snapshot
        risex, hedge = self._market_pair_observations(
            snapshot.risex_market, snapshot.hedge_market, now
        )
        execution_healthy = (
            risex.health is not None
            and hedge.health is not None
            and risex.health.data_quality is DataQuality.COMPLETE
            and hedge.health.data_quality is DataQuality.COMPLETE
            and (Venue.RISEX, snapshot.risex_market.venue_symbol)
            in self._trade_stream_ready
            and (
                snapshot.hedge_market.venue,
                snapshot.hedge_market.venue_symbol,
            ) in self._trade_stream_ready
        )
        if execution_healthy:
            before = lifecycle.snapshot
            candidate = lifecycle.detached()
            risex_capture = self._execution_capture(risex, now)
            hedge_capture = self._execution_capture(hedge, now)
            await candidate.recover(
                recovered_at=now,
                risex_observation=risex,
                hedge_observation=hedge,
                risex_capture=risex_capture,
                hedge_capture=hedge_capture,
            )
            if self.lifecycle is not lifecycle or lifecycle.snapshot is not before:
                return
            if candidate.fill_provenance and not self._captures_are_current(
                risex_capture, hedge_capture
            ):
                return
            self.repository.save_decision(
                recorded_at=now,
                lifecycle_snapshot=candidate.snapshot,
                fill_provenance=candidate.fill_provenance,
            )
            lifecycle.publish_candidate(candidate)
            if candidate.snapshot.lifecycle_state is LifecycleState.FLAT:
                self.lifecycle = None

    async def recover_snapshot(
        self, book: OrderBook, *, at: datetime | None = None
    ) -> bool:
        now = at or self.clock.now()
        async with self._position_event_lock:
            await self._recover_snapshot_locked(book, at=now)
        return True

    async def _publish_recovery_snapshot(
        self,
        key: tuple[Venue, str],
        episode: RecoveryEpisode,
        generation: RecoveryAttemptGeneration,
        recovered: OrderBook,
        *,
        at: datetime,
        buffered: int,
        replayed: int,
        source: str,
    ) -> bool:
        venue, symbol = key
        expected_buffer = tuple(episode.buffer)
        projected_stream = BookStream(venue, symbol)
        projected_stream.connected(at)
        projected_stream.snapshot(recovered)
        projected_stream.connection_confirmed(at)
        health = projected_stream.health(at)
        current_observation = self.observations.get(key)
        projected_observation = (
            None if current_observation is None
            else replace(current_observation, book=recovered, health=health)
        )
        components = dict(self.component_readiness.get(venue, {}))
        ready = VenueReadiness(True, "PUBLIC_STREAM_RECOVERED", at)
        components[f"book:{symbol}"] = ready
        components[f"connection_book:{symbol}"] = ready
        catalog = components.get("catalog")
        by_symbol: dict[str, list[VenueReadiness]] = {}
        for name, row in components.items():
            _, separator, component_symbol = name.partition(":")
            if separator and not (
                venue is Venue.EXTENDED and name.startswith("applied_funding:")
            ):
                by_symbol.setdefault(component_symbol, []).append(row)
        symbol_ready = not by_symbol or any(
            all(row.available for row in rows) for rows in by_symbol.values()
        )
        available = (catalog is None or catalog.available) and symbol_ready
        failed = next((
            row for name, row in components.items()
            if not row.available and not (
                venue is Venue.EXTENDED and name.startswith("applied_funding:")
            )
        ), None)
        venue_readiness = VenueReadiness(
            available,
            "PUBLIC_STREAM_RECOVERED" if available or failed is None else failed.detail,
            at,
        )
        lifecycle_owner = self.lifecycle
        lifecycle_candidate = None
        lifecycle_before = None if lifecycle_owner is None else lifecycle_owner.snapshot
        lifecycle_after = lifecycle_before
        execution_captures: tuple[BookExecutionCapture, ...] = ()
        fill_provenance: tuple[tuple[str, FillProvenance], ...] = ()
        if lifecycle_owner is not None and lifecycle_before is not None and lifecycle_before.gap_open:
            def projected_row(row_venue: Venue, row_symbol: str) -> MarketObservation:
                if (row_venue, row_symbol) == key and projected_observation is not None:
                    return projected_observation
                return self._observation(row_venue, row_symbol, at)

            risex = projected_row(Venue.RISEX, lifecycle_before.risex_market.venue_symbol)
            hedge = projected_row(
                lifecycle_before.hedge_market.venue,
                lifecycle_before.hedge_market.venue_symbol,
            )
            execution_healthy = (
                risex.health is not None and hedge.health is not None
                and risex.health.data_quality is DataQuality.COMPLETE
                and hedge.health.data_quality is DataQuality.COMPLETE
                and (Venue.RISEX, lifecycle_before.risex_market.venue_symbol)
                in self._trade_stream_ready
                and (lifecycle_before.hedge_market.venue,
                     lifecycle_before.hedge_market.venue_symbol)
                in self._trade_stream_ready
            )
            if execution_healthy:
                def recovery_capture(
                    observation: MarketObservation,
                ) -> BookExecutionCapture | None:
                    if observation.book is None or observation.health is None:
                        return None
                    observation_key = (
                        observation.book.venue,
                        observation.book.canonical_market,
                    )
                    if observation_key != key:
                        return self._execution_capture(observation, at)
                    return BookExecutionCapture(
                        observation.book,
                        observation.health,
                        observation.health.last_market_event_at
                        or observation.book.observed_at,
                        at,
                        episode.owned_stream_session_id.value,
                        generation.value,
                        self._book_revisions.get(key, 0) + 1,
                        (
                            projected_stream.risex_checksum()
                            if venue is Venue.RISEX else None
                        ),
                    )

                risex_capture = recovery_capture(risex)
                hedge_capture = recovery_capture(hedge)
                lifecycle_candidate = lifecycle_owner.detached()
                await lifecycle_candidate.recover(
                    recovered_at=at,
                    risex_observation=risex,
                    hedge_observation=hedge,
                    risex_capture=risex_capture,
                    hedge_capture=hedge_capture,
                )
                lifecycle_after = lifecycle_candidate.snapshot
                execution_captures = tuple(
                    capture for capture in (risex_capture, hedge_capture)
                    if capture is not None
                )
                fill_provenance = lifecycle_candidate.fill_provenance
        detail = (
            ("symbol", symbol), ("buffered", buffered),
            ("replayed", replayed), ("source", source),
            ("episode_id", episode.episode_id.value),
            ("generation", generation.value),
            ("stream_session_id", episode.owned_stream_session_id.value),
        )
        candidate = RecoveryPublicationCandidate(
            recovered, health, projected_observation, tuple(components.items()),
            venue_readiness, lifecycle_owner, lifecycle_candidate,
            lifecycle_before, lifecycle_after, execution_captures,
            fill_provenance, expected_buffer, detail,
        )
        async with self._position_event_lock:
            if (
                not self._owns_recovery(key, episode, generation)
                or tuple(episode.buffer) != candidate.buffer
                or self.lifecycle is not candidate.lifecycle_owner
                or (candidate.lifecycle_owner is not None
                    and candidate.lifecycle_owner.snapshot is not candidate.lifecycle_before)
            ):
                return False
            if candidate.fill_provenance:
                for capture in candidate.execution_captures:
                    capture_key = (
                        capture.book.venue, capture.book.canonical_market
                    )
                    if capture_key == key:
                        if (
                            capture.stream_session_id
                            != episode.owned_stream_session_id.value
                            or capture.recovery_generation != generation.value
                            or capture.book_revision
                            != self._book_revisions.get(key, 0) + 1
                        ):
                            return False
                    elif not self._captures_are_current(capture):
                        return False
            completion_detail = dict(candidate.completion_detail)
            self.repository.save_decision(
                recorded_at=at,
                lifecycle_snapshot=(candidate.lifecycle_after
                    if candidate.lifecycle_after != candidate.lifecycle_before else None),
                fill_provenance=candidate.fill_provenance,
                runtime_evidence=((at, "PUBLIC_SNAPSHOT_RECOVERY_COMPLETED",
                                   venue.value, completion_detail),),
                venue_readiness=(
                    venue.value,
                    candidate.venue_readiness.updated_at,
                    candidate.venue_readiness.available,
                    candidate.venue_readiness.detail,
                ),
            )
            stream = self.coordinator.stream(venue, symbol)
            stream.connected(at)
            stream.snapshot(candidate.recovered)
            stream.connection_confirmed(at)
            self._bump_book_revision(
                key,
                recovery_generation=generation.value,
                checksum=(stream.risex_checksum() if venue is Venue.RISEX else None),
            )
            self._live_book_ready.add(key)
            self.component_readiness[venue] = dict(candidate.components)
            self.readiness[venue] = candidate.venue_readiness
            if candidate.observation is not None:
                self.observations[key] = candidate.observation
            if (
                candidate.lifecycle_owner is not None
                and candidate.lifecycle_candidate is not None
            ):
                candidate.lifecycle_owner.publish_candidate(
                    candidate.lifecycle_candidate
                )
                if (
                    candidate.lifecycle_candidate.snapshot.lifecycle_state
                    is LifecycleState.FLAT
                ):
                    self.lifecycle = None
            episode.terminal = "COMPLETE"
            episode.buffer.clear()
        self._notify_outage(
            "PUBLIC_SNAPSHOT_RECOVERY_COMPLETED", degraded=False, venue=venue,
            detail=completion_detail,
            event_id=f"data-recovery:{episode.episode_id.value}",
            kind="DATA_RECOVERY", occurred_at=at,
            text=f"Public data recovered: {venue.value} PUBLIC_SNAPSHOT_RECOVERY_COMPLETED",
        )
        return True

    async def apply_book_event(
        self,
        event: OrderBook | BookDelta,
        *,
        stream_session_id: StreamSessionId,
    ) -> bool:
        if self._shutdown_started:
            return False
        now = self.clock.now()
        key = (event.venue, event.canonical_market)
        stream_key = (
            (event.venue, event.canonical_market, "book")
            if event.venue is Venue.EXTENDED
            else (event.venue, "*", "combined")
        )
        episode = self._recoveries.get(key)
        active = episode is not None and episode.terminal is None
        if episode is not None and episode.terminal == "FAILED":
            return False
        required_session = (
            episode.owned_stream_session_id
            if active else self._stream_sessions.get(stream_key)
        )
        if required_session != stream_session_id:
            return False
        if not active:
            self.coordinator.stream(*key).connection_confirmed(now)
        if isinstance(event, OrderBook):
            if not active:
                async with self._position_event_lock:
                    if not self._owns_stream_session(
                        stream_key, stream_session_id
                    ):
                        return False
                    await self._recover_snapshot_locked(event, at=now)
                    return self._owns_stream_session(
                        stream_key, stream_session_id
                    )
            assert episode is not None
            if event.venue is Venue.EXTENDED and event.sequence is None:
                self._record(
                    "PUBLIC_REST_SNAPSHOT_IGNORED_FOR_WS_RESYNC",
                    at=now, venue=event.venue,
                    detail={"symbol": event.canonical_market},
                )
                return False
            try:
                recovered, buffered, replayed = self._build_recovery_book(
                    event, episode
                )
            except ValueError as exc:
                await self.mark_disconnected(
                    event.venue, event.canonical_market, at=now,
                    stream_kind="book", exception=exc,
                    stream_session_id=stream_session_id,
                )
                if event.venue is not Venue.RISEX:
                    self._restart_recovery_attempt(key, episode)
                return False
            return await self._publish_recovery_snapshot(
                key, episode, episode.attempt_generation, recovered,
                at=now, buffered=buffered, replayed=replayed,
                source=(
                    "WS_RESUBSCRIBE_SNAPSHOT"
                    if event.venue is Venue.RISEX else "WS_SNAPSHOT"
                ),
            )
        if active:
            assert episode is not None
            if len(episode.buffer) >= 2048:
                episode.overflows += 1
                episode.buffer.clear()
                self.coordinator.stream(*key).gap()
                self._set_component_readiness(
                    event.venue, f"book:{event.canonical_market}", False,
                    "PUBLIC_RECOVERY_BUFFER_OVERFLOW", now,
                )
                if episode.overflows == 1:
                    self._record(
                        "PUBLIC_SNAPSHOT_RECOVERY_OVERFLOW", at=now,
                        venue=event.venue,
                        detail={
                            "symbol": event.canonical_market,
                            "capacity": 2048,
                            "episode_id": episode.episode_id.value,
                            "generation": episode.attempt_generation.value,
                        },
                    )
                if episode.overflows >= 3:
                    self._retire_recovery_task(episode.task)
                    episode.task = None
                    episode.terminal = "FAILED"
                    self._record(
                        "PUBLIC_SNAPSHOT_RECOVERY_FAILED", at=now,
                        venue=event.venue,
                        detail={
                            "symbol": event.canonical_market,
                            "episode_id": episode.episode_id.value,
                            "generation": episode.attempt_generation.value,
                            "attempts": episode.attempts,
                            "overflows": episode.overflows,
                            "cause": "BUFFER_OVERFLOW_LIMIT",
                        },
                    )
                elif event.venue is not Venue.RISEX:
                    self._restart_recovery_attempt(key, episode)
            else:
                episode.buffer.append(event)
            return False
        stream = self.coordinator.stream(event.venue, event.canonical_market)
        if not stream.apply_delta(event):
            await self.mark_disconnected(
                event.venue, event.canonical_market, at=now,
                stream_kind="book", stream_session_id=stream_session_id,
            )
            if event.venue is Venue.RISEX:
                return False
            self._start_snapshot_recovery(
                event.venue, event.canonical_market,
                displaced_stream_session_id=stream_session_id,
            )
            return False
        else:
            self._bump_book_revision(key, checksum=event.checksum)
            self._live_book_ready.add(key)
            if self.lifecycle is not None:
                await self._evaluate_relevant_book_event(key, now)
        return True

    async def _evaluate_relevant_book_event(
        self, key: tuple[Venue, str], at: datetime
    ) -> None:
        """Serialize event-driven Hard Basis checks without periodic samples."""
        async with self._position_event_lock:
            lifecycle = self.lifecycle
            if lifecycle is None:
                return
            snapshot = lifecycle.snapshot
            required = {
                (Venue.RISEX, snapshot.risex_market.venue_symbol),
                (snapshot.hedge_market.venue, snapshot.hedge_market.venue_symbol),
            }
            if key not in required:
                return
            risex, hedge = self._market_pair_observations(
                snapshot.risex_market, snapshot.hedge_market, at
            )
            before = lifecycle.snapshot
            candidate = lifecycle.detached()
            risex_capture = self._execution_capture(risex, at)
            hedge_capture = self._execution_capture(hedge, at)
            await candidate.evaluate(
                evaluated_at=at,
                risex_observation=risex,
                hedge_observation=hedge,
                record_sample=False,
                hard_basis_only=True,
                risex_capture=risex_capture,
                hedge_capture=hedge_capture,
            )
            if self.lifecycle is not lifecycle or lifecycle.snapshot is not before:
                return
            if candidate.fill_provenance and not self._captures_are_current(
                risex_capture, hedge_capture
            ):
                return
            if candidate.snapshot != before:
                self.repository.save_decision(
                    recorded_at=at,
                    lifecycle_snapshot=candidate.snapshot,
                    fill_provenance=candidate.fill_provenance,
                )
                lifecycle.publish_candidate(candidate)
                after = candidate.snapshot
                if after.lifecycle_state is LifecycleState.FLAT:
                    self.lifecycle = None
            else:
                return
        self._notify_lifecycle_transition(before, after, at)

    async def _resubscribe_risex_orderbooks(
        self,
        ws: object,
        adapter: PublicAdapter,
        symbols: tuple[str, ...],
        *,
        triggering_symbol: str,
        stream_session_id: StreamSessionId,
    ) -> None:
        now = self.clock.now()
        stream_key = (Venue.RISEX, "*", "combined")
        if not self._owns_stream_session(stream_key, stream_session_id):
            return
        for symbol in symbols:
            if symbol != triggering_symbol:
                await self.mark_disconnected(
                    Venue.RISEX, symbol, at=now, stream_kind="book",
                    exception=ValueError("orderbook channel resubscribe"),
                    stream_session_id=stream_session_id,
                )
                if not self._owns_stream_session(stream_key, stream_session_id):
                    return
        for symbol in symbols:
            self._start_risex_recovery(symbol, stream_session_id)
        market_ids = [adapter.market_id(symbol) for symbol in symbols]  # type: ignore[attr-defined]
        self._record(
            "PUBLIC_BOOK_RESYNC_STARTED", at=now, venue=Venue.RISEX,
            detail={"symbols": list(symbols), "source": "WS_RESUBSCRIBE"},
        )
        if not self._owns_stream_session(stream_key, stream_session_id):
            return
        await ws.send_json(adapter.orderbook_unsubscription())  # type: ignore[attr-defined]
        if not self._owns_stream_session(stream_key, stream_session_id):
            return
        await ws.send_json(adapter.orderbook_subscription(market_ids))  # type: ignore[attr-defined]

    def _start_risex_recovery(
        self, symbol: str, session_id: StreamSessionId
    ) -> RecoveryEpisode:
        key = (Venue.RISEX, symbol)
        current = self._recoveries.get(key)
        if (
            current is not None
            and current.terminal is None
            and current.owned_stream_session_id == session_id
        ):
            return current
        if current is not None:
            self._retire_recovery_task(current.task)
        episode = self._new_recovery_episode(key, session_id)
        self._record_recovery_started(key, episode)
        return episode

    def _replace_displaced_combined_recoveries(
        self,
        venue: Venue,
        symbols: tuple[str, ...],
        stream_session_id: StreamSessionId,
    ) -> tuple[str, ...]:
        replaced: list[str] = []
        for symbol in symbols:
            key = (venue, symbol)
            current = self._recoveries.get(key)
            if current is None or current.terminal == "COMPLETE":
                continue
            if (
                current.terminal is None
                and current.owned_stream_session_id == stream_session_id
            ):
                continue
            self._retire_recovery_task(current.task)
            episode = self._new_recovery_episode(key, stream_session_id)
            self._record_recovery_started(key, episode)
            if venue is Venue.NADO:
                self._spawn_recovery_task(
                    key, episode,
                    self._recover_snapshot_in_background(
                        venue, symbol, episode
                    ),
                )
            replaced.append(symbol)
        return tuple(replaced)

    def _record_recovery_started(
        self, key: tuple[Venue, str], episode: RecoveryEpisode
    ) -> None:
        self._record(
            "PUBLIC_SNAPSHOT_RECOVERY_STARTED", venue=key[0],
            detail={
                "symbol": key[1],
                "episode_id": episode.episode_id.value,
                "generation": episode.attempt_generation.value,
                "stream_session_id": episode.owned_stream_session_id.value,
            },
        )

    def _start_snapshot_recovery(
        self,
        venue: Venue,
        symbol: str,
        *,
        displaced_stream_session_id: StreamSessionId,
    ) -> RecoveryEpisode:
        key = (venue, symbol)
        current = self._recoveries.get(key)
        if current is not None and current.terminal is None:
            return current
        session_id = (
            self._new_stream_session((venue, symbol, "book"))
            if venue is Venue.EXTENDED else displaced_stream_session_id
        )
        episode = self._new_recovery_episode(key, session_id)
        self._record_recovery_started(key, episode)
        operation = (
            self._restart_extended_book_stream(symbol, episode)
            if venue is Venue.EXTENDED
            else self._recover_snapshot_in_background(venue, symbol, episode)
        )
        self._spawn_recovery_task(key, episode, operation)
        return episode

    def _spawn_recovery_task(
        self,
        key: tuple[Venue, str],
        episode: RecoveryEpisode,
        operation: Awaitable[None],
    ) -> None:
        task = asyncio.create_task(operation)
        episode.task = task

        def release(done: asyncio.Task[None]) -> None:
            if self._recoveries.get(key) is episode and episode.task is done:
                episode.task = None
            self._retired_recovery_tasks.discard(done)

        task.add_done_callback(release)

    def _build_recovery_book(
        self, snapshot: OrderBook, episode: RecoveryEpisode
    ) -> tuple[OrderBook, int, int]:
        stream = BookStream(snapshot.venue, snapshot.canonical_market)
        stream.connected(self.clock.now())
        stream.snapshot(snapshot)
        buffered = len(episode.buffer)
        replayed = 0
        if snapshot.sequence is None:
            recovered = stream.book()
            if recovered is None:
                raise ValueError("snapshot recovery produced no book")
            return recovered, buffered, replayed
        for delta in episode.buffer:
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
        return recovered, buffered, replayed

    async def _restart_extended_book_stream(
        self, symbol: str, episode: RecoveryEpisode
    ) -> None:
        key = (Venue.EXTENDED, symbol)
        task_key = (Venue.EXTENDED, symbol, "book")
        generation = episode.attempt_generation
        if not self._owns_recovery(key, episode, generation):
            return
        self._record(
            "PUBLIC_BOOK_RESYNC_STARTED", venue=Venue.EXTENDED,
            detail={"symbol": symbol, "source": "WS_RECONNECT"},
        )
        current = self._stream_tasks.get(task_key)
        if current is not None and current is not asyncio.current_task():
            current.cancel()
            await asyncio.gather(current, return_exceptions=True)
        if not self._owns_recovery(key, episode, generation):
            return
        adapter = None if self.adapters is None else self.adapters.get(Venue.EXTENDED)
        if (
            isinstance(adapter, ExtendedAdapter)
            and self._stop_event is not None
            and not self._stop_event.is_set()
        ):
            self._stream_tasks[task_key] = asyncio.create_task(
                self._extended_stream(
                    adapter, symbol, "book", episode.owned_stream_session_id
                )
            )

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
        session_id = self._new_stream_session(key)
        self._stream_tasks[key] = asyncio.create_task(
            self._extended_stream(adapter, symbol, kind, session_id)
        )

    async def _restart_extended_stream(self, symbol: str, kind: str) -> None:
        key = (Venue.EXTENDED, symbol, kind)
        current = self._stream_tasks.get(key)
        if current is not None and current is not asyncio.current_task():
            self._stream_tasks.pop(key, None)
            self._stream_sessions.pop(key, None)
            self._start_extended_stream(symbol, kind)
            current.cancel()
            self._retired_stream_tasks.add(current)
            current.add_done_callback(self._retired_stream_task_done)
            await asyncio.sleep(0)
            return
        self._start_extended_stream(symbol, kind)

    async def _extended_heartbeat(
        self,
        ws: object,
        stream_key: tuple[Venue, str, str],
        stream_session_id: StreamSessionId,
    ) -> None:
        try:
            while self._stop_event is not None and not self._stop_event.is_set():
                await self._sleep(10)
                if (
                    self._stop_event.is_set()
                    or not self._owns_stream_session(
                        stream_key, stream_session_id
                    )
                ):
                    return
                await ws.ping()  # type: ignore[attr-defined]
                if not self._owns_stream_session(stream_key, stream_session_id):
                    return
        except asyncio.CancelledError:
            raise
        except Exception:
            close = getattr(ws, "close", None)
            if close is not None:
                await close()
            raise

    async def _recover_snapshot_in_background(
        self, venue: Venue, symbol: str, episode: RecoveryEpisode
    ) -> None:
        key = (venue, symbol)
        assert self.adapters is not None
        started = self.clock.now()
        generation = episode.attempt_generation
        for attempt in range(1, 4):
            try:
                if not self._owns_recovery(key, episode, generation):
                    return
                episode.attempts = attempt
                snapshot = await self._public_call(
                    venue,
                    lambda: self.adapters[venue].fetch_book(symbol),
                )
                if not self._owns_recovery(key, episode, generation):
                    return
                recovered, buffered, replayed = self._build_recovery_book(
                    snapshot, episode
                )
                if not self._owns_recovery(key, episode, generation):
                    return
                if not await self._publish_recovery_snapshot(
                    key, episode, generation, recovered,
                    at=self.clock.now(), buffered=buffered, replayed=replayed,
                    source="REST_SNAPSHOT",
                ):
                    return
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not self._owns_recovery(key, episode, generation):
                    return
                self._set_component_readiness(
                    venue, f"stream:{symbol}:book", False,
                    f"PUBLIC_SNAPSHOT_RECOVERY_FAILED:book:{type(exc).__name__}:"
                    f"{str(exc)[:120]}", self.clock.now(),
                )
                delay = min(2 ** (attempt - 1), 30)
                if attempt < 3:
                    await self._sleep(delay)
                    continue
                episode.terminal = "FAILED"
                self._record(
                    "PUBLIC_SNAPSHOT_RECOVERY_FAILED", venue=venue,
                    detail={
                        "symbol": symbol,
                        "episode_id": episode.episode_id.value,
                        "generation": episode.attempt_generation.value,
                        "exception_class": type(exc).__name__,
                        "attempts": attempt,
                        "retry_after_seconds": delay,
                        "elapsed_seconds": str(Decimal(str(
                            (self.clock.now() - started).total_seconds()
                        ))),
                    },
                )

    def _restart_recovery_attempt(
        self, key: tuple[Venue, str], episode: RecoveryEpisode
    ) -> None:
        if self._recoveries.get(key) is not episode or episode.terminal is not None:
            return
        self._retire_recovery_task(episode.task)
        episode.task = None
        episode.buffer.clear()
        self._recovery_attempt_number += 1
        episode.attempt_generation = RecoveryAttemptGeneration(
            self._recovery_attempt_number
        )
        self._spawn_recovery_task(
            key, episode,
            self._recover_snapshot_in_background(key[0], key[1], episode),
        )

    async def _extended_stream(
        self,
        adapter: ExtendedAdapter,
        symbol: str,
        kind: str,
        stream_session_id: StreamSessionId,
    ) -> None:
        assert self._session is not None
        task_key = (Venue.EXTENDED, symbol, kind)
        socket_identity = (Venue.EXTENDED, kind, (symbol,))
        url = {
            "book": adapter.orderbook_stream_url(symbol),
            "trade": adapter.trades_stream_url(symbol),
            "funding": adapter.funding_stream_url(symbol),
        }[kind]
        delay = 1
        while self._stop_event is not None and not self._stop_event.is_set():
            if not self._owns_stream_session(task_key, stream_session_id):
                return
            session_established = False
            try:
                async with self._session.ws_connect(url, heartbeat=None, autoping=False) as ws:
                    if not self._owns_stream_session(task_key, stream_session_id):
                        return
                    session_established = True
                    if kind == "book":
                        key = (Venue.EXTENDED, symbol)
                        current = self._recoveries.get(key)
                        if (
                            self.coordinator.stream(*key).book() is not None
                            or current is not None
                        ) and (
                            current is None
                            or current.terminal is not None
                            or current.owned_stream_session_id != stream_session_id
                        ):
                            if current is not None:
                                self._retire_recovery_task(current.task)
                            current = self._new_recovery_episode(
                                key, stream_session_id
                            )
                            self._record_recovery_started(key, current)
                    last_trade_sequence: int | None = None
                    trade_discontinuity_recorded = False
                    self._confirm_extended_stream(
                        symbol, kind, self.clock.now(), data_ready=False,
                        stream_session_id=stream_session_id,
                    )
                    self._socket_reconnected(
                        socket_identity, at=self.clock.now(),
                        stream_session_id=stream_session_id,
                    )
                    if not self._owns_stream_session(task_key, stream_session_id):
                        return
                    self._watchdog_restarted(
                        socket_identity, at=self.clock.now()
                    )
                    ordinal = 0
                    heartbeat = asyncio.create_task(self._extended_heartbeat(
                        ws, task_key, stream_session_id
                    ))
                    try:
                        async for message in ws:
                            if not self._owns_stream_session(
                                task_key, stream_session_id
                            ):
                                return
                            if message.type is aiohttp.WSMsgType.PING:
                                await ws.pong(message.data)
                                if not self._owns_stream_session(
                                    task_key, stream_session_id
                                ):
                                    return
                                delay = 1
                                self._confirm_extended_stream(
                                    symbol, kind, self.clock.now(), data_ready=False,
                                    stream_session_id=stream_session_id,
                                )
                                continue
                            if message.type is aiohttp.WSMsgType.PONG:
                                delay = 1
                                self._confirm_extended_stream(
                                    symbol, kind, self.clock.now(), data_ready=False,
                                    stream_session_id=stream_session_id,
                                )
                                continue
                            if message.type in {
                                aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING,
                                aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR,
                            }:
                                raise _PublicSocketClosed(message.type.name)
                            if message.type is not aiohttp.WSMsgType.TEXT:
                                continue
                            payload = json.loads(message.data, parse_float=Decimal)
                            if kind == "book":
                                received_at = self.clock.now()
                                healthy = await self.apply_book_event(
                                    adapter.normalize_book_message(
                                        payload, received_at=received_at
                                    ),
                                    stream_session_id=stream_session_id,
                                )
                                if not self._owns_stream_session(
                                    task_key, stream_session_id
                                ):
                                    return
                                delay = 1
                                if healthy:
                                    self._confirm_extended_stream(
                                        symbol, kind, self.clock.now(), data_ready=True,
                                        stream_session_id=stream_session_id,
                                    )
                            elif kind == "trade":
                                received_at = self.clock.now()
                                sequence, trades = adapter.normalize_trade_message(
                                    payload, received_at=received_at,
                                    session_id=str(id(ws)), starting_ordinal=ordinal,
                                )
                                ordinal += len(trades)
                                previous = last_trade_sequence
                                if previous is not None and sequence != previous + 1:
                                    if not trade_discontinuity_recorded:
                                        forward = sequence > previous + 1
                                        self._record(
                                            "PUBLIC_TRADE_SEQUENCE_DISCONTINUITY",
                                            at=received_at,
                                            venue=Venue.EXTENDED,
                                            detail={
                                                "action": (
                                                    "ACCEPT_MONOTONIC" if forward
                                                    else "IGNORE_NON_MONOTONIC"
                                                ),
                                                "classification": (
                                                    "FORWARD_GAP" if forward else (
                                                        "DUPLICATE" if sequence == previous
                                                        else "OUT_OF_ORDER"
                                                    )
                                                ),
                                                "current_sequence": sequence,
                                                "previous_sequence": previous,
                                                "stream": "trade",
                                                "symbol": symbol,
                                            },
                                        )
                                        trade_discontinuity_recorded = True
                                    if sequence <= previous:
                                        delay = 1
                                        self._confirm_extended_stream(
                                            symbol, kind, received_at, data_ready=True,
                                            stream_session_id=stream_session_id,
                                        )
                                        continue
                                last_trade_sequence = sequence
                                delay = 1
                                self._confirm_extended_stream(
                                    symbol, kind, received_at, data_ready=True,
                                    stream_session_id=stream_session_id,
                                )
                                for trade in trades:
                                    if not self._owns_stream_session(
                                        task_key, stream_session_id
                                    ):
                                        return
                                    await self.deliver_trade(trade)
                            else:
                                row = self.observations.get((Venue.EXTENDED, symbol))
                                if row is not None:
                                    settlement = adapter.normalize_applied_funding_message(
                                        payload, row.market,
                                    )
                                    delay = 1
                                    self._confirm_extended_stream(
                                        symbol, kind, self.clock.now(), data_ready=True,
                                        stream_session_id=stream_session_id,
                                    )
                                    if settlement is not None:
                                        if not self._owns_stream_session(
                                            task_key, stream_session_id
                                        ):
                                            return
                                        await self._apply_extended_funding_record(settlement)
                    finally:
                        heartbeat.cancel()
                        await asyncio.gather(heartbeat, return_exceptions=True)
                    if self._stop_event is None or not self._stop_event.is_set():
                        raise _PublicSocketClosed("EOF")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self._stop_event is not None and self._stop_event.is_set():
                    break
                if (
                    not self._owns_stream_session(task_key, stream_session_id)
                    or (
                        self._stream_tasks.get(task_key) is not None
                        and self._stream_tasks.get(task_key)
                        is not asyncio.current_task()
                    )
                ):
                    return
                if session_established:
                    self._record_transport_disconnect(
                        socket_identity, at=self.clock.now(), exception=exc,
                        stream_session_id=stream_session_id,
                    )
                await self.mark_disconnected(
                    Venue.EXTENDED, symbol, stream_kind=kind, exception=exc,
                    stream_session_id=stream_session_id,
                )
                if not self._owns_stream_session(task_key, stream_session_id):
                    return
                await self._sleep(delay)
                if not self._owns_stream_session(task_key, stream_session_id):
                    return
                delay = min(delay * 2, 30)
                stream_session_id = self._new_stream_session(task_key)

    async def _combined_stream(
        self,
        venue: Venue,
        adapter: PublicAdapter,
        symbols: tuple[str, ...],
        stream_session_id: StreamSessionId,
    ) -> None:
        assert self._session is not None
        task_key = (venue, "*", "combined")
        ordered_symbols = tuple(sorted(symbols))
        socket_identity = (venue, "combined", ordered_symbols)
        delay = 1
        if not symbols:
            return
        while self._stop_event is not None and not self._stop_event.is_set():
            if not self._owns_stream_session(task_key, stream_session_id):
                return
            session_established = False
            try:
                async with self._session.ws_connect(
                    adapter.ws_base, heartbeat=10, autoping=False, compress=15
                ) as ws:
                    if not self._owns_stream_session(task_key, stream_session_id):
                        return
                    session_established = True
                    replaced_recoveries = (
                        self._replace_displaced_combined_recoveries(
                            venue, symbols, stream_session_id
                        )
                    )
                    if venue is Venue.RISEX:
                        ids = [adapter.market_id(symbol) for symbol in symbols]  # type: ignore[attr-defined]
                        await ws.send_json(adapter.orderbook_subscription(ids))  # type: ignore[attr-defined]
                        if not self._owns_stream_session(task_key, stream_session_id):
                            return
                        await ws.send_json(adapter.trades_subscription(ids))  # type: ignore[attr-defined]
                    else:
                        for symbol in symbols:
                            product = adapter.product_id(symbol)  # type: ignore[attr-defined]
                            for kind in ("book_depth", "trade", "funding_rate", "funding_payment"):
                                await ws.send_json(adapter.subscription(kind, product))  # type: ignore[attr-defined]
                                if not self._owns_stream_session(
                                    task_key, stream_session_id
                                ):
                                    return
                    self._socket_reconnected(
                        socket_identity, at=self.clock.now(),
                        stream_session_id=stream_session_id,
                    )
                    if not self._owns_stream_session(task_key, stream_session_id):
                        return
                    risex_resync = (
                        venue is Venue.RISEX
                        and bool(replaced_recoveries)
                    )
                    if risex_resync:
                        await self._resubscribe_risex_orderbooks(
                            ws, adapter, symbols,
                            triggering_symbol=replaced_recoveries[0],
                            stream_session_id=stream_session_id,
                        )
                    for symbol in symbols:
                        if not self._owns_stream_session(task_key, stream_session_id):
                            return
                        stream = self.coordinator.stream(venue, symbol)
                        stream.connected(self.clock.now())
                        book_ready = False
                        if risex_resync:
                            book_ready = False
                        elif (
                            venue is Venue.NADO
                            and (episode := self._recoveries.get((venue, symbol)))
                            is not None
                            and episode.terminal is None
                            and episode.owned_stream_session_id
                            == stream_session_id
                        ):
                            if episode.task is not None:
                                await episode.task
                            if not self._owns_stream_session(
                                task_key, stream_session_id
                            ):
                                return
                            book_ready = episode.terminal == "COMPLETE"
                        else:
                            snapshot = await self._public_call(
                                venue,
                                lambda: adapter.fetch_book(symbol),
                            )
                            if not self._owns_stream_session(
                                task_key, stream_session_id
                            ):
                                return
                            book_ready = await self.apply_book_event(
                                snapshot, stream_session_id=stream_session_id
                            )
                        if book_ready:
                            self.mark_trade_stream_connected(venue, symbol)
                            for component in (
                                "book", "trade", "funding",
                                "connection_combined",
                            ):
                                self._set_component_readiness(
                                    venue, f"{component}:{symbol}", True,
                                    "PUBLIC_STREAM_CONNECTED", self.clock.now(),
                                )
                    delay = 1
                    ordinal = 0
                    async for message in ws:
                        if not self._owns_stream_session(
                            task_key, stream_session_id
                        ):
                            return
                        if message.type is aiohttp.WSMsgType.PING:
                            await ws.pong(message.data)
                            if not self._owns_stream_session(
                                task_key, stream_session_id
                            ):
                                return
                            for symbol in symbols:
                                self.coordinator.stream(venue, symbol).connection_confirmed(self.clock.now())
                            continue
                        if message.type is aiohttp.WSMsgType.PONG:
                            for symbol in symbols:
                                self.coordinator.stream(venue, symbol).connection_confirmed(self.clock.now())
                            continue
                        if message.type in {
                            aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING,
                            aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR,
                        }:
                            raise _PublicSocketClosed(message.type.name)
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
                            received_at = self.clock.now()
                            event = (
                                adapter.normalize_book_message(
                                    payload, received_at=received_at
                                )
                                if isinstance(adapter, (NadoAdapter, RisexAdapter))
                                else adapter.normalize_book_message(payload)
                            )  # type: ignore[attr-defined]
                            healthy = await self.apply_book_event(
                                event, stream_session_id=stream_session_id
                            )
                            if not self._owns_stream_session(
                                task_key, stream_session_id
                            ):
                                return
                            episode = self._recoveries.get(
                                (venue, event.canonical_market)
                            )
                            if (
                                venue is Venue.RISEX and not healthy
                                and (episode is None or episode.terminal is not None)
                            ):
                                await self._resubscribe_risex_orderbooks(
                                    ws, adapter, symbols,
                                    triggering_symbol=event.canonical_market,
                                    stream_session_id=stream_session_id,
                                )
                            elif healthy:
                                self.mark_trade_stream_connected(
                                    venue, event.canonical_market
                                )
                                for component in (
                                    "book", "trade", "funding",
                                    "connection_combined",
                                ):
                                    self._set_component_readiness(
                                        venue,
                                        f"{component}:{event.canonical_market}",
                                        True, "PUBLIC_STREAM_CONNECTED",
                                        self.clock.now(),
                                    )
                        elif "trade" in kind:
                            if not self._owns_stream_session(
                                task_key, stream_session_id
                            ):
                                return
                            await self.deliver_trade(
                                adapter.normalize_trade(
                                    payload,
                                    received_at=self.clock.now(),
                                    session_id=str(id(ws)),
                                    ordinal=ordinal,
                                )
                            )
                            if not self._owns_stream_session(
                                task_key, stream_session_id
                            ):
                                return
                        elif venue is Venue.NADO and "funding_rate" in kind:
                            product = int(payload["product_id"])
                            symbol = adapter.symbol_for_product(product)  # type: ignore[attr-defined]
                            row = self.observations.get((venue, symbol))
                            mid = self._book_mid(venue, symbol)
                            if row is not None:
                                received_at = self.clock.now()
                                quote = adapter.normalize_funding_rate_message(  # type: ignore[attr-defined]
                                    payload,
                                    row.market,
                                    index_price_x18=(
                                        None
                                        if mid is None
                                        else str(mid * Decimal("1000000000000000000"))
                                    ),
                                    received_at=received_at,
                                    assumed_open_at=received_at,
                                )
                                if not self._owns_stream_session(
                                    task_key, stream_session_id
                                ):
                                    return
                                await self._apply_nado_funding_quote(
                                    quote, stream_session_id, ()
                                )
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
                                if not self._owns_stream_session(
                                    task_key, stream_session_id
                                ):
                                    return
                                await self._apply_nado_funding_quote(
                                    quote,
                                    stream_session_id,
                                    (
                                        (
                                            (symbol, "long"),
                                            payload.get(
                                                "cumulative_funding_long_x18"
                                            ),
                                        ),
                                        (
                                            (symbol, "short"),
                                            payload.get(
                                                "cumulative_funding_short_x18"
                                            ),
                                        ),
                                    ),
                                )
                    if self._stop_event is None or not self._stop_event.is_set():
                        raise _PublicSocketClosed("EOF")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self._stop_event is not None and self._stop_event.is_set():
                    break
                if (
                    not self._owns_stream_session(task_key, stream_session_id)
                    or (
                        self._stream_tasks.get(task_key) is not None
                        and self._stream_tasks.get(task_key)
                        is not asyncio.current_task()
                    )
                ):
                    return
                if session_established:
                    self._record_transport_disconnect(
                        socket_identity, at=self.clock.now(), exception=exc,
                        stream_session_id=stream_session_id,
                    )
                for symbol in symbols:
                    await self.mark_disconnected(
                        venue, symbol, stream_kind="combined", exception=exc,
                        stream_session_id=stream_session_id,
                    )
                if not self._owns_stream_session(task_key, stream_session_id):
                    return
                await self._sleep(delay)
                if not self._owns_stream_session(task_key, stream_session_id):
                    return
                delay = min(delay * 2, 30)
                stream_session_id = self._new_stream_session(task_key)

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
            changed_symbols = set(current) | set(wanted)
            now = self.clock.now()
            for symbol in changed_symbols:
                if self._catalog_reconciliation_in_progress:
                    self._trade_stream_ready.discard((venue, symbol))
                    self._live_book_ready.discard((venue, symbol))
                if symbol not in wanted:
                    self.observations.pop((venue, symbol), None)
                    continue
                if self._catalog_reconciliation_in_progress:
                    self.coordinator.stream(venue, symbol).gap()
                    for component in (
                        "book", "trade", "funding", "connection_combined"
                    ):
                        self._set_component_readiness(
                            venue, f"{component}:{symbol}", False,
                            "PUBLIC_STREAM_RECONCILIATION_PENDING", now,
                        )
            self._combined_symbols[venue] = wanted
            self._remove_obsolete_components(venue, set(wanted), self.clock.now())
            if wanted:
                stream_session_id = self._new_stream_session(key)
                self._stream_tasks[key] = asyncio.create_task(
                    self._combined_stream(
                        venue, adapter, wanted, stream_session_id
                    )
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
        current: set[tuple[Venue, str, str]] = set()
        for key, task in tuple(self._stream_tasks.items()):
            if key[0] is not Venue.EXTENDED:
                continue
            if task.done():
                self._stream_tasks.pop(key, None)
            else:
                current.add(key)
        self._remove_obsolete_components(
            Venue.EXTENDED, {key[1] for key in wanted}, self.clock.now()
        )
        for key in sorted(wanted - current, key=lambda row: (row[1], row[2])):
            _, symbol, kind = key
            self._live_book_ready.discard((Venue.EXTENDED, symbol))
            if kind == "trade":
                self._trade_stream_ready.discard((Venue.EXTENDED, symbol))
            data_component = "applied_funding" if kind == "funding" else kind
            self._set_component_readiness(
                Venue.EXTENDED, f"{data_component}:{symbol}", False,
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
            self._live_book_ready.discard((Venue.EXTENDED, key[1]))
            if key[2] == "trade":
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
        wanted_symbols = {key[1] for key in wanted}
        for symbol in {
            key[1] for key in removed
        } - wanted_symbols:
            self.observations.pop((Venue.EXTENDED, symbol), None)

    async def _pause_or_stop(self, seconds: float) -> None:
        assert self._stop_event is not None
        sleep_task = asyncio.create_task(self._sleep(seconds))
        stop_task = asyncio.create_task(self._stop_event.wait())
        waiters: list[asyncio.Future[Any]] = [sleep_task, stop_task]
        refresh = self._refresh_task
        if self._pending_full_scan_at is not None and refresh is not None:
            waiters.append(asyncio.ensure_future(asyncio.shield(refresh)))
        try:
            await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in waiters:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*waiters, return_exceptions=True)

    async def _tick_or_stop(self) -> bool:
        """Run one tick while allowing an external stop to cancel its awaits."""
        assert self._stop_event is not None
        tick_task = asyncio.create_task(self.tick())
        self._tick_task = tick_task
        stop_task = asyncio.create_task(self._stop_event.wait())
        handoff_to_shutdown = False
        try:
            done, _ = await asyncio.wait(
                (tick_task, stop_task), return_when=asyncio.FIRST_COMPLETED
            )
            if tick_task in done:
                await tick_task
                return False
            tick_task.cancel()
            # Let shutdown close the transport before awaiting this owner.
            handoff_to_shutdown = True
            return True
        except asyncio.CancelledError:
            tick_task.cancel()
            raise
        finally:
            stop_task.cancel()
            await asyncio.gather(stop_task, return_exceptions=True)
            if (
                self._tick_task is tick_task
                and tick_task.done()
                and not handoff_to_shutdown
            ):
                self._tick_task = None

    def _next_wakeup_at(self, now: datetime) -> datetime:
        if self.broker is None:
            self._refresh_focused_cycle(now)
        deadlines = [value for value in (
            self.next_full_scan_at, self.next_focused_scan_at,
            self.next_position_monitor_at, self.next_health_check_at,
            self.next_extended_catalog_at,
            self._pending_full_deadline_at,
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

    async def _await_startup_or_stop(
        self, operation: Awaitable[Any]
    ) -> tuple[bool, Any]:
        """Make every startup public-data phase interruptible by intentional stop."""
        assert self._stop_event is not None
        operation_task = asyncio.ensure_future(operation)
        stop_task = asyncio.create_task(self._stop_event.wait())
        try:
            done, _ = await asyncio.wait(
                (operation_task, stop_task),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if operation_task in done:
                result = await operation_task
                return self._stop_event.is_set(), result
            operation_task.cancel()
            await asyncio.gather(operation_task, return_exceptions=True)
            return True, None
        except asyncio.CancelledError:
            operation_task.cancel()
            await asyncio.gather(operation_task, return_exceptions=True)
            raise
        finally:
            stop_task.cancel()
            await asyncio.gather(stop_task, return_exceptions=True)

    async def _startup_phase(
        self, operation: Awaitable[Any], *, stop_cause: str
    ) -> bool:
        stopped, _ = await self._await_startup_or_stop(operation)
        if not stopped:
            return False
        if self._stop_cause is None:
            self._request_stop(stop_cause)
        return True

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
            startup_stop_cause = (
                "STOP_EVENT" if stop_event is not None
                else "UNKNOWN_EXTERNAL_STOP"
            )
            if await self._startup_phase(
                self.scan(), stop_cause=startup_stop_cause
            ):
                return {"status": "STOPPED_SAFE", "forced_close": False}
            self.next_extended_catalog_at = started_at + timedelta(
                seconds=self.config.extended_universe_refresh_seconds
            )
            assert self.last_scan is not None
            if await self._startup_phase(
                self._restore(self.last_scan.logical_at),
                stop_cause=startup_stop_cause,
            ):
                return {"status": "STOPPED_SAFE", "forced_close": False}
            self.next_full_scan_at = self.last_scan.logical_at + timedelta(
                seconds=self.config.normal_scan_seconds
            )
            self.next_health_check_at = self.last_scan.logical_at + timedelta(seconds=10)
            if await self._startup_phase(
                self.start_streams(), stop_cause=startup_stop_cause
            ):
                return {"status": "STOPPED_SAFE", "forced_close": False}
            startup_ready = self._try_mark_startup_ready()
            if not startup_ready:
                self._record(
                    "PAPER_RUN_NOT_READY",
                    at=self.clock.now(),
                    detail=self._startup_gate_detail(self.clock.now()),
                )
            # Initial REST observations can predate completion of a slow
            # bootstrap. Seed the existing single-flight refresh now so the
            # first 120-second FULL scan does not inherit that bootstrap age.
            self._start_public_refresh()
            self._start_background_catalog_refresh(
                include_extended_universe=not startup_ready
            )
            while not self._stop_event.is_set():
                if await self._tick_or_stop():
                    break
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
                    "RUNTIME_FATAL", at=self._requested_at,
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
            self._requested_at = self.clock.now()
        if self._stop_event is not None:
            self._stop_event.set()

    def _detach_tasks(
        self, tasks: set[asyncio.Future[Any]] | tuple[asyncio.Future[Any], ...]
    ) -> None:
        for task in tasks:
            if task.done():
                _consume_future_result(task)
                continue
            self._detached_tasks.add(task)
            task.add_done_callback(self._detached_tasks.discard)
            task.add_done_callback(_consume_future_result)

    async def _close_session_bounded(self) -> None:
        session = self._session
        close = getattr(session, "close", None)
        if session is None or close is None or getattr(session, "closed", False):
            return
        try:
            operation = close()
        except BaseException:
            return
        if operation is None:
            return
        task = asyncio.ensure_future(operation)
        done, pending = await asyncio.wait(
            (task,), timeout=CANCELLATION_DRAIN_TIMEOUT_SECONDS
        )
        if pending:
            task.cancel()
            done_after_cancel, still_pending = await asyncio.wait(
                pending, timeout=0
            )
            for completed in done_after_cancel:
                _consume_future_result(completed)
            self._detach_tasks(still_pending)
        else:
            for completed in done:
                _consume_future_result(completed)
        # Give a cancellation-resistant tick owner one scheduling turn after
        # close has been requested, preserving the handoff boundary before the
        # final ownership cleanup below.
        await asyncio.sleep(0)

    def _has_owned_background_work(self) -> bool:
        return bool(
            self._stream_tasks
            or self._retired_stream_tasks
            or self._refresh_task is not None
            or self._recoveries
            or self._retired_recovery_tasks
            or self._extended_health_recovery_task is not None
            or self._extended_health_recovery_requests
            or self._extended_universe_task is not None
            or self._tick_task is not None
        )

    def _owned_background_tasks(self) -> set[asyncio.Task[None]]:
        owned = set(self._stream_tasks.values())
        owned.update(self._retired_stream_tasks)
        if self._refresh_task is not None:
            owned.add(self._refresh_task)
        owned.update(
            episode.task for episode in self._recoveries.values()
            if episode.task is not None
        )
        owned.update(self._retired_recovery_tasks)
        if self._extended_health_recovery_task is not None:
            owned.add(self._extended_health_recovery_task)
        if self._extended_universe_task is not None:
            owned.add(self._extended_universe_task)
        if self._tick_task is not None:
            owned.add(self._tick_task)
        return owned

    async def _cancel_owned_background_tasks(self) -> None:
        owned = self._owned_background_tasks()
        if not owned:
            return
        pending = await _cancel_tasks_bounded(owned)
        self._detach_tasks(pending)

    async def shutdown(self) -> None:
        if not self.accepting_entries and not self._has_owned_background_work():
            return
        self.accepting_entries = False
        at = self.clock.now()
        if self._stop_cause is None:
            self._request_stop("UNKNOWN_EXTERNAL_STOP")
        self._shutdown_started = True
        if self._stop_event is not None:
            self._stop_event.set()
        for task in self._owned_background_tasks():
            if not task.done():
                task.cancel()
        await self._close_session_bounded()
        if self.broker is not None and self.broker.state.lifecycle_state is LifecycleState.ENTRY_MAKER_OPEN:
            state = await self.broker.cancel_for_process_restart(restarted_at=at)
            self.repository.save_decision(recorded_at=at, entry_state=state)
        await self._cancel_owned_background_tasks()
        at = self.clock.now()
        stop_detail: dict[str, object] = {
            "forced_close": False,
            "open_position_preserved": self.lifecycle is not None,
            "stop_cause": self._stop_cause,
            "requested_at": (
                None if self._requested_at is None
                else self._requested_at.isoformat()
            ),
        }
        if self._stop_cause in {"SIGINT", "SIGTERM"}:
            stop_detail["signal"] = self._stop_cause
        self._record(
            (
                "RUNTIME_STOPPED_FATAL"
                if self._stop_cause == "RUNTIME_FATAL" else "STOPPED_SAFE"
            ),
            at=at,
            detail=stop_detail,
        )
        if self._notification_run_id is not None and self._stop_cause != "RUNTIME_FATAL":
            self._notify_event(
                f"{self._notification_run_id}:stopped", "SAFE_STOP", at,
                "Paper runtime stopped safely",
            )
        self._stream_tasks.clear()
        self._retired_stream_tasks.clear()
        self._stream_sessions.clear()
        self._recoveries.clear()
        self._retired_recovery_tasks.clear()
        self._extended_health_recovery_requests.clear()
        self._extended_health_recovery_task = None
        self._pending_socket_episodes.clear()
        self._pending_watchdog_episodes.clear()
        self._refresh_task = None
        self._pending_full_scan_at = None
        self._pending_full_deadline_at = None
        self._catalog_refresh_pending = False
        self._extended_universe_task = None
        self._tick_task = None

    async def close(self) -> None:
        if self.accepting_entries or self._has_owned_background_work():
            await self.shutdown()
        if not self._shutdown_started:
            await self._close_session_bounded()


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
