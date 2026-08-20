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
from .exchanges.base import PublicAdapter
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
    Venue,
)
from .paper_broker import PaperEntryBroker, PaperEntryState
from .scanner import MarketObservation, RoutePlan, ScanSnapshot, activation_schedule, scan_once
from .storage import PaperRepository


class Clock(Protocol):
    def now(self) -> datetime: ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


def _public_session() -> aiohttp.ClientSession:
    return aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))


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


def _route_row(plan: RoutePlan) -> dict[str, object]:
    return {
        "canonical_asset": plan.canonical_asset,
        "hedge_venue": plan.hedge_venue.value,
        "direction": plan.direction.value,
        "entry_allowed": plan.entry_allowed,
        "blockers": list(plan.no_trade_reasons),
        "route_liquidity_usd": (
            None if plan.route is None else str(plan.route.route_liquidity_usd)
        ),
        "target_cycle_start": (
            None if plan.target_cycle is None else plan.target_cycle.start_at.isoformat()
        ),
        "canonical_quantity": (
            None if plan.canonical_quantity is None else str(plan.canonical_quantity)
        ),
        "expected_funding_usd": (
            None
            if plan.expected_target_cycle_funding_usd is None
            else str(plan.expected_target_cycle_funding_usd)
        ),
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
            "risex_funding": (
                "UNKNOWN"
                if plan.target_cycle is None
                else "PAPER_ASSUMPTION_OR_OFFICIAL_APPLIED"
            ),
            "hedge_funding": "OFFICIAL_PUBLIC_OR_UNKNOWN",
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
        self._stream_tasks: list[asyncio.Task[None]] = []
        self._stop_event: asyncio.Event | None = None
        self._attempt_number = 0
        self._nado_cumulative_funding: dict[tuple[str, str], object] = {}
        self._trade_stream_ready: set[tuple[Venue, str]] = set()
        self._last_readiness_evidence_at: dict[Venue, datetime] = {}

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

    async def _catalog(self, venue: Venue, adapter: PublicAdapter) -> None:
        at = self.clock.now()
        try:
            markets, volumes = await asyncio.gather(
                adapter.fetch_markets(), adapter.fetch_volumes()  # type: ignore[attr-defined]
            )
            self.markets[venue] = tuple(markets)
            for volume in volumes:
                self.volumes[(venue, volume.canonical_market)] = volume
            self._set_readiness(venue, True, "PUBLIC_REST_READY", self.clock.now())
        except Exception as exc:
            self.markets[venue] = ()
            self._set_readiness(
                venue, False, f"PUBLIC_REST_UNAVAILABLE:{type(exc).__name__}", at
            )

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
            and market.contract_type is ContractType.LINEAR
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
            by_venue_asset[(venue, asset)]
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

    async def _market_observation(self, market: Any, assumed_at: datetime) -> None:
        assert self.adapters is not None
        adapter = self.adapters[market.venue]
        key = (market.venue, market.venue_symbol)
        volume = self.volumes.get(key)
        try:
            book, funding = await asyncio.gather(
                adapter.fetch_book(market.venue_symbol),
                adapter.fetch_funding_quote(market, assumed_open_at=assumed_at),
            )
            logical_at = self.clock.now()
            funding = _quote_for_open_time(funding, logical_at)
            stream = self.coordinator.stream(market.venue, market.venue_symbol)
            stream.connected(logical_at)
            stream.snapshot(book)
            stream.connection_confirmed(logical_at)
            self.observations[key] = MarketObservation(
                market, volume, stream.book(), funding, stream.health(logical_at)
            )
        except Exception as exc:
            logical_at = self.clock.now()
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
            self._set_readiness(
                market.venue,
                False,
                f"PUBLIC_MARKET_UNAVAILABLE:{type(exc).__name__}",
                logical_at,
            )

    async def scan(self) -> dict[str, object]:
        if self.adapters is None:
            raise RuntimeError("runtime must be entered before scanning")
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
            funding = observation.funding
            if funding is not None:
                funding = _quote_for_open_time(funding, logical_at)
            health = observation.health
            if health is not None and health.last_connection_confirmation_at is not None:
                health = replace(health, last_connection_confirmation_at=logical_at)
            normalized.append(replace(observation, funding=funding, health=health))
        snapshot = await scan_once(normalized, logical_at, config=self.config)
        self.last_scan = snapshot
        self.repository.save_decision(
            recorded_at=logical_at,
            scan_snapshot=snapshot,
            funding_quotes=tuple(
                row.funding for row in normalized if row.funding is not None
            ),
        )
        self._record(
            "PUBLIC_SCAN",
            at=logical_at,
            detail={
                "evaluation_count": len(snapshot.evaluations),
                "eligible_count": sum(plan.entry_allowed for plan in snapshot.evaluations),
                "assumption_flags": _assumption_flags(),
            },
        )
        rows = sorted(
            snapshot.evaluations,
            key=lambda plan: (
                not plan.entry_allowed,
                plan.planned_maker_net_pnl_usd is None,
                -(
                    plan.planned_maker_net_pnl_usd
                    if plan.planned_maker_net_pnl_usd is not None
                    else Decimal("0")
                ),
                plan.canonical_asset,
                plan.hedge_venue.value,
                plan.direction.value,
            ),
        )[:15]
        unavailable = {
            venue.value: state.detail
            for venue, state in self.readiness.items()
            if not state.available
        }
        return {
            "status": "OPPORTUNITY" if snapshot.winner is not None else "NO_TRADE",
            "reason": (
                None
                if snapshot.winner is not None
                else ("VENUE_SPECIFIC_BLOCKERS" if unavailable else "NO_ELIGIBLE_ROUTE")
            ),
            "eligible_count": sum(plan.entry_allowed for plan in snapshot.evaluations),
            "winner": None if snapshot.winner is None else snapshot.winner.canonical_asset,
            "routes": [_route_row(plan) for plan in rows],
            "venue_readiness": {
                venue.value: {
                    "available": state.available,
                    "detail": state.detail,
                    "updated_at": state.updated_at.isoformat(),
                }
                for venue, state in self.readiness.items()
            },
            "assumption_flags": _assumption_flags(),
        }

    def _observation(self, venue: Venue, symbol: str, at: datetime) -> MarketObservation:
        row = self.observations[(venue, symbol)]
        stream = self.coordinator.stream(venue, symbol)
        return replace(row, book=stream.book(), health=stream.health(at))

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
        if self.last_scan is None or self.next_full_scan_at is None or now >= self.next_full_scan_at:
            await self.scan()
            now = self.last_scan.logical_at
            self.next_full_scan_at = now + timedelta(seconds=self.config.normal_scan_seconds)
        if self.lifecycle is not None:
            if (
                self.next_position_monitor_at is None
                or now >= self.next_position_monitor_at
            ):
                risex, hedge = self._market_pair_observations(
                    self.lifecycle.snapshot.risex_market,
                    self.lifecycle.snapshot.hedge_market,
                    now,
                )
                await self.lifecycle.evaluate(
                    evaluated_at=now,
                    risex_observation=risex,
                    hedge_observation=hedge,
                )
                self.repository.save_decision(
                    recorded_at=now, lifecycle_snapshot=self.lifecycle.snapshot
                )
                if self.lifecycle.snapshot.lifecycle_state is LifecycleState.FLAT:
                    self.lifecycle = None
                self.next_position_monitor_at = now + timedelta(
                    seconds=self.config.open_position_monitor_seconds
                )
            return
        if self.broker is not None and self.broker.state.lifecycle_state is LifecycleState.ENTRY_MAKER_OPEN:
            order = self.broker.state.order
            assert order is not None
            if now >= order.cutoff_at or self.next_focused_scan_at is None or now >= self.next_focused_scan_at:
                await self.scan()
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
                self.next_focused_scan_at = now + timedelta(
                    seconds=self.config.focused_scan_seconds
                )
            return
        if not self.accepting_entries or self.last_scan is None or self.last_scan.winner is None:
            return
        plan = self.last_scan.winner
        assert plan.target_cycle is not None
        schedule = activation_schedule(plan.target_cycle)
        focused_start = plan.target_cycle.start_at - timedelta(
            seconds=self.config.focused_window_seconds
        )
        if now >= focused_start and (
            self.next_focused_scan_at is None or now >= self.next_focused_scan_at
        ):
            await self.scan()
            now = self.last_scan.logical_at
            plan = self.last_scan.winner
            self.next_focused_scan_at = now + timedelta(
                seconds=self.config.focused_scan_seconds
            )
            if plan is None or plan.target_cycle is None:
                return
            schedule = activation_schedule(plan.target_cycle)
        if schedule.should_activate(now):
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
            if self.lifecycle.snapshot.lifecycle_state is LifecycleState.FLAT:
                self.lifecycle = None

    def mark_trade_stream_connected(
        self, venue: Venue, symbol: str, *, at: datetime | None = None
    ) -> None:
        now = at or self.clock.now()
        self._trade_stream_ready.add((venue, symbol))
        stream = self.coordinator.stream(venue, symbol)
        stream.connected(now)
        stream.connection_confirmed(now)

    async def deliver_settlement(self, settlement: FundingSettlement) -> None:
        self.repository.upsert_settlement(settlement)
        if self.lifecycle is not None and settlement.key in {
            row.key for row in self.lifecycle.snapshot.settlements
        }:
            await self.lifecycle.reconcile_settlement(settlement)
            self.repository.save_decision(
                recorded_at=self.clock.now(), lifecycle_snapshot=self.lifecycle.snapshot
            )

    def _book_mid(self, venue: Venue, symbol: str) -> Decimal | None:
        book = self.coordinator.stream(venue, symbol).book()
        if book is None or not book.bids or not book.asks:
            return None
        bid = max(level.canonical_price for level in book.bids)
        ask = min(level.canonical_price for level in book.asks)
        return (bid + ask) / Decimal("2")

    async def _apply_funding_quote(self, quote: FundingCashQuote) -> None:
        key = (quote.venue, quote.canonical_market)
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
        self, venue: Venue, symbol: str, *, at: datetime | None = None
    ) -> None:
        now = at or self.clock.now()
        self._trade_stream_ready.discard((venue, symbol))
        self.coordinator.stream(venue, symbol).disconnected()
        self._set_readiness(venue, False, "PUBLIC_STREAM_DISCONNECTED", now)
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

    async def recover_snapshot(self, book: OrderBook, *, at: datetime | None = None) -> None:
        now = at or self.clock.now()
        stream = self.coordinator.stream(book.venue, book.canonical_market)
        stream.connected(now)
        stream.snapshot(book)
        stream.connection_confirmed(now)
        self._set_readiness(book.venue, True, "PUBLIC_STREAM_RECOVERED", now)
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

    async def apply_book_event(self, event: OrderBook | BookDelta) -> None:
        now = self.clock.now()
        self.coordinator.stream(event.venue, event.canonical_market).connection_confirmed(now)
        if isinstance(event, OrderBook):
            await self.recover_snapshot(event, at=now)
            return
        stream = self.coordinator.stream(event.venue, event.canonical_market)
        if not stream.apply_delta(event):
            await self.mark_disconnected(event.venue, event.canonical_market, at=now)
            assert self.adapters is not None
            try:
                snapshot = await self.adapters[event.venue].fetch_book(
                    event.canonical_market
                )
                await self.recover_snapshot(snapshot, at=self.clock.now())
            except (aiohttp.ClientError, TimeoutError, ValueError, KeyError):
                return
        elif self.lifecycle is not None:
            self.next_position_monitor_at = now
            await self.tick(now)

    async def _extended_stream(
        self, adapter: ExtendedAdapter, symbol: str, kind: str
    ) -> None:
        assert self._session is not None
        url = {
            "book": adapter.orderbook_stream_url(symbol),
            "trade": adapter.trades_stream_url(symbol),
            "funding": adapter.funding_stream_url(symbol),
        }[kind]
        delay = 1
        while self._stop_event is not None and not self._stop_event.is_set():
            try:
                async with self._session.ws_connect(url, heartbeat=None) as ws:
                    self.coordinator.stream(Venue.EXTENDED, symbol).connected(self.clock.now())
                    if kind == "trade":
                        self.mark_trade_stream_connected(Venue.EXTENDED, symbol)
                    if kind == "book":
                        await self.recover_snapshot(await adapter.fetch_book(symbol))
                    delay = 1
                    ordinal = 0
                    async for message in ws:
                        if message.type is aiohttp.WSMsgType.PING:
                            await ws.pong(message.data)
                            self.coordinator.stream(Venue.EXTENDED, symbol).connection_confirmed(self.clock.now())
                            continue
                        if message.type is not aiohttp.WSMsgType.TEXT:
                            continue
                        payload = json.loads(message.data, parse_float=Decimal)
                        if kind == "book":
                            await self.apply_book_event(adapter.normalize_book_message(payload))
                        elif kind == "trade":
                            ordinal += 1
                            await self.deliver_trade(
                                adapter.normalize_trade(
                                    payload,
                                    received_at=self.clock.now(),
                                    session_id=str(id(ws)),
                                    ordinal=ordinal,
                                )
                            )
                        else:
                            row = self.observations.get((Venue.EXTENDED, symbol))
                            if row is not None:
                                quote = adapter.normalize_funding_message(
                                    payload,
                                    row.market,
                                    mark_price=self._book_mid(Venue.EXTENDED, symbol),
                                    assumed_open_at=self.clock.now(),
                                )
                                await self._apply_funding_quote(quote)
            except Exception:
                await self.mark_disconnected(Venue.EXTENDED, symbol)
                await self._sleep(delay)
                delay = min(delay * 2, 30)

    async def _combined_stream(self, venue: Venue, adapter: PublicAdapter) -> None:
        assert self._session is not None
        delay = 1
        symbols = [market.venue_symbol for market in self._candidate_markets() if market.venue is venue]
        if not symbols:
            return
        while self._stop_event is not None and not self._stop_event.is_set():
            try:
                async with self._session.ws_connect(adapter.ws_base, heartbeat=10) as ws:
                    if venue is Venue.RISEX:
                        ids = [adapter.market_id(symbol) for symbol in symbols]  # type: ignore[attr-defined]
                        await ws.send_json(adapter.orderbook_subscription(ids))  # type: ignore[attr-defined]
                        await ws.send_json(adapter.trades_subscription(ids))  # type: ignore[attr-defined]
                    else:
                        for symbol in symbols:
                            product = adapter.product_id(symbol)  # type: ignore[attr-defined]
                            for kind in ("book_depth", "trade", "funding_rate", "funding_payment"):
                                await ws.send_json(adapter.subscription(kind, product))  # type: ignore[attr-defined]
                    for symbol in symbols:
                        self.coordinator.stream(venue, symbol).connected(self.clock.now())
                        self.mark_trade_stream_connected(venue, symbol)
                        await self.recover_snapshot(await adapter.fetch_book(symbol))
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
                        if "orderbook" in kind or "book_depth" in kind:
                            await self.apply_book_event(adapter.normalize_book_message(payload))  # type: ignore[attr-defined]
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
            except Exception:
                for symbol in symbols:
                    await self.mark_disconnected(venue, symbol)
                await self._sleep(delay)
                delay = min(delay * 2, 30)

    async def start_streams(self) -> None:
        if self._session is None or self.adapters is None:
            return
        self._stop_event = self._stop_event or asyncio.Event()
        risex = self.adapters.get(Venue.RISEX)
        nado = self.adapters.get(Venue.NADO)
        if risex is not None:
            self._stream_tasks.append(asyncio.create_task(self._combined_stream(Venue.RISEX, risex)))
        if nado is not None:
            self._stream_tasks.append(asyncio.create_task(self._combined_stream(Venue.NADO, nado)))
        extended = self.adapters.get(Venue.EXTENDED)
        if isinstance(extended, ExtendedAdapter):
            for market in self._candidate_markets():
                if market.venue is Venue.EXTENDED:
                    for kind in ("book", "trade", "funding"):
                        self._stream_tasks.append(
                            asyncio.create_task(self._extended_stream(extended, market.venue_symbol, kind))
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

    async def run(self, *, stop_event: asyncio.Event | None = None) -> dict[str, object]:
        self._stop_event = stop_event or asyncio.Event()
        loop = asyncio.get_running_loop()
        installed: list[signal.Signals] = []
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._stop_event.set)
                installed.append(sig)
            except (NotImplementedError, RuntimeError):
                pass
        try:
            await self.scan()
            await self._restore(self.last_scan.logical_at)
            self.next_full_scan_at = self.last_scan.logical_at + timedelta(
                seconds=self.config.normal_scan_seconds
            )
            await self.start_streams()
            while not self._stop_event.is_set():
                await self.tick()
                await self._pause_or_stop(1)
        finally:
            for sig in installed:
                loop.remove_signal_handler(sig)
            await self.shutdown()
        return {"status": "STOPPED_SAFE", "forced_close": False}

    async def shutdown(self) -> None:
        if not self.accepting_entries:
            return
        self.accepting_entries = False
        at = self.clock.now()
        if self.broker is not None and self.broker.state.lifecycle_state is LifecycleState.ENTRY_MAKER_OPEN:
            state = await self.broker.cancel_for_process_restart(restarted_at=at)
            self.repository.save_decision(recorded_at=at, entry_state=state)
        self._record(
            "STOPPED_SAFE",
            at=at,
            detail={"forced_close": False, "open_position_preserved": self.lifecycle is not None},
        )
        if self._stop_event is not None:
            self._stop_event.set()
        for task in self._stream_tasks:
            task.cancel()
        if self._stream_tasks:
            await asyncio.gather(*self._stream_tasks, return_exceptions=True)
        self._stream_tasks.clear()

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
) -> dict[str, object]:
    async with PublicPaperRuntime(
        repository,
        adapters=adapters,
        session_factory=session_factory,
        clock=clock,
        sleep=sleep,
    ) as runtime:
        return await runtime.run(stop_event=stop_event)
