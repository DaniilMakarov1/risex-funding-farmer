"""Fixture-driven orchestration for deterministic CI runs."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from .lifecycle import LifecycleEngine, LifecycleSnapshot, restart_paper_entry_state
from .models import (
    BookLevel,
    CanonicalMarket,
    ContractType,
    DataQuality,
    FundingAccrualMethod,
    FundingCashQuote,
    FundingQuality,
    FundingSettlement,
    LifecycleState,
    MarketType,
    MarketVolume,
    OrderBook,
    RouteDirection,
    SettlementStatus,
    Side,
    StreamHealth,
    TradeEvidence,
    Venue,
)
from .paper_broker import PaperEntryBroker, PaperEntryState
from .scanner import MarketObservation, ScanSnapshot, scan_once
from .storage import PaperRepository


D = Decimal
DEFAULT_LOGICAL_AT = datetime(2027, 7, 1, 12, tzinfo=UTC)


def load_fixture(path: str | Path) -> dict[str, object]:
    value = json.loads(Path(path).read_text())
    if not isinstance(value, dict):
        raise ValueError("fixture root must be an object")
    return value


def _logical_at(spec: dict[str, object]) -> datetime:
    raw = spec.get("logical_at")
    if raw is None:
        return DEFAULT_LOGICAL_AT
    if not isinstance(raw, str):
        raise ValueError("logical_at must be an ISO timestamp")
    return datetime.fromisoformat(raw)


def _market(
    venue: Venue,
    asset: str,
    *,
    multiplier_known: bool = True,
) -> CanonicalMarket:
    return CanonicalMarket(
        asset,
        venue,
        f"{asset}-{venue.value}",
        MarketType.PERPETUAL,
        ContractType.LINEAR,
        D("1") if multiplier_known else None,
        "USDC",
        "USDC",
        D("1"),
        D("1"),
        D("1"),
        D("10"),
        None,
        True,
        False,
        False,
    )


def _observation(
    venue: Venue,
    asset: str,
    at: datetime,
    settlement_at: datetime,
    *,
    bid: str = "99",
    ask: str = "101",
    funding_cash: str | None = "5",
    multiplier_known: bool = True,
) -> MarketObservation:
    market = _market(venue, asset, multiplier_known=multiplier_known)
    known = funding_cash is not None
    cash = None if funding_cash is None else D(funding_cash)
    return MarketObservation(
        market,
        MarketVolume(
            venue, market.venue_symbol, D("1000000"), at, "synthetic-fixture"
        ),
        OrderBook(
            venue,
            market.venue_symbol,
            (BookLevel(D(bid), D("10")),),
            (BookLevel(D(ask), D("10")),),
            at,
            1,
        ),
        FundingCashQuote(
            venue,
            market.venue_symbol,
            at,
            at,
            settlement_at,
            FundingQuality.PREDICTED if known else FundingQuality.UNKNOWN,
            (
                FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT
                if known
                else FundingAccrualMethod.UNKNOWN
            ),
            known,
            cash,
            cash,
            "synthetic-fixture",
        ),
        StreamHealth(at, at, True, True, True, DataQuality.COMPLETE),
    )


async def fixture_scan(
    spec: dict[str, object]
) -> tuple[ScanSnapshot, tuple[MarketObservation, MarketObservation]]:
    logical_at = _logical_at(spec)
    target = logical_at + timedelta(seconds=120)
    asset = str(spec.get("asset", "ABC"))
    scenario = str(spec.get("scenario", "no_opportunity"))
    risex = _observation(
        Venue.RISEX,
        asset,
        logical_at,
        target,
        multiplier_known=scenario != "no_opportunity",
        funding_cash=None if scenario == "no_opportunity" else "5",
    )
    hedge = _observation(Venue.EXTENDED, asset, logical_at, target)
    return await scan_once((risex, hedge), logical_at), (risex, hedge)


def _entry_trade(order, at: datetime) -> TradeEvidence:
    return TradeEvidence(
        f"{order.attempt_id}:entry-trade",
        order.venue,
        order.canonical_market,
        at - timedelta(microseconds=1),
        at,
        "synthetic-fixture-entry",
        order.canonical_quantity,
        (
            order.active_version.limit_price + order.route_plan.hedge_market.tick_size_raw
            if order.side is Side.SELL
            else order.active_version.limit_price - order.route_plan.hedge_market.tick_size_raw
        ),
        Side.BUY if order.side is Side.SELL else Side.SELL,
        True,
    )


def _exit_trade(order, at: datetime) -> TradeEvidence:
    version = order.active_version
    assert version is not None
    tick = D("1")
    return TradeEvidence(
        f"{order.order_id}:trade",
        order.venue,
        order.canonical_market,
        at,
        at,
        "synthetic-fixture-exit",
        order.canonical_quantity,
        version.limit_price - tick if order.side is Side.BUY else version.limit_price + tick,
        Side.SELL if order.side is Side.BUY else Side.BUY,
        True,
    )


async def _funding_recomputer(plan, opened_at: datetime, cash: str | None):
    value = None if cash is None else D(cash)
    return tuple(
        FundingCashQuote(
            venue,
            symbol,
            opened_at,
            opened_at,
            plan.target_cycle.risex_event.settlement_at,
            FundingQuality.PREDICTED if value is not None else FundingQuality.UNKNOWN,
            (
                FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT
                if value is not None
                else FundingAccrualMethod.UNKNOWN
            ),
            value is not None,
            value,
            value,
            "synthetic-fixture-recomputed",
        )
        for venue, symbol in (
            (Venue.RISEX, plan.risex_market.venue_symbol),
            (plan.hedge_venue, plan.hedge_market.venue_symbol),
        )
    )


async def run_fixture(
    spec: dict[str, object], repository: PaperRepository
) -> dict[str, object]:
    scenario = str(spec.get("scenario", "no_opportunity"))
    if scenario == "restart_open":
        return await _restart_fixture(spec, repository)
    existing = repository.load_runtime()
    if existing is not None and existing.lifecycle_state is not LifecycleState.FLAT:
        return {
            "status": "STOPPED_WITH_EXISTING_RUNTIME",
            "state": existing.lifecycle_state.value,
            "forced_close": False,
        }
    snapshot, observations = await fixture_scan(spec)
    direction_name = str(
        spec.get("direction", RouteDirection.LONG_RISEX_SHORT_HEDGE.value)
    )
    direction = RouteDirection(direction_name)
    if snapshot.winner is not None:
        requested = next(
            plan
            for plan in snapshot.evaluations
            if plan.direction is direction and plan.entry_allowed
        )
        snapshot = replace(snapshot, winner=requested)
    quotes = tuple(
        observation.funding
        for observation in observations
        if observation.funding is not None
    )
    repository.save_decision(
        recorded_at=snapshot.logical_at,
        scan_snapshot=snapshot,
        funding_quotes=quotes,
    )
    if snapshot.winner is None:
        return {"status": "NO_TRADE", "reason": "NO_ELIGIBLE_ROUTE"}
    if scenario == "scan_only":
        return {"status": "OPPORTUNITY", "asset": snapshot.winner.canonical_asset}

    winner = snapshot.winner
    assert winner is not None
    attempt_id = str(spec.get("attempt_id", "fixture-attempt"))
    broker = PaperEntryBroker()
    state = await broker.activate(
        snapshot, attempt_id=attempt_id, activated_at=snapshot.logical_at
    )
    repository.save_decision(recorded_at=snapshot.logical_at, entry_state=state)
    if scenario == "maker_never_fills":
        return {"status": "STOPPED_WITH_OPEN_ENTRY_ORDER", "forced_close": False}

    opened_at = snapshot.logical_at + timedelta(seconds=1)
    trade = _entry_trade(broker.state.order, opened_at)
    post_entry_cash = "5" if scenario in {"open_position", "restart_open"} else "0"
    if scenario in {"unresolved_closed", "unknown_open_position"}:
        post_entry_cash = None

    async def recompute(plan, actual_opened_at):
        return await _funding_recomputer(plan, actual_opened_at, post_entry_cash)

    result = await broker.process_trade(
        trade,
        observed_version_id=broker.state.order.active_version.version_id,
        processed_at=opened_at,
        risex_observation=observations[0],
        hedge_observation=observations[1],
        recompute_funding=recompute,
    )
    repository.save_decision(
        recorded_at=opened_at,
        trade_events=(trade,),
        entry_state=result.state,
    )
    lifecycle = LifecycleEngine(result.state)
    repository.save_decision(recorded_at=opened_at, lifecycle_snapshot=lifecycle.snapshot)
    if scenario in {"open_position", "unknown_open_position"}:
        return {
            "status": "STOPPED_WITH_OPEN_POSITION",
            "state": lifecycle.snapshot.lifecycle_state.value,
            "forced_close": False,
        }

    target = snapshot.winner.target_cycle.start_at
    close_at = opened_at
    if scenario in {"unresolved_closed", "estimated_closed"}:
        close_at = target + timedelta(seconds=1)
        rows = lifecycle.snapshot.settlements
        if scenario == "unresolved_closed":
            await lifecycle.reconcile_settlement(
                replace(rows[0], status=SettlementStatus.UNRESOLVED, cash_usd=None)
            )
        else:
            await lifecycle.reconcile_settlement(
                replace(rows[0], status=SettlementStatus.APPLIED_RATE, cash_usd=D("5"))
            )
            await lifecycle.reconcile_settlement(
                replace(rows[1], status=SettlementStatus.ESTIMATED, cash_usd=D("3"))
            )
    if scenario in {"long_exit", "exiting_aggressive_open"}:
        close_at = opened_at + timedelta(seconds=10)
    risex_bid, risex_ask = (
        ("105", "107") if scenario == "positive_closed" else ("99", "101")
    )
    asset = winner.canonical_asset
    risex = _observation(
        Venue.RISEX, asset, close_at, target, bid=risex_bid, ask=risex_ask
    )
    hedge = _observation(Venue.EXTENDED, asset, close_at, target)
    if scenario == "degraded_closed":
        await lifecycle.start_gap(started_at=opened_at)
        close_at = opened_at + timedelta(seconds=1)
        risex = _observation(Venue.RISEX, asset, close_at, target)
        hedge = _observation(Venue.EXTENDED, asset, close_at, target)
        await lifecycle.recover(
            recovered_at=close_at,
            risex_observation=risex,
            hedge_observation=hedge,
        )
    else:
        await lifecycle.evaluate(
            evaluated_at=close_at,
            risex_observation=risex,
            hedge_observation=hedge,
        )
    if scenario in {"exiting_normal_open", "exiting_aggressive_open"}:
        repository.save_decision(
            recorded_at=close_at, lifecycle_snapshot=lifecycle.snapshot
        )
        return {
            "status": "STOPPED_WITH_OPEN_POSITION",
            "state": lifecycle.snapshot.lifecycle_state.value,
            "forced_close": False,
        }
    if lifecycle.snapshot.lifecycle_state is not LifecycleState.FLAT:
        order = lifecycle.snapshot.exit_order
        exit_evidence = _exit_trade(order, close_at)
        await lifecycle.process_exit_trade(
            exit_evidence,
            observed_version_id=order.active_version.version_id,
            processed_at=close_at,
            risex_observation=risex,
            hedge_observation=hedge,
        )
        repository.save_decision(
            recorded_at=close_at,
            trade_events=(exit_evidence,),
            lifecycle_snapshot=lifecycle.snapshot,
        )
    else:
        repository.save_decision(
            recorded_at=close_at, lifecycle_snapshot=lifecycle.snapshot
        )
    return {
        "status": "CLOSED",
        "close_reason": lifecycle.snapshot.closed_trade.close_reason.value,
        "simulated_closed_net_pnl_usd": (
            None
            if lifecycle.snapshot.closed_trade.simulated_closed_net_pnl_usd is None
            else str(lifecycle.snapshot.closed_trade.simulated_closed_net_pnl_usd)
        ),
    }


async def _restart_fixture(
    spec: dict[str, object], repository: PaperRepository
) -> dict[str, object]:
    runtime = repository.load_runtime()
    if runtime is None:
        return {"status": "NO_RUNTIME_TO_RESTART"}
    recovered_at = _logical_at(spec)
    last_known = repository.runtime_updated_at() or recovered_at
    if isinstance(runtime, PaperEntryState):
        restarted = restart_paper_entry_state(runtime, restarted_at=recovered_at)
        repository.save_decision(recorded_at=recovered_at, entry_state=restarted)
        return {"status": "RESTARTED", "state": restarted.lifecycle_state.value}
    if not isinstance(runtime, LifecycleSnapshot):
        raise TypeError("unsupported runtime payload")
    if runtime.lifecycle_state is LifecycleState.FLAT:
        return {"status": "RESTARTED", "state": LifecycleState.FLAT.value}
    asset = runtime.position.route_key.canonical_asset
    target = (
        recovered_at + timedelta(seconds=120)
        if runtime.active_cycle is None
        else runtime.active_cycle.start_at
    )
    risex = _observation(Venue.RISEX, asset, recovered_at, target)
    hedge = _observation(runtime.hedge_market.venue, asset, recovered_at, target)
    engine = LifecycleEngine.from_snapshot(runtime)
    await engine.restart(
        last_known_at=last_known,
        recovered_at=recovered_at,
        risex_observation=risex,
        hedge_observation=hedge,
    )
    repository.save_decision(
        recorded_at=recovered_at, lifecycle_snapshot=engine.snapshot
    )
    return {
        "status": "RESTARTED",
        "state": engine.snapshot.lifecycle_state.value,
        "forced_close": False,
    }
