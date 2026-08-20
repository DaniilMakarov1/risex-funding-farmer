from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from risex_farmer.lifecycle import (
    CloseReason,
    ExitTradeOutcome,
    ExitVersionReason,
    ExitVersionStatus,
    LifecycleEngine,
    LifecycleEventType,
    restart_paper_entry_state,
)
from risex_farmer.models import (
    BookLevel,
    CanonicalMarket,
    ContractType,
    DataQuality,
    FundingAccrualMethod,
    FundingCashQuote,
    FundingEvent,
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
    TargetFundingCycle,
    TradeEvidence,
    Venue,
)
from risex_farmer.paper_broker import CancellationReason, PaperEntryBroker
from risex_farmer.scanner import MarketObservation, scan_once


D = Decimal
NOW = datetime(2027, 6, 1, 12, tzinfo=UTC)
OPENED = NOW + timedelta(seconds=1)
TARGET = NOW + timedelta(seconds=120)


def market(venue: Venue, asset: str = "ABC") -> CanonicalMarket:
    return CanonicalMarket(
        asset,
        venue,
        f"{asset}-{venue.value}",
        MarketType.PERPETUAL,
        ContractType.LINEAR,
        D("1"),
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


def observation(
    venue: Venue,
    *,
    at: datetime = NOW,
    asset: str = "ABC",
    bid: str = "99",
    ask: str = "101",
    bid_depth: str = "10",
    ask_depth: str = "10",
    healthy: bool = True,
) -> MarketObservation:
    normalized = market(venue, asset)
    return MarketObservation(
        normalized,
        MarketVolume(
            venue, normalized.venue_symbol, D("1000000"), at, "synthetic"
        ),
        OrderBook(
            venue,
            normalized.venue_symbol,
            (BookLevel(D(bid), D(bid_depth)),),
            (BookLevel(D(ask), D(ask_depth)),),
            at,
            1,
        ),
        FundingCashQuote(
            venue,
            normalized.venue_symbol,
            at,
            at,
            TARGET,
            FundingQuality.PREDICTED,
            FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT,
            True,
            D("5"),
            D("5"),
            "synthetic",
        ),
        StreamHealth(
            at,
            at,
            healthy,
            healthy,
            healthy,
            DataQuality.COMPLETE if healthy else DataQuality.DEGRADED,
        ),
    )


async def open_state(
    *,
    direction: RouteDirection = RouteDirection.LONG_RISEX_SHORT_HEDGE,
    recomputed_cash: str | None = "5",
    asset: str = "ABC",
):
    risex = observation(Venue.RISEX, asset=asset)
    hedge = observation(Venue.EXTENDED, asset=asset)
    snapshot = await scan_once((risex, hedge), NOW)
    winner = next(
        plan for plan in snapshot.evaluations
        if plan.direction is direction and plan.entry_allowed
    )
    snapshot = replace(snapshot, winner=winner)
    broker = PaperEntryBroker()
    await broker.activate(snapshot, attempt_id="attempt", activated_at=NOW)

    async def recompute(plan, opened_at):
        value = None if recomputed_cash is None else D(recomputed_cash)
        return tuple(
            FundingCashQuote(
                venue,
                symbol,
                opened_at,
                opened_at,
                TARGET,
                FundingQuality.PREDICTED if value is not None else FundingQuality.UNKNOWN,
                (
                    FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT
                    if value is not None
                    else FundingAccrualMethod.UNKNOWN
                ),
                value is not None,
                value,
                value,
                "synthetic-recomputed",
            )
            for venue, symbol in (
                (Venue.RISEX, plan.risex_market.venue_symbol),
                (plan.hedge_venue, plan.hedge_market.venue_symbol),
            )
        )

    hedge_side = broker.state.order.side
    entry_trade = TradeEvidence(
        "entry",
        Venue.EXTENDED,
        f"{asset}-EXTENDED",
        OPENED - timedelta(microseconds=1),
        OPENED,
        "synthetic-entry",
        D("5"),
        D("101") if hedge_side is Side.SELL else D("99"),
        Side.BUY if hedge_side is Side.SELL else Side.SELL,
        True,
    )
    result = await broker.process_trade(
        entry_trade,
        observed_version_id=broker.state.order.active_version.version_id,
        processed_at=OPENED,
        risex_observation=risex,
        hedge_observation=hedge,
        recompute_funding=recompute,
    )
    return result.state


def exit_trade(
    key: str,
    side: Side,
    *,
    quantity: str = "5",
    at: datetime = OPENED + timedelta(seconds=2),
) -> TradeEvidence:
    return TradeEvidence(
        key,
        Venue.EXTENDED,
        "ABC-EXTENDED",
        at,
        at + timedelta(microseconds=1),
        "synthetic-exit",
        D(quantity),
        D("99") if side is Side.BUY else D("101"),
        Side.SELL if side is Side.BUY else Side.BUY,
        True,
    )


def fresh_pair(at: datetime = OPENED):
    return observation(Venue.RISEX, at=at), observation(Venue.EXTENDED, at=at)


@pytest.mark.asyncio
async def test_settlement_authority_replaces_estimate_and_recomputes() -> None:
    engine = LifecycleEngine(await open_state())
    rows = engine.snapshot.settlements
    await engine.reconcile_settlement(
        replace(rows[0], status=SettlementStatus.ESTIMATED, cash_usd=D("4"))
    )
    await engine.reconcile_settlement(
        replace(rows[1], status=SettlementStatus.ESTIMATED, cash_usd=D("3"))
    )
    await engine.reconcile_settlement(
        replace(rows[0], status=SettlementStatus.APPLIED_RATE, cash_usd=D("5"))
    )
    with pytest.raises(ValueError, match="conflicting duplicate"):
        await engine.reconcile_settlement(
            replace(rows[0], status=SettlementStatus.APPLIED_RATE, cash_usd=D("6"))
        )

    risex, hedge = fresh_pair(TARGET + timedelta(seconds=1))
    await engine.evaluate(
        evaluated_at=TARGET + timedelta(seconds=1),
        risex_observation=risex,
        hedge_observation=hedge,
    )
    assert engine.snapshot.samples[-1].lifecycle_recognized_funding_usd == D("8")
    assert engine.snapshot.lifecycle_state is LifecycleState.EXITING_NORMAL


@pytest.mark.asyncio
async def test_strict_open_and_close_settlement_eligibility_boundaries() -> None:
    state = await open_state()
    original = state.position.target_cycle
    at_open = FundingEvent(
        Venue.RISEX,
        state.position.risex_taker_fill.canonical_market,
        OPENED,
        D("5"),
        True,
    )
    later = replace(original.hedge_event, settlement_at=OPENED + timedelta(seconds=3))
    cycle = TargetFundingCycle(
        "boundary",
        OPENED,
        later.settlement_at,
        3,
        at_open,
        later,
    )
    state = replace(
        state,
        lifecycle_state=LifecycleState.EXITING_NORMAL,
        position=replace(state.position, target_cycle=cycle),
    )
    engine = LifecycleEngine(state)
    assert engine.snapshot.settlements[0].status is SettlementStatus.SKIPPED_POSITION_NOT_OPEN
    risex, hedge = fresh_pair(OPENED)
    await engine.evaluate(
        evaluated_at=OPENED,
        risex_observation=risex,
        hedge_observation=hedge,
    )
    order = engine.snapshot.exit_order
    closed_at = later.settlement_at
    await engine.process_exit_trade(
        exit_trade("boundary-close", order.side, at=closed_at),
        observed_version_id=order.active_version.version_id,
        processed_at=closed_at,
        risex_observation=observation(Venue.RISEX, at=closed_at),
        hedge_observation=observation(Venue.EXTENDED, at=closed_at),
    )
    assert {
        row.status for row in engine.snapshot.settlements
    } == {
        SettlementStatus.SKIPPED_POSITION_NOT_OPEN,
        SettlementStatus.SKIPPED_POSITION_CLOSED,
    }


@pytest.mark.asyncio
async def test_resolved_cycle_registers_only_one_current_next_cycle() -> None:
    engine = LifecycleEngine(await open_state())
    for row, cash in zip(engine.snapshot.settlements, (D("4"), D("3")), strict=True):
        await engine.reconcile_settlement(
            replace(row, status=SettlementStatus.ESTIMATED, cash_usd=cash)
        )
    next_at = TARGET + timedelta(seconds=60)
    next_cycle = TargetFundingCycle(
        "next",
        next_at,
        next_at,
        0,
        FundingEvent(Venue.RISEX, "ABC-RISEX", next_at, D("2"), True),
        FundingEvent(Venue.EXTENDED, "ABC-EXTENDED", next_at, D("2"), True),
    )
    at = TARGET + timedelta(seconds=1)
    await engine.evaluate(
        evaluated_at=at,
        risex_observation=observation(Venue.RISEX, at=at),
        hedge_observation=observation(Venue.EXTENDED, at=at),
        next_cycle=next_cycle,
    )
    assert engine.snapshot.active_cycle.cycle_id == "next"
    assert len(engine.snapshot.settlements) == 4
    assert engine.snapshot.samples[-1].remaining_funding_usd == D("4")
    assert engine.snapshot.lifecycle_state is LifecycleState.HOLDING


@pytest.mark.asyncio
async def test_unresolved_with_estimate_is_recognized_but_never_rolls_cycle() -> None:
    engine = LifecycleEngine(await open_state())
    rows = engine.snapshot.settlements
    await engine.reconcile_settlement(
        replace(rows[0], status=SettlementStatus.ESTIMATED, cash_usd=D("4"))
    )
    current = engine.snapshot.settlements[0]
    await engine.reconcile_settlement(
        replace(current, status=SettlementStatus.UNRESOLVED, cash_usd=None)
    )
    await engine.reconcile_settlement(
        replace(rows[1], status=SettlementStatus.ESTIMATED, cash_usd=D("3"))
    )
    next_at = TARGET + timedelta(seconds=60)
    next_cycle = TargetFundingCycle(
        "must-not-roll",
        next_at,
        next_at,
        0,
        FundingEvent(Venue.RISEX, "ABC-RISEX", next_at, D("2"), True),
        FundingEvent(Venue.EXTENDED, "ABC-EXTENDED", next_at, D("2"), True),
    )
    at = TARGET + timedelta(seconds=1)
    await engine.evaluate(
        evaluated_at=at,
        risex_observation=observation(Venue.RISEX, at=at),
        hedge_observation=observation(Venue.EXTENDED, at=at),
        next_cycle=next_cycle,
    )
    assert engine.snapshot.samples[-1].lifecycle_recognized_funding_usd == D("7")
    assert engine.snapshot.active_cycle.cycle_id != "must-not-roll"
    assert len(engine.snapshot.settlements) == 2
    assert engine.snapshot.lifecycle_state is LifecycleState.EXITING_NORMAL


@pytest.mark.asyncio
async def test_normal_to_aggressive_is_exact_sticky_and_reversion_is_forbidden() -> None:
    engine = LifecycleEngine(await open_state(recomputed_cash="0"))
    risex, hedge = fresh_pair(OPENED)
    await engine.evaluate(
        evaluated_at=OPENED,
        risex_observation=risex,
        hedge_observation=hedge,
    )
    first = engine.snapshot.exit_order.active_version
    assert first.mode is LifecycleState.EXITING_NORMAL

    before = OPENED + timedelta(seconds=10) - timedelta(microseconds=1)
    risex, hedge = fresh_pair(before)
    await engine.evaluate(
        evaluated_at=before,
        risex_observation=risex,
        hedge_observation=hedge,
    )
    assert engine.snapshot.lifecycle_state is LifecycleState.EXITING_NORMAL

    boundary = OPENED + timedelta(seconds=10)
    risex, hedge = fresh_pair(boundary)
    await engine.evaluate(
        evaluated_at=boundary,
        risex_observation=risex,
        hedge_observation=hedge,
    )
    assert engine.snapshot.lifecycle_state is LifecycleState.EXITING_AGGRESSIVE
    assert engine.snapshot.exit_order.versions[0].close_reason is ExitVersionReason.AGGRESSIVE_TRANSITION
    assert engine.snapshot.exit_order.active_version.cumulative_eligible_quantity == 0

    later = boundary + timedelta(seconds=20)
    risex, hedge = fresh_pair(later)
    await engine.evaluate(
        evaluated_at=later,
        risex_observation=risex,
        hedge_observation=hedge,
    )
    assert engine.snapshot.lifecycle_state is LifecycleState.EXITING_AGGRESSIVE


@pytest.mark.asyncio
async def test_exit_trade_accumulates_dedups_and_closes_both_legs() -> None:
    engine = LifecycleEngine(await open_state(recomputed_cash="0"))
    risex, hedge = fresh_pair(OPENED)
    await engine.evaluate(
        evaluated_at=OPENED,
        risex_observation=risex,
        hedge_observation=hedge,
    )
    order = engine.snapshot.exit_order
    token = order.active_version.version_id
    first = await engine.process_exit_trade(
        exit_trade("one", order.side, quantity="2"),
        observed_version_id=token,
        processed_at=OPENED + timedelta(seconds=2),
        risex_observation=observation(Venue.RISEX, at=OPENED + timedelta(seconds=2)),
        hedge_observation=observation(Venue.EXTENDED, at=OPENED + timedelta(seconds=2)),
    )
    duplicate = await engine.process_exit_trade(
        exit_trade("one", order.side, quantity="3"),
        observed_version_id=token,
        processed_at=OPENED + timedelta(seconds=2),
        risex_observation=risex,
        hedge_observation=hedge,
    )
    closed_at = OPENED + timedelta(seconds=3)
    second = await engine.process_exit_trade(
        exit_trade("two", order.side, quantity="3", at=closed_at),
        observed_version_id=token,
        processed_at=closed_at,
        risex_observation=observation(Venue.RISEX, at=closed_at),
        hedge_observation=observation(Venue.EXTENDED, at=closed_at),
    )
    assert first.outcome is ExitTradeOutcome.ACCUMULATED
    assert duplicate.detail == "DUPLICATE_EVENT_KEY"
    assert second.outcome is ExitTradeOutcome.CLOSED
    assert engine.snapshot.lifecycle_state is LifecycleState.FLAT
    assert engine.snapshot.position is None
    closed = engine.snapshot.closed_trade
    assert closed.close_reason is CloseReason.NORMAL_MAKER
    assert closed.applied_rate_closed_net_pnl_usd is not None
    assert all(
        row.status is SettlementStatus.SKIPPED_POSITION_CLOSED
        for row in engine.snapshot.settlements
    )


@pytest.mark.asyncio
async def test_aggressive_maker_can_close_but_has_no_taker_timeout() -> None:
    engine = LifecycleEngine(await open_state(recomputed_cash="0"))
    at = OPENED + timedelta(seconds=10)
    await engine.evaluate(
        evaluated_at=at,
        risex_observation=observation(Venue.RISEX, at=at),
        hedge_observation=observation(Venue.EXTENDED, at=at),
    )
    order = engine.snapshot.exit_order
    assert engine.snapshot.lifecycle_state is LifecycleState.EXITING_AGGRESSIVE
    assert engine.snapshot.position is not None
    result = await engine.process_exit_trade(
        exit_trade("aggressive-fill", order.side, at=at + timedelta(seconds=1)),
        observed_version_id=order.active_version.version_id,
        processed_at=at + timedelta(seconds=1),
        risex_observation=observation(Venue.RISEX, at=at + timedelta(seconds=1)),
        hedge_observation=observation(Venue.EXTENDED, at=at + timedelta(seconds=1)),
    )
    assert result.outcome is ExitTradeOutcome.CLOSED
    assert engine.snapshot.closed_trade.close_reason is CloseReason.AGGRESSIVE_MAKER


@pytest.mark.asyncio
async def test_lost_risex_depth_suspends_version_and_recreates() -> None:
    engine = LifecycleEngine(await open_state(recomputed_cash="0"))
    risex, hedge = fresh_pair(OPENED)
    await engine.evaluate(
        evaluated_at=OPENED,
        risex_observation=risex,
        hedge_observation=hedge,
    )
    old_token = engine.snapshot.exit_order.active_version.version_id
    shallow_at = OPENED + timedelta(seconds=2)
    await engine.evaluate(
        evaluated_at=shallow_at,
        risex_observation=observation(
            Venue.RISEX, at=shallow_at, bid_depth="1"
        ),
        hedge_observation=observation(Venue.EXTENDED, at=shallow_at),
    )
    assert engine.snapshot.exit_order.active_version is None
    assert engine.snapshot.exit_order.versions[-1].close_reason is ExitVersionReason.UNWIND_UNAVAILABLE
    assert engine.snapshot.lifecycle_state is LifecycleState.EXITING_NORMAL

    recovered_at = OPENED + timedelta(seconds=3)
    await engine.evaluate(
        evaluated_at=recovered_at,
        risex_observation=observation(Venue.RISEX, at=recovered_at),
        hedge_observation=observation(Venue.EXTENDED, at=recovered_at),
    )
    assert engine.snapshot.exit_order.active_version.version_id != old_token


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "direction",
    (
        RouteDirection.LONG_RISEX_SHORT_HEDGE,
        RouteDirection.SHORT_RISEX_LONG_HEDGE,
    ),
)
async def test_hard_basis_closes_both_directions_at_exact_taker_quotes(direction) -> None:
    engine = LifecycleEngine(await open_state(direction=direction))
    at = OPENED + timedelta(seconds=2)
    if direction is RouteDirection.LONG_RISEX_SHORT_HEDGE:
        risex = observation(Venue.RISEX, at=at, bid="90", ask="92")
        hedge = observation(Venue.EXTENDED, at=at, bid="108", ask="110")
    else:
        risex = observation(Venue.RISEX, at=at, bid="108", ask="110")
        hedge = observation(Venue.EXTENDED, at=at, bid="90", ask="92")
    await engine.evaluate(
        evaluated_at=at,
        risex_observation=risex,
        hedge_observation=hedge,
    )
    assert engine.snapshot.lifecycle_state is LifecycleState.FLAT
    assert engine.snapshot.closed_trade.close_reason is CloseReason.HARD_BASIS
    assert engine.snapshot.closed_trade.hedge_exit_fill.fee.liquidity_role.value == "TAKER"
    assert engine.snapshot.closed_trade.risex_exit_fill.fee.liquidity_role.value == "TAKER"


@pytest.mark.asyncio
async def test_unavailable_hard_basis_quote_keeps_position_and_degrades() -> None:
    engine = LifecycleEngine(await open_state())
    at = OPENED + timedelta(seconds=2)
    await engine.evaluate(
        evaluated_at=at,
        risex_observation=observation(Venue.RISEX, at=at, bid_depth="1"),
        hedge_observation=observation(Venue.EXTENDED, at=at),
    )
    assert engine.snapshot.position is not None
    assert engine.snapshot.data_quality is DataQuality.DEGRADED
    assert engine.snapshot.events[-1].event_type is LifecycleEventType.UNWIND_QUOTE_UNAVAILABLE


@pytest.mark.asyncio
async def test_gap_and_restart_preserve_exit_timer_and_degrade_permanently() -> None:
    engine = LifecycleEngine(await open_state(recomputed_cash="0"))
    risex, hedge = fresh_pair(OPENED)
    await engine.evaluate(
        evaluated_at=OPENED,
        risex_observation=risex,
        hedge_observation=hedge,
    )
    await engine.start_gap(started_at=OPENED + timedelta(seconds=2))
    assert engine.snapshot.exit_order.active_version is None
    recovered_at = OPENED + timedelta(seconds=12)
    await engine.recover(
        recovered_at=recovered_at,
        risex_observation=observation(Venue.RISEX, at=recovered_at),
        hedge_observation=observation(Venue.EXTENDED, at=recovered_at),
    )
    assert engine.snapshot.lifecycle_state is LifecycleState.EXITING_AGGRESSIVE
    assert engine.snapshot.exiting_normal_started_at == OPENED
    assert engine.snapshot.data_quality is DataQuality.DEGRADED
    assert engine.snapshot.gap_count == 1
    assert engine.snapshot.maximum_gap_duration == timedelta(seconds=10)

    restart_at = recovered_at + timedelta(seconds=5)
    await engine.restart(
        last_known_at=recovered_at,
        recovered_at=restart_at,
        risex_observation=observation(Venue.RISEX, at=restart_at),
        hedge_observation=observation(Venue.EXTENDED, at=restart_at),
    )
    assert engine.snapshot.lifecycle_state is LifecycleState.EXITING_AGGRESSIVE
    assert engine.snapshot.exit_order.active_version.mode is LifecycleState.EXITING_AGGRESSIVE
    assert engine.snapshot.gap_count == 2


@pytest.mark.asyncio
async def test_gap_records_funding_overlap_while_holding() -> None:
    engine = LifecycleEngine(await open_state())
    await engine.start_gap(started_at=TARGET - timedelta(seconds=1))
    recovered_at = TARGET + timedelta(seconds=1)
    await engine.recover(
        recovered_at=recovered_at,
        risex_observation=observation(Venue.RISEX, at=recovered_at),
        hedge_observation=observation(Venue.EXTENDED, at=recovered_at),
    )
    assert engine.snapshot.gaps[-1].overlapped_funding is True
    assert engine.snapshot.data_quality is DataQuality.DEGRADED


@pytest.mark.asyncio
async def test_funding_during_exit_and_partial_applied_close_recompute() -> None:
    engine = LifecycleEngine(await open_state(recomputed_cash="0"))
    risex, hedge = fresh_pair(OPENED)
    await engine.evaluate(
        evaluated_at=OPENED,
        risex_observation=risex,
        hedge_observation=hedge,
    )
    rows = engine.snapshot.settlements
    await engine.reconcile_settlement(
        replace(rows[0], status=SettlementStatus.APPLIED_RATE, cash_usd=D("5"))
    )
    await engine.reconcile_settlement(
        replace(rows[1], status=SettlementStatus.ESTIMATED, cash_usd=D("3"))
    )
    close_at = TARGET + timedelta(seconds=1)
    await engine.evaluate(
        evaluated_at=close_at,
        risex_observation=observation(Venue.RISEX, at=close_at),
        hedge_observation=observation(Venue.EXTENDED, at=close_at),
    )
    order = engine.snapshot.exit_order
    await engine.process_exit_trade(
        exit_trade("funded-close", order.side, at=close_at),
        observed_version_id=order.active_version.version_id,
        processed_at=close_at,
        risex_observation=observation(Venue.RISEX, at=close_at),
        hedge_observation=observation(Venue.EXTENDED, at=close_at),
    )
    closed = engine.snapshot.closed_trade
    assert closed.funding_while_exiting_usd == D("8")
    assert closed.simulated_recognized_funding_usd == D("8")
    assert closed.applied_rate_closed_net_pnl_usd is None
    assert closed.primary_metrics_valid is True

    estimated = next(
        row for row in engine.snapshot.settlements
        if row.status is SettlementStatus.ESTIMATED
    )
    await engine.reconcile_settlement(
        replace(estimated, status=SettlementStatus.APPLIED_RATE, cash_usd=D("4"))
    )
    assert engine.snapshot.closed_trade.simulated_recognized_funding_usd == D("9")
    assert engine.snapshot.closed_trade.applied_rate_closed_net_pnl_usd is not None


@pytest.mark.asyncio
async def test_unknown_post_entry_funding_retains_required_keys_and_fails_closed() -> None:
    state = await open_state(recomputed_cash=None)
    assert state.position.target_cycle is None
    engine = LifecycleEngine(state)
    assert engine.snapshot.active_cycle is None
    assert len(engine.snapshot.settlements) == 2
    first = engine.snapshot.settlements[0]
    await engine.reconcile_settlement(
        replace(first, status=SettlementStatus.UNRESOLVED, cash_usd=None)
    )
    close_at = TARGET + timedelta(seconds=1)
    await engine.evaluate(
        evaluated_at=close_at,
        risex_observation=observation(Venue.RISEX, at=close_at),
        hedge_observation=observation(Venue.EXTENDED, at=close_at),
    )
    order = engine.snapshot.exit_order
    await engine.process_exit_trade(
        exit_trade("unknown-close", order.side, at=close_at),
        observed_version_id=order.active_version.version_id,
        processed_at=close_at,
        risex_observation=observation(Venue.RISEX, at=close_at),
        hedge_observation=observation(Venue.EXTENDED, at=close_at),
    )
    closed = engine.snapshot.closed_trade
    assert {row.status for row in engine.snapshot.settlements} == {
        SettlementStatus.PENDING,
        SettlementStatus.UNRESOLVED,
    }
    assert closed.applied_rate_closed_net_pnl_usd is None
    assert closed.primary_metrics_valid is False


@pytest.mark.asyncio
async def test_unknown_funding_future_close_skips_complete_required_key_set() -> None:
    engine = LifecycleEngine(await open_state(recomputed_cash=None))
    risex, hedge = fresh_pair(OPENED)
    await engine.evaluate(
        evaluated_at=OPENED,
        risex_observation=risex,
        hedge_observation=hedge,
    )
    order = engine.snapshot.exit_order
    close_at = OPENED + timedelta(seconds=2)
    await engine.process_exit_trade(
        exit_trade("unknown-future-skip", order.side, at=close_at),
        observed_version_id=order.active_version.version_id,
        processed_at=close_at,
        risex_observation=observation(Venue.RISEX, at=close_at),
        hedge_observation=observation(Venue.EXTENDED, at=close_at),
    )
    assert len(engine.snapshot.settlements) == 2
    assert all(
        row.status is SettlementStatus.SKIPPED_POSITION_CLOSED
        for row in engine.snapshot.settlements
    )
    assert engine.snapshot.closed_trade.applied_rate_closed_net_pnl_usd is not None
    assert engine.snapshot.closed_trade.primary_metrics_valid is True


@pytest.mark.asyncio
async def test_restart_flat_entry_and_holding_contracts() -> None:
    assert restart_paper_entry_state(
        replace(await open_state(), lifecycle_state=LifecycleState.FLAT, position=None),
        restarted_at=OPENED,
    ).lifecycle_state is LifecycleState.FLAT

    risex = observation(Venue.RISEX)
    hedge = observation(Venue.EXTENDED)
    snapshot = await scan_once((risex, hedge), NOW)
    broker = PaperEntryBroker()
    entry = await broker.activate(snapshot, attempt_id="restart", activated_at=NOW)
    restarted = restart_paper_entry_state(entry, restarted_at=OPENED)
    assert restarted.lifecycle_state is LifecycleState.FLAT
    assert restarted.order.cancellation_reason is CancellationReason.PROCESS_RESTART
    assert restarted.position is None

    holding = LifecycleEngine(await open_state())
    pending = holding.snapshot.settlements[0]
    recovered_at = OPENED + timedelta(seconds=5)
    await holding.restart(
        last_known_at=OPENED + timedelta(seconds=2),
        recovered_at=recovered_at,
        risex_observation=observation(Venue.RISEX, at=recovered_at),
        hedge_observation=observation(Venue.EXTENDED, at=recovered_at),
        settlement_updates=(
            replace(pending, status=SettlementStatus.ESTIMATED, cash_usd=D("4")),
        ),
    )
    assert holding.snapshot.lifecycle_state is LifecycleState.HOLDING
    assert holding.snapshot.data_quality is DataQuality.DEGRADED
    assert holding.snapshot.settlements[0].status is SettlementStatus.ESTIMATED

    normal = LifecycleEngine(await open_state(recomputed_cash="0"))
    risex, hedge = fresh_pair(OPENED)
    await normal.evaluate(
        evaluated_at=OPENED,
        risex_observation=risex,
        hedge_observation=hedge,
    )
    old_version = normal.snapshot.exit_order.active_version.version_id
    recovered_at = OPENED + timedelta(seconds=5)
    await normal.restart(
        last_known_at=OPENED + timedelta(seconds=1),
        recovered_at=recovered_at,
        risex_observation=observation(Venue.RISEX, at=recovered_at),
        hedge_observation=observation(Venue.EXTENDED, at=recovered_at),
    )
    assert normal.snapshot.lifecycle_state is LifecycleState.EXITING_NORMAL
    assert normal.snapshot.exit_order.active_version.version_id != old_version
    assert normal.snapshot.exit_order.versions[-2].close_reason is ExitVersionReason.PROCESS_RESTART
