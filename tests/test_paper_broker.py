from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from risex_farmer.models import (
    BookExecutionCapture,
    BookLevel,
    CanonicalMarket,
    ContractType,
    DataQuality,
    FundingAccrualMethod,
    FundingCashQuote,
    FundingQuality,
    LifecycleState,
    MarketType,
    MarketVolume,
    OrderBook,
    RouteDirection,
    Side,
    StreamHealth,
    TradeEvidence,
    Venue,
)
from risex_farmer.paper_broker import (
    CancellationReason,
    PaperEntryBroker,
    PaperOrderStatus,
    PaperVersionStatus,
    TradeProcessOutcome,
    VersionCloseReason,
)
from risex_farmer.scanner import MarketObservation, NoTradeReason, RoutePlan, scan_once


D = Decimal
NOW = datetime(2027, 5, 1, 12, tzinfo=UTC)
TARGET = NOW + timedelta(seconds=120)


def market(venue: Venue) -> CanonicalMarket:
    return CanonicalMarket(
        "ABC",
        venue,
        f"ABC-{venue.value}",
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
    bid: str = "99",
    ask: str = "101",
    depth: str = "10",
    healthy: bool = True,
    cash: str = "5",
) -> MarketObservation:
    normalized = market(venue)
    book = OrderBook(
        venue,
        normalized.venue_symbol,
        (BookLevel(D(bid), D(depth)),),
        (BookLevel(D(ask), D(depth)),),
        at,
        1,
    )
    funding = FundingCashQuote(
        venue,
        normalized.venue_symbol,
        at,
        at,
        TARGET,
        FundingQuality.PREDICTED,
        FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT,
        True,
        D(cash),
        D(cash),
        "synthetic",
    )
    health = StreamHealth(
        at,
        at,
        healthy,
        healthy,
        healthy,
        DataQuality.COMPLETE if healthy else DataQuality.DEGRADED,
    )
    return MarketObservation(
        normalized,
        MarketVolume(venue, normalized.venue_symbol, D("1000000"), at, "synthetic"),
        book,
        funding,
        health,
    )


def fractional_observation(
    venue: Venue,
    *,
    at: datetime,
    maker_price: str,
    depth: str = "10",
    cash: str = "5",
) -> MarketObservation:
    row = observation(
        venue,
        at=at,
        bid=str(D(maker_price) - D("0.03")),
        ask=str(D(maker_price) + D("0.01")),
        depth=depth,
        cash=cash,
    )
    return replace(
        row,
        market=replace(
            row.market,
            tick_size_raw=D("0.01"),
            quantity_step_raw=D("0.001"),
            minimum_quantity_raw=D("0.001"),
        ),
    )


def capture(row: MarketObservation, decision_at: datetime) -> BookExecutionCapture:
    return BookExecutionCapture(
        row.book, row.health, row.health.last_market_event_at,
        decision_at, 1, 0, 1,
    )


async def active_broker() -> tuple[PaperEntryBroker, MarketObservation, MarketObservation]:
    risex = observation(Venue.RISEX)
    hedge = observation(Venue.EXTENDED)
    snapshot = await scan_once((risex, hedge), NOW)
    assert snapshot.winner is not None
    broker = PaperEntryBroker()
    await broker.activate(snapshot, attempt_id="attempt-1", activated_at=NOW)
    return broker, risex, hedge


def trade(
    key: str,
    *,
    quantity: str = "5",
    price: str = "101",
    aggressor: Side | None = Side.BUY,
    exchange_at: datetime = NOW + timedelta(seconds=1),
    received_at: datetime = NOW + timedelta(seconds=2),
) -> TradeEvidence:
    return TradeEvidence(
        key,
        Venue.EXTENDED,
        "ABC-EXTENDED",
        exchange_at,
        received_at,
        "synthetic-raw",
        D(quantity),
        D(price),
        aggressor,
        True,
    )


def funding_recomputer(
    *,
    cash: str | None = "5",
    quality: FundingQuality = FundingQuality.PREDICTED,
    accrual: FundingAccrualMethod = FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT,
    eligible: bool = True,
    seen: list[datetime] | None = None,
):
    async def recompute(plan: RoutePlan, opened_at: datetime):
        if seen is not None:
            seen.append(opened_at)
        value = None if cash is None else D(cash)
        return tuple(
            FundingCashQuote(
                venue,
                venue_symbol,
                opened_at,
                opened_at,
                TARGET,
                quality,
                accrual,
                eligible,
                value,
                value,
                "synthetic-recomputed",
            )
            for venue, venue_symbol in (
                (Venue.RISEX, plan.risex_market.venue_symbol),
                (plan.hedge_venue, plan.hedge_market.venue_symbol),
            )
        )

    return recompute


@pytest.mark.asyncio
async def test_trade_through_is_strict_and_dedup_is_global() -> None:
    broker, risex, hedge = await active_broker()
    version_id = broker.state.order.active_version.version_id
    assert broker.state.order.order_type == "LIMIT"
    assert broker.state.order.post_only is True

    equal_touch = await broker.process_trade(
        trade("equal", price="100"),
        observed_version_id=version_id,
        processed_at=NOW + timedelta(seconds=2),
        risex_observation=risex,
        hedge_observation=hedge,
        recompute_funding=funding_recomputer(),
    )
    wrong_side = await broker.process_trade(
        trade("wrong", aggressor=Side.SELL),
        observed_version_id=version_id,
        processed_at=NOW + timedelta(seconds=2),
        risex_observation=risex,
        hedge_observation=hedge,
        recompute_funding=funding_recomputer(),
    )
    first = await broker.process_trade(
        trade("one", quantity="2"),
        observed_version_id=version_id,
        processed_at=NOW + timedelta(seconds=2),
        risex_observation=risex,
        hedge_observation=hedge,
        recompute_funding=funding_recomputer(),
    )
    duplicate = await broker.process_trade(
        trade("one", quantity="3"),
        observed_version_id=version_id,
        processed_at=NOW + timedelta(seconds=3),
        risex_observation=risex,
        hedge_observation=hedge,
        recompute_funding=funding_recomputer(),
    )

    assert equal_touch.outcome is wrong_side.outcome is TradeProcessOutcome.IGNORED
    assert first.outcome is TradeProcessOutcome.ACCUMULATED
    assert duplicate.detail == "DUPLICATE_EVENT_KEY"
    assert broker.state.order.active_version.cumulative_eligible_quantity == D("2")
    completed = await broker.process_trade(
        trade("two", quantity="3"),
        observed_version_id=version_id,
        processed_at=NOW + timedelta(seconds=3),
        risex_observation=risex,
        hedge_observation=hedge,
        recompute_funding=funding_recomputer(),
        risex_capture=capture(risex, NOW + timedelta(seconds=3)),
    )
    assert completed.outcome is TradeProcessOutcome.OPENED
    assert broker.state.processed_trade_keys == {"equal", "wrong", "one", "two"}


@pytest.mark.asyncio
async def test_replacement_resets_quantity_and_old_version_token_cannot_fill() -> None:
    broker, risex, hedge = await active_broker()
    old_id = broker.state.order.active_version.version_id
    await broker.process_trade(
        trade("partial", quantity="2"),
        observed_version_id=old_id,
        processed_at=NOW + timedelta(seconds=2),
        risex_observation=risex,
        hedge_observation=hedge,
        recompute_funding=funding_recomputer(),
    )
    refreshed = replace(
        broker.state.order.route_plan,
        logical_at=NOW + timedelta(seconds=10),
        hedge_entry_price=D("101"),
    )
    fresh_risex = observation(Venue.RISEX, at=NOW + timedelta(seconds=10))
    fresh_hedge = observation(
        Venue.EXTENDED,
        at=NOW + timedelta(seconds=10),
        bid="100",
        ask="102",
    )
    await broker.refresh(
        refreshed,
        fresh_risex,
        evaluated_at=NOW + timedelta(seconds=10),
        hedge_observation=fresh_hedge,
    )

    order = broker.state.order
    assert len(order.versions) == 2
    assert order.versions[0].status is PaperVersionStatus.REPLACED
    assert order.versions[0].close_reason == VersionCloseReason.PRICE_CHANGED.value
    assert order.active_version.cumulative_eligible_quantity == 0
    result = await broker.process_trade(
        trade("queued-old", quantity="5", price="102"),
        observed_version_id=old_id,
        processed_at=NOW + timedelta(seconds=11),
        risex_observation=fresh_risex,
        hedge_observation=fresh_hedge,
        recompute_funding=funding_recomputer(),
    )
    assert result.outcome is TradeProcessOutcome.IGNORED
    assert "queued-old" in broker.state.processed_trade_keys


@pytest.mark.asyncio
async def test_refresh_cancellation_precedence_and_exact_reasons() -> None:
    broker, _, _ = await active_broker()
    stale = observation(Venue.RISEX, at=NOW, healthy=False)
    state = await broker.refresh(None, stale, evaluated_at=TARGET - timedelta(seconds=5))
    assert state.order.cancellation_reason is CancellationReason.CUTOFF

    broker, _, _ = await active_broker()
    state = await broker.refresh(None, stale, evaluated_at=NOW + timedelta(seconds=10))
    assert state.order.cancellation_reason is CancellationReason.DATA_STALE

    broker, _, hedge = await active_broker()
    shallow = observation(Venue.RISEX, at=NOW + timedelta(seconds=10), depth="1")
    state = await broker.refresh(
        None,
        shallow,
        evaluated_at=NOW + timedelta(seconds=10),
        hedge_observation=hedge,
    )
    assert state.order.cancellation_reason is CancellationReason.RISEX_ENTRY_DEPTH_UNAVAILABLE

    broker, _, hedge = await active_broker()
    fresh = observation(Venue.RISEX, at=NOW + timedelta(seconds=10))
    state = await broker.refresh(
        None,
        fresh,
        evaluated_at=NOW + timedelta(seconds=10),
        hedge_observation=hedge,
    )
    assert state.order.cancellation_reason is CancellationReason.ROUTE_INVALID

    broker, _, hedge = await active_broker()
    negative = replace(
        broker.state.order.route_plan,
        logical_at=NOW + timedelta(seconds=10),
        planned_maker_net_pnl_usd=D("-1"),
        no_trade_reasons=(NoTradeReason.PLANNED_NET_PNL_NEGATIVE,),
    )
    zero_risex = observation(Venue.RISEX, at=NOW + timedelta(seconds=10), cash="0")
    zero_hedge = observation(Venue.EXTENDED, at=NOW + timedelta(seconds=10), cash="0")
    state = await broker.refresh(
        negative,
        zero_risex,
        evaluated_at=NOW + timedelta(seconds=10),
        hedge_observation=zero_hedge,
    )
    assert state.order.cancellation_reason is CancellationReason.PLANNED_NET_NEGATIVE


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "initial_maker_price",
        "refreshed_maker_price",
        "refreshed_depth",
        "initial_quantity",
        "refreshed_quantity",
    ),
    (
        ("126.91", "126.85", "10", D("3.939"), D("3.941")),
        ("125.20", "125.03", "10", D("3.993"), D("3.999")),
    ),
)
async def test_refresh_uses_locked_quantity_when_scanner_target_drifts(
    initial_maker_price: str,
    refreshed_maker_price: str,
    refreshed_depth: str,
    initial_quantity: Decimal,
    refreshed_quantity: Decimal,
) -> None:
    initial_risex = fractional_observation(
        Venue.RISEX, at=NOW, maker_price=initial_maker_price
    )
    initial_hedge = fractional_observation(
        Venue.EXTENDED, at=NOW, maker_price=initial_maker_price
    )
    initial_snapshot = await scan_once((initial_risex, initial_hedge), NOW)
    initial_plan = next(
        plan
        for plan in initial_snapshot.evaluations
        if plan.direction.value == "LONG_RISEX_SHORT_HEDGE"
    )
    assert initial_plan.canonical_quantity == initial_quantity
    assert initial_plan.entry_allowed
    broker = PaperEntryBroker()
    await broker.activate(
        replace(initial_snapshot, winner=initial_plan),
        attempt_id="locked-drift",
        activated_at=NOW,
    )
    order_before = broker.state.order
    assert order_before is not None
    assert order_before.locked_quantity == initial_quantity

    refreshed_at = NOW + timedelta(seconds=10)
    refreshed_risex = fractional_observation(
        Venue.RISEX,
        at=refreshed_at,
        maker_price=refreshed_maker_price,
        depth=refreshed_depth,
    )
    refreshed_hedge = fractional_observation(
        Venue.EXTENDED,
        at=refreshed_at,
        maker_price=refreshed_maker_price,
        depth=refreshed_depth,
    )
    refreshed_snapshot = await scan_once(
        (refreshed_risex, refreshed_hedge), refreshed_at
    )
    refreshed_plan = next(
        plan
        for plan in refreshed_snapshot.evaluations
        if plan.direction is initial_plan.direction
    )
    assert refreshed_plan.canonical_quantity == refreshed_quantity
    assert refreshed_plan.entry_allowed

    state = await broker.refresh(
        refreshed_plan,
        refreshed_risex,
        evaluated_at=refreshed_at,
        hedge_observation=refreshed_hedge,
    )

    order_after = state.order
    assert state.lifecycle_state is LifecycleState.ENTRY_MAKER_OPEN
    assert order_after is not None
    assert order_after.attempt_id == order_before.attempt_id
    assert order_after.locked_quantity == initial_quantity
    assert order_after.canonical_quantity == initial_quantity
    assert order_after.route_plan.canonical_quantity == initial_quantity
    assert len(order_after.versions) == 2
    assert order_after.versions[0].status is PaperVersionStatus.REPLACED
    assert order_after.active_version.cumulative_eligible_quantity == D("0")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("market_field", "value", "scanner_reason"),
    (
        ("quantity_step_raw", "2", None),
        ("minimum_quantity_raw", "6", NoTradeReason.MINIMUM_ORDER),
        ("minimum_notional_usd", "1000", NoTradeReason.MINIMUM_ORDER),
    ),
)
async def test_refresh_cancels_when_locked_quantity_fails_grid_or_minimum(
    market_field: str,
    value: str,
    scanner_reason: str | None,
) -> None:
    broker, _, _ = await active_broker()
    evaluated_at = NOW + timedelta(seconds=10)
    fresh_risex = observation(Venue.RISEX, at=evaluated_at)
    fresh_hedge = observation(Venue.EXTENDED, at=evaluated_at)
    fresh_hedge = replace(
        fresh_hedge,
        market=replace(fresh_hedge.market, **{market_field: D(value)}),
    )
    refreshed_snapshot = await scan_once(
        (fresh_risex, fresh_hedge), evaluated_at
    )
    refreshed_plan = next(
        plan
        for plan in refreshed_snapshot.evaluations
        if plan.direction is broker.state.order.route_key.direction
    )
    if scanner_reason is None:
        assert refreshed_plan.entry_allowed
    else:
        assert scanner_reason in refreshed_plan.no_trade_reasons

    state = await broker.refresh(
        refreshed_plan,
        fresh_risex,
        evaluated_at=evaluated_at,
        hedge_observation=fresh_hedge,
    )
    assert state.lifecycle_state is LifecycleState.FLAT
    assert state.order.cancellation_reason is CancellationReason.ROUTE_INVALID


@pytest.mark.asyncio
async def test_refresh_cancels_unknown_locked_funding_economics() -> None:
    broker, _, _ = await active_broker()
    evaluated_at = NOW + timedelta(seconds=10)
    fresh_risex = observation(Venue.RISEX, at=evaluated_at)
    fresh_hedge = observation(Venue.EXTENDED, at=evaluated_at)
    assert fresh_hedge.funding is not None
    fresh_hedge = replace(
        fresh_hedge,
        funding=replace(
            fresh_hedge.funding,
            eligibility_known=False,
            long_cash_per_canonical_base_usd=None,
            short_cash_per_canonical_base_usd=None,
        ),
    )
    refreshed_snapshot = await scan_once(
        (fresh_risex, fresh_hedge), evaluated_at
    )
    refreshed_plan = next(
        plan
        for plan in refreshed_snapshot.evaluations
        if plan.direction is broker.state.order.route_key.direction
    )
    assert NoTradeReason.FUNDING_ELIGIBILITY_UNKNOWN in refreshed_plan.no_trade_reasons

    state = await broker.refresh(
        refreshed_plan,
        fresh_risex,
        evaluated_at=evaluated_at,
        hedge_observation=fresh_hedge,
    )
    assert state.lifecycle_state is LifecycleState.FLAT
    assert state.order.cancellation_reason is CancellationReason.ROUTE_INVALID


@pytest.mark.asyncio
async def test_refresh_cancels_invalid_locked_post_only_quote() -> None:
    broker, _, _ = await active_broker()
    evaluated_at = NOW + timedelta(seconds=10)
    fresh_risex = observation(Venue.RISEX, at=evaluated_at)
    fresh_hedge = observation(Venue.EXTENDED, at=evaluated_at)
    assert fresh_hedge.book is not None
    fresh_hedge = replace(
        fresh_hedge,
        book=replace(
            fresh_hedge.book,
            bids=(BookLevel(D("101"), D("10")),),
            asks=(BookLevel(D("100"), D("10")),),
        ),
    )
    refreshed_snapshot = await scan_once(
        (fresh_risex, fresh_hedge), evaluated_at
    )
    refreshed_plan = next(
        plan
        for plan in refreshed_snapshot.evaluations
        if plan.direction is broker.state.order.route_key.direction
    )
    assert NoTradeReason.INVALID_BBO in refreshed_plan.no_trade_reasons

    state = await broker.refresh(
        refreshed_plan,
        fresh_risex,
        evaluated_at=evaluated_at,
        hedge_observation=fresh_hedge,
    )
    assert state.lifecycle_state is LifecycleState.FLAT
    assert state.order.cancellation_reason is CancellationReason.ROUTE_INVALID


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ("direction", "cycle"))
async def test_refresh_cancels_locked_route_direction_or_cycle_change(
    mutation: str,
) -> None:
    broker, _, _ = await active_broker()
    evaluated_at = NOW + timedelta(seconds=10)
    fresh_risex = observation(Venue.RISEX, at=evaluated_at)
    fresh_hedge = observation(Venue.EXTENDED, at=evaluated_at)
    refreshed_snapshot = await scan_once(
        (fresh_risex, fresh_hedge), evaluated_at
    )
    direction = broker.state.order.route_key.direction
    refreshed_plan = next(
        plan for plan in refreshed_snapshot.evaluations if plan.direction is direction
    )
    if mutation == "direction":
        changed_direction = next(
            candidate for candidate in RouteDirection if candidate is not direction
        )
        refreshed_plan = next(
            plan
            for plan in refreshed_snapshot.evaluations
            if plan.direction is changed_direction
        )
    else:
        assert refreshed_plan.target_cycle is not None
        refreshed_plan = replace(
            refreshed_plan,
            target_cycle=replace(
                refreshed_plan.target_cycle,
                cycle_id=f"{refreshed_plan.target_cycle.cycle_id}:changed",
            ),
        )

    state = await broker.refresh(
        refreshed_plan,
        fresh_risex,
        evaluated_at=evaluated_at,
        hedge_observation=fresh_hedge,
    )
    assert state.lifecycle_state is LifecycleState.FLAT
    assert state.order.cancellation_reason is CancellationReason.ROUTE_INVALID


@pytest.mark.asyncio
async def test_process_restart_contract_cancels_without_fill_reconstruction() -> None:
    broker, _, _ = await active_broker()
    state = await broker.cancel_for_process_restart(
        restarted_at=NOW + timedelta(seconds=4)
    )
    assert state.lifecycle_state is LifecycleState.FLAT
    assert state.position is None
    assert state.order.cancellation_reason is CancellationReason.PROCESS_RESTART


@pytest.mark.asyncio
async def test_cutoff_cancellation_requires_deadline_and_is_terminal() -> None:
    broker, _, _ = await active_broker()
    cutoff = broker.state.order.cutoff_at
    with pytest.raises(ValueError, match="premature"):
        await broker.cancel_for_cutoff(cancelled_at=cutoff - timedelta(microseconds=1))
    state = await broker.cancel_for_cutoff(cancelled_at=cutoff)
    assert state.lifecycle_state is LifecycleState.FLAT
    assert state.position is None
    assert state.order.cancellation_reason is CancellationReason.CUTOFF
    with pytest.raises(ValueError, match="no open paper entry order"):
        await broker.cancel_for_cutoff(cancelled_at=cutoff)


@pytest.mark.asyncio
async def test_fill_opens_both_legs_atomically_with_actual_times_and_fees() -> None:
    broker, risex, hedge = await active_broker()
    risex = replace(
        risex,
        book=replace(
            risex.book,
            asks=(BookLevel(D("101"), D("2")), BookLevel(D("103"), D("3"))),
        ),
    )
    version_id = broker.state.order.active_version.version_id
    exchange_at = NOW + timedelta(seconds=1)
    received_at = NOW + timedelta(seconds=20)
    opened_at = received_at
    seen: list[datetime] = []
    result = await broker.process_trade(
        trade("fill", exchange_at=exchange_at, received_at=received_at),
        observed_version_id=version_id,
        processed_at=opened_at,
        risex_observation=risex,
        hedge_observation=hedge,
        recompute_funding=funding_recomputer(seen=seen),
        risex_capture=capture(risex, opened_at),
    )

    position = result.state.position
    assert result.outcome is TradeProcessOutcome.OPENED
    assert result.state.order.status is PaperOrderStatus.FILLED
    assert position is not None
    assert position.hedge_maker_fill_exchange_at == exchange_at
    assert position.hedge_maker_fill_received_at == received_at
    assert position.risex_taker_fill_at == opened_at == position.position_opened_at
    assert seen == [opened_at]
    assert all(
        quote.assumed_or_actual_position_opened_at == opened_at
        for quote in position.recomputed_funding_quotes
    )
    assert position.hedge_maker_fill.fee.amount_usd == 0
    assert position.risex_taker_fill.canonical_price == D("102.2")
    assert position.risex_taker_fill.fee.amount_usd == D("511") * D("0.00021")
    assert position.canonical_quantity == D("5")
    assert result.state.lifecycle_state is LifecycleState.HOLDING


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("recompute", "expected_funding"),
    (
        (funding_recomputer(cash="0"), D("0")),
        (
            funding_recomputer(
                cash=None,
                quality=FundingQuality.UNKNOWN,
                accrual=FundingAccrualMethod.UNKNOWN,
                eligible=False,
            ),
            None,
        ),
    ),
)
async def test_unknown_or_equal_hold_value_exits_normally(recompute, expected_funding) -> None:
    broker, risex, hedge = await active_broker()
    result = await broker.process_trade(
        trade("fill"),
        observed_version_id=broker.state.order.active_version.version_id,
        processed_at=NOW + timedelta(seconds=3),
        risex_observation=risex,
        hedge_observation=hedge,
        recompute_funding=recompute,
        risex_capture=capture(risex, NOW + timedelta(seconds=3)),
    )
    assert result.state.position.remaining_target_funding_usd == expected_funding
    assert result.state.lifecycle_state is LifecycleState.EXITING_NORMAL


@pytest.mark.asyncio
async def test_cutoff_is_exchange_time_strict_even_when_receipt_is_late() -> None:
    broker, risex, hedge = await active_broker()
    cutoff = broker.state.order.cutoff_at
    received_at = cutoff + timedelta(seconds=30)
    risex = observation(Venue.RISEX, at=received_at)
    hedge = observation(Venue.EXTENDED, at=received_at)
    result = await broker.process_trade(
        trade(
            "before",
            exchange_at=cutoff - timedelta(microseconds=1),
            received_at=received_at,
        ),
        observed_version_id=broker.state.order.active_version.version_id,
        processed_at=received_at,
        risex_observation=risex,
        hedge_observation=hedge,
        recompute_funding=funding_recomputer(),
        risex_capture=capture(risex, received_at),
    )
    assert result.outcome is TradeProcessOutcome.OPENED

    broker, risex, hedge = await active_broker()
    result = await broker.process_trade(
        trade("at", exchange_at=cutoff),
        observed_version_id=broker.state.order.active_version.version_id,
        processed_at=NOW + timedelta(seconds=3),
        risex_observation=risex,
        hedge_observation=hedge,
        recompute_funding=funding_recomputer(),
    )
    assert result.outcome is TradeProcessOutcome.IGNORED
