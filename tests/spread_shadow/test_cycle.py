from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal as D
import hashlib

from risex_farmer.models import (
    BookLevel,
    CanonicalMarket,
    ContractType,
    MarketType,
    Venue,
)

from risex_spread_shadow.cycle import (
    CycleActionKind,
    CycleAttempt,
    CycleClock,
    CycleKernel,
    CycleKernelState,
    CycleReason,
    CycleScenario,
    CycleTerminalState,
)
from risex_spread_shadow.causal import CausalEvent, CausalOutcome
from risex_spread_shadow.economics import build_hypothetical_maker_quote
from risex_spread_shadow.models import (
    BookEvidence,
    DataGapEvidence,
    QuotePolicy,
    QuoteVersion,
    Side,
    SpreadDirection,
    TradeEvidence,
)


UTC = timezone.utc
NOW = datetime(2026, 9, 5, tzinfo=UTC)
ENTRY_ACTIVATION = 500_000_100
PARTIAL_RECEIVED = ENTRY_ACTIVATION + 1
ENTRY_CANCEL_EFFECTIVE = 1_000_000_101
ENTRY_HEDGE_DUE = 1_500_000_101


def _market(
    venue: Venue,
    symbol: str,
    *,
    minimum_quantity: str = "0.01",
    minimum_notional: str = "0",
) -> CanonicalMarket:
    return CanonicalMarket(
        canonical_asset="BTC",
        venue=venue,
        venue_symbol=symbol,
        market_type=MarketType.PERPETUAL,
        contract_type=ContractType.LINEAR,
        base_multiplier=D("1"),
        quote_asset="USDC",
        settlement_asset="USDC",
        tick_size_raw=D("1"),
        quantity_step_raw=D("0.01"),
        minimum_quantity_raw=D(minimum_quantity),
        minimum_notional_usd=D(minimum_notional),
        minimum_fee_notional_usd=None,
        is_active=True,
        is_rfq=False,
        is_off_hours=False,
    )


def _book(
    venue: Venue,
    *,
    received: int,
    revision: int,
    bids: tuple[tuple[str, str], ...] = (("99", "10"),),
    asks: tuple[tuple[str, str], ...] = (("101", "10"),),
    session: str | None = None,
    recovery: int = 0,
    fresh: bool = True,
    sequence_valid: bool = True,
    checksum_valid: bool = True,
) -> BookEvidence:
    session = session or ("risex-s2" if venue is Venue.RISEX else "lighter-s2")
    return BookEvidence(
        venue=venue,
        canonical_market="BTC",
        bids=tuple(BookLevel(D(price), D(quantity)) for price, quantity in bids),
        asks=tuple(BookLevel(D(price), D(quantity)) for price, quantity in asks),
        received_monotonic_ns=received,
        stream_session_id=session,
        recovery_generation=recovery,
        book_revision=revision,
        sequence=revision,
        checksum=revision,
        sequence_valid=sequence_valid,
        checksum_valid=checksum_valid,
        received_utc=NOW,
        fresh=fresh,
        ingress_received_monotonic_ns=received,
        normalized_ready_monotonic_ns=received,
        block_number=(10_000 + revision if venue is Venue.RISEX else None),
        log_index=(revision if venue is Venue.RISEX else None),
        worker_timestamp=(received if venue is Venue.RISEX else None),
    )


def _trade(
    key: str,
    *,
    received: int,
    quantity: str,
    price: str,
    aggressor: Side = Side.BUY,
    normalized: int | None = None,
    decision_ready: int | None = None,
    block_number: int | None = None,
    session: str = "risex-s2",
    recovery: int = 0,
) -> TradeEvidence:
    digest = hashlib.sha256(key.encode()).hexdigest()
    maker_order_id = "0x" + digest[:48]
    taker_order_id = "0x" + hashlib.sha256(("taker:" + key).encode()).hexdigest()[:48]
    source_trade_id = f"{maker_order_id}-{taker_order_id}"
    return TradeEvidence(
        trade_event_key=f"RISEX|BTC|{source_trade_id}",
        venue=Venue.RISEX,
        canonical_market="BTC",
        canonical_price=D(price),
        canonical_quantity=D(quantity),
        aggressor_side=aggressor,
        received_utc=NOW,
        received_monotonic_ns=received,
        stream_session_id=session,
        recovery_generation=recovery,
        ingress_received_monotonic_ns=received,
        normalized_ready_monotonic_ns=received if normalized is None else normalized,
        decision_ready_monotonic_ns=decision_ready,
        source_trade_id=source_trade_id,
        maker_order_id=maker_order_id,
        taker_order_id=taker_order_id,
        maker="0x" + "11" * 32,
        taker="0x" + "22" * 32,
        tx_hash="0x" + hashlib.sha256(("tx:" + key).encode()).hexdigest(),
        block_number=20_000 + received if block_number is None else block_number,
        log_index=received,
        worker_timestamp=received,
    )


def _version(
    version_id: str = "s2-prefix",
    *,
    decision_ready: int = 100,
    risex_bids: tuple[tuple[str, str], ...] = (("99", "10"),),
    risex_asks: tuple[tuple[str, str], ...] = (("101", "10"),),
    lighter_bids: tuple[tuple[str, str], ...] = (("99", "10"),),
    lighter_asks: tuple[tuple[str, str], ...] = (("100", "10"),),
    risex_market: CanonicalMarket | None = None,
    lighter_market: CanonicalMarket | None = None,
) -> tuple[QuoteVersion, tuple[BookEvidence, ...]]:
    risex_market = risex_market or _market(Venue.RISEX, "BTC/USDC")
    lighter_market = lighter_market or _market(Venue.LIGHTER, "BTC")
    initial_risex = _book(
        Venue.RISEX,
        received=0,
        revision=1,
        bids=risex_bids,
        asks=risex_asks,
    )
    initial_lighter = _book(
        Venue.LIGHTER,
        received=0,
        revision=1,
        bids=lighter_bids,
        asks=lighter_asks,
    )
    policy = QuotePolicy(
        canonical_market="BTC",
        direction=SpreadDirection.RISEX_SELL_LIGHTER_BUY,
        target_notional_usd=D("100"),
        target_margin_bps=D("1"),
        risex_maker_fee_rate=D("0.0001"),
        lighter_taker_fee_rate=D("0"),
        risex_fee_source="FIXTURE_RISEX_MAKER",
        lighter_fee_source="FIXTURE_LIGHTER_TAKER",
        risex_market=risex_market,
        lighter_market=lighter_market,
        risex_best_bid=D(risex_bids[0][0]),
        risex_best_ask=D(risex_asks[0][0]),
        risex_tick_size=D("1"),
        fee_observed_or_configured_at=NOW,
    )
    quote = build_hypothetical_maker_quote(policy, initial_lighter)
    assert quote.is_active
    version = QuoteVersion(
        version_id=version_id,
        quote=quote,
        quote_created_utc=NOW,
        quote_created_monotonic_ns=0,
        stream_session_id="risex-s2",
        recovery_generation=0,
        hedge_stream_session_id="lighter-s2",
        hedge_recovery_generation=0,
        risex_book_revision=initial_risex.book_revision,
        lighter_book_revision=initial_lighter.book_revision,
        risex_book_revision_id=initial_risex.book_revision_id,
        lighter_book_revision_id=initial_lighter.book_revision_id,
        ingress_received_monotonic_ns=0,
        normalized_ready_monotonic_ns=0,
        decision_ready_monotonic_ns=decision_ready,
    )
    return version, (initial_risex, initial_lighter)


def test_prefix_mutates_partial_fill_and_keeps_hedge_until_explicit_due_boundary() -> None:
    version, source_books = _version()
    kernel = CycleKernel()

    admission = kernel.admit(version, source_books=source_books)
    assert admission.accepted
    assert kernel.state() is CycleKernelState.PENDING

    kernel.advance(
        _trade(
            "prefix-partial",
            received=PARTIAL_RECEIVED,
            quantity="0.50",
            price="102",
        )
    )
    prefix = kernel.snapshot()
    assert prefix is not None
    assert prefix.status is CycleTerminalState.PENDING
    assert prefix.entry_quantity == D("0.50")
    assert prefix.entry_measurement is not None
    assert prefix.entry_measurement.observed_filled_quantity == D("0.50")
    assert prefix.positions.risex_signed_quantity == D("-0.50")
    assert prefix.positions.lighter_signed_quantity == D("0")
    assert len(prefix.fills) == 1
    assert prefix.cashflows[0].gross_cashflow_usd == D("50.50")
    assert {action.action_id for action in prefix.pending_actions} == {
        "entry-maker",
        "entry-cancel",
    }

    blocked = kernel.admit(version)
    assert not blocked.accepted
    assert blocked.reason == CycleReason.ACTIVE_CYCLE.value

    kernel.advance_clock(PARTIAL_RECEIVED + 1)
    before_cancel = kernel.snapshot()
    assert before_cancel is not None
    assert before_cancel.status is CycleTerminalState.PENDING
    assert not any(action.kind.value == "ENTRY_HEDGE" for action in before_cancel.actions)
    assert before_cancel.positions.lighter_signed_quantity == D("0")

    kernel.advance_clock(ENTRY_CANCEL_EFFECTIVE)
    after_cancel = kernel.snapshot()
    assert after_cancel is not None
    assert after_cancel.status is CycleTerminalState.PENDING
    assert after_cancel.positions.risex_signed_quantity == D("-0.50")
    assert after_cancel.positions.lighter_signed_quantity == D("0")
    assert all(action.status.value != "PENDING" for action in after_cancel.actions if action.action_id in {"entry-maker", "entry-cancel"})

    hedge_risex = _book(
        Venue.RISEX,
        received=1_200_000_000,
        revision=2,
        bids=(("99", "10"),),
        asks=(("105", "10"),),
    )
    hedge_lighter = _book(
        Venue.LIGHTER,
        received=1_200_000_000,
        revision=2,
        bids=(("103", "10"),),
        asks=(("104", "10"),),
    )
    kernel.advance(hedge_risex)
    kernel.advance(hedge_lighter)
    still_waiting = kernel.snapshot()
    assert still_waiting is not None
    assert still_waiting.positions.lighter_signed_quantity == D("0")
    assert not any(action.action_id == "entry-hedge" for action in still_waiting.actions)

    kernel.advance_clock(ENTRY_HEDGE_DUE)
    hedged = kernel.snapshot()
    assert hedged is not None
    assert hedged.status is CycleTerminalState.PENDING
    assert hedged.positions.risex_signed_quantity == D("-0.50")
    assert hedged.positions.lighter_signed_quantity == D("0.50")
    assert hedged.hedged_quantity == D("0.50")
    assert any(action.action_id == "entry-hedge" and action.status.value == "COMPLETED" for action in hedged.actions)
    assert any(action.action_id == "exit-maker" and action.status.value == "PENDING" for action in hedged.actions)


def _full_cycle_events(
    *,
    close_bid: str = "99",
    entry_quantity: str = "1.00",
    exit_quantity: str = "1.00",
    exit_received: int = 1_600_000_300,
    exit_normalized: int | None = None,
    exit_decision_ready: int | None = None,
    hedge_received: int = 900_000_000,
    close_received: int = 1_900_000_300,
) -> tuple[object, ...]:
    """Return an ordered fixture whose clock advances only at observed inputs."""

    return (
        _trade(
            "cycle-entry",
            received=500_000_200,
            quantity=entry_quantity,
            price="102",
        ),
        _book(
            Venue.RISEX,
            received=hedge_received,
            revision=2,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        ),
        _book(
            Venue.LIGHTER,
            received=hedge_received,
            revision=2,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        ),
        _trade(
            "cycle-exit",
            received=exit_received,
            normalized=exit_normalized,
            decision_ready=exit_decision_ready,
            quantity=exit_quantity,
            price="97",
            aggressor=Side.SELL,
        ),
        _book(
            Venue.RISEX,
            received=close_received,
            revision=3,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        ),
        _book(
            Venue.LIGHTER,
            received=close_received,
            revision=3,
            bids=((close_bid, "10"),),
            asks=((str(D(close_bid) + D("1")), "10"),),
        ),
    )


def _independent_cycle_totals(result) -> tuple[D, D, D, D, D, D]:
    """Recompute signed fills, fees, cash flows, positions, and turnover locally."""

    risex = D("0")
    lighter = D("0")
    fees = D("0")
    cashflow = D("0")
    turnover = D("0")
    scenario_cost = D("0")
    for fill in result.fills:
        notional = fill.quantity * fill.price
        turnover += notional
        rate = (
            D("0.0001")
            if fill.venue is Venue.RISEX and fill.liquidity_role.value == "MAKER"
            else D("0.0003")
            if fill.venue is Venue.RISEX
            else D("0")
        )
        fees += notional * rate
        if result.scenario is CycleScenario.STRESS and fill.venue is Venue.RISEX:
            scenario_cost += notional * D("0.0001")
        cashflow += notional if fill.side is Side.SELL else -notional
        if fill.venue is Venue.RISEX:
            risex += fill.quantity if fill.side is Side.BUY else -fill.quantity
        else:
            lighter += fill.quantity if fill.side is Side.BUY else -fill.quantity
    return risex, lighter, fees, cashflow, scenario_cost, turnover


def test_positive_full_cycle_is_flat_and_checker_matches_execution_only_pnl() -> None:
    version, source_books = _version("s2-positive")
    kernel = CycleKernel()
    result = kernel.run(
        version,
        _full_cycle_events(),
        source_books=source_books,
        end_monotonic_ns=2_100_000_300,
    )

    assert result.status is CycleTerminalState.NORMAL
    assert result.is_flat
    assert result.cashflow_complete
    assert result.pnl_usd is not None and result.pnl_usd > D("0")
    assert result.entry_edge_usd is not None and result.entry_edge_usd > D("0")
    assert result.positions.risex_signed_quantity == D("0")
    assert result.positions.lighter_signed_quantity == D("0")
    assert not result.pending_actions
    assert all(action.status.value != "PENDING" for action in result.actions)
    assert [fill.side for fill in result.fills] == [Side.SELL, Side.BUY, Side.BUY, Side.SELL]
    assert [(fill.quantity, fill.price, fill.notional_usd) for fill in result.fills] == [
        (D("1.00"), D("101"), D("101.00")),
        (D("1.00"), D("100"), D("100.00")),
        (D("1.00"), D("98"), D("98.00")),
        (D("1.00"), D("99"), D("99.00")),
    ]
    risex, lighter, fees, cashflow, scenario_cost, turnover = _independent_cycle_totals(result)
    assert (risex, lighter) == (D("0"), D("0"))
    assert fees == result.ledger.total_fees_usd
    assert cashflow == result.ledger.signed_cashflow_usd
    assert scenario_cost == result.ledger.scenario_cost_usd == D("0")
    assert turnover == result.turnover_usd
    assert fees == D("0.0199")
    assert cashflow == D("2.00")
    assert turnover == D("398.00")
    assert result.pnl_usd == D("1.9801") == cashflow - fees - scenario_cost


def test_positive_entry_edge_can_end_in_negative_complete_cycle() -> None:
    version, source_books = _version("s2-negative")
    result = CycleKernel().run(
        version,
        _full_cycle_events(close_bid="90", close_received=1_900_000_300),
        source_books=source_books,
        end_monotonic_ns=2_100_000_300,
    )

    assert result.status is CycleTerminalState.NORMAL
    assert result.is_flat
    assert result.entry_edge_usd is not None and result.entry_edge_usd > D("0")
    assert result.pnl_usd is not None and result.pnl_usd < D("0")
    assert result.funding_status == "UNKNOWN"
    assert result.pnl_usd == (
        _independent_cycle_totals(result)[3]
        - _independent_cycle_totals(result)[2]
        - _independent_cycle_totals(result)[4]
    )


def test_stream_and_replay_share_the_same_transitions_and_outcome() -> None:
    version, source_books = _version("s2-replay")
    events = _full_cycle_events()
    streamed = CycleKernel().run(
        version,
        events,
        source_books=source_books,
        end_monotonic_ns=2_100_000_300,
    )
    replayed = CycleKernel().replay(
        version,
        events,
        source_books=source_books,
        end_monotonic_ns=2_100_000_300,
    )
    assert streamed == replayed
    assert streamed.entry_measurement is not None
    assert streamed.entry_measurement.outcome is CausalOutcome.FULL_FILL
    assert streamed.exit_measurement is not None
    assert streamed.exit_measurement.outcome is CausalOutcome.FULL_FILL


def test_partial_exit_closes_queued_quantity_then_forces_remaining_without_over_close() -> None:
    version, source_books = _version("s2-partial-exit")
    kernel = CycleKernel()
    admission = kernel.admit(version, source_books=source_books)
    assert admission.accepted
    events = _full_cycle_events(
        exit_quantity="0.40",
        exit_normalized=1_700_000_300,
        exit_decision_ready=1_800_000_300,
        close_received=2_200_000_300,
    )
    for event in events[:4]:
        kernel.advance(event)
    before_close = kernel.snapshot()
    assert before_close is not None
    assert before_close.status is CycleTerminalState.PENDING
    assert before_close.positions.risex_signed_quantity == D("-0.60")
    assert before_close.positions.lighter_signed_quantity == D("1.00")
    assert any(action.kind is CycleActionKind.EXIT_HEDGE_CLOSE and action.status.value == "PENDING" for action in before_close.actions)
    assert kernel.state() is CycleKernelState.PENDING
    kernel.advance(events[4])
    kernel.advance(events[5])
    kernel.advance_clock(2_300_000_300)
    kernel.advance(
        _book(
            Venue.RISEX,
            received=2_600_000_300,
            revision=4,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        )
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=2_600_000_300,
            revision=4,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        )
    )
    result = kernel.finish(end_monotonic_ns=2_800_000_300)

    assert result.status is CycleTerminalState.FORCED
    assert result.is_flat
    assert result.positions.is_zero
    assert sum((fill.quantity for fill in result.fills if fill.action_id == "exit-maker"), D("0")) == D("0.40")
    assert sum((fill.quantity for fill in result.fills if fill.action_id == "exit-close:0"), D("0")) == D("0.40")
    assert sum((fill.quantity for fill in result.fills if fill.action_id == "forced-risex"), D("0")) == D("0.60")
    assert sum((fill.quantity for fill in result.fills if fill.action_id == "forced-lighter"), D("0")) == D("0.60")
    assert all(fill.quantity > D("0") for fill in result.fills)
    assert all(action.status.value not in {"PENDING", "UNRESOLVED"} for action in result.actions)
    assert [(fill.action_id, fill.quantity, fill.price, fill.notional_usd) for fill in result.fills] == [
        ("entry-maker", D("1.00"), D("101"), D("101.00")),
        ("entry-hedge", D("1.00"), D("100"), D("100.00")),
        ("exit-maker", D("0.40"), D("98"), D("39.20")),
        ("exit-close:0", D("0.40"), D("99"), D("39.60")),
        ("forced-risex", D("0.60"), D("105"), D("63.00")),
        ("forced-lighter", D("0.60"), D("99"), D("59.40")),
    ]
    assert result.ledger.total_fees_usd == D("0.03292")
    assert result.ledger.signed_cashflow_usd == D("-2.20")
    assert result.turnover_usd == D("402.20")
    assert result.pnl_usd == D("-2.23292")


def test_queued_exit_partials_can_complete_before_cancel_effective_and_close_each_once() -> None:
    version, source_books = _version("s2-exit-queued")
    kernel = CycleKernel()
    assert kernel.admit(version, source_books=source_books).accepted
    for event in (
        _trade("exit-queued-entry", received=500_000_200, quantity="1.00", price="102"),
        _book(Venue.RISEX, received=900_000_200, revision=2, bids=(("99", "10"),), asks=(("105", "10"),)),
        _book(Venue.LIGHTER, received=900_000_200, revision=2, bids=(("99", "10"),), asks=(("100", "10"),)),
        _trade("exit-queued-first", received=1_600_000_300, quantity="0.40", price="97", aggressor=Side.SELL),
        _trade("exit-queued-second", received=1_700_000_300, quantity="0.60", price="97", aggressor=Side.SELL),
        _book(Venue.RISEX, received=1_900_000_300, revision=3, bids=(("99", "10"),), asks=(("105", "10"),)),
        _book(Venue.LIGHTER, received=1_900_000_300, revision=3, bids=(("99", "10"),), asks=(("100", "10"),)),
    ):
        kernel.advance(event)
    result = kernel.finish(end_monotonic_ns=2_200_000_300)
    assert result.status is CycleTerminalState.NORMAL
    assert result.is_flat
    assert sum((fill.quantity for fill in result.fills if fill.action_id == "exit-maker"), D("0")) == D("1.00")
    assert sum((fill.quantity for fill in result.fills if fill.action_id == "exit-close:0"), D("0")) == D("0.40")
    assert sum((fill.quantity for fill in result.fills if fill.action_id == "exit-close:1"), D("0")) == D("0.60")
    cancel = next(action for action in result.actions if action.action_id == "exit-cancel")
    assert cancel.status.value == "NOT_REQUIRED"
    assert cancel.remaining_quantity == D("0")
    assert not any(action.status.value in {"PENDING", "UNRESOLVED"} for action in result.actions)


def test_duplicate_volume_is_ignored_and_conflicting_identity_halts() -> None:
    version, source_books = _version("s2-duplicates")
    duplicate = _trade("duplicate", received=500_000_200, quantity="0.40", price="102")
    kernel = CycleKernel()
    assert kernel.admit(version, source_books=source_books).accepted
    kernel.advance(duplicate)
    kernel.advance(duplicate)
    prefix = kernel.snapshot()
    assert prefix is not None
    assert prefix.entry_quantity == D("0.40")
    assert prefix.entry_measurement is not None
    assert prefix.entry_measurement.duplicate_event_count == 1

    conflicting = _trade("duplicate", received=500_000_201, quantity="0.60", price="103")
    kernel.advance(conflicting)
    halted = kernel.last_result()
    assert halted is not None
    assert halted.status is CycleTerminalState.UNRESOLVED
    assert CycleReason.DUPLICATE_CONFLICT.value in halted.reason_codes
    assert halted.entry_quantity == D("0.40")
    assert halted.positions.risex_signed_quantity == D("-0.40")
    assert kernel.state() is CycleKernelState.UNRESOLVED_HALTED
    assert not kernel.admit(version).accepted


def test_late_processed_entry_fill_is_explicit_uncertainty_after_exit_commit() -> None:
    version, source_books = _version("s2-late-entry")
    kernel = CycleKernel()
    assert kernel.admit(version, source_books=source_books).accepted
    for event in (
        _trade("late-entry-prefix", received=500_000_200, quantity="0.50", price="102"),
        _book(Venue.RISEX, received=1_200_000_000, revision=2, bids=(("99", "10"),), asks=(("105", "10"),)),
        _book(Venue.LIGHTER, received=1_200_000_000, revision=2, bids=(("103", "10"),), asks=(("104", "10"),)),
    ):
        kernel.advance(event)
    kernel.advance_clock(1_500_000_200)
    prefix = kernel.snapshot()
    assert prefix is not None and prefix.exit_measurement is not None

    late = _trade(
        "late-entry-after-hedge",
        received=750_000_000,
        quantity="0.25",
        price="102",
        normalized=1_600_000_000,
    )
    kernel.advance(late)
    halted = kernel.last_result()
    assert halted is not None
    assert halted.status is CycleTerminalState.UNRESOLVED
    assert CycleReason.ENTRY_CAUSAL_UNCERTAINTY.value in halted.reason_codes
    assert CycleReason.LATE_OLDER_EVENT.value in halted.reason_codes
    assert halted.entry_measurement is not None
    assert halted.entry_measurement.decisions[-1].classification == "UNCERTAIN"
    assert halted.positions.risex_signed_quantity == D("-0.50")
    assert halted.positions.lighter_signed_quantity == D("0.50")


def _terminal_partial_cycle_events() -> tuple[object, ...]:
    return (
        _trade("terminal-entry", received=500_000_200, quantity="0.50", price="102"),
        _book(
            Venue.RISEX,
            received=1_200_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        ),
        _book(
            Venue.LIGHTER,
            received=1_200_000_000,
            revision=2,
            bids=(("103", "10"),),
            asks=(("104", "10"),),
        ),
        _trade(
            "terminal-exit",
            received=2_100_000_300,
            quantity="0.50",
            price="101",
            aggressor=Side.SELL,
        ),
        _book(
            Venue.RISEX,
            received=2_400_000_300,
            revision=3,
            bids=(("100", "10"),),
            asks=(("105", "10"),),
        ),
        _book(
            Venue.LIGHTER,
            received=2_400_000_300,
            revision=3,
            bids=(("100", "10"),),
            asks=(("105", "10"),),
        ),
    )


def test_late_terminal_entry_evidence_invalidates_stream_and_replay_without_new_fill() -> None:
    version, source_books = _version("s2-late-terminal")
    late = _trade(
        "terminal-late-entry",
        received=750_000_000,
        quantity="0.25",
        price="102",
        normalized=2_700_000_300,
    )
    events = _terminal_partial_cycle_events() + (CycleClock(2_600_000_300), late)

    streamed = CycleKernel().run(version, events, source_books=source_books)
    replayed = CycleKernel().replay(version, events, source_books=source_books)

    assert streamed == replayed
    assert streamed.status is CycleTerminalState.UNRESOLVED
    assert not streamed.is_flat
    assert not streamed.positions.authoritative
    assert streamed.entry_quantity == D("0.50")
    assert len(streamed.fills) == 4
    assert all(fill.evidence_id != late.trade_event_key for fill in streamed.fills)
    assert streamed.entry_measurement is not None
    assert streamed.entry_measurement.outcome is CausalOutcome.CAUSAL_UNCERTAIN
    assert streamed.entry_measurement.decisions[-1].classification == "UNCERTAIN"
    assert CycleReason.ENTRY_CAUSAL_UNCERTAINTY.value in streamed.reason_codes
    assert CycleReason.LATE_OLDER_EVENT.value in streamed.reason_codes


def _shifted_full_cycle_events(shift: int, *, prefix: str) -> tuple[object, ...]:
    return (
        _trade(f"{prefix}-entry", received=500_000_200 + shift, quantity="1.00", price="102"),
        _book(
            Venue.RISEX,
            received=900_000_000 + shift,
            revision=2,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        ),
        _book(
            Venue.LIGHTER,
            received=900_000_000 + shift,
            revision=2,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        ),
        _trade(
            f"{prefix}-exit",
            received=1_600_000_300 + shift,
            quantity="1.00",
            price="97",
            aggressor=Side.SELL,
        ),
        _book(
            Venue.RISEX,
            received=1_900_000_300 + shift,
            revision=3,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        ),
        _book(
            Venue.LIGHTER,
            received=1_900_000_300 + shift,
            revision=3,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        ),
    )


def _fresh_reentry_version(version_id: str, decision: int) -> tuple[object, tuple[object, ...]]:
    version, source_books = _version(version_id, decision_ready=decision)
    received = decision - 100
    fresh_books = tuple(
        replace(
            book,
            received_monotonic_ns=received,
            ingress_received_monotonic_ns=received,
            normalized_ready_monotonic_ns=received,
        )
        for book in source_books
    )
    return version, fresh_books


def test_late_terminal_identity_survives_two_later_completions_with_bounded_retention() -> None:
    first, first_books = _version("s2-retained-first")
    kernel = CycleKernel()
    assert kernel.admit(first, source_books=first_books).accepted
    for event in _terminal_partial_cycle_events():
        kernel.advance(event)
    kernel.advance_clock(2_600_000_300)
    first_result = kernel.last_result()
    assert first_result is not None and first_result.is_flat

    second_decision = 3_100_000_300
    second, second_books = _fresh_reentry_version("s2-retained-second", second_decision)
    assert kernel.admit(second, source_books=second_books).accepted
    second_shift = second_decision - 100
    for event in _shifted_full_cycle_events(second_shift, prefix="retained-second"):
        kernel.advance(event)
    kernel.advance_clock(second_decision + 2_100_000_200)
    second_result = kernel.last_result()
    assert second_result is not None and second_result.is_flat

    third_decision = 6_300_000_300
    third, third_books = _fresh_reentry_version("s2-retained-third", third_decision)
    assert kernel.admit(third, source_books=third_books).accepted
    third_shift = third_decision - 100
    for event in _shifted_full_cycle_events(third_shift, prefix="retained-third"):
        kernel.advance(event)
    kernel.advance_clock(third_decision + 2_100_000_200)
    third_result = kernel.last_result()
    assert third_result is not None and third_result.is_flat
    assert len(third_result.fills) == 4

    late = _trade(
        "retained-first-late-entry",
        received=750_000_000,
        quantity="0.25",
        price="102",
        normalized=2_700_000_300,
    )
    kernel.advance(late)
    propagated = kernel.last_result()
    assert propagated is not None
    assert propagated.status is CycleTerminalState.UNRESOLVED
    assert not propagated.is_flat
    assert not propagated.positions.authoritative
    assert len(propagated.fills) == 4
    assert CycleReason.ENTRY_CAUSAL_UNCERTAINTY.value in propagated.reason_codes
    assert CycleReason.LATE_OLDER_EVENT.value in propagated.reason_codes
    assert kernel.state() is CycleKernelState.UNRESOLVED_HALTED


def test_exact_replay_of_prior_terminal_event_is_benign_and_not_refilled() -> None:
    first, first_books = _version("s2-cross-attempt-duplicate-first")
    kernel = CycleKernel()
    assert kernel.admit(first, source_books=first_books).accepted
    first_events = _terminal_partial_cycle_events()
    for event in first_events:
        kernel.advance(event)
    kernel.advance_clock(2_600_000_300)
    first_result = kernel.last_result()
    assert first_result is not None and first_result.is_flat

    second_decision = 3_100_000_300
    second, second_books = _fresh_reentry_version("s2-cross-attempt-duplicate-second", second_decision)
    assert kernel.admit(second, source_books=second_books).accepted
    progress = kernel.advance(first_events[0])
    current = kernel.snapshot()

    assert progress.kernel_state is CycleKernelState.PENDING
    assert current is not None
    assert current.status is CycleTerminalState.PENDING
    assert current.entry_quantity == D("0")
    assert not current.fills
    assert current.entry_measurement is not None
    assert current.entry_measurement.duplicate_event_count == 1
    assert current.entry_measurement.outcome is CausalOutcome.NO_FILL
    assert CycleReason.ENTRY_CAUSAL_UNCERTAINTY.value not in current.reason_codes


def test_default_terminal_retention_supports_twenty_sequential_complete_cycles() -> None:
    kernel = CycleKernel()
    assert kernel.terminal_retention_capacity >= 20

    for index in range(20):
        decision = 100 + index * 3_000_000_000
        version, source_books = _fresh_reentry_version(f"s2-retention-capacity-{index}", decision)
        assert kernel.admit(version, source_books=source_books).accepted
        shift = decision - 100
        for event in _shifted_full_cycle_events(shift, prefix=f"retention-capacity-{index}"):
            kernel.advance(event)
        result = kernel.finish(end_monotonic_ns=decision + 2_100_000_300)
        assert result.status is CycleTerminalState.NORMAL
        assert result.is_flat

    assert len(kernel.admissions_for()) == 20
    assert kernel.state() is CycleKernelState.FLAT


def test_terminal_retention_exhaustion_halts_only_at_explicit_capacity() -> None:
    kernel = CycleKernel(terminal_retention_capacity=2)
    for index in range(2):
        decision = 100 + index * 3_000_000_000
        version, source_books = _fresh_reentry_version(f"s2-explicit-capacity-{index}", decision)
        assert kernel.admit(version, source_books=source_books).accepted
        shift = decision - 100
        for event in _shifted_full_cycle_events(shift, prefix=f"explicit-capacity-{index}"):
            kernel.advance(event)
        assert kernel.finish(end_monotonic_ns=decision + 2_100_000_300).is_flat

    version, source_books = _fresh_reentry_version("s2-explicit-capacity-third", 6_000_000_100)
    exhausted = kernel.admit(version, source_books=source_books)
    assert not exhausted.accepted
    assert exhausted.reason == CycleReason.TERMINAL_RETENTION_EXHAUSTED.value
    assert kernel.state() is CycleKernelState.UNRESOLVED_HALTED


def test_terminal_identity_conflict_is_checked_after_full_fill() -> None:
    version, source_books = _version("s2-full-terminal-conflict")
    events = _full_cycle_events()
    kernel = CycleKernel()
    clean = kernel.run(version, events, source_books=source_books, end_monotonic_ns=2_100_000_300)
    assert clean.status is CycleTerminalState.NORMAL
    assert clean.is_flat

    conflicting = replace(events[0], canonical_quantity=D("0.50"))
    kernel.advance(conflicting)
    halted = kernel.last_result()
    assert halted is not None
    assert halted.status is CycleTerminalState.UNRESOLVED
    assert not halted.is_flat
    assert halted.positions.is_zero
    assert len(halted.fills) == 4
    assert CycleReason.DUPLICATE_CONFLICT.value in halted.reason_codes


def test_run_sequence_refreshes_prior_results_after_late_terminal_conflict() -> None:
    first, first_books = _version("s2-sequence-conflict-first")
    first_events = _full_cycle_events()
    second_decision = 3_100_000_300
    second, second_books = _fresh_reentry_version("s2-sequence-conflict-second", second_decision)
    conflicting = replace(first_events[0], canonical_quantity=D("0.50"))
    attempts = (
        CycleAttempt(
            quote_version=first,
            events=first_events,
            source_books=first_books,
            end_monotonic_ns=2_100_000_300,
        ),
        CycleAttempt(
            quote_version=second,
            events=(conflicting,),
            source_books=second_books,
            end_monotonic_ns=second_decision + 1_000_000_000,
        ),
    )

    results = CycleKernel().run_sequence(attempts)

    assert len(results) == 2
    assert results[0].status is CycleTerminalState.UNRESOLVED
    assert not results[0].is_flat
    assert not results[0].positions.authoritative
    assert CycleReason.DUPLICATE_CONFLICT.value in results[0].reason_codes
    assert len(results[0].fills) == 4
    assert results[1].status is CycleTerminalState.UNRESOLVED
    assert not results[1].is_flat


def test_late_processed_exit_fill_across_cancel_boundary_is_uncertain_and_not_over_closed() -> None:
    version, source_books = _version("s2-late-exit")
    kernel = CycleKernel()
    assert kernel.admit(version, source_books=source_books).accepted
    events = _full_cycle_events(exit_quantity="0.40", close_received=2_000_000_300)
    for event in events[:4]:
        kernel.advance(event)
    kernel.advance(events[4])
    kernel.advance(events[5])
    kernel.advance_clock(2_100_000_300)
    pending_force = kernel.snapshot()
    assert pending_force is not None
    assert pending_force.status is CycleTerminalState.PENDING
    assert any(action.action_id == "forced-risex" and action.status.value == "PENDING" for action in pending_force.actions)

    # A fill received inside the immutable exit interval but processed after
    # the forced close is no longer safely incorporable.
    late = _trade(
        "late-exit-after-force",
        received=1_650_000_300,
        quantity="0.20",
        price="97",
        aggressor=Side.SELL,
        normalized=2_500_000_300,
    )
    kernel.advance(late)
    after = kernel.last_result()
    assert after is not None
    assert after.status is CycleTerminalState.UNRESOLVED
    assert CycleReason.EXIT_CAUSAL_UNCERTAINTY.value in after.reason_codes
    assert CycleReason.LATE_OLDER_EVENT.value in after.reason_codes
    assert after.positions.risex_signed_quantity == D("-0.60")
    assert after.positions.lighter_signed_quantity == D("0.60")
    assert sum((fill.quantity for fill in after.fills if fill.action_id == "exit-maker"), D("0")) == D("0.40")


def test_queued_entry_partial_reacts_at_processing_ready_without_erasing_receipt_fill() -> None:
    version, source_books = _version("s2-queued-entry")
    kernel = CycleKernel()
    assert kernel.admit(version, source_books=source_books).accepted
    queued = _trade(
        "queued-entry",
        received=500_000_250,
        quantity="0.40",
        price="102",
        normalized=650_000_250,
        decision_ready=800_000_250,
    )
    kernel.advance(queued)
    prefix = kernel.snapshot()
    assert prefix is not None
    assert prefix.status is CycleTerminalState.PENDING
    assert prefix.entry_quantity == D("0.40")
    assert prefix.positions.risex_signed_quantity == D("-0.40")
    assert prefix.positions.lighter_signed_quantity == D("0")
    assert prefix.entry_measurement is not None
    assert prefix.entry_measurement.fills[0].processed_ready_monotonic_ns == 800_000_250
    assert next(action for action in prefix.actions if action.action_id == "entry-cancel").requested_monotonic_ns == 800_000_250
    assert not any(action.action_id == "entry-hedge" for action in prefix.actions)

    kernel.advance_clock(1_300_000_250)
    kernel.advance(
        _book(
            Venue.RISEX,
            received=1_500_000_250,
            revision=2,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        )
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=1_500_000_250,
            revision=2,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        )
    )
    kernel.advance_clock(1_800_000_250)
    hedged = kernel.snapshot()
    assert hedged is not None
    assert hedged.positions.risex_signed_quantity == D("-0.40")
    assert hedged.positions.lighter_signed_quantity == D("0.40")
    assert hedged.hedged_quantity == D("0.40")
    assert any(action.action_id == "entry-hedge" and action.status.value == "COMPLETED" for action in hedged.actions)
    assert hedged.exit_measurement is not None


def test_late_entry_fill_before_cancel_effective_can_join_pending_hedge_without_phase_regression() -> None:
    version, source_books = _version("s2-late-entry-before-hedge")
    kernel = CycleKernel()
    assert kernel.admit(version, source_books=source_books).accepted
    kernel.advance(_trade("entry-early-partial", received=500_000_200, quantity="0.50", price="102"))
    kernel.advance_clock(1_100_000_200)
    late = _trade(
        "entry-late-but-before-hedge",
        received=750_000_200,
        quantity="0.25",
        price="102",
        normalized=1_200_000_200,
    )
    kernel.advance(late)
    snapshot = kernel.snapshot()
    assert snapshot is not None
    assert snapshot.status is CycleTerminalState.PENDING
    assert snapshot.entry_quantity == D("0.75")
    assert snapshot.positions.risex_signed_quantity == D("-0.75")
    assert not any(action.action_id == "entry-hedge" for action in snapshot.actions)
    cancel = next(action for action in snapshot.actions if action.action_id == "entry-cancel")
    assert cancel.requested_quantity == D("0.25")

    for venue, asks, bids in (
        (Venue.RISEX, (("105", "10"),), (("99", "10"),)),
        (Venue.LIGHTER, (("100", "10"),), (("99", "10"),)),
    ):
        kernel.advance(_book(venue, received=1_300_000_200, revision=2, bids=bids, asks=asks))
    kernel.advance_clock(1_600_000_200)
    hedged = kernel.snapshot()
    assert hedged is not None
    assert hedged.positions.risex_signed_quantity == D("-0.75")
    assert hedged.positions.lighter_signed_quantity == D("0.75")
    assert hedged.exit_measurement is not None


def test_entry_partial_can_finish_before_cancel_effective_and_preserves_cancel_barrier() -> None:
    version, source_books = _version("s2-entry-before-cancel")
    kernel = CycleKernel()
    assert kernel.admit(version, source_books=source_books).accepted
    first = _trade("entry-first", received=500_000_200, quantity="0.40", price="102")
    second = _trade("entry-second", received=600_000_200, quantity="0.60", price="103")
    kernel.advance(first)
    kernel.advance(second)
    snapshot = kernel.snapshot()
    assert snapshot is not None
    assert snapshot.entry_quantity == D("1.00")
    assert snapshot.entry_measurement is not None
    assert snapshot.entry_measurement.outcome is CausalOutcome.FULL_FILL
    cancel = next(action for action in snapshot.actions if action.action_id == "entry-cancel")
    assert cancel.status.value == "NOT_REQUIRED"
    assert cancel.remaining_quantity == D("0")
    assert snapshot.positions.risex_signed_quantity == D("-1.00")
    assert not any(action.action_id == "entry-cancel" and action.status.value == "PENDING" for action in snapshot.actions)


def _run_partial_entry_to_hedge(
    *,
    lighter_asks: tuple[tuple[str, str], ...],
    lighter_market: CanonicalMarket | None = None,
) -> tuple[CycleKernel, object]:
    version, source_books = _version("s2-residue", lighter_market=lighter_market)
    kernel = CycleKernel()
    assert kernel.admit(version, source_books=source_books).accepted
    kernel.advance(_trade("residue-entry", received=500_000_200, quantity="0.50", price="102"))
    kernel.advance(
        _book(
            Venue.RISEX,
            received=1_200_000_200,
            revision=2,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        )
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=1_200_000_200,
            revision=2,
            bids=(("99", "10"),),
            asks=lighter_asks,
        )
    )
    kernel.advance_clock(1_500_000_200)
    return kernel, kernel.snapshot()


def test_insufficient_hedge_depth_retains_unmatched_residue_and_can_unwind_it() -> None:
    kernel, hedged = _run_partial_entry_to_hedge(lighter_asks=(("100", "0.30"),))
    assert hedged is not None
    assert hedged.status is CycleTerminalState.PENDING
    assert hedged.hedged_quantity == D("0.30")
    assert hedged.unmatched_entry_quantity == D("0.20")
    assert hedged.positions.risex_signed_quantity == D("-0.50")
    assert hedged.positions.lighter_signed_quantity == D("0.30")
    assert CycleReason.INSUFFICIENT_DEPTH.value in hedged.reason_codes
    assert CycleReason.HEDGE_PARTIAL.value in hedged.reason_codes

    for venue, asks, bids in (
        (Venue.RISEX, (("105", "10"),), (("99", "10"),)),
        (Venue.LIGHTER, (("100", "10"),), (("99", "10"),)),
    ):
        kernel.advance(
            _book(
                venue,
                received=1_700_000_200,
                revision=3,
                bids=bids,
                asks=asks,
            )
        )
    kernel.advance_clock(2_000_000_200)
    after_unmatched = kernel.snapshot()
    assert after_unmatched is not None
    assert after_unmatched.unmatched_entry_quantity == D("0.20")
    assert after_unmatched.positions.risex_signed_quantity == D("-0.30")
    assert after_unmatched.positions.lighter_signed_quantity == D("0.30")
    assert any(action.action_id == "unmatched-risex" and action.status.value == "COMPLETED" for action in after_unmatched.actions)
    assert after_unmatched.exit_measurement is not None


def test_grid_residue_is_recorded_instead_of_silently_rounding_to_a_hedge() -> None:
    kernel, hedged = _run_partial_entry_to_hedge(lighter_asks=(("100", "0.005"),))
    assert hedged is not None
    assert hedged.hedged_quantity == D("0")
    assert hedged.unmatched_entry_quantity == D("0.50")
    assert CycleReason.GRID_RESIDUE.value in hedged.reason_codes
    hedge = next(action for action in hedged.actions if action.action_id == "entry-hedge")
    assert hedge.executed_quantity == D("0")
    assert hedge.remaining_quantity == D("0.50")

    kernel.advance(
        _book(
            Venue.RISEX,
            received=1_700_000_200,
            revision=3,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        )
    )
    kernel.advance_clock(2_000_000_200)
    result = kernel.last_result()
    assert result is not None
    assert result.status is CycleTerminalState.FORCED
    assert result.is_flat
    assert result.positions.is_zero


def test_minimum_residue_is_distinct_from_depth_and_forced_unwind_can_finish_flat() -> None:
    lighter_market = _market(Venue.LIGHTER, "BTC", minimum_quantity="1")
    kernel, hedged = _run_partial_entry_to_hedge(
        lighter_asks=(("100", "10"),),
        lighter_market=lighter_market,
    )
    assert hedged is not None
    assert hedged.hedged_quantity == D("0")
    assert hedged.unmatched_entry_quantity == D("0.50")
    assert CycleReason.MINIMUM_RESIDUE.value in hedged.reason_codes
    assert CycleReason.GRID_RESIDUE.value not in hedged.reason_codes

    kernel.advance(
        _book(
            Venue.RISEX,
            received=1_700_000_200,
            revision=3,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        )
    )
    kernel.advance_clock(2_000_000_200)
    result = kernel.last_result()
    assert result is not None
    assert result.status is CycleTerminalState.FORCED
    assert result.is_flat


def _admit_full_entry(version_id: str) -> tuple[CycleKernel, QuoteVersion]:
    version, source_books = _version(version_id)
    kernel = CycleKernel()
    assert kernel.admit(version, source_books=source_books).accepted
    kernel.advance(_trade("unresolved-entry", received=500_000_200, quantity="1.00", price="102"))
    return kernel, version


def test_missing_required_hedge_data_is_terminal_unresolved_and_blocks_reentry() -> None:
    kernel, version = _admit_full_entry("s2-missing-hedge")
    kernel.advance_clock(1_000_000_200)
    result = kernel.last_result()
    assert result is not None
    assert result.status is CycleTerminalState.UNRESOLVED
    assert not result.is_flat
    assert result.positions.risex_signed_quantity == D("-1.00")
    assert result.positions.lighter_signed_quantity == D("0")
    assert any(action.action_id == "entry-hedge" and action.status.value == "UNRESOLVED" for action in result.actions)
    assert kernel.state() is CycleKernelState.UNRESOLVED_HALTED
    blocked = kernel.admit(version)
    assert not blocked.accepted
    assert blocked.reason == CycleReason.UNRESOLVED_HALTED.value


def test_cycle_admission_requires_exact_s1_input_witnesses_before_stream_events() -> None:
    version, source_books = _version("s2-admission-inputs")
    invalid_sources = (
        (),
        tuple(replace(book, book_revision=99) for book in source_books),
        tuple(replace(book, normalized_ready_monotonic_ns=200) for book in source_books),
        tuple(replace(book, fresh=False) for book in source_books),
    )

    for books in invalid_sources:
        result = CycleKernel().run(
            version,
            _full_cycle_events(),
            source_books=books,
            end_monotonic_ns=2_100_000_300,
        )
        assert result.status is CycleTerminalState.UNRESOLVED
        assert not result.is_flat
        assert not result.positions.authoritative
        assert result.entry_measurement is not None
        assert result.entry_measurement.outcome is CausalOutcome.CAUSAL_UNCERTAIN
        assert result.entry_quantity == D("0")
        assert not result.fills
        assert any(
            reason in result.reason_codes
            for reason in (
                CycleReason.ENTRY_INPUT_AMBIGUOUS.value,
                CycleReason.ENTRY_INPUT_STALE.value,
            )
        )


def test_stale_required_action_book_fails_closed_at_the_hedge_boundary() -> None:
    kernel, _ = _admit_full_entry("s2-stale-hedge")
    kernel.advance(
        _book(
            Venue.RISEX,
            received=900_000_200,
            revision=2,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
            fresh=False,
        )
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=900_000_200,
            revision=2,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
            fresh=False,
        )
    )
    kernel.advance_clock(1_000_000_200)
    result = kernel.last_result()
    assert result is not None
    assert result.status is CycleTerminalState.UNRESOLVED
    assert CycleReason.REQUIRED_ACTION_DATA_STALE.value in result.reason_codes
    assert result.positions.risex_signed_quantity == D("-1.00")
    assert result.positions.lighter_signed_quantity == D("0")


def test_future_book_cannot_be_used_to_retroactively_execute_a_due_hedge() -> None:
    kernel, _ = _admit_full_entry("s2-future-book")
    kernel.advance(
        _book(
            Venue.RISEX,
            received=1_100_000_200,
            revision=2,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        )
    )
    result = kernel.last_result()
    assert result is not None
    assert result.status is CycleTerminalState.UNRESOLVED
    assert CycleReason.FUTURE_BOOK_REJECTED.value in result.reason_codes
    assert result.positions.risex_signed_quantity == D("-1.00")
    assert result.positions.lighter_signed_quantity == D("0")


def test_due_action_uses_latest_eligible_book_and_matches_explicit_clock_boundary() -> None:
    version, source_books = _version("s2-eligible-book")
    prefix = (
        _trade("eligible-entry", received=500_000_101, quantity="0.50", price="102"),
        _book(
            Venue.RISEX,
            received=1_200_000_000,
            revision=2,
            bids=(("103", "10"),),
            asks=(("104", "10"),),
        ),
        _book(
            Venue.LIGHTER,
            received=1_200_000_000,
            revision=2,
            bids=(("103", "10"),),
            asks=(("104", "10"),),
        ),
    )
    future_risex = _book(
        Venue.RISEX,
        received=1_600_000_000,
        revision=3,
        bids=(("103", "10"),),
        asks=(("104", "10"),),
    )

    event_driven = CycleKernel()
    assert event_driven.admit(version, source_books=source_books).accepted
    for event in (*prefix, future_risex):
        event_driven.advance(event)
    event_result = event_driven.snapshot()

    clock_driven = CycleKernel()
    assert clock_driven.admit(version, source_books=source_books).accepted
    for event in prefix:
        clock_driven.advance(event)
    clock_driven.advance_clock(1_500_000_101)
    clock_result = clock_driven.snapshot()

    assert event_result is not None and clock_result is not None
    assert event_result.status is CycleTerminalState.PENDING
    assert clock_result.status is CycleTerminalState.PENDING
    assert event_result.reason_codes == clock_result.reason_codes == ()
    assert [(fill.action_id, fill.quantity, fill.price, fill.book_revision_id) for fill in event_result.fills] == [
        ("entry-maker", D("0.50"), D("101"), None),
        ("entry-hedge", D("0.50"), D("104"), "LIGHTER|BTC|lighter-s2|0|2"),
    ]
    assert [(fill.action_id, fill.quantity, fill.price, fill.book_revision_id) for fill in clock_result.fills] == [
        ("entry-maker", D("0.50"), D("101"), None),
        ("entry-hedge", D("0.50"), D("104"), "LIGHTER|BTC|lighter-s2|0|2"),
    ]


def test_required_action_gap_halts_with_exposure_retained() -> None:
    kernel, _ = _admit_full_entry("s2-gap")
    gap = DataGapEvidence(
        source_venue=Venue.LIGHTER,
        canonical_market="BTC",
        stream_session_id="lighter-s2",
        recovery_generation=0,
        gap_start_monotonic_ns=950_000_200,
        gap_end_monotonic_ns=1_100_000_200,
        reason="HEDGE_BOOK_GAP",
    )
    kernel.advance(gap)
    result = kernel.last_result()
    assert result is not None
    assert result.status is CycleTerminalState.UNRESOLVED
    assert CycleReason.REQUIRED_ACTION_DATA_GAP.value in result.reason_codes
    assert result.positions.risex_signed_quantity == D("-1.00")
    assert result.positions.lighter_signed_quantity == D("0")


def test_max_hold_cancels_the_exit_and_forces_both_legs_flat() -> None:
    version, source_books = _version("s2-max-hold")
    kernel = CycleKernel()
    assert kernel.admit(version, source_books=source_books).accepted
    kernel.advance(_trade("max-hold-entry", received=500_000_200, quantity="1.00", price="102"))
    for venue, asks, bids in (
        (Venue.RISEX, (("105", "10"),), (("99", "10"),)),
        (Venue.LIGHTER, (("100", "10"),), (("99", "10"),)),
    ):
        kernel.advance(_book(venue, received=900_000_200, revision=2, bids=bids, asks=asks))
    kernel.advance_clock(1_000_000_200)
    kernel.advance_clock(1_500_000_200)
    max_hold = 120_500_000_200
    kernel.advance_clock(max_hold)
    waiting = kernel.snapshot()
    assert waiting is not None
    assert waiting.status is CycleTerminalState.PENDING
    assert CycleReason.MAX_HOLD.value in waiting.reason_codes
    assert any(action.action_id == "exit-cancel" and action.status.value == "PENDING" for action in waiting.actions)

    cancel_effective = max_hold + 500_000_000
    for venue, asks, bids in (
        (Venue.RISEX, (("105", "10"),), (("99", "10"),)),
        (Venue.LIGHTER, (("100", "10"),), (("99", "10"),)),
    ):
        kernel.advance(_book(venue, received=cancel_effective, revision=3, bids=bids, asks=asks))
    forced_due = cancel_effective + 500_000_000
    kernel.advance_clock(forced_due)
    result = kernel.last_result()
    assert result is not None
    assert result.status is CycleTerminalState.FORCED
    assert result.is_flat
    assert result.positions.is_zero
    assert CycleReason.FORCED_UNWIND.value in result.reason_codes
    assert all(action.status.value not in {"PENDING", "UNRESOLVED"} for action in result.actions)


def test_primary_and_stress_are_independent_alternatives_with_stress_cost_only() -> None:
    version, source_books = _version("s2-alternatives")
    events = (
        _trade("alternative-entry", received=1_200_000_200, quantity="1.00", price="102"),
        _book(Venue.RISEX, received=1_600_000_200, revision=2, bids=(("99", "10"),), asks=(("105", "10"),)),
        _book(Venue.LIGHTER, received=1_600_000_200, revision=2, bids=(("99", "10"),), asks=(("100", "10"),)),
        _trade("primary-hedge-boundary", received=1_800_000_200, quantity="0.10", price="100"),
        _book(Venue.RISEX, received=2_100_000_200, revision=3, bids=(("99", "10"),), asks=(("105", "10"),)),
        _book(Venue.LIGHTER, received=2_100_000_200, revision=3, bids=(("99", "10"),), asks=(("100", "10"),)),
        _trade("alternative-exit", received=3_300_000_300, quantity="1.00", price="97", aggressor=Side.SELL),
        _book(Venue.RISEX, received=3_600_000_300, revision=4, bids=(("99", "10"),), asks=(("105", "10"),)),
        _book(Venue.LIGHTER, received=3_600_000_300, revision=4, bids=(("99", "10"),), asks=(("100", "10"),)),
        _book(Venue.RISEX, received=3_800_000_300, revision=5, bids=(("99", "10"),), asks=(("105", "10"),)),
        _book(Venue.LIGHTER, received=3_800_000_300, revision=5, bids=(("99", "10"),), asks=(("100", "10"),)),
        _book(Venue.RISEX, received=4_000_000_300, revision=6, bids=(("99", "10"),), asks=(("105", "10"),)),
        _book(Venue.LIGHTER, received=4_000_000_300, revision=6, bids=(("99", "10"),), asks=(("100", "10"),)),
        _book(Venue.RISEX, received=4_300_000_300, revision=7, bids=(("99", "10"),), asks=(("105", "10"),)),
    )
    alternatives = CycleKernel().alternatives(version, events, source_books=source_books)
    assert alternatives.primary.status is CycleTerminalState.NORMAL
    assert alternatives.stress.status is CycleTerminalState.NORMAL
    assert alternatives.primary.is_flat and alternatives.stress.is_flat
    assert alternatives.primary.pnl_usd is not None and alternatives.stress.pnl_usd is not None
    assert alternatives.primary.pnl_usd > alternatives.stress.pnl_usd
    assert alternatives.primary.ledger.scenario_cost_usd == D("0")
    assert alternatives.stress.ledger.scenario_cost_usd > D("0")
    assert alternatives.primary.pnl_usd == D("1.9801")
    assert alternatives.stress.ledger.scenario_cost_usd == D("0.0199")
    assert alternatives.stress.pnl_usd == D("1.9602")
    primary_hedge = next(action for action in alternatives.primary.actions if action.action_id == "entry-hedge")
    stress_hedge = next(action for action in alternatives.stress.actions if action.action_id == "entry-hedge")
    assert (primary_hedge.due_monotonic_ns, stress_hedge.due_monotonic_ns) == (1_700_000_200, 2_200_000_200)
    primary_close = next(action for action in alternatives.primary.actions if action.action_id == "exit-close:0")
    stress_close = next(action for action in alternatives.stress.actions if action.action_id == "exit-close:0")
    assert (primary_close.due_monotonic_ns, stress_close.due_monotonic_ns) == (3_800_000_300, 4_300_000_300)
    assert _independent_cycle_totals(alternatives.primary)[0:2] == (D("0"), D("0"))
    assert _independent_cycle_totals(alternatives.stress)[0:2] == (D("0"), D("0"))
    assert alternatives.by_scenario == (alternatives.primary, alternatives.stress)


def test_completion_requires_true_flatness_before_a_new_decision() -> None:
    version, source_books = _version("s2-reentry-first")
    kernel = CycleKernel()
    assert kernel.admit(version, source_books=source_books).accepted
    active_rejection = _version("s2-reentry-active", decision_ready=200)[0]
    blocked = kernel.admit(active_rejection)
    assert not blocked.accepted
    assert blocked.reason == CycleReason.ACTIVE_CYCLE.value
    # Finish the admitted lane through the same events without admitting a
    # second cycle into it.
    for event in _full_cycle_events():
        kernel.advance(event)
    flat = kernel.finish(end_monotonic_ns=2_100_000_300)
    assert flat.is_flat
    inside = _version("s2-reentry-inside", decision_ready=2_000_000_300)[0]
    rejected_inside = kernel.admit(inside)
    assert not rejected_inside.accepted
    assert rejected_inside.reason == CycleReason.DECISION_WITHIN_PREVIOUS_CYCLE.value

    reentry_decision = _version("s2-reentry-flat", decision_ready=3_100_000_300)[0]
    accepted = kernel.admit(reentry_decision)
    assert accepted.accepted
    assert kernel.state() is CycleKernelState.PENDING
    reentry_result = kernel.finish(end_monotonic_ns=9_100_000_300)
    assert reentry_result.status is CycleTerminalState.ABORTED
    assert reentry_result.is_flat


def test_no_fill_aborts_without_claiming_terminal_execution_pnl() -> None:
    version, source_books = _version("s2-no-fill")
    kernel = CycleKernel()
    assert kernel.admit(version, source_books=source_books).accepted
    result = kernel.finish(end_monotonic_ns=6_000_000_100)
    assert result.status is CycleTerminalState.ABORTED
    assert result.is_flat
    assert not result.cashflow_complete
    assert result.pnl_usd is None
    assert CycleReason.NO_ENTRY.value in result.reason_codes


def test_invalid_exit_post_only_route_uses_forced_unwind_and_still_reconciles_flat() -> None:
    version, source_books = _version("s2-invalid-exit")
    kernel = CycleKernel()
    assert kernel.admit(version, source_books=source_books).accepted
    kernel.advance(_trade("invalid-exit-entry", received=500_000_200, quantity="1.00", price="102"))
    for venue, asks, bids in (
        (Venue.RISEX, (("105", "10"),), (("99", "10"),)),
        (Venue.LIGHTER, (("100", "10"),), (("0.5", "10"),)),
    ):
        kernel.advance(_book(venue, received=900_000_200, revision=2, bids=bids, asks=asks))
    kernel.advance_clock(1_000_000_200)
    waiting = kernel.snapshot()
    assert waiting is not None
    assert waiting.status is CycleTerminalState.PENDING
    assert CycleReason.EXIT_QUOTE_INVALID.value in waiting.reason_codes
    assert waiting.exit_measurement is None
    assert any(action.action_id == "forced-risex" and action.status.value == "PENDING" for action in waiting.actions)

    for venue, asks, bids in (
        (Venue.RISEX, (("105", "10"),), (("99", "10"),)),
        (Venue.LIGHTER, (("100", "10"),), (("0.5", "10"),)),
    ):
        kernel.advance(_book(venue, received=1_200_000_200, revision=3, bids=bids, asks=asks))
    kernel.advance_clock(1_500_000_200)
    result = kernel.last_result()
    assert result is not None
    assert result.status is CycleTerminalState.FORCED
    assert result.is_flat


def test_missing_trade_identity_is_uncertain_and_never_a_clean_fill_or_no_fill() -> None:
    version, source_books = _version("s2-missing-identity")
    kernel = CycleKernel()
    assert kernel.admit(version, source_books=source_books).accepted
    trade = _trade("missing-identity", received=500_000_200, quantity="0.20", price="102")
    kernel.advance(CausalEvent.from_trade(trade, source_identity=""))
    result = kernel.last_result()
    assert result is not None
    assert result.status is CycleTerminalState.UNRESOLVED
    assert result.entry_measurement is not None
    assert result.entry_measurement.outcome is CausalOutcome.CAUSAL_UNCERTAIN
    assert CausalOutcome.NO_FILL not in {result.entry_measurement.outcome}
    assert CycleReason.ENTRY_CAUSAL_UNCERTAINTY.value in result.reason_codes
    assert result.positions.is_zero


def test_missing_s1_timing_is_not_upgraded_to_a_clean_entry_outcome() -> None:
    version, source_books = _version("s2-missing-timing")
    kernel = CycleKernel()
    assert kernel.admit(version, source_books=source_books).accepted
    trade = replace(
        _trade("missing-timing", received=500_000_200, quantity="0.20", price="102"),
        ingress_received_monotonic_ns=None,
        normalized_ready_monotonic_ns=None,
        decision_ready_monotonic_ns=None,
    )
    kernel.advance(trade)
    result = kernel.last_result()
    assert result is not None
    assert result.status is CycleTerminalState.UNRESOLVED
    assert result.entry_measurement is not None
    assert result.entry_measurement.outcome is CausalOutcome.CAUSAL_UNCERTAIN
    assert CycleReason.EVENT_NOT_READY.value in result.reason_codes
    assert result.entry_quantity == D("0")


def test_non_executable_forced_residue_remains_unresolved_and_non_flat() -> None:
    version, source_books = _version("s2-forced-residue")
    kernel = CycleKernel()
    assert kernel.admit(version, source_books=source_books).accepted
    events = _full_cycle_events(exit_quantity="0.40", close_received=2_000_000_300)
    for event in events[:4]:
        kernel.advance(event)
    kernel.advance(events[4])
    kernel.advance(events[5])
    kernel.advance_clock(2_100_000_300)
    kernel.advance(
        _book(
            Venue.RISEX,
            received=2_400_000_300,
            revision=4,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        )
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=2_400_000_300,
            revision=4,
            bids=(("99", "0.30"),),
            asks=(("100", "10"),),
        )
    )
    kernel.advance_clock(2_600_000_300)
    result = kernel.last_result()
    assert result is not None
    assert result.status is CycleTerminalState.UNRESOLVED
    assert not result.is_flat
    assert result.pnl_usd is None
    assert result.positions.risex_signed_quantity == D("0")
    assert result.positions.lighter_signed_quantity == D("0.30")
    assert CycleReason.TERMINAL_NON_FLAT.value in result.reason_codes
    assert any(action.action_id == "forced-lighter" and action.remaining_quantity == D("0.30") for action in result.actions)
