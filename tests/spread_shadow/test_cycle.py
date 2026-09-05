from __future__ import annotations

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
    CycleKernel,
    CycleKernelState,
    CycleReason,
    CycleTerminalState,
)
from risex_spread_shadow.economics import build_hypothetical_maker_quote
from risex_spread_shadow.models import (
    BookEvidence,
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


def _market(venue: Venue, symbol: str) -> CanonicalMarket:
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
        minimum_quantity_raw=D("0.01"),
        minimum_notional_usd=D("0"),
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
) -> BookEvidence:
    session = session or ("risex-s2" if venue is Venue.RISEX else "lighter-s2")
    return BookEvidence(
        venue=venue,
        canonical_market="BTC",
        bids=tuple(BookLevel(D(price), D(quantity)) for price, quantity in bids),
        asks=tuple(BookLevel(D(price), D(quantity)) for price, quantity in asks),
        received_monotonic_ns=received,
        stream_session_id=session,
        recovery_generation=0,
        book_revision=revision,
        sequence=revision,
        checksum=revision,
        sequence_valid=True,
        checksum_valid=True,
        received_utc=NOW,
        fresh=True,
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
        stream_session_id="risex-s2",
        recovery_generation=0,
        ingress_received_monotonic_ns=received,
        normalized_ready_monotonic_ns=received,
        source_trade_id=source_trade_id,
        maker_order_id=maker_order_id,
        taker_order_id=taker_order_id,
        maker="0x" + "11" * 32,
        taker="0x" + "22" * 32,
        tx_hash="0x" + hashlib.sha256(("tx:" + key).encode()).hexdigest(),
        block_number=20_000 + received,
        log_index=received,
        worker_timestamp=received,
    )


def _version() -> tuple[QuoteVersion, tuple[BookEvidence, ...]]:
    risex_market = _market(Venue.RISEX, "BTC/USDC")
    lighter_market = _market(Venue.LIGHTER, "BTC")
    initial_risex = _book(
        Venue.RISEX,
        received=0,
        revision=1,
        bids=(("99", "10"),),
        asks=(("101", "10"),),
    )
    initial_lighter = _book(
        Venue.LIGHTER,
        received=0,
        revision=1,
        bids=(("99", "10"),),
        asks=(("100", "10"),),
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
        risex_best_bid=D("99"),
        risex_best_ask=D("101"),
        risex_tick_size=D("1"),
        fee_observed_or_configured_at=NOW,
    )
    quote = build_hypothetical_maker_quote(policy, initial_lighter)
    assert quote.is_active
    version = QuoteVersion(
        version_id="s2-prefix",
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
        decision_ready_monotonic_ns=100,
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
