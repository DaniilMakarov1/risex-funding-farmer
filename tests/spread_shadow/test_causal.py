from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal as D
import hashlib
import json

import pytest

from risex_farmer.models import BookLevel, CanonicalMarket, ContractType, MarketType
from risex_spread_shadow import (
    BookEvidence,
    CausalEvent,
    CausalEventKind,
    CausalOutcome,
    CausalRestingQuote,
    CausalSourceIdentity,
    CausalUncertainty,
    DataGapEvidence,
    HypotheticalBlockWatermark,
    QuotePolicy,
    QuoteVersion,
    Side,
    SpreadDirection,
    TradeEvidence,
    Venue,
    build_causal_resting_quote,
    build_hypothetical_maker_quote,
    measure_causal_quote,
)


UTC = timezone.utc
NOW = datetime(2026, 9, 5, tzinfo=UTC)


def _wire_trade_identity(key: str) -> tuple[str, str, str, str, str]:
    digest = hashlib.sha256(key.encode()).hexdigest()
    maker_order_id = "0x" + digest[:48]
    taker_order_id = "0x" + hashlib.sha256(("taker:" + key).encode()).hexdigest()[:48]
    source_trade_id = f"{maker_order_id}-{taker_order_id}"
    trade_event_key = f"RISEX|BTC|{source_trade_id}"
    tx_hash = "0x" + hashlib.sha256(("tx:" + key).encode()).hexdigest()
    return trade_event_key, source_trade_id, maker_order_id, taker_order_id, tx_hash


def quote(
    *,
    quantity: str = "3",
    decision: int | None = 100,
    activation_delay: int = 10,
    cancel_requested: int | None = None,
    cancel_delay: int = 0,
    cancel_on_first_partial: bool = False,
    replacement: int | None = None,
    session: str = "risex-1",
    recovery: int = 0,
    ingress: int | None = None,
    normalized: int | None = None,
    source_book: BookEvidence | None = "DEFAULT",  # type: ignore[assignment]
    hedge_source_book: BookEvidence | None = "DEFAULT",  # type: ignore[assignment]
    source_book_max_age: int | None = None,
    watermark: HypotheticalBlockWatermark | None = None,
) -> CausalRestingQuote:
    if source_book == "DEFAULT":
        source_book = book(received=90, ingress=80, normalized=85)
    if hedge_source_book == "DEFAULT":
        hedge_source_book = book(
            received=90,
            ingress=80,
            normalized=85,
            venue=Venue.LIGHTER,
            session="lighter-1",
        )
    return CausalRestingQuote(
        quote_id="q-1",
        canonical_market="BTC",
        maker_side=Side.BUY,
        price=D("100"),
        quantity=D(quantity),
        stream_session_id=session,
        recovery_generation=recovery,
        decision_ready_monotonic_ns=decision,
        activation_delay_ns=activation_delay,
        cancel_requested_monotonic_ns=cancel_requested,
        cancel_delay_ns=cancel_delay,
        cancel_on_first_partial=cancel_on_first_partial,
        replacement_effective_monotonic_ns=replacement,
        ingress_received_monotonic_ns=ingress,
        normalized_ready_monotonic_ns=normalized,
        source_book=source_book,
        hedge_source_book=hedge_source_book,
        source_book_freshness_max_age_ns=source_book_max_age,
        block_watermark=watermark,
    )


def trade(
    key: str,
    *,
    received: int,
    price: str = "99",
    quantity: str = "1",
    aggressor: Side = Side.SELL,
    session: str = "risex-1",
    recovery: int = 0,
    ingress: int | None = None,
    normalized: int | None = None,
    exchange_utc: datetime | None = None,
    source_trade_id: str | None = None,
    block_number: int | None = None,
    log_index: int | None = None,
    worker_timestamp: int | None = None,
) -> TradeEvidence:
    trade_event_key, default_trade_id, maker_order_id, taker_order_id, tx_hash = (
        _wire_trade_identity(key)
    )
    source_trade_id = default_trade_id if source_trade_id is None else source_trade_id
    block_number = 2000 + received if block_number is None else block_number
    log_index = received if log_index is None else log_index
    worker_timestamp = received if worker_timestamp is None else worker_timestamp
    return TradeEvidence(
        trade_event_key=trade_event_key,
        venue=Venue.RISEX,
        canonical_market="BTC",
        canonical_price=D(price),
        canonical_quantity=D(quantity),
        aggressor_side=aggressor,
        received_utc=NOW,
        received_monotonic_ns=received,
        stream_session_id=session,
        recovery_generation=recovery,
        exchange_event_utc=exchange_utc,
        exchange_event_time_provenance=(
            "FIXTURE_EXCHANGE_UTC" if exchange_utc is not None else None
        ),
        ingress_received_monotonic_ns=ingress,
        normalized_ready_monotonic_ns=normalized,
        source_trade_id=source_trade_id,
        maker_order_id=maker_order_id,
        taker_order_id=taker_order_id,
        maker="0x" + "33" * 32,
        taker="0x" + "44" * 32,
        tx_hash=tx_hash,
        block_number=block_number,
        log_index=log_index,
        worker_timestamp=worker_timestamp,
    )


def book(
    *,
    received: int = 90,
    ingress: int | None = None,
    normalized: int | None = None,
    session: str = "risex-1",
    recovery: int = 0,
    revision: int = 1,
    fresh: bool = True,
    sequence_valid: bool = True,
    checksum_valid: bool = True,
    venue: Venue = Venue.RISEX,
    block_number: int | None = None,
    log_index: int | None = None,
    worker_timestamp: int | None = None,
) -> BookEvidence:
    if venue is Venue.RISEX:
        block_number = (1000 + revision) if block_number is None else block_number
        log_index = revision if log_index is None else log_index
        worker_timestamp = received if worker_timestamp is None else worker_timestamp
    else:
        block_number = None
        log_index = None
        worker_timestamp = None
    return BookEvidence(
        venue=venue,
        canonical_market="BTC",
        bids=(BookLevel(D("99"), D("5")),),
        asks=(BookLevel(D("101"), D("5")),),
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
        ingress_received_monotonic_ns=ingress,
        normalized_ready_monotonic_ns=normalized,
        tx_hash=(None if venue is Venue.LIGHTER else f"0xbook-{revision}"),
        block_number=block_number,
        log_index=log_index,
        worker_timestamp=worker_timestamp,
    )


def gap(
    *,
    start: int,
    end: int | None,
    session: str = "risex-1",
    recovery: int = 0,
    venue: Venue = Venue.RISEX,
) -> DataGapEvidence:
    return DataGapEvidence(
        source_venue=venue,
        canonical_market="BTC",
        stream_session_id=session,
        recovery_generation=recovery,
        gap_start_monotonic_ns=start,
        gap_end_monotonic_ns=end,
        reason="FIXTURE_GAP",
    )


def _factory_market(venue: Venue, symbol: str) -> CanonicalMarket:
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
        quantity_step_raw=D("1"),
        minimum_quantity_raw=D("1"),
        minimum_notional_usd=D("0"),
        minimum_fee_notional_usd=None,
        is_active=True,
        is_rfq=False,
        is_off_hours=False,
    )


def _factory_version(
    *, include_bindings: bool = True
) -> tuple[BookEvidence, BookEvidence, QuoteVersion]:
    source = book(
        received=90,
        ingress=80,
        normalized=85,
        revision=1,
        block_number=1001,
        log_index=2,
    )
    hedge = book(
        received=90,
        ingress=80,
        normalized=85,
        venue=Venue.LIGHTER,
        session="lighter-1",
        revision=1,
    )
    risex_market = _factory_market(Venue.RISEX, "BTC/USDC")
    lighter_market = _factory_market(Venue.LIGHTER, "BTC")
    policy = QuotePolicy(
        canonical_market="BTC",
        direction=SpreadDirection.RISEX_BUY_LIGHTER_SELL,
        target_notional_usd=D("100"),
        target_margin_bps=D("1"),
        risex_maker_fee_rate=D("0"),
        lighter_taker_fee_rate=D("0"),
        risex_market=risex_market,
        lighter_market=lighter_market,
        risex_best_bid=D("99"),
        risex_best_ask=D("101"),
        risex_tick_size=D("1"),
        fee_observed_or_configured_at=NOW,
    )
    production_quote = build_hypothetical_maker_quote(
        policy,
        hedge,
        risex_market=risex_market,
        lighter_market=lighter_market,
        risex_best_bid=D("99"),
        risex_best_ask=D("101"),
        risex_tick_size=D("1"),
    )
    assert production_quote.is_active
    bindings = (
        {
            "risex_book_revision": source.book_revision,
            "lighter_book_revision": hedge.book_revision,
            "risex_book_revision_id": source.book_revision_id,
            "lighter_book_revision_id": hedge.book_revision_id,
        }
        if include_bindings
        else {}
    )
    return source, hedge, QuoteVersion(
        version_id="factory-version",
        quote=production_quote,
        quote_created_utc=NOW,
        quote_created_monotonic_ns=100,
        stream_session_id="risex-1",
        recovery_generation=0,
        quote_expires_monotonic_ns=500,
        hedge_stream_session_id="lighter-1",
        hedge_recovery_generation=0,
        ingress_received_monotonic_ns=80,
        normalized_ready_monotonic_ns=85,
        decision_ready_monotonic_ns=100,
        **bindings,
    )


def causal_trade(value: TradeEvidence, **kwargs: object) -> CausalEvent:
    return CausalEvent.from_trade(value, **kwargs)


def test_trade_requires_separate_causal_timing_and_respects_decision_activation_boundaries() -> None:
    result = measure_causal_quote(
        quote(quantity="1"),
        (
            trade("before-decision", received=99, ingress=99, normalized=100),
            trade("before-activation", received=109, ingress=109, normalized=110),
            trade("at-activation", received=110, ingress=110, normalized=111),
        ),
    )

    assert result.outcome is CausalOutcome.FULL_FILL
    assert result.filled_quantity == D("1")
    assert result.decisions[0].reason == "BEFORE_DECISION_READY"
    assert result.decisions[1].reason == "BEFORE_HYPOTHETICAL_ACTIVATION"
    assert result.timing.first_event_ingress_received_monotonic_ns == 99

    source_clock_lies_later = measure_causal_quote(
        quote(quantity="1"),
        (
            causal_trade(
                trade("ingress-before-activation", received=109, ingress=109, normalized=110),
                source_event_monotonic_ns=120,
            ),
        ),
    )
    assert source_clock_lies_later.outcome is CausalOutcome.NO_FILL

    source_position_late = measure_causal_quote(
        quote(quantity="2"),
        (
            causal_trade(
                trade(
                    "source-new",
                    received=120,
                    ingress=120,
                    normalized=121,
                    block_number=200,
                    log_index=1,
                ),
            ),
            causal_trade(
                trade(
                    "source-old",
                    received=121,
                    ingress=121,
                    normalized=122,
                    block_number=199,
                    log_index=99,
                ),
            ),
        ),
    )
    assert source_position_late.outcome is CausalOutcome.CAUSAL_UNCERTAIN
    assert CausalUncertainty.LATE_OLDER_EVENT in source_position_late.uncertainty_reasons

    legacy = measure_causal_quote(
        quote(quantity="1"),
        (trade("legacy-only-receipt", received=110),),
    )
    assert legacy.outcome is CausalOutcome.CAUSAL_UNCERTAIN
    assert legacy.filled_quantity == D("0")
    assert legacy.observed_filled_quantity == D("0")
    assert CausalUncertainty.MISSING_CAUSAL_TIMING in legacy.uncertainty_reasons
    assert not legacy.is_clean_no_fill

    ingress_only = measure_causal_quote(
        quote(quantity="1"),
        (trade("ingress-only", received=120, ingress=120),),
    )
    assert ingress_only.outcome is CausalOutcome.CAUSAL_UNCERTAIN
    assert CausalUncertainty.MISSING_CAUSAL_TIMING in ingress_only.uncertainty_reasons


def test_cancel_delay_counts_partial_fills_and_never_resets_remaining_quantity() -> None:
    result = measure_causal_quote(
        quote(
            cancel_requested=130,
            cancel_delay=10,
            cancel_on_first_partial=False,
        ),
        (
            trade("first", received=120, ingress=120, normalized=121),
            trade("second", received=139, ingress=139, normalized=139),
            trade("at-effective-cancel", received=140, ingress=140, normalized=141),
        ),
    )

    assert result.outcome is CausalOutcome.PARTIAL_FILL
    assert result.observed_filled_quantity == D("2")
    assert result.filled_quantity == D("2")
    assert result.remaining_quantity == D("1")
    assert result.fills[-1].remaining_quantity == D("1")
    assert result.decisions[-1].reason == "AT_OR_AFTER_CANCEL_EFFECTIVE"

    auto_cancel = measure_causal_quote(
        quote(quantity="3", cancel_delay=20, cancel_on_first_partial=True),
        (
            trade("p1", received=120, ingress=120, normalized=121),
            trade("p2", received=130, ingress=130, normalized=160),
            trade("p3", received=141, ingress=141, normalized=200),
        ),
    )
    assert auto_cancel.outcome is CausalOutcome.PARTIAL_FILL
    assert auto_cancel.observed_filled_quantity == D("2")
    assert auto_cancel.cancel_requested_monotonic_ns == 121
    assert auto_cancel.cancel_effective_monotonic_ns == 141
    assert auto_cancel.fills[-1].processed_ready_monotonic_ns == 160


def test_cancel_window_uses_trade_receipt_even_when_processing_is_late() -> None:
    result = measure_causal_quote(
        quote(quantity="3", cancel_requested=130, cancel_delay=10),
        (
            trade("late-before-request", received=125, ingress=125, normalized=150),
            trade("processing-equal-before-cancel", received=139, ingress=139, normalized=140),
            trade("received-at-cancel", received=140, ingress=140, normalized=200),
        ),
        end_monotonic_ns=200,
    )

    assert result.outcome is CausalOutcome.PARTIAL_FILL
    assert result.observed_filled_quantity == D("2")
    assert result.filled_quantity == D("2")
    assert result.remaining_quantity == D("1")
    assert result.cancel_requested_monotonic_ns == 130
    assert result.effective_cancel_monotonic_ns == 140
    assert result.fills[0].received_monotonic_ns == 125
    assert result.fills[0].processed_ready_monotonic_ns == 150
    assert result.fills[1].received_monotonic_ns == 139
    assert result.fills[1].processed_ready_monotonic_ns == 140
    assert result.decisions[-1].reason == "AT_OR_AFTER_CANCEL_EFFECTIVE"


def test_duplicate_identity_consumes_volume_once_and_conflict_is_uncertain() -> None:
    first = trade("same", received=120, ingress=120, normalized=121)
    duplicate = trade(
        "same",
        received=121,
        ingress=121,
        normalized=122,
        block_number=2120,
        log_index=120,
        worker_timestamp=120,
    )
    deduped = measure_causal_quote(quote(quantity="2"), (first, duplicate))
    assert deduped.outcome is CausalOutcome.PARTIAL_FILL
    assert deduped.observed_filled_quantity == D("1")
    assert deduped.duplicate_event_count == 1

    conflict = measure_causal_quote(
        quote(quantity="2"),
        (
            first,
            trade("same", received=121, ingress=121, normalized=122, price="98"),
        ),
    )
    assert conflict.outcome is CausalOutcome.CAUSAL_UNCERTAIN
    assert conflict.filled_quantity == D("0")
    assert conflict.observed_filled_quantity == D("1")
    assert CausalUncertainty.CONFLICTING_DUPLICATE in conflict.uncertainty_reasons

    wire_conflict = measure_causal_quote(
        quote(quantity="2"),
        (
            first,
            trade(
                "same",
                received=121,
                ingress=121,
                normalized=122,
                block_number=2120,
                log_index=120,
                worker_timestamp=999,
            ),
        ),
    )
    assert wire_conflict.outcome is CausalOutcome.CAUSAL_UNCERTAIN
    assert wire_conflict.filled_quantity == D("0")
    assert wire_conflict.observed_filled_quantity == D("1")
    assert CausalUncertainty.CONFLICTING_DUPLICATE in wire_conflict.uncertainty_reasons


def test_missing_identity_late_older_and_recovery_transition_never_become_clean_results() -> None:
    missing = causal_trade(
        trade("missing", received=120, ingress=120, normalized=121),
        source_identity="",
    )
    late = trade("late", received=119, ingress=119, normalized=120)
    mismatched = trade(
        "other-session",
        received=121,
        ingress=121,
        normalized=122,
        session="risex-2",
    )
    transitioned = trade(
        "other-recovery",
        received=122,
        ingress=122,
        normalized=123,
        recovery=1,
    )
    result = measure_causal_quote(
        quote(quantity="2"),
        (trade("first", received=120, ingress=120, normalized=121), missing, late, mismatched, transitioned),
    )

    assert result.outcome is CausalOutcome.CAUSAL_UNCERTAIN
    assert not result.is_clean_no_fill
    assert CausalUncertainty.MISSING_SOURCE_IDENTITY in result.uncertainty_reasons
    assert CausalUncertainty.LATE_OLDER_EVENT in result.uncertainty_reasons
    assert CausalUncertainty.SOURCE_IDENTITY_MISMATCH in result.uncertainty_reasons
    assert CausalUncertainty.RECOVERY_TRANSITION in result.uncertainty_reasons

    forged_identity = CausalSourceIdentity(
        venue=Venue.RISEX,
        canonical_market="BTC",
        stream_session_id="risex-1",
        recovery_generation=0,
        source_event_id="forged-stream-binding",
    )
    forged = measure_causal_quote(
        quote(quantity="1"),
        (
            causal_trade(
                trade("payload-stream-2", received=120, ingress=120, normalized=121, session="risex-2"),
                source_identity=forged_identity,
            ),
        ),
    )
    assert forged.outcome is CausalOutcome.CAUSAL_UNCERTAIN
    assert CausalUncertainty.SOURCE_IDENTITY_MISMATCH in forged.uncertainty_reasons


def test_book_and_trade_from_one_match_remain_ambiguous_in_both_arrival_orders() -> None:
    retained_book = book(
        received=120,
        ingress=120,
        normalized=121,
        block_number=700,
        log_index=2,
    )
    fence = HypotheticalBlockWatermark(700, "RETAINED_RISEX_BOOK")
    fenced_quote = quote(quantity="1", source_book=retained_book, watermark=fence)
    retained_book_event = CausalEvent.from_book(retained_book)
    retained_trade_event = causal_trade(
        trade(
            "retained-block-trade",
            received=120,
            ingress=120,
            normalized=121,
            block_number=700,
            log_index=3,
        )
    )
    for events in ((retained_book_event, retained_trade_event), (retained_trade_event, retained_book_event)):
        result = measure_causal_quote(fenced_quote, events)
        assert result.outcome is CausalOutcome.CAUSAL_UNCERTAIN
        assert result.observed_filled_quantity == D("0")
        assert CausalUncertainty.WATERMARK_BOUNDARY_AMBIGUOUS in result.uncertainty_reasons

    book_event = CausalEvent.from_book(
        book(received=120, ingress=120, normalized=121), match_id="match-1"
    )
    trade_event = causal_trade(
        trade("match-trade", received=120, ingress=120, normalized=121),
        match_id="match-1",
    )

    for events in ((book_event, trade_event), (trade_event, book_event)):
        result = measure_causal_quote(quote(quantity="1"), events)
        assert result.outcome is CausalOutcome.CAUSAL_UNCERTAIN
        assert result.observed_filled_quantity == D("0")
        assert CausalUncertainty.SAME_MATCH_ORDER_UNPROVEN in result.uncertainty_reasons


def test_gaps_are_stream_scoped_and_open_matching_gap_blocks_clean_conclusions() -> None:
    unrelated = gap(start=115, end=130, venue=Venue.LIGHTER, session="lighter-1")
    clean = measure_causal_quote(
        quote(quantity="1"),
        (unrelated, trade("fill", received=120, ingress=120, normalized=121)),
    )
    assert clean.outcome is CausalOutcome.FULL_FILL
    assert not clean.uncertainty_reasons

    matching = measure_causal_quote(
        quote(quantity="1"),
        (gap(start=115, end=None), trade("fill", received=120, ingress=120, normalized=121)),
    )
    assert matching.outcome is CausalOutcome.CAUSAL_UNCERTAIN
    assert matching.filled_quantity == D("0")
    assert CausalUncertainty.DATA_GAP in matching.uncertainty_reasons


def test_watermark_is_hypothetical_and_missing_block_identity_is_not_a_fill() -> None:
    watermark = HypotheticalBlockWatermark(
        later_block_number=10,
        source="FIXTURE_ONLY",
    )
    pre = measure_causal_quote(
        quote(quantity="1", watermark=watermark),
        (
            causal_trade(
                trade(
                    "pre",
                    received=120,
                    ingress=120,
                    normalized=121,
                    block_number=10,
                ),
            ),
        ),
    )
    assert pre.outcome is CausalOutcome.CAUSAL_UNCERTAIN
    assert not pre.is_clean_no_fill
    assert pre.filled_quantity == D("0")
    assert CausalUncertainty.WATERMARK_BOUNDARY_AMBIGUOUS in pre.uncertainty_reasons

    _missing_block_trade = trade(
        "missing-block", received=120, ingress=120, normalized=121
    )
    object.__setattr__(_missing_block_trade, "block_number", None)
    missing = measure_causal_quote(
        quote(quantity="1", watermark=watermark), (_missing_block_trade,)
    )
    assert missing.outcome is CausalOutcome.CAUSAL_UNCERTAIN
    assert CausalUncertainty.WATERMARK_IDENTITY_MISSING in missing.uncertainty_reasons

    with pytest.raises(ValueError, match="watermark semantics"):
        HypotheticalBlockWatermark(10, "FIXTURE_ONLY", semantics="EXCHANGE_CURSOR")


def test_source_book_block_is_an_implicit_strict_watermark() -> None:
    for block_number in (999, 1001):
        result = measure_causal_quote(
            quote(quantity="1"),
            (
                causal_trade(
                    trade(
                        f"implicit-fence-{block_number}",
                        received=120,
                        ingress=120,
                        normalized=121,
                        block_number=block_number,
                    )
                ),
            ),
        )
        assert result.outcome is CausalOutcome.CAUSAL_UNCERTAIN
        assert result.observed_filled_quantity == D("0")
        assert result.filled_quantity == D("0")
        assert CausalUncertainty.WATERMARK_BOUNDARY_AMBIGUOUS in result.uncertainty_reasons

    later = measure_causal_quote(
        quote(quantity="1"),
        (
            causal_trade(
                trade(
                    "implicit-fence-later",
                    received=120,
                    ingress=120,
                    normalized=121,
                    block_number=1002,
                )
            ),
        ),
    )
    assert later.outcome is CausalOutcome.FULL_FILL
    assert not later.uncertainty_reasons

    missing_source_block = book(received=90, ingress=80, normalized=85)
    object.__setattr__(missing_source_block, "block_number", None)
    missing_fence = measure_causal_quote(
        quote(quantity="1", source_book=missing_source_block),
        (trade("missing-implicit-fence", received=120, ingress=120, normalized=121),),
    )
    assert missing_fence.outcome is CausalOutcome.CAUSAL_UNCERTAIN
    assert missing_fence.filled_quantity == D("0")
    assert CausalUncertainty.MISSING_SOURCE_IDENTITY in missing_fence.uncertainty_reasons


def test_source_book_diagnostics_keep_skew_freshness_and_quote_age_separate() -> None:
    source = book(received=90, ingress=80, normalized=85)
    result = measure_causal_quote(
        quote(
            quantity="1",
            ingress=70,
            normalized=80,
            source_book=source,
            source_book_max_age=30,
        ),
        (trade("fill", received=120, ingress=120, normalized=121),),
        end_monotonic_ns=150,
    )
    assert result.outcome is CausalOutcome.FULL_FILL
    assert result.timing.normalization_delay_ns == 10
    assert result.timing.decision_delay_ns == 20
    assert result.timing.source_book_receipt_skew_ns == 10
    assert result.timing.source_book_age_at_decision_ns == 10
    assert result.timing.source_book_age_at_activation_ns == 20
    assert result.timing.resting_quote_age_ns == 40
    assert result.timing.input_receipt_fresh is True

    stale = measure_causal_quote(
        quote(quantity="1", source_book=book(received=50, ingress=50, normalized=51), source_book_max_age=30),
        (trade("fill", received=120, ingress=120, normalized=121),),
    )
    assert stale.outcome is CausalOutcome.CAUSAL_UNCERTAIN
    assert CausalUncertainty.SOURCE_BOOK_STALE in stale.uncertainty_reasons
    assert stale.timing.input_receipt_fresh is False

    stale_hedge = measure_causal_quote(
        quote(
            quantity="1",
            hedge_source_book=book(
                received=95,
                ingress=95,
                normalized=96,
                venue=Venue.LIGHTER,
                session="lighter-1",
                fresh=False,
            ),
            source_book_max_age=30,
        ),
        (trade("fill-hedge-stale", received=120, ingress=120, normalized=121),),
    )
    assert stale_hedge.outcome is CausalOutcome.CAUSAL_UNCERTAIN
    assert CausalUncertainty.SOURCE_BOOK_STALE in stale_hedge.uncertainty_reasons
    assert stale_hedge.timing.input_receipt_fresh is False


def test_quote_version_factory_binds_both_input_witnesses_before_measurement() -> None:
    source, hedge, version = _factory_version()

    causal_quote = build_causal_resting_quote(
        version,
        source_book=source,
        hedge_source_book=hedge,
    )
    assert causal_quote.source_book_revision == source.book_revision
    assert causal_quote.source_book_revision_id == source.book_revision_id
    assert causal_quote.hedge_stream_session_id == hedge.stream_session_id
    assert causal_quote.hedge_recovery_generation == hedge.recovery_generation
    assert causal_quote.hedge_source_book_revision == hedge.book_revision
    assert causal_quote.hedge_source_book_revision_id == hedge.book_revision_id
    assert causal_quote.source_identity == CausalSourceIdentity.from_book(source)

    result = measure_causal_quote(
        causal_quote,
        (
            trade(
                "factory-fill",
                received=120,
                price="97",
                ingress=120,
                normalized=121,
                block_number=1002,
            ),
        ),
        end_monotonic_ns=130,
    )
    assert result.outcome is CausalOutcome.FULL_FILL
    assert result.filled_quantity == D("1")
    assert not result.uncertainty_reasons


@pytest.mark.parametrize(
    ("witness", "expected_reason"),
    (
        ("risex-session", CausalUncertainty.SOURCE_IDENTITY_MISMATCH),
        ("risex-recovery", CausalUncertainty.SOURCE_IDENTITY_MISMATCH),
        ("risex-revision", CausalUncertainty.SOURCE_IDENTITY_MISMATCH),
        ("lighter-session", CausalUncertainty.SOURCE_IDENTITY_MISMATCH),
        ("lighter-recovery", CausalUncertainty.SOURCE_IDENTITY_MISMATCH),
        ("lighter-revision", CausalUncertainty.SOURCE_IDENTITY_MISMATCH),
    ),
)
def test_quote_version_factory_rejects_each_adverse_input_binding(
    witness: str,
    expected_reason: CausalUncertainty,
) -> None:
    source, hedge, version = _factory_version()
    adverse_source = source
    adverse_hedge = hedge
    if witness == "risex-session":
        adverse_source = book(received=90, ingress=80, normalized=85, session="risex-2", block_number=1001)
    elif witness == "risex-recovery":
        adverse_source = book(received=90, ingress=80, normalized=85, recovery=1, block_number=1001)
    elif witness == "risex-revision":
        adverse_source = book(received=90, ingress=80, normalized=85, revision=2, block_number=1002)
    elif witness == "lighter-session":
        adverse_hedge = book(
            received=90, ingress=80, normalized=85, venue=Venue.LIGHTER, session="lighter-2"
        )
    elif witness == "lighter-recovery":
        adverse_hedge = book(
            received=90, ingress=80, normalized=85, venue=Venue.LIGHTER, session="lighter-1", recovery=1
        )
    else:
        adverse_hedge = book(
            received=90, ingress=80, normalized=85, venue=Venue.LIGHTER, session="lighter-1", revision=2
        )

    causal_quote = build_causal_resting_quote(
        version,
        source_book=adverse_source,
        hedge_source_book=adverse_hedge,
    )
    result = measure_causal_quote(
        causal_quote,
        (
            trade(
                "factory-adverse-fill",
                received=120,
                price="97",
                ingress=120,
                normalized=121,
                block_number=1003,
            ),
        ),
        end_monotonic_ns=130,
    )
    assert result.outcome is CausalOutcome.CAUSAL_UNCERTAIN
    assert result.filled_quantity == D("0")
    assert expected_reason in result.uncertainty_reasons


def test_quote_version_factory_does_not_upgrade_legacy_missing_bindings() -> None:
    source, hedge, legacy_version = _factory_version(include_bindings=False)
    causal_quote = build_causal_resting_quote(
        legacy_version,
        source_book=source,
        hedge_source_book=hedge,
    )
    result = measure_causal_quote(
        causal_quote,
        (
            trade(
                "legacy-factory-fill",
                received=120,
                price="97",
                ingress=120,
                normalized=121,
                block_number=1002,
            ),
        ),
        end_monotonic_ns=130,
    )
    assert result.outcome is CausalOutcome.CAUSAL_UNCERTAIN
    assert result.observed_filled_quantity == D("1")
    assert result.filled_quantity == D("0")
    assert CausalUncertainty.MISSING_SOURCE_IDENTITY in result.uncertainty_reasons
    assert not result.is_clean_no_fill


def test_measurement_record_is_json_safe_and_public_aliases_have_same_result() -> None:
    event = causal_trade(trade("fill", received=120, ingress=120, normalized=121))
    result = measure_causal_quote(quote(quantity="1"), (event,))

    encoded = json.dumps(result.to_record(), sort_keys=True)
    assert "CAUSAL_QUOTE_MEASUREMENT_V1" in encoded
    assert result.status is CausalOutcome.FULL_FILL
    assert result.timing.first_event_received_monotonic_ns == 120
    assert event.kind is CausalEventKind.TRADE
    assert event.source_identity_complete
    assert isinstance(event.source_identity, CausalSourceIdentity)


def test_book_chain_preserves_causal_phases_without_upgrading_legacy_rows() -> None:
    from risex_spread_shadow import BookRevisionEncoder, reconstruct_book_records

    observed = book(received=90, ingress=80, normalized=85)
    record = BookRevisionEncoder().encode(observed, source_kind="SNAPSHOT")
    assert record["ingress_received_monotonic_ns"] == 80
    assert record["normalized_ready_monotonic_ns"] == 85
    restored = next(reconstruct_book_records((record,)))
    assert restored.causal_timing is not None
    assert restored.causal_timing.normalization_delay_ns == 5

    legacy = {
        key: value
        for key, value in record.items()
        if key
        not in {
            "ingress_received_monotonic_ns",
            "normalized_ready_monotonic_ns",
            "decision_ready_monotonic_ns",
        }
    }
    legacy_restored = next(reconstruct_book_records((legacy,)))
    assert legacy_restored.causal_timing is None


def test_replacement_boundary_prevents_old_quote_overlap() -> None:
    result = measure_causal_quote(
        quote(quantity="2", cancel_requested=130, replacement=130),
        (
            trade("before-replacement", received=120, ingress=120, normalized=121),
            trade("at-replacement", received=135, ingress=135, normalized=136),
        ),
    )
    assert result.outcome is CausalOutcome.PARTIAL_FILL
    assert result.observed_filled_quantity == D("1")
    assert result.decisions[-1].reason in {
        "AT_OR_AFTER_CANCEL_EFFECTIVE",
        "AT_OR_AFTER_REPLACEMENT",
    }


def test_replacement_keeps_pre_replacement_receipts_after_late_processing() -> None:
    result = measure_causal_quote(
        quote(quantity="3", cancel_requested=130, cancel_delay=10, replacement=200),
        (
            trade("before-replacement-late", received=139, ingress=139, normalized=250),
            trade("at-replacement-boundary", received=200, ingress=200, normalized=300),
        ),
        end_monotonic_ns=300,
    )

    assert result.outcome is CausalOutcome.PARTIAL_FILL
    assert result.observed_filled_quantity == D("1")
    assert result.fills[0].received_monotonic_ns == 139
    assert result.fills[0].processed_ready_monotonic_ns == 250
    assert result.decisions[-1].reason == "AT_OR_AFTER_CANCEL_EFFECTIVE"


def test_missing_input_witness_or_trade_wire_identity_cannot_be_positive() -> None:
    missing_trade_identity = trade(
        "synthetic",
        received=120,
        source_trade_id=None,
    )
    object.__setattr__(missing_trade_identity, "source_trade_id", None)
    missing_book = measure_causal_quote(
        quote(quantity="1", source_book=None, hedge_source_book=None),
        (missing_trade_identity,),
    )
    assert missing_book.outcome is CausalOutcome.CAUSAL_UNCERTAIN
    assert missing_book.filled_quantity == D("0")
    assert CausalUncertainty.MISSING_SOURCE_IDENTITY in missing_book.uncertainty_reasons
    assert CausalUncertainty.MISSING_CAUSAL_TIMING in missing_book.uncertainty_reasons


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("maker_order_id", "maker-order"),
        ("taker_order_id", "taker-order"),
        ("tx_hash", "0xshort"),
        ("trade_event_key", "synthetic-fallback"),
    ),
)
def test_risex_source_identity_requires_official_wire_shapes(field: str, value: str) -> None:
    malformed = trade("wire-shape", received=120, ingress=120, normalized=121)
    object.__setattr__(malformed, field, value)
    event = CausalEvent.from_trade(malformed)
    assert not event.source_identity_complete

    result = measure_causal_quote(quote(quantity="1"), (event,))
    assert result.outcome is CausalOutcome.CAUSAL_UNCERTAIN
    assert result.filled_quantity == D("0")
    assert CausalUncertainty.MISSING_SOURCE_IDENTITY in result.uncertainty_reasons


def test_normalization_after_decision_invalidates_positive_trade_measurement() -> None:
    late_book = book(received=90, ingress=90, normalized=500)
    result = measure_causal_quote(
        quote(
            quantity="1",
            decision=100,
            activation_delay=10,
            source_book=late_book,
        ),
        (trade("during-calculation", received=120, ingress=120, normalized=121),),
    )
    assert result.outcome is CausalOutcome.CAUSAL_UNCERTAIN
    assert result.observed_filled_quantity == D("1")
    assert result.filled_quantity == D("0")
    assert CausalUncertainty.SOURCE_BOOK_AFTER_DECISION in result.uncertainty_reasons


def test_auto_cancel_uses_first_partial_processing_ready_and_accounts_for_queue() -> None:
    result = measure_causal_quote(
        quote(quantity="3", cancel_delay=10, cancel_on_first_partial=True),
        (
            trade("first-partial", received=120, ingress=120, normalized=200),
            trade("queued-before-cancel", received=150, ingress=150, normalized=205),
        ),
    )
    assert result.outcome is CausalOutcome.PARTIAL_FILL
    assert result.observed_filled_quantity == D("2")
    assert result.cancel_requested_monotonic_ns == 200
    assert result.effective_cancel_monotonic_ns == 210
    assert result.fills[0].processed_ready_monotonic_ns == 200
    assert result.fills[0].price == D("100")
    assert result.fills[0].observed_trade_price == D("99")


def test_event_not_ready_by_explicit_end_cannot_become_a_fill_or_clean_no_fill() -> None:
    result = measure_causal_quote(
        quote(quantity="1"),
        (trade("processing-after-end", received=120, ingress=120, normalized=200),),
        end_monotonic_ns=150,
    )
    assert result.outcome is CausalOutcome.CAUSAL_UNCERTAIN
    assert result.observed_filled_quantity == D("0")
    assert result.filled_quantity == D("0")
    assert CausalUncertainty.EVENT_NOT_READY_BY_END in result.uncertainty_reasons
    assert result.decisions[-1].reason == CausalUncertainty.EVENT_NOT_READY_BY_END.value
