from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal as D

import pytest

from risex_farmer.models import CanonicalMarket, ContractType, MarketType
from risex_spread_shadow import (
    BookEvidence,
    BookLevel,
    DataGapEvidence,
    EntryViabilityOutcome,
    QuotePolicy,
    QuoteVersion,
    Side,
    SpreadDirection,
    TradeEvidence,
    Venue,
    build_entry_viability_episode,
    build_hypothetical_maker_quote,
    capture_horizon,
    compute_sizing_evidence,
    detect_strict_would_fill,
    horizon_deadline_monotonic_ns,
    queue_overflow_gap,
    validate_quote_economics,
    validate_sizing_evidence,
)


UTC = timezone.utc
NOW = datetime(2026, 9, 3, tzinfo=UTC)


def market(venue: Venue, *, minimum_notional: str = "0", step: str = "1") -> CanonicalMarket:
    return CanonicalMarket(
        canonical_asset="BTC",
        venue=venue,
        venue_symbol="BTC-PERP",
        market_type=MarketType.PERPETUAL,
        contract_type=ContractType.LINEAR,
        base_multiplier=D("1"),
        quote_asset="USDC",
        settlement_asset="USDC",
        tick_size_raw=D("1"),
        quantity_step_raw=D(step),
        minimum_quantity_raw=D("1"),
        minimum_notional_usd=D(minimum_notional),
        minimum_fee_notional_usd=None,
        is_active=True,
        is_rfq=False,
        is_off_hours=False,
    )


RISEX_MARKET = market(Venue.RISEX)
LIGHTER_MARKET = market(Venue.LIGHTER)


def book(
    *,
    bids=(("101", "1"), ("103", "2")),
    asks=(("105", "10"),),
    received=100,
    session="lighter-1",
    recovery=0,
    revision=1,
    sequence=1,
    fresh=True,
) -> BookEvidence:
    return BookEvidence(
        venue=Venue.LIGHTER,
        canonical_market="BTC",
        bids=tuple(BookLevel(D(price), D(quantity)) for price, quantity in bids),
        asks=tuple(BookLevel(D(price), D(quantity)) for price, quantity in asks),
        received_monotonic_ns=received,
        stream_session_id=session,
        recovery_generation=recovery,
        book_revision=revision,
        sequence=sequence,
        checksum="ok",
        sequence_valid=True,
        checksum_valid=True,
        received_utc=NOW,
        fresh=fresh,
    )


def policy(
    direction=SpreadDirection.RISEX_BUY_LIGHTER_SELL,
    *,
    target="307",
    margin="0",
    risex_fee="0",
    lighter_fee="0",
    best_bid="99",
    best_ask="101",
    tick="1",
    risex=RISEX_MARKET,
    lighter=LIGHTER_MARKET,
) -> QuotePolicy:
    return QuotePolicy(
        canonical_market="BTC",
        direction=direction,
        target_notional_usd=D(target),
        target_margin_bps=D(margin),
        risex_maker_fee_rate=D(risex_fee),
        lighter_taker_fee_rate=D(lighter_fee),
        risex_market=risex,
        lighter_market=lighter,
        risex_best_bid=D(best_bid),
        risex_best_ask=D(best_ask),
        risex_tick_size=D(tick),
    )


def active_quote_version(
    *,
    direction=SpreadDirection.RISEX_BUY_LIGHTER_SELL,
    quote_book: BookEvidence | None = None,
    version_id="v1",
    created=100,
    risex_fee="0",
    lighter_fee="0",
) -> QuoteVersion:
    quote_book = quote_book or book(received=90)
    p = policy(direction, risex_fee=risex_fee, lighter_fee=lighter_fee)
    quote = build_hypothetical_maker_quote(p, quote_book)
    assert quote.outcome is EntryViabilityOutcome.QUOTE_ACTIVE
    return QuoteVersion(
        version_id=version_id,
        quote=quote,
        quote_created_utc=NOW,
        quote_created_monotonic_ns=created,
        stream_session_id="risex-1",
        recovery_generation=0,
        quote_expires_monotonic_ns=500,
        hedge_stream_session_id="lighter-1",
        hedge_recovery_generation=0,
    )


def trade(
    key: str,
    *,
    price="99",
    quantity="1",
    received=101,
    aggressor=Side.SELL,
    session="risex-1",
    recovery=0,
    exchange_at=NOW,
    exchange_provenance="OFFICIAL_EVENT_UTC",
) -> TradeEvidence:
    return TradeEvidence(
        trade_event_key=key,
        venue=Venue.RISEX,
        canonical_market="BTC",
        canonical_price=D(price),
        canonical_quantity=D(quantity),
        aggressor_side=aggressor,
        received_utc=NOW,
        received_monotonic_ns=received,
        stream_session_id=session,
        recovery_generation=recovery,
        exchange_event_utc=exchange_at,
        exchange_event_time_provenance=exchange_provenance,
    )


def would_fill(*, detected=120):
    version = active_quote_version()
    evidence = detect_strict_would_fill(
        version,
        [trade("t2", quantity="2", received=102), trade("t1", received=101)],
        would_fill_detected_monotonic_ns=detected,
    )
    assert evidence is not None
    return evidence


def test_buy_edge_uses_exact_accumulated_notional_for_repeating_vwap() -> None:
    quote = build_hypothetical_maker_quote(policy(), book())

    assert quote.outcome is EntryViabilityOutcome.QUOTE_ACTIVE
    assert quote.canonical_quantity == D("3")
    assert quote.lighter_notional_usd == D("307")
    assert quote.lighter_vwap_price == D("102.3333333333333333333333333")
    assert quote.maker_price == D("100")
    assert quote.actual_edge_usd == D("7")


def test_sell_edge_has_the_opposite_exact_sign() -> None:
    sell_policy = policy(
        SpreadDirection.RISEX_SELL_LIGHTER_BUY,
        target="297",
        best_bid="101",
        best_ask="103",
    )
    sell_book = book(bids=(("95", "10"),), asks=(("99", "1"), ("100", "2")))
    quote = build_hypothetical_maker_quote(sell_policy, sell_book)

    assert quote.outcome is EntryViabilityOutcome.QUOTE_ACTIVE
    assert quote.lighter_notional_usd == D("299")
    assert quote.maker_price == D("102")
    assert quote.actual_edge_usd == D("7")


def test_visible_depth_is_not_subtracted_from_exact_vwap_a_second_time() -> None:
    quote = build_hypothetical_maker_quote(policy(), book())

    # 307 - 300 is already the depth-aware exact entry edge.
    assert quote.actual_edge_usd == D("7")


def test_each_fee_component_is_applied_once_with_provenance() -> None:
    quote = build_hypothetical_maker_quote(
        policy(risex_fee="0.01", lighter_fee="0.02", best_bid="89", best_ask="91"),
        book(),
    )

    assert quote.outcome is EntryViabilityOutcome.QUOTE_ACTIVE
    assert [fee.amount_usd for fee in quote.fee_components] == [D("2.70"), D("6.14")]
    assert quote.total_entry_fees_usd == D("8.84")
    assert quote.actual_edge_usd == D("28.16")
    assert [fee.source for fee in quote.fee_components] == [
        "CONFIGURED_RISEX_RESEARCH_INPUT",
        "OFFICIAL_LIGHTER_STANDARD_RESEARCH_INPUT",
    ]


def test_post_only_cap_and_post_rounding_revalidation_are_fail_closed() -> None:
    quote = build_hypothetical_maker_quote(
        policy(target="307", margin="10000"),
        book(),
    )
    assert quote.outcome is EntryViabilityOutcome.QUOTE_NOT_ECONOMIC

    crossed = build_hypothetical_maker_quote(
        policy(best_bid="100", best_ask="100"),
        book(),
    )
    assert crossed.outcome is EntryViabilityOutcome.QUOTE_NOT_POST_ONLY


def test_sizing_recomputes_raw_common_floor_and_both_minimums() -> None:
    evidence = compute_sizing_evidence(
        policy(target="307"), RISEX_MARKET, LIGHTER_MARKET, book()
    )

    assert evidence.reference_price == D("101")
    assert evidence.q_raw == D("3.039603960396039603960396040")
    assert evidence.common_quantity_step == D("1")
    assert evidence.floored_quantity == D("3")
    assert evidence.raw_venue_quantities == (D("3"), D("3"))
    assert evidence.risex_minimum_ok and evidence.lighter_minimum_ok
    assert validate_sizing_evidence(evidence, policy(target="307"), book())


@pytest.mark.parametrize(
    "change",
    [
        {"direction": SpreadDirection.RISEX_SELL_LIGHTER_BUY},
        {"target_notional_usd": D("500")},
        {"floored_quantity": D("4")},
        {"risex_min_notional_ok": False},
    ],
)
def test_mismatched_or_forged_sizing_is_not_economic(change) -> None:
    p = policy()
    evidence = compute_sizing_evidence(p, RISEX_MARKET, LIGHTER_MARKET, book())
    forged = replace(evidence, **change)

    assert not validate_sizing_evidence(forged, p, book())
    quote = build_hypothetical_maker_quote(p, book(), sizing_evidence=forged)
    assert quote.outcome is EntryViabilityOutcome.QUOTE_NOT_ECONOMIC


def test_quote_with_missing_economics_cannot_be_active() -> None:
    p = policy()
    quote = build_hypothetical_maker_quote(p, book(), sizing_evidence=None)
    assert quote.sizing_evidence is not None
    assert validate_quote_economics(quote)
    forged = replace(quote, actual_edge_usd=None)
    assert not validate_quote_economics(forged)


def test_strict_fill_requires_quote_before_trade_by_local_receipt() -> None:
    version = active_quote_version()
    later_exchange = datetime(2026, 9, 3, 0, 0, 1, tzinfo=UTC)
    assert detect_strict_would_fill(
        version,
        [trade("too-early", received=99, exchange_at=later_exchange, quantity="3")],
    ) is None


def test_strict_fill_stops_at_first_local_threshold_and_retains_no_late_trade() -> None:
    version = active_quote_version()
    evidence = detect_strict_would_fill(
        version,
        [
            trade("late", quantity="9", received=130),
            trade("second", quantity="2", received=102),
            trade("first", quantity="1", received=101),
        ],
    )

    assert evidence is not None
    assert evidence.qualifying_trade_event_keys == ("first", "second")
    assert evidence.cumulative_eligible_quantity == D("3")
    assert evidence.would_fill_detected_monotonic_ns == 102
    assert detect_strict_would_fill(
        version,
        [
            trade("first", quantity="1", received=101),
            trade("second", quantity="2", received=102),
        ],
        would_fill_detected_monotonic_ns=101,
    ) is None
    delayed = detect_strict_would_fill(
        version,
        [
            trade("late", quantity="9", received=130),
            trade("first", quantity="1", received=101),
            trade("second", quantity="2", received=102),
        ],
        would_fill_detected_monotonic_ns=150,
    )
    assert delayed is not None
    assert delayed.qualifying_trade_event_keys == ("first", "second")
    assert delayed.would_fill_detected_monotonic_ns == 150


def test_wrong_aggressor_does_not_fill() -> None:
    version = active_quote_version()
    assert detect_strict_would_fill(
        version,
        [trade("wrong", aggressor=Side.BUY, quantity="3")],
    ) is None


def test_one_tick_trade_through_boundary_is_exact() -> None:
    version = active_quote_version()
    assert detect_strict_would_fill(version, [trade("at-limit", price="100", quantity="3")]) is None
    evidence = detect_strict_would_fill(version, [trade("one-tick", price="99", quantity="3")])
    assert evidence is not None


def test_duplicate_trade_key_cannot_supply_quantity_twice() -> None:
    version = active_quote_version()
    duplicate = trade("same", quantity="2")
    assert detect_strict_would_fill(version, [duplicate, duplicate]) is None


def test_conflicting_duplicate_trade_key_fails_closed_independent_of_input_order() -> None:
    version = active_quote_version()
    first = trade("same", quantity="2", received=101)
    conflicting = trade("same", quantity="3", received=101)
    other = trade("other", quantity="1", received=102)

    assert detect_strict_would_fill(version, [first, conflicting, other]) is None
    assert detect_strict_would_fill(version, [other, conflicting, first]) is None

    identical = detect_strict_would_fill(version, [other, first, first])
    assert identical is not None
    assert identical.qualifying_trade_event_keys == ("same", "other")
    assert identical.cumulative_eligible_quantity == D("3")


@pytest.mark.parametrize(
    ("exchange_at", "exchange_provenance"),
    [(None, "OFFICIAL_EVENT_UTC"), (NOW, None)],
)
def test_exchange_utc_and_provenance_must_be_supplied_as_a_pair(
    exchange_at, exchange_provenance
) -> None:
    with pytest.raises(ValueError):
        trade(
            "unpaired",
            exchange_at=exchange_at,
            exchange_provenance=exchange_provenance,
        )


def test_replacement_resets_version_local_fill_evidence() -> None:
    old = active_quote_version(version_id="old", created=100)
    new = active_quote_version(version_id="new", created=200)
    old_trade = trade("old-fill", received=101, quantity="3")
    assert detect_strict_would_fill(old, [old_trade]) is not None
    assert detect_strict_would_fill(new, [old_trade]) is None
    new_trade = trade("new-fill", received=201, quantity="3")
    evidence = detect_strict_would_fill(new, [old_trade, new_trade])
    assert evidence is not None
    assert evidence.quote_version_id == "new"
    assert evidence.qualifying_trade_event_keys == ("new-fill",)


def test_expiry_and_risex_gap_forbid_would_fill_but_lighter_gap_does_not() -> None:
    version = active_quote_version()
    lighter_gap = DataGapEvidence(Venue.LIGHTER, "BTC", "lighter-1", 0, 100, 130)
    assert detect_strict_would_fill(
        version,
        [trade("fill", quantity="3")],
        data_gaps=[lighter_gap],
    ) is not None
    risex_gap = DataGapEvidence(Venue.RISEX, "BTC", "risex-1", 0, 100, 130)
    assert detect_strict_would_fill(
        version,
        [trade("fill", quantity="3")],
        data_gaps=[risex_gap],
    ) is None
    assert detect_strict_would_fill(
        active_quote_version(),
        [trade("expired", quantity="3", received=499)],
        would_fill_detected_monotonic_ns=500,
    ) is None


def test_horizon_deadline_is_local_detection_plus_integer_ms() -> None:
    assert horizon_deadline_monotonic_ns(1_000_000_000, 500) == 1_500_000_000
    assert horizon_deadline_monotonic_ns(1_000_000_000, 0) == 1_000_000_000
    with pytest.raises(ValueError):
        horizon_deadline_monotonic_ns(1_000_000_000, 1)


def test_horizon_rejects_book_received_one_ns_after_deadline() -> None:
    capture = capture_horizon(
        would_fill(),
        [book(received=121)],
        horizon_ms=0,
        expected_stream_session_id="lighter-1",
        expected_recovery_generation=0,
    )
    assert capture.outcome is EntryViabilityOutcome.HEDGE_DATA_MISSING
    assert capture.book is None


def test_horizon_uses_exact_notional_and_full_multilevel_vwap() -> None:
    capture = capture_horizon(
        would_fill(),
        [book(received=120)],
        horizon_ms=0,
    )

    assert capture.outcome is EntryViabilityOutcome.HEDGE_FULL
    assert capture.filled_quantity == D("3")
    assert capture.notional_usd == D("307")
    assert capture.vwap_price == D("102.3333333333333333333333333")


def test_horizon_distinguishes_partial_zero_depth_missing_stale_and_displaced() -> None:
    evidence = would_fill()
    partial = capture_horizon(evidence, [book(bids=(("101", "1"),), received=120)], horizon_ms=0)
    zero = capture_horizon(evidence, [book(bids=(), received=120)], horizon_ms=0)
    missing = capture_horizon(evidence, [], horizon_ms=0)
    stale_book = book(received=120, fresh=False)
    displaced_book = book(received=120, session="new-session")
    stale = capture_horizon(evidence, [stale_book], horizon_ms=0)
    displaced = capture_horizon(evidence, [displaced_book], horizon_ms=0)

    assert partial.outcome is EntryViabilityOutcome.HEDGE_PARTIAL
    assert partial.filled_quantity == D("1") and partial.notional_usd == D("101")
    assert zero.outcome is EntryViabilityOutcome.HEDGE_DEPTH_UNAVAILABLE
    assert zero.filled_quantity == D("0")
    assert missing.outcome is EntryViabilityOutcome.HEDGE_DATA_MISSING
    assert stale.outcome is EntryViabilityOutcome.HEDGE_DATA_STALE
    assert displaced.outcome is EntryViabilityOutcome.HEDGE_SESSION_DISPLACED
    assert stale.book == stale_book
    assert stale.book_received_monotonic_ns == 120
    assert stale.book_stream_session_id == "lighter-1"
    assert stale.book_recovery_generation == 0
    assert stale.book_revision == stale_book.book_revision
    assert stale.sequence == stale_book.sequence and stale.checksum == stale_book.checksum
    assert displaced.book == displaced_book
    assert displaced.book_stream_session_id == "new-session"
    assert displaced.book_recovery_generation == 0
    assert displaced.book_revision == displaced_book.book_revision


def test_horizon_binds_recovery_generation_and_freshness_policy() -> None:
    evidence = would_fill()
    displaced_book = book(received=120, recovery=1)
    displaced = capture_horizon(
        evidence,
        [displaced_book],
        horizon_ms=0,
        expected_stream_session_id="lighter-1",
        expected_recovery_generation=0,
    )
    stale_book = book(received=100, fresh=True)
    stale = capture_horizon(
        evidence,
        [stale_book],
        horizon_ms=0,
        freshness_max_age_ns=19,
    )
    assert displaced.outcome is EntryViabilityOutcome.HEDGE_SESSION_DISPLACED
    assert stale.outcome is EntryViabilityOutcome.HEDGE_DATA_STALE
    assert displaced.book == displaced_book
    assert displaced.book_recovery_generation == 1
    assert stale.book == stale_book
    assert stale.book_received_monotonic_ns == 100


def test_horizon_gap_from_pre_detection_book_to_zero_deadline_is_retained() -> None:
    evidence = would_fill()
    selected = book(received=100)
    gap = DataGapEvidence(Venue.LIGHTER, "BTC", "lighter-1", 0, 110, 119)

    capture = capture_horizon(
        evidence,
        [selected],
        horizon_ms=0,
        data_gaps=[gap],
    )

    assert capture.outcome is EntryViabilityOutcome.HEDGE_DATA_GAP
    assert capture.book == selected
    assert capture.gap_evidence == gap
    assert capture.book_received_monotonic_ns == selected.received_monotonic_ns
    assert capture.book_stream_session_id == selected.stream_session_id
    assert capture.book_recovery_generation == selected.recovery_generation
    assert capture.book_revision == selected.book_revision
    assert capture.sequence == selected.sequence and capture.checksum == selected.checksum


def test_horizon_tied_latest_books_fail_closed_with_order_independent_provenance() -> None:
    evidence = would_fill()
    first_book = book(
        received=120,
        revision=7,
        sequence=42,
        bids=(("101", "1"), ("103", "2")),
    )
    second_book = book(
        received=120,
        revision=7,
        sequence=42,
        bids=(("101", "3"),),
    )

    first = capture_horizon(evidence, [first_book, second_book], horizon_ms=0)
    second = capture_horizon(evidence, [second_book, first_book], horizon_ms=0)

    assert first == second
    assert first.outcome is EntryViabilityOutcome.HEDGE_OUTCOME_UNKNOWN
    assert first.book is None
    assert first.ambiguous_books == tuple(sorted((first_book, second_book), key=repr))


def test_quote_validation_binds_exact_vwap_without_requiring_rounded_notional() -> None:
    quote = build_hypothetical_maker_quote(policy(), book())
    assert quote.exact_hedge_vwap is not None
    assert quote.lighter_notional_usd != quote.canonical_quantity * quote.lighter_vwap_price
    assert validate_quote_economics(quote)

    for change in (
        {"requested_quantity": D("4")},
        {"filled_quantity": D("2")},
        {"notional_usd": D("308")},
        {"price": D("102")},
    ):
        forged_vwap = replace(quote.exact_hedge_vwap, **change)
        forged_quote = replace(quote, exact_hedge_vwap=forged_vwap)
        assert not validate_quote_economics(forged_quote)


def test_horizon_outcomes_reject_known_misclassification_and_preserve_truthful_states() -> None:
    evidence = would_fill()
    full = capture_horizon(evidence, [book(received=120)], horizon_ms=0)
    partial = capture_horizon(
        evidence,
        [book(received=120, bids=(("101", "1"),))],
        horizon_ms=0,
    )
    zero = capture_horizon(evidence, [book(received=120, bids=())], horizon_ms=0)
    missing = capture_horizon(evidence, [], horizon_ms=0)
    stale_book = book(received=120, fresh=False)
    stale = capture_horizon(evidence, [stale_book], horizon_ms=0)
    displaced_book = book(received=120, recovery=1)
    displaced = capture_horizon(evidence, [displaced_book], horizon_ms=0)
    gap = DataGapEvidence(Venue.LIGHTER, "BTC", "lighter-1", 0, 110, 119)
    gapped = capture_horizon(evidence, [book(received=100)], horizon_ms=0, data_gaps=[gap])

    assert full.outcome is EntryViabilityOutcome.HEDGE_FULL
    assert partial.outcome is EntryViabilityOutcome.HEDGE_PARTIAL
    assert zero.outcome is EntryViabilityOutcome.HEDGE_DEPTH_UNAVAILABLE
    assert missing.book is None
    assert stale.book == stale_book
    assert displaced.book == displaced_book
    assert gapped.gap_evidence == gap

    with pytest.raises(ValueError):
        replace(missing, outcome=EntryViabilityOutcome.HEDGE_OUTCOME_UNKNOWN)
    with pytest.raises(ValueError):
        replace(stale, outcome=EntryViabilityOutcome.HEDGE_OUTCOME_UNKNOWN)
    with pytest.raises(ValueError):
        replace(displaced, outcome=EntryViabilityOutcome.HEDGE_OUTCOME_UNKNOWN)
    with pytest.raises(ValueError):
        replace(gapped, outcome=EntryViabilityOutcome.HEDGE_OUTCOME_UNKNOWN)
    with pytest.raises(ValueError):
        replace(partial, outcome=EntryViabilityOutcome.HEDGE_OUTCOME_UNKNOWN)
    with pytest.raises(ValueError):
        replace(zero, outcome=EntryViabilityOutcome.HEDGE_OUTCOME_UNKNOWN)
    with pytest.raises(ValueError):
        replace(full, outcome=EntryViabilityOutcome.HEDGE_DATA_MISSING, filled_quantity=D("0"), notional_usd=D("0"), vwap_price=None)


def test_non_fill_episode_and_horizon_factory_cannot_produce_captures() -> None:
    version = active_quote_version()
    episode = build_entry_viability_episode(
        version,
        [],
        books_by_horizon={0: (book(received=120),)},
        horizons=(0,),
    )

    assert episode.outcome is EntryViabilityOutcome.NO_WOULD_FILL
    assert episode.horizon_captures == ()
    with pytest.raises(TypeError):
        capture_horizon(episode, [], horizon_ms=0)
    with pytest.raises(ValueError):
        replace(
            episode,
            horizon_captures=(capture_horizon(would_fill(), [book(received=120)], horizon_ms=0),),
        )


def test_only_lighter_gap_invalidates_hedge_evidence_and_retains_identity() -> None:
    evidence = would_fill()
    risex_gap = DataGapEvidence(Venue.RISEX, "BTC", "risex-1", 0, 100, 130)
    lighter_gap = DataGapEvidence(Venue.LIGHTER, "BTC", "lighter-1", 0, 100, 130)
    unaffected = capture_horizon(evidence, [book(received=120)], horizon_ms=0, data_gaps=[risex_gap])
    blocked = capture_horizon(evidence, [book(received=120)], horizon_ms=0, data_gaps=[lighter_gap])

    assert unaffected.outcome is EntryViabilityOutcome.HEDGE_FULL
    assert blocked.outcome is EntryViabilityOutcome.HEDGE_DATA_GAP
    assert blocked.gap_evidence == lighter_gap
    assert blocked.gap_evidence.source_venue is Venue.LIGHTER
    assert blocked.gap_evidence.stream_session_id == "lighter-1"
    assert blocked.gap_evidence.recovery_generation == 0


def test_queue_overflow_is_an_explicit_gap_contract_without_a_queue() -> None:
    gap = queue_overflow_gap(
        source_venue=Venue.RISEX,
        canonical_market="BTC",
        stream_session_id="risex-1",
        recovery_generation=0,
        gap_start_monotonic_ns=100,
        gap_end_monotonic_ns=120,
    )
    assert gap.reason == "QUEUE_OVERFLOW"
    assert gap.source_venue is Venue.RISEX


def test_deterministic_replay_produces_equal_immutable_evidence() -> None:
    version = active_quote_version()
    trades = (trade("t2", quantity="2", received=102), trade("t1", received=101))
    horizon_books = {0: (book(received=120),), 300: (book(received=120),)}
    first = build_entry_viability_episode(
        version,
        trades,
        books_by_horizon=horizon_books,
        expected_stream_session_id="lighter-1",
        expected_recovery_generation=0,
        would_fill_detected_monotonic_ns=120,
        horizons=(0, 300),
    )
    second = build_entry_viability_episode(
        version,
        tuple(reversed(trades)),
        books_by_horizon=horizon_books,
        expected_stream_session_id="lighter-1",
        expected_recovery_generation=0,
        would_fill_detected_monotonic_ns=120,
        horizons=(0, 300),
    )

    assert first == second
    assert first.outcome is EntryViabilityOutcome.WOULD_FILL
    assert [capture.outcome for capture in first.horizon_captures] == [
        EntryViabilityOutcome.HEDGE_FULL,
        EntryViabilityOutcome.HEDGE_FULL,
    ]
