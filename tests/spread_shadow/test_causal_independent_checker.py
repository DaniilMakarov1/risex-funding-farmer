from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal as D, ROUND_FLOOR
from fractions import Fraction
from math import gcd

from risex_farmer.models import (
    BookLevel,
    CanonicalMarket,
    ContractType,
    MarketType,
    Side,
)
from risex_spread_shadow import (
    BookEvidence,
    QuotePolicy,
    SpreadDirection,
    Venue,
)


UTC = timezone.utc
NOW = datetime(2026, 9, 5, tzinfo=UTC)


def _independent_common_step(
    risex_step: D,
    risex_multiplier: D,
    lighter_step: D,
    lighter_multiplier: D,
) -> D:
    """Small checker kept independent of production sizing/economics code."""

    risex_grid = Fraction(risex_step) * Fraction(risex_multiplier)
    lighter_grid = Fraction(lighter_step) * Fraction(lighter_multiplier)
    # The smallest shared grid is the least common multiple of the two
    # rational increments, represented without Decimal rounding.
    numerator_lcm = abs(risex_grid.numerator * lighter_grid.numerator) // gcd(
        risex_grid.numerator, lighter_grid.numerator
    )
    denominator_gcd = gcd(risex_grid.denominator, lighter_grid.denominator)
    return D(numerator_lcm) / D(denominator_gcd)


def _independent_level_notional(
    levels: tuple[tuple[D, D], ...],
) -> D:
    return sum((price * quantity for price, quantity in levels), D("0"))


def _independent_fee(notional: D, rate: D) -> D:
    return notional * rate


def _independent_floor_to_tick(value: D, tick: D) -> D:
    return (value / tick).to_integral_value(rounding=ROUND_FLOOR) * tick


def _independent_exact_vwap(
    side: Side,
    quantity: D,
    bids: tuple[tuple[D, D], ...],
    asks: tuple[tuple[D, D], ...],
) -> tuple[D, D, D | None]:
    levels = asks if side is Side.BUY else bids
    remaining = quantity
    filled = D("0")
    notional = D("0")
    for price, available in levels:
        consumed = min(remaining, available)
        filled += consumed
        notional += consumed * price
        remaining -= consumed
        if remaining == 0:
            break
    return filled, notional, None if filled != quantity else notional / filled


def _independent_edge(
    direction: SpreadDirection,
    quantity: D,
    maker_price: D,
    hedge_notional: D,
    fees: D,
) -> D:
    maker_notional = quantity * maker_price
    if direction.maker_side is Side.BUY:
        return hedge_notional - maker_notional - fees
    return maker_notional - hedge_notional - fees


def _market(
    venue: Venue,
    *,
    quantity_step: str,
    tick: str,
    minimum_quantity: str | None = None,
    minimum_notional: str = "1",
) -> CanonicalMarket:
    return CanonicalMarket(
        canonical_asset="BTC",
        venue=venue,
        venue_symbol="BTC",
        market_type=MarketType.PERPETUAL,
        contract_type=ContractType.LINEAR,
        base_multiplier=D("1"),
        quote_asset="USD",
        settlement_asset="USD",
        tick_size_raw=D(tick),
        quantity_step_raw=D(quantity_step),
        minimum_quantity_raw=D(minimum_quantity or quantity_step),
        minimum_notional_usd=D(minimum_notional),
        minimum_fee_notional_usd=None,
        is_active=True,
        is_rfq=False,
        is_off_hours=False,
    )


def test_independent_checker_recomputes_and_compares_production_sizing_vwap_edge_and_markout() -> None:
    # These imports are deliberately inside the comparison test.  The
    # reference functions above remain independent and cannot accidentally
    # call production arithmetic.
    from risex_spread_shadow import (
        EntryViabilityOutcome,
        FillabilityModel,
        QuoteVersion,
        ShadowConfig,
        TradeEvidence,
        build_entry_viability_episode,
        build_hypothetical_maker_quote,
        compute_sizing_evidence,
        exact_vwap,
    )
    from types import SimpleNamespace
    from risex_spread_shadow.runner import SpreadObserver

    risex_market = _market(Venue.RISEX, quantity_step="0.01", tick="0.01")
    lighter_market = _market(Venue.LIGHTER, quantity_step="0.0025", tick="0.0025")
    lighter_book = _book(
        1,
        1,
        (("100.01", "2"), ("100.00", "5")),
        (("100.51", "5"), ("100.52", "5")),
        venue=Venue.LIGHTER,
        session="lighter-1",
    )
    policy = QuotePolicy(
        canonical_market="BTC",
        direction=SpreadDirection.RISEX_BUY_LIGHTER_SELL,
        target_notional_usd=D("300"),
        target_margin_bps=D("1"),
        risex_maker_fee_rate=D("0.001"),
        lighter_taker_fee_rate=D("0.0005"),
        risex_market=risex_market,
        lighter_market=lighter_market,
        risex_best_bid=D("99"),
        risex_best_ask=D("101"),
        risex_tick_size=D("0.01"),
        fee_observed_or_configured_at=NOW,
    )

    common_step = _independent_common_step(D("0.01"), D("1"), D("0.0025"), D("1"))
    reference_price = D("100.01")
    reference_quantity = (D("300") / reference_price // common_step) * common_step
    sizing = compute_sizing_evidence(
        policy,
        risex_market,
        lighter_market,
        lighter_book,
    )
    assert common_step == D("0.01")
    assert sizing.common_quantity_step == common_step
    assert sizing.floored_quantity == reference_quantity
    assert sizing.risex_min_quantity_ok and sizing.lighter_min_quantity_ok
    assert sizing.risex_min_notional_ok and sizing.lighter_min_notional_ok

    expected_filled, expected_notional, expected_price = _independent_exact_vwap(
        Side.SELL,
        reference_quantity,
        tuple((level.canonical_price, level.canonical_quantity) for level in lighter_book.bids),
        tuple((level.canonical_price, level.canonical_quantity) for level in lighter_book.asks),
    )
    production_vwap = exact_vwap(
        Side.SELL,
        sizing.floored_quantity,
        lighter_book.bids,
        lighter_book.asks,
    )
    assert production_vwap.filled_quantity == expected_filled
    assert production_vwap.notional_usd == expected_notional
    assert production_vwap.price == expected_price

    assert expected_price is not None
    margin = D("1") / D("10000")
    expected_raw_bound = expected_price * (D("1") - D("0.0005") - margin) / (
        D("1") + D("0.001")
    )
    expected_maker_price = min(
        _independent_floor_to_tick(expected_raw_bound, D("0.01")),
        D("101") - D("0.01"),
    )
    production_quote = build_hypothetical_maker_quote(
        policy,
        lighter_book,
        risex_market=risex_market,
        lighter_market=lighter_market,
        risex_best_bid=D("99"),
        risex_best_ask=D("101"),
        risex_tick_size=D("0.01"),
    )
    assert production_quote.outcome is EntryViabilityOutcome.QUOTE_ACTIVE
    assert production_quote.canonical_quantity == reference_quantity
    assert production_quote.maker_price == expected_maker_price
    assert production_quote.raw_risex_price_bound == expected_raw_bound
    assert production_quote.lighter_filled_quantity == expected_filled
    assert production_quote.lighter_notional_usd == expected_notional
    assert production_quote.lighter_vwap_price == expected_price

    expected_maker_notional = reference_quantity * expected_maker_price
    expected_maker_fee = _independent_fee(expected_maker_notional, D("0.001"))
    expected_hedge_fee = _independent_fee(expected_notional, D("0.0005"))
    expected_total_fees = expected_maker_fee + expected_hedge_fee
    expected_entry_edge = _independent_edge(
        policy.direction,
        reference_quantity,
        expected_maker_price,
        expected_notional,
        expected_total_fees,
    )
    assert production_quote.total_entry_fees_usd == expected_total_fees
    assert sum((fee.amount_usd for fee in production_quote.fee_components), D("0")) == expected_total_fees
    assert production_quote.actual_edge_usd == expected_entry_edge

    version = QuoteVersion(
        version_id="episode-version",
        quote=production_quote,
        quote_created_utc=NOW,
        quote_created_monotonic_ns=10,
        stream_session_id="risex-1",
        recovery_generation=0,
        quote_expires_monotonic_ns=1_000_000_000,
        hedge_stream_session_id="lighter-1",
        hedge_recovery_generation=0,
    )
    trade = TradeEvidence(
        trade_event_key="episode-trade",
        venue=Venue.RISEX,
        canonical_market="BTC",
        canonical_price=expected_maker_price - D("0.01"),
        canonical_quantity=reference_quantity,
        aggressor_side=Side.SELL,
        received_utc=NOW,
        received_monotonic_ns=20,
        stream_session_id="risex-1",
        recovery_generation=0,
        exchange_event_utc=NOW,
        exchange_event_time_provenance="CHECKER_FIXTURE",
    )
    horizon_book = _book(
        2,
        300,
        (("99", "5"), ("98", "5")),
        (("100", "5"),),
        venue=Venue.LIGHTER,
        session="lighter-1",
    )
    episode = build_entry_viability_episode(
        version,
        (trade,),
        books_by_horizon={0: (lighter_book,), 300: (horizon_book,)},
        expected_stream_session_id="lighter-1",
        expected_recovery_generation=0,
        would_fill_detected_monotonic_ns=20,
        detected_utc=NOW,
        horizons=(0, 300),
        freshness_max_age_ns=1_000_000_000,
        fillability_model=FillabilityModel.STRICT_LOWER_BOUND,
    )
    assert episode.outcome is EntryViabilityOutcome.WOULD_FILL
    assert episode.would_fill_evidence is not None
    assert len(episode.horizon_captures) == 2

    expected_horizon_filled, expected_horizon_notional, expected_horizon_vwap = _independent_exact_vwap(
        Side.SELL,
        reference_quantity,
        ((D("99"), D("5")), (D("98"), D("5"))),
        ((D("100"), D("5")),),
    )
    horizon_capture = episode.horizon_captures[1]
    assert horizon_capture.filled_quantity == expected_horizon_filled == reference_quantity
    assert horizon_capture.notional_usd == expected_horizon_notional
    assert horizon_capture.vwap_price == expected_horizon_vwap

    observer = SpreadObserver(
        ShadowConfig(),
        (),
        SimpleNamespace(run_id="checker"),
    )
    horizon_record = observer._horizon_record(version, horizon_capture)
    expected_horizon_maker_fee = _independent_fee(expected_maker_notional, D("0.001"))
    expected_horizon_hedge_fee = _independent_fee(expected_horizon_notional, D("0.0005"))
    expected_exit_edge = _independent_edge(
        policy.direction,
        reference_quantity,
        expected_maker_price,
        expected_horizon_notional,
        expected_horizon_maker_fee + expected_horizon_hedge_fee,
    )
    expected_markout = expected_exit_edge - expected_entry_edge
    assert horizon_record["entry_edge_usd"] == expected_exit_edge
    assert horizon_record["conditional_markout_usd"] == expected_markout

    # An independently assigned adverse minimum case must reject the same
    # quantity on both legs; the checker does not trust production flags.
    adverse_risex = _market(
        Venue.RISEX,
        quantity_step="0.01",
        tick="0.01",
        minimum_quantity="3",
        minimum_notional="300",
    )
    adverse_lighter = _market(
        Venue.LIGHTER,
        quantity_step="0.0025",
        tick="0.0025",
        minimum_quantity="3",
        minimum_notional="300",
    )
    adverse = compute_sizing_evidence(
        policy,
        adverse_risex,
        adverse_lighter,
        lighter_book,
    )
    assert adverse.risex_min_quantity_ok == (reference_quantity >= D("3"))
    assert adverse.risex_min_notional_ok == (reference_quantity * D("99") >= D("300"))
    assert adverse.lighter_min_quantity_ok == (reference_quantity >= D("3"))
    assert adverse.lighter_min_notional_ok == (reference_quantity * D("100.01") >= D("300"))
    assert not adverse.is_valid


def _book(
    revision: int,
    received: int,
    bids: tuple[tuple[str, str], ...],
    asks: tuple[tuple[str, str], ...],
    *,
    venue: Venue = Venue.RISEX,
    session: str = "risex-1",
) -> BookEvidence:
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
    )


def test_adverse_full_delta_chains_end_at_independently_assigned_terminal_books() -> None:
    # The expected terminal states are written independently of the encoder's
    # change calculation.  This catches an omitted delete as well as an
    # accidentally retained stale level.
    cases = (
        (
            _book(1, 1, (("100", "3"), ("99", "2")), (("101", "4"), ("102", "1"))),
            _book(2, 2, (("98", "5"),), (("101", "6"),)),
            ((D("98"), D("5")),),
            ((D("101"), D("6")),),
        ),
        (
            _book(1, 11, (("110", "1"),), (("111", "2"), ("112", "3"))),
            _book(2, 12, (("110", "1"), ("109", "4")), (("112", "3"), ("113", "1"))),
            ((D("110"), D("1")), (D("109"), D("4"))),
            ((D("112"), D("3")), (D("113"), D("1"))),
        ),
    )

    # Local import keeps the independent arithmetic test above free of all
    # production sizing/VWAP/fee/reconstruction imports.
    from risex_spread_shadow import BookRevisionEncoder, reconstruct_book_records

    for first, second, expected_bids, expected_asks in cases:
        encoder = BookRevisionEncoder()
        full = encoder.encode(first, source_kind="SNAPSHOT")
        delta = encoder.encode(second, source_kind="DELTA")
        reconstructed = tuple(reconstruct_book_records((full, delta)))
        terminal = reconstructed[-1]
        assert tuple(
            (level.canonical_price, level.canonical_quantity) for level in terminal.bids
        ) == expected_bids
        assert tuple(
            (level.canonical_price, level.canonical_quantity) for level in terminal.asks
        ) == expected_asks
        assert delta["book_encoding"] == "DELTA"
