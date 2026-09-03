"""Pure fillability and no-lookahead horizon operations for SS-001A."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from decimal import Decimal

from risex_farmer.economics import exact_quantity_vwap, is_tick_aligned
from risex_farmer.models import Side, Venue

from .economics import validate_quote_economics
from .models import (
    BookEvidence,
    DataGapEvidence,
    EntryViabilityEpisode,
    EntryViabilityOutcome,
    FillabilityModel,
    HedgeHorizonCapture,
    QuoteVersion,
    SpreadDirection,
    TradeEvidence,
    WouldFillEvidence,
)


def _int(value: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be int")
    if value < 0:
        raise ValueError(f"{name} must be non-negative")
    return value


def _trade_qualifies(quote_version: QuoteVersion, trade: TradeEvidence, tick: Decimal) -> bool:
    quote = quote_version.quote
    if trade.venue is not Venue.RISEX:
        return False
    if trade.canonical_market != quote_version.canonical_market:
        return False
    if trade.stream_session_id != quote_version.stream_session_id:
        return False
    if trade.recovery_generation != quote_version.recovery_generation:
        return False
    if trade.received_monotonic_ns <= quote_version.quote_created_monotonic_ns:
        return False
    if quote_version.quote_expires_monotonic_ns is not None and (
        trade.received_monotonic_ns >= quote_version.quote_expires_monotonic_ns
    ):
        return False
    if not is_tick_aligned(trade.canonical_price, tick):
        return False
    if quote.maker_side is Side.BUY:
        return (
            trade.aggressor_side is Side.SELL
            and trade.canonical_price <= quote.maker_price - tick  # type: ignore[operator]
        )
    return (
        trade.aggressor_side is Side.BUY
        and trade.canonical_price >= quote.maker_price + tick  # type: ignore[operator]
    )


def is_eligible_trade(quote_version: QuoteVersion, trade: TradeEvidence) -> bool:
    """Return whether a trade is relevant to at least one active quote version.

    Eligibility deliberately stops before price qualification.  A trade can be
    eligible while merely missing, touching, or passing a quote; those are
    separate fillability observations.  Local receipt ordering and the quote's
    stream identity remain the only ordering/identity authorities.
    """

    quote = quote_version.quote
    if not quote_version.is_active or quote.maker_price is None:
        return False
    return (
        trade.venue is Venue.RISEX
        and trade.canonical_market == quote_version.canonical_market
        and trade.stream_session_id == quote_version.stream_session_id
        and trade.recovery_generation == quote_version.recovery_generation
        and trade.received_monotonic_ns > quote_version.quote_created_monotonic_ns
        and (
            quote_version.quote_expires_monotonic_ns is None
            or trade.received_monotonic_ns < quote_version.quote_expires_monotonic_ns
        )
        and trade.aggressor_side is quote_version.direction.hedge_side
    )


def _optimistic_trade_qualifies(
    quote_version: QuoteVersion, trade: TradeEvidence
) -> bool:
    """Apply the at-or-through optimistic upper-bound price rule."""

    if not is_eligible_trade(quote_version, trade):
        return False
    quote = quote_version.quote
    if quote.maker_side is Side.BUY:
        return trade.canonical_price <= quote.maker_price  # type: ignore[operator]
    return trade.canonical_price >= quote.maker_price  # type: ignore[operator]


def detect_optimistic_would_fill(
    quote_version: QuoteVersion,
    trades: Iterable[TradeEvidence],
    *,
    data_gaps: Iterable[DataGapEvidence] = (),
    would_fill_detected_monotonic_ns: int | None = None,
    detected_utc=None,
    hedge_stream_session_id: str | int | None = None,
    hedge_recovery_generation: int | None = None,
) -> WouldFillEvidence | None:
    """Detect the explicitly labelled at-or-through public upper bound.

    This model assumes that every eligible public quantity is allocated to the
    quote (zero queue ahead and no hidden liquidity).  It is intentionally
    separate from :func:`detect_strict_would_fill`: equality and sub-tick
    prices qualify here, while the strict lower-bound detector remains a
    one-full-tick-through, tick-aligned contract.
    """

    quote = quote_version.quote
    if not quote_version.is_active or not validate_quote_economics(quote):
        return None
    if quote.maker_price is None or quote.canonical_quantity is None:
        return None
    grouped: dict[str, list[TradeEvidence]] = {}
    for trade in tuple(trades):
        grouped.setdefault(trade.trade_event_key, []).append(trade)
    resolved_list: list[TradeEvidence] = []
    for _key, group in sorted(grouped.items()):
        if any(trade != group[0] for trade in group[1:]):
            return None
        resolved_list.append(group[0])
    ordered = sorted(
        resolved_list,
        key=lambda trade: (trade.received_monotonic_ns, trade.trade_event_key),
    )
    qualifying: list[TradeEvidence] = []
    cumulative = Decimal("0")
    threshold_received: int | None = None
    for trade in ordered:
        if _optimistic_trade_qualifies(quote_version, trade):
            qualifying.append(trade)
            cumulative += trade.canonical_quantity
            if cumulative >= quote.canonical_quantity:
                threshold_received = trade.received_monotonic_ns
                break
    if not qualifying or threshold_received is None:
        return None
    detection = (
        threshold_received
        if would_fill_detected_monotonic_ns is None
        else would_fill_detected_monotonic_ns
    )
    _int(detection, "would_fill_detected_monotonic_ns")
    if detection < threshold_received or detection <= quote_version.quote_created_monotonic_ns:
        return None
    if quote_version.quote_expires_monotonic_ns is not None and detection >= quote_version.quote_expires_monotonic_ns:
        return None
    if _matching_risex_gap(data_gaps, quote_version, detection) is not None:
        return None
    if hedge_stream_session_id is None:
        hedge_stream_session_id = quote_version.hedge_stream_session_id
    if hedge_recovery_generation is None:
        hedge_recovery_generation = quote_version.hedge_recovery_generation
    return WouldFillEvidence(
        quote_version_id=quote_version.version_id,
        venue=Venue.RISEX,
        canonical_market=quote_version.canonical_market,
        direction=quote_version.direction,
        canonical_quantity=quote.canonical_quantity,
        cumulative_eligible_quantity=cumulative,
        qualifying_trade_event_keys=tuple(trade.trade_event_key for trade in qualifying),
        would_fill_detected_monotonic_ns=detection,
        qualifying_trades=tuple(qualifying),
        detected_utc=detected_utc,
        hedge_stream_session_id=hedge_stream_session_id,
        hedge_recovery_generation=hedge_recovery_generation,
        fillability_model=FillabilityModel.OPTIMISTIC_UPPER_BOUND,
    )


def _matching_risex_gap(
    gaps: Iterable[DataGapEvidence],
    quote_version: QuoteVersion,
    end_monotonic_ns: int,
) -> DataGapEvidence | None:
    for gap in gaps:
        if gap.matches(
            Venue.RISEX,
            quote_version.canonical_market,
            quote_version.stream_session_id,
            quote_version.recovery_generation,
        ) and gap.overlaps(quote_version.quote_created_monotonic_ns, end_monotonic_ns):
            return gap
    return None


def detect_strict_would_fill(
    quote_version: QuoteVersion,
    trades: Iterable[TradeEvidence],
    *,
    data_gaps: Iterable[DataGapEvidence] = (),
    would_fill_detected_monotonic_ns: int | None = None,
    detected_utc=None,
    hedge_stream_session_id: str | int | None = None,
    hedge_recovery_generation: int | None = None,
) -> WouldFillEvidence | None:
    """Detect a conservative fill using only local receipt monotonic order.

    Exchange UTC is retained on each trade as optional provenance, but it is
    intentionally never used to order a quote against a trade.  The input is
    treated as an unordered immutable collection and replayed deterministically
    by receipt monotonic time and event key.
    """

    quote = quote_version.quote
    if not quote_version.is_active or not validate_quote_economics(quote):
        return None
    if quote.maker_price is None or quote.canonical_quantity is None:
        return None
    tick = quote.risex_tick_size or quote.policy.risex_tick_size
    if tick is None or tick <= 0:
        return None
    grouped: dict[str, list[TradeEvidence]] = {}
    for trade in tuple(trades):
        grouped.setdefault(trade.trade_event_key, []).append(trade)
    # Resolve duplicates by event key before ordering.  A conflicting key is
    # unusable rather than being selected by the iterable's arrival order.
    resolved_list: list[TradeEvidence] = []
    for _key, group in sorted(grouped.items()):
        if any(trade != group[0] for trade in group[1:]):
            return None
        resolved_list.append(group[0])
    resolved = tuple(resolved_list)
    ordered = sorted(
        resolved,
        key=lambda trade: (trade.received_monotonic_ns, trade.trade_event_key),
    )
    qualifying: list[TradeEvidence] = []
    cumulative = Decimal("0")
    threshold_received: int | None = None
    for trade in ordered:
        if _trade_qualifies(quote_version, trade, tick):
            qualifying.append(trade)
            cumulative += trade.canonical_quantity
            if cumulative >= quote.canonical_quantity:
                threshold_received = trade.received_monotonic_ns
                break
    if not qualifying or threshold_received is None:
        return None
    detection = (
        threshold_received
        if would_fill_detected_monotonic_ns is None
        else would_fill_detected_monotonic_ns
    )
    _int(detection, "would_fill_detected_monotonic_ns")
    if detection < threshold_received or detection <= quote_version.quote_created_monotonic_ns:
        return None
    if quote_version.quote_expires_monotonic_ns is not None and detection >= quote_version.quote_expires_monotonic_ns:
        return None
    if _matching_risex_gap(data_gaps, quote_version, detection) is not None:
        return None
    if hedge_stream_session_id is None:
        hedge_stream_session_id = quote_version.hedge_stream_session_id
    if hedge_recovery_generation is None:
        hedge_recovery_generation = quote_version.hedge_recovery_generation
    return WouldFillEvidence(
        quote_version_id=quote_version.version_id,
        venue=Venue.RISEX,
        canonical_market=quote_version.canonical_market,
        direction=quote_version.direction,
        canonical_quantity=quote.canonical_quantity,
        cumulative_eligible_quantity=cumulative,
        qualifying_trade_event_keys=tuple(trade.trade_event_key for trade in qualifying),
        would_fill_detected_monotonic_ns=detection,
        qualifying_trades=tuple(qualifying),
        detected_utc=detected_utc,
        hedge_stream_session_id=hedge_stream_session_id,
        hedge_recovery_generation=hedge_recovery_generation,
    )


def horizon_deadline_monotonic_ns(
    would_fill_detected_monotonic_ns: int,
    horizon_ms: int,
) -> int:
    """Construct an absolute local deadline without using an exchange clock."""

    _int(would_fill_detected_monotonic_ns, "would_fill_detected_monotonic_ns")
    _int(horizon_ms, "horizon_ms")
    if horizon_ms not in (0, 300, 500, 1000, 2000):
        raise ValueError("unsupported horizon; use 0/300/500/1000 ms")
    return would_fill_detected_monotonic_ns + horizon_ms * 1_000_000


def _book_identity_candidates(
    books: Sequence[BookEvidence],
    *,
    market: str,
    deadline: int,
) -> tuple[BookEvidence, ...]:
    return tuple(
        book
        for book in books
        if book.venue is Venue.LIGHTER
        and book.canonical_market == market
        and book.received_monotonic_ns <= deadline
    )


def _book_identity_rank(book: BookEvidence) -> tuple[int, int, int]:
    return (
        book.received_monotonic_ns,
        book.book_revision,
        -1 if book.sequence is None else book.sequence,
    )


def _book_rank(book: BookEvidence) -> tuple[int, int, int, str]:
    return (*_book_identity_rank(book), repr(book))


def _latest_book(books: Sequence[BookEvidence]) -> BookEvidence:
    if not books:
        raise ValueError("at least one book is required")
    return max(books, key=_book_rank)


def _ambiguous_latest_books(books: Sequence[BookEvidence]) -> tuple[BookEvidence, ...]:
    """Return a deterministic conflicting latest-identity group, if present."""

    if not books:
        return ()
    latest_identity = max(_book_identity_rank(book) for book in books)
    unique: list[BookEvidence] = []
    for book in books:
        if _book_identity_rank(book) == latest_identity and book not in unique:
            unique.append(book)
    if len(unique) > 1:
        return tuple(sorted(unique, key=repr))
    return ()


def _gap_for_horizon(
    gaps: Iterable[DataGapEvidence],
    *,
    market: str,
    session: str | int,
    recovery: int,
    start: int,
    deadline: int,
) -> DataGapEvidence | None:
    for gap in gaps:
        if gap.matches(Venue.LIGHTER, market, session, recovery) and gap.overlaps(start, deadline):
            return gap
    return None


def _capture(
    *,
    horizon_ms: int,
    detected: int,
    deadline: int,
    expected_session: str | int,
    expected_recovery: int,
    market: str,
    requested: Decimal,
    outcome: EntryViabilityOutcome,
    book: BookEvidence | None = None,
    filled: Decimal = Decimal("0"),
    notional: Decimal = Decimal("0"),
    vwap: Decimal | None = None,
    gap: DataGapEvidence | None = None,
    freshness_max_age_ns: int | None = None,
    ambiguous_books: tuple[BookEvidence, ...] = (),
    fillability_model: FillabilityModel = FillabilityModel.STRICT_LOWER_BOUND,
) -> HedgeHorizonCapture:
    return HedgeHorizonCapture(
        horizon_ms=horizon_ms,
        would_fill_detected_monotonic_ns=detected,
        horizon_deadline_monotonic_ns=deadline,
        expected_stream_session_id=expected_session,
        expected_recovery_generation=expected_recovery,
        canonical_market=market,
        requested_quantity=requested,
        outcome=outcome,
        book=book,
        book_received_monotonic_ns=None if book is None else book.received_monotonic_ns,
        book_stream_session_id=None if book is None else book.stream_session_id,
        book_recovery_generation=None if book is None else book.recovery_generation,
        book_revision=None if book is None else book.book_revision,
        sequence=None if book is None else book.sequence,
        checksum=None if book is None else book.checksum,
        filled_quantity=filled,
        notional_usd=notional,
        vwap_price=vwap,
        gap_evidence=gap,
        freshness_max_age_ns=freshness_max_age_ns,
        ambiguous_books=ambiguous_books,
        fillability_model=fillability_model,
    )


def capture_horizon(
    would_fill: WouldFillEvidence,
    books: Iterable[BookEvidence],
    *,
    horizon_ms: int,
    expected_stream_session_id: str | int | None = None,
    expected_recovery_generation: int | None = None,
    data_gaps: Iterable[DataGapEvidence] = (),
    freshness_max_age_ns: int | None = None,
    fillability_model: FillabilityModel | str | None = None,
) -> HedgeHorizonCapture:
    """Select the latest eligible Lighter book without look-ahead."""

    if not isinstance(would_fill, WouldFillEvidence):
        raise TypeError("horizon capture requires WouldFillEvidence, not a non-fill episode")
    _int(horizon_ms, "horizon_ms")
    if horizon_ms not in (0, 300, 500, 1000, 2000):
        raise ValueError("unsupported horizon; use 0/300/500/1000 ms")
    detected = _int(
        would_fill.would_fill_detected_monotonic_ns,
        "would_fill_detected_monotonic_ns",
    )
    deadline = horizon_deadline_monotonic_ns(detected, horizon_ms)
    expected_session = expected_stream_session_id
    if expected_session is None:
        expected_session = getattr(would_fill, "hedge_stream_session_id", None)
    expected_recovery = expected_recovery_generation
    if expected_recovery is None:
        expected_recovery = getattr(would_fill, "hedge_recovery_generation", None)
    if expected_session is None or expected_recovery is None:
        raise ValueError("expected Lighter session and recovery generation are required")
    if freshness_max_age_ns is not None:
        _int(freshness_max_age_ns, "freshness_max_age_ns")
    model = (
        would_fill.fillability_model
        if fillability_model is None
        else fillability_model
    )
    if not isinstance(model, FillabilityModel):
        model = FillabilityModel(model)
    books_tuple = tuple(books)
    gaps_tuple = tuple(data_gaps)

    def finish(outcome: EntryViabilityOutcome, **kwargs) -> HedgeHorizonCapture:
        return _capture(
            horizon_ms=horizon_ms,
            detected=detected,
            deadline=deadline,
            expected_session=expected_session,
            expected_recovery=expected_recovery,
            market=would_fill.canonical_market,
            requested=would_fill.canonical_quantity,
            outcome=outcome,
            freshness_max_age_ns=freshness_max_age_ns,
            fillability_model=model,
            **kwargs,
        )

    candidates = _book_identity_candidates(
        books_tuple,
        market=would_fill.canonical_market,
        deadline=deadline,
    )
    if not candidates:
        gap = _gap_for_horizon(
            gaps_tuple,
            market=would_fill.canonical_market,
            session=expected_session,
            recovery=expected_recovery,
            start=detected,
            deadline=deadline,
        )
        if gap is not None:
            return finish(EntryViabilityOutcome.HEDGE_DATA_GAP, gap=gap)
        return finish(EntryViabilityOutcome.HEDGE_DATA_MISSING)
    identity = tuple(
        book
        for book in candidates
        if book.stream_session_id == expected_session
        and book.recovery_generation == expected_recovery
    )
    if not identity:
        displaced = _latest_book(candidates)
        return finish(EntryViabilityOutcome.HEDGE_SESSION_DISPLACED, book=displaced)
    ambiguous = _ambiguous_latest_books(identity)
    if ambiguous:
        return finish(EntryViabilityOutcome.HEDGE_OUTCOME_UNKNOWN, ambiguous_books=ambiguous)
    latest_identity = _latest_book(identity)
    gap = _gap_for_horizon(
        gaps_tuple,
        market=would_fill.canonical_market,
        session=expected_session,
        recovery=expected_recovery,
        start=latest_identity.received_monotonic_ns,
        deadline=deadline,
    )
    if gap is not None:
        return finish(EntryViabilityOutcome.HEDGE_DATA_GAP, book=latest_identity, gap=gap)
    healthy = tuple(book for book in identity if book.is_sequence_healthy)
    if not healthy:
        return finish(EntryViabilityOutcome.HEDGE_OUTCOME_UNKNOWN, book=latest_identity)
    fresh = tuple(book for book in healthy if book.fresh)
    if freshness_max_age_ns is not None:
        _int(freshness_max_age_ns, "freshness_max_age_ns")
        fresh = tuple(
            book
            for book in fresh
            if deadline - book.received_monotonic_ns <= freshness_max_age_ns
        )
    if not fresh:
        stale_book = _latest_book(healthy)
        return finish(EntryViabilityOutcome.HEDGE_DATA_STALE, book=stale_book)
    ambiguous = _ambiguous_latest_books(fresh)
    if ambiguous:
        return finish(EntryViabilityOutcome.HEDGE_OUTCOME_UNKNOWN, ambiguous_books=ambiguous)
    selected = _latest_book(fresh)
    gap = _gap_for_horizon(
        gaps_tuple,
        market=would_fill.canonical_market,
        session=expected_session,
        recovery=expected_recovery,
        start=selected.received_monotonic_ns,
        deadline=deadline,
    )
    if gap is not None:
        return finish(EntryViabilityOutcome.HEDGE_DATA_GAP, book=selected, gap=gap)
    try:
        vwap = exact_quantity_vwap(
            would_fill.direction.hedge_side,
            would_fill.canonical_quantity,
            selected.bids,
            selected.asks,
        )
    except (TypeError, ValueError, ArithmeticError):
        return finish(EntryViabilityOutcome.HEDGE_OUTCOME_UNKNOWN, book=selected)
    if vwap.filled_quantity == would_fill.canonical_quantity:
        outcome = EntryViabilityOutcome.HEDGE_FULL
    elif vwap.filled_quantity > 0:
        outcome = EntryViabilityOutcome.HEDGE_PARTIAL
    else:
        outcome = EntryViabilityOutcome.HEDGE_DEPTH_UNAVAILABLE
    return finish(
        outcome,
        book=selected,
        filled=vwap.filled_quantity,
        notional=vwap.notional_usd,
        vwap=vwap.price if vwap.filled_quantity > 0 else None,
    )


def build_entry_viability_episode(
    quote_version: QuoteVersion,
    trades: Iterable[TradeEvidence],
    *,
    books_by_horizon: Mapping[int, Iterable[BookEvidence]] | None = None,
    expected_stream_session_id: str | int | None = None,
    expected_recovery_generation: int | None = None,
    data_gaps: Iterable[DataGapEvidence] = (),
    would_fill_detected_monotonic_ns: int | None = None,
    detected_utc=None,
    horizons: Sequence[int] = (0, 300, 500, 1000),
    freshness_max_age_ns: int | None = None,
    fillability_model: FillabilityModel | str = FillabilityModel.STRICT_LOWER_BOUND,
) -> EntryViabilityEpisode:
    """Replay one immutable quote/evidence set into an episode."""

    quote = quote_version.quote
    model = (
        fillability_model
        if isinstance(fillability_model, FillabilityModel)
        else FillabilityModel(fillability_model)
    )
    if quote.outcome is EntryViabilityOutcome.QUOTE_NOT_POST_ONLY:
        return EntryViabilityEpisode(
            quote_version,
            EntryViabilityOutcome.QUOTE_NOT_POST_ONLY,
            fillability_model=model,
        )
    if not quote_version.is_active or not validate_quote_economics(quote):
        return EntryViabilityEpisode(
            quote_version,
            EntryViabilityOutcome.QUOTE_NOT_ECONOMIC,
            fillability_model=model,
        )
    data_gaps = tuple(data_gaps)
    detector = (
        detect_strict_would_fill
        if model is FillabilityModel.STRICT_LOWER_BOUND
        else detect_optimistic_would_fill
    )
    would_fill = detector(
        quote_version,
        trades,
        data_gaps=data_gaps,
        would_fill_detected_monotonic_ns=would_fill_detected_monotonic_ns,
        detected_utc=detected_utc,
        hedge_stream_session_id=expected_stream_session_id
        if expected_stream_session_id is not None
        else quote_version.hedge_stream_session_id,
        hedge_recovery_generation=expected_recovery_generation
        if expected_recovery_generation is not None
        else quote_version.hedge_recovery_generation,
    )
    if would_fill is None:
        return EntryViabilityEpisode(
            quote_version,
            EntryViabilityOutcome.NO_WOULD_FILL,
            fillability_model=model,
        )
    if books_by_horizon is None:
        books_by_horizon = {}
    captures = tuple(
        capture_horizon(
            would_fill,
            books_by_horizon.get(horizon, ()),
            horizon_ms=horizon,
            expected_stream_session_id=expected_stream_session_id
            if expected_stream_session_id is not None
            else quote_version.hedge_stream_session_id,
            expected_recovery_generation=expected_recovery_generation
            if expected_recovery_generation is not None
            else quote_version.hedge_recovery_generation,
            data_gaps=data_gaps,
            freshness_max_age_ns=freshness_max_age_ns,
            fillability_model=model,
        )
        for horizon in horizons
    )
    return EntryViabilityEpisode(
        quote_version,
        EntryViabilityOutcome.WOULD_FILL,
        would_fill,
        captures,
        model,
    )


# Direct names make the two gates discoverable without introducing a second
# implementation or any runtime/event-bus surface.
would_fill_evidence = detect_strict_would_fill
select_horizon_capture = capture_horizon


__all__ = [
    "build_entry_viability_episode",
    "capture_horizon",
    "detect_optimistic_would_fill",
    "detect_strict_would_fill",
    "horizon_deadline_monotonic_ns",
    "is_eligible_trade",
    "select_horizon_capture",
    "would_fill_evidence",
]
