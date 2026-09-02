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
    ordered = sorted(
        tuple(trades),
        key=lambda trade: (trade.received_monotonic_ns, trade.trade_event_key),
    )
    seen: set[str] = set()
    qualifying: list[TradeEvidence] = []
    cumulative = Decimal("0")
    for trade in ordered:
        if trade.trade_event_key in seen:
            continue
        seen.add(trade.trade_event_key)
        if _trade_qualifies(quote_version, trade, tick):
            qualifying.append(trade)
            cumulative += trade.canonical_quantity
    if not qualifying or cumulative < quote.canonical_quantity:
        return None
    last_received = max(trade.received_monotonic_ns for trade in qualifying)
    detection = last_received if would_fill_detected_monotonic_ns is None else would_fill_detected_monotonic_ns
    _int(detection, "would_fill_detected_monotonic_ns")
    if detection < last_received or detection <= quote_version.quote_created_monotonic_ns:
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
) -> HedgeHorizonCapture:
    """Select the latest eligible Lighter book without look-ahead."""

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
    books_tuple = tuple(books)
    gaps_tuple = tuple(data_gaps)
    candidates = _book_identity_candidates(
        books_tuple,
        market=would_fill.canonical_market,
        deadline=deadline,
    )
    gap = _gap_for_horizon(
        gaps_tuple,
        market=would_fill.canonical_market,
        session=expected_session,
        recovery=expected_recovery,
        start=detected,
        deadline=deadline,
    )
    if gap is not None:
        return _capture(
            horizon_ms=horizon_ms,
            detected=detected,
            deadline=deadline,
            expected_session=expected_session,
            expected_recovery=expected_recovery,
            market=would_fill.canonical_market,
            requested=would_fill.canonical_quantity,
            outcome=EntryViabilityOutcome.HEDGE_DATA_GAP,
            gap=gap,
            freshness_max_age_ns=freshness_max_age_ns,
        )
    if not candidates:
        return _capture(
            horizon_ms=horizon_ms,
            detected=detected,
            deadline=deadline,
            expected_session=expected_session,
            expected_recovery=expected_recovery,
            market=would_fill.canonical_market,
            requested=would_fill.canonical_quantity,
            outcome=EntryViabilityOutcome.HEDGE_DATA_MISSING,
            freshness_max_age_ns=freshness_max_age_ns,
        )
    identity = tuple(
        book
        for book in candidates
        if book.stream_session_id == expected_session
        and book.recovery_generation == expected_recovery
    )
    if not identity:
        return _capture(
            horizon_ms=horizon_ms,
            detected=detected,
            deadline=deadline,
            expected_session=expected_session,
            expected_recovery=expected_recovery,
            market=would_fill.canonical_market,
            requested=would_fill.canonical_quantity,
            outcome=EntryViabilityOutcome.HEDGE_SESSION_DISPLACED,
            freshness_max_age_ns=freshness_max_age_ns,
        )
    healthy = tuple(book for book in identity if book.is_sequence_healthy)
    if not healthy:
        return _capture(
            horizon_ms=horizon_ms,
            detected=detected,
            deadline=deadline,
            expected_session=expected_session,
            expected_recovery=expected_recovery,
            market=would_fill.canonical_market,
            requested=would_fill.canonical_quantity,
            outcome=EntryViabilityOutcome.HEDGE_OUTCOME_UNKNOWN,
            freshness_max_age_ns=freshness_max_age_ns,
        )
    fresh = tuple(book for book in healthy if book.fresh)
    if freshness_max_age_ns is not None:
        _int(freshness_max_age_ns, "freshness_max_age_ns")
        fresh = tuple(
            book
            for book in fresh
            if deadline - book.received_monotonic_ns <= freshness_max_age_ns
        )
    if not fresh:
        return _capture(
            horizon_ms=horizon_ms,
            detected=detected,
            deadline=deadline,
            expected_session=expected_session,
            expected_recovery=expected_recovery,
            market=would_fill.canonical_market,
            requested=would_fill.canonical_quantity,
            outcome=EntryViabilityOutcome.HEDGE_DATA_STALE,
            freshness_max_age_ns=freshness_max_age_ns,
        )
    selected = max(
        fresh,
        key=lambda book: (
            book.received_monotonic_ns,
            book.book_revision,
            -1 if book.sequence is None else book.sequence,
        ),
    )
    try:
        vwap = exact_quantity_vwap(
            would_fill.direction.hedge_side,
            would_fill.canonical_quantity,
            selected.bids,
            selected.asks,
        )
    except (TypeError, ValueError, ArithmeticError):
        return _capture(
            horizon_ms=horizon_ms,
            detected=detected,
            deadline=deadline,
            expected_session=expected_session,
            expected_recovery=expected_recovery,
            market=would_fill.canonical_market,
            requested=would_fill.canonical_quantity,
            outcome=EntryViabilityOutcome.HEDGE_OUTCOME_UNKNOWN,
            book=selected,
            freshness_max_age_ns=freshness_max_age_ns,
        )
    if vwap.filled_quantity == would_fill.canonical_quantity:
        outcome = EntryViabilityOutcome.HEDGE_FULL
    elif vwap.filled_quantity > 0:
        outcome = EntryViabilityOutcome.HEDGE_PARTIAL
    else:
        outcome = EntryViabilityOutcome.HEDGE_DEPTH_UNAVAILABLE
    return _capture(
        horizon_ms=horizon_ms,
        detected=detected,
        deadline=deadline,
        expected_session=expected_session,
        expected_recovery=expected_recovery,
        market=would_fill.canonical_market,
        requested=would_fill.canonical_quantity,
        outcome=outcome,
        book=selected,
        filled=vwap.filled_quantity,
        notional=vwap.notional_usd,
        vwap=vwap.price if vwap.filled_quantity > 0 else None,
        freshness_max_age_ns=freshness_max_age_ns,
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
) -> EntryViabilityEpisode:
    """Replay one immutable quote/evidence set into an episode."""

    quote = quote_version.quote
    if quote.outcome is EntryViabilityOutcome.QUOTE_NOT_POST_ONLY:
        return EntryViabilityEpisode(quote_version, EntryViabilityOutcome.QUOTE_NOT_POST_ONLY)
    if not quote_version.is_active or not validate_quote_economics(quote):
        return EntryViabilityEpisode(quote_version, EntryViabilityOutcome.QUOTE_NOT_ECONOMIC)
    data_gaps = tuple(data_gaps)
    would_fill = detect_strict_would_fill(
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
        return EntryViabilityEpisode(quote_version, EntryViabilityOutcome.NO_WOULD_FILL)
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
        )
        for horizon in horizons
    )
    return EntryViabilityEpisode(
        quote_version,
        EntryViabilityOutcome.WOULD_FILL,
        would_fill,
        captures,
    )


# Direct names make the two gates discoverable without introducing a second
# implementation or any runtime/event-bus surface.
would_fill_evidence = detect_strict_would_fill
select_horizon_capture = capture_horizon


__all__ = [
    "build_entry_viability_episode",
    "capture_horizon",
    "detect_strict_would_fill",
    "horizon_deadline_monotonic_ns",
    "select_horizon_capture",
    "would_fill_evidence",
]
