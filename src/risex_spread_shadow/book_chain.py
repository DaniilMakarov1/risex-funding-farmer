"""Lossless normalized public-book revision chains for SS-001G.

The public feed still exposes complete immutable :class:`BookEvidence`
objects to the economics and fillability code.  This module is only the
durable evidence representation and its bounded reconstruction/audit path:
one full snapshot anchors an identity chain and later records contain the
exact level changes from their explicit predecessor.

No venue protocol is interpreted here.  Sequence/checksum validity remains a
feed concern; this layer binds the already accepted normalized book state to
its local evidence revision identity.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
import hashlib
import json
from typing import Any, Iterator

from risex_farmer.models import BookLevel, Venue

from .models import BookEvidence, make_book_revision_id


FULL_BOOK_ENCODING = "FULL"
DELTA_BOOK_ENCODING = "DELTA"
_BOOK_ENCODINGS = frozenset({FULL_BOOK_ENCODING, DELTA_BOOK_ENCODING})


class BookRevisionChainError(ValueError):
    """Raised when a normalized book chain cannot be reconstructed safely."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"book revision chain failure: {reason}")


def book_chain_id(book: BookEvidence) -> str:
    """Return the identity shared by every revision in one stream chain."""

    return (
        f"{book.venue.value}|{book.canonical_market}|{book.stream_session_id}|"
        f"{book.recovery_generation}"
    )


def _chain_key(
    venue: Venue,
    market: str,
    session: str | int,
    recovery: int,
) -> tuple[Venue, str, str | int, int]:
    return venue, market, session, recovery


def _level_value(value: Any, name: str) -> Decimal:
    if isinstance(value, Decimal):
        result = value
    else:
        try:
            result = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise BookRevisionChainError(f"{name} is not a finite decimal") from exc
    if not result.is_finite():
        raise BookRevisionChainError(f"{name} is not a finite decimal")
    return result


def _book_level_record(level: BookLevel) -> dict[str, str]:
    return {
        "price": _canonical_decimal(level.canonical_price),
        "quantity": _canonical_decimal(level.canonical_quantity),
    }


def _canonical_decimal(value: Decimal) -> str:
    """Render equivalent Decimal values to one stable non-exponent form."""

    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in {"", "-0"} else text


def _levels_state_sha256(
    bids: Mapping[Decimal, Decimal], asks: Mapping[Decimal, Decimal]
) -> str:
    payload = {
        "bids": tuple(_book_level_record(level) for level in _sorted_levels(bids, side="bids")),
        "asks": tuple(_book_level_record(level) for level in _sorted_levels(asks, side="asks")),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def book_state_sha256(book: BookEvidence) -> str:
    """Hash the canonical normalized levels, excluding transport metadata."""

    bids = _normalized_level_map(book.bids, side="bids")
    asks = _normalized_level_map(book.asks, side="asks")
    return _levels_state_sha256(bids, asks)


def _normalized_level_map(
    levels: Iterable[BookLevel],
    *,
    side: str,
    allow_zero: bool = False,
) -> dict[Decimal, Decimal]:
    result: dict[Decimal, Decimal] = {}
    for level in levels:
        if not isinstance(level, BookLevel):
            raise BookRevisionChainError("BOOK_LEVEL_TYPE_INVALID")
        price = _level_value(level.canonical_price, f"{side}.price")
        quantity = _level_value(level.canonical_quantity, f"{side}.quantity")
        if price <= 0:
            raise BookRevisionChainError("BOOK_LEVEL_PRICE_INVALID")
        if quantity < 0 or (quantity == 0 and not allow_zero):
            raise BookRevisionChainError("BOOK_LEVEL_QUANTITY_INVALID")
        if price in result:
            raise BookRevisionChainError("DUPLICATE_BOOK_LEVEL")
        result[price] = quantity
    return result


def _sorted_levels(levels: Mapping[Decimal, Decimal], *, side: str) -> tuple[BookLevel, ...]:
    ordered = sorted(levels.items(), key=lambda item: item[0], reverse=side == "bids")
    return tuple(BookLevel(price, quantity) for price, quantity in ordered if quantity > 0)


def _sorted_changes(
    levels: Mapping[Decimal, Decimal], *, side: str
) -> tuple[BookLevel, ...]:
    ordered = sorted(levels.items(), key=lambda item: item[0], reverse=side == "bids")
    return tuple(BookLevel(price, quantity) for price, quantity in ordered)


def _parse_record_level(
    value: Any,
    *,
    side: str,
    allow_zero: bool,
) -> tuple[Decimal, Decimal]:
    if not isinstance(value, Mapping):
        raise BookRevisionChainError("BOOK_LEVEL_RECORD_INVALID")
    if set(value) != {"price", "quantity"}:
        raise BookRevisionChainError("BOOK_LEVEL_RECORD_FIELDS_INVALID")
    price = _level_value(value.get("price"), f"{side}.price")
    quantity = _level_value(value.get("quantity"), f"{side}.quantity")
    if price <= 0:
        raise BookRevisionChainError("BOOK_LEVEL_PRICE_INVALID")
    if quantity < 0 or (quantity == 0 and not allow_zero):
        raise BookRevisionChainError("BOOK_LEVEL_QUANTITY_INVALID")
    return price, quantity


def _parse_levels(
    values: Any,
    *,
    side: str,
    allow_zero: bool,
) -> dict[Decimal, Decimal]:
    if not isinstance(values, (tuple, list)):
        raise BookRevisionChainError("BOOK_LEVELS_NOT_A_SEQUENCE")
    result: dict[Decimal, Decimal] = {}
    for value in values:
        price, quantity = _parse_record_level(
            value,
            side=side,
            allow_zero=allow_zero,
        )
        if price in result:
            raise BookRevisionChainError("DUPLICATE_BOOK_LEVEL")
        result[price] = quantity
    return result


def _int_field(record: Mapping[str, Any], name: str, *, required: bool = True) -> int | None:
    value = record.get(name)
    if value is None and not required:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise BookRevisionChainError(f"{name.upper()}_INVALID")
    return value


def _str_field(record: Mapping[str, Any], name: str, *, required: bool = True) -> str | None:
    value = record.get(name)
    if value is None and not required:
        return None
    if not isinstance(value, str) or not value:
        raise BookRevisionChainError(f"{name.upper()}_INVALID")
    return value


def _validate_sha256(value: Any, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise BookRevisionChainError(f"{name.upper()}_INVALID")
    return value


def _validate_record_state_digest(
    record: Mapping[str, Any],
    state_sha256: str,
    *,
    required: bool,
) -> None:
    supplied = record.get("book_state_sha256")
    if supplied is None:
        if required:
            raise BookRevisionChainError("BOOK_STATE_DIGEST_REQUIRED")
        return
    if _validate_sha256(supplied, "book_state_digest") != state_sha256:
        raise BookRevisionChainError("BOOK_STATE_DIGEST_MISMATCH")


def _datetime_field(record: Mapping[str, Any]) -> datetime | None:
    value = record.get("received_utc")
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
            raise BookRevisionChainError("RECEIVED_UTC_INVALID")
        return value.astimezone(UTC)
    if not isinstance(value, str):
        raise BookRevisionChainError("RECEIVED_UTC_INVALID")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise BookRevisionChainError("RECEIVED_UTC_INVALID") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise BookRevisionChainError("RECEIVED_UTC_INVALID")
    return parsed.astimezone(UTC)


def _record_identity(record: Mapping[str, Any]) -> tuple[Venue, str, str | int, int, int]:
    try:
        venue = Venue(record.get("venue"))
    except (TypeError, ValueError) as exc:
        raise BookRevisionChainError("BOOK_VENUE_INVALID") from exc
    if venue not in (Venue.RISEX, Venue.LIGHTER):
        raise BookRevisionChainError("BOOK_VENUE_INVALID")
    market = _str_field(record, "canonical_market")
    session = record.get("stream_session_id")
    if isinstance(session, bool) or not isinstance(session, (str, int)) or session == "":
        raise BookRevisionChainError("STREAM_SESSION_ID_INVALID")
    recovery = _int_field(record, "recovery_generation")
    revision = _int_field(record, "book_revision")
    assert market is not None and recovery is not None and revision is not None
    return venue, market, session, recovery, revision


def _metadata_book(
    record: Mapping[str, Any],
    *,
    venue: Venue,
    market: str,
    session: str | int,
    recovery: int,
    revision: int,
    bids: Mapping[Decimal, Decimal],
    asks: Mapping[Decimal, Decimal],
) -> BookEvidence:
    sequence = record.get("sequence")
    if sequence is not None and (
        isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0
    ):
        raise BookRevisionChainError("SEQUENCE_INVALID")
    checksum = record.get("checksum")
    if checksum is not None and not isinstance(checksum, (int, str)):
        raise BookRevisionChainError("CHECKSUM_INVALID")
    sequence_valid = record.get("sequence_valid", True)
    checksum_valid = record.get("checksum_valid", True)
    fresh = record.get("fresh", True)
    if not isinstance(sequence_valid, bool) or not isinstance(checksum_valid, bool) or not isinstance(fresh, bool):
        raise BookRevisionChainError("BOOK_HEALTH_FLAGS_INVALID")
    received_ns = _int_field(record, "received_monotonic_ns")
    assert received_ns is not None
    return BookEvidence(
        venue=venue,
        canonical_market=market,
        bids=_sorted_levels(bids, side="bids"),
        asks=_sorted_levels(asks, side="asks"),
        received_monotonic_ns=received_ns,
        stream_session_id=session,
        recovery_generation=recovery,
        book_revision=revision,
        sequence=sequence,
        checksum=checksum,
        sequence_valid=sequence_valid,
        checksum_valid=checksum_valid,
        received_utc=_datetime_field(record),
        fresh=fresh,
    )


@dataclass(slots=True)
class _EncoderState:
    venue: Venue
    market: str
    session: str | int
    recovery: int
    revision: int
    revision_id: str
    bids: dict[Decimal, Decimal]
    asks: dict[Decimal, Decimal]
    state_sha256: str


def _changed_levels(
    previous: Mapping[Decimal, Decimal],
    current: Mapping[Decimal, Decimal],
    *,
    side: str,
) -> tuple[BookLevel, ...]:
    changed: dict[Decimal, Decimal] = {}
    for price in previous.keys() | current.keys():
        old = previous.get(price)
        new = current.get(price)
        if old != new:
            changed[price] = Decimal("0") if new is None else new
    return _sorted_changes(changed, side=side)


class BookRevisionEncoder:
    """Encode complete books into one full anchor plus exact level deltas."""

    def __init__(self) -> None:
        self._states: dict[tuple[Venue, str, str | int, int], _EncoderState] = {}
        self._active_identities: dict[tuple[Venue, str], tuple[Venue, str, str | int, int]] = {}

    @property
    def chain_count(self) -> int:
        return len(self._states)

    @property
    def current_level_count(self) -> int:
        return sum(len(state.bids) + len(state.asks) for state in self._states.values())

    def reset(self) -> None:
        self._states.clear()
        self._active_identities.clear()

    def state_sha256_for(self, book: BookEvidence) -> str:
        """Return the cached digest for a just-encoded book when available."""

        identity = _chain_key(
            book.venue,
            book.canonical_market,
            book.stream_session_id,
            book.recovery_generation,
        )
        state = self._states.get(identity)
        if state is not None and state.revision_id == book.book_revision_id:
            return state.state_sha256
        return book_state_sha256(book)

    def encode(self, book: BookEvidence, *, source_kind: str) -> dict[str, Any]:
        if not isinstance(book, BookEvidence):
            raise TypeError("book revision encoding requires BookEvidence")
        if book.venue not in (Venue.RISEX, Venue.LIGHTER):
            raise BookRevisionChainError("BOOK_VENUE_INVALID")
        if source_kind not in {"SNAPSHOT", "DELTA"}:
            raise BookRevisionChainError("BOOK_SOURCE_KIND_INVALID")
        bids = _normalized_level_map(book.bids, side="bids")
        asks = _normalized_level_map(book.asks, side="asks")
        identity = _chain_key(
            book.venue,
            book.canonical_market,
            book.stream_session_id,
            book.recovery_generation,
        )
        revision_id = book.book_revision_id
        previous = self._states.get(identity)
        if previous is None:
            if source_kind != "SNAPSHOT":
                raise BookRevisionChainError("MISSING_PREDECESSOR_SNAPSHOT")
            encoding = FULL_BOOK_ENCODING
            predecessor_id = None
            predecessor_revision = None
            output_bids = _sorted_levels(bids, side="bids")
            output_asks = _sorted_levels(asks, side="asks")
        else:
            if book.book_revision <= previous.revision:
                raise BookRevisionChainError("DUPLICATE_OR_OUT_OF_ORDER_REVISION")
            if source_kind == "SNAPSHOT":
                encoding = FULL_BOOK_ENCODING
                predecessor_id = None
                predecessor_revision = None
                output_bids = _sorted_levels(bids, side="bids")
                output_asks = _sorted_levels(asks, side="asks")
            else:
                if book.book_revision != previous.revision + 1:
                    raise BookRevisionChainError("PREDECESSOR_REVISION_MISMATCH")
                encoding = DELTA_BOOK_ENCODING
                predecessor_id = previous.revision_id
                predecessor_revision = previous.revision
                output_bids = _changed_levels(previous.bids, bids, side="bids")
                output_asks = _changed_levels(previous.asks, asks, side="asks")
        market_identity = (book.venue, book.canonical_market)
        previous_identity = self._active_identities.get(market_identity)
        if previous_identity is not None and previous_identity != identity:
            # A single live public stream cannot legitimately return to an old
            # session/recovery identity.  Discarding its compact state keeps
            # reconnect churn bounded; the observer's admissibility gate
            # rejects any late event from that displaced identity.
            self._states.pop(previous_identity, None)
        self._active_identities[market_identity] = identity
        self._states[identity] = _EncoderState(
            venue=book.venue,
            market=book.canonical_market,
            session=book.stream_session_id,
            recovery=book.recovery_generation,
            revision=book.book_revision,
            revision_id=revision_id,
            bids=dict(bids),
            asks=dict(asks),
            state_sha256=_levels_state_sha256(bids, asks),
        )
        return {
            "kind": "BOOK",
            "canonical_market": book.canonical_market,
            "venue": book.venue.value,
            "source_kind": source_kind,
            "book_encoding": encoding,
            "book_chain_id": book_chain_id(book),
            "book_revision_id": revision_id,
            "book_state_sha256": self._states[identity].state_sha256,
            "predecessor_book_revision_id": predecessor_id,
            "predecessor_book_revision": predecessor_revision,
            "checksum_validation": None,
            "received_utc": book.received_utc,
            "received_monotonic_ns": book.received_monotonic_ns,
            "stream_session_id": book.stream_session_id,
            "recovery_generation": book.recovery_generation,
            "book_revision": book.book_revision,
            "sequence": book.sequence,
            "checksum": book.checksum,
            "sequence_valid": book.sequence_valid,
            "checksum_valid": book.checksum_valid,
            "fresh": book.fresh,
            "bids": tuple(_book_level_record(level) for level in output_bids),
            "asks": tuple(_book_level_record(level) for level in output_asks),
            "observed_monotonic_ns": book.received_monotonic_ns,
        }


# Descriptive aliases keep the public surface discoverable without creating
# another representation or a second chain implementation.
BookChainEncoder = BookRevisionEncoder
LosslessBookEncoder = BookRevisionEncoder


@dataclass(frozen=True, slots=True)
class BookAuditResult:
    """Bounded summary from a deterministic physical BOOK replay."""

    book_count: int
    full_snapshot_count: int
    delta_count: int
    chain_count: int
    maximum_level_count: int
    current_level_count: int


class BookRevisionReconstructor:
    """Reconstruct one BOOK stream at a time using bounded current state."""

    def __init__(self) -> None:
        self._states: dict[
            tuple[Venue, str, str | int, int], _EncoderState
        ] = {}
        self.book_count = 0
        self.full_snapshot_count = 0
        self.delta_count = 0
        self.maximum_level_count = 0
        self._broken_chains: set[tuple[Venue, str, str | int, int]] = set()

    @property
    def chain_count(self) -> int:
        return len(self._states)

    @property
    def current_level_count(self) -> int:
        return sum(len(state.bids) + len(state.asks) for state in self._states.values())

    def _decode(self, record: Mapping[str, Any]) -> tuple[BookEvidence, str]:
        venue, market, session, recovery, revision = _record_identity(record)
        identity = _chain_key(venue, market, session, recovery)
        revision_id = make_book_revision_id(
            venue, market, session, recovery, revision
        )
        supplied_revision_id = record.get("book_revision_id")
        if supplied_revision_id is not None and supplied_revision_id != revision_id:
            raise BookRevisionChainError("REVISION_ID_MISMATCH")
        encoding_value = record.get("book_encoding")
        legacy_full = encoding_value is None
        # Pre-SS-001G BOOK rows were always complete state.  Their source_kind
        # may say DELTA, but absent the explicit encoding they are legacy full
        # records and remain readable by design.
        encoding = FULL_BOOK_ENCODING if legacy_full else encoding_value
        if not isinstance(encoding, str):
            raise BookRevisionChainError("BOOK_ENCODING_INVALID")
        if encoding not in _BOOK_ENCODINGS:
            raise BookRevisionChainError("BOOK_ENCODING_INVALID")
        source_kind = record.get("source_kind")
        if source_kind is not None:
            if not isinstance(source_kind, str) or source_kind not in {"SNAPSHOT", "DELTA"}:
                raise BookRevisionChainError("BOOK_SOURCE_KIND_INVALID")
            if not legacy_full and (
                (encoding == FULL_BOOK_ENCODING and source_kind != "SNAPSHOT")
                or (encoding == DELTA_BOOK_ENCODING and source_kind != "DELTA")
            ):
                raise BookRevisionChainError("BOOK_ENCODING_SOURCE_MISMATCH")
        supplied_chain_id = record.get("book_chain_id")
        expected_chain_id = f"{venue.value}|{market}|{session}|{recovery}"
        if supplied_chain_id is None:
            if not legacy_full:
                raise BookRevisionChainError("BOOK_CHAIN_ID_REQUIRED")
        elif supplied_chain_id != expected_chain_id:
            raise BookRevisionChainError("BOOK_CHAIN_ID_MISMATCH")
        if supplied_revision_id is None and not legacy_full:
            raise BookRevisionChainError("REVISION_ID_REQUIRED")
        supplied_state_digest = record.get("book_state_sha256")
        if supplied_state_digest is None and not legacy_full:
            raise BookRevisionChainError("BOOK_STATE_DIGEST_REQUIRED")
        previous = self._states.get(identity)
        if encoding == FULL_BOOK_ENCODING:
            if previous is not None and revision <= previous.revision:
                raise BookRevisionChainError("DUPLICATE_OR_OUT_OF_ORDER_REVISION")
            if record.get("predecessor_book_revision_id") is not None or record.get(
                "predecessor_book_revision"
            ) is not None:
                raise BookRevisionChainError("FULL_BOOK_HAS_PREDECESSOR")
            bids = _parse_levels(record.get("bids"), side="bids", allow_zero=False)
            asks = _parse_levels(record.get("asks"), side="asks", allow_zero=False)
            book = _metadata_book(
                record,
                venue=venue,
                market=market,
                session=session,
                recovery=recovery,
                revision=revision,
                bids=bids,
                asks=asks,
            )
            state_digest = _levels_state_sha256(bids, asks)
            _validate_record_state_digest(
                record,
                state_digest,
                required=not legacy_full,
            )
            state = _EncoderState(
                venue=venue,
                market=market,
                session=session,
                recovery=recovery,
                revision=revision,
                revision_id=revision_id,
                bids=bids,
                asks=asks,
                state_sha256=state_digest,
            )
            self._states[identity] = state
            self._broken_chains.discard(identity)
            return book, encoding
        if previous is None or identity in self._broken_chains:
            raise BookRevisionChainError("MISSING_PREDECESSOR_CHAIN")
        predecessor_id = record.get("predecessor_book_revision_id")
        predecessor_revision = _int_field(
            record,
            "predecessor_book_revision",
            required=False,
        )
        if predecessor_id != previous.revision_id or predecessor_revision != previous.revision:
            raise BookRevisionChainError("PREDECESSOR_REFERENCE_MISMATCH")
        if revision != previous.revision + 1:
            raise BookRevisionChainError("DELTA_REVISION_NOT_CONTIGUOUS")
        bid_changes = _parse_levels(record.get("bids"), side="bids", allow_zero=True)
        ask_changes = _parse_levels(record.get("asks"), side="asks", allow_zero=True)
        bids = dict(previous.bids)
        asks = dict(previous.asks)
        for price, quantity in bid_changes.items():
            if quantity == 0:
                if price not in bids:
                    raise BookRevisionChainError("BOOK_DELETE_MISSING_LEVEL")
                bids.pop(price, None)
            else:
                if bids.get(price) == quantity:
                    raise BookRevisionChainError("BOOK_CHANGE_IS_NOOP")
                bids[price] = quantity
        for price, quantity in ask_changes.items():
            if quantity == 0:
                if price not in asks:
                    raise BookRevisionChainError("BOOK_DELETE_MISSING_LEVEL")
                asks.pop(price, None)
            else:
                if asks.get(price) == quantity:
                    raise BookRevisionChainError("BOOK_CHANGE_IS_NOOP")
                asks[price] = quantity
        book = _metadata_book(
            record,
            venue=venue,
            market=market,
            session=session,
            recovery=recovery,
            revision=revision,
            bids=bids,
            asks=asks,
        )
        state_digest = _levels_state_sha256(bids, asks)
        _validate_record_state_digest(
            record,
            state_digest,
            required=True,
        )
        self._states[identity] = _EncoderState(
            venue=venue,
            market=market,
            session=session,
            recovery=recovery,
            revision=revision,
            revision_id=revision_id,
            bids=bids,
            asks=asks,
            state_sha256=state_digest,
        )
        return book, encoding

    def mark_gap(
        self,
        *,
        venue: Venue,
        market: str,
        session: str | int,
        recovery: int,
    ) -> None:
        """Break a chain after a matching data gap until a full snapshot."""

        self._broken_chains.add(_chain_key(venue, market, session, recovery))

    def append(self, record: Mapping[str, Any]) -> BookEvidence:
        if not isinstance(record, Mapping) or record.get("kind") != "BOOK":
            raise BookRevisionChainError("BOOK_RECORD_REQUIRED")
        book, encoding = self._decode(record)
        self.book_count += 1
        if encoding == FULL_BOOK_ENCODING:
            self.full_snapshot_count += 1
        else:
            self.delta_count += 1
        self.maximum_level_count = max(
            self.maximum_level_count,
            len(book.bids) + len(book.asks),
        )
        return book

    def audit(self) -> BookAuditResult:
        return BookAuditResult(
            book_count=self.book_count,
            full_snapshot_count=self.full_snapshot_count,
            delta_count=self.delta_count,
            chain_count=self.chain_count,
            maximum_level_count=self.maximum_level_count,
            current_level_count=self.current_level_count,
        )

    def validate_reference(
        self,
        *,
        venue: Venue,
        market: str,
        session: str | int,
        recovery: int,
        revision: int,
        revision_id: str,
        require_current: bool = False,
    ) -> str:
        """Validate a quote/horizon reference against the observed chain."""

        expected_id = make_book_revision_id(
            venue, market, session, recovery, revision
        )
        if revision_id != expected_id:
            raise BookRevisionChainError("REFERENCE_REVISION_ID_MISMATCH")
        state = self._states.get(_chain_key(venue, market, session, recovery))
        if state is None:
            raise BookRevisionChainError("REFERENCE_CHAIN_NOT_FOUND")
        if require_current and _chain_key(venue, market, session, recovery) in self._broken_chains:
            raise BookRevisionChainError("REFERENCE_CHAIN_BROKEN")
        if require_current and revision != state.revision:
            raise BookRevisionChainError("REFERENCE_NOT_CURRENT")
        if revision > state.revision:
            raise BookRevisionChainError("REFERENCE_REVISION_NOT_OBSERVED")
        return state.state_sha256


BookReconstructor = BookRevisionReconstructor


def _mark_gap_record(
    reconstructor: BookRevisionReconstructor,
    record: Mapping[str, Any],
) -> None:
    if not isinstance(record, Mapping) or record.get("kind") != "DATA_GAP":
        return
    try:
        venue = Venue(record.get("venue"))
    except (TypeError, ValueError):
        return
    session = record.get("stream_session_id")
    recovery = record.get("recovery_generation")
    market = record.get("canonical_market")
    if (
        venue not in (Venue.RISEX, Venue.LIGHTER)
        or isinstance(session, bool)
        or not isinstance(session, (str, int))
        or session == ""
        or not isinstance(recovery, int)
        or isinstance(recovery, bool)
        or recovery < 0
        or not isinstance(market, str)
        or not market
    ):
        return
    reconstructor.mark_gap(
        venue=venue,
        market=market,
        session=session,
        recovery=recovery,
    )


def reconstruct_book_records(records: Iterable[Mapping[str, Any]]) -> Iterator[BookEvidence]:
    """Yield complete books from physical BOOK records with fail-closed audit."""

    reconstructor = BookRevisionReconstructor()
    for record in records:
        _mark_gap_record(reconstructor, record)
        if isinstance(record, Mapping) and record.get("kind") == "BOOK":
            yield reconstructor.append(record)


def audit_book_revisions(records: Iterable[Mapping[str, Any]]) -> BookAuditResult:
    """Audit BOOK chains without retaining historical full-book states."""

    reconstructor = BookRevisionReconstructor()
    for record in records:
        _mark_gap_record(reconstructor, record)
        if isinstance(record, Mapping) and record.get("kind") == "BOOK":
            reconstructor.append(record)
    return reconstructor.audit()


__all__ = [
    "BookAuditResult",
    "BookChainEncoder",
    "BookReconstructor",
    "BookRevisionChainError",
    "BookRevisionEncoder",
    "BookRevisionReconstructor",
    "DELTA_BOOK_ENCODING",
    "FULL_BOOK_ENCODING",
    "LosslessBookEncoder",
    "audit_book_revisions",
    "book_chain_id",
    "reconstruct_book_records",
    "book_state_sha256",
]
