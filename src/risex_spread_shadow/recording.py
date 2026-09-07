"""Bounded public market recording and fail-closed offline readback.

The recording path is deliberately a small extension of the existing public
feed, observer, append-only store, and FULL/DELTA book chain.  It records a
compact receipt before the feed's application filters a frame, then records a
single processing outcome linked to either a reconstructable BOOK revision or
an explicit no-record/loss status.  Readback consumes JSONL as a stream and
reconstructs only the current state of each book chain.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any

from risex_farmer.models import Venue

from .book_chain import (
    BookRevisionChainError,
    BookRevisionReconstructor,
)
from .store import iter_records


RECORDING_SCHEMA_VERSION = 1
RECORDING_MAX_RECORDS = 1_000_000
RECORDING_RECORD_RESERVE = 100_000
RECORDING_MAX_BYTES = 4 * 1024 * 1024 * 1024
RECORDING_BYTES_RESERVE = 512 * 1024 * 1024
RECORDING_TERMINAL_RETENTION_CAPACITY = 64
RECORDING_FRAME_LENGTH_CAP = 65_536
_TERMINAL_KINDS = frozenset({"RUN_STOP", "RUN_FAILED"})
_LOSS_REASONS = frozenset(
    {
        "QUEUE_OVERFLOW",
        "EVIDENCE_STORE_WRITE_FAILED",
        "EVIDENCE_STORAGE_LIMIT",
        "EVIDENCE_APPEND_CANCELLED",
    }
)
_EXPECTED_STOP_GAP_REASONS = frozenset(
    {"PUBLIC_SMOKE_STOPPED", "PUBLIC_SOCKET_DISCONNECTED"}
)
_DATA_STATUSES = frozenset(
    {"VALID", "VALID_UNCHANGED_STATE", "STALE", "INVALID", "UNKNOWN"}
)
_OUTCOME_KINDS = frozenset({"CHANGED", "UNCHANGED", "REJECTED", "UNKNOWN"})
_LINK_STATUSES = frozenset(
    {"PERSISTED_BOOK", "EXISTING_BOOK_STATE", "NO_RECORD", "OBSERVED_LOSS"}
)
_MAX_DIAGNOSTIC_ITEMS = 16
_MAX_INTERVALS = 128


class RecordingReadbackError(ValueError):
    """Raised when a saved recording cannot support a truthful readback."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"recording readback failure: {reason}")


def _fail(reason: str) -> None:
    raise RecordingReadbackError(reason)


def _text(value: Any, name: str, *, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or not value:
        _fail(f"{name.upper()}_INVALID")
    return value


def _non_negative_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        _fail(f"{name.upper()}_INVALID")
    return value


def _optional_non_negative_int(value: Any, name: str) -> int | None:
    if value is None:
        return None
    return _non_negative_int(value, name)


def _venue(value: Any, name: str = "venue") -> Venue:
    try:
        resolved = Venue(value)
    except (TypeError, ValueError):
        _fail(f"{name.upper()}_INVALID")
    if resolved not in (Venue.RISEX, Venue.LIGHTER):
        _fail(f"{name.upper()}_INVALID")
    return resolved


def _session(value: Any, name: str = "stream_session_id") -> str | int:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        _fail(f"{name.upper()}_INVALID")
    if value == "":
        _fail(f"{name.upper()}_INVALID")
    return value


def _utc_text(value: Any, name: str) -> str:
    text = _text(value, name)
    assert text is not None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        _fail(f"{name.upper()}_INVALID")
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        _fail(f"{name.upper()}_INVALID")
    return text


def _sha256(value: Any, name: str) -> str:
    text = _text(value, name)
    assert text is not None
    if len(text) != 64 or any(character not in "0123456789abcdef" for character in text):
        _fail(f"{name.upper()}_INVALID")
    return text


def _optional_market(value: Any, name: str = "canonical_market") -> str | None:
    result = _text(value, name, optional=True)
    if name == "canonical_market" and result not in (None, "BTC"):
        _fail("RECORDING_MARKET_NOT_BTC")
    return result


def _record_time(record: Mapping[str, Any], *, required: bool = True) -> int | None:
    value = record.get("observed_monotonic_ns")
    if value is None:
        value = record.get("received_monotonic_ns")
    if value is None and not required:
        return None
    return _non_negative_int(value, "observed_monotonic_ns")


def _record_count(counter: Counter[str]) -> dict[str, int]:
    return {key: counter[key] for key in sorted(counter)}


def _bounded_append(values: list[dict[str, Any]], value: dict[str, Any]) -> None:
    if len(values) < _MAX_DIAGNOSTIC_ITEMS:
        values.append(value)


def _validate_envelope(metadata: Mapping[str, Any]) -> None:
    envelope = metadata.get("recording_envelope")
    if not isinstance(envelope, Mapping):
        _fail("RECORDING_ENVELOPE_MISSING")
    expected = {
        "max_records": RECORDING_MAX_RECORDS,
        "record_reserve": RECORDING_RECORD_RESERVE,
        "max_bytes": RECORDING_MAX_BYTES,
        "bytes_reserve": RECORDING_BYTES_RESERVE,
        "terminal_retention_capacity": RECORDING_TERMINAL_RETENTION_CAPACITY,
    }
    for name, expected_value in expected.items():
        if envelope.get(name) != expected_value:
            _fail(f"RECORDING_ENVELOPE_{name.upper()}_MISMATCH")


def _interval_status(data_status: str) -> str:
    if data_status in {"VALID", "VALID_UNCHANGED_STATE"}:
        return data_status
    if data_status == "STALE":
        return "STALE"
    if data_status == "INVALID":
        return "INVALID"
    return "UNKNOWN"


def _append_interval(
    intervals: list[dict[str, Any]],
    *,
    start_ns: int,
    status: str,
    reason: str,
) -> None:
    if intervals:
        previous = intervals[-1]
        if previous["status"] == status and previous["reason"] == reason:
            previous["end_monotonic_ns"] = start_ns
            return
    if len(intervals) >= _MAX_INTERVALS:
        return
    intervals.append(
        {
            "start_monotonic_ns": start_ns,
            "end_monotonic_ns": None,
            "status": status,
            "reason": reason,
        }
    )


class _Availability:
    """Integrate current two-venue eligibility; silence never refreshes state."""

    def __init__(self, start: int) -> None:
        self.at = start
        self.start = start
        self.books: dict[Venue, Any] = {}
        self.durations: Counter[str] = Counter()
        self.intervals: list[dict[str, Any]] = []
        self.truncated = False

    def reason(self) -> str:
        for venue in (Venue.RISEX, Venue.LIGHTER):
            b = self.books.get(venue)
            if b is None:
                return "MISSING_" + venue.value + "_BOOK"
            if not b.fresh or not b.is_sequence_healthy:
                return "UNHEALTHY_" + venue.value + "_BOOK"
            if self.at - b.received_monotonic_ns > 500_000_000:
                return "STALE_" + venue.value + "_BOOK"
        if abs(self.books[Venue.RISEX].received_monotonic_ns -
               self.books[Venue.LIGHTER].received_monotonic_ns) > 500_000_000:
            return "INPUT_RECEIPT_SKEW"
        return "VALID"

    def advance(self, until: int) -> None:
        if until < self.at:
            _fail("AVAILABILITY_CLOCK_REGRESSION")
        while self.at < until:
            end = min([until] + [b.received_monotonic_ns + 500_000_001
                      for b in self.books.values()
                      if self.at < b.received_monotonic_ns + 500_000_001 < until])
            reason = self.reason()
            self.durations[reason] += end - self.at
            if self.intervals and self.intervals[-1]["reason"] == reason and self.intervals[-1]["end_monotonic_ns"] == self.at:
                self.intervals[-1]["end_monotonic_ns"] = end
            elif len(self.intervals) < _MAX_INTERVALS:
                self.intervals.append({"start_monotonic_ns": self.at, "end_monotonic_ns": end,
                                       "status": "VALID" if reason == "VALID" else "INELIGIBLE",
                                       "reason": reason})
            else:
                self.truncated = True
            self.at = end


def build_recording_readback(path: str | Path) -> dict[str, Any]:
    """Stream one recording file and return deterministic compact facts."""

    evidence_path = Path(path)
    try:
        records = iter_records(evidence_path)
    except OSError as exc:
        raise RecordingReadbackError("EVIDENCE_FILE_UNREADABLE") from exc

    metadata: Mapping[str, Any] | None = None
    run_id: str | None = None
    record_count = 0
    byte_count = 0
    kind_counts: Counter[str] = Counter()
    receipt_counts: Counter[str] = Counter()
    outcome_counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    data_status_counts: Counter[str] = Counter()
    link_status_counts: Counter[str] = Counter()
    receipts: dict[str, dict[str, Any]] = {}
    candidate_receipts: set[str] = set()
    outcome_receipts: set[str] = set()
    gap_receipts: set[str] = set()
    book_by_id: dict[str, dict[str, Any]] = {}
    book_receipts: list[tuple[str, str]] = []
    pending_link_ids: list[tuple[str, str, str, int]] = []
    reconstructor = BookRevisionReconstructor()
    diagnostic_receipts: list[dict[str, Any]] = []
    diagnostic_outcomes: list[dict[str, Any]] = []
    diagnostic_gaps: list[dict[str, Any]] = []
    intervals: list[dict[str, Any]] = []
    first_receipt_ns: int | None = None
    last_receipt_ns: int | None = None
    first_candidate_ns: int | None = None
    last_candidate_ns: int | None = None
    terminal_kind: str | None = None
    terminal_record: Mapping[str, Any] | None = None
    loss_count = 0
    missing_receipt_count = 0
    missing_outcome_count = 0
    linked_book_count = 0
    unlinked_no_record_count = 0
    unchanged_count = 0
    changed_count = 0
    rejected_count = 0
    interval_truncated = False
    availability: _Availability | None = None
    incomplete_observations: set[str] = set()
    last_processing_ready_ns = 0

    try:
        for record in records:
            if not isinstance(record, Mapping):
                _fail("RECORD_NOT_OBJECT")
            record_count += 1
            if record_count > RECORDING_MAX_RECORDS:
                _fail("RECORD_COUNT_EXCEEDS_ENVELOPE")
            kind = record.get("kind")
            if not isinstance(kind, str) or not kind:
                _fail("RECORD_KIND_INVALID")
            kind_counts[kind] += 1
            index = _non_negative_int(record.get("record_index"), "record_index")
            if index != record_count - 1:
                _fail("RECORD_INDEX_NOT_CONTIGUOUS")
            record_run_id = _text(record.get("run_id"), "run_id")
            assert record_run_id is not None
            if run_id is None:
                run_id = record_run_id
            elif record_run_id != run_id:
                _fail("RUN_ID_MISMATCH")
            if kind in _TERMINAL_KINDS:
                if terminal_kind is not None:
                    _fail("DUPLICATE_TERMINAL_MARKER")
                terminal_kind = kind
                terminal_record = record
                if record_count != 0 and kind_counts[kind] != 1:
                    _fail("TERMINAL_MARKER_DUPLICATE")
                continue
            if terminal_kind is not None:
                _fail("RECORD_AFTER_TERMINAL")

            if kind == "RUN_METADATA":
                if record_count != 1 or metadata is not None:
                    _fail("RUN_METADATA_NOT_FIRST_OR_DUPLICATE")
                raw_metadata = record.get("metadata")
                if not isinstance(raw_metadata, Mapping):
                    _fail("RUN_METADATA_INVALID")
                metadata = raw_metadata
                if metadata.get("run_id") not in {None, run_id}:
                    _fail("RUN_METADATA_RUN_ID_MISMATCH")
                if metadata.get("recording_mode") is not True:
                    _fail("RECORDING_MODE_MISSING")
                _validate_envelope(metadata)
                availability = _Availability(_non_negative_int(metadata.get("started_monotonic_ns"), "started_monotonic_ns"))
                continue
            if metadata is None:
                _fail("RUN_METADATA_MISSING")

            if kind == "PUBLIC_RECEIPT":
                receipt_id = _text(record.get("receipt_id"), "receipt_id")
                assert receipt_id is not None
                if receipt_id in receipts:
                    _fail("DUPLICATE_RECEIPT_ID")
                venue = _venue(record.get("venue"))
                market = _optional_market(record.get("canonical_market"))
                session = _session(record.get("stream_session_id"))
                recovery = _non_negative_int(
                    record.get("recovery_generation"), "recovery_generation"
                )
                received_ns = _non_negative_int(
                    record.get("received_monotonic_ns"), "received_monotonic_ns"
                )
                _utc_text(record.get("received_utc"), "received_utc")
                frame_kind = _text(record.get("frame_kind"), "frame_kind")
                frame_category = _text(record.get("frame_category"), "frame_category")
                frame_length = _non_negative_int(record.get("frame_length"), "frame_length")
                if frame_length > RECORDING_FRAME_LENGTH_CAP:
                    _fail("FRAME_LENGTH_EXCEEDS_CAP")
                _sha256(record.get("frame_sha256"), "frame_sha256")
                assert availability is not None
                if received_ns < availability.start:
                    _fail("RECEIPT_BEFORE_RUN_START")
                candidate = record.get("orderbook_candidate")
                if not isinstance(candidate, bool):
                    _fail("ORDERBOOK_CANDIDATE_INVALID")
                _optional_market(record.get("venue_symbol"), "venue_symbol")
                market_id = record.get("market_id")
                if market_id is not None and (
                    isinstance(market_id, bool)
                    or not isinstance(market_id, (str, int))
                    or market_id == ""
                ):
                    _fail("MARKET_ID_INVALID")
                _optional_market(record.get("message_type"), "message_type")
                _optional_market(record.get("channel"), "channel")
                receipts[receipt_id] = {
                    "venue": venue.value,
                    "canonical_market": market,
                    "stream_session_id": session,
                    "recovery_generation": recovery,
                    "received_monotonic_ns": received_ns,
                    "orderbook_candidate": candidate,
                }
                receipt_counts["orderbook_candidate" if candidate else "other"] += 1
                first_receipt_ns = (
                    received_ns
                    if first_receipt_ns is None
                    else min(first_receipt_ns, received_ns)
                )
                last_receipt_ns = (
                    received_ns
                    if last_receipt_ns is None
                    else max(last_receipt_ns, received_ns)
                )
                if candidate:
                    candidate_receipts.add(receipt_id)
                    first_candidate_ns = (
                        received_ns
                        if first_candidate_ns is None
                        else min(first_candidate_ns, received_ns)
                    )
                    last_candidate_ns = (
                        received_ns
                        if last_candidate_ns is None
                        else max(last_candidate_ns, received_ns)
                    )
                _bounded_append(
                    diagnostic_receipts,
                    {
                        "receipt_id": receipt_id,
                        "venue": venue.value,
                        "canonical_market": market,
                        "received_monotonic_ns": received_ns,
                        "frame_category": frame_category,
                        "orderbook_candidate": candidate,
                    },
                )
                continue

            if kind == "BOOK":
                receipt_id = _text(record.get("receipt_id"), "receipt_id")
                if receipt_id is None:
                    _fail("BOOK_RECEIPT_LINK_MISSING")
                try:
                    book = reconstructor.append(record)
                except BookRevisionChainError as exc:
                    _fail(f"BOOK_CHAIN_{exc.reason}")
                book_id = _text(record.get("book_revision_id"), "book_revision_id")
                assert book_id is not None
                if book_id in book_by_id:
                    _fail("DUPLICATE_BOOK_REVISION_ID")
                state_digest = record.get("book_state_sha256")
                if not isinstance(state_digest, str):
                    _fail("BOOK_STATE_DIGEST_MISSING")
                book_by_id[book_id] = {
                    "book_revision_id": book_id,
                    "venue": book.venue.value,
                    "canonical_market": book.canonical_market,
                    "stream_session_id": book.stream_session_id,
                    "recovery_generation": book.recovery_generation,
                    "book_revision": book.book_revision,
                    "book_state_sha256": state_digest,
                    "received_monotonic_ns": book.received_monotonic_ns,
                    "sequence_valid": book.sequence_valid,
                    "checksum_valid": book.checksum_valid,
                    "fresh": book.fresh,
                }
                source = receipts.get(receipt_id)
                if source is None:
                    _fail("BOOK_RECEIPT_LINK_MISSING")
                if (not source["orderbook_candidate"] or source["venue"] != book.venue.value
                    or source["canonical_market"] != book.canonical_market
                    or source["stream_session_id"] != book.stream_session_id
                    or source["recovery_generation"] != book.recovery_generation
                    or source["received_monotonic_ns"] != book.received_monotonic_ns):
                    _fail("BOOK_RECEIPT_CONTEXT_MISMATCH")
                ready = max(x for x in (book.received_monotonic_ns, book.normalized_ready_monotonic_ns,
                                       book.decision_ready_monotonic_ns) if x is not None)
                if book.normalized_ready_monotonic_ns is None or ready < book.received_monotonic_ns:
                    _fail("BOOK_READINESS_INVALID")
                assert availability is not None
                availability.advance(max(availability.at, ready))
                availability.books[book.venue] = book
                book_by_id[book_id]["receipt_id"] = receipt_id
                book_by_id[book_id]["ready_ns"] = ready
                book_receipts.append((receipt_id, book_id))
                continue

            if kind == "DATA_GAP":
                venue = _venue(record.get("venue"))
                market = _text(record.get("canonical_market"), "canonical_market")
                assert market is not None
                session = _session(record.get("stream_session_id"))
                recovery = _non_negative_int(
                    record.get("recovery_generation"), "recovery_generation"
                )
                start_ns = _non_negative_int(
                    record.get("gap_start_monotonic_ns"), "gap_start_monotonic_ns"
                )
                _optional_non_negative_int(
                    record.get("gap_end_monotonic_ns"), "gap_end_monotonic_ns"
                )
                reason = _text(record.get("reason"), "reason")
                assert reason is not None
                receipt_id = _text(record.get("receipt_id"), "receipt_id", optional=True)
                if receipt_id is not None:
                    gap_receipts.add(receipt_id)
                if market != "UNKNOWN":
                    reconstructor.mark_gap(
                        venue=venue,
                        market=market,
                        session=session,
                        recovery=recovery,
                    )
                reason_counts[reason] += 1
                is_loss = reason in _LOSS_REASONS
                if record.get("transport_event") == "UNEXPECTED_FAILURE":
                    incomplete_observations.add("PUBLIC_SOCKET_TRANSPORT_FAILURE")
                if is_loss:
                    loss_count += 1
                    incomplete_observations.add(reason)
                assert availability is not None
                availability.advance(max(availability.at, start_ns))
                availability.books.pop(venue, None)
                _bounded_append(
                    diagnostic_gaps,
                    {
                        "venue": venue.value,
                        "canonical_market": market,
                        "reason": reason,
                        "receipt_id": receipt_id,
                        "gap_start_monotonic_ns": start_ns,
                        "loss": is_loss,
                    },
                )
                continue

            if kind == "PUBLIC_RECEIPT_OUTCOME":
                receipt_id = _text(record.get("receipt_id"), "receipt_id")
                assert receipt_id is not None
                if receipt_id in outcome_receipts:
                    _fail("DUPLICATE_RECEIPT_OUTCOME")
                outcome = _text(record.get("processing_outcome"), "processing_outcome")
                assert outcome is not None
                if outcome not in _OUTCOME_KINDS:
                    _fail("PROCESSING_OUTCOME_INVALID")
                if record.get("outcome") not in {None, outcome}:
                    _fail("PROCESSING_OUTCOME_MISMATCH")
                venue = _venue(record.get("venue"))
                market = _optional_market(record.get("canonical_market"))
                received_ns = _non_negative_int(
                    record.get("received_monotonic_ns"), "received_monotonic_ns"
                )
                processing_ready = _non_negative_int(record.get("processing_ready_monotonic_ns"), "processing_ready_monotonic_ns")
                last_processing_ready_ns = max(last_processing_ready_ns, processing_ready)
                if processing_ready < received_ns:
                    _fail("OUTCOME_PROCESSING_PRECEDES_RECEIPT")
                reason = _text(record.get("reason"), "reason")
                assert reason is not None
                link_status = _text(record.get("link_status"), "link_status")
                assert link_status is not None
                if link_status not in _LINK_STATUSES:
                    _fail("LINK_STATUS_INVALID")
                data_status = _text(record.get("data_status"), "data_status")
                assert data_status is not None
                if data_status not in _DATA_STATUSES:
                    _fail("DATA_STATUS_INVALID")
                data_status_reason = _text(
                    record.get("data_status_reason"),
                    "data_status_reason",
                    optional=True,
                )
                if data_status_reason is None:
                    _fail("DATA_STATUS_REASON_MISSING")
                linked_book_id = _text(
                    record.get("linked_book_revision_id"),
                    "linked_book_revision_id",
                    optional=True,
                )
                if outcome == "CHANGED" and link_status != "PERSISTED_BOOK":
                    _fail("CHANGED_OUTCOME_LINK_INVALID")
                if outcome == "UNCHANGED" and link_status != "EXISTING_BOOK_STATE":
                    _fail("UNCHANGED_OUTCOME_LINK_INVALID")
                if outcome == "REJECTED" and link_status not in {
                    "NO_RECORD",
                    "OBSERVED_LOSS",
                }:
                    _fail("REJECTED_OUTCOME_LINK_INVALID")
                if link_status in {"PERSISTED_BOOK", "EXISTING_BOOK_STATE"}:
                    if linked_book_id is None:
                        _fail("BOOK_LINK_MISSING")
                    linked_digest = _sha256(
                        record.get("linked_book_state_sha256"),
                        "linked_book_state_sha256",
                    )
                    linked_revision = _non_negative_int(
                        record.get("linked_book_revision"),
                        "linked_book_revision",
                    )
                    target = book_by_id.get(linked_book_id)
                    if target is None:
                        _fail("BOOK_LINK_TARGET_MISSING_OR_FUTURE")
                    source = receipts.get(receipt_id)
                    if source is None:
                        _fail("RECEIPT_LINK_MISSING")
                    if (source["stream_session_id"] != target["stream_session_id"]
                        or source["recovery_generation"] != target["recovery_generation"]):
                        _fail("BOOK_LINK_SESSION_MISMATCH")
                    if outcome == "CHANGED" and target["receipt_id"] != receipt_id:
                        _fail("CHANGED_BOOK_RECEIPT_MISMATCH")
                    if outcome == "UNCHANGED" and target["ready_ns"] > received_ns:
                        _fail("UNCHANGED_FUTURE_BOOK")
                    pending_link_ids.append(
                        (receipt_id, linked_book_id, linked_digest, linked_revision)
                    )
                    linked_book_count += 1
                elif linked_book_id is not None or record.get("linked_book_state_sha256") is not None:
                    _fail("NO_RECORD_HAS_BOOK_LINK")
                if receipt_id not in receipts:
                    if link_status == "OBSERVED_LOSS":
                        missing_receipt_count += 1
                    else:
                        _fail("RECEIPT_LINK_MISSING")
                else:
                    source = receipts[receipt_id]
                    if not source["orderbook_candidate"]:
                        _fail("OUTCOME_LINKS_NON_ORDERBOOK_RECEIPT")
                    if source["venue"] != venue.value:
                        _fail("RECEIPT_OUTCOME_VENUE_MISMATCH")
                    if market is not None and source["canonical_market"] not in {
                        None,
                        market,
                    }:
                        _fail("RECEIPT_OUTCOME_MARKET_MISMATCH")
                    if received_ns < source["received_monotonic_ns"]:
                        _fail("RECEIPT_OUTCOME_PRECEDES_RECEIPT")
                if link_status == "OBSERVED_LOSS":
                    incomplete_observations.add(reason)
                if data_status_reason == "PROCESSING_RESULT_NOT_OBSERVED":
                    incomplete_observations.add("PROCESSING_OUTCOME_UNKNOWN")
                outcome_receipts.add(receipt_id)
                outcome_counts[outcome] += 1
                reason_counts[reason] += 1
                data_status_counts[data_status] += 1
                link_status_counts[link_status] += 1
                if outcome == "CHANGED":
                    changed_count += 1
                elif outcome == "UNCHANGED":
                    unchanged_count += 1
                elif outcome == "REJECTED":
                    rejected_count += 1
                status = _interval_status(data_status)
                if len(intervals) >= _MAX_INTERVALS:
                    interval_truncated = True
                _append_interval(
                    intervals,
                    start_ns=received_ns,
                    status=status,
                    reason=data_status_reason,
                )
                if link_status in {"NO_RECORD", "OBSERVED_LOSS"}:
                    unlinked_no_record_count += 1
                _bounded_append(
                    diagnostic_outcomes,
                    {
                        "receipt_id": receipt_id,
                        "venue": venue.value,
                        "canonical_market": market,
                        "processing_outcome": outcome,
                        "reason": reason,
                        "link_status": link_status,
                        "data_status": data_status,
                        "received_monotonic_ns": received_ns,
                    },
                )
                continue

            # Trades, run-start metadata, and future additive records are
            # retained as physical evidence.  They do not affect receipt
            # accounting, but their local clock remains bounded when present.
            if kind == "RISEX_TRADE":
                _venue(record.get("venue"))
                _text(record.get("canonical_market"), "canonical_market")
            _record_time(record, required=False)

        if record_count == 0:
            _fail("EVIDENCE_EMPTY")
        if metadata is None:
            _fail("RUN_METADATA_MISSING")
        if terminal_kind is None or terminal_record is None:
            _fail("TERMINAL_MARKER_MISSING")
        if record_count > RECORDING_MAX_RECORDS:
            _fail("RECORD_COUNT_EXCEEDS_ENVELOPE")
        try:
            byte_count = evidence_path.stat().st_size
        except OSError as exc:
            raise RecordingReadbackError("EVIDENCE_FILE_UNREADABLE") from exc
        if byte_count > RECORDING_MAX_BYTES:
            _fail("FILE_BYTES_EXCEED_ENVELOPE")
        if record_count > 0 and terminal_kind not in _TERMINAL_KINDS:
            _fail("TERMINAL_MARKER_INVALID")
        for receipt_id, book_id in book_receipts:
            source = receipts.get(receipt_id)
            if source is None:
                _fail("BOOK_RECEIPT_LINK_MISSING")
            if not source["orderbook_candidate"]:
                _fail("BOOK_LINKS_NON_ORDERBOOK_RECEIPT")
            book = book_by_id[book_id]
            if source["venue"] != book["venue"] or source["canonical_market"] not in {
                None,
                book["canonical_market"],
            }:
                _fail("BOOK_RECEIPT_CONTEXT_MISMATCH")
        for receipt_id, book_id, linked_digest, linked_revision in pending_link_ids:
            book = book_by_id.get(book_id)
            if book is None:
                _fail("BOOK_LINK_TARGET_MISSING")
            if book["book_state_sha256"] != linked_digest:
                _fail("BOOK_LINK_DIGEST_MISMATCH")
            if book["book_revision"] != linked_revision:
                _fail("BOOK_LINK_REVISION_MISMATCH")
            source = receipts.get(receipt_id)
            if source is not None and (
                source["venue"] != book["venue"]
                or source["canonical_market"] not in {None, book["canonical_market"]}
            ):
                _fail("BOOK_LINK_CONTEXT_MISMATCH")
        for receipt_id in candidate_receipts:
            if receipt_id not in outcome_receipts:
                missing_outcome_count += 1
        if missing_outcome_count and terminal_kind == "RUN_STOP":
            _fail("RECEIPT_OUTCOME_MISSING")
        terminal_ns = _record_time(terminal_record)
        assert availability is not None and terminal_ns is not None
        if terminal_ns < max(last_receipt_ns or 0, last_processing_ready_ns):
            _fail("TERMINAL_PRECEDES_OBSERVATIONS")
        availability.advance(terminal_ns)
        collector_duration_ns = terminal_ns - availability.start
        intervals = availability.intervals
        interval_truncated = availability.truncated
        unexpected_gap_reasons = sorted(
            reason
            for reason in reason_counts
            if reason not in _EXPECTED_STOP_GAP_REASONS
            and reason in _LOSS_REASONS
        )
        incomplete_reasons = list(unexpected_gap_reasons) + list(incomplete_observations)
        if terminal_kind == "RUN_FAILED":
            incomplete_reasons.append(
                str(terminal_record.get("fatal_reason") or "RUN_FAILED")
            )
        if missing_outcome_count:
            incomplete_reasons.append("RECEIPT_OUTCOME_MISSING")
        incomplete_reasons = sorted(set(incomplete_reasons))
        if terminal_kind == "RUN_STOP" and terminal_record.get("fatal_reason") is not None:
            _fail("RUN_STOP_HAS_FATAL_REASON")
        if data_status_counts and set(data_status_counts) == {"VALID"}:
            data_status = "VALID"
        elif data_status_counts and set(data_status_counts) <= {
            "VALID",
            "VALID_UNCHANGED_STATE",
        }:
            data_status = "VALID"
        elif data_status_counts:
            data_status = "MIXED"
        else:
            data_status = "UNKNOWN"
        audit = reconstructor.audit()
        return {
            "schema_version": RECORDING_SCHEMA_VERSION,
            "path": str(evidence_path),
            "run_id": run_id,
            "terminal": terminal_kind,
            "recording_status": "INCOMPLETE" if incomplete_reasons else "COMPLETE",
            "incomplete_reasons": incomplete_reasons,
            "application_silence": receipt_counts["orderbook_candidate"] == 0,
            "data_status": "VALID" if availability.durations.get("VALID", 0) == collector_duration_ns and collector_duration_ns > 0 else "MIXED_OR_INELIGIBLE",
            "data_eligibility_duration_ns": dict(sorted(availability.durations.items())),
            "data_valid_duration_ns": availability.durations.get("VALID", 0),
            "data_ineligible_duration_ns": collector_duration_ns - availability.durations.get("VALID", 0),
            "record_count": record_count,
            "byte_count": byte_count,
            "record_counts": _record_count(kind_counts),
            "receipt_counts": _record_count(receipt_counts),
            "outcome_counts": _record_count(outcome_counts),
            "reason_counts": _record_count(reason_counts),
            "data_status_counts": _record_count(data_status_counts),
            "link_status_counts": _record_count(link_status_counts),
            "receipt_count": len(receipts),
            "orderbook_receipt_count": len(candidate_receipts),
            "outcome_count": len(outcome_receipts),
            "missing_outcome_count": missing_outcome_count,
            "missing_receipt_count": missing_receipt_count,
            "changed_count": changed_count,
            "unchanged_count": unchanged_count,
            "rejected_count": rejected_count,
            "unknown_processing_count": outcome_counts["UNKNOWN"],
            "linked_book_count": linked_book_count,
            "no_record_or_loss_count": unlinked_no_record_count,
            "observed_loss_count": loss_count,
            "collector_observation": {
                "first_receipt_monotonic_ns": first_receipt_ns,
                "last_receipt_monotonic_ns": last_receipt_ns,
                "first_orderbook_receipt_monotonic_ns": first_candidate_ns,
                "last_orderbook_receipt_monotonic_ns": last_candidate_ns,
                "duration_ns": collector_duration_ns,
            },
            "data_intervals": intervals,
            "data_intervals_truncated": interval_truncated,
            "book_audit": {
                "book_count": audit.book_count,
                "full_snapshot_count": audit.full_snapshot_count,
                "delta_count": audit.delta_count,
                "chain_count": audit.chain_count,
                "maximum_level_count": audit.maximum_level_count,
                "current_level_count": audit.current_level_count,
            },
            "diagnostic_sample": {
                "receipts": diagnostic_receipts,
                "outcomes": diagnostic_outcomes,
                "gaps": diagnostic_gaps,
            },
        }
    except RecordingReadbackError:
        raise
    except (OSError, ValueError, TypeError, KeyError) as exc:
        raise RecordingReadbackError("READBACK_PARSE_FAILURE") from exc


def render_recording_readback(
    path: str | Path,
    *,
    format: str = "json",
) -> str:
    """Render the deterministic readback as JSON or a compact table."""

    if format not in {"json", "table"}:
        raise ValueError("recording report format must be json or table")
    report = build_recording_readback(path)
    if format == "json":
        return json.dumps(report, sort_keys=True, separators=(",", ":"))
    lines = [
        "Recording readback",
        f"path: {report['path']}",
        f"run: {report['run_id']}",
        f"status: {report['recording_status']}",
        f"data_status: {report['data_status']}",
        f"terminal: {report['terminal']}",
        f"records: {report['record_count']} ({report['byte_count']} bytes)",
        f"receipts: {report['receipt_count']} ({report['orderbook_receipt_count']} orderbook)",
        f"outcomes: {report['outcome_count']} changed={report['changed_count']} "
        f"unchanged={report['unchanged_count']} rejected={report['rejected_count']}",
        f"books: {report['book_audit']['book_count']} "
        f"full={report['book_audit']['full_snapshot_count']} "
        f"delta={report['book_audit']['delta_count']}",
        f"losses: {report['observed_loss_count']}",
        f"collector_duration_ns: {report['collector_observation']['duration_ns']}",
    ]
    if report["incomplete_reasons"]:
        lines.append("incomplete_reasons: " + ",".join(report["incomplete_reasons"]))
    return "\n".join(lines)


__all__ = [
    "RECORDING_BYTES_RESERVE",
    "RECORDING_FRAME_LENGTH_CAP",
    "RECORDING_MAX_BYTES",
    "RECORDING_MAX_RECORDS",
    "RECORDING_RECORD_RESERVE",
    "RECORDING_SCHEMA_VERSION",
    "RECORDING_TERMINAL_RETENTION_CAPACITY",
    "RecordingReadbackError",
    "build_recording_readback",
    "render_recording_readback",
]
