"""Deterministic offline aggregation of SS-001H JSONL evidence."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
from typing import Any, Iterator

from risex_farmer.models import Venue

from .book_chain import (
    BookAuditResult,
    BookRevisionChainError,
    BookRevisionReconstructor,
    book_state_sha256,
)
from .models import make_book_revision_id
from .store import iter_records


_STRICT_MODEL = "STRICT_LOWER_BOUND"
_OPTIMISTIC_MODEL = "OPTIMISTIC_UPPER_BOUND"
_MODELS = (_STRICT_MODEL, _OPTIMISTIC_MODEL)
_HORIZONS = (0, 300, 500, 1000)
_QUANTILE_SAMPLE_CAP = 256
# The public grid permits at most three markets, 24 policies per market
# (direction x three notionals x four margins), and 500 eligible trades.  The
# report keeps exact episode/version identity through that whole bound; 256 is
# retained only for diagnostic quantiles, never for prospective coverage.
_MAX_PROSPECTIVE_MARKETS = 3
_MAX_POLICIES_PER_MARKET = 2 * 3 * 4
_MAX_PROSPECTIVE_POLICIES = _MAX_PROSPECTIVE_MARKETS * _MAX_POLICIES_PER_MARKET
_MAX_PROSPECTIVE_ELIGIBLE_TRADES = 500
_MAX_MODEL_EPISODES = _MAX_PROSPECTIVE_POLICIES * _MAX_PROSPECTIVE_ELIGIBLE_TRADES
_MAX_TOTAL_EPISODES = _MAX_MODEL_EPISODES * len(_MODELS)
_POLICY_FILL_VERSION_CAP = _MAX_PROSPECTIVE_ELIGIBLE_TRADES
_HORIZON_VERSION_CAP = _MAX_MODEL_EPISODES
_EPISODE_CONTEXT_CAP = _MAX_TOTAL_EPISODES
_RECENT_TRADE_KEY_CAP = 4096
_RECENT_GAP_CAP = 64
_TERMINAL_KINDS = frozenset({"RUN_STOP", "RUN_FAILED"})
_MAX_BOOK_REFERENCE_REQUESTS = _MAX_TOTAL_EPISODES * len(_HORIZONS)


class EvidenceIntegrityError(ValueError):
    """Raised when the physical evidence stream cannot support a report."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"evidence integrity failure: {reason}")


def _is_terminal_kind(value: Any) -> bool:
    return isinstance(value, str) and value in _TERMINAL_KINDS


class _BoundedValues:
    """Deterministic bounded reservoir for diagnostic quantiles."""

    __slots__ = ("count", "_values")

    def __init__(self) -> None:
        self.count = 0
        self._values: list[Decimal] = []

    def add(self, value: Decimal | None) -> None:
        if value is None:
            return
        self.count += 1
        if len(self._values) < _QUANTILE_SAMPLE_CAP:
            self._values.append(value)
            return
        # A fixed arithmetic progression makes the bounded sample stable
        # across consecutive report reads without using process randomness.
        slot = (self.count * 1_103_515_245 + 12_345) % self.count
        if slot < _QUANTILE_SAMPLE_CAP:
            self._values[slot] = value

    def ordered(self) -> list[Decimal]:
        return sorted(self._values)


@dataclass(slots=True)
class _NumberStats:
    count: int = 0
    total: Decimal = Decimal("0")
    values: _BoundedValues = field(default_factory=_BoundedValues)

    def add(self, value: Decimal | None) -> None:
        if value is None:
            return
        self.count += 1
        self.total += value
        self.values.add(value)

    def mean(self) -> Decimal | None:
        return self.total / Decimal(self.count) if self.count else None

    def median(self) -> Decimal | None:
        ordered = self.values.ordered()
        if not ordered:
            return None
        middle = len(ordered) // 2
        if len(ordered) % 2:
            return ordered[middle]
        return (ordered[middle - 1] + ordered[middle]) / Decimal("2")

    def percentile(self, fraction: Decimal) -> Decimal | None:
        ordered = self.values.ordered()
        if not ordered:
            return None
        index = int((len(ordered) - 1) * fraction)
        return ordered[index]

    def minimum(self) -> Decimal | None:
        ordered = self.values.ordered()
        return ordered[0] if ordered else None

    def maximum(self) -> Decimal | None:
        ordered = self.values.ordered()
        return ordered[-1] if ordered else None


@dataclass(slots=True)
class _HorizonStats:
    """Raw horizon evidence plus episode-local valid classifications."""

    observation_count: int = 0
    outcomes: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    filled_quantity: _NumberStats = field(default_factory=_NumberStats)
    notional: _NumberStats = field(default_factory=_NumberStats)
    raw_entry_edge: _NumberStats = field(default_factory=_NumberStats)
    raw_markout: _NumberStats = field(default_factory=_NumberStats)
    entry_edge: _NumberStats = field(default_factory=_NumberStats)
    markout: _NumberStats = field(default_factory=_NumberStats)
    version_ids: set[str] = field(default_factory=set)
    version_id_capacity_exceeded: bool = False
    contaminated: bool = False
    gap_reasons: set[str] = field(default_factory=set)
    valid_observation_count: int = 0
    valid_outcomes: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    valid_filled_quantity: _NumberStats = field(default_factory=_NumberStats)
    valid_notional: _NumberStats = field(default_factory=_NumberStats)
    contaminated_observation_count: int = 0
    contaminated_outcomes: dict[str, int] = field(
        default_factory=lambda: defaultdict(int)
    )
    contaminated_filled_quantity: _NumberStats = field(default_factory=_NumberStats)
    contaminated_notional: _NumberStats = field(default_factory=_NumberStats)
    contaminated_entry_edge: _NumberStats = field(default_factory=_NumberStats)
    contaminated_markout: _NumberStats = field(default_factory=_NumberStats)
    contamination_reason_counts: dict[str, int] = field(
        default_factory=lambda: defaultdict(int)
    )
    missing_expected_count: int = 0
    edge_excluded_by_fill_count: int = 0

    def add_raw(self, record: dict[str, Any], *, track_version: bool = True) -> None:
        self.observation_count += 1
        outcome = _key_text(record.get("outcome"))
        self.outcomes[outcome] += 1
        self.filled_quantity.add(_decimal(record.get("filled_quantity")))
        self.notional.add(_decimal(record.get("notional_usd")))
        if outcome == "HEDGE_FULL":
            self.raw_entry_edge.add(_decimal(record.get("entry_edge_usd")))
            self.raw_markout.add(_decimal(record.get("conditional_markout_usd")))
        version_id = record.get("quote_version_id")
        if track_version and version_id is not None:
            if len(self.version_ids) < _HORIZON_VERSION_CAP:
                self.version_ids.add(_key_text(version_id))
            else:
                self.version_id_capacity_exceeded = True

    def classify(
        self,
        record: dict[str, Any],
        *,
        contaminated_reasons: set[str] | frozenset[str] = frozenset(),
        edge_allowed: bool = True,
    ) -> None:
        """Classify one already-counted raw horizon without mutating raw data."""

        outcome = _key_text(record.get("outcome"))
        reasons = tuple(sorted({_key_text(reason) for reason in contaminated_reasons if reason}))
        if reasons:
            self.contaminated_observation_count += 1
            self.contaminated_outcomes[outcome] += 1
            self.contaminated_filled_quantity.add(
                _decimal(record.get("filled_quantity"))
            )
            self.contaminated_notional.add(_decimal(record.get("notional_usd")))
            if outcome == "HEDGE_FULL":
                self.contaminated_entry_edge.add(
                    _decimal(record.get("entry_edge_usd"))
                )
                self.contaminated_markout.add(
                    _decimal(record.get("conditional_markout_usd"))
                )
            for reason in reasons:
                self.contamination_reason_counts[reason] += 1
            self.contaminated = True
            self.gap_reasons.update(reasons)
            return
        self.valid_observation_count += 1
        self.valid_outcomes[outcome] += 1
        self.valid_filled_quantity.add(_decimal(record.get("filled_quantity")))
        self.valid_notional.add(_decimal(record.get("notional_usd")))
        if outcome == "HEDGE_FULL":
            if edge_allowed:
                self.entry_edge.add(_decimal(record.get("entry_edge_usd")))
                self.markout.add(_decimal(record.get("conditional_markout_usd")))
            else:
                self.edge_excluded_by_fill_count += 1

    def add(self, record: dict[str, Any], *, track_version: bool = True) -> None:
        """Backward-compatible raw-only alias used by older callers."""

        self.add_raw(record, track_version=track_version)


@dataclass(slots=True)
class _ModelStats:
    fill_count: int = 0
    filled_notional: Decimal = Decimal("0")
    qualifying_volume: Decimal = Decimal("0")
    qualifying_notional: Decimal = Decimal("0")
    threshold_volume: Decimal = Decimal("0")
    threshold_notional: Decimal = Decimal("0")
    time_to_fill_ms: _NumberStats = field(default_factory=_NumberStats)
    horizons: dict[int, _HorizonStats] = field(
        default_factory=lambda: {horizon: _HorizonStats() for horizon in _HORIZONS}
    )
    fill_version_ids: set[str] = field(default_factory=set)
    fill_version_id_capacity_exceeded: bool = False
    valid_fill_count: int = 0
    valid_filled_notional: Decimal = Decimal("0")
    valid_threshold_volume: Decimal = Decimal("0")
    valid_threshold_notional: Decimal = Decimal("0")
    valid_time_to_fill_ms: _NumberStats = field(default_factory=_NumberStats)
    raw_detection_timestamps: set[int] = field(default_factory=set)
    valid_detection_timestamps: set[int] = field(default_factory=set)
    contaminated_fill_count: int = 0
    contaminated_detection_timestamps: set[int] = field(default_factory=set)
    contaminated_filled_notional: Decimal = Decimal("0")
    contaminated_threshold_volume: Decimal = Decimal("0")
    contaminated_threshold_notional: Decimal = Decimal("0")
    contaminated_time_to_fill_ms: _NumberStats = field(default_factory=_NumberStats)
    fill_contamination_reason_counts: dict[str, int] = field(
        default_factory=lambda: defaultdict(int)
    )

    def classify_fill(
        self,
        record: dict[str, Any],
        info: _QuoteInfo | None,
        reasons: set[str] | frozenset[str],
    ) -> None:
        """Add one raw fill to the valid or contaminated episode ledger."""

        quantity = (
            None if info is None else info.quantity
        ) or _decimal(record.get("canonical_quantity"))
        maker_price = None if info is None else info.maker_price
        if maker_price is None:
            maker_price = _decimal(record.get("maker_price"))
        filled_notional = (
            quantity * maker_price
            if quantity is not None and maker_price is not None
            else Decimal("0")
        )
        cumulative = _decimal(record.get("cumulative_eligible_quantity")) or Decimal("0")
        threshold_notional = (
            cumulative * maker_price
            if maker_price is not None
            else Decimal("0")
        )
        detected = _record_int(record, "would_fill_detected_monotonic_ns")
        created = None if info is None else info.created
        if created is None:
            created = _record_int(record, "quote_created_monotonic_ns")
        time_to_fill = (
            Decimal(max(0, detected - created)) / Decimal("1000000")
            if detected is not None and created is not None
            else None
        )
        if reasons:
            self.contaminated_fill_count += 1
            self.contaminated_filled_notional += filled_notional
            self.contaminated_threshold_volume += cumulative
            self.contaminated_threshold_notional += threshold_notional
            self.contaminated_time_to_fill_ms.add(time_to_fill)
            if detected is not None:
                self.contaminated_detection_timestamps.add(detected)
            for reason in sorted(reasons):
                self.fill_contamination_reason_counts[reason] += 1
            return
        self.valid_fill_count += 1
        self.valid_filled_notional += filled_notional
        self.valid_threshold_volume += cumulative
        self.valid_threshold_notional += threshold_notional
        self.valid_time_to_fill_ms.add(time_to_fill)
        if detected is not None:
            self.valid_detection_timestamps.add(detected)


@dataclass(slots=True)
class _PolicyStats:
    policy_id: str
    market: str = ""
    direction: str = ""
    target: str = ""
    margin: str = ""
    quote_count: int = 0
    quoteable_count: int = 0
    observed_start: int | None = None
    observed_end: int | None = None
    union_total: int = 0
    union_start: int | None = None
    union_end: int | None = None
    quote_lifetime_ms: _NumberStats = field(default_factory=_NumberStats)
    snapshot_edge: _NumberStats = field(default_factory=_NumberStats)
    distance_ticks: _NumberStats = field(default_factory=_NumberStats)
    distance_bps: _NumberStats = field(default_factory=_NumberStats)
    eligible_trade_count: int = 0
    touch_count: int = 0
    at_or_through_count: int = 0
    strict_price_through_count: int = 0
    strict: _ModelStats = field(default_factory=_ModelStats)
    optimistic: _ModelStats = field(default_factory=_ModelStats)

    def model(self, name: str) -> _ModelStats:
        return self.strict if name == _STRICT_MODEL else self.optimistic

    def set_identity(self, record: dict[str, Any]) -> None:
        self.market = self.market or _key_text(record.get("canonical_market"))
        self.direction = self.direction or _key_text(record.get("direction"))
        self.target = self.target or _key_text(record.get("target_notional_usd"))
        self.margin = self.margin or _key_text(record.get("target_margin_bps"))

    def add_interval(self, start: int, end: int) -> None:
        self.observed_start = start if self.observed_start is None else min(self.observed_start, start)
        self.observed_end = end if self.observed_end is None else max(self.observed_end, end)
        if end <= start:
            return
        if self.union_start is None:
            self.union_start, self.union_end = start, end
        elif start <= self.union_end:  # evidence is append-ordered by local time
            self.union_end = max(self.union_end, end)
        else:
            self.union_total += self.union_end - self.union_start
            self.union_start, self.union_end = start, end

    def finish_union(self) -> int:
        if self.union_start is None or self.union_end is None:
            return self.union_total
        return self.union_total + max(0, self.union_end - self.union_start)


@dataclass(slots=True)
class _QuoteInfo:
    policy_id: str
    version_id: str
    market: str
    direction: str
    created: int
    expiry: int | None
    stream_session: str | int | None
    recovery: int | None
    hedge_stream_session: str | int | None
    hedge_recovery: int | None
    maker_price: Decimal | None
    quantity: Decimal | None
    tick: Decimal | None
    post_only_bound: Decimal | None
    actual_edge: Decimal | None


@dataclass(slots=True)
class _EpisodeContext:
    model: str
    version_id: str
    policy_id: str
    market: str
    quote_created: int | None
    detected: int | None
    quote_info: _QuoteInfo | None = None
    fill_record: dict[str, Any] = field(default_factory=dict)
    risex_stream_session: str | int | None = None
    risex_recovery: int | None = None
    hedge_stream_session: str | int | None = None
    hedge_recovery: int | None = None
    horizons_seen: set[int] = field(default_factory=set)
    horizon_records: dict[int, dict[str, Any]] = field(default_factory=dict)
    fill_contamination_reasons: set[str] = field(default_factory=set)
    horizon_contamination_reasons: dict[int, set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )

    def horizon_interval(self, horizon: int) -> dict[str, Any]:
        return {
            "kind": "HEDGE_HORIZON",
            "canonical_market": self.market,
            "venue": "LIGHTER",
            "expected_stream_session_id": self.hedge_stream_session,
            "expected_recovery_generation": self.hedge_recovery,
            "would_fill_detected_monotonic_ns": self.detected,
            "horizon_deadline_monotonic_ns": None
            if self.detected is None
            else self.detected + horizon * 1_000_000,
        }


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _key_text(value: Any) -> str:
    return "" if value is None else str(value)


def _json_number(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _record_int(record: dict[str, Any], name: str) -> int | None:
    value = record.get(name)
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _record_interval(record: dict[str, Any]) -> tuple[int, int] | None:
    kind = record.get("kind")
    if kind == "QUOTE":
        start = _record_int(record, "quote_created_monotonic_ns")
        end = _record_int(record, "quote_expires_monotonic_ns")
        if start is None:
            return None
        return start, start if end is None else end
    if kind == "WOULD_FILL":
        detected = _record_int(record, "would_fill_detected_monotonic_ns")
        if detected is None:
            return None
        start = _record_int(record, "quote_created_monotonic_ns")
        return (detected if start is None else start), detected
    if kind == "HEDGE_HORIZON":
        start = _record_int(record, "would_fill_detected_monotonic_ns")
        end = _record_int(record, "horizon_deadline_monotonic_ns")
        if start is None or end is None or end < start:
            return None
        return start, end
    return None


def _record_venue(record: dict[str, Any], default: str | None = None) -> str | None:
    value = record.get("venue", default)
    text = _key_text(value).upper()
    return text or None


def _record_stream_identity(
    record: dict[str, Any], *, venue: str | None
) -> tuple[Any, int | None]:
    if venue == "LIGHTER":
        session = next(
            (
                record.get(name)
                for name in (
                    "expected_stream_session_id",
                    "book_stream_session_id",
                    "hedge_stream_session_id",
                )
                if record.get(name) is not None
            ),
            None,
        )
        recovery = next(
            (
                _record_int(record, name)
                for name in (
                    "expected_recovery_generation",
                    "book_recovery_generation",
                    "hedge_recovery_generation",
                )
                if record.get(name) is not None
            ),
            None,
        )
        return session, recovery
    return record.get("stream_session_id"), _record_int(record, "recovery_generation")


def _gap_contaminates(
    gap: dict[str, Any],
    record: dict[str, Any],
    *,
    default_venue: str | None = None,
) -> bool:
    if _key_text(gap.get("canonical_market")) != _key_text(
        record.get("canonical_market")
    ):
        return False
    record_venue = _record_venue(record, default_venue)
    gap_venue = _record_venue(gap)
    if record_venue is not None and gap_venue is not None and record_venue != gap_venue:
        return False
    record_session, record_recovery = _record_stream_identity(
        record, venue=record_venue
    )
    gap_session = gap.get("stream_session_id")
    if (
        gap_session is not None
        and gap_session != "unknown"
        and record_session is not None
        and gap_session != record_session
    ):
        return False
    gap_recovery = _record_int(gap, "recovery_generation")
    if (
        gap_recovery is not None
        and record_recovery is not None
        and gap_recovery != record_recovery
    ):
        return False
    interval = _record_interval(record)
    if interval is None:
        # Missing timestamps or identity cannot prove that the evidence is
        # clean; retain fail-closed behaviour for malformed/legacy records.
        return True
    if interval[1] < interval[0]:
        return True
    gap_start = _record_int(gap, "gap_start_monotonic_ns")
    if gap_start is None:
        return True
    if "gap_end_monotonic_ns" not in gap:
        return True
    if gap.get("gap_end_monotonic_ns") is None:
        # A null end is an open interval.  It only reaches evidence whose
        # interval has not ended before the gap begins; later matching
        # evidence remains contaminated until session/recovery identity
        # changes.
        return interval[1] >= gap_start
    gap_end = _record_int(gap, "gap_end_monotonic_ns")
    if gap_end is None or gap_end < gap_start:
        return True
    return not (gap_end < interval[0] or gap_start > interval[1])


def _embedded_horizon_gap(record: dict[str, Any]) -> dict[str, Any] | None:
    if record.get("outcome") != "HEDGE_DATA_GAP" or record.get("gap_reason") is None:
        return None
    return {
        "canonical_market": record.get("canonical_market"),
        "venue": record.get("gap_source_venue", record.get("venue")),
        "stream_session_id": record.get(
            "expected_stream_session_id", record.get("book_stream_session_id")
        ),
        "recovery_generation": record.get(
            "expected_recovery_generation", record.get("book_recovery_generation")
        ),
        "gap_start_monotonic_ns": record.get("gap_start_monotonic_ns"),
        "gap_end_monotonic_ns": record.get("gap_end_monotonic_ns"),
        "reason": record.get("gap_reason"),
    }


def _record_model(record: dict[str, Any]) -> str:
    value = record.get("fillability_model") or record.get("model")
    return _OPTIMISTIC_MODEL if _key_text(value).upper() == _OPTIMISTIC_MODEL else _STRICT_MODEL


def _validated_records(path: str | Path) -> Iterator[dict[str, Any]]:
    """Validate physical append identity and terminal placement while streaming."""

    previous_index: int | None = None
    terminal_kind: str | None = None
    for record in iter_records(path):
        value = record.get("record_index")
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise EvidenceIntegrityError("INVALID_RECORD_INDEX")
        if previous_index is None:
            if value != 0:
                raise EvidenceIntegrityError("MISSING_RECORD_INDEX")
        elif value == previous_index:
            raise EvidenceIntegrityError("DUPLICATE_RECORD_INDEX")
        elif value < previous_index:
            raise EvidenceIntegrityError("DECREASING_RECORD_INDEX")
        elif value != previous_index + 1:
            raise EvidenceIntegrityError(
                "MISSING_RECORD_INDEX / NON_CONTIGUOUS_RECORD_INDEX"
            )

        kind = record.get("kind")
        if terminal_kind is not None:
            if _is_terminal_kind(kind):
                raise EvidenceIntegrityError("MULTIPLE_TERMINAL_MARKERS")
            raise EvidenceIntegrityError(
                "RECORD_AFTER_TERMINAL / TERMINAL_MARKER_NOT_LAST"
            )
        if _is_terminal_kind(kind):
            terminal_kind = kind
        previous_index = value
        yield record
    if terminal_kind is None:
        raise EvidenceIntegrityError("MISSING_TERMINAL_MARKER")


def _reference_fields_present(record: dict[str, Any], prefix: str) -> bool:
    return any(
        record.get(name) is not None
        for name in (
            f"{prefix}_revision",
            f"{prefix}_revision_id",
            f"{prefix}_stream_session_id",
            f"{prefix}_recovery_generation",
        )
    )


def _validate_book_reference(
    reconstructor: BookRevisionReconstructor,
    record: dict[str, Any],
    *,
    prefix: str,
    venue: Venue,
    require_current: bool,
    allow_legacy_id: bool = False,
) -> tuple[tuple[Venue, str, str | int, int, int, str], str] | None:
    """Validate one optional exact book witness and return its replay key."""

    if not _reference_fields_present(record, prefix):
        return None
    revision_value = record.get(f"{prefix}_revision")
    revision = (
        revision_value
        if isinstance(revision_value, int) and not isinstance(revision_value, bool)
        else None
    )
    revision_id = record.get(f"{prefix}_revision_id")
    session = record.get(f"{prefix}_stream_session_id")
    recovery_value = record.get(f"{prefix}_recovery_generation")
    recovery = (
        recovery_value
        if isinstance(recovery_value, int) and not isinstance(recovery_value, bool)
        else None
    )
    market = record.get("canonical_market")
    if (
        revision is None
        or revision < 0
        or isinstance(session, bool)
        or not isinstance(session, (str, int))
        or session == ""
        or recovery is None
        or recovery < 0
        or not isinstance(market, str)
        or not market
    ):
        raise EvidenceIntegrityError("BOOK_REFERENCE_FIELDS_INVALID")
    if revision_id is None:
        if not allow_legacy_id:
            raise EvidenceIntegrityError("BOOK_REFERENCE_FIELDS_INVALID")
        revision_id = make_book_revision_id(
            venue,
            market,
            session,
            recovery,
            revision,
        )
    elif not isinstance(revision_id, str) or not revision_id:
        raise EvidenceIntegrityError("BOOK_REFERENCE_FIELDS_INVALID")
    try:
        digest = reconstructor.validate_reference(
            venue=venue,
            market=market,
            session=session,
            recovery=recovery,
            revision=revision,
            revision_id=revision_id,
            require_current=require_current,
        )
    except BookRevisionChainError as exc:
        raise EvidenceIntegrityError(exc.reason) from exc
    supplied_digest = record.get(f"{prefix}_state_sha256")
    if supplied_digest is not None:
        if (
            not isinstance(supplied_digest, str)
            or len(supplied_digest) != 64
            or any(character not in "0123456789abcdef" for character in supplied_digest)
        ):
            raise EvidenceIntegrityError("BOOK_REFERENCE_DIGEST_INVALID")
        if require_current and supplied_digest != digest:
            raise EvidenceIntegrityError("BOOK_REFERENCE_DIGEST_MISMATCH")
    return (
        venue,
        market,
        session,
        recovery,
        revision,
        revision_id,
    ), (supplied_digest or "")


def _audit_book_evidence(path: str | Path) -> BookAuditResult:
    """Replay BOOK chains and verify all new exact calculation witnesses.

    The first pass retains only one reconstructed state per identity chain.
    Horizon references are bounded by the prospective episode cap and are
    checked in a second streaming pass so an older referenced book never
    requires retaining every historical full book in memory.
    """

    reconstructor = BookRevisionReconstructor()
    horizon_requests: dict[
        tuple[Venue, str, str | int, int, int, str], str
    ] = {}
    for record in _validated_records(path):
        kind = record.get("kind")
        if kind == "BOOK":
            try:
                reconstructor.append(record)
            except BookRevisionChainError as exc:
                raise EvidenceIntegrityError(exc.reason) from exc
        elif kind == "DATA_GAP":
            try:
                venue = Venue(record.get("venue"))
            except (TypeError, ValueError):
                venue = None
            session = record.get("stream_session_id")
            recovery = _record_int(record, "recovery_generation")
            market = _key_text(record.get("canonical_market"))
            if (
                venue in (Venue.RISEX, Venue.LIGHTER)
                and isinstance(session, (str, int))
                and not isinstance(session, bool)
                and session != ""
                and recovery is not None
                and market
            ):
                reconstructor.mark_gap(
                    venue=venue,
                    market=market,
                    session=session,
                    recovery=recovery,
                )
        elif kind == "QUOTE":
            # Quote construction is bound to the current pair of books.  A
            # new quote may not point behind the current chain tip.
            risex_reference = _validate_book_reference(
                reconstructor,
                record,
                prefix="risex_book",
                venue=Venue.RISEX,
                require_current=True,
            )
            lighter_reference = _validate_book_reference(
                reconstructor,
                record,
                prefix="lighter_book",
                venue=Venue.LIGHTER,
                require_current=True,
            )
            if (risex_reference is None) != (lighter_reference is None):
                raise EvidenceIntegrityError("QUOTE_BOOK_REFERENCE_PAIR_INCOMPLETE")
            if risex_reference is not None and lighter_reference is not None:
                risex_key = risex_reference[0]
                lighter_key = lighter_reference[0]
                expected_risex_session = record.get(
                    "risex_book_stream_session_id",
                    record.get("quote_stream_session_id", record.get("stream_session_id")),
                )
                expected_risex_recovery = _record_int(
                    record,
                    "risex_book_recovery_generation",
                )
                if expected_risex_recovery is None:
                    expected_risex_recovery = _record_int(
                        record,
                        "quote_recovery_generation",
                    )
                expected_lighter_session = record.get(
                    "lighter_book_stream_session_id",
                    record.get("hedge_stream_session_id"),
                )
                expected_lighter_recovery = _record_int(
                    record,
                    "lighter_book_recovery_generation",
                )
                if expected_lighter_recovery is None:
                    expected_lighter_recovery = _record_int(
                        record,
                        "hedge_recovery_generation",
                    )
                if (
                    expected_risex_session is not None
                    and risex_key[2] != expected_risex_session
                ) or (
                    expected_risex_recovery is not None
                    and risex_key[3] != expected_risex_recovery
                ) or (
                    expected_lighter_session is not None
                    and lighter_key[2] != expected_lighter_session
                ) or (
                    expected_lighter_recovery is not None
                    and lighter_key[3] != expected_lighter_recovery
                ):
                    raise EvidenceIntegrityError("QUOTE_BOOK_REFERENCE_IDENTITY_MISMATCH")
        elif kind == "HEDGE_HORIZON":
            prefix = "book"
            book_reference_present = _reference_fields_present(record, prefix)
            lighter_reference_present = _reference_fields_present(record, "lighter_book")
            if book_reference_present and lighter_reference_present:
                raise EvidenceIntegrityError("HORIZON_BOOK_REFERENCE_AMBIGUOUS")
            if not book_reference_present and lighter_reference_present:
                prefix = "lighter_book"
            result = _validate_book_reference(
                reconstructor,
                record,
                prefix=prefix,
                venue=Venue.LIGHTER,
                require_current=False,
                allow_legacy_id=True,
            )
            if result is not None:
                key, digest = result
                # A SESSION_DISPLACED outcome intentionally retains the
                # latest book from the wrong stream as diagnostic evidence;
                # its selected-book identity must not equal the expected
                # stream.  Other selected-book outcomes must remain bound to
                # the expected Lighter identity.
                if record.get("outcome") != "HEDGE_SESSION_DISPLACED":
                    expected_session = record.get("expected_stream_session_id")
                    if expected_session is None:
                        expected_session = record.get("book_stream_session_id")
                    expected_recovery = _record_int(
                        record,
                        "expected_recovery_generation",
                    )
                    if expected_recovery is None:
                        expected_recovery = _record_int(
                            record,
                            "book_recovery_generation",
                        )
                    if (
                        expected_session is not None
                        and key[2] != expected_session
                    ) or (
                        expected_recovery is not None
                        and key[3] != expected_recovery
                    ):
                        raise EvidenceIntegrityError("HORIZON_BOOK_REFERENCE_IDENTITY_MISMATCH")
                if len(horizon_requests) >= _MAX_BOOK_REFERENCE_REQUESTS and key not in horizon_requests:
                    raise EvidenceIntegrityError("BOOK_REFERENCE_CAPACITY")
                previous = horizon_requests.get(key)
                if previous is not None and previous != digest:
                    raise EvidenceIntegrityError("BOOK_REFERENCE_DIGEST_CONFLICT")
                horizon_requests[key] = digest

    if horizon_requests:
        replay = BookRevisionReconstructor()
        remaining = set(horizon_requests)
        for record in _validated_records(path):
            if record.get("kind") == "DATA_GAP":
                try:
                    venue = Venue(record.get("venue"))
                except (TypeError, ValueError):
                    venue = None
                session = record.get("stream_session_id")
                recovery = _record_int(record, "recovery_generation")
                market = _key_text(record.get("canonical_market"))
                if (
                    venue in (Venue.RISEX, Venue.LIGHTER)
                    and isinstance(session, (str, int))
                    and not isinstance(session, bool)
                    and session != ""
                    and recovery is not None
                    and market
                ):
                    replay.mark_gap(
                        venue=venue,
                        market=market,
                        session=session,
                        recovery=recovery,
                    )
                continue
            if record.get("kind") != "BOOK":
                continue
            try:
                book = replay.append(record)
            except BookRevisionChainError as exc:
                raise EvidenceIntegrityError(exc.reason) from exc
            key = (
                book.venue,
                book.canonical_market,
                book.stream_session_id,
                book.recovery_generation,
                book.book_revision,
                book.book_revision_id,
            )
            if key not in remaining:
                continue
            expected_digest = horizon_requests[key]
            if expected_digest:
                if expected_digest != book_state_sha256(book):
                    raise EvidenceIntegrityError("BOOK_REFERENCE_DIGEST_MISMATCH")
            remaining.remove(key)
        if remaining:
            raise EvidenceIntegrityError("BOOK_REFERENCE_NOT_REPLAYED")
    return reconstructor.audit()


def _record_decimal(record: dict[str, Any], *names: str) -> Decimal | None:
    for name in names:
        value = _decimal(record.get(name))
        if value is not None:
            return value
    return None


def _quote_info_from_record(
    record: dict[str, Any],
    *,
    policy_id: str | None = None,
) -> _QuoteInfo | None:
    version_id = record.get("quote_version_id")
    created = _record_int(record, "quote_created_monotonic_ns")
    if version_id is None or created is None:
        return None
    resolved_policy = policy_id or _key_text(record.get("policy_id"))
    if not resolved_policy:
        return None
    return _QuoteInfo(
        policy_id=resolved_policy,
        version_id=_key_text(version_id),
        market=_key_text(record.get("canonical_market")),
        direction=_key_text(record.get("direction")),
        created=created,
        expiry=_record_int(record, "quote_expires_monotonic_ns"),
        stream_session=record.get("quote_stream_session_id", record.get("stream_session_id")),
        recovery=_record_int(record, "quote_recovery_generation"),
        hedge_stream_session=record.get("hedge_stream_session_id"),
        hedge_recovery=_record_int(record, "hedge_recovery_generation"),
        maker_price=_record_decimal(record, "maker_price"),
        quantity=_record_decimal(record, "canonical_quantity", "quote_canonical_quantity"),
        tick=_record_decimal(record, "risex_tick_size"),
        post_only_bound=_record_decimal(record, "post_only_bound_price"),
        actual_edge=_record_decimal(record, "actual_edge_usd"),
    )


def _fill_quote_info(
    fill: dict[str, Any],
    quote: _QuoteInfo | None,
    *,
    policy_id: str,
) -> _QuoteInfo | None:
    merged: dict[str, Any] = {}
    if quote is not None:
        merged.update(
            {
                "quote_version_id": quote.version_id,
                "quote_created_monotonic_ns": quote.created,
                "quote_expires_monotonic_ns": quote.expiry,
                "quote_stream_session_id": quote.stream_session,
                "quote_recovery_generation": quote.recovery,
                "hedge_stream_session_id": quote.hedge_stream_session,
                "hedge_recovery_generation": quote.hedge_recovery,
                "canonical_market": quote.market,
                "direction": quote.direction,
                "maker_price": quote.maker_price,
                "canonical_quantity": quote.quantity,
                "risex_tick_size": quote.tick,
                "post_only_bound_price": quote.post_only_bound,
                "actual_edge_usd": quote.actual_edge,
            }
        )
    merged.update(fill)
    if merged.get("quote_created_monotonic_ns") is None and quote is not None:
        merged["quote_created_monotonic_ns"] = quote.created
    return _quote_info_from_record(merged, policy_id=policy_id)


def _trade_time(record: dict[str, Any]) -> int | None:
    for name in ("received_monotonic_ns", "observed_monotonic_ns"):
        value = _record_int(record, name)
        if value is not None:
            return value
    return None


def _tick_aligned(price: Decimal | None, tick: Decimal | None) -> bool:
    if price is None or tick is None or tick <= 0:
        return False
    return price % tick == 0


def _info_trade_eligible(info: _QuoteInfo, trade: dict[str, Any]) -> bool:
    received = _trade_time(trade)
    if received is None or received <= info.created:
        return False
    if info.expiry is not None and received >= info.expiry:
        return False
    if _key_text(trade.get("canonical_market")) != info.market:
        return False
    if _record_venue(trade) not in (None, "RISEX"):
        return False
    if info.stream_session is None or trade.get("stream_session_id") != info.stream_session:
        return False
    if info.recovery is None or _record_int(trade, "recovery_generation") != info.recovery:
        return False
    expected = "SELL" if info.direction == "RISEX_BUY_LIGHTER_SELL" else "BUY"
    return _key_text(trade.get("aggressor_side")).upper() == expected


def _at_or_through(info: _QuoteInfo, price: Decimal | None) -> bool:
    if price is None or info.maker_price is None:
        return False
    if info.direction == "RISEX_BUY_LIGHTER_SELL":
        return price <= info.maker_price
    return price >= info.maker_price


def _strict_price_through(info: _QuoteInfo, price: Decimal | None) -> bool:
    if not _tick_aligned(price, info.tick) or price is None or info.maker_price is None or info.tick is None:
        return False
    if info.direction == "RISEX_BUY_LIGHTER_SELL":
        return price <= info.maker_price - info.tick
    return price >= info.maker_price + info.tick


def _stats_payload(stats: _NumberStats) -> dict[str, Any]:
    return {
        "count": stats.count,
        "mean": _json_number(stats.mean()),
        "median": _json_number(stats.median()),
        "p05": _json_number(stats.percentile(Decimal("0.05"))),
        "minimum": _json_number(stats.minimum()),
        "maximum": _json_number(stats.maximum()),
    }


def _rate(count: int, denominator: int) -> str | None:
    return _json_number(Decimal(count) / Decimal(denominator)) if denominator else None


def _model_payload(
    model_name: str,
    stats: _ModelStats,
    horizon: _HorizonStats,
    *,
    completeness: str,
    implemented: bool,
) -> dict[str, Any]:
    outcome_counts = {key: horizon.outcomes[key] for key in sorted(horizon.outcomes)}
    valid_outcome_counts = {
        key: horizon.valid_outcomes[key] for key in sorted(horizon.valid_outcomes)
    }
    contaminated_outcome_counts = {
        key: horizon.contaminated_outcomes[key]
        for key in sorted(horizon.contaminated_outcomes)
    }

    def missing_count(outcomes: dict[str, int]) -> int:
        return sum(
            outcomes.get(name, 0)
            for name in (
                "HEDGE_DEPTH_UNAVAILABLE",
                "HEDGE_DATA_MISSING",
                "HEDGE_DATA_STALE",
                "HEDGE_SESSION_DISPLACED",
                "HEDGE_DATA_GAP",
                "HEDGE_OUTCOME_UNKNOWN",
            )
        )

    raw_full = horizon.outcomes.get("HEDGE_FULL", 0)
    valid_full = horizon.valid_outcomes.get("HEDGE_FULL", 0)
    contaminated_full = horizon.contaminated_outcomes.get("HEDGE_FULL", 0)
    raw_partial = horizon.outcomes.get("HEDGE_PARTIAL", 0)
    valid_partial = horizon.valid_outcomes.get("HEDGE_PARTIAL", 0)
    contaminated_partial = horizon.contaminated_outcomes.get("HEDGE_PARTIAL", 0)
    raw_missing = missing_count(horizon.outcomes)
    valid_missing = missing_count(horizon.valid_outcomes)
    contaminated_missing = missing_count(horizon.contaminated_outcomes)
    return {
        "model": model_name,
        "implemented": implemented,
        "would_fill_count": stats.fill_count,
        "fill_count": stats.fill_count,
        "raw_would_fill_count": stats.fill_count,
        "valid_would_fill_count": stats.valid_fill_count,
        "contaminated_would_fill_count": stats.contaminated_fill_count,
        "filled_notional_usd": _json_number(stats.filled_notional),
        "raw_filled_notional_usd": _json_number(stats.filled_notional),
        "valid_filled_notional_usd": _json_number(stats.valid_filled_notional),
        "contaminated_filled_notional_usd": _json_number(
            stats.contaminated_filled_notional
        ),
        "cumulative_qualifying_volume": _json_number(stats.qualifying_volume),
        "cumulative_qualifying_notional_usd": _json_number(stats.qualifying_notional),
        "threshold_qualifying_volume": _json_number(stats.threshold_volume),
        "threshold_qualifying_notional_usd": _json_number(stats.threshold_notional),
        "valid_threshold_qualifying_volume": _json_number(
            stats.valid_threshold_volume
        ),
        "valid_threshold_qualifying_notional_usd": _json_number(
            stats.valid_threshold_notional
        ),
        "contaminated_threshold_qualifying_volume": _json_number(
            stats.contaminated_threshold_volume
        ),
        "contaminated_threshold_qualifying_notional_usd": _json_number(
            stats.contaminated_threshold_notional
        ),
        "time_to_fill_ms": _stats_payload(stats.time_to_fill_ms),
        "raw_time_to_fill_ms": _stats_payload(stats.time_to_fill_ms),
        "valid_time_to_fill_ms": _stats_payload(stats.valid_time_to_fill_ms),
        "contaminated_time_to_fill_ms": _stats_payload(
            stats.contaminated_time_to_fill_ms
        ),
        "raw_detection_timestamp_count": len(stats.raw_detection_timestamps),
        "valid_detection_timestamp_count": len(stats.valid_detection_timestamps),
        "contaminated_detection_timestamp_count": len(
            stats.contaminated_detection_timestamps
        ),
        "fill_contamination_reason_counts": {
            key: stats.fill_contamination_reason_counts[key]
            for key in sorted(stats.fill_contamination_reason_counts)
        },
        "horizon": {
            "observation_count": horizon.observation_count,
            "raw_observation_count": horizon.observation_count,
            "valid_observation_count": horizon.valid_observation_count,
            "contaminated_observation_count": horizon.contaminated_observation_count,
            "outcome_counts": outcome_counts,
            "raw_outcome_counts": outcome_counts,
            "valid_outcome_counts": valid_outcome_counts,
            "contaminated_outcome_counts": contaminated_outcome_counts,
            "full_hedge_rate": _rate(valid_full, horizon.valid_observation_count),
            "raw_full_hedge_rate": _rate(raw_full, horizon.observation_count),
            "valid_full_hedge_rate": _rate(valid_full, horizon.valid_observation_count),
            "contaminated_full_hedge_rate": _rate(
                contaminated_full, horizon.contaminated_observation_count
            ),
            "partial_hedge_rate": _rate(valid_partial, horizon.valid_observation_count),
            "raw_partial_hedge_rate": _rate(raw_partial, horizon.observation_count),
            "contaminated_partial_hedge_rate": _rate(
                contaminated_partial, horizon.contaminated_observation_count
            ),
            "missing_hedge_rate": _rate(valid_missing, horizon.valid_observation_count),
            "raw_missing_hedge_rate": _rate(raw_missing, horizon.observation_count),
            "contaminated_missing_hedge_rate": _rate(
                contaminated_missing, horizon.contaminated_observation_count
            ),
            "partial_or_missing_rate": _rate(
                valid_partial + valid_missing, horizon.valid_observation_count
            ),
            "raw_partial_or_missing_rate": _rate(
                raw_partial + raw_missing, horizon.observation_count
            ),
            "contaminated_partial_or_missing_rate": _rate(
                contaminated_partial + contaminated_missing,
                horizon.contaminated_observation_count,
            ),
            "filled_quantity": _stats_payload(horizon.valid_filled_quantity),
            "raw_filled_quantity": _stats_payload(horizon.filled_quantity),
            "contaminated_filled_quantity": _stats_payload(
                horizon.contaminated_filled_quantity
            ),
            "notional_usd": _stats_payload(horizon.valid_notional),
            "raw_notional_usd": _stats_payload(horizon.notional),
            "contaminated_notional_usd": _stats_payload(horizon.contaminated_notional),
            "entry_edge_usd": _stats_payload(horizon.entry_edge),
            "raw_entry_edge_usd": _stats_payload(horizon.raw_entry_edge),
            "contaminated_entry_edge_usd": _stats_payload(
                horizon.contaminated_entry_edge
            ),
            "conditional_markout_usd": _stats_payload(horizon.markout),
            "raw_conditional_markout_usd": _stats_payload(horizon.raw_markout),
            "contaminated_conditional_markout_usd": _stats_payload(
                horizon.contaminated_markout
            ),
            "valid_edge_count": horizon.entry_edge.count,
            "raw_edge_count": horizon.raw_entry_edge.count,
            "contaminated_edge_count": horizon.contaminated_entry_edge.count,
            "edge_excluded_by_fill_count": horizon.edge_excluded_by_fill_count,
            "gap_reasons": sorted(horizon.gap_reasons),
            "contamination_reason_counts": {
                key: horizon.contamination_reason_counts[key]
                for key in sorted(horizon.contamination_reason_counts)
            },
            "missing_expected_count": horizon.missing_expected_count,
            "data_completeness": completeness,
        },
    }


def build_report(path: str | Path) -> dict[str, Any]:
    book_audit = _audit_book_evidence(path)
    metadata: dict[str, Any] = {}
    mode = "OBSERVATIONAL"
    record_count = 0
    gap_count = 0
    horizon_record_count = 0
    failed_run = False
    clean_stop_count = 0
    optimistic_supported = False
    markets: set[str] = set()
    policies: dict[str, _PolicyStats] = {}
    active_by_policy: dict[str, _QuoteInfo] = {}
    active_by_version: dict[str, _QuoteInfo] = {}
    episodes: dict[tuple[str, str], _EpisodeContext] = {}
    completed_episodes: dict[tuple[str, str], _EpisodeContext] = {}
    deferred_horizons: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    episode_context_truncated = False
    recent_gaps: deque[dict[str, Any]] = deque(maxlen=_RECENT_GAP_CAP)
    recent_gap_truncated = False
    pending_stop_gaps: deque[dict[str, Any]] = deque(maxlen=_RECENT_GAP_CAP)
    gap_count_by_market: dict[str, int] = defaultdict(int)
    seen_trade_keys: set[str] = set()
    trade_key_order: deque[str] = deque(maxlen=_RECENT_TRADE_KEY_CAP)
    first_sample_stop: dict[str, Any] | None = None
    replay_seen = False
    transport_event_counts: dict[str, int] = defaultdict(int)
    transport_failure_class_counts: dict[str, int] = defaultdict(int)
    transport_exception_type_counts: dict[str, int] = defaultdict(int)
    unexpected_transport_failure = False

    def policy_for(policy_id: str | None, record: dict[str, Any]) -> _PolicyStats | None:
        if policy_id is None or not policy_id:
            return None
        policy = policies.get(policy_id)
        if policy is None:
            policy = _PolicyStats(policy_id)
            policies[policy_id] = policy
        policy.set_identity(record)
        return policy

    def quote_for_version(version_id: str | None) -> _QuoteInfo | None:
        if version_id is None:
            return None
        resolved = _key_text(version_id)
        info = active_by_version.get(resolved)
        if info is not None:
            return info
        for candidate in active_by_policy.values():
            if candidate.version_id == resolved:
                return candidate
        return None

    def mark_gap(gap: dict[str, Any]) -> None:
        for episode in episodes.values():
            mark_gap_for_episode(gap, episode)
        for episode in completed_episodes.values():
            mark_gap_for_completed_episode(gap, episode)

    def process_quote(record: dict[str, Any]) -> None:
        policy_id = _key_text(record.get("policy_id"))
        policy = policy_for(policy_id, record)
        if policy is None:
            return
        policy.quote_count += 1
        created = _record_int(record, "quote_created_monotonic_ns")
        expiry = _record_int(record, "quote_expires_monotonic_ns")
        if created is not None:
            policy.observed_start = created if policy.observed_start is None else min(policy.observed_start, created)
            end_for_observation = expiry if expiry is not None else created
            policy.observed_end = end_for_observation if policy.observed_end is None else max(policy.observed_end, end_for_observation)
        if record.get("outcome") != "QUOTE_ACTIVE" or created is None:
            old = active_by_policy.pop(policy_id, None)
            if old is not None:
                active_by_version.pop(old.version_id, None)
            return
        policy.quoteable_count += 1
        end = expiry
        if end is None:
            lifetime = _record_int(record, "quote_lifetime_ns")
            end = None if lifetime is None else created + lifetime
        if end is not None:
            policy.add_interval(created, end)
            policy.quote_lifetime_ms.add(Decimal(max(0, end - created)) / Decimal("1000000"))
        tick = _decimal(record.get("risex_tick_size"))
        bound = _decimal(record.get("post_only_bound_price"))
        maker = _decimal(record.get("maker_price"))
        if tick is not None and tick > 0 and bound is not None and maker is not None:
            policy.distance_ticks.add(abs(bound - maker) / tick)
            if maker != 0:
                policy.distance_bps.add(abs(bound - maker) / abs(maker) * Decimal("10000"))
        policy.snapshot_edge.add(_decimal(record.get("actual_edge_usd")))
        info = _quote_info_from_record(record, policy_id=policy_id)
        if info is None:
            return
        old = active_by_policy.get(policy_id)
        if old is not None:
            active_by_version.pop(old.version_id, None)
        active_by_policy[policy_id] = info
        active_by_version[info.version_id] = info

    def process_trade(record: dict[str, Any]) -> None:
        key_value = record.get("trade_event_key")
        key = (
            f"{_key_text(record.get('canonical_market'))}\x00{_key_text(key_value)}"
            if key_value is not None
            else ""
        )
        if key and key in seen_trade_keys:
            return
        if key:
            if len(seen_trade_keys) >= _RECENT_TRADE_KEY_CAP:
                expired = trade_key_order.popleft()
                seen_trade_keys.discard(expired)
            seen_trade_keys.add(key)
            trade_key_order.append(key)
        explicit_ids = record.get("eligible_policy_ids")
        if isinstance(explicit_ids, (tuple, list)):
            candidate_infos = tuple(
                active_by_policy[policy_id]
                for policy_id in sorted({_key_text(value) for value in explicit_ids})
                if policy_id in active_by_policy
            )
        else:
            candidate_infos = tuple(active_by_policy.values())
        eligible_infos = tuple(
            info for info in candidate_infos if _info_trade_eligible(info, record)
        )
        explicit_eligible = record.get("eligible_trade")
        eligible = (
            explicit_eligible
            if isinstance(explicit_eligible, bool)
            else bool(eligible_infos)
        )
        if eligible:
            root_counts["eligible_trade_count"] += 1
        price = _decimal(record.get("canonical_price"))
        quantity = _decimal(record.get("canonical_quantity"))
        for info in eligible_infos:
            policy = policy_for(info.policy_id, record)
            if policy is None:
                continue
            policy.set_identity(
                {
                    "canonical_market": info.market,
                    "direction": info.direction,
                }
            )
            policy.eligible_trade_count += 1
            at_or_through = _at_or_through(info, price)
            if price is not None and info.maker_price is not None and price == info.maker_price:
                policy.touch_count += 1
            if at_or_through:
                policy.at_or_through_count += 1
                if quantity is not None:
                    policy.optimistic.qualifying_volume += quantity
                    if price is not None:
                        policy.optimistic.qualifying_notional += quantity * price
            strict_through = _strict_price_through(info, price)
            if strict_through:
                policy.strict_price_through_count += 1
                if quantity is not None:
                    policy.strict.qualifying_volume += quantity
                    if price is not None:
                        policy.strict.qualifying_notional += quantity * price

    def process_fill(record: dict[str, Any]) -> bool:
        nonlocal optimistic_supported, episode_context_truncated
        model = _record_model(record)
        if model == _OPTIMISTIC_MODEL:
            optimistic_supported = True
        version_id = record.get("quote_version_id")
        version_text = _key_text(version_id) if version_id is not None else ""
        info = quote_for_version(version_id)
        policy_id = _key_text(record.get("policy_id")) or (None if info is None else info.policy_id)
        policy = policy_for(policy_id, record)
        if policy is None:
            return False
        if info is None:
            info = _fill_quote_info(record, None, policy_id=policy_id)
        if version_text and (model, version_text) in completed_episodes:
            return False
        if version_text and version_text in policy.model(model).fill_version_ids:
            return False
        if version_text:
            if len(policy.model(model).fill_version_ids) < _POLICY_FILL_VERSION_CAP:
                policy.model(model).fill_version_ids.add(version_text)
            else:
                policy.model(model).fill_version_id_capacity_exceeded = True
        stats = policy.model(model)
        stats.fill_count += 1
        if info is not None:
            quantity = info.quantity
            maker_price = info.maker_price
            if quantity is not None and maker_price is not None:
                stats.filled_notional += quantity * maker_price
        cumulative = _decimal(record.get("cumulative_eligible_quantity"))
        if cumulative is not None:
            stats.threshold_volume += cumulative
            if info is not None and info.maker_price is not None:
                stats.threshold_notional += cumulative * info.maker_price
        detected = _record_int(record, "would_fill_detected_monotonic_ns")
        if detected is not None:
            stats.raw_detection_timestamps.add(detected)
        if info is not None and detected is not None:
            stats.time_to_fill_ms.add(Decimal(max(0, detected - info.created)) / Decimal("1000000"))
        if not version_text:
            stats.contaminated_fill_count += 1
            stats.fill_contamination_reason_counts["EPISODE_CONTEXT_MISSING"] += 1
            return True
        if len(episodes) + len(completed_episodes) >= _EPISODE_CONTEXT_CAP:
            episode_context_truncated = True
            stats.contaminated_fill_count += 1
            stats.fill_contamination_reason_counts["EPISODE_CONTEXT_CAPACITY"] += 1
            return True
        if info is None:
            created = _record_int(record, "quote_created_monotonic_ns")
            market = _key_text(record.get("canonical_market"))
        else:
            created = info.created
            market = info.market
        episode = _EpisodeContext(
            model=model,
            version_id=version_text,
            policy_id=policy_id,
            market=market,
            quote_created=created,
            detected=detected,
            quote_info=info,
            fill_record=dict(record),
            risex_stream_session=None if info is None else info.stream_session,
            risex_recovery=None if info is None else info.recovery,
            hedge_stream_session=(
                record.get("hedge_stream_session_id")
                if record.get("hedge_stream_session_id") is not None
                else (None if info is None else info.hedge_stream_session)
            ),
            hedge_recovery=(
                _record_int(record, "hedge_recovery_generation")
                if record.get("hedge_recovery_generation") is not None
                else (None if info is None else info.hedge_recovery)
            ),
        )
        episodes[(model, version_text)] = episode
        for gap in recent_gaps:
            mark_gap_for_episode(gap, episode)
        deferred = deferred_horizons.pop((model, version_text), ())
        for horizon_record in deferred:
            attach_horizon_record(horizon_record, episode)
        return True

    def mark_gap_for_episode(gap: dict[str, Any], episode: _EpisodeContext) -> None:
        fill_record = {
            "kind": "WOULD_FILL",
            "canonical_market": episode.market,
            "venue": "RISEX",
            "quote_created_monotonic_ns": episode.quote_created,
            "would_fill_detected_monotonic_ns": episode.detected,
            "stream_session_id": episode.risex_stream_session,
            "recovery_generation": episode.risex_recovery,
        }
        gap_venue = _record_venue(gap)
        if gap_venue == "RISEX" and _gap_contaminates(gap, fill_record):
            episode.fill_contamination_reasons.add(
                _key_text(gap.get("reason")) or "RISEX_GAP_OVERLAP"
            )
            return
        if gap_venue != "LIGHTER":
            return
        for horizon in _HORIZONS:
            if _gap_contaminates(gap, episode.horizon_interval(horizon)):
                episode.horizon_contamination_reasons[horizon].add(
                    _key_text(gap.get("reason")) or "LIGHTER_GAP_OVERLAP"
                )

    def mark_gap_for_completed_episode(
        gap: dict[str, Any], episode: _EpisodeContext
    ) -> None:
        mark_gap_for_episode(gap, episode)

    def retire_episode(key: tuple[str, str], episode: _EpisodeContext) -> None:
        completed_episodes[key] = episode
        episodes.pop(key, None)

    def attach_horizon_record(
        record: dict[str, Any], episode: _EpisodeContext
    ) -> None:
        """Attach a raw horizon that may have preceded its fill in the store."""

        horizon = int(record["horizon_ms"])
        episode.horizons_seen.add(horizon)
        episode.horizon_records[horizon] = dict(record)
        embedded = _embedded_horizon_gap(record)
        if embedded is not None:
            embedded_reason = _key_text(embedded.get("reason")) or "HEDGE_DATA_GAP"
            if embedded_reason != "PUBLIC_SMOKE_STOPPED":
                episode.horizon_contamination_reasons[horizon].add(embedded_reason)
        elif record.get("outcome") == "HEDGE_DATA_GAP":
            episode.horizon_contamination_reasons[horizon].add("HEDGE_DATA_GAP")
        if record.get("outcome") == "HEDGE_OUTCOME_UNKNOWN":
            episode.horizon_contamination_reasons[horizon].add(
                "HEDGE_OUTCOME_UNKNOWN"
            )
        if len(episode.horizons_seen) == len(_HORIZONS):
            retire_episode((episode.model, episode.version_id), episode)

    def process_horizon(record: dict[str, Any]) -> None:
        nonlocal horizon_record_count, optimistic_supported
        horizon_record_count += 1
        model = _record_model(record)
        if model == _OPTIMISTIC_MODEL:
            optimistic_supported = True
        try:
            horizon = int(record.get("horizon_ms"))
        except (TypeError, ValueError):
            return
        if horizon not in _HORIZONS:
            return
        version_id = record.get("quote_version_id")
        version_text = _key_text(version_id) if version_id is not None else ""
        episode = (
            episodes.get((model, version_text))
            or completed_episodes.get((model, version_text))
            if version_text
            else None
        )
        info = quote_for_version(version_id)
        policy_id = _key_text(record.get("policy_id")) or (
            episode.policy_id if episode is not None else None
        ) or (None if info is None else info.policy_id)
        policy = policy_for(policy_id, record)
        if policy is None:
            return
        stats = policy.model(model)
        horizon_stats = stats.horizons[horizon]
        if version_text and version_text in horizon_stats.version_ids:
            return
        horizon_stats.add_raw(record)
        if episode is not None:
            attach_horizon_record(record, episode)
        elif version_text:
            # AppendOnlyEvidenceStore orders each batch by observed time, so a
            # fixture or replay may legitimately place a horizon before its
            # WOULD_FILL row. Keep the raw row deferred until exact fill
            # identity arrives; only an unresolved row is contaminated.
            deferred_horizons[(model, version_text)].append(dict(record))
        else:
            # A horizon without its exact fill context cannot be attributed to
            # a clean episode, even though the raw row remains retained.
            horizon_stats.contaminated = True
            horizon_stats.contamination_reason_counts["EPISODE_CONTEXT_MISSING"] += 1
            horizon_stats.contaminated_observation_count += 1
            horizon_stats.contaminated_outcomes[_key_text(record.get("outcome"))] += 1

    def process_gap(record: dict[str, Any]) -> None:
        nonlocal gap_count, recent_gap_truncated, unexpected_transport_failure
        gap_count += 1
        market = _key_text(record.get("canonical_market"))
        gap_count_by_market[market] += 1
        transport_event = _key_text(record.get("transport_event"))
        if not transport_event:
            transport_event = {
                "PUBLIC_SOCKET_GRACEFUL_CLOSE": "GRACEFUL_CLOSE",
                "PUBLIC_SOCKET_RECONNECTED": "RECONNECT",
                "PUBLIC_SOCKET_TRANSPORT_FAILURE": "UNEXPECTED_FAILURE",
            }.get(_key_text(record.get("reason")), "")
        if transport_event:
            transport_event_counts[transport_event] += 1
            if transport_event == "UNEXPECTED_FAILURE":
                unexpected_transport_failure = True
        failure_class = _key_text(record.get("transport_failure_class"))
        if failure_class:
            transport_failure_class_counts[failure_class] += 1
        exception_type = _key_text(record.get("transport_exception_type"))
        if exception_type:
            transport_exception_type_counts[exception_type] += 1
        if record.get("reason") == "PUBLIC_SMOKE_STOPPED":
            pending_stop_gaps.append(record)
        else:
            if len(recent_gaps) == _RECENT_GAP_CAP:
                recent_gap_truncated = True
            recent_gaps.append(record)
            mark_gap(record)

    root_counts = {
        "eligible_trade_count": 0,
        "strict_episode_count": 0,
        "optimistic_episode_count": 0,
    }
    for record in _validated_records(path):
        record_count += 1
        kind = record.get("kind")
        market = record.get("canonical_market")
        if market is not None and len(markets) < 64:
            markets.add(_key_text(market))
        if kind == "RUN_METADATA":
            candidate = record.get("metadata")
            if isinstance(candidate, dict):
                metadata = candidate
        elif kind == "REPLAY_MODE":
            mode = "FIXTURE"
            replay_seen = True
        elif kind == "RUN_FAILED":
            failed_run = True
        elif kind == "RUN_STOP" and record.get("fatal_reason") in (None, ""):
            clean_stop_count += 1
        elif kind == "SAMPLE_STOP" and first_sample_stop is None:
            first_sample_stop = record
        elif kind == "QUOTE":
            process_quote(record)
        elif kind == "RISEX_TRADE":
            process_trade(record)
        elif kind == "WOULD_FILL":
            counted = process_fill(record)
            if not counted:
                continue
            model = _record_model(record)
            if model == _STRICT_MODEL:
                root_counts["strict_episode_count"] += 1
            else:
                root_counts["optimistic_episode_count"] += 1
        elif kind == "HEDGE_HORIZON":
            process_horizon(record)
        elif kind == "DATA_GAP":
            process_gap(record)

    ordinary_duration_completion = clean_stop_count == 1 and not failed_run
    if not ordinary_duration_completion:
        for gap in pending_stop_gaps:
            mark_gap(gap)

    mode = "FIXTURE" if mode == "FIXTURE" or replay_seen else _key_text(
        metadata.get("evidence_mode", mode)
    )
    if _OPTIMISTIC_MODEL in {
        _key_text(value).upper()
        for value in metadata.get("fillability_models", ())
    }:
        optimistic_supported = True

    # Classification is deliberately finalized only after the complete
    # physical stream has been read.  A gap can arrive after the fourth
    # horizon row, so adding an edge to the valid distribution at row time
    # would allow a late gap to poison an already-published statistic.
    all_episodes = tuple(episodes.values()) + tuple(completed_episodes.values())
    for episode in all_episodes:
        policy = policies.get(episode.policy_id)
        if policy is None:
            continue
        model_stats = policy.model(episode.model)
        # A valid episode is stronger than a valid individual horizon: the
        # fill interval and every required horizon must be present and clean.
        # Keep the horizon ledger local below, but fold all horizon reasons
        # into the episode-level fill/edge verdict before classifying it.
        episode_reasons = set(episode.fill_contamination_reasons)
        horizon_reasons: dict[int, set[str]] = {}
        for horizon in _HORIZONS:
            horizon_stats = model_stats.horizons[horizon]
            record = episode.horizon_records.get(horizon)
            if record is None:
                reasons = {"HORIZON_RECORD_MISSING"}
                horizon_stats.missing_expected_count += 1
                horizon_stats.contaminated = True
                horizon_stats.contamination_reason_counts[
                    "HORIZON_RECORD_MISSING"
                ] += 1
            else:
                reasons = set(
                    episode.horizon_contamination_reasons.get(horizon, set())
                )
            horizon_reasons[horizon] = reasons
            episode_reasons.update(reasons)
        model_stats.classify_fill(
            episode.fill_record,
            episode.quote_info,
            episode_reasons,
        )
        for horizon in _HORIZONS:
            record = episode.horizon_records.get(horizon)
            if record is None:
                continue
            horizon_stats = model_stats.horizons[horizon]
            horizon_stats.classify(
                record,
                contaminated_reasons=horizon_reasons[horizon],
                edge_allowed=not episode_reasons,
            )

    # A raw horizon whose exact fill never appeared is retained, but cannot
    # enter a valid attribution distribution.
    for (model, _version_text), records in deferred_horizons.items():
        for record in records:
            policy_id = _key_text(record.get("policy_id"))
            policy = policies.get(policy_id)
            if policy is None:
                continue
            try:
                horizon = int(record.get("horizon_ms"))
            except (TypeError, ValueError):
                continue
            if horizon not in _HORIZONS:
                continue
            horizon_stats = policy.model(model).horizons[horizon]
            horizon_stats.contaminated = True
            horizon_stats.contamination_reason_counts["EPISODE_CONTEXT_MISSING"] += 1
            horizon_stats.contaminated_observation_count += 1
            horizon_stats.contaminated_outcomes[_key_text(record.get("outcome"))] += 1

    root_counts["strict_valid_episode_count"] = sum(
        policy.strict.valid_fill_count for policy in policies.values()
    )
    root_counts["strict_contaminated_episode_count"] = sum(
        policy.strict.contaminated_fill_count for policy in policies.values()
    )
    root_counts["optimistic_valid_episode_count"] = sum(
        policy.optimistic.valid_fill_count for policy in policies.values()
    )
    root_counts["optimistic_contaminated_episode_count"] = sum(
        policy.optimistic.contaminated_fill_count for policy in policies.values()
    )

    def completeness_for(
        policy: _PolicyStats,
        model: str,
        horizon: int,
    ) -> str:
        if model == _OPTIMISTIC_MODEL and not optimistic_supported:
            return "NOT_IMPLEMENTED"
        stats = policy.model(model)
        observations = stats.horizons[horizon]
        expected = stats.fill_count
        coverage = expected == observations.observation_count
        if stats.fill_version_id_capacity_exceeded or observations.version_id_capacity_exceeded:
            coverage = False
        if expected <= _POLICY_FILL_VERSION_CAP and len(stats.fill_version_ids) < expected:
            coverage = False
        if expected <= _HORIZON_VERSION_CAP and len(observations.version_ids) < expected:
            coverage = False
        # Episode-level fill contamination is intentionally separate from the
        # horizon ledger.  A gap in the 300 ms horizon of one episode must not
        # degrade an otherwise covered 0 ms horizon for that same policy; the
        # fill/edge episode is contaminated, while only the overlapping
        # horizon observation is contaminated here.
        contaminated = observations.contaminated
        if episode_context_truncated or recent_gap_truncated:
            contaminated = True
        if not ordinary_duration_completion:
            contaminated = contaminated or bool(pending_stop_gaps)
        if unexpected_transport_failure:
            contaminated = True
        return "COMPLETE" if ordinary_duration_completion and coverage and not contaminated else "DEGRADED"

    model_fill_totals = {
        model: sum(policy.model(model).fill_count for policy in policies.values())
        for model in _MODELS
    }
    valid_model_fill_totals = {
        model: sum(policy.model(model).valid_fill_count for policy in policies.values())
        for model in _MODELS
    }
    eligible_totals = sum(policy.eligible_trade_count for policy in policies.values())
    model_volume_totals = {
        model: sum(policy.model(model).qualifying_volume for policy in policies.values())
        for model in _MODELS
    }
    episode_by_dimension: dict[str, dict[str, int]] = {
        "market": defaultdict(int),
        "direction": defaultdict(int),
        "target": defaultdict(int),
        "margin": defaultdict(int),
    }
    for policy in policies.values():
        for model in _MODELS:
            count = policy.model(model).fill_count
            episode_by_dimension["market"][policy.market] += count
            episode_by_dimension["direction"][policy.direction] += count
            episode_by_dimension["target"][policy.target] += count
            episode_by_dimension["margin"][policy.margin] += count

    output_groups: list[dict[str, Any]] = []
    for policy_id in sorted(policies):
        policy = policies[policy_id]
        for horizon in _HORIZONS:
            strict_horizon = policy.strict.horizons[horizon]
            optimistic_horizon = policy.optimistic.horizons[horizon]
            strict_complete = completeness_for(policy, _STRICT_MODEL, horizon)
            optimistic_complete = completeness_for(policy, _OPTIMISTIC_MODEL, horizon)
            strict_edges = strict_horizon.entry_edge
            strict_markouts = strict_horizon.markout
            positive = sum(
                1
                for value in strict_edges.values.ordered()
                if value > 0
            )
            strict_edge_count = strict_edges.count
            strict_raw_edges = strict_horizon.raw_entry_edge
            strict_raw_markouts = strict_horizon.raw_markout
            strict_raw_positive = sum(
                1
                for value in strict_raw_edges.values.ordered()
                if value > 0
            )
            strict_model_payload = _model_payload(
                _STRICT_MODEL,
                policy.strict,
                strict_horizon,
                completeness=strict_complete,
                implemented=True,
            )
            optimistic_model_payload = _model_payload(
                _OPTIMISTIC_MODEL,
                policy.optimistic,
                optimistic_horizon,
                completeness=optimistic_complete,
                implemented=optimistic_supported,
            )
            dimension_shares = {
                name: _rate(
                    policy.model(_STRICT_MODEL).fill_count,
                    episode_by_dimension[name].get(getattr(policy, name), 0),
                )
                for name in ("market", "direction", "target", "margin")
            }
            row = {
                "canonical_market": policy.market,
                "direction": policy.direction,
                "target_notional_usd": policy.target,
                "target_margin_bps": policy.margin,
                "policy_id": policy.policy_id,
                "horizon_ms": horizon,
                "horizon_label": "DIAGNOSTIC_500MS" if horizon == 500 else f"{horizon}MS",
                "opportunity_count": policy.quoteable_count,
                "quote_evaluation_count": policy.quote_count,
                "quoteable_time_share": (
                    _json_number(
                        min(
                            Decimal(policy.finish_union())
                            / Decimal(max((policy.observed_end or 0) - (policy.observed_start or 0), 1)),
                            Decimal("1"),
                        )
                    )
                    if policy.quote_count
                    else None
                ),
                "snapshot_quoteable_time_share": (
                    _json_number(
                        min(
                            Decimal(policy.finish_union())
                            / Decimal(max((policy.observed_end or 0) - (policy.observed_start or 0), 1)),
                            Decimal("1"),
                        )
                    )
                    if policy.quote_count
                    else None
                ),
                "median_quote_lifetime_ms": _json_number(policy.quote_lifetime_ms.median()),
                "risex_bbo_distance_ticks": _json_number(policy.distance_ticks.median()),
                "median_risex_bbo_distance_ticks": _json_number(policy.distance_ticks.median()),
                "p95_risex_bbo_distance_ticks": _json_number(policy.distance_ticks.percentile(Decimal("0.95"))),
                "median_risex_bbo_distance_bps": _json_number(policy.distance_bps.median()),
                "p95_risex_bbo_distance_bps": _json_number(policy.distance_bps.percentile(Decimal("0.95"))),
                "snapshot_edge_usd": _stats_payload(policy.snapshot_edge),
                "mean_snapshot_edge_usd": _json_number(policy.snapshot_edge.mean()),
                "median_snapshot_edge_usd": _json_number(policy.snapshot_edge.median()),
                "p05_snapshot_edge_usd": _json_number(policy.snapshot_edge.percentile(Decimal("0.05"))),
                "eligible_trade_count": policy.eligible_trade_count,
                "touch_count": policy.touch_count,
                "at_or_through_count": policy.at_or_through_count,
                "strict_price_through_count": policy.strict_price_through_count,
                "strict_would_fill_count": policy.strict.fill_count,
                "strict_raw_would_fill_count": policy.strict.fill_count,
                "strict_valid_would_fill_count": policy.strict.valid_fill_count,
                "strict_contaminated_would_fill_count": policy.strict.contaminated_fill_count,
                "strict_valid_episode_count": policy.strict.valid_fill_count,
                "strict_contaminated_episode_count": policy.strict.contaminated_fill_count,
                "optimistic_upper_bound_count": policy.optimistic.fill_count,
                "optimistic_raw_would_fill_count": policy.optimistic.fill_count,
                "optimistic_valid_would_fill_count": policy.optimistic.valid_fill_count,
                "optimistic_contaminated_would_fill_count": policy.optimistic.contaminated_fill_count,
                "optimistic_valid_episode_count": policy.optimistic.valid_fill_count,
                "optimistic_contaminated_episode_count": policy.optimistic.contaminated_fill_count,
                "optimistic_model": "IMPLEMENTED" if optimistic_supported else "NOT_IMPLEMENTED",
                "strict_cumulative_qualifying_volume": _json_number(policy.strict.qualifying_volume),
                "optimistic_cumulative_qualifying_volume": _json_number(policy.optimistic.qualifying_volume),
                "strict_cumulative_qualifying_notional_usd": _json_number(policy.strict.qualifying_notional),
                "optimistic_cumulative_qualifying_notional_usd": _json_number(policy.optimistic.qualifying_notional),
                "strict_threshold_qualifying_volume": _json_number(policy.strict.threshold_volume),
                "optimistic_threshold_qualifying_volume": _json_number(policy.optimistic.threshold_volume),
                "strict_filled_notional_usd": _json_number(policy.strict.filled_notional),
                "strict_raw_filled_notional_usd": _json_number(policy.strict.filled_notional),
                "strict_valid_filled_notional_usd": _json_number(
                    policy.strict.valid_filled_notional
                ),
                "strict_contaminated_filled_notional_usd": _json_number(
                    policy.strict.contaminated_filled_notional
                ),
                "optimistic_filled_notional_usd": _json_number(policy.optimistic.filled_notional),
                "optimistic_raw_filled_notional_usd": _json_number(
                    policy.optimistic.filled_notional
                ),
                "optimistic_valid_filled_notional_usd": _json_number(
                    policy.optimistic.valid_filled_notional
                ),
                "optimistic_contaminated_filled_notional_usd": _json_number(
                    policy.optimistic.contaminated_filled_notional
                ),
                "strict_time_to_fill_ms": _stats_payload(policy.strict.time_to_fill_ms),
                "strict_raw_time_to_fill_ms": _stats_payload(
                    policy.strict.time_to_fill_ms
                ),
                "strict_valid_time_to_fill_ms": _stats_payload(
                    policy.strict.valid_time_to_fill_ms
                ),
                "strict_contaminated_time_to_fill_ms": _stats_payload(
                    policy.strict.contaminated_time_to_fill_ms
                ),
                "optimistic_time_to_fill_ms": _stats_payload(policy.optimistic.time_to_fill_ms),
                "optimistic_raw_time_to_fill_ms": _stats_payload(
                    policy.optimistic.time_to_fill_ms
                ),
                "optimistic_valid_time_to_fill_ms": _stats_payload(
                    policy.optimistic.valid_time_to_fill_ms
                ),
                "optimistic_contaminated_time_to_fill_ms": _stats_payload(
                    policy.optimistic.contaminated_time_to_fill_ms
                ),
                "strict_fill_contamination_reason_counts": {
                    key: policy.strict.fill_contamination_reason_counts[key]
                    for key in sorted(policy.strict.fill_contamination_reason_counts)
                },
                "optimistic_fill_contamination_reason_counts": {
                    key: policy.optimistic.fill_contamination_reason_counts[key]
                    for key in sorted(policy.optimistic.fill_contamination_reason_counts)
                },
                "fillability_models": {
                    _STRICT_MODEL: strict_model_payload,
                    _OPTIMISTIC_MODEL: optimistic_model_payload,
                },
                "full_hedge_rate": _rate(
                    strict_horizon.valid_outcomes.get("HEDGE_FULL", 0),
                    strict_horizon.valid_observation_count,
                ),
                "raw_full_hedge_rate": _rate(
                    strict_horizon.outcomes.get("HEDGE_FULL", 0),
                    strict_horizon.observation_count,
                ),
                "valid_full_hedge_rate": _rate(
                    strict_horizon.valid_outcomes.get("HEDGE_FULL", 0),
                    strict_horizon.valid_observation_count,
                ),
                "contaminated_full_hedge_rate": _rate(
                    strict_horizon.contaminated_outcomes.get("HEDGE_FULL", 0),
                    strict_horizon.contaminated_observation_count,
                ),
                "partial_or_missing_rate": _rate(
                    strict_horizon.valid_outcomes.get("HEDGE_PARTIAL", 0)
                    + sum(
                        strict_horizon.valid_outcomes.get(name, 0)
                        for name in (
                            "HEDGE_DEPTH_UNAVAILABLE",
                            "HEDGE_DATA_MISSING",
                            "HEDGE_DATA_STALE",
                            "HEDGE_SESSION_DISPLACED",
                            "HEDGE_DATA_GAP",
                            "HEDGE_OUTCOME_UNKNOWN",
                        )
                    ),
                    strict_horizon.valid_observation_count,
                ),
                "raw_partial_or_missing_rate": _rate(
                    strict_horizon.outcomes.get("HEDGE_PARTIAL", 0)
                    + sum(
                        strict_horizon.outcomes.get(name, 0)
                        for name in (
                            "HEDGE_DEPTH_UNAVAILABLE",
                            "HEDGE_DATA_MISSING",
                            "HEDGE_DATA_STALE",
                            "HEDGE_SESSION_DISPLACED",
                            "HEDGE_DATA_GAP",
                            "HEDGE_OUTCOME_UNKNOWN",
                        )
                    ),
                    strict_horizon.observation_count,
                ),
                "contaminated_partial_or_missing_rate": _rate(
                    strict_horizon.contaminated_outcomes.get("HEDGE_PARTIAL", 0)
                    + sum(
                        strict_horizon.contaminated_outcomes.get(name, 0)
                        for name in (
                            "HEDGE_DEPTH_UNAVAILABLE",
                            "HEDGE_DATA_MISSING",
                            "HEDGE_DATA_STALE",
                            "HEDGE_SESSION_DISPLACED",
                            "HEDGE_DATA_GAP",
                            "HEDGE_OUTCOME_UNKNOWN",
                        )
                    ),
                    strict_horizon.contaminated_observation_count,
                ),
                "mean_entry_edge_usd": _json_number(strict_edges.mean()),
                "median_entry_edge_usd": _json_number(strict_edges.median()),
                "p05_entry_edge_usd": _json_number(strict_edges.percentile(Decimal("0.05"))),
                "raw_mean_entry_edge_usd": _json_number(strict_raw_edges.mean()),
                "raw_median_entry_edge_usd": _json_number(strict_raw_edges.median()),
                "raw_p05_entry_edge_usd": _json_number(
                    strict_raw_edges.percentile(Decimal("0.05"))
                ),
                "mean_conditional_markout_usd": _json_number(strict_markouts.mean()),
                "median_conditional_markout_usd": _json_number(strict_markouts.median()),
                "p05_conditional_markout_usd": _json_number(strict_markouts.percentile(Decimal("0.05"))),
                "raw_mean_conditional_markout_usd": _json_number(
                    strict_raw_markouts.mean()
                ),
                "raw_median_conditional_markout_usd": _json_number(
                    strict_raw_markouts.median()
                ),
                "raw_p05_conditional_markout_usd": _json_number(
                    strict_raw_markouts.percentile(Decimal("0.05"))
                ),
                "contaminated_mean_conditional_markout_usd": _json_number(
                    strict_horizon.contaminated_markout.mean()
                ),
                "contaminated_median_conditional_markout_usd": _json_number(
                    strict_horizon.contaminated_markout.median()
                ),
                "contaminated_p05_conditional_markout_usd": _json_number(
                    strict_horizon.contaminated_markout.percentile(Decimal("0.05"))
                ),
                "positive_edge_share": _rate(positive, strict_edge_count),
                "raw_positive_edge_share": _rate(
                    strict_raw_positive, strict_raw_edges.count
                ),
                "valid_edge_count": strict_horizon.entry_edge.count,
                "raw_edge_count": strict_raw_edges.count,
                "contaminated_edge_count": strict_horizon.contaminated_entry_edge.count,
                "edge_excluded_by_fill_count": strict_horizon.edge_excluded_by_fill_count,
                "maximum_adverse_markout_usd": _json_number(strict_markouts.minimum()),
                "hypothetical_risex_filled_notional_usd": _json_number(policy.strict.filled_notional),
                "concentration": {
                    "strict_episode_share": _rate(policy.strict.fill_count, model_fill_totals[_STRICT_MODEL]),
                    "strict_valid_episode_share": _rate(
                        policy.strict.valid_fill_count,
                        valid_model_fill_totals[_STRICT_MODEL],
                    ),
                    "optimistic_episode_share": _rate(policy.optimistic.fill_count, model_fill_totals[_OPTIMISTIC_MODEL]),
                    "optimistic_valid_episode_share": _rate(
                        policy.optimistic.valid_fill_count,
                        valid_model_fill_totals[_OPTIMISTIC_MODEL],
                    ),
                    "eligible_trade_share": _rate(policy.eligible_trade_count, eligible_totals),
                    "strict_qualifying_volume_share": _json_number(
                        policy.strict.qualifying_volume / model_volume_totals[_STRICT_MODEL]
                        if model_volume_totals[_STRICT_MODEL]
                        else None
                    ),
                    "optimistic_qualifying_volume_share": _json_number(
                        policy.optimistic.qualifying_volume / model_volume_totals[_OPTIMISTIC_MODEL]
                        if model_volume_totals[_OPTIMISTIC_MODEL]
                        else None
                    ),
                    "by_dimension": {
                        name: {
                            "key": getattr(policy, name),
                            "strict_episode_share": dimension_shares[name],
                        }
                        for name in ("market", "direction", "target", "margin")
                    },
                },
                "data_gap_count": gap_count_by_market.get(policy.market, 0),
                "strict_raw_detection_timestamp_count": len(
                    policy.strict.raw_detection_timestamps
                ),
                "strict_valid_detection_timestamp_count": len(
                    policy.strict.valid_detection_timestamps
                ),
                "strict_contaminated_detection_timestamp_count": len(
                    policy.strict.contaminated_detection_timestamps
                ),
                "optimistic_raw_detection_timestamp_count": len(
                    policy.optimistic.raw_detection_timestamps
                ),
                "optimistic_valid_detection_timestamp_count": len(
                    policy.optimistic.valid_detection_timestamps
                ),
                "optimistic_contaminated_detection_timestamp_count": len(
                    policy.optimistic.contaminated_detection_timestamps
                ),
                "data_completeness": strict_complete,
                "strict_data_completeness": strict_complete,
                "optimistic_data_completeness": optimistic_complete,
                "evidence_mode": mode,
            }
            output_groups.append(row)
    output_groups.sort(
        key=lambda row: (
            row["canonical_market"],
            row["direction"],
            row["target_notional_usd"],
            row["target_margin_bps"],
            row["horizon_ms"],
        )
    )
    sample_stop_payload = None
    if first_sample_stop is not None:
        sample_stop_payload = {
            key: first_sample_stop.get(key)
            for key in (
                "reason",
                "strict_episode_count",
                "optimistic_episode_count",
                "eligible_trade_count",
                "integrity_reason",
                "observed_monotonic_ns",
                "material_policy_id",
                "material_valid_strict_episode_count",
                "material_detection_timestamp_count",
            )
        }
    return {
        "schema_version": 1,
        "run_id": metadata.get("run_id"),
        "source_commit": metadata.get("source_commit"),
        "evidence_mode": mode,
        "record_count": record_count,
        "byte_count": Path(path).stat().st_size,
        "gap_count": gap_count,
        "failed_run": failed_run,
        "clean_stop_count": clean_stop_count,
        "strict_would_fill_count": root_counts["strict_episode_count"],
        "strict_raw_episode_count": root_counts["strict_episode_count"],
        "strict_valid_episode_count": root_counts["strict_valid_episode_count"],
        "strict_contaminated_episode_count": root_counts[
            "strict_contaminated_episode_count"
        ],
        "optimistic_upper_bound_count": root_counts["optimistic_episode_count"],
        "optimistic_raw_episode_count": root_counts["optimistic_episode_count"],
        "optimistic_valid_episode_count": root_counts[
            "optimistic_valid_episode_count"
        ],
        "optimistic_contaminated_episode_count": root_counts[
            "optimistic_contaminated_episode_count"
        ],
        "eligible_trade_count": root_counts["eligible_trade_count"],
        "strict_episode_count": root_counts["strict_episode_count"],
        "optimistic_episode_count": root_counts["optimistic_episode_count"],
        "horizon_record_count": horizon_record_count,
        "book_record_count": book_audit.book_count,
        "full_book_snapshot_count": book_audit.full_snapshot_count,
        "book_delta_count": book_audit.delta_count,
        "book_chain_count": book_audit.chain_count,
        "maximum_reconstructed_book_levels": book_audit.maximum_level_count,
        "current_reconstructed_book_levels": book_audit.current_level_count,
        "sample_stop_reason": None if sample_stop_payload is None else sample_stop_payload["reason"],
        "sample_stop_signal": sample_stop_payload,
        "transport_event_counts": {
            key: transport_event_counts[key] for key in sorted(transport_event_counts)
        },
        "graceful_close_count": transport_event_counts.get("GRACEFUL_CLOSE", 0),
        "reconnect_count": transport_event_counts.get("RECONNECT", 0),
        "unexpected_transport_failure_count": transport_event_counts.get(
            "UNEXPECTED_FAILURE", 0
        ),
        "transport_failure_class_counts": {
            key: transport_failure_class_counts[key]
            for key in sorted(transport_failure_class_counts)
        },
        "transport_exception_type_counts": {
            key: transport_exception_type_counts[key]
            for key in sorted(transport_exception_type_counts)
        },
        "fail_closed": failed_run
        or transport_event_counts.get("UNEXPECTED_FAILURE", 0) > 0,
        "optimistic_model": "IMPLEMENTED" if optimistic_supported else "NOT_IMPLEMENTED",
        "markets": sorted(markets),
        "groups": output_groups,
    }


def render_report(path: str | Path, *, format: str = "json") -> str:
    report = build_report(path)
    if format == "json":
        return json.dumps(report, sort_keys=True, separators=(",", ":"))
    if format != "table":
        raise ValueError("report format must be json or table")
    lines = [
        f"run_id={report.get('run_id')}",
        f"mode={report.get('evidence_mode')} records={report.get('record_count')} bytes={report.get('byte_count')} gaps={report.get('gap_count')}",
        "market direction size margin horizon fills full_rate markout completeness",
    ]
    for row in report["groups"]:
        lines.append(
            " ".join(
                (
                    row["canonical_market"],
                    row["direction"],
                    row["target_notional_usd"],
                    row["target_margin_bps"],
                    str(row["horizon_ms"]),
                    str(row["strict_would_fill_count"]),
                    _key_text(row["full_hedge_rate"]),
                    _key_text(row["median_conditional_markout_usd"]),
                    row["data_completeness"],
                )
            )
        )
    return "\n".join(lines)


__all__ = ["EvidenceIntegrityError", "build_report", "render_report"]
