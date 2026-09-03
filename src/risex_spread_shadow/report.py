"""Deterministic offline aggregation of SS-001F JSONL evidence."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
from typing import Any, Iterator

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
    observation_count: int = 0
    outcomes: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    filled_quantity: _NumberStats = field(default_factory=_NumberStats)
    notional: _NumberStats = field(default_factory=_NumberStats)
    entry_edge: _NumberStats = field(default_factory=_NumberStats)
    markout: _NumberStats = field(default_factory=_NumberStats)
    version_ids: set[str] = field(default_factory=set)
    version_id_capacity_exceeded: bool = False
    contaminated: bool = False
    gap_reasons: set[str] = field(default_factory=set)

    def add(self, record: dict[str, Any], *, track_version: bool = True) -> None:
        self.observation_count += 1
        outcome = _key_text(record.get("outcome"))
        self.outcomes[outcome] += 1
        self.filled_quantity.add(_decimal(record.get("filled_quantity")))
        self.notional.add(_decimal(record.get("notional_usd")))
        if outcome == "HEDGE_FULL":
            self.entry_edge.add(_decimal(record.get("entry_edge_usd")))
            self.markout.add(_decimal(record.get("conditional_markout_usd")))
        version_id = record.get("quote_version_id")
        if track_version and version_id is not None:
            if len(self.version_ids) < _HORIZON_VERSION_CAP:
                self.version_ids.add(_key_text(version_id))
            else:
                self.version_id_capacity_exceeded = True
        if outcome == "HEDGE_DATA_GAP":
            self.contaminated = True
            self.gap_reasons.add(_key_text(record.get("gap_reason")) or outcome)
        elif outcome == "HEDGE_OUTCOME_UNKNOWN":
            self.contaminated = True


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
    risex_stream_session: str | int | None = None
    risex_recovery: int | None = None
    hedge_stream_session: str | int | None = None
    hedge_recovery: int | None = None
    horizons_seen: set[int] = field(default_factory=set)
    contaminated_horizons: set[int] = field(default_factory=set)

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


@dataclass(frozen=True, slots=True)
class _CompletedEpisodeIndex:
    """Minimal identity retained for gaps that arrive after horizon completion."""

    model: str
    version_id: str
    policy_id: str
    market: str
    quote_created: int | None
    detected: int | None
    risex_stream_session: str | int | None
    risex_recovery: int | None
    hedge_stream_session: str | int | None
    hedge_recovery: int | None

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
    gap_start = _record_int(gap, "gap_start_monotonic_ns")
    if gap_start is None:
        return True
    gap_end = _record_int(gap, "gap_end_monotonic_ns")
    if gap_end is None:
        gap_end = interval[1]
    if gap_end < gap_start:
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
    outcome_counts = {
        key: horizon.outcomes[key] for key in sorted(horizon.outcomes)
    }
    full = horizon.outcomes.get("HEDGE_FULL", 0)
    partial = horizon.outcomes.get("HEDGE_PARTIAL", 0)
    missing = sum(
        horizon.outcomes.get(name, 0)
        for name in (
            "HEDGE_DEPTH_UNAVAILABLE",
            "HEDGE_DATA_MISSING",
            "HEDGE_DATA_STALE",
            "HEDGE_SESSION_DISPLACED",
            "HEDGE_DATA_GAP",
            "HEDGE_OUTCOME_UNKNOWN",
        )
    )
    return {
        "model": model_name,
        "implemented": implemented,
        "would_fill_count": stats.fill_count,
        "fill_count": stats.fill_count,
        "filled_notional_usd": _json_number(stats.filled_notional),
        "cumulative_qualifying_volume": _json_number(stats.qualifying_volume),
        "cumulative_qualifying_notional_usd": _json_number(stats.qualifying_notional),
        "threshold_qualifying_volume": _json_number(stats.threshold_volume),
        "threshold_qualifying_notional_usd": _json_number(stats.threshold_notional),
        "time_to_fill_ms": _stats_payload(stats.time_to_fill_ms),
        "horizon": {
            "observation_count": horizon.observation_count,
            "outcome_counts": outcome_counts,
            "full_hedge_rate": _rate(full, horizon.observation_count),
            "partial_hedge_rate": _rate(partial, horizon.observation_count),
            "missing_hedge_rate": _rate(missing, horizon.observation_count),
            "partial_or_missing_rate": _rate(partial + missing, horizon.observation_count),
            "filled_quantity": _stats_payload(horizon.filled_quantity),
            "notional_usd": _stats_payload(horizon.notional),
            "entry_edge_usd": _stats_payload(horizon.entry_edge),
            "conditional_markout_usd": _stats_payload(horizon.markout),
            "data_completeness": completeness,
        },
    }


def build_report(path: str | Path) -> dict[str, Any]:
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
    completed_episodes: dict[tuple[str, str], _CompletedEpisodeIndex] = {}
    episode_context_truncated = False
    recent_gaps: deque[dict[str, Any]] = deque(maxlen=_RECENT_GAP_CAP)
    recent_gap_truncated = False
    pending_stop_gaps: deque[dict[str, Any]] = deque(maxlen=_RECENT_GAP_CAP)
    gap_count_by_market: dict[str, int] = defaultdict(int)
    seen_trade_keys: set[str] = set()
    trade_key_order: deque[str] = deque(maxlen=_RECENT_TRADE_KEY_CAP)
    first_sample_stop: dict[str, Any] | None = None
    replay_seen = False

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
        if info is not None and detected is not None:
            stats.time_to_fill_ms.add(Decimal(max(0, detected - info.created)) / Decimal("1000000"))
        if not version_text:
            return True
        if len(episodes) + len(completed_episodes) >= _EPISODE_CONTEXT_CAP:
            episode_context_truncated = True
            return True
        if info is None:
            created = _record_int(record, "quote_created_monotonic_ns")
            market = _key_text(record.get("canonical_market"))
        else:
            created = info.created
            market = info.market
        episode = _EpisodeContext(
            model,
            version_text,
            policy_id,
            market,
            created,
            detected,
            None if info is None else info.stream_session,
            None if info is None else info.recovery,
            record.get("hedge_stream_session_id")
            if record.get("hedge_stream_session_id") is not None
            else (None if info is None else info.hedge_stream_session),
            _record_int(record, "hedge_recovery_generation")
            if record.get("hedge_recovery_generation") is not None
            else (None if info is None else info.hedge_recovery),
        )
        episodes[(model, version_text)] = episode
        for gap in recent_gaps:
            mark_gap_for_episode(gap, episode)
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
        if _gap_contaminates(gap, fill_record):
            episode.contaminated_horizons.update(_HORIZONS)
            return
        for horizon in _HORIZONS:
            if _gap_contaminates(gap, episode.horizon_interval(horizon)):
                episode.contaminated_horizons.add(horizon)

    def mark_gap_for_completed_episode(
        gap: dict[str, Any], episode: _CompletedEpisodeIndex
    ) -> None:
        fill_record = {
            "kind": "WOULD_FILL",
            "canonical_market": episode.market,
            "venue": "RISEX",
            "quote_created_monotonic_ns": episode.quote_created,
            "would_fill_detected_monotonic_ns": episode.detected,
            "stream_session_id": episode.risex_stream_session,
            "recovery_generation": episode.risex_recovery,
        }
        stats = policies.get(episode.policy_id)
        if stats is None:
            return
        if _gap_contaminates(gap, fill_record):
            for horizon in _HORIZONS:
                stats.model(episode.model).horizons[horizon].contaminated = True
            return
        for horizon in _HORIZONS:
            if _gap_contaminates(gap, episode.horizon_interval(horizon)):
                stats.model(episode.model).horizons[horizon].contaminated = True

    def retire_episode(key: tuple[str, str], episode: _EpisodeContext) -> None:
        stats = policies.get(episode.policy_id)
        if stats is not None:
            model_stats = stats.model(episode.model)
            for horizon in episode.contaminated_horizons:
                model_stats.horizons[horizon].contaminated = True
        completed_episodes[key] = _CompletedEpisodeIndex(
            model=episode.model,
            version_id=episode.version_id,
            policy_id=episode.policy_id,
            market=episode.market,
            quote_created=episode.quote_created,
            detected=episode.detected,
            risex_stream_session=episode.risex_stream_session,
            risex_recovery=episode.risex_recovery,
            hedge_stream_session=episode.hedge_stream_session,
            hedge_recovery=episode.hedge_recovery,
        )
        episodes.pop(key, None)

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
        episode = episodes.get((model, version_text)) if version_text else None
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
        horizon_stats.add(record)
        if episode is not None:
            episode.horizons_seen.add(horizon)
            if horizon in episode.contaminated_horizons:
                horizon_stats.contaminated = True
            embedded = _embedded_horizon_gap(record)
            if embedded is not None and embedded.get("reason") != "PUBLIC_SMOKE_STOPPED":
                episode.contaminated_horizons.add(horizon)
                horizon_stats.contaminated = True
            if len(episode.horizons_seen) == len(_HORIZONS):
                retire_episode((model, version_text), episode)

    def process_gap(record: dict[str, Any]) -> None:
        nonlocal gap_count, recent_gap_truncated
        gap_count += 1
        market = _key_text(record.get("canonical_market"))
        gap_count_by_market[market] += 1
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

    for episode in episodes.values():
        policy = policies.get(episode.policy_id)
        if policy is None:
            continue
        for horizon in episode.contaminated_horizons:
            policy.model(episode.model).horizons[horizon].contaminated = True

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
        contaminated = observations.contaminated
        if episode_context_truncated or recent_gap_truncated:
            contaminated = True
        if not ordinary_duration_completion:
            contaminated = contaminated or bool(pending_stop_gaps)
        return "COMPLETE" if ordinary_duration_completion and coverage and not contaminated else "DEGRADED"

    model_fill_totals = {
        model: sum(policy.model(model).fill_count for policy in policies.values())
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
                "optimistic_upper_bound_count": policy.optimistic.fill_count,
                "optimistic_model": "IMPLEMENTED" if optimistic_supported else "NOT_IMPLEMENTED",
                "strict_cumulative_qualifying_volume": _json_number(policy.strict.qualifying_volume),
                "optimistic_cumulative_qualifying_volume": _json_number(policy.optimistic.qualifying_volume),
                "strict_cumulative_qualifying_notional_usd": _json_number(policy.strict.qualifying_notional),
                "optimistic_cumulative_qualifying_notional_usd": _json_number(policy.optimistic.qualifying_notional),
                "strict_threshold_qualifying_volume": _json_number(policy.strict.threshold_volume),
                "optimistic_threshold_qualifying_volume": _json_number(policy.optimistic.threshold_volume),
                "strict_filled_notional_usd": _json_number(policy.strict.filled_notional),
                "optimistic_filled_notional_usd": _json_number(policy.optimistic.filled_notional),
                "strict_time_to_fill_ms": _stats_payload(policy.strict.time_to_fill_ms),
                "optimistic_time_to_fill_ms": _stats_payload(policy.optimistic.time_to_fill_ms),
                "fillability_models": {
                    _STRICT_MODEL: strict_model_payload,
                    _OPTIMISTIC_MODEL: optimistic_model_payload,
                },
                "full_hedge_rate": _rate(
                    strict_horizon.outcomes.get("HEDGE_FULL", 0),
                    strict_horizon.observation_count,
                ),
                "partial_or_missing_rate": _rate(
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
                "mean_entry_edge_usd": _json_number(strict_edges.mean()),
                "median_entry_edge_usd": _json_number(strict_edges.median()),
                "p05_entry_edge_usd": _json_number(strict_edges.percentile(Decimal("0.05"))),
                "mean_conditional_markout_usd": _json_number(strict_markouts.mean()),
                "median_conditional_markout_usd": _json_number(strict_markouts.median()),
                "p05_conditional_markout_usd": _json_number(strict_markouts.percentile(Decimal("0.05"))),
                "positive_edge_share": _rate(positive, strict_edge_count),
                "maximum_adverse_markout_usd": _json_number(strict_markouts.minimum()),
                "hypothetical_risex_filled_notional_usd": _json_number(policy.strict.filled_notional),
                "concentration": {
                    "strict_episode_share": _rate(policy.strict.fill_count, model_fill_totals[_STRICT_MODEL]),
                    "optimistic_episode_share": _rate(policy.optimistic.fill_count, model_fill_totals[_OPTIMISTIC_MODEL]),
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
        "strict_would_fill_count": root_counts["strict_episode_count"],
        "optimistic_upper_bound_count": root_counts["optimistic_episode_count"],
        "eligible_trade_count": root_counts["eligible_trade_count"],
        "strict_episode_count": root_counts["strict_episode_count"],
        "optimistic_episode_count": root_counts["optimistic_episode_count"],
        "horizon_record_count": horizon_record_count,
        "sample_stop_reason": None if sample_stop_payload is None else sample_stop_payload["reason"],
        "sample_stop_signal": sample_stop_payload,
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
