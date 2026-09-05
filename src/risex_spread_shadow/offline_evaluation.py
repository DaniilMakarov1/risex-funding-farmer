"""Bounded offline SCAN-002 evaluation of the frozen two-arm question.

This module deliberately sits beside the accepted report.  It does not change
the legacy SS-001H/SS-001J aggregates and it does not consume their retained
record list.  A path input is replayed in separate bounded passes: one pass
keeps only the small quote/trade/episode ledgers needed for the fixed BTC 1/2
bps slice, and another reconstructs only referenced or latest-before-deadline
books.

The result is evidence, not a trading decision.  In particular, a fixture or
an evidence stream without the later stage metadata is never promoted to a
public calibration/holdout candidate.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Mapping

from risex_farmer.models import BookLevel, Side, Venue

from .book_chain import (
    BookRevisionChainError,
    BookRevisionReconstructor,
    book_state_sha256,
)
from .economics import exact_vwap
from .models import make_book_revision_id
from .store import iter_records


HORIZONS = (0, 300, 500, 1000)
STRICT_MODEL = "STRICT_LOWER_BOUND"
OPTIMISTIC_MODEL = "OPTIMISTIC_UPPER_BOUND"
EVALUATION_SECTION = "SCAN_002_BOUNDED_OFFLINE_EVALUATION"
_NS_PER_MINUTE = 60_000_000_000
_NS_PER_FIVE_MINUTES = 300_000_000_000
_MAX_QUOTES = 250_000
_MAX_ELIGIBLE_TRADES = 250
_MAX_FILLS = 2_000
_MAX_HORIZONS = 8_000
_MAX_GAPS = 100_000
_MISSING = object()

_POLICY_PAYLOAD = {
    "market": "BTC",
    "direction": "RISEX_SELL_LIGHTER_BUY",
    "target_notional_usd": "100",
    "target_margins_bps": ["1", "2"],
    "horizons_ms": list(HORIZONS),
    "risex_maker_fee_rate": "0.0001",
    "risex_fee_tier": "TIER_1",
    "lighter_taker_fee_rate": "0",
    "lighter_taker_latency_ms": "300",
}


def _fingerprint(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


_POLICY_FINGERPRINT = _fingerprint(_POLICY_PAYLOAD)


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _integer(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _canonical_decimal(value: Any) -> str:
    parsed = _decimal(value)
    return "" if parsed is None else format(parsed.normalize(), "f")


def _identity(value: Any) -> tuple[str, str] | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return "int", str(value)
    if isinstance(value, str) and value:
        return "str", value.casefold()
    return None


def _json_default(value: Any) -> str:
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def _tie(record: Mapping[str, Any]) -> str:
    return json.dumps(record, sort_keys=True, separators=(",", ":"), default=_json_default)


def _rate(numerator: int, denominator: int) -> str | None:
    return str(Decimal(numerator) / Decimal(denominator)) if denominator else None


def _stats(values: Iterable[Decimal]) -> dict[str, Any]:
    ordered = sorted(values)
    if not ordered:
        return {
            "count": 0,
            "sum": "0",
            "mean": None,
            "median": None,
            "p05": None,
            "minimum": None,
            "maximum": None,
            "gross_positive": "0",
            "gross_negative": "0",
            "gross_negative_abs": "0",
            "negative_count": 0,
            "positive_count": 0,
            "positive_share": None,
        }
    middle = len(ordered) // 2
    median = (
        ordered[middle]
        if len(ordered) % 2
        else (ordered[middle - 1] + ordered[middle]) / Decimal("2")
    )
    positive = [value for value in ordered if value > 0]
    negative = [value for value in ordered if value < 0]
    return {
        "count": len(ordered),
        "sum": str(sum(ordered, Decimal("0"))),
        "mean": str(sum(ordered, Decimal("0")) / Decimal(len(ordered))),
        "median": str(median),
        "p05": str(ordered[int(Decimal("0.05") * Decimal(len(ordered) - 1))]),
        "minimum": str(ordered[0]),
        "maximum": str(ordered[-1]),
        "gross_positive": str(sum(positive, Decimal("0"))),
        "gross_negative": str(sum(negative, Decimal("0"))),
        "gross_negative_abs": str(-sum(negative, Decimal("0"))),
        "negative_count": len(negative),
        "positive_count": len(positive),
        "positive_share": _rate(len(positive), len(ordered)),
    }


def _as_sequence(value: Any) -> tuple[Any, ...] | None:
    if isinstance(value, (list, tuple)):
        return tuple(value)
    return None


def _same_sequence(value: Any, expected: tuple[Any, ...], *, decimals: bool = False) -> bool:
    sequence = _as_sequence(value)
    if sequence is None:
        return False
    if decimals:
        parsed = tuple(_decimal(item) for item in sequence)
        return all(item is not None for item in parsed) and parsed == expected
    return sequence == expected


def _parse_order_ids(key: Any) -> tuple[tuple[str, str] | None, tuple[str, str] | None, set[str]]:
    """Parse the persisted event key when possible, without time inference."""

    errors: set[str] = set()
    if not isinstance(key, str) or not key:
        return None, None, {"TRADE_EVENT_KEY_MISSING_OR_MALFORMED"}
    parts = key.split("|")
    if len(parts) >= 3 and parts[0].upper() == "RISEX":
        order_part = parts[-1]
        if "-" in order_part:
            left, right = order_part.split("-", 1)
            maker, taker = _identity(left), _identity(right)
            if maker is not None and taker is not None:
                return maker, taker, errors
            errors.add("TRADE_EVENT_KEY_ORDER_ID_MALFORMED")
    # Direct deterministic fixtures may carry opaque event keys, but both
    # explicit order IDs are still required.  No timestamp proximity is used.
    return None, None, errors


def _resolve_trade_ids(record: Mapping[str, Any]) -> tuple[tuple[str, str] | None, tuple[str, str] | None, set[str]]:
    maker_from_key, taker_from_key, errors = _parse_order_ids(record.get("trade_event_key"))
    maker_explicit = _identity(record.get("maker_order_id"))
    taker_explicit = _identity(record.get("taker_order_id"))
    if record.get("maker_order_id") is not None and maker_explicit is None:
        errors.add("MAKER_ORDER_ID_MALFORMED")
    if record.get("taker_order_id") is not None and taker_explicit is None:
        errors.add("TAKER_ORDER_ID_MALFORMED")
    if maker_from_key is not None and maker_explicit is not None and maker_from_key != maker_explicit:
        errors.add("MAKER_ORDER_ID_CONFLICT")
    if taker_from_key is not None and taker_explicit is not None and taker_from_key != taker_explicit:
        errors.add("TAKER_ORDER_ID_CONFLICT")
    maker = maker_explicit or maker_from_key
    taker = taker_explicit or taker_from_key
    if maker is None:
        errors.add("MAKER_ORDER_ID_MISSING")
    if taker is None:
        errors.add("TAKER_ORDER_ID_MISSING")
    return maker, taker, errors


@dataclass(frozen=True, slots=True)
class _Quote:
    policy_id: str
    version_id: str
    market: str
    direction: str
    target: Decimal | None
    margin: Decimal | None
    created: int | None
    expiry: int | None
    session: str | int | None
    recovery: int | None
    hedge_session: str | int | None
    hedge_recovery: int | None
    maker_price: Decimal | None
    tick: Decimal | None
    quantity: Decimal | None
    maker_order_id: tuple[str, str] | None
    risex_fee_rate: Decimal | None
    lighter_fee_rate: Decimal | None
    risex_fee_source: str
    lighter_fee_source: str
    risex_book_ref: tuple[Any, ...] | None
    lighter_book_ref: tuple[Any, ...] | None
    risex_book_digest: str | None
    lighter_book_digest: str | None
    issues: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class _CompactBook:
    """Only the identity, digest, and exact-q ask prefix of a witness."""

    canonical_market: str
    asks: tuple[BookLevel, ...]
    received_monotonic_ns: int
    stream_session_id: str | int
    recovery_generation: int
    book_revision: int
    book_revision_id: str
    sequence: int | None
    sequence_valid: bool
    checksum_valid: bool
    fresh: bool
    state_sha256: str

    @property
    def bids(self) -> tuple[BookLevel, ...]:
        return ()

    @property
    def is_sequence_healthy(self) -> bool:
        return self.sequence_valid and self.checksum_valid


def _compact_book(
    book: Any,
    quantity: Decimal | None,
    *,
    state_sha256_value: str | None = None,
) -> _CompactBook:
    remaining = quantity if quantity is not None and quantity > 0 else Decimal("0")
    asks: list[BookLevel] = []
    if remaining > 0:
        for level in book.asks:
            asks.append(level)
            remaining -= level.canonical_quantity
            if remaining <= 0:
                break
    return _CompactBook(
        canonical_market=book.canonical_market,
        asks=tuple(asks),
        received_monotonic_ns=book.received_monotonic_ns,
        stream_session_id=book.stream_session_id,
        recovery_generation=book.recovery_generation,
        book_revision=book.book_revision,
        book_revision_id=book.book_revision_id,
        sequence=book.sequence,
        sequence_valid=book.sequence_valid,
        checksum_valid=book.checksum_valid,
        fresh=book.fresh,
        state_sha256=(
            book_state_sha256(book)
            if state_sha256_value is None
            else state_sha256_value
        ),
    )


@dataclass(frozen=True, slots=True)
class _Trade:
    key: str
    market: str
    price: Decimal | None
    quantity: Decimal | None
    side: str
    received: int | None
    session: str | int | None
    recovery: int | None
    maker_order_id: tuple[str, str] | None
    taker_order_id: tuple[str, str] | None
    policy_ids: frozenset[str]
    contexts: tuple[tuple[str, tuple[_Quote, ...]], ...] = ()
    identity_issues: tuple[str, ...] = ()

    def arm_context(self, arm: str) -> tuple[_Quote, ...]:
        return dict(self.contexts).get(arm, ())


@dataclass(frozen=True, slots=True)
class _Fill:
    key: tuple[str, str]
    policy_id: str
    version_id: str
    market: str
    direction: str
    margin: Decimal | None
    quantity: Decimal | None
    cumulative: Decimal | None
    detected: int | None
    trade_keys: tuple[str, ...]
    record: Mapping[str, Any]
    quote: _Quote | None = None
    issues: tuple[str, ...] = ()


@dataclass(slots=True)
class _Unit:
    unit_id: str
    trade_keys: list[str] = field(default_factory=list)
    fills: list[_Fill] = field(default_factory=list)
    identity_issues: set[str] = field(default_factory=set)
    gap_issues: set[str] = field(default_factory=set)
    episode_issues: set[str] = field(default_factory=set)
    active_by_arm: dict[str, dict[str, tuple[_Quote, ...]]] = field(default_factory=dict)
    pair_classification: str = "EFFECTIVE_LEVEL_UNRESOLVED"
    pair_comparisons: list[dict[str, Any]] = field(default_factory=list)
    status: str = "UNRESOLVED"


class _UnionFind:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def add(self, item: str) -> None:
        self.parent.setdefault(item, item)

    def find(self, item: str) -> str:
        self.add(item)
        parent = self.parent[item]
        if parent != item:
            parent = self.find(parent)
            self.parent[item] = parent
        return parent

    def union(self, left: str, right: str) -> None:
        left_root, right_root = self.find(left), self.find(right)
        if left_root != right_root:
            self.parent[right_root] = left_root


def _reference(record: Mapping[str, Any], prefix: str, venue: Venue) -> tuple[Any, ...] | None:
    names = (
        f"{prefix}_revision",
        f"{prefix}_revision_id",
        f"{prefix}_stream_session_id",
        f"{prefix}_recovery_generation",
    )
    present = [record.get(name) is not None for name in names]
    if not any(present):
        return None
    revision = _integer(record.get(names[0]))
    revision_id = record.get(names[1])
    session = record.get(names[2])
    recovery = _integer(record.get(names[3]))
    market = record.get("canonical_market")
    if (
        revision is None
        or not isinstance(revision_id, str)
        or not revision_id
        or isinstance(session, bool)
        or not isinstance(session, (str, int))
        or session == ""
        or recovery is None
        or not isinstance(market, str)
        or not market
    ):
        raise ValueError("BOOK_WITNESS_FIELDS_INVALID")
    expected = make_book_revision_id(venue, market, session, recovery, revision)
    if revision_id != expected:
        raise ValueError("BOOK_WITNESS_REVISION_ID_MISMATCH")
    return venue.value, market, session, recovery, revision, revision_id


def _reference_digest(record: Mapping[str, Any], prefix: str) -> str | None:
    value = record.get(f"{prefix}_state_sha256")
    if value is None:
        return None
    if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
        raise ValueError("BOOK_WITNESS_DIGEST_INVALID")
    return value


def _quote_from_record(record: Mapping[str, Any]) -> _Quote | None:
    version = record.get("quote_version_id")
    policy = _text(record.get("policy_id"))
    if not isinstance(version, str) or not version or not policy:
        return None
    issues: set[str] = set()
    for name in ("maker_price", "risex_tick_size", "canonical_quantity", "target_notional_usd", "target_margin_bps"):
        if _decimal(record.get(name)) is None:
            issues.add(f"{name.upper()}_MISSING_OR_MALFORMED")
    risex_fee_rate = _decimal(record.get("risex_maker_fee_rate"))
    lighter_fee_rate = _decimal(record.get("lighter_taker_fee_rate"))
    if risex_fee_rate != Decimal("0.0001"):
        issues.add("FIXED_RISEX_FEE_RATE_MISMATCH")
    if lighter_fee_rate != Decimal("0"):
        issues.add("FIXED_LIGHTER_FEE_RATE_MISMATCH")
    try:
        risex_ref = _reference(record, "risex_book", Venue.RISEX)
        lighter_ref = _reference(record, "lighter_book", Venue.LIGHTER)
        risex_digest = _reference_digest(record, "risex_book")
        lighter_digest = _reference_digest(record, "lighter_book")
    except ValueError as exc:
        issues.add(str(exc))
        risex_ref = lighter_ref = None
        risex_digest = lighter_digest = None
    expiry = _integer(record.get("quote_expires_monotonic_ns"))
    if expiry is None:
        lifetime = _integer(record.get("quote_lifetime_ns"))
        created = _integer(record.get("quote_created_monotonic_ns"))
        if lifetime is not None and created is not None:
            expiry = created + lifetime
    return _Quote(
        policy_id=policy,
        version_id=version,
        market=_text(record.get("canonical_market")),
        direction=_text(record.get("direction")),
        target=_decimal(record.get("target_notional_usd")),
        margin=_decimal(record.get("target_margin_bps")),
        created=_integer(record.get("quote_created_monotonic_ns")),
        expiry=expiry,
        session=record.get("quote_stream_session_id", record.get("stream_session_id")),
        recovery=_integer(record.get("quote_recovery_generation", record.get("recovery_generation"))),
        hedge_session=record.get("hedge_stream_session_id"),
        hedge_recovery=_integer(record.get("hedge_recovery_generation")),
        maker_price=_decimal(record.get("maker_price")),
        tick=_decimal(record.get("risex_tick_size")),
        quantity=_decimal(record.get("canonical_quantity", record.get("quote_canonical_quantity"))),
        maker_order_id=_identity(record.get("maker_order_id")),
        risex_fee_rate=risex_fee_rate,
        lighter_fee_rate=lighter_fee_rate,
        risex_fee_source=_text(record.get("risex_fee_source")),
        lighter_fee_source=_text(record.get("lighter_fee_source")),
        risex_book_ref=risex_ref,
        lighter_book_ref=lighter_ref,
        risex_book_digest=risex_digest,
        lighter_book_digest=lighter_digest,
        issues=tuple(sorted(issues)),
    )


def _trade_from_record(
    record: Mapping[str, Any],
    contexts: Mapping[str, tuple[_Quote, ...]] | None = None,
) -> _Trade | None:
    key = record.get("trade_event_key")
    if not isinstance(key, str) or not key:
        return None
    maker, taker, issues = _resolve_trade_ids(record)
    policy_ids = record.get("eligible_policy_ids")
    parsed_policies = (
        frozenset(_text(value) for value in policy_ids if _text(value))
        if isinstance(policy_ids, (list, tuple))
        else frozenset()
    )
    side = _text(record.get("aggressor_side")).upper()
    if side not in {"BUY", "SELL"}:
        issues.add("AGGRESSOR_SIDE_MISSING_OR_MALFORMED")
    return _Trade(
        key=key,
        market=_text(record.get("canonical_market")),
        price=_decimal(record.get("canonical_price")),
        quantity=_decimal(record.get("canonical_quantity")),
        side=side,
        received=_integer(record.get("received_monotonic_ns", record.get("observed_monotonic_ns"))),
        session=record.get("stream_session_id"),
        recovery=_integer(record.get("recovery_generation")),
        maker_order_id=maker,
        taker_order_id=taker,
        policy_ids=parsed_policies,
        contexts=tuple(sorted((contexts or {}).items())),
        identity_issues=tuple(sorted(issues)),
    )


def _fill_from_record(record: Mapping[str, Any], quote: _Quote | None = None) -> _Fill | None:
    version = record.get("quote_version_id")
    if not isinstance(version, str) or not version:
        return None
    model = _text(record.get("fillability_model") or record.get("model")).upper() or STRICT_MODEL
    key = (model, version)
    raw_keys = record.get("qualifying_trade_event_keys")
    if raw_keys is None:
        raw_keys = record.get("trade_event_keys")
    issues: set[str] = set()
    if not isinstance(raw_keys, (list, tuple)):
        keys: tuple[str, ...] = ()
        issues.add("QUALIFYING_TRADE_KEYS_MISSING")
    else:
        keys = tuple(_text(value) for value in raw_keys if _text(value))
        if not keys or len(keys) != len(set(keys)):
            issues.add("QUALIFYING_TRADE_KEYS_DUPLICATE_OR_MISSING")
    return _Fill(
        key=key,
        policy_id=_text(record.get("policy_id")),
        version_id=version,
        market=_text(record.get("canonical_market")),
        direction=_text(record.get("direction")),
        margin=_decimal(record.get("target_margin_bps")),
        quantity=_decimal(record.get("canonical_quantity", record.get("quote_canonical_quantity"))),
        cumulative=_decimal(record.get("cumulative_eligible_quantity")),
        detected=_integer(record.get("would_fill_detected_monotonic_ns")),
        trade_keys=keys,
        record=record,
        quote=quote,
        issues=tuple(sorted(issues)),
    )


def _active_quote(quote: _Quote, trade: _Trade) -> bool:
    if quote.created is None or quote.expiry is None or trade.received is None:
        return False
    if not quote.market or quote.market != trade.market:
        return False
    if quote.direction != "RISEX_SELL_LIGHTER_BUY":
        return False
    if quote.target != Decimal("100") or quote.margin not in (Decimal("1"), Decimal("2")):
        return False
    if trade.received <= quote.created or trade.received >= quote.expiry:
        return False
    if quote.session is None or trade.session != quote.session or quote.recovery is None or trade.recovery != quote.recovery:
        return False
    if trade.side != "BUY":
        return False
    return quote.maker_price is not None and quote.tick is not None and quote.quantity is not None


def _strict_trade_for_quote(quote: _Quote, trade: _Trade) -> bool:
    if not _active_quote(quote, trade) or trade.price is None or quote.tick is None or quote.maker_price is None:
        return False
    if quote.tick <= 0 or trade.price % quote.tick != 0:
        return False
    return trade.price >= quote.maker_price + quote.tick


def _gap_matches(gap: Mapping[str, Any], record: Mapping[str, Any], *, venue: str) -> bool:
    if _text(gap.get("venue", gap.get("source_venue"))).upper() != venue:
        return False
    if _text(gap.get("canonical_market")) != _text(record.get("canonical_market")):
        return False
    gap_session = gap.get("stream_session_id")
    record_session = record.get("stream_session_id", record.get("expected_stream_session_id"))
    if gap_session not in (None, "unknown") and record_session is not None and gap_session != record_session:
        return False
    gap_recovery = _integer(gap.get("recovery_generation"))
    record_recovery = _integer(record.get("recovery_generation", record.get("expected_recovery_generation")))
    if gap_recovery is not None and record_recovery is not None and gap_recovery != record_recovery:
        return False
    start = _integer(gap.get("gap_start_monotonic_ns"))
    if start is None:
        return True
    end = gap.get("gap_end_monotonic_ns", _MISSING)
    left = _integer(record.get("interval_start"))
    right = _integer(record.get("interval_end"))
    if left is None or right is None:
        return True
    if right < left:
        return True
    if end is None or end is _MISSING:
        return right >= start
    parsed_end = _integer(end)
    if parsed_end is None or parsed_end < start:
        return True
    return not (parsed_end < left or start > right)


def _interval_record(*, market: str, venue: str, session: Any, recovery: Any, start: int | None, end: int | None) -> dict[str, Any]:
    return {
        "canonical_market": market,
        "venue": venue,
        "stream_session_id": session,
        "recovery_generation": recovery,
        "interval_start": start,
        "interval_end": end,
    }


def _contaminating_gaps(gaps: Iterable[Mapping[str, Any]], interval: Mapping[str, Any], *, venue: str) -> set[str]:
    reasons: set[str] = set()
    for gap in gaps:
        if _gap_matches(gap, interval, venue=venue):
            reason = _text(gap.get("reason")) or "DATA_GAP"
            if reason != "PUBLIC_SMOKE_STOPPED":
                reasons.add(reason)
    return reasons


def _book_key(book: Any) -> tuple[Any, ...]:
    return (
        book.venue.value,
        book.canonical_market,
        book.stream_session_id,
        book.recovery_generation,
        book.book_revision,
        book.book_revision_id,
    )


def _book_capture_rank(book: Any) -> tuple[Any, ...]:
    """Rank used by the capture contract before revision-id tie breaking."""

    return (
        book.received_monotonic_ns,
        book.book_revision,
        -1 if book.sequence is None else book.sequence,
    )


def _expected_ref_from_horizon(record: Mapping[str, Any]) -> tuple[Any, ...] | None:
    prefix = "lighter_book" if any(record.get(f"lighter_book_{field}") is not None for field in ("revision", "revision_id", "stream_session_id", "recovery_generation")) else "book"
    return _reference(record, prefix, Venue.LIGHTER)


def _stage_contract(metadata: Mapping[str, Any], mode: str, run_id: str | None) -> dict[str, Any]:
    """Validate the one explicit producer contract for SCAN-002.

    The producer shape is ``RUN_METADATA.metadata.scan_002``. Supporting
    aliases here would make an unrelated legacy record look like a completed
    stage, so absent or malformed stage metadata remains closed.
    """

    raw = metadata.get("scan_002")
    section = raw if isinstance(raw, Mapping) else {}
    missing: list[str] = []
    invalid: list[str] = []

    def required(name: str) -> Any:
        value = section.get(name, _MISSING)
        if value is _MISSING:
            missing.append(name)
            return None
        return value

    supplied_kind = required("stage_kind")
    if supplied_kind is None:
        stage_kind = "FIXTURE" if mode.upper() == "FIXTURE" else "CLOSED"
    else:
        stage_kind = _text(supplied_kind).upper()
        if stage_kind not in {"PUBLIC", "FIXTURE"}:
            invalid.append("stage_kind")
    if mode.upper() == "FIXTURE" and stage_kind != "FIXTURE":
        invalid.append("replay_mode_stage_kind_mismatch")
        stage_kind = "FIXTURE"

    stage_name = required("stage_name")
    if not isinstance(stage_name, str) or not stage_name:
        (missing if stage_name is None else invalid).append("stage_name")
    stage_run_id = section.get("run_id", run_id)
    if stage_run_id is None or not isinstance(stage_run_id, str) or not stage_run_id:
        missing.append("run_id")
        stage_run_id = None
    accepted_source = required("accepted_source")
    if not isinstance(accepted_source, str) or not accepted_source or accepted_source == "UNKNOWN":
        missing.append("accepted_source")
        accepted_source = None if accepted_source is _MISSING else accepted_source

    markets = required("canonical_markets")
    direction = required("direction")
    target_notionals = required("target_notionals_usd")
    target_margins = required("target_margins_bps")
    horizons = required("horizons_ms")
    if not _same_sequence(markets, ("BTC",)):
        invalid.append("canonical_markets")
    if direction != "RISEX_SELL_LIGHTER_BUY":
        invalid.append("direction")
    if not _same_sequence(target_notionals, (Decimal("100"),), decimals=True):
        invalid.append("target_notionals_usd")
    if not _same_sequence(target_margins, (Decimal("1"), Decimal("2")), decimals=True):
        invalid.append("target_margins_bps")
    if not _same_sequence(horizons, HORIZONS):
        invalid.append("horizons_ms")

    fees = required("fees")
    if not isinstance(fees, Mapping):
        missing.append("fees")
        fees = {}
    fee_values = {
        "risex_maker_fee_rate": fees.get("risex_maker_fee_rate", _MISSING),
        "risex_fee_tier": fees.get("risex_fee_tier", _MISSING),
        "risex_fee_provenance": fees.get("risex_fee_provenance", _MISSING),
        "lighter_taker_fee_rate": fees.get("lighter_taker_fee_rate", _MISSING),
        "lighter_taker_latency_ms": fees.get("lighter_taker_latency_ms", _MISSING),
        "lighter_fee_provenance": fees.get("lighter_fee_provenance", _MISSING),
    }
    for name, value in fee_values.items():
        if value is _MISSING:
            missing.append(f"fees.{name}")
    if _decimal(fee_values["risex_maker_fee_rate"]) != Decimal("0.0001"):
        invalid.append("fees.risex_maker_fee_rate")
    if _text(fee_values["risex_fee_tier"]).upper() != "TIER_1":
        invalid.append("fees.risex_fee_tier")
    if not _text(fee_values["risex_fee_provenance"]):
        invalid.append("fees.risex_fee_provenance")
    if _decimal(fee_values["lighter_taker_fee_rate"]) != Decimal("0"):
        invalid.append("fees.lighter_taker_fee_rate")
    if _decimal(fee_values["lighter_taker_latency_ms"]) != Decimal("300"):
        invalid.append("fees.lighter_taker_latency_ms")
    if not _text(fee_values["lighter_fee_provenance"]):
        invalid.append("fees.lighter_fee_provenance")

    interval = required("sample_interval")
    if not isinstance(interval, Mapping):
        missing.append("sample_interval")
        interval = {}
    sample_start = _integer(interval.get("start_monotonic_ns"))
    sample_end = _integer(interval.get("end_monotonic_ns"))
    if sample_start is None:
        missing.append("sample_interval.start_monotonic_ns")
    if sample_end is None:
        missing.append("sample_interval.end_monotonic_ns")
    if sample_start is not None and sample_end is not None and sample_end < sample_start:
        invalid.append("sample_interval")

    limits = required("limits")
    if not isinstance(limits, Mapping):
        missing.append("limits")
        limits = {}
    expected_limits = {
        "eligible_trade_limit": 250,
        "wall_clock_seconds": 1200,
        "record_cap": 1_000_000,
        "byte_cap": 4 * 1024 * 1024 * 1024,
    }
    for name, expected in expected_limits.items():
        value = limits.get(name, _MISSING)
        if value is _MISSING:
            missing.append(f"limits.{name}")
        elif _integer(value) != expected:
            invalid.append(f"limits.{name}")
    fill_count_stop = limits.get("fill_count_stop", _MISSING)
    if fill_count_stop is _MISSING:
        missing.append("limits.fill_count_stop")
    elif fill_count_stop is not None:
        invalid.append("limits.fill_count_stop")

    supplied_policy = section.get("policy_fingerprint", _MISSING)
    if supplied_policy is _MISSING:
        missing.append("policy_fingerprint")
    elif supplied_policy != _POLICY_FINGERPRINT:
        invalid.append("policy_fingerprint")

    stage_payload = {
        "stage_kind": stage_kind,
        "stage_name": stage_name,
        "run_id": stage_run_id,
        "accepted_source": accepted_source,
        "policy_fields": {
            "canonical_markets": markets,
            "direction": direction,
            "target_notionals_usd": target_notionals,
            "target_margins_bps": target_margins,
            "horizons_ms": horizons,
            "fees": {
                name: fee_values[name]
                for name in (
                    "risex_maker_fee_rate",
                    "risex_fee_tier",
                    "risex_fee_provenance",
                    "lighter_taker_fee_rate",
                    "lighter_taker_latency_ms",
                    "lighter_fee_provenance",
                )
            },
        },
        "sample_interval": {"start_monotonic_ns": sample_start, "end_monotonic_ns": sample_end},
        "limits": {name: limits.get(name) for name in (*expected_limits, "fill_count_stop")},
        "policy_fingerprint": _POLICY_FINGERPRINT,
    }
    computed_stage = _fingerprint(stage_payload)
    supplied_stage = section.get("stage_fingerprint", _MISSING)
    if supplied_stage is _MISSING:
        missing.append("stage_fingerprint")
    elif supplied_stage != computed_stage:
        invalid.append("stage_fingerprint")

    missing = sorted(set(missing))
    invalid = sorted(set(invalid))
    return {
        "stage_kind": stage_kind,
        "stage_name": stage_name if isinstance(stage_name, str) else None,
        "run_id": stage_run_id,
        "accepted_source": accepted_source,
        "required": {
            "canonical_markets": markets,
            "direction": direction,
            "target_notionals_usd": target_notionals,
            "target_margins_bps": target_margins,
            "horizons_ms": horizons,
            "fees": dict(fees),
            "sample_interval": dict(interval),
            "limits": dict(limits),
        },
        "missing_fields": missing,
        "invalid_fields": invalid,
        "policy_fingerprint": None if supplied_policy is _MISSING else supplied_policy,
        "stage_fingerprint": None if supplied_stage is _MISSING else supplied_stage,
        "computed_policy_fingerprint": _POLICY_FINGERPRINT,
        "computed_stage_fingerprint": computed_stage,
        "valid": not missing and not invalid and stage_kind == "PUBLIC" and isinstance(stage_name, str) and bool(stage_name),
        "synthetic": stage_kind == "FIXTURE",
    }


def _stats_for_units(values: list[Decimal]) -> dict[str, Any]:
    return _stats(values)


def _score_for_episode(episode: Mapping[str, Any], horizon: int) -> Decimal | None:
    value = episode.get("edges", {}).get(horizon)
    return value if isinstance(value, Decimal) else None


def _markout_for_episode(episode: Mapping[str, Any], horizon: int) -> Decimal | None:
    value = episode.get("markouts", {}).get(horizon)
    return value if isinstance(value, Decimal) else None


def _evaluate_horizon(
    fill: _Fill,
    quote: _Quote,
    horizon: Mapping[str, Any] | None,
    *,
    selected_book: Any | None,
    latest_book: Any | None,
    selected_key: tuple[Any, ...] | None,
    latest_key: tuple[Any, ...] | None,
    latest_tie: bool = False,
) -> tuple[Decimal | None, set[str], dict[str, Any]]:
    issues: set[str] = set()
    detail: dict[str, Any] = {
        "horizon_ms": None if horizon is None else horizon.get("horizon_ms"),
        "outcome": None if horizon is None else horizon.get("outcome"),
        "selected_book_revision_id": None if selected_book is None else selected_book.book_revision_id,
        "latest_book_revision_id": None if latest_book is None else latest_book.book_revision_id,
        "selected_book_received_monotonic_ns": None if selected_book is None else selected_book.received_monotonic_ns,
        "latest_book_identity_ambiguous": latest_tie,
        "recomputed_entry_edge_usd": None,
        "recomputed_conditional_markout_usd": None,
        "issues": [],
    }
    if horizon is None:
        issues.add("HORIZON_RECORD_MISSING")
        detail["issues"] = sorted(issues)
        return None, issues, detail
    horizon_ms = _integer(horizon.get("horizon_ms"))
    detected = fill.detected
    deadline = _integer(horizon.get("horizon_deadline_monotonic_ns"))
    if horizon_ms not in HORIZONS:
        issues.add("HORIZON_UNSUPPORTED")
    if detected is None or deadline is None or horizon_ms is None or deadline != detected + horizon_ms * 1_000_000:
        issues.add("HORIZON_DEADLINE_INVALID")
    if _text(horizon.get("outcome")) != "HEDGE_FULL":
        issues.add(f"HEDGE_OUTCOME_{_text(horizon.get('outcome')) or 'MISSING'}")
    if selected_book is None or selected_key is None:
        issues.add("BOOK_WITNESS_MISSING")
    if latest_book is None or latest_key is None:
        issues.add("LATEST_BOOK_BEFORE_DEADLINE_MISSING")
    if latest_key is not None and selected_key is not None and selected_key != latest_key:
        issues.add("SELECTED_BOOK_NOT_LATEST_BEFORE_DEADLINE")
    if latest_tie:
        issues.add("LATEST_BOOK_IDENTITY_AMBIGUOUS")
    if selected_book is not None:
        if selected_book.received_monotonic_ns > (deadline if deadline is not None else -1):
            issues.add("BOOK_RECEIVED_AFTER_DEADLINE")
        if not selected_book.is_sequence_healthy:
            issues.add("SELECTED_BOOK_UNHEALTHY")
        if not selected_book.fresh:
            issues.add("SELECTED_BOOK_STALE")
        freshness_max_age = _integer(horizon.get("freshness_max_age_ns"))
        if freshness_max_age is None:
            issues.add("FRESHNESS_MAX_AGE_MISSING")
        elif deadline is None or deadline - selected_book.received_monotonic_ns > freshness_max_age:
            issues.add("SELECTED_BOOK_STALE_AGE")
        expected_session = horizon.get("expected_stream_session_id")
        expected_recovery = _integer(horizon.get("expected_recovery_generation"))
        if expected_session is None or expected_recovery is None:
            issues.add("EXPECTED_BOOK_IDENTITY_MISSING")
        elif selected_book.stream_session_id != expected_session or selected_book.recovery_generation != expected_recovery:
            issues.add("SELECTED_BOOK_IDENTITY_MISMATCH")
        if quote.hedge_session is not None and expected_session != quote.hedge_session:
            issues.add("EXPECTED_BOOK_SESSION_QUOTE_MISMATCH")
        if quote.hedge_recovery is not None and expected_recovery != quote.hedge_recovery:
            issues.add("EXPECTED_BOOK_RECOVERY_QUOTE_MISMATCH")
        for field, actual in (
            ("book_received_monotonic_ns", selected_book.received_monotonic_ns),
            ("book_stream_session_id", selected_book.stream_session_id),
            ("book_recovery_generation", selected_book.recovery_generation),
            ("book_revision", selected_book.book_revision),
            ("book_revision_id", selected_book.book_revision_id),
        ):
            supplied = horizon.get(field)
            if supplied is not None and str(supplied) != str(actual):
                issues.add(f"{field.upper()}_MISMATCH")
        supplied_digest = horizon.get("book_state_sha256")
        if supplied_digest is None:
            issues.add("BOOK_STATE_DIGEST_MISSING")
        elif supplied_digest != selected_book.state_sha256:
            issues.add("BOOK_STATE_DIGEST_MISMATCH")
    q = quote.quantity
    maker_price = quote.maker_price
    if q is None or q <= 0 or maker_price is None or maker_price <= 0:
        issues.add("QUOTE_EXACT_FIELDS_INVALID")
    if quote.risex_fee_rate is None or quote.lighter_fee_rate is None:
        issues.add("FEE_FIELDS_MISSING_OR_MALFORMED")
    if not issues and selected_book is not None and q is not None and maker_price is not None:
        try:
            vwap = exact_vwap(
                Side.BUY,
                q,
                selected_book.bids,
                selected_book.asks,
            )
        except (TypeError, ValueError, ArithmeticError):
            issues.add("EXACT_Q_VWAP_RECOMPUTE_FAILED")
        else:
            if vwap.filled_quantity != q or not vwap.is_executable:
                issues.add("EXACT_Q_NOT_FULL")
            if _decimal(horizon.get("requested_quantity")) != q:
                issues.add("REQUESTED_QUANTITY_MISMATCH")
            if _decimal(horizon.get("filled_quantity")) != vwap.filled_quantity:
                issues.add("FILLED_QUANTITY_MISMATCH")
            if _decimal(horizon.get("notional_usd")) != vwap.notional_usd:
                issues.add("HEDGE_NOTIONAL_MISMATCH")
            if vwap.price is None or _decimal(horizon.get("vwap_price")) != vwap.price:
                issues.add("HEDGE_VWAP_MISMATCH")
            maker_notional = q * maker_price
            fees = maker_notional * (quote.risex_fee_rate or Decimal("0")) + vwap.notional_usd * (quote.lighter_fee_rate or Decimal("0"))
            if quote.direction == "RISEX_SELL_LIGHTER_BUY":
                edge = maker_notional - vwap.notional_usd - fees
            else:
                edge = vwap.notional_usd - maker_notional - fees
            detail["recomputed_entry_edge_usd"] = edge
            supplied_edge = _decimal(horizon.get("entry_edge_usd"))
            if supplied_edge is None or supplied_edge != edge:
                issues.add("ENTRY_EDGE_ARITHMETIC_MISMATCH")
            supplied_markout = _decimal(horizon.get("conditional_markout_usd"))
            detail["stored_entry_edge_usd"] = supplied_edge
            detail["stored_conditional_markout_usd"] = supplied_markout
            # The markout is recomputed from this edge and the episode h0 by
            # the caller.  The persisted conditional value is diagnostic only.
            detail["computed_edge"] = edge
    detail["issues"] = sorted(issues)
    if issues or detail.get("computed_edge") is None:
        return None, issues, detail
    return detail["computed_edge"], issues, detail


def _classify_pair(unit: _Unit) -> str:
    comparisons = unit.pair_comparisons
    if not comparisons:
        return "EFFECTIVE_LEVEL_UNRESOLVED"
    if any(item["classification"] == "UNRESOLVED" for item in comparisons):
        return "EFFECTIVE_LEVEL_UNRESOLVED"
    separations = [item["signed_tick_separation"] for item in comparisons]
    if any(value < 0 for value in separations):
        if any(value >= 0 for value in separations):
            return "MIXED_EFFECTIVE_LEVEL"
        return "NOMINAL_WIDER_REVERSED"
    if any(value == 0 for value in separations):
        return "EFFECTIVE_PRICE_COLLISION"
    if all(value >= 1 for value in separations):
        return "DISTINCT_EFFECTIVE_LEVEL"
    return "EFFECTIVE_LEVEL_UNRESOLVED"


def _pair_unit(unit: _Unit) -> None:
    narrow_by_event = unit.active_by_arm.get("1", {})
    wide_by_event = unit.active_by_arm.get("2", {})
    for event_key in sorted(set(narrow_by_event) | set(wide_by_event)):
        narrow = narrow_by_event.get(event_key, ())
        wide = wide_by_event.get(event_key, ())
        if not narrow or not wide:
            unit.pair_comparisons.append({"event_key": event_key, "classification": "UNRESOLVED"})
            continue
        if len(narrow) != 1 or len(wide) != 1:
            unit.pair_comparisons.append({"event_key": event_key, "classification": "UNRESOLVED"})
            continue
        left, right = narrow[0], wide[0]
        if left.maker_price is None or right.maker_price is None or left.tick is None or right.tick is None or left.tick != right.tick or left.maker_price % left.tick != 0 or right.maker_price % right.tick != 0:
            unit.pair_comparisons.append({"event_key": event_key, "classification": "UNRESOLVED"})
            continue
        signed = right.maker_price - left.maker_price
        if left.direction == "RISEX_BUY_LIGHTER_SELL":
            signed = left.maker_price - right.maker_price
        signed_ticks = signed / left.tick
        unit.pair_comparisons.append(
            {
                "event_key": event_key,
                "narrow_quote_version_id": left.version_id,
                "wide_quote_version_id": right.version_id,
                "narrow_maker_price": str(left.maker_price),
                "wide_maker_price": str(right.maker_price),
                "signed_price_separation": str(signed),
                "signed_tick_separation": signed_ticks,
                "classification": "COMPARABLE",
            }
        )
    unit.pair_classification = _classify_pair(unit)


def _select_arm(arm_passes: Mapping[str, bool], selector_scores: Mapping[str, Mapping[str, Any]]) -> str | None:
    """Select the qualifying arm from common-denominator 300ms sums."""

    if arm_passes.get("1") and arm_passes.get("2"):
        first = Decimal(selector_scores["1"]["sum_300ms"])
        second = Decimal(selector_scores["2"]["sum_300ms"])
        return "1" if first >= second else "2"
    if arm_passes.get("1"):
        return "1"
    if arm_passes.get("2"):
        return "2"
    return None


def _build_source(source: str | Path | Iterable[Mapping[str, Any]]) -> tuple[Callable[[], Iterator[Mapping[str, Any]]], bool]:
    if isinstance(source, (str, Path)):
        path = Path(source)
        return lambda: iter_records(path), True
    rows = tuple(dict(record) for record in source if isinstance(record, Mapping))
    return lambda: iter(rows), False


def _stream(
    factory: Callable[[], Iterator[Mapping[str, Any]]],
    require_indices: bool,
    integrity: list[str],
    *,
    validate_terminal: bool = True,
) -> Iterator[Mapping[str, Any]]:
    previous: int | None = None
    terminal = False
    for record in factory():
        if not isinstance(record, Mapping):
            integrity.append("RECORD_NOT_MAPPING")
            continue
        if require_indices:
            index = record.get("record_index")
            if isinstance(index, bool) or not isinstance(index, int) or index < 0:
                integrity.append("INVALID_RECORD_INDEX")
            elif previous is not None and index != previous + 1:
                integrity.append("NON_CONTIGUOUS_RECORD_INDEX")
            elif previous is None and index != 0:
                integrity.append("MISSING_RECORD_INDEX")
            if isinstance(index, int) and not isinstance(index, bool):
                previous = index
        kind = record.get("kind")
        if validate_terminal and terminal:
            integrity.append("RECORD_AFTER_TERMINAL")
        if kind in {"RUN_STOP", "RUN_FAILED"}:
            if validate_terminal and terminal:
                integrity.append("MULTIPLE_TERMINAL_MARKERS")
            terminal = True
        yield record
    if validate_terminal and require_indices and not terminal:
        integrity.append("MISSING_TERMINAL_MARKER")


def _source_metadata(
    factory: Callable[[], Iterator[Mapping[str, Any]]],
    require_indices: bool,
    integrity: list[str],
    *,
    retain_books: bool,
) -> tuple[dict[str, Any], str, str | None, list[Mapping[str, Any]]]:
    metadata: dict[str, Any] = {}
    mode = "OBSERVATIONAL"
    run_id: str | None = None
    compact: list[Mapping[str, Any]] = []
    for record in _stream(factory, require_indices, integrity):
        kind = record.get("kind")
        if kind == "RUN_METADATA" and isinstance(record.get("metadata"), Mapping):
            metadata = dict(record["metadata"])
            if metadata.get("evidence_mode") is not None:
                mode = _text(metadata["evidence_mode"]).upper()
            if metadata.get("run_id") is not None:
                run_id = _text(metadata["run_id"])
        elif kind == "REPLAY_MODE":
            mode = "FIXTURE"
        # A path is reread for the actual compact collector below.  Keeping
        # non-BOOK rows here would recreate the legacy growing allocation.
        if retain_books:
            compact.append(record)
    return metadata, mode, run_id, compact


def _replay_books(
    factory: Callable[[], Iterator[Mapping[str, Any]]],
    require_indices: bool,
    integrity: list[str],
    wanted: Mapping[tuple[Any, ...], Decimal | None],
    horizon_requests: Mapping[tuple[str, str, int], tuple[str, int, str | int | None, int | None, Decimal | None]],
) -> tuple[
    dict[tuple[Any, ...], _CompactBook],
    dict[tuple[str, str, int], _CompactBook],
    dict[tuple[str, str, int], tuple[Any, ...]],
    dict[tuple[str, str, int], tuple[tuple[Any, ...], ...]],
    list[str],
    int,
]:
    reconstructor = BookRevisionReconstructor()
    referenced: dict[tuple[Any, ...], _CompactBook] = {}
    latest: dict[tuple[str, str, int], _CompactBook] = {}
    latest_key: dict[tuple[str, str, int], tuple[Any, ...]] = {}
    latest_ties: dict[tuple[str, str, int], set[tuple[Any, ...]]] = {}
    errors: list[str] = []
    count = 0
    for record in _stream(factory, require_indices, integrity, validate_terminal=False):
        kind = record.get("kind")
        if kind == "DATA_GAP":
            try:
                venue = Venue(record.get("venue", record.get("source_venue")))
            except (TypeError, ValueError):
                continue
            session = record.get("stream_session_id")
            recovery = _integer(record.get("recovery_generation"))
            market = _text(record.get("canonical_market"))
            if venue in (Venue.RISEX, Venue.LIGHTER) and isinstance(session, (str, int)) and not isinstance(session, bool) and session != "" and recovery is not None and market:
                reconstructor.mark_gap(venue=venue, market=market, session=session, recovery=recovery)
            continue
        if kind != "BOOK":
            continue
        count += 1
        try:
            book = reconstructor.append(record)
        except BookRevisionChainError as exc:
            errors.append(exc.reason)
            continue
        key = _book_key(book)
        state_digest: str | None = None
        if key in wanted:
            state_digest = book_state_sha256(book)
            referenced[key] = _compact_book(book, wanted[key], state_sha256_value=state_digest)
        if book.venue is not Venue.LIGHTER:
            continue
        for request_key, (market, deadline, _expected_session, _expected_recovery, quantity) in horizon_requests.items():
            if market != book.canonical_market or book.received_monotonic_ns > deadline:
                continue
            current = latest.get(request_key)
            if current is None or _book_capture_rank(book) > _book_capture_rank(current):
                if state_digest is None:
                    state_digest = book_state_sha256(book)
                latest[request_key] = _compact_book(book, quantity, state_sha256_value=state_digest)
                latest_key[request_key] = key
                latest_ties.pop(request_key, None)
            elif _book_capture_rank(book) == _book_capture_rank(current) and key != latest_key.get(request_key):
                latest_ties.setdefault(request_key, {latest_key[request_key]}).add(key)
    return (
        referenced,
        latest,
        latest_key,
        {request_key: tuple(sorted(keys, key=str)) for request_key, keys in latest_ties.items()},
        errors,
        count,
    )


def _metadata_from_compact(compact: Iterable[Mapping[str, Any]]) -> tuple[dict[str, Any], str, str | None]:
    metadata: dict[str, Any] = {}
    mode = "OBSERVATIONAL"
    run_id: str | None = None
    for record in compact:
        if record.get("kind") == "RUN_METADATA" and isinstance(record.get("metadata"), Mapping):
            metadata = dict(record["metadata"])
            mode = _text(metadata.get("evidence_mode", mode)).upper()
            run_id = None if metadata.get("run_id") is None else _text(metadata["run_id"])
        elif record.get("kind") == "REPLAY_MODE":
            mode = "FIXTURE"
    return metadata, mode, run_id


def build_offline_evaluation(source: str | Path | Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Build the bounded SCAN-002 numerical contract from evidence.

    ``source`` may be an evidence JSONL path or a deterministic record
    iterable.  A path is preferred by the report because it permits two
    bounded passes without retaining physical BOOK rows.
    """

    factory, is_path = _build_source(source)
    integrity: list[str] = []
    metadata, mode, run_id, compact = _source_metadata(
        factory,
        is_path,
        integrity,
        retain_books=not is_path,
    )
    if not is_path:
        metadata, mode, run_id = _metadata_from_compact(compact)
        source_factory = lambda: iter(compact)
    else:
        source_factory = factory
    stage = _stage_contract(metadata, mode, run_id)

    # Only the latest active version per policy is retained.  A fill stores a
    # snapshot of that quote, so later nominal replacements cannot rewrite its
    # episode and no quote-history archive is needed.
    active_by_policy: dict[str, _Quote] = {}
    active_by_version: dict[str, _Quote] = {}
    trades: dict[str, _Trade] = {}
    trade_conflicts: set[str] = set()
    eligible_keys: list[str] = []
    fills: dict[tuple[str, str], _Fill] = {}
    duplicate_fills = 0
    fill_conflicts: set[tuple[str, str]] = set()
    horizons: dict[tuple[str, str, int], Mapping[str, Any]] = {}
    duplicate_horizons = 0
    horizon_conflicts: set[tuple[str, str, int]] = set()
    gaps: list[Mapping[str, Any]] = []
    reference_quantities: dict[tuple[Any, ...], Decimal | None] = {}
    horizon_requests: dict[tuple[str, str, int], tuple[str, int, str | int | None, int | None, Decimal | None]] = {}
    record_counts: defaultdict[str, int] = defaultdict(int)

    for record in compact if not is_path else _stream(source_factory, False, integrity, validate_terminal=False):
        kind = record.get("kind")
        record_counts[_text(kind).lower()] += 1
        if kind == "RUN_FAILED":
            integrity.append("RUN_FAILED_TERMINAL")
        elif kind == "RUN_STOP" and record.get("fatal_reason") not in (None, ""):
            integrity.append("RUN_STOP_FATAL")
        if kind == "QUOTE":
            quote = _quote_from_record(record)
            if quote is None:
                integrity.append("QUOTE_IDENTITY_MISSING")
                continue
            previous = active_by_policy.pop(quote.policy_id, None)
            if previous is not None:
                active_by_version.pop(previous.version_id, None)
            if (
                _text(record.get("outcome")) == "QUOTE_ACTIVE"
                and quote.market == "BTC"
                and quote.direction == "RISEX_SELL_LIGHTER_BUY"
                and quote.target == Decimal("100")
                and quote.margin in (Decimal("1"), Decimal("2"))
            ):
                if quote.version_id in active_by_version:
                    integrity.append("DUPLICATE_ACTIVE_QUOTE_VERSION")
                elif len(active_by_version) >= _MAX_QUOTES:
                    integrity.append("ACTIVE_QUOTE_CAPACITY")
                else:
                    active_by_policy[quote.policy_id] = quote
                    active_by_version[quote.version_id] = quote
        elif kind == "RISEX_TRADE":
            if record.get("eligible_trade") is not True:
                continue
            trade = _trade_from_record(record)
            if trade is None:
                integrity.append("ELIGIBLE_TRADE_IDENTITY_MISSING")
                continue
            contexts: dict[str, tuple[_Quote, ...]] = {}
            for arm in ("1", "2"):
                candidates = tuple(
                    quote
                    for quote in active_by_policy.values()
                    if _canonical_decimal(quote.margin) == arm
                    and _active_quote(quote, trade)
                    and (not trade.policy_ids or quote.policy_id in trade.policy_ids)
                )
                contexts[arm] = tuple(sorted(candidates, key=lambda item: (item.created or -1, item.version_id)))
            trade = _trade_from_record(record, contexts)
            if trade is None:
                integrity.append("ELIGIBLE_TRADE_IDENTITY_MISSING")
                continue
            old = trades.get(trade.key)
            if old is not None:
                if old != trade:
                    trade_conflicts.add(trade.key)
                    integrity.append("DUPLICATE_ELIGIBLE_TRADE_CONFLICT")
                continue
            if len(eligible_keys) >= _MAX_ELIGIBLE_TRADES:
                integrity.append("ELIGIBLE_TRADE_STOP_OVERRUN")
            else:
                trades[trade.key] = trade
                eligible_keys.append(trade.key)
        elif kind == "WOULD_FILL":
            version = _text(record.get("quote_version_id"))
            quote = active_by_version.get(version)
            if quote is None:
                raw_trade_keys = record.get("qualifying_trade_event_keys")
                if raw_trade_keys is None:
                    raw_trade_keys = record.get("trade_event_keys")
                recovered: dict[str, _Quote] = {}
                if isinstance(raw_trade_keys, (list, tuple)):
                    for raw_key in raw_trade_keys:
                        trade = trades.get(_text(raw_key))
                        if trade is None:
                            continue
                        for arm in ("1", "2"):
                            for candidate in trade.arm_context(arm):
                                if candidate.version_id == version:
                                    recovered[candidate.version_id] = candidate
                if len(recovered) == 1:
                    quote = next(iter(recovered.values()))
            fill = _fill_from_record(record, quote)
            if fill is None:
                integrity.append("WOULD_FILL_IDENTITY_MISSING")
                continue
            if fill.key in fills:
                duplicate_fills += 1
                if _tie(fills[fill.key].record) != _tie(record):
                    fill_conflicts.add(fill.key)
                    integrity.append("DUPLICATE_WOULD_FILL_CONFLICT")
            elif len(fills) >= _MAX_FILLS:
                integrity.append("FILL_CAPACITY")
            else:
                fills[fill.key] = fill
        elif kind == "HEDGE_HORIZON":
            model = _text(record.get("fillability_model") or record.get("model")).upper() or STRICT_MODEL
            version = record.get("quote_version_id")
            horizon = _integer(record.get("horizon_ms"))
            if not isinstance(version, str) or not version or horizon is None:
                integrity.append("HORIZON_IDENTITY_MISSING")
                continue
            key = (model, version, horizon)
            if key in horizons:
                duplicate_horizons += 1
                if _tie(horizons[key]) != _tie(record):
                    horizon_conflicts.add(key)
                    integrity.append("DUPLICATE_HORIZON_CONFLICT")
            elif len(horizons) >= _MAX_HORIZONS:
                integrity.append("HORIZON_CAPACITY")
            else:
                horizons[key] = record
            if horizon in HORIZONS:
                try:
                    reference = _expected_ref_from_horizon(record)
                except ValueError as exc:
                    integrity.append(str(exc))
                    reference = None
                if reference is not None:
                    quantity = _decimal(record.get("requested_quantity"))
                    previous_quantity = reference_quantities.get(reference)
                    if reference not in reference_quantities or (
                        quantity is not None
                        and (previous_quantity is None or quantity > previous_quantity)
                    ):
                        reference_quantities[reference] = quantity
                detected = _integer(record.get("would_fill_detected_monotonic_ns"))
                deadline = _integer(record.get("horizon_deadline_monotonic_ns"))
                if detected is not None and deadline is not None:
                    horizon_requests[key] = (
                        _text(record.get("canonical_market")),
                        deadline,
                        record.get("expected_stream_session_id"),
                        _integer(record.get("expected_recovery_generation")),
                        _decimal(record.get("requested_quantity")),
                    )
        elif kind == "DATA_GAP":
            if len(gaps) >= _MAX_GAPS:
                integrity.append("GAP_CAPACITY")
            else:
                gaps.append(record)

    interval = stage["required"].get("sample_interval")
    if isinstance(interval, Mapping):
        interval_start = _integer(interval.get("start_monotonic_ns"))
        interval_end = _integer(interval.get("end_monotonic_ns"))
        if interval_start is not None and interval_end is not None and interval_end - interval_start > 1_200 * 1_000_000_000:
            integrity.append("STAGE_WALL_CLOCK_OVERRUN")
    record_total = sum(record_counts.values())
    if record_total > 1_000_000:
        integrity.append("RECORD_CAP_EXCEEDED")
    if is_path and Path(source).stat().st_size > 4 * 1024 * 1024 * 1024:
        integrity.append("BYTE_CAP_EXCEEDED")

    referenced, latest_books, latest_keys, latest_ties, book_errors, book_count = _replay_books(
        source_factory,
        False,
        integrity,
        reference_quantities,
        horizon_requests,
    )
    integrity.extend(book_errors)

    # The graph is intentionally built from every eligible trade before any
    # gap, horizon, or economic filtering.  A cumulative fill is a bridge,
    # so all of its trade keys share one dependence component.
    graph = _UnionFind()
    for key in eligible_keys:
        graph.add(f"event:{key}")
        trade = trades[key]
        if trade.maker_order_id is not None:
            graph.union(
                f"event:{key}",
                f"order:{trade.market}:{trade.maker_order_id[0]}:{trade.maker_order_id[1]}",
            )
        if trade.taker_order_id is not None:
            graph.union(
                f"event:{key}",
                f"order:{trade.market}:{trade.taker_order_id[0]}:{trade.taker_order_id[1]}",
            )
    for fill in fills.values():
        event_nodes = [f"event:{key}" for key in fill.trade_keys if key in trades]
        for node in event_nodes[1:]:
            graph.union(event_nodes[0], node)

    units_by_root: dict[str, _Unit] = {}
    for key in sorted(eligible_keys):
        root = graph.find(f"event:{key}")
        unit = units_by_root.setdefault(root, _Unit(unit_id=f"UNIT-{len(units_by_root) + 1:05d}"))
        unit.trade_keys.append(key)
        unit.identity_issues.update(trades[key].identity_issues)
        if key in trade_conflicts:
            unit.identity_issues.add("DUPLICATE_ELIGIBLE_TRADE_CONFLICT")
    for fill in fills.values():
        roots = {graph.find(f"event:{key}") for key in fill.trade_keys if key in trades}
        if not roots:
            unit = _Unit(unit_id=f"UNRESOLVED-{len(units_by_root) + 1:05d}")
            unit.identity_issues.update(fill.issues)
            unit.identity_issues.add("FILL_TRADE_UNIT_UNRESOLVED")
            unit.fills.append(fill)
            units_by_root[f"unresolved:{fill.key}"] = unit
            continue
        if len(roots) > 1:
            # This is defensive: the union above should bridge all keys.  It
            # also makes a malformed graph visible rather than silently
            # assigning a fill to one of several components.
            unit = _Unit(unit_id=f"UNRESOLVED-{len(units_by_root) + 1:05d}")
            unit.identity_issues.add("FILL_TRADE_UNIT_DISCONNECTED")
            unit.fills.append(fill)
            units_by_root[f"unresolved:{fill.key}"] = unit
            continue
        unit = units_by_root.setdefault(next(iter(roots)), _Unit(unit_id=f"UNIT-{len(units_by_root) + 1:05d}"))
        unit.fills.append(fill)
        unit.identity_issues.update(fill.issues)
        if fill.key in fill_conflicts:
            unit.identity_issues.add("DUPLICATE_WOULD_FILL_CONFLICT")
        for key in fill.trade_keys:
            if key not in trades:
                unit.identity_issues.add("FILL_REFERENCES_MISSING_ELIGIBLE_TRADE")

    strict_fills = {key: fill for key, fill in fills.items() if key[0] == STRICT_MODEL}
    unit_by_trade_key = {
        trade_key: unit
        for unit in units_by_root.values()
        for trade_key in unit.trade_keys
    }
    strict_fill_versions = {fill.version_id for fill in strict_fills.values()}
    qualifying_by_version: dict[str, tuple[_Quote, list[str], Decimal]] = {}
    for trade_key, trade in trades.items():
        for arm in ("1", "2"):
            for quote in trade.arm_context(arm):
                if quote.version_id in strict_fill_versions:
                    continue
                if not _strict_trade_for_quote(quote, trade):
                    continue
                previous = qualifying_by_version.get(quote.version_id)
                if previous is None:
                    qualifying_by_version[quote.version_id] = (
                        quote,
                        [trade_key],
                        trade.quantity or Decimal("0"),
                    )
                else:
                    previous[1].append(trade_key)
                    qualifying_by_version[quote.version_id] = (
                        previous[0],
                        previous[1],
                        previous[2] + (trade.quantity or Decimal("0")),
                    )
    for quote, trade_keys, quantity in qualifying_by_version.values():
        if quote.quantity is None or quantity < quote.quantity:
            continue
        for trade_key in trade_keys:
            unit = unit_by_trade_key.get(trade_key)
            if unit is not None:
                unit.episode_issues.add("STRICT_EPISODE_MISSING")
    unit_episodes: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for unit in units_by_root.values():
        for fill in unit.fills:
            if fill.key not in strict_fills:
                continue
            quote = fill.quote
            if quote is None:
                unit.identity_issues.add("QUOTE_VERSION_MISSING")
                unit.episode_issues.add("STRICT_EPISODE_UNRESOLVED")
                continue
            unit.identity_issues.update(quote.issues)
            if fill.policy_id and fill.policy_id != quote.policy_id:
                unit.identity_issues.add("FILL_POLICY_ID_MISMATCH")
            if fill.market and fill.market != quote.market:
                unit.identity_issues.add("FILL_MARKET_MISMATCH")
            if fill.direction and fill.direction != quote.direction:
                unit.identity_issues.add("FILL_DIRECTION_MISMATCH")
            if fill.margin is not None and fill.margin != quote.margin:
                unit.identity_issues.add("FILL_MARGIN_MISMATCH")
            if fill.quantity is not None and quote.quantity is not None and fill.quantity != quote.quantity:
                unit.identity_issues.add("FILL_QUANTITY_MISMATCH")
            if quote.margin not in (Decimal("1"), Decimal("2")) or quote.direction != "RISEX_SELL_LIGHTER_BUY" or quote.target != Decimal("100"):
                unit.identity_issues.add("FIXED_POLICY_SCOPE_MISMATCH")
                continue
            if not fill.trade_keys:
                unit.episode_issues.add("STRICT_EPISODE_INCOMPLETE")
                continue
            quantity_sum = Decimal("0")
            for key in fill.trade_keys:
                trade = trades.get(key)
                if trade is None:
                    unit.episode_issues.add("STRICT_EPISODE_TRADE_MISSING")
                    continue
                if trade.quantity is not None:
                    quantity_sum += trade.quantity
                if not any(
                    candidate.version_id == quote.version_id
                    for candidate in trade.arm_context(_canonical_decimal(quote.margin))
                ):
                    unit.episode_issues.add("STRICT_EPISODE_QUOTE_CONTEXT_MISSING")
                if not _strict_trade_for_quote(quote, trade):
                    unit.episode_issues.add("STRICT_EPISODE_TRADE_NOT_STRICT")
            if fill.quantity is None or fill.quantity <= 0 or fill.cumulative is None or fill.cumulative < fill.quantity:
                unit.episode_issues.add("STRICT_EPISODE_EXACT_Q_INVALID")
            if fill.cumulative is not None and quantity_sum != fill.cumulative:
                unit.episode_issues.add("CUMULATIVE_FILL_BRIDGE_ARITHMETIC_MISMATCH")
            if fill.detected is None:
                unit.episode_issues.add("STRICT_EPISODE_DETECTION_MISSING")
            elif quote.created is None or fill.detected <= quote.created or (quote.expiry is not None and fill.detected >= quote.expiry):
                unit.episode_issues.add("STRICT_EPISODE_QUOTE_INTERVAL_INVALID")
            fill_gap_interval = _interval_record(
                market=fill.market or quote.market,
                venue="RISEX",
                session=quote.session,
                recovery=quote.recovery,
                start=quote.created,
                end=fill.detected,
            )
            unit.gap_issues.update(_contaminating_gaps(gaps, fill_gap_interval, venue="RISEX"))
            episodes: dict[int, dict[str, Any]] = {}
            for horizon_ms in HORIZONS:
                horizon = horizons.get((STRICT_MODEL, fill.version_id, horizon_ms))
                request_key = (STRICT_MODEL, fill.version_id, horizon_ms)
                expected_ref = None
                if horizon is not None:
                    try:
                        expected_ref = _expected_ref_from_horizon(horizon)
                    except ValueError as exc:
                        unit.episode_issues.add(str(exc))
                selected = referenced.get(expected_ref) if expected_ref is not None else None
                selected_key = expected_ref
                latest = latest_books.get(request_key)
                latest_key = latest_keys.get(request_key)
                edge, horizon_issues, detail = _evaluate_horizon(
                    fill,
                    quote,
                    horizon,
                    selected_book=selected,
                    latest_book=latest,
                    selected_key=selected_key,
                    latest_key=latest_key,
                    latest_tie=bool(latest_ties.get(request_key)),
                )
                if horizon is not None and fill.detected is not None:
                    expected_session = horizon.get("expected_stream_session_id", quote.hedge_session)
                    expected_recovery = horizon.get("expected_recovery_generation", quote.hedge_recovery)
                    interval = _interval_record(
                        market=fill.market or quote.market,
                        venue="LIGHTER",
                        session=expected_session,
                        recovery=expected_recovery,
                        start=fill.detected,
                        end=_integer(horizon.get("horizon_deadline_monotonic_ns")),
                    )
                    unit.gap_issues.update(_contaminating_gaps(gaps, interval, venue="LIGHTER"))
                if horizon_issues:
                    unit.episode_issues.update(horizon_issues)
                episodes[horizon_ms] = {"edge": edge, "detail": detail}
            h0 = episodes.get(0, {}).get("edge")
            if h0 is None:
                unit.episode_issues.add("H0_EDGE_UNAVAILABLE")
            edges = {
                horizon: value["edge"]
                for horizon, value in episodes.items()
                if value.get("edge") is not None
            }
            markouts: dict[int, Decimal] = {}
            for value in episodes.values():
                edge = value.get("edge")
                if edge is not None and h0 is not None:
                    markout = edge - h0
                    markouts[value["detail"]["horizon_ms"]] = markout
            unit_episodes[unit.unit_id].append(
                {
                    "fill": fill,
                    "quote": quote,
                    "edges": edges,
                    "markouts": markouts,
                    "details": {horizon: value["detail"] for horizon, value in episodes.items()},
                }
            )

        # Active context is calculated for every eligible trade, including
        # trades that never filled.  A supplied policy list can only narrow
        # admissible context; it may not create missing context.
        for arm in ("1", "2"):
            per_event: dict[str, tuple[_Quote, ...]] = {}
            for trade_key in unit.trade_keys:
                trade = trades[trade_key]
                per_event[trade_key] = trade.arm_context(arm)
                for quote in per_event[trade_key]:
                    unit.identity_issues.update(quote.issues)
            unit.active_by_arm[arm] = per_event
        for trade_key in unit.trade_keys:
            trade = trades[trade_key]
            for arm in ("1", "2"):
                for quote in trade.arm_context(arm):
                    interval = _interval_record(
                        market=trade.market or quote.market,
                        venue="RISEX",
                        session=quote.session,
                        recovery=quote.recovery,
                        start=quote.created,
                        end=trade.received,
                    )
                    unit.gap_issues.update(_contaminating_gaps(gaps, interval, venue="RISEX"))
        _pair_unit(unit)
        if unit.identity_issues:
            unit.status = "UNRESOLVED"
        elif unit.gap_issues:
            unit.status = "CONTAMINATED"
        elif unit.episode_issues:
            if any("UNKNOWN" in issue or "MISSING" in issue or "UNRESOLVED" in issue for issue in unit.episode_issues):
                unit.status = "UNRESOLVED"
            else:
                unit.status = "CONTAMINATED"
        elif any(not unit.active_by_arm.get(arm, {}).get(key) for arm in ("1", "2") for key in unit.trade_keys):
            unit.status = "INACTIVE"
        else:
            unit.status = "CLEAN"

    clean_units = [unit for unit in units_by_root.values() if unit.status == "CLEAN"]
    status_counts = {status.lower(): sum(unit.status == status for unit in units_by_root.values()) for status in ("CLEAN", "CONTAMINATED", "INACTIVE", "UNRESOLVED")}
    common_units = clean_units
    arms: dict[str, dict[str, Any]] = {}
    sample_interval = stage["required"].get("sample_interval")
    stage_start = _integer(sample_interval.get("start_monotonic_ns")) if isinstance(sample_interval, Mapping) else None

    def arm_rows(arm: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        filled_rows: list[dict[str, Any]] = []
        scored_rows: list[dict[str, Any]] = []
        for unit in common_units:
            episodes_for_arm = [episode for episode in unit_episodes.get(unit.unit_id, ()) if episode["quote"].margin == Decimal(arm)]
            if episodes_for_arm:
                filled_rows.append({"unit": unit, "episodes": episodes_for_arm})
                scored_rows.append({"unit": unit, "episodes": episodes_for_arm, "filled": True})
            else:
                scored_rows.append({"unit": unit, "episodes": [], "filled": False})
        return filled_rows, scored_rows

    for arm in ("1", "2"):
        filled_rows, scored_rows = arm_rows(arm)
        horizon_values: dict[int, list[Decimal]] = {horizon: [] for horizon in HORIZONS}
        markout_values: dict[int, list[Decimal]] = {horizon: [] for horizon in HORIZONS}
        raw_filled_episode_count = 0
        clusters: set[tuple[Any, ...]] = set()
        detection_timestamps: set[int] = set()
        unit_summaries: list[dict[str, Any]] = []
        for row in scored_rows:
            unit = row["unit"]
            episodes_for_arm = row["episodes"]
            if not episodes_for_arm:
                unit_summaries.append({"unit_id": unit.unit_id, "filled": False, "score": {str(h): "0" for h in HORIZONS}})
                continue
            raw_filled_episode_count += len(episodes_for_arm)
            for episode in episodes_for_arm:
                fill = episode["fill"]
                for trade_key in fill.trade_keys:
                    trade = trades.get(trade_key)
                    if trade is not None and trade.taker_order_id is not None:
                        clusters.add((trade.market, trade.side, trade.taker_order_id[0], trade.taker_order_id[1]))
                if fill.detected is not None:
                    detection_timestamps.add(fill.detected)
            score_payload: dict[str, str | None] = {}
            for horizon in HORIZONS:
                clean_edges = [value for episode in episodes_for_arm for value in [_score_for_episode(episode, horizon)] if value is not None]
                clean_markouts = [value for episode in episodes_for_arm for value in [_markout_for_episode(episode, horizon)] if value is not None]
                if not clean_edges:
                    # A filled but arithmetic-invalid episode never reaches a
                    # clean common unit; keep the score visibly unresolved.
                    unit_summaries.append({"unit_id": unit.unit_id, "filled": True, "score": {str(h): None for h in HORIZONS}, "issues": sorted(unit.episode_issues)})
                    break
                minimum_edge = min(clean_edges)
                minimum_markout = min(clean_markouts) if clean_markouts else Decimal("0")
                horizon_values[horizon].append(minimum_edge)
                markout_values[horizon].append(minimum_markout)
                score_payload[str(horizon)] = str(minimum_edge)
            else:
                unit_summaries.append({"unit_id": unit.unit_id, "filled": True, "score": score_payload})
        full_hedge_shares: dict[str, Any] = {}
        for horizon in HORIZONS:
            filled_count = sum(1 for row in filled_rows if any(_score_for_episode(ep, horizon) is not None for ep in row["episodes"]))
            full_hedge_shares[str(horizon)] = {
                "numerator": filled_count,
                "denominator": len(filled_rows),
                "share": _rate(filled_count, len(filled_rows)),
                "exact_q": filled_count == len(filled_rows) and bool(filled_rows),
            }
        detection_values = [
            min(ep["fill"].detected for ep in row["episodes"] if ep["fill"].detected is not None)
            for row in filled_rows
            if any(ep["fill"].detected is not None for ep in row["episodes"])
        ]
        concentration: dict[str, Any] = {}
        for name, width in (("one_minute", _NS_PER_MINUTE), ("five_minute", _NS_PER_FIVE_MINUTES)):
            buckets: defaultdict[int, int] = defaultdict(int)
            missing = 0
            if stage_start is None:
                missing = len(detection_values)
            else:
                for value in detection_values:
                    if value < stage_start:
                        missing += 1
                    else:
                        buckets[stage_start + ((value - stage_start) // width) * width] += 1
            total = sum(buckets.values())
            concentration[name] = {
                "bucket_width_ms": width // 1_000_000,
                "bucket_count": len(buckets),
                "unit_count": total,
                "missing_detection_timestamp_count": missing,
                "top_bucket_share": _rate(max(buckets.values()), total) if buckets else None,
                "buckets": [{"bucket_start_monotonic_ns": key, "unit_count": buckets[key]} for key in sorted(buckets)],
            }
        arms[arm] = {
            "nominal_margin_bps": arm,
            "raw_filled_episode_count": raw_filled_episode_count,
            "clean_filled_unit_count": len(filled_rows),
            "distinct_venue_cluster_count": len(clusters),
            "distinct_detection_timestamp_count": len(detection_timestamps),
            "horizon_scores": {str(horizon): _stats_for_units(horizon_values[horizon]) for horizon in HORIZONS},
            "conditional_markout_scores": {str(horizon): _stats_for_units(markout_values[horizon]) for horizon in HORIZONS},
            "full_hedge_shares": full_hedge_shares,
            "concentration": concentration,
            "unit_scores": unit_summaries,
        }

    paired_clean_units = [
        unit
        for unit in clean_units
        if any(episode["quote"].margin == Decimal("1") for episode in unit_episodes.get(unit.unit_id, ()))
        and any(episode["quote"].margin == Decimal("2") for episode in unit_episodes.get(unit.unit_id, ()))
    ]
    pair_counts: defaultdict[str, int] = defaultdict(int)
    signed_prices: list[Decimal] = []
    signed_ticks: list[Decimal] = []
    for unit in paired_clean_units:
        pair_counts[unit.pair_classification] += 1
        for comparison in unit.pair_comparisons:
            if comparison.get("signed_price_separation") is not None:
                signed_prices.append(_decimal(comparison["signed_price_separation"]) or Decimal("0"))
            if isinstance(comparison.get("signed_tick_separation"), Decimal):
                signed_ticks.append(comparison["signed_tick_separation"])
    distinct_count = pair_counts["DISTINCT_EFFECTIVE_LEVEL"]
    collision_count = pair_counts["EFFECTIVE_PRICE_COLLISION"]
    reversed_count = pair_counts["NOMINAL_WIDER_REVERSED"] + pair_counts["MIXED_EFFECTIVE_LEVEL"]
    unresolved_count = pair_counts["EFFECTIVE_LEVEL_UNRESOLVED"]
    paired_count = len(paired_clean_units)
    pairing = {
        "paired_clean_unit_count": paired_count,
        "distinct_effective_level_unit_count": distinct_count,
        "distinct_effective_level_share": _rate(distinct_count, paired_count),
        "collision_unit_count": collision_count,
        "collision_share": _rate(collision_count, paired_count),
        "reversed_unit_count": reversed_count,
        "unresolved_unit_count": unresolved_count,
        "classification_counts": {key: pair_counts[key] for key in sorted(pair_counts)},
        "signed_price_separation": _stats(signed_prices),
        "signed_tick_separation": _stats(signed_ticks),
        "units": [
            {
                "unit_id": unit.unit_id,
                "classification": unit.pair_classification,
                "comparisons": [
                    {key: (str(value) if isinstance(value, Decimal) else value) for key, value in comparison.items()}
                    for comparison in unit.pair_comparisons
                ],
            }
            for unit in sorted(paired_clean_units, key=lambda item: item.unit_id)
        ],
    }

    failed_gates: list[str] = []
    gate_results: dict[str, dict[str, Any]] = {}

    def gate(name: str, passed: bool, observed: Any = None, required: Any = None) -> None:
        gate_results[name] = {"passed": bool(passed), "observed": observed, "required": required}
        if not passed:
            failed_gates.append(name)

    gate("STAGE_METADATA_VALID", bool(stage["valid"]), stage["missing_fields"] or stage["invalid_fields"], "complete non-FIXTURE fingerprint/provenance")
    gate("FIXTURE_MODE_NOT_PUBLIC_STAGE", not stage["synthetic"], stage["stage_kind"], "PUBLIC")
    if integrity:
        gate("EVIDENCE_INTEGRITY", False, sorted(set(integrity)), "no integrity failures")
    else:
        gate("EVIDENCE_INTEGRITY", True, [], "no integrity failures")
    gate("COMMON_ELIGIBLE_UNIT_FLOOR", len(common_units) >= 50, len(common_units), ">=50")
    bad_population = status_counts["contaminated"] + status_counts["inactive"] + status_counts["unresolved"]
    gate("ALL_ELIGIBLE_UNITS_CLEAN", bad_population == 0, status_counts, "contaminated=inactive=unresolved=0")
    for arm in ("1", "2"):
        row = arms[arm]
        gate(f"ARM_{arm}_CLEAN_FILLED_UNIT_FLOOR", row["clean_filled_unit_count"] >= 20, row["clean_filled_unit_count"], ">=20")
        gate(f"ARM_{arm}_VENUE_CLUSTER_FLOOR", row["distinct_venue_cluster_count"] >= 20, row["distinct_venue_cluster_count"], ">=20")
        gate(f"ARM_{arm}_DETECTION_TIMESTAMP_FLOOR", row["distinct_detection_timestamp_count"] >= 15, row["distinct_detection_timestamp_count"], ">=15")
        one = row["concentration"]["one_minute"]["top_bucket_share"]
        five = row["concentration"]["five_minute"]["top_bucket_share"]
        one_missing = row["concentration"]["one_minute"]["missing_detection_timestamp_count"]
        five_missing = row["concentration"]["five_minute"]["missing_detection_timestamp_count"]
        gate(f"ARM_{arm}_ONE_MINUTE_CONCENTRATION", one_missing == 0 and one is not None and Decimal(one) <= Decimal("0.25"), one, "<=0.25 and no missing stage-window timestamps")
        gate(f"ARM_{arm}_FIVE_MINUTE_CONCENTRATION", five_missing == 0 and five is not None and Decimal(five) <= Decimal("0.50"), five, "<=0.50 and no missing stage-window timestamps")
        for horizon in HORIZONS:
            share = row["full_hedge_shares"][str(horizon)]["share"]
            gate(f"ARM_{arm}_FULL_HEDGE_{horizon}MS", share == "1", share, "1")
            scores = row["horizon_scores"][str(horizon)]
            markouts = row["conditional_markout_scores"][str(horizon)]
            gate(f"ARM_{arm}_POSITIVE_SUM_{horizon}MS", Decimal(scores["sum"]) > 0, scores["sum"], ">0")
            if horizon == 0:
                gate(f"ARM_{arm}_EDGE_P05_0MS", scores["p05"] is not None and Decimal(scores["p05"]) >= Decimal("0.01"), scores["p05"], ">=0.01")
            elif horizon == 300:
                gate(f"ARM_{arm}_EDGE_P05_300MS", scores["p05"] is not None and Decimal(scores["p05"]) >= Decimal("0.01"), scores["p05"], ">=0.01")
                gate(f"ARM_{arm}_EDGE_MEDIAN_300MS", scores["median"] is not None and Decimal(scores["median"]) >= Decimal("0.01"), scores["median"], ">=0.01")
                gate(f"ARM_{arm}_POSITIVE_SHARE_300MS", scores["positive_share"] is not None and Decimal(scores["positive_share"]) >= Decimal("0.95"), scores["positive_share"], ">=0.95")
                gate(f"ARM_{arm}_MARKOUT_P05_300MS", markouts["p05"] is not None and Decimal(markouts["p05"]) >= Decimal("-0.005"), markouts["p05"], ">=-0.005")
                gate(f"ARM_{arm}_MARKOUT_MEDIAN_300MS", markouts["median"] is not None and Decimal(markouts["median"]) >= 0, markouts["median"], ">=0")
            elif horizon == 500:
                gate(f"ARM_{arm}_EDGE_P05_500MS", scores["p05"] is not None and Decimal(scores["p05"]) >= Decimal("0.005"), scores["p05"], ">=0.005")
                gate(f"ARM_{arm}_EDGE_MEDIAN_500MS", scores["median"] is not None and Decimal(scores["median"]) >= Decimal("0.01"), scores["median"], ">=0.01")
                gate(f"ARM_{arm}_MARKOUT_P05_500MS", markouts["p05"] is not None and Decimal(markouts["p05"]) >= Decimal("-0.01"), markouts["p05"], ">=-0.01")
                gate(f"ARM_{arm}_MARKOUT_MEDIAN_500MS", markouts["median"] is not None and Decimal(markouts["median"]) >= Decimal("-0.005"), markouts["median"], ">=-0.005")
            else:
                gate(f"ARM_{arm}_EDGE_P05_1000MS", scores["p05"] is not None and Decimal(scores["p05"]) > 0, scores["p05"], ">0")
                gate(f"ARM_{arm}_EDGE_MEDIAN_1000MS", scores["median"] is not None and Decimal(scores["median"]) >= Decimal("0.005"), scores["median"], ">=0.005")
                gate(f"ARM_{arm}_MARKOUT_P05_1000MS", markouts["p05"] is not None and Decimal(markouts["p05"]) >= Decimal("-0.015"), markouts["p05"], ">=-0.015")
                gate(f"ARM_{arm}_MARKOUT_MEDIAN_1000MS", markouts["median"] is not None and Decimal(markouts["median"]) >= Decimal("-0.01"), markouts["median"], ">=-0.01")
    gate("PAIRED_CLEAN_UNIT_FLOOR", paired_count >= 20, paired_count, ">=20")
    gate("DISTINCT_EFFECTIVE_LEVEL_FLOOR", distinct_count >= 16, distinct_count, ">=16")
    gate("DISTINCT_EFFECTIVE_LEVEL_SHARE", distinct_count >= 16 and paired_count > 0 and Decimal(distinct_count) / Decimal(paired_count) >= Decimal("0.80"), pairing["distinct_effective_level_share"], ">=0.80")
    gate("EFFECTIVE_LEVEL_COLLISION_COUNT", collision_count <= 4, collision_count, "<=4")
    gate("EFFECTIVE_LEVEL_COLLISION_SHARE", paired_count > 0 and Decimal(collision_count) / Decimal(paired_count) <= Decimal("0.20"), pairing["collision_share"], "<=0.20")
    gate("EFFECTIVE_LEVEL_REVERSED", reversed_count == 0, reversed_count, "0")
    gate("EFFECTIVE_LEVEL_UNRESOLVED", unresolved_count == 0, unresolved_count, "0")

    selector_scores: dict[str, dict[str, Any]] = {}
    for arm in ("1", "2"):
        values: list[Decimal] = []
        unknown_count = 0
        row = arms[arm]
        for item in row["unit_scores"]:
            score = item["score"].get("300")
            if not item["filled"]:
                values.append(Decimal("0"))
            elif score is None:
                unknown_count += 1
            else:
                values.append(Decimal(score))
        selector_scores[arm] = {
            "denominator": len(values),
            "common_denominator": len(common_units),
            "unknown_count": unknown_count,
            "sum_300ms": str(sum(values, Decimal("0"))),
            "scores": [str(value) for value in values],
        }
    selector_unknown_count = sum(row["unknown_count"] for row in selector_scores.values())
    gate("SELECTOR_INPUT_COMPLETE", selector_unknown_count == 0, selector_unknown_count, 0)
    arm_passes = {
        arm: not any(name.startswith(f"ARM_{arm}_") for name in failed_gates)
        for arm in ("1", "2")
    }
    selected_arm = _select_arm(arm_passes, selector_scores)
    mathematical_shared_failed = {
        name
        for name in failed_gates
        if not name.startswith("ARM_")
        and name not in {"STAGE_METADATA_VALID", "FIXTURE_MODE_NOT_PUBLIC_STAGE"}
    }
    selection_pass = (
        selected_arm is not None
        and not mathematical_shared_failed
        and arm_passes[selected_arm]
    )
    if not selection_pass:
        failed_gates.append("NO_ARM_QUALIFIES")
    if stage["synthetic"]:
        stage_verdict = "FIXTURE_ONLY"
    elif not stage["valid"] or integrity:
        stage_verdict = "DATA_INSUFFICIENT"
    elif not selection_pass:
        stage_verdict = "CALIBRATION_FAILED"
    else:
        # Numerical qualification is intentionally not stage admission.  The
        # producer/first-stop/start-window/holdout gate is SCAN-003 scope.
        stage_verdict = "DATA_INSUFFICIENT"
    if selection_pass:
        mathematical_verdict = "NUMERICAL_QUALIFIED"
    elif not stage["valid"] or integrity:
        mathematical_verdict = "DATA_INSUFFICIENT"
    else:
        mathematical_verdict = "NUMERICAL_FAILED"

    unit_payload = [
        {
            "unit_id": unit.unit_id,
            "status": unit.status,
            "eligible_trade_count": len(unit.trade_keys),
            "trade_event_keys": list(sorted(unit.trade_keys)),
            "strict_fill_count": sum(fill.key[0] == STRICT_MODEL for fill in unit.fills),
            "strict_fill_versions": sorted(fill.version_id for fill in unit.fills if fill.key[0] == STRICT_MODEL),
            "reasons": sorted(unit.identity_issues | unit.gap_issues | unit.episode_issues),
            "pair_classification": unit.pair_classification,
        }
        for unit in sorted(units_by_root.values(), key=lambda item: item.unit_id)
    ]
    return {
        "schema_version": 1,
        "section": EVALUATION_SECTION,
        "descriptive_only": True,
        "conditional_entry_edge_only": True,
        "no_executable_pnl": True,
        "no_confidence_estimate": True,
        "stage_verdict": stage_verdict,
        "stage_qualified": False,
        "stage_admission": {
            "status": "CLOSED_PENDING_SCAN_003",
            "open": False,
            "reason": "producer, first-stop, cap, start-window, and holdout admission are later scope",
        },
        "mathematical_verdict": mathematical_verdict,
        "mathematical_stage_qualified": selection_pass,
        "public_candidate_verdict": None,
        "candidate_eligible": False,
        "provenance": stage,
        "record_counts": {key: record_counts[key] for key in sorted(record_counts)},
        "book_record_count": book_count,
        "integrity_issues": sorted(set(integrity)),
        "duplicate_fill_count": duplicate_fills,
        "duplicate_horizon_count": duplicate_horizons,
        "population": {
            "raw_unit_count": len(units_by_root),
            "clean_unit_count": status_counts["clean"],
            "contaminated_unit_count": status_counts["contaminated"],
            "inactive_unit_count": status_counts["inactive"],
            "unresolved_unit_count": status_counts["unresolved"],
            "common_eligible_unit_count": len(common_units),
            "units": unit_payload,
        },
        "arms": arms,
        "effective_level_pairing": pairing,
        "selector": {
            "common_denominator_unit_count": len(common_units),
            "arm_scores": selector_scores,
            "arm_qualifies": arm_passes,
            "selected_margin_bps": selected_arm,
            "selection_pass": selection_pass,
            "mathematical_shared_failed_gates": sorted(mathematical_shared_failed),
            "tie_break": "1 bps" if Decimal(selector_scores["1"]["sum_300ms"]) == Decimal(selector_scores["2"]["sum_300ms"]) else None,
        },
        "gate_results": gate_results,
        "failed_gates": sorted(set(failed_gates)),
        "limits": {
            "horizons_ms": list(HORIZONS),
            "unit_state_bounded": True,
            "max_quote_state": _MAX_QUOTES,
            "max_eligible_trade_state": _MAX_ELIGIBLE_TRADES,
            "max_fill_state": _MAX_FILLS,
            "max_horizon_state": _MAX_HORIZONS,
            "book_state": "referenced_and_latest_before_deadline_only",
        },
    }


__all__ = [
    "EVALUATION_SECTION",
    "HORIZONS",
    "build_offline_evaluation",
]
