"""Offline causal measurement for one hypothetical public RISEx quote.

This module is deliberately a small evidence calculator.  It does not place,
sign, cancel, reconcile, or prepare an order, and it does not infer an
exchange-side execution cursor from a book and a trade stream.  A
``CausalRestingQuote`` is an explicit hypothetical schedule; the result is a
diagnostic about the supplied public evidence, not an execution claim.

The older receipt-order fillability detectors remain in :mod:`evidence` and
are intentionally not used here.  In particular, a legacy evidence row with
only ``received_monotonic_ns`` is never promoted to causal evidence.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
import re
from typing import Any

from risex_farmer.models import Side, Venue

from .models import (
    BookEvidence,
    CausalTiming,
    DataGapEvidence,
    QuoteVersion,
    TradeEvidence,
)


_RISEX_ORDER_ID_RE = re.compile(r"^0x[0-9a-fA-F]{48}$")
_RISEX_TX_HASH_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")


def _risex_trade_event_key_matches(
    source_event_id: str,
    source_trade_id: str,
    canonical_market: str,
    venue_symbol: str | None,
) -> bool:
    """Validate the observed venue key against the normalized market identity."""

    parts = source_event_id.split("|")
    accepted_markets = {canonical_market}
    if venue_symbol is not None:
        accepted_markets.add(venue_symbol)
    return (
        len(parts) == 3
        and parts[0] == Venue.RISEX.value
        and parts[1] in accepted_markets
        and parts[2] == source_trade_id
    )


def _non_negative_int(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be int")
    if value < 0:
        raise ValueError(f"{name} must be non-negative")


def _optional_non_negative_int(value: int | None, name: str) -> None:
    if value is not None:
        _non_negative_int(value, name)


def _positive_decimal(value: Decimal, name: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be Decimal")
    if not value.is_finite() or value <= 0:
        raise ValueError(f"{name} must be a finite positive Decimal")
    return value


def _non_negative_decimal(value: Decimal, name: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be Decimal")
    if not value.is_finite() or value < 0:
        raise ValueError(f"{name} must be a finite non-negative Decimal")
    return value


def _text(value: str, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _phase_timestamps(
    ingress_received_monotonic_ns: int | None,
    normalized_ready_monotonic_ns: int | None,
    decision_ready_monotonic_ns: int | None,
    *,
    allow_decision_without_normalized: bool = False,
) -> None:
    """Validate phase ordering without filling missing phases by inference."""

    _optional_non_negative_int(
        ingress_received_monotonic_ns, "ingress_received_monotonic_ns"
    )
    _optional_non_negative_int(
        normalized_ready_monotonic_ns, "normalized_ready_monotonic_ns"
    )
    _optional_non_negative_int(
        decision_ready_monotonic_ns, "decision_ready_monotonic_ns"
    )
    if normalized_ready_monotonic_ns is not None and ingress_received_monotonic_ns is None:
        raise ValueError("normalized readiness requires ingress receipt")
    if (
        decision_ready_monotonic_ns is not None
        and normalized_ready_monotonic_ns is None
        and not allow_decision_without_normalized
    ):
        raise ValueError("decision readiness requires normalized readiness")
    if (
        ingress_received_monotonic_ns is not None
        and normalized_ready_monotonic_ns is not None
        and normalized_ready_monotonic_ns < ingress_received_monotonic_ns
    ):
        raise ValueError("normalized readiness must not precede ingress receipt")
    if (
        normalized_ready_monotonic_ns is not None
        and decision_ready_monotonic_ns is not None
        and decision_ready_monotonic_ns < normalized_ready_monotonic_ns
    ):
        raise ValueError("decision readiness must not precede normalized readiness")


class CausalEventKind(StrEnum):
    """The three kinds of public evidence accepted by the S1 calculator."""

    TRADE = "TRADE"
    BOOK = "BOOK"
    DATA_GAP = "DATA_GAP"


class CausalOutcome(StrEnum):
    """Conservative result labels for one hypothetical quote."""

    DECISION_NOT_READY = "DECISION_NOT_READY"
    NO_FILL = "NO_FILL"
    PARTIAL_FILL = "PARTIAL_FILL"
    FULL_FILL = "FULL_FILL"
    CANCELLED_NO_FILL = "CANCELLED_NO_FILL"
    REPLACED_NO_FILL = "REPLACED_NO_FILL"
    CAUSAL_UNCERTAIN = "CAUSAL_UNCERTAIN"

    CANCELED_NO_FILL = CANCELLED_NO_FILL
    NOT_READY = DECISION_NOT_READY
    UNCERTAIN = CAUSAL_UNCERTAIN


class CausalUncertainty(StrEnum):
    """Reasons that prevent a positive or clean no-fill conclusion."""

    MISSING_CAUSAL_TIMING = "MISSING_CAUSAL_TIMING"
    MISSING_SOURCE_IDENTITY = "MISSING_SOURCE_IDENTITY"
    CONFLICTING_DUPLICATE = "CONFLICTING_DUPLICATE"
    LATE_OLDER_EVENT = "LATE_OLDER_EVENT"
    SOURCE_IDENTITY_MISMATCH = "SOURCE_IDENTITY_MISMATCH"
    RECOVERY_TRANSITION = "RECOVERY_TRANSITION"
    SAME_MATCH_ORDER_UNPROVEN = "SAME_MATCH_ORDER_UNPROVEN"
    DATA_GAP = "DATA_GAP"
    SOURCE_BOOK_STALE = "SOURCE_BOOK_STALE"
    SOURCE_BOOK_UNHEALTHY = "SOURCE_BOOK_UNHEALTHY"
    SOURCE_BOOK_AFTER_DECISION = "SOURCE_BOOK_AFTER_DECISION"
    SOURCE_BOOK_RECEIPT_SKEW = "SOURCE_BOOK_RECEIPT_SKEW"
    WATERMARK_IDENTITY_MISSING = "WATERMARK_IDENTITY_MISSING"
    WATERMARK_SEMANTICS_UNPROVEN = "WATERMARK_SEMANTICS_UNPROVEN"
    WATERMARK_BOUNDARY_AMBIGUOUS = "WATERMARK_BOUNDARY_AMBIGUOUS"
    EQUAL_TIME_ORDER_UNPROVEN = "EQUAL_TIME_ORDER_UNPROVEN"
    QUOTE_TOUCH_ORDER_UNPROVEN = "QUOTE_TOUCH_ORDER_UNPROVEN"
    EVENT_NOT_READY_BY_END = "EVENT_NOT_READY_BY_END"

    MISSING_TIMING = MISSING_CAUSAL_TIMING
    MISSING_IDENTITY = MISSING_SOURCE_IDENTITY
    CONFLICTING_DUPLICATE_EVENT = CONFLICTING_DUPLICATE
    RECOVERY = RECOVERY_TRANSITION
    DATA_LOSS = DATA_GAP
    CAUSAL_AMBIGUITY = SAME_MATCH_ORDER_UNPROVEN
    STALE_SOURCE_IDENTITY = SOURCE_IDENTITY_MISMATCH


@dataclass(frozen=True, slots=True)
class CausalSourceIdentity:
    """Public source identity attached to one event or source-book witness."""

    venue: Venue
    canonical_market: str
    stream_session_id: str | int
    recovery_generation: int
    source_event_id: str | None = None
    block_number: int | None = None
    sequence: int | None = None
    revision: int | None = None
    match_id: str | None = None
    source_kind: CausalEventKind | str | None = None
    source_trade_id: str | None = None
    maker_order_id: str | None = None
    taker_order_id: str | None = None
    maker: str | None = None
    taker: str | None = None
    tx_hash: str | None = None
    log_index: int | None = None
    worker_timestamp: str | int | None = None
    venue_symbol: str | None = None

    def __post_init__(self) -> None:
        venue = self.venue if isinstance(self.venue, Venue) else Venue(self.venue)
        object.__setattr__(self, "venue", venue)
        _text(self.canonical_market, "canonical_market")
        if self.venue_symbol is not None:
            _text(self.venue_symbol, "venue_symbol")
        if isinstance(self.stream_session_id, bool) or not isinstance(
            self.stream_session_id, (str, int)
        ):
            raise TypeError("stream_session_id must be str or int")
        if isinstance(self.stream_session_id, str) and not self.stream_session_id:
            raise ValueError("stream_session_id must be non-empty")
        _non_negative_int(self.recovery_generation, "recovery_generation")
        if self.source_event_id is not None:
            _text(self.source_event_id, "source_event_id")
        _optional_non_negative_int(self.block_number, "block_number")
        _optional_non_negative_int(self.sequence, "sequence")
        _optional_non_negative_int(self.revision, "revision")
        if self.match_id is not None:
            _text(self.match_id, "match_id")
        if self.source_kind is not None:
            object.__setattr__(self, "source_kind", CausalEventKind(self.source_kind))
        for value, name in (
            (self.source_trade_id, "source_trade_id"),
            (self.maker_order_id, "maker_order_id"),
            (self.taker_order_id, "taker_order_id"),
            (self.maker, "maker"),
            (self.taker, "taker"),
            (self.tx_hash, "tx_hash"),
        ):
            if value is not None:
                _text(value, name)
        _optional_non_negative_int(self.log_index, "log_index")
        if self.worker_timestamp is not None:
            if isinstance(self.worker_timestamp, bool) or not isinstance(
                self.worker_timestamp, (str, int)
            ):
                raise TypeError("worker_timestamp must be a string or integer")
            if isinstance(self.worker_timestamp, int) and self.worker_timestamp < 0:
                raise ValueError("worker_timestamp must be non-negative")

    @property
    def is_complete(self) -> bool:
        if self.source_event_id is None or self.source_kind is None:
            return False
        if self.source_kind is CausalEventKind.TRADE:
            complete = all(
                value is not None
                for value in (
                    self.source_trade_id,
                    self.maker_order_id,
                    self.taker_order_id,
                    self.maker,
                    self.taker,
                    self.tx_hash,
                    self.block_number,
                    self.log_index,
                    self.worker_timestamp,
                )
            )
            if not complete:
                return False
            if self.venue is not Venue.RISEX:
                return True
            assert self.source_trade_id is not None
            assert self.maker_order_id is not None
            assert self.taker_order_id is not None
            assert self.tx_hash is not None
            return (
                _RISEX_ORDER_ID_RE.fullmatch(self.maker_order_id) is not None
                and _RISEX_ORDER_ID_RE.fullmatch(self.taker_order_id) is not None
                and _RISEX_TX_HASH_RE.fullmatch(self.tx_hash) is not None
                and self.source_trade_id
                == f"{self.maker_order_id}-{self.taker_order_id}"
                and _risex_trade_event_key_matches(
                    self.source_event_id,
                    self.source_trade_id,
                    self.canonical_market,
                    self.venue_symbol,
                )
            )
        if self.source_kind is CausalEventKind.BOOK:
            if self.venue is Venue.RISEX:
                return all(
                    value is not None
                    for value in (
                        self.block_number,
                        self.log_index,
                        self.worker_timestamp,
                    )
                )
            if self.venue is Venue.LIGHTER:
                return self.sequence is not None
            return False
        return self.source_kind is CausalEventKind.DATA_GAP

    @property
    def event_id(self) -> str | None:
        return self.source_event_id

    @property
    def stream_key(self) -> tuple[Venue, str, str | int, int]:
        return (
            self.venue,
            self.canonical_market,
            self.stream_session_id,
            self.recovery_generation,
        )

    @property
    def stream_key_text(self) -> str:
        venue, market, session, recovery = self.stream_key
        return f"{venue.value}|{market}|{session}|{recovery}"

    @classmethod
    def from_trade(cls, trade: TradeEvidence) -> "CausalSourceIdentity":
        return cls(
            venue=trade.venue,
            canonical_market=trade.canonical_market,
            venue_symbol=trade.venue_symbol,
            stream_session_id=trade.stream_session_id,
            recovery_generation=trade.recovery_generation,
            source_event_id=trade.trade_event_key,
            source_kind=CausalEventKind.TRADE,
            source_trade_id=trade.source_trade_id,
            maker_order_id=trade.maker_order_id,
            taker_order_id=trade.taker_order_id,
            maker=trade.maker,
            taker=trade.taker,
            tx_hash=trade.tx_hash,
            block_number=trade.block_number,
            log_index=trade.log_index,
            worker_timestamp=trade.worker_timestamp,
        )

    @classmethod
    def from_book(cls, book: BookEvidence) -> "CausalSourceIdentity":
        return cls(
            venue=book.venue,
            canonical_market=book.canonical_market,
            stream_session_id=book.stream_session_id,
            recovery_generation=book.recovery_generation,
            source_event_id=book.book_revision_id,
            sequence=book.sequence,
            revision=book.book_revision,
            source_kind=CausalEventKind.BOOK,
            tx_hash=book.tx_hash,
            block_number=book.block_number,
            log_index=book.log_index,
            worker_timestamp=book.worker_timestamp,
        )

    @classmethod
    def from_gap(cls, gap: DataGapEvidence) -> "CausalSourceIdentity":
        end = "open" if gap.gap_end_monotonic_ns is None else str(gap.gap_end_monotonic_ns)
        return cls(
            venue=gap.source_venue,
            canonical_market=gap.canonical_market,
            stream_session_id=gap.stream_session_id,
            recovery_generation=gap.recovery_generation,
            source_event_id=(
                f"gap:{gap.gap_start_monotonic_ns}:{end}:{gap.reason}"
            ),
            source_kind=CausalEventKind.DATA_GAP,
        )


@dataclass(frozen=True, slots=True)
class CausalEvent:
    """One normalized event with explicit phase and source identity."""

    payload: TradeEvidence | BookEvidence | DataGapEvidence
    kind: CausalEventKind | str | None = None
    source_identity: CausalSourceIdentity | str | None = None
    source_event_monotonic_ns: int | None = None
    block_number: int | None = None
    sequence: int | None = None
    revision: int | None = None
    match_id: str | None = None
    ingress_received_monotonic_ns: int | None = None
    normalized_ready_monotonic_ns: int | None = None
    decision_ready_monotonic_ns: int | None = None
    tx_hash: str | None = None
    log_index: int | None = None
    worker_timestamp: str | int | None = None

    def __post_init__(self) -> None:
        expected_kind: CausalEventKind
        if isinstance(self.payload, TradeEvidence):
            expected_kind = CausalEventKind.TRADE
        elif isinstance(self.payload, BookEvidence):
            expected_kind = CausalEventKind.BOOK
        elif isinstance(self.payload, DataGapEvidence):
            expected_kind = CausalEventKind.DATA_GAP
        else:
            raise TypeError("payload must be TradeEvidence, BookEvidence, or DataGapEvidence")
        kind = expected_kind if self.kind is None else CausalEventKind(self.kind)
        if kind is not expected_kind:
            raise ValueError("event kind does not match payload")
        object.__setattr__(self, "kind", kind)

        # Carry the already-normalized wire position into the causal event.
        # The event layer does not parse venue frames and never treats this
        # position as a cross-channel cursor.
        if self.block_number is None:
            object.__setattr__(self, "block_number", getattr(self.payload, "block_number", None))
        if self.sequence is None:
            object.__setattr__(self, "sequence", getattr(self.payload, "sequence", None))
        if self.revision is None:
            object.__setattr__(
                self,
                "revision",
                getattr(self.payload, "book_revision", None),
            )
        if self.tx_hash is None:
            object.__setattr__(self, "tx_hash", getattr(self.payload, "tx_hash", None))
        if self.log_index is None:
            object.__setattr__(self, "log_index", getattr(self.payload, "log_index", None))
        if self.worker_timestamp is None:
            object.__setattr__(
                self,
                "worker_timestamp",
                getattr(self.payload, "worker_timestamp", None),
            )

        payload_timing = getattr(self.payload, "causal_timing", None)
        ingress = (
            self.ingress_received_monotonic_ns
            if self.ingress_received_monotonic_ns is not None
            else (None if payload_timing is None else payload_timing.ingress_received_monotonic_ns)
        )
        normalized = (
            self.normalized_ready_monotonic_ns
            if self.normalized_ready_monotonic_ns is not None
            else (None if payload_timing is None else payload_timing.normalized_ready_monotonic_ns)
        )
        decision = (
            self.decision_ready_monotonic_ns
            if self.decision_ready_monotonic_ns is not None
            else (None if payload_timing is None else payload_timing.decision_ready_monotonic_ns)
        )
        _phase_timestamps(ingress, normalized, decision)
        object.__setattr__(self, "ingress_received_monotonic_ns", ingress)
        object.__setattr__(self, "normalized_ready_monotonic_ns", normalized)
        object.__setattr__(self, "decision_ready_monotonic_ns", decision)

        identity = self.source_identity
        if identity is None:
            if isinstance(self.payload, TradeEvidence):
                identity = CausalSourceIdentity.from_trade(self.payload)
            elif isinstance(self.payload, BookEvidence):
                identity = CausalSourceIdentity.from_book(self.payload)
            else:
                identity = CausalSourceIdentity.from_gap(self.payload)
        elif isinstance(identity, str):
            if identity:
                identity = CausalSourceIdentity(
                    venue=self.venue,
                    canonical_market=self.canonical_market,
                    venue_symbol=getattr(self.payload, "venue_symbol", None),
                    stream_session_id=self.stream_session_id,
                    recovery_generation=self.payload_recovery_generation,
                    source_event_id=identity,
                    block_number=self.block_number,
                    sequence=self.sequence,
                    revision=self.revision,
                    match_id=self.match_id,
                    source_kind=kind,
                    source_trade_id=getattr(self.payload, "source_trade_id", None),
                    maker_order_id=getattr(self.payload, "maker_order_id", None),
                    taker_order_id=getattr(self.payload, "taker_order_id", None),
                    maker=getattr(self.payload, "maker", None),
                    taker=getattr(self.payload, "taker", None),
                    tx_hash=self.tx_hash,
                    log_index=self.log_index,
                    worker_timestamp=self.worker_timestamp,
                )
            # An empty string is a deliberate missing-identity sentinel used
            # by fixtures.  It must not be silently replaced by a payload key.
        elif not isinstance(identity, CausalSourceIdentity):
            raise TypeError("source_identity must be CausalSourceIdentity, str, or None")
        object.__setattr__(self, "source_identity", identity)

        if self.source_event_monotonic_ns is None:
            event_time = (
                self.payload.received_monotonic_ns
                if isinstance(self.payload, (TradeEvidence, BookEvidence))
                else self.payload.gap_start_monotonic_ns
            )
        else:
            event_time = self.source_event_monotonic_ns
        _non_negative_int(event_time, "source_event_monotonic_ns")
        object.__setattr__(self, "source_event_monotonic_ns", event_time)
        _optional_non_negative_int(self.block_number, "block_number")
        _optional_non_negative_int(self.sequence, "sequence")
        _optional_non_negative_int(self.revision, "revision")
        _optional_non_negative_int(self.log_index, "log_index")
        if self.tx_hash is not None:
            _text(self.tx_hash, "tx_hash")
        if self.worker_timestamp is not None:
            if isinstance(self.worker_timestamp, bool) or not isinstance(
                self.worker_timestamp, (str, int)
            ):
                raise TypeError("worker_timestamp must be a string or integer")
            if isinstance(self.worker_timestamp, int) and self.worker_timestamp < 0:
                raise ValueError("worker_timestamp must be non-negative")
        if self.match_id is not None:
            _text(self.match_id, "match_id")

        if isinstance(identity, CausalSourceIdentity):
            if self.block_number is None:
                object.__setattr__(self, "block_number", identity.block_number)
            if self.sequence is None:
                object.__setattr__(self, "sequence", identity.sequence)
            if self.revision is None:
                object.__setattr__(self, "revision", identity.revision)
            if self.match_id is None:
                object.__setattr__(self, "match_id", identity.match_id)
            if self.tx_hash is None:
                object.__setattr__(self, "tx_hash", identity.tx_hash)
            if self.log_index is None:
                object.__setattr__(self, "log_index", identity.log_index)
            if self.worker_timestamp is None:
                object.__setattr__(self, "worker_timestamp", identity.worker_timestamp)
            # Payload-derived identities do not carry optional block/match
            # metadata.  Preserve the explicit event metadata in the identity
            # for fills and records, while leaving contradictory supplied
            # values visible through ``identity_metadata_consistent``.
            object.__setattr__(
                self,
                "source_identity",
                CausalSourceIdentity(
                    venue=identity.venue,
                    canonical_market=identity.canonical_market,
                    venue_symbol=identity.venue_symbol,
                    stream_session_id=identity.stream_session_id,
                    recovery_generation=identity.recovery_generation,
                    source_event_id=identity.source_event_id,
                    block_number=(
                        identity.block_number
                        if identity.block_number is not None
                        else self.block_number
                    ),
                    sequence=(
                        identity.sequence
                        if identity.sequence is not None
                        else self.sequence
                    ),
                    revision=(
                        identity.revision
                        if identity.revision is not None
                        else self.revision
                    ),
                    match_id=(identity.match_id if identity.match_id is not None else self.match_id),
                    source_kind=identity.source_kind,
                    source_trade_id=identity.source_trade_id,
                    maker_order_id=identity.maker_order_id,
                    taker_order_id=identity.taker_order_id,
                    maker=identity.maker,
                    taker=identity.taker,
                    tx_hash=(identity.tx_hash if identity.tx_hash is not None else self.tx_hash),
                    log_index=(
                        identity.log_index
                        if identity.log_index is not None
                        else self.log_index
                    ),
                    worker_timestamp=(
                        identity.worker_timestamp
                        if identity.worker_timestamp is not None
                        else self.worker_timestamp
                    ),
                ),
            )

    @property
    def venue(self) -> Venue:
        if isinstance(self.payload, DataGapEvidence):
            return self.payload.source_venue
        return self.payload.venue

    @property
    def canonical_market(self) -> str:
        return self.payload.canonical_market

    @property
    def stream_session_id(self) -> str | int:
        if isinstance(self.payload, DataGapEvidence):
            return self.payload.stream_session_id
        return self.payload.stream_session_id

    @property
    def payload_recovery_generation(self) -> int:
        if isinstance(self.payload, DataGapEvidence):
            return self.payload.recovery_generation
        return self.payload.recovery_generation

    @property
    def recovery_generation(self) -> int:
        if isinstance(self.source_identity, CausalSourceIdentity):
            return self.source_identity.recovery_generation
        return self.payload_recovery_generation

    @property
    def event_time_ns(self) -> int:
        return self.source_event_monotonic_ns  # type: ignore[return-value]

    @property
    def causal_monotonic_ns(self) -> int:
        """Local ingress time used for causality and interval boundaries."""

        return (
            self.ingress_received_monotonic_ns
            if self.ingress_received_monotonic_ns is not None
            else self.event_time_ns
        )

    @property
    def event_id(self) -> str | None:
        if isinstance(self.source_identity, CausalSourceIdentity):
            return self.source_identity.event_id
        return None

    @property
    def stream_key(self) -> tuple[Venue, str, str | int, int] | None:
        if isinstance(self.source_identity, CausalSourceIdentity):
            return self.source_identity.stream_key
        return None

    @property
    def timing(self) -> CausalTiming | None:
        if self.ingress_received_monotonic_ns is None:
            return None
        return CausalTiming(
            ingress_received_monotonic_ns=self.ingress_received_monotonic_ns,
            normalized_ready_monotonic_ns=self.normalized_ready_monotonic_ns,
            decision_ready_monotonic_ns=self.decision_ready_monotonic_ns,
        )

    @property
    def has_causal_timing(self) -> bool:
        return self.ingress_received_monotonic_ns is not None

    @property
    def has_complete_timing(self) -> bool:
        return (
            self.ingress_received_monotonic_ns is not None
            and self.normalized_ready_monotonic_ns is not None
            and self.decision_ready_monotonic_ns is not None
        )

    @property
    def source_identity_complete(self) -> bool:
        return (
            isinstance(self.source_identity, CausalSourceIdentity)
            and self.source_identity.is_complete
        )

    @property
    def identity_metadata_consistent(self) -> bool:
        if not isinstance(self.source_identity, CausalSourceIdentity):
            return False
        identity = self.source_identity
        if (
            identity.venue is not self.venue
            or identity.canonical_market != self.canonical_market
            or identity.stream_session_id != self.stream_session_id
            or identity.recovery_generation != self.payload_recovery_generation
            or identity.source_kind is not self.kind
        ):
            return False
        expected_source_id = (
            self.payload.trade_event_key
            if isinstance(self.payload, TradeEvidence)
            else self.payload.book_revision_id
            if isinstance(self.payload, BookEvidence)
            else None
        )
        if expected_source_id is not None and identity.source_event_id != expected_source_id:
            return False
        for event_value, identity_value in (
            (self.block_number, identity.block_number),
            (self.sequence, identity.sequence),
            (self.revision, identity.revision),
            (self.match_id, identity.match_id),
            (self.tx_hash, identity.tx_hash),
            (self.log_index, identity.log_index),
            (self.worker_timestamp, identity.worker_timestamp),
        ):
            if event_value != identity_value:
                return False
        for payload_value, event_value in (
            (getattr(self.payload, "block_number", None), self.block_number),
            (getattr(self.payload, "sequence", None), self.sequence),
            (getattr(self.payload, "book_revision", None), self.revision),
            (getattr(self.payload, "tx_hash", None), self.tx_hash),
            (getattr(self.payload, "log_index", None), self.log_index),
            (getattr(self.payload, "worker_timestamp", None), self.worker_timestamp),
        ):
            if payload_value != event_value:
                return False
        for payload_value, identity_value in (
            (getattr(self.payload, "venue_symbol", None), identity.venue_symbol),
            (getattr(self.payload, "source_trade_id", None), identity.source_trade_id),
            (getattr(self.payload, "maker_order_id", None), identity.maker_order_id),
            (getattr(self.payload, "taker_order_id", None), identity.taker_order_id),
            (getattr(self.payload, "maker", None), identity.maker),
            (getattr(self.payload, "taker", None), identity.taker),
        ):
            if payload_value != identity_value:
                return False
        return True

    @property
    def trade(self) -> TradeEvidence | None:
        return self.payload if self.kind is CausalEventKind.TRADE else None  # type: ignore[return-value]

    @property
    def book(self) -> BookEvidence | None:
        return self.payload if self.kind is CausalEventKind.BOOK else None  # type: ignore[return-value]

    @property
    def gap(self) -> DataGapEvidence | None:
        return self.payload if self.kind is CausalEventKind.DATA_GAP else None  # type: ignore[return-value]

    @classmethod
    def from_trade(cls, trade: TradeEvidence, **kwargs: Any) -> "CausalEvent":
        return cls(payload=trade, kind=CausalEventKind.TRADE, **kwargs)

    @classmethod
    def from_book(cls, book: BookEvidence, **kwargs: Any) -> "CausalEvent":
        return cls(payload=book, kind=CausalEventKind.BOOK, **kwargs)

    @classmethod
    def from_gap(cls, gap: DataGapEvidence, **kwargs: Any) -> "CausalEvent":
        return cls(payload=gap, kind=CausalEventKind.DATA_GAP, **kwargs)


def causal_trade_event(trade: TradeEvidence, **kwargs: Any) -> CausalEvent:
    return CausalEvent.from_trade(trade, **kwargs)


def causal_book_event(book: BookEvidence, **kwargs: Any) -> CausalEvent:
    return CausalEvent.from_book(book, **kwargs)


def causal_gap_event(gap: DataGapEvidence, **kwargs: Any) -> CausalEvent:
    return CausalEvent.from_gap(gap, **kwargs)


@dataclass(frozen=True, slots=True)
class HypotheticalBlockWatermark:
    """A conservative, explicitly hypothetical block filter."""

    later_block_number: int
    source: str
    semantics: str = "HYPOTHETICAL_CONSERVATIVE_FILTER"

    def __post_init__(self) -> None:
        _non_negative_int(self.later_block_number, "later_block_number")
        _text(self.source, "source")
        if self.semantics != "HYPOTHETICAL_CONSERVATIVE_FILTER":
            raise ValueError("watermark semantics are not established")

    @property
    def minimum_block_number(self) -> int:
        return self.later_block_number

    @property
    def block_number(self) -> int:
        return self.later_block_number

    @property
    def is_hypothetical_only(self) -> bool:
        return True


@dataclass(frozen=True, slots=True)
class CausalRestingQuote:
    """One fixed-price, fixed-target-quantity hypothetical resting quote."""

    quote_id: str
    canonical_market: str
    maker_side: Side
    price: Decimal
    quantity: Decimal
    stream_session_id: str | int
    recovery_generation: int
    decision_ready_monotonic_ns: int | None = None
    activation_delay_ns: int = 0
    hypothetical_activation_monotonic_ns: int | None = None
    cancel_requested_monotonic_ns: int | None = None
    cancel_delay_ns: int = 0
    cancel_on_first_partial: bool = False
    hypothetical_cancel_effective_monotonic_ns: int | None = None
    replacement_effective_monotonic_ns: int | None = None
    ingress_received_monotonic_ns: int | None = None
    normalized_ready_monotonic_ns: int | None = None
    quote_created_monotonic_ns: int | None = None
    source_identity: CausalSourceIdentity | str | None = None
    source_book: BookEvidence | None = None
    source_book_freshness_max_age_ns: int | None = None
    block_watermark: HypotheticalBlockWatermark | None = None
    quote_version_id: str | None = None
    venue: Venue = Venue.RISEX
    tick_size: Decimal | None = None
    hedge_source_book: BookEvidence | None = None
    source_book_revision: int | None = None
    source_book_revision_id: str | None = None
    source_book_binding_required: bool = False
    hedge_stream_session_id: str | int | None = None
    hedge_recovery_generation: int | None = None
    hedge_source_book_revision: int | None = None
    hedge_source_book_revision_id: str | None = None
    hedge_source_book_binding_required: bool = False

    def __post_init__(self) -> None:
        _text(self.quote_id, "quote_id")
        _text(self.canonical_market, "canonical_market")
        side = self.maker_side if isinstance(self.maker_side, Side) else Side(self.maker_side)
        object.__setattr__(self, "maker_side", side)
        venue = self.venue if isinstance(self.venue, Venue) else Venue(self.venue)
        object.__setattr__(self, "venue", venue)
        if venue is not Venue.RISEX:
            raise ValueError("S1 causal quote must be a RISEx maker quote")
        _positive_decimal(self.price, "price")
        _positive_decimal(self.quantity, "quantity")
        if self.tick_size is not None:
            _positive_decimal(self.tick_size, "tick_size")
        if isinstance(self.stream_session_id, bool) or not isinstance(
            self.stream_session_id, (str, int)
        ):
            raise TypeError("stream_session_id must be str or int")
        if isinstance(self.stream_session_id, str) and not self.stream_session_id:
            raise ValueError("stream_session_id must be non-empty")
        _non_negative_int(self.recovery_generation, "recovery_generation")
        for value, name in (
            (self.activation_delay_ns, "activation_delay_ns"),
            (self.cancel_delay_ns, "cancel_delay_ns"),
        ):
            _non_negative_int(value, name)
        _optional_non_negative_int(
            self.decision_ready_monotonic_ns, "decision_ready_monotonic_ns"
        )
        _optional_non_negative_int(
            self.hypothetical_activation_monotonic_ns,
            "hypothetical_activation_monotonic_ns",
        )
        _optional_non_negative_int(
            self.cancel_requested_monotonic_ns, "cancel_requested_monotonic_ns"
        )
        _optional_non_negative_int(
            self.hypothetical_cancel_effective_monotonic_ns,
            "hypothetical_cancel_effective_monotonic_ns",
        )
        _optional_non_negative_int(
            self.replacement_effective_monotonic_ns,
            "replacement_effective_monotonic_ns",
        )
        _phase_timestamps(
            self.ingress_received_monotonic_ns,
            self.normalized_ready_monotonic_ns,
            self.decision_ready_monotonic_ns,
            allow_decision_without_normalized=True,
        )
        _optional_non_negative_int(
            self.quote_created_monotonic_ns, "quote_created_monotonic_ns"
        )
        if not isinstance(self.cancel_on_first_partial, bool):
            raise TypeError("cancel_on_first_partial must be bool")
        if self.quote_version_id is not None:
            _text(self.quote_version_id, "quote_version_id")
        if self.source_book_freshness_max_age_ns is not None:
            _non_negative_int(
                self.source_book_freshness_max_age_ns,
                "source_book_freshness_max_age_ns",
            )
        if self.block_watermark is not None and not isinstance(
            self.block_watermark, HypotheticalBlockWatermark
        ):
            raise TypeError("block_watermark must be HypotheticalBlockWatermark or None")
        if self.source_book is not None:
            if not isinstance(self.source_book, BookEvidence):
                raise TypeError("source_book must be BookEvidence or None")
        if self.hedge_source_book is not None and not isinstance(
            self.hedge_source_book, BookEvidence
        ):
            raise TypeError("hedge_source_book must be BookEvidence or None")
        if self.source_book_revision is not None:
            _non_negative_int(self.source_book_revision, "source_book_revision")
        if self.source_book_revision_id is not None:
            _text(self.source_book_revision_id, "source_book_revision_id")
        if not isinstance(self.source_book_binding_required, bool):
            raise TypeError("source_book_binding_required must be bool")
        if self.hedge_stream_session_id is not None:
            if isinstance(self.hedge_stream_session_id, bool) or not isinstance(
                self.hedge_stream_session_id, (str, int)
            ):
                raise TypeError("hedge_stream_session_id must be str or int")
            if isinstance(self.hedge_stream_session_id, str) and not self.hedge_stream_session_id:
                raise ValueError("hedge_stream_session_id must be non-empty")
        if self.hedge_recovery_generation is not None:
            _non_negative_int(self.hedge_recovery_generation, "hedge_recovery_generation")
        if self.hedge_source_book_revision is not None:
            _non_negative_int(self.hedge_source_book_revision, "hedge_source_book_revision")
        if self.hedge_source_book_revision_id is not None:
            _text(self.hedge_source_book_revision_id, "hedge_source_book_revision_id")
        if not isinstance(self.hedge_source_book_binding_required, bool):
            raise TypeError("hedge_source_book_binding_required must be bool")
        if self.source_identity is None and self.source_book is not None:
            object.__setattr__(
                self,
                "source_identity",
                CausalSourceIdentity.from_book(self.source_book),
            )
        if self.source_identity is not None and not isinstance(
            self.source_identity, (CausalSourceIdentity, str)
        ):
            raise TypeError("source_identity must be CausalSourceIdentity, str, or None")
        if isinstance(self.source_identity, str) and self.source_identity:
            object.__setattr__(
                self,
                "source_identity",
                CausalSourceIdentity(
                    venue=venue,
                    canonical_market=self.canonical_market,
                    stream_session_id=self.stream_session_id,
                    recovery_generation=self.recovery_generation,
                    source_event_id=self.source_identity,
                    source_kind=CausalEventKind.BOOK,
                ),
            )

        activation = self.activation_monotonic_ns
        if self.decision_ready_monotonic_ns is None:
            if self.hypothetical_activation_monotonic_ns is not None:
                raise ValueError("activation requires decision readiness")
        else:
            expected_activation = (
                self.decision_ready_monotonic_ns + self.activation_delay_ns
            )
            if self.hypothetical_activation_monotonic_ns is not None:
                if self.hypothetical_activation_monotonic_ns != expected_activation:
                    raise ValueError("activation does not match the explicit delay")
            else:
                object.__setattr__(
                    self,
                    "hypothetical_activation_monotonic_ns",
                    expected_activation,
                )
                activation = expected_activation
        if self.cancel_requested_monotonic_ns is None:
            if self.hypothetical_cancel_effective_monotonic_ns is not None:
                raise ValueError("cancel effective time requires a cancel request")
        else:
            expected_cancel = self.cancel_requested_monotonic_ns + self.cancel_delay_ns
            if self.hypothetical_cancel_effective_monotonic_ns is not None:
                if self.hypothetical_cancel_effective_monotonic_ns != expected_cancel:
                    raise ValueError("cancel effective time does not match the explicit delay")
            else:
                object.__setattr__(
                    self,
                    "hypothetical_cancel_effective_monotonic_ns",
                    expected_cancel,
                )
        if self.replacement_effective_monotonic_ns is not None:
            if activation is None:
                raise ValueError("replacement requires hypothetical activation")
            if self.replacement_effective_monotonic_ns <= activation:
                raise ValueError("replacement must follow the old quote activation")
            if (
                self.cancel_requested_monotonic_ns is None
                or self.hypothetical_cancel_effective_monotonic_ns is None
            ):
                raise ValueError("replacement requires completed cancellation")
            if (
                self.replacement_effective_monotonic_ns
                < self.hypothetical_cancel_effective_monotonic_ns
            ):
                raise ValueError("replacement must follow completed cancellation")

    @property
    def quote_version(self) -> str:
        return self.quote_version_id or self.quote_id

    @property
    def side(self) -> Side:
        return self.maker_side

    @property
    def activation_monotonic_ns(self) -> int | None:
        if self.decision_ready_monotonic_ns is None:
            return None
        if self.hypothetical_activation_monotonic_ns is not None:
            return self.hypothetical_activation_monotonic_ns
        return self.decision_ready_monotonic_ns + self.activation_delay_ns

    @property
    def cancel_effective_monotonic_ns(self) -> int | None:
        return self.hypothetical_cancel_effective_monotonic_ns

    @property
    def source_stream_key(self) -> tuple[Venue, str, str | int, int]:
        return (
            self.venue,
            self.canonical_market,
            self.stream_session_id,
            self.recovery_generation,
        )

    @property
    def causal_timing(self) -> CausalTiming | None:
        if self.ingress_received_monotonic_ns is None:
            return None
        return CausalTiming(
            ingress_received_monotonic_ns=self.ingress_received_monotonic_ns,
            normalized_ready_monotonic_ns=self.normalized_ready_monotonic_ns,
            decision_ready_monotonic_ns=self.decision_ready_monotonic_ns,
        )

    @property
    def is_decision_ready(self) -> bool:
        return self.decision_ready_monotonic_ns is not None


def _book_matches_binding(
    book: BookEvidence | None,
    *,
    venue: Venue,
    canonical_market: str,
    stream_session_id: str | int | None,
    recovery_generation: int | None,
    book_revision: int | None,
    book_revision_id: str | None,
) -> bool:
    """Match every persisted witness field before treating a book as bound."""

    return (
        isinstance(book, BookEvidence)
        and stream_session_id is not None
        and recovery_generation is not None
        and book_revision is not None
        and book_revision_id is not None
        and book.venue is venue
        and book.canonical_market == canonical_market
        and book.stream_session_id == stream_session_id
        and book.recovery_generation == recovery_generation
        and book.book_revision == book_revision
        and book.book_revision_id == book_revision_id
    )


def build_causal_resting_quote(
    quote_version: QuoteVersion,
    *,
    decision_ready_monotonic_ns: int | None = None,
    activation_delay_ns: int = 0,
    cancel_requested_monotonic_ns: int | None = None,
    cancel_delay_ns: int = 0,
    cancel_on_first_partial: bool = False,
    replacement_effective_monotonic_ns: int | None = None,
    source_book: BookEvidence | None = None,
    hedge_source_book: BookEvidence | None = None,
    source_book_freshness_max_age_ns: int | None = None,
    block_watermark: HypotheticalBlockWatermark | None = None,
) -> CausalRestingQuote:
    """Adapt an accepted quote version without upgrading old timing evidence."""

    if not isinstance(quote_version, QuoteVersion):
        raise TypeError("quote_version must be QuoteVersion")
    if quote_version.maker_price is None or quote_version.canonical_quantity is None:
        raise ValueError("quote version must contain fixed price and quantity")
    decision = (
        quote_version.decision_ready_monotonic_ns
        if decision_ready_monotonic_ns is None
        else decision_ready_monotonic_ns
    )
    maker_binding_matches = _book_matches_binding(
        source_book,
        venue=Venue.RISEX,
        canonical_market=quote_version.canonical_market,
        stream_session_id=quote_version.stream_session_id,
        recovery_generation=quote_version.recovery_generation,
        book_revision=quote_version.risex_book_revision,
        book_revision_id=quote_version.risex_book_revision_id,
    )
    source_identity = (
        CausalSourceIdentity.from_book(source_book)
        if maker_binding_matches
        else CausalSourceIdentity(
            venue=Venue.RISEX,
            canonical_market=quote_version.canonical_market,
            stream_session_id=quote_version.stream_session_id,
            recovery_generation=quote_version.recovery_generation,
            source_event_id=(
                quote_version.risex_book_revision_id or quote_version.version_id
            ),
            revision=quote_version.risex_book_revision,
            source_kind=CausalEventKind.BOOK,
        )
    )
    return CausalRestingQuote(
        quote_id=quote_version.version_id,
        quote_version_id=quote_version.version_id,
        canonical_market=quote_version.canonical_market,
        maker_side=quote_version.quote.maker_side,
        price=quote_version.maker_price,
        quantity=quote_version.canonical_quantity,
        stream_session_id=quote_version.stream_session_id,
        recovery_generation=quote_version.recovery_generation,
        ingress_received_monotonic_ns=quote_version.ingress_received_monotonic_ns,
        normalized_ready_monotonic_ns=quote_version.normalized_ready_monotonic_ns,
        decision_ready_monotonic_ns=decision,
        quote_created_monotonic_ns=quote_version.quote_created_monotonic_ns,
        activation_delay_ns=activation_delay_ns,
        cancel_requested_monotonic_ns=cancel_requested_monotonic_ns,
        cancel_delay_ns=cancel_delay_ns,
        cancel_on_first_partial=cancel_on_first_partial,
        replacement_effective_monotonic_ns=replacement_effective_monotonic_ns,
        source_identity=source_identity,
        source_book=source_book,
        hedge_source_book=hedge_source_book,
        source_book_revision=quote_version.risex_book_revision,
        source_book_revision_id=quote_version.risex_book_revision_id,
        source_book_binding_required=True,
        hedge_stream_session_id=quote_version.hedge_stream_session_id,
        hedge_recovery_generation=quote_version.hedge_recovery_generation,
        hedge_source_book_revision=quote_version.lighter_book_revision,
        hedge_source_book_revision_id=quote_version.lighter_book_revision_id,
        hedge_source_book_binding_required=True,
        source_book_freshness_max_age_ns=source_book_freshness_max_age_ns,
        block_watermark=block_watermark,
        tick_size=quote_version.quote.risex_tick_size,
    )


@dataclass(frozen=True, slots=True)
class CausalFill:
    """One conservatively consumed trade quantity."""

    source_event_id: str
    source_identity: CausalSourceIdentity
    received_monotonic_ns: int
    price: Decimal
    observed_quantity: Decimal
    consumed_quantity: Decimal
    remaining_quantity: Decimal
    match_id: str | None = None
    observed_trade_price: Decimal | None = None
    processed_ready_monotonic_ns: int | None = None

    def __post_init__(self) -> None:
        _text(self.source_event_id, "source_event_id")
        if not isinstance(self.source_identity, CausalSourceIdentity):
            raise TypeError("source_identity must be CausalSourceIdentity")
        _non_negative_int(self.received_monotonic_ns, "received_monotonic_ns")
        _positive_decimal(self.price, "price")
        if self.observed_trade_price is not None:
            _positive_decimal(self.observed_trade_price, "observed_trade_price")
        _positive_decimal(self.observed_quantity, "observed_quantity")
        _non_negative_decimal(self.consumed_quantity, "consumed_quantity")
        _non_negative_decimal(self.remaining_quantity, "remaining_quantity")
        if self.consumed_quantity > self.observed_quantity:
            raise ValueError("consumed quantity cannot exceed observed quantity")
        if self.match_id is not None:
            _text(self.match_id, "match_id")
        _optional_non_negative_int(
            self.processed_ready_monotonic_ns,
            "processed_ready_monotonic_ns",
        )

    @property
    def trade_event_key(self) -> str:
        return self.source_event_id

    @property
    def quantity(self) -> Decimal:
        return self.consumed_quantity


@dataclass(frozen=True, slots=True)
class CausalEventDecision:
    """Auditable classification of one input event."""

    kind: CausalEventKind
    source_event_id: str | None
    received_monotonic_ns: int | None
    classification: str
    reason: str | None = None
    consumed_quantity: Decimal = Decimal("0")

    def __post_init__(self) -> None:
        kind = self.kind if isinstance(self.kind, CausalEventKind) else CausalEventKind(self.kind)
        object.__setattr__(self, "kind", kind)
        if self.source_event_id is not None:
            _text(self.source_event_id, "source_event_id")
        _optional_non_negative_int(self.received_monotonic_ns, "received_monotonic_ns")
        _text(self.classification, "classification")
        if self.reason is not None:
            _text(self.reason, "reason")
        _non_negative_decimal(self.consumed_quantity, "consumed_quantity")

    @property
    def event_id(self) -> str | None:
        return self.source_event_id


@dataclass(frozen=True, slots=True)
class CausalTimingDiagnostics:
    """Timing diagnostics that never collapse freshness into quote age."""

    quote_ingress_received_monotonic_ns: int | None = None
    quote_normalized_ready_monotonic_ns: int | None = None
    quote_decision_ready_monotonic_ns: int | None = None
    quote_activation_monotonic_ns: int | None = None
    source_book_ingress_received_monotonic_ns: int | None = None
    source_book_received_monotonic_ns: int | None = None
    source_book_normalized_ready_monotonic_ns: int | None = None
    source_book_receipt_skew_ns: int | None = None
    source_book_age_at_decision_ns: int | None = None
    source_book_age_at_activation_ns: int | None = None
    hedge_book_ingress_received_monotonic_ns: int | None = None
    hedge_book_received_monotonic_ns: int | None = None
    hedge_book_normalized_ready_monotonic_ns: int | None = None
    hedge_book_age_at_decision_ns: int | None = None
    hedge_book_age_at_activation_ns: int | None = None
    risex_lighter_input_receipt_skew_ns: int | None = None
    resting_quote_age_ns: int | None = None
    first_event_ingress_received_monotonic_ns: int | None = None
    last_event_ingress_received_monotonic_ns: int | None = None
    first_event_received_monotonic_ns: int | None = None
    last_event_received_monotonic_ns: int | None = None
    input_receipt_fresh: bool | None = None

    def __post_init__(self) -> None:
        for value, name in (
            (self.quote_ingress_received_monotonic_ns, "quote_ingress_received_monotonic_ns"),
            (self.quote_normalized_ready_monotonic_ns, "quote_normalized_ready_monotonic_ns"),
            (self.quote_decision_ready_monotonic_ns, "quote_decision_ready_monotonic_ns"),
            (self.quote_activation_monotonic_ns, "quote_activation_monotonic_ns"),
            (
                self.source_book_ingress_received_monotonic_ns,
                "source_book_ingress_received_monotonic_ns",
            ),
            (self.source_book_received_monotonic_ns, "source_book_received_monotonic_ns"),
            (
                self.source_book_normalized_ready_monotonic_ns,
                "source_book_normalized_ready_monotonic_ns",
            ),
            (
                self.hedge_book_ingress_received_monotonic_ns,
                "hedge_book_ingress_received_monotonic_ns",
            ),
            (
                self.hedge_book_received_monotonic_ns,
                "hedge_book_received_monotonic_ns",
            ),
            (
                self.hedge_book_normalized_ready_monotonic_ns,
                "hedge_book_normalized_ready_monotonic_ns",
            ),
            (self.first_event_ingress_received_monotonic_ns, "first_event_ingress_received_monotonic_ns"),
            (self.last_event_ingress_received_monotonic_ns, "last_event_ingress_received_monotonic_ns"),
            (self.first_event_received_monotonic_ns, "first_event_received_monotonic_ns"),
            (self.last_event_received_monotonic_ns, "last_event_received_monotonic_ns"),
        ):
            _optional_non_negative_int(value, name)
        for value, name in (
            (self.source_book_receipt_skew_ns, "source_book_receipt_skew_ns"),
            (self.source_book_age_at_decision_ns, "source_book_age_at_decision_ns"),
            (self.source_book_age_at_activation_ns, "source_book_age_at_activation_ns"),
            (self.hedge_book_age_at_decision_ns, "hedge_book_age_at_decision_ns"),
            (self.hedge_book_age_at_activation_ns, "hedge_book_age_at_activation_ns"),
            (self.resting_quote_age_ns, "resting_quote_age_ns"),
            (
                self.risex_lighter_input_receipt_skew_ns,
                "risex_lighter_input_receipt_skew_ns",
            ),
        ):
            if value is not None and not isinstance(value, int):
                raise TypeError(f"{name} must be int or None")
        if self.input_receipt_fresh is not None and not isinstance(
            self.input_receipt_fresh, bool
        ):
            raise TypeError("input_receipt_fresh must be bool or None")

    @property
    def normalization_delay_ns(self) -> int | None:
        if (
            self.quote_ingress_received_monotonic_ns is None
            or self.quote_normalized_ready_monotonic_ns is None
        ):
            return None
        return (
            self.quote_normalized_ready_monotonic_ns
            - self.quote_ingress_received_monotonic_ns
        )

    @property
    def decision_delay_ns(self) -> int | None:
        if (
            self.quote_normalized_ready_monotonic_ns is None
            or self.quote_decision_ready_monotonic_ns is None
        ):
            return None
        return (
            self.quote_decision_ready_monotonic_ns
            - self.quote_normalized_ready_monotonic_ns
        )

    @property
    def input_receipt_skew_ns(self) -> int | None:
        """Absolute skew between the exact RISEx and Lighter input receipts."""

        return self.risex_lighter_input_receipt_skew_ns

    @property
    def source_book_normalization_delay_ns(self) -> int | None:
        if (
            self.source_book_ingress_received_monotonic_ns is None
            or self.source_book_normalized_ready_monotonic_ns is None
        ):
            return None
        return (
            self.source_book_normalized_ready_monotonic_ns
            - self.source_book_ingress_received_monotonic_ns
        )

    @property
    def hedge_book_normalization_delay_ns(self) -> int | None:
        if (
            self.hedge_book_ingress_received_monotonic_ns is None
            or self.hedge_book_normalized_ready_monotonic_ns is None
        ):
            return None
        return (
            self.hedge_book_normalized_ready_monotonic_ns
            - self.hedge_book_ingress_received_monotonic_ns
        )


@dataclass(frozen=True, slots=True)
class CausalQuoteMeasurement:
    """Conservative measurement result for one hypothetical quote."""

    quote: CausalRestingQuote
    outcome: CausalOutcome
    observed_filled_quantity: Decimal
    filled_quantity: Decimal
    remaining_quantity: Decimal
    fills: tuple[CausalFill, ...] = ()
    decisions: tuple[CausalEventDecision, ...] = ()
    uncertainty_reasons: tuple[CausalUncertainty, ...] = ()
    timing: CausalTimingDiagnostics = CausalTimingDiagnostics()
    event_count: int = 0
    duplicate_event_count: int = 0
    ignored_event_count: int = 0
    last_event_monotonic_ns: int | None = None
    effective_cancel_monotonic_ns: int | None = None
    cancel_requested_monotonic_ns: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.quote, CausalRestingQuote):
            raise TypeError("quote must be CausalRestingQuote")
        outcome = self.outcome if isinstance(self.outcome, CausalOutcome) else CausalOutcome(self.outcome)
        object.__setattr__(self, "outcome", outcome)
        _non_negative_decimal(self.observed_filled_quantity, "observed_filled_quantity")
        _non_negative_decimal(self.filled_quantity, "filled_quantity")
        _non_negative_decimal(self.remaining_quantity, "remaining_quantity")
        if self.observed_filled_quantity > self.quote.quantity:
            raise ValueError("observed filled quantity cannot exceed quote quantity")
        if self.remaining_quantity != self.quote.quantity - self.observed_filled_quantity:
            raise ValueError("remaining quantity must equal target minus observed quantity")
        if self.filled_quantity > self.observed_filled_quantity:
            raise ValueError("proven filled quantity cannot exceed observed quantity")
        if not isinstance(self.fills, tuple) or not isinstance(self.decisions, tuple):
            raise TypeError("fills and decisions must be tuples")
        reasons = tuple(
            reason
            if isinstance(reason, CausalUncertainty)
            else CausalUncertainty(reason)
            for reason in self.uncertainty_reasons
        )
        object.__setattr__(self, "uncertainty_reasons", tuple(dict.fromkeys(reasons)))
        if not isinstance(self.timing, CausalTimingDiagnostics):
            raise TypeError("timing must be CausalTimingDiagnostics")
        for value, name in (
            (self.event_count, "event_count"),
            (self.duplicate_event_count, "duplicate_event_count"),
            (self.ignored_event_count, "ignored_event_count"),
        ):
            _non_negative_int(value, name)
        _optional_non_negative_int(self.last_event_monotonic_ns, "last_event_monotonic_ns")
        _optional_non_negative_int(
            self.effective_cancel_monotonic_ns,
            "effective_cancel_monotonic_ns",
        )
        _optional_non_negative_int(
            self.cancel_requested_monotonic_ns,
            "cancel_requested_monotonic_ns",
        )

    @property
    def status(self) -> CausalOutcome:
        return self.outcome

    @property
    def uncertain(self) -> bool:
        return self.outcome in {
            CausalOutcome.CAUSAL_UNCERTAIN,
            CausalOutcome.DECISION_NOT_READY,
        } or bool(self.uncertainty_reasons)

    @property
    def is_proven_fill(self) -> bool:
        return (
            self.outcome in {CausalOutcome.PARTIAL_FILL, CausalOutcome.FULL_FILL}
            and not self.uncertainty_reasons
        )

    @property
    def is_clean_no_fill(self) -> bool:
        return (
            self.outcome
            in {
                CausalOutcome.NO_FILL,
                CausalOutcome.CANCELLED_NO_FILL,
                CausalOutcome.REPLACED_NO_FILL,
            }
            and not self.uncertainty_reasons
            and self.quote.is_decision_ready
        )

    @property
    def proven_filled_quantity(self) -> Decimal:
        return self.filled_quantity

    @property
    def exact_remaining_quantity(self) -> Decimal:
        return self.remaining_quantity

    @property
    def causal_ready(self) -> bool:
        return self.quote.is_decision_ready and not self.uncertainty_reasons

    @property
    def activation_monotonic_ns(self) -> int | None:
        return self.quote.activation_monotonic_ns

    @property
    def cancel_effective_monotonic_ns(self) -> int | None:
        if self.effective_cancel_monotonic_ns is not None:
            return self.effective_cancel_monotonic_ns
        return self.quote.cancel_effective_monotonic_ns

    def to_record(self) -> dict[str, Any]:
        return {
            "schema": "CAUSAL_QUOTE_MEASUREMENT_V1",
            "quote": _quote_record(self.quote),
            "outcome": self.outcome.value,
            "observed_filled_quantity": _decimal_text(self.observed_filled_quantity),
            "filled_quantity": _decimal_text(self.filled_quantity),
            "remaining_quantity": _decimal_text(self.remaining_quantity),
            "fills": tuple(_fill_record(fill) for fill in self.fills),
            "decisions": tuple(_decision_record(decision) for decision in self.decisions),
            "uncertainty_reasons": tuple(reason.value for reason in self.uncertainty_reasons),
            "timing": _timing_record(self.timing),
            "event_count": self.event_count,
            "duplicate_event_count": self.duplicate_event_count,
            "ignored_event_count": self.ignored_event_count,
            "last_event_monotonic_ns": self.last_event_monotonic_ns,
            "effective_cancel_monotonic_ns": self.effective_cancel_monotonic_ns,
            "cancel_requested_monotonic_ns": self.cancel_requested_monotonic_ns,
        }


def _decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return "0" if text in {"", "-0"} else text


def _identity_record(identity: CausalSourceIdentity | None) -> dict[str, Any] | None:
    if identity is None:
        return None
    return {
        "venue": identity.venue.value,
        "canonical_market": identity.canonical_market,
        "venue_symbol": identity.venue_symbol,
        "stream_session_id": identity.stream_session_id,
        "recovery_generation": identity.recovery_generation,
        "source_event_id": identity.source_event_id,
        "block_number": identity.block_number,
        "sequence": identity.sequence,
        "revision": identity.revision,
        "match_id": identity.match_id,
        "source_kind": (
            None
            if identity.source_kind is None
            else identity.source_kind.value
        ),
        "source_trade_id": identity.source_trade_id,
        "maker_order_id": identity.maker_order_id,
        "taker_order_id": identity.taker_order_id,
        "maker": identity.maker,
        "taker": identity.taker,
        "tx_hash": identity.tx_hash,
        "log_index": identity.log_index,
        "worker_timestamp": identity.worker_timestamp,
    }


def _quote_record(quote: CausalRestingQuote) -> dict[str, Any]:
    return {
        "quote_id": quote.quote_id,
        "quote_version_id": quote.quote_version_id,
        "venue": quote.venue.value,
        "canonical_market": quote.canonical_market,
        "maker_side": quote.maker_side.value,
        "price": _decimal_text(quote.price),
        "quantity": _decimal_text(quote.quantity),
        "stream_session_id": quote.stream_session_id,
        "recovery_generation": quote.recovery_generation,
        "decision_ready_monotonic_ns": quote.decision_ready_monotonic_ns,
        "activation_delay_ns": quote.activation_delay_ns,
        "hypothetical_activation_monotonic_ns": quote.activation_monotonic_ns,
        "cancel_requested_monotonic_ns": quote.cancel_requested_monotonic_ns,
        "cancel_delay_ns": quote.cancel_delay_ns,
        "hypothetical_cancel_effective_monotonic_ns": quote.cancel_effective_monotonic_ns,
        "replacement_effective_monotonic_ns": quote.replacement_effective_monotonic_ns,
        "cancel_on_first_partial": quote.cancel_on_first_partial,
        "source_identity": (
            _identity_record(quote.source_identity)
            if isinstance(quote.source_identity, CausalSourceIdentity)
            else None
        ),
        "tick_size": (
            None if quote.tick_size is None else _decimal_text(quote.tick_size)
        ),
        "source_book_identity": (
            None
            if quote.source_book is None
            else _identity_record(CausalSourceIdentity.from_book(quote.source_book))
        ),
        "hedge_source_book_identity": (
            None
            if quote.hedge_source_book is None
            else _identity_record(
                CausalSourceIdentity.from_book(quote.hedge_source_book)
            )
        ),
        "source_book_revision": quote.source_book_revision,
        "source_book_revision_id": quote.source_book_revision_id,
        "source_book_binding_required": quote.source_book_binding_required,
        "hedge_stream_session_id": quote.hedge_stream_session_id,
        "hedge_recovery_generation": quote.hedge_recovery_generation,
        "hedge_source_book_revision": quote.hedge_source_book_revision,
        "hedge_source_book_revision_id": quote.hedge_source_book_revision_id,
        "hedge_source_book_binding_required": quote.hedge_source_book_binding_required,
        "block_watermark": (
            None
            if quote.block_watermark is None
            else {
                "later_block_number": quote.block_watermark.later_block_number,
                "source": quote.block_watermark.source,
                "semantics": quote.block_watermark.semantics,
            }
        ),
    }


def _fill_record(fill: CausalFill) -> dict[str, Any]:
    return {
        "source_event_id": fill.source_event_id,
        "source_identity": _identity_record(fill.source_identity),
        "received_monotonic_ns": fill.received_monotonic_ns,
        "price": _decimal_text(fill.price),
        "observed_trade_price": (
            None
            if fill.observed_trade_price is None
            else _decimal_text(fill.observed_trade_price)
        ),
        "processed_ready_monotonic_ns": fill.processed_ready_monotonic_ns,
        "observed_quantity": _decimal_text(fill.observed_quantity),
        "consumed_quantity": _decimal_text(fill.consumed_quantity),
        "remaining_quantity": _decimal_text(fill.remaining_quantity),
        "match_id": fill.match_id,
    }


def _decision_record(decision: CausalEventDecision) -> dict[str, Any]:
    return {
        "kind": decision.kind.value,
        "source_event_id": decision.source_event_id,
        "received_monotonic_ns": decision.received_monotonic_ns,
        "classification": decision.classification,
        "reason": decision.reason,
        "consumed_quantity": _decimal_text(decision.consumed_quantity),
    }


def _timing_record(timing: CausalTimingDiagnostics) -> dict[str, Any]:
    return {
        name: getattr(timing, name)
        for name in (
            "quote_ingress_received_monotonic_ns",
            "quote_normalized_ready_monotonic_ns",
            "quote_decision_ready_monotonic_ns",
            "quote_activation_monotonic_ns",
            "source_book_ingress_received_monotonic_ns",
            "source_book_received_monotonic_ns",
            "source_book_normalized_ready_monotonic_ns",
            "source_book_receipt_skew_ns",
            "source_book_age_at_decision_ns",
            "source_book_age_at_activation_ns",
            "hedge_book_ingress_received_monotonic_ns",
            "hedge_book_received_monotonic_ns",
            "hedge_book_normalized_ready_monotonic_ns",
            "hedge_book_age_at_decision_ns",
            "hedge_book_age_at_activation_ns",
            "risex_lighter_input_receipt_skew_ns",
            "resting_quote_age_ns",
            "first_event_ingress_received_monotonic_ns",
            "last_event_ingress_received_monotonic_ns",
            "first_event_received_monotonic_ns",
            "last_event_received_monotonic_ns",
            "input_receipt_fresh",
            "normalization_delay_ns",
            "decision_delay_ns",
        )
    }


def _coerce_event(value: CausalEvent | TradeEvidence | BookEvidence | DataGapEvidence) -> CausalEvent:
    if isinstance(value, CausalEvent):
        return value
    if isinstance(value, TradeEvidence):
        return CausalEvent.from_trade(value)
    if isinstance(value, BookEvidence):
        return CausalEvent.from_book(value)
    if isinstance(value, DataGapEvidence):
        return CausalEvent.from_gap(value)
    raise TypeError("events must be CausalEvent or public evidence objects")


def _event_signature(event: CausalEvent) -> tuple[Any, ...]:
    payload = event.payload
    identity = event.source_identity
    identity_value = (
        None
        if not isinstance(identity, CausalSourceIdentity)
        else (
            identity.source_kind,
            identity.source_event_id,
            identity.source_trade_id,
            identity.maker_order_id,
            identity.taker_order_id,
            identity.maker,
            identity.taker,
            identity.tx_hash,
            identity.block_number,
            identity.log_index,
            identity.worker_timestamp,
        )
    )
    if isinstance(payload, TradeEvidence):
        payload_value: Any = (
            payload.canonical_price,
            payload.canonical_quantity,
            payload.aggressor_side.value,
            event.match_id,
            event.block_number,
            event.sequence,
            event.revision,
            identity_value,
        )
    elif isinstance(payload, BookEvidence):
        payload_value = (
            tuple((level.canonical_price, level.canonical_quantity) for level in payload.bids),
            tuple((level.canonical_price, level.canonical_quantity) for level in payload.asks),
            payload.sequence,
            payload.checksum,
            payload.sequence_valid,
            payload.checksum_valid,
            event.match_id,
            event.block_number,
            event.sequence,
            event.revision,
            identity_value,
        )
    else:
        payload_value = (
            payload.gap_start_monotonic_ns,
            payload.gap_end_monotonic_ns,
            payload.reason,
            payload.transport_event,
            payload.transport_failure_class,
            event.match_id,
            event.block_number,
            event.sequence,
            event.revision,
            identity_value,
        )
    return event.kind, payload_value


def _event_sort_key(event: CausalEvent) -> tuple[Any, ...]:
    kind_order = {
        CausalEventKind.DATA_GAP: 0,
        CausalEventKind.TRADE: 1,
        CausalEventKind.BOOK: 2,
    }
    return (
        event.causal_monotonic_ns,
        kind_order[event.kind],
        event.event_id or "",
        event.match_id or "",
    )


def _identity_key(event: CausalEvent) -> tuple[Any, ...] | None:
    if not event.source_identity_complete:
        return None
    identity = event.source_identity
    assert isinstance(identity, CausalSourceIdentity)
    return identity.stream_key, identity.event_id


def _expected_stream_matches(quote: CausalRestingQuote, event: CausalEvent) -> bool:
    return event.venue is quote.venue and event.canonical_market == quote.canonical_market


def _processing_ready_ns(event: CausalEvent) -> int:
    """Return the earliest local time at which normalized event data is usable."""

    return max(
        value
        for value in (
            event.causal_monotonic_ns,
            event.normalized_ready_monotonic_ns,
            event.decision_ready_monotonic_ns,
        )
        if value is not None
    )


def _venue_position(event: CausalEvent) -> tuple[int, ...] | None:
    """Return an observed venue position for this event's own stream kind."""

    if event.venue is Venue.RISEX:
        if event.block_number is None or event.log_index is None:
            return None
        return event.block_number, event.log_index
    if event.venue is Venue.LIGHTER and event.sequence is not None:
        return (event.sequence,)
    return None


def _trade_price_classification(
    quote: CausalRestingQuote,
    trade: TradeEvidence,
) -> tuple[bool, bool]:
    """Return ``(trade_through, boundary_ambiguous)`` conservatively."""

    if quote.maker_side is Side.BUY:
        if trade.canonical_price > quote.price:
            return False, False
        improvement = quote.price - trade.canonical_price
    else:
        if trade.canonical_price < quote.price:
            return False, False
        improvement = trade.canonical_price - quote.price
    if improvement == 0:
        return False, True
    if quote.tick_size is None:
        # A strict trade-through is observable without pretending that an
        # equal-price match was ordered against the resting quote.
        return True, False
    return improvement >= quote.tick_size, improvement < quote.tick_size


def _known_block_fence(
    quote: CausalRestingQuote,
    source_book: BookEvidence | None,
) -> int | None:
    fences: list[int] = []
    if quote.block_watermark is not None:
        fences.append(quote.block_watermark.minimum_block_number)
    if (
        source_book is not None
        and source_book.venue is quote.venue
        and source_book.canonical_market == quote.canonical_market
        and source_book.block_number is not None
    ):
        fences.append(source_book.block_number)
    return max(fences) if fences else None


def _event_is_in_measurement_window(
    event: CausalEvent,
    quote: CausalRestingQuote,
    end_monotonic_ns: int,
) -> bool:
    if event.kind is CausalEventKind.DATA_GAP:
        gap = event.gap
        assert gap is not None
        return gap.gap_start_monotonic_ns <= end_monotonic_ns and (
            gap.gap_end_monotonic_ns is None or gap.gap_end_monotonic_ns >= (quote.activation_monotonic_ns or 0)
        )
    return event.causal_monotonic_ns <= end_monotonic_ns


def _gap_overlaps_active_interval(
    gap: DataGapEvidence,
    quote: CausalRestingQuote,
    end_monotonic_ns: int,
) -> bool:
    start = quote.activation_monotonic_ns
    if start is None:
        start = quote.decision_ready_monotonic_ns
    if start is None:
        return False
    end = end_monotonic_ns
    cancel = quote.cancel_effective_monotonic_ns
    replacement = quote.replacement_effective_monotonic_ns
    if cancel is not None:
        end = min(end, cancel)
    if replacement is not None:
        end = min(end, replacement)
    if end < start:
        return False
    return gap.overlaps(start, end)


def _input_book_for_quote(
    quote: CausalRestingQuote,
    source_books: tuple[BookEvidence, ...],
    *,
    venue: Venue,
    explicit: BookEvidence | None,
) -> BookEvidence | None:
    """Select an explicitly bound input, never an arbitrary latest book."""

    if explicit is not None:
        return explicit
    candidates = [
        book
        for book in source_books
        if book.venue is venue and book.canonical_market == quote.canonical_market
    ]
    identity = quote.source_identity
    if venue is quote.venue and isinstance(identity, CausalSourceIdentity):
        if quote.source_book_binding_required:
            bound = [
                book
                for book in candidates
                if _book_matches_binding(
                    book,
                    venue=quote.venue,
                    canonical_market=quote.canonical_market,
                    stream_session_id=quote.stream_session_id,
                    recovery_generation=quote.recovery_generation,
                    book_revision=quote.source_book_revision,
                    book_revision_id=quote.source_book_revision_id,
                )
            ]
            return bound[0] if len(bound) == 1 else None
        bound = [
            book
            for book in candidates
            if book.book_revision_id == identity.source_event_id
        ]
        if len(bound) == 1:
            return bound[0]
    if venue is Venue.LIGHTER and quote.hedge_source_book_binding_required:
        bound = [
            book
            for book in candidates
            if _book_matches_binding(
                book,
                venue=Venue.LIGHTER,
                canonical_market=quote.canonical_market,
                stream_session_id=quote.hedge_stream_session_id,
                recovery_generation=quote.hedge_recovery_generation,
                book_revision=quote.hedge_source_book_revision,
                book_revision_id=quote.hedge_source_book_revision_id,
            )
        ]
        return bound[0] if len(bound) == 1 else None
    return candidates[0] if len(candidates) == 1 else None


def _input_books_for_quote(
    quote: CausalRestingQuote,
    source_books: tuple[BookEvidence, ...],
) -> tuple[BookEvidence | None, BookEvidence | None]:
    return (
        _input_book_for_quote(
            quote,
            source_books,
            venue=quote.venue,
            explicit=quote.source_book,
        ),
        _input_book_for_quote(
            quote,
            source_books,
            venue=Venue.LIGHTER,
            explicit=quote.hedge_source_book,
        ),
    )


def _timing_diagnostics(
    quote: CausalRestingQuote,
    events: tuple[CausalEvent, ...],
    source_book: BookEvidence | None,
    hedge_book: BookEvidence | None,
    end_monotonic_ns: int,
) -> tuple[CausalTimingDiagnostics, set[CausalUncertainty]]:
    reasons: set[CausalUncertainty] = set()
    event_times = [event.causal_monotonic_ns for event in events]
    event_ingress = [
        event.ingress_received_monotonic_ns
        for event in events
        if event.ingress_received_monotonic_ns is not None
    ]
    source_book_ingress = (
        None
        if source_book is None
        else source_book.ingress_received_monotonic_ns
    )
    source_book_received = None if source_book is None else source_book.received_monotonic_ns
    source_book_normalized = (
        None if source_book is None else source_book.normalized_ready_monotonic_ns
    )
    receipt_skew = (
        None
        if source_book is None or source_book_ingress is None
        else source_book_received - source_book_ingress
    )
    if receipt_skew is not None and receipt_skew < 0:
        reasons.add(CausalUncertainty.SOURCE_BOOK_RECEIPT_SKEW)
    decision = quote.decision_ready_monotonic_ns
    activation = quote.activation_monotonic_ns
    age_at_decision = (
        None if source_book_received is None or decision is None else decision - source_book_received
    )
    age_at_activation = (
        None
        if source_book_received is None or activation is None
        else activation - source_book_received
    )
    hedge_book_ingress = (
        None if hedge_book is None else hedge_book.ingress_received_monotonic_ns
    )
    hedge_book_received = (
        None if hedge_book is None else hedge_book.received_monotonic_ns
    )
    hedge_book_normalized = (
        None if hedge_book is None else hedge_book.normalized_ready_monotonic_ns
    )
    hedge_age_at_decision = (
        None
        if hedge_book_received is None or decision is None
        else decision - hedge_book_received
    )
    hedge_age_at_activation = (
        None
        if hedge_book_received is None or activation is None
        else activation - hedge_book_received
    )
    input_receipt_skew = (
        None
        if source_book_ingress is None or hedge_book_ingress is None
        else abs(source_book_ingress - hedge_book_ingress)
    )
    if source_book is None or hedge_book is None:
        reasons.add(CausalUncertainty.MISSING_CAUSAL_TIMING)
    if quote.source_book_binding_required:
        if (
            source_book is None
            or not _book_matches_binding(
                source_book,
                venue=quote.venue,
                canonical_market=quote.canonical_market,
                stream_session_id=quote.stream_session_id,
                recovery_generation=quote.recovery_generation,
                book_revision=quote.source_book_revision,
                book_revision_id=quote.source_book_revision_id,
            )
        ):
            if (
                quote.source_book_revision is None
                or quote.source_book_revision_id is None
            ):
                reasons.add(CausalUncertainty.MISSING_SOURCE_IDENTITY)
            elif source_book is not None:
                reasons.add(CausalUncertainty.SOURCE_IDENTITY_MISMATCH)
            else:
                reasons.add(CausalUncertainty.MISSING_SOURCE_IDENTITY)
    if quote.hedge_source_book_binding_required:
        if (
            hedge_book is None
            or not _book_matches_binding(
                hedge_book,
                venue=Venue.LIGHTER,
                canonical_market=quote.canonical_market,
                stream_session_id=quote.hedge_stream_session_id,
                recovery_generation=quote.hedge_recovery_generation,
                book_revision=quote.hedge_source_book_revision,
                book_revision_id=quote.hedge_source_book_revision_id,
            )
        ):
            if (
                quote.hedge_stream_session_id is None
                or quote.hedge_recovery_generation is None
                or quote.hedge_source_book_revision is None
                or quote.hedge_source_book_revision_id is None
            ):
                reasons.add(CausalUncertainty.MISSING_SOURCE_IDENTITY)
            elif hedge_book is not None:
                reasons.add(CausalUncertainty.SOURCE_IDENTITY_MISMATCH)
            else:
                reasons.add(CausalUncertainty.MISSING_SOURCE_IDENTITY)
    for input_book, expected_venue in (
        (source_book, quote.venue),
        (hedge_book, Venue.LIGHTER),
    ):
        if input_book is None:
            continue
        if (
            input_book.venue is not expected_venue
            or input_book.canonical_market != quote.canonical_market
        ):
            reasons.add(CausalUncertainty.SOURCE_IDENTITY_MISMATCH)
        if not CausalSourceIdentity.from_book(input_book).is_complete:
            reasons.add(CausalUncertainty.MISSING_SOURCE_IDENTITY)
        if (
            input_book.ingress_received_monotonic_ns is None
            or input_book.normalized_ready_monotonic_ns is None
        ):
            reasons.add(CausalUncertainty.MISSING_CAUSAL_TIMING)
        if not input_book.fresh:
            reasons.add(CausalUncertainty.SOURCE_BOOK_STALE)
        if not input_book.is_sequence_healthy:
            reasons.add(CausalUncertainty.SOURCE_BOOK_UNHEALTHY)
    if source_book is not None:
        if (
            source_book.stream_session_id != quote.stream_session_id
            or source_book.recovery_generation != quote.recovery_generation
        ):
            reasons.add(CausalUncertainty.SOURCE_IDENTITY_MISMATCH)
        if (
            isinstance(quote.source_identity, CausalSourceIdentity)
            and quote.source_identity.source_event_id != source_book.book_revision_id
        ):
            reasons.add(CausalUncertainty.SOURCE_IDENTITY_MISMATCH)
        if source_book_normalized is not None and decision is not None and source_book_normalized > decision:
            reasons.add(CausalUncertainty.SOURCE_BOOK_AFTER_DECISION)
        if age_at_decision is not None and age_at_decision < 0:
            reasons.add(CausalUncertainty.SOURCE_BOOK_AFTER_DECISION)
    if hedge_book is not None:
        if hedge_book_normalized is not None and decision is not None and hedge_book_normalized > decision:
            reasons.add(CausalUncertainty.SOURCE_BOOK_AFTER_DECISION)
        if hedge_age_at_decision is not None and hedge_age_at_decision < 0:
            reasons.add(CausalUncertainty.SOURCE_BOOK_AFTER_DECISION)
    max_age = quote.source_book_freshness_max_age_ns
    age_values = [
        age
        for age in (
            age_at_decision,
            hedge_age_at_decision,
        )
        if age is not None
    ]
    if max_age is not None and any(age < 0 or age > max_age for age in age_values):
        reasons.add(CausalUncertainty.SOURCE_BOOK_STALE)
    input_fresh = None
    if source_book is not None and hedge_book is not None:
        input_fresh = all(
            input_book.fresh
            and input_book.is_sequence_healthy
            and input_book.ingress_received_monotonic_ns is not None
            and input_book.normalized_ready_monotonic_ns is not None
            for input_book in (source_book, hedge_book)
        )
        if max_age is not None:
            input_fresh = input_fresh and bool(age_values) and all(
                0 <= age <= max_age for age in age_values
            )
        if source_book_normalized is not None and decision is not None:
            input_fresh = input_fresh and source_book_normalized <= decision
        if hedge_book_normalized is not None and decision is not None:
            input_fresh = input_fresh and hedge_book_normalized <= decision
    resting_age = None
    if activation is not None:
        resting_age = max(0, end_monotonic_ns - activation)
    return (
        CausalTimingDiagnostics(
            quote_ingress_received_monotonic_ns=quote.ingress_received_monotonic_ns,
            quote_normalized_ready_monotonic_ns=quote.normalized_ready_monotonic_ns,
            quote_decision_ready_monotonic_ns=decision,
            quote_activation_monotonic_ns=activation,
            source_book_ingress_received_monotonic_ns=source_book_ingress,
            source_book_received_monotonic_ns=source_book_received,
            source_book_normalized_ready_monotonic_ns=source_book_normalized,
            source_book_receipt_skew_ns=receipt_skew,
            source_book_age_at_decision_ns=age_at_decision,
            source_book_age_at_activation_ns=age_at_activation,
            hedge_book_ingress_received_monotonic_ns=hedge_book_ingress,
            hedge_book_received_monotonic_ns=hedge_book_received,
            hedge_book_normalized_ready_monotonic_ns=hedge_book_normalized,
            hedge_book_age_at_decision_ns=hedge_age_at_decision,
            hedge_book_age_at_activation_ns=hedge_age_at_activation,
            risex_lighter_input_receipt_skew_ns=input_receipt_skew,
            resting_quote_age_ns=resting_age,
            first_event_ingress_received_monotonic_ns=min(event_ingress) if event_ingress else None,
            last_event_ingress_received_monotonic_ns=max(event_ingress) if event_ingress else None,
            first_event_received_monotonic_ns=min(event_times) if event_times else None,
            last_event_received_monotonic_ns=max(event_times) if event_times else None,
            input_receipt_fresh=input_fresh,
        ),
        reasons,
    )


def _decision(
    event: CausalEvent,
    classification: str,
    reason: str | None = None,
    consumed_quantity: Decimal = Decimal("0"),
) -> CausalEventDecision:
    return CausalEventDecision(
        kind=event.kind,
        source_event_id=event.event_id,
        received_monotonic_ns=event.ingress_received_monotonic_ns,
        classification=classification,
        reason=reason,
        consumed_quantity=consumed_quantity,
    )


def measure_causal_quote(
    quote: CausalRestingQuote,
    events: Iterable[CausalEvent | TradeEvidence | BookEvidence | DataGapEvidence],
    *,
    end_monotonic_ns: int | None = None,
    source_books: Iterable[BookEvidence] = (),
    source_book: BookEvidence | None = None,
) -> CausalQuoteMeasurement:
    """Measure one fixed quote using only explicit causal public evidence.

    Event boundaries are inclusive for decision readiness and activation, and
    exclusive for effective cancellation and replacement.  Equal-time events
    are ordered deterministically by kind and source identity for repeatable
    offline output.  That deterministic fixture order is not presented as an
    exchange cursor; a same-match book/trade pair remains uncertain.
    """

    if not isinstance(quote, CausalRestingQuote):
        raise TypeError("quote must be CausalRestingQuote")
    materialized_events = tuple(_coerce_event(event) for event in events)
    books = tuple(source_books)
    if source_book is not None:
        if not isinstance(source_book, BookEvidence):
            raise TypeError("source_book must be BookEvidence or None")
        books = (source_book, *books)
    if any(not isinstance(book, BookEvidence) for book in books):
        raise TypeError("source_books must contain BookEvidence")
    observed_end_candidates = [
        _processing_ready_ns(event)
        for event in materialized_events
        if event.kind is not CausalEventKind.DATA_GAP
    ]
    observed_end_candidates.extend(
        event.causal_monotonic_ns
        for event in materialized_events
        if event.kind is CausalEventKind.DATA_GAP
    )
    for value in (
        quote.activation_monotonic_ns,
        quote.cancel_effective_monotonic_ns,
        quote.replacement_effective_monotonic_ns,
    ):
        if value is not None:
            observed_end_candidates.append(value)
    if end_monotonic_ns is None:
        end = max(observed_end_candidates, default=quote.decision_ready_monotonic_ns or 0)
    else:
        _non_negative_int(end_monotonic_ns, "end_monotonic_ns")
        end = end_monotonic_ns

    maker_book, hedge_book = _input_books_for_quote(quote, books)
    block_fence = _known_block_fence(quote, maker_book)
    reasons: set[CausalUncertainty] = set()
    if quote.source_identity == "":
        reasons.add(CausalUncertainty.MISSING_SOURCE_IDENTITY)
    elif isinstance(quote.source_identity, CausalSourceIdentity):
        if not quote.source_identity.is_complete:
            reasons.add(CausalUncertainty.MISSING_SOURCE_IDENTITY)
        if (
            quote.source_identity.venue is not quote.venue
            or quote.source_identity.canonical_market != quote.canonical_market
            or quote.source_identity.stream_session_id != quote.stream_session_id
            or quote.source_identity.recovery_generation != quote.recovery_generation
        ):
            reasons.add(CausalUncertainty.SOURCE_IDENTITY_MISMATCH)
    else:
        reasons.add(CausalUncertainty.MISSING_SOURCE_IDENTITY)
    decisions: list[CausalEventDecision] = []
    fills: list[CausalFill] = []
    duplicate_count = 0
    ignored_count = 0
    observed_filled = Decimal("0")
    remaining = quote.quantity
    seen: dict[tuple[Any, ...], tuple[Any, ...]] = {}
    arrival_last_time: dict[tuple[Any, ...], int] = {}
    arrival_last_position: dict[tuple[Any, ...], tuple[int, ...]] = {}
    match_kinds: dict[str, set[CausalEventKind]] = {}

    # Arrival order is retained solely for detecting late older observations;
    # semantic processing uses a deterministic offline order below.
    relevant_events: list[CausalEvent] = []
    candidate_events: list[CausalEvent] = []
    for event in materialized_events:
        if not _event_is_in_measurement_window(event, quote, end):
            ignored_count += 1
            continue
        if not _expected_stream_matches(quote, event):
            # An unrelated public stream cannot invalidate this quote and is
            # intentionally not used to establish cross-channel ordering.
            ignored_count += 1
            continue
        relevant_events.append(event)
        if event.match_id is not None:
            match_kinds.setdefault(event.match_id, set()).add(event.kind)
        key = _identity_key(event)
        if key is None:
            reasons.add(CausalUncertainty.MISSING_SOURCE_IDENTITY)
            candidate_events.append(event)
            continue
        signature = _event_signature(event)
        previous = seen.get(key)
        if previous is not None:
            if previous == signature:
                duplicate_count += 1
                ignored_count += 1
                continue
            reasons.add(CausalUncertainty.CONFLICTING_DUPLICATE)
            decisions.append(
                _decision(event, "CONFLICTING_DUPLICATE", CausalUncertainty.CONFLICTING_DUPLICATE.value)
            )
            continue
        seen[key] = signature
        stream_key = key[0]
        order_key = (stream_key, event.kind)
        previous_time = arrival_last_time.get(order_key)
        previous_position = arrival_last_position.get(order_key)
        current_position = _venue_position(event)
        if previous_time is not None and event.causal_monotonic_ns < previous_time:
            reasons.add(CausalUncertainty.LATE_OLDER_EVENT)
            decisions.append(
                _decision(event, "LATE_OLDER_EVENT", CausalUncertainty.LATE_OLDER_EVENT.value)
            )
            continue
        if (
            previous_position is not None
            and current_position is not None
            and current_position < previous_position
        ):
            reasons.add(CausalUncertainty.LATE_OLDER_EVENT)
            decisions.append(
                _decision(event, "LATE_OLDER_EVENT", CausalUncertainty.LATE_OLDER_EVENT.value)
            )
            continue
        candidate_events.append(event)
        arrival_last_time[order_key] = event.causal_monotonic_ns
        if current_position is not None:
            arrival_last_position[order_key] = current_position

    for match_id, kinds in match_kinds.items():
        if CausalEventKind.TRADE in kinds and CausalEventKind.BOOK in kinds:
            reasons.add(CausalUncertainty.SAME_MATCH_ORDER_UNPROVEN)

    timing, timing_reasons = _timing_diagnostics(
        quote, tuple(relevant_events), maker_book, hedge_book, end
    )
    reasons.update(timing_reasons)

    if quote.decision_ready_monotonic_ns is None:
        reasons.add(CausalUncertainty.MISSING_CAUSAL_TIMING)
        return CausalQuoteMeasurement(
            quote=quote,
            outcome=CausalOutcome.DECISION_NOT_READY,
            observed_filled_quantity=Decimal("0"),
            filled_quantity=Decimal("0"),
            remaining_quantity=quote.quantity,
            fills=(),
            decisions=tuple(decisions),
            uncertainty_reasons=tuple(sorted(reasons, key=lambda value: value.value)),
            timing=timing,
            event_count=len(materialized_events),
            duplicate_event_count=duplicate_count,
            ignored_event_count=ignored_count,
            last_event_monotonic_ns=(
                max((event.causal_monotonic_ns for event in relevant_events), default=None)
            ),
        )

    activation = quote.activation_monotonic_ns
    if activation is None:
        reasons.add(CausalUncertainty.MISSING_CAUSAL_TIMING)
    ordered_events = sorted(candidate_events, key=_event_sort_key)
    active_cancel = quote.cancel_effective_monotonic_ns
    auto_cancel_requested: int | None = None
    auto_cancel_effective: int | None = None
    last_event_ns: int | None = None

    for event in ordered_events:
        last_event_ns = (
            event.causal_monotonic_ns
            if last_event_ns is None
            else max(last_event_ns, event.causal_monotonic_ns)
        )
        identity = event.source_identity
        if (
            event.kind is CausalEventKind.TRADE
            and block_fence is not None
            and event.block_number is None
        ):
            reasons.add(CausalUncertainty.WATERMARK_IDENTITY_MISSING)
        if not event.source_identity_complete:
            # Missing source identity was recorded in the arrival pass.  Do
            # not let a malformed event become a no-fill or a fill.
            if isinstance(identity, CausalSourceIdentity) and (
                identity.venue is not quote.venue
                or identity.canonical_market != quote.canonical_market
                or identity.stream_session_id != quote.stream_session_id
                or identity.recovery_generation != quote.recovery_generation
                or not event.identity_metadata_consistent
            ):
                reasons.add(CausalUncertainty.SOURCE_IDENTITY_MISMATCH)
            decisions.append(_decision(event, "UNCERTAIN", CausalUncertainty.MISSING_SOURCE_IDENTITY.value))
            continue
        assert isinstance(identity, CausalSourceIdentity)
        if (
            (
                event.ingress_received_monotonic_ns is None
                or event.normalized_ready_monotonic_ns is None
            )
            and event.kind is not CausalEventKind.DATA_GAP
        ):
            reasons.add(CausalUncertainty.MISSING_CAUSAL_TIMING)
            decisions.append(_decision(event, "UNCERTAIN", CausalUncertainty.MISSING_CAUSAL_TIMING.value))
            continue
        if identity.venue is not quote.venue or identity.canonical_market != quote.canonical_market:
            reasons.add(CausalUncertainty.SOURCE_IDENTITY_MISMATCH)
            decisions.append(_decision(event, "UNCERTAIN", CausalUncertainty.SOURCE_IDENTITY_MISMATCH.value))
            continue
        if not event.identity_metadata_consistent:
            reasons.add(CausalUncertainty.SOURCE_IDENTITY_MISMATCH)
            decisions.append(_decision(event, "UNCERTAIN", CausalUncertainty.SOURCE_IDENTITY_MISMATCH.value))
            continue
        if identity.stream_session_id != quote.stream_session_id:
            reasons.add(CausalUncertainty.SOURCE_IDENTITY_MISMATCH)
            reasons.add(CausalUncertainty.RECOVERY_TRANSITION)
            decisions.append(_decision(event, "UNCERTAIN", CausalUncertainty.SOURCE_IDENTITY_MISMATCH.value))
            continue
        if identity.recovery_generation != quote.recovery_generation:
            reasons.add(CausalUncertainty.RECOVERY_TRANSITION)
            decisions.append(_decision(event, "UNCERTAIN", CausalUncertainty.RECOVERY_TRANSITION.value))
            continue
        processing_ready = _processing_ready_ns(event)
        if (
            event.kind is not CausalEventKind.DATA_GAP
            and processing_ready > end
        ):
            reasons.add(CausalUncertainty.EVENT_NOT_READY_BY_END)
            decisions.append(
                _decision(
                    event,
                    "UNCERTAIN",
                    CausalUncertainty.EVENT_NOT_READY_BY_END.value,
                )
            )
            continue
        if event.kind is CausalEventKind.DATA_GAP:
            gap = event.gap
            assert gap is not None
            if _gap_overlaps_active_interval(gap, quote, end):
                reasons.add(CausalUncertainty.DATA_GAP)
                decisions.append(_decision(event, "UNCERTAIN", CausalUncertainty.DATA_GAP.value))
            else:
                ignored_count += 1
                decisions.append(_decision(event, "IGNORED", "GAP_OUTSIDE_ACTIVE_INTERVAL"))
            continue
        if event.match_id is not None and len(match_kinds.get(event.match_id, set())) > 1:
            if event.kind is CausalEventKind.TRADE:
                ignored_count += 1
            decisions.append(_decision(event, "UNCERTAIN", CausalUncertainty.SAME_MATCH_ORDER_UNPROVEN.value))
            continue
        if event.kind is CausalEventKind.BOOK:
            ignored_count += 1
            decisions.append(_decision(event, "BOOK_OBSERVED", "BOOK_DOES_NOT_ESTABLISH_FILL_ORDER"))
            continue

        trade = event.trade
        assert trade is not None
        event_time = event.causal_monotonic_ns
        if event_time < quote.decision_ready_monotonic_ns:
            ignored_count += 1
            decisions.append(_decision(event, "IGNORED", "BEFORE_DECISION_READY"))
            continue
        if activation is None or event_time < activation:
            ignored_count += 1
            decisions.append(_decision(event, "IGNORED", "BEFORE_HYPOTHETICAL_ACTIVATION"))
            continue
        if block_fence is not None:
            if event.block_number is None:
                reasons.add(CausalUncertainty.WATERMARK_IDENTITY_MISSING)
                decisions.append(
                    _decision(event, "UNCERTAIN", CausalUncertainty.WATERMARK_IDENTITY_MISSING.value)
                )
                continue
            if event.block_number <= block_fence:
                reasons.add(CausalUncertainty.WATERMARK_BOUNDARY_AMBIGUOUS)
                decisions.append(
                    _decision(
                        event,
                        "UNCERTAIN",
                        CausalUncertainty.WATERMARK_BOUNDARY_AMBIGUOUS.value,
                    )
                )
                continue
        effective_cancel = active_cancel
        if auto_cancel_effective is not None:
            effective_cancel = (
                auto_cancel_effective
                if effective_cancel is None
                else min(effective_cancel, auto_cancel_effective)
            )
        # Cancellation is a modeled local receipt boundary.  Processing may
        # learn an already-received event after the effective time; that
        # delay cannot erase the event from the modeled active window.
        if effective_cancel is not None and event_time >= effective_cancel:
            ignored_count += 1
            decisions.append(_decision(event, "IGNORED", "AT_OR_AFTER_CANCEL_EFFECTIVE"))
            continue
        if (
            quote.replacement_effective_monotonic_ns is not None
            and event_time >= quote.replacement_effective_monotonic_ns
        ):
            ignored_count += 1
            decisions.append(_decision(event, "IGNORED", "AT_OR_AFTER_REPLACEMENT"))
            continue
        expected_aggressor = Side.SELL if quote.maker_side is Side.BUY else Side.BUY
        if trade.aggressor_side is not expected_aggressor:
            ignored_count += 1
            decisions.append(_decision(event, "IGNORED", "WRONG_AGGRESSOR_SIDE"))
            continue
        crosses, boundary_ambiguous = _trade_price_classification(quote, trade)
        if boundary_ambiguous:
            reasons.add(CausalUncertainty.QUOTE_TOUCH_ORDER_UNPROVEN)
            decisions.append(
                _decision(
                    event,
                    "UNCERTAIN",
                    CausalUncertainty.QUOTE_TOUCH_ORDER_UNPROVEN.value,
                )
            )
            continue
        if not crosses:
            ignored_count += 1
            decisions.append(_decision(event, "IGNORED", "NOT_TRADE_THROUGH_QUOTE_PRICE"))
            continue
        if remaining <= 0:
            ignored_count += 1
            decisions.append(_decision(event, "IGNORED", "QUOTE_QUANTITY_EXHAUSTED"))
            continue
        consumed = min(trade.canonical_quantity, remaining)
        remaining -= consumed
        observed_filled += consumed
        fills.append(
            CausalFill(
                source_event_id=identity.source_event_id,  # type: ignore[arg-type]
                source_identity=identity,
                received_monotonic_ns=event.ingress_received_monotonic_ns,  # type: ignore[arg-type]
                price=quote.price,
                observed_quantity=trade.canonical_quantity,
                consumed_quantity=consumed,
                remaining_quantity=remaining,
                match_id=event.match_id,
                observed_trade_price=trade.canonical_price,
                processed_ready_monotonic_ns=processing_ready,
            )
        )
        decisions.append(_decision(event, "FILL", "ELIGIBLE_TRADE", consumed))
        if quote.cancel_on_first_partial and auto_cancel_effective is None and consumed < quote.quantity:
            auto_cancel_requested = processing_ready
            auto_cancel_effective = processing_ready + quote.cancel_delay_ns

    effective_cancel = (
        quote.cancel_effective_monotonic_ns
        if auto_cancel_effective is None
        else (
            auto_cancel_effective
            if quote.cancel_effective_monotonic_ns is None
            else min(quote.cancel_effective_monotonic_ns, auto_cancel_effective)
        )
    )
    effective_cancel_request = (
        quote.cancel_requested_monotonic_ns
        if auto_cancel_requested is None
        else (
            auto_cancel_requested
            if quote.cancel_effective_monotonic_ns is None
            or auto_cancel_effective < quote.cancel_effective_monotonic_ns
            else quote.cancel_requested_monotonic_ns
        )
    )

    if reasons:
        outcome = CausalOutcome.CAUSAL_UNCERTAIN
        proven_filled = Decimal("0")
    elif remaining == 0:
        outcome = CausalOutcome.FULL_FILL
        proven_filled = observed_filled
    elif observed_filled > 0:
        outcome = CausalOutcome.PARTIAL_FILL
        proven_filled = observed_filled
    elif (
        quote.replacement_effective_monotonic_ns is not None
        and end >= quote.replacement_effective_monotonic_ns
    ):
        outcome = CausalOutcome.REPLACED_NO_FILL
        proven_filled = Decimal("0")
    elif effective_cancel is not None and end >= effective_cancel:
        outcome = CausalOutcome.CANCELLED_NO_FILL
        proven_filled = Decimal("0")
    else:
        outcome = CausalOutcome.NO_FILL
        proven_filled = Decimal("0")
    return CausalQuoteMeasurement(
        quote=quote,
        outcome=outcome,
        observed_filled_quantity=observed_filled,
        filled_quantity=proven_filled,
        remaining_quantity=remaining,
        fills=tuple(fills),
        decisions=tuple(decisions),
        uncertainty_reasons=tuple(sorted(reasons, key=lambda value: value.value)),
        timing=timing,
        event_count=len(materialized_events),
        duplicate_event_count=duplicate_count,
        ignored_event_count=ignored_count,
        last_event_monotonic_ns=last_event_ns,
        effective_cancel_monotonic_ns=effective_cancel,
        cancel_requested_monotonic_ns=effective_cancel_request,
    )


measure_quote_execution = measure_causal_quote
measure_causal_execution = measure_causal_quote
measure_resting_quote = measure_causal_quote
RestingQuote = CausalRestingQuote
CausalQuote = CausalRestingQuote
CausalExecutionResult = CausalQuoteMeasurement


__all__ = [
    "CausalEvent",
    "CausalEventDecision",
    "CausalEventKind",
    "CausalExecutionResult",
    "CausalFill",
    "CausalOutcome",
    "CausalQuote",
    "CausalQuoteMeasurement",
    "CausalRestingQuote",
    "CausalSourceIdentity",
    "CausalTimingDiagnostics",
    "CausalUncertainty",
    "HypotheticalBlockWatermark",
    "RestingQuote",
    "build_causal_resting_quote",
    "causal_book_event",
    "causal_gap_event",
    "causal_trade_event",
    "measure_causal_execution",
    "measure_causal_quote",
    "measure_quote_execution",
    "measure_resting_quote",
]
