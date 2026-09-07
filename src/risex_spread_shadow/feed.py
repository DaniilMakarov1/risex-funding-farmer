"""Limited RISEx/Lighter public feed ingress for SS-001H."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
import hashlib
import json
import time
from typing import Any, Callable

import aiohttp
from aiohttp import WSCloseCode

from risex_farmer.exchanges.lighter import LighterAdapter
from risex_farmer.exchanges.risex import RisexAdapter
from risex_farmer.market_data import BookStream
from risex_farmer.models import BookDelta, CanonicalMarket, OrderBook, Venue

from .config import MAX_PUBLIC_DURATION_SECONDS, ShadowConfig
from .book_chain import book_state_sha256
from .models import BookEvidence, DataGapEvidence, TradeEvidence


_DRAIN_SCHEDULING_ALLOWANCE_SECONDS = 1.2
_PROTOCOL_FRAME_LENGTH_CAP = 65_536
_NORMAL_CLOSE_CODE = int(WSCloseCode.OK)


class _UnexpectedPublicClose(ConnectionError):
    """Sanitized marker for a close without the normal WebSocket code."""


def _transport_failure_class(exc: BaseException) -> str:
    """Classify transport failures without retaining exception details."""

    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return "TIMEOUT"
    if isinstance(
        exc,
        (
            ConnectionResetError,
            ConnectionAbortedError,
            BrokenPipeError,
        ),
    ):
        return "RESET"
    if isinstance(exc, ConnectionError):
        return "ERROR"
    return "EXCEPTION"


def _sanitized_exception_type(exc: BaseException) -> str:
    """Return only a bounded printable exception type, never its message."""

    value = type(exc).__name__
    sanitized = "".join(
        character if 0x20 <= ord(character) <= 0x7E else "?"
        for character in value
    )
    return sanitized[:64] or "UNKNOWN"


def _protocol_frame_evidence(data: Any) -> tuple[int, str]:
    """Return bounded, non-raw evidence for one invalid public frame."""

    if isinstance(data, str):
        raw = data.encode("utf-8", "replace")
    elif isinstance(data, bytes):
        raw = data
    elif isinstance(data, (bytearray, memoryview)):
        raw = bytes(data)
    elif data is None:
        raw = b""
    else:
        # Do not retain or stringify an arbitrary payload.  Its type is enough
        # to distinguish an unexpected object from a text/binary frame.
        raw = type(data).__name__.encode("ascii", "replace")
    bounded_length = min(len(raw), _PROTOCOL_FRAME_LENGTH_CAP)
    digest = hashlib.sha256(raw).hexdigest()
    return bounded_length, digest


@dataclass(frozen=True, slots=True)
class MarketPair:
    """One canonical public RISEx/Lighter market pair."""

    canonical_market: str
    risex_market: CanonicalMarket
    lighter_market: CanonicalMarket

    def __post_init__(self) -> None:
        if not self.canonical_market:
            raise ValueError("canonical_market must be non-empty")
        if self.risex_market.venue is not Venue.RISEX:
            raise ValueError("risex_market must identify RISEx")
        if self.lighter_market.venue is not Venue.LIGHTER:
            raise ValueError("lighter_market must identify Lighter")
        for market in (self.risex_market, self.lighter_market):
            identities = {
                value
                for value in (
                    market.canonical_asset,
                    market.venue_symbol,
                    getattr(market, "canonical_market", None),
                )
                if isinstance(value, str) and value
            }
            if self.canonical_market not in identities:
                raise ValueError("market pair identities do not match")


@dataclass(frozen=True, slots=True)
class FeedBookEvent:
    book: BookEvidence
    market_pair: MarketPair
    source_kind: str
    checksum_validation: str
    receipt_id: str | None = None
    unchanged: bool = False

    def __post_init__(self) -> None:
        if self.book.venue not in (Venue.RISEX, Venue.LIGHTER):
            raise ValueError("feed book must identify RISEx or Lighter")
        if self.book.canonical_market != self.market_pair.canonical_market:
            raise ValueError("feed book market does not match pair")
        if self.source_kind not in {"SNAPSHOT", "DELTA"}:
            raise ValueError("unsupported feed book source kind")
        if not self.checksum_validation:
            raise ValueError("checksum validation label must be non-empty")
        if self.receipt_id is not None and not self.receipt_id:
            raise ValueError("receipt_id must be non-empty when supplied")
        if not isinstance(self.unchanged, bool):
            raise TypeError("unchanged must be bool")


@dataclass(frozen=True, slots=True)
class FeedReceiptEvent:
    """Compact application-level receipt captured before market filtering."""

    receipt_id: str
    venue: Venue
    stream_session_id: str | int
    recovery_generation: int
    received_utc: datetime
    received_monotonic_ns: int
    frame_kind: str
    frame_category: str
    frame_length: int
    frame_sha256: str
    orderbook_candidate: bool
    canonical_market: str | None = None
    venue_symbol: str | None = None
    market_id: str | int | None = None
    message_type: str | None = None
    channel: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.receipt_id, str) or not self.receipt_id:
            raise ValueError("receipt_id must be a non-empty string")
        venue = self.venue if isinstance(self.venue, Venue) else Venue(self.venue)
        if venue not in (Venue.RISEX, Venue.LIGHTER):
            raise ValueError("receipt venue must be RISEx or Lighter")
        object.__setattr__(self, "venue", venue)
        if isinstance(self.stream_session_id, bool) or not isinstance(
            self.stream_session_id, (str, int)
        ):
            raise TypeError("receipt stream_session_id must be str or int")
        if isinstance(self.stream_session_id, str) and not self.stream_session_id:
            raise ValueError("receipt stream_session_id must be non-empty")
        if (
            isinstance(self.recovery_generation, bool)
            or not isinstance(self.recovery_generation, int)
            or self.recovery_generation < 0
        ):
            raise ValueError("receipt recovery_generation must be non-negative")
        if not isinstance(self.received_utc, datetime):
            raise TypeError("receipt received_utc must be datetime")
        if self.received_utc.tzinfo is None or self.received_utc.utcoffset() is None:
            raise ValueError("receipt received_utc must be timezone-aware")
        if self.received_utc.utcoffset().total_seconds() != 0:
            raise ValueError("receipt received_utc must use UTC")
        if (
            isinstance(self.received_monotonic_ns, bool)
            or not isinstance(self.received_monotonic_ns, int)
            or self.received_monotonic_ns < 0
        ):
            raise ValueError("receipt received_monotonic_ns must be non-negative")
        for value, name, limit in (
            (self.frame_kind, "frame_kind", 64),
            (self.frame_category, "frame_category", 96),
        ):
            if (
                not isinstance(value, str)
                or not value
                or len(value) > limit
                or any(ord(character) < 0x20 or ord(character) > 0x7E for character in value)
            ):
                raise ValueError(f"receipt {name} must be bounded printable ASCII")
        if (
            isinstance(self.frame_length, bool)
            or not isinstance(self.frame_length, int)
            or not 0 <= self.frame_length <= _PROTOCOL_FRAME_LENGTH_CAP
        ):
            raise ValueError("receipt frame_length must be bounded")
        if (
            not isinstance(self.frame_sha256, str)
            or len(self.frame_sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.frame_sha256)
        ):
            raise ValueError("receipt frame_sha256 must be lowercase SHA-256")
        if not isinstance(self.orderbook_candidate, bool):
            raise TypeError("receipt orderbook_candidate must be bool")
        for value, name in (
            (self.canonical_market, "canonical_market"),
            (self.venue_symbol, "venue_symbol"),
            (self.message_type, "message_type"),
            (self.channel, "channel"),
        ):
            if value is not None and (not isinstance(value, str) or not value):
                raise ValueError(f"receipt {name} must be non-empty when supplied")
        if self.market_id is not None and (
            isinstance(self.market_id, bool)
            or not isinstance(self.market_id, (str, int))
            or self.market_id == ""
        ):
            raise TypeError("receipt market_id must be a non-empty string or int")


@dataclass(frozen=True, slots=True)
class FeedTradeEvent:
    trade: TradeEvidence
    market_pair: MarketPair
    raw_timestamp: str | int | None = None
    receipt_id: str | None = None

    def __post_init__(self) -> None:
        if self.trade.venue is not Venue.RISEX:
            raise ValueError("feed trades are RISEx maker evidence only")
        if self.trade.canonical_market != self.market_pair.canonical_market:
            raise ValueError("feed trade market does not match pair")
        if self.receipt_id is not None and not self.receipt_id:
            raise ValueError("receipt_id must be non-empty when supplied")


@dataclass(frozen=True, slots=True)
class FeedGapEvent:
    gap: DataGapEvidence


IngressItem = FeedReceiptEvent | FeedBookEvent | FeedTradeEvent | FeedGapEvent


class IngressQueue:
    """A bounded non-blocking ingress with a durable-in-memory gap latch."""

    def __init__(self, capacity: int, *, preserve_offer_order: bool = False) -> None:
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity <= 0:
            raise ValueError("ingress queue capacity must be positive")
        if not isinstance(preserve_offer_order, bool):
            raise TypeError("preserve_offer_order must be bool")
        self._preserve_offer_order = preserve_offer_order
        self._queue: asyncio.Queue[tuple[IngressItem, int, int]] = asyncio.Queue(
            maxsize=capacity
        )
        self._queued_entries: deque[tuple[IngressItem, int, int]] = deque()
        self._wake = asyncio.Event()
        self._latched: dict[tuple[Venue, str, str | int, int], DataGapEvidence] = {}
        self._latched_received: dict[tuple[Venue, str, str | int, int], int] = {}
        self._latched_serial: dict[tuple[Venue, str, str | int, int], int] = {}
        self._in_flight: tuple[IngressItem, int] | None = None
        self._consumer_failed = False
        self._progress = asyncio.Event()
        self._closed = False
        self._offer_serial = 0

    @property
    def capacity(self) -> int:
        return self._queue.maxsize

    @property
    def qsize(self) -> int:
        return self._queue.qsize()

    @property
    def has_latched_gap(self) -> bool:
        return bool(self._latched)

    @property
    def has_pending(self) -> bool:
        return bool(self._latched) or not self._queue.empty()

    @property
    def offer_serial(self) -> int:
        """Monotonic count used by the observer's idle-queue watermark."""

        return self._offer_serial

    @staticmethod
    def _item_received_monotonic_ns(item: IngressItem) -> int:
        if isinstance(item, FeedReceiptEvent):
            return item.received_monotonic_ns
        if isinstance(item, FeedBookEvent):
            return item.book.received_monotonic_ns
        if isinstance(item, FeedTradeEvent):
            return item.trade.received_monotonic_ns
        return item.gap.gap_start_monotonic_ns

    @staticmethod
    def _is_lighter_horizon_item(item: IngressItem) -> bool:
        if isinstance(item, FeedReceiptEvent):
            return False
        if isinstance(item, FeedBookEvent):
            return item.book.venue is Venue.LIGHTER
        if isinstance(item, FeedGapEvent):
            return item.gap.source_venue is Venue.LIGHTER
        return False

    def _has_lighter_before(self, deadline_ns: int) -> bool:
        if (
            self._in_flight is not None
            and self._in_flight[1] <= deadline_ns
            and self._is_lighter_horizon_item(self._in_flight[0])
        ):
            return True
        if any(
            received <= deadline_ns and self._is_lighter_horizon_item(item)
            for item, received, _serial in self._queued_entries
        ):
            return True
        return any(
            received <= deadline_ns
            and self._is_lighter_horizon_item(
                FeedGapEvent(self._latched[key])
            )
            for key, received in self._latched_received.items()
        )

    async def wait_for_lighter_before(self, deadline_ns: int) -> bool:
        """Wait until received-at-deadline Lighter ingress has been handled.

        A horizon task must wait for the observer's consumer acknowledgement,
        not merely yield to the event loop.  The wait is limited to
        Lighter books/gaps whose receipt timestamp is within the horizon;
        later books remain in ingress and are still excluded by the pure
        deadline selector.
        """

        if isinstance(deadline_ns, bool) or not isinstance(deadline_ns, int):
            raise TypeError("deadline_ns must be int")
        if deadline_ns < 0:
            raise ValueError("deadline_ns must be non-negative")
        while True:
            if self._consumer_failed:
                return False
            if not self._has_lighter_before(deadline_ns):
                watermark = self._offer_serial
                self._progress.clear()
                if (
                    not self._has_lighter_before(deadline_ns)
                    and self._offer_serial == watermark
                ):
                    return True
                continue
            self._progress.clear()
            if not self._has_lighter_before(deadline_ns):
                continue
            await self._progress.wait()

    @staticmethod
    def _item_gap(item: IngressItem) -> DataGapEvidence:
        if isinstance(item, FeedGapEvent):
            return item.gap
        if isinstance(item, FeedReceiptEvent):
            return DataGapEvidence(
                source_venue=item.venue,
                canonical_market=item.canonical_market or "UNKNOWN",
                stream_session_id=item.stream_session_id,
                recovery_generation=item.recovery_generation,
                gap_start_monotonic_ns=item.received_monotonic_ns,
                reason="QUEUE_OVERFLOW",
                receipt_id=item.receipt_id,
            )
        if isinstance(item, FeedBookEvent):
            book = item.book
            return DataGapEvidence(
                source_venue=book.venue,
                canonical_market=book.canonical_market,
                stream_session_id=book.stream_session_id,
                recovery_generation=book.recovery_generation,
                gap_start_monotonic_ns=book.received_monotonic_ns,
                reason="QUEUE_OVERFLOW",
                receipt_id=item.receipt_id,
            )
        trade = item.trade
        return DataGapEvidence(
            source_venue=trade.venue,
            canonical_market=trade.canonical_market,
            stream_session_id=trade.stream_session_id,
            recovery_generation=trade.recovery_generation,
            gap_start_monotonic_ns=trade.received_monotonic_ns,
            reason="QUEUE_OVERFLOW",
            receipt_id=item.receipt_id,
        )

    def _latch(self, gap: DataGapEvidence, *, offer_serial: int) -> None:
        identity = (
            gap.source_venue,
            gap.canonical_market,
            gap.stream_session_id,
            gap.recovery_generation,
        )
        previous = self._latched.get(identity)
        if previous is None:
            self._latched[identity] = gap
            self._latched_received[identity] = gap.gap_start_monotonic_ns
            self._latched_serial[identity] = offer_serial
            return
        end = gap.gap_end_monotonic_ns
        if previous.gap_end_monotonic_ns is None or end is None:
            end = None
        else:
            end = max(previous.gap_end_monotonic_ns, end)
        protocol_gap = next(
            (
                candidate
                for candidate in (previous, gap)
                if candidate.protocol_frame_kind is not None
            ),
            None,
        )
        transport_gap = next(
            (
                candidate
                for candidate in (gap, previous)
                if candidate.transport_event == "UNEXPECTED_FAILURE"
            ),
            next(
                (
                    candidate
                    for candidate in (previous, gap)
                    if candidate.transport_event is not None
                ),
                None,
            ),
        )
        self._latched[identity] = DataGapEvidence(
            source_venue=previous.source_venue,
            canonical_market=previous.canonical_market,
            stream_session_id=previous.stream_session_id,
            recovery_generation=previous.recovery_generation,
            gap_start_monotonic_ns=min(
                previous.gap_start_monotonic_ns, gap.gap_start_monotonic_ns
            ),
            gap_end_monotonic_ns=end,
            reason=("QUEUE_OVERFLOW" if protocol_gap is None else protocol_gap.reason),
            protocol_frame_kind=(
                None if protocol_gap is None else protocol_gap.protocol_frame_kind
            ),
            protocol_frame_category=(
                None if protocol_gap is None else protocol_gap.protocol_frame_category
            ),
            protocol_frame_length=(
                None if protocol_gap is None else protocol_gap.protocol_frame_length
            ),
            protocol_frame_sha256=(
                None if protocol_gap is None else protocol_gap.protocol_frame_sha256
            ),
            transport_event=(
                None if transport_gap is None else transport_gap.transport_event
            ),
            transport_failure_class=(
                None
                if transport_gap is None
                else transport_gap.transport_failure_class
            ),
            transport_exception_type=(
                None
                if transport_gap is None
                else transport_gap.transport_exception_type
            ),
            receipt_id=next(
                (
                    candidate.receipt_id
                    for candidate in (previous, gap)
                    if candidate.receipt_id is not None
                ),
                None,
            ),
        )
        self._latched_received[identity] = min(
            self._latched_received[identity], gap.gap_start_monotonic_ns
        )
        self._latched_serial[identity] = min(
            self._latched_serial[identity], offer_serial
        )

    def offer(self, item: IngressItem) -> bool:
        """Enqueue without waiting; an overflow always latches explicit gap evidence."""

        self._offer_serial += 1
        received = self._item_received_monotonic_ns(item)
        if self._closed:
            self._latch(self._item_gap(item), offer_serial=self._offer_serial)
            self._wake.set()
            self._progress.set()
            return False
        try:
            entry = (item, received, self._offer_serial)
            self._queue.put_nowait(entry)
            self._queued_entries.append(entry)
        except asyncio.QueueFull:
            self._latch(self._item_gap(item), offer_serial=self._offer_serial)
            self._wake.set()
            self._progress.set()
            return False
        self._wake.set()
        self._progress.set()
        return True

    async def next_item(self) -> IngressItem | None:
        while True:
            if self._latched:
                if self._preserve_offer_order:
                    latched_key = min(
                        self._latched,
                        key=lambda value: self._latched_serial[value],
                    )
                    latched_serial = self._latched_serial[latched_key]
                    if self._queued_entries and self._queued_entries[0][2] < latched_serial:
                        entry = self._queue.get_nowait()
                        queued = self._queued_entries.popleft()
                        if queued != entry:
                            raise RuntimeError("ingress queue coordination lost item order")
                        self._in_flight = (entry[0], entry[1])
                        return entry[0]
                else:
                    latched_key = sorted(
                        self._latched,
                        key=lambda value: (
                            value[1],
                            value[0].value,
                            str(value[2]),
                            value[3],
                        ),
                    )[0]
                gap = self._latched.pop(latched_key)
                received = self._latched_received.pop(latched_key)
                self._latched_serial.pop(latched_key)
                item = FeedGapEvent(gap)
                self._in_flight = (item, received)
                return item
            try:
                entry = self._queue.get_nowait()
                queued = self._queued_entries.popleft()
                if queued != entry:
                    raise RuntimeError("ingress queue coordination lost item order")
                self._in_flight = (entry[0], entry[1])
                return entry[0]
            except asyncio.QueueEmpty:
                pass
            if self._closed:
                return None

            # Clear only after observing an empty, open queue.  The second
            # check closes the small same-loop race with an offer/close that
            # happened before the consumer started waiting.
            self._wake.clear()
            if self._latched or not self._queue.empty() or self._closed:
                continue
            await self._wake.wait()

    def close(self) -> None:
        self._closed = True
        self._wake.set()
        self._progress.set()

    def complete_item(self, *, success: bool = True) -> None:
        """Acknowledge the item currently being handled by the observer."""

        if not success:
            self._consumer_failed = True
        self._in_flight = None
        self._progress.set()


@dataclass(slots=True)
class _StreamState:
    market_pair: MarketPair
    venue: Venue
    venue_symbol: str
    stream: BookStream
    session_id: str | int | None = None
    recovery_generation: int = 0
    book_revision: int = 0
    last_emitted_state_sha256: str | None = None
    connected: bool = False
    awaiting_snapshot: bool = True


class PublicFeedRunner:
    """Only the two active public streams; no observer or store is called here."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        market_pairs: tuple[MarketPair, ...] | list[MarketPair],
        ingress: IngressQueue,
        *,
        config: ShadowConfig | None = None,
        risex_adapter: RisexAdapter | None = None,
        lighter_adapter: LighterAdapter | None = None,
        now_utc: Callable[[], datetime] | None = None,
        monotonic_ns: Callable[[], int] | None = None,
        capture_receipts: bool = False,
    ) -> None:
        pairs = tuple(market_pairs)
        if not pairs or len(pairs) > 3:
            raise ValueError("a smoke runner requires one to three market pairs")
        if len({pair.canonical_market for pair in pairs}) != len(pairs):
            raise ValueError("market pairs must be unique")
        self.session = session
        self.market_pairs = pairs
        self.ingress = ingress
        self.config = config or ShadowConfig()
        self.risex = risex_adapter or RisexAdapter(session)
        self.lighter = lighter_adapter or LighterAdapter(session)
        self._now_utc = now_utc or (lambda: datetime.now(UTC))
        self._monotonic_ns = monotonic_ns or time.monotonic_ns
        if not isinstance(capture_receipts, bool):
            raise TypeError("capture_receipts must be bool")
        self.capture_receipts = capture_receipts
        self._states: dict[tuple[Venue, str], _StreamState] = {}
        self._pair_by_risex_symbol = {
            pair.risex_market.venue_symbol: pair for pair in pairs
        }
        self._pair_by_lighter_symbol = {
            pair.lighter_market.venue_symbol: pair for pair in pairs
        }
        self._risex_by_market_id = {
            self.risex.market_id(pair.risex_market.venue_symbol): pair for pair in pairs
        }
        self._lighter_by_market_id = {
            self.lighter.market_id(pair.lighter_market.venue_symbol): pair for pair in pairs
        }
        for pair in pairs:
            self._states[(Venue.RISEX, pair.canonical_market)] = _StreamState(
                pair,
                Venue.RISEX,
                pair.risex_market.venue_symbol,
                BookStream(Venue.RISEX, pair.risex_market.venue_symbol),
            )
            self._states[(Venue.LIGHTER, pair.canonical_market)] = _StreamState(
                pair,
                Venue.LIGHTER,
                pair.lighter_market.venue_symbol,
                BookStream(Venue.LIGHTER, pair.lighter_market.venue_symbol),
            )
        self.fatal_reason: str | None = None
        self._connection_serial = {Venue.RISEX: 0, Venue.LIGHTER: 0}
        self._receipt_serial = {Venue.RISEX: 0, Venue.LIGHTER: 0}
        self._external_stop_event: asyncio.Event | None = None

    def state(self, venue: Venue, canonical_market: str) -> _StreamState:
        return self._states[(venue, canonical_market)]

    def _session_name(self, venue: Venue) -> str:
        self._connection_serial[venue] += 1
        return f"{venue.value.lower()}-public-{self._connection_serial[venue]}"

    def _gap(
        self,
        state: _StreamState,
        *,
        reason: str,
        start: int | None = None,
        session_id: str | int | None = None,
        recovery_generation: int | None = None,
        protocol_evidence: Mapping[str, Any] | None = None,
        transport_evidence: Mapping[str, Any] | None = None,
        receipt_id: str | None = None,
    ) -> None:
        if session_id is None:
            session_id = state.session_id
        if session_id is None:
            if protocol_evidence is None and transport_evidence is None:
                return
            session_id = "unknown"
        if recovery_generation is None:
            recovery_generation = state.recovery_generation
        protocol_fields = {} if protocol_evidence is None else dict(protocol_evidence)
        transport_fields = {} if transport_evidence is None else dict(transport_evidence)
        self.ingress.offer(
            FeedGapEvent(
                DataGapEvidence(
                    source_venue=state.venue,
                    canonical_market=state.market_pair.canonical_market,
                    stream_session_id=session_id,
                    recovery_generation=recovery_generation,
                    gap_start_monotonic_ns=(
                        self._monotonic_ns() if start is None else start
                    ),
                    reason=reason,
                    protocol_frame_kind=protocol_fields.get("protocol_frame_kind"),
                    protocol_frame_category=protocol_fields.get("protocol_frame_category"),
                    protocol_frame_length=protocol_fields.get("protocol_frame_length"),
                    protocol_frame_sha256=protocol_fields.get("protocol_frame_sha256"),
                    transport_event=transport_fields.get("transport_event"),
                    transport_failure_class=transport_fields.get(
                        "transport_failure_class"
                    ),
                    transport_exception_type=transport_fields.get(
                        "transport_exception_type"
                    ),
                    receipt_id=receipt_id,
                )
            )
        )

    def begin_connection(self, venue: Venue, session_id: str | int | None = None) -> None:
        session_id = session_id or self._session_name(venue)
        now = self._now_utc()
        for pair in self.market_pairs:
            state = self.state(venue, pair.canonical_market)
            if state.connected and state.session_id is not None:
                old_session = state.session_id
                old_generation = state.recovery_generation
                self._gap(
                    state,
                    reason="PUBLIC_SOCKET_RECONNECTED",
                    session_id=old_session,
                    recovery_generation=old_generation,
                    transport_evidence={"transport_event": "RECONNECT"},
                )
                state.recovery_generation += 1
            elif state.session_id is not None:
                # disconnect() already persisted the old identity's transport
                # gap.  Reusing that identity here only advances the recovery
                # generation; emitting another gap would double-count one
                # graceful rollover.
                state.recovery_generation += 1
            state.stream.disconnected()
            state.session_id = session_id
            state.connected = True
            state.awaiting_snapshot = True
            state.last_emitted_state_sha256 = None
            state.stream.connected(now)

    def disconnect(
        self,
        venue: Venue,
        *,
        reason: str = "PUBLIC_SOCKET_DISCONNECTED",
        transport_evidence: Mapping[str, Any] | None = None,
    ) -> None:
        for pair in self.market_pairs:
            state = self.state(venue, pair.canonical_market)
            if not state.connected:
                continue
            self._gap(
                state,
                reason=reason,
                transport_evidence=transport_evidence,
            )
            state.stream.disconnected()
            state.connected = False
            state.awaiting_snapshot = True
            state.last_emitted_state_sha256 = None

    def _record_transport_failure(self, venue: Venue, exc: BaseException) -> None:
        """Persist one sanitized unexpected transport failure and halt closed."""

        self.fatal_reason = "PUBLIC_SOCKET_TRANSPORT_FAILURE"
        if self._external_stop_event is not None:
            self._external_stop_event.set()
        evidence = {
            "transport_event": "UNEXPECTED_FAILURE",
            "transport_failure_class": _transport_failure_class(exc),
            "transport_exception_type": _sanitized_exception_type(exc),
        }
        for pair in self.market_pairs:
            state = self.state(venue, pair.canonical_market)
            self._gap(
                state,
                reason="PUBLIC_SOCKET_DISCONNECTED",
                transport_evidence=evidence,
            )
            state.stream.gap()
            state.connected = False
            state.awaiting_snapshot = True
            state.last_emitted_state_sha256 = None

    def _state_for_symbol(self, venue: Venue, symbol: str) -> _StreamState | None:
        pair = (
            self._pair_by_risex_symbol.get(symbol)
            if venue is Venue.RISEX
            else self._pair_by_lighter_symbol.get(symbol)
        )
        return None if pair is None else self.state(venue, pair.canonical_market)

    @staticmethod
    def _mapping_frame_evidence(payload: Mapping[str, Any]) -> tuple[int, str]:
        """Hash a synthetic fixture mapping without persisting its contents."""

        try:
            encoded = json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ).encode("utf-8")
        except (TypeError, ValueError):
            encoded = type(payload).__name__.encode("ascii", "replace")
        return _protocol_frame_evidence(encoded)

    @staticmethod
    def _bounded_text(value: Any, *, limit: int) -> str | None:
        if value is None:
            return None
        text = str(value)
        sanitized = "".join(
            character if 0x20 <= ord(character) <= 0x7E else "?"
            for character in text
        )[:limit]
        return sanitized or None

    @staticmethod
    def _payload_market_id(payload: Mapping[str, Any]) -> str | int | None:
        value = payload.get("market_id")
        if value is None and isinstance(payload.get("data"), Mapping):
            value = payload["data"].get("market_id")
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            return None
        if isinstance(value, str) and not value:
            return None
        return value

    @staticmethod
    def _payload_is_orderbook(payload: Mapping[str, Any] | None) -> bool:
        if payload is None:
            return False
        message_type = str(payload.get("type", "")).lower()
        channel = str(payload.get("channel", "")).lower()
        return (
            channel in {"orderbook", "order_book"}
            or "orderbook" in message_type
            or "order_book" in message_type
            or isinstance(payload.get("order_book"), Mapping)
        ) and message_type not in {"subscribed", "ack", "connected", "unsubscribed"}

    def _receipt_context(
        self,
        venue: Venue,
        payload: Mapping[str, Any] | None,
    ) -> tuple[_StreamState | None, str | None, str | None, str | int | None]:
        state: _StreamState | None = None
        market_id = None if payload is None else self._payload_market_id(payload)
        if market_id is not None:
            if venue is Venue.RISEX:
                pair = self._risex_by_market_id.get(market_id)
                if pair is None:
                    try:
                        pair = self._risex_by_market_id.get(int(str(market_id)))
                    except (TypeError, ValueError):
                        pair = None
            else:
                pair = self._lighter_by_market_id.get(market_id)
                if pair is None:
                    try:
                        pair = self._lighter_by_market_id.get(int(str(market_id)))
                    except (TypeError, ValueError):
                        pair = None
            if pair is not None:
                state = self.state(venue, pair.canonical_market)
        if state is None:
            state = next(
                (
                    candidate
                    for (candidate_venue, _market), candidate in self._states.items()
                    if candidate_venue is venue
                ),
                None,
            )
        symbol = None if state is None else state.venue_symbol
        canonical_market = None if state is None else state.market_pair.canonical_market
        return state, canonical_market, symbol, market_id

    def _capture_receipt(
        self,
        venue: Venue,
        *,
        frame_kind: str,
        frame_data: Any,
        payload: Mapping[str, Any] | None,
        received_at: datetime,
        received_monotonic_ns: int,
    ) -> FeedReceiptEvent | None:
        if not self.capture_receipts:
            return None
        state, canonical_market, venue_symbol, market_id = self._receipt_context(
            venue, payload
        )
        if payload is not None and frame_data is None:
            frame_length, frame_sha256 = self._mapping_frame_evidence(payload)
        else:
            frame_length, frame_sha256 = _protocol_frame_evidence(frame_data)
        message_type = (
            None
            if payload is None
            else self._bounded_text(payload.get("type"), limit=64)
        )
        channel = (
            None
            if payload is None
            else self._bounded_text(payload.get("channel"), limit=96)
        )
        orderbook_candidate = self._payload_is_orderbook(payload)
        if orderbook_candidate:
            category = "ORDERBOOK"
        elif payload is None:
            category = "UNPARSED_FRAME"
        elif message_type in {"subscribed", "ack", "connected", "unsubscribed"}:
            category = "CONTROL"
        else:
            category = "NON_ORDERBOOK"
        self._receipt_serial[venue] += 1
        receipt = FeedReceiptEvent(
            receipt_id=f"{venue.value}:receipt-{self._receipt_serial[venue]}",
            venue=venue,
            stream_session_id=(
                "unknown" if state is None or state.session_id is None else state.session_id
            ),
            recovery_generation=0 if state is None else state.recovery_generation,
            received_utc=received_at,
            received_monotonic_ns=received_monotonic_ns,
            frame_kind=self._bounded_text(frame_kind, limit=64) or "UNKNOWN",
            frame_category=category,
            frame_length=frame_length,
            frame_sha256=frame_sha256,
            orderbook_candidate=orderbook_candidate,
            canonical_market=canonical_market,
            venue_symbol=venue_symbol,
            market_id=market_id,
            message_type=message_type,
            channel=channel,
        )
        self.ingress.offer(receipt)
        return receipt

    def _protocol_failure(
        self,
        venue: Venue,
        reason: str,
        *,
        frame_kind: str = "UNKNOWN",
        frame_category: str = "UNCLASSIFIED",
        frame_data: Any = None,
    ) -> None:
        self.fatal_reason = reason
        if self._external_stop_event is not None:
            self._external_stop_event.set()
        sanitized_kind = "".join(
            character if 0x20 <= ord(character) <= 0x7E else "?"
            for character in str(frame_kind)
        )[:64] or "UNKNOWN"
        sanitized_category = "".join(
            character if 0x20 <= ord(character) <= 0x7E else "?"
            for character in str(frame_category)
        )[:96] or "UNCLASSIFIED"
        frame_length, frame_sha256 = _protocol_frame_evidence(frame_data)
        protocol_evidence = {
            "protocol_frame_kind": sanitized_kind,
            "protocol_frame_category": sanitized_category,
            "protocol_frame_length": frame_length,
            "protocol_frame_sha256": frame_sha256,
        }
        for pair in self.market_pairs:
            state = self.state(venue, pair.canonical_market)
            self._gap(
                state,
                reason=reason,
                protocol_evidence=protocol_evidence,
            )
            state.stream.gap()
            state.awaiting_snapshot = True
            state.last_emitted_state_sha256 = None

    def _emit_book(
        self,
        state: _StreamState,
        *,
        source_kind: str,
        checksum: int | str | None,
        checksum_validation: str,
        normalized_source: OrderBook | BookDelta | None = None,
        ingress_received_utc: datetime | None = None,
        ingress_received_monotonic_ns: int | None = None,
        normalized_ready_monotonic_ns: int | None = None,
        receipt_id: str | None = None,
    ) -> None:
        orderbook = state.stream.book()
        if orderbook is None or state.session_id is None:
            return
        received_at = (
            self._now_utc()
            if ingress_received_utc is None
            else ingress_received_utc
        )
        received_ns = (
            self._monotonic_ns()
            if ingress_received_monotonic_ns is None
            else ingress_received_monotonic_ns
        )
        normalized_ns = (
            received_ns
            if normalized_ready_monotonic_ns is None
            else normalized_ready_monotonic_ns
        )
        candidate = BookEvidence(
            venue=state.venue,
            canonical_market=state.market_pair.canonical_market,
            bids=orderbook.bids,
            asks=orderbook.asks,
            received_monotonic_ns=received_ns,
            stream_session_id=state.session_id,
            recovery_generation=state.recovery_generation,
            book_revision=state.book_revision,
            sequence=orderbook.sequence,
            checksum=checksum,
            sequence_valid=state.stream.book_sequence_valid,
            checksum_valid=state.stream.book_sequence_valid,
            received_utc=received_at,
            fresh=True,
            ingress_received_monotonic_ns=received_ns,
            normalized_ready_monotonic_ns=normalized_ns,
            tx_hash=(None if normalized_source is None else normalized_source.tx_hash),
            block_number=(
                None if normalized_source is None else normalized_source.block_number
            ),
            log_index=(
                None if normalized_source is None else normalized_source.log_index
            ),
            worker_timestamp=(
                None
                if normalized_source is None
                else normalized_source.worker_timestamp
            ),
        )
        state_digest = book_state_sha256(candidate)
        unchanged = (
            self.capture_receipts
            and state.last_emitted_state_sha256 is not None
            and state.last_emitted_state_sha256 == state_digest
        )
        if not unchanged:
            state.book_revision += 1
            candidate = replace(candidate, book_revision=state.book_revision)
        if self.capture_receipts:
            state.last_emitted_state_sha256 = state_digest
        offered = self.ingress.offer(
            FeedBookEvent(
                candidate,
                state.market_pair,
                source_kind,
                checksum_validation,
                receipt_id=receipt_id,
                unchanged=unchanged,
            )
        )
        if self.capture_receipts and not offered:
            # Do not let an undelivered event make a later message look like
            # an unchanged durable state.
            state.last_emitted_state_sha256 = None

    async def _recover_market(
        self,
        state: _StreamState,
        ws: Any | None,
        *,
        reason: str,
        receipt_id: str | None = None,
    ) -> None:
        old_generation = state.recovery_generation
        self._gap(
            state,
            reason=reason,
            recovery_generation=old_generation,
            receipt_id=receipt_id,
        )
        state.stream.gap()
        state.recovery_generation += 1
        state.awaiting_snapshot = True
        state.last_emitted_state_sha256 = None
        if state.venue is Venue.RISEX:
            # RISEx exposes the selected orderbooks through one aggregate
            # subscription.  Its unsubscribe/resubscribe therefore invalidates
            # every selected market, not only the market whose checksum failed.
            for other_pair in self.market_pairs:
                other = self.state(Venue.RISEX, other_pair.canonical_market)
                if other is state:
                    continue
                self._gap(other, reason=reason)
                other.stream.gap()
                other.recovery_generation += 1
                other.awaiting_snapshot = True
                other.last_emitted_state_sha256 = None
            if ws is None or state.session_id is None:
                return
            await ws.send_json(self.risex.orderbook_unsubscription())
            await ws.send_json(
                self.risex.orderbook_subscription(
                    [self.risex.market_id(pair.risex_market.venue_symbol) for pair in self.market_pairs]
                )
            )
        else:
            if ws is None or state.session_id is None:
                return
            await ws.send_json(
                self.lighter.subscription(
                    "book", self.lighter.market_id(state.venue_symbol)
                )
            )

    @staticmethod
    def _payload(value: Any) -> Mapping[str, Any] | None:
        if isinstance(value, Mapping):
            return value
        data = getattr(value, "data", None)
        if isinstance(data, Mapping):
            return data
        if isinstance(data, bytes):
            try:
                decoded = data.decode("utf-8")
            except UnicodeDecodeError:
                return None
        elif isinstance(data, str):
            decoded = data
        else:
            return None
        try:
            parsed = json.loads(decoded, parse_float=Decimal)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        return parsed if isinstance(parsed, Mapping) else None

    async def ingest_risex_payload(
        self,
        payload: Mapping[str, Any],
        *,
        received_at: datetime | None = None,
        ingress_received_monotonic_ns: int | None = None,
        normalized_ready_monotonic_ns: int | None = None,
        ws: Any | None = None,
        receipt: FeedReceiptEvent | None = None,
    ) -> None:
        received_at = self._now_utc() if received_at is None else received_at
        if ingress_received_monotonic_ns is None:
            ingress_received_monotonic_ns = self._monotonic_ns()
        if self.capture_receipts and receipt is None:
            receipt = self._capture_receipt(
                Venue.RISEX,
                frame_kind="MAPPING",
                frame_data=None,
                payload=payload,
                received_at=received_at,
                received_monotonic_ns=ingress_received_monotonic_ns,
            )
        receipt_id = None if receipt is None else receipt.receipt_id
        channel = str(payload.get("channel", ""))
        message_type = str(payload.get("type", "")).lower()
        if message_type in {"subscribed", "unsubscribed", "ack", "connected"}:
            return
        trade_channel = channel in {"trades", "trade"}
        if not trade_channel and (
            channel == "orderbook" or "block_number" in payload
        ):
            try:
                normalized = self.risex.normalize_book_message(
                    payload, received_at=received_at
                )
                normalized_ready_ns = (
                    self._monotonic_ns()
                    if normalized_ready_monotonic_ns is None
                    else normalized_ready_monotonic_ns
                )
                state = self._state_for_symbol(Venue.RISEX, normalized.canonical_market)
                if state is None:
                    return
                state.stream.connection_confirmed(received_at)
                if isinstance(normalized, OrderBook):
                    state.stream.snapshot(normalized, sequence=normalized.sequence)
                    if state.stream.book() is None:
                        await self._recover_market(
                            state,
                            ws,
                            reason="RISEX_SNAPSHOT_INVALID",
                            receipt_id=receipt_id,
                        )
                        return
                    state.awaiting_snapshot = False
                    self._emit_book(
                        state,
                        source_kind="SNAPSHOT",
                        checksum=None,
                        checksum_validation="BOOKSTREAM_SNAPSHOT_ACCEPTED_NO_WIRE_CHECKSUM",
                        normalized_source=normalized,
                        ingress_received_utc=received_at,
                        ingress_received_monotonic_ns=ingress_received_monotonic_ns,
                        normalized_ready_monotonic_ns=normalized_ready_ns,
                        receipt_id=receipt_id,
                    )
                    return
                if not isinstance(normalized, BookDelta):
                    return
                if state.awaiting_snapshot or not state.stream.book_initialized:
                    return
                if not state.stream.apply_delta(normalized):
                    await self._recover_market(
                        state,
                        ws,
                        reason="RISEX_CHECKSUM_OR_SEQUENCE_INVALID",
                        receipt_id=receipt_id,
                    )
                    return
                self._emit_book(
                    state,
                    source_kind="DELTA",
                    checksum=normalized.checksum,
                    checksum_validation="BOOKSTREAM_OFFICIAL_WIRE_CHECKSUM",
                    normalized_source=normalized,
                    ingress_received_utc=received_at,
                    ingress_received_monotonic_ns=ingress_received_monotonic_ns,
                    normalized_ready_monotonic_ns=normalized_ready_ns,
                    receipt_id=receipt_id,
                )
            except (AttributeError, KeyError, TypeError, ValueError, ArithmeticError):
                symbol = str(payload.get("market_id", ""))
                try:
                    state = self._state_for_symbol(
                        Venue.RISEX,
                        self._risex_by_market_id[int(symbol)].risex_market.venue_symbol,
                    )
                except (KeyError, TypeError, ValueError):
                    self.fatal_reason = "RISEX_BOOK_SCHEMA_UNCLASSIFIED"
                    return
                await self._recover_market(
                    state,
                    ws,
                    reason="RISEX_BOOK_SCHEMA_INVALID",
                    receipt_id=receipt_id,
                )
            return

        if channel not in {"trades", "trade"} and not any(
            key in payload for key in ("maker_side", "price", "size", "quantity")
        ):
            return
        try:
            state = self._state_for_symbol(
                Venue.RISEX,
                self._risex_by_market_id[int(str(payload.get("market_id")))].risex_market.venue_symbol,
            )
            normalized_trade = self.risex.normalize_trade(
                payload,
                received_at=received_at,
                session_id=str(state.session_id),
                ordinal=state.book_revision,
            )
            normalized_ready_ns = (
                self._monotonic_ns()
                if normalized_ready_monotonic_ns is None
                else normalized_ready_monotonic_ns
            )
            if normalized_trade.aggressor_side is None or state.session_id is None:
                raise ValueError("RISEx trade aggressor is not proven")
            event = TradeEvidence(
                trade_event_key=normalized_trade.trade_event_key,
                venue=Venue.RISEX,
                canonical_market=state.market_pair.canonical_market,
                venue_symbol=state.market_pair.risex_market.venue_symbol,
                canonical_price=normalized_trade.canonical_price,
                canonical_quantity=normalized_trade.canonical_quantity,
                aggressor_side=normalized_trade.aggressor_side,
                received_utc=received_at,
                received_monotonic_ns=ingress_received_monotonic_ns,
                stream_session_id=state.session_id,
                recovery_generation=state.recovery_generation,
                exchange_event_utc=normalized_trade.exchange_timestamp,
                exchange_event_time_provenance=(
                    "RISEX_ADAPTER_PUBLIC_TIMESTAMP"
                    if normalized_trade.exchange_timestamp is not None
                    else None
                ),
                ingress_received_monotonic_ns=ingress_received_monotonic_ns,
                normalized_ready_monotonic_ns=normalized_ready_ns,
                source_trade_id=normalized_trade.source_trade_id,
                maker_order_id=normalized_trade.maker_order_id,
                taker_order_id=normalized_trade.taker_order_id,
                maker=normalized_trade.maker,
                taker=normalized_trade.taker,
                tx_hash=normalized_trade.tx_hash,
                block_number=normalized_trade.block_number,
                log_index=normalized_trade.log_index,
                worker_timestamp=normalized_trade.worker_timestamp,
            )
            self.ingress.offer(
                FeedTradeEvent(
                    event,
                    state.market_pair,
                    normalized_trade.raw_timestamp,
                    receipt_id=receipt_id,
                )
            )
        except (AttributeError, KeyError, TypeError, ValueError, ArithmeticError):
            try:
                state = self._state_for_symbol(
                    Venue.RISEX,
                    self._risex_by_market_id[int(str(payload.get("market_id")))].risex_market.venue_symbol,
                )
            except (KeyError, TypeError, ValueError):
                self.fatal_reason = "RISEX_TRADE_SCHEMA_UNCLASSIFIED"
                return
            self._gap(
                state,
                reason="RISEX_TRADE_SCHEMA_INVALID",
                receipt_id=receipt_id,
            )

    async def ingest_lighter_payload(
        self,
        payload: Mapping[str, Any],
        *,
        received_at: datetime | None = None,
        ingress_received_monotonic_ns: int | None = None,
        normalized_ready_monotonic_ns: int | None = None,
        ws: Any | None = None,
        receipt: FeedReceiptEvent | None = None,
    ) -> None:
        received_at = self._now_utc() if received_at is None else received_at
        if ingress_received_monotonic_ns is None:
            ingress_received_monotonic_ns = self._monotonic_ns()
        if self.capture_receipts and receipt is None:
            receipt = self._capture_receipt(
                Venue.LIGHTER,
                frame_kind="MAPPING",
                frame_data=None,
                payload=payload,
                received_at=received_at,
                received_monotonic_ns=ingress_received_monotonic_ns,
            )
        receipt_id = None if receipt is None else receipt.receipt_id
        message_type = str(payload.get("type", ""))
        if not message_type.endswith("order_book"):
            return
        initial = message_type == "subscribed/order_book"
        try:
            normalized = self.lighter.normalize_book_message(
                payload, received_at=received_at, initial=initial
            )
            normalized_ready_ns = (
                self._monotonic_ns()
                if normalized_ready_monotonic_ns is None
                else normalized_ready_monotonic_ns
            )
            state = self._state_for_symbol(Venue.LIGHTER, normalized.canonical_market)
            if state is None:
                return
            state.stream.connection_confirmed(received_at)
            if initial and isinstance(normalized, OrderBook):
                state.stream.snapshot(normalized, sequence=normalized.sequence)
                if state.stream.book() is None:
                    await self._recover_market(
                        state,
                        ws,
                        reason="LIGHTER_SNAPSHOT_INVALID",
                        receipt_id=receipt_id,
                    )
                    return
                state.awaiting_snapshot = False
                self._emit_book(
                    state,
                    source_kind="SNAPSHOT",
                    checksum=None,
                    checksum_validation="BOOKSTREAM_LIGHTER_FRESH_SUBSCRIPTION_SNAPSHOT",
                    normalized_source=normalized,
                    ingress_received_utc=received_at,
                    ingress_received_monotonic_ns=ingress_received_monotonic_ns,
                    normalized_ready_monotonic_ns=normalized_ready_ns,
                    receipt_id=receipt_id,
                )
            elif not initial and isinstance(normalized, BookDelta):
                if state.awaiting_snapshot or not state.stream.book_initialized:
                    return
                if not state.stream.apply_delta(normalized):
                    await self._recover_market(
                        state,
                        ws,
                        reason="LIGHTER_SEQUENCE_INVALID_FRESH_RESUBSCRIBE",
                        receipt_id=receipt_id,
                    )
                    return
                self._emit_book(
                    state,
                    source_kind="DELTA",
                    checksum=None,
                    checksum_validation="BOOKSTREAM_LIGHTER_NONCE_CHAIN",
                    normalized_source=normalized,
                    ingress_received_utc=received_at,
                    ingress_received_monotonic_ns=ingress_received_monotonic_ns,
                    normalized_ready_monotonic_ns=normalized_ready_ns,
                    receipt_id=receipt_id,
                )
        except (AttributeError, KeyError, TypeError, ValueError, ArithmeticError):
            market_id = payload.get("market_id")
            try:
                pair = self._lighter_by_market_id[int(market_id)]
                state = self.state(Venue.LIGHTER, pair.canonical_market)
            except (KeyError, TypeError, ValueError):
                self.fatal_reason = "LIGHTER_BOOK_SCHEMA_UNCLASSIFIED"
                return
            await self._recover_market(
                state,
                ws,
                reason="LIGHTER_BOOK_SCHEMA_INVALID",
                receipt_id=receipt_id,
            )

    async def _send_risex_heartbeat(self, ws: Any) -> None:
        action = self.risex.client_ping_action()
        await ws.send_json(action)

    async def _send_lighter_heartbeat(self, ws: Any) -> None:
        action = self.lighter.client_ping_action()
        payload = action.payload.decode("utf-8")
        await ws.send_str(payload)

    def _confirm_venue(self, venue: Venue) -> None:
        at = self._now_utc()
        for pair in self.market_pairs:
            state = self.state(venue, pair.canonical_market)
            state.stream.connection_confirmed(at)

    @staticmethod
    def _message_kind(message: Any) -> str:
        value = getattr(message, "type", None)
        if value is None:
            return "TEXT"
        enum_name = getattr(value, "name", None)
        if enum_name in {
            "BINARY",
            "CONTINUATION",
            "CLOSE",
            "CLOSED",
            "CLOSING",
            "ERROR",
            "PING",
            "PONG",
            "TEXT",
        }:
            return enum_name
        text = str(value)
        if text.endswith("TEXT") or text in {"1", "TEXT"}:
            return "TEXT"
        if text.endswith("PING") or text in {"9", "PING"}:
            return "PING"
        if text.endswith("PONG") or text in {"10", "PONG"}:
            return "PONG"
        if text.endswith("CLOSED") or text in {"257", "CLOSED"}:
            return "CLOSED"
        if text.endswith("CLOSING") or text in {"256", "CLOSING"}:
            return "CLOSING"
        if text.endswith("CLOSE") or text in {"8", "CLOSE"}:
            return "CLOSE"
        if text.endswith("ERROR") or text in {"ERROR"}:
            return "ERROR"
        return text.upper()

    @staticmethod
    def _normal_close_code(ws: Any, message: Any, kind: str) -> bool:
        """Accept only aiohttp's explicit protocol-normal close code."""

        if kind == "CLOSE":
            code = getattr(message, "data", None)
        elif kind == "CLOSED":
            # aiohttp emits CLOSED with no frame data after EOF, but records
            # the observed close code on the response itself.  No such
            # response-level code exists on an unknown/malformed fixture.
            code = getattr(ws, "close_code", None)
        else:
            # CLOSING means the close handshake is incomplete; it carries no
            # demonstrably normal peer close code.
            return False
        return (
            isinstance(code, int)
            and not isinstance(code, bool)
            and code == _NORMAL_CLOSE_CODE
        )

    def _unexpected_close(self, venue: Venue) -> None:
        """Persist sanitized unexpected-close evidence and fail closed."""

        self._record_transport_failure(venue, _UnexpectedPublicClose())

    async def _read_risex(self, ws: Any, stop: asyncio.Event) -> str | None:
        while not stop.is_set():
            try:
                message = await asyncio.wait_for(
                    ws.receive(), timeout=10
                )
            except asyncio.TimeoutError:
                await self._send_risex_heartbeat(ws)
                continue
            ingress_received_utc = self._now_utc()
            ingress_received_monotonic_ns = self._monotonic_ns()
            kind = self._message_kind(message)
            frame_data = getattr(message, "data", None)
            payload = self._payload(message) if kind == "TEXT" else None
            receipt = self._capture_receipt(
                Venue.RISEX,
                frame_kind=kind,
                frame_data=frame_data,
                payload=payload,
                received_at=ingress_received_utc,
                received_monotonic_ns=ingress_received_monotonic_ns,
            )
            if kind == "PING":
                action = self.risex.handle_server_ping(frame_data)
                await ws.pong(action.payload)
                if action.connection_confirmed:
                    self._confirm_venue(Venue.RISEX)
                continue
            elif kind in {"CLOSE", "CLOSED", "CLOSING"}:
                if self._normal_close_code(ws, message, kind):
                    return "GRACEFUL_CLOSE"
                self._unexpected_close(Venue.RISEX)
                return "UNEXPECTED_CLOSE"
            elif kind == "PONG":
                self._confirm_venue(Venue.RISEX)
            elif kind == "ERROR":
                raise ConnectionError("RISEx public websocket error")
            elif kind == "TEXT":
                if payload is not None:
                    if payload == {"type": "pong"}:
                        self._confirm_venue(Venue.RISEX)
                    await self.ingest_risex_payload(
                        payload,
                        received_at=ingress_received_utc,
                        ingress_received_monotonic_ns=ingress_received_monotonic_ns,
                        ws=ws,
                        receipt=receipt,
                    )
                else:
                    self._protocol_failure(
                        Venue.RISEX,
                        "RISEX_PUBLIC_MESSAGE_INVALID",
                        frame_kind=kind,
                        frame_category="INVALID_MESSAGE_PAYLOAD",
                        frame_data=getattr(message, "data", None),
                    )
                    return
            if self.fatal_reason is not None:
                return
            elif kind not in {"PONG", "TEXT"}:
                self._protocol_failure(
                    Venue.RISEX,
                    "RISEX_PUBLIC_FRAME_INVALID",
                    frame_kind=kind,
                    frame_category="UNSUPPORTED_FRAME_KIND",
                    frame_data=getattr(message, "data", None),
                )
                return

    async def _read_lighter(self, ws: Any, stop: asyncio.Event) -> str | None:
        while not stop.is_set():
            try:
                message = await asyncio.wait_for(ws.receive(), timeout=10)
            except asyncio.TimeoutError:
                await self._send_lighter_heartbeat(ws)
                continue
            ingress_received_utc = self._now_utc()
            ingress_received_monotonic_ns = self._monotonic_ns()
            kind = self._message_kind(message)
            frame_data = getattr(message, "data", None)
            payload = self._payload(message) if kind == "TEXT" else None
            receipt = self._capture_receipt(
                Venue.LIGHTER,
                frame_kind=kind,
                frame_data=frame_data,
                payload=payload,
                received_at=ingress_received_utc,
                received_monotonic_ns=ingress_received_monotonic_ns,
            )
            if kind == "PING":
                await ws.pong(frame_data)
                self._confirm_venue(Venue.LIGHTER)
                continue
            if kind in {"CLOSE", "CLOSED", "CLOSING"}:
                if self._normal_close_code(ws, message, kind):
                    return "GRACEFUL_CLOSE"
                self._unexpected_close(Venue.LIGHTER)
                return "UNEXPECTED_CLOSE"
            if kind == "PONG":
                self._confirm_venue(Venue.LIGHTER)
                continue
            if kind == "ERROR":
                raise ConnectionError("Lighter public websocket error")
            if kind == "TEXT":
                if payload is not None:
                    if payload == {"type": "pong"}:
                        self._confirm_venue(Venue.LIGHTER)
                    await self.ingest_lighter_payload(
                        payload,
                        received_at=ingress_received_utc,
                        ingress_received_monotonic_ns=ingress_received_monotonic_ns,
                        ws=ws,
                        receipt=receipt,
                    )
                else:
                    self._protocol_failure(
                        Venue.LIGHTER,
                        "LIGHTER_PUBLIC_MESSAGE_INVALID",
                        frame_kind=kind,
                        frame_category="INVALID_MESSAGE_PAYLOAD",
                        frame_data=getattr(message, "data", None),
                    )
                    return
            if self.fatal_reason is not None:
                return
            if kind != "TEXT":
                self._protocol_failure(
                    Venue.LIGHTER,
                    "LIGHTER_PUBLIC_FRAME_INVALID",
                    frame_kind=kind,
                    frame_category="UNSUPPORTED_FRAME_KIND",
                    frame_data=getattr(message, "data", None),
                )
                return

    async def _transport_loop(self, venue: Venue, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                termination: str | None = None
                if venue is Venue.RISEX:
                    async with self.session.ws_connect(
                        self.risex.ws_base, heartbeat=None, autoping=False
                    ) as ws:
                        self.begin_connection(Venue.RISEX)
                        ids = [
                            self.risex.market_id(pair.risex_market.venue_symbol)
                            for pair in self.market_pairs
                        ]
                        await ws.send_json(self.risex.orderbook_subscription(ids))
                        await ws.send_json(self.risex.trades_subscription(ids))
                        termination = await self._read_risex(ws, stop)
                else:
                    async with self.session.ws_connect(
                        self.lighter.ws_base, heartbeat=None, autoping=False
                    ) as ws:
                        self.begin_connection(Venue.LIGHTER)
                        for pair in self.market_pairs:
                            await ws.send_json(
                                self.lighter.subscription(
                                    "book", self.lighter.market_id(pair.lighter_market.venue_symbol)
                                )
                            )
                        termination = await self._read_lighter(ws, stop)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not stop.is_set():
                    self._record_transport_failure(venue, exc)
                    stop.set()
            else:
                if not stop.is_set():
                    if self.fatal_reason is not None:
                        stop.set()
                    elif termination == "GRACEFUL_CLOSE":
                        self.disconnect(
                            venue,
                            reason="PUBLIC_SOCKET_DISCONNECTED",
                            transport_evidence={"transport_event": "GRACEFUL_CLOSE"},
                        )
                        # Reconnect is intentionally a new session/recovery
                        # chain.  Preserve the bounded retry pause so a
                        # close/reconnect loop cannot monopolize the event
                        # loop or silently join books.
                        try:
                            await asyncio.wait_for(stop.wait(), timeout=1)
                        except asyncio.TimeoutError:
                            pass
                    else:
                        self.disconnect(venue, reason="PUBLIC_SOCKET_DISCONNECTED")

    async def run(
        self,
        *,
        duration_seconds: int | None = None,
        stop_event: asyncio.Event | None = None,
        drain_event: asyncio.Event | None = None,
        duration_limit_seconds: int | None = None,
    ) -> None:
        duration = self.config.duration_seconds if duration_seconds is None else duration_seconds
        duration_limit = (
            MAX_PUBLIC_DURATION_SECONDS
            if duration_limit_seconds is None
            else duration_limit_seconds
        )
        if (
            isinstance(duration, bool)
            or not isinstance(duration, int)
            or duration <= 0
            or isinstance(duration_limit, bool)
            or not isinstance(duration_limit, int)
            or duration_limit <= 0
            or duration > duration_limit
        ):
            raise ValueError(
                f"public smoke duration must be 1..{duration_limit} seconds"
            )
        # The prospective wall-clock gate is independent from the operator's
        # requested duration.  A shorter configured sample wall-clock limit
        # therefore always wins without allowing a longer feed timer to
        # bypass it.
        effective_duration = min(duration, self.config.sample_wall_clock_seconds)
        stop = asyncio.Event()
        risex_stop = asyncio.Event()
        tasks = [
            asyncio.create_task(self._transport_loop(Venue.RISEX, risex_stop)),
            asyncio.create_task(self._transport_loop(Venue.LIGHTER, stop)),
        ]
        self._external_stop_event = stop_event
        timer: asyncio.Task[None] | None = None
        requested_stop: asyncio.Task[bool] | None = None
        drain_timer: asyncio.Task[None] | None = None
        drain_waiter: asyncio.Task[bool] | None = None
        try:
            if stop_event is None:
                await asyncio.sleep(effective_duration)
            else:
                requested_stop = asyncio.create_task(stop_event.wait())
                if duration >= self.config.sample_wall_clock_seconds:
                    # At the configured wall boundary, the observer's
                    # explicit timer is authoritative.  Waiting for its
                    # latched event avoids a duration-timer race that could
                    # close the feed before the SAMPLE_STOP marker and drain
                    # tail are established.
                    await requested_stop
                    done = {requested_stop}
                else:
                    timer = asyncio.create_task(asyncio.sleep(effective_duration))
                    done, _ = await asyncio.wait(
                        (timer, requested_stop),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                sample_stop_requested = requested_stop in done and self.fatal_reason is None
                if sample_stop_requested:
                    # No further RISEx books or trades may extend the sample.
                    # Keep only Lighter ingress alive for the already-created
                    # horizon captures, bounded by the largest configured
                    # horizon plus normal scheduling allowance.
                    risex_stop.set()
                    tasks[0].cancel()
                    await asyncio.gather(tasks[0], return_exceptions=True)
                    drain_seconds = (
                        max(self.config.horizons_ms) / 1000
                        + _DRAIN_SCHEDULING_ALLOWANCE_SECONDS
                    )
                    if drain_event is None:
                        drain_timer = asyncio.create_task(asyncio.sleep(drain_seconds))
                        await drain_timer
                    elif not drain_event.is_set():
                        drain_timer = asyncio.create_task(asyncio.sleep(drain_seconds))
                        drain_waiter = asyncio.create_task(drain_event.wait())
                        await asyncio.wait(
                            (drain_timer, drain_waiter),
                            return_when=asyncio.FIRST_COMPLETED,
                        )
        finally:
            self._external_stop_event = None
            for waiter in (timer, requested_stop, drain_timer, drain_waiter):
                if waiter is not None and not waiter.done():
                    waiter.cancel()
            waiters = tuple(
                waiter
                for waiter in (timer, requested_stop, drain_timer, drain_waiter)
                if waiter is not None
            )
            if waiters:
                await asyncio.gather(*waiters, return_exceptions=True)
            stop.set()
            risex_stop.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.disconnect(Venue.RISEX, reason="PUBLIC_SMOKE_STOPPED")
            self.disconnect(Venue.LIGHTER, reason="PUBLIC_SMOKE_STOPPED")


async def select_public_market_pairs(
    risex: RisexAdapter,
    lighter: LighterAdapter,
    *,
    requested_markets: tuple[str, ...] = (),
    max_markets: int = 3,
) -> tuple[MarketPair, ...]:
    """Select at most three exact canonical intersections from public catalogs."""

    if isinstance(max_markets, bool) or not isinstance(max_markets, int) or not 1 <= max_markets <= 3:
        raise ValueError("max_markets must be 1..3")
    wanted = {item.strip().upper() for item in requested_markets if item.strip()}
    risex_markets = await risex.fetch_markets()
    lighter_markets = await lighter.fetch_markets()
    lighter_by_asset = {
        market.canonical_asset.upper(): market
        for market in lighter_markets
        if market.is_active and market.base_multiplier is not None
    }
    candidates = []
    for initial_risex in risex_markets:
        asset = initial_risex.canonical_asset.upper()
        if wanted and asset not in wanted:
            continue
        if not initial_risex.is_active:
            continue
        lighter_market = lighter_by_asset.get(asset)
        if lighter_market is None:
            continue
        candidates.append(
            MarketPair(asset, initial_risex, lighter_market)
        )
    candidates.sort(key=lambda pair: pair.canonical_market)
    selected: list[MarketPair] = []
    for pair in candidates[:max_markets]:
        try:
            # RISEx marks the linear-contract identity only after both public
            # book and public trade unit evidence have been observed.  These
            # are bounded selection proofs, not a discovery sample.
            await risex.fetch_book(pair.risex_market.venue_symbol)
            refreshed_risex = await risex.prime_recent_trade_evidence(
                pair.risex_market
            )
        except (AttributeError, KeyError, TypeError, ValueError, ArithmeticError):
            continue
        if refreshed_risex.base_multiplier is None or not refreshed_risex.is_active:
            continue
        selected.append(MarketPair(pair.canonical_market, refreshed_risex, pair.lighter_market))
    selected_tuple = tuple(selected)
    if not selected_tuple:
        raise RuntimeError("no exact active public RISEx/Lighter market intersection")
    return selected_tuple


__all__ = [
    "FeedBookEvent",
    "FeedGapEvent",
    "FeedReceiptEvent",
    "FeedTradeEvent",
    "IngressItem",
    "IngressQueue",
    "MarketPair",
    "PublicFeedRunner",
    "select_public_market_pairs",
]
