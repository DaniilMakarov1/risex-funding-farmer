"""Venue-neutral book recovery and stream-health coordination."""

from __future__ import annotations

import binascii
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Iterable

from .models import BookDelta, BookLevel, DataQuality, OrderBook, StreamHealth, Venue


X18 = Decimal("1000000000000000000")


class BookStream:
    """Maintains one public book and fails closed on any ordering ambiguity."""

    def __init__(self, venue: Venue, canonical_market: str) -> None:
        self.venue = venue
        self.canonical_market = canonical_market
        self._bids: dict[Decimal, Decimal] = {}
        self._asks: dict[Decimal, Decimal] = {}
        self._observed_at: datetime | None = None
        self._sequence: int | None = None
        self.last_market_event_at: datetime | None = None
        self.last_connection_confirmation_at: datetime | None = None
        self.stream_connected = False
        self.book_initialized = False
        self.book_sequence_valid = False

    def connected(self, at: datetime) -> None:
        self.stream_connected = True
        self.last_connection_confirmation_at = at

    def connection_confirmed(self, at: datetime) -> None:
        if self.stream_connected:
            self.last_connection_confirmation_at = at

    def disconnected(self) -> None:
        self.stream_connected = False
        self.book_initialized = False
        self.book_sequence_valid = False

    def gap(self) -> None:
        self.book_initialized = False
        self.book_sequence_valid = False

    def snapshot(self, book: OrderBook, *, sequence: int | None = None) -> None:
        if book.venue is not self.venue or book.canonical_market != self.canonical_market:
            raise ValueError("snapshot does not belong to this stream")
        self._bids = {level.canonical_price: level.canonical_quantity for level in book.bids}
        self._asks = {level.canonical_price: level.canonical_quantity for level in book.asks}
        self._observed_at = book.observed_at
        self.last_market_event_at = book.observed_at
        self._sequence = sequence if sequence is not None else book.sequence
        self.book_initialized = True
        self.book_sequence_valid = self._valid_bbo()

    def extended_delta(
        self,
        bids: Iterable[BookLevel],
        asks: Iterable[BookLevel],
        *,
        sequence: int,
        observed_at: datetime,
    ) -> bool:
        if not self.book_initialized or self._sequence is None or sequence != self._sequence + 1:
            self.gap()
            return False
        self._apply_absolute(bids, asks)
        self._sequence = sequence
        return self._finish_update(observed_at)

    def apply_delta(self, delta: BookDelta) -> bool:
        if delta.venue is not self.venue or delta.canonical_market != self.canonical_market:
            raise ValueError("delta does not belong to this stream")
        if self.venue is Venue.EXTENDED and delta.sequence is not None:
            return self.extended_delta(
                delta.bids,
                delta.asks,
                sequence=delta.sequence,
                observed_at=delta.observed_at,
            )
        if (
            self.venue is Venue.NADO
            and delta.sequence is not None
            and delta.previous_sequence is not None
        ):
            return self.nado_delta(
                delta.bids,
                delta.asks,
                last_max_timestamp=delta.previous_sequence,
                max_timestamp=delta.sequence,
                observed_at=delta.observed_at,
            )
        if self.venue is Venue.RISEX and delta.checksum is not None:
            return self.risex_update(
                delta.bids,
                delta.asks,
                checksum=delta.checksum,
                observed_at=delta.observed_at,
            )
        self.gap()
        return False

    def nado_delta(
        self,
        bids: Iterable[BookLevel],
        asks: Iterable[BookLevel],
        *,
        last_max_timestamp: int,
        max_timestamp: int,
        observed_at: datetime,
    ) -> bool:
        if (
            not self.book_initialized
            or self._sequence is None
            or last_max_timestamp != self._sequence
        ):
            self.gap()
            return False
        self._apply_absolute(bids, asks)
        self._sequence = max_timestamp
        return self._finish_update(observed_at)

    def risex_update(
        self,
        bids: Iterable[BookLevel],
        asks: Iterable[BookLevel],
        *,
        checksum: int,
        observed_at: datetime,
    ) -> bool:
        if not self.book_initialized:
            return False
        self._apply_absolute(bids, asks)
        if self.risex_checksum() != checksum:
            self.gap()
            return False
        return self._finish_update(observed_at)

    def _apply_absolute(
        self, bids: Iterable[BookLevel], asks: Iterable[BookLevel]
    ) -> None:
        for target, levels in ((self._bids, bids), (self._asks, asks)):
            for level in levels:
                if level.canonical_quantity == 0:
                    target.pop(level.canonical_price, None)
                elif level.canonical_quantity > 0:
                    target[level.canonical_price] = level.canonical_quantity
                else:
                    raise ValueError("absolute book quantity cannot be negative")

    def _finish_update(self, observed_at: datetime) -> bool:
        self._observed_at = observed_at
        self.last_market_event_at = observed_at
        self.book_sequence_valid = self._valid_bbo()
        if not self.book_sequence_valid:
            self.gap()
            return False
        return True

    def _valid_bbo(self) -> bool:
        return bool(self._bids and self._asks and max(self._bids) < min(self._asks))

    def risex_checksum(self) -> int:
        bids = sorted(self._bids.items(), reverse=True)
        asks = sorted(self._asks.items())
        parts: list[str] = []
        for index in range(max(len(bids), len(asks))):
            if index < len(bids):
                parts.extend(self._wei_strings(bids[index]))
            if index < len(asks):
                parts.extend(self._wei_strings(asks[index]))
        return binascii.crc32(":".join(parts).encode()) & 0xFFFFFFFF

    @staticmethod
    def _wei_strings(level: tuple[Decimal, Decimal]) -> tuple[str, str]:
        return str(int(level[0] * X18)), str(int(level[1] * X18))

    def health(self, now: datetime, *, max_silence_seconds: int = 25) -> StreamHealth:
        confirmation_fresh = (
            self.last_connection_confirmation_at is not None
            and now - self.last_connection_confirmation_at
            <= timedelta(seconds=max_silence_seconds)
        )
        usable = (
            self.stream_connected
            and confirmation_fresh
            and self.book_initialized
            and self.book_sequence_valid
            and self._valid_bbo()
        )
        return StreamHealth(
            self.last_market_event_at,
            self.last_connection_confirmation_at,
            self.stream_connected,
            self.book_initialized,
            self.book_sequence_valid,
            DataQuality.COMPLETE if usable else DataQuality.DEGRADED,
        )

    def book(self) -> OrderBook | None:
        if not self.book_initialized or not self.book_sequence_valid or self._observed_at is None:
            return None
        bids = tuple(BookLevel(price, quantity) for price, quantity in sorted(self._bids.items(), reverse=True))
        asks = tuple(BookLevel(price, quantity) for price, quantity in sorted(self._asks.items()))
        return OrderBook(
            self.venue,
            self.canonical_market,
            bids,
            asks,
            self._observed_at,
            self._sequence,
        )


class MarketDataCoordinator:
    def __init__(self) -> None:
        self._streams: dict[tuple[Venue, str], BookStream] = {}

    def stream(self, venue: Venue, canonical_market: str) -> BookStream:
        key = (venue, canonical_market)
        if key not in self._streams:
            self._streams[key] = BookStream(venue, canonical_market)
        return self._streams[key]


def funding_is_fresh(
    observed_at: datetime, now: datetime, *, max_age_seconds: int = 120
) -> bool:
    return now >= observed_at and now - observed_at <= timedelta(seconds=max_age_seconds)
