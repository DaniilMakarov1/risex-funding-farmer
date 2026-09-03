"""The single SS-001B observer, evidence path, and public-smoke orchestration."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
import platform
import time
from typing import Callable, Any

import aiohttp

from risex_farmer.exchanges.lighter import LighterAdapter
from risex_farmer.exchanges.risex import RisexAdapter
from risex_farmer.models import BookLevel, Venue

from .config import ShadowConfig
from .economics import build_hypothetical_maker_quote, exact_entry_edge_usd
from .evidence import capture_horizon, detect_strict_would_fill
from .feed import (
    FeedBookEvent,
    FeedGapEvent,
    FeedTradeEvent,
    IngressItem,
    IngressQueue,
    MarketPair,
    PublicFeedRunner,
    select_public_market_pairs,
)
from .models import (
    BookEvidence,
    DataGapEvidence,
    EntryViabilityOutcome,
    HedgeHorizonCapture,
    HypotheticalMakerQuote,
    QuotePolicy,
    QuoteVersion,
    SpreadDirection,
    TradeEvidence,
    WouldFillEvidence,
)
from .store import AppendOnlyEvidenceStore


def _level_record(level: BookLevel) -> dict[str, str]:
    return {
        "price": str(level.canonical_price),
        "quantity": str(level.canonical_quantity),
    }


class HistoryCapacityExceeded(RuntimeError):
    """Raised when configured history capacity would discard active evidence."""

    def __init__(self, gap: DataGapEvidence) -> None:
        super().__init__("book history capacity reached; evidence gap is required")
        self.gap = gap


class BookHistory:
    """Time-retained book evidence with no silent fixed-count eviction."""

    def __init__(self, *, retention_ns: int, capacity: int | None = None) -> None:
        self.retention_ns = retention_ns
        self.capacity = capacity
        self._books: dict[tuple[Venue, str], list[BookEvidence]] = {}
        self._gaps: list[DataGapEvidence] = []
        self._pending: dict[str, tuple[int, int]] = {}
        self._pending_identity: dict[
            str, tuple[Venue, str, str | int, int] | None
        ] = {}

    @property
    def book_count(self) -> int:
        return sum(len(items) for items in self._books.values())

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    def _prune(self, now_ns: int) -> None:
        cutoff = max(0, now_ns - self.retention_ns)
        pending = tuple(self._pending.items())
        if pending:
            def gap_needed(gap: DataGapEvidence) -> bool:
                for version_id, (detected, deadline) in pending:
                    expected = self._pending_identity.get(version_id)
                    if expected is not None and not gap.matches(*expected):
                        continue
                    if gap.gap_start_monotonic_ns > deadline:
                        continue
                    if (
                        gap.gap_end_monotonic_ns is None
                        or gap.gap_end_monotonic_ns >= detected
                    ):
                        return True
                return (
                    gap.gap_end_monotonic_ns is None
                    and gap.gap_start_monotonic_ns >= cutoff
                    or gap.gap_end_monotonic_ns is not None
                    and gap.gap_end_monotonic_ns >= cutoff
                )

            self._gaps = [gap for gap in self._gaps if gap_needed(gap)]
            # A pending episode may still need a pre-detection book to prove
            # a stale/no-depth outcome at a later deadline.  Keep all books
            # until every registered horizon has been captured.
            return
        self._gaps = [
            gap
            for gap in self._gaps
            if (
                gap.gap_end_monotonic_ns is None
                and gap.gap_start_monotonic_ns >= cutoff
                or gap.gap_end_monotonic_ns is not None
                and gap.gap_end_monotonic_ns >= cutoff
            )
        ]
        for identity, books in tuple(self._books.items()):
            retained = [book for book in books if book.received_monotonic_ns >= cutoff]
            if retained:
                self._books[identity] = retained
            else:
                del self._books[identity]

    def register_pending(
        self,
        version_id: str,
        detected_ns: int,
        deadline_ns: int,
        *,
        identity: tuple[Venue, str, str | int, int] | None = None,
    ) -> None:
        self._pending[version_id] = (detected_ns, deadline_ns)
        self._pending_identity[version_id] = identity

    def complete(self, version_id: str, now_ns: int) -> None:
        self._pending.pop(version_id, None)
        self._pending_identity.pop(version_id, None)
        self._prune(now_ns)

    def add_book(self, book: BookEvidence) -> None:
        self._prune(book.received_monotonic_ns)
        if self.capacity is not None and self.book_count >= self.capacity:
            raise HistoryCapacityExceeded(
                DataGapEvidence(
                    source_venue=book.venue,
                    canonical_market=book.canonical_market,
                    stream_session_id=book.stream_session_id,
                    recovery_generation=book.recovery_generation,
                    gap_start_monotonic_ns=book.received_monotonic_ns,
                    reason="BOOK_HISTORY_CAPACITY",
                )
            )
        identity = (book.venue, book.canonical_market)
        self._books.setdefault(identity, []).append(book)

    def add_gap(self, gap: DataGapEvidence) -> None:
        if gap not in self._gaps:
            self._gaps.append(gap)
        self._prune(gap.gap_start_monotonic_ns)

    def books(self, venue: Venue, market: str, deadline_ns: int | None = None) -> tuple[BookEvidence, ...]:
        values = tuple(self._books.get((venue, market), ()))
        if deadline_ns is not None:
            values = tuple(book for book in values if book.received_monotonic_ns <= deadline_ns)
        return tuple(
            sorted(
                values,
                key=lambda book: (
                    book.received_monotonic_ns,
                    book.book_revision,
                    -1 if book.sequence is None else book.sequence,
                    repr(book),
                ),
            )
        )

    def gaps(self) -> tuple[DataGapEvidence, ...]:
        return tuple(self._gaps)


@dataclass(slots=True)
class _PendingEpisode:
    version: QuoteVersion
    would_fill: WouldFillEvidence
    captures: dict[int, HedgeHorizonCapture] = field(default_factory=dict)


class SpreadObserver:
    """Consumes accepted feed events and persists one deterministic evidence path."""

    def __init__(
        self,
        config: ShadowConfig,
        market_pairs: Sequence[MarketPair],
        store: AppendOnlyEvidenceStore,
        *,
        now_utc: Callable[[], datetime] | None = None,
        monotonic_ns: Callable[[], int] | None = None,
    ) -> None:
        self.config = config
        self.market_pairs = tuple(market_pairs)
        self.store = store
        self.ingress = IngressQueue(config.ingress_queue_capacity)
        self._now_utc = now_utc or (lambda: datetime.now(UTC))
        self._monotonic_ns = monotonic_ns or time.monotonic_ns
        self.history = BookHistory(
            retention_ns=config.book_history_retention_ns,
            capacity=config.book_history_capacity,
        )
        self._current_books: dict[tuple[Venue, str], BookEvidence] = {}
        self._gapped_identities: dict[
            tuple[Venue, str], tuple[str | int, int]
        ] = {}
        self._current_stream_identities: dict[
            tuple[Venue, str], tuple[str | int, int]
        ] = {}
        self._last_book_ranks: dict[tuple[Venue, str], tuple[int, int, int]] = {}
        self._awaiting_fresh_snapshot: set[tuple[Venue, str]] = set()
        self._active_versions: dict[str, QuoteVersion] = {}
        self._trades: dict[str, dict[str, TradeEvidence]] = {}
        self._pending: dict[str, _PendingEpisode] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._append_lock = asyncio.Lock()
        self._version_serial = 0
        self.fatal_reason: str | None = None
        self._closing = False
        self._replay_mode = False

    @property
    def active_version_count(self) -> int:
        return len(self._active_versions)

    @property
    def pending_episode_count(self) -> int:
        return len(self._pending)

    @staticmethod
    def policy_id(policy: QuotePolicy) -> str:
        return "|".join(
            (
                policy.canonical_market,
                policy.direction.value,
                str(policy.target_notional_usd),
                str(policy.target_margin_bps),
            )
        )

    async def _append(self, records: Sequence[Mapping[str, Any]]) -> None:
        if not records:
            return
        try:
            # Keep synchronous file synchronization off the websocket/event
            # loop.  The lock preserves append order when horizon tasks wake
            # together at an absolute deadline.
            async with self._append_lock:
                await asyncio.to_thread(self.store.append_batch, records)
        except Exception:
            self.fatal_reason = "EVIDENCE_STORE_WRITE_FAILED"
            raise

    def _book_record(self, event: FeedBookEvent) -> dict[str, Any]:
        book = event.book
        return {
            "kind": "BOOK",
            "canonical_market": book.canonical_market,
            "venue": book.venue.value,
            "source_kind": event.source_kind,
            "checksum_validation": event.checksum_validation,
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
            "bids": tuple(_level_record(level) for level in book.bids),
            "asks": tuple(_level_record(level) for level in book.asks),
            "observed_monotonic_ns": book.received_monotonic_ns,
        }

    def _gap_record(self, gap: DataGapEvidence) -> dict[str, Any]:
        return {
            "kind": "DATA_GAP",
            "canonical_market": gap.canonical_market,
            "venue": gap.source_venue.value,
            "stream_session_id": gap.stream_session_id,
            "recovery_generation": gap.recovery_generation,
            "gap_start_monotonic_ns": gap.gap_start_monotonic_ns,
            "gap_end_monotonic_ns": gap.gap_end_monotonic_ns,
            "reason": gap.reason,
            "observed_monotonic_ns": gap.gap_start_monotonic_ns,
        }

    def _book_admissible(self, event: FeedBookEvent) -> bool:
        book = event.book
        key = (book.venue, book.canonical_market)
        identity = (book.stream_session_id, book.recovery_generation)
        blocked = self._gapped_identities.get(key)
        current = self._current_stream_identities.get(key)
        if blocked == identity or current == identity and key in self._awaiting_fresh_snapshot:
            return False
        if key in self._awaiting_fresh_snapshot:
            return event.source_kind == "SNAPSHOT" and identity != current
        if current is None:
            return event.source_kind == "SNAPSHOT"
        return identity == current

    def _trade_admissible(self, event: FeedTradeEvent) -> bool:
        trade = event.trade
        key = (trade.venue, trade.canonical_market)
        identity = (trade.stream_session_id, trade.recovery_generation)
        return (
            key not in self._awaiting_fresh_snapshot
            and identity == self._current_stream_identities.get(key)
            and identity != self._gapped_identities.get(key)
        )

    def _sizing_record(self, quote: HypotheticalMakerQuote) -> dict[str, Any] | None:
        sizing = quote.sizing_evidence
        if sizing is None:
            return None
        return {
            "target_notional_usd": sizing.target_notional_usd,
            "reference_price": sizing.reference_price,
            "risex_validation_price": sizing.risex_validation_price,
            "q_raw": sizing.q_raw,
            "common_quantity_step": sizing.common_quantity_step,
            "floored_quantity": sizing.floored_quantity,
            "risex_raw_quantity": sizing.risex_raw_quantity,
            "lighter_raw_quantity": sizing.lighter_raw_quantity,
            "risex_quantity_step_raw": sizing.risex_quantity_step_raw,
            "lighter_quantity_step_raw": sizing.lighter_quantity_step_raw,
            "risex_base_multiplier": sizing.risex_base_multiplier,
            "lighter_base_multiplier": sizing.lighter_base_multiplier,
            "risex_minimum_quantity_raw": sizing.risex_minimum_quantity_raw,
            "lighter_minimum_quantity_raw": sizing.lighter_minimum_quantity_raw,
            "risex_minimum_notional_usd": sizing.risex_minimum_notional_usd,
            "lighter_minimum_notional_usd": sizing.lighter_minimum_notional_usd,
            "risex_min_quantity_ok": sizing.risex_min_quantity_ok,
            "risex_min_notional_ok": sizing.risex_min_notional_ok,
            "lighter_min_quantity_ok": sizing.lighter_min_quantity_ok,
            "lighter_min_notional_ok": sizing.lighter_min_notional_ok,
        }

    def _quote_record(
        self,
        policy: QuotePolicy,
        quote: HypotheticalMakerQuote,
        *,
        version: QuoteVersion | None,
        created_ns: int,
    ) -> dict[str, Any]:
        return {
            "kind": "QUOTE",
            "policy_id": self.policy_id(policy),
            "canonical_market": policy.canonical_market,
            "direction": policy.direction.value,
            "target_notional_usd": policy.target_notional_usd,
            "target_margin_bps": policy.target_margin_bps,
            "risex_maker_fee_rate": policy.risex_maker_fee_rate,
            "lighter_taker_fee_rate": policy.lighter_taker_fee_rate,
            "risex_fee_source": policy.risex_fee_source,
            "lighter_fee_source": policy.lighter_fee_source,
            "outcome": quote.outcome.value,
            "quote_version_id": None if version is None else version.version_id,
            "quote_created_utc": None if version is None else version.quote_created_utc,
            "quote_created_monotonic_ns": created_ns,
            "quote_expires_monotonic_ns": None if version is None else version.quote_expires_monotonic_ns,
            "quote_lifetime_ns": self.config.quote_lifetime_ns,
            "maker_side": quote.maker_side.value,
            "hedge_side": quote.lighter_side.value,
            "canonical_quantity": quote.canonical_quantity,
            "maker_price": quote.maker_price,
            "lighter_vwap_price": quote.lighter_vwap_price,
            "lighter_filled_quantity": quote.lighter_filled_quantity,
            "lighter_notional_usd": quote.lighter_notional_usd,
            "maker_notional_usd": quote.maker_notional_usd,
            "total_entry_fees_usd": quote.total_entry_fees_usd,
            "target_edge_usd": quote.target_edge_usd,
            "actual_edge_usd": quote.actual_edge_usd,
            "raw_risex_price_bound": quote.raw_risex_price_bound,
            "post_only_bound_price": quote.post_only_bound_price,
            "risex_tick_size": quote.risex_tick_size,
            "fee_components": tuple(
                {
                    "venue": fee.venue.value,
                    "liquidity_role": fee.liquidity_role.value,
                    "fill_notional_usd": fee.fill_notional_usd,
                    "fee_base_notional_usd": fee.fee_base_notional_usd,
                    "rate": fee.rate,
                    "amount_usd": fee.amount_usd,
                    "source": fee.source,
                }
                for fee in quote.fee_components
            ),
            "sizing": self._sizing_record(quote),
            "observed_monotonic_ns": created_ns,
        }

    @staticmethod
    def _policy(
        config: ShadowConfig,
        pair: MarketPair,
        direction: SpreadDirection,
        notional: Decimal,
        margin: Decimal,
        risex_book: BookEvidence,
    ) -> QuotePolicy:
        if not risex_book.bids or not risex_book.asks:
            raise ValueError("RISEx BBO is unavailable")
        return QuotePolicy(
            canonical_market=pair.canonical_market,
            direction=direction,
            target_notional_usd=notional,
            target_margin_bps=margin,
            risex_maker_fee_rate=config.risex_maker_fee_rate,
            lighter_taker_fee_rate=config.lighter_taker_fee_rate,
            risex_fee_source=config.risex_fee_source,
            lighter_fee_source=config.lighter_fee_source,
            risex_market=pair.risex_market,
            lighter_market=pair.lighter_market,
            risex_best_bid=risex_book.bids[0].canonical_price,
            risex_best_ask=risex_book.asks[0].canonical_price,
            risex_tick_size=pair.risex_market.tick_size_raw,
            fee_observed_or_configured_at=risex_book.received_utc,
        )

    def _versions_for_book(
        self,
        event: FeedBookEvent,
    ) -> tuple[list[dict[str, Any]], dict[str, QuoteVersion]]:
        market = event.market_pair.canonical_market
        risex = self._current_books.get((Venue.RISEX, market))
        lighter = self._current_books.get((Venue.LIGHTER, market))
        if risex is None or lighter is None or not risex.is_sequence_healthy or not lighter.is_sequence_healthy:
            return [], {}
        if not risex.fresh or not lighter.fresh:
            return [], {}
        created_ns = max(risex.received_monotonic_ns, lighter.received_monotonic_ns)
        if (
            created_ns - min(risex.received_monotonic_ns, lighter.received_monotonic_ns)
            > self.config.freshness_max_age_ns
        ):
            return [], {}
        created_utc = max(
            (risex.received_utc or self._now_utc()),
            (lighter.received_utc or self._now_utc()),
        )
        records: list[dict[str, Any]] = []
        versions: dict[str, QuoteVersion] = {}
        for direction in (
            SpreadDirection.RISEX_BUY_LIGHTER_SELL,
            SpreadDirection.RISEX_SELL_LIGHTER_BUY,
        ):
            for notional in self.config.target_notionals_usd:
                for margin in self.config.target_margins_bps:
                    policy = self._policy(
                        self.config,
                        event.market_pair,
                        direction,
                        notional,
                        margin,
                        risex,
                    )
                    quote = build_hypothetical_maker_quote(
                        policy,
                        lighter,
                        risex_market=event.market_pair.risex_market,
                        lighter_market=event.market_pair.lighter_market,
                        risex_best_bid=policy.risex_best_bid,
                        risex_best_ask=policy.risex_best_ask,
                        risex_tick_size=policy.risex_tick_size,
                    )
                    policy_key = self.policy_id(policy)
                    version: QuoteVersion | None = None
                    if quote.outcome is EntryViabilityOutcome.QUOTE_ACTIVE:
                        self._version_serial += 1
                        version = QuoteVersion(
                            version_id=f"{self.store.run_id}:q{self._version_serial}:{policy_key}",
                            quote=quote,
                            quote_created_utc=created_utc,
                            quote_created_monotonic_ns=created_ns,
                            stream_session_id=risex.stream_session_id,
                            recovery_generation=risex.recovery_generation,
                            quote_expires_monotonic_ns=created_ns + self.config.quote_lifetime_ns,
                            hedge_stream_session_id=lighter.stream_session_id,
                            hedge_recovery_generation=lighter.recovery_generation,
                        )
                        versions[policy_key] = version
                    records.append(
                        self._quote_record(
                            policy,
                            quote,
                            version=version,
                            created_ns=created_ns,
                        )
                    )
        return records, versions

    async def handle_book(self, event: FeedBookEvent) -> None:
        book = event.book
        if not self._book_admissible(event):
            return
        stream_key = (book.venue, book.canonical_market)
        identity = (book.stream_session_id, book.recovery_generation)
        if self._current_stream_identities.get(stream_key) == identity:
            rank = (
                book.received_monotonic_ns,
                book.book_revision,
                -1 if book.sequence is None else book.sequence,
            )
            previous_rank = self._last_book_ranks.get(stream_key)
            if previous_rank is not None and rank <= previous_rank:
                return
            self._last_book_ranks[stream_key] = rank
        else:
            self._last_book_ranks[stream_key] = (
                book.received_monotonic_ns,
                book.book_revision,
                -1 if book.sequence is None else book.sequence,
            )
        if stream_key in self._awaiting_fresh_snapshot:
            self._awaiting_fresh_snapshot.remove(stream_key)
            self._gapped_identities.pop(stream_key, None)
        self._current_stream_identities[stream_key] = (
            book.stream_session_id,
            book.recovery_generation,
        )
        try:
            self.history.add_book(book)
        except HistoryCapacityExceeded as exc:
            self.fatal_reason = "BOOK_HISTORY_CAPACITY"
            await self.handle_gap(FeedGapEvent(exc.gap))
            return
        self._current_books[(book.venue, book.canonical_market)] = book
        records, versions = self._versions_for_book(event)
        for policy_key in set(self._active_versions) - set(versions):
            if policy_key.startswith(f"{book.canonical_market}|"):
                self._active_versions.pop(policy_key, None)
        self._active_versions.update(versions)
        await self._append((self._book_record(event), *records))
        self._prune_state(book.received_monotonic_ns)

    async def handle_gap(self, event: FeedGapEvent) -> None:
        gap = event.gap
        stream_key = (gap.source_venue, gap.canonical_market)
        self._gapped_identities[stream_key] = (
            gap.stream_session_id,
            gap.recovery_generation,
        )
        self._awaiting_fresh_snapshot.add(stream_key)
        self.history.add_gap(gap)
        self._current_books.pop((gap.source_venue, gap.canonical_market), None)
        for policy_key in tuple(self._active_versions):
            if policy_key.startswith(f"{gap.canonical_market}|"):
                self._active_versions.pop(policy_key, None)
        await self._append((self._gap_record(gap),))

    def _trade_record(self, event: FeedTradeEvent) -> dict[str, Any]:
        trade = event.trade
        return {
            "kind": "RISEX_TRADE",
            "canonical_market": trade.canonical_market,
            "venue": trade.venue.value,
            "trade_event_key": trade.trade_event_key,
            "canonical_price": trade.canonical_price,
            "canonical_quantity": trade.canonical_quantity,
            "aggressor_side": trade.aggressor_side.value,
            "exchange_event_utc": trade.exchange_event_utc,
            "exchange_event_time_provenance": trade.exchange_event_time_provenance,
            "received_utc": trade.received_utc,
            "received_monotonic_ns": trade.received_monotonic_ns,
            "stream_session_id": trade.stream_session_id,
            "recovery_generation": trade.recovery_generation,
            "raw_timestamp": event.raw_timestamp,
            "observed_monotonic_ns": trade.received_monotonic_ns,
        }

    def _would_fill_record(self, evidence: WouldFillEvidence) -> dict[str, Any]:
        return {
            "kind": "WOULD_FILL",
            "canonical_market": evidence.canonical_market,
            "venue": evidence.venue.value,
            "quote_version_id": evidence.quote_version_id,
            "direction": evidence.direction.value,
            "canonical_quantity": evidence.canonical_quantity,
            "cumulative_eligible_quantity": evidence.cumulative_eligible_quantity,
            "qualifying_trade_event_keys": evidence.qualifying_trade_event_keys,
            "would_fill_detected_monotonic_ns": evidence.would_fill_detected_monotonic_ns,
            "detected_utc": evidence.detected_utc,
            "hedge_stream_session_id": evidence.hedge_stream_session_id,
            "hedge_recovery_generation": evidence.hedge_recovery_generation,
            "observed_monotonic_ns": evidence.would_fill_detected_monotonic_ns,
        }

    def _hedge_edge(self, version: QuoteVersion, capture: HedgeHorizonCapture) -> Decimal | None:
        if capture.outcome is not EntryViabilityOutcome.HEDGE_FULL:
            return None
        quote = version.quote
        if quote.canonical_quantity is None or quote.maker_price is None:
            return None
        maker_notional = quote.canonical_quantity * quote.maker_price
        maker_fee = maker_notional * quote.policy.risex_maker_fee_rate
        lighter_fee = capture.notional_usd * quote.policy.lighter_taker_fee_rate
        return exact_entry_edge_usd(
            quote.policy.direction,
            quote.canonical_quantity,
            quote.maker_price,
            capture.notional_usd,
            maker_fee + lighter_fee,
        )

    def _horizon_record(
        self,
        version: QuoteVersion,
        capture: HedgeHorizonCapture,
    ) -> dict[str, Any]:
        edge = self._hedge_edge(version, capture)
        initial = version.quote.actual_edge_usd
        markout = None if edge is None or initial is None else edge - initial
        book = capture.book
        gap = capture.gap_evidence
        return {
            "kind": "HEDGE_HORIZON",
            "canonical_market": capture.canonical_market,
            "venue": Venue.LIGHTER.value,
            "policy_id": self.policy_id(version.quote.policy),
            "quote_version_id": version.version_id,
            "direction": version.direction.value,
            "target_notional_usd": version.quote.policy.target_notional_usd,
            "target_margin_bps": version.quote.policy.target_margin_bps,
            "horizon_ms": capture.horizon_ms,
            "would_fill_detected_monotonic_ns": capture.would_fill_detected_monotonic_ns,
            "horizon_deadline_monotonic_ns": capture.horizon_deadline_monotonic_ns,
            "expected_stream_session_id": capture.expected_stream_session_id,
            "expected_recovery_generation": capture.expected_recovery_generation,
            "outcome": capture.outcome.value,
            "requested_quantity": capture.requested_quantity,
            "filled_quantity": capture.filled_quantity,
            "notional_usd": capture.notional_usd,
            "vwap_price": capture.vwap_price,
            "entry_edge_usd": edge,
            "conditional_markout_usd": markout,
            "freshness_max_age_ns": capture.freshness_max_age_ns,
            "book_received_monotonic_ns": capture.book_received_monotonic_ns,
            "book_stream_session_id": capture.book_stream_session_id,
            "book_recovery_generation": capture.book_recovery_generation,
            "book_revision": capture.book_revision,
            "sequence": capture.sequence,
            "checksum": capture.checksum,
            "gap_source_venue": None if gap is None else gap.source_venue.value,
            "gap_reason": None if gap is None else gap.reason,
            "gap_start_monotonic_ns": None if gap is None else gap.gap_start_monotonic_ns,
            "gap_end_monotonic_ns": None if gap is None else gap.gap_end_monotonic_ns,
            "ambiguous_book_count": len(capture.ambiguous_books),
            "observed_monotonic_ns": capture.horizon_deadline_monotonic_ns,
        }

    async def handle_trade(self, event: FeedTradeEvent) -> None:
        trade = event.trade
        if not self._trade_admissible(event):
            return
        market_trades = self._trades.setdefault(trade.canonical_market, {})
        existing = market_trades.get(trade.trade_event_key)
        if existing is not None:
            if existing == trade:
                return
            gap = DataGapEvidence(
                source_venue=Venue.RISEX,
                canonical_market=trade.canonical_market,
                stream_session_id=trade.stream_session_id,
                recovery_generation=trade.recovery_generation,
                gap_start_monotonic_ns=trade.received_monotonic_ns,
                reason="TRADE_DEDUP_CONFLICT",
            )
            await self.handle_gap(FeedGapEvent(gap))
            return
        market_trades[trade.trade_event_key] = trade
        records: list[Mapping[str, Any]] = [self._trade_record(event)]
        for policy_key, version in tuple(self._active_versions.items()):
            if version.canonical_market != trade.canonical_market:
                continue
            if version.version_id in self._pending:
                continue
            evidence = detect_strict_would_fill(
                version,
                tuple(market_trades.values()),
                data_gaps=self.history.gaps(),
                would_fill_detected_monotonic_ns=trade.received_monotonic_ns,
                detected_utc=trade.received_utc,
            )
            if evidence is None:
                continue
            pending = _PendingEpisode(version, evidence)
            self._pending[version.version_id] = pending
            latest_deadline = max(
                evidence.would_fill_detected_monotonic_ns
                + horizon * 1_000_000
                for horizon in self.config.horizons_ms
            )
            self.history.register_pending(
                version.version_id,
                evidence.would_fill_detected_monotonic_ns,
                latest_deadline,
                identity=(
                    Venue.LIGHTER,
                    evidence.canonical_market,
                    evidence.hedge_stream_session_id
                    or version.hedge_stream_session_id
                    or "unknown",
                    evidence.hedge_recovery_generation
                    if evidence.hedge_recovery_generation is not None
                    else version.hedge_recovery_generation or 0,
                ),
            )
            records.append(self._would_fill_record(evidence))
            if not self._replay_mode:
                for horizon in self.config.horizons_ms:
                    task = asyncio.create_task(
                        self._capture_later(version.version_id, horizon)
                    )
                    self._tasks.add(task)
                    task.add_done_callback(self._capture_task_done)
        await self._append(records)
        self._prune_state(trade.received_monotonic_ns)

    async def handle_item(self, item: IngressItem) -> None:
        if isinstance(item, FeedBookEvent):
            await self.handle_book(item)
        elif isinstance(item, FeedTradeEvent):
            await self.handle_trade(item)
        else:
            await self.handle_gap(item)

    async def consume(self) -> None:
        while True:
            item = await self.ingress.next_item()
            if item is None:
                return
            await self.handle_item(item)

    async def _capture_one(self, version_id: str, horizon: int, *, force: bool) -> None:
        pending = self._pending.get(version_id)
        if pending is None or horizon in pending.captures:
            return
        evidence = pending.would_fill
        deadline = evidence.would_fill_detected_monotonic_ns + horizon * 1_000_000
        if not force and self._closing and self._monotonic_ns() < deadline:
            stop_gap = DataGapEvidence(
                source_venue=Venue.LIGHTER,
                canonical_market=evidence.canonical_market,
                stream_session_id=(
                    evidence.hedge_stream_session_id
                    or pending.version.hedge_stream_session_id
                    or "shutdown"
                ),
                recovery_generation=(
                    evidence.hedge_recovery_generation
                    if evidence.hedge_recovery_generation is not None
                    else pending.version.hedge_recovery_generation or 0
                ),
                gap_start_monotonic_ns=self._monotonic_ns(),
                reason="RUN_STOPPED_BEFORE_HORIZON",
            )
            gaps = (*self.history.gaps(), stop_gap)
        else:
            gaps = self.history.gaps()
        try:
            capture = capture_horizon(
                evidence,
                self.history.books(Venue.LIGHTER, evidence.canonical_market, deadline),
                horizon_ms=horizon,
                expected_stream_session_id=evidence.hedge_stream_session_id
                or pending.version.hedge_stream_session_id,
                expected_recovery_generation=evidence.hedge_recovery_generation
                if evidence.hedge_recovery_generation is not None
                else pending.version.hedge_recovery_generation,
                data_gaps=gaps,
                freshness_max_age_ns=self.config.freshness_max_age_ns,
            )
        except (TypeError, ValueError, ArithmeticError):
            capture = HedgeHorizonCapture(
                horizon_ms=horizon,
                would_fill_detected_monotonic_ns=evidence.would_fill_detected_monotonic_ns,
                horizon_deadline_monotonic_ns=deadline,
                expected_stream_session_id=evidence.hedge_stream_session_id
                or pending.version.hedge_stream_session_id
                or "unknown",
                expected_recovery_generation=evidence.hedge_recovery_generation
                if evidence.hedge_recovery_generation is not None
                else pending.version.hedge_recovery_generation or 0,
                canonical_market=evidence.canonical_market,
                requested_quantity=evidence.canonical_quantity,
                outcome=EntryViabilityOutcome.HEDGE_DATA_MISSING,
                book=None,
                book_received_monotonic_ns=None,
                book_stream_session_id=None,
                book_recovery_generation=None,
                book_revision=None,
                sequence=None,
                checksum=None,
                filled_quantity=Decimal("0"),
                notional_usd=Decimal("0"),
                vwap_price=None,
            )
        pending.captures[horizon] = capture
        await self._append((self._horizon_record(pending.version, capture),))
        if len(pending.captures) == len(self.config.horizons_ms):
            self._pending.pop(version_id, None)
            self.history.complete(version_id, self._monotonic_ns())
            self._prune_state(self._monotonic_ns())

    async def _capture_later(self, version_id: str, horizon: int) -> None:
        pending = self._pending.get(version_id)
        if pending is None:
            return
        deadline = pending.would_fill.would_fill_detected_monotonic_ns + horizon * 1_000_000
        delay_ns = deadline - self._monotonic_ns()
        if delay_ns > 0:
            await asyncio.sleep(delay_ns / 1_000_000_000)
        await asyncio.sleep(0)
        await self._capture_one(version_id, horizon, force=False)

    async def flush_pending(self, *, force: bool = False) -> None:
        if force:
            tasks = tuple(self._tasks)
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self._tasks.clear()
        if not self._pending:
            return
        if not force:
            deadline = self._monotonic_ns() + 1_200_000_000
            while self._pending and self._tasks and self._monotonic_ns() < deadline:
                await asyncio.sleep(0.02)
        for version_id, pending in tuple(self._pending.items()):
            for horizon in self.config.horizons_ms:
                await self._capture_one(version_id, horizon, force=force)

    async def close(self) -> None:
        self._closing = True
        await self.flush_pending(force=False)
        if self._tasks:
            tasks = tuple(self._tasks)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._tasks.clear()
        self.ingress.close()
        self._active_versions.clear()

    def _prune_state(self, now_ns: int) -> None:
        floor = max(0, now_ns - self.config.trade_retention_ns)
        active_starts = tuple(
            version.quote_created_monotonic_ns
            for version in self._active_versions.values()
        )
        if active_starts:
            floor = max(floor, min(active_starts))
        for market, trades in tuple(self._trades.items()):
            retained = {
                key: trade
                for key, trade in trades.items()
                if trade.received_monotonic_ns >= floor
            }
            if retained:
                self._trades[market] = retained
            else:
                self._trades.pop(market, None)

    def _capture_task_done(self, task: asyncio.Task[None]) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            return
        try:
            failure = task.exception()
        except (asyncio.CancelledError, RuntimeError):
            return
        if failure is not None and self.fatal_reason is None:
            self.fatal_reason = "HEDGE_CAPTURE_FAILED"


class SpreadShadowRunner:
    """Connect the limited feed and observer for one bounded smoke."""

    def __init__(self, feed: PublicFeedRunner, observer: SpreadObserver) -> None:
        if feed.ingress is not observer.ingress:
            raise ValueError("feed and observer must share one ingress queue")
        self.feed = feed
        self.observer = observer

    async def run(self, *, duration_seconds: int | None = None) -> None:
        consumer = asyncio.create_task(self.observer.consume())
        try:
            await self.feed.run(duration_seconds=duration_seconds)
        finally:
            self.feed.ingress.close()
            await consumer
            await self.observer.close()


class ReplayHarness:
    """Deterministic fixture/replay path, explicitly separate from live evidence."""

    def __init__(self, observer: SpreadObserver) -> None:
        self.observer = observer

    async def run(self, items: Iterable[IngressItem]) -> None:
        await self.observer._append(
            ({"kind": "REPLAY_MODE", "evidence_mode": "FIXTURE", "observed_monotonic_ns": 0},)
        )
        ordered = tuple(
            item
            for _, item in sorted(
                enumerate(items),
                key=lambda numbered: (
                    numbered[1].gap.gap_start_monotonic_ns
                    if isinstance(numbered[1], FeedGapEvent)
                    else numbered[1].book.received_monotonic_ns
                    if isinstance(numbered[1], FeedBookEvent)
                    else numbered[1].trade.received_monotonic_ns,
                    numbered[0],
                ),
            )
        )
        self.observer._replay_mode = True
        try:
            for item in ordered:
                await self.observer.handle_item(item)
        finally:
            self.observer._replay_mode = False
        await self.observer.flush_pending(force=True)


async def run_public_smoke(
    store_root: str,
    *,
    config: ShadowConfig | None = None,
    requested_markets: tuple[str, ...] = (),
    source_commit: str = "UNKNOWN",
    duration_seconds: int | None = None,
) -> dict[str, Any]:
    """Run one public-only, 1–3 market smoke and return sanitized run facts."""

    config = config or ShadowConfig()
    started_utc = datetime.now(UTC)
    metadata = {
        "schema_version": 1,
        "source_commit": source_commit,
        "python_version": platform.python_version(),
        "evidence_mode": "OBSERVATIONAL",
        "feed_scope": (Venue.RISEX.value, Venue.LIGHTER.value),
        "requested_markets": requested_markets,
        "started_utc": started_utc,
        "target_notionals_usd": config.target_notionals_usd,
        "target_margins_bps": config.target_margins_bps,
        "horizons_ms": config.horizons_ms,
        "freshness_max_age_ns": config.freshness_max_age_ns,
        "quote_lifetime_ns": config.quote_lifetime_ns,
        "risex_maker_fee_rate": config.risex_maker_fee_rate,
        "lighter_taker_fee_rate": config.lighter_taker_fee_rate,
        "risex_fee_source": config.risex_fee_source,
        "lighter_fee_source": config.lighter_fee_source,
        "created_utc": started_utc,
    }
    store = AppendOnlyEvidenceStore.create(store_root, metadata=metadata)
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            risex = RisexAdapter(session)
            lighter = LighterAdapter(session)
            pairs = await select_public_market_pairs(
                risex,
                lighter,
                requested_markets=requested_markets,
                max_markets=config.max_markets,
            )
            store.append_batch(
                ({
                    "kind": "RUN_START",
                    "markets": tuple(pair.canonical_market for pair in pairs),
                    "duration_seconds": config.duration_seconds if duration_seconds is None else duration_seconds,
                    "started_utc": started_utc,
                    "observed_monotonic_ns": 0,
                },)
            )
            observer = SpreadObserver(config, pairs, store)
            feed = PublicFeedRunner(
                session,
                pairs,
                observer.ingress,
                config=config,
                risex_adapter=risex,
                lighter_adapter=lighter,
            )
            await SpreadShadowRunner(feed, observer).run(duration_seconds=duration_seconds)
            store.append_batch(
                ({
                    "kind": "RUN_STOP",
                    "fatal_reason": feed.fatal_reason or observer.fatal_reason,
                    "stopped_utc": datetime.now(UTC),
                    "observed_monotonic_ns": time.monotonic_ns(),
                },)
            )
            return {
                "run_id": store.run_id,
                "store_path": str(store.path),
                "markets": tuple(pair.canonical_market for pair in pairs),
                "duration_seconds": config.duration_seconds if duration_seconds is None else duration_seconds,
                "fatal_reason": feed.fatal_reason or observer.fatal_reason,
            }
    except Exception as exc:
        store.append_batch(
            ({
                "kind": "RUN_FAILED",
                "failure_class": type(exc).__name__,
                "failed_utc": datetime.now(UTC),
                "observed_monotonic_ns": time.monotonic_ns(),
            },)
        )
        raise
    finally:
        store.close()


__all__ = [
    "BookHistory",
    "HistoryCapacityExceeded",
    "ReplayHarness",
    "SpreadObserver",
    "SpreadShadowRunner",
    "run_public_smoke",
]
