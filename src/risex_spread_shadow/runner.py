"""The single SS-001H observer, evidence path, and public-smoke orchestration."""

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
from .evidence import (
    capture_horizon,
    detect_optimistic_would_fill,
    detect_strict_would_fill,
    is_eligible_trade,
)
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
    FillabilityModel,
    HedgeHorizonCapture,
    HypotheticalMakerQuote,
    QuotePolicy,
    QuoteVersion,
    SampleStopReason,
    SampleStopSignal,
    SpreadDirection,
    TradeEvidence,
    WouldFillEvidence,
)
from .book_chain import (
    BookRevisionChainError,
    BookRevisionEncoder,
    book_state_sha256,
)
from .store import AppendOnlyEvidenceStore, EvidenceStorageLimitExceeded


_SHUTDOWN_TIMEOUT_SECONDS = 2.0
_HORIZON_INGRESS_BARRIER_TIMEOUT_SECONDS = 2.0
_TERMINAL_KINDS = frozenset({"RUN_STOP", "RUN_FAILED"})
_MATERIAL_VALID_STRICT_EPISODE_LIMIT = 10
_MATERIAL_DETECTION_TIMESTAMP_LIMIT = 5
# This is an internal bounded identity ledger, not a scanner configuration
# knob.  It is deliberately larger than the fixed eligible-trade stop so
# irrelevant accepted trades do not consume the economic sample prematurely;
# overflow remains a named integrity failure rather than an unsafe eviction.
_TRADE_IDENTITY_CAPACITY = 100_000


class HistoryCapacityExceeded(RuntimeError):
    """Raised when configured history capacity would discard active evidence."""

    def __init__(self, gap: DataGapEvidence) -> None:
        super().__init__("book history capacity reached; evidence gap is required")
        self.gap = gap


@dataclass(slots=True)
class _HistoryBookRecord:
    """Book metadata plus either a full anchor or compact level changes."""

    metadata: BookEvidence
    full: bool
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]

    def materialize(
        self,
        bids: Mapping[Decimal, Decimal],
        asks: Mapping[Decimal, Decimal],
    ) -> BookEvidence:
        return BookEvidence(
            venue=self.metadata.venue,
            canonical_market=self.metadata.canonical_market,
            bids=tuple(
                BookLevel(price, quantity)
                for price, quantity in sorted(
                    bids.items(), key=lambda item: item[0], reverse=True
                )
                if quantity > 0
            ),
            asks=tuple(
                BookLevel(price, quantity)
                for price, quantity in sorted(asks.items(), key=lambda item: item[0])
                if quantity > 0
            ),
            received_monotonic_ns=self.metadata.received_monotonic_ns,
            stream_session_id=self.metadata.stream_session_id,
            recovery_generation=self.metadata.recovery_generation,
            book_revision=self.metadata.book_revision,
            sequence=self.metadata.sequence,
            checksum=self.metadata.checksum,
            sequence_valid=self.metadata.sequence_valid,
            checksum_valid=self.metadata.checksum_valid,
            received_utc=self.metadata.received_utc,
            fresh=self.metadata.fresh,
            ingress_received_monotonic_ns=self.metadata.ingress_received_monotonic_ns,
            normalized_ready_monotonic_ns=self.metadata.normalized_ready_monotonic_ns,
            decision_ready_monotonic_ns=self.metadata.decision_ready_monotonic_ns,
            tx_hash=self.metadata.tx_hash,
            block_number=self.metadata.block_number,
            log_index=self.metadata.log_index,
            worker_timestamp=self.metadata.worker_timestamp,
        )


@dataclass(slots=True)
class _HistoryChain:
    """One bounded identity chain with a compact retained revision tail."""

    entries: list[_HistoryBookRecord] = field(default_factory=list)
    base_bids: dict[Decimal, Decimal] = field(default_factory=dict)
    base_asks: dict[Decimal, Decimal] = field(default_factory=dict)
    current_bids: dict[Decimal, Decimal] = field(default_factory=dict)
    current_asks: dict[Decimal, Decimal] = field(default_factory=dict)
    last_revision: int | None = None


def _history_levels(levels: Iterable[BookLevel]) -> dict[Decimal, Decimal]:
    result: dict[Decimal, Decimal] = {}
    for level in levels:
        if not isinstance(level, BookLevel):
            raise TypeError("book levels must be BookLevel values")
        if level.canonical_price <= 0 or level.canonical_quantity <= 0:
            raise ValueError("book levels must have positive price and quantity")
        if level.canonical_price in result:
            raise ValueError("book levels must not repeat a price")
        result[level.canonical_price] = level.canonical_quantity
    return result


def _history_changes(
    previous: Mapping[Decimal, Decimal],
    current: Mapping[Decimal, Decimal],
    *,
    reverse: bool,
) -> tuple[BookLevel, ...]:
    changed: list[BookLevel] = []
    for price in sorted(previous.keys() | current.keys(), reverse=reverse):
        old = previous.get(price)
        new = current.get(price)
        if old != new:
            changed.append(BookLevel(price, Decimal("0") if new is None else new))
    return tuple(changed)


def _apply_history_record(
    record: _HistoryBookRecord,
    bids: dict[Decimal, Decimal],
    asks: dict[Decimal, Decimal],
) -> None:
    if record.full:
        bids.clear()
        asks.clear()
        bids.update(_history_levels(record.bids))
        asks.update(_history_levels(record.asks))
        return
    for level in record.bids:
        if level.canonical_quantity == 0:
            bids.pop(level.canonical_price, None)
        else:
            bids[level.canonical_price] = level.canonical_quantity
    for level in record.asks:
        if level.canonical_quantity == 0:
            asks.pop(level.canonical_price, None)
        else:
            asks[level.canonical_price] = level.canonical_quantity


@dataclass(slots=True)
class SampleStopController:
    """Latch the first prospective SS-001H sample-stop condition.

    Strict episodes are retained as a raw diagnostic counter, but they are no
    longer an aggregate stop condition.  The strict stop is earned only by a
    single exact policy after its completed, currently valid episodes reach
    the frozen count and detection-timestamp thresholds.
    """

    started_monotonic_ns: int
    strict_episode_limit: int = 50
    eligible_trade_limit: int = 500
    wall_clock_limit_ns: int = 20 * 60 * 1_000_000_000
    strict_episode_count: int = 0
    eligible_trade_count: int = 0
    optimistic_episode_count: int = 0
    signal: SampleStopSignal | None = None
    material_valid_strict_episode_limit: int = _MATERIAL_VALID_STRICT_EPISODE_LIMIT
    material_detection_timestamp_limit: int = _MATERIAL_DETECTION_TIMESTAMP_LIMIT
    _material_episodes: dict[str, dict[str, int]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for value, name in (
            (self.started_monotonic_ns, "started_monotonic_ns"),
            (self.strict_episode_limit, "strict_episode_limit"),
            (self.eligible_trade_limit, "eligible_trade_limit"),
            (self.wall_clock_limit_ns, "wall_clock_limit_ns"),
            (self.strict_episode_count, "strict_episode_count"),
            (self.eligible_trade_count, "eligible_trade_count"),
            (self.optimistic_episode_count, "optimistic_episode_count"),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if self.strict_episode_limit <= 0:
            raise ValueError("strict_episode_limit must be positive")
        if self.eligible_trade_limit <= 0:
            raise ValueError("eligible_trade_limit must be positive")
        if self.wall_clock_limit_ns <= 0:
            raise ValueError("wall_clock_limit_ns must be positive")
        if (
            isinstance(self.material_valid_strict_episode_limit, bool)
            or not isinstance(self.material_valid_strict_episode_limit, int)
            or self.material_valid_strict_episode_limit <= 0
        ):
            raise ValueError("material valid strict episode limit must be positive")
        if (
            isinstance(self.material_detection_timestamp_limit, bool)
            or not isinstance(self.material_detection_timestamp_limit, int)
            or self.material_detection_timestamp_limit <= 0
        ):
            raise ValueError("material detection timestamp limit must be positive")

    def _signal(
        self,
        *,
        reason: SampleStopReason,
        observed_monotonic_ns: int,
        integrity_reason: str | None = None,
        material_policy_id: str | None = None,
    ) -> SampleStopSignal:
        material_count = 0
        timestamp_count = 0
        if material_policy_id is not None:
            episodes = self._material_episodes[material_policy_id]
            material_count = len(episodes)
            timestamp_count = len(set(episodes.values()))
        return SampleStopSignal(
            reason=reason,
            observed_monotonic_ns=observed_monotonic_ns,
            strict_episode_count=self.strict_episode_count,
            eligible_trade_count=self.eligible_trade_count,
            optimistic_episode_count=self.optimistic_episode_count,
            integrity_reason=integrity_reason,
            material_policy_id=material_policy_id,
            material_valid_strict_episode_count=material_count,
            material_detection_timestamp_count=timestamp_count,
        )

    def _observe_non_material_stop(
        self,
        *,
        observed_monotonic_ns: int,
        integrity_reason: str | None,
    ) -> SampleStopSignal | None:
        """Evaluate only the unchanged eligible/wall/integrity stops."""

        if integrity_reason is not None:
            reason = SampleStopReason.INTEGRITY_FAILURE
        elif self.eligible_trade_count >= self.eligible_trade_limit:
            reason = SampleStopReason.ELIGIBLE_TRADE_LIMIT
        elif observed_monotonic_ns - self.started_monotonic_ns >= self.wall_clock_limit_ns:
            reason = SampleStopReason.WALL_CLOCK_LIMIT
        else:
            return None
        self.signal = self._signal(
            reason=reason,
            observed_monotonic_ns=observed_monotonic_ns,
            integrity_reason=integrity_reason,
        )
        return self.signal

    def observe(
        self,
        *,
        observed_monotonic_ns: int,
        strict_episode_increment: int = 0,
        eligible_trade_increment: int = 0,
        optimistic_episode_increment: int = 0,
        integrity_reason: str | None = None,
    ) -> SampleStopSignal | None:
        """Advance counters once and latch a deterministic first-stop signal."""

        if isinstance(observed_monotonic_ns, bool) or not isinstance(observed_monotonic_ns, int):
            raise TypeError("observed_monotonic_ns must be int")
        if observed_monotonic_ns < self.started_monotonic_ns:
            raise ValueError("observed_monotonic_ns must not precede sample start")
        for value, name in (
            (strict_episode_increment, "strict_episode_increment"),
            (eligible_trade_increment, "eligible_trade_increment"),
            (optimistic_episode_increment, "optimistic_episode_increment"),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if integrity_reason is not None and not integrity_reason:
            raise ValueError("integrity_reason must be non-empty when supplied")
        if self.signal is not None:
            return self.signal
        self.strict_episode_count += strict_episode_increment
        self.eligible_trade_count += eligible_trade_increment
        self.optimistic_episode_count += optimistic_episode_increment
        return self._observe_non_material_stop(
            observed_monotonic_ns=observed_monotonic_ns,
            integrity_reason=integrity_reason,
        )

    def observe_material(
        self,
        *,
        policy_id: str,
        episode_id: str,
        detection_monotonic_ns: int,
        observed_monotonic_ns: int,
    ) -> SampleStopSignal | None:
        """Record one completed valid strict episode for its exact policy."""

        if not isinstance(policy_id, str) or not policy_id:
            raise ValueError("material policy_id must be a non-empty string")
        if not isinstance(episode_id, str) or not episode_id:
            raise ValueError("material episode_id must be a non-empty string")
        if (
            isinstance(detection_monotonic_ns, bool)
            or not isinstance(detection_monotonic_ns, int)
            or detection_monotonic_ns < self.started_monotonic_ns
        ):
            raise ValueError("material detection timestamp must be an in-range integer")
        if (
            isinstance(observed_monotonic_ns, bool)
            or not isinstance(observed_monotonic_ns, int)
            or observed_monotonic_ns < detection_monotonic_ns
        ):
            raise ValueError(
                "material observed timestamp must not precede detection timestamp"
            )
        if self.signal is not None:
            return self.signal
        episodes = self._material_episodes.setdefault(policy_id, {})
        episodes.setdefault(episode_id, detection_monotonic_ns)
        candidates = tuple(
            candidate
            for candidate in sorted(self._material_episodes)
            if len(self._material_episodes[candidate])
            >= self.material_valid_strict_episode_limit
            and len(set(self._material_episodes[candidate].values()))
            >= self.material_detection_timestamp_limit
        )
        if candidates:
            self.signal = self._signal(
                reason=SampleStopReason.STRICT_EPISODE_LIMIT,
                observed_monotonic_ns=observed_monotonic_ns,
                material_policy_id=candidates[0],
            )
            return self.signal
        return self._observe_non_material_stop(
            observed_monotonic_ns=observed_monotonic_ns,
            integrity_reason=None,
        )

    def invalidate_material(self, *, policy_id: str, episode_id: str) -> None:
        """Remove a late-contaminated candidate before a stop is latched."""

        if self.signal is not None:
            return
        episodes = self._material_episodes.get(policy_id)
        if episodes is None:
            return
        episodes.pop(episode_id, None)
        if not episodes:
            self._material_episodes.pop(policy_id, None)

    def material_counts(self, policy_id: str) -> tuple[int, int]:
        episodes = self._material_episodes.get(policy_id, {})
        return len(episodes), len(set(episodes.values()))

    def observe_counts(
        self,
        *,
        observed_monotonic_ns: int,
        strict_episode_count: int,
        eligible_trade_count: int,
        optimistic_episode_count: int = 0,
        integrity_reason: str | None = None,
    ) -> SampleStopSignal | None:
        """Advance from absolute counts without allowing counter rollback."""

        if any(
            isinstance(value, bool) or not isinstance(value, int) or value < 0
            for value in (
                strict_episode_count,
                eligible_trade_count,
                optimistic_episode_count,
            )
        ):
            raise ValueError("sample counts must be non-negative integers")
        if strict_episode_count < self.strict_episode_count:
            raise ValueError("strict episode count cannot move backwards")
        if eligible_trade_count < self.eligible_trade_count:
            raise ValueError("eligible trade count cannot move backwards")
        if optimistic_episode_count < self.optimistic_episode_count:
            raise ValueError("optimistic episode count cannot move backwards")
        return self.observe(
            observed_monotonic_ns=observed_monotonic_ns,
            strict_episode_increment=strict_episode_count - self.strict_episode_count,
            eligible_trade_increment=eligible_trade_count - self.eligible_trade_count,
            optimistic_episode_increment=optimistic_episode_count - self.optimistic_episode_count,
            integrity_reason=integrity_reason,
        )


class BookHistory:
    """Time-retained books with bounded compact revision-chain storage."""

    def __init__(self, *, retention_ns: int, capacity: int | None = None) -> None:
        self.retention_ns = retention_ns
        self.capacity = capacity
        self._chains: dict[
            tuple[Venue, str, str | int, int], _HistoryChain
        ] = {}
        self._active_identities: dict[
            tuple[Venue, str], tuple[Venue, str, str | int, int]
        ] = {}
        self._gaps: list[DataGapEvidence] = []
        self._pending: dict[str, tuple[int, int]] = {}
        self._pending_identity: dict[
            str, tuple[Venue, str, str | int, int] | None
        ] = {}

    @property
    def book_count(self) -> int:
        return sum(len(chain.entries) for chain in self._chains.values())

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
        for identity, chain in tuple(self._chains.items()):
            split_at = 0
            for entry in chain.entries:
                if entry.metadata.received_monotonic_ns >= cutoff:
                    break
                _apply_history_record(entry, chain.base_bids, chain.base_asks)
                split_at += 1
            if split_at:
                del chain.entries[:split_at]
            # Keep the current compact state even when the retained time
            # window has no entries.  A later DELTA in the same stream still
            # has a valid predecessor in memory; only the historical query
            # tail has been pruned.
            market_identity = (identity[0], identity[1])
            if (
                not chain.entries
                and self._active_identities.get(market_identity) != identity
                and not any(
                    pending_identity == identity
                    for pending_identity in self._pending_identity.values()
                )
            ):
                del self._chains[identity]

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

    def add_book(self, book: BookEvidence, *, source_kind: str | None = None) -> None:
        if source_kind not in {None, "SNAPSHOT", "DELTA"}:
            raise ValueError("unsupported book source kind")
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
        identity = (
            book.venue,
            book.canonical_market,
            book.stream_session_id,
            book.recovery_generation,
        )
        chain = self._chains.get(identity)
        current_bids = _history_levels(book.bids)
        current_asks = _history_levels(book.asks)
        if chain is None:
            if source_kind == "DELTA":
                raise ValueError("delta book requires a retained predecessor snapshot")
            full = True
            stored_bids = tuple(book.bids)
            stored_asks = tuple(book.asks)
            chain = _HistoryChain()
            self._chains[identity] = chain
            self._active_identities[(book.venue, book.canonical_market)] = identity
            for old_identity in tuple(self._chains):
                if old_identity == identity or old_identity[:2] != identity[:2]:
                    continue
                if any(
                    pending_identity == old_identity
                    for pending_identity in self._pending_identity.values()
                ):
                    continue
                del self._chains[old_identity]
        else:
            if chain.last_revision is not None and book.book_revision <= chain.last_revision:
                raise ValueError("book revision is duplicate or out of order")
            if source_kind in {None, "SNAPSHOT"}:
                full = True
                stored_bids = tuple(book.bids)
                stored_asks = tuple(book.asks)
            else:
                if chain.last_revision is not None and book.book_revision != chain.last_revision + 1:
                    raise ValueError("book revision predecessor is missing")
                full = False
                stored_bids = _history_changes(
                    chain.current_bids, current_bids, reverse=True
                )
                stored_asks = _history_changes(
                    chain.current_asks, current_asks, reverse=False
                )
        metadata = BookEvidence(
            venue=book.venue,
            canonical_market=book.canonical_market,
            bids=(),
            asks=(),
            received_monotonic_ns=book.received_monotonic_ns,
            stream_session_id=book.stream_session_id,
            recovery_generation=book.recovery_generation,
            book_revision=book.book_revision,
            sequence=book.sequence,
            checksum=book.checksum,
            sequence_valid=book.sequence_valid,
            checksum_valid=book.checksum_valid,
            received_utc=book.received_utc,
            fresh=book.fresh,
            ingress_received_monotonic_ns=book.ingress_received_monotonic_ns,
            normalized_ready_monotonic_ns=book.normalized_ready_monotonic_ns,
            decision_ready_monotonic_ns=book.decision_ready_monotonic_ns,
            tx_hash=book.tx_hash,
            block_number=book.block_number,
            log_index=book.log_index,
            worker_timestamp=book.worker_timestamp,
        )
        chain.entries.append(
            _HistoryBookRecord(metadata, full, stored_bids, stored_asks)
        )
        chain.current_bids = current_bids
        chain.current_asks = current_asks
        chain.last_revision = book.book_revision

    def add_gap(self, gap: DataGapEvidence) -> None:
        if gap not in self._gaps:
            self._gaps.append(gap)
        self._prune(gap.gap_start_monotonic_ns)

    def books(self, venue: Venue, market: str, deadline_ns: int | None = None) -> tuple[BookEvidence, ...]:
        values: list[BookEvidence] = []
        for (entry_venue, entry_market, _session, _recovery), chain in self._chains.items():
            if entry_venue is not venue or entry_market != market:
                continue
            bids = dict(chain.base_bids)
            asks = dict(chain.base_asks)
            for entry in chain.entries:
                if deadline_ns is not None and entry.metadata.received_monotonic_ns > deadline_ns:
                    continue
                _apply_history_record(entry, bids, asks)
                values.append(entry.materialize(bids, asks))
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

    def latest_books(
        self,
        venue: Venue,
        market: str,
        deadline_ns: int | None = None,
    ) -> tuple[BookEvidence, ...]:
        """Return only the latest revision per identity, retaining tied ambiguity."""

        candidates: list[BookEvidence] = []
        for (entry_venue, entry_market, _session, _recovery), chain in self._chains.items():
            if entry_venue is not venue or entry_market != market:
                continue
            bids = dict(chain.base_bids)
            asks = dict(chain.base_asks)
            latest: _HistoryBookRecord | None = None
            for entry in chain.entries:
                if deadline_ns is not None and entry.metadata.received_monotonic_ns > deadline_ns:
                    continue
                _apply_history_record(entry, bids, asks)
                latest = entry
            if latest is not None:
                candidates.append(latest.materialize(bids, asks))
        if not candidates:
            return ()
        latest_rank = max(
            (
                book.received_monotonic_ns,
                book.book_revision,
                -1 if book.sequence is None else book.sequence,
            )
            for book in candidates
        )
        return tuple(
            book
            for book in candidates
            if (
                book.received_monotonic_ns,
                book.book_revision,
                -1 if book.sequence is None else book.sequence,
            )
            == latest_rank
        )

    def gaps(self) -> tuple[DataGapEvidence, ...]:
        return tuple(self._gaps)


@dataclass(slots=True)
class _PendingEpisode:
    version: QuoteVersion
    would_fill: WouldFillEvidence
    captures: dict[int, HedgeHorizonCapture] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class _MaterialCandidate:
    """Compact identity retained for late gap invalidation."""

    version_id: str
    market: str
    quote_created: int
    detected: int
    risex_stream_session: str | int | None
    risex_recovery: int | None
    hedge_stream_session: str | int | None
    hedge_recovery: int | None


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
        sample_started_monotonic_ns: int | None = None,
        directions: Sequence[SpreadDirection] | None = None,
        material_stop_enabled: bool = True,
        enforce_sample_deadline: bool = False,
    ) -> None:
        self.config = config
        self.market_pairs = tuple(market_pairs)
        self.store = store
        self.directions = tuple(
            directions
            if directions is not None
            else (
                SpreadDirection.RISEX_BUY_LIGHTER_SELL,
                SpreadDirection.RISEX_SELL_LIGHTER_BUY,
            )
        )
        if not self.directions:
            raise ValueError("observer directions must not be empty")
        if any(not isinstance(direction, SpreadDirection) for direction in self.directions):
            raise TypeError("observer directions must be SpreadDirection values")
        if len(set(self.directions)) != len(self.directions):
            raise ValueError("observer directions must not repeat")
        if not isinstance(material_stop_enabled, bool):
            raise TypeError("material_stop_enabled must be bool")
        self.material_stop_enabled = material_stop_enabled
        if not isinstance(enforce_sample_deadline, bool):
            raise TypeError("enforce_sample_deadline must be bool")
        self.enforce_sample_deadline = enforce_sample_deadline
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
        self._book_encoder = BookRevisionEncoder()
        self._last_book_ranks: dict[tuple[Venue, str], tuple[int, int, int]] = {}
        self._awaiting_fresh_snapshot: set[tuple[Venue, str]] = set()
        self._active_versions: dict[str, QuoteVersion] = {}
        self._trades: dict[str, dict[str, TradeEvidence]] = {}
        self._trade_identities: dict[str, tuple[Any, ...]] = {}
        self._trade_identity_capacity = max(
            _TRADE_IDENTITY_CAPACITY,
            config.eligible_trade_limit,
        )
        self._pending: dict[str, _PendingEpisode] = {}
        self._optimistic_pending: dict[str, _PendingEpisode] = {}
        # Strict episodes remain available until the run ends so a late
        # matching gap can remove a not-yet-counted material candidate.  The
        # bounded prospective episode cap keeps this memory finite.  Retain
        # only exact gap-matching identity after the four captures complete;
        # do not keep potentially large hedge books alive for the whole run.
        self._material_candidates: dict[str, tuple[str, _MaterialCandidate]] = {}
        self._material_boundary_task: asyncio.Task[None] | None = None
        self._material_finalize_lock = asyncio.Lock()
        self._handling_item = 0
        # One completed model/version must not be rediscovered while its
        # quote remains active.  The map is keyed by the fixed policy grid,
        # so it stays bounded while allowing a replacement quote version to
        # start a fresh episode.
        self._last_episode_versions: dict[tuple[FillabilityModel, str], str] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._append_lock = asyncio.Lock()
        # Evidence writes are deliberately buffered only at this observer
        # boundary.  The ingress queue remains bounded and the store remains
        # append-only JSONL; a batch is always written and fsynced as one
        # operation.  Keeping the pending records here removes one fsync per
        # event from the hot feed path without introducing another queue or
        # persistence service.
        self._pending_records: list[Mapping[str, Any]] = []
        self._pending_started_at: float | None = None
        self._batch_loop_task: asyncio.Task[None] | None = None
        self._batch_stop_event = asyncio.Event()
        self._batch_wakeup = asyncio.Event()
        self._append_failure: BaseException | None = None
        self._append_operation: asyncio.Task[Any] | None = None
        self._last_append_succeeded = False
        self._append_cancelled = False
        self._version_serial = 0
        sample_started = (
            0
            if sample_started_monotonic_ns is None
            else sample_started_monotonic_ns
        )
        self._sample_stop = SampleStopController(
            started_monotonic_ns=sample_started,
            strict_episode_limit=config.strict_episode_limit,
            eligible_trade_limit=config.eligible_trade_limit,
            wall_clock_limit_ns=config.sample_wall_clock_limit_ns,
        )
        self._sample_stop_event = asyncio.Event()
        self._horizon_drain_event = asyncio.Event()
        self._sample_frozen = False
        self._sample_stop_recorded = False
        self.fatal_reason: str | None = None
        self._closing = False
        self._closed_for_append = False
        self._terminal_started = False
        self._terminal_written = False
        self._terminal_error: BaseException | None = None
        self._terminal_complete_event = asyncio.Event()
        self._replay_mode = False

    @property
    def active_version_count(self) -> int:
        return len(self._active_versions)

    @property
    def pending_episode_count(self) -> int:
        # Preserve the SS-001B public meaning: this is the strict lower-bound
        # pending count.  The two models have independent counters below.
        return len(self._pending)

    @property
    def optimistic_pending_episode_count(self) -> int:
        return len(self._optimistic_pending)

    @property
    def total_pending_episode_count(self) -> int:
        return len(self._pending) + len(self._optimistic_pending)

    @property
    def strict_episode_count(self) -> int:
        return self._sample_stop.strict_episode_count

    @property
    def optimistic_episode_count(self) -> int:
        return self._sample_stop.optimistic_episode_count

    @property
    def eligible_trade_count(self) -> int:
        return self._sample_stop.eligible_trade_count

    @property
    def sample_stop_signal(self) -> SampleStopSignal | None:
        return self._sample_stop.signal

    @property
    def sample_started_monotonic_ns(self) -> int:
        return self._sample_stop.started_monotonic_ns

    @property
    def sample_stop_event(self) -> asyncio.Event:
        return self._sample_stop_event

    @property
    def horizon_drain_event(self) -> asyncio.Event:
        return self._horizon_drain_event

    def _refresh_horizon_drain_event(self) -> None:
        if self._sample_frozen and not self._pending and not self._optimistic_pending:
            self._horizon_drain_event.set()

    def _schedule_material_stop_boundary(self) -> None:
        """Schedule one stable idle-queue watermark for material candidates."""

        if (
            self._sample_frozen
            or self.fatal_reason is not None
            or not self._material_candidates
            or self._material_boundary_task is not None
        ):
            return
        self._material_boundary_task = asyncio.create_task(
            self._material_stop_boundary()
        )

    async def _material_stop_boundary(self) -> None:
        """Wait across an idle turn before publishing a material stop.

        A fourth horizon can complete before a matching gap already offered by
        the feed reaches the observer.  The stable offer serial plus an empty
        ingress and no active consumer item form the bounded queue watermark:
        all evidence offered through that serial has been handled, while an
        offer arriving during the settling turn cancels this attempt.
        """

        try:
            await asyncio.sleep(0)
            watermark = self.ingress.offer_serial
            await asyncio.sleep(0)
            if (
                self._handling_item
                or self.ingress.has_pending
                or self.ingress.offer_serial != watermark
            ):
                return
            await self._finalize_material_stop(
                observed_monotonic_ns=max(
                    self._sample_stop.started_monotonic_ns,
                    self._monotonic_ns(),
                )
            )
        finally:
            self._material_boundary_task = None

    async def _finalize_material_stop(
        self,
        *,
        observed_monotonic_ns: int,
    ) -> dict[str, Any] | None:
        """Count only candidates clean at the queue/watermark boundary."""

        async with self._material_finalize_lock:
            if self._sample_frozen or self.fatal_reason is not None:
                return None
            if (
                isinstance(observed_monotonic_ns, bool)
                or not isinstance(observed_monotonic_ns, int)
            ):
                raise TypeError("material observed timestamp must be int")
            for version_id, (policy_id, candidate) in tuple(
                self._material_candidates.items()
            ):
                if not self._gap_overlaps_material_candidate_is_clean(candidate):
                    self._sample_stop.invalidate_material(
                        policy_id=policy_id,
                        episode_id=version_id,
                    )
                    self._material_candidates.pop(version_id, None)
                    continue
                signal = self._sample_stop.observe_material(
                    policy_id=policy_id,
                    episode_id=version_id,
                    detection_monotonic_ns=candidate.detected,
                    observed_monotonic_ns=max(
                        observed_monotonic_ns,
                        candidate.detected,
                    ),
                )
                if signal is None:
                    continue
                self._sample_frozen = True
                self._sample_stop_event.set()
                self._refresh_horizon_drain_event()
                record = self._sample_stop_record(signal)
                if record is not None:
                    await self._append((record,))
                return record
        return None

    @property
    def pending_record_count(self) -> int:
        """Return the number of evidence records awaiting a durable sync."""

        return len(self._pending_records)

    @property
    def append_quiescent(self) -> bool:
        """Whether no thread-backed store append remains in flight."""

        operation = self._append_operation
        return operation is None or operation.done()

    @property
    def terminal_written(self) -> bool:
        return self._terminal_written

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

    def _register_append_failure(self, exc: BaseException) -> None:
        """Latch one writer failure and turn it into an integrity stop."""

        if self._append_failure is not None:
            return
        self._append_failure = exc
        self.fatal_reason = (
            "EVIDENCE_STORAGE_LIMIT"
            if isinstance(exc, EvidenceStorageLimitExceeded)
            else "EVIDENCE_STORE_WRITE_FAILED"
        )
        self._pending_records.clear()
        self._pending_started_at = None
        self._observe_sample_stop(
            observed_monotonic_ns=max(
                self._sample_stop.started_monotonic_ns,
                self._monotonic_ns(),
            ),
            integrity_reason=self.fatal_reason,
        )

    def _register_append_cancelled(self, *, terminal: bool) -> None:
        """Latch cancellation without permitting an unsafe append retry."""

        self._append_cancelled = True
        if self.fatal_reason is None:
            self.fatal_reason = "EVIDENCE_APPEND_CANCELLED"
        if not terminal:
            self._observe_sample_stop(
                observed_monotonic_ns=max(
                    self._sample_stop.started_monotonic_ns,
                    self._monotonic_ns(),
                ),
                integrity_reason=self.fatal_reason,
            )

    async def _wait_store_operation(
        self,
        operation: asyncio.Task[Any],
    ) -> Any:
        """Wait for a thread-backed append even when its caller is cancelled."""

        while True:
            if operation.cancelled():
                raise RuntimeError("evidence append worker was cancelled")
            try:
                return await asyncio.shield(operation)
            except asyncio.CancelledError:
                # Cancellation of this coroutine must not cancel the actual
                # worker.  Keep waiting until the file operation is known to
                # have finished, so no later append can overlap it.
                continue

    async def _store_append(
        self,
        records: Sequence[Mapping[str, Any]],
        *,
        terminal: bool = False,
    ) -> Any:
        """Run one physical append and retain its completion across cancel."""

        if self._append_operation is not None:
            raise RuntimeError("concurrent evidence append is not permitted")
        operation = asyncio.create_task(
            asyncio.to_thread(self.store.append_batch, records)
        )
        self._append_operation = operation
        self._last_append_succeeded = False
        try:
            result = await asyncio.shield(operation)
            self._last_append_succeeded = True
            return result
        except asyncio.CancelledError:
            try:
                result = await self._wait_store_operation(operation)
            except Exception as exc:
                self._register_append_failure(exc)
                raise
            self._last_append_succeeded = True
            self._register_append_cancelled(terminal=terminal)
            # The physical append is complete, but the caller's cancellation
            # remains a fail-closed run condition.
            raise
        except Exception as exc:
            self._register_append_failure(exc)
            raise
        finally:
            if operation.done() and self._append_operation is operation:
                self._append_operation = None

    async def _flush_pending_batch_locked(self, *, allow_failure: bool = False) -> None:
        """Durably append the current batch while ``_append_lock`` is held."""

        if self._append_failure is not None and not (
            allow_failure and isinstance(self._append_failure, EvidenceStorageLimitExceeded)
        ):
            raise self._append_failure
        if not self._pending_records:
            return
        # Detach before the blocking file operation.  A failed write is a
        # terminal condition and must never be blindly replayed: a store may
        # have partially written before reporting an OS error.
        records = tuple(self._pending_records)
        self._pending_records.clear()
        self._pending_started_at = None
        try:
            # Keep JSON serialization, flush, and fsync off the event loop.
            # append_batch performs one sync for the whole batch.
            await self._store_append(records)
        except Exception as exc:
            # _store_append latches the first physical failure.  Preserve the
            # original exception here without attempting a second append.
            if not isinstance(exc, asyncio.CancelledError):
                if self._append_failure is None:
                    self._register_append_failure(exc)
            raise

    async def _flush_pending_batch(self) -> None:
        """Durably append the current batch, preserving one append order."""

        async with self._append_lock:
            await self._flush_pending_batch_locked()

    async def _batch_loop(self) -> None:
        """Flush below-threshold evidence at the configured interval."""

        interval = float(self.config.store_batch_interval_seconds)
        loop = asyncio.get_running_loop()
        try:
            while not self._batch_stop_event.is_set():
                async with self._append_lock:
                    pending_started_at = self._pending_started_at
                if pending_started_at is None:
                    self._batch_wakeup.clear()
                    if self._batch_stop_event.is_set():
                        break
                    # Close the race between the empty check and wait: an
                    # append either is visible here or sets the wake event
                    # before waiting begins.
                    async with self._append_lock:
                        if self._pending_records:
                            continue
                    if self._batch_stop_event.is_set():
                        break
                    await self._batch_wakeup.wait()
                    continue
                remaining = max(0.0, interval - (loop.time() - pending_started_at))
                self._batch_wakeup.clear()
                try:
                    await asyncio.wait_for(self._batch_wakeup.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    pass
                if self._batch_stop_event.is_set():
                    break
                if self._batch_wakeup.is_set():
                    continue
                try:
                    await self._flush_pending_batch()
                except asyncio.CancelledError:
                    raise
                except BaseException:
                    # The first failure is retained for the consumer/close
                    # path.  Do not keep retrying an ambiguous append.
                    return
        except asyncio.CancelledError:
            raise

    def _ensure_batch_loop(self) -> None:
        if self._batch_stop_event.is_set():
            return
        task = self._batch_loop_task
        if task is None or task.done():
            task = asyncio.create_task(self._batch_loop())
            self._batch_loop_task = task
            task.add_done_callback(self._batch_task_done)

    def _batch_task_done(self, task: asyncio.Task[None]) -> None:
        if task is self._batch_loop_task:
            self._batch_loop_task = None
        if task.cancelled():
            return
        try:
            failure = task.exception()
        except (asyncio.CancelledError, RuntimeError):
            return
        if failure is not None:
            self._register_append_failure(failure)

    async def _append(self, records: Sequence[Mapping[str, Any]]) -> None:
        if not records:
            return
        async with self._append_lock:
            if self._append_failure is not None:
                raise self._append_failure
            if self._append_cancelled:
                raise RuntimeError("evidence append was cancelled; run is fail-closed")
            if self._terminal_started:
                raise RuntimeError("terminal evidence append has already started")
            if self._closed_for_append:
                raise RuntimeError("evidence writer is closed")

            # Keep each domain event contiguous.  If adding it would cross
            # the configured count, flush the prior records first, then
            # enqueue the event as a fresh batch.  A single event may itself
            # contain several quote rows; it is never split or dropped.
            incoming = tuple(dict(record) for record in records)
            if any(
                isinstance(record.get("kind"), str)
                and record["kind"] in _TERMINAL_KINDS
                for record in incoming
            ):
                raise ValueError("terminal evidence must use append_terminal")
            if self._pending_records and (
                len(self._pending_records) + len(incoming)
                > self.config.store_batch_size
            ):
                await self._flush_pending_batch_locked()
            if self._pending_started_at is None:
                self._pending_started_at = asyncio.get_running_loop().time()
            self._pending_records.extend(incoming)
            self._ensure_batch_loop()
            self._batch_wakeup.set()
            if len(self._pending_records) >= self.config.store_batch_size:
                await self._flush_pending_batch_locked()

            # Integrity markers and explicit gaps are safety evidence.  Persist
            # them immediately so a later cap/failure cannot hide the reserved
            # terminal evidence; normal books/quotes/trades stay batched.
            if any(
                record.get("kind") in {"DATA_GAP", "SAMPLE_STOP"}
                for record in incoming
            ):
                await self._flush_pending_batch_locked()

    def _book_record(self, event: FeedBookEvent) -> dict[str, Any]:
        book = event.book
        record = self._book_encoder.encode(book, source_kind=event.source_kind)
        record["checksum_validation"] = event.checksum_validation
        return record

    def _gap_record(self, gap: DataGapEvidence) -> dict[str, Any]:
        record: dict[str, Any] = {
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
        if gap.protocol_frame_kind is not None:
            record.update(
                {
                    "protocol_frame_kind": gap.protocol_frame_kind,
                    "protocol_frame_category": gap.protocol_frame_category,
                    "protocol_frame_length": gap.protocol_frame_length,
                    "protocol_frame_sha256": gap.protocol_frame_sha256,
                }
            )
        if gap.transport_event is not None:
            record["transport_event"] = gap.transport_event
        if gap.transport_failure_class is not None:
            record["transport_failure_class"] = gap.transport_failure_class
        if gap.transport_exception_type is not None:
            record["transport_exception_type"] = gap.transport_exception_type
        return record

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
        record = {
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
        return record

    def _quote_record(
        self,
        policy: QuotePolicy,
        quote: HypotheticalMakerQuote,
        *,
        version: QuoteVersion | None,
        created_ns: int,
        risex_book: BookEvidence | None = None,
        lighter_book: BookEvidence | None = None,
        risex_state_sha256: str | None = None,
        lighter_state_sha256: str | None = None,
    ) -> dict[str, Any]:
        risex_revision = (
            version.risex_book_revision
            if version is not None
            else None
            if risex_book is None
            else risex_book.book_revision
        )
        lighter_revision = (
            version.lighter_book_revision
            if version is not None
            else None
            if lighter_book is None
            else lighter_book.book_revision
        )
        risex_revision_id = (
            version.risex_book_revision_id
            if version is not None
            else None
            if risex_book is None
            else risex_book.book_revision_id
        )
        lighter_revision_id = (
            version.lighter_book_revision_id
            if version is not None
            else None
            if lighter_book is None
            else lighter_book.book_revision_id
        )
        record = {
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
            "quote_stream_session_id": None if version is None else version.stream_session_id,
            "quote_recovery_generation": None if version is None else version.recovery_generation,
            "hedge_stream_session_id": None if version is None else version.hedge_stream_session_id,
            "hedge_recovery_generation": None if version is None else version.hedge_recovery_generation,
            "risex_book_revision": risex_revision,
            "risex_book_revision_id": risex_revision_id,
            "risex_book_state_sha256": (
                risex_state_sha256
                if risex_state_sha256 is not None
                else None if risex_book is None else book_state_sha256(risex_book)
            ),
            "risex_book_stream_session_id": (
                None
                if version is None and risex_book is None
                else version.stream_session_id
                if version is not None
                else risex_book.stream_session_id
            ),
            "risex_book_recovery_generation": (
                None
                if version is None and risex_book is None
                else version.recovery_generation
                if version is not None
                else risex_book.recovery_generation
            ),
            "lighter_book_revision": lighter_revision,
            "lighter_book_revision_id": lighter_revision_id,
            "lighter_book_state_sha256": (
                lighter_state_sha256
                if lighter_state_sha256 is not None
                else None if lighter_book is None else book_state_sha256(lighter_book)
            ),
            "lighter_book_stream_session_id": (
                None
                if version is None and lighter_book is None
                else version.hedge_stream_session_id
                if version is not None
                else lighter_book.stream_session_id
            ),
            "lighter_book_recovery_generation": (
                None
                if version is None and lighter_book is None
                else version.hedge_recovery_generation
                if version is not None
                else lighter_book.recovery_generation
            ),
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
        if version is not None:
            for name in (
                "ingress_received_monotonic_ns",
                "normalized_ready_monotonic_ns",
                "decision_ready_monotonic_ns",
            ):
                value = getattr(version, name)
                if value is not None:
                    record[name] = value
        return record

    @staticmethod
    def _pending_key(model: FillabilityModel, version_id: str) -> str:
        if model is FillabilityModel.STRICT_LOWER_BOUND:
            return version_id
        return f"{model.value}:{version_id}"

    def _pending_for_model(self, model: FillabilityModel) -> dict[str, _PendingEpisode]:
        return (
            self._pending
            if model is FillabilityModel.STRICT_LOWER_BOUND
            else self._optimistic_pending
        )

    def _sample_stop_record(
        self,
        signal: SampleStopSignal | None,
    ) -> dict[str, Any] | None:
        if signal is None or self._sample_stop_recorded:
            return None
        self._sample_stop_recorded = True
        return {
            "kind": "SAMPLE_STOP",
            "reason": signal.reason.value,
            "strict_episode_count": signal.strict_episode_count,
            "optimistic_episode_count": signal.optimistic_episode_count,
            "eligible_trade_count": signal.eligible_trade_count,
            "integrity_reason": signal.integrity_reason,
            "material_policy_id": signal.material_policy_id,
            "material_valid_strict_episode_count": (
                signal.material_valid_strict_episode_count
            ),
            "material_detection_timestamp_count": (
                signal.material_detection_timestamp_count
            ),
            "observed_monotonic_ns": signal.observed_monotonic_ns,
        }

    def _observe_sample_stop(
        self,
        *,
        observed_monotonic_ns: int,
        strict_episode_increment: int = 0,
        eligible_trade_increment: int = 0,
        optimistic_episode_increment: int = 0,
        integrity_reason: str | None = None,
    ) -> dict[str, Any] | None:
        signal = self._sample_stop.observe(
            observed_monotonic_ns=observed_monotonic_ns,
            strict_episode_increment=strict_episode_increment,
            eligible_trade_increment=eligible_trade_increment,
            optimistic_episode_increment=optimistic_episode_increment,
            integrity_reason=integrity_reason,
        )
        if signal is not None:
            self._sample_frozen = True
            self._sample_stop_event.set()
            self._refresh_horizon_drain_event()
        return self._sample_stop_record(signal)

    def _gap_overlaps_pending_episode(
        self,
        gap: DataGapEvidence,
        pending: _PendingEpisode,
    ) -> bool:
        """Check only the venue-local interval a candidate actually uses."""

        version = pending.version
        evidence = pending.would_fill
        if gap.source_venue is Venue.RISEX:
            return gap.matches(
                Venue.RISEX,
                version.canonical_market,
                version.stream_session_id,
                version.recovery_generation,
            ) and gap.overlaps(
                version.quote_created_monotonic_ns,
                evidence.would_fill_detected_monotonic_ns,
            )
        if gap.source_venue is Venue.LIGHTER:
            session = (
                evidence.hedge_stream_session_id
                or version.hedge_stream_session_id
            )
            recovery = (
                evidence.hedge_recovery_generation
                if evidence.hedge_recovery_generation is not None
                else version.hedge_recovery_generation
            )
            if session is None or recovery is None or not gap.matches(
                Venue.LIGHTER,
                evidence.canonical_market,
                session,
                recovery,
            ):
                return False
            return any(
                gap.overlaps(
                    evidence.would_fill_detected_monotonic_ns,
                    evidence.would_fill_detected_monotonic_ns + horizon * 1_000_000,
                )
                for horizon in self.config.horizons_ms
            )
        return False

    def _gap_overlaps_material_candidate(
        self,
        gap: DataGapEvidence,
        candidate: _MaterialCandidate,
    ) -> bool:
        """Apply the same venue-local interval rule to compact identity."""

        if gap.source_venue is Venue.RISEX:
            if candidate.risex_stream_session is None or candidate.risex_recovery is None:
                return False
            return gap.matches(
                Venue.RISEX,
                candidate.market,
                candidate.risex_stream_session,
                candidate.risex_recovery,
            ) and gap.overlaps(candidate.quote_created, candidate.detected)
        if gap.source_venue is Venue.LIGHTER:
            if (
                candidate.hedge_stream_session is None
                or candidate.hedge_recovery is None
            ):
                return False
            if not gap.matches(
                Venue.LIGHTER,
                candidate.market,
                candidate.hedge_stream_session,
                candidate.hedge_recovery,
            ):
                return False
            return any(
                gap.overlaps(
                    candidate.detected,
                    candidate.detected + horizon * 1_000_000,
                )
                for horizon in self.config.horizons_ms
            )
        return False

    def _gap_overlaps_material_candidate_is_clean(
        self,
        candidate: _MaterialCandidate,
    ) -> bool:
        return not any(
            self._gap_overlaps_material_candidate(gap, candidate)
            for gap in self.history.gaps()
        )

    def _pending_is_material_valid(self, pending: _PendingEpisode) -> bool:
        """Return whether all four completed captures are clean evidence."""

        if set(pending.captures) != set(self.config.horizons_ms):
            return False
        if any(
            capture.outcome
            in {
                EntryViabilityOutcome.HEDGE_DATA_GAP,
                EntryViabilityOutcome.HEDGE_OUTCOME_UNKNOWN,
            }
            or capture.gap_evidence is not None
            for capture in pending.captures.values()
        ):
            return False
        return not any(
            self._gap_overlaps_pending_episode(gap, pending)
            for gap in self.history.gaps()
        )

    def _observe_material_stop(
        self,
        *,
        policy_id: str,
        pending: _PendingEpisode,
    ) -> None:
        if not self.material_stop_enabled:
            return
        if pending.version.direction not in (
            SpreadDirection.RISEX_BUY_LIGHTER_SELL,
            SpreadDirection.RISEX_SELL_LIGHTER_BUY,
        ):
            return
        if not self._pending_is_material_valid(pending):
            return
        version_id = pending.version.version_id
        self._material_candidates[version_id] = (
            policy_id,
            _MaterialCandidate(
                version_id=version_id,
                market=pending.version.canonical_market,
                quote_created=pending.version.quote_created_monotonic_ns,
                detected=pending.would_fill.would_fill_detected_monotonic_ns,
                risex_stream_session=pending.version.stream_session_id,
                risex_recovery=pending.version.recovery_generation,
                hedge_stream_session=(
                    pending.would_fill.hedge_stream_session_id
                    or pending.version.hedge_stream_session_id
                ),
                hedge_recovery=(
                    pending.would_fill.hedge_recovery_generation
                    if pending.would_fill.hedge_recovery_generation is not None
                    else pending.version.hedge_recovery_generation
                ),
            ),
        )
        self._schedule_material_stop_boundary()

    async def trigger_wall_clock_stop(self) -> None:
        """Persist the wall-clock stop even when the public feed is silent."""

        observed = max(self._sample_stop.started_monotonic_ns, self._monotonic_ns())
        stop_record = self._observe_sample_stop(observed_monotonic_ns=observed)
        if stop_record is not None:
            await self._append((stop_record,))
        self._refresh_horizon_drain_event()

    async def wait_for_wall_clock_stop(self) -> None:
        deadline = (
            self._sample_stop.started_monotonic_ns
            + self._sample_stop.wall_clock_limit_ns
        )
        delay_ns = deadline - self._monotonic_ns()
        if delay_ns > 0:
            timer = asyncio.create_task(asyncio.sleep(delay_ns / 1_000_000_000))
            already_stopped = asyncio.create_task(self._sample_stop_event.wait())
            done, _ = await asyncio.wait(
                (timer, already_stopped),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if already_stopped in done:
                timer.cancel()
                await asyncio.gather(timer, return_exceptions=True)
                return
            already_stopped.cancel()
            await asyncio.gather(already_stopped, return_exceptions=True)
        await self.trigger_wall_clock_stop()

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
        risex_state_sha256 = self._book_encoder.state_sha256_for(risex)
        lighter_state_sha256 = self._book_encoder.state_sha256_for(lighter)
        for direction in self.directions:
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
                    # This is the actual offline decision boundary: the
                    # clock is sampled only after the production quote
                    # calculation returns.  Source-book normalization stays
                    # on the BOOK witness and is never inferred from this
                    # boundary.
                    decision_ready_ns = self._monotonic_ns()
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
                            risex_book_revision=risex.book_revision,
                            lighter_book_revision=lighter.book_revision,
                            risex_book_revision_id=risex.book_revision_id,
                            lighter_book_revision_id=lighter.book_revision_id,
                            decision_ready_monotonic_ns=decision_ready_ns,
                        )
                        versions[policy_key] = version
                    records.append(
                        self._quote_record(
                            policy,
                            quote,
                            version=version,
                            created_ns=created_ns,
                            risex_book=risex,
                            lighter_book=lighter,
                            risex_state_sha256=risex_state_sha256,
                            lighter_state_sha256=lighter_state_sha256,
                        )
                    )
        return records, versions

    async def handle_book(self, event: FeedBookEvent) -> None:
        book = event.book
        if self._sample_frozen and book.venue is not Venue.LIGHTER:
            return
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
                self.fatal_reason = "BOOK_REVISION_CHAIN_INVALID"
                await self.handle_gap(
                    FeedGapEvent(
                        DataGapEvidence(
                            source_venue=book.venue,
                            canonical_market=book.canonical_market,
                            stream_session_id=book.stream_session_id,
                            recovery_generation=book.recovery_generation,
                            gap_start_monotonic_ns=book.received_monotonic_ns,
                            reason="BOOK_REVISION_CHAIN_INVALID",
                        )
                    )
                )
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
            book_record = self._book_record(event)
        except BookRevisionChainError:
            # A feed event that cannot bind to the immediately preceding
            # persisted revision is not safely representable.  Preserve the
            # named gap and stop the sample rather than writing a guessed
            # delta or silently falling back to a repeated full book.
            self.fatal_reason = "BOOK_REVISION_CHAIN_INVALID"
            await self.handle_gap(
                FeedGapEvent(
                    DataGapEvidence(
                        source_venue=book.venue,
                        canonical_market=book.canonical_market,
                        stream_session_id=book.stream_session_id,
                        recovery_generation=book.recovery_generation,
                        gap_start_monotonic_ns=book.received_monotonic_ns,
                        reason="BOOK_REVISION_CHAIN_INVALID",
                    )
                )
            )
            return
        try:
            self.history.add_book(book, source_kind=event.source_kind)
        except HistoryCapacityExceeded as exc:
            self.fatal_reason = "BOOK_HISTORY_CAPACITY"
            await self.handle_gap(FeedGapEvent(exc.gap))
            return
        except (TypeError, ValueError):
            self.fatal_reason = "BOOK_REVISION_CHAIN_INVALID"
            await self.handle_gap(
                FeedGapEvent(
                    DataGapEvidence(
                        source_venue=book.venue,
                        canonical_market=book.canonical_market,
                        stream_session_id=book.stream_session_id,
                        recovery_generation=book.recovery_generation,
                        gap_start_monotonic_ns=book.received_monotonic_ns,
                        reason="BOOK_REVISION_CHAIN_INVALID",
                    )
                )
            )
            return
        self._current_books[(book.venue, book.canonical_market)] = book
        if self._sample_frozen:
            await self._append((book_record,))
            self._refresh_horizon_drain_event()
            return
        records, versions = self._versions_for_book(event)
        for policy_key in set(self._active_versions) - set(versions):
            if policy_key.startswith(f"{book.canonical_market}|"):
                self._active_versions.pop(policy_key, None)
        self._active_versions.update(versions)
        output_records: list[Mapping[str, Any]] = [book_record, *records]
        stop_record = self._observe_sample_stop(
            observed_monotonic_ns=book.received_monotonic_ns,
            integrity_reason=self.fatal_reason,
        )
        if stop_record is not None:
            output_records.append(stop_record)
        await self._append(output_records)
        self._prune_state(book.received_monotonic_ns)

    async def handle_gap(self, event: FeedGapEvent) -> None:
        gap = event.gap
        if gap.transport_event == "UNEXPECTED_FAILURE" or gap.reason == "PUBLIC_SOCKET_TRANSPORT_FAILURE":
            # A graceful CLOSE is a bounded data gap.  An unexpected socket
            # failure is a terminal measurement-integrity condition even when
            # it arrived as an otherwise well-formed gap event.
            self.fatal_reason = "PUBLIC_SOCKET_TRANSPORT_FAILURE"
        if self._sample_frozen and gap.source_venue is not Venue.LIGHTER:
            return
        stream_key = (gap.source_venue, gap.canonical_market)
        self._gapped_identities[stream_key] = (
            gap.stream_session_id,
            gap.recovery_generation,
        )
        self._awaiting_fresh_snapshot.add(stream_key)
        self.history.add_gap(gap)
        self._current_books.pop((gap.source_venue, gap.canonical_market), None)
        for version_id, (policy_id, candidate) in tuple(self._material_candidates.items()):
            if self._gap_overlaps_material_candidate(gap, candidate):
                self._sample_stop.invalidate_material(
                    policy_id=policy_id,
                    episode_id=version_id,
                )
                self._material_candidates.pop(version_id, None)
        for policy_key in tuple(self._active_versions):
            if policy_key.startswith(f"{gap.canonical_market}|"):
                self._active_versions.pop(policy_key, None)
        output_records: list[Mapping[str, Any]] = [self._gap_record(gap)]
        stop_record = self._observe_sample_stop(
            observed_monotonic_ns=max(
                self._sample_stop.started_monotonic_ns,
                gap.gap_start_monotonic_ns,
            ),
            integrity_reason=self.fatal_reason,
        )
        if stop_record is not None:
            output_records.append(stop_record)
        await self._append(output_records)

    def _trade_record(
        self,
        event: FeedTradeEvent,
        *,
        eligible: bool = False,
        eligible_policy_ids: Sequence[str] = (),
    ) -> dict[str, Any]:
        trade = event.trade
        record = {
            "kind": "RISEX_TRADE",
            "canonical_market": trade.canonical_market,
            "venue": trade.venue.value,
            "trade_event_key": trade.trade_event_key,
            "venue_symbol": trade.venue_symbol,
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
            "eligible_trade": eligible,
            "eligible_policy_ids": tuple(eligible_policy_ids),
            "observed_monotonic_ns": trade.received_monotonic_ns,
        }
        for name in (
            "ingress_received_monotonic_ns",
            "normalized_ready_monotonic_ns",
            "decision_ready_monotonic_ns",
            "source_trade_id",
            "maker_order_id",
            "taker_order_id",
            "maker",
            "taker",
            "tx_hash",
            "block_number",
            "log_index",
            "worker_timestamp",
        ):
            value = getattr(trade, name)
            if value is not None:
                record[name] = value
        return record

    @staticmethod
    def _trade_semantic_fingerprint(trade: TradeEvidence) -> tuple[Any, ...]:
        """Return trade meaning while excluding receipt/session metadata."""

        return (
            trade.venue,
            trade.canonical_market,
            trade.venue_symbol,
            trade.canonical_price,
            trade.canonical_quantity,
            trade.aggressor_side,
            trade.exchange_event_utc,
            trade.exchange_event_time_provenance,
        )

    async def _trade_identity_failure(
        self,
        trade: TradeEvidence,
        *,
        reason: str,
    ) -> None:
        self.fatal_reason = reason
        await self.handle_gap(
            FeedGapEvent(
                DataGapEvidence(
                    source_venue=Venue.RISEX,
                    canonical_market=trade.canonical_market,
                    stream_session_id=trade.stream_session_id,
                    recovery_generation=trade.recovery_generation,
                    gap_start_monotonic_ns=trade.received_monotonic_ns,
                    reason=reason,
                )
            )
        )

    def _remember_trade_identity(
        self,
        trade: TradeEvidence,
    ) -> str:
        """Classify one run-wide key without evicting prior identities."""

        fingerprint = self._trade_semantic_fingerprint(trade)
        key = trade.trade_event_key
        existing = self._trade_identities.get(key)
        if existing is not None:
            return "DUPLICATE" if existing == fingerprint else "CONFLICT"
        if len(self._trade_identities) >= self._trade_identity_capacity:
            return "CAPACITY"
        self._trade_identities[key] = fingerprint
        return "NEW"

    def _would_fill_record(
        self,
        evidence: WouldFillEvidence,
        version: QuoteVersion | None = None,
    ) -> dict[str, Any]:
        record = {
            "kind": "WOULD_FILL",
            "canonical_market": evidence.canonical_market,
            "venue": evidence.venue.value,
            "quote_version_id": evidence.quote_version_id,
            "direction": evidence.direction.value,
            "canonical_quantity": evidence.canonical_quantity,
            "cumulative_eligible_quantity": evidence.cumulative_eligible_quantity,
            "qualifying_trade_event_keys": evidence.qualifying_trade_event_keys,
            "fillability_model": evidence.fillability_model.value,
            "would_fill_detected_monotonic_ns": evidence.would_fill_detected_monotonic_ns,
            "detected_utc": evidence.detected_utc,
            "hedge_stream_session_id": evidence.hedge_stream_session_id,
            "hedge_recovery_generation": evidence.hedge_recovery_generation,
            "observed_monotonic_ns": evidence.would_fill_detected_monotonic_ns,
        }
        if version is not None:
            record.update(
                {
                    "policy_id": self.policy_id(version.quote.policy),
                    "quote_created_monotonic_ns": version.quote_created_monotonic_ns,
                    "quote_stream_session_id": version.stream_session_id,
                    "quote_recovery_generation": version.recovery_generation,
                    "maker_price": version.quote.maker_price,
                    "quote_canonical_quantity": version.quote.canonical_quantity,
                    "post_only_bound_price": version.quote.post_only_bound_price,
                    "risex_tick_size": version.quote.risex_tick_size,
                }
            )
        return record

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
            "fillability_model": capture.fillability_model.value,
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
            "book_revision_id": capture.book_revision_id,
            "book_state_sha256": (
                None if book is None else book_state_sha256(book)
            ),
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
        if self._sample_frozen:
            if (
                self.enforce_sample_deadline
                and self.sample_stop_signal is not None
                and self.sample_stop_signal.reason is SampleStopReason.WALL_CLOCK_LIMIT
                and trade.received_monotonic_ns
                < self.sample_started_monotonic_ns + self.config.sample_wall_clock_limit_ns
            ):
                self.fatal_reason = self.fatal_reason or "PRE_CUTOFF_INGRESS_AFTER_STOP"
                await self._append(
                    (
                        {
                            "kind": "SCANNER_CUTOFF_VIOLATION",
                            "canonical_market": trade.canonical_market,
                            "trade_event_key": trade.trade_event_key,
                            "reason": "PRE_CUTOFF_INGRESS_AFTER_STOP",
                            "observed_monotonic_ns": trade.received_monotonic_ns,
                        },
                    )
                )
            return
        if (
            self.enforce_sample_deadline
            and trade.received_monotonic_ns
            >= self.sample_started_monotonic_ns + self.config.sample_wall_clock_limit_ns
        ):
            stop_record = self._observe_sample_stop(
                observed_monotonic_ns=trade.received_monotonic_ns,
            )
            if stop_record is not None:
                await self._append((stop_record,))
            return
        if not self._trade_admissible(event):
            return
        identity_status = self._remember_trade_identity(trade)
        if identity_status == "DUPLICATE":
            return
        if identity_status == "CONFLICT":
            await self._trade_identity_failure(
                trade,
                reason="TRADE_DEDUP_CONFLICT",
            )
            return
        if identity_status == "CAPACITY":
            await self._trade_identity_failure(
                trade,
                reason="TRADE_DEDUP_CAPACITY",
            )
            return
        market_trades = self._trades.setdefault(trade.canonical_market, {})
        existing = market_trades.get(trade.trade_event_key)
        if existing is not None:
            if self._trade_semantic_fingerprint(existing) == self._trade_semantic_fingerprint(trade):
                return
            await self._trade_identity_failure(
                trade,
                reason="TRADE_DEDUP_CONFLICT",
            )
            return
        market_trades[trade.trade_event_key] = trade
        relevant_versions = tuple(
            (policy_key, version)
            for policy_key, version in self._active_versions.items()
            if version.canonical_market == trade.canonical_market
            and is_eligible_trade(version, trade)
        )
        records: list[Mapping[str, Any]] = [
            self._trade_record(
                event,
                eligible=bool(relevant_versions),
                eligible_policy_ids=tuple(sorted(policy_key for policy_key, _ in relevant_versions)),
            )
        ]
        strict_increment = 0
        optimistic_increment = 0
        for model, detector in (
            (FillabilityModel.STRICT_LOWER_BOUND, detect_strict_would_fill),
            (FillabilityModel.OPTIMISTIC_UPPER_BOUND, detect_optimistic_would_fill),
        ):
            pending_by_version = self._pending_for_model(model)
            for _policy_key, version in tuple(self._active_versions.items()):
                if version.canonical_market != trade.canonical_market:
                    continue
                if version.version_id in pending_by_version:
                    continue
                if self._last_episode_versions.get((model, _policy_key)) == version.version_id:
                    continue
                evidence = detector(
                    version,
                    tuple(market_trades.values()),
                    data_gaps=self.history.gaps(),
                    would_fill_detected_monotonic_ns=trade.received_monotonic_ns,
                    detected_utc=trade.received_utc,
                )
                if evidence is None:
                    continue
                pending = _PendingEpisode(version, evidence)
                pending_by_version[version.version_id] = pending
                self._last_episode_versions[(model, _policy_key)] = version.version_id
                latest_deadline = max(
                    evidence.would_fill_detected_monotonic_ns
                    + horizon * 1_000_000
                    for horizon in self.config.horizons_ms
                )
                self.history.register_pending(
                    self._pending_key(model, version.version_id),
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
                records.append(self._would_fill_record(evidence, version))
                if model is FillabilityModel.STRICT_LOWER_BOUND:
                    strict_increment += 1
                else:
                    optimistic_increment += 1
                if not self._replay_mode:
                    for horizon in self.config.horizons_ms:
                        task = asyncio.create_task(
                            self._capture_later(
                                version.version_id,
                                horizon,
                                model=model,
                            )
                        )
                        self._tasks.add(task)
                        task.add_done_callback(self._capture_task_done)
        stop_record = self._observe_sample_stop(
            observed_monotonic_ns=trade.received_monotonic_ns,
            strict_episode_increment=strict_increment,
            eligible_trade_increment=1 if relevant_versions else 0,
            optimistic_episode_increment=optimistic_increment,
            integrity_reason=self.fatal_reason,
        )
        if stop_record is not None:
            records.append(stop_record)
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
                await self._finalize_material_stop(
                    observed_monotonic_ns=max(
                        self._sample_stop.started_monotonic_ns,
                        self._monotonic_ns(),
                    )
                )
                return
            self._handling_item += 1
            succeeded = False
            try:
                await self.handle_item(item)
                succeeded = True
            finally:
                self._handling_item -= 1
                self.ingress.complete_item(success=succeeded)
            self._schedule_material_stop_boundary()

    async def _capture_one(
        self,
        version_id: str,
        horizon: int,
        *,
        force: bool,
        model: FillabilityModel = FillabilityModel.STRICT_LOWER_BOUND,
    ) -> None:
        pending_by_version = self._pending_for_model(model)
        pending = pending_by_version.get(version_id)
        if pending is None or horizon in pending.captures:
            return
        deadline = (
            pending.would_fill.would_fill_detected_monotonic_ns
            + horizon * 1_000_000
        )
        if not await self._await_horizon_ingress(deadline):
            return
        if self._append_failure is not None or self._append_cancelled:
            return
        # A timer capture and the final drain can wait on the same ingress
        # watermark.  Revalidate after that wait before claiming the slot so
        # exactly one path can persist a given model/version/horizon.
        pending = pending_by_version.get(version_id)
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
                self.history.latest_books(
                    Venue.LIGHTER, evidence.canonical_market, deadline
                ),
                horizon_ms=horizon,
                expected_stream_session_id=evidence.hedge_stream_session_id
                or pending.version.hedge_stream_session_id,
                expected_recovery_generation=evidence.hedge_recovery_generation
                if evidence.hedge_recovery_generation is not None
                else pending.version.hedge_recovery_generation,
                data_gaps=gaps,
                freshness_max_age_ns=self.config.freshness_max_age_ns,
                fillability_model=model,
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
                fillability_model=model,
            )
        pending.captures[horizon] = capture
        await self._append((self._horizon_record(pending.version, capture),))
        if len(pending.captures) == len(self.config.horizons_ms):
            if model is FillabilityModel.STRICT_LOWER_BOUND:
                self._observe_material_stop(
                    policy_id=self.policy_id(pending.version.quote.policy),
                    pending=pending,
                )
            pending_by_version.pop(version_id, None)
            self.history.complete(self._pending_key(model, version_id), self._monotonic_ns())
            self._prune_state(self._monotonic_ns())
            self._refresh_horizon_drain_event()

    async def _await_horizon_ingress(self, deadline: int) -> bool:
        """Bound the receipt/processing barrier and fail closed on no ack."""

        try:
            ready = await asyncio.wait_for(
                self.ingress.wait_for_lighter_before(deadline),
                timeout=_HORIZON_INGRESS_BARRIER_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            self.fatal_reason = "HORIZON_INGRESS_BARRIER_TIMEOUT"
            ready = False
        if not ready:
            if self.fatal_reason is None:
                self.fatal_reason = "HORIZON_INGRESS_CONSUMER_FAILED"
            stop_record = self._observe_sample_stop(
                observed_monotonic_ns=max(
                    self._sample_stop.started_monotonic_ns,
                    self._monotonic_ns(),
                ),
                integrity_reason=self.fatal_reason,
            )
            if stop_record is not None:
                await self._append((stop_record,))
            return False
        return True

    async def _capture_later(
        self,
        version_id: str,
        horizon: int,
        *,
        model: FillabilityModel = FillabilityModel.STRICT_LOWER_BOUND,
    ) -> None:
        pending = self._pending_for_model(model).get(version_id)
        if pending is None:
            return
        deadline = pending.would_fill.would_fill_detected_monotonic_ns + horizon * 1_000_000
        delay_ns = deadline - self._monotonic_ns()
        if delay_ns > 0:
            await asyncio.sleep(delay_ns / 1_000_000_000)
        await asyncio.sleep(0)
        await self._capture_one(version_id, horizon, force=False, model=model)

    async def flush_pending(self, *, force: bool = False) -> None:
        if force:
            tasks = tuple(self._tasks)
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self._tasks.clear()
        if not self._pending and not self._optimistic_pending:
            self._refresh_horizon_drain_event()
            return
        if not force:
            deadline = self._monotonic_ns() + 1_200_000_000
            while (self._pending or self._optimistic_pending) and self._tasks and self._monotonic_ns() < deadline:
                await asyncio.sleep(0.02)
        for model in (
            FillabilityModel.STRICT_LOWER_BOUND,
            FillabilityModel.OPTIMISTIC_UPPER_BOUND,
        ):
            for version_id, pending in tuple(self._pending_for_model(model).items()):
                for horizon in self.config.horizons_ms:
                    await self._capture_one(
                        version_id,
                        horizon,
                        force=force,
                        model=model,
                    )
        # Horizon captures append through the same bounded writer as feed
        # events.  Force/replay flushes are durable before returning, while a
        # normal drain still uses the configured count/interval path and then
        # performs one final sync for any short tail.
        await self._flush_pending_batch()
        self._refresh_horizon_drain_event()

    async def _stop_batch_loop(self) -> None:
        """Stop the interval flusher only after its worker is quiescent."""

        self._batch_stop_event.set()
        self._batch_wakeup.set()
        task = self._batch_loop_task
        if task is None or task is asyncio.current_task():
            return
        was_cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                # Keep waiting for the loop's current append.  Re-raise only
                # after the physical worker has stopped so a caller cannot
                # start a direct terminal append concurrently.
                was_cancelled = True
            except BaseException:
                break
        await asyncio.gather(task, return_exceptions=True)
        if self._batch_loop_task is task:
            self._batch_loop_task = None
        if was_cancelled:
            raise asyncio.CancelledError

    async def _wait_append_quiescent(self) -> None:
        """Wait for any physical append even when its owner task is gone."""

        operation = self._append_operation
        if operation is None:
            return
        try:
            await self._wait_store_operation(operation)
        except Exception as exc:
            if self._append_failure is None:
                self._register_append_failure(exc)
            raise

    async def _wait_terminal_completion(self) -> None:
        """Keep store shutdown behind a terminal call running in another task."""

        if not self._terminal_started or self._terminal_written:
            return
        was_cancelled = False
        while not self._terminal_complete_event.is_set():
            try:
                await asyncio.shield(self._terminal_complete_event.wait())
            except asyncio.CancelledError:
                was_cancelled = True
        if was_cancelled:
            raise asyncio.CancelledError
        if self._terminal_error is not None:
            raise self._terminal_error

    async def _acquire_append_lock_uncancelled(self) -> bool:
        """Acquire the append lock without abandoning a waiter on cancel."""

        waiter = asyncio.create_task(self._append_lock.acquire())
        was_cancelled = False
        while True:
            try:
                await asyncio.shield(waiter)
                return was_cancelled
            except asyncio.CancelledError:
                was_cancelled = True

    async def _cancel_capture_tasks_and_wait(self) -> None:
        """Cancel capture tasks and wait for their append work to quiesce."""

        tasks = {task for task in self._tasks if task is not asyncio.current_task()}
        for task in tasks:
            task.cancel()
        was_cancelled = False
        pending = tasks
        while pending:
            try:
                _done, pending = await asyncio.wait(pending)
            except asyncio.CancelledError:
                # The child tasks may be unwinding a shielded thread append;
                # do not let caller cancellation make store.close race them.
                was_cancelled = True
        self._tasks.difference_update(tasks)
        if was_cancelled:
            raise asyncio.CancelledError

    async def _cancel_material_boundary_and_wait(self) -> None:
        """Do not leave a deferred material-stop task behind on shutdown."""

        task = self._material_boundary_task
        if task is None or task is asyncio.current_task():
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        if self._material_boundary_task is task:
            self._material_boundary_task = None

    async def append_terminal(self, record: Mapping[str, Any]) -> None:
        """Append the sole terminal marker after every prior append is done."""

        if not isinstance(record, Mapping):
            raise TypeError("terminal evidence must be a mapping")
        kind = record.get("kind")
        if kind not in _TERMINAL_KINDS:
            raise ValueError("terminal evidence must be RUN_STOP or RUN_FAILED")

        cancelled_while_claiming = await self._acquire_append_lock_uncancelled()
        try:
            if self._terminal_written:
                if cancelled_while_claiming:
                    raise asyncio.CancelledError
                return
            if self._terminal_started:
                if self._terminal_error is not None:
                    raise self._terminal_error
                raise RuntimeError("terminal evidence append has already started")
            self._terminal_started = True
            self._closing = True
            self._batch_stop_event.set()
            self._batch_wakeup.set()
        finally:
            self._append_lock.release()

        try:
            await self._cancel_capture_tasks_and_wait()
            await self._cancel_material_boundary_and_wait()
            await self._stop_batch_loop()
        except BaseException as exc:
            self._terminal_error = exc
            self._closed_for_append = True
            self._active_versions.clear()
            self._terminal_complete_event.set()
            raise

        terminal_operation_started = False
        cancelled_while_writing = await self._acquire_append_lock_uncancelled()
        try:
            try:
                # A known pre-append cap failure still permits the reserved
                # RUN_FAILED marker.  Ambiguous physical write failures do
                # not permit any retry.
                await self._flush_pending_batch_locked(allow_failure=True)
                terminal_operation_started = True
                await self._store_append((dict(record),), terminal=True)
            except asyncio.CancelledError:
                if terminal_operation_started and self._last_append_succeeded:
                    self._terminal_written = True
                else:
                    self._terminal_error = asyncio.CancelledError()
                self._closed_for_append = True
                self._active_versions.clear()
                self._terminal_complete_event.set()
                raise
            except BaseException as exc:
                self._terminal_error = exc
                self._closed_for_append = True
                self._active_versions.clear()
                self._terminal_complete_event.set()
                raise
            else:
                self._terminal_written = True
                self._closed_for_append = True
                self._active_versions.clear()
                self._terminal_complete_event.set()
        finally:
            self._append_lock.release()
        if cancelled_while_claiming or cancelled_while_writing:
            raise asyncio.CancelledError

    async def close(self) -> None:
        await self._wait_terminal_completion()
        if self._closed_for_append:
            if self._append_failure is not None:
                raise self._append_failure
            if self._terminal_error is not None:
                raise self._terminal_error
            self.ingress.close()
            return
        self._closing = True
        failure: BaseException | None = None
        try:
            await self.flush_pending(force=False)
            await self._cancel_capture_tasks_and_wait()
            await self._cancel_material_boundary_and_wait()
        except BaseException as exc:
            failure = exc
        finally:
            try:
                await self._cancel_capture_tasks_and_wait()
            except BaseException as exc:
                if failure is None:
                    failure = exc
            try:
                await self._cancel_material_boundary_and_wait()
            except BaseException as exc:
                if failure is None:
                    failure = exc
            # Stop the interval flusher only after horizon tasks have drained
            # so no producer can race the final durable append.  A sleeping
            # loop exits promptly after the event is set; a write already in
            # progress is awaited rather than cancelled (cancelling a
            # thread-backed fsync could overlap a subsequent append).
            try:
                await self._stop_batch_loop()
            except BaseException as exc:
                if failure is None:
                    failure = exc
            try:
                await self._wait_append_quiescent()
            except BaseException as exc:
                if failure is None:
                    failure = exc
            if failure is None:
                try:
                    await self._flush_pending_batch()
                except BaseException as exc:
                    failure = exc
            self.ingress.close()
            self._active_versions.clear()
            self._closed_for_append = True
            self._refresh_horizon_drain_event()
        if failure is not None:
            raise failure

    def _prune_state(self, now_ns: int) -> None:
        floor = max(0, now_ns - self.config.trade_retention_ns)
        active_starts = tuple(
            version.quote_created_monotonic_ns
            for version in self._active_versions.values()
        )
        if active_starts:
            floor = max(floor, min(active_starts))

        def relevant_to_active_quote(trade: TradeEvidence) -> bool:
            return any(
                is_eligible_trade(version, trade)
                for version in self._active_versions.values()
            )

        for market, trades in tuple(self._trades.items()):
            retained = {
                key: trade
                for key, trade in trades.items()
                if trade.received_monotonic_ns >= floor
                or relevant_to_active_quote(trade)
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
            self._observe_sample_stop(
                observed_monotonic_ns=max(
                    self._sample_stop.started_monotonic_ns,
                    self._monotonic_ns(),
                ),
                integrity_reason=self.fatal_reason,
            )
        self._refresh_horizon_drain_event()


class SpreadShadowRunner:
    """Connect the limited feed and observer for one bounded smoke."""

    def __init__(self, feed: PublicFeedRunner, observer: SpreadObserver) -> None:
        if feed.ingress is not observer.ingress:
            raise ValueError("feed and observer must share one ingress queue")
        self.feed = feed
        self.observer = observer

    @staticmethod
    def _consume_task_result(task: asyncio.Task[Any]) -> None:
        try:
            task.exception()
        except BaseException:
            pass

    async def _await_shutdown_task(
        self,
        task: asyncio.Task[Any],
        *,
        failure_reason: str,
    ) -> None:
        try:
            await asyncio.wait_for(
                asyncio.shield(task), timeout=_SHUTDOWN_TIMEOUT_SECONDS
            )
        except asyncio.TimeoutError:
            task.cancel()
            task.add_done_callback(self._consume_task_result)
            if self.observer.fatal_reason is None:
                self.observer.fatal_reason = failure_reason
            raise

    async def run(self, *, duration_seconds: int | None = None) -> None:
        consumer = asyncio.create_task(self.observer.consume())
        sample_stop_event = getattr(self.observer, "sample_stop_event", None)
        horizon_drain_event = getattr(self.observer, "horizon_drain_event", None)
        wall_clock_task: asyncio.Task[Any] | None = None
        wait_for_wall_clock_stop = getattr(self.observer, "wait_for_wall_clock_stop", None)
        trigger_wall_clock_stop = getattr(self.observer, "trigger_wall_clock_stop", None)
        sample_started = getattr(self.observer, "sample_started_monotonic_ns", None)
        wall_clock_enabled = (
            sample_stop_event is not None
            and wait_for_wall_clock_stop is not None
            and isinstance(sample_started, int)
            and sample_started > 0
        )
        if wall_clock_enabled:
            wall_clock_task = asyncio.create_task(wait_for_wall_clock_stop())
        failure: BaseException | None = None
        try:
            feed_kwargs: dict[str, Any] = {"duration_seconds": duration_seconds}
            if sample_stop_event is not None:
                feed_kwargs["stop_event"] = sample_stop_event
            if horizon_drain_event is not None:
                feed_kwargs["drain_event"] = horizon_drain_event
            await self.feed.run(**feed_kwargs)
            if (
                sample_stop_event is not None
                and not sample_stop_event.is_set()
                and wall_clock_enabled
                and time.monotonic_ns()
                >= self.observer.sample_started_monotonic_ns
                + self.observer.config.sample_wall_clock_limit_ns
                and trigger_wall_clock_stop is not None
            ):
                await trigger_wall_clock_stop()
        except BaseException as exc:
            failure = exc
        finally:
            if wall_clock_task is not None:
                wall_signal = getattr(self.observer, "sample_stop_signal", None)
                if (
                    not wall_clock_task.done()
                    and wall_signal is not None
                    and wall_signal.reason is SampleStopReason.WALL_CLOCK_LIMIT
                ):
                    # The timer has latched the wall stop and may still be
                    # persisting its marker.  Let that append finish before
                    # closing the ingress/store.
                    await asyncio.gather(wall_clock_task, return_exceptions=True)
                elif not wall_clock_task.done():
                    wall_clock_task.cancel()
                    await asyncio.gather(wall_clock_task, return_exceptions=True)
                else:
                    await asyncio.gather(wall_clock_task, return_exceptions=True)
                if failure is None and not wall_clock_task.cancelled():
                    try:
                        wall_failure = wall_clock_task.exception()
                    except BaseException:
                        wall_failure = None
                    if wall_failure is not None:
                        failure = wall_failure
            try:
                self.feed.ingress.close()
            except BaseException as exc:
                if failure is None:
                    failure = exc
            try:
                await self._await_shutdown_task(
                    consumer,
                    failure_reason="INGRESS_DRAIN_TIMEOUT",
                )
            except BaseException as exc:
                if failure is None:
                    failure = exc
            if self.feed.ingress.has_pending and self.observer.fatal_reason is None:
                self.observer.fatal_reason = "INGRESS_DRAIN_INCOMPLETE"
                if failure is None:
                    failure = RuntimeError("public feed ingress did not drain")
            try:
                observer_close = asyncio.create_task(self.observer.close())
                await self._await_shutdown_task(
                    observer_close,
                    failure_reason="OBSERVER_SHUTDOWN_TIMEOUT",
                )
            except BaseException as exc:
                if failure is None:
                    failure = exc
        if failure is not None:
            raise failure


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
        await self.observer._finalize_material_stop(
            observed_monotonic_ns=max(
                self.observer.sample_started_monotonic_ns,
                self.observer._monotonic_ns(),
            )
        )
        await self.observer.append_terminal(
            {
                "kind": "RUN_STOP",
                "fatal_reason": None,
                "observed_monotonic_ns": 0,
            }
        )


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
        "fillability_models": tuple(model.value for model in FillabilityModel),
        "strict_episode_limit": config.strict_episode_limit,
        "material_valid_strict_episode_limit": _MATERIAL_VALID_STRICT_EPISODE_LIMIT,
        "material_detection_timestamp_limit": _MATERIAL_DETECTION_TIMESTAMP_LIMIT,
        "eligible_trade_limit": config.eligible_trade_limit,
        "sample_wall_clock_seconds": config.sample_wall_clock_seconds,
        "freshness_max_age_ns": config.freshness_max_age_ns,
        "quote_lifetime_ns": config.quote_lifetime_ns,
        "risex_maker_fee_rate": config.risex_maker_fee_rate,
        "lighter_taker_fee_rate": config.lighter_taker_fee_rate,
        "risex_fee_source": config.risex_fee_source,
        "lighter_fee_source": config.lighter_fee_source,
        "created_utc": started_utc,
    }
    store = AppendOnlyEvidenceStore.create(store_root, metadata=metadata)
    observer: SpreadObserver | None = None
    feed: PublicFeedRunner | None = None
    terminal_written = False
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
            observer = SpreadObserver(
                config,
                pairs,
                store,
                sample_started_monotonic_ns=time.monotonic_ns(),
            )
            await observer._append(
                ({
                    "kind": "RUN_START",
                    "markets": tuple(pair.canonical_market for pair in pairs),
                    "duration_seconds": config.duration_seconds if duration_seconds is None else duration_seconds,
                    "started_utc": started_utc,
                    "observed_monotonic_ns": 0,
                },)
            )
            await observer.flush_pending()
            feed = PublicFeedRunner(
                session,
                pairs,
                observer.ingress,
                config=config,
                risex_adapter=risex,
                lighter_adapter=lighter,
            )
            await SpreadShadowRunner(feed, observer).run(duration_seconds=duration_seconds)
            fatal_reason = feed.fatal_reason or observer.fatal_reason
            if fatal_reason is None:
                await observer.append_terminal(
                    {
                        "kind": "RUN_STOP",
                        "fatal_reason": None,
                        "stopped_utc": datetime.now(UTC),
                        "observed_monotonic_ns": time.monotonic_ns(),
                    }
                )
            else:
                await observer.append_terminal(
                    {
                        "kind": "RUN_FAILED",
                        "failure_class": "FATAL_RUNTIME",
                        "fatal_reason": fatal_reason,
                        "failed_utc": datetime.now(UTC),
                        "observed_monotonic_ns": time.monotonic_ns(),
                    }
                )
            terminal_written = True
            result = {
                "run_id": store.run_id,
                "store_path": str(store.path),
                "markets": tuple(pair.canonical_market for pair in pairs),
                "duration_seconds": config.duration_seconds if duration_seconds is None else duration_seconds,
                "fatal_reason": fatal_reason,
                "record_count": store.record_count,
                "byte_count": store.byte_count,
            }
            return result
    except BaseException as exc:
        if not terminal_written:
            fatal_reason = (
                None if feed is None else feed.fatal_reason
            ) or (None if observer is None else observer.fatal_reason)
            failure_record = {
                "kind": "RUN_FAILED",
                "failure_class": type(exc).__name__,
                "fatal_reason": fatal_reason,
                "failed_utc": datetime.now(UTC),
                "observed_monotonic_ns": time.monotonic_ns(),
            }
            try:
                if observer is None:
                    # Before an observer exists there is no background writer
                    # or buffered append that could race this one safe direct
                    # terminal append (for example, market selection failed).
                    store.append_batch((failure_record,))
                else:
                    await observer.append_terminal(failure_record)
                terminal_written = True
            except BaseException as marker_exc:
                exc.add_note(
                    f"unable to persist RUN_FAILED marker: {type(marker_exc).__name__}"
                )
        raise
    finally:
        store.close()


__all__ = [
    "BookHistory",
    "HistoryCapacityExceeded",
    "ReplayHarness",
    "SampleStopController",
    "SpreadObserver",
    "SpreadShadowRunner",
    "run_public_smoke",
]
