"""Immutable contracts for the pure RISEx/Lighter spread-shadow domain.

This module deliberately contains values and provenance only.  It does not
open connections, persist data, or know anything about the legacy strategy
runtime.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from risex_farmer.models import (
    BookLevel,
    CanonicalMarket,
    ExactVwap,
    LiquidityRole,
    Side,
    Venue,
)


def _decimal(value: Decimal, name: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be Decimal")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    return value


def _non_negative_int(value: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be int")
    if value < 0:
        raise ValueError(f"{name} must be non-negative")
    return value


def _session(value: str | int, name: str = "stream_session_id") -> str | int:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise TypeError(f"{name} must be str or int")
    if isinstance(value, str) and not value:
        raise ValueError(f"{name} must not be empty")
    return value


def _utc(value: datetime | None, name: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be datetime or None")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware UTC provenance")
    if value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must use UTC offset +00:00")
    return value


def _book_levels_well_formed(book: BookEvidence) -> bool:
    for level in (*book.bids, *book.asks):
        if not isinstance(level, BookLevel):
            return False
        if any(
            not isinstance(value, Decimal) or not value.is_finite() or value <= 0
            for value in (level.canonical_price, level.canonical_quantity)
        ):
            return False
    return True


def _market_identities(market: CanonicalMarket) -> tuple[str, ...]:
    """Return accepted canonical and venue-symbol identities without guessing."""

    values = (
        getattr(market, "canonical_market", None),
        getattr(market, "venue_symbol", None),
        getattr(market, "canonical_asset", None),
    )
    identities = tuple(value for value in values if isinstance(value, str) and value)
    if not identities:
        raise ValueError("market must expose a non-empty canonical identity")
    return tuple(dict.fromkeys(identities))


class SpreadDirection(StrEnum):
    """The only two maker/hedge directions in SS-001A."""

    RISEX_BUY_LIGHTER_SELL = "RISEX_BUY_LIGHTER_SELL"
    RISEX_SELL_LIGHTER_BUY = "RISEX_SELL_LIGHTER_BUY"

    # Explicit aliases keep the direction readable at call sites without
    # introducing additional behaviour or a second direction model.
    MAKER_BUY_LIGHTER_SELL = RISEX_BUY_LIGHTER_SELL
    MAKER_SELL_LIGHTER_BUY = RISEX_SELL_LIGHTER_BUY

    @property
    def maker_side(self) -> Side:
        return Side.BUY if self is self.RISEX_BUY_LIGHTER_SELL else Side.SELL

    @property
    def hedge_side(self) -> Side:
        return Side.SELL if self is self.RISEX_BUY_LIGHTER_SELL else Side.BUY


class FillabilityModel(StrEnum):
    """The two explicitly bounded public fillability interpretations."""

    STRICT_LOWER_BOUND = "STRICT_LOWER_BOUND"
    OPTIMISTIC_UPPER_BOUND = "OPTIMISTIC_UPPER_BOUND"

    # Short aliases keep call sites readable without adding another model.
    STRICT = STRICT_LOWER_BOUND
    OPTIMISTIC = OPTIMISTIC_UPPER_BOUND


class SampleStopReason(StrEnum):
    """The bounded prospective sample-stop conditions."""

    STRICT_EPISODE_LIMIT = "STRICT_EPISODE_LIMIT"
    ELIGIBLE_TRADE_LIMIT = "ELIGIBLE_TRADE_LIMIT"
    WALL_CLOCK_LIMIT = "WALL_CLOCK_LIMIT"
    INTEGRITY_FAILURE = "INTEGRITY_FAILURE"

    STRICT_EPISODES = STRICT_EPISODE_LIMIT
    ELIGIBLE_TRADES = ELIGIBLE_TRADE_LIMIT
    WALL_CLOCK = WALL_CLOCK_LIMIT
    INTEGRITY = INTEGRITY_FAILURE


@dataclass(frozen=True, slots=True)
class SampleStopSignal:
    """One latched, deterministic first-stop-wins observation."""

    reason: SampleStopReason
    observed_monotonic_ns: int
    strict_episode_count: int
    eligible_trade_count: int
    optimistic_episode_count: int = 0
    integrity_reason: str | None = None

    def __post_init__(self) -> None:
        reason = self.reason if isinstance(self.reason, SampleStopReason) else SampleStopReason(self.reason)
        object.__setattr__(self, "reason", reason)
        _non_negative_int(self.observed_monotonic_ns, "observed_monotonic_ns")
        for value, name in (
            (self.strict_episode_count, "strict_episode_count"),
            (self.eligible_trade_count, "eligible_trade_count"),
            (self.optimistic_episode_count, "optimistic_episode_count"),
        ):
            _non_negative_int(value, name)
        if self.integrity_reason is not None and not self.integrity_reason:
            raise ValueError("integrity_reason must be non-empty when supplied")
        if reason is SampleStopReason.INTEGRITY_FAILURE and self.integrity_reason is None:
            raise ValueError("integrity stop requires an integrity reason")

    @property
    def stop_reason(self) -> SampleStopReason:
        return self.reason


class EntryViabilityOutcome(StrEnum):
    """Fail-closed quote, fill, and hedge outcomes."""

    QUOTE_NOT_POST_ONLY = "QUOTE_NOT_POST_ONLY"
    QUOTE_NOT_ECONOMIC = "QUOTE_NOT_ECONOMIC"
    QUOTE_ACTIVE = "QUOTE_ACTIVE"
    NO_WOULD_FILL = "NO_WOULD_FILL"
    WOULD_FILL = "WOULD_FILL"
    HEDGE_FULL = "HEDGE_FULL"
    HEDGE_PARTIAL = "HEDGE_PARTIAL"
    HEDGE_DEPTH_UNAVAILABLE = "HEDGE_DEPTH_UNAVAILABLE"
    HEDGE_DATA_MISSING = "HEDGE_DATA_MISSING"
    HEDGE_DATA_STALE = "HEDGE_DATA_STALE"
    HEDGE_SESSION_DISPLACED = "HEDGE_SESSION_DISPLACED"
    HEDGE_DATA_GAP = "HEDGE_DATA_GAP"
    HEDGE_OUTCOME_UNKNOWN = "HEDGE_OUTCOME_UNKNOWN"

    # Some earlier task wording used this descriptive spelling.  It denotes
    # the same exact missing-data outcome, not a new catch-all state.
    HEDGE_DATA_UNAVAILABLE = HEDGE_DATA_MISSING


@dataclass(frozen=True, slots=True)
class QuotePolicy:
    """A deterministic research policy for one target size and direction."""

    canonical_market: str
    direction: SpreadDirection
    target_notional_usd: Decimal
    target_margin_bps: Decimal
    risex_maker_fee_rate: Decimal
    lighter_taker_fee_rate: Decimal
    risex_fee_source: str = "CONFIGURED_RISEX_RESEARCH_INPUT"
    lighter_fee_source: str = "OFFICIAL_LIGHTER_STANDARD_RESEARCH_INPUT"
    risex_market: CanonicalMarket | None = None
    lighter_market: CanonicalMarket | None = None
    risex_best_bid: Decimal | None = None
    risex_best_ask: Decimal | None = None
    risex_tick_size: Decimal | None = None
    fee_observed_or_configured_at: datetime | None = None
    quote_venue: Venue = Venue.RISEX
    hedge_venue: Venue = Venue.LIGHTER

    def __post_init__(self) -> None:
        if not isinstance(self.canonical_market, str) or not self.canonical_market:
            raise ValueError("canonical_market must be a non-empty string")
        direction = self.direction
        if not isinstance(direction, SpreadDirection):
            direction = SpreadDirection(direction)
            object.__setattr__(self, "direction", direction)
        target = _decimal(self.target_notional_usd, "target_notional_usd")
        margin = _decimal(self.target_margin_bps, "target_margin_bps")
        risex_fee = _decimal(self.risex_maker_fee_rate, "risex_maker_fee_rate")
        lighter_fee = _decimal(self.lighter_taker_fee_rate, "lighter_taker_fee_rate")
        if target <= 0:
            raise ValueError("target_notional_usd must be positive")
        if margin < 0:
            raise ValueError("target_margin_bps must be non-negative")
        if risex_fee < 0 or risex_fee >= 1:
            raise ValueError("risex_maker_fee_rate must be in [0, 1)")
        if lighter_fee < 0 or lighter_fee >= 1:
            raise ValueError("lighter_taker_fee_rate must be in [0, 1)")
        if not self.risex_fee_source or not self.lighter_fee_source:
            raise ValueError("fee sources must be non-empty")
        _utc(self.fee_observed_or_configured_at, "fee_observed_or_configured_at")
        for venue, expected, name in (
            (self.quote_venue, Venue.RISEX, "quote_venue"),
            (self.hedge_venue, Venue.LIGHTER, "hedge_venue"),
        ):
            if not isinstance(venue, Venue):
                venue = Venue(venue)
                object.__setattr__(self, name, venue)
            if venue is not expected:
                raise ValueError(f"{name} must be {expected}")
        if self.risex_market is not None:
            if self.risex_market.venue is not Venue.RISEX:
                raise ValueError("risex_market must identify RISEx")
            if self.canonical_market not in _market_identities(self.risex_market):
                raise ValueError("risex_market does not match canonical_market")
        if self.lighter_market is not None:
            if self.lighter_market.venue is not Venue.LIGHTER:
                raise ValueError("lighter_market must identify Lighter")
            if self.canonical_market not in _market_identities(self.lighter_market):
                raise ValueError("lighter_market does not match canonical_market")
        bbo_values = (self.risex_best_bid, self.risex_best_ask, self.risex_tick_size)
        if any(value is not None for value in bbo_values):
            if any(value is None for value in bbo_values):
                raise ValueError("RISEx BBO and tick must be supplied together")
            for value, name in zip(
                bbo_values, ("risex_best_bid", "risex_best_ask", "risex_tick_size")
            ):
                _decimal(value, name)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class FeeEvidence:
    """One exact fee component retained with its configured provenance."""

    venue: Venue
    liquidity_role: LiquidityRole
    fill_notional_usd: Decimal
    fee_base_notional_usd: Decimal
    rate: Decimal
    amount_usd: Decimal
    source: str
    observed_or_configured_at: datetime | None = None

    def __post_init__(self) -> None:
        venue = self.venue if isinstance(self.venue, Venue) else Venue(self.venue)
        role = (
            self.liquidity_role
            if isinstance(self.liquidity_role, LiquidityRole)
            else LiquidityRole(self.liquidity_role)
        )
        object.__setattr__(self, "venue", venue)
        object.__setattr__(self, "liquidity_role", role)
        for value, name in (
            (self.fill_notional_usd, "fill_notional_usd"),
            (self.fee_base_notional_usd, "fee_base_notional_usd"),
            (self.rate, "rate"),
            (self.amount_usd, "amount_usd"),
        ):
            _decimal(value, name)
        if self.fill_notional_usd < 0 or self.fee_base_notional_usd < 0:
            raise ValueError("fee notionals must be non-negative")
        if self.rate < 0:
            raise ValueError("fee rate must be non-negative")
        if self.amount_usd < 0:
            raise ValueError("fee amount must be non-negative")
        if not self.source:
            raise ValueError("fee source must be non-empty")
        _utc(self.observed_or_configured_at, "observed_or_configured_at")


@dataclass(frozen=True, slots=True)
class SizingEvidence:
    """Reproducible quantity-sizing inputs and all venue minimum flags."""

    canonical_market: str
    direction: SpreadDirection
    target_notional_usd: Decimal
    reference_price: Decimal
    risex_validation_price: Decimal
    q_raw: Decimal
    common_quantity_step: Decimal
    floored_quantity: Decimal
    risex_raw_quantity: Decimal
    lighter_raw_quantity: Decimal
    risex_quantity_step_raw: Decimal
    lighter_quantity_step_raw: Decimal
    risex_base_multiplier: Decimal
    lighter_base_multiplier: Decimal
    risex_minimum_quantity_raw: Decimal
    lighter_minimum_quantity_raw: Decimal
    risex_minimum_notional_usd: Decimal
    lighter_minimum_notional_usd: Decimal
    risex_min_quantity_ok: bool
    risex_min_notional_ok: bool
    lighter_min_quantity_ok: bool
    lighter_min_notional_ok: bool
    risex_market: CanonicalMarket | None = None
    lighter_market: CanonicalMarket | None = None

    def __post_init__(self) -> None:
        if not self.canonical_market:
            raise ValueError("canonical_market must be non-empty")
        if not isinstance(self.direction, SpreadDirection):
            object.__setattr__(self, "direction", SpreadDirection(self.direction))
        for value, name in (
            (self.target_notional_usd, "target_notional_usd"),
            (self.reference_price, "reference_price"),
            (self.risex_validation_price, "risex_validation_price"),
            (self.q_raw, "q_raw"),
            (self.common_quantity_step, "common_quantity_step"),
            (self.floored_quantity, "floored_quantity"),
            (self.risex_raw_quantity, "risex_raw_quantity"),
            (self.lighter_raw_quantity, "lighter_raw_quantity"),
            (self.risex_quantity_step_raw, "risex_quantity_step_raw"),
            (self.lighter_quantity_step_raw, "lighter_quantity_step_raw"),
            (self.risex_base_multiplier, "risex_base_multiplier"),
            (self.lighter_base_multiplier, "lighter_base_multiplier"),
            (self.risex_minimum_quantity_raw, "risex_minimum_quantity_raw"),
            (self.lighter_minimum_quantity_raw, "lighter_minimum_quantity_raw"),
            (self.risex_minimum_notional_usd, "risex_minimum_notional_usd"),
            (self.lighter_minimum_notional_usd, "lighter_minimum_notional_usd"),
        ):
            _decimal(value, name)
        if any(
            value <= 0
            for value in (
                self.target_notional_usd,
                self.reference_price,
                self.risex_validation_price,
                self.q_raw,
                self.common_quantity_step,
                self.risex_quantity_step_raw,
                self.lighter_quantity_step_raw,
                self.risex_base_multiplier,
                self.lighter_base_multiplier,
            )
        ):
            raise ValueError("sizing prices, steps, multipliers, and target must be positive")
        if any(
            value < 0
            for value in (
                self.floored_quantity,
                self.risex_raw_quantity,
                self.lighter_raw_quantity,
                self.risex_minimum_quantity_raw,
                self.lighter_minimum_quantity_raw,
                self.risex_minimum_notional_usd,
                self.lighter_minimum_notional_usd,
            )
        ):
            raise ValueError("sizing quantities and minimums must be non-negative")
        for flag_name in (
            "risex_min_quantity_ok",
            "risex_min_notional_ok",
            "lighter_min_quantity_ok",
            "lighter_min_notional_ok",
        ):
            if not isinstance(getattr(self, flag_name), bool):
                raise TypeError(f"{flag_name} must be bool")

    @property
    def quantity(self) -> Decimal:
        return self.floored_quantity

    @property
    def policy_target_notional_usd(self) -> Decimal:
        return self.target_notional_usd

    @property
    def quote_direction(self) -> SpreadDirection:
        return self.direction

    @property
    def risex_minimum_ok(self) -> bool:
        return self.risex_min_quantity_ok and self.risex_min_notional_ok

    @property
    def lighter_minimum_ok(self) -> bool:
        return self.lighter_min_quantity_ok and self.lighter_min_notional_ok

    @property
    def is_valid(self) -> bool:
        return self.floored_quantity > 0 and self.risex_minimum_ok and self.lighter_minimum_ok

    @property
    def raw_venue_quantities(self) -> tuple[Decimal, Decimal]:
        return self.risex_raw_quantity, self.lighter_raw_quantity


@dataclass(frozen=True, slots=True)
class HypotheticalMakerQuote:
    """A quote candidate with its complete exact entry economics."""

    policy: QuotePolicy
    outcome: EntryViabilityOutcome
    maker_side: Side
    lighter_side: Side
    canonical_quantity: Decimal | None
    maker_price: Decimal | None
    lighter_vwap_price: Decimal | None
    lighter_filled_quantity: Decimal | None
    lighter_notional_usd: Decimal | None
    maker_notional_usd: Decimal | None
    fee_components: tuple[FeeEvidence, ...]
    total_entry_fees_usd: Decimal | None
    target_edge_usd: Decimal | None
    actual_edge_usd: Decimal | None
    raw_risex_price_bound: Decimal | None
    post_only_bound_price: Decimal | None
    sizing_evidence: SizingEvidence | None
    exact_hedge_vwap: ExactVwap | None = None
    risex_tick_size: Decimal | None = None

    def __post_init__(self) -> None:
        outcome = (
            self.outcome
            if isinstance(self.outcome, EntryViabilityOutcome)
            else EntryViabilityOutcome(self.outcome)
        )
        object.__setattr__(self, "outcome", outcome)
        maker_side = self.maker_side if isinstance(self.maker_side, Side) else Side(self.maker_side)
        lighter_side = (
            self.lighter_side if isinstance(self.lighter_side, Side) else Side(self.lighter_side)
        )
        object.__setattr__(self, "maker_side", maker_side)
        object.__setattr__(self, "lighter_side", lighter_side)
        if maker_side is not self.policy.direction.maker_side:
            raise ValueError("maker_side does not match policy direction")
        if lighter_side is not self.policy.direction.hedge_side:
            raise ValueError("lighter_side does not match policy direction")
        for value, name in (
            (self.canonical_quantity, "canonical_quantity"),
            (self.maker_price, "maker_price"),
            (self.lighter_vwap_price, "lighter_vwap_price"),
            (self.lighter_filled_quantity, "lighter_filled_quantity"),
            (self.lighter_notional_usd, "lighter_notional_usd"),
            (self.maker_notional_usd, "maker_notional_usd"),
            (self.total_entry_fees_usd, "total_entry_fees_usd"),
            (self.target_edge_usd, "target_edge_usd"),
            (self.actual_edge_usd, "actual_edge_usd"),
            (self.raw_risex_price_bound, "raw_risex_price_bound"),
            (self.post_only_bound_price, "post_only_bound_price"),
            (self.risex_tick_size, "risex_tick_size"),
        ):
            if value is not None:
                _decimal(value, name)
        if not isinstance(self.fee_components, tuple):
            raise TypeError("fee_components must be a tuple")
        if self.exact_hedge_vwap is not None and not isinstance(self.exact_hedge_vwap, ExactVwap):
            raise TypeError("exact_hedge_vwap must be ExactVwap or None")

    @property
    def is_active(self) -> bool:
        if self.outcome is not EntryViabilityOutcome.QUOTE_ACTIVE:
            return False
        from .economics import validate_quote_economics

        try:
            return validate_quote_economics(self)
        except (AttributeError, TypeError, ValueError, ArithmeticError):
            return False

    @property
    def is_economic(self) -> bool:
        return self.is_active

    @property
    def entry_edge_usd(self) -> Decimal | None:
        return self.actual_edge_usd

    @property
    def hedge_notional_usd(self) -> Decimal | None:
        return self.lighter_notional_usd

    @property
    def quantity(self) -> Decimal | None:
        return self.canonical_quantity


@dataclass(frozen=True, slots=True)
class QuoteVersion:
    """Version-local quote identity and local clock provenance."""

    version_id: str
    quote: HypotheticalMakerQuote
    quote_created_utc: datetime
    quote_created_monotonic_ns: int
    stream_session_id: str | int
    recovery_generation: int
    quote_expires_monotonic_ns: int | None = None
    hedge_stream_session_id: str | int | None = None
    hedge_recovery_generation: int | None = None

    def __post_init__(self) -> None:
        if not self.version_id:
            raise ValueError("version_id must be non-empty")
        _utc(self.quote_created_utc, "quote_created_utc")
        _non_negative_int(self.quote_created_monotonic_ns, "quote_created_monotonic_ns")
        _session(self.stream_session_id)
        _non_negative_int(self.recovery_generation, "recovery_generation")
        if self.quote_expires_monotonic_ns is not None:
            _non_negative_int(self.quote_expires_monotonic_ns, "quote_expires_monotonic_ns")
            if self.quote_expires_monotonic_ns <= self.quote_created_monotonic_ns:
                raise ValueError("quote expiry must be after quote creation")
        if self.hedge_stream_session_id is not None:
            _session(self.hedge_stream_session_id, "hedge_stream_session_id")
        if self.hedge_recovery_generation is not None:
            _non_negative_int(self.hedge_recovery_generation, "hedge_recovery_generation")

    @property
    def canonical_market(self) -> str:
        return self.quote.policy.canonical_market

    @property
    def direction(self) -> SpreadDirection:
        return self.quote.policy.direction

    @property
    def canonical_quantity(self) -> Decimal | None:
        return self.quote.canonical_quantity

    @property
    def maker_price(self) -> Decimal | None:
        return self.quote.maker_price

    @property
    def quote_version_id(self) -> str:
        return self.version_id

    @property
    def is_active(self) -> bool:
        return self.quote.is_active and self.quote.is_economic


@dataclass(frozen=True, slots=True)
class TradeEvidence:
    """Public RISEx trade evidence with local receipt ordering."""

    trade_event_key: str
    venue: Venue
    canonical_market: str
    canonical_price: Decimal
    canonical_quantity: Decimal
    aggressor_side: Side
    received_utc: datetime
    received_monotonic_ns: int
    stream_session_id: str | int
    recovery_generation: int
    exchange_event_utc: datetime | None = None
    exchange_event_time_provenance: str | None = None

    def __post_init__(self) -> None:
        if not self.trade_event_key:
            raise ValueError("trade_event_key must be non-empty")
        venue = self.venue if isinstance(self.venue, Venue) else Venue(self.venue)
        side = self.aggressor_side if isinstance(self.aggressor_side, Side) else Side(self.aggressor_side)
        object.__setattr__(self, "venue", venue)
        object.__setattr__(self, "aggressor_side", side)
        if not self.canonical_market:
            raise ValueError("canonical_market must be non-empty")
        price = _decimal(self.canonical_price, "canonical_price")
        quantity = _decimal(self.canonical_quantity, "canonical_quantity")
        if price <= 0 or quantity <= 0:
            raise ValueError("trade price and quantity must be positive")
        _utc(self.received_utc, "received_utc")
        _utc(self.exchange_event_utc, "exchange_event_utc")
        if (self.exchange_event_utc is None) != (self.exchange_event_time_provenance is None):
            raise ValueError("exchange UTC requires explicit provenance")
        if self.exchange_event_time_provenance is not None and not self.exchange_event_time_provenance:
            raise ValueError("exchange event provenance must be non-empty")
        _non_negative_int(self.received_monotonic_ns, "received_monotonic_ns")
        _session(self.stream_session_id)
        _non_negative_int(self.recovery_generation, "recovery_generation")

    @property
    def received_at(self) -> datetime:
        return self.received_utc

    @property
    def exchange_timestamp(self) -> datetime | None:
        return self.exchange_event_utc

    @property
    def exchange_timestamp_provenance(self) -> str | None:
        return self.exchange_event_time_provenance

    @property
    def trade_received_monotonic_ns(self) -> int:
        return self.received_monotonic_ns


@dataclass(frozen=True, slots=True)
class BookEvidence:
    """Immutable Lighter book observation and stream-health provenance."""

    venue: Venue
    canonical_market: str
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    received_monotonic_ns: int
    stream_session_id: str | int
    recovery_generation: int
    book_revision: int
    sequence: int | None = None
    checksum: int | str | None = None
    sequence_valid: bool = True
    checksum_valid: bool = True
    received_utc: datetime | None = None
    fresh: bool = True

    def __post_init__(self) -> None:
        venue = self.venue if isinstance(self.venue, Venue) else Venue(self.venue)
        object.__setattr__(self, "venue", venue)
        if not self.canonical_market:
            raise ValueError("canonical_market must be non-empty")
        if not isinstance(self.bids, tuple) or not isinstance(self.asks, tuple):
            raise TypeError("book levels must be tuples")
        _non_negative_int(self.received_monotonic_ns, "received_monotonic_ns")
        _session(self.stream_session_id)
        _non_negative_int(self.recovery_generation, "recovery_generation")
        _non_negative_int(self.book_revision, "book_revision")
        if self.sequence is not None:
            _non_negative_int(self.sequence, "sequence")
        if not isinstance(self.sequence_valid, bool) or not isinstance(self.checksum_valid, bool):
            raise TypeError("sequence_valid and checksum_valid must be bool")
        if not isinstance(self.fresh, bool):
            raise TypeError("fresh must be bool")
        _utc(self.received_utc, "received_utc")

    @property
    def book_received_monotonic_ns(self) -> int:
        return self.received_monotonic_ns

    @property
    def is_sequence_healthy(self) -> bool:
        return self.sequence_valid and self.checksum_valid


LighterBookEvidence = BookEvidence


@dataclass(frozen=True, slots=True)
class DataGapEvidence:
    """A bounded public-data gap; source venue is part of its identity."""

    source_venue: Venue
    canonical_market: str
    stream_session_id: str | int
    recovery_generation: int
    gap_start_monotonic_ns: int
    gap_end_monotonic_ns: int | None = None
    reason: str = "DATA_GAP"
    protocol_frame_kind: str | None = None
    protocol_frame_category: str | None = None
    protocol_frame_length: int | None = None
    protocol_frame_sha256: str | None = None

    def __post_init__(self) -> None:
        venue = self.source_venue if isinstance(self.source_venue, Venue) else Venue(self.source_venue)
        object.__setattr__(self, "source_venue", venue)
        if venue not in (Venue.RISEX, Venue.LIGHTER):
            raise ValueError("data-gap source must be RISEx or Lighter")
        if not self.canonical_market:
            raise ValueError("canonical_market must be non-empty")
        _session(self.stream_session_id)
        _non_negative_int(self.recovery_generation, "recovery_generation")
        _non_negative_int(self.gap_start_monotonic_ns, "gap_start_monotonic_ns")
        if self.gap_end_monotonic_ns is not None:
            _non_negative_int(self.gap_end_monotonic_ns, "gap_end_monotonic_ns")
            if self.gap_end_monotonic_ns < self.gap_start_monotonic_ns:
                raise ValueError("gap end must not precede gap start")
        if not self.reason:
            raise ValueError("gap reason must be non-empty")
        protocol_values = (
            self.protocol_frame_kind,
            self.protocol_frame_category,
            self.protocol_frame_length,
            self.protocol_frame_sha256,
        )
        if any(value is not None for value in protocol_values):
            if any(value is None for value in protocol_values):
                raise ValueError("protocol failure evidence must be complete")
            for value, name, limit in (
                (self.protocol_frame_kind, "protocol_frame_kind", 64),
                (self.protocol_frame_category, "protocol_frame_category", 96),
            ):
                if not isinstance(value, str) or not value or len(value) > limit:
                    raise ValueError(f"{name} must be a bounded non-empty string")
                if any(ord(character) < 0x20 or ord(character) > 0x7E for character in value):
                    raise ValueError(f"{name} must use printable ASCII")
            _non_negative_int(self.protocol_frame_length, "protocol_frame_length")  # type: ignore[arg-type]
            if self.protocol_frame_length > 65_536:  # type: ignore[operator]
                raise ValueError("protocol_frame_length must be bounded")
            digest = self.protocol_frame_sha256
            if (
                not isinstance(digest, str)
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise ValueError("protocol_frame_sha256 must be a lowercase SHA-256 digest")

    @property
    def venue(self) -> Venue:
        return self.source_venue

    def matches(self, venue: Venue, market: str, session: str | int, recovery: int) -> bool:
        return (
            self.source_venue is venue
            and self.canonical_market == market
            and self.stream_session_id == session
            and self.recovery_generation == recovery
        )

    def overlaps(self, start_monotonic_ns: int, end_monotonic_ns: int) -> bool:
        _non_negative_int(start_monotonic_ns, "start_monotonic_ns")
        _non_negative_int(end_monotonic_ns, "end_monotonic_ns")
        if end_monotonic_ns < start_monotonic_ns:
            raise ValueError("interval end must not precede start")
        if self.gap_end_monotonic_ns is not None and self.gap_end_monotonic_ns < start_monotonic_ns:
            return False
        return self.gap_start_monotonic_ns <= end_monotonic_ns


def queue_overflow_gap(
    *,
    source_venue: Venue,
    canonical_market: str,
    stream_session_id: str | int,
    recovery_generation: int,
    gap_start_monotonic_ns: int,
    gap_end_monotonic_ns: int | None = None,
) -> DataGapEvidence:
    """Create the explicit input contract used when a bounded queue overflows."""

    return DataGapEvidence(
        source_venue=source_venue,
        canonical_market=canonical_market,
        stream_session_id=stream_session_id,
        recovery_generation=recovery_generation,
        gap_start_monotonic_ns=gap_start_monotonic_ns,
        gap_end_monotonic_ns=gap_end_monotonic_ns,
        reason="QUEUE_OVERFLOW",
    )


@dataclass(frozen=True, slots=True)
class WouldFillEvidence:
    """Version-local hypothetical maker fill evidence for one bound."""

    quote_version_id: str
    venue: Venue
    canonical_market: str
    direction: SpreadDirection
    canonical_quantity: Decimal
    cumulative_eligible_quantity: Decimal
    qualifying_trade_event_keys: tuple[str, ...]
    would_fill_detected_monotonic_ns: int
    qualifying_trades: tuple[TradeEvidence, ...] = ()
    detected_utc: datetime | None = None
    hedge_stream_session_id: str | int | None = None
    hedge_recovery_generation: int | None = None
    fillability_model: FillabilityModel = FillabilityModel.STRICT_LOWER_BOUND

    def __post_init__(self) -> None:
        if not self.quote_version_id:
            raise ValueError("quote_version_id must be non-empty")
        venue = self.venue if isinstance(self.venue, Venue) else Venue(self.venue)
        object.__setattr__(self, "venue", venue)
        model = (
            self.fillability_model
            if isinstance(self.fillability_model, FillabilityModel)
            else FillabilityModel(self.fillability_model)
        )
        object.__setattr__(self, "fillability_model", model)
        if venue is not Venue.RISEX:
            raise ValueError("would-fill evidence must come from RISEx")
        if not self.canonical_market:
            raise ValueError("canonical_market must be non-empty")
        if not isinstance(self.direction, SpreadDirection):
            object.__setattr__(self, "direction", SpreadDirection(self.direction))
        quantity = _decimal(self.canonical_quantity, "canonical_quantity")
        cumulative = _decimal(self.cumulative_eligible_quantity, "cumulative_eligible_quantity")
        if quantity <= 0 or cumulative < quantity:
            raise ValueError("would-fill cumulative quantity must reach exact quantity")
        if not self.qualifying_trade_event_keys or len(
            set(self.qualifying_trade_event_keys)
        ) != len(self.qualifying_trade_event_keys):
            raise ValueError("qualifying trade keys must be non-empty and unique")
        if not isinstance(self.qualifying_trades, tuple):
            raise TypeError("qualifying_trades must be a tuple")
        _non_negative_int(
            self.would_fill_detected_monotonic_ns,
            "would_fill_detected_monotonic_ns",
        )
        _utc(self.detected_utc, "detected_utc")
        if self.hedge_stream_session_id is not None:
            _session(self.hedge_stream_session_id, "hedge_stream_session_id")
        if self.hedge_recovery_generation is not None:
            _non_negative_int(self.hedge_recovery_generation, "hedge_recovery_generation")
        if self.qualifying_trades:
            if tuple(trade.trade_event_key for trade in self.qualifying_trades) != self.qualifying_trade_event_keys:
                raise ValueError("qualifying trade keys do not match retained trades")
            if any(
                trade.venue is not Venue.RISEX
                or trade.canonical_market != self.canonical_market
                or trade.aggressor_side is not self.direction.hedge_side
                for trade in self.qualifying_trades
            ):
                raise ValueError("qualifying trade identity does not match would-fill evidence")
            if max(trade.received_monotonic_ns for trade in self.qualifying_trades) > self.would_fill_detected_monotonic_ns:
                raise ValueError("detection time precedes retained trade receipt")

    @property
    def detected_monotonic_ns(self) -> int:
        return self.would_fill_detected_monotonic_ns

    @property
    def filled_quantity(self) -> Decimal:
        return self.canonical_quantity

    @property
    def model(self) -> FillabilityModel:
        return self.fillability_model


@dataclass(frozen=True, slots=True)
class HedgeHorizonCapture:
    """One no-lookahead Lighter hedge observation at a fixed horizon."""

    horizon_ms: int
    would_fill_detected_monotonic_ns: int
    horizon_deadline_monotonic_ns: int
    expected_stream_session_id: str | int
    expected_recovery_generation: int
    canonical_market: str
    requested_quantity: Decimal
    outcome: EntryViabilityOutcome
    book: BookEvidence | None
    book_received_monotonic_ns: int | None
    book_stream_session_id: str | int | None
    book_recovery_generation: int | None
    book_revision: int | None
    sequence: int | None
    checksum: int | str | None
    filled_quantity: Decimal
    notional_usd: Decimal
    vwap_price: Decimal | None
    gap_evidence: DataGapEvidence | None = None
    freshness_max_age_ns: int | None = None
    ambiguous_books: tuple[BookEvidence, ...] = ()
    fillability_model: FillabilityModel = FillabilityModel.STRICT_LOWER_BOUND

    def __post_init__(self) -> None:
        if isinstance(self.horizon_ms, bool) or not isinstance(self.horizon_ms, int):
            raise TypeError("horizon_ms must be int")
        if self.horizon_ms not in (0, 300, 500, 1000, 2000):
            raise ValueError("unsupported horizon; use 0/300/500/1000 ms")
        _non_negative_int(
            self.would_fill_detected_monotonic_ns,
            "would_fill_detected_monotonic_ns",
        )
        _non_negative_int(self.horizon_deadline_monotonic_ns, "horizon_deadline_monotonic_ns")
        expected_deadline = self.would_fill_detected_monotonic_ns + self.horizon_ms * 1_000_000
        if self.horizon_deadline_monotonic_ns != expected_deadline:
            raise ValueError("horizon deadline must be detection time plus exact integer horizon")
        _session(self.expected_stream_session_id, "expected_stream_session_id")
        _non_negative_int(self.expected_recovery_generation, "expected_recovery_generation")
        if not self.canonical_market:
            raise ValueError("canonical_market must be non-empty")
        requested = _decimal(self.requested_quantity, "requested_quantity")
        filled = _decimal(self.filled_quantity, "filled_quantity")
        notional = _decimal(self.notional_usd, "notional_usd")
        if requested <= 0 or filled < 0 or filled > requested or notional < 0:
            raise ValueError("invalid hedge quantities or notional")
        outcome = (
            self.outcome
            if isinstance(self.outcome, EntryViabilityOutcome)
            else EntryViabilityOutcome(self.outcome)
        )
        object.__setattr__(self, "outcome", outcome)
        model = (
            self.fillability_model
            if isinstance(self.fillability_model, FillabilityModel)
            else FillabilityModel(self.fillability_model)
        )
        object.__setattr__(self, "fillability_model", model)
        if self.vwap_price is not None and _decimal(self.vwap_price, "vwap_price") <= 0:
            raise ValueError("vwap_price must be positive when present")
        if self.freshness_max_age_ns is not None:
            _non_negative_int(self.freshness_max_age_ns, "freshness_max_age_ns")
        if not isinstance(self.ambiguous_books, tuple):
            raise TypeError("ambiguous_books must be a tuple")
        executable_outcomes = {
            EntryViabilityOutcome.HEDGE_FULL,
            EntryViabilityOutcome.HEDGE_PARTIAL,
            EntryViabilityOutcome.HEDGE_DEPTH_UNAVAILABLE,
        }
        data_failure_outcomes = {
            EntryViabilityOutcome.HEDGE_DATA_MISSING,
            EntryViabilityOutcome.HEDGE_DATA_STALE,
            EntryViabilityOutcome.HEDGE_SESSION_DISPLACED,
            EntryViabilityOutcome.HEDGE_DATA_GAP,
        }
        if outcome not in executable_outcomes | data_failure_outcomes | {
            EntryViabilityOutcome.HEDGE_OUTCOME_UNKNOWN,
        }:
            raise ValueError("horizon capture requires a hedge outcome")
        if self.book is not None:
            if self.book.venue is not Venue.LIGHTER or self.book.canonical_market != self.canonical_market:
                raise ValueError("selected book identity does not match capture")
            if self.book_received_monotonic_ns != self.book.received_monotonic_ns:
                raise ValueError("book receipt provenance does not match selected book")
            if self.book_stream_session_id != self.book.stream_session_id:
                raise ValueError("book session provenance does not match selected book")
            if self.book_recovery_generation != self.book.recovery_generation:
                raise ValueError("book recovery provenance does not match selected book")
            if self.book_revision != self.book.book_revision:
                raise ValueError("book revision provenance does not match selected book")
            if self.sequence != self.book.sequence or self.checksum != self.book.checksum:
                raise ValueError("book sequence/checksum provenance does not match selected book")
            if self.book.received_monotonic_ns > self.horizon_deadline_monotonic_ns:
                raise ValueError("book received after horizon deadline")
        elif any(
            value is not None
            for value in (
                self.book_received_monotonic_ns,
                self.book_stream_session_id,
                self.book_recovery_generation,
                self.book_revision,
                self.sequence,
                self.checksum,
            )
        ):
            raise ValueError("book provenance requires a retained book")
        book_is_current = self.book is not None and (
            self.book.stream_session_id == self.expected_stream_session_id
            and self.book.recovery_generation == self.expected_recovery_generation
        )
        book_is_healthy = self.book is not None and self.book.is_sequence_healthy
        book_fails_age = self.book is not None and (
            self.freshness_max_age_ns is not None
            and self.horizon_deadline_monotonic_ns - self.book.received_monotonic_ns
            > self.freshness_max_age_ns
        )
        book_is_stale = book_is_current and book_is_healthy and (
            not self.book.fresh or book_fails_age
        )
        book_is_executable = book_is_current and book_is_healthy and (
            self.book.fresh and not book_fails_age
        )
        if self.gap_evidence is not None:
            if not self.gap_evidence.matches(
                Venue.LIGHTER,
                self.canonical_market,
                self.expected_stream_session_id,
                self.expected_recovery_generation,
            ):
                raise ValueError("gap evidence identity does not match expected Lighter stream")
        for ambiguous in self.ambiguous_books:
            if (
                ambiguous.venue is not Venue.LIGHTER
                or ambiguous.canonical_market != self.canonical_market
                or ambiguous.stream_session_id != self.expected_stream_session_id
                or ambiguous.recovery_generation != self.expected_recovery_generation
                or ambiguous.received_monotonic_ns > self.horizon_deadline_monotonic_ns
            ):
                raise ValueError("ambiguous book provenance does not match capture identity")
        if self.ambiguous_books:
            first_ambiguous = self.ambiguous_books[0]
            identity = (
                first_ambiguous.received_monotonic_ns,
                first_ambiguous.book_revision,
                first_ambiguous.sequence,
            )
            if len(self.ambiguous_books) < 2 or any(
                book == first_ambiguous
                or (
                    book.received_monotonic_ns,
                    book.book_revision,
                    book.sequence,
                )
                != identity
                for book in self.ambiguous_books[1:]
            ):
                raise ValueError("ambiguous book provenance must contain a conflicting tied group")
        if self.ambiguous_books and outcome is not EntryViabilityOutcome.HEDGE_OUTCOME_UNKNOWN:
            raise ValueError("ambiguous book provenance requires UNKNOWN outcome")
        if self.ambiguous_books and self.book is not None:
            raise ValueError("ambiguous book provenance cannot also select a book")
        if outcome is EntryViabilityOutcome.HEDGE_FULL and filled != requested:
            raise ValueError("HEDGE_FULL requires exact requested quantity")
        if outcome is EntryViabilityOutcome.HEDGE_PARTIAL and not (0 < filled < requested):
            raise ValueError("HEDGE_PARTIAL requires positive quantity below exact q")
        if outcome is EntryViabilityOutcome.HEDGE_DEPTH_UNAVAILABLE and filled != 0:
            raise ValueError("HEDGE_DEPTH_UNAVAILABLE requires zero executable quantity")
        if outcome in {EntryViabilityOutcome.HEDGE_FULL, EntryViabilityOutcome.HEDGE_PARTIAL}:
            if notional <= 0:
                raise ValueError("positive hedge outcomes require positive notional")
        if outcome is EntryViabilityOutcome.HEDGE_FULL and self.vwap_price is None:
            raise ValueError("HEDGE_FULL requires a derived VWAP price")
        if outcome is EntryViabilityOutcome.HEDGE_DEPTH_UNAVAILABLE and (
            notional != 0 or self.vwap_price is not None
        ):
            raise ValueError("HEDGE_DEPTH_UNAVAILABLE requires zero notional and no VWAP")
        if outcome in executable_outcomes and not book_is_executable:
            raise ValueError("executable hedge outcomes require a healthy fresh Lighter book")
        if outcome in executable_outcomes and (self.gap_evidence is not None or self.ambiguous_books):
            raise ValueError("executable hedge outcomes cannot retain gap or ambiguity evidence")
        if outcome is EntryViabilityOutcome.HEDGE_DATA_MISSING:
            if self.book is not None or self.gap_evidence is not None or self.ambiguous_books:
                raise ValueError("HEDGE_DATA_MISSING requires no book or gap selection")
        elif outcome is EntryViabilityOutcome.HEDGE_SESSION_DISPLACED:
            if self.book is None or book_is_current:
                raise ValueError("HEDGE_SESSION_DISPLACED requires displaced book provenance")
            if self.gap_evidence is not None or self.ambiguous_books:
                raise ValueError("displaced outcome cannot retain gap or ambiguity evidence")
        elif outcome is EntryViabilityOutcome.HEDGE_DATA_STALE:
            if not book_is_stale:
                raise ValueError("HEDGE_DATA_STALE requires a stale current healthy book")
            if self.gap_evidence is not None or self.ambiguous_books:
                raise ValueError("stale outcome cannot retain gap or ambiguity evidence")
        elif outcome is EntryViabilityOutcome.HEDGE_DATA_GAP:
            if self.gap_evidence is None:
                raise ValueError("HEDGE_DATA_GAP requires gap evidence")
            if self.book is not None and not book_is_current:
                raise ValueError("gap outcome cannot retain displaced book provenance")
            if self.ambiguous_books:
                raise ValueError("gap outcome cannot retain ambiguity evidence")
        elif outcome is EntryViabilityOutcome.HEDGE_OUTCOME_UNKNOWN:
            if self.gap_evidence is not None:
                raise ValueError("UNKNOWN cannot mask a known data gap")
            if not self.ambiguous_books:
                if self.book is None:
                    raise ValueError("UNKNOWN cannot mask missing book data")
                if not book_is_current:
                    raise ValueError("UNKNOWN cannot mask displaced book data")
                if book_is_stale:
                    raise ValueError("UNKNOWN cannot mask stale book data")
                if book_is_executable and _book_levels_well_formed(self.book) and (
                    (filled == 0 and notional == 0 and self.vwap_price is None)
                    or (0 < filled <= requested and notional > 0)
                ):
                    raise ValueError("UNKNOWN cannot mask a known hedge-depth outcome")
        if outcome in data_failure_outcomes and (
            filled != 0 or notional != 0 or self.vwap_price is not None
        ):
            raise ValueError("data-failure hedge outcomes require zero executable evidence")

    @property
    def deadline_monotonic_ns(self) -> int:
        return self.horizon_deadline_monotonic_ns

    @property
    def exact_notional_usd(self) -> Decimal:
        return self.notional_usd

    @property
    def model(self) -> FillabilityModel:
        return self.fillability_model


@dataclass(frozen=True, slots=True)
class EntryViabilityEpisode:
    """Deterministic episode linking one quote version to fill and horizons."""

    quote_version: QuoteVersion
    outcome: EntryViabilityOutcome
    would_fill_evidence: WouldFillEvidence | None = None
    horizon_captures: tuple[HedgeHorizonCapture, ...] = ()
    fillability_model: FillabilityModel | None = None

    def __post_init__(self) -> None:
        outcome = (
            self.outcome
            if isinstance(self.outcome, EntryViabilityOutcome)
            else EntryViabilityOutcome(self.outcome)
        )
        object.__setattr__(self, "outcome", outcome)
        model = self.fillability_model
        if model is None:
            model = (
                self.would_fill_evidence.fillability_model
                if self.would_fill_evidence is not None
                else FillabilityModel.STRICT_LOWER_BOUND
            )
        elif not isinstance(model, FillabilityModel):
            model = FillabilityModel(model)
        object.__setattr__(self, "fillability_model", model)
        if not isinstance(self.horizon_captures, tuple):
            raise TypeError("horizon_captures must be a tuple")
        if outcome is not EntryViabilityOutcome.WOULD_FILL and self.horizon_captures:
            raise ValueError("non-fill episodes cannot contain horizon captures")
        if self.would_fill_evidence is not None:
            evidence = self.would_fill_evidence
            if evidence.quote_version_id != self.quote_version.version_id:
                raise ValueError("would-fill evidence belongs to another quote version")
            if evidence.canonical_market != self.quote_version.canonical_market:
                raise ValueError("would-fill market does not match quote version")
            if evidence.direction is not self.quote_version.direction:
                raise ValueError("would-fill direction does not match quote version")
            if evidence.fillability_model is not model:
                raise ValueError("would-fill model does not match episode")
            if outcome is not EntryViabilityOutcome.WOULD_FILL:
                raise ValueError("would-fill evidence requires WOULD_FILL episode outcome")
        if outcome is EntryViabilityOutcome.WOULD_FILL and self.would_fill_evidence is None:
            raise ValueError("WOULD_FILL requires evidence")
        for capture in self.horizon_captures:
            if capture.canonical_market != self.quote_version.canonical_market:
                raise ValueError("horizon market does not match quote version")
            if self.would_fill_evidence is not None:
                if (
                    capture.would_fill_detected_monotonic_ns
                    != self.would_fill_evidence.would_fill_detected_monotonic_ns
                ):
                    raise ValueError("horizon detection time does not match would-fill evidence")
            if capture.fillability_model is not model:
                raise ValueError("horizon model does not match episode")

    @property
    def captures_by_horizon(self) -> tuple[HedgeHorizonCapture, ...]:
        return self.horizon_captures

    @property
    def model(self) -> FillabilityModel:
        return self.fillability_model


__all__ = [
    "BookEvidence",
    "BookLevel",
    "CanonicalMarket",
    "DataGapEvidence",
    "EntryViabilityEpisode",
    "EntryViabilityOutcome",
    "ExactVwap",
    "FeeEvidence",
    "FillabilityModel",
    "HedgeHorizonCapture",
    "HypotheticalMakerQuote",
    "LighterBookEvidence",
    "QuotePolicy",
    "QuoteVersion",
    "SampleStopReason",
    "SampleStopSignal",
    "SizingEvidence",
    "SpreadDirection",
    "TradeEvidence",
    "Venue",
    "Side",
    "queue_overflow_gap",
]
