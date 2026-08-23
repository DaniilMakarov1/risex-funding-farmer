"""Immutable domain contracts for the paper trader."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum


class Venue(StrEnum):
    RISEX = "RISEX"
    EXTENDED = "EXTENDED"
    NADO = "NADO"


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class PositionSide(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


class LiquidityRole(StrEnum):
    MAKER = "MAKER"
    TAKER = "TAKER"


class MarketType(StrEnum):
    PERPETUAL = "PERPETUAL"
    SPOT = "SPOT"
    OTHER = "OTHER"


class ContractType(StrEnum):
    LINEAR = "LINEAR"
    INVERSE = "INVERSE"
    OTHER = "OTHER"


class RouteDirection(StrEnum):
    LONG_RISEX_SHORT_HEDGE = "LONG_RISEX_SHORT_HEDGE"
    SHORT_RISEX_LONG_HEDGE = "SHORT_RISEX_LONG_HEDGE"


class FundingQuality(StrEnum):
    PREDICTED = "PREDICTED"
    ESTIMATED = "ESTIMATED"
    APPLIED_RATE = "APPLIED_RATE"
    UNKNOWN = "UNKNOWN"


class FundingAccrualMethod(StrEnum):
    SNAPSHOT_AT_SETTLEMENT = "SNAPSHOT_AT_SETTLEMENT"
    TIME_WEIGHTED = "TIME_WEIGHTED"
    OTHER_DOCUMENTED = "OTHER_DOCUMENTED"
    UNKNOWN = "UNKNOWN"


class SettlementStatus(StrEnum):
    PENDING = "PENDING"
    ESTIMATED = "ESTIMATED"
    APPLIED_RATE = "APPLIED_RATE"
    UNRESOLVED = "UNRESOLVED"
    SKIPPED_POSITION_NOT_OPEN = "SKIPPED_POSITION_NOT_OPEN"
    SKIPPED_POSITION_CLOSED = "SKIPPED_POSITION_CLOSED"


class DataQuality(StrEnum):
    COMPLETE = "COMPLETE"
    DEGRADED = "DEGRADED"


class LifecycleState(StrEnum):
    FLAT = "FLAT"
    ENTRY_MAKER_OPEN = "ENTRY_MAKER_OPEN"
    HOLDING = "HOLDING"
    EXITING_NORMAL = "EXITING_NORMAL"
    EXITING_AGGRESSIVE = "EXITING_AGGRESSIVE"


@dataclass(frozen=True, slots=True)
class CanonicalMarket:
    canonical_asset: str
    venue: Venue
    venue_symbol: str
    market_type: MarketType
    contract_type: ContractType
    base_multiplier: Decimal | None
    quote_asset: str
    settlement_asset: str
    tick_size_raw: Decimal
    quantity_step_raw: Decimal
    minimum_quantity_raw: Decimal
    minimum_notional_usd: Decimal
    minimum_fee_notional_usd: Decimal | None
    is_active: bool
    is_rfq: bool
    is_off_hours: bool
    evidence_blockers: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class BookLevel:
    canonical_price: Decimal
    canonical_quantity: Decimal


@dataclass(frozen=True, slots=True)
class OrderBook:
    venue: Venue
    canonical_market: str
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    observed_at: datetime
    sequence: int | None = None


@dataclass(frozen=True, slots=True)
class BookDelta:
    venue: Venue
    canonical_market: str
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    observed_at: datetime
    sequence: int | None = None
    previous_sequence: int | None = None
    checksum: int | None = None


@dataclass(frozen=True, slots=True)
class ExactVwap:
    requested_quantity: Decimal
    filled_quantity: Decimal
    notional_usd: Decimal
    price: Decimal | None

    @property
    def is_executable(self) -> bool:
        return self.price is not None and self.filled_quantity == self.requested_quantity


@dataclass(frozen=True, slots=True)
class Route:
    canonical_asset: str
    risex_market: CanonicalMarket
    hedge_market: CanonicalMarket
    hedge_venue: Venue
    direction: RouteDirection
    route_liquidity_usd: Decimal


@dataclass(frozen=True, slots=True)
class MarketVolume:
    venue: Venue
    canonical_market: str
    quote_volume_usd: Decimal | None
    observed_at: datetime
    source: str


@dataclass(frozen=True, slots=True)
class FundingCashQuote:
    venue: Venue
    canonical_market: str
    observed_at: datetime
    assumed_or_actual_position_opened_at: datetime
    settlement_at: datetime
    quality: FundingQuality
    accrual_method: FundingAccrualMethod
    eligibility_known: bool
    long_cash_per_canonical_base_usd: Decimal | None
    short_cash_per_canonical_base_usd: Decimal | None
    source: str


@dataclass(frozen=True, slots=True)
class FundingEvent:
    venue: Venue
    canonical_market: str
    settlement_at: datetime
    expected_cash_usd: Decimal | None
    eligibility_known: bool
    status: SettlementStatus = SettlementStatus.PENDING


@dataclass(frozen=True, slots=True)
class TargetFundingCycle:
    cycle_id: str
    start_at: datetime
    end_at: datetime
    span_seconds: int
    risex_event: FundingEvent
    hedge_event: FundingEvent


@dataclass(frozen=True, slots=True)
class FundingSettlement:
    venue: Venue
    canonical_market: str
    settlement_at: datetime
    status: SettlementStatus
    cash_usd: Decimal | None = None

    @property
    def key(self) -> tuple[Venue, str, datetime]:
        return self.venue, self.canonical_market, self.settlement_at


@dataclass(frozen=True, slots=True)
class Fee:
    venue: Venue
    liquidity_role: LiquidityRole
    fill_notional_usd: Decimal
    fee_base_notional_usd: Decimal
    rate: Decimal
    amount_usd: Decimal
    source: str
    observed_or_configured_at: datetime


@dataclass(frozen=True, slots=True)
class Fill:
    venue: Venue
    canonical_market: str
    side: Side
    canonical_quantity: Decimal
    canonical_price: Decimal
    exchange_at: datetime
    receipt_at: datetime
    fee: Fee


@dataclass(frozen=True, slots=True)
class StreamHealth:
    last_market_event_at: datetime | None
    last_connection_confirmation_at: datetime | None
    stream_connected: bool
    book_initialized: bool
    book_sequence_valid: bool
    data_quality: DataQuality


@dataclass(frozen=True, slots=True)
class TradeEvidence:
    trade_event_key: str
    venue: Venue
    canonical_market: str
    exchange_timestamp: datetime | None
    received_at: datetime
    raw_timestamp: str | int | None
    canonical_quantity: Decimal
    canonical_price: Decimal
    aggressor_side: Side | None
    is_orderbook_match: bool | None
    source_marker: str = "OFFICIAL_PUBLIC"
    paper_assumptions: tuple[str, ...] = ()
    risex_contract_assumption_used: bool = False
    risex_funding_eligibility_assumption_used: bool = False
    risex_funding_estimate_assumption_used: bool = False
    paper_assumption_used: bool = False


@dataclass(frozen=True, slots=True)
class BookExecutionCapture:
    """Immutable public book input and its runtime ownership identity."""

    book: OrderBook
    health: StreamHealth
    received_at: datetime
    decision_at: datetime
    stream_session_id: int
    recovery_generation: int
    book_revision: int
    checksum: int | None = None


@dataclass(frozen=True, slots=True)
class MakerFillProvenance:
    venue: Venue
    canonical_market: str
    side: Side
    order_id: str
    order_version_id: str
    limit_price: Decimal
    tick_size: Decimal
    qualifying_trades: tuple[TradeEvidence, ...]
    decision_at: datetime


@dataclass(frozen=True, slots=True)
class TakerFillProvenance:
    venue: Venue
    canonical_market: str
    side: Side
    stream_session_id: int
    recovery_generation: int
    book_revision: int
    observed_at: datetime
    received_at: datetime
    decision_at: datetime
    sequence: int | None
    checksum: int | None
    consumed_levels: tuple[BookLevel, ...]
    requested_quantity: Decimal
    executed_quantity: Decimal
    notional_usd: Decimal
    vwap_price: Decimal


FillProvenance = MakerFillProvenance | TakerFillProvenance
