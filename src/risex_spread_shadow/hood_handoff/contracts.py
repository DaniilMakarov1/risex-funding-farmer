"""Fail-closed contracts for the one-attempt HOOD close/reopen operation.

The legacy scanner does not import this package.  Everything in this module is
plain data and validation; network clients and signing live in :mod:`sdk`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any, Mapping


class Direction(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"

    @property
    def sign(self) -> int:
        return 1 if self is Direction.LONG else -1

    @property
    def source_side(self) -> str:
        return "SELL" if self is Direction.LONG else "BUY"

    @property
    def receiver_side(self) -> str:
        return "BUY" if self is Direction.LONG else "SELL"


class Outcome(StrEnum):
    PREVIEW = "PREVIEW"
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    FAILED_PREFLIGHT_BLOCKED = "FAILED_PREFLIGHT_BLOCKED"
    UNKNOWN = "UNKNOWN"


class Phase(StrEnum):
    PREFLIGHT = "PREFLIGHT"
    SOURCE_DISPATCH = "SOURCE_DISPATCH"
    SOURCE_RESTING = "SOURCE_RESTING"
    RECEIVER_DISPATCH = "RECEIVER_DISPATCH"
    CANCEL_SOURCE = "CANCEL_SOURCE"
    RECONCILIATION = "RECONCILIATION"
    COMPLETE = "COMPLETE"


TERMINAL_ORDER_STATUSES = frozenset(
    {
        "filled",
        "canceled",
        "cancelled",
        "canceled-post-only",
        "canceled-reduce-only",
        "canceled-invalid-balance",
        "rejected",
        "expired",
        "failed",
    }
)
ACTIVE_ORDER_STATUSES = frozenset({"open", "active", "pending", "in-progress"})


class ContractError(ValueError):
    """A fail-closed contract or schema error."""


class PreflightBlocked(ContractError):
    """A required pre-mutation condition is not proven."""


def _decimal(value: Any, name: str) -> Decimal:
    if isinstance(value, bool):
        raise ContractError(f"{name} must be a finite decimal")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ContractError(f"{name} must be a finite decimal") from exc
    if not result.is_finite():
        raise ContractError(f"{name} must be finite")
    return result


def _positive(value: Any, name: str) -> Decimal:
    result = _decimal(value, name)
    if result <= 0:
        raise ContractError(f"{name} must be positive")
    return result


def _nonnegative(value: Any, name: str) -> Decimal:
    result = _decimal(value, name)
    if result < 0:
        raise ContractError(f"{name} must be non-negative")
    return result


def _int(value: Any, name: str, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ContractError(f"{name} must be an integer")
    if minimum is not None and value < minimum:
        raise ContractError(f"{name} must be at least {minimum}")
    return value


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{name} must be non-empty text")
    return value.strip()


def _bool(value: Any, name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
    raise ContractError(f"{name} must be bool")


def _timestamp(value: Any, name: str) -> float:
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ContractError(f"{name} must be timezone-aware")
        return value.astimezone(timezone.utc).timestamp()
    if isinstance(value, bool):
        raise ContractError(f"{name} must be a timestamp")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ContractError(f"{name} must be a timestamp") from exc
    if not result >= 0 or result == float("inf"):
        raise ContractError(f"{name} must be a finite non-negative timestamp")
    return result


def _wire_decimal(value: Decimal) -> str:
    return format(value, "f")


def decimal_to_integer(value: Decimal, decimals: int, name: str) -> int:
    """Convert an exact grid value without rounding exposure upward."""

    _int(decimals, "decimals", minimum=0)
    value = _positive(value, name)
    scale = Decimal(10) ** decimals
    scaled = value * scale
    integer = scaled.to_integral_value()
    if scaled != integer:
        raise ContractError(f"{name}={value} is off the {decimals}-decimal grid")
    result = int(integer)
    if result <= 0:
        raise ContractError(f"{name} converts to zero")
    return result


@dataclass(frozen=True, slots=True)
class MarketMetadata:
    market_id: int
    symbol: str
    status: str
    price_decimals: int
    size_decimals: int
    minimum_base_amount: Decimal
    minimum_quote_amount: Decimal
    source_fee_rate: Decimal | None
    receiver_fee_rate: Decimal | None
    observed_at: float
    margin_evidence: str = ""

    def __post_init__(self) -> None:
        _int(self.market_id, "market_id", minimum=0)
        _text(self.symbol, "symbol")
        _text(self.status, "status")
        _int(self.price_decimals, "price_decimals", minimum=0)
        _int(self.size_decimals, "size_decimals", minimum=0)
        _positive(self.minimum_base_amount, "minimum_base_amount")
        _positive(self.minimum_quote_amount, "minimum_quote_amount")
        if self.source_fee_rate is not None:
            _nonnegative(self.source_fee_rate, "source_fee_rate")
        if self.receiver_fee_rate is not None:
            _nonnegative(self.receiver_fee_rate, "receiver_fee_rate")
        _timestamp(self.observed_at, "observed_at")
        _text(self.margin_evidence, "margin_evidence")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "MarketMetadata":
        return cls(
            market_id=_int(value.get("market_id"), "market_id", minimum=0),
            symbol=_text(value.get("symbol", value.get("market_symbol")), "symbol"),
            status=_text(value.get("status"), "status"),
            price_decimals=_int(
                value.get("price_decimals", value.get("supported_price_decimals")),
                "price_decimals",
                minimum=0,
            ),
            size_decimals=_int(
                value.get("size_decimals", value.get("supported_size_decimals")),
                "size_decimals",
                minimum=0,
            ),
            minimum_base_amount=_positive(
                value.get("minimum_base_amount", value.get("min_base_amount")),
                "minimum_base_amount",
            ),
            minimum_quote_amount=_positive(
                value.get("minimum_quote_amount", value.get("min_quote_amount")),
                "minimum_quote_amount",
            ),
            source_fee_rate=(
                None
                if value.get("source_fee_rate") is None
                else _nonnegative(value.get("source_fee_rate"), "source_fee_rate")
            ),
            receiver_fee_rate=(
                None
                if value.get("receiver_fee_rate") is None
                else _nonnegative(value.get("receiver_fee_rate"), "receiver_fee_rate")
            ),
            observed_at=_timestamp(value.get("observed_at"), "observed_at"),
            margin_evidence=_text(value.get("margin_evidence"), "margin_evidence"),
        )


@dataclass(frozen=True, slots=True)
class OrderSnapshot:
    account_index: int
    market_id: int
    order_id: str
    client_order_index: int | str | None
    status: str
    side: str
    order_type: str
    time_in_force: str
    reduce_only: bool
    initial_quantity: Decimal
    remaining_quantity: Decimal
    filled_quantity: Decimal
    price: Decimal | None
    observed_at: float

    def __post_init__(self) -> None:
        _int(self.account_index, "account_index", minimum=0)
        _int(self.market_id, "market_id", minimum=0)
        _text(self.order_id, "order_id")
        if self.client_order_index is not None and (
            isinstance(self.client_order_index, bool)
            or not isinstance(self.client_order_index, (int, str))
            or (isinstance(self.client_order_index, str) and not self.client_order_index.strip())
        ):
            raise ContractError("client_order_index must be a non-empty integer/string")
        _text(self.status, "status")
        side = _text(self.side, "side").upper()
        if side not in {"BUY", "SELL"}:
            raise ContractError("order side must be BUY or SELL")
        _text(self.order_type, "order_type")
        _text(self.time_in_force, "time_in_force")
        if not isinstance(self.reduce_only, bool):
            raise ContractError("reduce_only must be bool")
        for value, name in (
            (self.initial_quantity, "initial_quantity"),
            (self.remaining_quantity, "remaining_quantity"),
            (self.filled_quantity, "filled_quantity"),
        ):
            _nonnegative(value, name)
        if self.filled_quantity + self.remaining_quantity > self.initial_quantity:
            raise ContractError("order filled + remaining exceeds initial quantity")
        if self.price is not None:
            _positive(self.price, "price")
        _timestamp(self.observed_at, "observed_at")

    @property
    def terminal(self) -> bool:
        return self.status.lower() in TERMINAL_ORDER_STATUSES

    @property
    def active(self) -> bool:
        return self.status.lower() in ACTIVE_ORDER_STATUSES

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "OrderSnapshot":
        def pick(*names: str, default: Any = None) -> Any:
            for name in names:
                if name in value:
                    return value[name]
            return default

        return cls(
            account_index=_int(pick("account_index", "owner_account_index"), "account_index", minimum=0),
            market_id=_int(pick("market_id", "market_index"), "market_id", minimum=0),
            order_id=_text(pick("order_id", "order_index"), "order_id"),
            client_order_index=pick("client_order_index", "client_order_id"),
            status=_text(pick("status"), "status"),
            side=_text(pick("side", default="SELL"), "side"),
            order_type=_text(pick("type", "order_type", default="unknown"), "order_type"),
            time_in_force=_text(pick("time_in_force", default="unknown"), "time_in_force"),
            reduce_only=_bool(pick("reduce_only", default=False), "reduce_only"),
            initial_quantity=_positive(
                pick("initial_quantity", "initial_base_amount", "base_amount"),
                "initial_quantity",
            ),
            remaining_quantity=_nonnegative(
                pick("remaining_quantity", "remaining_base_amount", default="0"),
                "remaining_quantity",
            ),
            filled_quantity=_nonnegative(
                pick("filled_quantity", "filled_base_amount", default="0"),
                "filled_quantity",
            ),
            price=(
                None
                if pick("price", "base_price") is None
                else _positive(pick("price", "base_price"), "price")
            ),
            observed_at=_timestamp(pick("observed_at", "updated_at", "timestamp"), "observed_at"),
        )


@dataclass(frozen=True, slots=True)
class AccountSnapshot:
    account_index: int
    market_id: int
    signed_position: Decimal
    active_orders: tuple[OrderSnapshot, ...]
    observed_at: float
    authorized: bool
    ready: bool
    margin_available: Decimal | None
    margin_required: Decimal | None
    fee_rate: Decimal | None
    source_identity: str

    def __post_init__(self) -> None:
        _int(self.account_index, "account_index", minimum=0)
        _int(self.market_id, "market_id", minimum=0)
        _decimal(self.signed_position, "signed_position")
        _timestamp(self.observed_at, "observed_at")
        if not isinstance(self.authorized, bool) or not isinstance(self.ready, bool):
            raise ContractError("authorized and ready must be bool")
        for value, name in (
            (self.margin_available, "margin_available"),
            (self.margin_required, "margin_required"),
            (self.fee_rate, "fee_rate"),
        ):
            if value is not None:
                _nonnegative(value, name)
        _text(self.source_identity, "source_identity")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "AccountSnapshot":
        position = value.get("signed_position")
        if position is None:
            raw_position = _decimal(value.get("position"), "position")
            sign = int(value.get("sign", 1))
            position = raw_position * sign
        raw_orders = value.get("active_orders", ())
        return cls(
            account_index=_int(value.get("account_index"), "account_index", minimum=0),
            market_id=_int(value.get("market_id"), "market_id", minimum=0),
            signed_position=_decimal(position, "signed_position"),
            active_orders=tuple(
                item if isinstance(item, OrderSnapshot) else OrderSnapshot.from_mapping(item)
                for item in raw_orders
            ),
            observed_at=_timestamp(value.get("observed_at"), "observed_at"),
            authorized=_bool(value.get("authorized", False), "authorized"),
            ready=_bool(value.get("ready", False), "ready"),
            margin_available=(
                None
                if value.get("margin_available") is None
                else _nonnegative(value.get("margin_available"), "margin_available")
            ),
            margin_required=(
                None
                if value.get("margin_required") is None
                else _nonnegative(value.get("margin_required"), "margin_required")
            ),
            fee_rate=(
                None
                if value.get("fee_rate") is None
                else _nonnegative(value.get("fee_rate"), "fee_rate")
            ),
            source_identity=_text(value.get("source_identity"), "source_identity"),
        )


@dataclass(frozen=True, slots=True)
class TradeReceipt:
    trade_id: str
    account_index: int
    market_id: int
    order_id: str
    side: str
    quantity: Decimal
    price: Decimal
    fee: Decimal | None
    counterparty_account_index: int | None
    observed_at: float

    def __post_init__(self) -> None:
        _text(self.trade_id, "trade_id")
        _int(self.account_index, "account_index", minimum=0)
        _int(self.market_id, "market_id", minimum=0)
        _text(self.order_id, "order_id")
        if _text(self.side, "side").upper() not in {"BUY", "SELL"}:
            raise ContractError("trade side must be BUY or SELL")
        _positive(self.quantity, "quantity")
        _positive(self.price, "price")
        if self.fee is not None:
            _nonnegative(self.fee, "fee")
        if self.counterparty_account_index is not None:
            _int(self.counterparty_account_index, "counterparty_account_index", minimum=0)
        _timestamp(self.observed_at, "observed_at")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "TradeReceipt":
        return cls(
            trade_id=_text(
                value.get("trade_id", value.get("trade_id_str", value.get("trade_index"))),
                "trade_id",
            ),
            account_index=_int(value.get("account_index"), "account_index", minimum=0),
            market_id=_int(value.get("market_id"), "market_id", minimum=0),
            order_id=_text(
                value.get(
                    "order_id",
                    value.get(
                        "order_index",
                        value.get(
                            "ask_order_id",
                            value.get("bid_order_id", value.get("ask_id", value.get("bid_id"))),
                        ),
                    ),
                ),
                "order_id",
            ),
            side=_text(value.get("side", "BUY"), "side"),
            quantity=_positive(value.get("quantity", value.get("size", value.get("base_amount"))), "quantity"),
            price=_positive(value.get("price"), "price"),
            fee=(
                None
                if value.get("fee") is None
                else _nonnegative(value.get("fee"), "fee")
            ),
            counterparty_account_index=(
                None
                if value.get("counterparty_account_index") is None
                else _int(value.get("counterparty_account_index"), "counterparty_account_index", minimum=0)
            ),
            observed_at=_timestamp(value.get("observed_at", value.get("timestamp")), "observed_at"),
        )


@dataclass(frozen=True, slots=True)
class HistoryPage:
    trades: tuple[TradeReceipt, ...] = ()
    orders: tuple[OrderSnapshot, ...] = ()
    next_cursor: str | None = None
    complete: bool = True

    def __post_init__(self) -> None:
        if self.next_cursor is not None:
            _text(self.next_cursor, "next_cursor")
        if not isinstance(self.complete, bool):
            raise ContractError("complete must be bool")


@dataclass(frozen=True, slots=True)
class HandoffConfig:
    market_id: int
    direction: Direction
    quantity: Decimal
    source_limit_price: Decimal
    receiver_worst_price: Decimal
    max_gross_notional: Decimal
    source_fee_budget: Decimal
    receiver_fee_budget: Decimal
    freshness_seconds: float
    request_timeout_seconds: float
    order_timeout_seconds: float
    reconcile_timeout_seconds: float
    poll_interval_seconds: float
    max_poll_count: int
    source_order_lifetime_seconds: int
    client_order_prefix: str
    journal_path: str
    market_symbol: str = "HOOD"
    environment: str = "mainnet"
    operator_execution_opt_in: bool = False
    api_base_url: str | None = None
    api_key_index: int | None = None
    chain_id: int | None = None

    def __post_init__(self) -> None:
        _int(self.market_id, "market_id", minimum=0)
        direction = self.direction if isinstance(self.direction, Direction) else Direction(self.direction)
        object.__setattr__(self, "direction", direction)
        _positive(self.quantity, "quantity")
        _positive(self.source_limit_price, "source_limit_price")
        _positive(self.receiver_worst_price, "receiver_worst_price")
        _positive(self.max_gross_notional, "max_gross_notional")
        _nonnegative(self.source_fee_budget, "source_fee_budget")
        _nonnegative(self.receiver_fee_budget, "receiver_fee_budget")
        for value, name in (
            (self.freshness_seconds, "freshness_seconds"),
            (self.request_timeout_seconds, "request_timeout_seconds"),
            (self.order_timeout_seconds, "order_timeout_seconds"),
            (self.reconcile_timeout_seconds, "reconcile_timeout_seconds"),
            (self.poll_interval_seconds, "poll_interval_seconds"),
        ):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 < float(value) < float("inf"):
                raise ContractError(f"{name} must be finite and positive")
        _int(self.max_poll_count, "max_poll_count", minimum=1)
        _int(self.source_order_lifetime_seconds, "source_order_lifetime_seconds", minimum=300)
        if self.source_order_lifetime_seconds > 30 * 24 * 60 * 60:
            raise ContractError("source_order_lifetime_seconds must not exceed 30 days")
        _text(self.client_order_prefix, "client_order_prefix")
        _text(self.journal_path, "journal_path")
        if self.market_symbol.upper() != "HOOD":
            raise ContractError("only HOOD is supported")
        if self.environment.lower() not in {"mainnet", "production"}:
            raise ContractError("environment must be the future user-operated production venue")
        if not isinstance(self.operator_execution_opt_in, bool):
            raise ContractError("operator_execution_opt_in must be bool")
        if self.api_key_index is not None:
            _int(self.api_key_index, "api_key_index", minimum=4)
            if self.api_key_index > 254:
                raise ContractError("api_key_index must be in 4..254")
        if self.chain_id is not None:
            _int(self.chain_id, "chain_id", minimum=0)

    def integer_order_values(self, metadata: MarketMetadata) -> tuple[int, int, int]:
        return (
            decimal_to_integer(self.quantity, metadata.size_decimals, "quantity"),
            decimal_to_integer(self.source_limit_price, metadata.price_decimals, "source_limit_price"),
            decimal_to_integer(self.receiver_worst_price, metadata.price_decimals, "receiver_worst_price"),
        )

    def validate_against(self, metadata: MarketMetadata, source: AccountSnapshot, receiver: AccountSnapshot, now: float) -> None:
        if metadata.market_id != self.market_id or source.market_id != self.market_id or receiver.market_id != self.market_id:
            raise PreflightBlocked("market identity does not match configured HOOD market")
        if metadata.symbol.upper() != "HOOD":
            raise PreflightBlocked("market identity is not HOOD")
        if metadata.status.lower() not in {"active", "open", "online", "listed"}:
            raise PreflightBlocked("HOOD market is not active")
        try:
            quantity_int, _, _ = self.integer_order_values(metadata)
        except ContractError as exc:
            raise PreflightBlocked(str(exc)) from exc
        if quantity_int <= 0:
            raise PreflightBlocked("quantity is below the venue size grid")
        if self.quantity < metadata.minimum_base_amount:
            raise PreflightBlocked("quantity is below the documented base minimum")
        max_price = max(self.source_limit_price, self.receiver_worst_price)
        if self.quantity * max_price < metadata.minimum_quote_amount:
            raise PreflightBlocked("gross notional is below the documented quote minimum")
        if self.quantity * max_price > self.max_gross_notional:
            raise PreflightBlocked("gross notional exceeds configured bound")
        for snapshot, label in ((source, "source"), (receiver, "receiver")):
            if not snapshot.authorized or not snapshot.ready:
                raise PreflightBlocked(f"{label} account authorization/readiness is unproven")
            if snapshot.observed_at > now:
                raise PreflightBlocked(f"{label} account state is from the future")
            if now - snapshot.observed_at > self.freshness_seconds:
                raise PreflightBlocked(f"{label} account state is stale")
            if metadata.observed_at > now:
                raise PreflightBlocked("market metadata is from the future")
            if now - metadata.observed_at > self.freshness_seconds:
                raise PreflightBlocked("market metadata is stale")
            if not snapshot.source_identity:
                raise PreflightBlocked(f"{label} account identity is missing")
            if snapshot.active_orders:
                raise PreflightBlocked(f"{label} has active HOOD orders")
            if snapshot.margin_available is None or snapshot.margin_required is None:
                raise PreflightBlocked(f"{label} margin evidence is missing")
            if snapshot.margin_required > snapshot.margin_available:
                raise PreflightBlocked(f"{label} margin is insufficient")
        if source.account_index == receiver.account_index:
            raise PreflightBlocked("source and receiver accounts must differ")
        expected = self.direction.sign
        if expected > 0 and source.signed_position < self.quantity:
            raise PreflightBlocked("source long position is smaller than Q")
        if expected < 0 and source.signed_position > -self.quantity:
            raise PreflightBlocked("source short position is smaller than Q")
        if receiver.signed_position * expected < 0:
            raise PreflightBlocked("receiver has opposite HOOD exposure")
        if metadata.source_fee_rate is None or metadata.receiver_fee_rate is None:
            raise PreflightBlocked("current source/receiver fee evidence is missing")
        if source.fee_rate is None or receiver.fee_rate is None:
            raise PreflightBlocked("current account fee evidence is missing")
        source_fee = self.quantity * self.source_limit_price * source.fee_rate
        receiver_fee = self.quantity * self.receiver_worst_price * receiver.fee_rate
        if source_fee > self.source_fee_budget:
            raise PreflightBlocked("source fee exceeds configured budget")
        if receiver_fee > self.receiver_fee_budget:
            raise PreflightBlocked("receiver fee exceeds configured budget")
        if not metadata.margin_evidence:
            raise PreflightBlocked("minimum/margin evidence is missing")


@dataclass(frozen=True, slots=True)
class OrderPlan:
    account_index: int
    market_id: int
    side: str
    quantity: Decimal
    quantity_int: int
    price: Decimal
    price_int: int
    order_type: str
    time_in_force: str
    reduce_only: bool
    order_expiry_ms: int
    client_order_index: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "account_index": self.account_index,
            "market_id": self.market_id,
            "side": self.side,
            "quantity": _wire_decimal(self.quantity),
            "quantity_int": self.quantity_int,
            "price": _wire_decimal(self.price),
            "price_int": self.price_int,
            "order_type": self.order_type,
            "time_in_force": self.time_in_force,
            "reduce_only": self.reduce_only,
            "order_expiry_ms": self.order_expiry_ms,
            "client_order_index": self.client_order_index,
        }


@dataclass(frozen=True, slots=True)
class HandoffPlan:
    run_id: str
    source: OrderPlan
    receiver: OrderPlan
    source_position_before: Decimal
    receiver_position_before: Decimal
    direction: Direction
    quantity: Decimal
    created_at: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "source": self.source.as_dict(),
            "receiver": self.receiver.as_dict(),
            "source_position_before": _wire_decimal(self.source_position_before),
            "receiver_position_before": _wire_decimal(self.receiver_position_before),
            "direction": self.direction.value,
            "quantity": _wire_decimal(self.quantity),
            "created_at": self.created_at,
        }


@dataclass(frozen=True, slots=True)
class MutationReceipt:
    accepted: bool
    order_id: str | None
    tx_hash: str | None
    error: str | None = None
    response_code: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.accepted, bool):
            raise ContractError("accepted must be bool")
        if self.order_id is not None:
            _text(self.order_id, "order_id")
        if self.tx_hash is not None:
            _text(self.tx_hash, "tx_hash")
        if self.error is not None:
            _text(self.error, "error")
        if self.response_code is not None:
            _int(self.response_code, "response_code", minimum=100)


@dataclass(frozen=True, slots=True)
class LegReconciliation:
    account_index: int
    order_id: str | None
    trades: tuple[TradeReceipt, ...]
    position_before: Decimal
    position_after: Decimal | None
    order: OrderSnapshot | None
    history_complete: bool
    unknown_reasons: tuple[str, ...] = ()

    @property
    def filled_quantity(self) -> Decimal:
        return sum((trade.quantity for trade in self.trades), Decimal(0))

    @property
    def fee_total(self) -> Decimal | None:
        if any(trade.fee is None for trade in self.trades):
            return None
        return sum((trade.fee or Decimal(0) for trade in self.trades), Decimal(0))

    def quantity_against(self, account_index: int) -> Decimal:
        """Quantity whose receipt names the other HCR account explicitly."""

        return sum(
            (
                trade.quantity
                for trade in self.trades
                if trade.counterparty_account_index == account_index
            ),
            Decimal(0),
        )


@dataclass(frozen=True, slots=True)
class HandoffResult:
    outcome: Outcome
    phase: Phase
    run_id: str
    plan: HandoffPlan | None
    source: LegReconciliation | None
    receiver: LegReconciliation | None
    reason: str | None = None
    unknown_reasons: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        def leg(value: LegReconciliation | None, counterparty: LegReconciliation | None = None) -> Any:
            if value is None:
                return None
            return {
                "account_index": value.account_index,
                "order_id": value.order_id,
                "filled_quantity": _wire_decimal(value.filled_quantity),
                "counterparty_matched_quantity": (
                    None
                    if counterparty is None
                    else _wire_decimal(value.quantity_against(counterparty.account_index))
                ),
                "fee_total": None if value.fee_total is None else _wire_decimal(value.fee_total),
                "position_before": _wire_decimal(value.position_before),
                "position_after": None if value.position_after is None else _wire_decimal(value.position_after),
                "history_complete": value.history_complete,
                "unknown_reasons": list(value.unknown_reasons),
                "trades": [
                    {
                        "trade_id": trade.trade_id,
                        "order_id": trade.order_id,
                        "side": trade.side,
                        "quantity": _wire_decimal(trade.quantity),
                        "price": _wire_decimal(trade.price),
                        "fee": None if trade.fee is None else _wire_decimal(trade.fee),
                        "counterparty_account_index": trade.counterparty_account_index,
                        "observed_at": trade.observed_at,
                    }
                    for trade in value.trades
                ],
                "order": None if value.order is None else {
                    "order_id": value.order.order_id,
                    "status": value.order.status,
                    "remaining_quantity": _wire_decimal(value.order.remaining_quantity),
                    "filled_quantity": _wire_decimal(value.order.filled_quantity),
                },
            }

        return {
            "outcome": self.outcome.value,
            "phase": self.phase.value,
            "run_id": self.run_id,
            "plan": None if self.plan is None else self.plan.as_dict(),
            "source": leg(self.source, self.receiver),
            "receiver": leg(self.receiver, self.source),
            "reason": self.reason,
            "unknown_reasons": list(self.unknown_reasons),
        }


def now_utc_seconds() -> float:
    return datetime.now(timezone.utc).timestamp()


__all__ = [
    "ACTIVE_ORDER_STATUSES",
    "AccountSnapshot",
    "ContractError",
    "Direction",
    "HandoffConfig",
    "HandoffPlan",
    "HandoffResult",
    "HistoryPage",
    "LegReconciliation",
    "MarketMetadata",
    "MutationReceipt",
    "OrderPlan",
    "OrderSnapshot",
    "Outcome",
    "Phase",
    "PreflightBlocked",
    "TERMINAL_ORDER_STATUSES",
    "TradeReceipt",
    "decimal_to_integer",
    "now_utc_seconds",
]
