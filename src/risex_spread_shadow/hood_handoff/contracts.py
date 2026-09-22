"""Fail-closed contracts for the one-attempt HOOD close/reopen operation.

The legacy scanner does not import this package.  Everything in this module is
plain data and validation; network clients and signing live in :mod:`sdk`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit


OFFICIAL_MAINNET_API_URL = "https://mainnet.zklighter.elliot.ai"
OFFICIAL_MAINNET_CHAIN_ID = 304
OFFICIAL_ROBINHOOD_API_URL = "https://api.rh.lighter.xyz"
OFFICIAL_ROBINHOOD_CHAIN_ID = 466324
ROBINHOOD_WEBSITE_URL = "https://robinhoodchain.lighter.xyz"
# Initial public compatibility targets.  Runtime configuration remains
# open-ended and must resolve the exact current perpetual catalog entry.
ROBINHOOD_PERPETUAL_SYMBOLS = frozenset({"BTC", "ETH"})
SUPPORTED_TIME_IN_FORCE = frozenset({"IOC", "GTT", "POST_ONLY"})
SUPPORTED_ORDER_TYPES = frozenset({"LIMIT", "MARKET"})


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


class OperationMode(StrEnum):
    """The explicit exposure operation represented by one bounded attempt."""

    CLOSE_REOPEN = "CLOSE_REOPEN"
    PAIRED_OPENING = "PAIRED_OPENING"
    PAIRED_CLOSING = "PAIRED_CLOSING"

    @classmethod
    def parse(cls, value: Any) -> "OperationMode":
        if isinstance(value, cls):
            return value
        if not isinstance(value, str):
            raise ContractError(
                "operation_mode must be CLOSE_REOPEN, PAIRED_OPENING, or PAIRED_CLOSING"
            )
        normalized = value.strip().upper().replace("-", "_").replace(" ", "_")
        aliases = {
            "CLOSE": cls.CLOSE_REOPEN,
            "CLOSE_REOPEN": cls.CLOSE_REOPEN,
            "CLOSE_REOPENING": cls.CLOSE_REOPEN,
            "PAIRED": cls.PAIRED_OPENING,
            "PAIRED_OPEN": cls.PAIRED_OPENING,
            "PAIRED_OPENING": cls.PAIRED_OPENING,
            "PAIRED_CLOSE": cls.PAIRED_CLOSING,
            "PAIRED_CLOSING": cls.PAIRED_CLOSING,
            "PAIRED_REDUCE": cls.PAIRED_CLOSING,
        }
        try:
            return aliases[normalized]
        except KeyError as exc:
            raise ContractError(
                "operation_mode must be CLOSE_REOPEN, PAIRED_OPENING, or PAIRED_CLOSING"
            ) from exc


# A short public alias keeps call sites readable while the serialized contract
# remains the explicit operation_mode field.
HandoffMode = OperationMode


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
        "canceled-post-only",
        "canceled-reduce-only",
        "canceled-invalid-balance",
        "canceled-position-not-allowed",
        "canceled-margin-not-allowed",
        "canceled-too-much-slippage",
        "canceled-not-enough-liquidity",
        "canceled-self-trade",
        "canceled-expired",
        "canceled-oco",
        "canceled-child",
        "canceled-liquidation",
    }
)
ACTIVE_ORDER_STATUSES = frozenset({"open", "pending", "in-progress"})


class ContractError(ValueError):
    """A fail-closed contract or schema error."""


class PreflightBlocked(ContractError):
    """A required pre-mutation condition is not proven."""


def _decimal(value: Any, name: str) -> Decimal:
    if isinstance(value, bool):
        raise ContractError(f"{name} must be a finite decimal")
    if isinstance(value, float):
        raise ContractError(f"{name} must use an exact decimal string or Decimal, not float")
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


def _finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise ContractError(f"{name} must be a finite positive number")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ContractError(f"{name} must be a finite positive number") from exc
    if not 0 < result < float("inf"):
        raise ContractError(f"{name} must be a finite positive number")
    return result


def _normalize_order_type(value: Any, name: str = "order_type") -> str:
    if isinstance(value, bool):
        raise ContractError(f"{name} must be LIMIT or MARKET")
    if isinstance(value, int):
        value = {0: "LIMIT", 1: "MARKET"}.get(value)
    if not isinstance(value, str):
        raise ContractError(f"{name} must be LIMIT or MARKET")
    normalized = value.strip().upper().replace("_", "-")
    normalized = {"LIMIT": "LIMIT", "MARKET": "MARKET"}.get(normalized, normalized)
    if normalized not in SUPPORTED_ORDER_TYPES:
        raise ContractError(f"{name} has unsupported value")
    return normalized


def _normalize_time_in_force(value: Any, name: str = "time_in_force") -> str:
    if isinstance(value, bool):
        raise ContractError(f"{name} has unsupported value")
    if isinstance(value, int):
        value = {0: "IOC", 1: "GTT", 2: "POST_ONLY"}.get(value)
    if not isinstance(value, str):
        raise ContractError(f"{name} has unsupported value")
    normalized = value.strip().upper().replace("-", "_")
    normalized = {
        "IMMEDIATE_OR_CANCEL": "IOC",
        "IOC": "IOC",
        "GOOD_TILL_TIME": "GTT",
        "GTT": "GTT",
        "POST_ONLY": "POST_ONLY",
    }.get(normalized, normalized)
    if normalized not in SUPPORTED_TIME_IN_FORCE:
        raise ContractError(f"{name} has unsupported value")
    return normalized


def _normalize_status(value: Any) -> str:
    normalized = _text(value, "status").lower().replace("_", "-")
    known = ACTIVE_ORDER_STATUSES | TERMINAL_ORDER_STATUSES
    if normalized not in known:
        raise ContractError("order status is not an official active or terminal value")
    return normalized


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
    market_type: str = "perp"
    venue: str = ""

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
        market_type = _text(self.market_type, "market_type").lower()
        if market_type != "perp":
            raise ContractError("market_type must be perp")
        object.__setattr__(self, "market_type", market_type)
        if self.venue in (None, ""):
            object.__setattr__(self, "venue", "")
        else:
            object.__setattr__(self, "venue", _text(self.venue, "venue").lower())

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
            market_type=_text(value.get("market_type", "perp"), "market_type").lower(),
            venue=(
                ""
                if value.get("venue") in (None, "")
                else _text(value.get("venue"), "venue").lower()
            ),
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
        object.__setattr__(self, "status", _normalize_status(self.status))
        side = _text(self.side, "side").upper()
        if side not in {"BUY", "SELL"}:
            raise ContractError("order side must be BUY or SELL")
        object.__setattr__(self, "side", side)
        object.__setattr__(self, "order_type", _normalize_order_type(self.order_type))
        object.__setattr__(self, "time_in_force", _normalize_time_in_force(self.time_in_force))
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

        raw_price = pick("price", "base_price")
        if raw_price is None:
            raise ContractError("order price is required")
        return cls(
            account_index=_int(pick("account_index", "owner_account_index"), "account_index", minimum=0),
            market_id=_int(pick("market_id", "market_index"), "market_id", minimum=0),
            order_id=_text(pick("order_id", "order_index"), "order_id"),
            client_order_index=pick("client_order_index", "client_order_id"),
            status=_normalize_status(pick("status")),
            side=_text(pick("side"), "side"),
            order_type=_normalize_order_type(pick("type", "order_type"), "order_type"),
            time_in_force=_normalize_time_in_force(pick("time_in_force"), "time_in_force"),
            reduce_only=_bool(pick("reduce_only"), "reduce_only"),
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
            price=_positive(raw_price, "price"),
            observed_at=_timestamp(pick("observed_at", "updated_at", "timestamp"), "observed_at"),
        )


def _optional_nonnegative_text(value: Any, name: str) -> str | None:
    """Validate a documented numeric string while preserving its raw text."""

    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{name} must be a non-negative numeric string")
    parsed = _nonnegative(value, name)
    del parsed
    return value


@dataclass(frozen=True, slots=True)
class AccountMarginEvidence:
    """Whitelisted, source-bound margin fields from one account response.

    Margin strings are deliberately retained as raw documented values.  Their
    response units are not inferred from the signing client's basis-point
    input encoding.  ``invalid_fields`` records rejected optional fields while
    keeping the rest of the account response usable for diagnostics.
    """

    account_index: int
    market_id: int
    source_identity: str
    observed_at: float
    selected_position_present: bool | None
    selected_margin_mode: int | None
    selected_initial_margin_fraction: str | None
    selected_allocated_margin: str | None
    selected_open_order_count: int | None
    selected_pending_order_count: int | None
    selected_position_tied_order_count: int | None
    account_total_order_count: int | None
    account_pending_order_count: int | None
    account_total_isolated_order_count: int | None
    account_cross_asset_value: str | None
    account_cross_initial_margin_requirement: str | None
    account_cross_maintenance_margin_requirement: str | None
    invalid_fields: tuple[str, ...] = ()
    source: str = "account response"
    sdk_version: str | None = None

    def __post_init__(self) -> None:
        _int(self.account_index, "account_index", minimum=0)
        _int(self.market_id, "market_id", minimum=0)
        _text(self.source_identity, "source_identity")
        _timestamp(self.observed_at, "observed_at")
        if self.selected_position_present is not None and not isinstance(self.selected_position_present, bool):
            raise ContractError("selected_position_present must be bool or None")
        if self.selected_margin_mode is not None:
            _int(self.selected_margin_mode, "selected_margin_mode", minimum=0)
        for value, name in (
            (self.selected_initial_margin_fraction, "selected_initial_margin_fraction"),
            (self.selected_allocated_margin, "selected_allocated_margin"),
            (self.account_cross_asset_value, "account_cross_asset_value"),
            (self.account_cross_initial_margin_requirement, "account_cross_initial_margin_requirement"),
            (self.account_cross_maintenance_margin_requirement, "account_cross_maintenance_margin_requirement"),
        ):
            _optional_nonnegative_text(value, name)
        for value, name in (
            (self.selected_open_order_count, "selected_open_order_count"),
            (self.selected_pending_order_count, "selected_pending_order_count"),
            (self.selected_position_tied_order_count, "selected_position_tied_order_count"),
            (self.account_total_order_count, "account_total_order_count"),
            (self.account_pending_order_count, "account_pending_order_count"),
            (self.account_total_isolated_order_count, "account_total_isolated_order_count"),
        ):
            if value is not None:
                _int(value, name, minimum=0)
        if not isinstance(self.invalid_fields, tuple) or any(
            not isinstance(item, str) or not item for item in self.invalid_fields
        ):
            raise ContractError("invalid_fields must be a tuple of non-empty names")
        _text(self.source, "source")
        if self.sdk_version is not None:
            _text(self.sdk_version, "sdk_version")

    @classmethod
    def from_response(
        cls,
        *,
        account_index: int,
        market_id: int,
        source_identity: str,
        observed_at: float,
        selected_position: Mapping[str, Any] | None,
        account: Mapping[str, Any],
        position_rows: Sequence[Mapping[str, Any]] | None = None,
        source: str = "account response",
        sdk_version: str | None = None,
    ) -> "AccountMarginEvidence":
        """Read only the documented margin/count keys from an account row."""

        invalid: list[str] = []

        def raw_text(
            mapping: Mapping[str, Any], name: str, *, invalid_name: str | None = None
        ) -> str | None:
            if name not in mapping or mapping[name] is None:
                return None
            value = mapping[name]
            if not isinstance(value, str) or not value.strip():
                invalid.append(invalid_name or name)
                return None
            try:
                _optional_nonnegative_text(value, name)
            except ContractError:
                invalid.append(invalid_name or name)
                return None
            return value

        def raw_count(
            mapping: Mapping[str, Any], name: str, *, invalid_name: str | None = None
        ) -> int | None:
            if name not in mapping or mapping[name] is None:
                return None
            value = mapping[name]
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                invalid.append(invalid_name or name)
                return None
            return value

        def raw_mode(
            mapping: Mapping[str, Any], name: str, *, invalid_name: str | None = None
        ) -> int | None:
            if name not in mapping or mapping[name] is None:
                return None
            value = mapping[name]
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                invalid.append(invalid_name or name)
                return None
            return value

        selected = selected_position or {}
        selected_present = selected_position is not None
        selected_open = raw_count(selected, "open_order_count") if selected_present else None
        selected_pending = (
            raw_count(
                selected,
                "pending_order_count",
                invalid_name="selected_pending_order_count",
            )
            if selected_present
            else None
        )
        selected_tied = raw_count(selected, "position_tied_order_count") if selected_present else None
        account_total = raw_count(account, "total_order_count")
        account_pending = raw_count(
            account,
            "pending_order_count",
            invalid_name="account_pending_order_count",
        )
        account_isolated = raw_count(account, "total_isolated_order_count")
        account_open_lower_bound = selected_open
        account_pending_lower_bound = selected_pending
        raw_rows: Any = position_rows
        if raw_rows is None:
            raw_rows = account.get("positions", ())
        if not isinstance(raw_rows, (list, tuple)):
            invalid.append("position_rows")
            raw_rows = ()
        for row in raw_rows:
            if not isinstance(row, Mapping):
                invalid.append("position_rows")
                continue
            # The selected row was parsed above so its field-specific invalid
            # names remain distinguishable from account-wide fields.
            if selected_position is not None and row is selected_position:
                continue
            row_open = raw_count(row, "open_order_count", invalid_name="position_row_counts")
            row_pending = raw_count(row, "pending_order_count", invalid_name="position_row_counts")
            row_tied = raw_count(row, "position_tied_order_count", invalid_name="position_row_counts")
            if row_pending is not None and row_open is not None and row_pending > row_open:
                invalid.append("position_row_counts")
            if row_tied is not None and row_open is not None and row_tied > row_open:
                invalid.append("position_row_counts")
            if row_open is not None:
                account_open_lower_bound = (
                    row_open
                    if account_open_lower_bound is None
                    else max(account_open_lower_bound, row_open)
                )
            if row_pending is not None:
                account_pending_lower_bound = (
                    row_pending
                    if account_pending_lower_bound is None
                    else max(account_pending_lower_bound, row_pending)
                )
        if (
            selected_pending is not None
            and selected_open is not None
            and selected_pending > selected_open
        ):
            invalid.append("selected_order_counts")
        if (
            selected_tied is not None
            and selected_open is not None
            and selected_tied > selected_open
        ):
            invalid.append("selected_order_counts")
        if (
            account_total is not None
            and account_pending is not None
            and account_pending > account_total
        ):
            invalid.append("account_order_counts")
        if (
            account_total is not None
            and account_isolated is not None
            and account_isolated > account_total
        ):
            invalid.append("account_order_counts")
        if account_total is not None and account_open_lower_bound is not None and account_open_lower_bound > account_total:
            invalid.append("account_order_counts")
        if account_pending is not None and account_pending_lower_bound is not None and account_pending_lower_bound > account_pending:
            invalid.append("account_order_counts")
        return cls(
            account_index=account_index,
            market_id=market_id,
            source_identity=source_identity,
            observed_at=observed_at,
            selected_position_present=selected_present,
            selected_margin_mode=raw_mode(selected, "margin_mode") if selected_present else None,
            selected_initial_margin_fraction=(
                raw_text(selected, "initial_margin_fraction") if selected_present else None
            ),
            selected_allocated_margin=raw_text(selected, "allocated_margin") if selected_present else None,
            selected_open_order_count=selected_open,
            selected_pending_order_count=selected_pending,
            selected_position_tied_order_count=selected_tied,
            account_total_order_count=account_total,
            account_pending_order_count=account_pending,
            account_total_isolated_order_count=account_isolated,
            account_cross_asset_value=raw_text(account, "cross_asset_value"),
            account_cross_initial_margin_requirement=raw_text(account, "cross_initial_margin_requirement"),
            account_cross_maintenance_margin_requirement=raw_text(
                account, "cross_maintenance_margin_requirement"
            ),
            invalid_fields=tuple(dict.fromkeys(invalid)),
            source=source,
            sdk_version=sdk_version,
        )

    @classmethod
    def unavailable(
        cls,
        *,
        account_index: int,
        market_id: int,
        source_identity: str,
        observed_at: float,
    ) -> "AccountMarginEvidence":
        """Create an explicit unavailable record for legacy/synthetic clients."""

        return cls(
            account_index=account_index,
            market_id=market_id,
            source_identity=source_identity,
            observed_at=observed_at,
            selected_position_present=None,
            selected_margin_mode=None,
            selected_initial_margin_fraction=None,
            selected_allocated_margin=None,
            selected_open_order_count=None,
            selected_pending_order_count=None,
            selected_position_tied_order_count=None,
            account_total_order_count=None,
            account_pending_order_count=None,
            account_total_isolated_order_count=None,
            account_cross_asset_value=None,
            account_cross_initial_margin_requirement=None,
            account_cross_maintenance_margin_requirement=None,
            source="not returned",
            sdk_version=None,
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "AccountMarginEvidence":
        """Construct a validated evidence record from a synthetic response."""

        selected = value.get("selected_position")
        if selected is not None and not isinstance(selected, Mapping):
            selected = None
        account = value.get("account")
        if not isinstance(account, Mapping):
            account = value
        return cls.from_response(
            account_index=_int(value.get("account_index"), "account_index", minimum=0),
            market_id=_int(value.get("market_id"), "market_id", minimum=0),
            source_identity=_text(value.get("source_identity"), "source_identity"),
            observed_at=_timestamp(value.get("observed_at"), "observed_at"),
            selected_position=selected,
            account=account,
            source=_text(value.get("source", "account response"), "source"),
            sdk_version=(
                None
                if value.get("sdk_version") is None
                else _text(value.get("sdk_version"), "sdk_version")
            ),
        )

    @property
    def incomplete(self) -> bool:
        if self.selected_margin_mode not in (0, 1):
            return True
        required = (
            self.selected_position_present,
            self.selected_margin_mode,
            self.selected_initial_margin_fraction,
            self.selected_allocated_margin,
            self.account_total_order_count,
            self.account_pending_order_count,
            self.account_total_isolated_order_count,
            self.account_cross_asset_value,
            self.account_cross_initial_margin_requirement,
        )
        return any(value is None for value in required)

    @property
    def status(self) -> str:
        if self.source == "not returned":
            return "UNAVAILABLE"
        if self.invalid_fields:
            return "INVALID"
        if self.selected_position_present is False:
            return "POSITION_ROW_ABSENT"
        if self.incomplete:
            return "INCOMPLETE"
        return "OBSERVED"

    @staticmethod
    def _field(
        name: str,
        value: Any,
        invalid_fields: tuple[str, ...],
        units: str,
        *,
        invalid_name: str | None = None,
    ) -> dict[str, Any]:
        invalid = (invalid_name or name) in invalid_fields or name in invalid_fields
        present = invalid or value is not None
        return {
            "present": present,
            "valid": False if invalid else (True if present else None),
            "raw": None if invalid else value,
            "units": units,
        }

    def as_dict(self) -> dict[str, Any]:
        invalid = self.invalid_fields
        margin_mode = self._field("margin_mode", self.selected_margin_mode, invalid, "enum_raw")
        if margin_mode["valid"] is False or not margin_mode["present"]:
            margin_mode["label"] = None
        else:
            margin_mode["label"] = {0: "cross", 1: "isolated"}.get(
                self.selected_margin_mode, "unknown"
            )
        return {
            "status": self.status,
            "binding": {
                "account_index": self.account_index,
                "market_id": self.market_id,
                "source_identity": self.source_identity,
            },
            "provenance": {
                "source": self.source,
                "sdk_version": self.sdk_version,
                "observed_at": self.observed_at,
            },
            "selected_position": {
                "present": self.selected_position_present,
                "margin_mode": margin_mode,
                "initial_margin_fraction": self._field(
                    "initial_margin_fraction", self.selected_initial_margin_fraction, invalid, "unverified"
                ),
                "allocated_margin": self._field(
                    "allocated_margin", self.selected_allocated_margin, invalid, "unverified"
                ),
                "open_order_count": self._field("open_order_count", self.selected_open_order_count, invalid, "count"),
                "pending_order_count": self._field(
                    "pending_order_count",
                    self.selected_pending_order_count,
                    invalid,
                    "count",
                    invalid_name="selected_pending_order_count",
                ),
                "position_tied_order_count": self._field(
                    "position_tied_order_count", self.selected_position_tied_order_count, invalid, "count"
                ),
            },
            "account": {
                "total_order_count": self._field(
                    "total_order_count", self.account_total_order_count, invalid, "count"
                ),
                "pending_order_count": self._field(
                    "pending_order_count",
                    self.account_pending_order_count,
                    invalid,
                    "count",
                    invalid_name="account_pending_order_count",
                ),
                "total_isolated_order_count": self._field(
                    "total_isolated_order_count", self.account_total_isolated_order_count, invalid, "count"
                ),
                "cross_asset_value": self._field(
                    "cross_asset_value", self.account_cross_asset_value, invalid, "unverified"
                ),
                "cross_initial_margin_requirement": self._field(
                    "cross_initial_margin_requirement",
                    self.account_cross_initial_margin_requirement,
                    invalid,
                    "unverified",
                ),
                "cross_maintenance_margin_requirement": self._field(
                    "cross_maintenance_margin_requirement",
                    self.account_cross_maintenance_margin_requirement,
                    invalid,
                    "unverified",
                ),
            },
            "invalid_fields": list(invalid),
            "units_note": "Margin numeric response units are unverified; no signing input encoding was applied.",
        }


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
    incremental_margin_required: Decimal | None = None
    incremental_margin_evidence: str = ""
    margin_evidence: AccountMarginEvidence | None = None
    # The official account response names this quote-currency field
    # ``available_balance``.  ``margin_available`` remains the established
    # internal alias used by the HCR-1 admission checks; keeping both values
    # explicit lets random-cycle sizing require the documented field without
    # changing the existing engine contract.
    available_balance: Decimal | None = None

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
            (self.available_balance, "available_balance"),
        ):
            if value is not None:
                _nonnegative(value, name)
        _text(self.source_identity, "source_identity")
        if self.incremental_margin_required is not None:
            _nonnegative(self.incremental_margin_required, "incremental_margin_required")
        if self.incremental_margin_evidence:
            _text(self.incremental_margin_evidence, "incremental_margin_evidence")
        if self.margin_evidence is not None:
            if not isinstance(self.margin_evidence, AccountMarginEvidence):
                raise ContractError("margin_evidence must be AccountMarginEvidence or None")
            if (
                self.margin_evidence.account_index != self.account_index
                or self.margin_evidence.market_id != self.market_id
                or self.margin_evidence.source_identity != self.source_identity
            ):
                raise ContractError("margin_evidence identity does not match account snapshot")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "AccountSnapshot":
        position = value.get("signed_position")
        if position is None:
            raw_position = _decimal(value.get("position"), "position")
            raw_sign = value.get("sign")
            if isinstance(raw_sign, bool) or not isinstance(raw_sign, int) or raw_sign not in {-1, 1}:
                raise ContractError("sign must be -1 or 1 when signed_position is absent")
            sign = raw_sign
            position = raw_position * sign
        raw_orders = value.get("active_orders", ())
        raw_incremental_evidence = value.get("incremental_margin_evidence")
        raw_margin_evidence = value.get("margin_evidence")
        margin_evidence = (
            None
            if raw_margin_evidence is None
            else raw_margin_evidence
            if isinstance(raw_margin_evidence, AccountMarginEvidence)
            else AccountMarginEvidence.from_mapping(raw_margin_evidence)
        )
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
                if value.get("margin_available", value.get("available_balance")) is None
                else _nonnegative(
                    value.get("margin_available", value.get("available_balance")),
                    "margin_available",
                )
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
            incremental_margin_required=(
                None
                if value.get("incremental_margin_required") is None
                else _nonnegative(value.get("incremental_margin_required"), "incremental_margin_required")
            ),
            incremental_margin_evidence=(
                ""
                if raw_incremental_evidence in (None, "")
                else _text(raw_incremental_evidence, "incremental_margin_evidence")
            ),
            margin_evidence=margin_evidence,
            available_balance=(
                None
                if value.get("available_balance") is None
                else _nonnegative(
                    value.get("available_balance"),
                    "available_balance",
                )
            ),
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
    counterparty_order_id: str | None = None
    counterparty_client_order_index: int | str | None = None
    client_order_index: int | str | None = None

    def __post_init__(self) -> None:
        _text(self.trade_id, "trade_id")
        _int(self.account_index, "account_index", minimum=0)
        _int(self.market_id, "market_id", minimum=0)
        _text(self.order_id, "order_id")
        side = _text(self.side, "side").upper()
        if side not in {"BUY", "SELL"}:
            raise ContractError("trade side must be BUY or SELL")
        object.__setattr__(self, "side", side)
        _positive(self.quantity, "quantity")
        _positive(self.price, "price")
        if self.fee is not None:
            _nonnegative(self.fee, "fee")
        if self.counterparty_account_index is not None:
            _int(self.counterparty_account_index, "counterparty_account_index", minimum=0)
        if self.counterparty_order_id is not None:
            _text(self.counterparty_order_id, "counterparty_order_id")
        if self.counterparty_client_order_index is not None and (
            isinstance(self.counterparty_client_order_index, bool)
            or not isinstance(self.counterparty_client_order_index, (int, str))
            or (
                isinstance(self.counterparty_client_order_index, str)
                and not self.counterparty_client_order_index.strip()
            )
        ):
            raise ContractError("counterparty_client_order_index must be a non-empty integer/string")
        if self.client_order_index is not None and (
            isinstance(self.client_order_index, bool)
            or not isinstance(self.client_order_index, (int, str))
            or (isinstance(self.client_order_index, str) and not self.client_order_index.strip())
        ):
            raise ContractError("client_order_index must be a non-empty integer/string")
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
            side=_text(value.get("side"), "side"),
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
            counterparty_order_id=(
                None
                if value.get("counterparty_order_id") is None
                else _text(value.get("counterparty_order_id"), "counterparty_order_id")
            ),
            counterparty_client_order_index=value.get("counterparty_client_order_index"),
            client_order_index=value.get("client_order_index"),
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
    operator_plan_reviewed: bool = False
    api_base_url: str | None = OFFICIAL_MAINNET_API_URL
    api_key_index: int | None = None
    chain_id: int | None = OFFICIAL_MAINNET_CHAIN_ID
    auth_token_lifetime_seconds: int = 600
    # Accepted only to read legacy cap-bearing configuration.  HCR-1 no longer
    # uses these fields for preflight, dispatch admission, or outcome status.
    max_gross_notional: Decimal | None = None
    source_fee_budget: Decimal | None = None
    receiver_fee_budget: Decimal | None = None
    receiver_price_cap: Decimal | None = None
    operation_mode: OperationMode | str = OperationMode.CLOSE_REOPEN
    # ``mode`` is accepted as a concise input alias.  It is normalized to the
    # same canonical operation_mode and is never used as a second policy.
    mode: OperationMode | str | None = None
    # Series children bind their exact expected pre-mutation positions here.
    # Direct one-attempt calls leave both unset; paired opening then requires
    # both selected-market positions to be flat.
    expected_source_position: Decimal | None = None
    expected_receiver_position: Decimal | None = None
    # HCR-8 opt-in: leave only the local incremental opening-margin estimate
    # unperformed.  The default remains fail-closed.
    defer_incremental_margin_calculation: bool = False
    # Optional parent lineage ordinal for bounded paired retries.
    attempt_index: int | None = None
    # Observation time of the public quote used to select the source price.
    # This is evidence only; when supplied it participates in the existing
    # freshness/deadline binding and never authorizes repricing.
    source_quote_observed_at: float | None = None
    max_quote_age_seconds: float | None = None
    max_source_to_receiver_seconds: float | None = None

    def __post_init__(self) -> None:
        _int(self.market_id, "market_id", minimum=0)
        try:
            direction = self.direction if isinstance(self.direction, Direction) else Direction(self.direction)
        except (TypeError, ValueError) as exc:
            raise ContractError("direction must be LONG or SHORT") from exc
        object.__setattr__(self, "direction", direction)
        operation_mode = OperationMode.parse(self.operation_mode)
        if self.mode is not None:
            mode_value = OperationMode.parse(self.mode)
            if operation_mode is not OperationMode.CLOSE_REOPEN and operation_mode is not mode_value:
                raise ContractError("mode conflicts with operation_mode")
            operation_mode = mode_value
        object.__setattr__(self, "operation_mode", operation_mode)
        if self.mode is not None:
            object.__setattr__(self, "mode", operation_mode)
        if (
            self.defer_incremental_margin_calculation is True
            and operation_mode is not OperationMode.PAIRED_OPENING
        ):
            raise ContractError(
                "defer_incremental_margin_calculation is only supported for PAIRED_OPENING"
            )
        for field_name in ("expected_source_position", "expected_receiver_position"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, _decimal(value, field_name))
        for field_name in (
            "quantity",
            "source_limit_price",
            "receiver_worst_price",
        ):
            object.__setattr__(self, field_name, _positive(getattr(self, field_name), field_name))
        if self.source_quote_observed_at is not None:
            object.__setattr__(
                self,
                "source_quote_observed_at",
                _timestamp(self.source_quote_observed_at, "source_quote_observed_at"),
            )
        if self.max_gross_notional is not None:
            object.__setattr__(self, "max_gross_notional", _positive(self.max_gross_notional, "max_gross_notional"))
        for field_name in ("source_fee_budget", "receiver_fee_budget"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, _nonnegative(value, field_name))
        if self.receiver_price_cap is not None:
            object.__setattr__(self, "receiver_price_cap", _positive(self.receiver_price_cap, "receiver_price_cap"))
        for value, name in (
            (self.freshness_seconds, "freshness_seconds"),
            (self.request_timeout_seconds, "request_timeout_seconds"),
            (self.order_timeout_seconds, "order_timeout_seconds"),
            (self.reconcile_timeout_seconds, "reconcile_timeout_seconds"),
            (self.poll_interval_seconds, "poll_interval_seconds"),
        ):
            object.__setattr__(self, name, _finite_float(value, name))
        _int(self.max_poll_count, "max_poll_count", minimum=1)
        for name in ("max_quote_age_seconds", "max_source_to_receiver_seconds"):
            value = getattr(self, name)
            if value is not None:
                value = _finite_float(value, name)
                if value > self.freshness_seconds:
                    raise ContractError(f"{name} must not exceed freshness_seconds")
                object.__setattr__(self, name, value)
        if self.max_quote_age_seconds is not None and self.source_quote_observed_at is None:
            raise ContractError("max_quote_age_seconds requires source_quote_observed_at")
        _int(self.source_order_lifetime_seconds, "source_order_lifetime_seconds", minimum=300)
        if self.source_order_lifetime_seconds > 30 * 24 * 60 * 60:
            raise ContractError("source_order_lifetime_seconds must not exceed 30 days")
        _text(self.client_order_prefix, "client_order_prefix")
        _text(self.journal_path, "journal_path")
        market_symbol = _text(self.market_symbol, "market_symbol").upper()
        object.__setattr__(self, "market_symbol", market_symbol)
        environment = _text(self.environment, "environment").lower()
        object.__setattr__(self, "environment", environment)
        if environment not in {"mainnet", "production", "robinhood"}:
            raise ContractError("environment must be mainnet, production, or robinhood")
        if not isinstance(self.operator_execution_opt_in, bool):
            raise ContractError("operator_execution_opt_in must be bool")
        if not isinstance(self.operator_plan_reviewed, bool):
            raise ContractError("operator_plan_reviewed must be bool")
        if not isinstance(self.defer_incremental_margin_calculation, bool):
            raise ContractError("defer_incremental_margin_calculation must be bool")
        if self.attempt_index is not None:
            _int(self.attempt_index, "attempt_index", minimum=1)
        if self.api_key_index is not None:
            _int(self.api_key_index, "api_key_index", minimum=4)
            if self.api_key_index > 254:
                raise ContractError("api_key_index must be in 4..254")
        if environment == "robinhood":
            # The configured symbol is intentionally open-ended.  The current
            # public catalog must prove its exact perpetual identity at
            # runtime; BTC/ETH are only the initial compatibility targets.
            if self.api_base_url is None:
                if self.operator_execution_opt_in:
                    raise ContractError("api_base_url is required for Robinhood execution")
            else:
                parsed = urlsplit(self.api_base_url)
                normalized_url = self.api_base_url.rstrip("/")
                if (
                    normalized_url != OFFICIAL_ROBINHOOD_API_URL
                    or parsed.scheme != "https"
                    or parsed.netloc != "api.rh.lighter.xyz"
                    or parsed.username is not None
                    or parsed.password is not None
                    or parsed.query
                    or parsed.fragment
                    or parsed.path not in {"", "/"}
                ):
                    raise ContractError("api_base_url must be the exact Robinhood Chain Lighter endpoint")
                object.__setattr__(self, "api_base_url", normalized_url)
            if self.chain_id != OFFICIAL_ROBINHOOD_CHAIN_ID:
                raise ContractError("chain_id must be Robinhood signing domain 466324")
        else:
            if market_symbol != "HOOD":
                raise ContractError("only HOOD is supported")
            if self.api_base_url is None:
                if self.operator_execution_opt_in:
                    raise ContractError("api_base_url is required for execution")
            else:
                parsed = urlsplit(self.api_base_url)
                normalized_url = self.api_base_url.rstrip("/")
                if (
                    normalized_url != OFFICIAL_MAINNET_API_URL
                    or parsed.scheme != "https"
                    or parsed.netloc != "mainnet.zklighter.elliot.ai"
                    or parsed.username is not None
                    or parsed.password is not None
                    or parsed.query
                    or parsed.fragment
                    or parsed.path not in {"", "/"}
                ):
                    raise ContractError("api_base_url must be the exact official Lighter mainnet endpoint")
                object.__setattr__(self, "api_base_url", normalized_url)
        if self.chain_id is None:
            raise ContractError("chain_id is required")
        if environment != "robinhood" and self.chain_id != OFFICIAL_MAINNET_CHAIN_ID:
            raise ContractError("chain_id must be official Lighter mainnet chain 304")
        _int(self.chain_id, "chain_id", minimum=0)
        _int(self.auth_token_lifetime_seconds, "auth_token_lifetime_seconds", minimum=60)
        if self.auth_token_lifetime_seconds > 8 * 60 * 60:
            raise ContractError("auth_token_lifetime_seconds must not exceed the documented 8-hour lifetime")

    def integer_order_values(self, metadata: MarketMetadata) -> tuple[int, int, int]:
        return (
            decimal_to_integer(self.quantity, metadata.size_decimals, "quantity"),
            decimal_to_integer(self.source_limit_price, metadata.price_decimals, "source_limit_price"),
            decimal_to_integer(self.receiver_worst_price, metadata.price_decimals, "receiver_worst_price"),
        )

    def validate_against(self, metadata: MarketMetadata, source: AccountSnapshot, receiver: AccountSnapshot, now: float) -> None:
        market_label = self.market_symbol.upper()
        if metadata.market_id != self.market_id or source.market_id != self.market_id or receiver.market_id != self.market_id:
            raise PreflightBlocked(f"market identity does not match configured {market_label} market")
        if metadata.symbol.upper() != market_label:
            raise PreflightBlocked(f"market identity is not {market_label}")
        if metadata.market_type.lower() != "perp":
            raise PreflightBlocked("market identity is not a perpetual")
        if self.environment == "robinhood" and metadata.venue.lower() not in {"", "robinhood", "robinhood-chain"}:
            raise PreflightBlocked("market identity is from the wrong venue")
        if metadata.status.lower() not in {"active", "open", "online", "listed"}:
            raise PreflightBlocked(f"{market_label} market is not active")
        try:
            quantity_int, _, _ = self.integer_order_values(metadata)
        except ContractError as exc:
            raise PreflightBlocked(str(exc)) from exc
        if quantity_int <= 0:
            raise PreflightBlocked("quantity is below the venue size grid")
        if self.quantity < metadata.minimum_base_amount:
            raise PreflightBlocked("quantity is below the documented base minimum")
        source_notional = self.quantity * self.source_limit_price
        if source_notional < metadata.minimum_quote_amount:
            raise PreflightBlocked("source maker notional is below the documented quote minimum")
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
            incremental_margin_missing = (
                snapshot.incremental_margin_required is None
                or not snapshot.incremental_margin_evidence
            )
            if (
                incremental_margin_missing
                and self.operation_mode is not OperationMode.PAIRED_CLOSING
                and not self.defer_incremental_margin_calculation
            ):
                raise PreflightBlocked(f"{label} incremental planned-operation margin evidence is missing")
            if (
                not incremental_margin_missing
                and snapshot.incremental_margin_required > snapshot.margin_available
            ):
                raise PreflightBlocked(f"{label} incremental planned-operation margin is insufficient")
        if self.source_quote_observed_at is not None:
            if self.source_quote_observed_at > now:
                raise PreflightBlocked("source quote is from the future")
            if now - self.source_quote_observed_at > self.freshness_seconds:
                raise PreflightBlocked("source quote is stale")
        if source.account_index == receiver.account_index:
            raise PreflightBlocked("source and receiver accounts must differ")
        expected = self.direction.sign
        if self.operation_mode is OperationMode.PAIRED_OPENING:
            if self.expected_source_position is None and self.expected_receiver_position is None:
                if source.signed_position != 0 or receiver.signed_position != 0:
                    raise PreflightBlocked("paired opening requires both selected-market positions to be flat")
            elif self.expected_source_position is None or self.expected_receiver_position is None:
                raise PreflightBlocked("paired opening expected source and receiver positions must both be bound")
            elif source.signed_position != self.expected_source_position:
                raise PreflightBlocked("source position does not match the expected paired-opening position")
            elif receiver.signed_position != self.expected_receiver_position:
                raise PreflightBlocked("receiver position does not match the expected paired-opening position")
            elif self.expected_source_position * expected > 0:
                raise PreflightBlocked("paired-opening source position has the receiver direction")
            elif self.expected_receiver_position * expected < 0:
                raise PreflightBlocked("paired-opening receiver position has the source direction")
        elif self.operation_mode is OperationMode.PAIRED_CLOSING:
            if self.expected_source_position is not None and source.signed_position != self.expected_source_position:
                raise PreflightBlocked("source position does not match the expected paired-closing position")
            if self.expected_receiver_position is not None and receiver.signed_position != self.expected_receiver_position:
                raise PreflightBlocked("receiver position does not match the expected paired-closing position")
            if expected > 0:
                if source.signed_position < self.quantity:
                    raise PreflightBlocked("paired closing source long position is smaller than Q")
                if receiver.signed_position > -self.quantity:
                    raise PreflightBlocked("paired closing receiver short position is smaller than Q")
            else:
                if source.signed_position > -self.quantity:
                    raise PreflightBlocked("paired closing source short position is smaller than Q")
                if receiver.signed_position < self.quantity:
                    raise PreflightBlocked("paired closing receiver long position is smaller than Q")
        else:
            if expected > 0 and source.signed_position < self.quantity:
                raise PreflightBlocked("source long position is smaller than Q")
            if expected < 0 and source.signed_position > -self.quantity:
                raise PreflightBlocked("source short position is smaller than Q")
            if receiver.signed_position * expected < 0:
                raise PreflightBlocked("receiver has opposite HOOD exposure")
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
    mutation_deadline_monotonic: float | None = None

    def __post_init__(self) -> None:
        _int(self.account_index, "account_index", minimum=0)
        _int(self.market_id, "market_id", minimum=0)
        side = _text(self.side, "side").upper()
        if side not in {"BUY", "SELL"}:
            raise ContractError("order side must be BUY or SELL")
        object.__setattr__(self, "side", side)
        object.__setattr__(self, "quantity", _positive(self.quantity, "quantity"))
        _int(self.quantity_int, "quantity_int", minimum=1)
        object.__setattr__(self, "price", _positive(self.price, "price"))
        _int(self.price_int, "price_int", minimum=1)
        object.__setattr__(self, "order_type", _normalize_order_type(self.order_type))
        object.__setattr__(self, "time_in_force", _normalize_time_in_force(self.time_in_force))
        if not isinstance(self.reduce_only, bool):
            raise ContractError("reduce_only must be bool")
        _int(self.order_expiry_ms, "order_expiry_ms", minimum=0)
        _int(self.client_order_index, "client_order_index", minimum=0)
        if self.mutation_deadline_monotonic is not None:
            object.__setattr__(
                self,
                "mutation_deadline_monotonic",
                _finite_float(self.mutation_deadline_monotonic, "mutation_deadline_monotonic"),
            )

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
    source_fee_rate: Decimal | None = None
    receiver_fee_rate: Decimal | None = None
    source_identity: str = ""
    receiver_identity: str = ""
    metadata_observed_at: float | None = None
    operation_mode: OperationMode | str = OperationMode.CLOSE_REOPEN
    defer_incremental_margin_calculation: bool = False

    def __post_init__(self) -> None:
        try:
            direction = self.direction if isinstance(self.direction, Direction) else Direction(self.direction)
        except (TypeError, ValueError) as exc:
            raise ContractError("direction must be LONG or SHORT") from exc
        object.__setattr__(self, "direction", direction)
        object.__setattr__(self, "operation_mode", OperationMode.parse(self.operation_mode))
        if not isinstance(self.defer_incremental_margin_calculation, bool):
            raise ContractError("defer_incremental_margin_calculation must be bool")
        _text(self.run_id, "run_id")
        object.__setattr__(self, "quantity", _positive(self.quantity, "quantity"))
        object.__setattr__(self, "source_position_before", _decimal(self.source_position_before, "source_position_before"))
        object.__setattr__(self, "receiver_position_before", _decimal(self.receiver_position_before, "receiver_position_before"))
        _timestamp(self.created_at, "created_at")
        if self.metadata_observed_at is not None:
            object.__setattr__(
                self,
                "metadata_observed_at",
                _timestamp(self.metadata_observed_at, "metadata_observed_at"),
            )
        for value, name in (
            (self.source_fee_rate, "source_fee_rate"),
            (self.receiver_fee_rate, "receiver_fee_rate"),
        ):
            if value is not None:
                object.__setattr__(self, name, _nonnegative(value, name))
        for value, name in (
            (self.source_identity, "source_identity"),
            (self.receiver_identity, "receiver_identity"),
        ):
            if value:
                object.__setattr__(self, name, _text(value, name))
        if self.source.account_index == self.receiver.account_index:
            raise ContractError("source and receiver plan accounts must differ")
        if self.source.market_id != self.receiver.market_id:
            raise ContractError("source and receiver plan markets must match")
        if self.source.quantity != self.quantity or self.receiver.quantity != self.quantity:
            raise ContractError("source and receiver plan quantities must equal configured quantity")
        if self.source.quantity_int != self.receiver.quantity_int:
            raise ContractError("source and receiver plan integer quantities must match")
        if self.source.side != direction.source_side or self.receiver.side != direction.receiver_side:
            raise ContractError("source/receiver plan sides conflict with direction")
        source_reduce_only = self.operation_mode in {
            OperationMode.CLOSE_REOPEN,
            OperationMode.PAIRED_CLOSING,
        }
        receiver_reduce_only = self.operation_mode is OperationMode.PAIRED_CLOSING
        if (
            self.source.order_type != "LIMIT"
            or self.source.time_in_force != "POST_ONLY"
            or self.source.reduce_only != source_reduce_only
            or self.source.order_expiry_ms <= 0
            or self.receiver.order_type != "MARKET"
            or self.receiver.time_in_force != "IOC"
            or self.receiver.reduce_only != receiver_reduce_only
            or self.receiver.order_expiry_ms != 0
        ):
            raise ContractError("source/receiver plan order semantics conflict with the selected operation mode")
        if self.source.client_order_index == self.receiver.client_order_index:
            raise ContractError("source and receiver client order identities must differ")
        if self.operation_mode is OperationMode.PAIRED_OPENING:
            if self.source_position_before * direction.sign > 0:
                raise ContractError("paired-opening source position must be opposite the receiver direction")
            if self.receiver_position_before * direction.sign < 0:
                raise ContractError("paired-opening receiver position must follow the receiver direction")
        elif self.operation_mode is OperationMode.PAIRED_CLOSING:
            if self.source_position_before * direction.sign < 0:
                raise ContractError("paired-closing source position must follow the source order direction")
            if self.receiver_position_before * direction.sign > 0:
                raise ContractError("paired-closing receiver position must oppose the source order direction")

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
            "source_fee_rate": (
                None if self.source_fee_rate is None else _wire_decimal(self.source_fee_rate)
            ),
            "receiver_fee_rate": (
                None if self.receiver_fee_rate is None else _wire_decimal(self.receiver_fee_rate)
            ),
            "source_identity": self.source_identity,
            "receiver_identity": self.receiver_identity,
            "metadata_observed_at": self.metadata_observed_at,
            "operation_mode": self.operation_mode.value,
            "defer_incremental_margin_calculation": self.defer_incremental_margin_calculation,
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
    dispatched: bool = True

    @property
    def filled_quantity(self) -> Decimal:
        return sum((trade.quantity for trade in self.trades), Decimal(0))

    @property
    def fee_total(self) -> Decimal | None:
        if any(trade.fee is None for trade in self.trades):
            return None
        return sum((trade.fee or Decimal(0) for trade in self.trades), Decimal(0))

    @property
    def gross_notional(self) -> Decimal:
        return sum((trade.quantity * trade.price for trade in self.trades), Decimal(0))

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

    @property
    def counterparty_match_status(self) -> str:
        """Separate a proven zero from a missing counterparty field."""

        if not self.trades or any(trade.counterparty_account_index is None for trade in self.trades):
            return "UNKNOWN"
        return "KNOWN"

    def counterparty_status_against(self, account_index: int) -> str:
        if not self.trades or any(trade.counterparty_account_index is None for trade in self.trades):
            return "UNKNOWN"
        return "MATCHED" if self.quantity_against(account_index) > 0 else "KNOWN_ZERO"


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
    joint_match_status: str = "UNKNOWN"
    joint_match_quantity: Decimal = Decimal(0)
    economic_status: str = "UNKNOWN"
    economic_findings: tuple[str, ...] = ()
    findings: tuple[str, ...] = ()
    operation_mode: OperationMode | str = OperationMode.CLOSE_REOPEN
    # Paired operations may be safely retried only after the source order has
    # been proven zero-fill, terminal/canceled, and fully reconciled.  This is
    # deliberately separate from ``outcome``: the child attempt can be a
    # guarded PARTIAL while the parent still has bounded retry budget.
    retryable_pair: bool = False
    attempt_index: int | None = None
    latency: Mapping[str, Any] | None = None
    priority_guard: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "operation_mode", OperationMode.parse(self.operation_mode))

    @property
    def mutual_execution_proven(self) -> bool:
        return bool(
            self.outcome is Outcome.SUCCESS and self.plan is not None
            and self.joint_match_status == "MATCHED"
            and self.joint_match_quantity == self.plan.quantity
            and self.source is not None and self.receiver is not None
            and self.source.filled_quantity == self.receiver.filled_quantity == self.plan.quantity
            and self.source.history_complete and self.receiver.history_complete
            and not self.source.unknown_reasons and not self.receiver.unknown_reasons
        )

    def as_dict(self) -> dict[str, Any]:
        operation_mode = self.plan.operation_mode if self.plan is not None else self.operation_mode

        def leg(value: LegReconciliation | None, counterparty: LegReconciliation | None = None) -> Any:
            if value is None:
                return None
            external = None if counterparty is None else sum(
                (trade.quantity for trade in value.trades
                 if trade.counterparty_account_index is not None
                 and trade.counterparty_account_index != counterparty.account_index), Decimal(0))
            return {
                "account_index": value.account_index,
                "order_id": value.order_id,
                "filled_quantity": _wire_decimal(value.filled_quantity),
                "external_counterparty_quantity": None if external is None else _wire_decimal(external),
                "unproved_counterparty_quantity": None if external is None else _wire_decimal(
                    max(Decimal(0), value.filled_quantity - external - self.joint_match_quantity)),
                "counterparty_matched_quantity": (
                    None
                    if counterparty is None
                    else _wire_decimal(self.joint_match_quantity)
                ),
                "counterparty_match_status": (
                    "UNKNOWN"
                    if counterparty is None
                    else self.joint_match_status
                ),
                "named_counterparty_quantity": (
                    None
                    if counterparty is None
                    else _wire_decimal(value.quantity_against(counterparty.account_index))
                ),
                "named_counterparty_status": (
                    "UNKNOWN"
                    if counterparty is None
                    else value.counterparty_status_against(counterparty.account_index)
                ),
                "fee_total": None if value.fee_total is None else _wire_decimal(value.fee_total),
                "gross_notional": _wire_decimal(value.gross_notional),
                "position_before": _wire_decimal(value.position_before),
                "position_after": None if value.position_after is None else _wire_decimal(value.position_after),
                "history_complete": value.history_complete,
                "dispatched": value.dispatched,
                "unknown_reasons": list(value.unknown_reasons),
                "trades": [
                    {
                        "trade_id": trade.trade_id,
                        "account_index": trade.account_index,
                        "market_id": trade.market_id,
                        "order_id": trade.order_id,
                        "side": trade.side,
                        "quantity": _wire_decimal(trade.quantity),
                        "price": _wire_decimal(trade.price),
                        "fee": None if trade.fee is None else _wire_decimal(trade.fee),
                        "counterparty_account_index": trade.counterparty_account_index,
                        "counterparty_order_id": trade.counterparty_order_id,
                        "counterparty_client_order_index": trade.counterparty_client_order_index,
                        "client_order_index": trade.client_order_index,
                        "observed_at": trade.observed_at,
                    }
                    for trade in value.trades
                ],
                "order": None if value.order is None else {
                    "order_id": value.order.order_id,
                    "client_order_index": value.order.client_order_index,
                    "account_index": value.order.account_index,
                    "market_id": value.order.market_id,
                    "side": value.order.side,
                    "order_type": value.order.order_type,
                    "time_in_force": value.order.time_in_force,
                    "reduce_only": value.order.reduce_only,
                    "price": None if value.order.price is None else _wire_decimal(value.order.price),
                    "status": value.order.status,
                    "initial_quantity": _wire_decimal(value.order.initial_quantity),
                    "remaining_quantity": _wire_decimal(value.order.remaining_quantity),
                    "filled_quantity": _wire_decimal(value.order.filled_quantity),
                    "observed_at": value.order.observed_at,
                },
            }

        return {
            "outcome": self.outcome.value,
            "phase": self.phase.value,
            "run_id": self.run_id,
            "operation_mode": operation_mode.value,
            "mutual_execution_proven": self.mutual_execution_proven,
            "plan": None if self.plan is None else self.plan.as_dict(),
            "source": leg(self.source, self.receiver),
            "receiver": leg(self.receiver, self.source),
            "reason": self.reason,
            "unknown_reasons": list(self.unknown_reasons),
            "joint_trade_match": {
                "status": self.joint_match_status,
                "quantity": _wire_decimal(self.joint_match_quantity),
            },
            "economic_status": self.economic_status,
            "economic_findings": list(self.economic_findings),
            "findings": list(self.findings),
            "retryable_pair": self.retryable_pair,
            "attempt_index": self.attempt_index,
            "latency": None if self.latency is None else dict(self.latency),
            "priority_guard": None if self.priority_guard is None else dict(self.priority_guard),
        }


def now_utc_seconds() -> float:
    return datetime.now(timezone.utc).timestamp()


__all__ = [
    "ACTIVE_ORDER_STATUSES",
    "AccountSnapshot",
    "ContractError",
    "Direction",
    "HandoffMode",
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
    "OperationMode",
    "Phase",
    "PreflightBlocked",
    "OFFICIAL_MAINNET_API_URL",
    "OFFICIAL_MAINNET_CHAIN_ID",
    "OFFICIAL_ROBINHOOD_API_URL",
    "OFFICIAL_ROBINHOOD_CHAIN_ID",
    "ROBINHOOD_PERPETUAL_SYMBOLS",
    "ROBINHOOD_WEBSITE_URL",
    "TERMINAL_ORDER_STATUSES",
    "TradeReceipt",
    "decimal_to_integer",
    "now_utc_seconds",
]
