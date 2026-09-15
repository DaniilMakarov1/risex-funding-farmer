"""Bounded Robinhood Chain perpetual slice sequencing.

This module is deliberately a small coordinator around the existing HCR-1
single-attempt engine.  It does not add a recovery service or a wallet loop:
one parent journal records the selection and child boundaries, while each
child gets its own HCR-1 journal.  A restart can reconcile an interrupted
child, but never submits a later child implicitly.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_DOWN
import hashlib
import math
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence, runtime_checkable

from .contracts import (
    AccountSnapshot,
    ContractError,
    Direction,
    HandoffConfig,
    HandoffResult,
    MarketMetadata,
    OFFICIAL_ROBINHOOD_API_URL,
    OFFICIAL_ROBINHOOD_CHAIN_ID,
    Outcome,
    OperationMode,
    PreflightBlocked,
)
from .engine import Clock, SystemClock, run_handoff
from .journal import DurableJournal, sanitize_exception


def _decimal(value: Any, name: str) -> Decimal:
    if isinstance(value, bool) or isinstance(value, float):
        raise ContractError(f"{name} must be an exact decimal string or Decimal")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ContractError(f"{name} must be a finite decimal") from exc
    if not parsed.is_finite():
        raise ContractError(f"{name} must be finite")
    return parsed


def _positive(value: Any, name: str) -> Decimal:
    parsed = _decimal(value, name)
    if parsed <= 0:
        raise ContractError(f"{name} must be positive")
    return parsed


def _nonnegative(value: Any, name: str) -> Decimal:
    parsed = _decimal(value, name)
    if parsed < 0:
        raise ContractError(f"{name} must be non-negative")
    return parsed


def _finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise ContractError(f"{name} must be a finite positive number")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ContractError(f"{name} must be a finite positive number") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise ContractError(f"{name} must be a finite positive number")
    return parsed


def _int(value: Any, name: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ContractError(f"{name} must be an integer at least {minimum}")
    return value


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{name} must be non-empty text")
    return value.strip()


def _timestamp(value: Any, name: str) -> float:
    """Validate a transport observation without renewing its age."""

    if isinstance(value, bool):
        raise ContractError(f"{name} must be a timestamp")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ContractError(f"{name} must be a timestamp") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise ContractError(f"{name} must be a finite non-negative timestamp")
    return parsed


@dataclass(frozen=True, slots=True)
class DepthLevel:
    """One public order-book level after exact decimal validation."""

    price: Decimal
    quantity: Decimal
    order_id: str | None = None
    owner_account_index: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "price", _positive(self.price, "price"))
        object.__setattr__(self, "quantity", _positive(self.quantity, "quantity"))
        if self.order_id is not None:
            object.__setattr__(self, "order_id", _text(self.order_id, "order_id"))
        if self.owner_account_index is not None:
            _int(self.owner_account_index, "owner_account_index")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "DepthLevel":
        if not isinstance(value, Mapping):
            raise ContractError("order-book level must be an object")
        raw_quantity = None
        for name in ("remaining_base_amount", "quantity", "size", "base_amount"):
            if name in value:
                raw_quantity = value[name]
                break
        if raw_quantity is None:
            raise ContractError("order-book level lacks remaining base amount")
        raw_order_id = value.get("order_id", value.get("order_index"))
        raw_owner = value.get("owner_account_index", value.get("owner_account_id"))
        owner = None if raw_owner is None else _int(int(raw_owner), "owner_account_index")
        return cls(
            price=_positive(value.get("price"), "price"),
            quantity=_positive(raw_quantity, "quantity"),
            order_id=None if raw_order_id is None else _text(str(raw_order_id), "order_id"),
            owner_account_index=owner,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "price": format(self.price, "f"),
            "quantity": format(self.quantity, "f"),
            "order_id": self.order_id,
            "owner_account_index": self.owner_account_index,
        }


@dataclass(frozen=True, slots=True)
class OrderBookSnapshot:
    """A fresh, identity-bound public Robinhood perp book."""

    market_id: int
    symbol: str
    bids: tuple[DepthLevel, ...]
    asks: tuple[DepthLevel, ...]
    observed_at: float
    market_type: str = "perp"
    venue: str = "robinhood"

    def __post_init__(self) -> None:
        _int(self.market_id, "market_id")
        symbol = _text(self.symbol, "symbol").upper()
        object.__setattr__(self, "symbol", symbol)
        market_type = _text(self.market_type, "market_type").lower()
        if market_type != "perp":
            raise ContractError("order book must be a perpetual market")
        object.__setattr__(self, "market_type", market_type)
        venue = _text(self.venue, "venue").lower()
        if venue not in {"robinhood", "robinhood-chain"}:
            raise ContractError("order book is from the wrong venue")
        object.__setattr__(self, "venue", venue)
        if not isinstance(self.observed_at, (int, float)) or isinstance(self.observed_at, bool):
            raise ContractError("observed_at must be a timestamp")
        if not math.isfinite(float(self.observed_at)) or float(self.observed_at) < 0:
            raise ContractError("observed_at must be a finite non-negative timestamp")
        self._validate_side(self.bids, "bids", descending=True)
        self._validate_side(self.asks, "asks", descending=False)
        ids = [level.order_id for level in (*self.bids, *self.asks) if level.order_id is not None]
        if len(ids) != len(set(ids)):
            raise ContractError("order book contains duplicate order identity")

    @staticmethod
    def _validate_side(levels: Sequence[DepthLevel], label: str, *, descending: bool) -> None:
        if not isinstance(levels, tuple):
            raise ContractError(f"{label} must be a tuple of levels")
        previous: Decimal | None = None
        for level in levels:
            if not isinstance(level, DepthLevel):
                raise ContractError(f"{label} contains an invalid level")
            if previous is not None and ((descending and level.price > previous) or (not descending and level.price < previous)):
                raise ContractError(f"{label} are not in official price order")
            previous = level.price

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, Any],
        *,
        market_id: int,
        symbol: str,
        observed_at: float,
        venue: str = "robinhood",
    ) -> "OrderBookSnapshot":
        if not isinstance(value, Mapping):
            raise ContractError("order-book response must be an object")
        code = value.get("code")
        if code is not None:
            try:
                if int(code) != 200:
                    raise ContractError("order-book response was not successful")
            except (TypeError, ValueError) as exc:
                raise ContractError("order-book response code is malformed") from exc
        raw_bids = value.get("bids")
        raw_asks = value.get("asks")
        if not isinstance(raw_bids, (list, tuple)) or not isinstance(raw_asks, (list, tuple)):
            raise ContractError("order-book response lacks bids/asks arrays")
        return cls(
            market_id=market_id,
            symbol=symbol,
            bids=tuple(DepthLevel.from_mapping(item) for item in raw_bids),
            asks=tuple(DepthLevel.from_mapping(item) for item in raw_asks),
            observed_at=observed_at,
            market_type=str(value.get("market_type", "perp")),
            venue=venue,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "market_id": self.market_id,
            "symbol": self.symbol,
            "market_type": self.market_type,
            "venue": self.venue,
            "observed_at": self.observed_at,
            "bids": [item.as_dict() for item in self.bids],
            "asks": [item.as_dict() for item in self.asks],
        }


@dataclass(frozen=True, slots=True)
class RobinhoodSeriesConfig:
    """Operator-supplied bounds for a finite BTC/ETH child series.

    ``allowed_price_deviation`` is a relative fraction.  For example, 0.01
    admits receiver-side depth up to 1% adverse to ``source_limit_price``;
    each child uses the conservative tick-rounded effective bound derived
    from that guard and ``receiver_worst_price``.
    """

    market_symbol: str
    direction: Direction
    total_quantity: Decimal
    desired_slice_quantity: Decimal
    allowed_price_deviation: Decimal
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
    market_id: int | None = None
    environment: str = "robinhood"
    api_base_url: str = OFFICIAL_ROBINHOOD_API_URL
    chain_id: int = OFFICIAL_ROBINHOOD_CHAIN_ID
    api_key_index: int | None = None
    auth_token_lifetime_seconds: int = 600
    operator_execution_opt_in: bool = False
    operator_plan_reviewed: bool = False
    operation_mode: OperationMode | str = OperationMode.CLOSE_REOPEN
    mode: OperationMode | str | None = None

    def __post_init__(self) -> None:
        symbol = _text(self.market_symbol, "market_symbol").upper()
        object.__setattr__(self, "market_symbol", symbol)
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
        for field_name in (
            "total_quantity",
            "desired_slice_quantity",
            "source_limit_price",
            "receiver_worst_price",
        ):
            object.__setattr__(self, field_name, _positive(getattr(self, field_name), field_name))
        object.__setattr__(self, "allowed_price_deviation", _nonnegative(self.allowed_price_deviation, "allowed_price_deviation"))
        for value, name in (
            (self.freshness_seconds, "freshness_seconds"),
            (self.request_timeout_seconds, "request_timeout_seconds"),
            (self.order_timeout_seconds, "order_timeout_seconds"),
            (self.reconcile_timeout_seconds, "reconcile_timeout_seconds"),
            (self.poll_interval_seconds, "poll_interval_seconds"),
        ):
            object.__setattr__(self, name, _finite_float(value, name))
        object.__setattr__(self, "max_poll_count", _int(self.max_poll_count, "max_poll_count", minimum=1))
        object.__setattr__(self, "source_order_lifetime_seconds", _int(self.source_order_lifetime_seconds, "source_order_lifetime_seconds", minimum=300))
        if self.source_order_lifetime_seconds > 30 * 24 * 60 * 60:
            raise ContractError("source_order_lifetime_seconds must not exceed 30 days")
        _text(self.client_order_prefix, "client_order_prefix")
        _text(self.journal_path, "journal_path")
        if self.market_id is not None:
            _int(self.market_id, "market_id")
        if self.environment.lower() != "robinhood":
            raise ContractError("environment must be robinhood")
        object.__setattr__(self, "environment", "robinhood")
        if self.api_base_url.rstrip("/") != OFFICIAL_ROBINHOOD_API_URL:
            raise ContractError("api_base_url must be the exact Robinhood Chain Lighter endpoint")
        object.__setattr__(self, "api_base_url", OFFICIAL_ROBINHOOD_API_URL)
        if self.chain_id != OFFICIAL_ROBINHOOD_CHAIN_ID:
            raise ContractError("chain_id must be Robinhood signing domain 466324")
        _int(self.chain_id, "chain_id")
        if self.api_key_index is not None:
            if self.api_key_index < 4 or self.api_key_index > 254:
                raise ContractError("api_key_index must be in 4..254")
        _int(self.auth_token_lifetime_seconds, "auth_token_lifetime_seconds", minimum=60)
        if self.auth_token_lifetime_seconds > 8 * 60 * 60:
            raise ContractError("auth_token_lifetime_seconds must not exceed the documented 8-hour lifetime")
        if not isinstance(self.operator_execution_opt_in, bool) or not isinstance(self.operator_plan_reviewed, bool):
            raise ContractError("operator execution and plan review flags must be bool")

    def attempt_config(
        self,
        *,
        market_id: int,
        quantity: Decimal,
        journal_path: str,
        receiver_worst_price: Decimal | None = None,
        expected_source_position: Decimal | None = None,
        expected_receiver_position: Decimal | None = None,
    ) -> HandoffConfig:
        """Create an exact HCR-1 child configuration for one slice."""

        return HandoffConfig(
            market_id=market_id,
            direction=self.direction,
            quantity=quantity,
            source_limit_price=self.source_limit_price,
            receiver_worst_price=(
                self.receiver_worst_price
                if receiver_worst_price is None
                else _positive(receiver_worst_price, "receiver_worst_price")
            ),
            freshness_seconds=self.freshness_seconds,
            request_timeout_seconds=self.request_timeout_seconds,
            order_timeout_seconds=self.order_timeout_seconds,
            reconcile_timeout_seconds=self.reconcile_timeout_seconds,
            poll_interval_seconds=self.poll_interval_seconds,
            max_poll_count=self.max_poll_count,
            source_order_lifetime_seconds=self.source_order_lifetime_seconds,
            client_order_prefix=self.client_order_prefix,
            journal_path=journal_path,
            market_symbol=self.market_symbol,
            environment="robinhood",
            operator_execution_opt_in=self.operator_execution_opt_in,
            operator_plan_reviewed=self.operator_plan_reviewed,
            api_base_url=OFFICIAL_ROBINHOOD_API_URL,
            api_key_index=self.api_key_index,
            chain_id=OFFICIAL_ROBINHOOD_CHAIN_ID,
            auth_token_lifetime_seconds=self.auth_token_lifetime_seconds,
            operation_mode=self.operation_mode,
            expected_source_position=expected_source_position,
            expected_receiver_position=expected_receiver_position,
        )

    def binding(
        self,
        *,
        source_account_index: int,
        receiver_account_index: int,
        resolved_market_id: int | None = None,
    ) -> dict[str, Any]:
        return {
            "venue": "robinhood-chain",
            "website_url": "https://robinhoodchain.lighter.xyz",
            "api_base_url": OFFICIAL_ROBINHOOD_API_URL,
            "chain_id": OFFICIAL_ROBINHOOD_CHAIN_ID,
            "market_symbol": self.market_symbol,
            "market_id": self.market_id if resolved_market_id is None else resolved_market_id,
            "direction": self.direction.value,
            "operation_mode": self.operation_mode.value,
            "mode": self.operation_mode.value,
            "total_quantity": format(self.total_quantity, "f"),
            "desired_slice_quantity": format(self.desired_slice_quantity, "f"),
            "allowed_price_deviation": format(self.allowed_price_deviation, "f"),
            "source_limit_price": format(self.source_limit_price, "f"),
            "receiver_worst_price": format(self.receiver_worst_price, "f"),
            "freshness_seconds": self.freshness_seconds,
            "request_timeout_seconds": self.request_timeout_seconds,
            "order_timeout_seconds": self.order_timeout_seconds,
            "reconcile_timeout_seconds": self.reconcile_timeout_seconds,
            "poll_interval_seconds": self.poll_interval_seconds,
            "max_poll_count": self.max_poll_count,
            "source_order_lifetime_seconds": self.source_order_lifetime_seconds,
            "client_order_prefix": self.client_order_prefix,
            "journal_path": self.journal_path,
            "source_account_index": source_account_index,
            "receiver_account_index": receiver_account_index,
            "implementation_fingerprint": _series_implementation_fingerprint(),
        }


@dataclass(frozen=True, slots=True)
class SliceSizing:
    requested_quantity: Decimal
    executable_depth: Decimal
    rounded_quantity: Decimal
    remaining_quantity: Decimal
    depth_price_bound: Decimal
    observed_at: float
    reason: str = ""

    @property
    def tradeable(self) -> bool:
        return self.rounded_quantity > 0 and not self.reason

    def as_dict(self) -> dict[str, Any]:
        return {
            "requested_quantity": format(self.requested_quantity, "f"),
            "executable_depth": format(self.executable_depth, "f"),
            "rounded_quantity": format(self.rounded_quantity, "f"),
            "remaining_quantity": format(self.remaining_quantity, "f"),
            "depth_price_bound": format(self.depth_price_bound, "f"),
            "effective_receiver_price": format(self.effective_receiver_price, "f"),
            "observed_at": self.observed_at,
            "reason": self.reason,
        }

    @property
    def effective_receiver_price(self) -> Decimal:
        """The exact child receiver bound after the deviation/tick guard."""

        return self.depth_price_bound


def _effective_receiver_bound(config: RobinhoodSeriesConfig, price_decimals: int) -> Decimal:
    """Return the conservative receiver limit implied by operator inputs."""

    _int(price_decimals, "price_decimals")
    price_step = Decimal(1).scaleb(-price_decimals)
    if config.direction is Direction.LONG:
        raw_bound = min(
            config.receiver_worst_price,
            config.source_limit_price * (Decimal("1") + config.allowed_price_deviation),
        )
        return (raw_bound / price_step).to_integral_value(rounding=ROUND_DOWN) * price_step
    raw_bound = max(
        config.receiver_worst_price,
        config.source_limit_price * (Decimal("1") - config.allowed_price_deviation),
    )
    return (raw_bound / price_step).to_integral_value(rounding=ROUND_CEILING) * price_step


def size_next_slice(
    config: RobinhoodSeriesConfig,
    metadata: MarketMetadata,
    book: OrderBookSnapshot,
    *,
    remaining_quantity: Decimal,
    now: float,
) -> SliceSizing:
    """Select a down-sized slice from fresh receiver-side executable depth."""

    remaining = _positive(remaining_quantity, "remaining_quantity")
    if metadata.market_id != book.market_id or metadata.symbol.upper() != book.symbol.upper():
        raise PreflightBlocked("order book identity does not match current market")
    if metadata.symbol.upper() != config.market_symbol or metadata.market_type.lower() != "perp":
        raise PreflightBlocked("current market is not the configured Robinhood perpetual")
    if book.symbol.upper() != config.market_symbol or book.market_type.lower() != "perp":
        raise PreflightBlocked("current order book is not the configured Robinhood perpetual")
    if metadata.venue and metadata.venue.lower() not in {"robinhood", "robinhood-chain"}:
        raise PreflightBlocked("current market is from the wrong venue")
    if book.venue.lower() not in {"robinhood", "robinhood-chain"}:
        raise PreflightBlocked("current order book is from the wrong venue")
    if book.observed_at > now:
        raise PreflightBlocked("order book observation is from the future")
    if now - book.observed_at > config.freshness_seconds:
        raise PreflightBlocked("order book observation is stale")
    effective_bound = _effective_receiver_bound(config, metadata.price_decimals)
    if config.direction is Direction.LONG:
        levels = tuple(level for level in book.asks if level.price <= effective_bound)
    else:
        levels = tuple(level for level in book.bids if level.price >= effective_bound)
    depth = sum((level.quantity for level in levels), Decimal("0"))
    requested = min(remaining, config.desired_slice_quantity)
    metadata_step = Decimal(1).scaleb(-metadata.size_decimals)
    rounded = (min(requested, depth) / metadata_step).to_integral_value(rounding=ROUND_DOWN) * metadata_step
    if rounded <= 0:
        reason = "no fresh executable depth" if depth <= 0 else "executable depth is below venue size step"
        return SliceSizing(requested, depth, Decimal("0"), remaining, effective_bound, book.observed_at, reason)
    if rounded < metadata.minimum_base_amount:
        return SliceSizing(requested, depth, Decimal("0"), remaining, effective_bound, book.observed_at, "remaining depth is below venue base minimum")
    if rounded * config.source_limit_price < metadata.minimum_quote_amount:
        return SliceSizing(requested, depth, Decimal("0"), remaining, effective_bound, book.observed_at, "remaining depth is below venue quote minimum")
    return SliceSizing(requested, depth, rounded, remaining, effective_bound, book.observed_at)


@dataclass(frozen=True, slots=True)
class SeriesResult:
    outcome: Outcome
    series_id: str
    market_symbol: str
    market_id: int | None
    target_quantity: Decimal
    completed_quantity: Decimal
    remaining_quantity: Decimal
    children: tuple[HandoffResult, ...] = ()
    actual_filled_quantity: Decimal | None = None
    reason: str = ""
    remainder_reason: str = ""
    actual_source_filled_quantity: Decimal | None = None
    actual_receiver_filled_quantity: Decimal | None = None
    operation_mode: OperationMode | str = OperationMode.CLOSE_REOPEN

    def __post_init__(self) -> None:
        object.__setattr__(self, "operation_mode", OperationMode.parse(self.operation_mode))

    def as_dict(self) -> dict[str, Any]:
        return {
            "outcome": self.outcome.value,
            "series_id": self.series_id,
            "operation_mode": self.operation_mode.value,
            "market_symbol": self.market_symbol,
            "market_id": self.market_id,
            "target_quantity": format(self.target_quantity, "f"),
            "completed_quantity": format(self.completed_quantity, "f"),
            "remaining_quantity": format(self.remaining_quantity, "f"),
            "actual_filled_quantity": None if self.actual_filled_quantity is None else format(self.actual_filled_quantity, "f"),
            "actual_source_filled_quantity": (
                None
                if self.actual_source_filled_quantity is None
                else format(self.actual_source_filled_quantity, "f")
            ),
            "actual_receiver_filled_quantity": (
                None
                if self.actual_receiver_filled_quantity is None
                else format(self.actual_receiver_filled_quantity, "f")
            ),
            "reason": self.reason,
            "remainder_reason": self.remainder_reason,
            "children": [child.as_dict() for child in self.children],
        }


def _series_implementation_fingerprint() -> str:
    package_dir = Path(__file__).resolve().parent
    digest = hashlib.sha256()
    for name in ("series.py", "contracts.py", "engine.py", "journal.py", "sdk.py"):
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update((package_dir / name).read_bytes())
        digest.update(b"\0")
    return f"sha256:{digest.hexdigest()}"


@runtime_checkable
class RobinhoodSeriesClient(Protocol):
    source_account_index: int
    receiver_account_index: int

    async def market_metadata(self, market_id: int) -> MarketMetadata | Mapping[str, Any]: ...

    async def account_snapshot(self, account_index: int, market_id: int) -> AccountSnapshot | Mapping[str, Any]: ...

    async def order_book(self, market_id: int) -> OrderBookSnapshot | Mapping[str, Any]: ...


def _as_account(value: AccountSnapshot | Mapping[str, Any]) -> AccountSnapshot:
    return value if isinstance(value, AccountSnapshot) else AccountSnapshot.from_mapping(value)


def _as_market(value: MarketMetadata | Mapping[str, Any]) -> MarketMetadata:
    return value if isinstance(value, MarketMetadata) else MarketMetadata.from_mapping(value)


class RobinhoodSeriesEngine:
    """Run finite children serially with an explicit continuity barrier."""

    def __init__(self, client: RobinhoodSeriesClient, *, clock: Clock | None = None) -> None:
        self.client = client
        self.clock = clock or SystemClock()

    async def execute(self, config: RobinhoodSeriesConfig) -> SeriesResult:
        try:
            parent = DurableJournal(config.journal_path, clock=self.clock.now)
        except Exception as exc:
            return self._blocked(config, "series journal unavailable: " + sanitize_exception(exc))
        try:
            parent.acquire_attempt()
        except Exception as exc:
            return self._blocked(config, "series journal ownership unavailable: " + sanitize_exception(exc))
        try:
            return await self._execute_locked(config, parent)
        finally:
            parent.release_attempt()

    async def _execute_locked(self, config: RobinhoodSeriesConfig, parent: DurableJournal) -> SeriesResult:
        try:
            source_index, receiver_index = self._account_indices()
        except Exception as exc:
            reason = sanitize_exception(exc)
            parent.append("SERIES_PREFLIGHT_BLOCKED", {"reason": reason})
            return self._result(config, parent.run_id, None, Decimal("0"), config.total_quantity, (), reason)
        events = parent.events
        existing = [event for event in events if event.event.startswith("SERIES_") or event.event.startswith("CHILD_")]
        if events and not existing:
            return self._blocked(config, "journal contains a different operation")
        persisted_started = next((event for event in existing if event.event == "SERIES_STARTED"), None)
        series_id = parent.run_id if persisted_started is None else persisted_started.run_id
        if any(event.event == "SERIES_COMPLETE" for event in existing):
            try:
                completed, market_id, source_actual, receiver_actual = self._stored_series_progress(
                    config,
                    existing,
                    source_index,
                    receiver_index,
                    require_complete=True,
                )
            except Exception as exc:
                reason = "completed series journal is malformed: " + sanitize_exception(exc)
                parent.append("SERIES_RESTART_BLOCKED", {"reason": reason})
                return self._result(config, series_id, None, Decimal("0"), config.total_quantity, (), reason)
            return self._result(
                config,
                series_id,
                market_id,
                completed,
                config.total_quantity - completed,
                (),
                "journal already contains a completed series; no mutation was attempted",
                outcome=Outcome.UNKNOWN,
                actual_filled_quantity=completed,
                actual_source_filled_quantity=source_actual,
                actual_receiver_filled_quantity=receiver_actual,
            )
        try:
            metadata = await self._resolve_market(config)
            self._validate_market(config, metadata)
        except Exception as exc:
            reason = sanitize_exception(exc)
            if existing:
                parent.append("SERIES_RESTART_BLOCKED", {"reason": reason})
                return self._result(config, series_id, None, Decimal("0"), config.total_quantity, (), reason)
            parent.append("SERIES_PREFLIGHT_BLOCKED", {"reason": reason})
            return self._result(config, series_id, None, Decimal("0"), config.total_quantity, (), reason)

        binding = config.binding(
            source_account_index=source_index,
            receiver_account_index=receiver_index,
            resolved_market_id=metadata.market_id,
        )
        if existing:
            started = next((event for event in existing if event.event == "SERIES_STARTED"), None)
            if started is None or started.payload.get("binding") != binding:
                reason = "series journal binding conflicts with current venue or selection inputs"
                parent.append("SERIES_RESTART_BLOCKED", {"reason": reason})
                return self._result(config, series_id, metadata.market_id, Decimal("0"), config.total_quantity, (), reason)
            try:
                restart_result = await self._handle_restart(config, parent, metadata, existing, series_id=series_id)
            except Exception as exc:
                reason = "restart journal is malformed: " + sanitize_exception(exc)
                parent.append("SERIES_RESTART_BLOCKED", {"reason": reason})
                try:
                    persisted_completed, persisted_market_id, persisted_source, persisted_receiver = (
                        self._stored_series_progress(
                            config,
                            existing,
                            source_index,
                            receiver_index,
                            require_complete=False,
                            series_id=series_id,
                        )
                    )
                except Exception:
                    persisted_completed = Decimal("0")
                    persisted_market_id = metadata.market_id
                    persisted_source = Decimal("0")
                    persisted_receiver = Decimal("0")
                return self._result(
                    config,
                    series_id,
                    persisted_market_id,
                    persisted_completed,
                    config.total_quantity - persisted_completed,
                    (),
                    reason,
                    actual_filled_quantity=self._paired_totals_or_none(
                        persisted_source, persisted_receiver
                    ),
                    actual_source_filled_quantity=persisted_source,
                    actual_receiver_filled_quantity=persisted_receiver,
                )
            if restart_result is not None:
                return restart_result
            reason = "restart has no safely resumable child state"
            parent.append("SERIES_RESTART_BLOCKED", {"reason": reason})
            return self._result(config, series_id, metadata.market_id, Decimal("0"), config.total_quantity, (), reason)

        try:
            source = _as_account(await self.client.account_snapshot(source_index, metadata.market_id))
            receiver = _as_account(await self.client.account_snapshot(receiver_index, metadata.market_id))
            self._validate_initial_accounts(config, metadata, source, receiver)
        except Exception as exc:
            reason = sanitize_exception(exc)
            parent.append("SERIES_PREFLIGHT_BLOCKED", {"reason": reason})
            return self._result(config, series_id, metadata.market_id, Decimal("0"), config.total_quantity, (), reason)
        if config.operator_execution_opt_in and config.operator_plan_reviewed:
            parent.append("SERIES_STARTED", {"binding": binding, "market_id": metadata.market_id, "symbol": metadata.symbol})

        if not config.operator_execution_opt_in or not config.operator_plan_reviewed:
            return self._result(
                config,
                series_id,
                metadata.market_id,
                Decimal("0"),
                config.total_quantity,
                (),
                "explicit series plan review and execution opt-in are required; no child mutation was attempted",
                outcome=Outcome.PREVIEW,
            )

        completed = Decimal("0")
        remaining = config.total_quantity
        children: list[HandoffResult] = []
        expected_source = source.signed_position
        expected_receiver = receiver.signed_position
        expected_source_identity = source.source_identity
        expected_receiver_identity = receiver.source_identity
        child_index = 0
        while remaining > 0:
            if child_index > 0:
                try:
                    source = _as_account(await self.client.account_snapshot(source_index, metadata.market_id))
                    receiver = _as_account(await self.client.account_snapshot(receiver_index, metadata.market_id))
                    self._validate_continuity(
                        config,
                        source,
                        receiver,
                        expected_source,
                        expected_receiver,
                        expected_source_identity,
                        expected_receiver_identity,
                    )
                except Exception as exc:
                    reason = sanitize_exception(exc)
                    parent.append("SERIES_STOPPED", {"reason": reason, "completed_quantity": format(completed, "f")})
                    return self._result(config, series_id, metadata.market_id, completed, remaining, tuple(children), reason)
            try:
                raw_book = await self._read_book(metadata.market_id)
                book = (
                    raw_book
                    if isinstance(raw_book, OrderBookSnapshot)
                    else self._book_from_mapping(raw_book, metadata)
                )
                sizing = size_next_slice(config, metadata, book, remaining_quantity=remaining, now=self.clock.now())
            except Exception as exc:
                reason = sanitize_exception(exc)
                parent.append("SERIES_STOPPED", {"reason": reason, "completed_quantity": format(completed, "f")})
                return self._result(config, series_id, metadata.market_id, completed, remaining, tuple(children), reason)
            if not sizing.tradeable:
                parent.append("SERIES_REMAINDER_UNTRADEABLE", {"child_index": child_index, "sizing": sizing.as_dict()})
                return self._result(
                    config,
                    series_id,
                    metadata.market_id,
                    completed,
                    remaining,
                    tuple(children),
                    sizing.reason,
                    outcome=Outcome.PARTIAL,
                    remainder_reason=sizing.reason,
                )
            child_path = self._child_path(config.journal_path, child_index)
            if Path(child_path).exists() or Path(child_path + ".lock").exists():
                reason = "child journal already exists; refusing a possible mutation replay"
                parent.append("SERIES_STOPPED", {"reason": reason, "child_index": child_index})
                return self._result(config, series_id, metadata.market_id, completed, remaining, tuple(children), reason)
            child_config = config.attempt_config(
                market_id=metadata.market_id,
                quantity=sizing.rounded_quantity,
                journal_path=child_path,
                receiver_worst_price=sizing.effective_receiver_price,
                expected_source_position=(
                    expected_source
                    if config.operation_mode is OperationMode.PAIRED_OPENING
                    else None
                ),
                expected_receiver_position=(
                    expected_receiver
                    if config.operation_mode is OperationMode.PAIRED_OPENING
                    else None
                ),
            )
            parent.append(
                "CHILD_INTENT",
                {
                    "child_index": child_index,
                    "child_journal_path": child_path,
                    "market_id": metadata.market_id,
                    "symbol": metadata.symbol,
                    "operation_mode": config.operation_mode.value,
                    "quantity": format(sizing.rounded_quantity, "f"),
                    "remaining_before": format(remaining, "f"),
                    "source_position_before": format(expected_source, "f"),
                    "receiver_position_before": format(expected_receiver, "f"),
                    "sizing": sizing.as_dict(),
                    "binding": binding,
                    "child_binding": self._child_binding(child_config),
                },
            )
            child = await run_handoff(child_config, self.client, clock=self.clock)
            children.append(child)
            parent.append(
                "CHILD_RESULT",
                {
                    "child_index": child_index,
                    "outcome": child.outcome.value,
                    "quantity": format(sizing.rounded_quantity, "f"),
                    "source_order_id": None if child.source is None else child.source.order_id,
                    "receiver_order_id": None if child.receiver is None else child.receiver.order_id,
                    "source_filled_quantity": self._journal_leg_quantity(child.source),
                    "receiver_filled_quantity": self._journal_leg_quantity(child.receiver),
                    "operation_mode": config.operation_mode.value,
                    "source_history_complete": None if child.source is None else child.source.history_complete,
                    "receiver_history_complete": None if child.receiver is None else child.receiver.history_complete,
                    "unknown_reasons": list(child.unknown_reasons),
                },
            )
            if child.outcome is not Outcome.SUCCESS:
                reason = child.reason or f"child {child_index} ended {child.outcome.value}"
                parent.append("SERIES_STOPPED", {"reason": reason, "child_index": child_index})
                actual = self._cumulative_paired_quantity(completed, child.source, child.receiver)
                return self._result(
                    config,
                    series_id,
                    metadata.market_id,
                    completed,
                    remaining,
                    tuple(children),
                    reason,
                    outcome=child.outcome,
                    actual_filled_quantity=actual,
                    actual_source_filled_quantity=self._cumulative_leg_quantity(
                        completed, child.source
                    ),
                    actual_receiver_filled_quantity=self._cumulative_leg_quantity(
                        completed, child.receiver
                    ),
                )
            try:
                self._validate_child_success(
                    config,
                    child,
                    sizing.rounded_quantity,
                    expected_source,
                    expected_receiver,
                    expected_source_identity,
                    expected_receiver_identity,
                )
                expected_source = child.source.position_after  # type: ignore[union-attr]
                expected_receiver = child.receiver.position_after  # type: ignore[union-attr]
            except Exception as exc:
                reason = sanitize_exception(exc)
                parent.append("SERIES_STOPPED", {"reason": reason, "child_index": child_index})
                return self._result(config, series_id, metadata.market_id, completed, remaining, tuple(children), reason)
            completed += sizing.rounded_quantity
            remaining = config.total_quantity - completed
            parent.append(
                "CHILD_COMPLETE",
                {
                    "child_index": child_index,
                    "quantity": format(sizing.rounded_quantity, "f"),
                    "completed_quantity": format(completed, "f"),
                    "remaining_quantity": format(remaining, "f"),
                    "source_position_after": format(expected_source, "f"),
                    "receiver_position_after": format(expected_receiver, "f"),
                    "source_filled_quantity": format(sizing.rounded_quantity, "f"),
                    "receiver_filled_quantity": format(sizing.rounded_quantity, "f"),
                    "operation_mode": config.operation_mode.value,
                },
            )
            child_index += 1
        parent.append(
            "SERIES_COMPLETE",
            {
                "outcome": Outcome.SUCCESS.value,
                "operation_mode": config.operation_mode.value,
                "completed_quantity": format(completed, "f"),
                "market_id": metadata.market_id,
                "symbol": metadata.symbol,
            },
        )
        return self._result(
            config,
            series_id,
            metadata.market_id,
            completed,
            Decimal("0"),
            tuple(children),
            "",
            outcome=Outcome.SUCCESS,
            actual_filled_quantity=completed,
            actual_source_filled_quantity=completed,
            actual_receiver_filled_quantity=completed,
        )

    @staticmethod
    def _child_binding(config: HandoffConfig) -> dict[str, Any]:
        return {
            "market_id": config.market_id,
            "market_symbol": config.market_symbol,
            "direction": config.direction.value,
            "operation_mode": config.operation_mode.value,
            "mode": config.operation_mode.value,
            "quantity": format(config.quantity, "f"),
            "source_limit_price": format(config.source_limit_price, "f"),
            "receiver_worst_price": format(config.receiver_worst_price, "f"),
            "journal_path": config.journal_path,
            "environment": config.environment,
            "api_base_url": config.api_base_url,
            "chain_id": config.chain_id,
            "expected_source_position": (
                None
                if config.expected_source_position is None
                else format(config.expected_source_position, "f")
            ),
            "expected_receiver_position": (
                None
                if config.expected_receiver_position is None
                else format(config.expected_receiver_position, "f")
            ),
        }

    @staticmethod
    def _journal_leg_quantity(leg: Any) -> str | None:
        if leg is None or not leg.history_complete:
            return None
        return format(leg.filled_quantity, "f")

    @staticmethod
    def _proven_leg_quantity(leg: Any) -> Decimal | None:
        if leg is None or not leg.history_complete:
            return None
        return leg.filled_quantity

    @classmethod
    def _cumulative_leg_quantity(cls, completed: Decimal, leg: Any) -> Decimal | None:
        proven = cls._proven_leg_quantity(leg)
        return None if proven is None else completed + proven

    @classmethod
    def _cumulative_paired_quantity(
        cls,
        completed: Decimal,
        source: Any,
        receiver: Any,
    ) -> Decimal | None:
        source_total = cls._cumulative_leg_quantity(completed, source)
        receiver_total = cls._cumulative_leg_quantity(completed, receiver)
        if (
            source_total is None
            or receiver_total is None
            or source_total <= 0
            or receiver_total <= 0
        ):
            return None
        return min(source_total, receiver_total)

    @staticmethod
    def _paired_totals_or_none(source: Decimal, receiver: Decimal) -> Decimal | None:
        if source <= 0 or receiver <= 0:
            return None
        return min(source, receiver)

    @staticmethod
    def _book_from_mapping(value: Mapping[str, Any], metadata: MarketMetadata) -> OrderBookSnapshot:
        if not isinstance(value, Mapping):
            raise ContractError("order-book mapping is malformed")
        if "market_id" not in value or "symbol" not in value:
            raise ContractError("order-book mapping lacks explicit market identity")
        raw_market_id = value.get("market_id")
        if isinstance(raw_market_id, bool) or not isinstance(raw_market_id, int):
            raise ContractError("order-book mapping market_id is malformed")
        if raw_market_id != metadata.market_id:
            raise PreflightBlocked("order-book mapping market identity does not match current market")
        raw_symbol = value.get("symbol")
        if not isinstance(raw_symbol, str) or raw_symbol.strip().upper() != metadata.symbol.upper():
            raise PreflightBlocked("order-book mapping symbol does not match current market")
        raw_market_type = value.get("market_type")
        if not isinstance(raw_market_type, str) or raw_market_type.strip().lower() != "perp":
            raise PreflightBlocked("order-book mapping is not a perpetual market")
        raw_venue = value.get("venue")
        if not isinstance(raw_venue, str) or raw_venue.strip().lower() not in {"robinhood", "robinhood-chain"}:
            raise PreflightBlocked("order-book mapping is from the wrong venue")
        if "observed_at" not in value:
            raise ContractError("order-book mapping lacks its original observation timestamp")
        observed_at = _timestamp(value.get("observed_at"), "order-book observed_at")
        return OrderBookSnapshot.from_mapping(
            value,
            market_id=metadata.market_id,
            symbol=metadata.symbol,
            observed_at=observed_at,
            venue=raw_venue,
        )

    @classmethod
    def _stored_series_progress(
        cls,
        config: RobinhoodSeriesConfig,
        existing: Sequence[Any],
        source_account_index: int,
        receiver_account_index: int,
        *,
        require_complete: bool,
        series_id: str | None = None,
    ) -> tuple[Decimal, int, Decimal, Decimal]:
        """Validate persisted parent identity and return proven cumulative totals."""

        started = next((event for event in existing if event.event == "SERIES_STARTED"), None)
        if started is None or not isinstance(started.payload.get("binding"), Mapping):
            raise ContractError("series journal lacks its immutable binding")
        stored_binding = started.payload["binding"]
        raw_market_id = stored_binding.get("market_id")
        if isinstance(raw_market_id, bool) or not isinstance(raw_market_id, int) or raw_market_id < 0:
            raise ContractError("series journal binding has no valid market_id")
        if config.market_id is not None and config.market_id != raw_market_id:
            raise ContractError("series journal market_id conflicts with current configuration")
        expected_binding = config.binding(
            source_account_index=source_account_index,
            receiver_account_index=receiver_account_index,
            resolved_market_id=raw_market_id,
        )
        if dict(stored_binding) != expected_binding:
            raise ContractError("series journal binding conflicts with current selection inputs")
        if series_id is not None and started.run_id != series_id:
            raise ContractError("series journal identity conflicts with its parent run")

        intent_events = [event for event in existing if event.event == "CHILD_INTENT"]
        intents: dict[int, Any] = {}
        for event in intent_events:
            raw_index = event.payload.get("child_index")
            if isinstance(raw_index, bool) or not isinstance(raw_index, int) or raw_index < 0:
                raise ContractError("series journal child intent index is malformed")
            if raw_index in intents:
                raise ContractError("series journal contains duplicate child intent")
            persisted_intent_binding = event.payload.get("binding")
            if persisted_intent_binding is not None and persisted_intent_binding != dict(stored_binding):
                raise ContractError("child intent parent binding conflicts with its series")
            intents[raw_index] = event

        complete_events = [event for event in existing if event.event == "CHILD_COMPLETE"]
        by_index: dict[int, Any] = {}
        for event in complete_events:
            raw_index = event.payload.get("child_index")
            if isinstance(raw_index, bool) or not isinstance(raw_index, int) or raw_index < 0:
                raise ContractError("series journal child completion index is malformed")
            if raw_index in by_index:
                raise ContractError("series journal contains duplicate child completion")
            by_index[raw_index] = event
        if by_index and set(by_index) != set(range(max(by_index) + 1)):
            raise ContractError("series journal child completions are not contiguous")
        completed = Decimal("0")
        source_actual = Decimal("0")
        receiver_actual = Decimal("0")
        for index in sorted(by_index):
            event = by_index[index]
            quantity = _positive(event.payload.get("quantity"), "completed child quantity")
            intent = intents.get(index)
            if intent is None:
                raise ContractError("series journal child completion lacks its intent")
            if _positive(intent.payload.get("quantity"), "child intent quantity") != quantity:
                raise ContractError("child completion quantity conflicts with its intent")
            if config.operation_mode is OperationMode.PAIRED_OPENING:
                source_before = _decimal(intent.payload.get("source_position_before"), "child source position before")
                receiver_before = _decimal(intent.payload.get("receiver_position_before"), "child receiver position before")
                expected_source_before = -completed * config.direction.sign
                expected_receiver_before = completed * config.direction.sign
                if source_before != expected_source_before or receiver_before != expected_receiver_before:
                    raise ContractError("paired child position continuity conflicts with its persisted identity")
                source_after = _decimal(event.payload.get("source_position_after"), "child source position after")
                receiver_after = _decimal(event.payload.get("receiver_position_after"), "child receiver position after")
                if source_after != expected_source_before - quantity * config.direction.sign or receiver_after != expected_receiver_before + quantity * config.direction.sign:
                    raise ContractError("paired child completion positions conflict with its persisted quantity")
            child_binding = intent.payload.get("child_binding")
            if child_binding is not None:
                if not isinstance(child_binding, Mapping):
                    raise ContractError("child intent binding is malformed")
                if (
                    child_binding.get("market_id") != raw_market_id
                    or str(child_binding.get("market_symbol", "")).upper() != config.market_symbol
                    or child_binding.get("direction") != config.direction.value
                    or child_binding.get("operation_mode", OperationMode.CLOSE_REOPEN.value)
                    != config.operation_mode.value
                    or _decimal(child_binding.get("quantity"), "child binding quantity") != quantity
                    or child_binding.get("journal_path") != cls._child_path(config.journal_path, index)
                ):
                    raise ContractError("child intent binding conflicts with its persisted identity")
            completed += quantity
            source_value = event.payload.get("source_filled_quantity", format(quantity, "f"))
            receiver_value = event.payload.get("receiver_filled_quantity", format(quantity, "f"))
            source_quantity = _positive(source_value, "completed source quantity")
            receiver_quantity = _positive(receiver_value, "completed receiver quantity")
            if source_quantity != quantity or receiver_quantity != quantity:
                raise ContractError("completed child leg quantities do not match the paired quantity")
            source_actual += source_quantity
            receiver_actual += receiver_quantity
            persisted_total = event.payload.get("completed_quantity")
            if persisted_total is not None and _decimal(persisted_total, "completed cumulative quantity") != completed:
                raise ContractError("completed child cumulative quantity is inconsistent")
        if completed > config.total_quantity:
            raise ContractError("series journal completed quantity exceeds configured total")
        complete_event = next((event for event in reversed(existing) if event.event == "SERIES_COMPLETE"), None)
        if require_complete:
            if complete_event is None:
                raise ContractError("series journal lacks SERIES_COMPLETE")
            if complete_event.payload.get("outcome") != Outcome.SUCCESS.value:
                raise ContractError("series complete event does not prove success")
            persisted_completed = _decimal(
                complete_event.payload.get("completed_quantity"),
                "series completed quantity",
            )
            if persisted_completed != completed or persisted_completed != config.total_quantity:
                raise ContractError("series complete total is inconsistent with child completions")
            persisted_market_id = complete_event.payload.get("market_id")
            if persisted_market_id is not None and persisted_market_id != raw_market_id:
                raise ContractError("series complete market identity is inconsistent")
            persisted_symbol = complete_event.payload.get("symbol")
            if persisted_symbol is not None and str(persisted_symbol).upper() != config.market_symbol:
                raise ContractError("series complete symbol identity is inconsistent")
        elif complete_event is not None:
            raise ContractError("series journal has a terminal completion before restart")
        return completed, raw_market_id, source_actual, receiver_actual

    @classmethod
    def _restart_child_config(
        cls,
        config: RobinhoodSeriesConfig,
        metadata: MarketMetadata,
        intent: Any,
        expected_parent_binding: Mapping[str, Any],
    ) -> tuple[str, Decimal, HandoffConfig]:
        if not isinstance(intent.payload, Mapping):
            raise ContractError("child intent payload is malformed")
        payload = intent.payload
        raw_index = payload.get("child_index")
        if isinstance(raw_index, bool) or not isinstance(raw_index, int) or raw_index < 0:
            raise ContractError("child intent index is malformed")
        child_path = payload.get("child_journal_path")
        expected_path = cls._child_path(config.journal_path, raw_index)
        if not isinstance(child_path, str) or child_path != expected_path:
            raise ContractError("child intent journal path conflicts with its parent")
        if payload.get("binding") != dict(expected_parent_binding):
            raise ContractError("child intent parent binding conflicts with its series")
        quantity = _positive(payload.get("quantity"), "child quantity")
        sizing = payload.get("sizing")
        if not isinstance(sizing, Mapping):
            raise ContractError("child intent lacks persisted sizing")
        persisted_quantity = _positive(sizing.get("rounded_quantity"), "persisted child quantity")
        if persisted_quantity != quantity:
            raise ContractError("child intent quantity conflicts with persisted sizing")
        persisted_bound = _positive(
            sizing.get("effective_receiver_price", sizing.get("depth_price_bound")),
            "persisted effective receiver price",
        )
        expected_bound = _effective_receiver_bound(config, metadata.price_decimals)
        if persisted_bound != expected_bound:
            raise ContractError("child intent effective receiver price conflicts with the configured deviation bound")
        if config.operation_mode is OperationMode.PAIRED_OPENING:
            expected_source_position = _decimal(
                payload.get("source_position_before"), "child source position before"
            )
            expected_receiver_position = _decimal(
                payload.get("receiver_position_before"), "child receiver position before"
            )
        else:
            expected_source_position = None
            expected_receiver_position = None
        child_config = config.attempt_config(
            market_id=metadata.market_id,
            quantity=quantity,
            journal_path=child_path,
            receiver_worst_price=persisted_bound,
            expected_source_position=expected_source_position,
            expected_receiver_position=expected_receiver_position,
        )
        persisted_child_binding = payload.get("child_binding")
        if persisted_child_binding is not None and persisted_child_binding != cls._child_binding(child_config):
            raise ContractError("child intent binding conflicts with persisted child configuration")
        return child_path, quantity, child_config

    @staticmethod
    def _stored_child_result_leg(event: Any, role: str) -> Decimal | None:
        value = event.payload.get(f"{role}_filled_quantity")
        history_complete = event.payload.get(f"{role}_history_complete")
        if value is None or history_complete is not True:
            return None
        return _nonnegative(value, f"stored {role} filled quantity")

    @staticmethod
    def _stored_reconciled_leg(event: Any, role: str) -> Decimal | None:
        """Return only a leg quantity whose reconciliation proved history complete."""

        value = event.payload.get(f"{role}_filled_quantity")
        if value is None or event.payload.get(f"{role}_history_complete") is not True:
            return None
        return _nonnegative(value, f"stored reconciled {role} filled quantity")

    @staticmethod
    def _stored_reconciled_pair(event: Any) -> Decimal | None:
        value = event.payload.get("paired_filled_quantity")
        if value is None:
            return None
        if event.payload.get("outcome") != Outcome.SUCCESS.value:
            raise ContractError("stored reconciled pair is not a proven successful child")
        return _positive(value, "stored reconciled paired quantity")

    async def _handle_restart(
        self,
        config: RobinhoodSeriesConfig,
        parent: DurableJournal,
        metadata: MarketMetadata,
        existing: Sequence[Any],
        *,
        series_id: str,
    ) -> SeriesResult | None:
        source_index, receiver_index = self._account_indices()
        completed_before, persisted_market_id, source_actual, receiver_actual = self._stored_series_progress(
            config,
            existing,
            source_index,
            receiver_index,
            require_complete=False,
            series_id=series_id,
        )
        if persisted_market_id != metadata.market_id:
            raise ContractError("restart market identity conflicts with current catalog")
        expected_parent_binding = config.binding(
            source_account_index=source_index,
            receiver_account_index=receiver_index,
            resolved_market_id=metadata.market_id,
        )

        def event_map(event_name: str) -> dict[int, Any]:
            result: dict[int, Any] = {}
            for event in existing:
                if event.event != event_name:
                    continue
                index = event.payload.get("child_index")
                if isinstance(index, bool) or not isinstance(index, int) or index < 0:
                    raise ContractError(f"{event_name} child index is malformed")
                if index in result:
                    raise ContractError(f"series journal contains duplicate {event_name}")
                result[index] = event
            return result

        child_intents = event_map("CHILD_INTENT")
        child_results = event_map("CHILD_RESULT")
        child_completes = event_map("CHILD_COMPLETE")
        reconciled_results = event_map("SERIES_RECONCILED_CHILD")
        remaining_before = config.total_quantity - completed_before
        pending = sorted(set(child_intents) - set(child_completes))
        if pending:
            if len(pending) != 1:
                raise ContractError("series journal contains more than one incomplete child")
            index = pending[-1]
            intent = child_intents[index]
            child_path = intent.payload.get("child_journal_path")
            if not isinstance(child_path, str) or not child_path:
                reason = "restart child intent lacks a durable child journal binding"
                parent.append("SERIES_RESTART_BLOCKED", {"reason": reason, "child_index": index})
                return self._result(config, series_id, metadata.market_id, completed_before, remaining_before, (), reason)
            if not Path(child_path).exists():
                reason = "restart child intent has no child journal; refusing to start a new attempt"
                parent.append("SERIES_RESTART_BLOCKED", {"reason": reason, "child_index": index})
                return self._result(
                    config,
                    series_id,
                    metadata.market_id,
                    completed_before,
                    remaining_before,
                    (),
                    reason,
                    actual_filled_quantity=self._paired_totals_or_none(source_actual, receiver_actual),
                    actual_source_filled_quantity=source_actual,
                    actual_receiver_filled_quantity=receiver_actual,
                )
            child_path, _quantity, child_config = self._restart_child_config(
                config,
                metadata,
                intent,
                expected_parent_binding,
            )
            child_journal = DurableJournal(child_path)
            if child_journal.has_unresolved_mutation():
                reconciled = await run_handoff(child_config, self.client, clock=self.clock)
                reconciled_source = self._cumulative_leg_quantity(completed_before, reconciled.source)
                reconciled_receiver = self._cumulative_leg_quantity(completed_before, reconciled.receiver)
                parent.append(
                    "SERIES_RECONCILED_CHILD",
                    {
                        "child_index": index,
                        "outcome": reconciled.outcome.value,
                        "source_filled_quantity": self._journal_leg_quantity(reconciled.source),
                        "receiver_filled_quantity": self._journal_leg_quantity(reconciled.receiver),
                        "source_history_complete": (
                            None if reconciled.source is None else reconciled.source.history_complete
                        ),
                        "receiver_history_complete": (
                            None if reconciled.receiver is None else reconciled.receiver.history_complete
                        ),
                        "source_order_id": None if reconciled.source is None else reconciled.source.order_id,
                        "receiver_order_id": None if reconciled.receiver is None else reconciled.receiver.order_id,
                        "unknown_reasons": list(reconciled.unknown_reasons),
                        "paired_filled_quantity": (
                            self._journal_leg_quantity(reconciled.source)
                            if reconciled.outcome is Outcome.SUCCESS
                            and reconciled.source is not None
                            and reconciled.receiver is not None
                            and reconciled.source.history_complete
                            and reconciled.receiver.history_complete
                            and reconciled.source.filled_quantity == reconciled.receiver.filled_quantity
                            else None
                        ),
                    },
                )
                reason = "restart reconciled the interrupted child; explicit series resume is required"
                return self._result(
                    config,
                    series_id,
                    metadata.market_id,
                    completed_before,
                    remaining_before,
                    (reconciled,),
                    reason,
                    outcome=Outcome.UNKNOWN,
                    actual_filled_quantity=(
                        None
                        if reconciled.outcome is not Outcome.SUCCESS
                        or reconciled_source is None
                        or reconciled_receiver is None
                        or reconciled_source != reconciled_receiver
                        else self._paired_totals_or_none(reconciled_source, reconciled_receiver)
                    ),
                    actual_source_filled_quantity=reconciled_source,
                    actual_receiver_filled_quantity=reconciled_receiver,
                )
            reason = "restart found an incomplete child without an unresolved mutation journal; refusing replay"
            parent.append("SERIES_RESTART_BLOCKED", {"reason": reason, "child_index": index})
            stored_result = child_results.get(index)
            reconciled_result = reconciled_results.get(index)
            if stored_result is not None and reconciled_result is not None:
                for role in ("source", "receiver"):
                    if self._stored_child_result_leg(stored_result, role) != self._stored_reconciled_leg(
                        reconciled_result, role
                    ):
                        raise ContractError("child result and reconciliation receipts conflict")
            source_leg = (
                self._stored_reconciled_leg(reconciled_result, "source")
                if reconciled_result is not None
                else self._stored_child_result_leg(stored_result, "source")
                if stored_result is not None
                else None
            )
            receiver_leg = (
                self._stored_reconciled_leg(reconciled_result, "receiver")
                if reconciled_result is not None
                else self._stored_child_result_leg(stored_result, "receiver")
                if stored_result is not None
                else None
            )
            result_source = None if source_leg is None else source_actual + source_leg
            result_receiver = None if receiver_leg is None else receiver_actual + receiver_leg
            persisted_pair = (
                None
                if reconciled_result is None
                else self._stored_reconciled_pair(reconciled_result)
            )
            if persisted_pair is not None and (
                source_leg is None
                or receiver_leg is None
                or persisted_pair != source_leg
                or persisted_pair != receiver_leg
            ):
                raise ContractError("stored reconciled paired quantity conflicts with leg receipts")
            return self._result(
                config,
                series_id,
                metadata.market_id,
                completed_before,
                remaining_before,
                (),
                reason,
                actual_filled_quantity=(
                    None
                    if result_source is None or result_receiver is None
                    else None
                    if reconciled_result is not None and persisted_pair is None
                    else (
                        completed_before + persisted_pair
                        if persisted_pair is not None
                        else self._paired_totals_or_none(result_source, result_receiver)
                    )
                ),
                actual_source_filled_quantity=result_source,
                actual_receiver_filled_quantity=result_receiver,
            )
        if child_completes:
            reason = "restart has completed children; a new child requires a new explicit series run"
            parent.append("SERIES_RESTART_BLOCKED", {"reason": reason})
            return self._result(
                config,
                series_id,
                metadata.market_id,
                completed_before,
                remaining_before,
                (),
                reason,
                actual_filled_quantity=self._paired_totals_or_none(source_actual, receiver_actual),
                actual_source_filled_quantity=source_actual,
                actual_receiver_filled_quantity=receiver_actual,
            )
        if child_results and not child_completes:
            reason = "restart has a child result without a durable child completion"
            parent.append("SERIES_RESTART_BLOCKED", {"reason": reason})
            return self._result(
                config,
                series_id,
                metadata.market_id,
                completed_before,
                remaining_before,
                (),
                reason,
                actual_source_filled_quantity=source_actual,
                actual_receiver_filled_quantity=receiver_actual,
            )
        return None

    async def _resolve_market(self, config: RobinhoodSeriesConfig) -> MarketMetadata:
        resolver = getattr(self.client, "resolve_market", None)
        if resolver is None:
            resolver = getattr(self.client, "resolve_perpetual_market", None)
        if callable(resolver):
            result = await resolver(config.market_symbol)
            return _as_market(result)
        if config.market_id is None:
            raise PreflightBlocked("current perp market ID cannot be resolved from the public catalog")
        return _as_market(await self.client.market_metadata(config.market_id))

    async def _read_book(self, market_id: int) -> OrderBookSnapshot | Mapping[str, Any]:
        method = getattr(self.client, "order_book", None)
        if method is None:
            method = getattr(self.client, "order_book_snapshot", None)
        if method is None:
            method = getattr(self.client, "public_order_book", None)
        if not callable(method):
            raise PreflightBlocked("client has no public Robinhood order-book reader")
        return await method(market_id)

    def _account_indices(self) -> tuple[int, int]:
        try:
            source = _int(getattr(self.client, "source_account_index"), "source_account_index")
            receiver = _int(getattr(self.client, "receiver_account_index"), "receiver_account_index")
        except (AttributeError, ContractError) as exc:
            raise PreflightBlocked("client must expose distinct source and receiver account indices") from exc
        if source == receiver:
            raise PreflightBlocked("source and receiver accounts must differ")
        return source, receiver

    @staticmethod
    def _validate_market(config: RobinhoodSeriesConfig, metadata: MarketMetadata) -> None:
        if metadata.symbol.upper() != config.market_symbol:
            raise PreflightBlocked("market catalog resolved the wrong symbol")
        if metadata.market_type.lower() != "perp":
            raise PreflightBlocked("market catalog resolved a spot market")
        if metadata.venue and metadata.venue.lower() not in {"robinhood", "robinhood-chain"}:
            raise PreflightBlocked("market catalog resolved the wrong venue")
        if config.market_id is not None and metadata.market_id != config.market_id:
            raise PreflightBlocked("configured market_id conflicts with current symbol mapping")
        if metadata.status.lower() not in {"active", "open", "online", "listed"}:
            raise PreflightBlocked("configured Robinhood perpetual is not active")

    @staticmethod
    def _validate_initial_accounts(config: RobinhoodSeriesConfig, metadata: MarketMetadata, source: AccountSnapshot, receiver: AccountSnapshot) -> None:
        if source.market_id != metadata.market_id or receiver.market_id != metadata.market_id:
            raise PreflightBlocked("initial account market identity does not match the resolved perpetual")
        if source.account_index == receiver.account_index:
            raise PreflightBlocked("source and receiver accounts must differ")
        expected = config.direction.sign
        if config.operation_mode is OperationMode.PAIRED_OPENING:
            if source.signed_position != 0 or receiver.signed_position != 0:
                raise PreflightBlocked("paired opening requires both selected-market positions to be flat")
        else:
            if expected > 0 and source.signed_position < config.total_quantity:
                raise PreflightBlocked("source position is smaller than the configured total quantity")
            if expected < 0 and source.signed_position > -config.total_quantity:
                raise PreflightBlocked("source short position is smaller than the configured total quantity")
            if receiver.signed_position * expected < 0:
                raise PreflightBlocked("receiver has opposite exposure")
        for snapshot, label in ((source, "source"), (receiver, "receiver")):
            if not snapshot.authorized or not snapshot.ready:
                raise PreflightBlocked(f"{label} authorization/readiness is unproven")
            if snapshot.active_orders:
                raise PreflightBlocked(f"{label} has active orders")
            if not snapshot.source_identity:
                raise PreflightBlocked(f"{label} account identity is missing")

    @staticmethod
    def _validate_continuity(
        config: RobinhoodSeriesConfig,
        source: AccountSnapshot,
        receiver: AccountSnapshot,
        expected_source: Decimal,
        expected_receiver: Decimal,
        expected_source_identity: str,
        expected_receiver_identity: str,
    ) -> None:
        if source.source_identity == "" or receiver.source_identity == "":
            raise PreflightBlocked("account identity is missing during series continuity check")
        if source.source_identity != expected_source_identity or receiver.source_identity != expected_receiver_identity:
            raise PreflightBlocked("account identity changed between children")
        if source.signed_position != expected_source or receiver.signed_position != expected_receiver:
            raise PreflightBlocked("account positions changed outside the completed child")
        if source.active_orders or receiver.active_orders:
            raise PreflightBlocked("active orders remain between children")

    @staticmethod
    def _validate_child_success(
        config: RobinhoodSeriesConfig,
        child: HandoffResult,
        quantity: Decimal,
        source_before: Decimal,
        receiver_before: Decimal,
        source_identity: str,
        receiver_identity: str,
    ) -> None:
        if child.source is None or child.receiver is None:
            raise PreflightBlocked("successful child has no paired leg reconciliation")
        if child.source.order is None or child.receiver.order is None or child.source.order.active or child.receiver.order.active:
            raise PreflightBlocked("successful child still has an active order")
        if child.plan is None or child.plan.source_identity != source_identity or child.plan.receiver_identity != receiver_identity:
            raise PreflightBlocked("successful child account identity conflicts with the series")
        if child.source.filled_quantity != quantity or child.receiver.filled_quantity != quantity:
            raise PreflightBlocked("successful child does not prove full paired quantity")
        expected = config.direction.sign
        if child.source.position_after - source_before != -expected * quantity:
            raise PreflightBlocked("source position delta conflicts with child quantity")
        if child.receiver.position_after - receiver_before != expected * quantity:
            raise PreflightBlocked("receiver position delta conflicts with child quantity")

    @staticmethod
    def _child_path(parent_path: str, child_index: int) -> str:
        path = Path(parent_path)
        return str(path.with_name(path.name + f".child-{child_index:04d}"))

    @staticmethod
    def _result(
        config: RobinhoodSeriesConfig,
        series_id: str,
        market_id: int | None,
        completed: Decimal,
        remaining: Decimal,
        children: tuple[HandoffResult, ...],
        reason: str,
        *,
        outcome: Outcome = Outcome.UNKNOWN,
        actual_filled_quantity: Decimal | None = None,
        remainder_reason: str = "",
        actual_source_filled_quantity: Decimal | None = None,
        actual_receiver_filled_quantity: Decimal | None = None,
    ) -> SeriesResult:
        return SeriesResult(
            outcome=outcome,
            series_id=series_id,
            market_symbol=config.market_symbol,
            market_id=market_id,
            target_quantity=config.total_quantity,
            completed_quantity=completed,
            remaining_quantity=remaining,
            children=children,
            actual_filled_quantity=actual_filled_quantity,
            reason=reason,
            remainder_reason=remainder_reason,
            actual_source_filled_quantity=actual_source_filled_quantity,
            actual_receiver_filled_quantity=actual_receiver_filled_quantity,
            operation_mode=config.operation_mode,
        )

    @staticmethod
    def _blocked(config: RobinhoodSeriesConfig, reason: str) -> SeriesResult:
        return SeriesResult(
            outcome=Outcome.UNKNOWN,
            series_id="",
            market_symbol=config.market_symbol,
            market_id=None,
            target_quantity=config.total_quantity,
            completed_quantity=Decimal("0"),
            remaining_quantity=config.total_quantity,
            reason=reason,
            operation_mode=config.operation_mode,
        )


async def run_series(config: RobinhoodSeriesConfig, client: RobinhoodSeriesClient, *, clock: Clock | None = None) -> SeriesResult:
    return await RobinhoodSeriesEngine(client, clock=clock).execute(config)


# Short aliases make the bounded API convenient without introducing a second
# concept: both names refer to the same series coordinator and result types.
SeriesConfig = RobinhoodSeriesConfig
SeriesEngine = RobinhoodSeriesEngine
SeriesClient = RobinhoodSeriesClient


__all__ = [
    "DepthLevel",
    "OrderBookSnapshot",
    "RobinhoodSeriesClient",
    "RobinhoodSeriesConfig",
    "RobinhoodSeriesEngine",
    "SeriesClient",
    "SeriesConfig",
    "SeriesEngine",
    "SeriesResult",
    "SliceSizing",
    "run_series",
    "size_next_slice",
]
