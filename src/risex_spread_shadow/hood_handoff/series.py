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

    def __post_init__(self) -> None:
        symbol = _text(self.market_symbol, "market_symbol").upper()
        object.__setattr__(self, "market_symbol", symbol)
        try:
            direction = self.direction if isinstance(self.direction, Direction) else Direction(self.direction)
        except (TypeError, ValueError) as exc:
            raise ContractError("direction must be LONG or SHORT") from exc
        object.__setattr__(self, "direction", direction)
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
    if book.observed_at > now:
        raise PreflightBlocked("order book observation is from the future")
    if now - book.observed_at > config.freshness_seconds:
        raise PreflightBlocked("order book observation is stale")
    source_reference = config.source_limit_price
    deviation = config.allowed_price_deviation
    price_step = Decimal(1).scaleb(-metadata.price_decimals)
    if config.direction is Direction.LONG:
        adverse_limit = source_reference * (Decimal("1") + deviation)
        raw_bound = min(config.receiver_worst_price, adverse_limit)
        hard_limit = (raw_bound / price_step).to_integral_value(rounding=ROUND_DOWN) * price_step
        levels = tuple(level for level in book.asks if level.price <= hard_limit)
    else:
        adverse_floor = source_reference * (Decimal("1") - deviation)
        raw_bound = max(config.receiver_worst_price, adverse_floor)
        hard_floor = (raw_bound / price_step).to_integral_value(rounding=ROUND_CEILING) * price_step
        levels = tuple(level for level in book.bids if level.price >= hard_floor)
    depth = sum((level.quantity for level in levels), Decimal("0"))
    requested = min(remaining, config.desired_slice_quantity)
    metadata_step = Decimal(1).scaleb(-metadata.size_decimals)
    rounded = (min(requested, depth) / metadata_step).to_integral_value(rounding=ROUND_DOWN) * metadata_step
    if rounded <= 0:
        reason = "no fresh executable depth" if depth <= 0 else "executable depth is below venue size step"
        return SliceSizing(requested, depth, Decimal("0"), remaining, hard_limit if config.direction is Direction.LONG else hard_floor, book.observed_at, reason)
    if rounded < metadata.minimum_base_amount:
        return SliceSizing(requested, depth, Decimal("0"), remaining, hard_limit if config.direction is Direction.LONG else hard_floor, book.observed_at, "remaining depth is below venue base minimum")
    if rounded * config.source_limit_price < metadata.minimum_quote_amount:
        return SliceSizing(requested, depth, Decimal("0"), remaining, hard_limit if config.direction is Direction.LONG else hard_floor, book.observed_at, "remaining depth is below venue quote minimum")
    return SliceSizing(requested, depth, rounded, remaining, hard_limit if config.direction is Direction.LONG else hard_floor, book.observed_at)


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

    def as_dict(self) -> dict[str, Any]:
        return {
            "outcome": self.outcome.value,
            "series_id": self.series_id,
            "market_symbol": self.market_symbol,
            "market_id": self.market_id,
            "target_quantity": format(self.target_quantity, "f"),
            "completed_quantity": format(self.completed_quantity, "f"),
            "remaining_quantity": format(self.remaining_quantity, "f"),
            "actual_filled_quantity": None if self.actual_filled_quantity is None else format(self.actual_filled_quantity, "f"),
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
        series_id = parent.run_id
        if any(event.event == "SERIES_COMPLETE" for event in existing):
            return self._result(config, series_id, None, Decimal("0"), config.total_quantity, (), "journal already contains a completed series")
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
                restart_result = await self._handle_restart(config, parent, metadata, existing)
            except Exception as exc:
                reason = "restart journal is malformed: " + sanitize_exception(exc)
                parent.append("SERIES_RESTART_BLOCKED", {"reason": reason})
                return self._result(config, series_id, metadata.market_id, Decimal("0"), config.total_quantity, (), reason)
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
                book = raw_book if isinstance(raw_book, OrderBookSnapshot) else OrderBookSnapshot.from_mapping(
                    raw_book,
                    market_id=metadata.market_id,
                    symbol=metadata.symbol,
                    observed_at=self.clock.now(),
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
            parent.append(
                "CHILD_INTENT",
                {
                    "child_index": child_index,
                    "child_journal_path": child_path,
                    "market_id": metadata.market_id,
                    "symbol": metadata.symbol,
                    "quantity": format(sizing.rounded_quantity, "f"),
                    "remaining_before": format(remaining, "f"),
                    "source_position_before": format(expected_source, "f"),
                    "receiver_position_before": format(expected_receiver, "f"),
                    "sizing": sizing.as_dict(),
                    "binding": binding,
                },
            )
            child_config = config.attempt_config(
                market_id=metadata.market_id,
                quantity=sizing.rounded_quantity,
                journal_path=child_path,
                receiver_worst_price=sizing.effective_receiver_price,
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
                },
            )
            if child.outcome is not Outcome.SUCCESS:
                reason = child.reason or f"child {child_index} ended {child.outcome.value}"
                parent.append("SERIES_STOPPED", {"reason": reason, "child_index": child_index})
                actual = self._actual_for_child(child)
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
                },
            )
            child_index += 1
        parent.append("SERIES_COMPLETE", {"outcome": Outcome.SUCCESS.value, "completed_quantity": format(completed, "f")})
        return self._result(config, series_id, metadata.market_id, completed, Decimal("0"), tuple(children), "", outcome=Outcome.SUCCESS)

    async def _handle_restart(
        self,
        config: RobinhoodSeriesConfig,
        parent: DurableJournal,
        metadata: MarketMetadata,
        existing: Sequence[Any],
    ) -> SeriesResult | None:
        child_intents = {event.payload.get("child_index"): event for event in existing if event.event == "CHILD_INTENT"}
        child_results = {event.payload.get("child_index"): event for event in existing if event.event == "CHILD_RESULT"}
        child_completes = {event.payload.get("child_index"): event for event in existing if event.event == "CHILD_COMPLETE"}
        completed_before = sum(
            (_decimal(item.payload.get("quantity"), "completed quantity") for item in child_completes.values()),
            Decimal("0"),
        )
        remaining_before = config.total_quantity - completed_before
        if completed_before < 0 or completed_before > config.total_quantity:
            raise ContractError("restart journal completed quantity exceeds configured total")
        pending = sorted(set(child_intents) - set(child_completes))
        if pending:
            index = pending[-1]
            intent = child_intents[index]
            child_path = intent.payload.get("child_journal_path")
            if not isinstance(child_path, str) or not child_path:
                reason = "restart child intent lacks a durable child journal binding"
                parent.append("SERIES_RESTART_BLOCKED", {"reason": reason, "child_index": index})
                return self._result(config, parent.run_id, metadata.market_id, completed_before, remaining_before, (), reason)
            if not Path(child_path).exists():
                reason = "restart child intent has no child journal; refusing to start a new attempt"
                parent.append("SERIES_RESTART_BLOCKED", {"reason": reason, "child_index": index})
                return self._result(config, parent.run_id, metadata.market_id, completed_before, remaining_before, (), reason)
            child_journal = DurableJournal(child_path)
            if child_journal.has_unresolved_mutation():
                quantity = _positive(intent.payload.get("quantity"), "child quantity")
                child_config = config.attempt_config(
                    market_id=metadata.market_id,
                    quantity=quantity,
                    journal_path=child_path,
                )
                reconciled = await run_handoff(child_config, self.client, clock=self.clock)
                parent.append("SERIES_RECONCILED_CHILD", {"child_index": index, "outcome": reconciled.outcome.value})
                reason = "restart reconciled the interrupted child; explicit series resume is required"
                return self._result(config, parent.run_id, metadata.market_id, completed_before, remaining_before, (reconciled,), reason, outcome=Outcome.UNKNOWN, actual_filled_quantity=self._actual_for_child(reconciled))
            reason = "restart found an incomplete child without an unresolved mutation journal; refusing replay"
            parent.append("SERIES_RESTART_BLOCKED", {"reason": reason, "child_index": index})
            return self._result(config, parent.run_id, metadata.market_id, completed_before, remaining_before, (), reason)
        if child_completes:
            reason = "restart has completed children; a new child requires a new explicit series run"
            parent.append("SERIES_RESTART_BLOCKED", {"reason": reason})
            return self._result(config, parent.run_id, metadata.market_id, completed_before, remaining_before, (), reason)
        if child_results and not child_completes:
            reason = "restart has a child result without a durable child completion"
            parent.append("SERIES_RESTART_BLOCKED", {"reason": reason})
            return self._result(config, parent.run_id, metadata.market_id, completed_before, remaining_before, (), reason)
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
    def _actual_for_child(child: HandoffResult) -> Decimal | None:
        if child.outcome is Outcome.SUCCESS:
            return child.quantity
        if child.outcome is Outcome.PARTIAL and child.source is not None and child.receiver is not None:
            return min(child.source.filled_quantity, child.receiver.filled_quantity)
        return None

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
