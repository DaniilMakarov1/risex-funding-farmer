"""One bounded random-size, timed paired-opening cycle.

The coordinator deliberately composes the existing HCR handoff engine.  It
adds only the finite operator lifecycle required by HCR-17: a fresh public
selection, one random legal size and hold, one paired reduce-only close, and
separately reconciled reduce-only market residuals until both accounts are
flat.  It is not a campaign runner and never adopts positions from an earlier
cycle.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
import hashlib
import math
import os
from pathlib import Path
import random
from typing import Any, Callable, Mapping, Protocol, Sequence, runtime_checkable
from urllib.parse import urlsplit

from .contracts import (
    AccountSnapshot,
    ContractError,
    Direction,
    HandoffConfig,
    HandoffResult,
    MarketMetadata,
    MutationReceipt,
    OFFICIAL_ROBINHOOD_API_URL,
    OFFICIAL_ROBINHOOD_CHAIN_ID,
    OperationMode,
    OrderPlan,
    OrderSnapshot,
    Outcome,
    Phase,
    PreflightBlocked,
    TradeReceipt,
    decimal_to_integer,
)
from .engine import (
    Clock,
    HandoffClient,
    SystemClock,
    _as_order,
    _as_page,
    _as_receipt,
    run_handoff,
)
from .journal import DurableJournal, sanitize_exception
from .local_attempt import select_automatic_prices
from .series import OrderBookSnapshot


MIN_HOLD_SECONDS = 20
MAX_HOLD_SECONDS = 300
CYCLE_JOURNAL_NAME = "cycle.jsonl"
OPENING_JOURNAL_NAME = "opening.jsonl"
CLOSING_JOURNAL_NAME = "closing.jsonl"


def _decimal(value: Any, name: str, *, positive: bool = False) -> Decimal:
    if isinstance(value, bool) or isinstance(value, float):
        raise ContractError(f"{name} must be an exact decimal string or Decimal")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ContractError(f"{name} must be a finite decimal") from exc
    if not parsed.is_finite() or (positive and parsed <= 0):
        raise ContractError(f"{name} must be a finite {'positive ' if positive else ''}decimal")
    return parsed


def _positive(value: Any, name: str) -> Decimal:
    return _decimal(value, name, positive=True)


def _int(value: Any, name: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ContractError(f"{name} must be an integer at least {minimum}")
    return value


def _finite_time(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise ContractError(f"{name} must be a finite positive number")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ContractError(f"{name} must be a finite positive number") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise ContractError(f"{name} must be a finite positive number")
    return parsed


def _text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{name} must be non-empty text")
    return value.strip()


@dataclass(frozen=True, slots=True)
class RandomCycleConfig:
    """Fixed operator inputs for one finite cycle.

    Quantity and hold are intentionally absent from the inputs.  They are
    sampled exactly once after the post-launch observations.  ``cycle_dir``
    and ``journal_path`` are interchangeable convenience inputs; the journal
    always lives inside the consumed cycle directory.
    """

    market_id: int
    market_symbol: str
    direction: Direction | str
    source_account_index: int
    receiver_account_index: int
    client_order_prefix: str = "hood-cycle"
    cycle_dir: Path | str | None = None
    journal_path: Path | str | None = None
    freshness_seconds: float = 10.0
    request_timeout_seconds: float = 5.0
    order_timeout_seconds: float = 10.0
    reconcile_timeout_seconds: float = 20.0
    poll_interval_seconds: float = 0.5
    max_poll_count: int = 40
    source_order_lifetime_seconds: int = 300
    environment: str = "robinhood"
    api_base_url: str | None = "https://api.rh.lighter.xyz"
    chain_id: int | None = 466324
    api_key_index: int | None = None
    auth_token_lifetime_seconds: int = 600
    operator_execution_opt_in: bool = False
    operator_plan_reviewed: bool = False
    defer_incremental_margin_calculation: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "market_id", _int(self.market_id, "market_id"))
        object.__setattr__(self, "market_symbol", _text(self.market_symbol, "market_symbol").upper())
        try:
            parsed_direction = self.direction if isinstance(self.direction, Direction) else Direction(str(self.direction).upper())
        except (TypeError, ValueError) as exc:
            raise ContractError("direction must be LONG or SHORT") from exc
        object.__setattr__(self, "direction", parsed_direction)
        object.__setattr__(self, "source_account_index", _int(self.source_account_index, "source_account_index"))
        object.__setattr__(self, "receiver_account_index", _int(self.receiver_account_index, "receiver_account_index"))
        if self.source_account_index == self.receiver_account_index:
            raise ContractError("source and receiver accounts must differ")
        object.__setattr__(self, "client_order_prefix", _text(self.client_order_prefix, "client_order_prefix"))

        raw_cycle_dir = None if self.cycle_dir is None else Path(self.cycle_dir)
        raw_journal = None if self.journal_path is None else Path(self.journal_path)
        if raw_cycle_dir is None and raw_journal is None:
            raise ContractError("cycle_dir or journal_path is required")
        if raw_cycle_dir is None:
            raw_cycle_dir = raw_journal.parent
        if raw_journal is None:
            raw_journal = raw_cycle_dir / CYCLE_JOURNAL_NAME
        if raw_journal.parent != raw_cycle_dir:
            raise ContractError("journal_path must be directly inside cycle_dir")
        if not str(raw_cycle_dir).strip() or raw_cycle_dir.name in {"", ".", ".."}:
            raise ContractError("cycle_dir must be a concrete directory")
        object.__setattr__(self, "cycle_dir", raw_cycle_dir)
        object.__setattr__(self, "journal_path", raw_journal)

        for name in (
            "freshness_seconds",
            "request_timeout_seconds",
            "order_timeout_seconds",
            "reconcile_timeout_seconds",
            "poll_interval_seconds",
        ):
            object.__setattr__(self, name, _finite_time(getattr(self, name), name))
        object.__setattr__(self, "max_poll_count", _int(self.max_poll_count, "max_poll_count", minimum=1))
        object.__setattr__(
            self,
            "source_order_lifetime_seconds",
            _int(self.source_order_lifetime_seconds, "source_order_lifetime_seconds", minimum=300),
        )
        if self.source_order_lifetime_seconds > 30 * 24 * 60 * 60:
            raise ContractError("source_order_lifetime_seconds must not exceed 30 days")
        environment = _text(self.environment, "environment").lower()
        if environment != "robinhood":
            raise ContractError("random cycle only supports the Robinhood Chain environment")
        object.__setattr__(self, "environment", environment)
        if self.api_base_url is None:
            if self.operator_execution_opt_in:
                raise ContractError("api_base_url is required for Robinhood execution")
        else:
            if not isinstance(self.api_base_url, str):
                raise ContractError("api_base_url must be the exact Robinhood Chain Lighter endpoint")
            parsed_url = urlsplit(self.api_base_url)
            normalized_url = self.api_base_url.rstrip("/")
            if (
                normalized_url != OFFICIAL_ROBINHOOD_API_URL
                or parsed_url.scheme != "https"
                or parsed_url.netloc != "api.rh.lighter.xyz"
                or parsed_url.username is not None
                or parsed_url.password is not None
                or parsed_url.query
                or parsed_url.fragment
                or parsed_url.path not in {"", "/"}
            ):
                raise ContractError("api_base_url must be the exact Robinhood Chain Lighter endpoint")
            object.__setattr__(self, "api_base_url", normalized_url)
        if self.chain_id != OFFICIAL_ROBINHOOD_CHAIN_ID:
            raise ContractError("chain_id must be Robinhood signing domain 466324")
        if self.api_key_index is not None:
            object.__setattr__(self, "api_key_index", _int(self.api_key_index, "api_key_index", minimum=4))
            if self.api_key_index > 254:
                raise ContractError("api_key_index must be in 4..254")
        if self.chain_id is not None:
            object.__setattr__(self, "chain_id", _int(self.chain_id, "chain_id"))
        object.__setattr__(
            self,
            "auth_token_lifetime_seconds",
            _int(self.auth_token_lifetime_seconds, "auth_token_lifetime_seconds", minimum=60),
        )
        if self.auth_token_lifetime_seconds > 8 * 60 * 60:
            raise ContractError("auth_token_lifetime_seconds must not exceed the documented 8-hour lifetime")
        for name in ("operator_execution_opt_in", "operator_plan_reviewed", "defer_incremental_margin_calculation"):
            if not isinstance(getattr(self, name), bool):
                raise ContractError(f"{name} must be bool")

    @property
    def opening_journal_path(self) -> Path:
        return self.cycle_dir / OPENING_JOURNAL_NAME

    @property
    def closing_journal_path(self) -> Path:
        return self.cycle_dir / CLOSING_JOURNAL_NAME

    def binding(self) -> dict[str, Any]:
        return {
            "market_id": self.market_id,
            "market_symbol": self.market_symbol,
            "direction": self.direction.value,
            "source_account_index": self.source_account_index,
            "receiver_account_index": self.receiver_account_index,
            "client_order_prefix": self.client_order_prefix,
            "environment": self.environment,
            "api_base_url": self.api_base_url,
            "chain_id": self.chain_id,
            "api_key_index": self.api_key_index,
            "auth_token_lifetime_seconds": self.auth_token_lifetime_seconds,
            "freshness_seconds": self.freshness_seconds,
            "request_timeout_seconds": self.request_timeout_seconds,
            "order_timeout_seconds": self.order_timeout_seconds,
            "reconcile_timeout_seconds": self.reconcile_timeout_seconds,
            "poll_interval_seconds": self.poll_interval_seconds,
            "max_poll_count": self.max_poll_count,
            "source_order_lifetime_seconds": self.source_order_lifetime_seconds,
            "defer_incremental_margin_calculation": self.defer_incremental_margin_calculation,
        }


@dataclass(frozen=True, slots=True)
class RandomQuantityBounds:
    opening_price: Decimal
    size_step: Decimal
    lower_tick: int
    upper_tick: int
    minimum_base_tick: int
    minimum_quote_tick: int
    source_available_balance: Decimal
    receiver_available_balance: Decimal

    @property
    def lower_quantity(self) -> Decimal:
        return self.lower_tick * self.size_step

    @property
    def upper_quantity(self) -> Decimal:
        return self.upper_tick * self.size_step

    def as_dict(self) -> dict[str, Any]:
        return {
            "opening_price": format(self.opening_price, "f"),
            "size_step": format(self.size_step, "f"),
            "minimum_base_tick": self.minimum_base_tick,
            "minimum_quote_tick": self.minimum_quote_tick,
            "lower_tick": self.lower_tick,
            "upper_tick": self.upper_tick,
            "lower_quantity": format(self.lower_quantity, "f"),
            "upper_quantity": format(self.upper_quantity, "f"),
            "source_available_balance": format(self.source_available_balance, "f"),
            "receiver_available_balance": format(self.receiver_available_balance, "f"),
        }


def _available_balance(snapshot: AccountSnapshot, label: str) -> Decimal:
    value = snapshot.available_balance
    if value is None:
        raise PreflightBlocked(f"{label} available_balance evidence is missing")
    return _decimal(value, f"{label}.available_balance")


def compute_quantity_bounds(
    metadata: MarketMetadata,
    source: AccountSnapshot,
    receiver: AccountSnapshot,
    opening_price: Decimal,
) -> RandomQuantityBounds:
    """Compute legal integer quantity ticks without leverage or float math."""

    price = _positive(opening_price, "opening_price")
    price_int = decimal_to_integer(price, metadata.price_decimals, "opening_price")
    del price_int
    step = Decimal(1).scaleb(-metadata.size_decimals)
    source_balance = _available_balance(source, "source")
    receiver_balance = _available_balance(receiver, "receiver")
    lower_base = int(
        (metadata.minimum_base_amount / step).to_integral_value(rounding=ROUND_CEILING)
    )
    lower_quote = int(
        (metadata.minimum_quote_amount / price / step).to_integral_value(rounding=ROUND_CEILING)
    )
    upper = int(
        (min(source_balance, receiver_balance) / price / step).to_integral_value(rounding=ROUND_FLOOR)
    )
    lower = max(lower_base, lower_quote)
    if lower <= 0:
        raise PreflightBlocked("venue minimums produce no positive size tick")
    if upper < lower:
        raise PreflightBlocked("smaller available balance cannot fund the venue minimum quantity")
    return RandomQuantityBounds(
        opening_price=price,
        size_step=step,
        lower_tick=lower,
        upper_tick=upper,
        minimum_base_tick=lower_base,
        minimum_quote_tick=lower_quote,
        source_available_balance=source_balance,
        receiver_available_balance=receiver_balance,
    )


def _draw_integer(rng: Any, lower: int, upper: int, name: str) -> int:
    method = getattr(rng, "randint", None)
    if callable(method):
        value = method(lower, upper)
    else:
        method = getattr(rng, "randrange", None)
        if not callable(method):
            raise ContractError(f"random source cannot draw {name}")
        value = method(lower, upper + 1)
    if isinstance(value, bool) or not isinstance(value, int) or not lower <= value <= upper:
        raise ContractError(f"random source returned an invalid {name} draw")
    return value


def select_random_quantity(
    bounds: RandomQuantityBounds,
    rng: Any,
    *,
    hold_seconds: int | None = None,
) -> tuple[Decimal, int, int]:
    """Draw one legal quantity tick and one fixed-range hold integer."""

    tick = _draw_integer(rng, bounds.lower_tick, bounds.upper_tick, "quantity")
    hold = (
        _draw_integer(rng, MIN_HOLD_SECONDS, MAX_HOLD_SECONDS, "hold_seconds")
        if hold_seconds is None
        else _int(hold_seconds, "hold_seconds", minimum=MIN_HOLD_SECONDS)
    )
    if hold > MAX_HOLD_SECONDS:
        raise ContractError("hold_seconds must not exceed 300")
    return tick * bounds.size_step, tick, hold


@dataclass(frozen=True, slots=True)
class RandomCycleSelection:
    quantity: Decimal
    quantity_tick: int
    hold_seconds: int
    opening_source_price: Decimal
    opening_receiver_bound: Decimal
    bounds: RandomQuantityBounds
    metadata_observed_at: float
    book_observed_at: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "quantity": format(self.quantity, "f"),
            "quantity_tick": self.quantity_tick,
            "hold_seconds": self.hold_seconds,
            "opening_source_price": format(self.opening_source_price, "f"),
            "opening_receiver_bound": format(self.opening_receiver_bound, "f"),
            "bounds": self.bounds.as_dict(),
            "metadata_observed_at": self.metadata_observed_at,
            "book_observed_at": self.book_observed_at,
        }


@dataclass(frozen=True, slots=True)
class FallbackResult:
    account_index: int
    side: str
    requested_quantity: Decimal
    attempted: bool
    outcome: Outcome
    order_id: str | None = None
    filled_quantity: Decimal | None = None
    position_after: Decimal | None = None
    reason: str | None = None
    attempt: int = 1

    def as_dict(self) -> dict[str, Any]:
        return {
            "account_index": self.account_index,
            "side": self.side,
            "requested_quantity": format(self.requested_quantity, "f"),
            "attempted": self.attempted,
            "outcome": self.outcome.value,
            "order_id": self.order_id,
            "filled_quantity": None if self.filled_quantity is None else format(self.filled_quantity, "f"),
            "position_after": None if self.position_after is None else format(self.position_after, "f"),
            "reason": self.reason,
            "attempt": self.attempt,
        }


@dataclass(frozen=True, slots=True)
class RandomCycleResult:
    outcome: Outcome
    phase: Phase
    run_id: str
    selection: RandomCycleSelection | None = None
    opening: HandoffResult | None = None
    closing: HandoffResult | None = None
    fallbacks: tuple[FallbackResult, ...] = ()
    remaining_source_position: Decimal | None = None
    remaining_receiver_position: Decimal | None = None
    reason: str | None = None
    journal_path: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "outcome": self.outcome.value,
            "phase": self.phase.value,
            "run_id": self.run_id,
            "selection": None if self.selection is None else self.selection.as_dict(),
            "opening": None if self.opening is None else self.opening.as_dict(),
            "closing": None if self.closing is None else self.closing.as_dict(),
            "fallbacks": [item.as_dict() for item in self.fallbacks],
            "remaining_positions": {
                "source": None
                if self.remaining_source_position is None
                else format(self.remaining_source_position, "f"),
                "receiver": None
                if self.remaining_receiver_position is None
                else format(self.remaining_receiver_position, "f"),
            },
            "reason": self.reason,
            "journal_path": self.journal_path,
        }


@runtime_checkable
class RandomCycleClient(HandoffClient, Protocol):
    source_account_index: int
    receiver_account_index: int

    async def order_book(self, market_id: int) -> OrderBookSnapshot | Mapping[str, Any]: ...


class _BoundMarketClient:
    """Keep pre-mutation metadata and account identities across a child run.

    ``run_handoff`` performs its own preflight and account rechecks.  A random
    cycle close has already established the opening account identities before
    entering that child, so a later account snapshot must be checked against
    those identities instead of becoming a new binding.
    """

    def __init__(
        self,
        delegate: RandomCycleClient,
        metadata: MarketMetadata,
        *,
        source_identity: str | None = None,
        receiver_identity: str | None = None,
        identity_failure_callback: Callable[[str], None] | None = None,
    ) -> None:
        self._delegate = delegate
        self._metadata = metadata
        self._source_identity = source_identity
        self._receiver_identity = receiver_identity
        self._identity_failure_callback = identity_failure_callback
        self.source_account_index = delegate.source_account_index
        self.receiver_account_index = delegate.receiver_account_index
        self.sdk_version = getattr(delegate, "sdk_version", None)

    async def market_metadata(self, market_id: int) -> MarketMetadata:
        if market_id != self._metadata.market_id:
            raise ContractError("bound market identity does not match configured market")
        return self._metadata

    async def account_snapshot(self, account_index: int, market_id: int) -> AccountSnapshot:
        if market_id != self._metadata.market_id:
            raise ContractError("bound account market identity does not match configured market")
        raw = await self._delegate.account_snapshot(account_index, market_id)
        snapshot = raw if isinstance(raw, AccountSnapshot) else AccountSnapshot.from_mapping(raw)
        if account_index == self.source_account_index:
            label = "source"
            expected_identity = self._source_identity
        elif account_index == self.receiver_account_index:
            label = "receiver"
            expected_identity = self._receiver_identity
        else:
            raise ContractError("bound account identity does not match configured cycle account")
        if expected_identity is not None and snapshot.source_identity != expected_identity:
            reason = f"{label} account identity changed after it was bound"
            if self._identity_failure_callback is not None:
                self._identity_failure_callback(reason)
            raise PreflightBlocked(reason)
        return snapshot

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)


def _as_market(value: MarketMetadata | Mapping[str, Any]) -> MarketMetadata:
    return value if isinstance(value, MarketMetadata) else MarketMetadata.from_mapping(value)


def _metadata_payload(metadata: MarketMetadata) -> dict[str, Any]:
    return {
        "market_id": metadata.market_id,
        "symbol": metadata.symbol,
        "status": metadata.status,
        "market_type": metadata.market_type,
        "venue": metadata.venue,
        "price_decimals": metadata.price_decimals,
        "size_decimals": metadata.size_decimals,
        "minimum_base_amount": format(metadata.minimum_base_amount, "f"),
        "minimum_quote_amount": format(metadata.minimum_quote_amount, "f"),
        "observed_at": metadata.observed_at,
        "margin_evidence": metadata.margin_evidence,
    }


def _order_payload(order: OrderSnapshot | None) -> dict[str, Any] | None:
    """Serialize one order with only the reconciliation fields we trust."""

    if order is None:
        return None
    return {
        "account_index": order.account_index,
        "market_id": order.market_id,
        "order_id": order.order_id,
        "client_order_index": order.client_order_index,
        "status": order.status,
        "side": order.side,
        "order_type": order.order_type,
        "time_in_force": order.time_in_force,
        "reduce_only": order.reduce_only,
        "initial_quantity": format(order.initial_quantity, "f"),
        "remaining_quantity": format(order.remaining_quantity, "f"),
        "filled_quantity": format(order.filled_quantity, "f"),
        "price": None if order.price is None else format(order.price, "f"),
        "observed_at": order.observed_at,
    }


def _trade_payload(trade: TradeReceipt) -> dict[str, Any]:
    """Serialize a sanitized trade receipt for durable post-run review."""

    return {
        "trade_id": trade.trade_id,
        "account_index": trade.account_index,
        "market_id": trade.market_id,
        "order_id": trade.order_id,
        "client_order_index": trade.client_order_index,
        "side": trade.side,
        "quantity": format(trade.quantity, "f"),
        "price": format(trade.price, "f"),
        "fee": None if trade.fee is None else format(trade.fee, "f"),
        "counterparty_account_index": trade.counterparty_account_index,
        "counterparty_order_id": trade.counterparty_order_id,
        "counterparty_client_order_index": trade.counterparty_client_order_index,
        "observed_at": trade.observed_at,
    }


def _account_payload(account: AccountSnapshot | None) -> dict[str, Any] | None:
    """Serialize the account state needed to verify a fallback admission."""

    if account is None:
        return None
    return {
        "account_index": account.account_index,
        "market_id": account.market_id,
        "signed_position": format(account.signed_position, "f"),
        "active_orders": [_order_payload(order) for order in account.active_orders],
        "observed_at": account.observed_at,
        "authorized": account.authorized,
        "ready": account.ready,
        "margin_available": (
            None if account.margin_available is None else format(account.margin_available, "f")
        ),
        "margin_required": (
            None if account.margin_required is None else format(account.margin_required, "f")
        ),
        "fee_rate": None if account.fee_rate is None else format(account.fee_rate, "f"),
        "source_identity": account.source_identity,
        "incremental_margin_required": (
            None
            if account.incremental_margin_required is None
            else format(account.incremental_margin_required, "f")
        ),
        "incremental_margin_evidence": account.incremental_margin_evidence,
        "available_balance": (
            None if account.available_balance is None else format(account.available_balance, "f")
        ),
        "margin_evidence": (
            None if account.margin_evidence is None else account.margin_evidence.as_dict()
        ),
    }


def _receipt_payload(receipt: MutationReceipt | None) -> dict[str, Any] | None:
    if receipt is None:
        return None
    return {
        "accepted": receipt.accepted,
        "order_id": receipt.order_id,
        "tx_hash": receipt.tx_hash,
        "response_code": receipt.response_code,
        "error": receipt.error,
    }


def _history_page_payload(page: Any) -> dict[str, Any]:
    """Keep page boundaries and every sanitized trade receipt."""

    return {
        "trades": [_trade_payload(trade) for trade in page.trades],
        "orders": [_order_payload(order) for order in page.orders],
        "next_cursor": page.next_cursor,
        "complete": page.complete,
    }


def _as_book(value: OrderBookSnapshot | Mapping[str, Any], metadata: MarketMetadata) -> OrderBookSnapshot:
    if isinstance(value, OrderBookSnapshot):
        return value
    if not isinstance(value, Mapping):
        raise ContractError("order-book observation has an unsupported shape")
    if "market_id" not in value or "symbol" not in value:
        raise ContractError("order-book observation lacks explicit market identity")
    raw_market_id = value.get("market_id")
    if isinstance(raw_market_id, bool) or not isinstance(raw_market_id, int):
        raise ContractError("order-book observation market_id is malformed")
    if raw_market_id != metadata.market_id:
        raise PreflightBlocked("order-book observation market identity does not match current market")
    raw_symbol = value.get("symbol")
    if not isinstance(raw_symbol, str) or raw_symbol.strip().upper() != metadata.symbol.upper():
        raise PreflightBlocked("order-book observation symbol does not match current market")
    raw_market_type = value.get("market_type")
    if not isinstance(raw_market_type, str) or raw_market_type.strip().lower() != "perp":
        raise PreflightBlocked("order-book observation is not a perpetual market")
    raw_venue = value.get("venue")
    if not isinstance(raw_venue, str) or raw_venue.strip().lower() not in {"robinhood", "robinhood-chain"}:
        raise PreflightBlocked("order-book observation is from the wrong venue")
    observed_at = value.get("observed_at")
    if observed_at is None:
        raise ContractError("order-book observation timestamp is missing")
    return OrderBookSnapshot.from_mapping(
        value,
        market_id=metadata.market_id,
        symbol=metadata.symbol,
        observed_at=observed_at,
        venue=raw_venue,
    )


def _clock_monotonic(clock: Clock) -> float:
    method = getattr(clock, "monotonic", None)
    if callable(method):
        value = method()
    else:
        value = clock.now()
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ContractError("cycle clock returned an invalid monotonic value") from exc
    if not math.isfinite(parsed):
        raise ContractError("cycle clock returned a non-finite monotonic value")
    return parsed


def _client_order_index(run_id: str, leg: str) -> int:
    digest = hashlib.sha256(f"{run_id}:{leg}".encode("utf-8")).digest()
    value = int.from_bytes(digest[:6], "big") & ((1 << 47) - 1)
    return value or 1


def _inverse(direction: Direction) -> Direction:
    return Direction.SHORT if direction is Direction.LONG else Direction.LONG


def _expected_cycle_signs(direction: Direction) -> tuple[int, int]:
    # The source opens the opposite exposure and the receiver follows it.
    return -direction.sign, direction.sign


def _position_residual(
    position: Decimal,
    expected_sign: int,
    cap: Decimal,
) -> Decimal | None:
    if position == 0:
        return Decimal(0)
    if position * expected_sign < 0:
        return None
    if abs(position) > cap:
        return None
    return abs(position)


def _validate_market_book(
    config: RandomCycleConfig,
    metadata: MarketMetadata,
    book: OrderBookSnapshot,
    now: float,
) -> None:
    if metadata.market_id != config.market_id or book.market_id != config.market_id:
        raise PreflightBlocked("current market/book identity does not match configured market")
    if metadata.symbol.upper() != config.market_symbol or book.symbol.upper() != config.market_symbol:
        raise PreflightBlocked("current market/book symbol does not match configured market")
    if metadata.market_type.lower() != "perp" or book.market_type.lower() != "perp":
        raise PreflightBlocked("current market/book is not a perpetual")
    if metadata.status.lower() not in {"active", "open", "online", "listed"}:
        raise PreflightBlocked("current market is not active")
    if metadata.venue.lower() not in {"", "robinhood", "robinhood-chain"}:
        raise PreflightBlocked("current market is from the wrong venue")
    if book.venue.lower() not in {"robinhood", "robinhood-chain"}:
        raise PreflightBlocked("current order book is from the wrong venue")
    for observed_at, label in ((metadata.observed_at, "market metadata"), (book.observed_at, "order book")):
        if observed_at > now:
            raise PreflightBlocked(f"{label} is from the future")
        if now - observed_at > config.freshness_seconds:
            raise PreflightBlocked(f"{label} is stale")
    if not book.bids or not book.asks or book.bids[0].price >= book.asks[0].price:
        raise PreflightBlocked("current order book must be two-sided and uncrossed")
    decimal_to_integer(book.bids[0].price, metadata.price_decimals, "best bid")
    decimal_to_integer(book.asks[0].price, metadata.price_decimals, "best ask")
    if not metadata.margin_evidence:
        raise PreflightBlocked("minimum/margin evidence is missing")


def _validate_account(
    config: RandomCycleConfig,
    snapshot: AccountSnapshot,
    label: str,
    *,
    expected_position: Decimal | None = None,
) -> None:
    account_index = config.source_account_index if label == "source" else config.receiver_account_index
    if snapshot.account_index != account_index or snapshot.market_id != config.market_id:
        raise PreflightBlocked(f"{label} account identity/market does not match cycle binding")
    if not snapshot.authorized or not snapshot.ready:
        raise PreflightBlocked(f"{label} account authorization/readiness is unproven")
    if snapshot.active_orders:
        raise PreflightBlocked(f"{label} has active cycle-market orders")
    if snapshot.margin_available is None or snapshot.margin_required is None:
        raise PreflightBlocked(f"{label} margin evidence is missing")
    if snapshot.margin_required > snapshot.margin_available:
        raise PreflightBlocked(f"{label} current margin is insufficient")
    if expected_position is not None and snapshot.signed_position != expected_position:
        raise PreflightBlocked(f"{label} position changed before the dependent cycle phase")


def _validate_account_fresh(config: RandomCycleConfig, snapshot: AccountSnapshot, label: str, now: float, *, expected_position: Decimal | None = None) -> None:
    _validate_account(config, snapshot, label, expected_position=expected_position)
    if snapshot.observed_at > now:
        raise PreflightBlocked(f"{label} account state is from the future")
    if now - snapshot.observed_at > config.freshness_seconds:
        raise PreflightBlocked(f"{label} account state is stale")
    if not snapshot.source_identity:
        raise PreflightBlocked(f"{label} account identity is missing")


def _cycle_exception_reason(exc: BaseException) -> str:
    """Keep local contract refusals readable while masking SDK boundaries."""

    if isinstance(exc, (ContractError, PreflightBlocked)):
        return str(exc)
    return sanitize_exception(exc)


class RandomCycleEngine:
    """Execute one sampled cycle and then stop."""

    def __init__(self, client: RandomCycleClient, *, clock: Clock | None = None, rng: Any | None = None) -> None:
        self.client = client
        self.clock = clock or SystemClock()
        self.rng = rng or random.SystemRandom()
        self._stage = "PREFLIGHT"
        self._identity_barrier: str | None = None

    def _mark_identity_failure(self, reason: str) -> None:
        """Keep the first identity mismatch as a cycle-wide dependency barrier."""

        if self._identity_barrier is None:
            self._identity_barrier = f"identity failure barrier: {reason}"

    async def execute(self, config: RandomCycleConfig) -> RandomCycleResult:
        self._stage = "PREFLIGHT"
        self._identity_barrier = None
        if not config.operator_execution_opt_in or not config.operator_plan_reviewed:
            return RandomCycleResult(
                outcome=Outcome.PREVIEW,
                phase=Phase.PREFLIGHT,
                run_id="",
                reason="explicit cycle launch and plan review are required; no reads or mutations were attempted",
                journal_path=str(config.journal_path),
            )
        journal: DurableJournal | None = None
        try:
            self._prepare_cycle_directory(config.cycle_dir)
            journal = DurableJournal(config.journal_path, clock=self.clock.now)
            journal.acquire_attempt()
            if journal.events:
                return RandomCycleResult(
                    outcome=Outcome.UNKNOWN,
                    phase=Phase.RECONCILIATION,
                    run_id=journal.run_id,
                    reason="cycle directory is already consumed; no mutation was replayed",
                    journal_path=str(config.journal_path),
                )
            journal.append("CYCLE_STARTED", {"binding": config.binding()})
            return await self._execute_locked(config, journal)
        except asyncio.CancelledError:
            if journal is not None:
                try:
                    journal.append("CYCLE_INTERRUPTED", {"reason": "operator/process cancellation; execution state is unknown"})
                except Exception:
                    pass
            raise
        except BaseException as exc:
            safe_reason = _cycle_exception_reason(exc)
            preflight = self._stage == "PREFLIGHT"
            reason = (
                f"cycle preflight blocked: {safe_reason}"
                if preflight
                else f"cycle execution outcome unknown: {safe_reason}"
            )
            if journal is not None:
                try:
                    journal.append("CYCLE_PREFLIGHT_BLOCKED" if preflight else "CYCLE_EXECUTION_UNKNOWN", {"reason": reason})
                except Exception:
                    pass
            return RandomCycleResult(
                outcome=Outcome.FAILED_PREFLIGHT_BLOCKED if preflight else Outcome.UNKNOWN,
                phase=Phase.PREFLIGHT if preflight else Phase.RECONCILIATION,
                run_id="" if journal is None else journal.run_id,
                reason=reason,
                journal_path=str(config.journal_path),
            )
        finally:
            if journal is not None:
                journal.release_attempt()

    @staticmethod
    def _prepare_cycle_directory(path: Path) -> None:
        if path.is_symlink():
            raise PreflightBlocked("cycle directory must not be a symlink")
        if not path.exists():
            path.mkdir(mode=0o700)
        if not path.is_dir():
            raise PreflightBlocked("cycle path exists and is not a directory")
        info = path.stat()
        if info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise PreflightBlocked("cycle directory must be owner-only")
        if any(path.iterdir()):
            raise PreflightBlocked("cycle directory is already consumed")
        os.chmod(path, 0o700)

    @staticmethod
    def validate_cycle_directory(path: Path) -> None:
        """Check a cycle slot without creating or consuming it.

        The launcher uses this read-only check after LAUNCH is requested but
        before it touches a Keychain or constructs an SDK client.  Creation
        and the durable claim still happen inside ``execute`` so cancelling a
        prompt does not leave an empty slot that falsely looks consumed.
        """

        if path.is_symlink():
            raise PreflightBlocked("cycle directory must not be a symlink")
        if not path.exists():
            return
        if not path.is_dir():
            raise PreflightBlocked("cycle path exists and is not a directory")
        info = path.stat()
        if info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise PreflightBlocked("cycle directory must be owner-only")
        if any(path.iterdir()):
            raise PreflightBlocked("cycle directory is already consumed")

    async def _execute_locked(self, config: RandomCycleConfig, journal: DurableJournal) -> RandomCycleResult:
        self._stage = "PREFLIGHT"
        metadata = _as_market(
            await self._bounded(self.client.market_metadata(config.market_id), config, "market metadata read")
        )
        book = _as_book(
            await self._bounded(self._order_book(config.market_id), config, "order book read"), metadata
        )
        now = self.clock.now()
        _validate_market_book(config, metadata, book, now)
        source, receiver = await self._accounts(config, now)
        if source.signed_position != 0 or receiver.signed_position != 0:
            raise PreflightBlocked("random cycle requires both selected-market positions to be exactly flat")
        proposal = select_automatic_prices(
            config.direction,
            metadata,
            book,
            now=now,
            freshness_seconds=config.freshness_seconds,
        )
        bounds = compute_quantity_bounds(metadata, source, receiver, proposal.source_limit_price)
        quantity, quantity_tick, hold_seconds = select_random_quantity(bounds, self.rng)
        selection = RandomCycleSelection(
            quantity=quantity,
            quantity_tick=quantity_tick,
            hold_seconds=hold_seconds,
            opening_source_price=proposal.source_limit_price,
            opening_receiver_bound=proposal.receiver_worst_price,
            bounds=bounds,
            metadata_observed_at=metadata.observed_at,
            book_observed_at=book.observed_at,
        )
        journal.append("SELECTION_PROVED", {"selection": selection.as_dict(), "metadata": _metadata_payload(metadata), "book_observed_at": book.observed_at})

        metadata, book, source, receiver = await self._revalidate_open(config, selection, metadata, book, source, receiver)
        opening_config = self._handoff_config(
            config,
            selection.quantity,
            proposal.source_limit_price,
            proposal.receiver_worst_price,
            config.opening_journal_path,
            OperationMode.PAIRED_OPENING,
            source.signed_position,
            receiver.signed_position,
        )
        journal.append("OPENING_PLAN_READY", {"config": opening_config_binding(opening_config), "selection": selection.as_dict()})
        self._stage = "OPENING"
        opening = await run_handoff(
            opening_config,
            _BoundMarketClient(
                self.client,
                metadata,
                source_identity=source.source_identity,
                receiver_identity=receiver.source_identity,
                identity_failure_callback=self._mark_identity_failure,
            ),
            clock=self.clock,
        )
        self._stage = "OPENING_RECONCILED"
        journal.append("OPENING_COMPLETE", {"result": opening.as_dict()})
        if opening.outcome is not Outcome.SUCCESS:
            fallbacks, remaining_source, remaining_receiver = await self._fallback_residuals(
                config,
                journal,
                selection,
                opening,
                None,
            )
            outcome = (
                Outcome.UNKNOWN
                if (
                    remaining_source is None
                    or remaining_receiver is None
                    or any(item.outcome is Outcome.UNKNOWN for item in fallbacks)
                )
                else opening.outcome
            )
            reason = opening.reason or "paired opening did not prove a complete cycle"
            result = RandomCycleResult(
                outcome=outcome,
                phase=Phase.COMPLETE,
                run_id=journal.run_id,
                selection=selection,
                opening=opening,
                fallbacks=tuple(fallbacks),
                remaining_source_position=remaining_source,
                remaining_receiver_position=remaining_receiver,
                reason=reason,
                journal_path=str(config.journal_path),
            )
            journal.append("CYCLE_COMPLETE", result.as_dict())
            return result

        anchor_wall = self.clock.now()
        anchor_mono = _clock_monotonic(self.clock)
        journal.append("HOLD_ANCHORED", {"hold_seconds": selection.hold_seconds, "anchor_wall": anchor_wall, "anchor_monotonic": anchor_mono})
        try:
            self._stage = "HOLD"
            await self._wait_hold(selection.hold_seconds, anchor_mono)
        except Exception as exc:
            reason = f"hold timer could not reach its persisted deadline: {sanitize_exception(exc)}"
            result = RandomCycleResult(
                outcome=Outcome.UNKNOWN,
                phase=Phase.RECONCILIATION,
                run_id=journal.run_id,
                selection=selection,
                opening=opening,
                reason=reason,
                journal_path=str(config.journal_path),
            )
            journal.append("CYCLE_COMPLETE", result.as_dict())
            return result

        self._stage = "CLOSING"
        closing, fallback_seed = await self._run_paired_close(config, journal, selection, opening)
        self._stage = "CLOSING_RECONCILED"
        self._stage = "FALLBACK"
        fallbacks, remaining_source, remaining_receiver = await self._fallback_residuals(
            config,
            journal,
            selection,
            opening,
            closing,
        )
        all_fallback_reconciled = bool(fallbacks) and all(
            item.outcome in {Outcome.PARTIAL, Outcome.SUCCESS}
            and item.position_after is not None
            for item in fallbacks
        )
        if self._identity_barrier is not None:
            outcome = Outcome.UNKNOWN
        elif (
            closing is not None
            and closing.outcome is Outcome.SUCCESS
            and not fallbacks
            and remaining_source == 0
            and remaining_receiver == 0
        ):
            outcome = Outcome.SUCCESS
        elif (
            closing is not None
            and closing.outcome is Outcome.SUCCESS
            and all_fallback_reconciled
            and remaining_source == 0
            and remaining_receiver == 0
        ):
            outcome = Outcome.SUCCESS
        elif fallbacks and all_fallback_reconciled and remaining_source == 0 and remaining_receiver == 0:
            outcome = Outcome.SUCCESS
        elif remaining_source is None or remaining_receiver is None:
            outcome = Outcome.UNKNOWN
        elif closing is not None and closing.outcome in {
            Outcome.UNKNOWN,
            Outcome.FAILED_PREFLIGHT_BLOCKED,
        }:
            # A known flat read cannot turn an unresolved close execution into
            # a successful cycle; preserve the child outcome's uncertainty.
            outcome = Outcome.UNKNOWN
        elif any(item.outcome is Outcome.UNKNOWN for item in fallbacks) or closing is None:
            outcome = Outcome.UNKNOWN
        else:
            outcome = Outcome.PARTIAL
        fallback_reason = next(
            (item.reason for item in fallbacks if item.reason),
            None,
        )
        reason = None if outcome is Outcome.SUCCESS else (
            self._identity_barrier
            or fallback_reason
            or (closing.reason if closing is not None else fallback_seed)
            or "cycle closure did not prove exact flat positions"
        )
        result = RandomCycleResult(
            outcome=outcome,
            phase=Phase.COMPLETE,
            run_id=journal.run_id,
            selection=selection,
            opening=opening,
            closing=closing,
            fallbacks=tuple(fallbacks),
            remaining_source_position=remaining_source,
            remaining_receiver_position=remaining_receiver,
            reason=reason,
            journal_path=str(config.journal_path),
        )
        journal.append("CYCLE_COMPLETE", result.as_dict())
        return result

    async def _accounts(self, config: RandomCycleConfig, now: float | None = None) -> tuple[AccountSnapshot, AccountSnapshot]:
        source = await self._bounded(self.client.account_snapshot(config.source_account_index, config.market_id), config, "source account read")
        receiver = await self._bounded(self.client.account_snapshot(config.receiver_account_index, config.market_id), config, "receiver account read")
        source = source if isinstance(source, AccountSnapshot) else AccountSnapshot.from_mapping(source)
        receiver = receiver if isinstance(receiver, AccountSnapshot) else AccountSnapshot.from_mapping(receiver)
        # Account observations are stamped by the reads themselves.  Validate
        # against the clock after both have completed so a real transport's
        # small read latency cannot make a fresh response look future-dated.
        del now
        validation_now = self.clock.now()
        _validate_account_fresh(config, source, "source", validation_now)
        _validate_account_fresh(config, receiver, "receiver", validation_now)
        return source, receiver

    async def _revalidate_open(
        self,
        config: RandomCycleConfig,
        selection: RandomCycleSelection,
        initial_metadata: MarketMetadata,
        initial_book: OrderBookSnapshot,
        initial_source: AccountSnapshot,
        initial_receiver: AccountSnapshot,
    ) -> tuple[MarketMetadata, OrderBookSnapshot, AccountSnapshot, AccountSnapshot]:
        metadata = _as_market(await self._bounded(self.client.market_metadata(config.market_id), config, "opening revalidation market read"))
        book = _as_book(await self._bounded(self._order_book(config.market_id), config, "opening revalidation order book read"), metadata)
        now = self.clock.now()
        _validate_market_book(config, metadata, book, now)
        proposal = select_automatic_prices(config.direction, metadata, book, now=now, freshness_seconds=config.freshness_seconds)
        if proposal.source_limit_price != selection.opening_source_price or proposal.receiver_worst_price != selection.opening_receiver_bound:
            raise PreflightBlocked("opening public quote changed before mutation")
        source, receiver = await self._accounts(config, now)
        if source.signed_position != initial_source.signed_position or receiver.signed_position != initial_receiver.signed_position:
            raise PreflightBlocked("account position changed before opening mutation")
        refreshed_bounds = compute_quantity_bounds(metadata, source, receiver, selection.opening_source_price)
        if not refreshed_bounds.lower_tick <= selection.quantity_tick <= refreshed_bounds.upper_tick:
            raise PreflightBlocked("fresh available balance no longer funds the selected quantity")
        if source.source_identity != initial_source.source_identity or receiver.source_identity != initial_receiver.source_identity:
            self._mark_identity_failure("account identity changed before opening mutation")
            raise PreflightBlocked("account identity changed before opening mutation")
        del initial_metadata, initial_book
        return metadata, book, source, receiver

    def _handoff_config(
        self,
        config: RandomCycleConfig,
        quantity: Decimal,
        source_price: Decimal,
        receiver_bound: Decimal,
        journal_path: Path,
        operation_mode: OperationMode,
        source_position: Decimal,
        receiver_position: Decimal,
    ) -> HandoffConfig:
        return HandoffConfig(
            market_id=config.market_id,
            market_symbol=config.market_symbol,
            direction=config.direction if operation_mode is OperationMode.PAIRED_OPENING else _inverse(config.direction),
            quantity=quantity,
            source_limit_price=source_price,
            receiver_worst_price=receiver_bound,
            freshness_seconds=config.freshness_seconds,
            request_timeout_seconds=config.request_timeout_seconds,
            order_timeout_seconds=config.order_timeout_seconds,
            reconcile_timeout_seconds=config.reconcile_timeout_seconds,
            poll_interval_seconds=config.poll_interval_seconds,
            max_poll_count=config.max_poll_count,
            source_order_lifetime_seconds=config.source_order_lifetime_seconds,
            client_order_prefix=config.client_order_prefix,
            journal_path=str(journal_path),
            environment=config.environment,
            operator_execution_opt_in=True,
            operator_plan_reviewed=True,
            api_base_url=config.api_base_url,
            api_key_index=config.api_key_index,
            chain_id=config.chain_id,
            auth_token_lifetime_seconds=config.auth_token_lifetime_seconds,
            operation_mode=operation_mode,
            expected_source_position=source_position,
            expected_receiver_position=receiver_position,
            # A reduce-only close does not add exposure.  The existing config
            # gate therefore remains strict for the opening only; carrying the
            # opening deferral flag into a close would be an invalid policy
            # binding rather than a useful margin proof.
            defer_incremental_margin_calculation=(
                config.defer_incremental_margin_calculation
                if operation_mode is OperationMode.PAIRED_OPENING
                else False
            ),
        )

    async def _run_paired_close(
        self,
        config: RandomCycleConfig,
        journal: DurableJournal,
        selection: RandomCycleSelection,
        opening: HandoffResult,
    ) -> tuple[HandoffResult | None, str | None]:
        def blocked(reason: str) -> tuple[None, str]:
            journal.append("CLOSING_BLOCKED", {"reason": reason})
            return None, reason

        try:
            # Closing is admitted only against the exact, independently
            # reconciled opening.  A fresh read is evidence for freshness and
            # readiness; it is never a new baseline that can adopt a manual
            # or external position change during the hold.
            if (
                opening.outcome is not Outcome.SUCCESS
                or opening.plan is None
                or opening.source is None
                or opening.receiver is None
            ):
                return blocked("opening reconciliation did not prove close lineage")
            opening_plan = opening.plan
            opening_source = opening.source
            opening_receiver = opening.receiver
            if (
                opening_plan.operation_mode is not OperationMode.PAIRED_OPENING
                or opening_source.position_after is None
                or opening_receiver.position_after is None
                or not opening_source.history_complete
                or not opening_receiver.history_complete
                or opening_source.unknown_reasons
                or opening_receiver.unknown_reasons
            ):
                return blocked("opening reconciliation is incomplete for a dependent close")
            metadata = _as_market(await self._bounded(self.client.market_metadata(config.market_id), config, "closing market read"))
            book = _as_book(await self._bounded(self._order_book(config.market_id), config, "closing order book read"), metadata)
            now = self.clock.now()
            _validate_market_book(config, metadata, book, now)
            source, receiver = await self._accounts(config, now)
            if source.source_identity != opening_plan.source_identity:
                self._mark_identity_failure("source account identity changed during the holding period")
                return blocked("source account identity changed during the holding period")
            if receiver.source_identity != opening_plan.receiver_identity:
                self._mark_identity_failure("receiver account identity changed during the holding period")
                return blocked("receiver account identity changed during the holding period")
            if source.signed_position != opening_source.position_after:
                return blocked("source position changed during the holding period")
            if receiver.signed_position != opening_receiver.position_after:
                return blocked("receiver position changed during the holding period")
            source_sign, receiver_sign = _expected_cycle_signs(config.direction)
            source_residual = _position_residual(source.signed_position, source_sign, selection.quantity)
            receiver_residual = _position_residual(receiver.signed_position, receiver_sign, selection.quantity)
            if source_residual is None or receiver_residual is None:
                return blocked("cycle position changed direction or exceeded the selected quantity before paired close")
            paired_quantity = min(source_residual, receiver_residual)
            if paired_quantity <= 0:
                return blocked("paired close has no two-account confirmed residual")
            close_direction = _inverse(config.direction)
            proposal = select_automatic_prices(close_direction, metadata, book, quantity=paired_quantity, now=now, freshness_seconds=config.freshness_seconds)
            close_config = self._handoff_config(
                config,
                paired_quantity,
                proposal.source_limit_price,
                proposal.receiver_worst_price,
                config.closing_journal_path,
                OperationMode.PAIRED_CLOSING,
                opening_source.position_after,
                opening_receiver.position_after,
            )
            journal.append("CLOSING_PLAN_READY", {"config": opening_config_binding(close_config), "paired_quantity": format(paired_quantity, "f")})
            closing = await run_handoff(
                close_config,
                _BoundMarketClient(
                    self.client,
                    metadata,
                    source_identity=opening_plan.source_identity,
                    receiver_identity=opening_plan.receiver_identity,
                    identity_failure_callback=self._mark_identity_failure,
                ),
                clock=self.clock,
            )
            journal.append("CLOSING_COMPLETE", {"result": closing.as_dict()})
            return closing, None
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            reason = _cycle_exception_reason(exc)
            journal.append("CLOSING_BLOCKED", {"reason": reason})
            return None, reason

    async def _fallback_residuals(
        self,
        config: RandomCycleConfig,
        journal: DurableJournal,
        selection: RandomCycleSelection,
        opening: HandoffResult,
        closing: HandoffResult | None,
    ) -> tuple[list[FallbackResult], Decimal | None, Decimal | None]:
        now = self.clock.now()
        try:
            source, receiver = await self._accounts(config, now)
        except Exception as exc:
            journal.append(
                "FALLBACK_RECONCILIATION_UNKNOWN",
                {"reason": f"fallback starting account state is unknown: {sanitize_exception(exc)}"},
            )
            return [], None, None
        if self._identity_barrier is not None:
            journal.append(
                "FALLBACK_BLOCKED_IDENTITY_BARRIER",
                {"reason": self._identity_barrier},
            )
            return [], source.signed_position, receiver.signed_position
        if opening.outcome not in {Outcome.PARTIAL, Outcome.SUCCESS}:
            return [], source.signed_position, receiver.signed_position

        # A fallback may act only on positions causally bound to a fully
        # reconciled leg.  This prevents a fresh account read from turning an
        # unrelated/external position into cycle inventory after an ambiguous
        # write or a changed close state.
        bound_result = closing
        if bound_result is not None and bound_result.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED:
            bound_result = None
        if bound_result is not None and bound_result.outcome is Outcome.UNKNOWN:
            return [], source.signed_position, receiver.signed_position
        bound_source = opening.source if bound_result is None else bound_result.source
        bound_receiver = opening.receiver if bound_result is None else bound_result.receiver
        bound_plan = opening.plan if bound_result is None else bound_result.plan
        if bound_plan is not None:
            if source.source_identity != bound_plan.source_identity:
                self._mark_identity_failure("source account identity changed during fallback admission")
            if receiver.source_identity != bound_plan.receiver_identity:
                self._mark_identity_failure("receiver account identity changed during fallback admission")
        if self._identity_barrier is not None:
            journal.append(
                "FALLBACK_BLOCKED_IDENTITY_BARRIER",
                {"reason": self._identity_barrier},
            )
            return [], source.signed_position, receiver.signed_position
        if (
            bound_source is None
            or bound_receiver is None
            or bound_plan is None
            or bound_source.position_after is None
            or bound_receiver.position_after is None
            or not bound_source.history_complete
            or not bound_receiver.history_complete
            or bound_source.unknown_reasons
            or bound_receiver.unknown_reasons
            or source.source_identity != bound_plan.source_identity
            or receiver.source_identity != bound_plan.receiver_identity
            or source.signed_position != bound_source.position_after
            or receiver.signed_position != bound_receiver.position_after
        ):
            journal.append(
                "FALLBACK_STOPPED_STATE_CHANGED",
                {"reason": "fresh account state is not causally bound to the reconciled cycle position"},
            )
            return [], source.signed_position, receiver.signed_position
        source_sign, receiver_sign = _expected_cycle_signs(config.direction)
        source_residual = _position_residual(source.signed_position, source_sign, selection.quantity)
        receiver_residual = _position_residual(receiver.signed_position, receiver_sign, selection.quantity)
        if closing is None and opening.outcome is Outcome.SUCCESS:
            # A missing paired-close phase is an external/state failure; never
            # infer a flat cycle from a read that was not causally reconciled.
            if source_residual == 0 and receiver_residual == 0:
                return [], Decimal(0), Decimal(0)
        if source_residual is None or receiver_residual is None:
            journal.append(
                "FALLBACK_STOPPED_STATE_CHANGED",
                {"reason": "cycle residual changed direction or exceeded the selected quantity"},
            )
            return [], source.signed_position, receiver.signed_position
        results: list[FallbackResult] = []
        current: dict[int, AccountSnapshot] = {
            config.source_account_index: source,
            config.receiver_account_index: receiver,
        }
        signs = {
            config.source_account_index: _expected_cycle_signs(config.direction)[0],
            config.receiver_account_index: _expected_cycle_signs(config.direction)[1],
        }
        pending = [
            account_index
            for account_index, residual in (
                (config.source_account_index, source_residual),
                (config.receiver_account_index, receiver_residual),
            )
            if residual > 0
        ]
        blocked: set[int] = set()
        ready_at = {account_index: _clock_monotonic(self.clock) for account_index in pending}
        attempt_ordinal = 0

        async def fresh_accounts() -> tuple[AccountSnapshot, AccountSnapshot] | None:
            """Read both accounts after an attempt before any next mutation."""

            try:
                return await self._accounts(config, self.clock.now())
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                journal.append(
                    "FALLBACK_RECONCILIATION_UNKNOWN",
                    {"reason": f"fallback account state is unknown: {sanitize_exception(exc)}"},
                )
                return None

        while pending:
            account_index = pending.pop(0)
            if account_index in blocked:
                continue
            before = current[account_index]
            residual = _position_residual(before.signed_position, signs[account_index], selection.quantity)
            if residual is None:
                journal.append(
                    "FALLBACK_STOPPED_STATE_CHANGED",
                    {"account_index": account_index, "reason": "cycle residual changed direction or exceeded the selected quantity"},
                )
                return results, current[config.source_account_index].signed_position, current[config.receiver_account_index].signed_position
            if residual <= 0:
                continue

            # A terminal zero-fill IOC is retried only after the declared
            # polling interval.  A delayed account is rotated behind any
            # other ready account so illiquidity on one side cannot starve it.
            while True:
                now_monotonic = _clock_monotonic(self.clock)
                wait_until = ready_at.get(account_index, now_monotonic)
                if now_monotonic >= wait_until:
                    break
                pending.append(account_index)
                if pending:
                    next_ready = min(
                        ready_at.get(candidate, now_monotonic)
                        for candidate in pending
                    )
                    if next_ready > now_monotonic and all(
                        _clock_monotonic(self.clock) < ready_at.get(candidate, now_monotonic)
                        for candidate in pending
                    ):
                        await self.clock.sleep(next_ready - now_monotonic)
                break
            if account_index in pending:
                continue

            attempt_ordinal += 1
            result = await self._fallback_one(
                config,
                journal,
                before,
                residual,
                attempt_ordinal=attempt_ordinal,
            )
            results.append(result)
            if self._identity_barrier is not None and result.outcome is not Outcome.UNKNOWN:
                observed = await fresh_accounts()
                if observed is None:
                    return results, None, None
                final_source, final_receiver = observed
                journal.append(
                    "FALLBACK_BLOCKED_IDENTITY_BARRIER",
                    {"reason": self._identity_barrier},
                )
                return results, final_source.signed_position, final_receiver.signed_position
            if result.outcome is Outcome.UNKNOWN:
                # An ambiguous mutation or reconciliation cannot be followed
                # by another write on either account.  Preserve any fresh
                # account positions that remain independently readable.
                observed = await fresh_accounts()
                if observed is None:
                    return results, None, None
                final_source, final_receiver = observed
                return results, final_source.signed_position, final_receiver.signed_position

            observed = await fresh_accounts()
            if observed is None:
                return results, None, None
            fresh_source, fresh_receiver = observed
            fresh = {
                config.source_account_index: fresh_source,
                config.receiver_account_index: fresh_receiver,
            }
            if any(
                fresh[candidate].source_identity != current[candidate].source_identity
                for candidate in (config.source_account_index, config.receiver_account_index)
            ):
                self._mark_identity_failure("fresh account identity changed after the fallback reconciliation")
                journal.append(
                    "FALLBACK_STOPPED_STATE_CHANGED",
                    {
                        "attempt": attempt_ordinal,
                        "account_index": account_index,
                        "reason": "fresh account identity changed after the fallback reconciliation",
                    },
                )
                return results, None, None
            expected_positions = {
                candidate: snapshot.signed_position for candidate, snapshot in current.items()
            }
            if result.position_after is not None:
                expected_positions[account_index] = result.position_after
            if any(
                fresh[candidate].signed_position != expected_positions[candidate]
                for candidate in (config.source_account_index, config.receiver_account_index)
            ):
                journal.append(
                    "FALLBACK_STOPPED_STATE_CHANGED",
                    {
                        "attempt": attempt_ordinal,
                        "account_index": account_index,
                        "reason": "fresh account positions disagree with the reconciled fallback result",
                    },
                )
                return results, None, None
            current = fresh
            fresh_residuals = {
                candidate: _position_residual(snapshot.signed_position, signs[candidate], selection.quantity)
                for candidate, snapshot in current.items()
            }
            if any(value is None for value in fresh_residuals.values()):
                journal.append(
                    "FALLBACK_STOPPED_STATE_CHANGED",
                    {
                        "attempt": attempt_ordinal,
                        "account_index": account_index,
                        "reason": "fresh account position is outside the cycle direction or quantity cap",
                    },
                )
                return results, None, None

            next_residual = fresh_residuals[account_index]
            if next_residual is None or next_residual <= 0:
                continue
            # A rejection, below-minimum residual, or another known refusal is
            # terminal for this account.  A fully reconciled known partial is
            # safe to requeue; no fixed attempt count is imposed.
            if result.position_after is None:
                blocked.add(account_index)
                continue
            if result.filled_quantity == 0:
                ready_at[account_index] = _clock_monotonic(self.clock) + config.poll_interval_seconds
            else:
                ready_at[account_index] = _clock_monotonic(self.clock)
            if account_index not in pending:
                pending.append(account_index)

        try:
            final_source, final_receiver = await self._accounts(config, self.clock.now())
            final = {
                config.source_account_index: final_source,
                config.receiver_account_index: final_receiver,
            }
            if any(
                final[candidate].source_identity != current[candidate].source_identity
                for candidate in (config.source_account_index, config.receiver_account_index)
            ):
                self._mark_identity_failure("final fallback account identity changed after reconciliation")
                journal.append(
                    "FALLBACK_STOPPED_STATE_CHANGED",
                    {"reason": "final fallback account identity changed after reconciliation"},
                )
                return results, None, None
            if any(
                final[candidate].signed_position != current[candidate].signed_position
                for candidate in (config.source_account_index, config.receiver_account_index)
            ):
                journal.append(
                    "FALLBACK_STOPPED_STATE_CHANGED",
                    {"reason": "final fallback account position changed after reconciliation"},
                )
                return results, None, None
            return results, final_source.signed_position, final_receiver.signed_position
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            journal.append(
                "FALLBACK_RECONCILIATION_UNKNOWN",
                {"reason": f"fallback final account state is unknown: {sanitize_exception(exc)}"},
            )
            return results, None, None

    async def _poll_fallback_order(
        self,
        config: RandomCycleConfig,
        journal: DurableJournal,
        plan: OrderPlan,
        receipt: MutationReceipt,
        *,
        attempt_ordinal: int,
    ) -> tuple[OrderSnapshot | None, str | None]:
        """Wait for one fresh terminal observation without resubmitting it."""

        deadline = self.clock.now() + config.order_timeout_seconds
        last_reason = "fallback order was not visible"
        for poll in range(1, config.max_poll_count + 1):
            if self.clock.now() > deadline:
                break
            try:
                raw = await self._bounded(
                    self.client.lookup_order(
                        plan.account_index,
                        plan.market_id,
                        order_id=receipt.order_id,
                        client_order_index=plan.client_order_index,
                    ),
                    config,
                    "fallback order read",
                )
                order = _as_order(raw)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                last_reason = f"fallback order read failed: {sanitize_exception(exc)}"
                journal.append(
                    "FALLBACK_ORDER_OBSERVATION",
                    {
                        "account_index": plan.account_index,
                        "attempt": attempt_ordinal,
                        "poll": poll,
                        "order": None,
                        "reason": last_reason,
                    },
                )
                order = None
            if order is not None:
                now = self.clock.now()
                if receipt.order_id is not None and order.order_id != str(receipt.order_id):
                    return None, "fallback order identifier conflicts with the dispatched receipt"
                if not self._fallback_order_identity_matches(order, plan):
                    return None, "fallback order identity or parameters conflict with the plan"
                if order.observed_at > now:
                    last_reason = "fallback order observation is from the future"
                elif now - order.observed_at > config.freshness_seconds:
                    last_reason = "fallback order observation is stale"
                elif not order.terminal:
                    last_reason = "fallback order is not terminal"
                elif not self._fallback_order_matches(order, plan):
                    return None, "fallback terminal order fields conflict with the plan"
                else:
                    journal.append(
                        "FALLBACK_ORDER_OBSERVATION",
                        {
                            "account_index": plan.account_index,
                            "attempt": attempt_ordinal,
                            "poll": poll,
                            "order": _order_payload(order),
                        },
                    )
                    return order, None
                journal.append(
                    "FALLBACK_ORDER_OBSERVATION",
                    {
                        "account_index": plan.account_index,
                        "attempt": attempt_ordinal,
                        "poll": poll,
                        "order": _order_payload(order),
                        "reason": last_reason,
                    },
                )
            if poll < config.max_poll_count:
                remaining = deadline - self.clock.now()
                if remaining <= 0:
                    break
                await self.clock.sleep(min(config.poll_interval_seconds, remaining))
        return None, f"fallback order did not become a fresh terminal observation: {last_reason}"

    async def _read_fallback_history(
        self,
        config: RandomCycleConfig,
        plan: OrderPlan,
        order: OrderSnapshot,
        *,
        attempt_ordinal: int,
    ) -> tuple[list[TradeReceipt], list[Any], str | None]:
        """Read all bounded history pages for one fallback order."""

        trades: list[TradeReceipt] = []
        pages: list[Any] = []
        seen_trade_ids: set[str] = set()
        cursor: str | None = None
        deadline = self.clock.now() + config.reconcile_timeout_seconds
        for page_number in range(1, config.max_poll_count + 1):
            if self.clock.now() > deadline:
                return trades, pages, "fallback trade history reconciliation deadline exceeded"
            try:
                page = _as_page(
                    await self._bounded(
                        self.client.list_trades(
                            plan.account_index,
                            plan.market_id,
                            order_id=order.order_id,
                            cursor=cursor,
                            limit=100,
                        ),
                        config,
                        "fallback trade history read",
                    )
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                return trades, pages, f"fallback trade history read failed: {sanitize_exception(exc)}"
            pages.append(page)
            now = self.clock.now()
            try:
                for trade in page.trades:
                    if (
                        trade.account_index != plan.account_index
                        or trade.market_id != plan.market_id
                        or trade.order_id != order.order_id
                    ):
                        raise ContractError("fallback trade history contains a foreign or conflicting receipt")
                    if trade.trade_id in seen_trade_ids:
                        raise ContractError("fallback trade history contains a duplicate receipt")
                    seen_trade_ids.add(trade.trade_id)
                    if trade.observed_at > now:
                        raise ContractError("fallback trade receipt is from the future")
                    if trade.side.upper() != plan.side:
                        raise ContractError("fallback trade side conflicts with the plan")
                    if not (
                        trade.price <= plan.price
                        if plan.side == "BUY"
                        else trade.price >= plan.price
                    ):
                        raise ContractError("fallback trade violates the executable price bound")
                    trades.append(trade)
            except Exception as exc:
                return trades, pages, f"fallback trade reconciliation is unknown: {_cycle_exception_reason(exc)}"
            if not page.next_cursor:
                if not page.complete:
                    return trades, pages, "fallback trade history is incomplete"
                return trades, pages, None
            if page.next_cursor == cursor:
                return trades, pages, "fallback trade history cursor repeated"
            cursor = page.next_cursor
        return trades, pages, "fallback trade history pagination exceeded configured bound"

    async def _fallback_one(
        self,
        config: RandomCycleConfig,
        journal: DurableJournal,
        before: AccountSnapshot,
        residual: Decimal,
        *,
        attempt_ordinal: int = 1,
    ) -> FallbackResult:
        initial_before = before
        side = "SELL" if before.signed_position > 0 else "BUY"
        metadata: MarketMetadata | None = None
        book: OrderBookSnapshot | None = None
        plan: OrderPlan | None = None
        receipt: MutationReceipt | None = None
        order: OrderSnapshot | None = None
        trades: list[TradeReceipt] = []
        history_pages: list[Any] = []
        history_complete: bool | None = None
        after: AccountSnapshot | None = None

        def finish(result: FallbackResult, *, reconciliation_event: str | None = None) -> FallbackResult:
            evidence = {
                "account_index": initial_before.account_index,
                "attempt": attempt_ordinal,
                "requested_quantity": format(residual, "f"),
                "outcome": result.outcome.value,
                "reason": result.reason,
                "plan": None if plan is None else plan.as_dict(),
                "receipt": _receipt_payload(receipt),
                "before": _account_payload(initial_before),
                "admission_before": _account_payload(before),
                "after": _account_payload(after),
                "order": _order_payload(order),
                "trades": [_trade_payload(trade) for trade in trades],
                "trade_ids": [trade.trade_id for trade in trades],
                "history_pages": [_history_page_payload(page) for page in history_pages],
                "history_complete": history_complete,
                "market_metadata": None if metadata is None else _metadata_payload(metadata),
                "order_book_observed_at": None if book is None else book.observed_at,
            }
            journal.append("FALLBACK_ATTEMPT_EVIDENCE", evidence)
            if reconciliation_event is not None:
                journal.append(reconciliation_event, evidence)
            return result

        if residual <= 0:
            return finish(
                FallbackResult(
                    before.account_index,
                    side,
                    residual,
                    False,
                    Outcome.PARTIAL,
                    reason="no confirmed residual position",
                    attempt=attempt_ordinal,
                )
            )
        try:
            metadata = _as_market(
                await self._bounded(
                    self.client.market_metadata(config.market_id),
                    config,
                    "fallback market read",
                )
            )
            book = _as_book(
                await self._bounded(self._order_book(config.market_id), config, "fallback order book read"),
                metadata,
            )
            _validate_market_book(config, metadata, book, self.clock.now())
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            reason = f"fallback market observation is unknown: {_cycle_exception_reason(exc)}"
            return finish(
                FallbackResult(
                    before.account_index,
                    side,
                    residual,
                    False,
                    Outcome.UNKNOWN,
                    reason=reason,
                    attempt=attempt_ordinal,
                ),
                reconciliation_event="FALLBACK_RECONCILIATION_UNKNOWN",
            )

        # Rebind the residual to a fresh account observation after the book
        # read.  A position or identity change between the reconciliation
        # barrier and this mutation must stop this account's fallback rather
        # than turn a new/external position into cycle inventory.
        try:
            current_raw = await self._bounded(
                self.client.account_snapshot(before.account_index, config.market_id),
                config,
                "fallback account recheck",
            )
            current = (
                current_raw
                if isinstance(current_raw, AccountSnapshot)
                else AccountSnapshot.from_mapping(current_raw)
            )
            label = "source" if before.account_index == config.source_account_index else "receiver"
            _validate_account_fresh(
                config,
                current,
                label,
                self.clock.now(),
                expected_position=before.signed_position,
            )
            if current.source_identity != before.source_identity:
                self._mark_identity_failure("fallback account identity changed before mutation")
                raise PreflightBlocked("fallback account identity changed before mutation")
            before = current
            side = "SELL" if before.signed_position > 0 else "BUY"
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            reason = f"fallback account state changed before mutation: {_cycle_exception_reason(exc)}"
            return finish(
                FallbackResult(
                    initial_before.account_index,
                    side,
                    residual,
                    False,
                    Outcome.PARTIAL,
                    reason=reason,
                    attempt=attempt_ordinal,
                )
            )
        try:
            quantity_int = decimal_to_integer(residual, metadata.size_decimals, "fallback residual quantity")
        except ContractError as exc:
            return finish(
                FallbackResult(
                    before.account_index,
                    side,
                    residual,
                    False,
                    Outcome.PARTIAL,
                    reason=str(exc),
                    attempt=attempt_ordinal,
                )
            )
        bound = book.asks[0].price if side == "BUY" else book.bids[0].price
        if (
            residual < metadata.minimum_base_amount
            or residual * bound < metadata.minimum_quote_amount
        ):
            return finish(
                FallbackResult(
                    before.account_index,
                    side,
                    residual,
                    False,
                    Outcome.PARTIAL,
                    reason="confirmed residual is below a documented venue minimum",
                    attempt=attempt_ordinal,
                )
            )
        price_int = decimal_to_integer(bound, metadata.price_decimals, "fallback executable price")
        client_order_index = _client_order_index(
            journal.run_id,
            f"fallback-{attempt_ordinal}-{before.account_index}",
        )
        plan = OrderPlan(
            account_index=before.account_index,
            market_id=config.market_id,
            side=side,
            quantity=residual,
            quantity_int=quantity_int,
            price=bound,
            price_int=price_int,
            order_type="MARKET",
            time_in_force="IOC",
            reduce_only=True,
            order_expiry_ms=0,
            client_order_index=client_order_index,
        )
        journal.append(
            "FALLBACK_DISPATCH_INTENT",
            {
                "plan": plan.as_dict(),
                "account_index": before.account_index,
                "attempt": attempt_ordinal,
            },
        )
        try:
            receipt = _as_receipt(
                await self._bounded(self.client.submit_order(plan), config, "fallback mutation")
            )
        except asyncio.CancelledError:
            reason = "cancellation after fallback intent"
            journal.append(
                "FALLBACK_DISPATCH_UNKNOWN",
                {"account_index": before.account_index, "attempt": attempt_ordinal, "reason": reason},
            )
            finish(
                FallbackResult(
                    before.account_index,
                    side,
                    residual,
                    True,
                    Outcome.UNKNOWN,
                    reason=f"fallback dispatch outcome unknown: {reason}",
                    attempt=attempt_ordinal,
                )
            )
            raise
        except BaseException as exc:
            reason = f"fallback dispatch outcome unknown: {sanitize_exception(exc)}"
            journal.append(
                "FALLBACK_DISPATCH_UNKNOWN",
                {"account_index": before.account_index, "attempt": attempt_ordinal, "reason": reason},
            )
            return finish(
                FallbackResult(
                    before.account_index,
                    side,
                    residual,
                    True,
                    Outcome.UNKNOWN,
                    reason=reason,
                    attempt=attempt_ordinal,
                )
            )
        journal.append(
            "FALLBACK_DISPATCH_RESULT",
            {
                "account_index": before.account_index,
                "attempt": attempt_ordinal,
                "accepted": receipt.accepted,
                "order_id": receipt.order_id,
                "receipt": _receipt_payload(receipt),
            },
        )
        if not receipt.accepted:
            return finish(
                FallbackResult(
                    before.account_index,
                    side,
                    residual,
                    True,
                    Outcome.PARTIAL,
                    order_id=receipt.order_id,
                    reason="fallback reduce-only market was rejected",
                    attempt=attempt_ordinal,
                )
            )

        try:
            order, order_reason = await self._poll_fallback_order(
                config,
                journal,
                plan,
                receipt,
                attempt_ordinal=attempt_ordinal,
            )
        except asyncio.CancelledError:
            finish(
                FallbackResult(
                    before.account_index,
                    side,
                    residual,
                    True,
                    Outcome.UNKNOWN,
                    order_id=receipt.order_id,
                    reason="fallback order reconciliation was interrupted",
                    attempt=attempt_ordinal,
                ),
                reconciliation_event="FALLBACK_RECONCILIATION_UNKNOWN",
            )
            raise
        except BaseException as exc:
            reason = f"fallback order reconciliation was interrupted: {sanitize_exception(exc)}"
            finish(
                FallbackResult(
                    before.account_index,
                    side,
                    residual,
                    True,
                    Outcome.UNKNOWN,
                    order_id=receipt.order_id,
                    reason=reason,
                    attempt=attempt_ordinal,
                ),
                reconciliation_event="FALLBACK_RECONCILIATION_UNKNOWN",
            )
            raise
        if order is None:
            reason = order_reason or "fallback order state is unknown"
            return finish(
                FallbackResult(
                    before.account_index,
                    side,
                    residual,
                    True,
                    Outcome.UNKNOWN,
                    order_id=receipt.order_id,
                    reason=reason,
                    attempt=attempt_ordinal,
                ),
                reconciliation_event="FALLBACK_RECONCILIATION_UNKNOWN",
            )

        try:
            history_complete = False
            trades, history_pages, history_reason = await self._read_fallback_history(
                config,
                plan,
                order,
                attempt_ordinal=attempt_ordinal,
            )
            history_complete = history_reason is None
        except asyncio.CancelledError:
            finish(
                FallbackResult(
                    before.account_index,
                    side,
                    residual,
                    True,
                    Outcome.UNKNOWN,
                    order_id=order.order_id,
                    filled_quantity=sum((item.quantity for item in trades), Decimal(0)),
                    reason="fallback trade reconciliation was interrupted",
                    attempt=attempt_ordinal,
                ),
                reconciliation_event="FALLBACK_RECONCILIATION_UNKNOWN",
            )
            raise
        except BaseException as exc:
            reason = f"fallback trade reconciliation was interrupted: {sanitize_exception(exc)}"
            finish(
                FallbackResult(
                    before.account_index,
                    side,
                    residual,
                    True,
                    Outcome.UNKNOWN,
                    order_id=order.order_id,
                    filled_quantity=sum((item.quantity for item in trades), Decimal(0)),
                    reason=reason,
                    attempt=attempt_ordinal,
                ),
                reconciliation_event="FALLBACK_RECONCILIATION_UNKNOWN",
            )
            raise
        if history_reason is not None:
            return finish(
                FallbackResult(
                    before.account_index,
                    side,
                    residual,
                    True,
                    Outcome.UNKNOWN,
                    order_id=order.order_id,
                    filled_quantity=sum((item.quantity for item in trades), Decimal(0)),
                    reason=history_reason,
                    attempt=attempt_ordinal,
                ),
                reconciliation_event="FALLBACK_RECONCILIATION_UNKNOWN",
            )

        try:
            after_raw = await self._bounded(
                self.client.account_snapshot(before.account_index, config.market_id),
                config,
                "fallback final account read",
            )
            after = (
                after_raw
                if isinstance(after_raw, AccountSnapshot)
                else AccountSnapshot.from_mapping(after_raw)
            )
            _validate_account_fresh(
                config,
                after,
                "source" if before.account_index == config.source_account_index else "receiver",
                self.clock.now(),
            )
            if after.source_identity != before.source_identity:
                self._mark_identity_failure("fallback final account identity changed after mutation")
                return finish(
                    FallbackResult(
                        before.account_index,
                        side,
                        residual,
                        True,
                        Outcome.UNKNOWN,
                        order_id=order.order_id,
                        filled_quantity=sum((item.quantity for item in trades), Decimal(0)),
                        position_after=after.signed_position,
                        reason="fallback final account identity changed after mutation",
                        attempt=attempt_ordinal,
                    ),
                    reconciliation_event="FALLBACK_RECONCILIATION_UNKNOWN",
                )
        except asyncio.CancelledError:
            finish(
                FallbackResult(
                    before.account_index,
                    side,
                    residual,
                    True,
                    Outcome.UNKNOWN,
                    order_id=order.order_id,
                    filled_quantity=sum((item.quantity for item in trades), Decimal(0)),
                    reason="fallback final account reconciliation was interrupted",
                    attempt=attempt_ordinal,
                ),
                reconciliation_event="FALLBACK_RECONCILIATION_UNKNOWN",
            )
            raise
        except Exception as exc:
            reason = f"fallback final position is unknown: {sanitize_exception(exc)}"
            return finish(
                FallbackResult(
                    before.account_index,
                    side,
                    residual,
                    True,
                    Outcome.UNKNOWN,
                    order_id=order.order_id,
                    filled_quantity=sum((item.quantity for item in trades), Decimal(0)),
                    reason=reason,
                    attempt=attempt_ordinal,
                ),
                reconciliation_event="FALLBACK_RECONCILIATION_UNKNOWN",
            )
        except BaseException as exc:
            reason = f"fallback final account reconciliation was interrupted: {sanitize_exception(exc)}"
            finish(
                FallbackResult(
                    before.account_index,
                    side,
                    residual,
                    True,
                    Outcome.UNKNOWN,
                    order_id=order.order_id,
                    filled_quantity=sum((item.quantity for item in trades), Decimal(0)),
                    reason=reason,
                    attempt=attempt_ordinal,
                ),
                reconciliation_event="FALLBACK_RECONCILIATION_UNKNOWN",
            )
            raise
        filled = sum((trade.quantity for trade in trades), Decimal(0))
        expected_after = before.signed_position + (-filled if side == "SELL" else filled)
        if order.filled_quantity != filled or after.signed_position != expected_after:
            reason = "fallback order, receipts and position disagree"
            return finish(
                FallbackResult(
                    before.account_index,
                    side,
                    residual,
                    True,
                    Outcome.UNKNOWN,
                    order_id=order.order_id,
                    filled_quantity=filled,
                    position_after=after.signed_position,
                    reason=reason,
                    attempt=attempt_ordinal,
                ),
                reconciliation_event="FALLBACK_RECONCILIATION_UNKNOWN",
            )
        outcome = (
            Outcome.SUCCESS
            if filled == residual and after.signed_position == 0 and order.terminal
            else Outcome.PARTIAL
        )
        reason = None if outcome is Outcome.SUCCESS else "fallback left a confirmed residual position"
        return finish(
            FallbackResult(
                before.account_index,
                side,
                residual,
                True,
                outcome,
                order_id=order.order_id,
                filled_quantity=filled,
                position_after=after.signed_position,
                reason=reason,
                attempt=attempt_ordinal,
            ),
            reconciliation_event="FALLBACK_RECONCILED",
        )

    @staticmethod
    def _fallback_order_identity_matches(order: OrderSnapshot, plan: OrderPlan) -> bool:
        """Match immutable order fields while a terminal state is pending."""

        return (
            order.account_index == plan.account_index
            and order.market_id == plan.market_id
            and order.order_id != ""
            and order.client_order_index is not None
            and str(order.client_order_index) == str(plan.client_order_index)
            and order.side == plan.side
            and order.order_type == "MARKET"
            and order.time_in_force == "IOC"
            and order.reduce_only is True
            and order.initial_quantity == plan.quantity
            and order.remaining_quantity + order.filled_quantity == order.initial_quantity
            and order.price is not None
            and (
                order.price <= plan.price
                if plan.side == "BUY"
                else order.price >= plan.price
            )
        )

    @staticmethod
    def _fallback_order_matches(order: OrderSnapshot, plan: OrderPlan) -> bool:
        return RandomCycleEngine._fallback_order_identity_matches(order, plan) and order.terminal

    async def _order_book(self, market_id: int) -> OrderBookSnapshot | Mapping[str, Any]:
        method = getattr(self.client, "order_book", None)
        if not callable(method):
            method = getattr(self.client, "order_book_snapshot", None)
        if not callable(method):
            method = getattr(self.client, "public_order_book", None)
        if not callable(method):
            raise ContractError("cycle client does not expose an order-book reader")
        return await method(market_id)

    async def _bounded(self, awaitable: Any, config: RandomCycleConfig, label: str) -> Any:
        try:
            return await asyncio.wait_for(awaitable, timeout=config.request_timeout_seconds)
        except asyncio.TimeoutError as exc:
            raise TimeoutError(f"{label} exceeded configured request timeout") from exc

    async def _wait_hold(self, hold_seconds: int, anchor_monotonic: float) -> None:
        deadline = anchor_monotonic + hold_seconds
        previous = anchor_monotonic
        stalled = 0
        while True:
            current = _clock_monotonic(self.clock)
            remaining = deadline - current
            if remaining <= 0:
                return
            await self.clock.sleep(min(remaining, 60.0))
            after_sleep = _clock_monotonic(self.clock)
            if after_sleep <= previous:
                stalled += 1
                if stalled >= 2:
                    raise TimeoutError("injected monotonic clock did not advance during hold")
            else:
                stalled = 0
            previous = after_sleep


def opening_config_binding(config: HandoffConfig) -> dict[str, Any]:
    return {
        "market_id": config.market_id,
        "market_symbol": config.market_symbol,
        "direction": config.direction.value,
        "quantity": format(config.quantity, "f"),
        "source_limit_price": format(config.source_limit_price, "f"),
        "receiver_worst_price": format(config.receiver_worst_price, "f"),
        "operation_mode": config.operation_mode.value,
        "expected_source_position": None if config.expected_source_position is None else format(config.expected_source_position, "f"),
        "expected_receiver_position": None if config.expected_receiver_position is None else format(config.expected_receiver_position, "f"),
        "journal_path": config.journal_path,
    }


async def run_random_cycle(
    config: RandomCycleConfig,
    client: RandomCycleClient,
    *,
    clock: Clock | None = None,
    rng: Any | None = None,
) -> RandomCycleResult:
    return await RandomCycleEngine(client, clock=clock, rng=rng).execute(config)


# Readable aliases for callers that use "cycle" rather than the HCR name.
run_cycle = run_random_cycle
calculate_quantity_bounds = compute_quantity_bounds
size_random_quantity = select_random_quantity


__all__ = [
    "CLOSING_JOURNAL_NAME",
    "CYCLE_JOURNAL_NAME",
    "FallbackResult",
    "MAX_HOLD_SECONDS",
    "MIN_HOLD_SECONDS",
    "OPENING_JOURNAL_NAME",
    "RandomCycleClient",
    "RandomCycleConfig",
    "RandomCycleEngine",
    "RandomCycleResult",
    "RandomCycleSelection",
    "RandomQuantityBounds",
    "calculate_quantity_bounds",
    "compute_quantity_bounds",
    "run_cycle",
    "run_random_cycle",
    "select_random_quantity",
    "size_random_quantity",
]
