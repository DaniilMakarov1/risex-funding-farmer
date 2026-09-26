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
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
import hashlib
import json
import math
import os
from pathlib import Path
import random
import time
from typing import Any, Callable, Mapping, Protocol, Sequence, runtime_checkable
from urllib.parse import urlsplit
import uuid

from .contracts import (
    AccountSnapshot,
    ContractError,
    Direction,
    HandoffConfig,
    HandoffResult,
    LeverageNotSent,
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
    TERMINAL_ORDER_STATUSES,
    _nonnegative,
    decimal_to_integer,
)
from .engine import (
    Clock,
    HandoffClient,
    HandoffPreflightContext,
    SystemClock,
    _as_order,
    _as_page,
    _as_receipt,
    run_handoff,
)
from .read_errors import read_rate_limit_delay
from .journal import DurableJournal, sanitize_exception
from .provenance import capture_provenance
from .local_attempt import select_automatic_prices
from .series import OrderBookSnapshot


MIN_HOLD_SECONDS = 20
MAX_HOLD_SECONDS = 180
# Owner /stop results and the hold poll used only while a stop can be observed.
OWNER_STOP_REASON = "owner /stop before the first order; no order was sent"
OWNER_STOP_RETRY_REASON = "owner /stop before another opening attempt"
OWNER_STOP_HOLD_POLL_SECONDS = 1.0
MAX_OPENING_ATTEMPTS = 6
MAX_CLOSING_ATTEMPTS = 15
CYCLE_JOURNAL_NAME = "cycle.jsonl"
OPENING_JOURNAL_NAME = "opening.jsonl"
CLOSING_JOURNAL_NAME = "closing.jsonl"
LAUNCH_METADATA_NAME = "launch.json"
ADMISSION_METADATA_NAME = "admission.json"
# Highest published Robinhood tier fees as of the pinned 2026-09-14 schedule.
# These bound an unknown account tier; live market fee evidence may raise them.
ROBINHOOD_MAKER_FEE_CAP = Decimal("0.00012")
ROBINHOOD_TAKER_FEE_CAP = Decimal("0.00035")


def _require_owner_only_directory(path: Path, *, label: str) -> None:
    if path.is_symlink() or not path.exists() or not path.is_dir():
        raise PreflightBlocked(f"{label} must be an existing non-symlink directory")
    try:
        info = path.stat()
    except OSError as exc:
        raise PreflightBlocked(f"{label} cannot be inspected") from exc
    if info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise PreflightBlocked(f"{label} must be owner-only")


def _atomic_launch_metadata(path: Path, payload: Mapping[str, Any]) -> None:
    """Persist the new slot/prefix binding without overwriting evidence."""

    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags, 0o600)
    except OSError as exc:
        raise PreflightBlocked("new cycle slot metadata could not be claimed safely") from exc
    try:
        encoded = json.dumps(dict(payload), sort_keys=True, separators=(",", ":")).encode("utf-8")
        offset = 0
        while offset < len(encoded):
            try:
                written = os.write(fd, encoded[offset:])
            except InterruptedError:
                continue
            if written <= 0:
                raise OSError("metadata write made no progress")
            offset += written
        os.fsync(fd)
    except OSError as exc:
        raise PreflightBlocked("new cycle slot metadata could not be persisted") from exc
    finally:
        os.close(fd)


def _validate_launch_metadata(
    path: Path,
    *,
    expected_client_order_prefix: str | None = None,
    expected_route: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate the immutable reservation before its one-time admission."""

    metadata_path = path / LAUNCH_METADATA_NAME
    if metadata_path.is_symlink() or not metadata_path.is_file():
        raise PreflightBlocked("cycle reservation metadata is missing or unsafe")
    try:
        info = metadata_path.stat()
        if info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise PreflightBlocked("cycle reservation metadata must be owner-only")
        value = json.loads(metadata_path.read_text(encoding="utf-8"))
    except PreflightBlocked:
        raise
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        raise PreflightBlocked("cycle reservation metadata is unreadable") from exc
    if not isinstance(value, Mapping):
        raise PreflightBlocked("cycle reservation metadata must be one object")
    if value.get("schema") != "hcr-19-simple-launch-v1":
        raise PreflightBlocked("cycle reservation metadata has an unknown schema")
    raw_cycle_dir = value.get("cycle_dir")
    raw_prefix = value.get("client_order_prefix")
    if not isinstance(raw_cycle_dir, str) or Path(raw_cycle_dir).resolve() != path.resolve():
        raise PreflightBlocked("cycle reservation is bound to a different directory")
    if not isinstance(raw_prefix, str) or not raw_prefix.strip():
        raise PreflightBlocked("cycle reservation has no client-order prefix")
    if expected_client_order_prefix is not None and raw_prefix != expected_client_order_prefix:
        raise PreflightBlocked("cycle reservation client-order prefix does not match cycle configuration")
    if "random_route" in value:
        route = _validate_random_route(value["random_route"])
        if expected_route is not None and route != dict(expected_route):
            raise PreflightBlocked("reserved random route does not match cycle configuration")
    return dict(value)


def _validate_random_route(value: Any) -> dict[str, Any]:
    fields = {"source_account_index", "receiver_account_index", "direction"}
    if not isinstance(value, Mapping) or set(value) != fields:
        raise PreflightBlocked("invalid reserved random route")
    source, receiver = value["source_account_index"], value["receiver_account_index"]
    if (isinstance(source, bool) or not isinstance(source, int) or source < 0
        or isinstance(receiver, bool) or not isinstance(receiver, int) or receiver < 0
        or source == receiver or value["direction"] not in ("LONG", "SHORT")):
        raise PreflightBlocked("invalid reserved random route")
    return dict(value)


def select_random_route(config: "RandomCycleConfig", rng: Any = None) -> dict[str, Any]:
    """Four equally likely combinations; direction names the receiver exposure."""
    draw = _draw_integer(random.SystemRandom() if rng is None else rng, 0, 3, "opening route")
    accounts = (config.source_account_index, config.receiver_account_index)
    return {"source_account_index": accounts[draw % 2],
            "receiver_account_index": accounts[1 - draw % 2],
            "direction": "LONG" if draw < 2 else "SHORT"}


def allocate_cycle_slot(
    operator_dir: Path | str,
    *,
    client_order_prefix: str = "hood-cycle",
    random_route: Mapping[str, Any] | None = None,
    wallet_selection: Mapping[str, Any] | None = None,
) -> tuple[Path, str]:
    """Atomically reserve the next owner-only cycle directory and prefix.

    This function is intentionally called only after the simple launch
    confirmation.  Existing directories are never inspected beyond their
    names and are never overwritten; a newly created directory is retained if
    metadata persistence fails so a caller cannot accidentally reuse a
    partially claimed slot.  A wallet-pool draw is stored inside the same
    immutable reservation (display/audit only; the route stays authoritative).
    """

    route = None if random_route is None else _validate_random_route(random_route)
    if wallet_selection is not None and not isinstance(wallet_selection, Mapping):
        raise PreflightBlocked("invalid wallet selection record")
    parent = Path(operator_dir)
    _require_owner_only_directory(parent, label="operator cycle directory")
    prefix = _text(client_order_prefix, "client_order_prefix")
    for ordinal in range(1, 1_000_000):
        candidate = parent / f"cycle-{ordinal:03d}"
        try:
            candidate.mkdir(mode=0o700)
        except FileExistsError:
            continue
        except OSError as exc:
            raise PreflightBlocked("new cycle directory could not be claimed safely") from exc
        try:
            info = candidate.stat()
            if info.st_uid != os.geteuid() or info.st_mode & 0o077:
                raise PreflightBlocked("new cycle directory must be owner-only")
            unique_prefix = f"{prefix}-{time.time_ns()}-{uuid.uuid4().hex[:12]}"
            _atomic_launch_metadata(
                candidate / LAUNCH_METADATA_NAME,
                {
                    "schema": "hcr-19-simple-launch-v1",
                    **({"random_route": route} if route is not None else {}),
                    **({"wallet_selection": dict(wallet_selection)} if wallet_selection is not None else {}),
                    "claimed_at": time.time(),
                    "cycle_dir": str(candidate),
                    "client_order_prefix": unique_prefix,
                },
            )
            # Persist directory entries as well as launch.json before any key/read.
            for directory in (candidate, parent):
                directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            return candidate, unique_prefix
        except BaseException:
            # The claimed directory and any durable partial metadata are
            # evidence, not a disposable retry target.
            raise
    raise PreflightBlocked("no unused cycle directory slot is available")


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
class OpeningMarginReserve:
    """Prospective quote-currency headroom, disabled until explicitly supplied.

    Planning must leave ``initial_quote`` on *each* account after choosing
    both quantity and IMF. Fresh pre-send evidence may consume the difference
    but must still leave ``dispatch_quote``. No implicit runtime default exists.
    """

    initial_quote: Decimal
    dispatch_quote: Decimal

    def __post_init__(self) -> None:
        initial = _nonnegative(self.initial_quote, "initial margin reserve")
        dispatch = _nonnegative(self.dispatch_quote, "dispatch margin reserve")
        if dispatch > initial:
            raise ContractError("dispatch margin reserve exceeds initial reserve")
        object.__setattr__(self, "initial_quote", initial)
        object.__setattr__(self, "dispatch_quote", dispatch)


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
    max_quote_age_seconds: float | None = None
    max_source_to_receiver_seconds: float | None = None
    receiver_admission: str = "strict"
    price_improvement_ticks: int | None = None
    margin_reserve: OpeningMarginReserve | None = None
    confirmed_pilot: bool = False
    pilot_allow_leverage_update: bool = False

    def __post_init__(self) -> None:
        if self.price_improvement_ticks is not None:
            _int(self.price_improvement_ticks, "price_improvement_ticks", minimum=1)
            if self.price_improvement_ticks > 5:
                raise ContractError("price_improvement_ticks must be between 1 and 5")
        if self.receiver_admission not in ("strict", "ws_confirmed", "ack"):
            raise ContractError("receiver_admission must be strict, ws_confirmed or ack")
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
        if self.margin_reserve is not None and not isinstance(self.margin_reserve, OpeningMarginReserve):
            raise ContractError("margin_reserve must be an explicit OpeningMarginReserve")

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
        for name in ("max_quote_age_seconds", "max_source_to_receiver_seconds"):
            value = getattr(self, name)
            if value is not None:
                value = _finite_time(value, name)
                if value > self.freshness_seconds:
                    raise ContractError(f"{name} must not exceed freshness_seconds")
                object.__setattr__(self, name, value)
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
        for name in ("operator_execution_opt_in", "operator_plan_reviewed", "defer_incremental_margin_calculation", "confirmed_pilot", "pilot_allow_leverage_update"):
            if not isinstance(getattr(self, name), bool):
                raise ContractError(f"{name} must be bool")
        if self.pilot_allow_leverage_update and not self.confirmed_pilot:
            raise ContractError("pilot leverage-update opt-in requires confirmed pilot mode")
        if self.confirmed_pilot and (
            self.market_id != 1 or self.market_symbol != "BTC"
            or {self.source_account_index, self.receiver_account_index} != {27331, 27337}
            or self.margin_reserve != OpeningMarginReserve(Decimal("0.10"), Decimal("0.02"))
        ):
            raise ContractError("confirmed pilot requires the authorized BTC accounts and reserve")

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
            **({"price_improvement_ticks": self.price_improvement_ticks} if self.price_improvement_ticks is not None else {}),
            **({"receiver_admission": self.receiver_admission} if self.receiver_admission != "strict" else {}),
            **({"confirmed_pilot": True} if self.confirmed_pilot else {}),
            **({"pilot_allow_leverage_update": True} if self.pilot_allow_leverage_update else {}),
            **({"margin_reserve": {
                "initial_quote": format(self.margin_reserve.initial_quote, "f"),
                "dispatch_quote": format(self.margin_reserve.dispatch_quote, "f"),
            }} if self.margin_reserve is not None else {}),
            **{name: getattr(self, name) for name in ("max_quote_age_seconds", "max_source_to_receiver_seconds")
               if getattr(self, name) is not None},
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


def _observed_leverage_fraction(snapshot: AccountSnapshot, label: str) -> int:
    evidence = snapshot.margin_evidence
    if (evidence is None or evidence.selected_position_present is not True
            or evidence.selected_margin_mode != 0
            or evidence.selected_initial_margin_fraction is None
            or any(field in {"margin_mode", "initial_margin_fraction"} for field in evidence.invalid_fields)):
        raise PreflightBlocked(f"{label} cross-margin leverage setting is unproved")
    # The account endpoint renders the signed integer fraction as a percent
    # string with two decimal places: 4166 -> "41.66". Never round a readback.
    raw = Decimal(evidence.selected_initial_margin_fraction) * 100
    if raw != raw.to_integral_value() or not 1 <= raw <= 10000:
        raise PreflightBlocked(f"{label} leverage fraction has unsupported precision")
    return int(raw)


def minimal_sufficient_leverage_fraction(
    available_balance: Decimal, notional: Decimal, market_minimum_fraction: int,
    *, fee_cost: Decimal = Decimal(0), adverse_entry_loss: Decimal = Decimal(0),
    initial_reserve_quote: Decimal = Decimal(0),
) -> int:
    """Largest integer IMF (lowest leverage) that fits the free-balance model.

    The venue remains authoritative about fees, risk and final order admission.
    """
    balance = _positive(available_balance, "available_balance")
    amount = _positive(notional, "notional")
    minimum = _int(market_minimum_fraction, "market_minimum_fraction", minimum=1)
    floor = max(2500, minimum)  # 4x maximum; 10000 is 1x.
    cost = _nonnegative(fee_cost, "fee_cost") + _nonnegative(adverse_entry_loss, "adverse_entry_loss")
    reserve = _nonnegative(initial_reserve_quote, "initial_reserve_quote")
    if cost + reserve >= balance:
        raise PreflightBlocked("opening fee, entry loss and reserve consume available balance")
    fraction = min(10000, int(((balance - cost - reserve) * 10000 / amount).to_integral_value(rounding=ROUND_FLOOR)))
    if fraction < floor:
        raise PreflightBlocked("selected quantity needs more than supported 4x leverage or available margin")
    return fraction


def _opening_budget_components(
    metadata: MarketMetadata, account: AccountSnapshot, *, label: str,
    quantity: Decimal, worst_price: Decimal, side: str,
) -> dict[str, Decimal]:
    """Conservative snapshot components for a flat Robinhood opening leg."""
    if metadata.mark_price is None:
        raise PreflightBlocked("fresh Robinhood mark price is missing")
    mark = _positive(metadata.mark_price, "mark_price")
    price = _positive(worst_price, "opening worst price")
    amount = _positive(quantity, "opening quantity")
    observed_rate = metadata.source_fee_rate if label == "source" else metadata.receiver_fee_rate
    account_rate = account.fee_rate
    rates = [ROBINHOOD_MAKER_FEE_CAP if label == "source" else ROBINHOOD_TAKER_FEE_CAP]
    for value in (observed_rate, account_rate):
        if value is not None:
            rates.append(_nonnegative(value, "opening fee rate"))
    fee_rate = max(rates)
    adverse_per_unit = max(Decimal(0), price - mark if side == "BUY" else mark - price)
    return {
        "mark_notional": amount * mark,
        "fee_cost": amount * price * fee_rate,
        "adverse_entry_loss": amount * adverse_per_unit,
        "fee_rate": fee_rate,
    }


def compute_quantity_bounds(
    metadata: MarketMetadata,
    source: AccountSnapshot,
    receiver: AccountSnapshot,
    opening_price: Decimal,
    *, receiver_bound: Decimal | None = None, direction: Direction | None = None,
    initial_reserve_quote: Decimal = Decimal(0),
) -> RandomQuantityBounds:
    """Compute legal integer ticks within the owner's fourfold free-balance cap."""

    price = _positive(opening_price, "opening_price")
    price_int = decimal_to_integer(price, metadata.price_decimals, "opening_price")
    del price_int
    step = Decimal(1).scaleb(-metadata.size_decimals)
    source_balance = _available_balance(source, "source")
    receiver_balance = _available_balance(receiver, "receiver")
    reserve = _nonnegative(initial_reserve_quote, "initial_reserve_quote")
    lower_base = int(
        (metadata.minimum_base_amount / step).to_integral_value(rounding=ROUND_CEILING)
    )
    lower_quote = int(
        (metadata.minimum_quote_amount / price / step).to_integral_value(rounding=ROUND_CEILING)
    )
    minimum_fraction = max(2500, metadata.minimum_initial_margin_fraction or 2500)
    if source.margin_evidence is not None or receiver.margin_evidence is not None:
        if (source.margin_evidence is None or receiver.margin_evidence is None
                or receiver_bound is None or direction is None):
            raise PreflightBlocked("opening margin budget inputs are incomplete")
        source_side = direction.source_side
        receiver_side = "BUY" if source_side == "SELL" else "SELL"
        source_cost = _opening_budget_components(metadata, source, label="source",
            quantity=Decimal(1), worst_price=price, side=source_side)
        receiver_cost = _opening_budget_components(metadata, receiver, label="receiver",
            quantity=Decimal(1), worst_price=receiver_bound, side=receiver_side)
        def unit_requirement(parts):
            return (parts['mark_notional'] * Decimal(minimum_fraction) / 10000
                    + parts['fee_cost'] + parts['adverse_entry_loss'])
        original_owner_cap = min(source_balance, receiver_balance) * 10000 / minimum_fraction / price
        upper_quantity = min(original_owner_cap,
                             (source_balance - reserve) / unit_requirement(source_cost),
                             (receiver_balance - reserve) / unit_requirement(receiver_cost))
        upper = int((upper_quantity / step).to_integral_value(rounding=ROUND_FLOOR))
    else:
        upper = int(
            (min(source_balance - reserve, receiver_balance - reserve)
             * 10000 / minimum_fraction / price / step)
            .to_integral_value(rounding=ROUND_FLOOR)
        )
    lower = max(lower_base, lower_quote)
    if lower <= 0:
        raise PreflightBlocked("venue minimums produce no positive size tick")
    if upper < lower:
        raise PreflightBlocked("available balance and allowed leverage cannot fund the venue minimum quantity")
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
        raise ContractError("hold_seconds must not exceed 180")
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
    best_bid_price: Decimal | None = None
    best_bid_quantity: Decimal | None = None
    best_ask_price: Decimal | None = None
    best_ask_quantity: Decimal | None = None

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
            "bbo": {
                "bid_price": None if self.best_bid_price is None else format(self.best_bid_price, "f"),
                "bid_quantity": None if self.best_bid_quantity is None else format(self.best_bid_quantity, "f"),
                "ask_price": None if self.best_ask_price is None else format(self.best_ask_price, "f"),
                "ask_quantity": None if self.best_ask_quantity is None else format(self.best_ask_quantity, "f"),
            },
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
    reconciliation_state: str = "UNKNOWN"
    position_observed_at: float | None = None
    economic_status: str = "UNKNOWN"
    economic_findings: tuple[str, ...] = ()
    fee_total: Decimal | None = None

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
            "reconciliation_state": self.reconciliation_state,
            "position_observed_at": self.position_observed_at,
            "economic_status": self.economic_status,
            "economic_findings": list(self.economic_findings),
            "fee_total": None if self.fee_total is None else format(self.fee_total, "f"),
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
    remaining_source_position_observed_at: float | None = None
    remaining_receiver_position_observed_at: float | None = None
    opening_reason: str | None = None
    reason: str | None = None
    journal_path: str | None = None
    paired_execution: str = "UNKNOWN"
    inventory: str = "UNKNOWN"
    economics: str = "UNKNOWN"
    economic_findings: tuple[str, ...] = ()
    boundary_books_available: bool | None = None
    latency: Mapping[str, Any] | None = None

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
            "remaining_position_observed_at": {
                "source": self.remaining_source_position_observed_at,
                "receiver": self.remaining_receiver_position_observed_at,
            },
            "opening_reason": self.opening_reason,
            "reason": self.reason,
            "journal_path": self.journal_path,
            "paired_execution": self.paired_execution,
            "inventory": self.inventory,
            "economics": self.economics,
            "economic_findings": list(self.economic_findings),
            "boundary_books_available": self.boundary_books_available,
            "classifications": {
                "paired_execution": self.paired_execution,
                "inventory": self.inventory,
                "economics": self.economics,
            },
            "latency": None if self.latency is None else dict(self.latency),
        }


@runtime_checkable
class RandomCycleClient(HandoffClient, Protocol):
    source_account_index: int
    receiver_account_index: int

    async def order_book(self, market_id: int) -> OrderBookSnapshot | Mapping[str, Any]: ...


class _AccountIdentityFailure(ContractError):
    """An account response cannot establish the requested identity binding."""


class _RetryablePreparationFailure(PreflightBlocked):
    """A bounded read/preparation failure that may become valid on refresh."""


def _pre_source_stream_transient(result: HandoffResult) -> bool:
    guard = result.priority_guard if isinstance(result.priority_guard, Mapping) else {}
    return bool(result.retryable_pair and guard.get("pre_source_stream") == "TRANSIENT")


def _stream_settle_payload(result: HandoffResult, config: "RandomCycleConfig") -> dict[str, Any]:
    if not _pre_source_stream_transient(result):
        return {}
    return {"stream_settle_seconds": config.poll_interval_seconds}


async def _stream_settle(clock: Any, result: HandoffResult, config: "RandomCycleConfig") -> None:
    """Give the local stream one poll interval to deliver pending terminal events.

    Used only after a proved zero-mutation pre-source refusal; the following
    attempt still performs its own complete fresh preparation and checks.
    """
    if _pre_source_stream_transient(result):
        await clock.sleep(config.poll_interval_seconds)


class _OpeningQuantityRefresh(_RetryablePreparationFailure):
    def __init__(self, reason, selection):
        super().__init__(reason)
        self.selection = selection


class _RateLimitedAccount(_RetryablePreparationFailure):
    """Fixed, credential-free HTTP 429 context for read-only backoff."""

    def __init__(self, label: str, retry_after: float = 0.0):
        super().__init__(f"{label} temporarily rate limited (HTTP 429)")
        self.retry_after = retry_after


def _account_rate_limit(exc: Exception, label: str) -> _RateLimitedAccount | None:
    delay = read_rate_limit_delay(exc)
    return None if delay is None else _RateLimitedAccount(label, delay)


@dataclass(slots=True)
class _PairAttemptBudget:
    """One shared lineage budget for preparation and source placements."""

    limit: int
    used: int = 0

    @property
    def available(self) -> bool:
        return self.used < self.limit

    def consume(self) -> int:
        if not self.available:
            raise PreflightBlocked(f"shared pair-attempt budget exhausted ({self.used}/{self.limit})")
        self.used += 1
        return self.used


def _coerce_cycle_account(
    raw: AccountSnapshot | Mapping[str, Any],
    account_index: int,
    market_id: int,
) -> AccountSnapshot:
    """Decode and bind one account response before any dependent mutation."""

    try:
        snapshot = raw if isinstance(raw, AccountSnapshot) else AccountSnapshot.from_mapping(raw)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        raise _AccountIdentityFailure("account snapshot identity/decoder failure") from exc
    if snapshot.account_index != account_index or snapshot.market_id != market_id:
        raise _AccountIdentityFailure("account snapshot returned a different requested account or market")
    if not isinstance(snapshot.source_identity, str) or not snapshot.source_identity.strip():
        raise _AccountIdentityFailure("account snapshot source identity is missing or invalid")
    return snapshot


class _BoundMarketClient:
    """Keep pre-mutation metadata and account identities across a child run.

    ``run_handoff`` validates the one-use pre-quote context, then rechecks
    accounts before receiver admission. A later snapshot must match the
    established identities instead of becoming a new binding.
    """

    def __init__(
        self,
        delegate: RandomCycleClient,
        metadata: MarketMetadata,
        *,
        source_identity: str | None = None,
        receiver_identity: str | None = None,
        identity_failure_callback: Callable[[str], None] | None = None,
        preflight_context: HandoffPreflightContext | None = None,
    ) -> None:
        self._delegate = delegate
        self._metadata = metadata
        self._source_identity = source_identity
        self._receiver_identity = receiver_identity
        self._identity_failure_callback = identity_failure_callback
        self.handoff_preflight_context = preflight_context
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
        if account_index == self.source_account_index:
            label = "source"
            expected_identity = self._source_identity
        elif account_index == self.receiver_account_index:
            label = "receiver"
            expected_identity = self._receiver_identity
        else:
            raise ContractError("bound account identity does not match configured cycle account")
        try:
            raw = await self._delegate.account_snapshot(account_index, market_id)
            snapshot = _coerce_cycle_account(raw, account_index, market_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # A typed transport limit proves no identity mismatch. Preserve it
            # for bounded read-only reconciliation; never resend the mutation.
            if read_rate_limit_delay(exc) is not None:
                raise
            reason = f"{label} account identity/read validation failed"
            if self._identity_failure_callback is not None:
                self._identity_failure_callback(reason)
            raise PreflightBlocked(reason) from exc
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
        "minimum_initial_margin_fraction": metadata.minimum_initial_margin_fraction,
        "market_margin_mode": metadata.market_margin_mode,
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


def _fallback_order_mismatch_map(
    order: OrderSnapshot,
    plan: OrderPlan,
    *,
    expected_order_id: str | None = None,
) -> dict[str, dict[str, Any]]:
    """Return a bounded, JSON-safe field map for one rejected observation.

    The map deliberately separates immutable identity/parameter checks from
    terminal execution state.  In particular, an IOC venue may report a
    terminal zero-fill with ``remaining_quantity == 0`` even though its
    ``initial_quantity`` was positive; that is a valid state observation, not
    an identity conflict.
    """

    def value(value: Any) -> Any:
        if isinstance(value, Decimal):
            return format(value, "f")
        return value

    mismatches: dict[str, dict[str, Any]] = {}

    def check(name: str, expected: Any, observed: Any, matches: bool = True) -> None:
        if matches:
            return
        mismatches[name] = {"expected": value(expected), "observed": value(observed)}

    check("account_index", plan.account_index, order.account_index, order.account_index == plan.account_index)
    check("market_id", plan.market_id, order.market_id, order.market_id == plan.market_id)
    if expected_order_id is not None:
        check("order_id", str(expected_order_id), order.order_id, order.order_id == str(expected_order_id))
    check(
        "client_order_index",
        plan.client_order_index,
        order.client_order_index,
        order.client_order_index is not None
        and str(order.client_order_index) == str(plan.client_order_index),
    )
    check("side", plan.side, order.side, order.side == plan.side)
    check("order_type", plan.order_type, order.order_type, order.order_type == plan.order_type)
    check(
        "time_in_force",
        plan.time_in_force,
        order.time_in_force,
        order.time_in_force == plan.time_in_force,
    )
    check("reduce_only", plan.reduce_only, order.reduce_only, order.reduce_only == plan.reduce_only)
    check(
        "initial_quantity",
        plan.quantity,
        order.initial_quantity,
        order.initial_quantity == plan.quantity,
    )
    if order.price is None:
        check("price", plan.price, None, False)
    else:
        price_matches = (
            order.price <= plan.price
            if plan.side == "BUY"
            else order.price >= plan.price
        )
        expected_price = (
            f"<= {format(plan.price, 'f')}"
            if plan.side == "BUY"
            else f">= {format(plan.price, 'f')}"
        )
        check("price", expected_price, order.price, price_matches)
    return mismatches


def _fallback_reconciliation_state(
    result: FallbackResult,
    *,
    receipt: MutationReceipt | None,
    order: OrderSnapshot | None,
    trades: Sequence[TradeReceipt],
    after: AccountSnapshot | None,
    requested_quantity: Decimal,
) -> str:
    """Classify only states proven by the bounded fallback evidence."""

    if result.outcome is Outcome.UNKNOWN:
        return "UNKNOWN"
    if receipt is not None and not receipt.accepted:
        return "REJECTED"
    if order is None or not order.terminal or after is None:
        return result.reconciliation_state
    filled = sum((trade.quantity for trade in trades), Decimal(0))
    if filled == 0 and not trades:
        return "TERMINAL_ZERO_FILL"
    if filled == requested_quantity and after.signed_position == 0 and result.outcome is Outcome.SUCCESS:
        return "FULL_FILL"
    if 0 < filled < requested_quantity and result.position_after is not None:
        return "PARTIAL_FILL"
    return result.reconciliation_state


def _fallback_economic_evidence(
    result: FallbackResult,
    *,
    receipt: MutationReceipt | None,
    order: OrderSnapshot | None,
    trades: Sequence[TradeReceipt],
    history_complete: bool | None,
) -> tuple[str, tuple[str, ...], Decimal | None]:
    """Classify fee evidence for one fully bounded fallback attempt.

    Fallback fills are executions in the cycle's economics, not merely
    inventory cleanup.  A fee total is reported only when the reconciled
    history is complete and every observed trade supplies a fee.  A proven
    zero-fill/rejection has no execution fee requirement, but does not invent
    a numeric fee value.
    """

    if trades:
        if history_complete is not True:
            return (
                "UNKNOWN",
                ("fallback fee economics UNKNOWN: trade history is incomplete",),
                None,
            )
        missing = tuple(trade.trade_id for trade in trades if trade.fee is None)
        if missing:
            shown = ", ".join(missing[:8])
            suffix = "" if len(missing) <= 8 else ", ..."
            return (
                "UNKNOWN",
                (f"fallback fee economics UNKNOWN: missing fee for trade(s) {shown}{suffix}",),
                None,
            )
        total = sum((trade.fee or Decimal(0) for trade in trades), Decimal(0))
        return (
            "PROVEN",
            (f"fallback fee economics PROVEN: {len(trades)} trade(s), fee {total}",),
            total,
        )

    if result.outcome is Outcome.UNKNOWN:
        return (
            "UNKNOWN",
            ("fallback fee economics UNKNOWN: execution state is not fully reconciled",),
            None,
        )
    if not result.attempted:
        return ("PROVEN", ("fallback not dispatched: no execution fee was due",), None)
    if receipt is not None and not receipt.accepted:
        return ("PROVEN", ("fallback dispatch rejected: no execution fee was due",), None)
    if result.reconciliation_state == "TERMINAL_ZERO_FILL":
        if order is not None and order.terminal and history_complete is True:
            return (
                "PROVEN",
                ("fallback terminal zero-fill: no execution fee was due",),
                None,
            )
        return (
            "UNKNOWN",
            ("fallback fee economics UNKNOWN: zero-fill history is incomplete",),
            None,
        )
    if order is not None and order.terminal and history_complete is True:
        return ("PROVEN", ("fallback reconciled with no execution: no fee was due",), None)
    return (
        "UNKNOWN",
        ("fallback fee economics UNKNOWN: execution fee evidence is incomplete",),
        None,
    )


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
        "fee_role": trade.fee_role,
        "venue_fee_raw": trade.venue_fee_raw,
        "integrator_fee_raw": trade.integrator_fee_raw,
        "fee_evidence": trade.fee_evidence,
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


def _child_journal_path(path: Path, attempt_index: int) -> Path:
    """Keep the first legacy child name while making later attempts immutable."""

    if attempt_index == 1:
        return path
    return path.with_name(f"{path.stem}-attempt-{attempt_index:03d}{path.suffix}")


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
    # cross_initial_margin_requirement describes margin already committed to
    # existing positions. available_balance is the remaining free collateral;
    # comparing the former with the latter would reject reduce-only closure
    # precisely when a position uses more than half of an account's equity.
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


def _recovery_stop_reason(journal: DurableJournal) -> str | None:
    """Preserve the terminal recovery refusal even when no order was sent."""
    for event in reversed(journal.events):
        if event.event in {
            "FALLBACK_RECONCILIATION_UNKNOWN", "FALLBACK_STOPPED_STATE_CHANGED",
            "FALLBACK_BLOCKED_IDENTITY_BARRIER",
        }:
            return event.payload.get("reason")
    return None


def _cycle_terminal_reason(
    opening: HandoffResult | None,
    closing: HandoffResult | None,
    fallbacks: Sequence[FallbackResult],
    fallback_seed: str | None,
    *,
    remaining_source: Decimal | None = None,
    remaining_receiver: Decimal | None = None,
    boundary_books_available: bool | None = None,
    recovery_reason: str | None = None,
) -> str | None:
    """Keep the opening cause and ordered recovery without stale residuals."""

    opening_reason = _opening_reason(opening)
    receiver_not_dispatched = any(
        phase is not None
        and getattr(phase, "receiver", None) is not None
        and not getattr(getattr(phase, "receiver", None), "dispatched", True)
        for phase in (opening, closing)
    )
    parts: list[str] = []
    if recovery_reason:
        parts.append(f"residual closure stopped: {recovery_reason}")
    if opening_reason:
        parts.append(opening_reason)
    if receiver_not_dispatched and not any("not dispatched" in part.lower() for part in parts):
        parts.append("receiver order was not dispatched")

    if (
        closing is not None
        and closing.outcome is not Outcome.SUCCESS
        and closing.reason
        and closing.reason not in parts
    ):
        closing_reason = (
            _canceled_post_only_fact(closing)
            if _canceled_post_only_zero_fill(closing)
            else closing.reason
        )
        parts.append(f"paired closing: {closing_reason}")

    fallback_summaries = tuple(_fallback_terminal_summary(item) for item in fallbacks)
    parts.extend(summary for summary in fallback_summaries if summary)
    if not fallback_summaries:
        later_reason = closing.reason if closing is not None else fallback_seed
        if later_reason:
            parts.append(later_reason)

    if remaining_source is not None and remaining_receiver is not None:
        if remaining_source == 0 and remaining_receiver == 0:
            parts.append("final inventory confirmed flat")
        else:
            parts.append(
                "final inventory known residual: "
                f"source={remaining_source}, receiver={remaining_receiver}"
            )
    if _cycle_economics_unknown(opening, closing, fallbacks):
        parts.append("fees UNKNOWN")
    if _boundary_books_available(opening, closing, boundary_books_available) is False:
        parts.append("boundary books unavailable")
    if not parts:
        return None
    return "; ".join(parts)


def _fallback_terminal_summary(item: FallbackResult) -> str | None:
    """Describe one fallback in order, using its final reconciled state."""

    attempt = item.attempt
    state = item.reconciliation_state
    if state == "TERMINAL_ZERO_FILL":
        return (
            f"fallback attempt {attempt} terminal-zero-filled "
            "(terminal zero-fill/cancel)"
        )
    if state == "FULL_FILL" and item.filled_quantity == item.requested_quantity:
        return f"fallback attempt {attempt} fully closed the residual"
    if state == "PARTIAL_FILL":
        return f"fallback attempt {attempt} partially filled and left a residual"
    if state == "REJECTED":
        return f"fallback attempt {attempt} was rejected"
    if item.reason:
        return f"fallback attempt {attempt}: {item.reason}"
    return f"fallback attempt {attempt}: state UNKNOWN"


def _terminal_decimal(value: Any) -> Decimal | None:
    """Coerce one terminal price without inventing a numeric value."""

    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _external_receiver_fill_facts(phase: Any) -> tuple[str, ...]:
    """Derive bounded external-maker facts only from compatible receipts."""

    source = getattr(phase, "source", None)
    receiver = getattr(phase, "receiver", None)
    if source is None or receiver is None or not getattr(receiver, "dispatched", False):
        return ()
    source_filled = getattr(source, "filled_quantity", Decimal(0))
    receiver_filled = getattr(receiver, "filled_quantity", Decimal(0))
    if source_filled != 0 or receiver_filled <= 0:
        return ()

    source_account = getattr(source, "account_index", None)
    receiver_account = getattr(receiver, "account_index", None)
    plan = getattr(phase, "plan", None)
    source_plan = getattr(plan, "source", None)
    receiver_plan = getattr(plan, "receiver", None)
    bound_price = _terminal_decimal(getattr(receiver_plan, "price", None))
    if bound_price is None:
        bound_price = _terminal_decimal(getattr(source_plan, "price", None))
    if bound_price is None:
        source_order = getattr(source, "order", None)
        bound_price = _terminal_decimal(getattr(source_order, "price", None))
    direction = str(
        getattr(receiver_plan, "side", None) or getattr(receiver, "side", "")
    ).upper()
    source_order = getattr(source, "order", None)
    source_status = "" if source_order is None else str(getattr(source_order, "status", "")).lower()
    source_state = "zero-fill/canceled" if source_status.startswith("cancel") else "zero-fill"
    facts: list[str] = []
    for trade in tuple(getattr(receiver, "trades", ()) or ())[:8]:
        counterparty = getattr(trade, "counterparty_account_index", None)
        if counterparty is None or counterparty in {source_account, receiver_account}:
            continue
        trade_side = str(getattr(trade, "side", "")).upper()
        fill_price = _terminal_decimal(getattr(trade, "price", None))
        better = (
            bound_price is not None
            and fill_price is not None
            and direction in {"BUY", "SELL"}
            and trade_side == direction
            and (fill_price < bound_price if direction == "BUY" else fill_price > bound_price)
        )
        if better:
            comparator = "<" if direction == "BUY" else ">"
            fact = (
                f"receiver {direction} filled at {format(fill_price, 'f')}; "
                "receiver filled against better-priced external maker: "
                f"fill {format(fill_price, 'f')} {comparator} {format(bound_price, 'f')} "
                f"({direction} bound), better-priced than bound/source {format(bound_price, 'f')}; "
                f"against external account {counterparty}"
            )
        else:
            if direction in {"BUY", "SELL"} and fill_price is not None:
                fact = (
                    f"receiver {direction} filled at {format(fill_price, 'f')} "
                    f"against external account {counterparty}"
                )
            else:
                fact = f"receiver filled against external account {counterparty}"
        counterparty_order = getattr(trade, "counterparty_order_id", None)
        trade_id = getattr(trade, "trade_id", None)
        if counterparty_order is not None:
            fact += f"; counterparty order {counterparty_order}"
        if trade_id is not None:
            fact += f"; trade {trade_id}"
        facts.append(f"{fact}; source {source_state} (source remained {source_state})")
    return tuple(dict.fromkeys(facts))


def _canceled_post_only_zero_fill(phase: Any) -> bool:
    """Identify the exact terminal source case without trusting a reason string."""

    source = getattr(phase, "source", None)
    receiver = getattr(phase, "receiver", None)
    order = None if source is None else getattr(source, "order", None)
    return bool(
        source is not None
        and receiver is not None
        and getattr(source, "dispatched", False)
        and not getattr(receiver, "dispatched", True)
        and getattr(source, "filled_quantity", Decimal(0)) == 0
        and not getattr(source, "trades", ())
        and order is not None
        and str(getattr(order, "status", "")).lower() == "canceled-post-only"
        and getattr(order, "filled_quantity", Decimal(0)) == 0
        and getattr(order, "remaining_quantity", Decimal(0)) == 0
        and getattr(source, "history_complete", False) is True
        and getattr(receiver, "history_complete", False) is True
        and not getattr(source, "unknown_reasons", ())
        and not getattr(receiver, "unknown_reasons", ())
        and getattr(source, "position_after", None) == getattr(source, "position_before", None)
        and getattr(receiver, "position_after", None) == getattr(receiver, "position_before", None)
    )


def _canceled_post_only_fact(phase: Any) -> str:
    """Describe a proven source cancellation in the phase that observed it."""

    operation_mode = getattr(phase, "operation_mode", None)
    if operation_mode is None:
        plan = getattr(phase, "plan", None)
        operation_mode = getattr(plan, "operation_mode", None)
    try:
        operation_mode = OperationMode.parse(operation_mode)
    except (ContractError, TypeError, ValueError):
        operation_mode = OperationMode.PAIRED_OPENING
    if operation_mode is OperationMode.PAIRED_CLOSING:
        return (
            "paired closing source canceled-post-only zero-fill; "
            "paired close not completed"
        )
    return "source canceled-post-only zero-fill; cycle not opened"


def _boundary_books_available(
    opening: Any,
    closing: Any,
    explicit: bool | None = None,
) -> bool | None:
    """Classify boundary-book availability without treating missing as false."""

    if explicit is not None:
        return explicit
    observed_values: list[Any] = []
    for phase in (opening, closing):
        if phase is None:
            continue
        guard = getattr(phase, "priority_guard", None)
        if isinstance(guard, Mapping) and "book_observed_at" in guard:
            observed_values.append(guard.get("book_observed_at"))
    if not observed_values:
        return None
    return all(value is not None for value in observed_values)


def _cycle_economics_unknown(
    opening: Any,
    closing: Any,
    fallbacks: Sequence[FallbackResult],
) -> bool:
    phases = [phase for phase in (opening, closing) if phase is not None]
    return (
        not phases
        or any(getattr(phase, "economic_status", "UNKNOWN") != "PROVEN" for phase in phases)
        or any(item.economic_status != "PROVEN" for item in fallbacks)
    )


def terminal_cycle_facts(result: Any) -> tuple[str, ...]:
    """Return bounded, production-derived terminal facts for operator output."""

    opening = getattr(result, "opening", None)
    closing = getattr(result, "closing", None)
    facts: list[str] = []
    for phase in (opening, closing):
        if phase is None:
            continue
        if _canceled_post_only_zero_fill(phase):
            facts.append(_canceled_post_only_fact(phase))
        source = getattr(phase, "source", None)
        receiver = getattr(phase, "receiver", None)
        if source is not None and receiver is not None:
            facts.extend(_external_receiver_fill_facts(phase))
    for fallback in tuple(getattr(result, "fallbacks", ()) or ()):
        summary = _fallback_terminal_summary(fallback)
        if summary:
            facts.append(summary)

    inventory = getattr(result, "inventory", "UNKNOWN")
    if inventory == "CONFIRMED_FLAT":
        facts.append("final inventory confirmed flat")
    elif inventory == "KNOWN_RESIDUAL":
        facts.append("final inventory has a known residual")
    if getattr(result, "economics", "UNKNOWN") == "UNKNOWN":
        facts.append("fees UNKNOWN")

    boundary_available = getattr(result, "boundary_books_available", None)
    if boundary_available is None:
        raw_boundary_books = getattr(result, "boundary_books", None)
        if raw_boundary_books is not None or hasattr(result, "boundary_books"):
            boundary_available = raw_boundary_books is not None
    if boundary_available is None:
        boundary_available = _boundary_books_available(
            opening,
            closing,
        )
    if boundary_available is False:
        facts.append("boundary books unavailable")
    return tuple(dict.fromkeys(facts))


def _opening_reason(opening: HandoffResult | None) -> str | None:
    """Retain the primary opening reason and a decisive source-fill fact."""

    if opening is None:
        return None
    reasons: list[str] = []
    opening_reason = getattr(opening, "reason", None)
    if opening_reason:
        reasons.append(opening_reason)
    for reason in getattr(opening, "unknown_reasons", ()) or ():
        if "source fill observed before receiver dispatch" in reason and not any(
            "source fill observed before receiver dispatch" in item for item in reasons
        ):
            reasons.append(reason)
    source = getattr(opening, "source", None)
    receiver = getattr(opening, "receiver", None)
    if _canceled_post_only_zero_fill(opening):
        reasons.append(_canceled_post_only_fact(opening))
    if (
        source is not None
        and source.filled_quantity > 0
        and receiver is not None
        and not getattr(receiver, "dispatched", True)
        and not any("source fill observed before receiver dispatch" in item for item in reasons)
    ):
        reasons.append("source fill observed before receiver dispatch")
    if source is not None and receiver is not None:
        for fact in _external_receiver_fill_facts(opening):
            if fact not in reasons:
                reasons.append(fact)
    return "; ".join(dict.fromkeys(reasons)) or None


def _inventory_order_is_terminal(order: Any) -> bool:
    """Use the order's validated terminal state when proving no live order."""

    terminal = getattr(order, "terminal", None)
    if isinstance(terminal, bool):
        return terminal
    status = getattr(order, "status", None)
    return isinstance(status, str) and status.strip().lower() in TERMINAL_ORDER_STATUSES


def _inventory_leg_is_resolved(leg: Any) -> bool:
    """Require causal order/history evidence for one final leg state."""

    if leg is None or getattr(leg, "position_after", None) is None:
        return False
    if getattr(leg, "unknown_reasons", ()):
        return False
    if getattr(leg, "history_complete", False) is not True:
        return False
    order = getattr(leg, "order", None)
    trades = tuple(getattr(leg, "trades", ()) or ())
    if not getattr(leg, "dispatched", True):
        # A leg that was never sent is resolved only when the account read
        # confirms that no order or trade appeared on that path.
        return order is None and not trades
    if order is None or not _inventory_order_is_terminal(order):
        return False
    if getattr(order, "active", False) is True:
        return False
    order_id = getattr(leg, "order_id", None) or getattr(order, "order_id", None)
    return isinstance(order_id, str) and bool(order_id.strip())


def _inventory_phase_is_resolved(phase: Any) -> bool:
    if phase is None:
        return False
    return _inventory_leg_is_resolved(
        getattr(phase, "source", None)
    ) and _inventory_leg_is_resolved(getattr(phase, "receiver", None))


def _inventory_fallback_is_resolved(fallback: FallbackResult) -> bool:
    """Accept only bounded terminal/rejected fallback states, independent of fees."""

    if getattr(fallback, "outcome", Outcome.UNKNOWN) == Outcome.UNKNOWN:
        return False
    if getattr(fallback, "position_after", None) is None:
        return False
    return getattr(fallback, "reconciliation_state", "UNKNOWN") in {
        "REJECTED",
        "TERMINAL_ZERO_FILL",
        "PARTIAL_FILL",
        "FULL_FILL",
    }


def _inventory_phase_positions(phase: Any) -> tuple[tuple[int, Decimal], tuple[int, Decimal]] | None:
    """Return the latest causally reconciled position for both cycle accounts."""

    if phase is None:
        return None
    source = getattr(phase, "source", None)
    receiver = getattr(phase, "receiver", None)
    if source is None or receiver is None:
        return None
    source_index = getattr(source, "account_index", None)
    receiver_index = getattr(receiver, "account_index", None)
    source_position = getattr(source, "position_after", None)
    receiver_position = getattr(receiver, "position_after", None)
    if (
        isinstance(source_index, bool)
        or not isinstance(source_index, int)
        or isinstance(receiver_index, bool)
        or not isinstance(receiver_index, int)
        or source_index == receiver_index
        or source_position is None
        or receiver_position is None
    ):
        return None
    return ((source_index, source_position), (receiver_index, receiver_position))


def _inventory_chain_matches_final(
    opening: Any,
    closing: Any,
    fallbacks: Sequence[FallbackResult],
    remaining_source: Decimal,
    remaining_receiver: Decimal,
) -> bool:
    """Bind final account reads to the latest causal phase/fallback evidence."""

    opening_positions = _inventory_phase_positions(opening)
    latest_positions = _inventory_phase_positions(closing if closing is not None else opening)
    if opening_positions is None or latest_positions is None:
        return False
    if closing is not None and tuple(index for index, _ in opening_positions) != tuple(
        index for index, _ in latest_positions
    ):
        return False
    expected = dict(latest_positions)
    for fallback in fallbacks:
        account_index = getattr(fallback, "account_index", None)
        position_after = getattr(fallback, "position_after", None)
        if account_index not in expected or position_after is None:
            return False
        expected[account_index] = position_after
    source_index, receiver_index = (index for index, _ in latest_positions)
    return (
        expected[source_index] == remaining_source
        and expected[receiver_index] == remaining_receiver
    )


def _cycle_classifications(
    opening: HandoffResult | None,
    closing: HandoffResult | None,
    fallbacks: Sequence[FallbackResult],
    remaining_source: Decimal | None,
    remaining_receiver: Decimal | None,
) -> tuple[str, str, str]:
    """Keep execution, inventory, and economics independent at the terminal boundary."""

    if remaining_source is None or remaining_receiver is None:
        inventory = "UNKNOWN"
    elif remaining_source == 0 and remaining_receiver == 0:
        phases = [phase for phase in (opening, closing) if phase is not None]
        causally_resolved = bool(phases) and all(
            _inventory_phase_is_resolved(phase) for phase in phases
        )
        causally_resolved = causally_resolved and all(
            _inventory_fallback_is_resolved(item) for item in fallbacks
        )
        causally_resolved = causally_resolved and _inventory_chain_matches_final(
            opening,
            closing,
            fallbacks,
            remaining_source,
            remaining_receiver,
        )
        inventory = "CONFIRMED_FLAT" if causally_resolved else "UNKNOWN"
    else:
        inventory = "KNOWN_RESIDUAL"

    phases = [phase for phase in (opening, closing) if phase is not None]
    if not phases:
        paired_execution = "UNKNOWN"
    elif (
        opening is not None
        and opening.source is not None
        and opening.receiver is not None
        and opening.receiver.dispatched
        and opening.receiver.filled_quantity > 0
        and opening.source.filled_quantity == 0
        and opening.joint_match_status != "MATCHED"
    ):
        # Historical cycle-004 shape: the receiver found an external maker
        # while the source maker stayed zero-fill/canceled.  A later fallback
        # may flatten inventory, but that does not turn the intended pair into
        # a successful paired execution.
        paired_execution = "FAILED"
    elif opening is None or opening.outcome is not Outcome.SUCCESS:
        if opening is not None and opening.retryable_pair:
            paired_execution = "FAILED"
        elif opening is not None and opening.outcome is Outcome.PARTIAL:
            paired_execution = "PARTIAL"
        elif opening is not None and opening.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED:
            paired_execution = "FAILED"
        else:
            paired_execution = "UNKNOWN"
    elif closing is None:
        paired_execution = "UNKNOWN"
    elif closing.outcome is Outcome.SUCCESS and not fallbacks:
        paired_execution = "SUCCESS"
    elif closing.outcome is Outcome.SUCCESS:
        paired_execution = "PARTIAL"
    elif closing.retryable_pair:
        paired_execution = "FAILED"
    elif closing.outcome is Outcome.PARTIAL:
        paired_execution = "PARTIAL"
    elif closing.outcome is Outcome.FAILED_PREFLIGHT_BLOCKED:
        paired_execution = "FAILED"
    else:
        paired_execution = "UNKNOWN"

    if paired_execution == "SUCCESS" and not all(phase.mutual_execution_proven for phase in phases):
        paired_execution = "FAILED" if any(phase.joint_match_status in {"KNOWN_ZERO", "PARTIAL", "CONFLICTING"} for phase in phases) else "UNKNOWN"

    if not phases or any(phase.economic_status == "UNKNOWN" for phase in phases):
        economics = "UNKNOWN"
    elif any(item.economic_status == "UNKNOWN" for item in fallbacks):
        economics = "UNKNOWN"
    elif all(phase.economic_status == "PROVEN" for phase in phases) and all(
        item.economic_status == "PROVEN" for item in fallbacks
    ):
        economics = "KNOWN"
    else:
        economics = "UNKNOWN"
    return paired_execution, inventory, economics


def _cycle_latency(
    opening: HandoffResult | None,
    closing: HandoffResult | None,
) -> dict[str, Any] | None:
    payload: dict[str, Any] = {}
    if opening is not None and opening.latency is not None:
        payload["opening"] = dict(opening.latency)
    if closing is not None and closing.latency is not None:
        payload["closing"] = dict(closing.latency)
    return payload or None


def _with_cycle_classifications(result: RandomCycleResult) -> RandomCycleResult:
    paired_execution, inventory, economics = _cycle_classifications(
        result.opening,
        result.closing,
        result.fallbacks,
        result.remaining_source_position,
        result.remaining_receiver_position,
    )
    economic_findings = tuple(
        dict.fromkeys(
            item
            for phase in (result.opening, result.closing)
            for item in (getattr(phase, "economic_findings", ()) or ())
        )
    )
    economic_findings = tuple(
        dict.fromkeys(
            (*economic_findings,)
            + tuple(
                item
                for fallback in result.fallbacks
                for item in fallback.economic_findings
            )
        )
    )
    boundary_books_available = result.boundary_books_available
    if boundary_books_available is None:
        raw_boundary_books = getattr(result, "boundary_books", None)
        if raw_boundary_books is not None or hasattr(result, "boundary_books"):
            boundary_books_available = raw_boundary_books is not None
    if boundary_books_available is None:
        boundary_books_available = _boundary_books_available(result.opening, result.closing)
    return replace(
        result,
        paired_execution=paired_execution,
        inventory=inventory,
        economics=economics,
        economic_findings=economic_findings,
        boundary_books_available=boundary_books_available,
        latency=_cycle_latency(result.opening, result.closing),
    )


def _is_price_offset_no_room(exc: BaseException) -> bool:
    return (isinstance(exc, ContractError) and str(exc) ==
            "PRICE_OFFSET_NO_ROOM: spread cannot fit the requested tick improvement")


def _is_retryable_preparation_error(exc: BaseException) -> bool:
    """Classify only finite pre-mutation facts that a fresh snapshot can fix."""

    if isinstance(exc, _RetryablePreparationFailure) or _is_price_offset_no_room(exc):
        return True
    if isinstance(exc, (TimeoutError, asyncio.TimeoutError, ConnectionError, OSError)):
        return True
    if not isinstance(exc, PreflightBlocked):
        return False
    reason = str(exc).lower()
    if any(token in reason for token in ("identity", "authorization", "authorized", "pending", "active cycle-market", "not active", "wrong venue", "minimum", "grid", "margin", "position")):
        return False
    if "stale" in reason or "future" in reason:
        return True
    if "fresh available balance no longer funds" in reason:
        return True
    if "transient" in reason:
        return True
    return False


def _exclusive_source_price_available(
    book: OrderBookSnapshot,
    direction: Direction,
    source_price: Decimal,
) -> bool:
    """Require one tick of exclusive source improvement before exposure.

    This is a pre-placement guard, so the source order is not present in the
    public book yet.  Any executable-side volume at the selected price or a
    better price would leave queue priority unproved; the caller retries
    preparation within the existing shared per-phase attempt budget.
    """

    levels = book.asks if direction is Direction.LONG else book.bids
    if direction is Direction.LONG:
        return all(level.price > source_price for level in levels)
    return all(level.price < source_price for level in levels)


class RandomCycleEngine:
    """Execute one sampled cycle and then stop."""

    def __init__(self, client: RandomCycleClient, *, clock: Clock | None = None, rng: Any | None = None,
                 stop_requested: Callable[[], bool] | None = None) -> None:
        self.client = client
        self.clock = clock or SystemClock()
        self.rng = rng or random.SystemRandom()
        # Owner /stop is honoured only at safe points: before the first
        # leverage/order mutation, before another opening attempt and during
        # the hold.  A mutation, its reconciliation and closing are never
        # interrupted.  None keeps the unchanged behavior.
        self._stop_requested = stop_requested
        self._owner_stop_journaled = False
        self._stage = "PREFLIGHT"
        self._identity_barrier: str | None = None
        self._selection: RandomCycleSelection | None = None
        self._leverage_fractions: dict[int, int] = {}
        self._opening_plan_evidence: dict[str, dict[str, Any]] = {}
        self._last_account_observations: dict[int, AccountSnapshot] = {}
        self._pending_nonces: dict[int, Any] = {}
        self._nonce_deadline: float | None = None

    async def _release_preflight_nonces(self) -> None:
        tokens = tuple(self._pending_nonces.values())
        self._pending_nonces.clear()
        self._nonce_deadline = None
        if not tokens:
            return
        invalidate = getattr(self.client, "invalidate_reserved_nonce", None)
        if callable(invalidate):
            await asyncio.gather(*(invalidate(token) for token in tokens))

    async def _reserve_preflight_nonces(self, config: RandomCycleConfig) -> None:
        reserve = getattr(self.client, "reserve_order_nonce", None)
        if not callable(reserve) or not callable(getattr(self.client, "invalidate_reserved_nonce", None)):
            return
        # Reservation spans preparation and admission; each individual read
        # still has its own request timeout. Do not turn that per-request
        # timeout into a shorter, shared budget for the entire pair.
        deadline = time.monotonic() + config.freshness_seconds
        self._nonce_deadline = deadline

        async def read(index: int) -> None:
            self._pending_nonces[index] = await self._bounded(
                reserve(index, deadline=deadline), config, "pre-quote nonce reservation")

        tasks = [asyncio.create_task(read(index)) for index in
                 (config.source_account_index, config.receiver_account_index)]
        try:
            await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self._release_preflight_nonces()
            raise

    async def _run_prepared_handoff(
        self, config: HandoffConfig, metadata: MarketMetadata,
        source: AccountSnapshot, receiver: AccountSnapshot,
    ) -> HandoffResult:
        context = HandoffPreflightContext(config, metadata, source, receiver,
                                           dict(self._pending_nonces), self._nonce_deadline)
        client = _BoundMarketClient(
            self.client, metadata, source_identity=source.source_identity,
            receiver_identity=receiver.source_identity,
            identity_failure_callback=self._mark_identity_failure,
            preflight_context=context,
        )
        try:
            return await run_handoff(config, client, clock=self.clock)
        finally:
            await self._release_preflight_nonces()

    def _mark_identity_failure(self, reason: str) -> None:
        """Keep the first identity mismatch as a cycle-wide dependency barrier."""

        if self._identity_barrier is None:
            self._identity_barrier = f"identity failure barrier: {reason}"

    @staticmethod
    def _pilot_notional_guard(quantity: Decimal, source_price: Decimal,
                              receiver_bound: Decimal) -> None:
        if (quantity <= 0 or source_price <= 0 or receiver_bound <= 0
                or max(quantity * source_price, quantity * receiver_bound) > Decimal("40.00")):
            raise PreflightBlocked("confirmed pilot exceeds 40.00 quote per account")

    async def execute(self, config: RandomCycleConfig) -> RandomCycleResult:
        self._stage = "PREFLIGHT"
        self._read_stream_attempted = False
        self._identity_barrier = None
        self._selection = None
        self._leverage_fractions: dict[int, int] = {}
        self._opening_plan_evidence = {}
        self._last_account_observations = {}
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
            self._prepare_cycle_directory(
                config.cycle_dir,
                expected_client_order_prefix=config.client_order_prefix,
                expected_route={key: config.binding()[key] for key in
                    ("source_account_index", "receiver_account_index", "direction")},
            )
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
            journal.append("CYCLE_STARTED", {"binding": config.binding(),
                                             "runtime_provenance": capture_provenance(config.binding())})
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
            result = RandomCycleResult(
                outcome=Outcome.FAILED_PREFLIGHT_BLOCKED if preflight else Outcome.UNKNOWN,
                phase=Phase.PREFLIGHT if preflight else Phase.RECONCILIATION,
                run_id="" if journal is None else journal.run_id,
                selection=self._selection,
                reason=reason,
                journal_path=str(config.journal_path),
            )
            if preflight and journal is not None:
                # A completed refusal before paired orders is not an abandoned
                # cycle. Do not infer flat inventory or erase setting intents.
                journal.append("CYCLE_COMPLETE", result.as_dict())
            return result
        finally:
            try:
                await self._release_preflight_nonces()
                if self._read_stream_attempted:
                    stop_stream = getattr(self.client, "stop_read_stream", None)
                    try:
                        if callable(stop_stream):
                            await stop_stream()
                    finally:
                        summary = getattr(self.client, "read_stream_summary", None)
                        if journal is not None and callable(summary):
                            value = summary()
                            if value is not None:
                                journal.append("PILOT_READ_STREAM_SUMMARY", value)
                        if journal is not None:
                            from .stream_timeline import compare_saved_stream
                            try:
                                timeline = compare_saved_stream(Path(config.cycle_dir) / "stream-events.jsonl")
                                if len(timeline["orders"]) > 16:
                                    timeline["orders"] = timeline["orders"][:16]
                                    timeline["report_rows_truncated"] = True
                                journal.append("PILOT_STREAM_TIMELINE", timeline)
                            except Exception:
                                journal.append("PILOT_STREAM_TIMELINE_UNKNOWN", {
                                    "reason": "saved stream evidence is absent or invalid"
                                })
                if journal is not None:
                    read_summary = getattr(self.client, "http_read_summary", None)
                    if callable(read_summary):
                        journal.append("HTTP_READ_TIMINGS", read_summary())
            finally:
                if journal is not None:
                    journal.release_attempt()

    def _prepare_cycle_directory(
        self,
        path: Path,
        *,
        expected_client_order_prefix: str,
        expected_route: Mapping[str, Any] | None = None,
    ) -> None:
        if path.is_symlink():
            raise PreflightBlocked("cycle directory must not be a symlink")
        if not path.exists():
            path.mkdir(mode=0o700)
            return
        if not path.is_dir():
            raise PreflightBlocked("cycle path exists and is not a directory")
        info = path.stat()
        if info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise PreflightBlocked("cycle directory must be owner-only")
        children = {item.name for item in path.iterdir()}
        if not children:
            os.chmod(path, 0o700)
            return
        if children == {LAUNCH_METADATA_NAME}:
            reservation = _validate_launch_metadata(
                path,
                expected_client_order_prefix=expected_client_order_prefix,
                expected_route=expected_route,
            )
            _atomic_launch_metadata(
                path / ADMISSION_METADATA_NAME,
                {
                    "schema": "hcr-19-admission-v1",
                    "admitted_at": time.time(),
                    "pid": os.getpid(),
                    "cycle_dir": str(path),
                    "client_order_prefix": reservation["client_order_prefix"],
                },
            )
            os.chmod(path, 0o700)
            return
        if ADMISSION_METADATA_NAME in children:
            raise PreflightBlocked("cycle directory was already admitted; no mutation was replayed")
        if children:
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
        children = {item.name for item in path.iterdir()}
        if not children:
            return
        if children == {LAUNCH_METADATA_NAME}:
            _validate_launch_metadata(path)
            return
        if ADMISSION_METADATA_NAME in children:
            raise PreflightBlocked("cycle directory was already admitted; no mutation was replayed")
        if children:
            raise PreflightBlocked("cycle directory is already consumed")

    async def _initial_open_context(self, config, journal):
        async def observe():
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
            now = self.clock.now()
            _validate_account_fresh(config, source, "source", now)
            _validate_account_fresh(config, receiver, "receiver", now)
            proposal = select_automatic_prices(
                config.direction,
                metadata,
                book,
                now=now,
                freshness_seconds=config.freshness_seconds,
                price_improvement_ticks=config.price_improvement_ticks,
            )
            return metadata, book, source, receiver, proposal

        # The successful path remains one observation, without a timer or sleep.
        try:
            return await observe()
        except ContractError as exc:
            if not _is_price_offset_no_room(exc) or config.confirmed_pilot:
                raise
        deadline = _clock_monotonic(self.clock) + config.reconcile_timeout_seconds
        for attempt in range(1, config.max_poll_count + 1):
            remaining = deadline - _clock_monotonic(self.clock)
            if attempt == config.max_poll_count or remaining <= config.poll_interval_seconds:
                raise PreflightBlocked("PRICE_OFFSET_NO_ROOM: initial spread wait exhausted; no orders sent")
            if attempt == 1:
                journal.append("INITIAL_SPREAD_WAIT", {
                    "price_improvement_ticks": config.price_improvement_ticks,
                    "maximum_reads": config.max_poll_count,
                    "timeout_seconds": config.reconcile_timeout_seconds,
                })
            await self.clock.sleep(config.poll_interval_seconds)
            remaining = deadline - _clock_monotonic(self.clock)
            if remaining <= 0:
                raise PreflightBlocked("PRICE_OFFSET_NO_ROOM: initial spread wait exhausted; no orders sent")
            try:
                context = await asyncio.wait_for(observe(), timeout=remaining)
                if _clock_monotonic(self.clock) >= deadline:
                    raise PreflightBlocked("initial spread readiness deadline exceeded")
                journal.append("INITIAL_SPREAD_READY", {"reads": attempt + 1})
                return context
            except ContractError as exc:
                if not _is_price_offset_no_room(exc):
                    raise
        raise AssertionError("initial spread wait must terminate")

    async def _execute_locked(self, config: RandomCycleConfig, journal: DurableJournal) -> RandomCycleResult:
        self._stage = "PREFLIGHT"
        start_stream = getattr(self.client, "start_read_stream", None)
        if callable(start_stream) and not config.confirmed_pilot:
            self._read_stream_attempted = True
            await start_stream(ready_timeout=5)
        metadata, book, source, receiver, proposal = await self._initial_open_context(config, journal)
        bounds = compute_quantity_bounds(metadata, source, receiver, proposal.source_limit_price,
                                         receiver_bound=proposal.receiver_worst_price, direction=config.direction,
                                         initial_reserve_quote=(config.margin_reserve.initial_quote
                                                                if config.margin_reserve else Decimal(0)))
        if config.confirmed_pilot:
            quantity = Decimal("0.00020")
            ticks = quantity / bounds.size_step
            if ticks != ticks.to_integral_value() or not bounds.lower_tick <= ticks <= bounds.upper_tick:
                raise PreflightBlocked("confirmed pilot exact quantity is outside fresh legal bounds")
            quantity_tick, hold_seconds = int(ticks), 20
            self._pilot_notional_guard(quantity, proposal.source_limit_price, proposal.receiver_worst_price)
            observed_rates = [rate for rate in (
                metadata.source_fee_rate, metadata.receiver_fee_rate,
                source.fee_rate, receiver.fee_rate,
            ) if rate is not None]
            fee_rate_bound = max(ROBINHOOD_MAKER_FEE_CAP, ROBINHOOD_TAKER_FEE_CAP,
                                 *observed_rates)
            if Decimal("240.00") * fee_rate_bound > Decimal("0.10"):
                raise PreflightBlocked("confirmed pilot modeled paired and recovery fee ceiling exceeds 0.10 quote")
        else:
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
            best_bid_price=book.bids[0].price,
            best_bid_quantity=book.bids[0].quantity,
            best_ask_price=book.asks[0].price,
            best_ask_quantity=book.asks[0].quantity,
        )
        self._selection = selection
        journal.append("SELECTION_PROVED", {"selection": selection.as_dict(), "metadata": _metadata_payload(metadata), "book_observed_at": book.observed_at})
        if self._owner_stop(journal, "BEFORE_LEVERAGE"):
            return self._owner_stopped_before_orders(config, journal, selection)

        source, receiver = await self._configure_leverage(
            config, journal, metadata, book, selection, source, receiver,
        )
        if config.confirmed_pilot:
            start_stream = getattr(self.client, "start_read_stream", None)
            self._read_stream_attempted = callable(start_stream)
            if not callable(start_stream) or not await start_stream(ready_timeout=5):
                raise PreflightBlocked("confirmed pilot read-only stream is not ready")


        pair_budget = _PairAttemptBudget(limit=1 if config.confirmed_pilot else MAX_OPENING_ATTEMPTS)
        initial_metadata = metadata
        initial_book = book
        initial_source = source
        initial_receiver = receiver
        prepared = await self._prepare_open_with_retries(
            config,
            journal,
            selection,
            metadata,
            book,
            source,
            receiver,
            pair_budget,
        )
        if isinstance(prepared, RandomCycleResult):
            result = _with_cycle_classifications(prepared)
            journal.append("CYCLE_COMPLETE", result.as_dict())
            return result
        metadata, book, source, receiver, selection = prepared
        self._selection = selection
        if self._owner_stop(journal, "BEFORE_FIRST_ORDER"):
            return self._owner_stopped_before_orders(config, journal, selection)
        opening_preparation_result: RandomCycleResult | None = None
        owner_stop_reason: str | None = None
        while True:
            attempt_index = pair_budget.used
            if config.confirmed_pilot:
                stream_ready = getattr(self.client, "read_stream_ready", None)
                if not callable(stream_ready) or not stream_ready():
                    raise PreflightBlocked("confirmed pilot read-only stream lost readiness before LIMIT")
            opening_path = _child_journal_path(config.opening_journal_path, attempt_index)
            opening_config = self._handoff_config(
                config,
                selection.quantity,
                selection.opening_source_price,
                selection.opening_receiver_bound,
                opening_path,
                OperationMode.PAIRED_OPENING,
                source.signed_position,
                receiver.signed_position,
                attempt_index=attempt_index,
                source_quote_observed_at=selection.book_observed_at,
            )
            journal.append(
                "OPENING_PLAN_READY",
                {
                    "config": opening_config_binding(opening_config),
                    "latency": dict(getattr(self, "_last_quote_read", {})),
                    "selection": selection.as_dict(),
                    "attempt": attempt_index,
                    "lineage": {"used": pair_budget.used, "limit": pair_budget.limit},
                },
            )
            journal.append(
                "FIRST_MUTATION_BOUNDARY",
                {
                    "message": "opening handoff admitted; later order state is authoritative only from its immutable child journal",
                    "selection": selection.as_dict(),
                    "attempt": attempt_index,
                    "journal_path": opening_config.journal_path,
                },
            )
            self._stage = "OPENING"
            opening = await self._run_prepared_handoff(opening_config, metadata, source, receiver)
            self._stage = "OPENING_RECONCILED"
            journal.append(
                "OPENING_COMPLETE",
                {"result": opening.as_dict(), "attempt": attempt_index, "journal_path": opening_config.journal_path},
            )
            if not opening.retryable_pair:
                break
            if self._owner_stop(journal, "BEFORE_OPENING_RETRY"):
                # No further attempt; any fill still goes to the unchanged
                # reduce-only residual closure below.
                owner_stop_reason = OWNER_STOP_RETRY_REASON
                break
            if not pair_budget.available:
                journal.append(
                    "PAIR_ATTEMPT_EXHAUSTED",
                    {
                        "phase": "PAIRED_OPENING",
                        "attempt": attempt_index,
                        "maximum_attempts": pair_budget.limit,
                        "reason": "safe zero-fill guard retry exhausted",
                    },
                )
                break
            journal.append(
                "PAIR_ATTEMPT_RETRY",
                {
                    "phase": "PAIRED_OPENING",
                    "from_attempt": attempt_index,
                    "next_attempt": pair_budget.used + 1,
                    "reason": opening.reason,
                    "guard": opening.priority_guard,
                    "lineage": {"used": pair_budget.used, "limit": pair_budget.limit},
                    **_stream_settle_payload(opening, config),
                },
            )
            await _stream_settle(self.clock, opening, config)
            retry_prepared = await self._prepare_open_with_retries(
                config,
                journal,
                selection,
                initial_metadata,
                initial_book,
                initial_source,
                initial_receiver,
                pair_budget,
            )
            if isinstance(retry_prepared, RandomCycleResult):
                opening_preparation_result = retry_prepared
                break
            metadata, book, source, receiver, selection = retry_prepared
            self._selection = selection
        if not opening.mutual_execution_proven:
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
            reason = _cycle_terminal_reason(
                opening,
                None,
                fallbacks,
                (
                    opening_preparation_result.reason
                    if opening_preparation_result is not None
                    else owner_stop_reason or "paired opening did not prove a complete cycle"
                ),
                remaining_source=remaining_source,
                remaining_receiver=remaining_receiver,
                recovery_reason=_recovery_stop_reason(journal),
            )
            result = _with_cycle_classifications(RandomCycleResult(
                outcome=outcome,
                phase=Phase.COMPLETE,
                run_id=journal.run_id,
                selection=selection,
                opening=opening,
                fallbacks=tuple(fallbacks),
                remaining_source_position=remaining_source,
                remaining_receiver_position=remaining_receiver,
                remaining_source_position_observed_at=(None if remaining_source is None else self._last_observed_at(config.source_account_index)),
                remaining_receiver_position_observed_at=(None if remaining_receiver is None else self._last_observed_at(config.receiver_account_index)),
                opening_reason=_opening_reason(opening),
                reason=reason,
                journal_path=str(config.journal_path),
            ))
            journal.append("CYCLE_COMPLETE", result.as_dict())
            return result

        anchor_wall = self.clock.now()
        anchor_mono = _clock_monotonic(self.clock)
        journal.append("HOLD_ANCHORED", {"hold_seconds": selection.hold_seconds, "anchor_wall": anchor_wall, "anchor_monotonic": anchor_mono})
        try:
            self._stage = "HOLD"
            held_seconds = await self._wait_hold(selection.hold_seconds, anchor_mono)
        except Exception as exc:
            reason = f"hold timer could not reach its persisted deadline: {sanitize_exception(exc)}"
            result = _with_cycle_classifications(RandomCycleResult(
                outcome=Outcome.UNKNOWN,
                phase=Phase.RECONCILIATION,
                run_id=journal.run_id,
                selection=selection,
                opening=opening,
                opening_reason=_opening_reason(opening),
                reason=reason,
                journal_path=str(config.journal_path),
            ))
            journal.append("CYCLE_COMPLETE", result.as_dict())
            return result
        if held_seconds is not None:
            # The owner ended the hold early; the unchanged paired closing and
            # residual recovery follow immediately.
            journal.append("HOLD_ENDED_BY_OWNER_STOP", {
                "hold_seconds": selection.hold_seconds, "held_seconds": held_seconds,
            })

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
            and closing.mutual_execution_proven
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
            outcome = Outcome.PARTIAL
        elif fallbacks and all_fallback_reconciled and remaining_source == 0 and remaining_receiver == 0:
            outcome = Outcome.PARTIAL
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
        recovered_after_pair_or_fallback = bool(fallbacks) or (
            closing is not None and closing.outcome is not Outcome.SUCCESS
        )
        reason = None
        if outcome is not Outcome.SUCCESS or recovered_after_pair_or_fallback:
            reason = (
                self._identity_barrier
                or _cycle_terminal_reason(
                    opening,
                    closing,
                    fallbacks,
                    fallback_seed,
                    remaining_source=remaining_source,
                    remaining_receiver=remaining_receiver,
                    recovery_reason=_recovery_stop_reason(journal),
                )
                or "cycle closure did not prove exact flat positions"
            )
        result = _with_cycle_classifications(RandomCycleResult(
            outcome=outcome,
            phase=Phase.COMPLETE,
            run_id=journal.run_id,
            selection=selection,
            opening=opening,
            closing=closing,
            fallbacks=tuple(fallbacks),
            remaining_source_position=remaining_source,
            remaining_receiver_position=remaining_receiver,
            remaining_source_position_observed_at=(None if remaining_source is None else self._last_observed_at(config.source_account_index)),
            remaining_receiver_position_observed_at=(None if remaining_receiver is None else self._last_observed_at(config.receiver_account_index)),
            opening_reason=_opening_reason(opening),
            reason=reason,
            journal_path=str(config.journal_path),
        ))
        journal.append("CYCLE_COMPLETE", result.as_dict())
        return result

    def _last_observed_at(self, account_index: int) -> float | None:
        snapshot = self._last_account_observations.get(account_index)
        return None if snapshot is None else snapshot.observed_at

    async def _configure_leverage(
        self, config: RandomCycleConfig, journal: DurableJournal,
        metadata: MarketMetadata, book: OrderBookSnapshot, selection: RandomCycleSelection,
        source: AccountSnapshot, receiver: AccountSnapshot,
    ) -> tuple[AccountSnapshot, AccountSnapshot]:
        setter = getattr(self.client, "update_leverage_fraction", None)
        if not callable(setter):
            if config.confirmed_pilot:
                raise PreflightBlocked("confirmed pilot requires fresh leverage readback without a setting write")
            # Legacy synthetic clients have no venue margin surface. A client
            # supplying real margin evidence must support exact configuration.
            if source.margin_evidence is not None or receiver.margin_evidence is not None:
                raise PreflightBlocked("leverage setting capability is missing")
            return source, receiver
        if (metadata.minimum_initial_margin_fraction is None
                or metadata.market_margin_mode != 0):
            raise PreflightBlocked("fresh Robinhood cross-margin leverage limits are unproved")
        if source.signed_position != 0 or receiver.signed_position != 0:
            raise PreflightBlocked("leverage can only be configured before a flat cycle")
        notional = selection.quantity * selection.opening_source_price
        original = {source.account_index: source, receiver.account_index: receiver}
        budgets = {}
        targets = {}
        initial_evidence = {}
        source_side = config.direction.source_side
        for label, account, worst_price, side in (
            ("source", source, selection.opening_source_price, source_side),
            ("receiver", receiver, selection.opening_receiver_bound,
             "BUY" if source_side == "SELL" else "SELL"),
        ):
            if account.margin_evidence is not None:
                parts = _opening_budget_components(metadata, account, label=label,
                    quantity=selection.quantity, worst_price=worst_price, side=side)
            else:
                parts = {'mark_notional': notional, 'fee_cost': Decimal(0),
                         'adverse_entry_loss': Decimal(0), 'fee_rate': Decimal(0)}
            budgets[label] = {key: format(value, 'f') for key, value in parts.items()}
            targets[account.account_index] = minimal_sufficient_leverage_fraction(
                _available_balance(account, label), parts['mark_notional'],
                metadata.minimum_initial_margin_fraction,
                fee_cost=parts['fee_cost'], adverse_entry_loss=parts['adverse_entry_loss'],
                initial_reserve_quote=(config.margin_reserve.initial_quote
                                       if config.margin_reserve else Decimal(0)))
            initial_required = (
                parts['mark_notional'] * Decimal(targets[account.account_index]) / 10000
                + parts['fee_cost'] + parts['adverse_entry_loss']
            )
            initial_evidence[label] = {
                'account_index': account.account_index,
                'account_source_identity': account.source_identity,
                'account_observed_at': account.observed_at,
                'metadata_market_id': metadata.market_id,
                'metadata_symbol': metadata.symbol,
                'metadata_observed_at': metadata.observed_at,
                'book_market_id': book.market_id,
                'book_symbol': book.symbol,
                'book_observed_at': book.observed_at,
                'available_balance': _available_balance(account, label),
                'mark_price': metadata.mark_price,
                'worst_price': worst_price,
                'required': initial_required,
                'headroom': _available_balance(account, label) - initial_required,
                'fee_rate': parts['fee_rate'],
            }
        current = {index: _observed_leverage_fraction(account, "source" if index == source.account_index else "receiver")
                   for index, account in original.items()}
        journal.append("LEVERAGE_PLAN", {
            "quantity": format(selection.quantity, "f"), "price": format(selection.opening_source_price, "f"),
            "notional": format(notional, "f"), "target_fraction_bps": targets,
            "observed_fraction_bps": current, "opening_budgets": budgets,
            "initial_plan_observations": initial_evidence,
            "initial_reserve_quote": (None if config.margin_reserve is None
                                      else format(config.margin_reserve.initial_quote, "f")),
            "dispatch_reserve_quote": (None if config.margin_reserve is None
                                       else format(config.margin_reserve.dispatch_quote, "f")),
        })
        if config.confirmed_pilot and any(target != 10000 for target in targets.values()):
            raise PreflightBlocked("confirmed pilot setting target must remain exactly 10000 bps")
        if (config.confirmed_pilot and not config.pilot_allow_leverage_update
                and any(current[index] != targets[index] for index in current)):
            raise PreflightBlocked("confirmed pilot forbids leverage or margin-setting changes")
        for index in (source.account_index, receiver.account_index):
            if current[index] == targets[index]:
                continue
            self._stage = "LEVERAGE"
            journal.append("LEVERAGE_UPDATE_INTENT", {
                "account_index": index, "market_id": config.market_id,
                "fraction_bps": targets[index], "margin_mode": 0,
                "source_identity": original[index].source_identity,
            })
            not_sent_recorded = False
            def record_not_sent():
                nonlocal not_sent_recorded
                journal.append("LEVERAGE_UPDATE_NOT_SENT", {
                    "account_index": index, "market_id": config.market_id,
                    "fraction_bps": targets[index],
                })
                not_sent_recorded = True
            setting_call = (
                setter(index, config.market_id, targets[index], 0,
                       prepared_intent=lambda identity: journal.append("LEVERAGE_TX_PREPARED", identity),
                       cancelled_before_transport=record_not_sent)
                if getattr(self.client, "supports_leverage_prepared_intent", False)
                else setter(index, config.market_id, targets[index], 0)
            )
            try:
                receipt = await self._bounded(setting_call, config, "leverage setting")
            except LeverageNotSent as exc:
                record_not_sent()
                self._stage = "PREFLIGHT"
                raise PreflightBlocked(f"account {index} leverage setting was not sent") from exc
            except TimeoutError as exc:
                if not_sent_recorded:
                    self._stage = "PREFLIGHT"
                    raise PreflightBlocked(f"account {index} leverage setting was not sent") from exc
                raise
            if not isinstance(receipt, MutationReceipt):
                raise PreflightBlocked(f"account {index} leverage setting response is undecidable")
            if not receipt.accepted:
                journal.append("LEVERAGE_UPDATE_REJECTED", {
                    "account_index": index, "market_id": config.market_id,
                    "fraction_bps": targets[index],
                    "reason": receipt.error,
                })
                self._stage = "PREFLIGHT"
                raise PreflightBlocked(f"account {index} leverage setting was rejected")
            # Acceptance is not effective state. Read both accounts again and
            # stop without another setting transaction on missing/mismatched proof.
            effective = None
            observed = None
            for attempt in range(min(3, config.max_poll_count)):
                source, receiver = await self._rate_limited_accounts(config, journal=journal)
                for account in (source, receiver):
                    before = original[account.account_index]
                    if account.source_identity != before.source_identity or account.signed_position != 0:
                        raise PreflightBlocked("account identity or position changed during leverage setting")
                observed = source if index == source.account_index else receiver
                effective = _observed_leverage_fraction(observed, "source" if index == source.account_index else "receiver")
                if effective == targets[index]:
                    break
                if attempt + 1 < min(3, config.max_poll_count):
                    await self.clock.sleep(config.poll_interval_seconds)
            if effective != targets[index] or observed is None:
                raise PreflightBlocked(f"account {index} leverage setting is not effective or readback is stale")
            journal.append("LEVERAGE_UPDATE_CONFIRMED", {
                "account_index": index, "market_id": config.market_id,
                "fraction_bps": effective, "observed_at": observed.observed_at,
            })
            current[index] = effective
        self._leverage_fractions = targets
        self._opening_plan_evidence = initial_evidence
        self._stage = "PREFLIGHT"
        return source, receiver

    async def _rate_limited_accounts(
        self, config: RandomCycleConfig,
        *, journal: DurableJournal | None = None,
    ) -> tuple[AccountSnapshot, AccountSnapshot]:
        return await self._recovery_read(
            config, lambda: self._accounts(config),
            "account snapshot", journal, rate_limit_only=True,
        )

    async def _accounts(self, config: RandomCycleConfig, now: float | None = None) -> tuple[AccountSnapshot, AccountSnapshot]:
        source, receiver = await self._parallel_accounts(config)
        # Account observations are stamped by the reads themselves.  Validate
        # against the clock after both have completed so a real transport's
        # small read latency cannot make a fresh response look future-dated.
        del now
        validation_now = self.clock.now()
        _validate_account_fresh(config, source, "source", validation_now)
        _validate_account_fresh(config, receiver, "receiver", validation_now)
        self._last_account_observations = {
            source.account_index: source,
            receiver.account_index: receiver,
        }
        return source, receiver

    async def _parallel_accounts(
        self,
        config: RandomCycleConfig,
    ) -> tuple[AccountSnapshot, AccountSnapshot]:
        """Read the two independent cycle accounts with a bounded fan-out.

        These are read-only observations.  Keep the source/receiver result
        order stable, cap concurrency at two, and drain the sibling if one
        read fails or the parent operation is cancelled before any dependent
        validation or mutation can continue.
        """

        tasks: list[asyncio.Task[Any]] = []
        try:
            tasks.append(
                asyncio.create_task(
                    self._read_account(config, config.source_account_index, "source account read")
                )
            )
            tasks.append(
                asyncio.create_task(
                    self._read_account(config, config.receiver_account_index, "receiver account read")
                )
            )
            source, receiver = await asyncio.gather(*tasks)
        except BaseException as exc:
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            if not isinstance(exc, asyncio.CancelledError):
                # A simultaneous transport error must not hide a conflicting
                # account identity and turn it into retryable evidence.
                for task in tasks:
                    if not task.cancelled() and isinstance(task.exception(), _AccountIdentityFailure):
                        raise task.exception()
                rate_limits = [task.exception() for task in tasks if not task.cancelled()
                               and isinstance(task.exception(), _RateLimitedAccount)]
                if rate_limits:
                    raise max(rate_limits, key=lambda error: error.retry_after)
                for task in tasks:
                    if task.cancelled():
                        continue
                    task_error = task.exception()
                    if task_error is not None:
                        raise task_error
            raise
        return source, receiver

    async def _recovery_read(
        self, config: RandomCycleConfig, read: Callable[[], Any],
        label: str, journal: DurableJournal | None = None,
        *, rate_limit_only: bool = False,
    ) -> Any:
        """Retry only read transport failures; never wrap a mutation here."""
        deadline = _clock_monotonic(self.clock) + config.reconcile_timeout_seconds
        for attempt in range(1, config.max_poll_count + 1):
            remaining = deadline - _clock_monotonic(self.clock)
            if remaining <= 0:
                raise _RetryablePreparationFailure(f"{label} recovery read deadline exceeded")
            try:
                return await asyncio.wait_for(read(), timeout=remaining)
            except (_RetryablePreparationFailure, TimeoutError, ConnectionError, OSError) as exc:
                if rate_limit_only and not isinstance(exc, _RateLimitedAccount):
                    raise
                remaining = deadline - _clock_monotonic(self.clock)
                delay = config.poll_interval_seconds
                if isinstance(exc, _RateLimitedAccount):
                    delay = max(delay, min(8.0, 2.0 ** min(attempt - 1, 3)), exc.retry_after)
                retry = attempt < config.max_poll_count and remaining > (delay if isinstance(exc, _RateLimitedAccount) else 0)
                if journal is not None:
                    journal.append("RECOVERY_READ_RETRY", {
                        "operation": label, "attempt": attempt, "will_retry": retry,
                        "reason": _cycle_exception_reason(exc),
                        "delay_seconds": delay if retry else 0.0,
                    })
                if not retry:
                    raise
                await self.clock.sleep(min(delay, remaining))

    async def _recovery_accounts(
        self, config: RandomCycleConfig, journal: DurableJournal | None = None,
    ) -> tuple[AccountSnapshot, AccountSnapshot]:
        return await self._recovery_read(config, lambda: self._accounts(config), "recovery accounts", journal)

    async def _read_account(
        self,
        config: RandomCycleConfig,
        account_index: int,
        label: str,
    ) -> AccountSnapshot:
        try:
            raw = await self._bounded(
                self.client.account_snapshot(account_index, config.market_id),
                config,
                label,
            )
            return _coerce_cycle_account(raw, account_index, config.market_id)
        except asyncio.CancelledError:
            raise
        except _AccountIdentityFailure:
            raise
        except (TimeoutError, asyncio.TimeoutError, ConnectionError, OSError) as exc:
            raise _RetryablePreparationFailure(f"{label} was transiently unavailable") from exc
        except Exception as exc:
            limited = _account_rate_limit(exc, label)
            if limited is not None:
                raise limited from None
            raise _AccountIdentityFailure("account snapshot read failed") from exc

    async def _prepare_open_with_retries(
        self,
        config: RandomCycleConfig,
        journal: DurableJournal,
        selection: RandomCycleSelection,
        metadata: MarketMetadata,
        book: OrderBookSnapshot,
        source: AccountSnapshot,
        receiver: AccountSnapshot,
        budget: _PairAttemptBudget,
    ) -> tuple[MarketMetadata, OrderBookSnapshot, AccountSnapshot, AccountSnapshot, RandomCycleSelection] | RandomCycleResult:
        """Refresh all pre-mutation facts inside the shared pair budget.

        The first quantity/hold draw is already proved in ``selection``.  A
        fresh quote may replace prices and bounds. A proved margin shortfall
        while opening positions are still at the initial baseline may reduce
        quantity, with a durable event and another fresh preparation attempt
        from the same shared budget; never draw again, grow or resize closing.
        Once this method returns successfully, callers are allowed
        to cross the first-mutation boundary.  Every failure is handled here
        so a terminal preflight result retains the proved selection.
        """

        initial_metadata = metadata
        initial_book = book
        initial_source = source
        initial_receiver = receiver
        current_selection = selection
        while budget.available:
            attempt = budget.consume()
            journal.append(
                "PREPARATION_ATTEMPT",
                {
                    "attempt": attempt,
                    "maximum_attempts": budget.limit,
                    "budget_used": budget.used,
                    "budget_remaining": budget.limit - budget.used,
                    "selected_quantity": format(current_selection.quantity, "f"),
                    "selected_quantity_tick": current_selection.quantity_tick,
                    "selected_hold_seconds": current_selection.hold_seconds,
                },
            )
            try:
                prepared_metadata, prepared_book, prepared_source, prepared_receiver, refreshed_selection = await self._revalidate_open(
                    config,
                    current_selection,
                    journal=journal,
                    initial_metadata=initial_metadata,
                    initial_book=initial_book,
                    initial_source=initial_source,
                    initial_receiver=initial_receiver,
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                reason = _cycle_exception_reason(exc)
                retryable = _is_retryable_preparation_error(exc)
                journal.append(
                    "PREPARATION_FAILED",
                    {
                        "attempt": attempt,
                        "retryable": retryable,
                        "reason": reason,
                    },
                )
                if retryable and budget.available:
                    if isinstance(exc, _OpeningQuantityRefresh):
                        journal.append("OPENING_QUANTITY_RECALCULATED", {
                            "attempt": attempt,
                            "old_quantity": format(current_selection.quantity, "f"),
                            "new_quantity": format(exc.selection.quantity, "f"),
                            "next_attempt": budget.used + 1,
                            "reason": "fresh quote and confirmed margin budget",
                        })
                        current_selection = exc.selection
                        self._selection = current_selection
                    journal.append(
                        "PREPARATION_RETRY",
                        {
                            "attempt": attempt,
                            "next_attempt": budget.used + 1,
                            "reason": reason,
                            "delay_seconds": config.poll_interval_seconds,
                            "budget_used": budget.used,
                            "budget_remaining": budget.limit - budget.used,
                        },
                    )
                    await self.clock.sleep(config.poll_interval_seconds)
                    continue
                journal.append(
                    "PREPARATION_EXHAUSTED" if retryable else "PREPARATION_BLOCKED",
                    {
                        "attempt": attempt,
                        "maximum_attempts": budget.limit,
                        "budget_used": budget.used,
                        "budget_remaining": budget.limit - budget.used,
                        "reason": reason,
                        "retryable": retryable,
                    },
                )
                terminal_reason = (
                    f"opening shared pair-attempt budget exhausted after {attempt}/{budget.limit}: {reason}"
                    if retryable
                    else f"opening preparation blocked: {reason}"
                )
                result = _with_cycle_classifications(RandomCycleResult(
                    outcome=Outcome.FAILED_PREFLIGHT_BLOCKED,
                    phase=Phase.PREFLIGHT,
                    run_id=journal.run_id,
                    selection=current_selection,
                    reason=terminal_reason,
                    journal_path=str(config.journal_path),
                ))
                journal.append("CYCLE_PREFLIGHT_BLOCKED", result.as_dict())
                return result

            if refreshed_selection.opening_source_price != current_selection.opening_source_price or refreshed_selection.opening_receiver_bound != current_selection.opening_receiver_bound:
                journal.append(
                    "OPENING_PRICE_UPDATED",
                    {
                        "attempt": attempt,
                        "old_source_price": format(current_selection.opening_source_price, "f"),
                        "new_source_price": format(refreshed_selection.opening_source_price, "f"),
                        "old_receiver_bound": format(current_selection.opening_receiver_bound, "f"),
                        "new_receiver_bound": format(refreshed_selection.opening_receiver_bound, "f"),
                        "old_metadata_observed_at": current_selection.metadata_observed_at,
                        "new_metadata_observed_at": refreshed_selection.metadata_observed_at,
                        "old_book_observed_at": current_selection.book_observed_at,
                        "new_book_observed_at": refreshed_selection.book_observed_at,
                    },
                )
            if refreshed_selection.bounds.as_dict() != current_selection.bounds.as_dict():
                journal.append(
                    "OPENING_BOUNDS_REFRESHED",
                    {
                        "attempt": attempt,
                        "old_bounds": current_selection.bounds.as_dict(),
                        "new_bounds": refreshed_selection.bounds.as_dict(),
                    },
                )
            journal.append(
                "PREPARATION_ACCEPTED",
                {
                    "attempt": attempt,
                    "selection": refreshed_selection.as_dict(),
                },
            )
            return prepared_metadata, prepared_book, prepared_source, prepared_receiver, refreshed_selection

        raise AssertionError("shared pair-attempt budget returned without a terminal result")

    async def _parallel_revalidation_context(
        self,
        config: RandomCycleConfig,
        *,
        market_read_label: str,
        warm_closing_transport: bool = False,
    ) -> tuple[MarketMetadata, AccountSnapshot, AccountSnapshot]:
        """Read metadata and both accounts before taking the final book quote.

        Account and metadata observations are independent read-only inputs for
        sizing.  The book is intentionally fetched by the caller only after
        these checks finish, so the source quote is bound as late as possible
        without reusing or renewing any observation.
        """

        async def read_metadata() -> MarketMetadata:
            return _as_market(
                await self._bounded(
                    self.client.market_metadata(config.market_id),
                    config,
                    market_read_label,
                )
            )

        await self._release_preflight_nonces()
        warm_task = None
        if warm_closing_transport:
            self._closing_http_warmup = {"status": "unavailable"}
            warm = getattr(self.client, "warm_mutation_http", None)
            if callable(warm):
                async def warm_once() -> None:
                    started = time.perf_counter()
                    try:
                        await asyncio.wait_for(warm(), timeout=min(1.0, config.request_timeout_seconds))
                        self._closing_http_warmup["status"] = "completed"
                    except asyncio.CancelledError:
                        self._closing_http_warmup["status"] = "unfinished_read_cancelled"
                        raise
                    except Exception:
                        self._closing_http_warmup["status"] = "failed"
                    finally:
                        self._closing_http_warmup["seconds"] = time.perf_counter() - started
                warm_task = asyncio.create_task(warm_once())
        tasks = [
            asyncio.create_task(read_metadata()),
            asyncio.create_task(self._rate_limited_accounts(config)),
            asyncio.create_task(self._reserve_preflight_nonces(config)),
        ]
        try:
            metadata, accounts, _ = await asyncio.gather(*tasks)
        except BaseException:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self._release_preflight_nonces()
            raise
        finally:
            if warm_task is not None:
                if not warm_task.done():
                    warm_task.cancel()
                await asyncio.gather(warm_task, return_exceptions=True)
        source, receiver = accounts
        return metadata, source, receiver

    async def _revalidate_open(
        self,
        config: RandomCycleConfig,
        selection: RandomCycleSelection,
        initial_metadata: MarketMetadata | None,
        initial_book: OrderBookSnapshot | None,
        initial_source: AccountSnapshot | None,
        initial_receiver: AccountSnapshot | None,
        journal: DurableJournal | None = None,
    ) -> tuple[MarketMetadata, OrderBookSnapshot, AccountSnapshot, AccountSnapshot, RandomCycleSelection]:
        metadata, source, receiver = await self._parallel_revalidation_context(
            config,
            market_read_label="opening revalidation market read",
        )
        book = _as_book(await self._bounded(self._order_book(config.market_id), config, "opening revalidation order book read"), metadata)
        now = self.clock.now()
        _validate_market_book(config, metadata, book, now)
        # The account observations are retained as read, with their original
        # timestamps.  Revalidate their age after the final book read so the
        # late quote cannot make an older account snapshot silently admissible.
        _validate_account_fresh(config, source, "source", now)
        _validate_account_fresh(config, receiver, "receiver", now)
        proposal = select_automatic_prices(
            config.direction, metadata, book, now=now,
            freshness_seconds=config.freshness_seconds,
            price_improvement_ticks=config.price_improvement_ticks,
        )
        # Record both independent budgets before any one leg's margin result
        # can suppress the other. These are quote-currency planning bounds,
        # never observed execution fees or an admission guarantee from venue.
        dispatch_reserve = config.margin_reserve.dispatch_quote if config.margin_reserve else Decimal(0)
        budgets: list[dict[str, Any]] = []
        budget_failures: list[tuple[str, int, Decimal]] = []
        budget_errors: list[BaseException] = []
        for account, label, worst, side in (
            (source, "source", proposal.source_limit_price, config.direction.source_side),
            (receiver, "receiver", proposal.receiver_worst_price,
             "BUY" if config.direction.source_side == "SELL" else "SELL"),
        ):
            target = self._leverage_fractions.get(account.account_index)
            plan = self._opening_plan_evidence.get(label, {})
            preparation_account = initial_source if label == "source" else initial_receiver
            account_baseline_proved = (
                plan.get("account_index") == account.account_index
                and plan.get("account_source_identity") == account.source_identity
                and plan.get("account_observed_at") is not None
            )
            metadata_baseline_proved = (
                plan.get("metadata_market_id") == metadata.market_id
                and plan.get("metadata_symbol") == metadata.symbol
                and plan.get("metadata_observed_at") is not None
            )
            book_baseline_proved = (
                plan.get("book_market_id") == book.market_id
                and plan.get("book_symbol") == book.symbol
                and plan.get("book_observed_at") is not None
            )
            complete_baseline = (account_baseline_proved and metadata_baseline_proved
                                 and book_baseline_proved)
            row: dict[str, Any] = {
                "role": label, "account_index": account.account_index,
                "quantity": format(selection.quantity, "f"),
                "target_imf_bps": target, "observed_imf_bps": None,
                "target_leverage": (None if target is None else format(Decimal(10000) / Decimal(target), "f")),
                "observed_leverage": None,
                "available_balance": None, "mark_price": None if metadata.mark_price is None else format(metadata.mark_price, "f"),
                "worst_price": format(worst, "f"), "fee_rate": None,
                "fee_rate_provenance": None, "fee_rate_candidates": None, "fee_bound": None,
                "mark_notional": None, "initial_margin": None,
                "adverse_entry_loss": None, "total_required": None,
                "headroom": None, "deficit": None, "shortfall_to_dispatch_reserve": None,
                "dispatch_reserve_quote": format(dispatch_reserve, "f"),
                "account_observed_at": account.observed_at,
                "account_age_seconds": max(0.0, now - account.observed_at),
                "metadata_observed_at": metadata.observed_at,
                "metadata_age_seconds": max(0.0, now - metadata.observed_at),
                "initial_account_index": plan.get("account_index") if account_baseline_proved else None,
                "initial_account_source_identity": (plan.get("account_source_identity")
                                                    if account_baseline_proved else None),
                "initial_account_observed_at": (plan.get("account_observed_at")
                                                if account_baseline_proved else None),
                "initial_metadata_market_id": (plan.get("metadata_market_id")
                                               if metadata_baseline_proved else None),
                "initial_metadata_symbol": (plan.get("metadata_symbol")
                                            if metadata_baseline_proved else None),
                "initial_metadata_observed_at": (plan.get("metadata_observed_at")
                                                 if metadata_baseline_proved else None),
                "initial_book_market_id": plan.get("book_market_id") if book_baseline_proved else None,
                "initial_book_symbol": plan.get("book_symbol") if book_baseline_proved else None,
                "initial_book_observed_at": (plan.get("book_observed_at")
                                             if book_baseline_proved else None),
                "initial_plan_provenance": ("COMPLETE" if complete_baseline else "INCOMPLETE_OR_CONFLICTING"),
                "preparation_reference_account_observed_at": (
                    None if preparation_account is None else preparation_account.observed_at),
                "preparation_reference_metadata_observed_at": selection.metadata_observed_at,
                "preparation_reference_book_observed_at": selection.book_observed_at,
                "book_observed_at": book.observed_at,
                "book_age_seconds": max(0.0, now - book.observed_at),
                "initial_to_fresh": {}, "status": "UNKNOWN",
            }
            if target is None:
                row["status"] = "NO_TARGET_IMF"
                budgets.append(row)
                continue
            try:
                row["observed_imf_bps"] = _observed_leverage_fraction(account, label)
                row["observed_leverage"] = format(
                    Decimal(10000) / Decimal(row["observed_imf_bps"]), "f"
                )
                balance = _available_balance(account, label)
                row["available_balance"] = format(balance, "f")
                if account.margin_evidence is not None:
                    parts = _opening_budget_components(metadata, account, label=label,
                        quantity=selection.quantity, worst_price=worst, side=side)
                    fee_cap = ROBINHOOD_MAKER_FEE_CAP if label == "source" else ROBINHOOD_TAKER_FEE_CAP
                    candidates = {"published_cap": fee_cap}
                    observed_rate = metadata.source_fee_rate if label == "source" else metadata.receiver_fee_rate
                    if observed_rate is not None:
                        candidates["market_observed"] = observed_rate
                    if account.fee_rate is not None:
                        candidates["account_observed"] = account.fee_rate
                    row["fee_rate_candidates"] = {name: format(rate, "f")
                                                   for name, rate in candidates.items()}
                    row["fee_rate_provenance"] = [name for name, rate in candidates.items()
                                                   if rate == parts["fee_rate"]]
                else:
                    parts = {"mark_notional": selection.quantity * proposal.source_limit_price,
                             "fee_cost": Decimal(0), "adverse_entry_loss": Decimal(0), "fee_rate": Decimal(0)}
                    row["fee_rate_provenance"] = ["synthetic_no_margin_evidence"]
                initial_margin = parts["mark_notional"] * Decimal(target) / 10000
                required = initial_margin + parts["fee_cost"] + parts["adverse_entry_loss"]
                headroom = balance - required
                row.update({"fee_rate": format(parts["fee_rate"], "f"),
                            "fee_bound": format(parts["fee_cost"], "f"),
                            "mark_notional": format(parts["mark_notional"], "f"),
                            "initial_margin": format(initial_margin, "f"),
                            "adverse_entry_loss": format(parts["adverse_entry_loss"], "f"),
                            "total_required": format(required, "f"),
                            "headroom": format(headroom, "f"),
                            "deficit": format(max(Decimal(0), -headroom), "f"),
                            "shortfall_to_dispatch_reserve": format(max(Decimal(0), dispatch_reserve - headroom), "f"),
                            "status": "ADMITTED" if headroom >= dispatch_reserve else "INSUFFICIENT"})
                for field, current, proved in (
                    ("available_balance", balance, account_baseline_proved),
                    ("mark_price", metadata.mark_price, metadata_baseline_proved),
                    ("worst_price", worst, metadata_baseline_proved and book_baseline_proved),
                    ("required", required, complete_baseline),
                    ("headroom", headroom, complete_baseline),
                    ("fee_rate", parts["fee_rate"], complete_baseline),
                ):
                    before = plan.get(field)
                    if proved and before is not None and current is not None:
                        row["initial_to_fresh"][field] = format(current - before, "f")
                if headroom < dispatch_reserve:
                    budget_failures.append((label, account.account_index, dispatch_reserve - headroom))
            except (PreflightBlocked, ContractError) as exc:
                row["status"] = "UNKNOWN"
                row["calculation_error"] = _cycle_exception_reason(exc)
                budget_errors.append(exc)
            budgets.append(row)
        if journal is not None:
            journal.append("FRESH_OPENING_MARGIN_BUDGET", {
                "observed_at": now, "quantity": format(selection.quantity, "f"),
                "initial_reserve_quote": (None if config.margin_reserve is None
                                          else format(config.margin_reserve.initial_quote, "f")),
                "dispatch_reserve_quote": format(dispatch_reserve, "f"),
                "legs": budgets,
            })
        if not _exclusive_source_price_available(
            book,
            config.direction,
            proposal.source_limit_price,
        ):
            raise _RetryablePreparationFailure(
                "public book does not permit an exclusive improved source price"
            )
        expected_source_position = 0 if initial_source is None else initial_source.signed_position
        expected_receiver_position = 0 if initial_receiver is None else initial_receiver.signed_position
        if source.signed_position != expected_source_position or receiver.signed_position != expected_receiver_position:
            raise PreflightBlocked("account position changed before opening mutation")
        if budget_errors:
            raise budget_errors[0]
        for row in budgets:
            if row["target_imf_bps"] is not None and row["observed_imf_bps"] != row["target_imf_bps"]:
                raise PreflightBlocked(f"{row['role']} leverage changed before opening mutation")
        # Identity is checked before a smaller size can be proposed.
        for current, initial in ((source, initial_source), (receiver, initial_receiver)):
            if initial is not None and current.source_identity != initial.source_identity:
                self._mark_identity_failure("account identity changed before opening mutation")
                raise PreflightBlocked("account identity changed before opening mutation")
        reason = "fresh available balance no longer funds the selected quantity"
        if budget_failures:
            label, account_index, shortfall = budget_failures[0]
            reason = (f"{label} selected quantity exceeds fresh free-balance margin model "
                      f"(account {account_index}; shortfall {format(shortfall, 'f')} quote)")
        try:
            refreshed_bounds = compute_quantity_bounds(metadata, source, receiver, proposal.source_limit_price,
                receiver_bound=proposal.receiver_worst_price, direction=config.direction,
                initial_reserve_quote=dispatch_reserve)
        except PreflightBlocked:
            if budget_failures:
                raise PreflightBlocked(reason) from None
            raise
        if refreshed_bounds.size_step != selection.bounds.size_step:
            raise PreflightBlocked("opening size grid changed before mutation")
        if selection.quantity_tick < refreshed_bounds.lower_tick:
            raise PreflightBlocked("selected quantity no longer meets a fresh venue minimum")
        if budget_failures or selection.quantity_tick > refreshed_bounds.upper_tick:
            # Opening positions are still exactly the initial (flat) baseline
            # here: the position check above runs on every preparation, and a
            # retry after an opening handoff is only admitted for a proved
            # zero-fill.  Quantity may therefore shrink on any opening
            # preparation, but never grow, never redraw and never in closing.
            if not config.confirmed_pilot:
                # Costs are linear in quantity at these exact observed quotes,
                # fee bounds and confirmed IMF. Round down; never raise leverage.
                planning_reserve = max(dispatch_reserve, config.margin_reserve.initial_quote
                                       if config.margin_reserve else dispatch_reserve)
                unit_costs = []
                for row in budgets:
                    if row["total_required"] is None or row["observed_imf_bps"] is None:
                        raise PreflightBlocked(reason)
                    unit_costs.append((Decimal(row["available_balance"]),
                                       Decimal(row["total_required"]) / selection.quantity))

                def affordable_tick(reserve: Decimal) -> int:
                    return min(int(((balance - reserve) / unit / refreshed_bounds.size_step)
                                   .to_integral_value(rounding=ROUND_FLOOR))
                               for balance, unit in unit_costs)

                # The new size must fit the dispatch reserve at this quote;
                # prefer the owner's planning reserve so the next fresh check
                # keeps the same slack as the original leverage plan.
                dispatch_tick = min(selection.quantity_tick, refreshed_bounds.upper_tick,
                                    affordable_tick(dispatch_reserve))
                upper_tick = min(dispatch_tick, max(affordable_tick(planning_reserve),
                                                    refreshed_bounds.lower_tick))
                if refreshed_bounds.lower_tick <= upper_tick < selection.quantity_tick:
                    resized = replace(selection, quantity=upper_tick * refreshed_bounds.size_step,
                                      quantity_tick=upper_tick, bounds=refreshed_bounds)
                    raise _OpeningQuantityRefresh(reason, resized)
            raise PreflightBlocked(reason)
        if config.confirmed_pilot:
            self._pilot_notional_guard(selection.quantity, proposal.source_limit_price,
                                       proposal.receiver_worst_price)
        if initial_source is not None and source.source_identity != initial_source.source_identity:
            self._mark_identity_failure("account identity changed before opening mutation")
            raise PreflightBlocked("account identity changed before opening mutation")
        if initial_receiver is not None and receiver.source_identity != initial_receiver.source_identity:
            self._mark_identity_failure("account identity changed before opening mutation")
            raise PreflightBlocked("account identity changed before opening mutation")
        del initial_metadata, initial_book
        refreshed_selection = replace(
            selection,
            quantity=selection.quantity_tick * refreshed_bounds.size_step,
            opening_source_price=proposal.source_limit_price,
            opening_receiver_bound=proposal.receiver_worst_price,
            bounds=refreshed_bounds,
            metadata_observed_at=metadata.observed_at,
            book_observed_at=book.observed_at,
            best_bid_price=book.bids[0].price,
            best_bid_quantity=book.bids[0].quantity,
            best_ask_price=book.asks[0].price,
            best_ask_quantity=book.asks[0].quantity,
        )
        return metadata, book, source, receiver, refreshed_selection

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
        *,
        attempt_index: int = 1,
        source_quote_observed_at: float | None = None,
    ) -> HandoffConfig:
        child_prefix = config.client_order_prefix
        if attempt_index != 1:
            child_prefix = f"{child_prefix}-{operation_mode.value.lower()}-{attempt_index:03d}"
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
            client_order_prefix=child_prefix,
            journal_path=str(journal_path),
            environment=config.environment,
            operator_execution_opt_in=True,
            operator_plan_reviewed=True,
            api_base_url=config.api_base_url,
            api_key_index=config.api_key_index,
            chain_id=config.chain_id,
            auth_token_lifetime_seconds=config.auth_token_lifetime_seconds,
            operation_mode=operation_mode,
            attempt_index=attempt_index,
            expected_source_position=source_position,
            expected_receiver_position=receiver_position,
            source_quote_observed_at=source_quote_observed_at,
            max_quote_age_seconds=config.max_quote_age_seconds,
            max_source_to_receiver_seconds=config.max_source_to_receiver_seconds,
            receiver_admission=config.receiver_admission,
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
                not opening.mutual_execution_proven
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
            budget = _PairAttemptBudget(limit=1 if config.confirmed_pilot else MAX_CLOSING_ATTEMPTS)
            while budget.available:
                attempt_index = budget.consume()
                preparation_started = time.perf_counter()
                try:
                    try:
                        metadata, source, receiver = await self._parallel_revalidation_context(
                            config,
                            market_read_label="closing market read",
                            warm_closing_transport=True,
                        )
                    except _AccountIdentityFailure:
                        self._mark_identity_failure("closing account identity/read validation failed")
                        raise
                    book = _as_book(
                        await self._bounded(self._order_book(config.market_id), config, "closing order book read"),
                        metadata,
                    )
                    now = self.clock.now()
                    _validate_market_book(config, metadata, book, now)
                    _validate_account_fresh(config, source, "source", now)
                    _validate_account_fresh(config, receiver, "receiver", now)
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
                    proposal = select_automatic_prices(
                        close_direction,
                        metadata,
                        book,
                        quantity=paired_quantity,
                        now=now,
                        freshness_seconds=config.freshness_seconds,
                        price_improvement_ticks=config.price_improvement_ticks,
                    )
                    if config.confirmed_pilot:
                        self._pilot_notional_guard(paired_quantity, proposal.source_limit_price,
                                                   proposal.receiver_worst_price)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    await self._release_preflight_nonces()
                    reason = _cycle_exception_reason(exc)
                    retryable = self._identity_barrier is None and _is_retryable_preparation_error(exc)
                    journal.append("CLOSING_PREPARATION_FAILED", {
                        "attempt": attempt_index, "reason": reason, "retryable": retryable,
                        "latency_seconds": max(0.0, time.perf_counter() - preparation_started),
                        "lineage": {"used": budget.used, "limit": budget.limit},
                    })
                    if not retryable:
                        return blocked(reason)
                    if not budget.available:
                        journal.append("PAIR_ATTEMPT_EXHAUSTED", {
                            "phase": "PAIRED_CLOSING", "attempt": attempt_index,
                            "maximum_attempts": budget.limit, "reason": reason,
                        })
                        return None, f"paired closing shared pair-attempt budget exhausted ({budget.used}/{budget.limit}): {reason}"
                    journal.append("CLOSING_PREPARATION_RETRY", {
                        "attempt": attempt_index, "next_attempt": budget.used + 1,
                        "reason": reason, "delay_seconds": config.poll_interval_seconds,
                        "lineage": {"used": budget.used, "limit": budget.limit},
                    })
                    await self.clock.sleep(config.poll_interval_seconds)
                    continue
                if not _exclusive_source_price_available(
                    book,
                    close_direction,
                    proposal.source_limit_price,
                ):
                    reason = "public book does not permit an exclusive improved closing source price"
                    journal.append(
                        "CLOSING_PREPARATION_FAILED",
                        {
                            "attempt": attempt_index,
                            "reason": reason,
                            "retryable": True,
                            "source_price": format(proposal.source_limit_price, "f"),
                            "book_observed_at": book.observed_at,
                            "lineage": {"used": budget.used, "limit": budget.limit},
                        },
                    )
                    if budget.available:
                        journal.append(
                            "CLOSING_PREPARATION_RETRY",
                            {
                                "attempt": attempt_index,
                                "next_attempt": budget.used + 1,
                                "reason": reason,
                                "delay_seconds": config.poll_interval_seconds,
                                "lineage": {"used": budget.used, "limit": budget.limit},
                            },
                        )
                        await self.clock.sleep(config.poll_interval_seconds)
                        continue
                    exhausted = f"paired closing shared pair-attempt budget exhausted ({budget.used}/{budget.limit})"
                    journal.append(
                        "PAIR_ATTEMPT_EXHAUSTED",
                        {
                            "phase": "PAIRED_CLOSING",
                            "attempt": attempt_index,
                            "maximum_attempts": budget.limit,
                            "reason": reason,
                        },
                    )
                    return None, exhausted
                close_config = self._handoff_config(
                    config,
                    paired_quantity,
                    proposal.source_limit_price,
                    proposal.receiver_worst_price,
                    _child_journal_path(config.closing_journal_path, attempt_index),
                    OperationMode.PAIRED_CLOSING,
                    opening_source.position_after,
                    opening_receiver.position_after,
                    attempt_index=attempt_index,
                    source_quote_observed_at=book.observed_at,
                )
                journal.append(
                    "CLOSING_PLAN_READY",
                    {
                        "config": opening_config_binding(close_config),
                        "http_warmup": dict(getattr(self, "_closing_http_warmup", {})),
                        "latency": {**dict(getattr(self, "_last_quote_read", {})),
                                    "preparation_seconds": max(0.0, time.perf_counter() - preparation_started)},
                        "paired_quantity": format(paired_quantity, "f"),
                        "attempt": attempt_index,
                        "lineage": {"used": budget.used, "limit": budget.limit},
                    },
                )
                closing = await self._run_prepared_handoff(close_config, metadata, source, receiver)
                journal.append(
                    "CLOSING_COMPLETE",
                    {
                        "result": closing.as_dict(),
                        "attempt": attempt_index,
                        "journal_path": close_config.journal_path,
                    },
                )
                if not closing.retryable_pair:
                    return closing, None
                if not budget.available:
                    reason = f"paired closing shared pair-attempt budget exhausted ({budget.used}/{budget.limit})"
                    journal.append(
                        "PAIR_ATTEMPT_EXHAUSTED",
                        {
                            "phase": "PAIRED_CLOSING",
                            "attempt": attempt_index,
                            "maximum_attempts": budget.limit,
                            "reason": reason,
                        },
                    )
                    return closing, reason
                journal.append(
                    "PAIR_ATTEMPT_RETRY",
                    {
                        "phase": "PAIRED_CLOSING",
                        "from_attempt": attempt_index,
                        "next_attempt": budget.used + 1,
                        "reason": closing.reason,
                        "guard": closing.priority_guard,
                        "lineage": {"used": budget.used, "limit": budget.limit},
                        **_stream_settle_payload(closing, config),
                    },
                )
                await _stream_settle(self.clock, closing, config)
            return blocked(f"paired closing shared pair-attempt budget exhausted ({budget.used}/{budget.limit})")
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
        try:
            source, receiver = await self._recovery_accounts(config, journal)
        except _AccountIdentityFailure:
            self._mark_identity_failure("fallback starting account identity/read validation failed")
            journal.append(
                "FALLBACK_RECONCILIATION_UNKNOWN",
                {"reason": self._identity_barrier},
            )
            return [], None, None
        except Exception as exc:
            journal.append(
                "FALLBACK_RECONCILIATION_UNKNOWN",
                {"reason": f"fallback starting account state is unknown: {_cycle_exception_reason(exc)}"},
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
        if closing is None and opening.mutual_execution_proven:
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
        return await self.close_reconciled_positions(config, journal, source, receiver)

    async def close_reconciled_positions(
        self, config: RandomCycleConfig, journal: DurableJournal,
        source: AccountSnapshot, receiver: AccountSnapshot,
    ) -> tuple[list[FallbackResult], Decimal | None, Decimal | None]:
        """Close an explicitly admitted baseline using the existing residual loop.

        The cycle caller proves lineage first. Operator recovery instead proves
        all previous intents terminal and records an explicitly adopted current
        baseline before calling. Neither caller may overlap unresolved writes.
        """
        _validate_account_fresh(config, source, "source", self.clock.now())
        _validate_account_fresh(config, receiver, "receiver", self.clock.now())
        source_residual, receiver_residual = abs(source.signed_position), abs(receiver.signed_position)
        caps = {source.account_index: source_residual, receiver.account_index: receiver_residual}
        results: list[FallbackResult] = []
        current: dict[int, AccountSnapshot] = {
            config.source_account_index: source,
            config.receiver_account_index: receiver,
        }
        signs = {
            config.source_account_index: 1 if source.signed_position >= 0 else -1,
            config.receiver_account_index: 1 if receiver.signed_position >= 0 else -1,
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
        pilot_attempts: dict[int, int] = {}

        async def fresh_accounts() -> tuple[AccountSnapshot, AccountSnapshot] | None:
            """Read both accounts after an attempt before any next mutation."""

            try:
                return await self._recovery_accounts(config, journal)
            except asyncio.CancelledError:
                raise
            except _AccountIdentityFailure:
                self._mark_identity_failure("fallback account identity/read validation failed")
                journal.append(
                    "FALLBACK_RECONCILIATION_UNKNOWN",
                    {"reason": self._identity_barrier},
                )
                return None
            except Exception as exc:
                journal.append(
                    "FALLBACK_RECONCILIATION_UNKNOWN",
                    {"reason": f"fallback account state is unknown: {_cycle_exception_reason(exc)}"},
                )
                return None

        while pending:
            account_index = pending.pop(0)
            if account_index in blocked:
                continue
            before = current[account_index]
            residual = _position_residual(before.signed_position, signs[account_index], caps[account_index])
            if residual is None:
                journal.append(
                    "FALLBACK_STOPPED_STATE_CHANGED",
                    {"account_index": account_index, "reason": "cycle residual changed direction or exceeded the selected quantity"},
                )
                return results, current[config.source_account_index].signed_position, current[config.receiver_account_index].signed_position
            if residual <= 0:
                continue
            if config.confirmed_pilot and pilot_attempts.get(account_index, 0) >= 1:
                journal.append("PILOT_RECOVERY_LIMIT", {
                    "account_index": account_index,
                    "remaining_position": format(before.signed_position, "f"),
                    "reason": "one exact reduce-only recovery attempt per account exhausted",
                })
                blocked.add(account_index)
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
            if config.confirmed_pilot:
                pilot_attempts[account_index] = pilot_attempts.get(account_index, 0) + 1
            reserve = getattr(self.client, "reserve_order_nonce", None)
            invalidate = getattr(self.client, "invalidate_reserved_nonce", None)
            prepared_capable = (callable(reserve) and callable(invalidate)
                               and callable(getattr(self.client, "prepare_order", None))
                               and callable(getattr(self.client, "submit_prepared_order", None))
                               and callable(getattr(self.client, "invalidate_prepared_order", None)))
            reserved_nonce = None
            result = None
            if prepared_capable:
                try:
                    reserved_nonce = await self._bounded(
                        reserve(account_index, deadline=time.monotonic() + config.freshness_seconds),
                        config, "fallback pre-quote nonce reservation",
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    reason = f"fallback pre-quote nonce reservation failed: {sanitize_exception(exc)}"
                    journal.append("FALLBACK_PREPARATION_FAILED", {
                        "account_index": account_index, "attempt": attempt_ordinal,
                        "reason": reason, "sent": False,
                    })
                    result = FallbackResult(
                        account_index, "SELL" if before.signed_position > 0 else "BUY",
                        residual, False, Outcome.PARTIAL, reason=reason,
                        attempt=attempt_ordinal,
                    )
            if result is None:
                try:
                    result = await self._fallback_one(
                        config, journal, before, residual,
                        attempt_ordinal=attempt_ordinal,
                        reserved_nonce=reserved_nonce,
                    )
                finally:
                    if reserved_nonce is not None:
                        await invalidate(reserved_nonce)
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
            journal.append(
                "FALLBACK_POST_ATTEMPT_ACCOUNT_OBSERVATION",
                {
                    "attempt": attempt_ordinal,
                    "account_index": account_index,
                    "reconciliation_state": result.reconciliation_state,
                    "source": _account_payload(fresh_source),
                    "receiver": _account_payload(fresh_receiver),
                },
            )
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
            if (
                result.reconciliation_state == "REJECTED"
                and result.position_after is None
            ):
                # A rejected dispatch has no order-side position_after, but
                # the mandatory post-attempt account read is still a distinct
                # and useful fact.  Bind it to the result without making the
                # rejected account eligible for another mutation.
                result = replace(
                    result,
                    position_after=fresh[account_index].signed_position,
                    position_observed_at=fresh[account_index].observed_at,
                )
                results[-1] = result
            current = fresh
            fresh_residuals = {
                candidate: _position_residual(snapshot.signed_position, signs[candidate], caps[candidate])
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
            if result.reconciliation_state == "REJECTED" or result.position_after is None:
                blocked.add(account_index)
                continue
            if result.filled_quantity == 0:
                ready_at[account_index] = _clock_monotonic(self.clock) + config.poll_interval_seconds
            else:
                ready_at[account_index] = _clock_monotonic(self.clock)
            if account_index not in pending:
                pending.append(account_index)

        try:
            final_source, final_receiver = await self._recovery_accounts(config, journal)
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
        except _AccountIdentityFailure:
            self._mark_identity_failure("final fallback account identity/read validation failed")
            journal.append(
                "FALLBACK_RECONCILIATION_UNKNOWN",
                {"reason": self._identity_barrier},
            )
            return results, None, None
        except Exception as exc:
            journal.append(
                "FALLBACK_RECONCILIATION_UNKNOWN",
                {"reason": f"fallback final account state is unknown: {_cycle_exception_reason(exc)}"},
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

        def record(
            *,
            poll: int,
            order: OrderSnapshot | None,
            reason: str | None = None,
            mismatch_map: Mapping[str, Any] | None = None,
            rejected: bool = False,
        ) -> None:
            payload: dict[str, Any] = {
                "account_index": plan.account_index,
                "market_id": plan.market_id,
                "attempt": attempt_ordinal,
                "poll": poll,
                "order": _order_payload(order),
            }
            if reason is not None:
                payload["reason"] = reason
            if mismatch_map:
                payload["mismatch_map"] = dict(mismatch_map)
            if rejected:
                payload["rejected"] = True
            journal.append("FALLBACK_ORDER_OBSERVATION", payload)
            if rejected:
                journal.append("FALLBACK_ORDER_OBSERVATION_REJECTED", payload)

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
                record(poll=poll, order=None, reason=last_reason)
                order = None
            if order is not None:
                now = self.clock.now()
                if receipt.order_id is not None and order.order_id != str(receipt.order_id):
                    mismatch_map = _fallback_order_mismatch_map(
                        order,
                        plan,
                        expected_order_id=str(receipt.order_id),
                    )
                    reason = "fallback order identifier conflicts with the dispatched receipt"
                    record(
                        poll=poll,
                        order=order,
                        reason=reason,
                        mismatch_map=mismatch_map or {
                            "order_id": {
                                "expected": str(receipt.order_id),
                                "observed": order.order_id,
                            }
                        },
                        rejected=True,
                    )
                    return None, reason
                mismatch_map = _fallback_order_mismatch_map(
                    order,
                    plan,
                    expected_order_id=(None if receipt.order_id is None else str(receipt.order_id)),
                )
                if mismatch_map:
                    reason = "fallback order identity or parameters conflict with the plan"
                    record(
                        poll=poll,
                        order=order,
                        reason=reason,
                        mismatch_map=mismatch_map,
                        rejected=True,
                    )
                    return None, reason
                if order.observed_at > now:
                    last_reason = "fallback order observation is from the future"
                elif now - order.observed_at > config.freshness_seconds:
                    last_reason = "fallback order observation is stale"
                elif not order.terminal:
                    last_reason = "fallback order is not terminal"
                else:
                    record(poll=poll, order=order)
                    return order, None
                record(poll=poll, order=order, reason=last_reason)
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
        """Read bounded pages/refreshes until receipts explain the terminal fill.

        An empty complete page can precede publication of a filled order's
        trades. Refresh reads only; never resubmit the closing order.
        """

        trades: list[TradeReceipt] = []
        pages: list[Any] = []
        seen_trade_ids: set[str] = set()
        observed_trades: dict[str, TradeReceipt] = {}
        pending_future_receipts: set[TradeReceipt] = set()
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
                    if trade.side.upper() != plan.side:
                        raise ContractError("fallback trade side conflicts with the plan")
                    if not (
                        trade.price <= plan.price
                        if plan.side == "BUY"
                        else trade.price >= plan.price
                    ):
                        raise ContractError("fallback trade violates the executable price bound")
                    if trade.observed_at > now:
                        pending_future_receipts.add(trade)
                        continue
                    pending_future_receipts.discard(trade)
                    previous = observed_trades.get(trade.trade_id)
                    if previous is not None:
                        if replace(trade, observed_at=previous.observed_at) != previous:
                            raise ContractError("fallback refreshed trade has conflicting receipt fields")
                    else:
                        observed_trades[trade.trade_id] = trade
                        trades.append(trade)
            except Exception as exc:
                return trades, pages, f"fallback trade reconciliation is unknown: {_cycle_exception_reason(exc)}"
            if not page.next_cursor:
                if not page.complete:
                    return trades, pages, "fallback trade history is incomplete"
                total = sum((trade.quantity for trade in trades), Decimal(0))
                if total == order.filled_quantity and not pending_future_receipts:
                    return trades, pages, None
                if total > order.filled_quantity:
                    return trades, pages, "fallback trade receipt sum exceeds terminal fill"
                remaining = deadline - self.clock.now()
                if page_number == config.max_poll_count or remaining <= 0:
                    if pending_future_receipts:
                        return trades, pages, "fallback trade receipt is from the future or lacks an exact valid reread"
                    return trades, pages, "fallback terminal fill history did not converge within configured bounds"
                await self.clock.sleep(min(config.poll_interval_seconds, remaining))
                cursor = None
                seen_trade_ids.clear()
                continue
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
        reserved_nonce: Any | None = None,
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
        preparation_timings: dict[str, float] = {}

        def finish(result: FallbackResult, *, reconciliation_event: str | None = None) -> FallbackResult:
            state = _fallback_reconciliation_state(
                result,
                receipt=receipt,
                order=order,
                trades=trades,
                after=after,
                requested_quantity=residual,
            )
            reconciled_result = replace(result, reconciliation_state=state)
            economic_status, economic_findings, fee_total = _fallback_economic_evidence(
                reconciled_result,
                receipt=receipt,
                order=order,
                trades=trades,
                history_complete=history_complete,
            )
            result = replace(
                reconciled_result,
                position_observed_at=(
                    reconciled_result.position_observed_at
                    if reconciled_result.position_observed_at is not None or after is None
                    else after.observed_at
                ),
                economic_status=economic_status,
                economic_findings=economic_findings,
                fee_total=fee_total,
            )
            evidence = {
                "account_index": initial_before.account_index,
                "attempt": attempt_ordinal,
                "requested_quantity": format(residual, "f"),
                "outcome": result.outcome.value,
                "reconciliation_state": result.reconciliation_state,
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
                "economic_status": result.economic_status,
                "economic_findings": list(result.economic_findings),
                "fee_total": None if result.fee_total is None else format(result.fee_total, "f"),
                "market_metadata": None if metadata is None else _metadata_payload(metadata),
                "order_book_observed_at": None if book is None else book.observed_at,
                "position_observed_at": result.position_observed_at,
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
            metadata_read_started = time.perf_counter()
            metadata = _as_market(
                await self._bounded(
                    self.client.market_metadata(config.market_id),
                    config,
                    "fallback market read",
                )
            )
            preparation_timings["metadata_read_seconds"] = time.perf_counter() - metadata_read_started
            book_read_started = time.perf_counter()
            book = _as_book(
                await self._bounded(self._order_book(config.market_id), config, "fallback order book read"),
                metadata,
            )
            preparation_timings["book_read_seconds"] = time.perf_counter() - book_read_started
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
            account_read_started = time.perf_counter()
            current = await self._recovery_read(
                config, lambda: self._read_account(config, before.account_index, "fallback account recheck"),
                "fallback account recheck", journal,
            )
            preparation_timings["account_recheck_seconds"] = time.perf_counter() - account_read_started
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
            _validate_market_book(config, metadata, book, self.clock.now())
            if residual != abs(before.signed_position):
                raise PreflightBlocked("fallback quantity differs from the exact confirmed residual")
        except asyncio.CancelledError:
            raise
        except _AccountIdentityFailure:
            self._mark_identity_failure("fallback account identity/read validation failed before mutation")
            return finish(
                FallbackResult(
                    initial_before.account_index,
                    side,
                    residual,
                    False,
                    Outcome.UNKNOWN,
                    reason="fallback account identity/read validation failed before mutation",
                    attempt=attempt_ordinal,
                )
            )
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
        if config.confirmed_pilot and residual * bound > Decimal("40.00"):
            return finish(FallbackResult(
                before.account_index, side, residual, False, Outcome.PARTIAL,
                reason="confirmed pilot residual exceeds 40.00 quote per account",
                attempt=attempt_ordinal,
            ))
        below_minimum = (
            residual < metadata.minimum_base_amount
            or residual * bound < metadata.minimum_quote_amount
        )
        # HCR-39: actual Robinhood BTC full-residual MARKET/IOC reduce-only
        # orders 844424841898572 (.00004) and 844424841891552 (.00007)
        # filled below both catalog minima. This is observed venue behavior,
        # not an opening exemption or authority for any other market.
        residual_minimum_exception = (
            config.api_base_url.rstrip("/") == OFFICIAL_ROBINHOOD_API_URL.rstrip("/")
            and config.chain_id == OFFICIAL_ROBINHOOD_CHAIN_ID
            and config.market_id == metadata.market_id == 1
            and config.market_symbol == metadata.symbol == "BTC"
        )
        if below_minimum and not residual_minimum_exception:
            return finish(
                FallbackResult(
                    before.account_index,
                    side,
                    residual,
                    False,
                    Outcome.PARTIAL,
                    reason=("confirmed residual is below a documented venue minimum: "
                            f"quantity={residual}, minimum_base={metadata.minimum_base_amount}, "
                            f"notional={residual * bound}, minimum_quote={metadata.minimum_quote_amount}"),
                    attempt=attempt_ordinal,
                )
            )
        if below_minimum:
            journal.append("FALLBACK_REDUCE_ONLY_MINIMUM_EXCEPTION", {
                "account_index": before.account_index, "market_id": metadata.market_id,
                "quantity": str(residual), "notional": str(residual * bound),
                "minimum_base_amount": str(metadata.minimum_base_amount),
                "minimum_quote_amount": str(metadata.minimum_quote_amount),
                "rule": "robinhood_btc_full_residual_reduce_only_market_ioc",
                "attempt": attempt_ordinal,
            })
        price_int = decimal_to_integer(bound, metadata.price_decimals, "fallback executable price")
        # The SDK must inherit the remaining lifetime of every observation
        # used for this close.  A fresh deadline created inside submit_order
        # would allow nonce acquisition/signing to outlive the book quote.
        barrier_now = self.clock.now()
        if any(observed_at > barrier_now for observed_at in
               (metadata.observed_at, book.observed_at, before.observed_at)):
            return finish(FallbackResult(
                before.account_index, side, residual, False, Outcome.PARTIAL,
                reason="fallback evidence is from the future before mutation",
                attempt=attempt_ordinal,
            ))
        evidence_remaining = min(
            config.freshness_seconds - (barrier_now - observed_at)
            for observed_at in (metadata.observed_at, book.observed_at, before.observed_at)
        )
        if evidence_remaining <= 0:
            return finish(
                FallbackResult(
                    before.account_index, side, residual, False, Outcome.PARTIAL,
                    reason="fallback market/account evidence expired before mutation",
                    attempt=attempt_ordinal,
                )
            )
        mutation_deadline = time.monotonic() + min(
            evidence_remaining, config.request_timeout_seconds,
        )
        if reserved_nonce is not None:
            mutation_deadline = min(mutation_deadline, reserved_nonce.deadline)
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
            mutation_deadline_monotonic=mutation_deadline,
        )
        journal.append("FALLBACK_SEND_BARRIER", {
            "account_index": before.account_index,
            "attempt": attempt_ordinal,
            "market_metadata_age_seconds": barrier_now - metadata.observed_at,
            "book_age_seconds": barrier_now - book.observed_at,
            "account_age_seconds": barrier_now - before.observed_at,
            "evidence_remaining_seconds": evidence_remaining,
            "send_deadline_budget_seconds": min(evidence_remaining, config.request_timeout_seconds),
            "preparation_timings": preparation_timings,
        })
        prepared = None
        if reserved_nonce is not None:
            try:
                prepared = await self._bounded(
                    self.client.prepare_order(plan, reserved_nonce=reserved_nonce),
                    config, "fallback order preparation",
                )
                if time.monotonic() >= mutation_deadline:
                    raise TimeoutError("fallback order preparation crossed the final mutation barrier")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                reason = f"fallback order preparation failed before send: {sanitize_exception(exc)}"
                journal.append("FALLBACK_PREPARATION_FAILED", {
                    "account_index": before.account_index, "attempt": attempt_ordinal,
                    "reason": reason, "sent": False, "preparation_timings": preparation_timings,
                })
                return finish(FallbackResult(
                    before.account_index, side, residual, False, Outcome.PARTIAL,
                    reason=reason, attempt=attempt_ordinal,
                ))
            finally:
                if prepared is not None and time.monotonic() >= mutation_deadline:
                    await self.client.invalidate_prepared_order(prepared)
        if prepared is not None:
            preparation_timings.update({
                key: value for key, value in prepared.diagnostic_timings.items()
                if key in {"preparation_lock_wait_seconds", "nonce_acquisition_seconds",
                           "signing_call_seconds", "nonce_reserved_before_quote"}
                and isinstance(value, (int, float)) and math.isfinite(value)
            })
        def refresh_transport_timing() -> None:
            if prepared is None:
                return
            value = prepared.diagnostic_timings.get("transport_roundtrip_seconds")
            if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0:
                preparation_timings["transport_roundtrip_seconds"] = value

        try:
            if prepared is not None:
                journal.append("FALLBACK_PREPARATION_TIMINGS", {
                    "account_index": before.account_index, "attempt": attempt_ordinal,
                    "timings": preparation_timings,
                })
                journal.append("FALLBACK_PREPARED_SEND_BARRIER", {
                    "account_index": before.account_index, "attempt": attempt_ordinal,
                    "book_age_seconds": self.clock.now() - book.observed_at,
                    "account_age_seconds": self.clock.now() - before.observed_at,
                    "deadline_remaining_seconds": mutation_deadline - time.monotonic(),
                })
            fallback_intent = journal.append(
                "FALLBACK_DISPATCH_INTENT",
                {
                    "plan": plan.as_dict(),
                    "account_index": before.account_index,
                    "attempt": attempt_ordinal,
                },
            )
        except BaseException:
            if prepared is not None:
                await self.client.invalidate_prepared_order(prepared)
            raise
        try:
            receipt = _as_receipt(
                await self._bounded(
                    self.client.submit_prepared_order(plan, prepared, deadline=mutation_deadline)
                    if prepared is not None else self.client.submit_order(plan),
                    config, "fallback mutation",
                )
            )
        except asyncio.CancelledError:
            refresh_transport_timing()
            reason = "cancellation after fallback intent"
            journal.append(
                "FALLBACK_DISPATCH_UNKNOWN",
                {"account_index": before.account_index, "attempt": attempt_ordinal,
                 "reason": reason, "preparation_timings": preparation_timings},
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
            refresh_transport_timing()
            reason = f"fallback dispatch outcome unknown: {sanitize_exception(exc)}"
            journal.append(
                "FALLBACK_DISPATCH_UNKNOWN",
                {"account_index": before.account_index, "attempt": attempt_ordinal,
                 "reason": reason, "preparation_timings": preparation_timings},
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
        refresh_transport_timing()
        journal.append(
            "FALLBACK_DISPATCH_RESULT",
            {
                "account_index": before.account_index,
                "attempt": attempt_ordinal,
                "accepted": receipt.accepted,
                "order_id": receipt.order_id,
                "receipt": _receipt_payload(receipt),
                "quote_age_at_response_seconds": self.clock.now() - book.observed_at,
                "intent_to_response_seconds": self.clock.now() - fallback_intent.at,
                "preparation_timings": preparation_timings,
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
                    reconciliation_state="REJECTED",
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
            after = await self._recovery_read(
                config, lambda: self._read_account(config, before.account_index, "fallback final account read"),
                "fallback final account read", journal,
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
        except _AccountIdentityFailure:
            self._mark_identity_failure("fallback final account identity/read validation failed after mutation")
            return finish(
                FallbackResult(
                    before.account_index,
                    side,
                    residual,
                    True,
                    Outcome.UNKNOWN,
                    order_id=order.order_id,
                    filled_quantity=sum((item.quantity for item in trades), Decimal(0)),
                    reason="fallback final account identity/read validation failed after mutation",
                    attempt=attempt_ordinal,
                ),
                reconciliation_event="FALLBACK_RECONCILIATION_UNKNOWN",
            )
        except Exception as exc:
            reason = f"fallback final position is unknown: {_cycle_exception_reason(exc)}"
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
        # The account projection can lag the terminal order and its receipts.
        # Refresh only the same bound account; never send another close while
        # the causal position is unresolved.
        position_deadline = self.clock.now() + config.reconcile_timeout_seconds
        for _ in range(1, config.max_poll_count):
            if after.signed_position == expected_after or after.active_orders:
                break
            remaining = position_deadline - self.clock.now()
            if remaining <= 0:
                break
            try:
                await self.clock.sleep(min(config.poll_interval_seconds, remaining))
                refreshed = await self._read_account(config, before.account_index, "fallback position propagation read")
                _validate_account_fresh(
                    config, refreshed,
                    "source" if before.account_index == config.source_account_index else "receiver",
                    self.clock.now(),
                )
                if refreshed.source_identity != before.source_identity:
                    raise _AccountIdentityFailure("fallback refreshed account identity changed")
                after = refreshed
            except asyncio.CancelledError:
                finish(FallbackResult(
                    before.account_index, side, residual, True, Outcome.UNKNOWN,
                    order_id=order.order_id, filled_quantity=filled,
                    reason="fallback position propagation was interrupted",
                    attempt=attempt_ordinal,
                ), reconciliation_event="FALLBACK_RECONCILIATION_UNKNOWN")
                raise
            except Exception as exc:
                # Preserve the error instead of treating the preceding stale
                # account projection as a successful close.
                if isinstance(exc, _AccountIdentityFailure):
                    self._mark_identity_failure("fallback refreshed account identity/read validation failed")
                return finish(FallbackResult(
                    before.account_index, side, residual, True, Outcome.UNKNOWN,
                    order_id=order.order_id, filled_quantity=filled,
                    reason=f"fallback position propagation unresolved: {_cycle_exception_reason(exc)}",
                    attempt=attempt_ordinal,
                ), reconciliation_event="FALLBACK_RECONCILIATION_UNKNOWN")
        if order.filled_quantity != filled or after.signed_position != expected_after or after.active_orders:
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
        if outcome is Outcome.SUCCESS:
            reconciliation_state = "FULL_FILL"
            reason = None
        elif filled == 0 and not trades and order.terminal:
            reconciliation_state = "TERMINAL_ZERO_FILL"
            reason = (
                f"fallback terminal zero-fill/cancel ({order.status}) left a confirmed residual position"
            )
        else:
            reconciliation_state = "PARTIAL_FILL"
            reason = "fallback partial fill left a confirmed residual position"
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
                reconciliation_state=reconciliation_state,
                position_observed_at=after.observed_at,
            ),
            reconciliation_event="FALLBACK_RECONCILED",
        )

    @staticmethod
    def _fallback_order_identity_matches(order: OrderSnapshot, plan: OrderPlan) -> bool:
        """Match immutable order fields while a terminal state is pending."""

        return (
            bool(order.order_id)
            and not _fallback_order_mismatch_map(order, plan)
        )

    @staticmethod
    def _fallback_order_matches(order: OrderSnapshot, plan: OrderPlan) -> bool:
        return RandomCycleEngine._fallback_order_identity_matches(order, plan) and order.terminal

    async def _order_book(self, market_id: int) -> OrderBookSnapshot | Mapping[str, Any]:
        method = getattr(self.client, "price_book", None)
        if not callable(method):
            method = getattr(self.client, "order_book", None)
        if not callable(method):
            method = getattr(self.client, "order_book_snapshot", None)
        if not callable(method):
            method = getattr(self.client, "public_order_book", None)
        if not callable(method):
            raise ContractError("cycle client does not expose an order-book reader")
        started_at = self.clock.now()
        started = time.perf_counter()
        result = await method(market_id)
        self._last_quote_read = {
            "quote_read_seconds": time.perf_counter() - started,
            "quote_read_started_at": started_at,
            "quote_read_finished_at": self.clock.now(),
        }
        return result

    async def _bounded(self, awaitable: Any, config: RandomCycleConfig, label: str) -> Any:
        try:
            return await asyncio.wait_for(awaitable, timeout=config.request_timeout_seconds)
        except asyncio.TimeoutError as exc:
            raise TimeoutError(f"{label} exceeded configured request timeout") from exc

    def _stop_observed(self) -> bool:
        if self._stop_requested is None:
            return False
        try:
            return bool(self._stop_requested())
        except Exception:
            return False

    def _owner_stop(self, journal: DurableJournal, stage: str) -> bool:
        """Observe an owner /stop at a safe point; journal the first observation."""
        requested = self._stop_observed()
        if requested and not self._owner_stop_journaled:
            self._owner_stop_journaled = True
            journal.append("OWNER_STOP_OBSERVED", {"stage": stage})
        return requested

    def _owner_stopped_before_orders(
        self, config: RandomCycleConfig, journal: DurableJournal, selection: RandomCycleSelection,
    ) -> RandomCycleResult:
        result = _with_cycle_classifications(RandomCycleResult(
            outcome=Outcome.FAILED_PREFLIGHT_BLOCKED,
            phase=Phase.PREFLIGHT,
            run_id=journal.run_id,
            selection=selection,
            reason=OWNER_STOP_REASON,
            journal_path=str(config.journal_path),
        ))
        journal.append("CYCLE_PREFLIGHT_BLOCKED", result.as_dict())
        journal.append("CYCLE_COMPLETE", result.as_dict())
        return result

    async def _wait_hold(self, hold_seconds: int, anchor_monotonic: float) -> float | None:
        """Wait for the persisted deadline.  With an owner stop hook the wait
        polls it and returns the seconds held when a stop is observed."""
        deadline = anchor_monotonic + hold_seconds
        previous = anchor_monotonic
        stalled = 0
        stoppable = self._stop_requested is not None
        while True:
            current = _clock_monotonic(self.clock)
            if stoppable and self._stop_observed():
                return max(0.0, current - anchor_monotonic)
            remaining = deadline - current
            if remaining <= 0:
                return None
            await self.clock.sleep(min(remaining, OWNER_STOP_HOLD_POLL_SECONDS if stoppable else 60.0))
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
        "attempt_index": config.attempt_index,
        "client_order_prefix": config.client_order_prefix,
        "source_quote_observed_at": config.source_quote_observed_at,
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
    stop_requested: Callable[[], bool] | None = None,
) -> RandomCycleResult:
    return await RandomCycleEngine(client, clock=clock, rng=rng, stop_requested=stop_requested).execute(config)


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
    "terminal_cycle_facts",
]
