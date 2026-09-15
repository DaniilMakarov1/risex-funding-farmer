"""Owner-local launcher for one bounded HCR-9 paired-opening attempt.

This module is deliberately a thin boundary around the existing HCR-1
contracts, SDK adapters, journal, and engine.  It owns operator input and the
small fixed diagnostic packet; it does not implement a second execution
state machine, retry loop, or storage service.

The default factories are only called after the operator has entered the
complete plan and explicitly typed ``LAUNCH``.  Tests inject synthetic
factories and therefore never import a live SDK module or make a request.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import hashlib
import inspect
import json
import math
import os
from pathlib import Path
from typing import Any, Awaitable, Callable, Mapping, Protocol, Sequence

from .contracts import (
    ContractError,
    Direction,
    HandoffConfig,
    HandoffResult,
    MarketMetadata,
    OperationMode,
    Outcome,
    OFFICIAL_ROBINHOOD_API_URL,
    OFFICIAL_ROBINHOOD_CHAIN_ID,
)
from .engine import HandoffClient, Clock, run_handoff
from .journal import sanitize, sanitize_exception
from .readiness import ReadinessConfig, ReadinessMarketMetadata, ReadOnlyLighterSdkClient
from .sdk import LighterSdkClient, REQUIRED_LIGHTER_SDK_VERSION, SecretProvider


PACKET_VERSION = 1
PACKET_NAME = "attempt-packet.json"
TERMINAL_RESULT_NAME = "terminal-result.json"
EXIT_STATUS_NAME = "exit-status.json"
JOURNAL_NAME = "intent.jsonl"
CLAIM_NAME = ".attempt.claim"
AUTH_TOKEN_LIFETIME_SECONDS = 600
LAUNCH_TOKEN = "LAUNCH"


class LocalAttemptInputError(ValueError):
    """An operator value is missing, malformed, or outside the fixed contract."""


class AttemptDirectoryError(ValueError):
    """The selected attempt directory cannot safely hold a new attempt."""


class LocalAttemptCancelled(Exception):
    """The operator declined the explicit launch prompt."""


InputFn = Callable[[str], str]
OutputFn = Callable[[str], Any]


def _required_text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise LocalAttemptInputError(f"{name} is required")
    return value.strip()


def _positive_decimal(value: Any, name: str) -> Decimal:
    if isinstance(value, bool) or isinstance(value, float):
        raise LocalAttemptInputError(f"{name} must be an exact positive decimal string")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value).strip())
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise LocalAttemptInputError(f"{name} must be an exact positive decimal string") from exc
    if not parsed.is_finite() or parsed <= 0:
        raise LocalAttemptInputError(f"{name} must be a finite positive decimal")
    return parsed


def _positive_time(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise LocalAttemptInputError(f"{name} must be a finite positive number")
    try:
        parsed = float(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise LocalAttemptInputError(f"{name} must be a finite positive number") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise LocalAttemptInputError(f"{name} must be a finite positive number")
    return parsed


def _positive_integer(value: Any, name: str, *, minimum: int = 1) -> int:
    if isinstance(value, bool):
        raise LocalAttemptInputError(f"{name} must be an integer")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if not text or text.startswith(("-", "+")) and not text[1:].isdigit() or not text.lstrip("+").isdigit():
            raise LocalAttemptInputError(f"{name} must be an integer")
        parsed = int(text)
    else:
        raise LocalAttemptInputError(f"{name} must be an integer")
    if parsed < minimum:
        raise LocalAttemptInputError(f"{name} must be at least {minimum}")
    return parsed


def _account_index(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise LocalAttemptInputError(f"{name} must be a non-negative integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise LocalAttemptInputError(f"{name} must be a non-negative integer") from exc
    if isinstance(value, float) and not value.is_integer():
        raise LocalAttemptInputError(f"{name} must be a non-negative integer")
    if parsed < 0:
        raise LocalAttemptInputError(f"{name} must be a non-negative integer")
    return parsed


def _prompt(input_fn: InputFn, prompt: str, name: str) -> str:
    try:
        value = input_fn(prompt)
    except (EOFError, KeyboardInterrupt) as exc:
        raise LocalAttemptInputError(f"{name} input was cancelled") from exc
    if not isinstance(value, str) or not value.strip():
        raise LocalAttemptInputError(f"{name} is required")
    return value.strip()


def _value_or_prompt(value: Any, input_fn: InputFn, prompt: str, name: str) -> Any:
    return value if value is not None else _prompt(input_fn, prompt, name)


@dataclass(frozen=True, slots=True)
class LocalAttemptInputs:
    """All operator-selected values required before a fresh observation."""

    market_symbol: str
    quantity: Decimal
    direction: Direction
    source_account_index: int
    receiver_account_index: int
    api_key_index: int
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
    attempt_dir: Path
    defer_incremental_margin_calculation: bool = False
    auth_token_lifetime_seconds: int = AUTH_TOKEN_LIFETIME_SECONDS

    def __post_init__(self) -> None:
        symbol = _required_text(self.market_symbol, "market_symbol").upper()
        object.__setattr__(self, "market_symbol", symbol)
        if not isinstance(self.direction, Direction):
            try:
                object.__setattr__(self, "direction", Direction(str(self.direction).upper()))
            except (TypeError, ValueError) as exc:
                raise LocalAttemptInputError("direction must be LONG or SHORT") from exc
        object.__setattr__(self, "quantity", _positive_decimal(self.quantity, "quantity"))
        source = _account_index(self.source_account_index, "source_account_index")
        receiver = _account_index(self.receiver_account_index, "receiver_account_index")
        if source == receiver:
            raise LocalAttemptInputError("source and receiver account indices must differ")
        object.__setattr__(self, "source_account_index", source)
        object.__setattr__(self, "receiver_account_index", receiver)
        if isinstance(self.api_key_index, bool) or not isinstance(self.api_key_index, int) or not 4 <= self.api_key_index <= 254:
            raise LocalAttemptInputError("api_key_index must be an integer in 4..254")
        object.__setattr__(self, "source_limit_price", _positive_decimal(self.source_limit_price, "source_limit_price"))
        object.__setattr__(self, "receiver_worst_price", _positive_decimal(self.receiver_worst_price, "receiver_worst_price"))
        for name in (
            "freshness_seconds",
            "request_timeout_seconds",
            "order_timeout_seconds",
            "reconcile_timeout_seconds",
            "poll_interval_seconds",
        ):
            object.__setattr__(self, name, _positive_time(getattr(self, name), name))
        object.__setattr__(self, "max_poll_count", _positive_integer(self.max_poll_count, "max_poll_count"))
        object.__setattr__(self, "source_order_lifetime_seconds", _positive_integer(self.source_order_lifetime_seconds, "source_order_lifetime_seconds", minimum=300))
        prefix = _required_text(self.client_order_prefix, "client_order_prefix")
        object.__setattr__(self, "client_order_prefix", prefix)
        path = Path(self.attempt_dir)
        if not str(path).strip():
            raise LocalAttemptInputError("attempt_dir is required")
        object.__setattr__(self, "attempt_dir", path)
        if not isinstance(self.defer_incremental_margin_calculation, bool):
            raise LocalAttemptInputError("defer_incremental_margin_calculation must be bool")
        object.__setattr__(self, "auth_token_lifetime_seconds", _positive_integer(self.auth_token_lifetime_seconds, "auth_token_lifetime_seconds", minimum=60))

    @property
    def journal_path(self) -> Path:
        return self.attempt_dir / JOURNAL_NAME

    @property
    def packet_path(self) -> Path:
        return self.attempt_dir / PACKET_NAME

    @property
    def terminal_result_path(self) -> Path:
        return self.attempt_dir / TERMINAL_RESULT_NAME

    @property
    def exit_status_path(self) -> Path:
        return self.attempt_dir / EXIT_STATUS_NAME

    def readiness_config(self) -> ReadinessConfig:
        return ReadinessConfig(
            market_symbol=self.market_symbol,
            quantity=self.quantity,
            direction=self.direction,
            source_account_index=self.source_account_index,
            receiver_account_index=self.receiver_account_index,
            api_key_index=self.api_key_index,
            freshness_seconds=self.freshness_seconds,
            request_timeout_seconds=self.request_timeout_seconds,
            source_limit_price=self.source_limit_price,
            receiver_worst_price=self.receiver_worst_price,
            api_base_url=OFFICIAL_ROBINHOOD_API_URL,
            chain_id=OFFICIAL_ROBINHOOD_CHAIN_ID,
            auth_token_lifetime_seconds=self.auth_token_lifetime_seconds,
        )

    def handoff_config(self, market_id: int) -> HandoffConfig:
        return HandoffConfig(
            market_id=market_id,
            market_symbol=self.market_symbol,
            environment="robinhood",
            direction=self.direction,
            quantity=self.quantity,
            source_limit_price=self.source_limit_price,
            receiver_worst_price=self.receiver_worst_price,
            freshness_seconds=self.freshness_seconds,
            request_timeout_seconds=self.request_timeout_seconds,
            order_timeout_seconds=self.order_timeout_seconds,
            reconcile_timeout_seconds=self.reconcile_timeout_seconds,
            poll_interval_seconds=self.poll_interval_seconds,
            max_poll_count=self.max_poll_count,
            source_order_lifetime_seconds=self.source_order_lifetime_seconds,
            client_order_prefix=self.client_order_prefix,
            journal_path=str(self.journal_path),
            api_base_url=OFFICIAL_ROBINHOOD_API_URL,
            api_key_index=self.api_key_index,
            chain_id=OFFICIAL_ROBINHOOD_CHAIN_ID,
            auth_token_lifetime_seconds=self.auth_token_lifetime_seconds,
            operation_mode=OperationMode.PAIRED_OPENING,
            operator_execution_opt_in=True,
            operator_plan_reviewed=True,
            defer_incremental_margin_calculation=self.defer_incremental_margin_calculation,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "market_symbol": self.market_symbol,
            "quantity": format(self.quantity, "f"),
            "direction": self.direction.value,
            "source_account_index": self.source_account_index,
            "receiver_account_index": self.receiver_account_index,
            "api_key_index": self.api_key_index,
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
            "attempt_dir": str(self.attempt_dir),
            "journal_path": str(self.journal_path),
            "operation_mode": OperationMode.PAIRED_OPENING.value,
            "environment": "robinhood",
            "api_base_url": OFFICIAL_ROBINHOOD_API_URL,
            "chain_id": OFFICIAL_ROBINHOOD_CHAIN_ID,
            "auth_token_lifetime_seconds": self.auth_token_lifetime_seconds,
            "operator_execution_opt_in": True,
            "operator_plan_reviewed": True,
            "defer_incremental_margin_calculation": self.defer_incremental_margin_calculation,
        }


def collect_local_attempt_inputs(
    *,
    market_symbol: Any,
    quantity: Any,
    direction: Any,
    source_account_index: Any,
    receiver_account_index: Any,
    api_key_index: Any,
    attempt_dir: Any,
    source_limit_price: Any = None,
    receiver_worst_price: Any = None,
    freshness_seconds: Any = None,
    request_timeout_seconds: Any = None,
    order_timeout_seconds: Any = None,
    reconcile_timeout_seconds: Any = None,
    poll_interval_seconds: Any = None,
    max_poll_count: Any = None,
    source_order_lifetime_seconds: Any = None,
    client_order_prefix: Any = None,
    defer_incremental_margin_calculation: bool = False,
    input_fn: InputFn = input,
) -> LocalAttemptInputs:
    """Normalize direct flags and collect every omitted price/time bound locally."""

    symbol = _required_text(market_symbol, "market_symbol")
    quantity_value = _positive_decimal(quantity, "quantity")
    try:
        parsed_direction = direction if isinstance(direction, Direction) else Direction(str(direction).upper())
    except (TypeError, ValueError) as exc:
        raise LocalAttemptInputError("direction must be LONG or SHORT") from exc

    values = {
        "source_limit_price": _positive_decimal(
            _value_or_prompt(source_limit_price, input_fn, "Source limit price (exact decimal): ", "source_limit_price"),
            "source_limit_price",
        ),
        "receiver_worst_price": _positive_decimal(
            _value_or_prompt(receiver_worst_price, input_fn, "Receiver worst price (exact decimal): ", "receiver_worst_price"),
            "receiver_worst_price",
        ),
        "freshness_seconds": _positive_time(
            _value_or_prompt(freshness_seconds, input_fn, "Freshness bound in seconds: ", "freshness_seconds"),
            "freshness_seconds",
        ),
        "request_timeout_seconds": _positive_time(
            _value_or_prompt(request_timeout_seconds, input_fn, "Request timeout in seconds: ", "request_timeout_seconds"),
            "request_timeout_seconds",
        ),
        "order_timeout_seconds": _positive_time(
            _value_or_prompt(order_timeout_seconds, input_fn, "Order timeout in seconds: ", "order_timeout_seconds"),
            "order_timeout_seconds",
        ),
        "reconcile_timeout_seconds": _positive_time(
            _value_or_prompt(reconcile_timeout_seconds, input_fn, "Reconciliation timeout in seconds: ", "reconcile_timeout_seconds"),
            "reconcile_timeout_seconds",
        ),
        "poll_interval_seconds": _positive_time(
            _value_or_prompt(poll_interval_seconds, input_fn, "Poll interval in seconds: ", "poll_interval_seconds"),
            "poll_interval_seconds",
        ),
        "max_poll_count": _positive_integer(
            _value_or_prompt(max_poll_count, input_fn, "Maximum order polls: ", "max_poll_count"),
            "max_poll_count",
        ),
        "source_order_lifetime_seconds": _positive_integer(
            _value_or_prompt(source_order_lifetime_seconds, input_fn, "Source order lifetime in seconds (minimum 300): ", "source_order_lifetime_seconds"),
            "source_order_lifetime_seconds",
            minimum=300,
        ),
        "client_order_prefix": _required_text(
            _value_or_prompt(client_order_prefix, input_fn, "Client order prefix: ", "client_order_prefix"),
            "client_order_prefix",
        ),
    }
    if isinstance(attempt_dir, Path):
        attempt_path = attempt_dir
    else:
        attempt_path = Path(_required_text(attempt_dir, "attempt_dir"))
    return LocalAttemptInputs(
        market_symbol=symbol,
        quantity=quantity_value,
        direction=parsed_direction,
        source_account_index=_account_index(source_account_index, "source_account_index"),
        receiver_account_index=_account_index(receiver_account_index, "receiver_account_index"),
        api_key_index=api_key_index,
        attempt_dir=attempt_path,
        defer_incremental_margin_calculation=defer_incremental_margin_calculation,
        **values,
    )


def preview_payload(inputs: LocalAttemptInputs) -> dict[str, Any]:
    """Return the complete pre-launch plan and known remaining-position rules."""

    sign = inputs.direction.sign
    source_after = format(-inputs.quantity * sign, "f")
    receiver_after = format(inputs.quantity * sign, "f")
    return {
        "outcome": "PREVIEW",
        "execution": "OWNER_LOCAL_PENDING_LAUNCH",
        "message": "no SDK import, credential access, signing, market request, or order is performed before LAUNCH.",
        "config": inputs.as_dict(),
        "operation": {
            "mode": OperationMode.PAIRED_OPENING.value,
            "source": {
                "account_index": inputs.source_account_index,
                "side": inputs.direction.source_side,
                "order_type": "LIMIT",
                "time_in_force": "POST_ONLY",
                "reduce_only": False,
                "price_bound": format(inputs.source_limit_price, "f"),
            },
            "receiver": {
                "account_index": inputs.receiver_account_index,
                "side": inputs.direction.receiver_side,
                "order_type": "MARKET",
                "time_in_force": "IOC",
                "reduce_only": False,
                "worst_price_bound": format(inputs.receiver_worst_price, "f"),
            },
            "quantity": format(inputs.quantity, "f"),
            "market_id": "resolved from one fresh current catalog observation after launch",
        },
        "remaining_positions": {
            "preflight": "Both selected-market accounts must prove flat before dispatch.",
            "if_both_legs_fully_open": {
                "source": {"account_index": inputs.source_account_index, "signed_position": source_after},
                "receiver": {"account_index": inputs.receiver_account_index, "signed_position": receiver_after},
            },
            "if_failure_partial_or_unknown": "Stop and report known and unknown inventory; no automatic flattening, retry, compensation, transfer, or series loop.",
            "fees": "UNKNOWN unless official terminal trade receipts include fee fields.",
            "incremental_margin": (
                "DEFERRED by explicit owner selection; no estimate is fabricated."
                if inputs.defer_incremental_margin_calculation
                else "STRICT: an explicit incremental opening-margin proof is required before dispatch."
            ),
        },
        "fixed_settings": {
            "api_base_url": OFFICIAL_ROBINHOOD_API_URL,
            "chain_id": OFFICIAL_ROBINHOOD_CHAIN_ID,
            "auth_token_lifetime_seconds": inputs.auth_token_lifetime_seconds,
            "source_order_expiry": "now + source_order_lifetime_seconds after fresh preflight",
            "automatic_price_selection": False,
            "automatic_sizing": False,
        },
    }


class MarketReader(Protocol):
    async def resolve_market(self, symbol: str) -> Any: ...


MarketReaderFactory = Callable[[LocalAttemptInputs, SecretProvider], MarketReader]
ExecutionClientFactory = Callable[[HandoffConfig, LocalAttemptInputs, SecretProvider], HandoffClient]
RunEngine = Callable[..., Awaitable[HandoffResult]]


def default_market_reader_factory(inputs: LocalAttemptInputs, secrets: SecretProvider) -> MarketReader:
    config = inputs.readiness_config()
    return ReadOnlyLighterSdkClient(
        config,
        source_account_index=inputs.source_account_index,
        receiver_account_index=inputs.receiver_account_index,
        secrets=secrets,
    )


def default_execution_client_factory(
    config: HandoffConfig,
    inputs: LocalAttemptInputs,
    secrets: SecretProvider,
) -> HandoffClient:
    # market_metadata is bound to the already captured fresh catalog result by
    # _MetadataBoundClient below.  The execution adapter therefore never
    # falls back to manually supplied market-evidence JSON.
    return LighterSdkClient(
        config,
        source_account_index=inputs.source_account_index,
        receiver_account_index=inputs.receiver_account_index,
        secrets=secrets,
        market_evidence={},
    )


def _metadata_provenance(value: Any, *, sdk_version: str | None = None) -> dict[str, Any]:
    """Keep only current catalog identity/precision/minimum provenance."""

    if isinstance(value, ReadinessMarketMetadata):
        margin = value.margin_evidence
        return {
            "source": "lighter-sdk.order_book_details response",
            "sdk_version": sdk_version,
            "observed_at": value.observed_at,
            "market_id": value.market_id,
            "symbol": value.symbol,
            "market_type": value.market_type,
            "venue": value.venue,
            "status": value.status,
            "price_decimals": value.price_decimals,
            "size_decimals": value.size_decimals,
            "minimum_base_amount": format(value.minimum_base_amount, "f"),
            "minimum_quote_amount": format(value.minimum_quote_amount, "f"),
            "margin_evidence": None if margin is None else margin.as_dict(),
        }
    if isinstance(value, MarketMetadata):
        return {
            "source": "lighter-sdk.order_book_details response",
            "sdk_version": sdk_version,
            "observed_at": value.observed_at,
            "market_id": value.market_id,
            "symbol": value.symbol,
            "market_type": value.market_type,
            "venue": value.venue,
            "status": value.status,
            "price_decimals": value.price_decimals,
            "size_decimals": value.size_decimals,
            "minimum_base_amount": format(value.minimum_base_amount, "f"),
            "minimum_quote_amount": format(value.minimum_quote_amount, "f"),
            "source_fee_rate": None if value.source_fee_rate is None else format(value.source_fee_rate, "f"),
            "receiver_fee_rate": None if value.receiver_fee_rate is None else format(value.receiver_fee_rate, "f"),
            "margin_evidence": value.margin_evidence,
        }
    if isinstance(value, Mapping):
        allowed = {
            "market_id",
            "symbol",
            "market_symbol",
            "market_type",
            "venue",
            "status",
            "price_decimals",
            "supported_price_decimals",
            "size_decimals",
            "supported_size_decimals",
            "minimum_base_amount",
            "min_base_amount",
            "minimum_quote_amount",
            "min_quote_amount",
            "observed_at",
            "source_fee_rate",
            "receiver_fee_rate",
            "margin_evidence",
        }
        return {key: value[key] for key in allowed if key in value}
    raise ContractError("fresh market metadata has an unsupported shape")


def _as_market_metadata(value: Any, *, sdk_version: str | None = None) -> MarketMetadata:
    if isinstance(value, MarketMetadata):
        return value
    if isinstance(value, ReadinessMarketMetadata):
        margin = value.margin_evidence
        evidence = "lighter-sdk.order_book_details response; margin units remain unverified"
        if margin is not None and margin.source:
            evidence = f"{margin.source}; margin units remain unverified"
        return MarketMetadata(
            market_id=value.market_id,
            symbol=value.symbol,
            status=value.status,
            price_decimals=value.price_decimals,
            size_decimals=value.size_decimals,
            minimum_base_amount=value.minimum_base_amount,
            minimum_quote_amount=value.minimum_quote_amount,
            source_fee_rate=None,
            receiver_fee_rate=None,
            observed_at=value.observed_at,
            margin_evidence=evidence,
            market_type=value.market_type,
            venue=value.venue,
        )
    if isinstance(value, Mapping):
        raw = dict(value)
        raw.setdefault("market_type", "perp")
        raw.setdefault("venue", "robinhood")
        raw.setdefault("margin_evidence", "lighter-sdk.order_book_details response; margin units remain unverified")
        return MarketMetadata.from_mapping(raw)
    raise ContractError("fresh market metadata has an unsupported shape")


class _MetadataBoundClient:
    """Expose one fresh catalog observation while delegating engine operations."""

    def __init__(self, delegate: HandoffClient, metadata: MarketMetadata) -> None:
        self._delegate = delegate
        self._metadata = metadata
        self.source_account_index = getattr(delegate, "source_account_index")
        self.receiver_account_index = getattr(delegate, "receiver_account_index")
        self.sdk_version = getattr(delegate, "sdk_version", None)

    async def market_metadata(self, market_id: int) -> MarketMetadata:
        if market_id != self._metadata.market_id:
            raise ContractError("fresh market identity does not match configured market")
        return self._metadata

    async def account_snapshot(self, account_index: int, market_id: int) -> Any:
        return await self._delegate.account_snapshot(account_index, market_id)

    async def lookup_order(self, account_index: int, market_id: int, *, order_id: str | None = None, client_order_index: int | None = None) -> Any:
        return await self._delegate.lookup_order(
            account_index,
            market_id,
            order_id=order_id,
            client_order_index=client_order_index,
        )

    async def list_trades(self, account_index: int, market_id: int, *, order_id: str | None = None, cursor: str | None = None, limit: int = 100) -> Any:
        return await self._delegate.list_trades(
            account_index,
            market_id,
            order_id=order_id,
            cursor=cursor,
            limit=limit,
        )

    async def submit_order(self, plan: Any) -> Any:
        return await self._delegate.submit_order(plan)

    async def cancel_order(self, account_index: int, market_id: int, order_id: str) -> Any:
        return await self._delegate.cancel_order(account_index, market_id, order_id)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)


def _validate_attempt_directory(path: Path) -> Path:
    """Read-only validation for a stable new owner-only attempt directory."""

    path = Path(path)
    if path.is_symlink():
        raise AttemptDirectoryError("attempt directory must not be a symlink")
    if path.exists():
        if not path.is_dir():
            raise AttemptDirectoryError("attempt path exists and is not a directory")
        try:
            info = path.stat()
            children = tuple(path.iterdir())
        except OSError as exc:
            raise AttemptDirectoryError("attempt directory cannot be inspected") from exc
        if info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise AttemptDirectoryError("attempt directory must be owner-only")
        if children:
            journal = path / JOURNAL_NAME
            raise AttemptDirectoryError(
                f"attempt directory already contains evidence; inspect preserved journal {journal} and rerun reconciliation manually"
            )
        return path
    parent = path.parent
    if not parent.exists() or not parent.is_dir() or parent.is_symlink():
        raise AttemptDirectoryError("attempt directory parent must be an existing non-symlink directory")
    try:
        parent_info = parent.stat()
    except OSError as exc:
        raise AttemptDirectoryError("attempt directory parent cannot be inspected") from exc
    if parent_info.st_uid != os.geteuid() or parent_info.st_mode & 0o077:
        raise AttemptDirectoryError("attempt directory parent must be owner-only")
    return path


def _ensure_new_attempt_directory(path: Path) -> Path:
    """Create the validated directory immediately before owner launch."""

    path = _validate_attempt_directory(path)
    if path.exists():
        return path
    try:
        path.mkdir(mode=0o700)
    except FileExistsError:
        return _validate_attempt_directory(path)
    except OSError as exc:
        raise AttemptDirectoryError("attempt directory cannot be created safely") from exc
    try:
        info = path.stat()
    except OSError as exc:
        raise AttemptDirectoryError("attempt directory cannot be inspected after creation") from exc
    if info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise AttemptDirectoryError("attempt directory must be owner-only")
    return path


def _claim_attempt_directory(path: Path) -> Path:
    """Claim the empty directory with an exclusive marker before credentials."""

    claim = path / CLAIM_NAME
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(claim, flags, 0o600)
    except FileExistsError as exc:
        raise AttemptDirectoryError(
            f"attempt directory is already claimed; inspect preserved journal {path / JOURNAL_NAME} and rerun reconciliation manually"
        ) from exc
    except OSError as exc:
        raise AttemptDirectoryError("attempt directory cannot be claimed safely") from exc
    try:
        payload = json.dumps({"pid": os.getpid(), "claim": "one-owner-local-attempt"}, sort_keys=True).encode("utf-8")
        os.write(fd, payload)
        os.fsync(fd)
    finally:
        os.close(fd)
    return claim


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    safe = sanitize(dict(value))
    if not isinstance(safe, dict):
        raise RuntimeError("diagnostic payload is not an object")
    temp = path.with_name(f".{path.name}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(temp, flags, 0o600)
    try:
        encoded = (json.dumps(safe, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        os.write(fd, encoded)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(temp, path)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def _config_packet(inputs: LocalAttemptInputs, config: HandoffConfig | None = None) -> dict[str, Any]:
    if config is None:
        return inputs.as_dict()
    payload = inputs.as_dict()
    payload["market_id"] = config.market_id
    payload["journal_path"] = config.journal_path
    return payload


def _initial_packet(inputs: LocalAttemptInputs) -> dict[str, Any]:
    return {
        "packet_version": PACKET_VERSION,
        "source": {
            "module": "risex_spread_shadow.hood_handoff.local_attempt",
            "source_fingerprint": _source_fingerprint(),
            "sdk_required_version": REQUIRED_LIGHTER_SDK_VERSION,
        },
        "attempt_directory": str(inputs.attempt_dir),
        "journal_path": str(inputs.journal_path),
        "config": _config_packet(inputs),
        "provenance": {
            "status": "PENDING",
            "source": "fresh lighter-sdk orderBookDetails observation after owner launch",
            "metadata": None,
        },
        "terminal": {
            "status": "MISSING",
            "result_file": str(inputs.terminal_result_path),
            "reason": "terminal result is not available until the single engine attempt completes",
        },
        "exit_status": {
            "status": "PENDING",
            "file": str(inputs.exit_status_path),
        },
    }


def _exit_code(result: HandoffResult) -> int:
    return 0 if result.outcome is Outcome.SUCCESS else 2


def _source_fingerprint() -> str:
    """Bind the packet to the local launcher and its execution interfaces."""

    package_dir = Path(__file__).resolve().parent
    digest = hashlib.sha256()
    for name in (
        "contracts.py",
        "engine.py",
        "journal.py",
        "readiness.py",
        "sdk.py",
        "cli.py",
        "local_attempt.py",
    ):
        path = package_dir / name
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return f"sha256:{digest.hexdigest()}"


@dataclass(frozen=True, slots=True)
class LocalAttemptResult:
    status: str
    exit_code: int
    packet_path: Path | None
    journal_path: Path | None
    terminal_status: str
    result: HandoffResult | None = None
    reason: str | None = None
    preview: Mapping[str, Any] | None = None
    provenance: Mapping[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return sanitize(
            {
                "status": self.status,
                "execution": "OWNER_LOCAL",
                "exit_code": self.exit_code,
                "packet_path": None if self.packet_path is None else str(self.packet_path),
                "journal_path": None if self.journal_path is None else str(self.journal_path),
                "terminal_status": self.terminal_status,
                "reason": self.reason,
                "preview": None if self.preview is None else dict(self.preview),
                "provenance": None if self.provenance is None else dict(self.provenance),
                "result": None if self.result is None else self.result.as_dict(),
            }
        )


async def _close_resource(value: Any) -> None:
    for name in ("aclose", "close"):
        method = getattr(value, name, None)
        if not callable(method):
            continue
        result = method()
        if inspect.isawaitable(result):
            await result
        return


def _prime_secrets(secrets: SecretProvider, inputs: LocalAttemptInputs) -> None:
    # Prompt or read both credentials before capturing metadata.  A delayed
    # hidden-key prompt therefore cannot age a fresh market observation.
    for account_index in (inputs.source_account_index, inputs.receiver_account_index):
        secrets.private_key(account_index, inputs.api_key_index)


async def run_local_attempt(
    inputs: LocalAttemptInputs,
    *,
    execute: bool,
    input_fn: InputFn = input,
    output_fn: OutputFn | None = None,
    secret_provider_factory: Callable[[LocalAttemptInputs], SecretProvider],
    market_reader_factory: MarketReaderFactory | None = None,
    execution_client_factory: ExecutionClientFactory | None = None,
    run_engine: RunEngine | None = None,
    clock: Clock | None = None,
) -> LocalAttemptResult:
    """Preview or launch one local attempt and persist a fixed diagnostic packet."""

    preview = preview_payload(inputs)
    if output_fn is not None:
        output_fn(json.dumps(preview, sort_keys=True, separators=(",", ":")))
    try:
        _validate_attempt_directory(inputs.attempt_dir)
    except AttemptDirectoryError as exc:
        return LocalAttemptResult(
            status="REFUSED",
            exit_code=2,
            packet_path=inputs.packet_path if inputs.packet_path.exists() else None,
            journal_path=inputs.journal_path if inputs.journal_path.exists() else inputs.journal_path,
            terminal_status="PRESERVED",
            reason=str(exc),
            preview=preview,
        )
    if not execute:
        return LocalAttemptResult(
            status="PREVIEW",
            exit_code=0,
            packet_path=None,
            journal_path=None,
            terminal_status="NOT_STARTED",
            preview=preview,
        )
    try:
        response = _prompt(input_fn, f"Type {LAUNCH_TOKEN} to launch this one paired-opening attempt: ", "launch confirmation")
    except LocalAttemptInputError as exc:
        return LocalAttemptResult(
            status="CANCELLED",
            exit_code=0,
            packet_path=None,
            journal_path=None,
            terminal_status="NOT_STARTED",
            reason=str(exc),
            preview=preview,
        )
    if response != LAUNCH_TOKEN:
        return LocalAttemptResult(
            status="CANCELLED",
            exit_code=0,
            packet_path=None,
            journal_path=None,
            terminal_status="NOT_STARTED",
            reason="operator did not enter the exact launch token; no SDK, credentials, market request, or order was used",
            preview=preview,
        )

    try:
        attempt_dir = _ensure_new_attempt_directory(inputs.attempt_dir)
        _claim_attempt_directory(attempt_dir)
    except AttemptDirectoryError as exc:
        return LocalAttemptResult(
            status="REFUSED",
            exit_code=2,
            packet_path=inputs.packet_path if inputs.packet_path.exists() else None,
            journal_path=inputs.journal_path,
            terminal_status="PRESERVED",
            reason=str(exc),
            preview=preview,
        )
    packet_path = attempt_dir / PACKET_NAME
    journal_path = attempt_dir / JOURNAL_NAME
    initial = _initial_packet(inputs)
    _atomic_json(packet_path, initial)
    _atomic_json(attempt_dir / EXIT_STATUS_NAME, {"status": "PENDING", "exit_code": None, "terminal_result": "MISSING"})

    secrets: SecretProvider | None = None
    reader: Any | None = None
    execution_client: Any | None = None
    stage = "secret_input"
    packet = initial
    metadata_provenance: dict[str, Any] | None = None
    config: HandoffConfig | None = None
    terminal_recorded = False
    result: HandoffResult | None = None
    exit_code: int | None = None
    try:
        secrets = secret_provider_factory(inputs)
        _prime_secrets(secrets, inputs)
        stage = "market_metadata"
        reader = (market_reader_factory or default_market_reader_factory)(inputs, secrets)
        raw_metadata = await reader.resolve_market(inputs.market_symbol)
        metadata_provenance = _metadata_provenance(
            raw_metadata,
            sdk_version=getattr(reader, "sdk_version", None),
        )
        metadata = _as_market_metadata(raw_metadata, sdk_version=getattr(reader, "sdk_version", None))
        if metadata.market_id < 0 or metadata.symbol.upper() != inputs.market_symbol.upper():
            raise ContractError("fresh market identity does not match operator symbol")
        config = inputs.handoff_config(metadata.market_id)
        packet = {
            **packet,
            "config": _config_packet(inputs, config),
            "provenance": {
                "status": "OBSERVED",
                "source": "fresh lighter-sdk orderBookDetails observation after owner launch",
                "metadata": metadata_provenance,
            },
        }
        _atomic_json(packet_path, packet)
        await _close_resource(reader)
        reader = None
        stage = "engine"
        execution_client = (execution_client_factory or default_execution_client_factory)(config, inputs, secrets)
        bound_client = _MetadataBoundClient(execution_client, metadata)
        result = await (run_engine or run_handoff)(config, bound_client, clock=clock)
        exit_code = _exit_code(result)
        terminal_payload = {
            "packet_version": PACKET_VERSION,
            "terminal_status": "RECORDED",
            "outcome": result.outcome.value,
            "phase": result.phase.value,
            "run_id": result.run_id,
            "result": result.as_dict(),
        }
        _atomic_json(inputs.terminal_result_path, terminal_payload)
        terminal_recorded = True
        packet = {
            **packet,
            "terminal": {
                "status": "RECORDED",
                "result_file": str(inputs.terminal_result_path),
                "outcome": result.outcome.value,
                "phase": result.phase.value,
                "run_id": result.run_id,
            },
            "exit_status": {
                "status": "RECORDED",
                "file": str(inputs.exit_status_path),
                "exit_code": exit_code,
            },
        }
        packet_write_error: str | None = None
        try:
            _atomic_json(inputs.exit_status_path, {
                "packet_version": PACKET_VERSION,
                "status": "RECORDED",
                "exit_code": exit_code,
                "terminal_result": "RECORDED",
                "outcome": result.outcome.value,
            })
            _atomic_json(packet_path, packet)
        except Exception as exc:
            # The terminal result is already durable.  Preserve it and report
            # packet finalization separately instead of replacing it with a
            # fabricated MISSING result.
            packet_write_error = sanitize_exception(exc)
        return LocalAttemptResult(
            status="COMPLETED",
            exit_code=exit_code,
            packet_path=packet_path,
            journal_path=journal_path,
            terminal_status="RECORDED",
            result=result,
            reason=(
                None
                if packet_write_error is None
                else f"terminal result recorded; diagnostic packet finalization failed: {packet_write_error}"
            ),
            preview=preview,
            provenance=metadata_provenance,
        )
    except Exception as exc:
        safe_error = sanitize_exception(exc)
        if terminal_recorded and result is not None and exit_code is not None:
            return LocalAttemptResult(
                status="COMPLETED",
                exit_code=exit_code,
                packet_path=packet_path,
                journal_path=journal_path,
                terminal_status="RECORDED",
                result=result,
                reason=f"terminal result recorded; packet finalization failed: {safe_error}",
                preview=preview,
                provenance=metadata_provenance,
            )
        missing = {
            "packet_version": PACKET_VERSION,
            "terminal_status": "MISSING",
            "terminal_result": None,
            "error_phase": stage,
            "error_class": safe_error,
            "reason": "operation stopped before a terminal engine result was recorded; inspect the journal and known inventory",
        }
        try:
            _atomic_json(inputs.terminal_result_path, missing)
            _atomic_json(inputs.exit_status_path, {
                "packet_version": PACKET_VERSION,
                "status": "INCOMPLETE",
                "exit_code": 2,
                "terminal_result": "MISSING",
                "error_phase": stage,
                "error_class": safe_error,
            })
            packet = {
                **packet,
                "terminal": {
                    "status": "MISSING",
                    "result_file": str(inputs.terminal_result_path),
                    "error_phase": stage,
                    "error_class": safe_error,
                    "reason": "terminal engine result is missing; packet is incomplete",
                },
                "exit_status": {
                    "status": "INCOMPLETE",
                    "file": str(inputs.exit_status_path),
                    "exit_code": 2,
                    "terminal_result": "MISSING",
                },
            }
            _atomic_json(packet_path, packet)
        except Exception:
            # The initial owner-only packet and journal remain the durable
            # checkpoint if terminalization itself is interrupted.
            pass
        return LocalAttemptResult(
            status="INCOMPLETE",
            exit_code=2,
            packet_path=packet_path,
            journal_path=journal_path,
            terminal_status="MISSING",
            reason=f"{stage} failed before terminal result: {safe_error}",
            preview=preview,
            provenance=metadata_provenance,
        )
    finally:
        if reader is not None:
            try:
                await _close_resource(reader)
            except Exception:
                pass
        if execution_client is not None:
            try:
                await _close_resource(execution_client)
            except Exception:
                pass
        if secrets is not None:
            try:
                await _close_resource(secrets)
            except Exception:
                pass


__all__ = [
    "AUTH_TOKEN_LIFETIME_SECONDS",
    "AttemptDirectoryError",
    "EXIT_STATUS_NAME",
    "JOURNAL_NAME",
    "LAUNCH_TOKEN",
    "LocalAttemptInputError",
    "LocalAttemptInputs",
    "LocalAttemptResult",
    "PACKET_NAME",
    "TERMINAL_RESULT_NAME",
    "collect_local_attempt_inputs",
    "default_execution_client_factory",
    "default_market_reader_factory",
    "preview_payload",
    "run_local_attempt",
]
