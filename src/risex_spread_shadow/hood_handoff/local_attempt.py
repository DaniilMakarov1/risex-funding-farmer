"""Owner-local launcher for one bounded HCR-12 paired-opening attempt.

This module is deliberately a thin boundary around the existing HCR-1
contracts, SDK adapters, journal, and engine.  It owns operator input and the
small fixed diagnostic packet; it does not implement a second execution
state machine, retry loop, or storage service.

The execution factory and secret provider are only called after the operator
has entered the fixed plan and explicitly typed ``LAUNCH``.  Automatic mode
then obtains one fresh public market observation and selects both exact prices
from that observation.  Tests inject synthetic factories and therefore never
import a live SDK module or make a request.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
import hashlib
import inspect
import json
import math
import os
from pathlib import Path
import re
from typing import Any, Awaitable, Callable, Mapping, Protocol

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
    decimal_to_integer,
)
from .engine import HandoffClient, Clock, SystemClock, run_handoff
from .journal import sanitize, sanitize_exception
from .readiness import ReadinessConfig, ReadinessMarketMetadata, ReadOnlyLighterSdkClient
from .sdk import (
    LighterSdkClient,
    REQUIRED_LIGHTER_SDK_VERSION,
    ROBINHOOD_ORDER_BOOK_LIMIT,
    SecretProvider,
)
from .series import OrderBookSnapshot


PACKET_VERSION = 1
PACKET_NAME = "attempt-packet.json"
TERMINAL_RESULT_NAME = "terminal-result.json"
EXIT_STATUS_NAME = "exit-status.json"
JOURNAL_NAME = "intent.jsonl"
CLAIM_NAME = ".attempt.claim"
AUTH_TOKEN_LIFETIME_SECONDS = 600
LAUNCH_TOKEN = "LAUNCH"

# HCR-11 declares one finite local default for every omitted timing bound.  A
# caller may still provide an explicit valid override.  These values are
# operating limits, not venue guarantees.
DEFAULT_FRESHNESS_SECONDS = 10.0
DEFAULT_REQUEST_TIMEOUT_SECONDS = 5.0
DEFAULT_ORDER_TIMEOUT_SECONDS = 10.0
DEFAULT_RECONCILE_TIMEOUT_SECONDS = 20.0
DEFAULT_POLL_INTERVAL_SECONDS = 0.5
DEFAULT_MAX_POLL_COUNT = 40
DEFAULT_SOURCE_ORDER_LIFETIME_SECONDS = 300

# Stable, secret-free local outcome labels.  They describe the launcher
# boundary only; the engine's result remains the authority for fills,
# positions, reconciliation, and any unknown execution state.
LOCAL_REASON_ATTEMPT_DIRECTORY = "ATTEMPT_DIRECTORY_REFUSED"
LOCAL_REASON_OPERATOR_CANCELLED = "OPERATOR_CANCELLED"
LOCAL_REASON_PUBLIC_READ_FAILED = "PUBLIC_READ_FAILED"
LOCAL_REASON_INVALID_OBSERVATION = "INVALID_PUBLIC_OBSERVATION"
LOCAL_REASON_STALE_OBSERVATION = "STALE_PUBLIC_OBSERVATION"
LOCAL_REASON_PRICE_CHANGED = "PRICE_CHANGED_AFTER_LAUNCH"
LOCAL_REASON_PROPOSAL_EXPIRED = "ORIGINAL_PROPOSAL_EXPIRED"
LOCAL_REASON_INITIAL_PACKET = "INITIAL_PACKET_WRITE_FAILED"
LOCAL_REASON_PACKET_WRITE = "PACKET_WRITE_FAILED"
LOCAL_REASON_PRE_EXECUTION = "PRE_EXECUTION_FAILURE"
LOCAL_REASON_TERMINAL_MISSING = "TERMINAL_RESULT_MISSING"
LOCAL_REASON_PACKET_FINALIZATION = "PACKET_FINALIZATION_INCOMPLETE"

EXECUTION_STATE_NOT_STARTED = "NOT_STARTED"
EXECUTION_STATE_READY = "READY_FOR_ENGINE"
EXECUTION_STATE_PRE_EXECUTION_STOP = "PRE_EXECUTION_STOP"
EXECUTION_STATE_UNKNOWN = "UNKNOWN_EXECUTION"
EXECUTION_STATE_TERMINAL_RECORDED = "TERMINAL_RECORDED"


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


def _unresolved_automatic_provenance() -> dict[str, Any]:
    """Describe the intentionally unresolved pre-launch automatic price slot."""

    return {
        "status": "UNRESOLVED_BEFORE_LAUNCH",
        "selection": "one fresh public metadata/book read after LAUNCH",
        "metadata": None,
        "book": None,
        "prices": None,
    }


def _classify_local_error(exc: BaseException, *, stage: str) -> str | None:
    """Map a boundary failure to a fixed safe reason code.

    Engine-entry knowledge takes precedence over text matching.  Only a few
    known words from a pre-engine validated contract-like exception are
    inspected.  The complete exception text is never returned to the operator
    or persisted; SDK and transport failures therefore remain opaque and
    secret-free.
    """

    if stage == "engine":
        # Once the execution client has been constructed, even a seemingly
        # harmless exception cannot prove that no mutation was attempted.
        return LOCAL_REASON_TERMINAL_MISSING
    if stage == "initial_packet":
        return LOCAL_REASON_INITIAL_PACKET
    if stage == "packet":
        return LOCAL_REASON_PACKET_WRITE
    if stage == "secret_input":
        return LOCAL_REASON_PRE_EXECUTION
    contract_error = isinstance(exc, (ContractError, ValueError, TypeError))
    if stage in {"market_metadata", "market_book", "market_quote_selection"} and not contract_error:
        # An opaque SDK/transport exception is a read failure even when its
        # text happens to contain words such as "stale" or "expired".
        return LOCAL_REASON_PUBLIC_READ_FAILED
    if not contract_error:
        return None
    text = str(exc).lower()
    if "price changed" in text or "quote changed" in text:
        return LOCAL_REASON_PRICE_CHANGED
    if "proposal" in text and "expired" in text:
        return LOCAL_REASON_PROPOSAL_EXPIRED
    if any(token in text for token in ("stale", "expired", "too old")):
        return LOCAL_REASON_STALE_OBSERVATION
    if any(
        token in text
        for token in (
            "future",
            "crossed",
            "identity",
            "malformed",
            "unsupported shape",
            "must contain",
            "not active",
            "wrong venue",
            "minimum",
            "grid",
            "decimals",
            "symbol",
        )
    ):
        return LOCAL_REASON_INVALID_OBSERVATION
    if stage in {"market_metadata", "market_book", "market_quote_selection"}:
        return LOCAL_REASON_INVALID_OBSERVATION
    return None


def _local_failure_message(reason_code: str, safe_error: str | None = None) -> str:
    """Return concise operator text for a classified local failure."""

    messages = {
        LOCAL_REASON_ATTEMPT_DIRECTORY: "attempt directory was refused before launch",
        LOCAL_REASON_OPERATOR_CANCELLED: "operator cancelled before launch; no credentials or orders were used",
        LOCAL_REASON_PUBLIC_READ_FAILED: "fresh public metadata/book read failed; stopped before engine dispatch",
        LOCAL_REASON_INVALID_OBSERVATION: "fresh public metadata/book observation was invalid; stopped before engine dispatch",
        LOCAL_REASON_STALE_OBSERVATION: "fresh public metadata/book observation was stale; stopped before engine dispatch",
        LOCAL_REASON_PRICE_CHANGED: "approved price changed before execution; stopped before engine dispatch",
        LOCAL_REASON_PROPOSAL_EXPIRED: "original automatic proposal expired before execution; stopped before engine dispatch",
        LOCAL_REASON_INITIAL_PACKET: "initial diagnostic packet failed before secrets; stopped before engine dispatch",
        LOCAL_REASON_PACKET_WRITE: "diagnostic packet could not be recorded; stopped before engine dispatch",
        LOCAL_REASON_PRE_EXECUTION: "operation stopped before engine dispatch; no terminal result was recorded",
        LOCAL_REASON_TERMINAL_MISSING: "terminal engine result is missing; execution state is unknown; inspect the journal and known inventory",
        LOCAL_REASON_PACKET_FINALIZATION: "terminal result recorded; diagnostic packet finalization incomplete",
    }
    message = messages.get(reason_code, "local attempt stopped before a complete terminal result")
    if safe_error is not None and reason_code not in {
        LOCAL_REASON_OPERATOR_CANCELLED,
        LOCAL_REASON_ATTEMPT_DIRECTORY,
    }:
        return f"{message} (error class: {safe_error})"
    return message


def _execution_packet(state: str, reason_code: str | None = None) -> dict[str, Any]:
    """Serialize launcher execution knowledge without claiming account state."""

    if state == EXECUTION_STATE_PRE_EXECUTION_STOP:
        dispatch_status = "NOT_DISPATCHED_BY_THIS_ATTEMPT"
    elif state == EXECUTION_STATE_NOT_STARTED:
        dispatch_status = "NOT_ATTEMPTED"
    elif state == EXECUTION_STATE_READY:
        dispatch_status = "NOT_DISPATCHED_YET"
    elif state == EXECUTION_STATE_TERMINAL_RECORDED:
        dispatch_status = "SEE_TERMINAL_RESULT"
    else:
        dispatch_status = "UNKNOWN"
    return {
        "state": state,
        "dispatch_status": dispatch_status,
        "known_pre_execution_stop": state == EXECUTION_STATE_PRE_EXECUTION_STOP,
        "reason_code": reason_code,
    }


@dataclass(frozen=True, slots=True)
class LocalAttemptInputs:
    """All operator-selected values required before a fresh observation."""

    market_symbol: str
    quantity: Decimal
    direction: Direction
    source_account_index: int
    receiver_account_index: int
    api_key_index: int
    source_limit_price: Decimal | None
    receiver_worst_price: Decimal | None
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
    automatic_price_selection_requested: bool | None = None

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
        source_price = (
            None
            if self.source_limit_price is None
            else _positive_decimal(self.source_limit_price, "source_limit_price")
        )
        receiver_price = (
            None
            if self.receiver_worst_price is None
            else _positive_decimal(self.receiver_worst_price, "receiver_worst_price")
        )
        if (source_price is None) != (receiver_price is None):
            raise LocalAttemptInputError(
                "source_limit_price and receiver_worst_price must both be omitted for automatic selection or both be supplied"
            )
        requested = self.automatic_price_selection_requested
        if requested is not None and not isinstance(requested, bool):
            raise LocalAttemptInputError("automatic_price_selection_requested must be bool or None")
        object.__setattr__(self, "source_limit_price", source_price)
        object.__setattr__(self, "receiver_worst_price", receiver_price)
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
    def automatic_price_selection(self) -> bool:
        """Whether this input waits for one public-book price proposal."""

        if self.automatic_price_selection_requested is not None:
            return self.automatic_price_selection_requested
        return self.source_limit_price is None and self.receiver_worst_price is None

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
        if self.source_limit_price is None or self.receiver_worst_price is None:
            raise LocalAttemptInputError(
                "automatic prices must be resolved from a fresh public book before engine configuration"
            )
        assert self.source_limit_price is not None
        assert self.receiver_worst_price is not None
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
            "source_limit_price": (
                None if self.source_limit_price is None else format(self.source_limit_price, "f")
            ),
            "receiver_worst_price": (
                None if self.receiver_worst_price is None else format(self.receiver_worst_price, "f")
            ),
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
            "automatic_price_selection": self.automatic_price_selection,
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
    automatic_price_selection: bool | None = None,
    input_fn: InputFn = input,
) -> LocalAttemptInputs:
    """Normalize direct flags and apply the HCR-12 automatic input policy.

    The command-line entry point passes ``automatic_price_selection=True``
    when both prices are omitted.  Direct callers receive the same automatic
    behavior whenever both prices are absent, so an input function cannot turn
    an unresolved plan into guessed prices or timing bounds.
    """

    symbol = _required_text(market_symbol, "market_symbol")
    quantity_value = _positive_decimal(quantity, "quantity")
    try:
        parsed_direction = direction if isinstance(direction, Direction) else Direction(str(direction).upper())
    except (TypeError, ValueError) as exc:
        raise LocalAttemptInputError("direction must be LONG or SHORT") from exc

    both_prices_omitted = source_limit_price is None and receiver_worst_price is None
    one_price_omitted = (source_limit_price is None) != (receiver_worst_price is None)
    if automatic_price_selection is None:
        automatic_price_selection = both_prices_omitted
    if not isinstance(automatic_price_selection, bool):
        raise LocalAttemptInputError("automatic_price_selection must be bool or None")
    if both_prices_omitted and not automatic_price_selection:
        raise LocalAttemptInputError(
            "both omitted prices require automatic public-book selection"
        )
    if automatic_price_selection and not both_prices_omitted:
        raise LocalAttemptInputError(
            "automatic price selection requires both source_limit_price and receiver_worst_price to be omitted"
        )
    if one_price_omitted:
        raise LocalAttemptInputError(
            "source_limit_price and receiver_worst_price must be supplied together or both omitted"
        )

    if automatic_price_selection:
        source_price: Decimal | None = None
        receiver_price: Decimal | None = None
    else:
        source_price = _positive_decimal(
            _value_or_prompt(
                source_limit_price,
                input_fn,
                "Source limit price (exact decimal): ",
                "source_limit_price",
            ),
            "source_limit_price",
        )
        receiver_price = _positive_decimal(
            _value_or_prompt(
                receiver_worst_price,
                input_fn,
                "Receiver worst price (exact decimal): ",
                "receiver_worst_price",
            ),
            "receiver_worst_price",
        )

    # With the HCR-11/HCR-12 policy the timing values are finite declared defaults;
    # explicit values continue to be validated exactly as before.
    def timing_value(value: Any, default: Any) -> Any:
        return default if value is None else value

    values = {
        "source_limit_price": source_price,
        "receiver_worst_price": receiver_price,
        "freshness_seconds": _positive_time(
            timing_value(
                freshness_seconds,
                DEFAULT_FRESHNESS_SECONDS,
            ),
            "freshness_seconds",
        ),
        "request_timeout_seconds": _positive_time(
            timing_value(
                request_timeout_seconds,
                DEFAULT_REQUEST_TIMEOUT_SECONDS,
            ),
            "request_timeout_seconds",
        ),
        "order_timeout_seconds": _positive_time(
            timing_value(
                order_timeout_seconds,
                DEFAULT_ORDER_TIMEOUT_SECONDS,
            ),
            "order_timeout_seconds",
        ),
        "reconcile_timeout_seconds": _positive_time(
            timing_value(
                reconcile_timeout_seconds,
                DEFAULT_RECONCILE_TIMEOUT_SECONDS,
            ),
            "reconcile_timeout_seconds",
        ),
        "poll_interval_seconds": _positive_time(
            timing_value(
                poll_interval_seconds,
                DEFAULT_POLL_INTERVAL_SECONDS,
            ),
            "poll_interval_seconds",
        ),
        "max_poll_count": _positive_integer(
            timing_value(
                max_poll_count,
                DEFAULT_MAX_POLL_COUNT,
            ),
            "max_poll_count",
        ),
        "source_order_lifetime_seconds": _positive_integer(
            timing_value(
                source_order_lifetime_seconds,
                DEFAULT_SOURCE_ORDER_LIFETIME_SECONDS,
            ),
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
        automatic_price_selection_requested=automatic_price_selection,
        **values,
    )


def preview_payload(inputs: LocalAttemptInputs) -> dict[str, Any]:
    """Return the complete pre-launch plan and known remaining-position rules."""

    sign = inputs.direction.sign
    source_after = format(-inputs.quantity * sign, "f")
    receiver_after = format(inputs.quantity * sign, "f")
    automatic = inputs.automatic_price_selection
    source_price = None if inputs.source_limit_price is None else format(inputs.source_limit_price, "f")
    receiver_price = None if inputs.receiver_worst_price is None else format(inputs.receiver_worst_price, "f")
    source_notional = (
        None
        if inputs.source_limit_price is None
        else format(inputs.quantity * inputs.source_limit_price, "f")
    )
    receiver_notional = (
        None
        if inputs.receiver_worst_price is None
        else format(inputs.quantity * inputs.receiver_worst_price, "f")
    )
    return {
        "outcome": "PREVIEW",
        "execution": "OWNER_LOCAL_PENDING_LAUNCH",
        "message": (
            "no SDK import, credential access, signing, market request, or order is performed before LAUNCH."
            if not automatic
            else "prices are unresolved until one fresh public metadata/book read after LAUNCH; no credentials or orders are used before confirmation."
        ),
        "config": inputs.as_dict(),
        "operation": {
            "mode": OperationMode.PAIRED_OPENING.value,
            "source": {
                "account_index": inputs.source_account_index,
                "side": inputs.direction.source_side,
                "order_type": "LIMIT",
                "time_in_force": "POST_ONLY",
                "reduce_only": False,
                "price_bound": source_price,
            },
            "receiver": {
                "account_index": inputs.receiver_account_index,
                "side": inputs.direction.receiver_side,
                "order_type": "MARKET",
                "time_in_force": "IOC",
                "reduce_only": False,
                "worst_price_bound": receiver_price,
            },
            "quantity": format(inputs.quantity, "f"),
            "source_limit_notional": source_notional,
            "receiver_worst_bound_notional": receiver_notional,
            "notional_semantics": (
                "selected price bounds multiplied by exact quantity; bounds/estimates only, "
                "not fills, execution prices, fees, or profit"
            ),
            "market_id": (
                "resolved from one fresh current catalog/book observation after LAUNCH"
                if automatic
                else "resolved from one fresh current catalog observation after launch"
            ),
        },
        "price_selection": {
            "automatic": automatic,
            "rule": (
                "SELL source: best ask minus one price tick when strictly above best bid, otherwise best ask; "
                "BUY source mirrors that rule; receiver bound equals selected source price"
                if automatic
                else "operator supplied exact source and receiver bounds"
            ),
            "source_limit_price": source_price,
            "receiver_worst_price": receiver_price,
            "observation": None,
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
            "automatic_price_selection": automatic,
            "automatic_sizing": False,
            "timing_defaults": {
                "freshness_seconds": inputs.freshness_seconds,
                "request_timeout_seconds": inputs.request_timeout_seconds,
                "order_timeout_seconds": inputs.order_timeout_seconds,
                "reconcile_timeout_seconds": inputs.reconcile_timeout_seconds,
                "poll_interval_seconds": inputs.poll_interval_seconds,
                "max_poll_count": inputs.max_poll_count,
                "source_order_lifetime_seconds": inputs.source_order_lifetime_seconds,
            },
        },
    }


def format_local_attempt_preview(inputs: LocalAttemptInputs) -> str:
    """Render the small human-readable plan shown before the launch token."""

    lines = [
        "OWNER_LOCAL_PENDING_LAUNCH",
        (
            f"venue=robinhood-chain api_base_url={OFFICIAL_ROBINHOOD_API_URL} "
            f"signing_domain=robinhood-chain chain_id={OFFICIAL_ROBINHOOD_CHAIN_ID}"
        ),
        (
            f"mode=PAIRED_OPENING market={inputs.market_symbol} direction={inputs.direction.value} "
            f"quantity={format(inputs.quantity, 'f')}"
        ),
        (
            f"accounts=source:{inputs.source_account_index} "
            f"receiver:{inputs.receiver_account_index} api_key_index:{inputs.api_key_index}"
        ),
        (
            f"source_leg={inputs.direction.source_side} LIMIT POST_ONLY reduce_only=False; "
            f"receiver_leg={inputs.direction.receiver_side} MARKET IOC reduce_only=False"
        ),
    ]
    if inputs.automatic_price_selection:
        lines.append(
            "prices=UNRESOLVED; automatic rule: source SELL uses best ask minus one tick "
            "when strictly above best bid, source BUY mirrors it, receiver bound equals source"
        )
    else:
        assert inputs.source_limit_price is not None
        assert inputs.receiver_worst_price is not None
        lines.append(
            f"prices=source:{format(inputs.source_limit_price, 'f')} "
            f"receiver_bound:{format(inputs.receiver_worst_price, 'f')} "
            f"notionals:{format(inputs.quantity * inputs.source_limit_price, 'f')}/"
            f"{format(inputs.quantity * inputs.receiver_worst_price, 'f')}"
        )
    lines.append(
        (
            f"timing=freshness:{inputs.freshness_seconds:g}s request:{inputs.request_timeout_seconds:g}s "
            f"order:{inputs.order_timeout_seconds:g}s reconcile:{inputs.reconcile_timeout_seconds:g}s "
            f"poll:{inputs.poll_interval_seconds:g}s max_polls:{inputs.max_poll_count} "
            f"source_expiry:{inputs.source_order_lifetime_seconds}s auth_lifetime:{inputs.auth_token_lifetime_seconds}s"
        )
    )
    lines.append(
        "margin_mode="
        + ("DEFERRED" if inputs.defer_incremental_margin_calculation else "STRICT")
    )
    lines.append(
        "pre-launch: no SDK import, credential access, market request, or order is performed before LAUNCH."
    )
    return "\n".join(lines)


@dataclass(frozen=True, slots=True)
class AutomaticPriceProposal:
    """One exact source/receiver price pair selected from a public book."""

    market_id: int
    symbol: str
    direction: Direction
    best_bid: Decimal
    best_ask: Decimal
    price_tick: Decimal
    source_limit_price: Decimal
    receiver_worst_price: Decimal
    observed_at: float
    used_tick_adjustment: bool

    def __post_init__(self) -> None:
        if not isinstance(self.direction, Direction):
            try:
                object.__setattr__(self, "direction", Direction(str(self.direction).upper()))
            except (TypeError, ValueError) as exc:
                raise LocalAttemptInputError("direction must be LONG or SHORT") from exc
        if isinstance(self.market_id, bool) or not isinstance(self.market_id, int) or self.market_id < 0:
            raise LocalAttemptInputError("market_id must be a non-negative integer")
        object.__setattr__(self, "symbol", _required_text(self.symbol, "symbol").upper())
        for name in (
            "best_bid",
            "best_ask",
            "price_tick",
            "source_limit_price",
            "receiver_worst_price",
        ):
            object.__setattr__(self, name, _positive_decimal(getattr(self, name), name))
        if self.best_bid >= self.best_ask:
            raise LocalAttemptInputError("public order book is crossed")
        if self.receiver_worst_price != self.source_limit_price:
            raise LocalAttemptInputError("receiver price bound must equal selected source price")
        if isinstance(self.observed_at, bool):
            raise LocalAttemptInputError("order-book observed_at must be a timestamp")
        try:
            observed_at = float(self.observed_at)
        except (TypeError, ValueError) as exc:
            raise LocalAttemptInputError("order-book observed_at must be a timestamp") from exc
        if not math.isfinite(observed_at) or observed_at < 0:
            raise LocalAttemptInputError("order-book observed_at must be a finite non-negative timestamp")
        object.__setattr__(self, "observed_at", observed_at)
        if not isinstance(self.used_tick_adjustment, bool):
            raise LocalAttemptInputError("used_tick_adjustment must be bool")

    def as_dict(self) -> dict[str, Any]:
        return {
            "market_id": self.market_id,
            "symbol": self.symbol,
            "direction": self.direction.value,
            "best_bid": format(self.best_bid, "f"),
            "best_ask": format(self.best_ask, "f"),
            "price_tick": format(self.price_tick, "f"),
            "source_limit_price": format(self.source_limit_price, "f"),
            "receiver_worst_price": format(self.receiver_worst_price, "f"),
            "observed_at": self.observed_at,
            "used_tick_adjustment": self.used_tick_adjustment,
        }


def _clock_now(clock: Clock | None) -> float:
    value = (clock or SystemClock()).now()
    return _finite_timestamp(value, "clock returned an invalid timestamp")


def _finite_timestamp(value: Any, error: str) -> float:
    if isinstance(value, bool):
        raise LocalAttemptInputError(error)
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise LocalAttemptInputError(error) from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise LocalAttemptInputError(error)
    return parsed


def _strict_market_id(value: Any) -> int:
    """Accept only exact integer values or wire integer strings."""

    if isinstance(value, bool):
        raise ContractError("order-book market_id is malformed")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and re.fullmatch(r"[+-]?[0-9]+", value):
        return int(value)
    raise ContractError("order-book market_id is malformed")


def _as_order_book_snapshot(value: Any, metadata: MarketMetadata) -> OrderBookSnapshot:
    """Normalize a reader result without inventing its observation timestamp."""

    if isinstance(value, OrderBookSnapshot):
        return value
    if not isinstance(value, Mapping):
        raise ContractError("public order-book response has an unsupported shape")
    if "market_id" in value:
        if _strict_market_id(value["market_id"]) != metadata.market_id:
            raise ContractError("order-book identity does not match current market")
    if "symbol" in value:
        raw_symbol = value["symbol"]
        if not isinstance(raw_symbol, str) or raw_symbol.strip().upper() != metadata.symbol.upper():
            raise ContractError("order-book symbol does not match current market")
    if "observed_at" not in value:
        raise ContractError("order-book response lacks its original observed_at timestamp")
    return OrderBookSnapshot.from_mapping(
        value,
        market_id=metadata.market_id,
        symbol=metadata.symbol,
        observed_at=value["observed_at"],
        venue=str(value.get("venue", metadata.venue or "robinhood")),
    )


def _validate_public_market_and_book(
    metadata: MarketMetadata,
    book: OrderBookSnapshot,
    *,
    now: float,
    freshness_seconds: float,
    quantity: Decimal | None = None,
) -> None:
    """Apply the existing market checks plus the HCR-12 book boundary."""

    if metadata.symbol.upper() == "" or metadata.symbol.upper() != book.symbol.upper():
        raise ContractError("public order-book identity does not match market symbol")
    if metadata.market_id != book.market_id:
        raise ContractError("public order-book identity does not match market id")
    if metadata.market_type.lower() != "perp" or book.market_type.lower() != "perp":
        raise ContractError("public market/order book must be a perpetual")
    if metadata.venue.lower() not in {"", "robinhood", "robinhood-chain"}:
        raise ContractError("public market is from the wrong venue")
    if book.venue.lower() not in {"robinhood", "robinhood-chain"}:
        raise ContractError("public order book is from the wrong venue")
    if metadata.status.lower() not in {"active", "open", "online", "listed"}:
        raise ContractError("public market is not active")
    if metadata.observed_at > now:
        raise ContractError("market metadata is from the future")
    if now - metadata.observed_at > freshness_seconds:
        raise ContractError("market metadata is stale")
    if book.observed_at > now:
        raise ContractError("order-book observation is from the future")
    if now - book.observed_at > freshness_seconds:
        raise ContractError("order-book observation is stale")
    if not book.bids or not book.asks:
        raise ContractError("public order book must contain both bid and ask sides")
    best_bid = book.bids[0].price
    best_ask = book.asks[0].price
    if best_bid >= best_ask:
        raise ContractError("public order book is crossed")
    # Reject an unusable level rather than silently rounding or selecting a
    # price from malformed depth.  The SDK adapter has already bounded the
    # number of levels returned by the public request.
    for level in (*book.bids, *book.asks):
        decimal_to_integer(level.price, metadata.price_decimals, "order-book price")
    if quantity is not None:
        decimal_to_integer(quantity, metadata.size_decimals, "quantity")
        if quantity < metadata.minimum_base_amount:
            raise ContractError("quantity is below the documented base minimum")


def select_automatic_prices(
    direction: Direction | str,
    metadata: MarketMetadata | Mapping[str, Any],
    book: OrderBookSnapshot | Mapping[str, Any],
    *,
    quantity: Decimal | None = None,
    price_improvement_ticks: int | None = None,
    now: float | None = None,
    freshness_seconds: float = DEFAULT_FRESHNESS_SECONDS,
) -> AutomaticPriceProposal:
    """Select one exact grid price from a fresh, uncrossed public book.

    For a source SELL the best ask is lowered by one tick when that remains
    strictly above the best bid.  For a source BUY the mirror is the best bid
    raised by one tick when that remains strictly below the best ask.  If the
    one-tick candidate would cross the opposite side, the original best quote
    is retained.  The receiver bound is always exactly the selected source
    price. An explicit price_improvement_ticks (1..5) instead requires that exact
    offset and refuses a narrow spread; only omitted legacy configuration falls back.
    """

    try:
        parsed_direction = direction if isinstance(direction, Direction) else Direction(str(direction).upper())
    except (TypeError, ValueError) as exc:
        raise LocalAttemptInputError("direction must be LONG or SHORT") from exc
    current = _as_market_metadata(metadata)
    current_book = _as_order_book_snapshot(book, current)
    effective_now = (
        _clock_now(None)
        if now is None
        else _finite_timestamp(now, "now must be a finite non-negative timestamp")
    )
    effective_freshness = _positive_time(freshness_seconds, "freshness_seconds")
    effective_quantity = (
        None if quantity is None else _positive_decimal(quantity, "quantity")
    )
    _validate_public_market_and_book(
        current,
        current_book,
        now=effective_now,
        freshness_seconds=effective_freshness,
        quantity=effective_quantity,
    )
    best_bid = current_book.bids[0].price
    best_ask = current_book.asks[0].price
    tick = Decimal(1).scaleb(-current.price_decimals)
    if price_improvement_ticks is not None and (type(price_improvement_ticks) is not int or not 1 <= price_improvement_ticks <= 5):
        raise LocalAttemptInputError("price_improvement_ticks must be an integer from 1 to 5")
    offset = (1 if price_improvement_ticks is None else price_improvement_ticks) * tick
    if parsed_direction is Direction.LONG:
        candidate = best_ask - offset
        selected = candidate if candidate > best_bid else best_ask
        used_tick = selected == candidate
    else:
        candidate = best_bid + offset
        selected = candidate if candidate < best_ask else best_bid
        used_tick = selected == candidate
    if price_improvement_ticks is not None and not used_tick:
        raise ContractError("PRICE_OFFSET_NO_ROOM: spread cannot fit the requested tick improvement")
    # Validate the final choice explicitly even when the fallback best quote
    # was selected.  No guessed/off-grid fallback is accepted.
    decimal_to_integer(selected, current.price_decimals, "automatic source_limit_price")
    if effective_quantity is not None and effective_quantity * selected < current.minimum_quote_amount:
        raise ContractError("automatic source price yields a notional below the documented quote minimum")
    return AutomaticPriceProposal(
        market_id=current.market_id,
        symbol=current.symbol,
        direction=parsed_direction,
        best_bid=best_bid,
        best_ask=best_ask,
        price_tick=tick,
        source_limit_price=selected,
        receiver_worst_price=selected,
        observed_at=current_book.observed_at,
        used_tick_adjustment=used_tick,
    )


# Descriptive alias for callers that prefer a verb over the selector name.
derive_automatic_prices = select_automatic_prices


class MarketReader(Protocol):
    async def resolve_market(self, symbol: str) -> Any: ...


MarketReaderFactory = Callable[[LocalAttemptInputs, SecretProvider | None], MarketReader]
ExecutionClientFactory = Callable[[HandoffConfig, LocalAttemptInputs, SecretProvider], HandoffClient]
RunEngine = Callable[..., Awaitable[HandoffResult]]


def default_market_reader_factory(
    inputs: LocalAttemptInputs,
    secrets: SecretProvider | None = None,
) -> MarketReader:
    config = inputs.readiness_config()
    return ReadOnlyLighterSdkClient(
        config,
        source_account_index=inputs.source_account_index,
        receiver_account_index=inputs.receiver_account_index,
        secrets=secrets,
    )


async def _read_public_book(reader: Any, market_id: int) -> Any:
    """Read one public book through either supported adapter method name."""

    method = getattr(reader, "order_book_snapshot", None)
    if not callable(method):
        method = getattr(reader, "order_book", None)
    if not callable(method):
        raise ContractError("market reader does not provide a public order-book method")
    result = method(market_id)
    if inspect.isawaitable(result):
        return await result
    return result


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
        retained = {key: value[key] for key in allowed if key in value}
        retained.setdefault("source", "lighter-sdk.order_book_details response")
        retained["sdk_version"] = sdk_version
        return sanitize(retained)
    raise ContractError("fresh market metadata has an unsupported shape")


def _book_provenance(value: Any, *, sdk_version: str | None = None) -> dict[str, Any]:
    """Retain the identity, timestamps and exact public levels used in a quote."""

    if isinstance(value, OrderBookSnapshot):
        payload = value.as_dict()
    elif isinstance(value, Mapping):
        payload = dict(value)
    else:
        raise ContractError("public order-book response has an unsupported shape")
    allowed = {"market_id", "symbol", "market_type", "venue", "observed_at", "bids", "asks"}
    retained = {key: payload[key] for key in allowed if key in payload}
    if "observed_at" not in retained:
        raise ContractError("order-book response lacks its original observed_at timestamp")
    retained["source"] = "lighter-sdk.order_book_orders response"
    retained["sdk_version"] = sdk_version
    return sanitize(retained)


def _unvalidated_metadata_provenance(
    value: Any,
    *,
    sdk_version: str | None = None,
) -> dict[str, Any]:
    """Capture bounded raw metadata when typed validation cannot complete."""

    if not isinstance(value, Mapping):
        return {
            "available": False,
            "source": "lighter-sdk.order_book_details response",
            "sdk_version": sdk_version,
            "observed_at": None,
            "validation": "UNVALIDATED",
        }
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
    retained = {key: value[key] for key in allowed if key in value}
    retained.setdefault("observed_at", None)
    retained.update(
        {
            "available": True,
            "source": "lighter-sdk.order_book_details response",
            "sdk_version": sdk_version,
            "validation": "UNVALIDATED",
        }
    )
    return sanitize(retained)


def _safe_metadata_provenance(
    value: Any,
    *,
    sdk_version: str | None = None,
) -> dict[str, Any]:
    """Retain safe observation evidence even if metadata normalization fails."""

    try:
        return _metadata_provenance(value, sdk_version=sdk_version)
    except Exception:
        return _unvalidated_metadata_provenance(value, sdk_version=sdk_version)


def _unvalidated_book_provenance(
    value: Any,
    *,
    sdk_version: str | None = None,
) -> dict[str, Any]:
    """Capture bounded raw book evidence without claiming it was valid."""

    if isinstance(value, OrderBookSnapshot):
        payload = value.as_dict()
    elif isinstance(value, Mapping):
        payload = dict(value)
    else:
        return {
            "available": False,
            "source": "lighter-sdk.order_book_orders response",
            "sdk_version": sdk_version,
            "observed_at": None,
            "validation": "UNVALIDATED",
        }
    allowed = {"market_id", "symbol", "market_type", "venue", "observed_at", "bids", "asks"}
    retained = {key: payload[key] for key in allowed if key in payload}
    retained.setdefault("observed_at", None)
    for side in ("bids", "asks"):
        levels = retained.get(side)
        if isinstance(levels, (list, tuple)):
            retained[side] = list(levels)[:ROBINHOOD_ORDER_BOOK_LIMIT]
    retained.update(
        {
            "available": True,
            "source": "lighter-sdk.order_book_orders response",
            "sdk_version": sdk_version,
            "validation": "UNVALIDATED",
        }
    )
    return sanitize(retained)


def _safe_book_provenance(
    value: Any,
    *,
    sdk_version: str | None = None,
) -> dict[str, Any]:
    """Retain safe observation evidence even if book normalization fails."""

    try:
        return _book_provenance(value, sdk_version=sdk_version)
    except Exception:
        return _unvalidated_book_provenance(value, sdk_version=sdk_version)


def _compact_provenance(value: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Keep result/CLI provenance bounded while packets retain full depth."""

    if value is None:
        return None
    compact: dict[str, Any] = {}
    for label in ("status", "selection"):
        if label in value:
            compact[label] = value[label]
    for label in ("metadata", "book"):
        item = value.get(label)
        if not isinstance(item, Mapping):
            compact[label] = item
            continue
        allowed = {
            "source",
            "sdk_version",
            "market_id",
            "symbol",
            "market_type",
            "venue",
            "status",
            "observed_at",
        }
        compact[label] = {
            key: item[key]
            for key in allowed
            if key in item
        }
    if "prices" in value:
        compact["prices"] = value["prices"]
    return sanitize(compact)


def format_local_attempt_selection(
    inputs: LocalAttemptInputs,
    proposal: AutomaticPriceProposal,
    *,
    sdk_version: str | None = None,
) -> str:
    """Render the exact post-launch prices selected from one fresh book."""

    return "\n".join(
        (
            "POST_LAUNCH_PRICE_SELECTED",
            (
                f"market={proposal.symbol}#{proposal.market_id} observation={proposal.observed_at:.6f} "
                f"best_bid={format(proposal.best_bid, 'f')} best_ask={format(proposal.best_ask, 'f')} "
                f"tick={format(proposal.price_tick, 'f')} sdk={sdk_version or 'unknown'}"
            ),
            (
                f"source={inputs.direction.source_side} LIMIT POST_ONLY "
                f"price={format(proposal.source_limit_price, 'f')} "
                f"notional={format(inputs.quantity * proposal.source_limit_price, 'f')}; "
                f"receiver={inputs.direction.receiver_side} MARKET IOC "
                f"worst_price={format(proposal.receiver_worst_price, 'f')} "
                f"notional={format(inputs.quantity * proposal.receiver_worst_price, 'f')}"
            ),
        )
    )


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


def _initial_packet(
    inputs: LocalAttemptInputs,
    *,
    proposal_provenance: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    unresolved = (
        _unresolved_automatic_provenance()
        if inputs.automatic_price_selection and proposal_provenance is None
        else proposal_provenance
    )
    provenance: dict[str, Any] = {
        "status": "PENDING" if unresolved is None else str(unresolved.get("status", "PENDING")),
        "source": (
            "one fresh public lighter-sdk metadata/order-book observation after owner launch"
            if inputs.automatic_price_selection
            else "fresh lighter-sdk orderBookDetails observation after owner launch"
        ),
        "metadata": None,
        "proposal": None if unresolved is None else dict(unresolved),
        "final": None,
    }
    if unresolved is not None:
        # Keep the historical proposal/final slots present while making it
        # explicit that automatic prices are unresolved before LAUNCH.
        provenance["metadata"] = unresolved.get("metadata")
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
        "provenance": provenance,
        "execution": _execution_packet(EXECUTION_STATE_NOT_STARTED),
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
    reason_code: str | None = None
    execution_state: str = EXECUTION_STATE_UNKNOWN

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
                "reason_code": self.reason_code,
                "execution_state": self.execution_state,
                "known_pre_execution_stop": self.execution_state == EXECUTION_STATE_PRE_EXECUTION_STOP,
                "preview": None if self.preview is None else dict(self.preview),
                "provenance": None if self.provenance is None else dict(self.provenance),
                "result": None if self.result is None else self.result.as_dict(),
            }
        )


def format_local_attempt_result(result: LocalAttemptResult) -> str:
    """Render a concise terminal line without dumping the result payload."""

    lines = [
        (
            f"LOCAL_ATTEMPT status={result.status} terminal={result.terminal_status} "
            f"execution_state={result.execution_state} exit_code={result.exit_code}"
        )
    ]
    if result.reason is not None:
        code = result.reason_code or "UNCLASSIFIED"
        lines.append(f"reason={code}: {result.reason}")
    if result.result is not None:
        engine = result.result
        lines.append(
            f"engine outcome={engine.outcome.value} phase={engine.phase.value}"
        )
        if engine.reason is not None:
            lines.append(f"engine_reason={sanitize(engine.reason)}")
        if engine.unknown_reasons:
            unknown = "; ".join(str(sanitize(item)) for item in engine.unknown_reasons)
            lines.append(f"engine_unknown_reasons={unknown}")
        inventory: list[str] = []
        for label, leg in (("source", engine.source), ("receiver", engine.receiver)):
            if leg is None:
                continue
            position = "UNKNOWN" if leg.position_after is None else format(leg.position_after, "f")
            inventory.append(
                f"{label}:position_after={position},filled={format(leg.filled_quantity, 'f')}"
            )
        lines.append(
            "inventory=" + ("; ".join(inventory) if inventory else "UNKNOWN")
        )
    if result.packet_path is not None:
        lines.append(f"packet={result.packet_path}")
    if result.journal_path is not None:
        lines.append(f"journal={result.journal_path}")
    return "\n".join(lines)


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
    # Prompt or read both credentials before the final metadata/book capture.
    # A delayed hidden-key prompt therefore cannot age that fresh observation.
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
    """Preview or launch one bounded local attempt.

    Automatic mode keeps prices unresolved while the operator reviews the
    fixed accounts, direction, quantity, and one-tick rule.  After the exact
    launch token, keys are loaded and one fresh public metadata/book
    observation selects both prices.  The explicit-price path keeps its
    existing order and packet shape.
    """

    preview = preview_payload(inputs)
    if output_fn is not None:
        output_fn(format_local_attempt_preview(inputs))
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
            reason_code=LOCAL_REASON_ATTEMPT_DIRECTORY,
            execution_state=EXECUTION_STATE_NOT_STARTED,
        )
    if not execute:
        return LocalAttemptResult(
            status="PREVIEW",
            exit_code=0,
            packet_path=None,
            journal_path=None,
            terminal_status="NOT_STARTED",
            preview=preview,
            execution_state=EXECUTION_STATE_NOT_STARTED,
        )

    effective_clock = clock or SystemClock()
    active_inputs = inputs
    proposal: AutomaticPriceProposal | None = None
    proposal_provenance: dict[str, Any] | None = (
        _unresolved_automatic_provenance()
        if inputs.automatic_price_selection
        else None
    )

    try:
        response = _prompt(
            input_fn,
            f"Type {LAUNCH_TOKEN} to launch this one paired-opening attempt: ",
            "launch confirmation",
        )
    except LocalAttemptInputError as exc:
        return LocalAttemptResult(
            status="CANCELLED",
            exit_code=0,
            packet_path=None,
            journal_path=None,
            terminal_status="NOT_STARTED",
            reason=_local_failure_message(LOCAL_REASON_OPERATOR_CANCELLED),
            preview=preview,
            provenance=_compact_provenance(proposal_provenance),
            reason_code=LOCAL_REASON_OPERATOR_CANCELLED,
            execution_state=EXECUTION_STATE_NOT_STARTED,
        )
    if response != LAUNCH_TOKEN:
        return LocalAttemptResult(
            status="CANCELLED",
            exit_code=0,
            packet_path=None,
            journal_path=None,
            terminal_status="NOT_STARTED",
            reason=_local_failure_message(LOCAL_REASON_OPERATOR_CANCELLED),
            preview=preview,
            provenance=_compact_provenance(proposal_provenance),
            reason_code=LOCAL_REASON_OPERATOR_CANCELLED,
            execution_state=EXECUTION_STATE_NOT_STARTED,
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
            provenance=_compact_provenance(proposal_provenance),
            reason_code=LOCAL_REASON_ATTEMPT_DIRECTORY,
            execution_state=EXECUTION_STATE_NOT_STARTED,
        )
    packet_path = attempt_dir / PACKET_NAME
    journal_path = attempt_dir / JOURNAL_NAME
    try:
        initial = _initial_packet(
            active_inputs,
            proposal_provenance=proposal_provenance,
        )
        _atomic_json(packet_path, initial)
        _atomic_json(
            attempt_dir / EXIT_STATUS_NAME,
            {
                "status": "PENDING",
                "exit_code": None,
                "terminal_result": "MISSING",
                "execution_state": EXECUTION_STATE_NOT_STARTED,
            },
        )
    except Exception as exc:
        safe_error = sanitize_exception(exc)
        return LocalAttemptResult(
            status="INCOMPLETE",
            exit_code=2,
            packet_path=packet_path if packet_path.exists() else None,
            journal_path=journal_path,
            terminal_status="MISSING",
            reason=_local_failure_message(LOCAL_REASON_INITIAL_PACKET, safe_error),
            preview=preview,
            provenance=_compact_provenance(proposal_provenance),
            reason_code=LOCAL_REASON_INITIAL_PACKET,
            execution_state=EXECUTION_STATE_PRE_EXECUTION_STOP,
        )

    secrets: SecretProvider | None = None
    reader: Any | None = None
    execution_client: Any | None = None
    stage = "secret_input"
    packet = initial
    metadata_provenance: dict[str, Any] | None = None
    final_provenance: dict[str, Any] | None = None
    result_provenance: Mapping[str, Any] | None = _compact_provenance(proposal_provenance)
    selection_complete = False
    config: HandoffConfig | None = None
    terminal_recorded = False
    result: HandoffResult | None = None
    exit_code: int | None = None
    try:
        # This is the first point at which either selected private key may be
        # requested.  Automatic mode deliberately resolves its price only
        # after these keys have been loaded and the owner has confirmed LAUNCH.
        secrets = secret_provider_factory(active_inputs)
        _prime_secrets(secrets, active_inputs)
        stage = "market_quote_selection" if active_inputs.automatic_price_selection else "market_metadata"
        if active_inputs.automatic_price_selection:
            # Establish an honest unavailable final observation before any
            # post-launch read.  A partial read or malformed response must
            # still leave a diagnosable final slot in the failure packet.
            final_provenance = {
                "status": "POST_LAUNCH_SELECTION_PENDING",
                "selection": "one fresh public metadata/book read after LAUNCH",
                "metadata": None,
                "book": None,
                "prices": None,
            }
            result_provenance = {
                "proposal": _compact_provenance(proposal_provenance),
                "final": _compact_provenance(final_provenance),
            }
        reader = (market_reader_factory or default_market_reader_factory)(active_inputs, secrets)
        raw_metadata = await reader.resolve_market(active_inputs.market_symbol)
        metadata_provenance = (
            _safe_metadata_provenance(
                raw_metadata,
                sdk_version=getattr(reader, "sdk_version", None),
            )
            if active_inputs.automatic_price_selection
            else _metadata_provenance(
                raw_metadata,
                sdk_version=getattr(reader, "sdk_version", None),
            )
        )
        if not active_inputs.automatic_price_selection:
            result_provenance = metadata_provenance
        elif final_provenance is not None:
            final_provenance["metadata"] = metadata_provenance
            packet = {
                **packet,
                "provenance": {
                    **packet.get("provenance", {}),
                    "status": "FINAL_REVALIDATION_PENDING",
                    "metadata": metadata_provenance,
                    "proposal": proposal_provenance,
                    "final": final_provenance,
                },
            }
        metadata = _as_market_metadata(
            raw_metadata,
            sdk_version=getattr(reader, "sdk_version", None),
        )
        if metadata.market_id < 0 or metadata.symbol.upper() != active_inputs.market_symbol.upper():
            raise ContractError("fresh market identity does not match operator symbol")

        if active_inputs.automatic_price_selection:
            raw_final_book = await _read_public_book(reader, metadata.market_id)
            assert final_provenance is not None
            final_provenance["book"] = _safe_book_provenance(
                raw_final_book,
                sdk_version=getattr(reader, "sdk_version", None),
            )
            result_provenance = {
                "proposal": _compact_provenance(proposal_provenance),
                "final": _compact_provenance(final_provenance),
            }
            # Keep an intermediate final-book observation in the packet so a
            # stale, crossed, or otherwise invalid revalidation remains
            # diagnosable even though no engine client is created.
            packet = {
                **packet,
                "provenance": {
                    **packet.get("provenance", {}),
                    "status": "FINAL_REVALIDATION_PENDING",
                    "metadata": metadata_provenance,
                    "proposal": proposal_provenance,
                    "final": final_provenance,
                },
            }
            final_book = _as_order_book_snapshot(raw_final_book, metadata)
            selected_now = _clock_now(effective_clock)
            proposal = select_automatic_prices(
                active_inputs.direction,
                metadata,
                final_book,
                quantity=active_inputs.quantity,
                now=selected_now,
                freshness_seconds=active_inputs.freshness_seconds,
            )
            final_provenance["prices"] = proposal.as_dict()
            final_provenance["status"] = "POST_LAUNCH_SELECTED"
            active_inputs = replace(
                active_inputs,
                source_limit_price=proposal.source_limit_price,
                receiver_worst_price=proposal.receiver_worst_price,
                automatic_price_selection_requested=True,
            )
            result_provenance = {
                "proposal": _compact_provenance(proposal_provenance),
                "final": _compact_provenance(final_provenance),
            }
            selection_complete = True
            packet = {
                **packet,
                "provenance": {
                    **packet.get("provenance", {}),
                    # Keep the HCR-11 status spelling for packet consumers;
                    # the final slot now records the one post-LAUNCH selection.
                    "status": "FINAL_REVALIDATED",
                    "metadata": metadata_provenance,
                    "proposal": proposal_provenance,
                    "final": final_provenance,
                },
            }

        config = active_inputs.handoff_config(metadata.market_id)
        packet_provenance: dict[str, Any] = {
            "status": "OBSERVED" if not active_inputs.automatic_price_selection else (
                "FINAL_REVALIDATED" if selection_complete else "FINAL_REVALIDATION_FAILED"
            ),
            "source": (
                "one fresh public metadata/order-book selection after owner launch"
                if active_inputs.automatic_price_selection
                else "fresh lighter-sdk orderBookDetails observation after owner launch"
            ),
            "metadata": metadata_provenance,
            "proposal": proposal_provenance,
            "final": final_provenance,
        }
        packet = {
            **packet,
            "config": _config_packet(active_inputs, config),
            "provenance": packet_provenance,
            "execution": _execution_packet(EXECUTION_STATE_READY),
        }
        # Persist the selected price and its source observation before entering
        # the engine.  The packet is the durable evidence barrier for this
        # launcher; no repricing or retry is possible after it is written.
        stage = "packet"
        _atomic_json(packet_path, packet)
        # A process can be interrupted after this point, including while an
        # engine client or its first dispatch is being admitted.  Persist an
        # UNKNOWN marker before that admission so a surviving packet never
        # claims that the engine was not reached.
        _atomic_json(
            active_inputs.exit_status_path,
            {
                "status": "PENDING",
                "exit_code": None,
                "terminal_result": "MISSING",
                "execution_state": EXECUTION_STATE_UNKNOWN,
            },
        )
        packet = {
            **packet,
            "execution": _execution_packet(EXECUTION_STATE_UNKNOWN),
        }
        _atomic_json(packet_path, packet)
        if active_inputs.automatic_price_selection and proposal is not None and output_fn is not None:
            output_fn(
                format_local_attempt_selection(
                    inputs,
                    proposal,
                    sdk_version=getattr(reader, "sdk_version", None),
                )
            )
        await _close_resource(reader)
        reader = None
        stage = "engine"
        execution_client = (execution_client_factory or default_execution_client_factory)(
            config,
            active_inputs,
            secrets,
        )
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
        _atomic_json(active_inputs.terminal_result_path, terminal_payload)
        terminal_recorded = True
        packet = {
            **packet,
            "execution": _execution_packet(EXECUTION_STATE_TERMINAL_RECORDED),
            "terminal": {
                "status": "RECORDED",
                "result_file": str(active_inputs.terminal_result_path),
                "outcome": result.outcome.value,
                "phase": result.phase.value,
                "run_id": result.run_id,
            },
            "exit_status": {
                "status": "RECORDED",
                "file": str(active_inputs.exit_status_path),
                "exit_code": exit_code,
            },
        }
        packet_write_error: str | None = None
        try:
            _atomic_json(
                active_inputs.exit_status_path,
                {
                    "packet_version": PACKET_VERSION,
                    "status": "RECORDED",
                    "exit_code": exit_code,
                    "terminal_result": "RECORDED",
                    "outcome": result.outcome.value,
                    "execution_state": EXECUTION_STATE_TERMINAL_RECORDED,
                },
            )
            _atomic_json(packet_path, packet)
        except Exception as exc:
            # The terminal result is already durable.  Preserve it and report
            # packet finalization separately instead of replacing it with a
            # fabricated MISSING result.
            packet_write_error = sanitize_exception(exc)
        if packet_write_error is not None:
            try:
                _atomic_json(
                    active_inputs.exit_status_path,
                    {
                        "packet_version": PACKET_VERSION,
                        "status": "INCOMPLETE",
                        "exit_code": 2,
                    "terminal_result": "RECORDED",
                    "diagnostic_finalization": "INCOMPLETE",
                    "error_class": packet_write_error,
                    "reason_code": LOCAL_REASON_PACKET_FINALIZATION,
                    "execution_state": EXECUTION_STATE_TERMINAL_RECORDED,
                },
                )
            except Exception:
                pass
            return LocalAttemptResult(
                status="INCOMPLETE",
                exit_code=2,
                packet_path=packet_path,
                journal_path=journal_path,
                terminal_status="RECORDED",
                result=result,
                reason=(
                    _local_failure_message(
                        LOCAL_REASON_PACKET_FINALIZATION,
                        packet_write_error,
                    )
                ),
                preview=preview,
                provenance=result_provenance,
                reason_code=LOCAL_REASON_PACKET_FINALIZATION,
                execution_state=EXECUTION_STATE_TERMINAL_RECORDED,
            )
        return LocalAttemptResult(
            status="COMPLETED",
            exit_code=exit_code,
            packet_path=packet_path,
            journal_path=journal_path,
            terminal_status="RECORDED",
            result=result,
            reason=None,
            preview=preview,
            provenance=result_provenance,
            execution_state=EXECUTION_STATE_TERMINAL_RECORDED,
        )
    except Exception as exc:
        safe_error = sanitize_exception(exc)
        if terminal_recorded and result is not None and exit_code is not None:
            return LocalAttemptResult(
                status="INCOMPLETE",
                exit_code=2,
                packet_path=packet_path,
                journal_path=journal_path,
                terminal_status="RECORDED",
                result=result,
                reason=_local_failure_message(
                    LOCAL_REASON_PACKET_FINALIZATION,
                    safe_error,
                ),
                preview=preview,
                provenance=result_provenance,
                reason_code=LOCAL_REASON_PACKET_FINALIZATION,
                execution_state=EXECUTION_STATE_TERMINAL_RECORDED,
            )
        reason_code = _classify_local_error(exc, stage=stage) or LOCAL_REASON_TERMINAL_MISSING
        execution_state = (
            EXECUTION_STATE_UNKNOWN
            if stage == "engine"
            else EXECUTION_STATE_PRE_EXECUTION_STOP
        )
        if active_inputs.automatic_price_selection and final_provenance is not None:
            result_provenance = {
                "proposal": _compact_provenance(proposal_provenance),
                "final": _compact_provenance(final_provenance),
            }
            packet = {
                **packet,
                "provenance": {
                    **packet.get("provenance", {}),
                    "status": (
                        "FINAL_REVALIDATED"
                        if selection_complete
                        else "FINAL_REVALIDATION_FAILED"
                    ),
                    "metadata": metadata_provenance,
                    "proposal": proposal_provenance,
                    "final": final_provenance,
                },
            }
        missing = {
            "packet_version": PACKET_VERSION,
            "terminal_status": "MISSING",
            "terminal_result": None,
            "error_phase": stage,
            "error_class": safe_error,
            "reason_code": reason_code,
            "execution_state": execution_state,
            "reason": "operation stopped before a terminal engine result was recorded; inspect the journal and known inventory",
        }
        try:
            _atomic_json(active_inputs.terminal_result_path, missing)
            _atomic_json(
                active_inputs.exit_status_path,
                {
                    "packet_version": PACKET_VERSION,
                    "status": "INCOMPLETE",
                    "exit_code": 2,
                    "terminal_result": "MISSING",
                    "error_phase": stage,
                    "error_class": safe_error,
                    "reason_code": reason_code,
                    "execution_state": execution_state,
                },
            )
            packet = {
                **packet,
                "execution": _execution_packet(execution_state, reason_code),
                "terminal": {
                    "status": "MISSING",
                    "result_file": str(active_inputs.terminal_result_path),
                    "error_phase": stage,
                    "error_class": safe_error,
                    "reason_code": reason_code,
                    "execution_state": execution_state,
                    "reason": "terminal engine result is missing; packet is incomplete",
                },
                "exit_status": {
                    "status": "INCOMPLETE",
                    "file": str(active_inputs.exit_status_path),
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
            reason=(
                _local_failure_message(reason_code, safe_error)
                if execution_state == EXECUTION_STATE_PRE_EXECUTION_STOP
                else _local_failure_message(LOCAL_REASON_TERMINAL_MISSING, safe_error)
            ),
            preview=preview,
            provenance=result_provenance,
            reason_code=reason_code,
            execution_state=execution_state,
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
    "AutomaticPriceProposal",
    "EXECUTION_STATE_NOT_STARTED",
    "EXECUTION_STATE_PRE_EXECUTION_STOP",
    "EXECUTION_STATE_READY",
    "EXECUTION_STATE_TERMINAL_RECORDED",
    "EXECUTION_STATE_UNKNOWN",
    "DEFAULT_FRESHNESS_SECONDS",
    "DEFAULT_MAX_POLL_COUNT",
    "DEFAULT_ORDER_TIMEOUT_SECONDS",
    "DEFAULT_POLL_INTERVAL_SECONDS",
    "DEFAULT_RECONCILE_TIMEOUT_SECONDS",
    "DEFAULT_REQUEST_TIMEOUT_SECONDS",
    "DEFAULT_SOURCE_ORDER_LIFETIME_SECONDS",
    "EXIT_STATUS_NAME",
    "JOURNAL_NAME",
    "LAUNCH_TOKEN",
    "LOCAL_REASON_ATTEMPT_DIRECTORY",
    "LOCAL_REASON_INITIAL_PACKET",
    "LOCAL_REASON_INVALID_OBSERVATION",
    "LOCAL_REASON_OPERATOR_CANCELLED",
    "LOCAL_REASON_PACKET_FINALIZATION",
    "LOCAL_REASON_PACKET_WRITE",
    "LOCAL_REASON_PRE_EXECUTION",
    "LOCAL_REASON_PROPOSAL_EXPIRED",
    "LOCAL_REASON_PUBLIC_READ_FAILED",
    "LOCAL_REASON_PRICE_CHANGED",
    "LOCAL_REASON_STALE_OBSERVATION",
    "LOCAL_REASON_TERMINAL_MISSING",
    "LocalAttemptInputError",
    "LocalAttemptInputs",
    "LocalAttemptResult",
    "PACKET_NAME",
    "TERMINAL_RESULT_NAME",
    "collect_local_attempt_inputs",
    "derive_automatic_prices",
    "default_execution_client_factory",
    "default_market_reader_factory",
    "format_local_attempt_preview",
    "format_local_attempt_result",
    "format_local_attempt_selection",
    "preview_payload",
    "run_local_attempt",
    "select_automatic_prices",
]
