"""Isolated Lighter testnet Level-C lifecycle.

This module is intentionally not imported by the normal PAPER runtime.  It
contains only the Lighter-specific testnet state machine and a narrow SDK
dispatch boundary.  Production use requires an explicit testnet execution
mode supplied by the caller; the default modes only validate or prepare
synthetic plans.

The durable store records public identity, order intent, nonces, digests,
sanitized outcomes, and bounded evidence summaries.  It never records API
private keys, authorization tokens, signed transaction JSON, signatures, or
raw private responses.
"""

from __future__ import annotations

import argparse
import asyncio
from contextlib import contextmanager
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_DOWN
from enum import Enum
import hashlib
import importlib
import inspect
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
import stat
import time
from typing import Any, Awaitable, Callable, Mapping, Protocol, Sequence
from uuid import uuid4

from .lighter_testnet_readiness import (
    EXPECTED_L1_ADDRESS,
    FIRST_USER_API_KEY_INDEX,
    BootstrapAsset,
    MarketContract,
    ReadinessResult,
    TESTNET_BOOTSTRAP_ASSET_BASELINE,
    canonical_api_public_key,
    canonical_testnet_address,
)


TESTNET_API_URL = "https://testnet.zklighter.elliot.ai"
TESTNET_CHAIN_ID = 300
LIGHTER_ACCOUNT_INDEX = 202
LIGHTER_API_KEY_INDEX = FIRST_USER_API_KEY_INDEX
LIGHTER_SYMBOL = "LIT"
LIGHTER_SDK_COMMIT = "ebc50660efc99f31e4055418e4514255456cb060"
LIGHTER_API_KEY_PRIVATE_PATH = Path(
    "/Users/daniilmakarov/.config/risex-farmer/lighter-testnet/api-key-4-private"
)
LIGHTER_IDENTITY_PATH = Path(
    "/Users/daniilmakarov/.config/risex-farmer/lighter-testnet/identity.json"
)

HOURLY_FUNDING_INTERVAL_MS = 3_600_000
MAX_PAGES = 256
MAX_PRIVATE_FILE_BYTES = 8192
MAX_PUBLIC_IDENTITY_BYTES = 32_768
MAX_WIRE_INTEGER = (1 << 63) - 1
LIGHTER_MAX_CLIENT_ORDER_INDEX = (1 << 48) - 1
MAX_MARKET_DECIMALS = 18
TERMINAL_ROUND_MAX_AGE_MS = 10_000
SDK_READ_TIMEOUT_SECONDS = 30.0
FUNDING_SETTLEMENT_WINDOW_SECONDS = 30.0
FUNDING_POLL_INTERVAL_SECONDS = 2.0

_HEX_HASH = re.compile(r"^0x[0-9a-fA-F]{64}$")
_SAFE_ID = re.compile(r"^[A-Za-z0-9._:-]{1,160}$")
_SENSITIVE_KEYS = frozenset(
    {
        "private_key",
        "api_key_private",
        "secret",
        "token",
        "auth_token",
        "signature",
        "signed_tx_info",
        "tx_info",
        "txinfo",
        "response",
        "wallet_private_key",
    }
)


class LifecycleHalt(RuntimeError):
    """A fail-closed lifecycle stop that must not be retried automatically."""

    def __init__(self, reason: str, *, failure_class: str = "SAFETY") -> None:
        super().__init__(reason)
        self.reason = reason
        self.failure_class = failure_class


class AmbiguousDispatch(LifecycleHalt):
    """The send outcome is unknown; replay is forbidden."""

    def __init__(self, reason: str = "AMBIGUOUS_DISPATCH") -> None:
        super().__init__(reason, failure_class="TRANSPORT")


class RunMode(str, Enum):
    DRY_RUN = "DRY_RUN"
    PREPARE_ONLY = "PREPARE_ONLY"
    TESTNET_WRITE = "TESTNET_WRITE"


class IntentKind(str, Enum):
    MAKER_PLACE = "MAKER_PLACE"
    MAKER_CANCEL = "MAKER_CANCEL"
    OPEN = "OPEN"
    CLOSE = "CLOSE"


class IntentState(str, Enum):
    PREPARED = "PREPARED"
    DISPATCHING = "DISPATCHING"
    DISPATCHED = "DISPATCHED"
    REJECTED = "REJECTED"
    RECONCILED = "RECONCILED"
    AMBIGUOUS = "AMBIGUOUS"


class RunnerResult(str, Enum):
    PREPARED = "PREPARED"
    COMPLETE = "COMPLETE"
    BLOCKED = "BLOCKED"
    HALTED_MANUAL_RECOVERY = "HALTED_MANUAL_RECOVERY"


def _decimal(value: Any, label: str, *, positive: bool = False) -> Decimal:
    if isinstance(value, bool):
        raise LifecycleHalt(f"{label}_INVALID", failure_class="SCHEMA")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise LifecycleHalt(f"{label}_INVALID", failure_class="SCHEMA")
    if not result.is_finite() or (positive and result <= 0):
        raise LifecycleHalt(f"{label}_INVALID", failure_class="SCHEMA")
    return result


def _int(value: Any, label: str, *, nonnegative: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise LifecycleHalt(f"{label}_INVALID", failure_class="SCHEMA")
    if nonnegative and value < 0:
        raise LifecycleHalt(f"{label}_INVALID", failure_class="SCHEMA")
    return value


def _text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise LifecycleHalt(f"{label}_INVALID", failure_class="SCHEMA")
    return value


def _grid(value: Decimal, step: Decimal) -> bool:
    if step <= 0:
        return False
    return (value / step).to_integral_value(rounding=ROUND_DOWN) == value / step


def _ceil_grid(value: Decimal, step: Decimal) -> Decimal:
    units = (value / step).to_integral_value(rounding=ROUND_CEILING)
    return units * step


def _market_decimals(value: Any, label: str) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < 0
        or value > MAX_MARKET_DECIMALS
    ):
        raise LifecycleHalt(f"{label}_INVALID", failure_class="SCHEMA")
    return value


def _wire_units(value: Any, decimals: Any, label: str) -> int:
    """Convert a decimal order field using the freshly observed market scale."""

    places = _market_decimals(decimals, f"{label}_DECIMALS")
    amount = _decimal(value, label, positive=True)
    try:
        scaled = amount * (Decimal(10) ** places)
        integral = scaled.to_integral_value(rounding=ROUND_DOWN)
    except (ArithmeticError, InvalidOperation):
        raise LifecycleHalt(f"{label}_WIRE_CONVERSION_INVALID", failure_class="SCHEMA") from None
    if integral != scaled or integral <= 0 or integral > MAX_WIRE_INTEGER:
        raise LifecycleHalt(f"{label}_WIRE_CONVERSION_INVALID", failure_class="SAFETY")
    return int(integral)


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _safe_hash(value: Any) -> str | None:
    if isinstance(value, str) and _HEX_HASH.fullmatch(value):
        return value.lower()
    return None


def _assert_public_json(value: Any) -> None:
    """Reject sensitive-looking values before anything reaches the journal."""

    if isinstance(value, bytes) or isinstance(value, bytearray):
        raise LifecycleHalt("PRIVATE_BYTES_NOT_PERSISTABLE", failure_class="SAFETY")
    if isinstance(value, Mapping):
        for key, child in value.items():
            if isinstance(key, str) and key.lower() in _SENSITIVE_KEYS:
                raise LifecycleHalt("SENSITIVE_FIELD_NOT_PERSISTABLE", failure_class="SAFETY")
            _assert_public_json(child)
    elif isinstance(value, (list, tuple)):
        for child in value:
            _assert_public_json(child)


@dataclass(frozen=True)
class IdentityMetadata:
    """Public identity metadata loaded from the fixed protected JSON path."""

    l1_address: str
    account_index: int
    api_key_index: int
    api_key_public_key: str
    sdk_commit: str

    def __post_init__(self) -> None:
        try:
            address = canonical_testnet_address(self.l1_address)
        except (TypeError, ValueError):
            raise LifecycleHalt("L1_IDENTITY_MISMATCH", failure_class="IDENTITY") from None
        if address != EXPECTED_L1_ADDRESS:
            raise LifecycleHalt("L1_IDENTITY_MISMATCH")
        if self.account_index != LIGHTER_ACCOUNT_INDEX:
            raise LifecycleHalt("ACCOUNT_INDEX_MISMATCH")
        if self.api_key_index != LIGHTER_API_KEY_INDEX:
            raise LifecycleHalt("API_KEY_INDEX_MISMATCH")
        try:
            public_key = canonical_api_public_key(self.api_key_public_key)
        except (TypeError, ValueError):
            raise LifecycleHalt("API_PUBLIC_KEY_INVALID", failure_class="AUTH") from None
        if public_key != self.api_key_public_key:
            raise LifecycleHalt("API_PUBLIC_KEY_NOT_CANONICAL")
        if self.sdk_commit != LIGHTER_SDK_COMMIT:
            raise LifecycleHalt("SDK_PIN_MISMATCH")

    def evidence(self) -> dict[str, Any]:
        return {
            "account_index": self.account_index,
            "api_key_index": self.api_key_index,
            "api_key_public_key": self.api_key_public_key,
            "l1_address": self.l1_address,
            "sdk_commit": self.sdk_commit,
        }


def _protected_file_metadata(path: Path, *, max_bytes: int) -> tuple[bool, str]:
    try:
        parent = os.lstat(path.parent)
    except FileNotFoundError:
        return False, "DIRECTORY_MISSING"
    except OSError:
        return False, "DIRECTORY_METADATA_UNAVAILABLE"
    if not stat.S_ISDIR(parent.st_mode) or parent.st_uid != os.getuid():
        return False, "DIRECTORY_OWNER_OR_TYPE_INVALID"
    if stat.S_IMODE(parent.st_mode) != 0o700:
        return False, "DIRECTORY_MODE_INVALID"
    try:
        item = os.lstat(path)
    except FileNotFoundError:
        return False, "FILE_MISSING"
    except OSError:
        return False, "FILE_METADATA_UNAVAILABLE"
    if not stat.S_ISREG(item.st_mode) or item.st_uid != os.getuid():
        return False, "FILE_OWNER_OR_TYPE_INVALID"
    if stat.S_IMODE(item.st_mode) != 0o600:
        return False, "FILE_MODE_INVALID"
    if item.st_nlink != 1:
        return False, "FILE_LINK_COUNT_INVALID"
    if item.st_size < 1 or item.st_size > max_bytes:
        return False, "FILE_SIZE_INVALID"
    return True, "PROTECTED_FIXED_FILE_VALID"


def load_identity_metadata(path: str | Path = LIGHTER_IDENTITY_PATH) -> IdentityMetadata:
    target = Path(path)
    if target != LIGHTER_IDENTITY_PATH:
        raise LifecycleHalt("FIXED_IDENTITY_PATH_REQUIRED")
    ok, reason = _protected_file_metadata(target, max_bytes=MAX_PUBLIC_IDENTITY_BYTES)
    if not ok:
        raise LifecycleHalt(f"IDENTITY_FILE_{reason}")
    try:
        with target.open("r", encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, ValueError, json.JSONDecodeError):
        raise LifecycleHalt("IDENTITY_FILE_SCHEMA_INVALID", failure_class="SCHEMA")
    if not isinstance(value, Mapping):
        raise LifecycleHalt("IDENTITY_FILE_SCHEMA_INVALID", failure_class="SCHEMA")
    try:
        _assert_public_json(value)
    except LifecycleHalt:
        raise LifecycleHalt("IDENTITY_FILE_CONTAINS_SECRET", failure_class="SAFETY")
    required = {"l1_address", "account_index", "api_key_index", "sdk_commit"}
    if not required <= set(value):
        raise LifecycleHalt("IDENTITY_FILE_SCHEMA_INVALID", failure_class="SCHEMA")
    public_key_values: dict[str, str] = {}
    for field_name in ("api_key_public", "api_key_public_key"):
        if field_name in value:
            try:
                public_key_values[field_name] = canonical_api_public_key(value[field_name])
            except (TypeError, ValueError):
                raise LifecycleHalt("IDENTITY_FILE_SCHEMA_INVALID", failure_class="SCHEMA")
    if not public_key_values:
        raise LifecycleHalt("IDENTITY_FILE_SCHEMA_INVALID", failure_class="SCHEMA")
    if len(public_key_values) == 2 and len(set(public_key_values.values())) != 1:
        raise LifecycleHalt("IDENTITY_FILE_API_PUBLIC_CONTRADICTORY", failure_class="IDENTITY")
    api_key_public_key = next(iter(public_key_values.values()))
    try:
        return IdentityMetadata(
            l1_address=value["l1_address"],
            account_index=value["account_index"],
            api_key_index=value["api_key_index"],
            api_key_public_key=api_key_public_key,
            sdk_commit=value["sdk_commit"],
        )
    except (TypeError, ValueError):
        raise LifecycleHalt("IDENTITY_FILE_SCHEMA_INVALID", failure_class="SCHEMA")


def load_api_key_private(path: str | Path = LIGHTER_API_KEY_PRIVATE_PATH) -> str:
    """Load the fixed API private key only for an injected SDK client.

    The returned value must stay in memory inside the SDK client.  This helper
    never logs, serializes, hashes, or writes the value.
    """

    target = Path(path)
    if target != LIGHTER_API_KEY_PRIVATE_PATH:
        raise LifecycleHalt("FIXED_API_KEY_PATH_REQUIRED")
    ok, reason = _protected_file_metadata(target, max_bytes=MAX_PRIVATE_FILE_BYTES)
    if not ok:
        raise LifecycleHalt(f"API_KEY_FILE_{reason}")
    try:
        value = target.read_text(encoding="ascii").strip()
    except (OSError, UnicodeDecodeError):
        raise LifecycleHalt("API_KEY_FILE_UNREADABLE", failure_class="AUTH")
    if not value or len(value) > MAX_PRIVATE_FILE_BYTES:
        raise LifecycleHalt("API_KEY_FILE_INVALID", failure_class="AUTH")
    return value


def validate_readiness(
    readiness: ReadinessResult,
    identity: IdentityMetadata,
    *,
    expected_api_public_key: str | None = None,
) -> None:
    if readiness.status != "READY" or readiness.failure_class != "NONE":
        raise LifecycleHalt("LEVEL_B_READINESS_NOT_READY", failure_class="SAFETY")
    if readiness.write_capable or readiness.write_authority != "NO_TESTNET_WRITE_AUTHORITY":
        raise LifecycleHalt("READINESS_WRITE_AUTHORITY_INVALID")
    if readiness.wallet_address.lower() != identity.l1_address.lower():
        raise LifecycleHalt("READINESS_L1_IDENTITY_MISMATCH", failure_class="IDENTITY")
    if readiness.account_index != identity.account_index:
        raise LifecycleHalt("READINESS_ACCOUNT_INDEX_MISMATCH", failure_class="IDENTITY")
    if readiness.api_key_index != LIGHTER_API_KEY_INDEX:
        raise LifecycleHalt("READINESS_API_KEY_INDEX_MISMATCH", failure_class="IDENTITY")
    if not all(
        (
            readiness.identity_verified,
            readiness.authorization_identity_verified,
            readiness.api_key_verified,
            readiness.api_key_public_key_verified,
            readiness.collateral_positive,
            readiness.fees_verified,
            readiness.active_orders_zero,
            readiness.positions_flat,
            readiness.unrelated_state_clear,
            readiness.trades_read,
            readiness.funding_history_read,
        )
    ):
        raise LifecycleHalt("LEVEL_B_SEMANTIC_GATES_INCOMPLETE", failure_class="SAFETY")
    if expected_api_public_key is not None and canonical_api_public_key(expected_api_public_key) != identity.api_key_public_key:
        raise LifecycleHalt("API_PUBLIC_KEY_EXPECTATION_MISMATCH", failure_class="AUTH")


@dataclass(frozen=True)
class BookLevel:
    price: Decimal
    quantity: Decimal

    def __post_init__(self) -> None:
        if self.price <= 0 or self.quantity <= 0:
            raise LifecycleHalt("BOOK_LEVEL_NOT_POSITIVE", failure_class="SCHEMA")


@dataclass(frozen=True)
class FundingSchedule:
    market_id: int
    symbol: str
    next_boundary_ms: int
    interval_ms: int
    rate: Decimal
    source: str
    observed_at_ms: int

    def validate(self, *, now_ms: int, require_future: bool = True) -> None:
        if self.market_id < 0 or self.symbol != LIGHTER_SYMBOL:
            raise LifecycleHalt("FUNDING_MARKET_IDENTITY_INVALID", failure_class="IDENTITY")
        if self.interval_ms != HOURLY_FUNDING_INTERVAL_MS:
            raise LifecycleHalt("FUNDING_INTERVAL_NOT_HOURLY", failure_class="SAFETY")
        if self.next_boundary_ms <= 0:
            raise LifecycleHalt("FUNDING_BOUNDARY_INVALID", failure_class="SCHEMA")
        if require_future and self.next_boundary_ms <= now_ms:
            raise LifecycleHalt("FUNDING_BOUNDARY_NOT_FUTURE", failure_class="SAFETY")
        if self.observed_at_ms > now_ms:
            raise LifecycleHalt("FUNDING_OBSERVATION_IN_FUTURE", failure_class="SCHEMA")
        _decimal(self.rate, "FUNDING_RATE")
        if not self.source:
            raise LifecycleHalt("FUNDING_SOURCE_MISSING", failure_class="SCHEMA")


@dataclass(frozen=True)
class MarketObservation:
    contract: MarketContract
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    funding: FundingSchedule
    observed_at_ms: int

    @property
    def market_id(self) -> int:
        return self.contract.market_id

    @property
    def size_step(self) -> Decimal:
        return Decimal(1).scaleb(-self.contract.size_decimals)

    @property
    def price_tick(self) -> Decimal:
        return Decimal(1).scaleb(-self.contract.price_decimals)

    @property
    def best_bid(self) -> Decimal:
        return max(level.price for level in self.bids)

    @property
    def best_ask(self) -> Decimal:
        return min(level.price for level in self.asks)

    def validate(
        self,
        *,
        now_ms: int,
        max_age_ms: int = 10_000,
        require_future_funding: bool = True,
    ) -> None:
        contract = self.contract
        if contract.symbol != LIGHTER_SYMBOL or contract.market_type != "perp":
            raise LifecycleHalt("LIT_PERPETUAL_CONTRACT_REQUIRED", failure_class="SAFETY")
        _market_decimals(contract.size_decimals, "SIZE_DECIMALS")
        _market_decimals(contract.price_decimals, "PRICE_DECIMALS")
        _market_decimals(contract.quote_decimals, "QUOTE_DECIMALS")
        if contract.status.lower() not in {"active", "1"} or contract.force_reduce_only:
            raise LifecycleHalt("MARKET_NOT_OPEN_FOR_ENTRY", failure_class="SAFETY")
        if contract.min_base_amount <= 0 or contract.min_quote_amount <= 0:
            raise LifecycleHalt("MARKET_MINIMUM_INVALID", failure_class="SCHEMA")
        if self.observed_at_ms > now_ms or now_ms - self.observed_at_ms > max_age_ms:
            raise LifecycleHalt("MARKET_OBSERVATION_STALE", failure_class="SAFETY")
        if not self.bids or not self.asks or self.best_bid >= self.best_ask:
            raise LifecycleHalt("TWO_SIDED_BOOK_REQUIRED", failure_class="SAFETY")
        tick = self.price_tick
        step = self.size_step
        for level in (*self.bids, *self.asks):
            if not _grid(level.price, tick) or not _grid(level.quantity, step):
                raise LifecycleHalt("BOOK_LEVEL_OFF_GRID", failure_class="SAFETY")
        self.funding.validate(now_ms=now_ms, require_future=require_future_funding)


@dataclass(frozen=True)
class FillSnapshot:
    trade_id: str
    order_index: int
    client_order_index: int
    account_index: int
    market_id: int
    quantity: Decimal
    price: Decimal
    is_ask: bool
    timestamp_ms: int

    def __post_init__(self) -> None:
        if not _SAFE_ID.fullmatch(self.trade_id):
            raise LifecycleHalt("TRADE_ID_INVALID", failure_class="SCHEMA")
        if self.quantity <= 0 or self.price <= 0:
            raise LifecycleHalt("FILL_NOT_POSITIVE", failure_class="SCHEMA")


@dataclass(frozen=True)
class OrderSnapshot:
    order_index: int
    client_order_index: int
    account_index: int
    market_id: int
    quantity: Decimal
    remaining_quantity: Decimal
    filled_quantity: Decimal
    filled_quote: Decimal
    price: Decimal
    is_ask: bool
    order_type: str
    time_in_force: str
    reduce_only: bool
    nonce: int
    status: str
    trigger: bool = False

    @property
    def active(self) -> bool:
        return self.status.upper() in {"OPEN", "ACTIVE", "PENDING", "IN-PROGRESS", "IN_PROGRESS"}


@dataclass(frozen=True)
class PositionSnapshot:
    account_index: int
    market_id: int
    signed_quantity: Decimal
    average_price: Decimal = Decimal(0)


@dataclass(frozen=True)
class AccountSnapshot:
    account_index: int
    l1_address: str
    collateral: Decimal
    maker_fee_tick: int
    taker_fee_tick: int
    orders: tuple[OrderSnapshot, ...]
    positions: tuple[PositionSnapshot, ...]
    fills: tuple[FillSnapshot, ...]
    funding_high_water: int
    unrelated_state_clear: bool
    observed_at_ms: int
    asset_count: int = 0

    @property
    def active_regular_orders(self) -> tuple[OrderSnapshot, ...]:
        return tuple(order for order in self.orders if order.active and not order.trigger)

    @property
    def active_trigger_orders(self) -> tuple[OrderSnapshot, ...]:
        return tuple(order for order in self.orders if order.active and order.trigger)

    def exact_position(self, market_id: int) -> PositionSnapshot | None:
        rows = [row for row in self.positions if row.market_id == market_id]
        if len(rows) > 1:
            raise LifecycleHalt("DUPLICATE_MARKET_POSITION", failure_class="SAFETY")
        return rows[0] if rows else None

    def validate_identity_and_safety(
        self,
        identity: IdentityMetadata,
        *,
        market_id: int,
        require_flat: bool,
        observed_at_ms: int,
    ) -> None:
        if self.account_index != identity.account_index:
            raise LifecycleHalt("ACCOUNT_IDENTITY_MISMATCH", failure_class="IDENTITY")
        if self.l1_address.lower() != identity.l1_address.lower():
            raise LifecycleHalt("ACCOUNT_L1_IDENTITY_MISMATCH", failure_class="IDENTITY")
        if self.collateral <= 0:
            raise LifecycleHalt("COLLATERAL_NOT_POSITIVE", failure_class="SAFETY")
        if self.maker_fee_tick != 0 or self.taker_fee_tick != 0:
            raise LifecycleHalt("STANDARD_FEE_TICKS_NOT_ZERO", failure_class="SAFETY")
        if self.active_regular_orders or self.active_trigger_orders:
            raise LifecycleHalt("ACTIVE_ORDERS_PRESENT", failure_class="SAFETY")
        if not self.unrelated_state_clear:
            raise LifecycleHalt("UNRELATED_ACCOUNT_STATE", failure_class="SAFETY")
        if self.observed_at_ms > observed_at_ms:
            raise LifecycleHalt("ACCOUNT_OBSERVATION_IN_FUTURE", failure_class="SCHEMA")
        position = self.exact_position(market_id)
        if require_flat and position is not None and position.signed_quantity != 0:
            raise LifecycleHalt("POSITION_NOT_FLAT", failure_class="SAFETY")
        if require_flat:
            for other in self.positions:
                if other.market_id != market_id and other.signed_quantity != 0:
                    raise LifecycleHalt("UNRELATED_POSITION_STATE", failure_class="SAFETY")


@dataclass(frozen=True)
class FundingRecord:
    funding_id: int
    market_id: int
    timestamp_ms: int
    change: Decimal
    rate: Decimal
    position_size: Decimal
    position_side: str


@dataclass(frozen=True)
class FundingHistory:
    complete: bool
    baseline_high_water: int
    high_water: int
    records: tuple[FundingRecord, ...]

    def attributable(
        self,
        *,
        market_id: int,
        quantity: Decimal,
        is_ask: bool,
        boundary_ms: int,
    ) -> FundingRecord:
        if not self.complete:
            raise LifecycleHalt("FUNDING_HISTORY_INCOMPLETE", failure_class="SAFETY")
        if self.high_water < self.baseline_high_water:
            raise LifecycleHalt("FUNDING_HIGH_WATER_REGRESSED", failure_class="SAFETY")
        if any(row.funding_id <= self.baseline_high_water for row in self.records):
            raise LifecycleHalt("FUNDING_BASELINE_NOT_EXCLUDED", failure_class="SAFETY")
        if any(row.market_id != market_id for row in self.records):
            raise LifecycleHalt("FUNDING_UNRELATED_MARKET", failure_class="SAFETY")
        matches = [
            row
            for row in self.records
            if row.market_id == market_id
            and boundary_ms <= row.timestamp_ms < boundary_ms + HOURLY_FUNDING_INTERVAL_MS
        ]
        if len(matches) != 1:
            raise LifecycleHalt("FUNDING_RECORD_MISSING_OR_CONTRADICTORY", failure_class="SAFETY")
        row = matches[0]
        expected_side = "short" if is_ask else "long"
        if row.position_size != quantity or row.position_side.lower() != expected_side:
            raise LifecycleHalt("FUNDING_POSITION_IDENTITY_MISMATCH", failure_class="IDENTITY")
        _decimal(row.change, "FUNDING_CHANGE")
        _decimal(row.rate, "FUNDING_RATE")
        return row


@dataclass(frozen=True)
class TerminalRound:
    round_id: str
    digest: str
    account_index: int
    market_id: int
    observed_at_ms: int
    active_regular_orders: int
    active_trigger_orders: int
    signed_position: Decimal
    unrelated_state_clear: bool

    def validate(
        self,
        identity: IdentityMetadata,
        *,
        market_id: int,
        now_ms: int | None = None,
        max_age_ms: int = TERMINAL_ROUND_MAX_AGE_MS,
    ) -> None:
        if not _SAFE_ID.fullmatch(self.round_id):
            raise LifecycleHalt("TERMINAL_ROUND_ID_INVALID", failure_class="SCHEMA")
        if not _HEX_HASH.fullmatch(self.digest):
            raise LifecycleHalt("TERMINAL_DIGEST_INVALID", failure_class="SCHEMA")
        if self.observed_at_ms < 0:
            raise LifecycleHalt("TERMINAL_TIMESTAMP_INVALID", failure_class="SCHEMA")
        if now_ms is not None:
            if self.observed_at_ms > now_ms:
                raise LifecycleHalt("TERMINAL_TIMESTAMP_IN_FUTURE", failure_class="SCHEMA")
            if now_ms - self.observed_at_ms > max_age_ms:
                raise LifecycleHalt("TERMINAL_ROUND_STALE", failure_class="SAFETY")
        if self.account_index != identity.account_index or self.market_id != market_id:
            raise LifecycleHalt("TERMINAL_IDENTITY_MISMATCH", failure_class="IDENTITY")
        if self.active_regular_orders != 0 or self.active_trigger_orders != 0:
            raise LifecycleHalt("TERMINAL_ORDERS_NOT_ZERO", failure_class="SAFETY")
        if self.signed_position != 0:
            raise LifecycleHalt("TERMINAL_POSITION_NOT_FLAT", failure_class="SAFETY")
        if not self.unrelated_state_clear:
            raise LifecycleHalt("TERMINAL_UNRELATED_STATE", failure_class="SAFETY")


@dataclass(frozen=True)
class OrderRequest:
    market_id: int
    client_order_index: int
    quantity: Decimal
    price: Decimal
    is_ask: bool
    order_type: str
    time_in_force: str
    reduce_only: bool
    order_expiry: int
    size_decimals: int
    price_decimals: int

    def __post_init__(self) -> None:
        if (
            isinstance(self.market_id, bool)
            or not isinstance(self.market_id, int)
            or self.market_id < 0
            or isinstance(self.client_order_index, bool)
            or not isinstance(self.client_order_index, int)
            or not 0 < self.client_order_index <= LIGHTER_MAX_CLIENT_ORDER_INDEX
        ):
            raise LifecycleHalt("ORDER_ID_INVALID", failure_class="SCHEMA")
        quantity = _decimal(self.quantity, "ORDER_QUANTITY", positive=True)
        price = _decimal(self.price, "ORDER_PRICE", positive=True)
        object.__setattr__(self, "quantity", quantity)
        object.__setattr__(self, "price", price)
        _wire_units(quantity, self.size_decimals, "BASE_AMOUNT")
        _wire_units(price, self.price_decimals, "PRICE")
        if not isinstance(self.is_ask, bool) or not isinstance(self.reduce_only, bool):
            raise LifecycleHalt("ORDER_BOOLEAN_INVALID", failure_class="SCHEMA")
        if self.order_expiry is not None and (
            isinstance(self.order_expiry, bool) or not isinstance(self.order_expiry, int)
        ):
            raise LifecycleHalt("ORDER_EXPIRY_INVALID", failure_class="SCHEMA")
        if self.quantity <= 0 or self.price <= 0:
            raise LifecycleHalt("ORDER_VECTOR_NOT_POSITIVE", failure_class="SCHEMA")
        if not isinstance(self.order_type, str) or self.order_type not in {"limit", "market"}:
            raise LifecycleHalt("ORDER_TYPE_INVALID", failure_class="SCHEMA")
        if not isinstance(self.time_in_force, str) or self.time_in_force not in {"post_only", "ioc"}:
            raise LifecycleHalt("ORDER_TIF_INVALID", failure_class="SCHEMA")
        if self.order_type == "limit" and self.time_in_force != "post_only":
            raise LifecycleHalt("MAKER_MUST_BE_POST_ONLY", failure_class="SAFETY")
        if self.order_type == "market" and self.time_in_force != "ioc":
            raise LifecycleHalt("TAKER_MUST_BE_IOC", failure_class="SAFETY")
        if self.reduce_only and self.order_type != "market":
            raise LifecycleHalt("REDUCE_ONLY_CLOSE_MUST_BE_MARKET", failure_class="SAFETY")

    def safe_payload(self) -> dict[str, Any]:
        return {
            "client_order_index": self.client_order_index,
            "is_ask": self.is_ask,
            "market_id": self.market_id,
            "order_expiry": self.order_expiry,
            "order_type": self.order_type,
            "price": str(self.price),
            "price_decimals": self.price_decimals,
            "quantity": str(self.quantity),
            "reduce_only": self.reduce_only,
            "size_decimals": self.size_decimals,
            "time_in_force": self.time_in_force,
        }


@dataclass(frozen=True)
class IntentSpec:
    intent_id: str
    kind: IntentKind
    request: OrderRequest | None
    target_order_index: int | None
    nonce: int
    api_key_index: int = LIGHTER_API_KEY_INDEX
    market_id: int | None = None

    def __post_init__(self) -> None:
        if not _SAFE_ID.fullmatch(self.intent_id):
            raise LifecycleHalt("INTENT_ID_INVALID", failure_class="SCHEMA")
        if self.api_key_index != LIGHTER_API_KEY_INDEX:
            raise LifecycleHalt("EXPLICIT_API_KEY_INDEX_REQUIRED", failure_class="SAFETY")
        if self.nonce < 0:
            raise LifecycleHalt("EXPLICIT_RECONCILED_NONCE_REQUIRED", failure_class="SAFETY")
        if self.kind == IntentKind.MAKER_CANCEL:
            if (
                self.request is not None
                or self.target_order_index is None
                or self.market_id is None
                or self.market_id < 0
            ):
                raise LifecycleHalt("CANCEL_INTENT_VECTOR_INVALID", failure_class="SCHEMA")
        elif (
            self.request is None
            or self.target_order_index is not None
            or (self.market_id is not None and self.market_id != self.request.market_id)
        ):
            raise LifecycleHalt("ORDER_INTENT_VECTOR_INVALID", failure_class="SCHEMA")

    def safe_payload(self) -> dict[str, Any]:
        return {
            "api_key_index": self.api_key_index,
            "kind": self.kind.value,
            "market_id": self.request.market_id if self.request is not None else self.market_id,
            "nonce": self.nonce,
            "request": None if self.request is None else self.request.safe_payload(),
            "target_order_index": self.target_order_index,
        }

    @property
    def payload_digest(self) -> str:
        return _digest(self.safe_payload())


@dataclass(frozen=True)
class DispatchOutcome:
    accepted: bool
    rejected: bool = False
    order_index: int | None = None
    tx_hash: str | None = None
    error_class: str | None = None

    def __post_init__(self) -> None:
        if self.accepted and self.rejected:
            raise LifecycleHalt("DISPATCH_OUTCOME_CONTRADICTORY", failure_class="SCHEMA")
        if self.accepted and self.error_class is not None:
            raise LifecycleHalt("DISPATCH_ACCEPTED_WITH_ERROR", failure_class="SCHEMA")
        if self.rejected and not self.error_class:
            raise LifecycleHalt("DISPATCH_REJECTED_WITHOUT_CLASS", failure_class="SCHEMA")


class LighterGateway(Protocol):
    async def discover_market(self) -> MarketObservation: ...

    async def market(self, market_id: int) -> MarketObservation: ...

    async def snapshot(
        self, market_id: int, *, client_order_index: int | None = None
    ) -> AccountSnapshot: ...

    async def reconcile_nonce(self, *, account_index: int, api_key_index: int) -> int: ...

    async def create_order(
        self, request: OrderRequest, *, nonce: int, api_key_index: int
    ) -> DispatchOutcome: ...

    async def cancel_order(
        self,
        market_id: int,
        order_index: int,
        *,
        nonce: int,
        api_key_index: int,
    ) -> DispatchOutcome: ...

    async def funding_history(
        self,
        market_id: int,
        *,
        account_index: int,
        baseline_high_water: int,
        boundary_ms: int | None = None,
    ) -> FundingHistory: ...

    async def terminal_round(self, market_id: int) -> TerminalRound: ...

    async def close(self) -> None: ...


@dataclass(frozen=True)
class RunnerReport:
    run_id: str
    result: RunnerResult
    failure_class: str
    reason: str
    market_id: int | None
    quantity: Decimal | None
    open_is_ask: bool | None
    funding_status: str
    funding_change: Decimal | None
    intent_count: int
    dispatch_count: int
    terminal_rounds: int
    sqlite_integrity: str
    mode: RunMode

    def sanitized(self) -> dict[str, Any]:
        return {
            "dispatch_count": self.dispatch_count,
            "failure_class": self.failure_class,
            "funding_change": None if self.funding_change is None else str(self.funding_change),
            "funding_status": self.funding_status,
            "intent_count": self.intent_count,
            "market_id": self.market_id,
            "mode": self.mode.value,
            "open_is_ask": self.open_is_ask,
            "quantity": None if self.quantity is None else str(self.quantity),
            "reason": self.reason,
            "result": self.result.value,
            "run_id": self.run_id,
            "sqlite_integrity": self.sqlite_integrity,
            "terminal_rounds": self.terminal_rounds,
        }


class LifecycleStore:
    """Small SQLite intent/evidence journal with durable one-way dispatch."""

    def __init__(self, path: str | Path, *, runtime_id: str | None = None) -> None:
        self.path = Path(path)
        self._created_stat = self._create_fresh_database_file()
        try:
            self._connection = sqlite3.connect(self.path)
            self._verify_database_file(self._created_stat)
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            self._remove_created_database_file(self._created_stat)
            if isinstance(exc, LifecycleHalt):
                raise
            raise LifecycleHalt("FRESH_DATABASE_OPEN_FAILED", failure_class="SAFETY") from None
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._initialize()
        self._run_id = runtime_id or f"lighter-testnet-{uuid4().hex}"
        self._restarted = False
        self._set_meta("runtime_id", self._run_id)
        self._set_meta("lifecycle_state", "NEW")

    def _create_fresh_database_file(self) -> os.stat_result:
        """Atomically reserve a new owner-only database path."""

        try:
            parent = os.lstat(self.path.parent)
        except (FileNotFoundError, OSError):
            raise LifecycleHalt("FRESH_DATABASE_PARENT_INVALID", failure_class="SAFETY") from None
        if (
            not stat.S_ISDIR(parent.st_mode)
            or parent.st_uid != os.getuid()
            or stat.S_IMODE(parent.st_mode) != 0o700
        ):
            raise LifecycleHalt("FRESH_DATABASE_PARENT_INVALID", failure_class="SAFETY")
        try:
            os.lstat(self.path)
        except FileNotFoundError:
            pass
        except OSError:
            raise LifecycleHalt("FRESH_DATABASE_PATH_UNAVAILABLE", failure_class="SAFETY") from None
        else:
            raise LifecycleHalt("FRESH_DATABASE_PATH_MUST_NOT_EXIST", failure_class="SAFETY")

        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        flags |= getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(self.path, flags, 0o600)
        except OSError:
            raise LifecycleHalt("FRESH_DATABASE_PATH_MUST_NOT_EXIST", failure_class="SAFETY") from None
        try:
            # Do not rely on umask: the final mode is a required invariant.
            os.fchmod(descriptor, 0o600)
            created = os.fstat(descriptor)
            if (
                not stat.S_ISREG(created.st_mode)
                or created.st_uid != os.getuid()
                or created.st_nlink != 1
                or stat.S_IMODE(created.st_mode) != 0o600
            ):
                raise LifecycleHalt("FRESH_DATABASE_METADATA_INVALID", failure_class="SAFETY")
            return created
        except LifecycleHalt:
            self._close_descriptor(descriptor)
            self._remove_created_database_file(None)
            raise
        except OSError:
            self._close_descriptor(descriptor)
            self._remove_created_database_file(None)
            raise LifecycleHalt("FRESH_DATABASE_PERMISSION_FAILURE", failure_class="SAFETY") from None
        finally:
            if descriptor is not None:
                self._close_descriptor(descriptor)

    @staticmethod
    def _close_descriptor(descriptor: int) -> None:
        try:
            os.close(descriptor)
        except OSError:
            pass

    def _remove_created_database_file(self, created: os.stat_result | None) -> None:
        try:
            current = os.lstat(self.path)
            if (
                stat.S_ISREG(current.st_mode)
                and current.st_uid == os.getuid()
                and current.st_nlink == 1
                and (created is None or (current.st_dev, current.st_ino) == (created.st_dev, created.st_ino))
            ):
                os.unlink(self.path)
        except FileNotFoundError:
            return
        except OSError:
            return

    def _verify_database_file(self, created: os.stat_result) -> None:
        try:
            current = os.lstat(self.path)
        except OSError:
            raise LifecycleHalt("FRESH_DATABASE_METADATA_INVALID", failure_class="SAFETY") from None
        if (
            (current.st_dev, current.st_ino) != (created.st_dev, created.st_ino)
            or not stat.S_ISREG(current.st_mode)
            or current.st_uid != os.getuid()
            or current.st_nlink != 1
            or stat.S_IMODE(current.st_mode) != 0o600
        ):
            raise LifecycleHalt("FRESH_DATABASE_METADATA_INVALID", failure_class="SAFETY")

    def _initialize(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS intents (
                intent_id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                state TEXT NOT NULL,
                payload_digest TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                market_id INTEGER,
                client_order_index INTEGER,
                target_order_index INTEGER,
                nonce INTEGER NOT NULL,
                api_key_index INTEGER NOT NULL,
                dispatch_count INTEGER NOT NULL CHECK(dispatch_count IN (0, 1)),
                outcome_code TEXT,
                outcome_order_index INTEGER,
                reconciled_digest TEXT,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS intents_nonce_key
                ON intents(api_key_index, nonce);
            CREATE TABLE IF NOT EXISTS evidence (
                evidence_id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                digest TEXT NOT NULL,
                summary_json TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS terminal_rounds (
                ordinal INTEGER PRIMARY KEY,
                round_id TEXT NOT NULL UNIQUE,
                digest TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL
            );
            """
        )
        self._connection.commit()

    def _get_meta(self, key: str) -> str | None:
        row = self._connection.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return None if row is None else str(row[0])

    def _set_meta(self, key: str, value: str) -> None:
        self._connection.execute(
            "INSERT INTO meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self._connection.commit()

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def restarted(self) -> bool:
        return self._restarted

    def begin(self) -> None:
        if self._restarted or self._get_meta("lifecycle_state") != "NEW":
            raise LifecycleHalt("RESTART_REQUIRES_FRESH_DATABASE", failure_class="SAFETY")
        self._set_meta("lifecycle_state", "RUNNING")

    def create_intent(self, intent: IntentSpec, *, now_ms: int) -> None:
        payload = intent.safe_payload()
        request = intent.request
        market_id = request.market_id if request is not None else intent.market_id
        client_order_index = request.client_order_index if request is not None else None
        try:
            self._connection.execute(
                """
                INSERT INTO intents(
                    intent_id, kind, state, payload_digest, payload_json, market_id,
                    client_order_index, target_order_index, nonce, api_key_index,
                    dispatch_count, created_at_ms, updated_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
                """,
                (
                    intent.intent_id,
                    intent.kind.value,
                    IntentState.PREPARED.value,
                    intent.payload_digest,
                    _canonical_json(payload),
                    market_id,
                    client_order_index,
                    intent.target_order_index,
                    intent.nonce,
                    intent.api_key_index,
                    now_ms,
                    now_ms,
                ),
            )
            self._connection.commit()
        except sqlite3.IntegrityError as exc:
            raise LifecycleHalt("DUPLICATE_OR_REPLAYED_INTENT", failure_class="SAFETY") from exc

    def claim_dispatch(self, intent_id: str, *, now_ms: int) -> None:
        with self._connection:
            row = self._connection.execute(
                "SELECT state, dispatch_count FROM intents WHERE intent_id = ?", (intent_id,)
            ).fetchone()
            if row is None or row[0] != IntentState.PREPARED.value or row[1] != 0:
                raise LifecycleHalt("DISPATCH_ALREADY_CLAIMED", failure_class="SAFETY")
            self._connection.execute(
                "UPDATE intents SET state = ?, dispatch_count = 1, updated_at_ms = ? WHERE intent_id = ?",
                (IntentState.DISPATCHING.value, now_ms, intent_id),
            )

    def record_dispatch(self, intent_id: str, outcome: DispatchOutcome, *, now_ms: int) -> None:
        if outcome.accepted:
            state = IntentState.DISPATCHED.value
            code = "ACCEPTED"
        else:
            state = IntentState.REJECTED.value
            code = outcome.error_class or "SDK_REJECTED"
        with self._connection:
            row = self._connection.execute(
                "SELECT state, dispatch_count FROM intents WHERE intent_id = ?", (intent_id,)
            ).fetchone()
            if row is None or row[0] != IntentState.DISPATCHING.value or row[1] != 1:
                raise LifecycleHalt("DISPATCH_RESULT_NOT_RECORDABLE", failure_class="SAFETY")
            self._connection.execute(
                "UPDATE intents SET state = ?, outcome_code = ?, outcome_order_index = ?, updated_at_ms = ? WHERE intent_id = ?",
                (state, code, outcome.order_index, now_ms, intent_id),
            )

    def mark_ambiguous(self, intent_id: str, *, now_ms: int, failure_class: str) -> None:
        with self._connection:
            row = self._connection.execute(
                "SELECT state, dispatch_count FROM intents WHERE intent_id = ?", (intent_id,)
            ).fetchone()
            if row is None or row[0] not in {
                IntentState.DISPATCHING.value,
                IntentState.DISPATCHED.value,
            } or row[1] != 1:
                raise LifecycleHalt("AMBIGUOUS_INTENT_NOT_DISPATCHING", failure_class="SAFETY")
            self._connection.execute(
                "UPDATE intents SET state = ?, outcome_code = ?, updated_at_ms = ? WHERE intent_id = ?",
                (IntentState.AMBIGUOUS.value, failure_class, now_ms, intent_id),
            )

    def reconcile(self, intent_id: str, evidence_digest: str, *, now_ms: int) -> None:
        with self._connection:
            row = self._connection.execute(
                "SELECT state FROM intents WHERE intent_id = ?", (intent_id,)
            ).fetchone()
            if row is None or row[0] != IntentState.DISPATCHED.value:
                raise LifecycleHalt("INTENT_NOT_RECONCILABLE", failure_class="SAFETY")
            self._connection.execute(
                "UPDATE intents SET state = ?, reconciled_digest = ?, updated_at_ms = ? WHERE intent_id = ?",
                (IntentState.RECONCILED.value, evidence_digest, now_ms, intent_id),
            )

    def record_evidence(self, kind: str, summary: Mapping[str, Any], *, now_ms: int) -> str:
        _assert_public_json(summary)
        digest = _digest(summary)
        evidence_id = f"{kind.lower()}-{uuid4().hex}"
        with self._connection:
            self._connection.execute(
                "INSERT INTO evidence(evidence_id, kind, digest, summary_json, created_at_ms) VALUES (?, ?, ?, ?, ?)",
                (evidence_id, kind, digest, _canonical_json(dict(summary)), now_ms),
            )
        return digest

    def record_terminal(self, round_value: TerminalRound, *, now_ms: int) -> None:
        ordinal = self._connection.execute("SELECT COUNT(*) FROM terminal_rounds").fetchone()[0] + 1
        with self._connection:
            self._connection.execute(
                "INSERT INTO terminal_rounds(ordinal, round_id, digest, created_at_ms) VALUES (?, ?, ?, ?)",
                (ordinal, round_value.round_id, round_value.digest, now_ms),
            )

    def intent_count(self) -> int:
        return int(self._connection.execute("SELECT COUNT(*) FROM intents").fetchone()[0])

    def dispatch_count(self) -> int:
        return int(self._connection.execute("SELECT COALESCE(SUM(dispatch_count), 0) FROM intents").fetchone()[0])

    def intent_state(self, intent_id: str) -> IntentState:
        row = self._connection.execute("SELECT state FROM intents WHERE intent_id = ?", (intent_id,)).fetchone()
        if row is None:
            raise LifecycleHalt("INTENT_NOT_FOUND", failure_class="SAFETY")
        return IntentState(str(row[0]))

    def all_intents_reconciled(self) -> bool:
        row = self._connection.execute(
            "SELECT COUNT(*) FROM intents WHERE state != ?", (IntentState.RECONCILED.value,)
        ).fetchone()
        return bool(row and row[0] == 0)

    def integrity(self) -> str:
        row = self._connection.execute("PRAGMA integrity_check").fetchone()
        return "ok" if row and row[0] == "ok" else "failed"

    def finish(self, state: str) -> None:
        self._set_meta("lifecycle_state", state)

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> "LifecycleStore":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()


def _walk(levels: Sequence[BookLevel], quantity: Decimal) -> tuple[Decimal, Decimal]:
    remaining = quantity
    quote = Decimal(0)
    worst = Decimal(0)
    for level in levels:
        take = min(remaining, level.quantity)
        quote += take * level.price
        remaining -= take
        worst = level.price
        if remaining == 0:
            return quote / quantity, worst
    raise LifecycleHalt("EXTERNAL_LIQUIDITY_INSUFFICIENT", failure_class="SAFETY")


def smallest_executable_quantity(market: MarketObservation, *, is_ask: bool) -> tuple[Decimal, Decimal]:
    """Return the smallest current grid quantity and its IOC protection price."""

    step = market.size_step
    levels = tuple(sorted(market.bids, key=lambda row: row.price, reverse=True)) if is_ask else tuple(
        sorted(market.asks, key=lambda row: row.price)
    )
    quantity = _ceil_grid(market.contract.min_base_amount, step)
    # Minimum quote is evaluated at actual executable depth, not at last/mark.
    for _ in range(100_000):
        vwap, worst = _walk(levels, quantity)
        if quantity * vwap >= market.contract.min_quote_amount:
            return quantity, worst
        quantity += step
    raise LifecycleHalt("MINIMUM_QUANTITY_SEARCH_EXHAUSTED", failure_class="SAFETY")


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex}"


def _new_client_order_index() -> int:
    """Generate an unpredictable positive client order index accepted by Lighter."""

    return int(uuid4().int & LIGHTER_MAX_CLIENT_ORDER_INDEX) or 1


class SdkSecretLogFilter(logging.Filter):
    """Drop official SDK DEBUG records that can contain private transaction data."""

    _markers = ("tx_info", "txinfo", "txresponse", "response:", "auth token", "signature")

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage().lower()
        except Exception:
            return False
        return not any(marker in message for marker in self._markers)


@contextmanager
def suppress_sdk_secret_debug() -> Any:
    """Temporarily suppress official SDK debug records carrying sensitive data."""

    guard = SdkSecretLogFilter()
    root = logging.getLogger()
    handlers = list(root.handlers)
    root.addFilter(guard)
    for handler in handlers:
        handler.addFilter(guard)
    try:
        yield
    finally:
        root.removeFilter(guard)
        for handler in handlers:
            handler.removeFilter(guard)


def _classify_sdk_error(error: BaseException) -> str:
    """Return a fixed error token without retaining exception text."""

    name = type(error).__name__.lower()
    if "timeout" in name or isinstance(error, (TimeoutError, asyncio.TimeoutError)):
        return "SDK_TIMEOUT"
    if "connection" in name or isinstance(error, ConnectionError):
        return "SDK_CONNECTION"
    if "badrequest" in name or "value" in name:
        return "SDK_REJECTED"
    return "SDK_TRANSPORT_FAILURE"


_SDK_MISSING = object()


def _sdk_field(value: Any, label: str, *names: str, default: Any = _SDK_MISSING) -> Any:
    for name in names:
        try:
            if hasattr(value, name):
                result = getattr(value, name)
                if result is not None:
                    return result
        except Exception:
            continue
    if default is not _SDK_MISSING:
        return default
    raise LifecycleHalt(f"{label}_MISSING", failure_class="SCHEMA")


def _sdk_sequence(value: Any, label: str) -> tuple[Any, ...]:
    if not isinstance(value, (list, tuple)):
        raise LifecycleHalt(f"{label}_INVALID", failure_class="SCHEMA")
    return tuple(value)


def _sdk_bool(value: Any, label: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    raise LifecycleHalt(f"{label}_INVALID", failure_class="SCHEMA")


def _sdk_order_type(value: Any) -> str:
    raw = str(value).strip().lower().replace("_", "-").replace(" ", "-")
    aliases = {
        "0": "limit",
        "1": "market",
        "limit": "limit",
        "market": "market",
    }
    if raw not in aliases:
        raise LifecycleHalt("ORDER_TYPE_INVALID", failure_class="SCHEMA")
    return aliases[raw]


def _sdk_tif(value: Any) -> str:
    raw = str(value).strip().lower().replace("_", "-").replace(" ", "-")
    aliases = {
        "0": "ioc",
        "1": "gtt",
        "2": "post-only",
        "ioc": "ioc",
        "immediate-or-cancel": "ioc",
        "gtt": "gtt",
        "good-till-time": "gtt",
        "post-only": "post_only",
    }
    if raw not in aliases:
        raise LifecycleHalt("ORDER_TIF_INVALID", failure_class="SCHEMA")
    return aliases[raw]


def _bootstrap_assets(account: Any) -> tuple[BootstrapAsset, ...]:
    rows = _sdk_sequence(_sdk_field(account, "ASSETS", "assets"), "ASSETS")
    expected = {asset.symbol: asset for asset in TESTNET_BOOTSTRAP_ASSET_BASELINE}
    observed: dict[str, BootstrapAsset] = {}
    asset_ids: set[int] = set()
    for row in rows:
        symbol = _text(_sdk_field(row, "ASSET_SYMBOL", "symbol"), "ASSET_SYMBOL")
        asset_id = _int(_sdk_field(row, "ASSET_ID", "asset_id"), "ASSET_ID", nonnegative=True)
        if symbol in observed or asset_id in asset_ids:
            raise LifecycleHalt("DUPLICATE_ACCOUNT_ASSET", failure_class="SAFETY")
        expected_asset = expected.get(symbol)
        if expected_asset is None:
            raise LifecycleHalt("UNKNOWN_BOOTSTRAP_ASSET", failure_class="SAFETY")
        observed_asset = BootstrapAsset(
            symbol=symbol,
            asset_id=asset_id,
            balance=_decimal(_sdk_field(row, "ASSET_BALANCE", "balance"), "ASSET_BALANCE"),
            locked_balance=_decimal(
                _sdk_field(row, "ASSET_LOCKED_BALANCE", "locked_balance"),
                "ASSET_LOCKED_BALANCE",
            ),
            margin_balance=_decimal(
                _sdk_field(row, "ASSET_MARGIN_BALANCE", "margin_balance"),
                "ASSET_MARGIN_BALANCE",
            ),
            margin_mode=_text(_sdk_field(row, "ASSET_MARGIN_MODE", "margin_mode"), "ASSET_MARGIN_MODE"),
        )
        _decimal(_sdk_field(row, "ASSET_MULTIPLIER", "multiplier"), "ASSET_MULTIPLIER")
        if observed_asset != expected_asset:
            raise LifecycleHalt("BOOTSTRAP_ASSET_BASELINE_MISMATCH", failure_class="SAFETY")
        observed[symbol] = observed_asset
        asset_ids.add(asset_id)
    if set(observed) != set(expected):
        raise LifecycleHalt("BOOTSTRAP_ASSET_BASELINE_MISMATCH", failure_class="SAFETY")
    return tuple(observed[asset.symbol] for asset in TESTNET_BOOTSTRAP_ASSET_BASELINE)


class OfficialSdkReadAdapter:
    """Narrow REST reader for the pinned official Lighter SDK.

    This adapter is constructed only by the isolated testnet entry point.  It
    reads generated public model attributes, ignores additive model fields,
    and never serializes a model or authorization value.
    """

    def __init__(
        self,
        *,
        identity: IdentityMetadata,
        authorization: str | None = None,
        authorization_provider: Callable[[], str | Awaitable[str]] | None = None,
        account_api: Any,
        order_api: Any,
        funding_api: Any,
        transaction_api: Any,
        clock_ms: Callable[[], int] | None = None,
        sleep: Callable[[float], Awaitable[Any]] | None = None,
        monotonic: Callable[[], float] | None = None,
    ) -> None:
        if authorization_provider is None and (not isinstance(authorization, str) or not authorization):
            raise LifecycleHalt("AUTHORIZATION_UNAVAILABLE", failure_class="AUTH")
        self.identity = identity
        self._authorization = authorization or ""
        self._authorization_provider = authorization_provider
        self._account_api = account_api
        self._order_api = order_api
        self._funding_api = funding_api
        self._transaction_api = transaction_api
        self._clock_ms = clock_ms or (lambda: int(time.time() * 1000))
        self._sleep = sleep or asyncio.sleep
        self._monotonic = monotonic or time.monotonic
        self._read_count = 0
        self._last_funding_count = 0
        self._last_funding_complete = False
        self._terminal_counter = 0

    @property
    def read_count(self) -> int:
        return self._read_count

    @property
    def last_funding_count(self) -> int:
        return self._last_funding_count

    @property
    def last_funding_complete(self) -> bool:
        return self._last_funding_complete

    async def _fresh_authorization(self) -> str:
        if self._authorization_provider is None:
            authorization = self._authorization
        else:
            try:
                with suppress_sdk_secret_debug():
                    authorization = self._authorization_provider()
                    if inspect.isawaitable(authorization):
                        authorization = await asyncio.wait_for(
                            authorization, timeout=SDK_READ_TIMEOUT_SECONDS
                        )
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException:
                raise LifecycleHalt("AUTH_TOKEN_REFRESH_FAILED", failure_class="AUTH") from None
        if not isinstance(authorization, str) or not authorization:
            raise LifecycleHalt("AUTH_TOKEN_UNAVAILABLE", failure_class="AUTH")
        self._authorization = authorization
        return authorization

    async def _call(self, method: Callable[..., Any], **kwargs: Any) -> Any:
        self._read_count += 1
        try:
            if "authorization" in kwargs:
                kwargs["authorization"] = await self._fresh_authorization()
            with suppress_sdk_secret_debug():
                result = method(**kwargs)
                if inspect.isawaitable(result):
                    result = await asyncio.wait_for(result, timeout=SDK_READ_TIMEOUT_SECONDS)
        except (KeyboardInterrupt, SystemExit):
            raise
        except LifecycleHalt:
            raise
        except BaseException as exc:
            raise LifecycleHalt(_classify_sdk_error(exc), failure_class="TRANSPORT") from None

        data = result
        response = None
        error = None
        if isinstance(result, tuple):
            if len(result) == 3:
                data, response, error = result
            elif len(result) == 2:
                data, response = result
            else:
                raise LifecycleHalt("SDK_READ_RESPONSE_INVALID", failure_class="SCHEMA")
        if error is not None:
            raise LifecycleHalt("SDK_READ_HTTP_ERROR", failure_class="HTTP")
        code = None
        for candidate in (response, data):
            if candidate is None:
                continue
            code = getattr(candidate, "status", None)
            if code is None:
                code = getattr(candidate, "status_code", None)
            if code is None:
                code = getattr(candidate, "code", None)
            if code is not None:
                break
        if code is not None and code != 200:
            raise LifecycleHalt("SDK_READ_HTTP_STATUS", failure_class="HTTP")
        if data is None:
            raise LifecycleHalt("SDK_READ_RESPONSE_EMPTY", failure_class="SCHEMA")
        return data

    def _detail_contract(self, detail: Any) -> MarketContract:
        config = _sdk_field(detail, "MARKET_CONFIG", "market_config")
        return MarketContract(
            market_id=_int(_sdk_field(detail, "MARKET_ID", "market_id"), "MARKET_ID", nonnegative=True),
            symbol=_text(_sdk_field(detail, "SYMBOL", "symbol"), "MARKET_SYMBOL"),
            market_type=_text(_sdk_field(detail, "MARKET_TYPE", "market_type"), "MARKET_TYPE"),
            min_base_amount=_decimal(_sdk_field(detail, "MIN_BASE_AMOUNT", "min_base_amount"), "MIN_BASE_AMOUNT", positive=True),
            min_quote_amount=_decimal(_sdk_field(detail, "MIN_QUOTE_AMOUNT", "min_quote_amount"), "MIN_QUOTE_AMOUNT", positive=True),
            size_decimals=_market_decimals(_sdk_field(detail, "SIZE_DECIMALS", "size_decimals"), "SIZE_DECIMALS"),
            price_decimals=_market_decimals(_sdk_field(detail, "PRICE_DECIMALS", "price_decimals"), "PRICE_DECIMALS"),
            quote_decimals=_market_decimals(
                _sdk_field(detail, "SUPPORTED_QUOTE_DECIMALS", "supported_quote_decimals"),
                "QUOTE_DECIMALS",
            ),
            status=_text(_sdk_field(detail, "MARKET_STATUS", "status"), "MARKET_STATUS"),
            force_reduce_only=_sdk_bool(
                _sdk_field(config, "FORCE_REDUCE_ONLY", "force_reduce_only"),
                "FORCE_REDUCE_ONLY",
            ),
        )

    async def _details(self, market_id: int | None) -> tuple[Any, ...]:
        data = await self._call(
            self._order_api.order_book_details,
            market_id=market_id,
            filter="perp",
            _request_timeout=SDK_READ_TIMEOUT_SECONDS,
        )
        rows = _sdk_sequence(_sdk_field(data, "ORDER_BOOK_DETAILS", "order_book_details"), "ORDER_BOOK_DETAILS")
        return rows

    def _select_detail(self, rows: Sequence[Any], market_id: int | None = None) -> Any:
        matches = []
        for row in rows:
            symbol = _sdk_field(row, "MARKET_SYMBOL", "symbol")
            market_type = _sdk_field(row, "MARKET_TYPE", "market_type")
            row_market_id = _int(_sdk_field(row, "MARKET_ID", "market_id"), "MARKET_ID", nonnegative=True)
            if symbol == LIGHTER_SYMBOL and market_type == "perp" and (
                market_id is None or row_market_id == market_id
            ):
                matches.append(row)
        if len(matches) != 1:
            raise LifecycleHalt("LIT_MARKET_DISCOVERY_NOT_EXACT", failure_class="IDENTITY")
        return matches[0]

    async def _book(self, market_id: int) -> tuple[tuple[BookLevel, ...], tuple[BookLevel, ...]]:
        data = await self._call(
            self._order_api.order_book_orders,
            market_id=market_id,
            limit=250,
            _request_timeout=SDK_READ_TIMEOUT_SECONDS,
        )
        bids = _sdk_sequence(_sdk_field(data, "BIDS", "bids"), "BOOK_BIDS")
        asks = _sdk_sequence(_sdk_field(data, "ASKS", "asks"), "BOOK_ASKS")
        total_bids = _int(_sdk_field(data, "TOTAL_BIDS", "total_bids"), "TOTAL_BIDS", nonnegative=True)
        total_asks = _int(_sdk_field(data, "TOTAL_ASKS", "total_asks"), "TOTAL_ASKS", nonnegative=True)
        if total_bids != len(bids) or total_asks != len(asks) or not bids or not asks:
            raise LifecycleHalt("BOOK_RESPONSE_INCOMPLETE", failure_class="SCHEMA")

        def parse(level: Any) -> BookLevel:
            return BookLevel(
                price=_decimal(_sdk_field(level, "BOOK_PRICE", "price"), "BOOK_PRICE", positive=True),
                quantity=_decimal(
                    _sdk_field(level, "BOOK_REMAINING", "remaining_base_amount"),
                    "BOOK_REMAINING",
                    positive=True,
                ),
            )

        return tuple(parse(row) for row in bids), tuple(parse(row) for row in asks)

    async def _funding_schedule(self, market_id: int) -> FundingSchedule:
        data = await self._call(
            self._funding_api.funding_rates,
            _request_timeout=SDK_READ_TIMEOUT_SECONDS,
        )
        rows = _sdk_sequence(_sdk_field(data, "FUNDING_RATES", "funding_rates"), "FUNDING_RATES")
        matches = []
        for row in rows:
            row_market_id = _int(_sdk_field(row, "FUNDING_MARKET_ID", "market_id"), "FUNDING_MARKET_ID", nonnegative=True)
            exchange = _text(_sdk_field(row, "FUNDING_EXCHANGE", "exchange"), "FUNDING_EXCHANGE")
            symbol = _text(_sdk_field(row, "FUNDING_SYMBOL", "symbol"), "FUNDING_SYMBOL")
            if row_market_id == market_id and exchange.lower() == "lighter" and symbol == LIGHTER_SYMBOL:
                matches.append(row)
        if len(matches) != 1:
            raise LifecycleHalt("FUNDING_SCHEDULE_NOT_EXACT", failure_class="SAFETY")
        now = self._clock_ms()
        boundary = ((now // HOURLY_FUNDING_INTERVAL_MS) + 1) * HOURLY_FUNDING_INTERVAL_MS
        return FundingSchedule(
            market_id=market_id,
            symbol=LIGHTER_SYMBOL,
            next_boundary_ms=boundary,
            interval_ms=HOURLY_FUNDING_INTERVAL_MS,
            rate=_decimal(_sdk_field(matches[0], "FUNDING_RATE", "rate"), "FUNDING_RATE"),
            source="official_funding_rates_hourly_utc",
            observed_at_ms=now,
        )

    async def _market(self, detail: Any) -> MarketObservation:
        contract = self._detail_contract(detail)
        bids, asks = await self._book(contract.market_id)
        funding = await self._funding_schedule(contract.market_id)
        observation = MarketObservation(contract, bids, asks, funding, self._clock_ms())
        observation.validate(now_ms=self._clock_ms())
        return observation

    async def discover_market(self) -> MarketObservation:
        return await self._market(self._select_detail(await self._details(None)))

    async def market(self, market_id: int) -> MarketObservation:
        if isinstance(market_id, bool) or not isinstance(market_id, int) or market_id < 0:
            raise LifecycleHalt("MARKET_ID_INVALID", failure_class="SCHEMA")
        return await self._market(self._select_detail(await self._details(market_id), market_id))

    async def _account(self) -> Any:
        cursor: str | None = None
        accounts: list[Any] = []
        for _ in range(MAX_PAGES):
            data = await self._call(
                self._account_api.account,
                by="index",
                value=str(self.identity.account_index),
                active_only=False,
                cursor=cursor,
                _request_timeout=SDK_READ_TIMEOUT_SECONDS,
            )
            page = _sdk_sequence(_sdk_field(data, "ACCOUNTS", "accounts"), "ACCOUNT_PAGE")
            accounts.extend(page)
            next_cursor = _sdk_field(data, "NEXT_CURSOR", "next_cursor", default="")
            if next_cursor in {None, ""}:
                break
            if not isinstance(next_cursor, str) or next_cursor == cursor:
                raise LifecycleHalt("ACCOUNT_PAGINATION_INVALID", failure_class="SCHEMA")
            cursor = next_cursor
        else:
            raise LifecycleHalt("ACCOUNT_PAGINATION_INCOMPLETE", failure_class="SAFETY")
        if len(accounts) != 1:
            raise LifecycleHalt("ACCOUNT_IDENTITY_NOT_EXACT", failure_class="IDENTITY")
        account = accounts[0]
        if (
            _int(_sdk_field(account, "ACCOUNT_INDEX", "account_index"), "ACCOUNT_INDEX", nonnegative=True)
            != self.identity.account_index
            or _int(_sdk_field(account, "ACCOUNT_TABLE_INDEX", "index"), "ACCOUNT_TABLE_INDEX", nonnegative=True)
            != self.identity.account_index
        ):
            raise LifecycleHalt("ACCOUNT_IDENTITY_MISMATCH", failure_class="IDENTITY")
        account_l1 = _sdk_field(account, "ACCOUNT_L1", "l1_address")
        if not isinstance(account_l1, str):
            raise LifecycleHalt("ACCOUNT_L1_INVALID", failure_class="SCHEMA")
        if account_l1.lower() != self.identity.l1_address.lower():
            raise LifecycleHalt("ACCOUNT_IDENTITY_MISMATCH", failure_class="IDENTITY")
        return account

    def _positions(self, account: Any) -> tuple[PositionSnapshot, ...]:
        positions = _sdk_sequence(_sdk_field(account, "POSITIONS", "positions"), "POSITIONS")
        parsed: list[PositionSnapshot] = []
        for row in positions:
            sign = _int(_sdk_field(row, "POSITION_SIGN", "sign"), "POSITION_SIGN")
            if sign not in {-1, 0, 1}:
                raise LifecycleHalt("POSITION_SIGN_INVALID", failure_class="SCHEMA")
            magnitude = _decimal(_sdk_field(row, "POSITION_SIZE", "position"), "POSITION_SIZE")
            if magnitude < 0 or (sign == 0 and magnitude != 0):
                raise LifecycleHalt("POSITION_VECTOR_INVALID", failure_class="SCHEMA")
            parsed.append(
                PositionSnapshot(
                    self.identity.account_index,
                    _int(_sdk_field(row, "POSITION_MARKET_ID", "market_id"), "POSITION_MARKET_ID", nonnegative=True),
                    magnitude * sign,
                    _decimal(_sdk_field(row, "POSITION_AVERAGE_PRICE", "avg_entry_price"), "POSITION_AVERAGE_PRICE"),
                )
            )
        return tuple(parsed)

    def _order(self, row: Any) -> OrderSnapshot:
        trigger_status = str(_sdk_field(row, "TRIGGER_STATUS", "trigger_status", default="na")).lower()
        trigger_price = _decimal(
            _sdk_field(row, "TRIGGER_PRICE", "trigger_price", default="0"),
            "TRIGGER_PRICE",
        )
        order_type = _sdk_order_type(_sdk_field(row, "ORDER_TYPE", "type"))
        trigger_types = {
            "stop-loss",
            "stop-loss-limit",
            "take-profit",
            "take-profit-limit",
        }
        return OrderSnapshot(
            order_index=_int(_sdk_field(row, "ORDER_INDEX", "order_index"), "ORDER_INDEX", nonnegative=True),
            client_order_index=_int(
                _sdk_field(row, "CLIENT_ORDER_INDEX", "client_order_index"),
                "CLIENT_ORDER_INDEX",
                nonnegative=True,
            ),
            account_index=_int(
                _sdk_field(row, "ORDER_ACCOUNT_INDEX", "owner_account_index"),
                "ORDER_ACCOUNT_INDEX",
                nonnegative=True,
            ),
            market_id=_int(_sdk_field(row, "ORDER_MARKET_ID", "market_index"), "ORDER_MARKET_ID", nonnegative=True),
            quantity=_decimal(_sdk_field(row, "ORDER_INITIAL_SIZE", "initial_base_amount"), "ORDER_INITIAL_SIZE", positive=True),
            remaining_quantity=_decimal(
                _sdk_field(row, "ORDER_REMAINING_SIZE", "remaining_base_amount"),
                "ORDER_REMAINING_SIZE",
            ),
            filled_quantity=_decimal(
                _sdk_field(row, "ORDER_FILLED_SIZE", "filled_base_amount"),
                "ORDER_FILLED_SIZE",
            ),
            filled_quote=_decimal(
                _sdk_field(row, "ORDER_FILLED_QUOTE", "filled_quote_amount"),
                "ORDER_FILLED_QUOTE",
            ),
            price=_decimal(_sdk_field(row, "ORDER_PRICE", "price"), "ORDER_PRICE", positive=True),
            is_ask=_sdk_bool(_sdk_field(row, "ORDER_IS_ASK", "is_ask"), "ORDER_IS_ASK"),
            order_type=order_type,
            time_in_force=_sdk_tif(_sdk_field(row, "ORDER_TIF", "time_in_force")),
            reduce_only=_sdk_bool(_sdk_field(row, "ORDER_REDUCE_ONLY", "reduce_only"), "ORDER_REDUCE_ONLY"),
            nonce=_int(_sdk_field(row, "ORDER_NONCE", "nonce"), "ORDER_NONCE", nonnegative=True),
            status=_text(_sdk_field(row, "ORDER_STATUS", "status"), "ORDER_STATUS"),
            trigger=order_type in trigger_types or trigger_status not in {"", "na", "none"} or trigger_price != 0,
        )

    async def _active_orders(self) -> tuple[OrderSnapshot, ...]:
        data = await self._call(
            self._order_api.account_active_orders,
            authorization=self._authorization,
            account_index=self.identity.account_index,
            market_id=None,
            market_type=None,
            _request_timeout=SDK_READ_TIMEOUT_SECONDS,
        )
        return tuple(self._order(row) for row in _sdk_sequence(_sdk_field(data, "ACTIVE_ORDERS", "orders"), "ACTIVE_ORDERS"))

    async def _target_orders(self, client_order_index: int | None) -> tuple[OrderSnapshot, ...]:
        if client_order_index is None:
            return ()
        data = await self._call(
            self._order_api.account_orders,
            authorization=self._authorization,
            client_order_indexes=str(client_order_index),
            account_index=self.identity.account_index,
            _request_timeout=SDK_READ_TIMEOUT_SECONDS,
        )
        return tuple(self._order(row) for row in _sdk_sequence(_sdk_field(data, "ACCOUNT_ORDERS", "orders"), "ACCOUNT_ORDERS"))

    @staticmethod
    def _merge_orders(*groups: Sequence[OrderSnapshot]) -> tuple[OrderSnapshot, ...]:
        by_index: dict[int, OrderSnapshot] = {}
        for group in groups:
            for row in group:
                previous = by_index.get(row.order_index)
                if previous is not None and previous != row:
                    raise LifecycleHalt("DUPLICATE_ORDER_CONTRADICTION", failure_class="SAFETY")
                by_index[row.order_index] = row
        return tuple(by_index.values())

    async def _fills(self) -> tuple[FillSnapshot, ...]:
        cursor: str | None = None
        fills: list[FillSnapshot] = []
        for _ in range(MAX_PAGES):
            data = await self._call(
                self._order_api.trades,
                sort_by="timestamp",
                limit=100,
                authorization=self._authorization,
                account_index=self.identity.account_index,
                cursor=cursor,
                sort_dir="desc",
                _request_timeout=SDK_READ_TIMEOUT_SECONDS,
            )
            rows = _sdk_sequence(_sdk_field(data, "TRADES", "trades"), "TRADES")
            for row in rows:
                ask_account = _int(_sdk_field(row, "ASK_ACCOUNT", "ask_account_id"), "ASK_ACCOUNT", nonnegative=True)
                bid_account = _int(_sdk_field(row, "BID_ACCOUNT", "bid_account_id"), "BID_ACCOUNT", nonnegative=True)
                if (ask_account == self.identity.account_index) == (bid_account == self.identity.account_index):
                    raise LifecycleHalt("TRADE_ACCOUNT_SIDE_INVALID", failure_class="IDENTITY")
                is_ask = ask_account == self.identity.account_index
                fills.append(
                    FillSnapshot(
                        trade_id=str(_sdk_field(row, "TRADE_ID", "trade_id")),
                        order_index=_int(
                            _sdk_field(row, "ASK_ORDER", "ask_id") if is_ask else _sdk_field(row, "BID_ORDER", "bid_id"),
                            "TRADE_ORDER_INDEX",
                            nonnegative=True,
                        ),
                        client_order_index=_int(
                            _sdk_field(row, "ASK_CLIENT", "ask_client_id") if is_ask else _sdk_field(row, "BID_CLIENT", "bid_client_id"),
                            "TRADE_CLIENT_ORDER_INDEX",
                            nonnegative=True,
                        ),
                        account_index=self.identity.account_index,
                        market_id=_int(_sdk_field(row, "TRADE_MARKET_ID", "market_id"), "TRADE_MARKET_ID", nonnegative=True),
                        quantity=_decimal(_sdk_field(row, "TRADE_SIZE", "size"), "TRADE_SIZE", positive=True),
                        price=_decimal(_sdk_field(row, "TRADE_PRICE", "price"), "TRADE_PRICE", positive=True),
                        is_ask=is_ask,
                        timestamp_ms=_int(_sdk_field(row, "TRADE_TIMESTAMP", "timestamp"), "TRADE_TIMESTAMP", nonnegative=True),
                    )
                )
            next_cursor = _sdk_field(data, "TRADES_NEXT_CURSOR", "next_cursor", default="")
            if next_cursor in {None, ""}:
                return tuple(fills)
            if not isinstance(next_cursor, str) or next_cursor == cursor:
                raise LifecycleHalt("TRADE_PAGINATION_INVALID", failure_class="SCHEMA")
            cursor = next_cursor
        raise LifecycleHalt("TRADE_PAGINATION_INCOMPLETE", failure_class="SAFETY")

    async def _funding_once(self, *, baseline_high_water: int) -> FundingHistory:
        cursor: str | None = None
        rows_out: list[FundingRecord] = []
        high_water = baseline_high_water
        for _ in range(MAX_PAGES):
            data = await self._call(
                self._account_api.position_funding,
                account_index=self.identity.account_index,
                limit=100,
                authorization=self._authorization,
                cursor=cursor,
                market_ids=None,
                _request_timeout=SDK_READ_TIMEOUT_SECONDS,
            )
            rows = _sdk_sequence(
                _sdk_field(data, "POSITION_FUNDINGS", "position_fundings"),
                "POSITION_FUNDINGS",
            )
            for row in rows:
                funding_id = _int(_sdk_field(row, "FUNDING_ID", "funding_id"), "FUNDING_ID", nonnegative=True)
                high_water = max(high_water, funding_id)
                parsed = FundingRecord(
                    funding_id=funding_id,
                    market_id=_int(_sdk_field(row, "FUNDING_MARKET_ID", "market_id"), "FUNDING_MARKET_ID", nonnegative=True),
                    timestamp_ms=_int(_sdk_field(row, "FUNDING_TIMESTAMP", "timestamp"), "FUNDING_TIMESTAMP", nonnegative=True),
                    change=_decimal(_sdk_field(row, "FUNDING_CHANGE", "change"), "FUNDING_CHANGE"),
                    rate=_decimal(_sdk_field(row, "FUNDING_RATE", "rate"), "FUNDING_RATE"),
                    position_size=_decimal(_sdk_field(row, "FUNDING_POSITION_SIZE", "position_size"), "FUNDING_POSITION_SIZE", positive=True),
                    position_side=_text(_sdk_field(row, "FUNDING_POSITION_SIDE", "position_side"), "FUNDING_POSITION_SIDE"),
                )
                if funding_id > baseline_high_water:
                    rows_out.append(parsed)
            next_cursor = _sdk_field(data, "FUNDING_NEXT_CURSOR", "next_cursor", default="")
            if next_cursor in {None, ""}:
                self._last_funding_count = len(rows_out)
                self._last_funding_complete = True
                return FundingHistory(True, baseline_high_water, high_water, tuple(rows_out))
            if not isinstance(next_cursor, str) or next_cursor == cursor:
                raise LifecycleHalt("FUNDING_PAGINATION_INVALID", failure_class="SCHEMA")
            cursor = next_cursor
        raise LifecycleHalt("FUNDING_PAGINATION_INCOMPLETE", failure_class="SAFETY")

    async def funding_history(
        self,
        market_id: int,
        *,
        account_index: int,
        baseline_high_water: int,
        boundary_ms: int | None = None,
    ) -> FundingHistory:
        if account_index != self.identity.account_index:
            raise LifecycleHalt("FUNDING_ACCOUNT_IDENTITY_INVALID", failure_class="IDENTITY")
        deadline = None if boundary_ms is None else self._monotonic() + FUNDING_SETTLEMENT_WINDOW_SECONDS
        latest = await self._funding_once(baseline_high_water=baseline_high_water)
        while boundary_ms is not None:
            if any(
                row.market_id == market_id
                and boundary_ms <= row.timestamp_ms < boundary_ms + HOURLY_FUNDING_INTERVAL_MS
                for row in latest.records
            ):
                return latest
            assert deadline is not None
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                return latest
            await self._sleep(min(FUNDING_POLL_INTERVAL_SECONDS, remaining))
            latest = await self._funding_once(baseline_high_water=baseline_high_water)
        return latest

    async def _api_key_matches(self) -> bool:
        data = await self._call(
            self._account_api.apikeys,
            account_index=self.identity.account_index,
            api_key_index=LIGHTER_API_KEY_INDEX,
            _request_timeout=SDK_READ_TIMEOUT_SECONDS,
        )
        rows = _sdk_sequence(_sdk_field(data, "API_KEYS", "api_keys"), "API_KEYS")
        if len(rows) != 1:
            raise LifecycleHalt("API_KEY_RESPONSE_NOT_EXACT", failure_class="AUTH")
        row = rows[0]
        if (
            _int(_sdk_field(row, "API_KEY_ACCOUNT", "account_index"), "API_KEY_ACCOUNT", nonnegative=True)
            != self.identity.account_index
            or _int(_sdk_field(row, "API_KEY_INDEX", "api_key_index"), "API_KEY_INDEX", nonnegative=True)
            != LIGHTER_API_KEY_INDEX
        ):
            raise LifecycleHalt("API_KEY_IDENTITY_MISMATCH", failure_class="AUTH")
        try:
            public_key = canonical_api_public_key(_sdk_field(row, "API_KEY_PUBLIC", "public_key"))
        except (TypeError, ValueError):
            raise LifecycleHalt("API_PUBLIC_KEY_INVALID", failure_class="AUTH") from None
        if public_key != self.identity.api_key_public_key:
            raise LifecycleHalt("API_PUBLIC_KEY_MISMATCH", failure_class="AUTH")
        return True

    async def nonce(self) -> int:
        data = await self._call(
            self._transaction_api.next_nonce,
            account_index=self.identity.account_index,
            api_key_index=LIGHTER_API_KEY_INDEX,
            _request_timeout=SDK_READ_TIMEOUT_SECONDS,
        )
        nonce = _int(_sdk_field(data, "NEXT_NONCE", "nonce"), "NEXT_NONCE", nonnegative=True)
        return nonce

    async def snapshot(
        self,
        market_id: int,
        *,
        client_order_index: int | None = None,
    ) -> AccountSnapshot:
        account = await self._account()
        limits = await self._call(
            self._account_api.account_limits,
            account_index=self.identity.account_index,
            authorization=self._authorization,
            _request_timeout=SDK_READ_TIMEOUT_SECONDS,
        )
        active_orders = await self._active_orders()
        target_orders = await self._target_orders(client_order_index)
        orders = self._merge_orders(active_orders, target_orders)
        fills = await self._fills()
        funding = await self._funding_once(baseline_high_water=0)
        assets = _bootstrap_assets(account)
        pool_info = _sdk_field(account, "POOL_INFO", "pool_info", default=None)
        shares = _sdk_sequence(_sdk_field(account, "SHARES", "shares"), "SHARES")
        pending_unlocks = _sdk_field(account, "PENDING_UNLOCKS", "pending_unlocks", default=None)
        if pending_unlocks is not None:
            pending_unlocks = _sdk_sequence(pending_unlocks, "PENDING_UNLOCKS")
        approved_integrators = _sdk_field(account, "APPROVED_INTEGRATORS", "approved_integrators", default=None)
        if approved_integrators is not None:
            approved_integrators = _sdk_sequence(approved_integrators, "APPROVED_INTEGRATORS")
        unrelated_clear = (
            (pool_info is None or pool_info == "")
            and not shares
            and not pending_unlocks
            and not approved_integrators
        )
        return AccountSnapshot(
            account_index=self.identity.account_index,
            l1_address=_text(_sdk_field(account, "ACCOUNT_L1", "l1_address"), "ACCOUNT_L1"),
            collateral=_decimal(_sdk_field(account, "COLLATERAL", "collateral"), "COLLATERAL"),
            maker_fee_tick=_int(
                _sdk_field(limits, "MAKER_FEE_TICK", "current_maker_fee_tick"),
                "MAKER_FEE_TICK",
            ),
            taker_fee_tick=_int(
                _sdk_field(limits, "TAKER_FEE_TICK", "current_taker_fee_tick"),
                "TAKER_FEE_TICK",
            ),
            orders=orders,
            positions=self._positions(account),
            fills=fills,
            funding_high_water=funding.high_water,
            unrelated_state_clear=unrelated_clear,
            observed_at_ms=self._clock_ms(),
            asset_count=len(assets),
        )

    async def readiness(self) -> ReadinessResult:
        market: MarketObservation | None = None
        account: AccountSnapshot | None = None
        api_key_ok = False
        trades_ok = False
        funding_ok = False
        try:
            market = await self.discover_market()
            market.validate(now_ms=self._clock_ms())
            account = await self.snapshot(market.market_id)
            account.validate_identity_and_safety(
                self.identity,
                market_id=market.market_id,
                require_flat=True,
                observed_at_ms=self._clock_ms(),
            )
            api_key_ok = await self._api_key_matches()
            await self.nonce()
            trades_ok = True
            funding_ok = self._last_funding_complete
            return ReadinessResult(
                status="READY",
                failure_class="NONE",
                reason="OFFICIAL_SDK_READS_READY",
                wallet_address=self.identity.l1_address,
                account_index=self.identity.account_index,
                api_key_index=LIGHTER_API_KEY_INDEX,
                requests=self._read_count,
                retries=0,
                identity_verified=True,
                authorization_identity_verified=api_key_ok,
                api_key_verified=api_key_ok,
                collateral_positive=account.collateral > 0,
                fees_verified=account.maker_fee_tick == 0 and account.taker_fee_tick == 0,
                active_orders_zero=not account.active_regular_orders and not account.active_trigger_orders,
                positions_flat=all(row.signed_quantity == 0 for row in account.positions),
                unrelated_state_clear=account.unrelated_state_clear,
                trades_read=trades_ok,
                funding_history_read=funding_ok,
                active_order_count=len(account.active_regular_orders) + len(account.active_trigger_orders),
                regular_order_count=len(account.active_regular_orders),
                trigger_order_count=len(account.active_trigger_orders),
                trade_count=len(account.fills),
                funding_count=self._last_funding_count,
                asset_count=account.asset_count,
                position_count=len(account.positions),
                api_key_public_key_verified=api_key_ok,
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except LifecycleHalt as exc:
            active_regular = () if account is None else account.active_regular_orders
            active_trigger = () if account is None else account.active_trigger_orders
            return ReadinessResult(
                status="BLOCKED",
                failure_class=exc.failure_class,
                reason=exc.reason,
                wallet_address=self.identity.l1_address,
                account_index=self.identity.account_index,
                api_key_index=LIGHTER_API_KEY_INDEX,
                requests=self._read_count,
                retries=0,
                identity_verified=(
                    account is not None
                    and isinstance(account.l1_address, str)
                    and account.l1_address.lower() == self.identity.l1_address.lower()
                ),
                authorization_identity_verified=api_key_ok,
                api_key_verified=api_key_ok,
                collateral_positive=account is not None and account.collateral > 0,
                fees_verified=account is not None and account.maker_fee_tick == 0 and account.taker_fee_tick == 0,
                active_orders_zero=not active_regular and not active_trigger,
                positions_flat=account is not None and all(row.signed_quantity == 0 for row in account.positions),
                unrelated_state_clear=account is not None and account.unrelated_state_clear,
                trades_read=trades_ok,
                funding_history_read=funding_ok,
                active_order_count=len(active_regular) + len(active_trigger),
                regular_order_count=len(active_regular),
                trigger_order_count=len(active_trigger),
                trade_count=0 if account is None else len(account.fills),
                funding_count=self._last_funding_count,
                asset_count=0 if account is None else account.asset_count,
                position_count=0 if account is None else len(account.positions),
                api_key_public_key_verified=api_key_ok,
            )

    async def terminal_round(self, market_id: int) -> TerminalRound:
        account = await self.snapshot(market_id)
        now = self._clock_ms()
        account.validate_identity_and_safety(
            self.identity,
            market_id=market_id,
            require_flat=True,
            observed_at_ms=now,
        )
        position = account.exact_position(market_id)
        self._terminal_counter += 1
        summary = {
            "account_index": account.account_index,
            "active_regular_orders": [row.order_index for row in account.active_regular_orders],
            "active_trigger_orders": [row.order_index for row in account.active_trigger_orders],
            "market_id": market_id,
            "position": "0" if position is None else str(position.signed_quantity),
            "unrelated_state_clear": account.unrelated_state_clear,
        }
        return TerminalRound(
            round_id=f"terminal-{now}-{self._terminal_counter}",
            digest="0x" + _digest(summary),
            account_index=self.identity.account_index,
            market_id=market_id,
            observed_at_ms=account.observed_at_ms,
            active_regular_orders=len(account.active_regular_orders),
            active_trigger_orders=len(account.active_trigger_orders),
            signed_position=Decimal("0") if position is None else position.signed_quantity,
            unrelated_state_clear=account.unrelated_state_clear,
        )


async def _await_close(resource: Any) -> None:
    close = getattr(resource, "close", None)
    if close is None:
        return
    if not callable(close):
        raise LifecycleHalt("SDK_CLOSE_UNAVAILABLE", failure_class="AUTH")
    result = close()
    if inspect.isawaitable(result):
        await result


async def _close_without_masking(resource: Any) -> None:
    try:
        await _await_close(resource)
    except BaseException:
        pass


class SdkLighterGateway:
    """Official SDK boundary with explicit key/nonce dispatch."""

    def __init__(
        self,
        signer_client: Any,
        *,
        market_discoverer: Callable[[], Awaitable[MarketObservation]] | None = None,
        market_reader: Callable[[int], Awaitable[MarketObservation]] | None = None,
        snapshot_reader: Callable[..., Awaitable[AccountSnapshot]] | None = None,
        funding_reader: Callable[..., Awaitable[FundingHistory]] | None = None,
        nonce_reader: Callable[[int, int], Awaitable[int]] | None = None,
        lighter_module: Any | None = None,
        readiness_reader: Callable[[], Awaitable[ReadinessResult]] | None = None,
        terminal_reader: Callable[[int], Awaitable[TerminalRound]] | None = None,
        identity: IdentityMetadata | None = None,
    ) -> None:
        if any(value is None for value in (market_discoverer, market_reader, snapshot_reader, funding_reader)):
            raise LifecycleHalt("INCOMPLETE_READ_ADAPTER", failure_class="SAFETY")
        self._signer = signer_client
        self._identity = identity
        self._market_discoverer = market_discoverer
        self._market_reader = market_reader
        self._snapshot_reader = snapshot_reader
        self._funding_reader = funding_reader
        self._nonce_reader = nonce_reader
        self._lighter = lighter_module
        self._readiness_reader = readiness_reader
        self._terminal_reader = terminal_reader
        self._closed = False

    @classmethod
    async def from_protected_files(
        cls,
        *,
        identity_path: str | Path = LIGHTER_IDENTITY_PATH,
        api_key_path: str | Path = LIGHTER_API_KEY_PRIVATE_PATH,
        market_discoverer: Callable[[], Awaitable[MarketObservation]] | None = None,
        market_reader: Callable[[int], Awaitable[MarketObservation]] | None = None,
        snapshot_reader: Callable[..., Awaitable[AccountSnapshot]] | None = None,
        funding_reader: Callable[..., Awaitable[FundingHistory]] | None = None,
    ) -> "SdkLighterGateway":
        identity = load_identity_metadata(identity_path)
        private_key = load_api_key_private(api_key_path)
        try:
            lighter = importlib.import_module("lighter")
            nonce_type = lighter.nonce_manager.NonceManagerType.NONE
            client = lighter.SignerClient(
                TESTNET_API_URL,
                identity.account_index,
                {LIGHTER_API_KEY_INDEX: private_key},
                nonce_management_type=nonce_type,
                chain_id=TESTNET_CHAIN_ID,
            )
        except LifecycleHalt:
            raise
        except Exception:
            raise LifecycleHalt("LIGHTER_SDK_UNAVAILABLE", failure_class="AUTH")
        finally:
            # The signer client owns the in-memory value; this local reference
            # is deliberately dropped and is never persisted or reported.
            del private_key
        try:
            if all(value is None for value in (market_discoverer, market_reader, snapshot_reader, funding_reader)):
                api_client = getattr(client, "api_client", None)
                if api_client is None:
                    raise LifecycleHalt("LIGHTER_SDK_READ_API_UNAVAILABLE", failure_class="AUTH")

                def refresh_authorization() -> str:
                    auth_result = client.create_auth_token_with_expiry(
                        api_key_index=LIGHTER_API_KEY_INDEX
                    )
                    if not isinstance(auth_result, tuple) or len(auth_result) != 2:
                        raise LifecycleHalt("AUTH_TOKEN_RESPONSE_INVALID", failure_class="AUTH")
                    authorization, auth_error = auth_result
                    if auth_error or not isinstance(authorization, str) or not authorization:
                        raise LifecycleHalt("AUTH_TOKEN_UNAVAILABLE", failure_class="AUTH")
                    return authorization

                adapter = OfficialSdkReadAdapter(
                    identity=identity,
                    authorization_provider=refresh_authorization,
                    account_api=lighter.AccountApi(api_client),
                    order_api=lighter.OrderApi(api_client),
                    funding_api=lighter.FundingApi(api_client),
                    transaction_api=lighter.TransactionApi(api_client),
                )
                return cls(
                    client,
                    market_discoverer=adapter.discover_market,
                    market_reader=adapter.market,
                    snapshot_reader=adapter.snapshot,
                    funding_reader=adapter.funding_history,
                    lighter_module=lighter,
                    readiness_reader=adapter.readiness,
                    terminal_reader=adapter.terminal_round,
                    identity=identity,
                )
            if not all(value is not None for value in (market_discoverer, market_reader, snapshot_reader, funding_reader)):
                raise LifecycleHalt("INCOMPLETE_READ_ADAPTER", failure_class="SAFETY")
            return cls(
                client,
                market_discoverer=market_discoverer,
                market_reader=market_reader,
                snapshot_reader=snapshot_reader,
                funding_reader=funding_reader,
                lighter_module=lighter,
                identity=identity,
            )
        except (KeyboardInterrupt, SystemExit):
            await _close_without_masking(client)
            raise
        except LifecycleHalt:
            await _close_without_masking(client)
            raise
        except BaseException:
            await _close_without_masking(client)
            raise LifecycleHalt("LIGHTER_SDK_READ_API_UNAVAILABLE", failure_class="AUTH") from None

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await _await_close(self._signer)

    async def discover_market(self) -> MarketObservation:
        assert self._market_discoverer is not None
        return await self._market_discoverer()

    async def market(self, market_id: int) -> MarketObservation:
        assert self._market_reader is not None
        return await self._market_reader(market_id)

    async def snapshot(
        self, market_id: int, *, client_order_index: int | None = None
    ) -> AccountSnapshot:
        assert self._snapshot_reader is not None
        return await self._snapshot_reader(market_id, client_order_index=client_order_index)

    async def reconcile_nonce(self, *, account_index: int, api_key_index: int) -> int:
        if account_index != LIGHTER_ACCOUNT_INDEX or api_key_index != LIGHTER_API_KEY_INDEX:
            raise LifecycleHalt("EXPLICIT_NONCE_IDENTITY_INVALID", failure_class="IDENTITY")
        if self._nonce_reader is not None:
            nonce = await self._nonce_reader(account_index, api_key_index)
        else:
            api = getattr(self._signer, "tx_api", None)
            if api is None:
                raise LifecycleHalt("NONCE_READER_UNAVAILABLE", failure_class="SAFETY")
            response = await api.next_nonce(
                account_index=account_index,
                api_key_index=api_key_index,
                _request_timeout=30.0,
            )
            nonce = getattr(response, "nonce", None)
        if isinstance(nonce, bool) or not isinstance(nonce, int) or nonce < 0:
            raise LifecycleHalt("RECONCILED_NONCE_INVALID", failure_class="SCHEMA")
        return nonce

    async def funding_history(
        self,
        market_id: int,
        *,
        account_index: int,
        baseline_high_water: int,
        boundary_ms: int | None = None,
    ) -> FundingHistory:
        assert self._funding_reader is not None
        return await self._funding_reader(
            market_id,
            account_index=account_index,
            baseline_high_water=baseline_high_water,
            boundary_ms=boundary_ms,
        )

    async def terminal_round(self, market_id: int) -> TerminalRound:
        if self._terminal_reader is not None:
            return await self._terminal_reader(market_id)
        raise LifecycleHalt("TERMINAL_READER_UNAVAILABLE", failure_class="SAFETY")

    async def readiness(self) -> ReadinessResult:
        if self._readiness_reader is None:
            raise LifecycleHalt("READINESS_READER_UNAVAILABLE", failure_class="SAFETY")
        return await self._readiness_reader()

    async def create_order(
        self, request: OrderRequest, *, nonce: int, api_key_index: int
    ) -> DispatchOutcome:
        if api_key_index != LIGHTER_API_KEY_INDEX or nonce < 0:
            raise LifecycleHalt("EXPLICIT_KEY_AND_NONCE_REQUIRED", failure_class="SAFETY")
        order_type = 0 if request.order_type == "limit" else 1
        tif = 2 if request.time_in_force == "post_only" else 0
        try:
            with suppress_sdk_secret_debug():
                created, response, error = await self._signer.create_order(
                    market_index=request.market_id,
                    client_order_index=request.client_order_index,
                    base_amount=_wire_units(request.quantity, request.size_decimals, "BASE_AMOUNT"),
                    price=_wire_units(request.price, request.price_decimals, "PRICE"),
                    is_ask=request.is_ask,
                    order_type=order_type,
                    time_in_force=tif,
                    reduce_only=request.reduce_only,
                    trigger_price=0,
                    order_expiry=request.order_expiry,
                    skip_nonce=0,
                    nonce=nonce,
                    api_key_index=api_key_index,
                )
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise AmbiguousDispatch(_classify_sdk_error(exc)) from None
        if error:
            return DispatchOutcome(accepted=False, rejected=True, error_class="SDK_REJECTED")
        if created is None or response is None or getattr(response, "code", None) != 200:
            return DispatchOutcome(accepted=False, rejected=True, error_class="SDK_REJECTED")
        # Inspect only public transaction fields; never touch created.sig or raw
        # transaction JSON.  The authoritative order read remains mandatory.
        if (
            getattr(created, "order_book_index", None) != request.market_id
            or getattr(created, "account_index", None) != LIGHTER_ACCOUNT_INDEX
            or getattr(created, "base_amount", None)
            != _wire_units(request.quantity, request.size_decimals, "BASE_AMOUNT")
            or getattr(created, "price", None)
            != _wire_units(request.price, request.price_decimals, "PRICE")
            or getattr(created, "is_ask", None) != int(request.is_ask)
            or getattr(created, "order_type", None) != order_type
            or getattr(created, "nonce", None) != nonce
        ):
            raise LifecycleHalt("SDK_ORDER_RESPONSE_MISMATCH", failure_class="SAFETY")
        return DispatchOutcome(
            accepted=True,
            tx_hash=_safe_hash(getattr(response, "tx_hash", None)),
        )

    async def cancel_order(
        self,
        market_id: int,
        order_index: int,
        *,
        nonce: int,
        api_key_index: int,
    ) -> DispatchOutcome:
        if api_key_index != LIGHTER_API_KEY_INDEX or nonce < 0:
            raise LifecycleHalt("EXPLICIT_KEY_AND_NONCE_REQUIRED", failure_class="SAFETY")
        try:
            with suppress_sdk_secret_debug():
                cancelled, response, error = await self._signer.cancel_order(
                    market_index=market_id,
                    order_index=order_index,
                    skip_nonce=0,
                    nonce=nonce,
                    api_key_index=api_key_index,
                )
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise AmbiguousDispatch(_classify_sdk_error(exc)) from None
        if error or cancelled is None or response is None or getattr(response, "code", None) != 200:
            return DispatchOutcome(accepted=False, rejected=True, error_class="SDK_REJECTED")
        if (
            getattr(cancelled, "order_book_index", None) != market_id
            or getattr(cancelled, "account_index", None) != LIGHTER_ACCOUNT_INDEX
            or getattr(cancelled, "nonce", None) != nonce
        ):
            # order_nonce identifies the target order, while nonce identifies
            # this cancel transaction.  Only the latter can be compared with
            # the explicit nonce supplied to the signer.
            raise LifecycleHalt("SDK_CANCEL_RESPONSE_MISMATCH", failure_class="SAFETY")
        return DispatchOutcome(
            accepted=True,
            tx_hash=_safe_hash(getattr(response, "tx_hash", None)),
        )


def _find_order(snapshot: AccountSnapshot, intent: IntentSpec) -> OrderSnapshot:
    request = intent.request
    if request is None:
        if intent.target_order_index is None:
            raise LifecycleHalt("CANCEL_TARGET_MISSING")
        matches = [row for row in snapshot.orders if row.order_index == intent.target_order_index]
    else:
        matches = [
            row
            for row in snapshot.orders
            if row.client_order_index == request.client_order_index
        ]
    if len(matches) != 1:
        raise LifecycleHalt("EXACT_ORDER_RECONCILIATION_FAILED", failure_class="SAFETY")
    order = matches[0]
    expected_market_id = request.market_id if request is not None else intent.market_id
    if (
        order.account_index != LIGHTER_ACCOUNT_INDEX
        or order.market_id != expected_market_id
        or (request is not None and (
            order.quantity != request.quantity
            or order.price != request.price
            or order.is_ask != request.is_ask
            or order.order_type != request.order_type
            or order.time_in_force != request.time_in_force
            or order.reduce_only != request.reduce_only
            or order.nonce != intent.nonce
        ))
    ):
        raise LifecycleHalt("EXACT_ORDER_FIELDS_MISMATCH", failure_class="SAFETY")
    return order


def _reconcile_filled(
    snapshot: AccountSnapshot,
    intent: IntentSpec,
    *,
    expected_signed_quantity: Decimal,
) -> OrderSnapshot:
    request = intent.request
    if request is None:
        raise LifecycleHalt("FILL_INTENT_REQUEST_MISSING")
    order = _find_order(snapshot, intent)
    if order.status.upper() not in {"FILLED", "CLOSED", "EXECUTED"}:
        raise LifecycleHalt("EXACT_FILL_NOT_TERMINAL", failure_class="SAFETY")
    if order.remaining_quantity != 0:
        raise LifecycleHalt("EXACT_FILL_REMAINS_OPEN", failure_class="SAFETY")
    if not snapshot.unrelated_state_clear or snapshot.collateral <= 0:
        raise LifecycleHalt("FILL_ACCOUNT_STATE_UNSAFE", failure_class="SAFETY")
    if snapshot.maker_fee_tick != 0 or snapshot.taker_fee_tick != 0:
        raise LifecycleHalt("FILL_FEE_STATE_UNSAFE", failure_class="SAFETY")
    if any(row.active for row in snapshot.orders if row.order_index != order.order_index):
        raise LifecycleHalt("UNRELATED_ACTIVE_ORDER_AFTER_FILL", failure_class="SAFETY")
    if any(
        row.market_id != request.market_id and row.signed_quantity != 0
        for row in snapshot.positions
    ):
        raise LifecycleHalt("UNRELATED_POSITION_AFTER_FILL", failure_class="SAFETY")
    fills = [
        row
        for row in snapshot.fills
        if row.client_order_index == request.client_order_index
        and row.order_index == order.order_index
        and row.account_index == LIGHTER_ACCOUNT_INDEX
        and row.market_id == request.market_id
    ]
    if not fills or len({row.trade_id for row in fills}) != len(fills):
        raise LifecycleHalt("FILL_IDENTITY_INVALID", failure_class="SAFETY")
    if any(row.is_ask != request.is_ask or row.timestamp_ms > snapshot.observed_at_ms for row in fills):
        raise LifecycleHalt("FILL_FIELDS_MISMATCH", failure_class="SAFETY")
    total = sum((row.quantity for row in fills), Decimal(0))
    quote_total = sum((row.quantity * row.price for row in fills), Decimal(0))
    if (
        total != request.quantity
        or order.filled_quantity != request.quantity
        or order.filled_quote != quote_total
    ):
        raise LifecycleHalt("FILL_QUANTITY_MISMATCH", failure_class="SAFETY")
    position = snapshot.exact_position(request.market_id)
    if expected_signed_quantity != 0 and (position is None or position.signed_quantity != expected_signed_quantity):
        raise LifecycleHalt("POSITION_FILL_MISMATCH", failure_class="SAFETY")
    if expected_signed_quantity == 0 and position is not None and position.signed_quantity != 0:
        raise LifecycleHalt("POSITION_FILL_MISMATCH", failure_class="SAFETY")
    return order


def _validate_post_only(
    order: OrderSnapshot, intent: IntentSpec, *, market: MarketObservation | None = None
) -> None:
    request = intent.request
    if request is None or intent.kind != IntentKind.MAKER_PLACE:
        raise LifecycleHalt("POST_ONLY_INTENT_INVALID")
    if not order.active or order.filled_quantity != 0 or order.remaining_quantity != request.quantity:
        raise LifecycleHalt("POST_ONLY_ORDER_NOT_RESTING", failure_class="SAFETY")
    if market is not None and (
        (request.is_ask and request.price <= market.best_bid)
        or (not request.is_ask and request.price >= market.best_ask)
    ):
        raise LifecycleHalt("POST_ONLY_ORDER_MARKETABLE", failure_class="SAFETY")


class LighterLevelCRunner:
    """Ordered maker-cancel, IOC-open, funding, reduce-only-close runner."""

    def __init__(
        self,
        gateway: LighterGateway,
        store: LifecycleStore,
        *,
        readiness: ReadinessResult,
        identity: IdentityMetadata,
        mode: RunMode = RunMode.DRY_RUN,
        clock_ms: Callable[[], int] | None = None,
        sleep: Callable[[float], Awaitable[Any]] | None = None,
    ) -> None:
        self.gateway = gateway
        self.store = store
        self.readiness = readiness
        self.identity = identity
        self.mode = mode
        self.clock_ms = clock_ms or (lambda: int(time.time() * 1000))
        self.sleep = sleep or asyncio.sleep
        self._market: MarketObservation | None = None
        self._quantity: Decimal | None = None
        self._open_is_ask: bool | None = None
        self._funding_status = "NOT_STARTED"
        self._funding_change: Decimal | None = None
        self._terminal_rounds = 0

    async def _preflight(self) -> tuple[MarketObservation, AccountSnapshot, Decimal, bool, Decimal]:
        validate_readiness(self.readiness, self.identity)
        # Market ids are observed venue data.  Discovery is deliberately a
        # separate gateway operation so the old LIT observation cannot become
        # a write-time constant.
        market = await self.gateway.discover_market()
        market.validate(now_ms=self.clock_ms())
        if market.contract.symbol != LIGHTER_SYMBOL:
            raise LifecycleHalt("LIT_MARKET_REQUIRED", failure_class="IDENTITY")
        account = await self.gateway.snapshot(market.market_id)
        account.validate_identity_and_safety(
            self.identity,
            market_id=market.market_id,
            require_flat=True,
            observed_at_ms=self.clock_ms(),
        )
        # Use both directions as a current observation and deterministically
        # choose the side with the smaller executable quote cost, never a
        # tentative market constant.
        buy_quantity, buy_worst = smallest_executable_quantity(market, is_ask=False)
        sell_quantity, sell_worst = smallest_executable_quantity(market, is_ask=True)
        if buy_quantity < sell_quantity or (buy_quantity == sell_quantity and buy_worst <= sell_worst):
            quantity, is_ask, worst = buy_quantity, False, buy_worst
        else:
            quantity, is_ask, worst = sell_quantity, True, sell_worst
        if quantity <= 0 or quantity * worst < market.contract.min_quote_amount:
            raise LifecycleHalt("SMALLEST_EXECUTABLE_QUANTITY_INVALID", failure_class="SAFETY")
        return market, account, quantity, is_ask, worst

    async def _fresh_prewrite(self) -> tuple[MarketObservation, AccountSnapshot]:
        if self._market is None:
            raise LifecycleHalt("MARKET_NOT_BOUND")
        market = await self.gateway.market(self._market.market_id)
        market_now = self.clock_ms()
        market.validate(now_ms=market_now)
        if (
            market.contract != self._market.contract
            or market.funding.next_boundary_ms != self._market.funding.next_boundary_ms
        ):
            raise LifecycleHalt("MARKET_CHANGED_BEFORE_WRITE", failure_class="SAFETY")
        self._market = market
        account = await self.gateway.snapshot(market.market_id)
        account_now = self.clock_ms()
        account.validate_identity_and_safety(
            self.identity,
            market_id=market.market_id,
            require_flat=True,
            observed_at_ms=account_now,
        )
        return market, account

    async def _fresh_nonce(self) -> int:
        nonce = await self.gateway.reconcile_nonce(
            account_index=self.identity.account_index,
            api_key_index=LIGHTER_API_KEY_INDEX,
        )
        if isinstance(nonce, bool) or not isinstance(nonce, int) or nonce < 0:
            raise LifecycleHalt("RECONCILED_NONCE_INVALID", failure_class="SAFETY")
        return nonce

    async def _dispatch(self, intent: IntentSpec) -> DispatchOutcome:
        self.store.create_intent(intent, now_ms=self.clock_ms())
        self.store.claim_dispatch(intent.intent_id, now_ms=self.clock_ms())
        try:
            if intent.kind == IntentKind.MAKER_CANCEL:
                if intent.target_order_index is None or intent.market_id is None:
                    raise LifecycleHalt("CANCEL_VECTOR_INVALID")
                outcome = await self.gateway.cancel_order(
                    intent.market_id,
                    intent.target_order_index,
                    nonce=intent.nonce,
                    api_key_index=intent.api_key_index,
                )
            else:
                if intent.request is None:
                    raise LifecycleHalt("ORDER_VECTOR_MISSING")
                outcome = await self.gateway.create_order(
                    intent.request,
                    nonce=intent.nonce,
                    api_key_index=intent.api_key_index,
                )
        except AmbiguousDispatch as exc:
            self._mark_ambiguous(intent, exc.reason)
            raise
        except LifecycleHalt:
            # A claimed dispatch has crossed the durable send boundary.  A
            # semantic SDK response failure is therefore not an ordinary
            # pre-write block: the venue may have accepted the operation.
            self._mark_ambiguous(intent, "POST_CLAIM_WRITE_UNRESOLVED")
            raise AmbiguousDispatch("POST_CLAIM_WRITE_UNRESOLVED") from None
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            self._mark_ambiguous(intent, "SDK_TRANSPORT_FAILURE")
            raise AmbiguousDispatch("SDK_TRANSPORT_FAILURE") from None
        try:
            self.store.record_dispatch(intent.intent_id, outcome, now_ms=self.clock_ms())
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            self._mark_ambiguous(intent, "DISPATCH_RESULT_NOT_RECORDABLE")
            raise AmbiguousDispatch("DISPATCH_RESULT_NOT_RECORDABLE") from None
        if not outcome.accepted:
            raise LifecycleHalt(outcome.error_class or "VENUE_REJECTED", failure_class="HTTP")
        return outcome

    def _mark_ambiguous(self, intent: IntentSpec, code: str) -> None:
        try:
            self.store.mark_ambiguous(intent.intent_id, now_ms=self.clock_ms(), failure_class=code)
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            # The journal failure itself is manual recovery evidence.  Do not
            # let it turn a post-claim uncertainty into an ordinary BLOCKED.
            raise AmbiguousDispatch("WRITE_OUTCOME_UNRESOLVED") from None

    async def _post_send_reconcile(
        self,
        intent: IntentSpec,
        operation: Callable[[], Awaitable[Any]],
    ) -> Any:
        """Run authoritative post-send checks and mark uncertainty durably."""

        try:
            return await operation()
        except (KeyboardInterrupt, SystemExit):
            raise
        except AmbiguousDispatch:
            self._mark_ambiguous(intent, "POST_SEND_RECONCILIATION_UNRESOLVED")
            raise
        except BaseException:
            self._mark_ambiguous(intent, "POST_SEND_RECONCILIATION_UNRESOLVED")
            raise AmbiguousDispatch("POST_SEND_RECONCILIATION_UNRESOLVED") from None

    def _record_reconciled(self, intent: IntentSpec, summary: Mapping[str, Any]) -> None:
        self.store.reconcile(intent.intent_id, _digest(summary), now_ms=self.clock_ms())
        self.store.record_evidence("ORDER", summary, now_ms=self.clock_ms())

    async def _maker_phase(self, market: MarketObservation, account: AccountSnapshot) -> None:
        market, account = await self._fresh_prewrite()
        maker_is_ask = not bool(self._open_is_ask)
        maker_price = market.best_ask if maker_is_ask else market.best_bid
        request = OrderRequest(
            market_id=market.market_id,
            client_order_index=_new_client_order_index(),
            quantity=self._quantity or Decimal(0),
            price=maker_price,
            is_ask=maker_is_ask,
            order_type="limit",
            time_in_force="post_only",
            reduce_only=False,
            order_expiry=-1,
            size_decimals=market.contract.size_decimals,
            price_decimals=market.contract.price_decimals,
        )
        nonce = await self._fresh_nonce()
        place = IntentSpec(_new_id("maker-place"), IntentKind.MAKER_PLACE, request, None, nonce)
        await self._dispatch(place)
        async def reconcile_place() -> OrderSnapshot:
            after_place = await self.gateway.snapshot(
                market.market_id, client_order_index=request.client_order_index
            )
            order = _find_order(after_place, place)
            _validate_post_only(order, place, market=market)
            active_other = tuple(
                row
                for row in (*after_place.active_regular_orders, *after_place.active_trigger_orders)
                if row.order_index != order.order_index
            )
            if active_other:
                raise LifecycleHalt("UNRELATED_ACTIVE_ORDER_AFTER_MAKER", failure_class="SAFETY")
            self._record_reconciled(
                place,
                {
                    "client_order_index": request.client_order_index,
                    "kind": place.kind.value,
                    "market_id": market.market_id,
                    "order_index": order.order_index,
                    "status": order.status,
                },
            )
            return order

        order = await self._post_send_reconcile(place, reconcile_place)
        cancel_nonce = await self._fresh_nonce()
        cancel = IntentSpec(
            _new_id("maker-cancel"),
            IntentKind.MAKER_CANCEL,
            None,
            order.order_index,
            cancel_nonce,
            market_id=market.market_id,
        )
        await self._dispatch(cancel)
        async def reconcile_cancel() -> OrderSnapshot:
            after_cancel = await self.gateway.snapshot(
                market.market_id, client_order_index=request.client_order_index
            )
            after_cancel.validate_identity_and_safety(
                self.identity,
                market_id=market.market_id,
                require_flat=True,
                observed_at_ms=self.clock_ms(),
            )
            if after_cancel.active_regular_orders or after_cancel.active_trigger_orders:
                raise LifecycleHalt("MAKER_CANCEL_DID_NOT_PROVE_ZERO_ORDERS", failure_class="SAFETY")
            cancelled = _find_order(after_cancel, cancel)
            if cancelled.status.upper() not in {"CANCELLED", "EXPIRED"} or cancelled.filled_quantity != 0:
                raise LifecycleHalt("MAKER_CANCEL_RECONCILIATION_FAILED", failure_class="SAFETY")
            self._record_reconciled(
                cancel,
                {
                    "kind": cancel.kind.value,
                    "market_id": market.market_id,
                    "order_index": order.order_index,
                    "status": cancelled.status,
                },
            )
            return cancelled

        await self._post_send_reconcile(cancel, reconcile_cancel)

    async def _open_phase(self, market: MarketObservation, account: AccountSnapshot) -> None:
        market, account = await self._fresh_prewrite()
        quantity, worst = smallest_executable_quantity(market, is_ask=bool(self._open_is_ask))
        is_ask = bool(self._open_is_ask)
        if quantity != self._quantity:
            raise LifecycleHalt("OPEN_QUANTITY_CHANGED_BEFORE_WRITE", failure_class="SAFETY")
        request = OrderRequest(
            market_id=market.market_id,
            client_order_index=_new_client_order_index(),
            quantity=quantity,
            price=worst,
            is_ask=is_ask,
            order_type="market",
            time_in_force="ioc",
            reduce_only=False,
            order_expiry=0,
            size_decimals=market.contract.size_decimals,
            price_decimals=market.contract.price_decimals,
        )
        nonce = await self._fresh_nonce()
        intent = IntentSpec(_new_id("open"), IntentKind.OPEN, request, None, nonce)
        await self._dispatch(intent)
        expected = -quantity if is_ask else quantity

        async def reconcile_open() -> OrderSnapshot:
            after = await self.gateway.snapshot(
                market.market_id, client_order_index=request.client_order_index
            )
            order = _reconcile_filled(after, intent, expected_signed_quantity=expected)
            self.store.record_evidence(
                "OPEN_FILL",
                {
                    "filled_quantity": str(order.filled_quantity),
                    "kind": intent.kind.value,
                    "market_id": market.market_id,
                    "order_index": order.order_index,
                    "position": str(expected),
                },
                now_ms=self.clock_ms(),
            )
            self.store.reconcile(
                intent.intent_id,
                _digest({"order_index": order.order_index, "position": str(expected)}),
                now_ms=self.clock_ms(),
            )
            return order

        await self._post_send_reconcile(intent, reconcile_open)

    async def _funding_phase(self, market: MarketObservation, account: AccountSnapshot) -> None:
        self._funding_status = "WAITING_FOR_BOUNDARY"
        baseline = account.funding_high_water
        self.store.record_evidence(
            "FUNDING_PRE",
            {"baseline_high_water": baseline, "boundary_ms": market.funding.next_boundary_ms, "market_id": market.market_id},
            now_ms=self.clock_ms(),
        )
        delay = max(0, (market.funding.next_boundary_ms - self.clock_ms()) / 1000)
        await self.sleep(delay)
        if self.clock_ms() < market.funding.next_boundary_ms:
            raise LifecycleHalt("FUNDING_BOUNDARY_NOT_REACHED", failure_class="SAFETY")
        at_boundary = await self.gateway.snapshot(market.market_id)
        at_boundary.validate_identity_and_safety(
            self.identity,
            market_id=market.market_id,
            require_flat=False,
            observed_at_ms=self.clock_ms(),
        )
        if any(
            row.market_id != market.market_id and row.signed_quantity != 0
            for row in at_boundary.positions
        ):
            raise LifecycleHalt("UNRELATED_POSITION_AT_FUNDING_BOUNDARY", failure_class="SAFETY")
        position = at_boundary.exact_position(market.market_id)
        expected = -self._quantity if self._open_is_ask else self._quantity
        if position is None or position.signed_quantity != expected:
            raise LifecycleHalt("POSITION_NOT_HELD_AT_FUNDING_BOUNDARY", failure_class="SAFETY")
        self.store.record_evidence(
            "FUNDING_BOUNDARY",
            {"boundary_ms": market.funding.next_boundary_ms, "market_id": market.market_id, "position": str(expected)},
            now_ms=self.clock_ms(),
        )
        history = await self.gateway.funding_history(
            market.market_id,
            account_index=self.identity.account_index,
            baseline_high_water=baseline,
            boundary_ms=market.funding.next_boundary_ms,
        )
        if history.baseline_high_water != baseline:
            raise LifecycleHalt("FUNDING_BASELINE_MISMATCH", failure_class="IDENTITY")
        record = history.attributable(
            market_id=market.market_id,
            quantity=self._quantity or Decimal(0),
            is_ask=bool(self._open_is_ask),
            boundary_ms=market.funding.next_boundary_ms,
        )
        self._funding_status = "AUTHORITATIVE"
        self._funding_change = record.change
        self.store.record_evidence(
            "FUNDING_POST",
            {
                "change": str(record.change),
                "funding_id": record.funding_id,
                "market_id": record.market_id,
                "timestamp_ms": record.timestamp_ms,
            },
            now_ms=self.clock_ms(),
        )

    async def _close_phase(self, market: MarketObservation) -> None:
        fresh_market = await self.gateway.market(market.market_id)
        market_now = self.clock_ms()
        fresh_market.validate(now_ms=market_now, require_future_funding=False)
        if fresh_market.contract != market.contract:
            raise LifecycleHalt("MARKET_CHANGED_BEFORE_CLOSE", failure_class="SAFETY")
        account = await self.gateway.snapshot(market.market_id)
        account_now = self.clock_ms()
        account.validate_identity_and_safety(
            self.identity,
            market_id=market.market_id,
            require_flat=False,
            observed_at_ms=account_now,
        )
        if any(
            row.market_id != market.market_id and row.signed_quantity != 0
            for row in account.positions
        ):
            raise LifecycleHalt("CLOSE_UNRELATED_POSITION", failure_class="SAFETY")
        position = account.exact_position(market.market_id)
        expected_open = -self._quantity if self._open_is_ask else self._quantity
        if position is None or position.signed_quantity != expected_open:
            raise LifecycleHalt("CLOSE_POSITION_IDENTITY_INVALID", failure_class="SAFETY")
        close_is_ask = position.signed_quantity > 0
        quantity, worst = smallest_executable_quantity(fresh_market, is_ask=close_is_ask)
        if quantity != abs(position.signed_quantity):
            # Close quantity is state-derived and may not be replaced by the
            # current minimum; exact position size is mandatory.
            _, worst = _walk(
                tuple(sorted(fresh_market.bids if close_is_ask else fresh_market.asks, key=lambda row: row.price, reverse=close_is_ask)),
                abs(position.signed_quantity),
            )
            quantity = abs(position.signed_quantity)
        request = OrderRequest(
            market_id=market.market_id,
            client_order_index=_new_client_order_index(),
            quantity=quantity,
            price=worst,
            is_ask=close_is_ask,
            order_type="market",
            time_in_force="ioc",
            reduce_only=True,
            order_expiry=0,
            size_decimals=fresh_market.contract.size_decimals,
            price_decimals=fresh_market.contract.price_decimals,
        )
        nonce = await self._fresh_nonce()
        intent = IntentSpec(_new_id("close"), IntentKind.CLOSE, request, None, nonce)
        await self._dispatch(intent)

        async def reconcile_close() -> OrderSnapshot:
            after = await self.gateway.snapshot(
                market.market_id, client_order_index=request.client_order_index
            )
            order = _reconcile_filled(after, intent, expected_signed_quantity=Decimal(0))
            self._record_reconciled(
                intent,
                {
                    "kind": intent.kind.value,
                    "market_id": market.market_id,
                    "order_index": order.order_index,
                    "position": "0",
                },
            )
            return order

        await self._post_send_reconcile(intent, reconcile_close)

    async def _terminal_phase(self, market: MarketObservation) -> None:
        if not self.store.all_intents_reconciled():
            raise LifecycleHalt("TERMINAL_INTENTS_NOT_RECONCILED", failure_class="SAFETY")
        first = await self.gateway.terminal_round(market.market_id)
        first_now = self.clock_ms()
        first.validate(
            self.identity,
            market_id=market.market_id,
            now_ms=first_now,
            max_age_ms=TERMINAL_ROUND_MAX_AGE_MS,
        )
        second = await self.gateway.terminal_round(market.market_id)
        second_now = self.clock_ms()
        second.validate(
            self.identity,
            market_id=market.market_id,
            now_ms=second_now,
            max_age_ms=TERMINAL_ROUND_MAX_AGE_MS,
        )
        rounds = (first, second)
        if (
            rounds[0].round_id == rounds[1].round_id
            or rounds[0].digest != rounds[1].digest
            or rounds[1].observed_at_ms <= rounds[0].observed_at_ms
            or rounds[1].observed_at_ms - rounds[0].observed_at_ms > TERMINAL_ROUND_MAX_AGE_MS
            or second_now - rounds[0].observed_at_ms > TERMINAL_ROUND_MAX_AGE_MS
        ):
            raise LifecycleHalt("TERMINAL_ROUNDS_DISAGREE", failure_class="SAFETY")
        for value in rounds:
            self.store.record_terminal(value, now_ms=value.observed_at_ms)
        self._terminal_rounds = 2

    def _report(self, result: RunnerResult, *, failure_class: str = "NONE", reason: str = "") -> RunnerReport:
        return RunnerReport(
            run_id=self.store.run_id,
            result=result,
            failure_class=failure_class,
            reason=reason,
            market_id=None if self._market is None else self._market.market_id,
            quantity=self._quantity,
            open_is_ask=self._open_is_ask,
            funding_status=self._funding_status,
            funding_change=self._funding_change,
            intent_count=self.store.intent_count(),
            dispatch_count=self.store.dispatch_count(),
            terminal_rounds=self._terminal_rounds,
            sqlite_integrity=self.store.integrity(),
            mode=self.mode,
        )

    async def run(self) -> RunnerReport:
        try:
            self.store.begin()
            market, account, quantity, is_ask, _ = await self._preflight()
            self._market = market
            self._quantity = quantity
            self._open_is_ask = is_ask
            if self.mode in {RunMode.DRY_RUN, RunMode.PREPARE_ONLY}:
                self._funding_status = "NOT_EXECUTED"
                self.store.finish(self.mode.value)
                return self._report(RunnerResult.PREPARED, reason="NO_TESTNET_DISPATCH_IN_PREPARE_MODE")
            await self._maker_phase(market, account)
            await self._open_phase(market, account)
            funding_failure: LifecycleHalt | None = None
            try:
                await self._funding_phase(market, account)
            except LifecycleHalt as exc:
                funding_failure = exc
                self._funding_status = "BLOCKED"
                self.store.record_evidence(
                    "FUNDING_BLOCKED",
                    {"failure_class": exc.failure_class, "reason": exc.reason},
                    now_ms=self.clock_ms(),
                )
            await self._close_phase(market)
            await self._terminal_phase(market)
            if funding_failure is not None:
                self.store.finish("BLOCKED")
                return self._report(
                    RunnerResult.BLOCKED,
                    failure_class=funding_failure.failure_class,
                    reason=funding_failure.reason,
                )
            self.store.finish("COMPLETE")
            return self._report(RunnerResult.COMPLETE, reason="LEVEL_C_COMPLETE")
        except AmbiguousDispatch as exc:
            self.store.finish("HALTED_MANUAL_RECOVERY")
            return self._report(RunnerResult.HALTED_MANUAL_RECOVERY, failure_class=exc.failure_class, reason=exc.reason)
        except LifecycleHalt as exc:
            self.store.finish("BLOCKED")
            return self._report(RunnerResult.BLOCKED, failure_class=exc.failure_class, reason=exc.reason)
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            self.store.finish("HALTED_MANUAL_RECOVERY")
            return self._report(RunnerResult.HALTED_MANUAL_RECOVERY, failure_class="UNCLASSIFIED", reason="UNCLASSIFIED_LIFECYCLE_FAILURE")


TESTNET_WRITE_ACTIVATION_TOKEN = "LIGHTER_TESTNET_LEVEL_C_WRITE"


async def run_isolated_lighter_testnet_level_c(
    *,
    db_path: str | Path | None,
    mode: RunMode = RunMode.DRY_RUN,
    activation_token: str | None = None,
    identity_path: str | Path = LIGHTER_IDENTITY_PATH,
    api_key_path: str | Path = LIGHTER_API_KEY_PRIVATE_PATH,
) -> RunnerReport:
    """Run only the isolated Lighter testnet path with a fresh journal."""

    if db_path is None:
        raise LifecycleHalt("FRESH_DATABASE_PATH_REQUIRED", failure_class="SAFETY")
    try:
        selected_mode = RunMode(mode)
    except (TypeError, ValueError):
        raise LifecycleHalt("RUN_MODE_INVALID", failure_class="SAFETY") from None
    if selected_mode is RunMode.TESTNET_WRITE and activation_token != TESTNET_WRITE_ACTIVATION_TOKEN:
        raise LifecycleHalt("EXPLICIT_TESTNET_WRITE_ACTIVATION_REQUIRED", failure_class="SAFETY")

    store: LifecycleStore | None = None
    gateway: Any | None = None
    primary_error: BaseException | None = None
    try:
        identity = load_identity_metadata(identity_path)
        store = LifecycleStore(db_path)
        gateway_factory = SdkLighterGateway.from_protected_files(
            identity_path=identity_path,
            api_key_path=api_key_path,
        )
        gateway = (
            await gateway_factory
            if inspect.isawaitable(gateway_factory)
            else gateway_factory
        )
        readiness = await gateway.readiness()
        runner = LighterLevelCRunner(
            gateway,
            store,
            readiness=readiness,
            identity=identity,
            mode=selected_mode,
        )
        return await runner.run()
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        try:
            if gateway is not None:
                if primary_error is None:
                    await _await_close(gateway)
                else:
                    await _close_without_masking(gateway)
        finally:
            if store is not None:
                store.close()


def run_lighter_testnet_level_c(
    *,
    db_path: str | Path | None,
    mode: RunMode = RunMode.DRY_RUN,
    activation_token: str | None = None,
    identity_path: str | Path = LIGHTER_IDENTITY_PATH,
    api_key_path: str | Path = LIGHTER_API_KEY_PRIVATE_PATH,
) -> RunnerReport:
    return asyncio.run(
        run_isolated_lighter_testnet_level_c(
            db_path=db_path,
            mode=mode,
            activation_token=activation_token,
            identity_path=identity_path,
            api_key_path=api_key_path,
        )
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lighter-testnet-level-c")
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--mode", choices=[value.value for value in RunMode], default=RunMode.DRY_RUN.value)
    parser.add_argument("--confirm-testnet-write", action="store_true")
    args = parser.parse_args(argv)
    token = TESTNET_WRITE_ACTIVATION_TOKEN if args.confirm_testnet_write else None
    try:
        report = run_lighter_testnet_level_c(
            db_path=args.db_path,
            mode=RunMode(args.mode),
            activation_token=token,
        )
    except LifecycleHalt as exc:
        print(json.dumps({"failure_class": exc.failure_class, "reason": exc.reason, "result": "BLOCKED"}, sort_keys=True))
        return 2
    print(json.dumps(report.sanitized(), sort_keys=True))
    return 0 if report.result in {RunnerResult.PREPARED, RunnerResult.COMPLETE} else 2


__all__ = [
    "AccountSnapshot",
    "AmbiguousDispatch",
    "BookLevel",
    "DispatchOutcome",
    "FillSnapshot",
    "FundingHistory",
    "FundingRecord",
    "FundingSchedule",
    "FUNDING_POLL_INTERVAL_SECONDS",
    "FUNDING_SETTLEMENT_WINDOW_SECONDS",
    "HOURLY_FUNDING_INTERVAL_MS",
    "IdentityMetadata",
    "IntentKind",
    "IntentSpec",
    "IntentState",
    "LifecycleHalt",
    "LifecycleStore",
    "LIGHTER_ACCOUNT_INDEX",
    "LIGHTER_API_KEY_INDEX",
    "LIGHTER_API_KEY_PRIVATE_PATH",
    "LIGHTER_IDENTITY_PATH",
    "LIGHTER_SDK_COMMIT",
    "LIGHTER_SYMBOL",
    "LighterGateway",
    "LighterLevelCRunner",
    "OfficialSdkReadAdapter",
    "MarketObservation",
    "OrderRequest",
    "OrderSnapshot",
    "PositionSnapshot",
    "RunMode",
    "RunnerReport",
    "RunnerResult",
    "SdkLighterGateway",
    "SdkSecretLogFilter",
    "SDK_READ_TIMEOUT_SECONDS",
    "TESTNET_WRITE_ACTIVATION_TOKEN",
    "TerminalRound",
    "load_api_key_private",
    "load_identity_metadata",
    "smallest_executable_quantity",
    "suppress_sdk_secret_debug",
    "validate_readiness",
    "run_isolated_lighter_testnet_level_c",
    "run_lighter_testnet_level_c",
]
