"""One-shot Extended mainnet authenticated read-only account gate.

The normal Farmer never imports this module.  Its only production entry point
uses the protected read-only API-key handle created by
``extended_mainnet_credential_onboarding`` and fixed mainnet endpoints.  The
gate performs GET requests only, never accepts or opens the reserved Stark
credential, never prepares a signed object, and never exposes a write-capable
transport.

The response decoders follow the current official Extended API documentation:
https://api.docs.extended.exchange/.  The official Python SDK is not needed
for this API-key-only read contract; direct REST/WebSocket requests are kept
explicit so the bounded pagination, identity, state, and redaction barriers
remain visible in this conformance implementation.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
import hashlib
import inspect
import json
import os
from pathlib import Path
import secrets
import sqlite3
import stat
import sys
import time
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import urlencode

import aiohttp

from . import extended_mainnet_credential_onboarding as onboarding


VENUE = "Extended"
ENVIRONMENT = "MAINNET"
STATUS_READY = "READY"
STATUS_BLOCKED = "BLOCKED"
# Kept as a narrow compatibility alias for callers that use the repository's
# existing READY/BLOCKED vocabulary.
BLOCKED = STATUS_BLOCKED
NO_MAINNET_WRITE_AUTHORITY = onboarding.NO_MAINNET_WRITE_AUTHORITY

MAINNET_REST_BASE_URL = "https://api.starknet.extended.exchange/api/v1"
MAINNET_STREAM_URL = (
    "wss://api.starknet.extended.exchange/"
    "stream.extended.exchange/v1/account"
)
REST_BASE_URL = MAINNET_REST_BASE_URL
STREAM_URL = MAINNET_STREAM_URL
USER_AGENT = "X10PythonTradingClient/2.5.0"
API_KEY_HEADER = "X-Api-Key"
HTTP_METHOD = "GET"

ACCOUNT_INFO_PATH = "/user/account/info"
ACCOUNTS_PATH = "/user/accounts"
BALANCE_PATH = "/user/balance"
SPOT_BALANCES_PATH = "/user/spot/balances"
ASSET_OPERATIONS_PATH = "/user/assetOperations"
FEES_PATH = "/user/fees"
OPEN_ORDERS_PATH = "/user/orders"
ORDER_HISTORY_PATH = "/user/orders/history"
TRADES_PATH = "/user/trades"
POSITIONS_PATH = "/user/positions"
POSITION_HISTORY_PATH = "/user/positions/history"
FUNDING_HISTORY_PATH = "/user/funding/history"
FEE_MARKET = "BTC-USD"
# Extended's live mainnet balance contract uses the denomination ``USD``.
# The spot-balance contract also documents ``USDC`` as a collateral entry.
# Keep this explicit set closed-world; an arbitrary asset is never collateral
# merely because it has a balance.
BALANCE_COLLATERAL_NAME = "USD"
_COLLATERAL_SPOT_ASSETS = frozenset({BALANCE_COLLATERAL_NAME, "USDC"})

EXPECTED_ACCOUNT_ID = 303919
EXPECTED_ACCOUNT_INDEX = 0
EXPECTED_L2_KEY = (
    "0xa78ee93989d14c80ea6a5423053260599b0d2d723d4381382799bd027dd555"
)
EXPECTED_L2_VAULT = 403919
EXPECTED_IDENTITY = onboarding.ExtendedPublicIdentity.from_inputs(
    str(EXPECTED_ACCOUNT_ID),
    str(EXPECTED_ACCOUNT_INDEX),
    EXPECTED_L2_KEY,
    str(EXPECTED_L2_VAULT),
)

SCHEMA_VERSION = 1
REQUEST_TIMEOUT_SECONDS = 10.0
STREAM_READ_TIMEOUT_SECONDS = 10.0
MAX_RESPONSE_BYTES = 1_048_576
MAX_STREAM_FRAME_BYTES = 1_048_576
MAX_ACCOUNT_ROWS = 64
MAX_PAGE_ITEMS = 256
ASSET_OPERATION_PAGE_ITEMS = 50
MAX_PAGES = 64

RUN_DIRECTORY = (
    Path.home() / ".config" / "risex-farmer" / "extended-mainnet-private-read"
)
RUN_STORE_BASENAME = "runs-v1.sqlite3"
RUN_STORE_PATH = RUN_DIRECTORY / RUN_STORE_BASENAME
REDACTED_RUN_STORE_PATH = (
    "<home>/.config/risex-farmer/extended-mainnet-private-read/" + RUN_STORE_BASENAME
)
RUN_DIRECTORY_MODE = 0o700
RUN_STORE_MODE = 0o600

FAILURE_CLASSES = frozenset(
    {"TRANSPORT", "HTTP", "SCHEMA", "AUTH", "IDENTITY", "SAFETY"}
)
_OPEN_ORDER_STATUSES = frozenset({"NEW", "PARTIALLY_FILLED", "UNTRIGGERED"})
_HISTORY_ORDER_STATUSES = frozenset({"FILLED", "CANCELLED", "REJECTED", "EXPIRED"})
_ALL_ORDER_STATUSES = _OPEN_ORDER_STATUSES | _HISTORY_ORDER_STATUSES | {
    "TRIGGERED"
}
_ORDER_TYPES = frozenset({"LIMIT", "MARKET", "CONDITIONAL", "TPSL", "TWAP"})
_ORDER_SIDES = frozenset({"BUY", "SELL"})
_POSITION_SIDES = frozenset({"LONG", "SHORT"})
_TRADE_TYPES = frozenset({"TRADE", "LIQUIDATION", "DELEVERAGE"})
_OPERATION_TYPES = frozenset({"DEPOSIT", "CLAIM", "TRANSFER", "WITHDRAWAL"})
_OPERATION_STATUSES = frozenset(
    {"CREATED", "IN_PROGRESS", "COMPLETED", "REJECTED"}
)
_STREAM_TYPES = frozenset({"BALANCE", "ORDER", "TRADE", "POSITION", "SPOT_BALANCE"})
_STREAM_COMPONENTS = frozenset({"BALANCE", "ORDER", "POSITION", "SPOT_BALANCE"})


class GateFailure(Exception):
    """A fixed, redaction-safe terminal gate failure."""

    def __init__(self, reason: str, failure_class: str) -> None:
        if (
            type(reason) is not str
            or not reason
            or any(char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_" for char in reason)
            or failure_class not in FAILURE_CLASSES
        ):
            raise ValueError("invalid gate failure")
        self.reason = reason
        self.failure_class = failure_class
        super().__init__(reason)


class TransportInterruption(Exception):
    """A transport interruption eligible for one bounded fresh retry."""


class StoreFailure(GateFailure):
    """A durable-journal or no-replay contract failure."""


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _digest(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _decimal(value: Any, field: str, *, positive: bool = False, nonnegative: bool = False) -> Decimal:
    if isinstance(value, Decimal):
        result = value
    elif type(value) is int:
        result = Decimal(value)
    elif type(value) is str:
        try:
            result = Decimal(value)
        except InvalidOperation as exc:
            raise GateFailure(f"DECIMAL_INVALID_{field}", "SCHEMA") from exc
    else:
        raise GateFailure(f"DECIMAL_INVALID_{field}", "SCHEMA")
    if not result.is_finite():
        raise GateFailure(f"DECIMAL_INVALID_{field}", "SCHEMA")
    if positive and result <= 0:
        raise GateFailure(f"POSITIVE_VALUE_REQUIRED_{field}", "SCHEMA")
    if nonnegative and result < 0:
        raise GateFailure(f"NONNEGATIVE_VALUE_REQUIRED_{field}", "SCHEMA")
    return result


def _number_text(value: Decimal) -> str:
    return format(value, "f")


def _required(value: Any, key: str, field: str) -> Any:
    if not isinstance(value, Mapping) or key not in value:
        raise GateFailure(f"FIELD_MISSING_{field}", "SCHEMA")
    return value[key]


def _text(value: Any, field: str) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise GateFailure(f"TEXT_INVALID_{field}", "SCHEMA")
    return value


def _integer(value: Any, field: str, *, positive: bool = False, nonnegative: bool = False) -> int:
    if type(value) is int and not isinstance(value, bool):
        result = value
    elif type(value) is str and value and value.isdecimal() and not (
        len(value) > 1 and value.startswith("0")
    ):
        result = int(value, 10)
    else:
        raise GateFailure(f"INTEGER_INVALID_{field}", "SCHEMA")
    if positive and result <= 0:
        raise GateFailure(f"POSITIVE_INTEGER_REQUIRED_{field}", "SCHEMA")
    if nonnegative and result < 0:
        raise GateFailure(f"NONNEGATIVE_INTEGER_REQUIRED_{field}", "SCHEMA")
    return result


def _optional_integer(value: Mapping[str, Any], key: str, field: str) -> int | None:
    if key not in value or value[key] is None:
        return None
    return _integer(value[key], field, nonnegative=True)


def _optional_decimal(
    value: Mapping[str, Any], key: str, field: str, *, nonnegative: bool = False
) -> Decimal | None:
    if key not in value or value[key] is None or value[key] == "":
        return None
    return _decimal(value[key], field, nonnegative=nonnegative)


def _canonical_l2_key(value: Any) -> str:
    if type(value) is not str or not value.startswith("0x") or not value[2:]:
        raise GateFailure("L2_KEY_INVALID", "SCHEMA")
    if any(char not in "0123456789abcdefABCDEF" for char in value[2:]):
        raise GateFailure("L2_KEY_INVALID", "SCHEMA")
    try:
        parsed = int(value[2:], 16)
    except ValueError as exc:
        raise GateFailure("L2_KEY_INVALID", "SCHEMA") from exc
    if parsed <= 0:
        raise GateFailure("L2_KEY_INVALID", "SCHEMA")
    return f"0x{parsed:x}"


def _operation_id(value: Any) -> str:
    if type(value) is int and value > 0:
        return str(value)
    if type(value) is str and value:
        if value.isdecimal() and int(value) > 0:
            return str(int(value))
        if value.startswith("0x") and len(value) > 2 and all(
            char in "0123456789abcdefABCDEF" for char in value[2:]
        ):
            return value.lower()
    raise GateFailure("OPERATION_ID_INVALID", "SCHEMA")


def _account_id(value: Any, field: str = "ACCOUNT_ID") -> int:
    return _integer(value, field, nonnegative=True)


def _canonical_identity_from_info(value: Mapping[str, Any]) -> dict[str, Any]:
    account_id = _account_id(_required(value, "accountId", "ACCOUNT_INFO"))
    account_index = _optional_integer(value, "accountIndex", "ACCOUNT_INDEX")
    l2_key = _canonical_l2_key(_required(value, "l2Key", "ACCOUNT_INFO"))
    l2_vault = _account_id(_required(value, "l2Vault", "ACCOUNT_INFO"), "L2_VAULT")
    status = _text(_required(value, "status", "ACCOUNT_INFO"), "ACCOUNT_STATUS")
    if status not in {"ACTIVE", "INACTIVE", "DISABLED", "SUSPENDED"}:
        raise GateFailure("ACCOUNT_STATUS_INVALID", "SCHEMA")
    bridge = value["bridgeStarknetAddress"]
    if bridge is not None:
        _text(bridge, "BRIDGE_ADDRESS")
    result: dict[str, Any] = {
        "account_id": account_id,
        "l2_key": l2_key,
        "l2_vault": l2_vault,
        "status": status,
    }
    if account_index is not None:
        result["account_index"] = account_index
    return result


def _decode_envelope(body: Any, endpoint: str) -> Any:
    if not isinstance(body, Mapping) or type(body.get("status")) is not str:
        raise GateFailure(f"RESPONSE_ENVELOPE_INVALID_{endpoint}", "SCHEMA")
    if body["status"] != "OK":
        if body["status"] == "ERROR":
            raise GateFailure(f"AUTHENTICATION_REJECTED_{endpoint}", "AUTH")
        raise GateFailure(f"RESPONSE_STATUS_INVALID_{endpoint}", "SCHEMA")
    if "data" not in body:
        raise GateFailure(f"RESPONSE_DATA_MISSING_{endpoint}", "SCHEMA")
    if "error" in body and body["error"] not in (None, ""):
        raise GateFailure(f"RESPONSE_ERROR_PRESENT_{endpoint}", "SCHEMA")
    return body["data"]


def _decode_list_envelope(body: Any, endpoint: str) -> tuple[list[Any], Mapping[str, Any] | None]:
    data = _decode_envelope(body, endpoint)
    if not isinstance(data, list):
        raise GateFailure(f"LIST_DATA_INVALID_{endpoint}", "SCHEMA")
    pagination = body.get("pagination") if isinstance(body, Mapping) else None
    if pagination is not None and not isinstance(pagination, Mapping):
        raise GateFailure(f"PAGINATION_INVALID_{endpoint}", "SCHEMA")
    return data, pagination


def _canonical_headers(headers: Mapping[str, str]) -> tuple[str, ...]:
    return tuple(sorted(str(key) for key in headers))


@dataclass(frozen=True)
class RestRequest:
    path: str
    query: tuple[tuple[str, str], ...]
    headers: Mapping[str, str]
    attempt: int

    @property
    def method(self) -> str:
        return HTTP_METHOD

    @property
    def url(self) -> str:
        suffix = "" if not self.query else "?" + urlencode(self.query)
        return MAINNET_REST_BASE_URL + self.path + suffix

    def metadata(self) -> dict[str, Any]:
        return {
            "method": HTTP_METHOD,
            "path": self.path,
            "query_keys": [key for key, _ in self.query],
            "header_names": list(_canonical_headers(self.headers)),
            "attempt": self.attempt,
        }


@dataclass(frozen=True)
class StreamRequest:
    headers: Mapping[str, str]

    @property
    def method(self) -> str:
        return HTTP_METHOD

    @property
    def url(self) -> str:
        return MAINNET_STREAM_URL

    def metadata(self) -> dict[str, Any]:
        return {
            "method": HTTP_METHOD,
            "path": "/stream.extended.exchange/v1/account",
            "header_names": list(_canonical_headers(self.headers)),
        }


@dataclass(frozen=True)
class RestReply:
    status: int
    final_url: str
    body: Any
    complete: bool = True
    body_bytes: int | None = None


@dataclass(frozen=True)
class StreamSnapshot:
    components: frozenset[str]
    balance: Mapping[str, Any] | None
    orders: tuple[Mapping[str, Any], ...]
    trades: tuple[Mapping[str, Any], ...]
    positions: tuple[Mapping[str, Any], ...]
    spot_balances: tuple[Mapping[str, Any], ...]
    frames: int
    last_sequence: int | None


@dataclass(frozen=True)
class PageEvidence:
    pages: int
    cursors: tuple[int, ...]
    observed_at_ms: tuple[int, ...]

    def to_metadata(self) -> dict[str, Any]:
        if not self.observed_at_ms or self.observed_at_ms != tuple(sorted(self.observed_at_ms)):
            raise ValueError("page observations must be monotonic")
        return {
            "pages": self.pages,
            "cursors": list(self.cursors),
            "observed_at_ms": list(self.observed_at_ms),
            "freshness": "MONOTONIC_LOCAL_OBSERVATIONS",
        }


@dataclass(frozen=True)
class ReadResult:
    status: str
    reason: str
    failure_class: str | None
    phase: str
    invocation_id: str
    config_hash: str
    counters: Mapping[str, int]
    identity: Mapping[str, Any] | None
    credential_fingerprint: str | None
    summary: Mapping[str, Any]
    rest_calls: int
    stream_frames: int

    @property
    def ready(self) -> bool:
        return self.status == STATUS_READY

    @property
    def identity_verified(self) -> bool:
        return bool(self.summary.get("identity_verified", False))

    def to_metadata(self) -> dict[str, Any]:
        return {
            "config_hash": self.config_hash,
            "credential_fingerprint": self.credential_fingerprint,
            "counters": dict(self.counters),
            "failure_class": self.failure_class,
            "identity": None if self.identity is None else dict(self.identity),
            "invocation_id": self.invocation_id,
            "mainnet_write_authority": NO_MAINNET_WRITE_AUTHORITY,
            "phase": self.phase,
            "reason": self.reason,
            "rest_calls": self.rest_calls,
            "status": self.status,
            "stream_frames": self.stream_frames,
            "summary": dict(self.summary),
            "write_ready": False,
        }

    def evidence(self) -> str:
        return _json(self.to_metadata())


def _empty_summary() -> dict[str, Any]:
    return {
        "identity_verified": False,
        "account_info": None,
        "account_list": {"count": 0, "other_account_indices": []},
        "balance": None,
        "spot_balances": {"count": 0, "noncollateral_nonzero_assets": []},
        "asset_operations": {"count": 0, "pending_count": 0, "pages": 0},
        "fees": {"count": 0, "markets": [], "rates": {}},
        "open_orders": {
            "count": 0,
            "regular_count": 0,
            "trigger_count": 0,
            "markets": [],
            "ids_digest": _digest([]),
        },
        "order_history": {"count": 0, "pages": 0, "markets": [], "ids_digest": _digest([])},
        "trades": {"count": 0, "pages": 0, "markets": [], "ids_digest": _digest([])},
        "positions": {"count": 0, "markets": [], "ids_digest": _digest([])},
        "position_history": {
            "count": 0,
            "pages": 0,
            "markets": [],
            "open_records": 0,
            "ids_digest": _digest([]),
        },
        "funding": {
            "status": "NOT_OBSERVED",
            "count": 0,
            "pages": 0,
            "markets": [],
            "cash_total": None,
            "records_digest": _digest([]),
        },
        "private_stream": {
            "status": "NOT_OBSERVED",
            "components": [],
            "frames": 0,
            "last_sequence": None,
            "rest_agreement": False,
        },
        "pagination": {},
        "flatness": {"exact": False, "zero_fields": {}},
        "unrelated_state": {"status": "UNKNOWN", "categories": []},
        "historical_activity": [],
        "transport": {
            "method": HTTP_METHOD,
            "rest_base": MAINNET_REST_BASE_URL,
            "stream_url": MAINNET_STREAM_URL,
            "write_methods_seen": [],
            "application_frames_sent": False,
        },
        "journal_path": REDACTED_RUN_STORE_PATH,
    }


def _validate_invocation_id(value: Any) -> str:
    if (
        type(value) is not str
        or not value
        or len(value) > 96
        or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for char in value)
    ):
        raise StoreFailure("INVOCATION_ID_INVALID", "SAFETY")
    return value


def _config_hash(invocation_id: str) -> str:
    value = {
        "schema_version": SCHEMA_VERSION,
        "invocation_id": invocation_id,
        "environment": ENVIRONMENT,
        "rest_base": MAINNET_REST_BASE_URL,
        "stream_url": MAINNET_STREAM_URL,
        "paths": [
            ACCOUNT_INFO_PATH,
            ACCOUNTS_PATH,
            BALANCE_PATH,
            SPOT_BALANCES_PATH,
            ASSET_OPERATIONS_PATH,
            FEES_PATH,
            OPEN_ORDERS_PATH,
            ORDER_HISTORY_PATH,
            TRADES_PATH,
            POSITIONS_PATH,
            POSITION_HISTORY_PATH,
            FUNDING_HISTORY_PATH,
        ],
        "timeout_seconds": REQUEST_TIMEOUT_SECONDS,
        "stream_timeout_seconds": STREAM_READ_TIMEOUT_SECONDS,
        "fee_market": FEE_MARKET,
        "expected_identity": EXPECTED_IDENTITY.to_metadata(),
        "pagination": {"max_pages": MAX_PAGES, "max_items": MAX_PAGE_ITEMS},
    }
    return _digest(value)


def _empty_counters() -> dict[str, int]:
    return {}


def _decode_counters(value: Any) -> dict[str, int]:
    if type(value) is str:
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise StoreFailure("DURABLE_COUNTERS_INVALID", "SAFETY") from exc
    if type(value) is not dict:
        raise StoreFailure("DURABLE_COUNTERS_INVALID", "SAFETY")
    for key, item in value.items():
        if (
            type(key) is not str
            or not key
            or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for char in key)
            or type(item) is not int
            or item < 0
            or item > 2
        ):
            raise StoreFailure("DURABLE_COUNTERS_INVALID", "SAFETY")
        if key.endswith("_completions") and value.get(key[:-12] + "_attempts", 0) < item:
            raise StoreFailure("DURABLE_COUNTERS_INVALID", "SAFETY")
    return dict(value)


def _validate_run_path(path: Path, *, may_create: bool) -> None:
    if not path.is_absolute():
        raise StoreFailure("STORE_PATH_INVALID", "SAFETY")
    try:
        parent = path.parent.stat()
    except OSError as exc:
        raise StoreFailure("STORE_PARENT_INVALID", "SAFETY") from exc
    if not stat.S_ISDIR(parent.st_mode) or path.parent.is_symlink() or parent.st_uid != os.getuid():
        raise StoreFailure("STORE_PARENT_INVALID", "SAFETY")
    try:
        item = path.lstat()
    except FileNotFoundError:
        if may_create:
            return
        raise StoreFailure("STORE_MISSING", "SAFETY") from None
    except OSError as exc:
        raise StoreFailure("STORE_UNAVAILABLE", "TRANSPORT") from exc
    if (
        path.is_symlink()
        or not stat.S_ISREG(item.st_mode)
        or item.st_uid != os.getuid()
        or item.st_nlink != 1
        or stat.S_IMODE(item.st_mode) != RUN_STORE_MODE
    ):
        raise StoreFailure("STORE_INVALID", "SAFETY")


def _ensure_run_directory(path: Path) -> None:
    if not path.is_absolute():
        raise StoreFailure("RUN_DIRECTORY_INVALID", "SAFETY")
    try:
        path.mkdir(parents=True, mode=RUN_DIRECTORY_MODE, exist_ok=True)
    except OSError as exc:
        raise StoreFailure("RUN_DIRECTORY_UNAVAILABLE", "SAFETY") from exc
    try:
        info = path.lstat()
    except OSError as exc:
        raise StoreFailure("RUN_DIRECTORY_UNAVAILABLE", "SAFETY") from exc
    if (
        not stat.S_ISDIR(info.st_mode)
        or path.is_symlink()
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != RUN_DIRECTORY_MODE
    ):
        raise StoreFailure("RUN_DIRECTORY_INVALID", "SAFETY")


class RunStore:
    """A protected append-by-invocation journal that never resumes reads."""

    _TABLE = "extended_mainnet_private_read_runs"
    _COLUMNS = (
        "invocation_id",
        "schema_version",
        "config_hash",
        "state",
        "phase",
        "counters",
        "evidence",
    )

    def __init__(self, path: Path | str, invocation_id: str):
        self.path = Path(path)
        self.invocation_id = _validate_invocation_id(invocation_id)
        self.config_hash = _config_hash(self.invocation_id)
        _validate_run_path(self.path, may_create=True)
        if not self.path.exists():
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            try:
                descriptor = os.open(self.path, flags, RUN_STORE_MODE)
            except OSError as exc:
                raise StoreFailure("STORE_CREATE_FAILED", "SAFETY") from exc
            try:
                os.fchmod(descriptor, RUN_STORE_MODE)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        _validate_run_path(self.path, may_create=False)
        try:
            with self._connect() as connection:
                connection.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self._TABLE} (
                        invocation_id TEXT PRIMARY KEY,
                        schema_version INTEGER NOT NULL,
                        config_hash TEXT NOT NULL,
                        state TEXT NOT NULL,
                        phase TEXT NOT NULL,
                        counters TEXT NOT NULL,
                        evidence TEXT
                    )
                    """
                )
                columns = tuple(
                    row[1]
                    for row in connection.execute(
                        f"PRAGMA table_info({self._TABLE})"
                    )
                )
                if columns != self._COLUMNS:
                    raise StoreFailure("DURABLE_SCHEMA_INVALID", "SAFETY")
                catalog = tuple(
                    connection.execute(
                        """
                        SELECT type,name,tbl_name FROM sqlite_master
                        WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name
                        """
                    )
                )
                if catalog != (("table", self._TABLE, self._TABLE),):
                    raise StoreFailure("DURABLE_SCHEMA_INVALID", "SAFETY")
        except StoreFailure:
            raise
        except sqlite3.DatabaseError as exc:
            raise StoreFailure("DURABLE_STORE_INVALID", "SAFETY") from exc

    def _connect(self) -> sqlite3.Connection:
        try:
            connection = sqlite3.connect(self.path)
            connection.execute("PRAGMA journal_mode=DELETE")
            connection.execute("PRAGMA synchronous=FULL")
            return connection
        except sqlite3.DatabaseError as exc:
            raise StoreFailure("DURABLE_STORE_INVALID", "SAFETY") from exc

    def _row(self) -> tuple[Any, ...] | None:
        with self._connect() as connection:
            return connection.execute(
                f"SELECT schema_version,config_hash,state,phase,counters,evidence "
                f"FROM {self._TABLE} WHERE invocation_id=?",
                (self.invocation_id,),
            ).fetchone()

    def _write_counters(self, counters: Mapping[str, int], phase: str) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                f"SELECT state,counters FROM {self._TABLE} WHERE invocation_id=?",
                (self.invocation_id,),
            ).fetchone()
            if row is None or row[0] != "RUNNING":
                raise StoreFailure("DURABLE_STATE_CONFLICT", "SAFETY")
            existing = _decode_counters(row[1])
            all_keys = set(existing) | set(counters)
            deltas = [counters.get(key, 0) - existing.get(key, 0) for key in all_keys]
            if any(delta not in {0, 1} for delta in deltas) or sum(deltas) != 1:
                raise StoreFailure("DURABLE_COUNTER_CONFLICT", "SAFETY")
            connection.execute(
                f"UPDATE {self._TABLE} SET phase=?,counters=? WHERE invocation_id=?",
                (phase, _json(dict(counters)), self.invocation_id),
            )

    def claim(self) -> ReadResult | None:
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    f"SELECT schema_version,config_hash,state,phase,counters,evidence "
                    f"FROM {self._TABLE} WHERE invocation_id=?",
                    (self.invocation_id,),
                ).fetchone()
                if row is None:
                    connection.execute(
                        f"INSERT INTO {self._TABLE} VALUES (?,?,?,?,?,?,NULL)",
                        (
                            self.invocation_id,
                            SCHEMA_VERSION,
                            self.config_hash,
                            "RUNNING",
                            "STARTED",
                            _json(_empty_counters()),
                        ),
                    )
                    return None
        except sqlite3.DatabaseError as exc:
            raise StoreFailure("DURABLE_STORE_INVALID", "SAFETY") from exc

        schema, config_hash, state, phase, raw_counters, evidence = row
        if schema != SCHEMA_VERSION or config_hash != self.config_hash:
            raise StoreFailure("DURABLE_IDENTITY_MISMATCH", "SAFETY")
        counters = _decode_counters(raw_counters)
        if state == "TERMINAL" and type(evidence) is str:
            result = _decode_result(evidence, self.invocation_id, self.config_hash)
            if dict(result.counters) != counters or result.phase != "TERMINAL":
                raise StoreFailure("DURABLE_EVIDENCE_INVALID", "SAFETY")
            return result
        if state == "RUNNING" and evidence is None:
            interrupted = ReadResult(
                STATUS_BLOCKED,
                "INTERRUPTED_RUNNING",
                "SAFETY",
                "TERMINAL",
                self.invocation_id,
                self.config_hash,
                counters,
                None,
                None,
                _empty_summary(),
                0,
                0,
            )
            return self._terminalize(interrupted)
        raise StoreFailure("DURABLE_STATE_INVALID", "SAFETY")

    def attempt(self, effect: str, *, retry: bool = False) -> None:
        if (
            type(effect) is not str
            or not effect
            or len(effect) > 160
            or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for char in effect)
        ):
            raise StoreFailure("DURABLE_EFFECT_INVALID", "SAFETY")
        row = self._row()
        if row is None or row[2] != "RUNNING":
            raise StoreFailure("DURABLE_STATE_CONFLICT", "SAFETY")
        counters = _decode_counters(row[4])
        attempts_key = effect + "_attempts"
        completions_key = effect + "_completions"
        attempts = counters.get(attempts_key, 0)
        completions = counters.get(completions_key, 0)
        if retry:
            if attempts != 1 or completions != 0:
                raise StoreFailure("DURABLE_RETRY_FORBIDDEN", "SAFETY")
        elif attempts != 0:
            raise StoreFailure("DURABLE_EFFECT_REPLAY", "SAFETY")
        counters[attempts_key] = attempts + 1
        self._write_counters(counters, effect.upper())

    def complete(self, effect: str) -> None:
        if type(effect) is not str or not effect:
            raise StoreFailure("DURABLE_EFFECT_INVALID", "SAFETY")
        row = self._row()
        if row is None or row[2] != "RUNNING":
            raise StoreFailure("DURABLE_STATE_CONFLICT", "SAFETY")
        counters = _decode_counters(row[4])
        attempts_key = effect + "_attempts"
        completions_key = effect + "_completions"
        if counters.get(attempts_key, 0) not in {1, 2} or counters.get(completions_key, 0) != 0:
            raise StoreFailure("DURABLE_COMPLETION_INVALID", "SAFETY")
        counters[completions_key] = 1
        self._write_counters(counters, effect.upper())

    def counters(self) -> dict[str, int]:
        row = self._row()
        if row is None:
            raise StoreFailure("DURABLE_STATE_INVALID", "SAFETY")
        return _decode_counters(row[4])

    def _terminalize(self, result: ReadResult) -> ReadResult:
        counters = self.counters()
        if counters.get("terminal_persistence_attempts", 0) != 0:
            raise StoreFailure("DURABLE_TERMINAL_REPLAY", "SAFETY")
        counters["terminal_persistence_attempts"] = 1
        counters["terminal_persistence_completions"] = 1
        terminal = replace(result, phase="TERMINAL", counters=counters)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                f"SELECT state FROM {self._TABLE} WHERE invocation_id=?",
                (self.invocation_id,),
            ).fetchone()
            if row is None or row[0] != "RUNNING":
                raise StoreFailure("DURABLE_STATE_CONFLICT", "SAFETY")
            connection.execute(
                f"UPDATE {self._TABLE} SET state='TERMINAL',phase='TERMINAL',counters=?,evidence=? "
                "WHERE invocation_id=? AND state='RUNNING'",
                (_json(counters), terminal.evidence(), self.invocation_id),
            )
        try:
            descriptor = os.open(self.path, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        except OSError as exc:
            raise StoreFailure("DURABLE_STORE_SYNC_FAILED", "SAFETY") from exc
        return terminal

    def terminal(self, result: ReadResult) -> ReadResult:
        if result.invocation_id != self.invocation_id or result.config_hash != self.config_hash:
            raise StoreFailure("DURABLE_IDENTITY_MISMATCH", "SAFETY")
        return self._terminalize(result)


def _decode_result(raw: str, invocation_id: str, config_hash: str) -> ReadResult:
    try:
        value = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise StoreFailure("DURABLE_EVIDENCE_INVALID", "SAFETY") from exc
    required = {
        "config_hash",
        "credential_fingerprint",
        "counters",
        "failure_class",
        "identity",
        "invocation_id",
        "mainnet_write_authority",
        "phase",
        "reason",
        "rest_calls",
        "status",
        "stream_frames",
        "summary",
        "write_ready",
    }
    if type(value) is not dict or set(value) != required:
        raise StoreFailure("DURABLE_EVIDENCE_INVALID", "SAFETY")
    if (
        value["invocation_id"] != invocation_id
        or value["config_hash"] != config_hash
        or value["phase"] != "TERMINAL"
        or value["status"] not in {STATUS_READY, STATUS_BLOCKED}
        or value["mainnet_write_authority"] != NO_MAINNET_WRITE_AUTHORITY
        or value["write_ready"] is not False
        or type(value["summary"]) is not dict
        or type(value["rest_calls"]) is not int
        or value["rest_calls"] < 0
        or type(value["stream_frames"]) is not int
        or value["stream_frames"] < 0
        or value["failure_class"] is not None
        and value["failure_class"] not in FAILURE_CLASSES
    ):
        raise StoreFailure("DURABLE_EVIDENCE_INVALID", "SAFETY")
    counters = _decode_counters(value["counters"])
    identity = value["identity"]
    if identity is not None and (
        type(identity) is not dict
        or set(identity) != {"account_id", "account_index", "l2_key", "l2_vault"}
    ):
        raise StoreFailure("DURABLE_EVIDENCE_INVALID", "SAFETY")
    fingerprint = value["credential_fingerprint"]
    if fingerprint is not None and (
        type(fingerprint) is not str
        or len(fingerprint) != 64
        or any(char not in "0123456789abcdef" for char in fingerprint)
    ):
        raise StoreFailure("DURABLE_EVIDENCE_INVALID", "SAFETY")
    return ReadResult(
        value["status"],
        value["reason"],
        value["failure_class"],
        "TERMINAL",
        invocation_id,
        config_hash,
        counters,
        identity,
        fingerprint,
        value["summary"],
        value["rest_calls"],
        value["stream_frames"],
    )


def _is_transport_failure(error: BaseException) -> bool:
    return isinstance(
        error,
        (
            TransportInterruption,
            aiohttp.ClientConnectionError,
            aiohttp.ClientPayloadError,
            asyncio.TimeoutError,
            asyncio.IncompleteReadError,
            ConnectionError,
            EOFError,
            OSError,
        ),
    )


def _coerce_reply(value: Any) -> RestReply:
    if isinstance(value, RestReply):
        reply = value
    elif isinstance(value, Mapping):
        required = {"status", "final_url", "body"}
        if not required.issubset(value):
            raise GateFailure("TRANSPORT_REPLY_SCHEMA_INVALID", "SCHEMA")
        reply = RestReply(
            value["status"],
            value["final_url"],
            value["body"],
            value.get("complete", True),
            value.get("body_bytes"),
        )
    else:
        raise GateFailure("TRANSPORT_REPLY_SCHEMA_INVALID", "SCHEMA")
    if (
        type(reply.status) is not int
        or type(reply.final_url) is not str
        or type(reply.complete) is not bool
        or reply.body_bytes is not None
        and (type(reply.body_bytes) is not int or reply.body_bytes < 0)
    ):
        raise GateFailure("TRANSPORT_REPLY_SCHEMA_INVALID", "SCHEMA")
    if not reply.complete:
        raise TransportInterruption()
    if reply.body_bytes is not None and reply.body_bytes > MAX_RESPONSE_BYTES:
        raise GateFailure("RESPONSE_TOO_LARGE", "SCHEMA")
    try:
        if reply.body_bytes is None:
            encoded = _json(reply.body).encode("utf-8")
        else:
            encoded = b""
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise GateFailure("RESPONSE_JSON_INVALID", "SCHEMA") from exc
    if len(encoded) > MAX_RESPONSE_BYTES:
        raise GateFailure("RESPONSE_TOO_LARGE", "SCHEMA")
    return reply


def _validate_transport_metadata(value: Any, expected_url: str) -> None:
    if not isinstance(value, Mapping):
        raise GateFailure("STREAM_TRANSPORT_UNVERIFIABLE", "SAFETY")
    required = {
        "actual_url",
        "method",
        "header_names",
        "direct_tls",
        "trust_env",
        "proxy",
        "redirects",
        "retries",
        "application_frames_sent",
    }
    if not required.issubset(value):
        raise GateFailure("STREAM_TRANSPORT_UNVERIFIABLE", "SAFETY")
    if (
        value["actual_url"] != expected_url
        or value["method"] != HTTP_METHOD
        or sorted(value["header_names"]) != sorted([API_KEY_HEADER, "User-Agent"])
        or value["direct_tls"] is not True
        or value["trust_env"] is not False
        or value["proxy"] is not None
        or value["redirects"] != 0
        or value["retries"] != 0
        or value["application_frames_sent"] is not False
    ):
        raise GateFailure("STREAM_TRANSPORT_UNVERIFIABLE", "SAFETY")


async def _call(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def _get_decoded(
    *,
    store: RunStore,
    transport: Any,
    key: str,
    path: str,
    query: tuple[tuple[str, str], ...],
    api_key: str,
    endpoint: str,
    decoder: Callable[[Any], Any],
    clock_ms: Callable[[], int],
    not_found: Callable[[int], Any] | None = None,
) -> tuple[Any, int]:
    for attempt in (1, 2):
        store.attempt(key, retry=attempt == 2)
        request = RestRequest(
            path=path,
            query=query,
            headers={"User-Agent": USER_AGENT, API_KEY_HEADER: api_key},
            attempt=attempt,
        )
        try:
            value = await _call(transport.get(request))
            reply = _coerce_reply(value)
            if reply.final_url != request.url:
                raise GateFailure("REDIRECT_FORBIDDEN", "SAFETY")
            if reply.status == 404 and not_found is not None:
                now = clock_ms()
                if type(now) is not int or now < 0:
                    raise GateFailure("CLOCK_INVALID", "SAFETY")
                data = not_found(now)
                store.complete(key)
                return data, now
            if reply.status in {401, 403}:
                raise GateFailure("AUTHENTICATION_REJECTED", "AUTH")
            if reply.status != 200:
                raise GateFailure("HTTP_STATUS_UNACCEPTED", "HTTP")
            data = decoder(reply.body)
            now = clock_ms()
            if type(now) is not int or now < 0:
                raise GateFailure("CLOCK_INVALID", "SAFETY")
            store.complete(key)
            return data, now
        except asyncio.CancelledError:
            raise
        except GateFailure:
            raise
        except BaseException as exc:
            if _is_transport_failure(exc):
                if attempt == 1:
                    continue
                raise GateFailure("TRANSPORT_RETRY_EXHAUSTED", "TRANSPORT") from None
            raise GateFailure("UNCLASSIFIED_FAILURE", "SAFETY") from None
    raise GateFailure("TRANSPORT_RETRY_EXHAUSTED", "TRANSPORT")


async def _read_pages(
    *,
    store: RunStore,
    transport: Any,
    api_key: str,
    name: str,
    path: str,
    limit: int,
    decoder: Callable[[Any], tuple[list[Any], Mapping[str, Any] | None]],
    item_decoder: Callable[[Any], Mapping[str, Any]],
    clock_ms: Callable[[], int],
    base_query: tuple[tuple[str, str], ...] = (),
) -> tuple[tuple[Mapping[str, Any], ...], PageEvidence]:
    if limit <= 0:
        raise GateFailure("PAGINATION_LIMIT_INVALID", "SAFETY")
    rows: list[Mapping[str, Any]] = []
    cursors: list[int] = []
    seen: set[int] = set()
    cursor: int | None = None
    observed_times: list[int] = []
    for page in range(1, MAX_PAGES + 1):
        query = list(base_query)
        query.append(("limit", str(limit)))
        if cursor is not None:
            query.append(("cursor", str(cursor)))
        data, observed = await _get_decoded(
            store=store,
            transport=transport,
            key=f"rest_{name}_page_{page}",
            path=path,
            query=tuple(query),
            api_key=api_key,
            endpoint=name.upper(),
            decoder=lambda body, endpoint=name.upper(): decoder(body),
            clock_ms=clock_ms,
        )
        raw_rows, pagination = data
        observed_times.append(observed)
        if len(raw_rows) > limit:
            raise GateFailure(f"PAGE_TOO_LARGE_{name.upper()}", "SCHEMA")
        if not raw_rows:
            if pagination is not None:
                count = _integer(_required(pagination, "count", "PAGINATION"), "PAGINATION_COUNT", nonnegative=True)
                if count != 0 or pagination.get("cursor") is not None:
                    raise GateFailure(f"EMPTY_PAGE_PAGINATION_{name.upper()}", "SCHEMA")
            break
        decoded = tuple(item_decoder(item) for item in raw_rows)
        rows.extend(decoded)
        if pagination is None:
            raise GateFailure(f"PAGINATION_MISSING_{name.upper()}", "SCHEMA")
        count = _integer(_required(pagination, "count", "PAGINATION"), "PAGINATION_COUNT", nonnegative=True)
        if count != len(raw_rows):
            raise GateFailure(f"PAGINATION_COUNT_DISAGREES_{name.upper()}", "SCHEMA")
        next_cursor = pagination.get("cursor")
        if len(raw_rows) < limit:
            break
        if next_cursor is None:
            raise GateFailure(f"PAGINATION_CURSOR_MISSING_{name.upper()}", "SCHEMA")
        next_cursor = _integer(next_cursor, "PAGINATION_CURSOR", positive=True)
        if next_cursor in seen:
            raise GateFailure(f"PAGINATION_CURSOR_REPEATED_{name.upper()}", "SAFETY")
        seen.add(next_cursor)
        cursors.append(next_cursor)
        cursor = next_cursor
    else:
        raise GateFailure(f"PAGINATION_LIMIT_{name.upper()}", "SAFETY")
    if observed_times != sorted(observed_times):
        raise GateFailure(f"OBSERVATION_TIME_REGRESSION_{name.upper()}", "SAFETY")
    return (
        tuple(rows),
        PageEvidence(len(observed_times), tuple(cursors), tuple(observed_times)),
    )


def _decode_account_info(body: Any) -> dict[str, Any]:
    data = _decode_envelope(body, "ACCOUNT_INFO")
    if not isinstance(data, Mapping):
        raise GateFailure("ACCOUNT_INFO_DATA_INVALID", "SCHEMA")
    for key in ("status", "l2Key", "l2Vault", "accountId", "bridgeStarknetAddress"):
        _required(data, key, "ACCOUNT_INFO")
    return _canonical_identity_from_info(data)


def _decode_accounts(body: Any) -> tuple[dict[str, Any], ...]:
    data = _decode_envelope(body, "ACCOUNTS")
    if not isinstance(data, list) or not data or len(data) > MAX_ACCOUNT_ROWS:
        raise GateFailure("ACCOUNTS_DATA_INVALID", "SCHEMA")
    rows: list[dict[str, Any]] = []
    ids: set[int] = set()
    indices: set[int] = set()
    for item in data:
        if not isinstance(item, Mapping):
            raise GateFailure("ACCOUNT_ROW_INVALID", "SCHEMA")
        for key in (
            "accountId",
            "accountIndex",
            "status",
            "l2Key",
            "l2Vault",
            "bridgeStarknetAddress",
            "accountIndexForKeyGeneration",
        ):
            _required(item, key, "ACCOUNT_ROW")
        account_id = _account_id(item["accountId"])
        account_index = _integer(item["accountIndex"], "ACCOUNT_INDEX", nonnegative=True)
        key_index = _integer(
            item["accountIndexForKeyGeneration"],
            "ACCOUNT_KEY_INDEX",
            nonnegative=True,
        )
        if account_id in ids or account_index in indices:
            raise GateFailure("ACCOUNT_LIST_DUPLICATE", "IDENTITY")
        status = _text(item["status"], "ACCOUNT_STATUS")
        if status not in {"ACTIVE", "INACTIVE", "DISABLED", "SUSPENDED"}:
            raise GateFailure("ACCOUNT_STATUS_INVALID", "SCHEMA")
        bridge = item["bridgeStarknetAddress"]
        if bridge is not None:
            _text(bridge, "BRIDGE_ADDRESS")
        ids.add(account_id)
        indices.add(account_index)
        rows.append(
            {
                "account_id": account_id,
                "account_index": account_index,
                "account_index_for_key_generation": key_index,
                "l2_key": _canonical_l2_key(item["l2Key"]),
                "l2_vault": _account_id(item["l2Vault"], "L2_VAULT"),
                "status": status,
            }
        )
    return tuple(rows)


_BALANCE_DECIMAL_FIELDS = (
    "balance",
    "equity",
    "availableForTrade",
    "availableForWithdrawal",
    "unrealisedPnl",
    "withdrawableUnrealisedPnl",
    "initialMargin",
    "marginRatio",
    "exposure",
    "leverage",
    "spotEquity",
    "spotEquityForAvailableForTrade",
    "collateralReservedForSpotOrders",
)


def _decode_balance_data(data: Any, *, stream: bool = False) -> dict[str, Any]:
    if not isinstance(data, Mapping):
        raise GateFailure("BALANCE_DATA_INVALID", "SCHEMA")
    required = {"collateralName", *_BALANCE_DECIMAL_FIELDS, "updatedTime"}
    if not required.issubset(data):
        raise GateFailure("BALANCE_FIELDS_MISSING", "SCHEMA")
    if data["collateralName"] != BALANCE_COLLATERAL_NAME:
        raise GateFailure("COLLATERAL_ASSET_UNEXPECTED", "SAFETY")
    result: dict[str, Any] = {"collateral_name": BALANCE_COLLATERAL_NAME}
    for field in _BALANCE_DECIMAL_FIELDS:
        value = _decimal(data[field], field)
        if field in {
            "initialMargin",
            "marginRatio",
            "exposure",
            "leverage",
            "spotEquity",
            "spotEquityForAvailableForTrade",
            "collateralReservedForSpotOrders",
        } and value < 0:
            raise GateFailure(f"BALANCE_NEGATIVE_{field.upper()}", "SAFETY")
        result[field] = _number_text(value)
    result["updated_time"] = _integer(data["updatedTime"], "BALANCE_UPDATED_TIME", positive=True)
    if "accountId" in data and data["accountId"] is not None:
        if _account_id(data["accountId"]) != EXPECTED_ACCOUNT_ID:
            raise GateFailure("BALANCE_ACCOUNT_MISMATCH", "IDENTITY")
    return result


def _decode_balance(body: Any) -> dict[str, Any]:
    return _decode_balance_data(_decode_envelope(body, "BALANCE"))


def _zero_balance(observed_at_ms: int) -> dict[str, Any]:
    if type(observed_at_ms) is not int or observed_at_ms <= 0:
        raise GateFailure("CLOCK_INVALID", "SAFETY")
    data = {
        "accountId": EXPECTED_ACCOUNT_ID,
        "collateralName": BALANCE_COLLATERAL_NAME,
        **{field: "0" for field in _BALANCE_DECIMAL_FIELDS},
        "updatedTime": observed_at_ms,
    }
    result = _decode_balance_data(data)
    result["balance_source"] = "OFFICIAL_404_ZERO_BALANCE"
    return result


def _zero_spot_balances(_observed_at_ms: int) -> tuple[dict[str, Any], ...]:
    return ()


def _noncollateral_nonzero_spot_assets(
    rows: Sequence[Mapping[str, Any]],
) -> list[str]:
    return sorted(
        row["asset"]
        for row in rows
        if row["asset"] not in _COLLATERAL_SPOT_ASSETS
        and Decimal(row["balance"]) != 0
    )


def _decode_spot_rows(body: Any) -> tuple[dict[str, Any], ...]:
    data = _decode_envelope(body, "SPOT_BALANCES")
    if not isinstance(data, list):
        raise GateFailure("SPOT_BALANCES_DATA_INVALID", "SCHEMA")
    rows: list[dict[str, Any]] = []
    seen_assets: set[str] = set()
    for item in data:
        if not isinstance(item, Mapping):
            raise GateFailure("SPOT_BALANCE_ROW_INVALID", "SCHEMA")
        for key in (
            "accountId",
            "asset",
            "balance",
            "indexPrice",
            "notionalValue",
            "contributionFactor",
            "equityContribution",
            "updatedAt",
        ):
            _required(item, key, "SPOT_BALANCE")
        account_id = _account_id(item["accountId"])
        if account_id != EXPECTED_ACCOUNT_ID:
            raise GateFailure("SPOT_BALANCE_ACCOUNT_MISMATCH", "IDENTITY")
        asset = _text(item["asset"], "SPOT_ASSET")
        if asset in seen_assets:
            raise GateFailure("SPOT_BALANCE_DUPLICATE_ASSET", "SAFETY")
        seen_assets.add(asset)
        values: dict[str, Any] = {
            "account_id": account_id,
            "asset": asset,
            "balance": _number_text(_decimal(item["balance"], "SPOT_BALANCE_VALUE")),
            "index_price": _number_text(_decimal(item["indexPrice"], "SPOT_INDEX_PRICE", positive=True)),
            "notional_value": _number_text(_decimal(item["notionalValue"], "SPOT_NOTIONAL_VALUE")),
            "contribution_factor": _number_text(_decimal(item["contributionFactor"], "SPOT_CONTRIBUTION_FACTOR", nonnegative=True)),
            "equity_contribution": _number_text(_decimal(item["equityContribution"], "SPOT_EQUITY_CONTRIBUTION")),
            "updated_at": _integer(item["updatedAt"], "SPOT_UPDATED_AT", positive=True),
        }
        for source, target in (
            ("availableToWithdraw", "available_to_withdraw"),
            ("absolutePnl", "absolute_pnl"),
            ("pnlPercentage", "pnl_percentage"),
            ("averageEntryPrice", "average_entry_price"),
        ):
            if source in item and item[source] is not None:
                values[target] = _number_text(_decimal(item[source], f"SPOT_{source.upper()}"))
        if asset in _COLLATERAL_SPOT_ASSETS:
            if Decimal(values["index_price"]) != Decimal("1") or Decimal(values["contribution_factor"]) != Decimal("1"):
                raise GateFailure("COLLATERAL_SPOT_ROW_INVALID", "SAFETY")
        rows.append(values)
    return tuple(rows)


def _decode_asset_operation_rows(body: Any) -> tuple[list[Any], Mapping[str, Any] | None]:
    return _decode_list_envelope(body, "ASSET_OPERATIONS")


def _decode_asset_operation(item: Any) -> dict[str, Any]:
    if not isinstance(item, Mapping):
        raise GateFailure("ASSET_OPERATION_ROW_INVALID", "SCHEMA")
    for key in ("id", "type", "status", "amount", "fee", "asset", "time", "accountId"):
        _required(item, key, "ASSET_OPERATION")
    result: dict[str, Any] = {
        "id": _operation_id(item["id"]),
        "type": _text(item["type"], "ASSET_OPERATION_TYPE"),
        "status": _text(item["status"], "ASSET_OPERATION_STATUS"),
        "amount": _number_text(_decimal(item["amount"], "ASSET_OPERATION_AMOUNT")),
        "fee": _number_text(_decimal(item["fee"], "ASSET_OPERATION_FEE", nonnegative=True)),
        "asset": str(item["asset"]),
        "time": _integer(item["time"], "ASSET_OPERATION_TIME", positive=True),
        "account_id": _account_id(item["accountId"]),
    }
    if result["type"] not in _OPERATION_TYPES or result["status"] not in _OPERATION_STATUSES:
        raise GateFailure("ASSET_OPERATION_ENUM_INVALID", "SCHEMA")
    if result["account_id"] != EXPECTED_ACCOUNT_ID:
        raise GateFailure("ASSET_OPERATION_ACCOUNT_MISMATCH", "IDENTITY")
    if "counterpartyAccountId" in item and item["counterpartyAccountId"] is not None:
        result["counterparty_account_id"] = _account_id(item["counterpartyAccountId"])
    for source, target in (("transactionHash", "transaction_hash"), ("chain", "chain")):
        if source in item and item[source] is not None:
            result[target] = _text(item[source], f"ASSET_OPERATION_{source.upper()}")
    return result


def _decode_fees(body: Any) -> tuple[dict[str, Any], ...]:
    data = _decode_envelope(body, "FEES")
    if not isinstance(data, list) or not data:
        raise GateFailure("FEE_DATA_MISSING", "SAFETY")
    rows: list[dict[str, Any]] = []
    markets: set[str] = set()
    for item in data:
        if not isinstance(item, Mapping):
            raise GateFailure("FEE_ROW_INVALID", "SCHEMA")
        for key in ("market", "makerFeeRate", "takerFeeRate"):
            _required(item, key, "FEE")
        market = _text(item["market"], "FEE_MARKET")
        if market in markets:
            raise GateFailure("FEE_MARKET_DUPLICATE", "SAFETY")
        markets.add(market)
        row: dict[str, Any] = {
            "market": market,
            "maker_fee_rate": _number_text(_decimal(item["makerFeeRate"], "MAKER_FEE_RATE", nonnegative=True)),
            "taker_fee_rate": _number_text(_decimal(item["takerFeeRate"], "TAKER_FEE_RATE", nonnegative=True)),
        }
        if "builderFeeRate" in item and item["builderFeeRate"] is not None:
            row["builder_fee_rate"] = _number_text(_decimal(item["builderFeeRate"], "BUILDER_FEE_RATE", nonnegative=True))
        rows.append(row)
    return tuple(rows)


def _decode_order(item: Any, allowed_statuses: frozenset[str], *, stream: bool = False) -> dict[str, Any]:
    if not isinstance(item, Mapping):
        raise GateFailure("ORDER_ROW_INVALID", "SCHEMA")
    for key in ("id", "accountId", "market", "status", "type", "side", "qty"):
        _required(item, key, "ORDER")
    order_id = _integer(item["id"], "ORDER_ID", positive=True)
    account_id = _account_id(item["accountId"])
    if account_id != EXPECTED_ACCOUNT_ID:
        raise GateFailure("ORDER_ACCOUNT_MISMATCH", "IDENTITY")
    status = _text(item["status"], "ORDER_STATUS")
    order_type = _text(item["type"], "ORDER_TYPE")
    side = _text(item["side"], "ORDER_SIDE")
    if status not in allowed_statuses or status not in _ALL_ORDER_STATUSES:
        raise GateFailure("ORDER_STATUS_INVALID", "SCHEMA")
    if order_type not in _ORDER_TYPES or side not in _ORDER_SIDES:
        raise GateFailure("ORDER_ENUM_INVALID", "SCHEMA")
    qty = _decimal(item["qty"], "ORDER_QTY", positive=True)
    filled = _optional_decimal(item, "filledQty", "ORDER_FILLED_QTY", nonnegative=True)
    if filled is not None and filled > qty:
        raise GateFailure("ORDER_FILLED_QTY_EXCEEDS_QTY", "SAFETY")
    result: dict[str, Any] = {
        "id": order_id,
        "account_id": account_id,
        "market": _text(item["market"], "ORDER_MARKET"),
        "status": status,
        "type": order_type,
        "side": side,
        "qty": _number_text(qty),
        "filled_qty": None if filled is None else _number_text(filled),
        "trigger": order_type in {"CONDITIONAL", "TPSL"} or item.get("trigger") is not None,
    }
    for source, target in (("price", "price"), ("averagePrice", "average_price"), ("payedFee", "payed_fee")):
        if source in item and item[source] not in (None, ""):
            result[target] = _number_text(_decimal(item[source], f"ORDER_{source.upper()}"))
    for key in ("reduceOnly", "postOnly"):
        if key in item and item[key] is not None and type(item[key]) is not bool:
            raise GateFailure("ORDER_BOOLEAN_INVALID", "SCHEMA")
    for source, target in (("createdTime", "created_time"), ("updatedTime", "updated_time"), ("expireTime", "expire_time")):
        if source in item and item[source] is not None:
            result[target] = _integer(item[source], f"ORDER_{source.upper()}", positive=True)
    if "externalId" in item and item["externalId"] is not None:
        result["external_id"] = _text(item["externalId"], "ORDER_EXTERNAL_ID")
    if "trigger" in item and item["trigger"] is not None and not isinstance(item["trigger"], Mapping):
        raise GateFailure("ORDER_TRIGGER_INVALID", "SCHEMA")
    return result


def _decode_open_orders(body: Any) -> tuple[list[Any], Mapping[str, Any] | None]:
    return _decode_list_envelope(body, "OPEN_ORDERS")


def _decode_history_orders(body: Any) -> tuple[list[Any], Mapping[str, Any] | None]:
    return _decode_list_envelope(body, "ORDER_HISTORY")


def _decode_trade(item: Any, *, stream: bool = False) -> dict[str, Any]:
    if not isinstance(item, Mapping):
        raise GateFailure("TRADE_ROW_INVALID", "SCHEMA")
    for key in ("id", "accountId", "market", "orderId", "side", "value", "fee", "isTaker", "tradeType"):
        _required(item, key, "TRADE")
    account_id = _account_id(item["accountId"])
    if account_id != EXPECTED_ACCOUNT_ID:
        raise GateFailure("TRADE_ACCOUNT_MISMATCH", "IDENTITY")
    if type(item["isTaker"]) is not bool:
        raise GateFailure("TRADE_TAKER_FLAG_INVALID", "SCHEMA")
    side = _text(item["side"], "TRADE_SIDE")
    trade_type = _text(item["tradeType"], "TRADE_TYPE")
    if side not in _ORDER_SIDES or trade_type not in _TRADE_TYPES:
        raise GateFailure("TRADE_ENUM_INVALID", "SCHEMA")
    price_value = item.get("averagePrice", item.get("price"))
    if price_value is None:
        raise GateFailure("TRADE_PRICE_MISSING", "SCHEMA")
    quantity_value = item.get("filledQty", item.get("qty"))
    if quantity_value is None:
        raise GateFailure("TRADE_QTY_MISSING", "SCHEMA")
    return {
        "id": _integer(item["id"], "TRADE_ID", positive=True),
        "account_id": account_id,
        "market": _text(item["market"], "TRADE_MARKET"),
        "order_id": _operation_id(item["orderId"]),
        "side": side,
        "average_price": _number_text(_decimal(price_value, "TRADE_PRICE", positive=True)),
        "filled_qty": _number_text(_decimal(quantity_value, "TRADE_QTY", positive=True)),
        "value": _number_text(_decimal(item["value"], "TRADE_VALUE", nonnegative=True)),
        "fee": _number_text(_decimal(item["fee"], "TRADE_FEE", nonnegative=True)),
        "is_taker": item["isTaker"],
        "trade_type": trade_type,
        "created_time": _integer(item.get("createdTime", item.get("createdAt")), "TRADE_TIME", positive=True),
    }


def _decode_trades(body: Any) -> tuple[list[Any], Mapping[str, Any] | None]:
    return _decode_list_envelope(body, "TRADES")


def _decode_position(item: Any, *, history: bool = False, stream: bool = False) -> dict[str, Any]:
    if not isinstance(item, Mapping):
        raise GateFailure("POSITION_ROW_INVALID", "SCHEMA")
    for key in ("id", "accountId", "market", "side", "size", "openPrice"):
        _required(item, key, "POSITION")
    account_id = _account_id(item["accountId"])
    if account_id != EXPECTED_ACCOUNT_ID:
        raise GateFailure("POSITION_ACCOUNT_MISMATCH", "IDENTITY")
    side = _text(item["side"], "POSITION_SIDE")
    if side not in _POSITION_SIDES:
        raise GateFailure("POSITION_SIDE_INVALID", "SCHEMA")
    size = _decimal(item["size"], "POSITION_SIZE", positive=True)
    result: dict[str, Any] = {
        "id": _integer(item["id"], "POSITION_ID", positive=True),
        "account_id": account_id,
        "market": _text(item["market"], "POSITION_MARKET"),
        "side": side,
        "size": _number_text(size),
        "open_price": _number_text(_decimal(item["openPrice"], "POSITION_OPEN_PRICE", positive=True)),
    }
    if "value" in item and item["value"] is not None:
        result["value"] = _number_text(_decimal(item["value"], "POSITION_VALUE", positive=True))
    for source, target in (("markPrice", "mark_price"), ("liquidationPrice", "liquidation_price"), ("margin", "margin"), ("unrealisedPnl", "unrealised_pnl"), ("realisedPnl", "realised_pnl"), ("paidFundingFee", "paid_funding_fee"), ("maxPositionSize", "max_position_size"), ("adl", "adl")):
        if source in item and item[source] is not None:
            result[target] = _number_text(_decimal(item[source], f"POSITION_{source.upper()}"))
    for source, target in (("createdTime", "created_time"), ("updatedTime", "updated_time"), ("createdAt", "created_time"), ("updatedAt", "updated_time"), ("closedTime", "closed_time")):
        if source in item and item[source] is not None:
            result[target] = _integer(item[source], f"POSITION_{source.upper()}", positive=True)
    if history and "closed_time" not in result:
        result["open_record"] = True
    else:
        result["open_record"] = False
    return result


def _decode_positions(body: Any) -> list[Any]:
    data = _decode_envelope(body, "POSITIONS")
    if not isinstance(data, list):
        raise GateFailure("POSITIONS_DATA_INVALID", "SCHEMA")
    return data


def _decode_position_history(body: Any) -> tuple[list[Any], Mapping[str, Any] | None]:
    return _decode_list_envelope(body, "POSITION_HISTORY")


def _decode_funding(body: Any) -> tuple[list[Any], Mapping[str, Any] | None]:
    return _decode_list_envelope(body, "FUNDING_HISTORY")


def _decode_funding_row(item: Any) -> dict[str, Any]:
    if not isinstance(item, Mapping):
        raise GateFailure("FUNDING_ROW_INVALID", "SCHEMA")
    for key in ("id", "accountId", "market", "positionId", "side", "value", "markPrice", "fundingFee", "fundingRate", "paidTime"):
        _required(item, key, "FUNDING")
    account_id = _account_id(item["accountId"])
    if account_id != EXPECTED_ACCOUNT_ID:
        raise GateFailure("FUNDING_ACCOUNT_MISMATCH", "IDENTITY")
    side = _text(item["side"], "FUNDING_SIDE")
    if side not in _POSITION_SIDES:
        raise GateFailure("FUNDING_SIDE_INVALID", "SCHEMA")
    return {
        "id": _integer(item["id"], "FUNDING_ID", positive=True),
        "account_id": account_id,
        "market": _text(item["market"], "FUNDING_MARKET"),
        "position_id": _operation_id(item["positionId"]),
        "side": side,
        "value": _number_text(_decimal(item["value"], "FUNDING_VALUE", nonnegative=True)),
        "mark_price": _number_text(_decimal(item["markPrice"], "FUNDING_MARK_PRICE", positive=True)),
        "funding_fee": _number_text(_decimal(item["fundingFee"], "FUNDING_FEE")),
        "funding_rate": _number_text(_decimal(item["fundingRate"], "FUNDING_RATE")),
        "paid_time": _integer(item["paidTime"], "FUNDING_PAID_TIME", positive=True),
    }


def _row_summary(rows: Sequence[Mapping[str, Any]], *, id_key: str = "id") -> dict[str, Any]:
    ids = [row[id_key] for row in rows]
    markets = sorted({str(row["market"]) for row in rows if "market" in row})
    return {
        "count": len(rows),
        "markets": markets,
        "ids_digest": _digest(ids),
    }


def _open_order_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    result = _row_summary(rows)
    result["regular_count"] = sum(not bool(row["trigger"]) for row in rows)
    result["trigger_count"] = sum(bool(row["trigger"]) for row in rows)
    return result


def _funding_summary(rows: Sequence[Mapping[str, Any]], pages: int) -> dict[str, Any]:
    result = _row_summary(rows)
    result["pages"] = pages
    if rows:
        total = sum((Decimal(row["funding_fee"]) for row in rows), Decimal("0"))
        result["status"] = "APPLIED_RECORDS"
        result["cash_total"] = _number_text(total)
        result["records_digest"] = _digest(list(rows))
    else:
        result["status"] = "AUTHORITATIVE_EMPTY_HISTORY"
        result["cash_total"] = None
        result["records_digest"] = _digest([])
    return result


def _reconcile_identity(info: Mapping[str, Any], accounts: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    matches = [
        row
        for row in accounts
        if row["account_id"] == info["account_id"]
        and row["l2_key"] == info["l2_key"]
        and row["l2_vault"] == info["l2_vault"]
    ]
    if len(matches) != 1:
        raise GateFailure("ACCOUNT_INFO_ACCOUNTS_DISAGREE", "IDENTITY")
    match = matches[0]
    if match["account_index"] != EXPECTED_ACCOUNT_INDEX:
        raise GateFailure("ACCOUNT_INDEX_MISMATCH", "IDENTITY")
    if info.get("account_index") is not None and info["account_index"] != match["account_index"]:
        raise GateFailure("ACCOUNT_INDEX_DISAGREEMENT", "IDENTITY")
    if match["status"] != info["status"] or match["status"] != "ACTIVE":
        raise GateFailure("ACCOUNT_STATUS_DISAGREEMENT", "IDENTITY")
    identity = {
        "account_id": match["account_id"],
        "account_index": match["account_index"],
        "l2_key": match["l2_key"],
        "l2_vault": match["l2_vault"],
    }
    if onboarding.ExtendedPublicIdentity.from_metadata(identity) != EXPECTED_IDENTITY:
        raise GateFailure("PROTECTED_IDENTITY_MISMATCH", "IDENTITY")
    return identity


def _validate_stream_frame(
    raw: Any,
    previous_sequence: int | None,
) -> tuple[str, Any, int]:
    if not isinstance(raw, Mapping):
        raise GateFailure("STREAM_FRAME_INVALID", "SCHEMA")
    for key in ("type", "data", "ts", "seq"):
        _required(raw, key, "STREAM_FRAME")
    frame_type = _text(raw["type"], "STREAM_TYPE")
    if frame_type not in _STREAM_TYPES:
        raise GateFailure("STREAM_TYPE_UNKNOWN", "SCHEMA")
    if raw.get("error") not in (None, ""):
        raise GateFailure("STREAM_AUTHENTICATION_ERROR", "AUTH")
    _integer(raw["ts"], "STREAM_TIMESTAMP", positive=True)
    sequence = _integer(raw["seq"], "STREAM_SEQUENCE", positive=True)
    if previous_sequence is not None and sequence < previous_sequence:
        raise GateFailure("STREAM_SEQUENCE_REGRESSION", "SAFETY")
    data = raw["data"]
    if not isinstance(data, Mapping):
        raise GateFailure("STREAM_DATA_INVALID", "SCHEMA")
    matching_key = {
        "BALANCE": "balance",
        "ORDER": "orders",
        "TRADE": "trades",
        "POSITION": "positions",
        "SPOT_BALANCE": "spotBalances",
    }[frame_type]
    if matching_key not in data:
        raise GateFailure("STREAM_COMPONENT_MISSING", "SCHEMA")
    if frame_type == "BALANCE":
        for key in ("orders", "trades", "positions", "spotBalances"):
            if data.get(key) not in (None, []):
                raise GateFailure("STREAM_COMPONENT_MIXED", "SCHEMA")
        value = _decode_balance_data(data[matching_key], stream=True)
    else:
        if not isinstance(data[matching_key], list):
            raise GateFailure("STREAM_COMPONENT_INVALID", "SCHEMA")
        for key in ("balance", "orders", "trades", "positions", "spotBalances"):
            if key != matching_key and data.get(key) not in (None, []):
                raise GateFailure("STREAM_COMPONENT_MIXED", "SCHEMA")
        value = data[matching_key]
    return frame_type, value, sequence


def _validate_stream_surface(stream: Any) -> None:
    _validate_transport_metadata(getattr(stream, "upgrade_metadata", None), MAINNET_STREAM_URL)
    if getattr(stream, "application_frames_sent", 0) != 0:
        raise GateFailure("STREAM_OUTBOUND_FORBIDDEN", "SAFETY")
    if getattr(stream, "reconnect_count", 0) != 0:
        raise GateFailure("STREAM_RECONNECT_FORBIDDEN", "SAFETY")


async def _read_stream_once(
    *,
    store: RunStore,
    transport: Any,
    api_key: str,
) -> StreamSnapshot:
    request = StreamRequest(
        headers={"User-Agent": USER_AGENT, API_KEY_HEADER: api_key}
    )
    try:
        stream = await _call(transport.open_stream(request))
    except asyncio.CancelledError:
        raise
    except GateFailure:
        raise
    except BaseException as exc:
        if _is_transport_failure(exc):
            raise TransportInterruption() from None
        raise GateFailure("UNCLASSIFIED_FAILURE", "SAFETY") from None
    try:
        _validate_stream_surface(stream)
    except BaseException:
        try:
            await _call(stream.close())
        except BaseException:
            pass
        raise
    components: set[str] = set()
    balance: Mapping[str, Any] | None = None
    orders: tuple[Mapping[str, Any], ...] = ()
    trades: tuple[Mapping[str, Any], ...] = ()
    positions: tuple[Mapping[str, Any], ...] = ()
    spot_balances: tuple[Mapping[str, Any], ...] = ()
    frames = 0
    previous_sequence: int | None = None
    deadline = asyncio.get_running_loop().time() + STREAM_READ_TIMEOUT_SECONDS
    failure: BaseException | None = None
    try:
        while not _STREAM_COMPONENTS.issubset(components):
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise TransportInterruption()
            try:
                raw = await asyncio.wait_for(stream.recv(), remaining)
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                raise TransportInterruption()
            except StopAsyncIteration as exc:
                raise TransportInterruption() from exc
            except BaseException as exc:
                if _is_transport_failure(exc):
                    raise TransportInterruption() from None
                raise GateFailure("UNCLASSIFIED_FAILURE", "SAFETY") from None
            frames += 1
            frame_type, value, previous_sequence = _validate_stream_frame(raw, previous_sequence)
            if frame_type == "BALANCE":
                balance = value
            elif frame_type == "ORDER":
                rows = tuple(_decode_order(item, _ALL_ORDER_STATUSES, stream=True) for item in value)
                if rows:
                    raise GateFailure("STREAM_ORDER_ACTIVITY", "SAFETY")
                orders = rows
            elif frame_type == "TRADE":
                rows = tuple(_decode_trade(item, stream=True) for item in value)
                if rows:
                    raise GateFailure("STREAM_TRADE_ACTIVITY", "SAFETY")
                trades = rows
            elif frame_type == "POSITION":
                rows = tuple(_decode_position(item, stream=True) for item in value)
                if rows:
                    raise GateFailure("STREAM_POSITION_ACTIVITY", "SAFETY")
                positions = rows
            elif frame_type == "SPOT_BALANCE":
                rows = tuple(_decode_spot_rows({"status": "OK", "data": value}))
                noncollateral = _noncollateral_nonzero_spot_assets(rows)
                if noncollateral:
                    raise GateFailure("STREAM_UNRELATED_SPOT_STATE", "SAFETY")
                spot_balances = rows
            components.add(frame_type)
    except BaseException as exc:
        failure = exc
    finally:
        try:
            await _call(stream.close())
        except BaseException as exc:
            if failure is None:
                failure = GateFailure("STREAM_CLOSE_FAILED", "TRANSPORT")
        if getattr(stream, "application_frames_sent", 0) != 0:
            if failure is None:
                failure = GateFailure("STREAM_OUTBOUND_FORBIDDEN", "SAFETY")
    if failure is not None:
        raise failure
    if balance is None or not _STREAM_COMPONENTS.issubset(components):
        raise TransportInterruption()
    return StreamSnapshot(
        frozenset(components),
        balance,
        orders,
        trades,
        positions,
        spot_balances,
        frames,
        previous_sequence,
    )


async def _read_stream(
    *, store: RunStore, transport: Any, api_key: str
) -> StreamSnapshot:
    for attempt in (1, 2):
        store.attempt("private_stream", retry=attempt == 2)
        try:
            snapshot = await _read_stream_once(store=store, transport=transport, api_key=api_key)
            store.complete("private_stream")
            return snapshot
        except asyncio.CancelledError:
            raise
        except TransportInterruption:
            if attempt == 1:
                continue
            raise GateFailure("TRANSPORT_RETRY_EXHAUSTED", "TRANSPORT") from None
        except GateFailure:
            raise
        except BaseException:
            raise GateFailure("UNCLASSIFIED_FAILURE", "SAFETY") from None
    raise GateFailure("TRANSPORT_RETRY_EXHAUSTED", "TRANSPORT")


def _stream_digest_rows(rows: Sequence[Mapping[str, Any]]) -> str:
    return _digest(list(rows))


def _reconcile_stream(
    stream: StreamSnapshot,
    balance: Mapping[str, Any],
    spot: Sequence[Mapping[str, Any]],
    orders: Sequence[Mapping[str, Any]],
    positions: Sequence[Mapping[str, Any]],
) -> None:
    rest_balance = dict(balance)
    stream_balance = dict(stream.balance)
    rest_balance.pop("balance_source", None)
    stream_balance.pop("balance_source", None)
    if stream_balance != rest_balance:
        raise GateFailure("REST_STREAM_BALANCE_DISAGREE", "SAFETY")
    if _stream_digest_rows(stream.spot_balances) != _stream_digest_rows(spot):
        raise GateFailure("REST_STREAM_SPOT_DISAGREE", "SAFETY")
    if _stream_digest_rows(stream.orders) != _stream_digest_rows(orders):
        raise GateFailure("REST_STREAM_ORDER_DISAGREE", "SAFETY")
    if _stream_digest_rows(stream.positions) != _stream_digest_rows(positions):
        raise GateFailure("REST_STREAM_POSITION_DISAGREE", "SAFETY")


def _set_summary_state(
    summary: dict[str, Any],
    *,
    balance: Mapping[str, Any],
    spot: Sequence[Mapping[str, Any]],
    asset_operations: Sequence[Mapping[str, Any]],
    fees: Sequence[Mapping[str, Any]],
    open_orders: Sequence[Mapping[str, Any]],
    order_history: Sequence[Mapping[str, Any]],
    trades: Sequence[Mapping[str, Any]],
    positions: Sequence[Mapping[str, Any]],
    position_history: Sequence[Mapping[str, Any]],
    funding: Sequence[Mapping[str, Any]],
    page_counts: Mapping[str, int],
) -> None:
    summary["balance"] = dict(balance)
    summary["spot_balances"] = {
        "count": len(spot),
        "assets": sorted(row["asset"] for row in spot),
        "noncollateral_nonzero_assets": _noncollateral_nonzero_spot_assets(spot),
        "rows_digest": _stream_digest_rows(spot),
    }
    pending = [
        row
        for row in asset_operations
        if row["status"] in {"CREATED", "IN_PROGRESS"}
    ]
    summary["asset_operations"] = {
        "count": len(asset_operations),
        "pending_count": len(pending),
        "pages": page_counts.get("asset_operations", 0),
        "types": sorted({row["type"] for row in asset_operations}),
        "ids_digest": _digest([row["id"] for row in asset_operations]),
    }
    summary["fees"] = {
        "count": len(fees),
        "markets": sorted(row["market"] for row in fees),
        "rates": {
            row["market"]: {
                "maker": row["maker_fee_rate"],
                "taker": row["taker_fee_rate"],
                **({"builder": row["builder_fee_rate"]} if "builder_fee_rate" in row else {}),
            }
            for row in fees
        },
    }
    summary["open_orders"] = _open_order_summary(open_orders)
    summary["order_history"] = {
        **_row_summary(order_history),
        "pages": page_counts.get("order_history", 0),
    }
    summary["trades"] = {
        **_row_summary(trades),
        "pages": page_counts.get("trades", 0),
    }
    summary["positions"] = _row_summary(positions)
    summary["position_history"] = {
        **_row_summary(position_history),
        "pages": page_counts.get("position_history", 0),
        "open_records": sum(bool(row.get("open_record")) for row in position_history),
    }
    summary["funding"] = _funding_summary(funding, page_counts.get("funding", 0))
    zero_fields = {
        field: balance[field]
        for field in (
            "initialMargin",
            "marginRatio",
            "exposure",
            "leverage",
            "unrealisedPnl",
            "withdrawableUnrealisedPnl",
            "collateralReservedForSpotOrders",
        )
    }
    exact_zero = all(Decimal(value) == 0 for value in zero_fields.values())
    exact_formula = (
        Decimal(balance["equity"])
        == Decimal(balance["balance"])
        + Decimal(balance["unrealisedPnl"])
        + Decimal(balance["spotEquity"])
        and Decimal(balance["availableForTrade"])
        == Decimal(balance["balance"])
        + Decimal(balance["unrealisedPnl"])
        + Decimal(balance["spotEquityForAvailableForTrade"])
        - Decimal(balance["initialMargin"])
    )
    summary["flatness"] = {
        "exact": bool(
            exact_zero
            and exact_formula
            and not positions
            and not open_orders
            and not summary["spot_balances"]["noncollateral_nonzero_assets"]
        ),
        "zero_fields": zero_fields,
        "formula_agreement": exact_formula,
    }
    categories: list[str] = []
    if open_orders:
        categories.append("OPEN_ORDERS")
    if summary["open_orders"]["trigger_count"]:
        categories.append("TRIGGER_ORDERS")
    if positions:
        categories.append("OPEN_POSITIONS")
    if summary["spot_balances"]["noncollateral_nonzero_assets"]:
        categories.append("NONCOLLATERAL_SPOT")
    if pending:
        categories.append("PENDING_ASSET_OPERATIONS")
    summary["unrelated_state"] = {
        "status": "CLEAR" if not categories else "PRESENT",
        "categories": categories,
        "active_order_count": len(open_orders),
        "open_position_count": len(positions),
    }
    historical: list[str] = []
    if order_history:
        historical.append("ORDER_HISTORY")
    if trades:
        historical.append("TRADE_HISTORY")
    if position_history:
        historical.append("POSITION_HISTORY")
    if funding:
        historical.append("FUNDING_HISTORY")
    if asset_operations:
        historical.append("ASSET_OPERATION_HISTORY")
    summary["historical_activity"] = historical


class _LoadedCredential:
    def __init__(self, capability: Any, api_key: str, identity: onboarding.ExtendedPublicIdentity):
        self.capability = capability
        self.api_key = api_key
        self.identity = identity
        self.fingerprint = hashlib.sha256(api_key.encode("ascii")).hexdigest()

    async def close(self) -> None:
        value = self.capability.close()
        if inspect.isawaitable(value):
            await value


async def _load_credential(source: Any, store: RunStore) -> _LoadedCredential:
    store.attempt("credential_loader")
    capability: Any = None
    try:
        capability = source.open()
        capability = await _call(capability)
        identity = getattr(capability, "identity", None)
        if not isinstance(identity, onboarding.ExtendedPublicIdentity):
            raise GateFailure("CREDENTIAL_IDENTITY_INVALID", "IDENTITY")
        method = getattr(capability, "api_key", None)
        if not callable(method):
            raise GateFailure("CREDENTIAL_CAPABILITY_INVALID", "AUTH")
        api_key = await _call(method())
        if (
            type(api_key) is not str
            or not api_key
            or api_key != api_key.strip()
            or any(ord(char) < 33 or ord(char) > 126 for char in api_key)
        ):
            raise GateFailure("CREDENTIAL_INVALID", "AUTH")
        fingerprint = hashlib.sha256(api_key.encode("ascii")).hexdigest()
        declared = getattr(capability, "api_key_fingerprint", fingerprint)
        if declared != fingerprint:
            raise GateFailure("CREDENTIAL_FINGERPRINT_MISMATCH", "AUTH")
        loaded = _LoadedCredential(capability, api_key, identity)
        store.complete("credential_loader")
        return loaded
    except asyncio.CancelledError:
        if capability is not None:
            try:
                await _call(capability.close())
            except BaseException:
                pass
        raise
    except GateFailure:
        if capability is not None:
            try:
                await _call(capability.close())
            except BaseException:
                pass
        raise
    except BaseException as exc:
        if capability is not None:
            try:
                await _call(capability.close())
            except BaseException:
                pass
        code = getattr(exc, "code", None)
        if type(code) is str and code in {"PROTECTED_FILES_MISSING", "PROTECTED_PATH_ALREADY_EXISTS"}:
            raise GateFailure("CREDENTIAL_PATH_UNAVAILABLE", "SAFETY") from None
        raise GateFailure("CREDENTIAL_UNAVAILABLE", "AUTH") from None


class _ProductionCredentialSource:
    def open(self) -> onboarding.ProtectedExtendedCredentials:
        return onboarding.discover_protected_credentials()


class MainnetRestTransport:
    """Direct fixed-host GET transport for the explicit operator gate."""

    def __init__(self, timeout_seconds: float = REQUEST_TIMEOUT_SECONDS):
        if type(timeout_seconds) not in {int, float} or timeout_seconds <= 0:
            raise ValueError("invalid timeout")
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=float(timeout_seconds)),
            trust_env=False,
        )

    async def get(self, request: RestRequest) -> RestReply:
        if request.path not in {
            ACCOUNT_INFO_PATH,
            ACCOUNTS_PATH,
            BALANCE_PATH,
            SPOT_BALANCES_PATH,
            ASSET_OPERATIONS_PATH,
            FEES_PATH,
            OPEN_ORDERS_PATH,
            ORDER_HISTORY_PATH,
            TRADES_PATH,
            POSITIONS_PATH,
            POSITION_HISTORY_PATH,
            FUNDING_HISTORY_PATH,
        }:
            raise GateFailure("ENDPOINT_NOT_ALLOWLISTED", "SAFETY")
        try:
            async with self._session.get(
                request.url,
                headers=dict(request.headers),
                allow_redirects=False,
                proxy=None,
            ) as response:
                raw = await response.content.read(MAX_RESPONSE_BYTES + 1)
                if len(raw) > MAX_RESPONSE_BYTES:
                    raise GateFailure("RESPONSE_TOO_LARGE", "SCHEMA")
                if response.status != 200:
                    body: Any = None
                else:
                    try:
                        body = json.loads(raw)
                    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                        raise GateFailure("RESPONSE_JSON_INVALID", "SCHEMA") from exc
                return RestReply(
                    response.status,
                    str(response.url),
                    body,
                    True,
                    len(raw),
                )
        except GateFailure:
            raise
        except asyncio.CancelledError:
            raise
        except (aiohttp.ClientPayloadError, aiohttp.ClientConnectionError, asyncio.IncompleteReadError, EOFError, OSError, asyncio.TimeoutError):
            raise TransportInterruption() from None
        except aiohttp.ClientError:
            raise TransportInterruption() from None

    async def open_stream(self, request: StreamRequest) -> Any:
        try:
            socket = await self._session.ws_connect(
                request.url,
                headers=dict(request.headers),
                proxy=None,
                timeout=REQUEST_TIMEOUT_SECONDS,
                autoclose=True,
                autoping=True,
                max_msg_size=MAX_STREAM_FRAME_BYTES,
            )
        except asyncio.CancelledError:
            raise
        except aiohttp.WSServerHandshakeError as exc:
            if exc.status in {401, 403}:
                raise GateFailure("STREAM_AUTHENTICATION_REJECTED", "AUTH") from None
            raise GateFailure("STREAM_HTTP_REJECTED", "HTTP") from None
        except (aiohttp.ClientConnectionError, aiohttp.ClientPayloadError, asyncio.TimeoutError, OSError):
            raise TransportInterruption() from None
        except aiohttp.ClientError:
            raise TransportInterruption() from None
        final_url = str(socket._response.url)
        if final_url != request.url:
            await socket.close()
            raise GateFailure("STREAM_REDIRECT_FORBIDDEN", "SAFETY")
        return _DirectStream(socket, request)

    async def close(self) -> None:
        await self._session.close()


class _DirectStream:
    def __init__(self, socket: aiohttp.ClientWebSocketResponse, request: StreamRequest):
        self._socket = socket
        self.application_frames_sent = 0
        self.reconnect_count = 0
        self.upgrade_metadata = {
            "actual_url": request.url,
            "method": HTTP_METHOD,
            "header_names": list(_canonical_headers(request.headers)),
            "direct_tls": True,
            "trust_env": False,
            "proxy": None,
            "redirects": 0,
            "retries": 0,
            "application_frames_sent": False,
        }

    async def recv(self) -> Any:
        try:
            message = await self._socket.receive()
        except asyncio.CancelledError:
            raise
        except (aiohttp.ClientConnectionError, aiohttp.ClientPayloadError, OSError, asyncio.TimeoutError):
            raise TransportInterruption() from None
        if message.type == aiohttp.WSMsgType.TEXT:
            if len(message.data.encode("utf-8")) > MAX_STREAM_FRAME_BYTES:
                raise GateFailure("STREAM_FRAME_TOO_LARGE", "SCHEMA")
            try:
                return json.loads(message.data)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise GateFailure("STREAM_FRAME_JSON_INVALID", "SCHEMA") from exc
        if message.type in {aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
            raise TransportInterruption()
        if message.type == aiohttp.WSMsgType.BINARY:
            raise GateFailure("STREAM_BINARY_FRAME", "SCHEMA")
        return await self.recv()

    async def close(self) -> None:
        await self._socket.close()


async def _run_gate(
    *,
    store: RunStore,
    credential_source: Any,
    transport: Any,
    clock_ms: Callable[[], int],
) -> ReadResult:
    existing = store.claim()
    if existing is not None:
        return existing
    summary = _empty_summary()
    loaded: _LoadedCredential | None = None
    stream_frames = 0
    rest_calls = 0
    result: ReadResult | None = None
    try:
        loaded = await _load_credential(credential_source, store)
        summary["identity_verified"] = loaded.identity == EXPECTED_IDENTITY
        summary["credential_contract"] = "READ_ONLY_X_API_KEY"
        if loaded.identity != EXPECTED_IDENTITY:
            raise GateFailure("PROTECTED_IDENTITY_MISMATCH", "IDENTITY")
        summary["account_info"] = loaded.identity.to_metadata()

        info, _ = await _get_decoded(
            store=store,
            transport=transport,
            key="rest_account_info",
            path=ACCOUNT_INFO_PATH,
            query=(),
            api_key=loaded.api_key,
            endpoint="ACCOUNT_INFO",
            decoder=_decode_account_info,
            clock_ms=clock_ms,
        )
        rest_calls += 1
        accounts, _ = await _get_decoded(
            store=store,
            transport=transport,
            key="rest_accounts",
            path=ACCOUNTS_PATH,
            query=(),
            api_key=loaded.api_key,
            endpoint="ACCOUNTS",
            decoder=_decode_accounts,
            clock_ms=clock_ms,
        )
        rest_calls += 1
        identity = _reconcile_identity(info, accounts)
        summary["identity"] = identity
        summary["account_info"] = identity
        summary["account_list"] = {
            "count": len(accounts),
            "other_account_indices": sorted(
                row["account_index"] for row in accounts if row["account_index"] != EXPECTED_ACCOUNT_INDEX
            ),
            "rows_digest": _digest(list(accounts)),
        }

        balance, _ = await _get_decoded(
            store=store,
            transport=transport,
            key="rest_balance",
            path=BALANCE_PATH,
            query=(),
            api_key=loaded.api_key,
            endpoint="BALANCE",
            decoder=_decode_balance,
            clock_ms=clock_ms,
            not_found=_zero_balance,
        )
        rest_calls += 1
        spot, _ = await _get_decoded(
            store=store,
            transport=transport,
            key="rest_spot_balances",
            path=SPOT_BALANCES_PATH,
            query=(("accountId", str(EXPECTED_ACCOUNT_ID)),),
            api_key=loaded.api_key,
            endpoint="SPOT_BALANCES",
            decoder=lambda body: _decode_spot_rows(body),
            clock_ms=clock_ms,
            not_found=_zero_spot_balances,
        )
        rest_calls += 1
        asset_operations, asset_page_evidence = await _read_pages(
            store=store,
            transport=transport,
            api_key=loaded.api_key,
            name="asset_operations",
            path=ASSET_OPERATIONS_PATH,
            limit=ASSET_OPERATION_PAGE_ITEMS,
            decoder=_decode_asset_operation_rows,
            item_decoder=_decode_asset_operation,
            clock_ms=clock_ms,
        )
        rest_calls += asset_page_evidence.pages
        fees, _ = await _get_decoded(
            store=store,
            transport=transport,
            key="rest_fees",
            path=FEES_PATH,
            query=(("market", FEE_MARKET),),
            api_key=loaded.api_key,
            endpoint="FEES",
            decoder=_decode_fees,
            clock_ms=clock_ms,
        )
        rest_calls += 1

        open_orders, open_page_evidence = await _read_open_orders(
            store=store,
            transport=transport,
            api_key=loaded.api_key,
            clock_ms=clock_ms,
        )
        rest_calls += open_page_evidence.pages
        order_history, order_history_page_evidence = await _read_pages(
            store=store,
            transport=transport,
            api_key=loaded.api_key,
            name="order_history",
            path=ORDER_HISTORY_PATH,
            limit=MAX_PAGE_ITEMS,
            decoder=_decode_history_orders,
            item_decoder=lambda item: _decode_order(item, _HISTORY_ORDER_STATUSES),
            clock_ms=clock_ms,
        )
        rest_calls += order_history_page_evidence.pages
        trades, trade_page_evidence = await _read_pages(
            store=store,
            transport=transport,
            api_key=loaded.api_key,
            name="trades",
            path=TRADES_PATH,
            limit=MAX_PAGE_ITEMS,
            decoder=_decode_trades,
            item_decoder=_decode_trade,
            clock_ms=clock_ms,
        )
        rest_calls += trade_page_evidence.pages
        positions_raw, _ = await _get_decoded(
            store=store,
            transport=transport,
            key="rest_positions",
            path=POSITIONS_PATH,
            query=(),
            api_key=loaded.api_key,
            endpoint="POSITIONS",
            decoder=_decode_positions,
            clock_ms=clock_ms,
        )
        rest_calls += 1
        positions = tuple(_decode_position(item) for item in positions_raw)
        position_history, position_history_page_evidence = await _read_pages(
            store=store,
            transport=transport,
            api_key=loaded.api_key,
            name="position_history",
            path=POSITION_HISTORY_PATH,
            limit=MAX_PAGE_ITEMS,
            decoder=_decode_position_history,
            item_decoder=lambda item: _decode_position(item, history=True),
            clock_ms=clock_ms,
        )
        rest_calls += position_history_page_evidence.pages
        funding, funding_page_evidence = await _read_pages(
            store=store,
            transport=transport,
            api_key=loaded.api_key,
            name="funding",
            path=FUNDING_HISTORY_PATH,
            limit=MAX_PAGE_ITEMS,
            base_query=(("startTime", "0"),),
            decoder=_decode_funding,
            item_decoder=_decode_funding_row,
            clock_ms=clock_ms,
        )
        rest_calls += funding_page_evidence.pages
        _set_summary_state(
            summary,
            balance=balance,
            spot=spot,
            asset_operations=asset_operations,
            fees=fees,
            open_orders=open_orders,
            order_history=order_history,
            trades=trades,
            positions=positions,
            position_history=position_history,
            funding=funding,
            page_counts={
                "asset_operations": asset_page_evidence.pages,
                "order_history": order_history_page_evidence.pages,
                "trades": trade_page_evidence.pages,
                "position_history": position_history_page_evidence.pages,
                "funding": funding_page_evidence.pages,
            },
        )
        summary["pagination"] = {
            name: evidence.to_metadata()
            for name, evidence in (
                ("asset_operations", asset_page_evidence),
                ("open_orders", open_page_evidence),
                ("order_history", order_history_page_evidence),
                ("trades", trade_page_evidence),
                ("position_history", position_history_page_evidence),
                ("funding", funding_page_evidence),
            )
        }

        stream = await _read_stream(store=store, transport=transport, api_key=loaded.api_key)
        stream_frames = stream.frames
        _reconcile_stream(stream, balance, spot, open_orders, positions)
        summary["private_stream"] = {
            "status": "READY",
            "components": sorted(stream.components),
            "frames": stream.frames,
            "last_sequence": stream.last_sequence,
            "rest_agreement": True,
            "source": "/stream.extended.exchange/v1/account",
        }
        categories = summary["unrelated_state"]["categories"]
        if categories:
            raise GateFailure("UNRELATED_STATE_PRESENT", "SAFETY")
        if not summary["flatness"]["exact"]:
            raise GateFailure("EXACT_FLATNESS_NOT_PROVEN", "SAFETY")
        if summary["funding"]["status"] not in {"APPLIED_RECORDS", "AUTHORITATIVE_EMPTY_HISTORY"}:
            raise GateFailure("FUNDING_EVIDENCE_MISSING", "SAFETY")
        result = ReadResult(
            STATUS_READY,
            "MAINNET_PRIVATE_READ_PROVED",
            None,
            "FINALIZING",
            store.invocation_id,
            store.config_hash,
            store.counters(),
            identity,
            loaded.fingerprint,
            summary,
            rest_calls,
            stream_frames,
        )
    except asyncio.CancelledError:
        result = ReadResult(
            STATUS_BLOCKED,
            "CANCELLED",
            "SAFETY",
            "CANCELLED",
            store.invocation_id,
            store.config_hash,
            store.counters(),
            summary.get("identity"),
            None if loaded is None else loaded.fingerprint,
            summary,
            rest_calls,
            stream_frames,
        )
    except GateFailure as exc:
        result = ReadResult(
            STATUS_BLOCKED,
            exc.reason,
            exc.failure_class,
            "FAILED",
            store.invocation_id,
            store.config_hash,
            store.counters(),
            summary.get("identity"),
            None if loaded is None else loaded.fingerprint,
            summary,
            rest_calls,
            stream_frames,
        )
    except Exception:
        result = ReadResult(
            STATUS_BLOCKED,
            "UNCLASSIFIED_FAILURE",
            "SAFETY",
            "FAILED",
            store.invocation_id,
            store.config_hash,
            store.counters(),
            summary.get("identity"),
            None if loaded is None else loaded.fingerprint,
            summary,
            rest_calls,
            stream_frames,
        )
    finally:
        if loaded is not None:
            try:
                await loaded.close()
            except BaseException:
                if result is None or result.status == STATUS_READY:
                    result = ReadResult(
                        STATUS_BLOCKED,
                        "CREDENTIAL_CLOSE_FAILED",
                        "SAFETY",
                        "FAILED",
                        store.invocation_id,
                        store.config_hash,
                        store.counters(),
                        summary.get("identity"),
                        loaded.fingerprint,
                        summary,
                        rest_calls,
                        stream_frames,
                    )
    if result is None:
        raise StoreFailure("UNCLASSIFIED_FAILURE", "SAFETY")
    return store.terminal(result)


async def _read_open_orders(
    *,
    store: RunStore,
    transport: Any,
    api_key: str,
    clock_ms: Callable[[], int],
) -> tuple[tuple[Mapping[str, Any], ...], PageEvidence]:
    rows: list[Mapping[str, Any]] = []
    seen: set[int] = set()
    cursor: int | None = None
    pages = 0
    cursors: list[int] = []
    observed_times: list[int] = []
    while pages < MAX_PAGES:
        query = () if cursor is None else (("cursor", str(cursor)),)
        page = pages + 1
        data, observed = await _get_decoded(
            store=store,
            transport=transport,
            key=f"rest_open_orders_page_{page}",
            path=OPEN_ORDERS_PATH,
            query=query,
            api_key=api_key,
            endpoint="OPEN_ORDERS",
            decoder=_decode_open_orders,
            clock_ms=clock_ms,
        )
        raw_rows, pagination = data
        pages += 1
        observed_times.append(observed)
        if len(raw_rows) > MAX_PAGE_ITEMS:
            raise GateFailure("PAGE_TOO_LARGE_OPEN_ORDERS", "SCHEMA")
        decoded = tuple(_decode_order(item, _OPEN_ORDER_STATUSES) for item in raw_rows)
        rows.extend(decoded)
        if pagination is None:
            break
        count = _integer(_required(pagination, "count", "PAGINATION"), "PAGINATION_COUNT", nonnegative=True)
        if count != len(raw_rows):
            raise GateFailure("PAGINATION_COUNT_DISAGREES_OPEN_ORDERS", "SCHEMA")
        next_cursor = pagination.get("cursor")
        if next_cursor is None or len(raw_rows) < MAX_PAGE_ITEMS:
            break
        next_cursor = _integer(next_cursor, "PAGINATION_CURSOR", positive=True)
        if next_cursor in seen:
            raise GateFailure("PAGINATION_CURSOR_REPEATED_OPEN_ORDERS", "SAFETY")
        seen.add(next_cursor)
        cursors.append(next_cursor)
        cursor = next_cursor
    else:
        raise GateFailure("PAGINATION_LIMIT_OPEN_ORDERS", "SAFETY")
    if observed_times != sorted(observed_times):
        raise GateFailure("OBSERVATION_TIME_REGRESSION_OPEN_ORDERS", "SAFETY")
    return (
        tuple(rows),
        PageEvidence(pages, tuple(cursors), tuple(observed_times)),
    )


def _new_invocation_id() -> str:
    return "extended-mainnet-read-" + secrets.token_hex(16)


async def _production_run() -> ReadResult:
    _ensure_run_directory(RUN_DIRECTORY)
    store = RunStore(RUN_STORE_PATH, _new_invocation_id())
    transport = MainnetRestTransport()
    try:
        return await _run_gate(
            store=store,
            credential_source=_ProductionCredentialSource(),
            transport=transport,
            clock_ms=lambda: int(time.time() * 1000),
        )
    finally:
        await transport.close()


async def run_fixture(
    *,
    store_path: Path | str,
    invocation_id: str,
    credential_source: Any,
    transport: Any,
    clock_ms: Callable[[], int] | None = None,
) -> ReadResult:
    """Synthetic seam for deterministic conformance tests; never production CLI."""

    store = RunStore(store_path, invocation_id)
    return await _run_gate(
        store=store,
        credential_source=credential_source,
        transport=transport,
        clock_ms=(lambda: int(time.time() * 1000)) if clock_ms is None else clock_ms,
    )


def main() -> int:
    if len(sys.argv) != 1:
        print(_json({"status": STATUS_BLOCKED, "reason": "ARGUMENTS_FORBIDDEN", "write_ready": False}))
        return 2
    try:
        result = asyncio.run(_production_run())
    except KeyboardInterrupt:
        print(_json({"status": STATUS_BLOCKED, "reason": "CANCELLED", "failure_class": "SAFETY", "write_ready": False}))
        return 1
    except GateFailure as exc:
        print(_json({"status": STATUS_BLOCKED, "reason": exc.reason, "failure_class": exc.failure_class, "write_ready": False}))
        return 1
    except Exception:
        print(_json({"status": STATUS_BLOCKED, "reason": "UNCLASSIFIED_FAILURE", "failure_class": "SAFETY", "write_ready": False}))
        return 1
    print(result.evidence())
    return 0 if result.ready else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ACCOUNT_INFO_PATH",
    "ACCOUNTS_PATH",
    "API_KEY_HEADER",
    "ASSET_OPERATIONS_PATH",
    "BALANCE_PATH",
    "BLOCKED",
    "ENVIRONMENT",
    "EXPECTED_ACCOUNT_ID",
    "EXPECTED_ACCOUNT_INDEX",
    "EXPECTED_IDENTITY",
    "EXPECTED_L2_KEY",
    "EXPECTED_L2_VAULT",
    "FAILURE_CLASSES",
    "FEE_MARKET",
    "FEES_PATH",
    "FUNDING_HISTORY_PATH",
    "GateFailure",
    "HTTP_METHOD",
    "MAINNET_REST_BASE_URL",
    "MAINNET_STREAM_URL",
    "MainnetRestTransport",
    "NO_MAINNET_WRITE_AUTHORITY",
    "OPEN_ORDERS_PATH",
    "ORDER_HISTORY_PATH",
    "POSITION_HISTORY_PATH",
    "POSITIONS_PATH",
    "ReadResult",
    "PageEvidence",
    "REDACTED_RUN_STORE_PATH",
    "RestReply",
    "RestRequest",
    "RunStore",
    "SPOT_BALANCES_PATH",
    "STATUS_BLOCKED",
    "STATUS_READY",
    "STREAM_URL",
    "StreamRequest",
    "StoreFailure",
    "TransportInterruption",
    "TRADES_PATH",
    "RUN_DIRECTORY",
    "RUN_STORE_PATH",
    "run_fixture",
]
