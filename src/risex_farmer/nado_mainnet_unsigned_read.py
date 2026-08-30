"""Nado mainnet unsigned exact-subaccount GET-only read gate.

This module is deliberately isolated from the paper product and from the
testnet private-read runners.  It binds every account-scoped request to the
one public wallet/subaccount identity provisioned by
``nado_mainnet_onboarding`` and exposes only the official Gateway REST query
surface.  The transport has one method: ``get``.  It never creates a request
body, reads a credential, signs data, or dispatches a state-changing call.

The current official Nado Python SDK was evaluated first.  Its public client
combines query and execute APIs and its engine query helper uses POST for the
generic query call, so it is not a sufficiently narrow dependency for this
GET-only boundary.  The direct conformance transport keeps the exact URL,
method, bounded retry rule, and response contract visible.

Official contract references used for this conformance gate:

* https://docs.nado.xyz/developer-resources/api/endpoints
* https://docs.nado.xyz/developer-resources/api/gateway/queries
* https://docs.nado.xyz/developer-resources/api/gateway/queries/subaccount-info
* https://docs.nado.xyz/developer-resources/api/gateway/queries/orders
* https://docs.nado.xyz/developer-resources/api/gateway/queries/fee-rates
* https://nadohq.github.io/nado-python-sdk/_modules/nado_protocol/engine_client/types/models.html
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
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

from . import nado_mainnet_onboarding as onboarding


VENUE = "Nado"
ENVIRONMENT = "MAINNET"
STATUS_BLOCKED = "BLOCKED"
BLOCKED = STATUS_BLOCKED
NO_MAINNET_WRITE_AUTHORITY = onboarding.NO_MAINNET_WRITE_AUTHORITY
NADO_MAINNET_CHAIN_ID = onboarding.NADO_MAINNET_CHAIN_ID
NADO_UNSIGNED_QUERY_AUTHENTICATION = onboarding.NADO_UNSIGNED_QUERY_AUTHENTICATION
NADO_UNSIGNED_READ_STATUS = onboarding.NADO_UNSIGNED_READ_STATUS
NADO_UNSIGNED_QUERY_SOURCE = "NADO_UNSIGNED_QUERY"

MAINNET_GATEWAY_BASE_URL = "https://gateway.prod.nado.xyz/v1"
MAINNET_QUERY_URL = MAINNET_GATEWAY_BASE_URL + "/query"
HTTP_METHOD = "GET"
QUERY_PATH = "/query"

EXPECTED_WALLET_ADDRESS = "0xf3c1b239f2978856839c3b676f22682c04500ac4"
EXPECTED_SUBACCOUNT_NAME = "default"
EXPECTED_SUBACCOUNT = (
    "0xf3c1b239f2978856839c3b676f22682c04500ac"
    "464656661756c740000000000"
)

REQUEST_TIMEOUT_SECONDS = 5.0
MAX_RESPONSE_BYTES = 1_048_576
MAX_PRODUCT_IDS = 256
MAX_ORDERS_PER_PRODUCT = 256
NON_ORDERBOOK_PRODUCT_IDS = frozenset({0, 11})
RUN_DIRECTORY = (
    Path.home() / ".config" / "risex-farmer" / "nado-mainnet-unsigned-read"
)
RUN_STORE_BASENAME = "runs-v1.sqlite3"
RUN_STORE_PATH = RUN_DIRECTORY / RUN_STORE_BASENAME
RUN_DIRECTORY_MODE = 0o700
RUN_STORE_MODE = 0o600
SCHEMA_VERSION = 1

FAILURE_CLASSES = frozenset(
    {"TRANSPORT", "HTTP", "SCHEMA", "AUTH", "IDENTITY", "SAFETY"}
)

PRIVATE_ONLY_BLOCKERS = (
    "TRIGGER_ORDER_LIST_SIGNED_POST_ONLY",
    "ARCHIVE_ORDER_HISTORY_POST_ONLY",
    "ARCHIVE_MATCH_HISTORY_POST_ONLY",
    "ARCHIVE_EVENT_HISTORY_POST_ONLY",
    "ARCHIVE_FUNDING_PAYMENTS_POST_ONLY",
    "FUNDING_ACCOUNT_ATTRIBUTION_UNAVAILABLE_IN_UNSIGNED_GET",
    "PRIVATE_ORDER_FILL_POSITION_STREAM_SIGNED_AUTH_REQUIRED",
)

UNSIGNED_GET_SURFACES = (
    "contracts",
    "all_products",
    "subaccount_info",
    "fee_rates",
    "linked_signer",
    "isolated_positions",
    "subaccount_orders",
)


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
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError, UnicodeError) as exc:
        raise GateFailure("SANITIZATION_FAILED", "SAFETY") from exc


def _digest(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _strict_json_bytes(raw: bytes) -> Any:
    if type(raw) is not bytes:
        raise GateFailure("RESPONSE_BYTES_INVALID", "SCHEMA")
    if len(raw) > MAX_RESPONSE_BYTES:
        raise GateFailure("RESPONSE_TOO_LARGE", "SCHEMA")

    def reject_duplicate(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise GateFailure("RESPONSE_DUPLICATE_KEY", "SCHEMA")
            result[key] = value
        return result

    def reject_constant(_value: str) -> Any:
        raise GateFailure("RESPONSE_NONFINITE_NUMBER", "SCHEMA")

    try:
        return json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=reject_duplicate,
            parse_constant=reject_constant,
        )
    except GateFailure:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GateFailure("RESPONSE_JSON_INVALID", "SCHEMA") from exc


def _strict_json_value(value: Any) -> Any:
    if type(value) is bytes:
        return _strict_json_bytes(value)
    if type(value) is bytearray:
        return _strict_json_bytes(bytes(value))
    if type(value) is str:
        try:
            return _strict_json_bytes(value.encode("utf-8"))
        except UnicodeEncodeError as exc:
            raise GateFailure("RESPONSE_JSON_INVALID", "SCHEMA") from exc
    if isinstance(value, (Mapping, list, tuple)) or value is None:
        # Fixture transports may provide already-decoded values.  Re-encoding
        # with allow_nan=False keeps those values under the same finite JSON
        # contract without retaining any unrecognized fields.
        encoded = _json(value).encode("utf-8")
        return _strict_json_bytes(encoded)
    raise GateFailure("RESPONSE_JSON_INVALID", "SCHEMA")


def _required(value: Any, key: str, field: str) -> Any:
    if not isinstance(value, Mapping) or key not in value:
        raise GateFailure(f"FIELD_MISSING_{field}", "SCHEMA")
    return value[key]


def _text(value: Any, field: str) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise GateFailure(f"TEXT_INVALID_{field}", "SCHEMA")
    return value


def _uint(value: Any, field: str, *, maximum: int | None = None) -> int:
    if type(value) is int and not isinstance(value, bool):
        result = value
    elif type(value) is str and value and value.isdecimal() and not (
        len(value) > 1 and value.startswith("0")
    ):
        result = int(value, 10)
    else:
        raise GateFailure(f"UINT_INVALID_{field}", "SCHEMA")
    if result < 0 or maximum is not None and result > maximum:
        raise GateFailure(f"UINT_RANGE_INVALID_{field}", "SCHEMA")
    return result


def _int_text(value: Any, field: str) -> str:
    if (
        type(value) is not str
        or not value
        or value[0] == "+"
        or value == "-"
        or not (value[1:] if value.startswith("-") else value).isdecimal()
        or len(value) > 2 and value.startswith("-0")
        or len(value) > 1 and not value.startswith("-") and value.startswith("0")
    ):
        raise GateFailure(f"INTEGER_TEXT_INVALID_{field}", "SCHEMA")
    return value


def _uint_text(value: Any, field: str) -> str:
    result = _int_text(value, field)
    if result.startswith("-"):
        raise GateFailure(f"UNSIGNED_TEXT_INVALID_{field}", "SCHEMA")
    return result


def _address(value: Any, field: str, *, allow_zero: bool = False) -> str:
    if (
        type(value) is not str
        or len(value) != 42
        or not value.startswith("0x")
        or any(char not in "0123456789abcdefABCDEF" for char in value[2:])
    ):
        raise GateFailure(f"ADDRESS_INVALID_{field}", "SCHEMA")
    lowered = value.lower()
    if not allow_zero and lowered == "0x" + "00" * 20:
        raise GateFailure(f"ADDRESS_ZERO_{field}", "SCHEMA")
    return lowered


def _bytes32(value: Any, field: str) -> str:
    if (
        type(value) is not str
        or len(value) != 66
        or not value.startswith("0x")
        or any(char not in "0123456789abcdefABCDEF" for char in value[2:])
    ):
        raise GateFailure(f"BYTES32_INVALID_{field}", "SCHEMA")
    return value.lower()


def _validate_invocation_id(value: Any) -> str:
    if (
        type(value) is not str
        or not 1 <= len(value) <= 128
        or any(
            char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-"
            for char in value
        )
    ):
        raise StoreFailure("INVOCATION_ID_INVALID", "SAFETY")
    return value


def _canonical_identity(identity: Any) -> dict[str, Any]:
    if not isinstance(identity, onboarding.NadoPublicIdentity):
        raise GateFailure("PROTECTED_PUBLIC_IDENTITY_UNAVAILABLE", "IDENTITY")
    try:
        exported = onboarding.export_unsigned_read_identity(identity)
    except onboarding.OnboardingViolation as exc:
        raise GateFailure("PROTECTED_PUBLIC_IDENTITY_INVALID", "IDENTITY") from exc
    if (
        exported.get("wallet_address") != EXPECTED_WALLET_ADDRESS
        or exported.get("subaccount_name") != EXPECTED_SUBACCOUNT_NAME
        or exported.get("subaccount") != EXPECTED_SUBACCOUNT
        or exported.get("environment") != ENVIRONMENT
        or exported.get("chain_id") != NADO_MAINNET_CHAIN_ID
        or exported.get("query_authentication") != NADO_UNSIGNED_QUERY_AUTHENTICATION
        or exported.get("read_status") != NADO_UNSIGNED_READ_STATUS
        or exported.get("mainnet_write_authority") != NO_MAINNET_WRITE_AUTHORITY
        or exported.get("write_ready") is not False
    ):
        raise GateFailure("EXACT_SUBACCOUNT_IDENTITY_MISMATCH", "IDENTITY")
    identity_tag = hashlib.sha256(
        (EXPECTED_WALLET_ADDRESS + "\0" + EXPECTED_SUBACCOUNT).encode("ascii")
    ).hexdigest()[:16]
    # Public identity is not a credential.  It is included so a durable
    # result can be independently tied to the exact requested account.
    return {
        "chain_id": NADO_MAINNET_CHAIN_ID,
        "environment": ENVIRONMENT,
        "identity_source": onboarding.NADO_PUBLIC_IDENTITY_SOURCE,
        "identity_tag": identity_tag,
        "mainnet_write_authority": NO_MAINNET_WRITE_AUTHORITY,
        "query_authentication": NADO_UNSIGNED_QUERY_AUTHENTICATION,
        "read_status": NADO_UNSIGNED_READ_STATUS,
        "subaccount": EXPECTED_SUBACCOUNT,
        "subaccount_name": EXPECTED_SUBACCOUNT_NAME,
        "venue": VENUE,
        "wallet_address": EXPECTED_WALLET_ADDRESS,
        "write_ready": False,
    }


def _config_hash() -> str:
    return hashlib.sha256(
        _json(
            {
                "environment": ENVIRONMENT,
                "gateway_query_url": MAINNET_QUERY_URL,
                "identity": EXPECTED_SUBACCOUNT,
                "schema_version": SCHEMA_VERSION,
                "surfaces": UNSIGNED_GET_SURFACES,
                "venue": VENUE,
            }
        ).encode("utf-8")
    ).hexdigest()


CONFIG_HASH = _config_hash()


@dataclass(frozen=True)
class GetRequest:
    """A closed-world unsigned Gateway query request."""

    query_type: str
    params: tuple[tuple[str, str], ...]
    attempt: int

    @property
    def method(self) -> str:
        return HTTP_METHOD

    @property
    def body(self) -> None:
        return None

    @property
    def url(self) -> str:
        return MAINNET_QUERY_URL + "?" + urlencode(self.params)

    def metadata(self) -> dict[str, Any]:
        return {
            "attempt": self.attempt,
            "method": HTTP_METHOD,
            "path": QUERY_PATH,
            "query_keys": [key for key, _ in self.params],
            "query_type": self.query_type,
            "body_present": False,
        }


@dataclass(frozen=True)
class GetReply:
    status: int
    final_url: str
    body: Any
    complete: bool = True
    body_bytes: int | None = None


@dataclass(frozen=True)
class QueryEvidence:
    surface: str
    request_type: str
    attempts: int
    observed_at_ms: int
    response_digest: str
    account_binding: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "account_binding": self.account_binding,
            "attempts": self.attempts,
            "method": HTTP_METHOD,
            "observed_at_ms": self.observed_at_ms,
            "path": QUERY_PATH,
            "request_type": self.request_type,
            "response_digest": self.response_digest,
            "status": "OBSERVED",
            "surface": self.surface,
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
    queries: tuple[Mapping[str, Any], ...]
    account: Mapping[str, Any] | None
    funding: Mapping[str, Any]
    blockers: tuple[str, ...]
    unrelated_state: Mapping[str, Any]
    surface_inventory: tuple[Mapping[str, Any], ...]
    read_complete: bool

    @property
    def write_ready(self) -> bool:
        return False

    @property
    def mainnet_write_authority(self) -> str:
        return NO_MAINNET_WRITE_AUTHORITY

    @property
    def ready(self) -> bool:
        return False

    def to_metadata(self) -> dict[str, Any]:
        return {
            "account": None if self.account is None else dict(self.account),
            "blockers": list(self.blockers),
            "config_hash": self.config_hash,
            "counters": dict(self.counters),
            "failure_class": self.failure_class,
            "funding": dict(self.funding),
            "identity": None if self.identity is None else dict(self.identity),
            "invocation_id": self.invocation_id,
            "mainnet_write_authority": NO_MAINNET_WRITE_AUTHORITY,
            "phase": self.phase,
            "queries": [dict(item) for item in self.queries],
            "read_complete": self.read_complete,
            "reason": self.reason,
            "status": self.status,
            "surface_inventory": [dict(item) for item in self.surface_inventory],
            "unrelated_state": dict(self.unrelated_state),
            "venue": VENUE,
            "write_ready": False,
        }

    def evidence(self) -> str:
        return _json(self.to_metadata())


def _empty_funding() -> dict[str, Any]:
    return {
        "account_attribution": "UNKNOWN",
        "current_cumulative_markers": [],
        "historical_payments": "UNKNOWN",
        "payment_status": "BLOCKED",
        "reason": "ARCHIVE_FUNDING_POST_ONLY",
    }


def _empty_unrelated_state() -> dict[str, Any]:
    return {
        "identity_bound_records_rejected": 0,
        "status": "ONLY_EXACT_SUBACCOUNT_SURFACES_CHECKED",
    }


def _surface_inventory() -> tuple[dict[str, Any], ...]:
    return (
        {
            "authentication": "SIGNED",
            "method": "POST",
            "reason": "TRIGGER_ORDER_LIST_SIGNED_POST_ONLY",
            "status": "BLOCKED",
            "surface": "trigger_orders",
        },
        {
            "authentication": "NONE",
            "method": "POST",
            "reason": "ARCHIVE_ORDER_HISTORY_POST_ONLY",
            "status": "BLOCKED",
            "surface": "archive_order_history",
        },
        {
            "authentication": "NONE",
            "method": "POST",
            "reason": "ARCHIVE_MATCH_HISTORY_POST_ONLY",
            "status": "BLOCKED",
            "surface": "archive_match_history",
        },
        {
            "authentication": "NONE",
            "method": "POST",
            "reason": "ARCHIVE_EVENT_HISTORY_POST_ONLY",
            "status": "BLOCKED",
            "surface": "archive_event_history",
        },
        {
            "authentication": "NONE",
            "method": "POST",
            "reason": "ARCHIVE_FUNDING_PAYMENTS_POST_ONLY",
            "status": "BLOCKED",
            "surface": "archive_funding_payments",
        },
        {
            "authentication": "SIGNED",
            "method": "WEBSOCKET",
            "reason": "PRIVATE_ORDER_FILL_POSITION_STREAM_SIGNED_AUTH_REQUIRED",
            "status": "BLOCKED",
            "surface": "private_order_fill_position_stream",
        },
    )


def _blocked_result(
    *,
    invocation_id: str,
    identity: Mapping[str, Any] | None,
    reason: str,
    failure_class: str | None,
    counters: Mapping[str, int],
    queries: Sequence[Mapping[str, Any]] = (),
    account: Mapping[str, Any] | None = None,
    funding: Mapping[str, Any] | None = None,
    blockers: Sequence[str] = (),
    unrelated_state: Mapping[str, Any] | None = None,
    read_complete: bool = False,
    phase: str = "RUNNING",
) -> ReadResult:
    return ReadResult(
        STATUS_BLOCKED,
        reason,
        failure_class,
        phase,
        invocation_id,
        CONFIG_HASH,
        dict(counters),
        None if identity is None else dict(identity),
        tuple(dict(item) for item in queries),
        None if account is None else dict(account),
        dict(_empty_funding() if funding is None else funding),
        tuple(blockers),
        dict(_empty_unrelated_state() if unrelated_state is None else unrelated_state),
        tuple(_surface_inventory()),
        read_complete,
    )


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
    result: dict[str, int] = {}
    for key, item in value.items():
        if (
            type(key) is not str
            or not key
            or len(key) > 180
            or any(
                char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
                for char in key
            )
            or type(item) is not int
            or isinstance(item, bool)
            or item < 0
            or item > 2
        ):
            raise StoreFailure("DURABLE_COUNTERS_INVALID", "SAFETY")
        result[key] = item
    for key, item in result.items():
        if key.endswith("_completions"):
            attempts_key = key[: -len("_completions")] + "_attempts"
            if result.get(attempts_key, 0) < item:
                raise StoreFailure("DURABLE_COUNTERS_INVALID", "SAFETY")
    return result


def _validate_run_path(path: Path, *, may_create: bool) -> None:
    if not path.is_absolute() or path.parent.is_symlink():
        raise StoreFailure("STORE_PATH_INVALID", "SAFETY")
    try:
        parent = path.parent.lstat()
    except OSError as exc:
        raise StoreFailure("STORE_PARENT_INVALID", "SAFETY") from exc
    if not stat.S_ISDIR(parent.st_mode) or parent.st_uid != os.getuid():
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
    if not path.is_absolute() or path.is_symlink():
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
    """Protected append-by-invocation journal; an interrupted run never resumes."""

    _TABLE = "nado_mainnet_unsigned_read_runs"
    _COLUMNS = (
        "invocation_id",
        "schema_version",
        "config_hash",
        "state",
        "phase",
        "counters",
        "evidence",
    )

    def __init__(
        self,
        path: Path | str,
        invocation_id: str,
        *,
        config_hash: str = CONFIG_HASH,
    ) -> None:
        self.path = Path(path)
        self.invocation_id = _validate_invocation_id(invocation_id)
        if (
            type(config_hash) is not str
            or len(config_hash) != 64
            or any(char not in "0123456789abcdef" for char in config_hash)
        ):
            raise StoreFailure("CONFIG_HASH_INVALID", "SAFETY")
        self.config_hash = config_hash
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
                    for row in connection.execute(f"PRAGMA table_info({self._TABLE})")
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
            interrupted = _blocked_result(
                invocation_id=self.invocation_id,
                identity=None,
                reason="INTERRUPTED_RUNNING_INVOCATION",
                failure_class="SAFETY",
                counters=counters,
                blockers=("INTERRUPTED_RUNNING_INVOCATION", *PRIVATE_ONLY_BLOCKERS),
            )
            return self._terminalize(interrupted)
        raise StoreFailure("DURABLE_STATE_INVALID", "SAFETY")

    def attempt(self, effect: str, *, retry: bool = False) -> None:
        if (
            type(effect) is not str
            or not effect
            or len(effect) > 180
            or any(
                char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
                for char in effect
            )
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
        if counters.get(attempts_key, 0) not in {1, 2} or counters.get(
            completions_key, 0
        ) != 0:
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
        terminal = ReadResult(
            result.status,
            result.reason,
            result.failure_class,
            "TERMINAL",
            result.invocation_id,
            result.config_hash,
            counters,
            result.identity,
            result.queries,
            result.account,
            result.funding,
            result.blockers,
            result.unrelated_state,
            result.surface_inventory,
            result.read_complete,
        )
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
        "account",
        "blockers",
        "config_hash",
        "counters",
        "failure_class",
        "funding",
        "identity",
        "invocation_id",
        "mainnet_write_authority",
        "phase",
        "queries",
        "read_complete",
        "reason",
        "status",
        "surface_inventory",
        "unrelated_state",
        "venue",
        "write_ready",
    }
    if type(value) is not dict or set(value) != required:
        raise StoreFailure("DURABLE_EVIDENCE_INVALID", "SAFETY")
    if (
        value["invocation_id"] != invocation_id
        or value["config_hash"] != config_hash
        or value["phase"] != "TERMINAL"
        or value["status"] != STATUS_BLOCKED
        or value["mainnet_write_authority"] != NO_MAINNET_WRITE_AUTHORITY
        or value["venue"] != VENUE
        or value["write_ready"] is not False
        or type(value["read_complete"]) is not bool
        or type(value["queries"]) is not list
        or type(value["blockers"]) is not list
        or type(value["surface_inventory"]) is not list
        or type(value["account"]) is not dict
        and value["account"] is not None
        or type(value["unrelated_state"]) is not dict
        or type(value["funding"]) is not dict
        or value["failure_class"] is not None
        and value["failure_class"] not in FAILURE_CLASSES
    ):
        raise StoreFailure("DURABLE_EVIDENCE_INVALID", "SAFETY")
    counters = _decode_counters(value["counters"])
    identity = value["identity"]
    if identity is not None and type(identity) is not dict:
        raise StoreFailure("DURABLE_EVIDENCE_INVALID", "SAFETY")
    if any(type(item) is not dict for item in value["queries"]):
        raise StoreFailure("DURABLE_EVIDENCE_INVALID", "SAFETY")
    if any(type(item) is not str for item in value["blockers"]):
        raise StoreFailure("DURABLE_EVIDENCE_INVALID", "SAFETY")
    if any(type(item) is not dict for item in value["surface_inventory"]):
        raise StoreFailure("DURABLE_EVIDENCE_INVALID", "SAFETY")
    if identity is not None:
        try:
            expected_identity = _canonical_identity(
                onboarding.NadoPublicIdentity(
                    wallet_address=identity.get("wallet_address", ""),
                    subaccount_name=identity.get("subaccount_name", ""),
                    subaccount=identity.get("subaccount", ""),
                )
            )
        except GateFailure as exc:
            raise StoreFailure("DURABLE_EVIDENCE_INVALID", "SAFETY") from exc
        if identity != expected_identity:
            raise StoreFailure("DURABLE_EVIDENCE_INVALID", "SAFETY")
    return ReadResult(
        STATUS_BLOCKED,
        value["reason"],
        value["failure_class"],
        "TERMINAL",
        invocation_id,
        config_hash,
        counters,
        identity,
        tuple(dict(item) for item in value["queries"]),
        value["account"],
        value["funding"],
        tuple(value["blockers"]),
        value["unrelated_state"],
        tuple(dict(item) for item in value["surface_inventory"]),
        value["read_complete"],
    )


@dataclass(frozen=True)
class ProductCatalog:
    ids: tuple[int, ...]
    spot_ids: frozenset[int]
    perp_ids: frozenset[int]

    @property
    def orderbook_ids(self) -> tuple[int, ...]:
        return tuple(
            product_id
            for product_id in self.ids
            if product_id not in NON_ORDERBOOK_PRODUCT_IDS
        )


@dataclass(frozen=True)
class SubaccountInfo:
    exists: bool
    summary: Mapping[str, Any]
    spot_balances: tuple[Mapping[str, Any], ...]
    perp_balances: tuple[Mapping[str, Any], ...]


def _decode_envelope(body: Any, expected_request_type: str) -> Any:
    if not isinstance(body, Mapping):
        raise GateFailure("RESPONSE_ENVELOPE_INVALID", "SCHEMA")
    if type(body.get("status")) is not str:
        raise GateFailure("RESPONSE_STATUS_INVALID", "SCHEMA")
    if body["status"] == "failure":
        # The human-readable error is intentionally never retained or echoed.
        raise GateFailure("NADO_QUERY_FAILURE", "HTTP")
    if body["status"] != "success":
        raise GateFailure("RESPONSE_STATUS_INVALID", "SCHEMA")
    if body.get("request_type") != expected_request_type:
        raise GateFailure("RESPONSE_REQUEST_TYPE_MISMATCH", "SCHEMA")
    if "data" not in body:
        raise GateFailure("RESPONSE_DATA_MISSING", "SCHEMA")
    if "error" in body and body["error"] not in (None, ""):
        raise GateFailure("RESPONSE_ERROR_PRESENT", "SCHEMA")
    return body["data"]


def _decode_contracts(data: Any) -> Mapping[str, Any]:
    chain_id = _required(data, "chain_id", "CONTRACTS_CHAIN_ID")
    endpoint = _required(data, "endpoint_addr", "CONTRACTS_ENDPOINT")
    if _uint(chain_id, "CONTRACTS_CHAIN_ID") != NADO_MAINNET_CHAIN_ID:
        raise GateFailure("MAINNET_CHAIN_ID_MISMATCH", "IDENTITY")
    endpoint = _address(endpoint, "CONTRACTS_ENDPOINT")
    return {
        "chain_id": NADO_MAINNET_CHAIN_ID,
        "endpoint_address": endpoint,
        "status": "OBSERVED",
    }


def _decode_product_ids(
    data: Any, key: str, label: str
) -> tuple[int, ...]:
    rows = _required(data, key, label)
    if type(rows) is not list:
        raise GateFailure(f"PRODUCT_LIST_INVALID_{label}", "SCHEMA")
    if len(rows) > MAX_PRODUCT_IDS:
        raise GateFailure(f"PRODUCT_LIST_TOO_LARGE_{label}", "SCHEMA")
    result: list[int] = []
    seen: set[int] = set()
    for row in rows:
        product_id = _uint(_required(row, "product_id", f"{label}_PRODUCT_ID"), f"{label}_PRODUCT_ID")
        if product_id in seen:
            raise GateFailure(f"PRODUCT_ID_REPEATED_{label}", "SCHEMA")
        seen.add(product_id)
        result.append(product_id)
    return tuple(result)


def _decode_all_products(data: Any) -> ProductCatalog:
    spot_ids = _decode_product_ids(data, "spot_products", "SPOT")
    perp_ids = _decode_product_ids(data, "perp_products", "PERP")
    overlap = set(spot_ids) & set(perp_ids)
    if overlap:
        raise GateFailure("PRODUCT_ID_CROSS_MARKET_REPEATED", "SCHEMA")
    ids = tuple(sorted((*spot_ids, *perp_ids)))
    if not ids:
        raise GateFailure("PRODUCT_CATALOG_EMPTY", "SCHEMA")
    if len(ids) > MAX_PRODUCT_IDS:
        raise GateFailure("PRODUCT_CATALOG_TOO_LARGE", "SCHEMA")
    if 0 not in spot_ids:
        raise GateFailure("QUOTE_PRODUCT_MISSING", "SCHEMA")
    return ProductCatalog(ids, frozenset(spot_ids), frozenset(perp_ids))


def _decode_healths(value: Any, field: str) -> tuple[dict[str, str], ...]:
    if type(value) is not list or len(value) != 3:
        raise GateFailure(f"HEALTHS_INVALID_{field}", "SCHEMA")
    result: list[dict[str, str]] = []
    for index, item in enumerate(value):
        assets = _int_text(
            _required(item, "assets", f"{field}_{index}_ASSETS"),
            f"{field}_{index}_ASSETS",
        )
        liabilities = _int_text(
            _required(item, "liabilities", f"{field}_{index}_LIABILITIES"),
            f"{field}_{index}_LIABILITIES",
        )
        health = _int_text(
            _required(item, "health", f"{field}_{index}_HEALTH"),
            f"{field}_{index}_HEALTH",
        )
        result.append(
            {"assets": assets, "health": health, "liabilities": liabilities}
        )
    return tuple(result)


def _decode_health_contributions(
    value: Any, catalog_ids: frozenset[int]
) -> tuple[tuple[str, ...], ...]:
    if type(value) is not list:
        raise GateFailure("HEALTH_CONTRIBUTIONS_INVALID", "SCHEMA")
    if len(value) != max(catalog_ids) + 1:
        raise GateFailure("HEALTH_CONTRIBUTION_COVERAGE_INCOMPLETE", "SCHEMA")
    result: list[tuple[str, ...]] = []
    for index, row in enumerate(value):
        if type(row) is not list or len(row) != 3:
            raise GateFailure(f"HEALTH_CONTRIBUTION_INVALID_{index}", "SCHEMA")
        parsed = tuple(
            _int_text(item, f"HEALTH_CONTRIBUTION_{index}_{offset}")
            for offset, item in enumerate(row)
        )
        if index not in catalog_ids and parsed != ("0", "0", "0"):
            raise GateFailure("UNUSED_HEALTH_CONTRIBUTION_NONZERO", "SAFETY")
        result.append(parsed)
    return tuple(result)


def _decode_spot_balance(
    row: Any, product_ids: frozenset[int], index: int
) -> dict[str, Any]:
    product_id = _uint(
        _required(row, "product_id", f"SPOT_BALANCE_{index}_PRODUCT_ID"),
        f"SPOT_BALANCE_{index}_PRODUCT_ID",
    )
    if product_id not in product_ids:
        raise GateFailure("BALANCE_PRODUCT_NOT_IN_CATALOG", "SCHEMA")
    balance = _required(row, "balance", f"SPOT_BALANCE_{index}_BALANCE")
    amount = _int_text(
        _required(balance, "amount", f"SPOT_BALANCE_{index}_AMOUNT"),
        f"SPOT_BALANCE_{index}_AMOUNT",
    )
    return {"amount_x18": amount, "product_id": product_id}


def _decode_perp_balance(
    row: Any, product_ids: frozenset[int], index: int
) -> dict[str, Any]:
    product_id = _uint(
        _required(row, "product_id", f"PERP_BALANCE_{index}_PRODUCT_ID"),
        f"PERP_BALANCE_{index}_PRODUCT_ID",
    )
    if product_id not in product_ids:
        raise GateFailure("BALANCE_PRODUCT_NOT_IN_CATALOG", "SCHEMA")
    balance = _required(row, "balance", f"PERP_BALANCE_{index}_BALANCE")
    amount = _int_text(
        _required(balance, "amount", f"PERP_BALANCE_{index}_AMOUNT"),
        f"PERP_BALANCE_{index}_AMOUNT",
    )
    v_quote = _int_text(
        _required(balance, "v_quote_balance", f"PERP_BALANCE_{index}_V_QUOTE"),
        f"PERP_BALANCE_{index}_V_QUOTE",
    )
    funding = _int_text(
        _required(
            balance,
            "last_cumulative_funding_x18",
            f"PERP_BALANCE_{index}_FUNDING",
        ),
        f"PERP_BALANCE_{index}_FUNDING",
    )
    return {
        "amount_x18": amount,
        "last_cumulative_funding_x18": funding,
        "product_id": product_id,
        "v_quote_balance_x18": v_quote,
    }


def _decode_product_references(
    value: Any,
    field: str,
    allowed: frozenset[int],
) -> tuple[int, ...]:
    if type(value) is not list:
        raise GateFailure(f"PRODUCT_REFERENCES_INVALID_{field}", "SCHEMA")
    result: list[int] = []
    seen: set[int] = set()
    for index, row in enumerate(value):
        product_id = _uint(
            _required(row, "product_id", f"{field}_{index}_PRODUCT_ID"),
            f"{field}_{index}_PRODUCT_ID",
        )
        if product_id not in allowed:
            raise GateFailure("PRODUCT_REFERENCE_NOT_IN_CATALOG", "SCHEMA")
        if product_id in seen:
            raise GateFailure(f"PRODUCT_REFERENCE_REPEATED_{field}", "SCHEMA")
        seen.add(product_id)
        result.append(product_id)
    if set(result) != set(allowed):
        raise GateFailure(f"PRODUCT_REFERENCE_COVERAGE_INCOMPLETE_{field}", "SCHEMA")
    return tuple(result)


def _decode_subaccount_info(
    data: Any, catalog: ProductCatalog
) -> SubaccountInfo:
    subaccount = _bytes32(
        _required(data, "subaccount", "SUBACCOUNT_INFO_SUBACCOUNT"),
        "SUBACCOUNT_INFO_SUBACCOUNT",
    )
    if subaccount != EXPECTED_SUBACCOUNT:
        raise GateFailure("SUBACCOUNT_RESPONSE_IDENTITY_MISMATCH", "IDENTITY")
    exists = _required(data, "exists", "SUBACCOUNT_INFO_EXISTS")
    if type(exists) is not bool:
        raise GateFailure("SUBACCOUNT_EXISTS_INVALID", "SCHEMA")
    healths = _decode_healths(
        _required(data, "healths", "SUBACCOUNT_INFO_HEALTHS"),
        "SUBACCOUNT_INFO",
    )
    raw_spot = _required(data, "spot_balances", "SUBACCOUNT_INFO_SPOT_BALANCES")
    raw_perp = _required(data, "perp_balances", "SUBACCOUNT_INFO_PERP_BALANCES")
    raw_spot_products = _required(
        data, "spot_products", "SUBACCOUNT_INFO_SPOT_PRODUCTS"
    )
    raw_perp_products = _required(
        data, "perp_products", "SUBACCOUNT_INFO_PERP_PRODUCTS"
    )
    if not exists:
        raw_contributions = _required(
            data,
            "health_contributions",
            "SUBACCOUNT_INFO_HEALTH_CONTRIBUTIONS",
        )
        if type(raw_spot) is not list or type(raw_perp) is not list:
            raise GateFailure("MISSING_SUBACCOUNT_VECTOR_INVALID", "SCHEMA")
        if type(raw_spot_products) is not list or type(raw_perp_products) is not list:
            raise GateFailure("MISSING_SUBACCOUNT_PRODUCT_LIST_INVALID", "SCHEMA")
        missing_spot_count = _uint(
            _required(data, "spot_count", "SUBACCOUNT_INFO_SPOT_COUNT"),
            "SUBACCOUNT_INFO_SPOT_COUNT",
        )
        missing_perp_count = _uint(
            _required(data, "perp_count", "SUBACCOUNT_INFO_PERP_COUNT"),
            "SUBACCOUNT_INFO_PERP_COUNT",
        )
        if (
            raw_spot
            or raw_perp
            or raw_spot_products
            or raw_perp_products
            or missing_spot_count != 0
            or missing_perp_count != 0
            or any(item["health"] != "0" for item in healths)
        ):
            raise GateFailure("NONEMPTY_MISSING_SUBACCOUNT", "SCHEMA")
        if type(raw_contributions) is not list:
            raise GateFailure("HEALTH_CONTRIBUTIONS_INVALID", "SCHEMA")
        if raw_contributions:
            health_contributions = _decode_health_contributions(
                raw_contributions,
                frozenset((*catalog.spot_ids, *catalog.perp_ids)),
            )
            if any(item != ("0", "0", "0") for item in health_contributions):
                raise GateFailure("NONZERO_MISSING_SUBACCOUNT", "SCHEMA")
        else:
            health_contributions = ()
        if "pre_state" in data and data["pre_state"] not in (None, {}):
            raise GateFailure("SIMULATED_PRE_STATE_PRESENT", "SAFETY")
        summary = {
            "exists": False,
            "health_contributions": [list(row) for row in health_contributions],
            "healths": [dict(item) for item in healths],
            "perp_balances": [],
            "perp_count": 0,
            "perp_products": [],
            "spot_balances": [],
            "spot_count": 0,
            "spot_products": [],
            "subaccount": EXPECTED_SUBACCOUNT,
        }
        return SubaccountInfo(False, summary, (), ())
    health_contributions = _decode_health_contributions(
        _required(
            data,
            "health_contributions",
            "SUBACCOUNT_INFO_HEALTH_CONTRIBUTIONS",
        ),
        frozenset((*catalog.spot_ids, *catalog.perp_ids)),
    )
    spot_count = _uint(
        _required(data, "spot_count", "SUBACCOUNT_INFO_SPOT_COUNT"),
        "SUBACCOUNT_INFO_SPOT_COUNT",
    )
    perp_count = _uint(
        _required(data, "perp_count", "SUBACCOUNT_INFO_PERP_COUNT"),
        "SUBACCOUNT_INFO_PERP_COUNT",
    )
    if type(raw_spot) is not list or len(raw_spot) > MAX_PRODUCT_IDS:
        raise GateFailure("SPOT_BALANCES_INVALID", "SCHEMA")
    if type(raw_perp) is not list or len(raw_perp) > MAX_PRODUCT_IDS:
        raise GateFailure("PERP_BALANCES_INVALID", "SCHEMA")
    spot_balances = tuple(
        _decode_spot_balance(row, catalog.spot_ids, index)
        for index, row in enumerate(raw_spot)
    )
    perp_balances = tuple(
        _decode_perp_balance(row, catalog.perp_ids, index)
        for index, row in enumerate(raw_perp)
    )
    if len({item["product_id"] for item in spot_balances}) != len(spot_balances):
        raise GateFailure("SPOT_BALANCE_PRODUCT_REPEATED", "SCHEMA")
    if len({item["product_id"] for item in perp_balances}) != len(perp_balances):
        raise GateFailure("PERP_BALANCE_PRODUCT_REPEATED", "SCHEMA")
    if set(item["product_id"] for item in spot_balances) & set(
        item["product_id"] for item in perp_balances
    ):
        raise GateFailure("BALANCE_PRODUCT_CROSS_MARKET_REPEATED", "SCHEMA")
    if set(item["product_id"] for item in spot_balances) != set(catalog.spot_ids):
        raise GateFailure("SPOT_BALANCE_COVERAGE_INCOMPLETE", "SCHEMA")
    if set(item["product_id"] for item in perp_balances) != set(catalog.perp_ids):
        raise GateFailure("PERP_BALANCE_COVERAGE_INCOMPLETE", "SCHEMA")
    if spot_count != len(spot_balances):
        raise GateFailure("SPOT_COUNT_DISAGREES", "SCHEMA")
    if perp_count != len(perp_balances):
        raise GateFailure("PERP_COUNT_DISAGREES", "SCHEMA")
    spot_products = _decode_product_references(
        raw_spot_products,
        "SPOT_PRODUCTS",
        catalog.spot_ids,
    )
    perp_products = _decode_product_references(
        raw_perp_products,
        "PERP_PRODUCTS",
        catalog.perp_ids,
    )
    if "pre_state" in data and data["pre_state"] not in (None, {}):
        raise GateFailure("SIMULATED_PRE_STATE_PRESENT", "SAFETY")
    summary = {
        "exists": exists,
        "health_contributions": [list(row) for row in health_contributions],
        "healths": [dict(item) for item in healths],
        "perp_balances": [dict(item) for item in perp_balances],
        "perp_count": perp_count,
        "perp_products": list(perp_products),
        "spot_balances": [dict(item) for item in spot_balances],
        "spot_count": spot_count,
        "spot_products": list(spot_products),
        "subaccount": EXPECTED_SUBACCOUNT,
    }
    return SubaccountInfo(exists, summary, spot_balances, perp_balances)


def _decode_fee_rates(data: Any, catalog: ProductCatalog) -> Mapping[str, Any]:
    arrays: dict[str, list[str]] = {}
    for field in (
        "taker_fee_rates_x18",
        "maker_fee_rates_x18",
        "withdraw_sequencer_fees",
    ):
        value = _required(data, field, f"FEE_RATES_{field.upper()}")
        if type(value) is not list or len(value) > MAX_PRODUCT_IDS:
            raise GateFailure("FEE_RATES_ARRAY_INVALID", "SCHEMA")
        arrays[field] = [
            _int_text(item, f"FEE_RATES_{field.upper()}_{index}")
            for index, item in enumerate(value)
        ]
    if not (
        len(arrays["taker_fee_rates_x18"])
        == len(arrays["maker_fee_rates_x18"])
        == len(arrays["withdraw_sequencer_fees"])
    ):
        raise GateFailure("FEE_RATES_ARRAY_LENGTH_MISMATCH", "SCHEMA")
    if len(arrays["taker_fee_rates_x18"]) <= max(catalog.ids):
        raise GateFailure("FEE_RATES_PRODUCT_COVERAGE_MISSING", "SCHEMA")
    sequencer: dict[str, str] = {}
    for field in (
        "liquidation_sequencer_fee",
        "health_check_sequencer_fee",
        "taker_sequencer_fee",
    ):
        sequencer[field] = _int_text(
            _required(data, field, f"FEE_RATES_{field.upper()}"),
            f"FEE_RATES_{field.upper()}",
        )
    return {
        **arrays,
        **sequencer,
        "max_product_id": max(catalog.ids),
        "status": "OBSERVED",
    }


def _decode_linked_signer(data: Any) -> Mapping[str, Any]:
    signer = _address(
        _required(data, "linked_signer", "LINKED_SIGNER"),
        "LINKED_SIGNER",
        allow_zero=True,
    )
    return {"linked_signer": signer, "status": "OBSERVED"}


def _decode_isolated_position(
    row: Any, catalog: ProductCatalog, index: int
) -> Mapping[str, Any]:
    isolated_subaccount = _bytes32(
        _required(row, "subaccount", f"ISOLATED_POSITION_{index}_SUBACCOUNT"),
        f"ISOLATED_POSITION_{index}_SUBACCOUNT",
    )
    # The engine reports the isolated child subaccount here.  Its first 20
    # bytes must still be the exact wallet bound to the requested parent.
    if isolated_subaccount[2:42] != EXPECTED_WALLET_ADDRESS[2:]:
        raise GateFailure("ISOLATED_POSITION_WALLET_MISMATCH", "IDENTITY")
    quote_balance = _required(
        row, "quote_balance", f"ISOLATED_POSITION_{index}_QUOTE_BALANCE"
    )
    base_balance = _required(
        row, "base_balance", f"ISOLATED_POSITION_{index}_BASE_BALANCE"
    )
    quote_product = _required(
        row, "quote_product", f"ISOLATED_POSITION_{index}_QUOTE_PRODUCT"
    )
    base_product = _required(
        row, "base_product", f"ISOLATED_POSITION_{index}_BASE_PRODUCT"
    )
    quote_product_id = _uint(
        _required(quote_product, "product_id", f"ISOLATED_POSITION_{index}_QUOTE_ID"),
        f"ISOLATED_POSITION_{index}_QUOTE_ID",
    )
    base_product_id = _uint(
        _required(base_product, "product_id", f"ISOLATED_POSITION_{index}_BASE_ID"),
        f"ISOLATED_POSITION_{index}_BASE_ID",
    )
    if quote_product_id not in catalog.spot_ids or base_product_id not in catalog.perp_ids:
        raise GateFailure("ISOLATED_POSITION_PRODUCT_NOT_IN_CATALOG", "SCHEMA")
    if _uint(
        _required(quote_balance, "product_id", f"ISOLATED_POSITION_{index}_QUOTE_BALANCE_ID"),
        f"ISOLATED_POSITION_{index}_QUOTE_BALANCE_ID",
    ) != quote_product_id:
        raise GateFailure("ISOLATED_QUOTE_PRODUCT_MISMATCH", "SCHEMA")
    if _uint(
        _required(base_balance, "product_id", f"ISOLATED_POSITION_{index}_BASE_BALANCE_ID"),
        f"ISOLATED_POSITION_{index}_BASE_BALANCE_ID",
    ) != base_product_id:
        raise GateFailure("ISOLATED_BASE_PRODUCT_MISMATCH", "SCHEMA")
    quote_inner = _required(
        quote_balance, "balance", f"ISOLATED_POSITION_{index}_QUOTE_INNER"
    )
    base_inner = _required(
        base_balance, "balance", f"ISOLATED_POSITION_{index}_BASE_INNER"
    )
    quote_amount = _int_text(
        _required(quote_inner, "amount", f"ISOLATED_POSITION_{index}_QUOTE_AMOUNT"),
        f"ISOLATED_POSITION_{index}_QUOTE_AMOUNT",
    )
    base_amount = _int_text(
        _required(base_inner, "amount", f"ISOLATED_POSITION_{index}_BASE_AMOUNT"),
        f"ISOLATED_POSITION_{index}_BASE_AMOUNT",
    )
    base_v_quote = _int_text(
        _required(
            base_inner,
            "v_quote_balance",
            f"ISOLATED_POSITION_{index}_V_QUOTE",
        ),
        f"ISOLATED_POSITION_{index}_V_QUOTE",
    )
    base_funding = _int_text(
        _required(
            base_inner,
            "last_cumulative_funding_x18",
            f"ISOLATED_POSITION_{index}_FUNDING",
        ),
        f"ISOLATED_POSITION_{index}_FUNDING",
    )
    healths = _decode_healths(
        _required(row, "healths", f"ISOLATED_POSITION_{index}_HEALTHS"),
        f"ISOLATED_POSITION_{index}_HEALTHS",
    )
    for field in ("quote_healths", "base_healths"):
        value = _required(row, field, f"ISOLATED_POSITION_{index}_{field.upper()}")
        if type(value) is not list:
            raise GateFailure(
                f"ISOLATED_POSITION_{index}_{field.upper()}_INVALID", "SCHEMA"
            )
    return {
        "base_amount_x18": base_amount,
        "base_funding_x18": base_funding,
        "base_product_id": base_product_id,
        "base_v_quote_balance_x18": base_v_quote,
        "healths": [dict(item) for item in healths],
        "isolated_subaccount": isolated_subaccount,
        "quote_amount_x18": quote_amount,
        "quote_product_id": quote_product_id,
    }


def _decode_isolated_positions(
    data: Any, catalog: ProductCatalog
) -> tuple[Mapping[str, Any], ...]:
    rows = _required(data, "isolated_positions", "ISOLATED_POSITIONS")
    if type(rows) is not list or len(rows) > MAX_PRODUCT_IDS:
        raise GateFailure("ISOLATED_POSITIONS_INVALID", "SCHEMA")
    result = tuple(
        _decode_isolated_position(row, catalog, index)
        for index, row in enumerate(rows)
    )
    if len({item["isolated_subaccount"] for item in result}) != len(result):
        raise GateFailure("ISOLATED_SUBACCOUNT_REPEATED", "SCHEMA")
    return result


def _decode_order(
    row: Any, product_id: int, index: int
) -> Mapping[str, Any]:
    returned_product_id = _uint(
        _required(row, "product_id", f"ORDER_{index}_PRODUCT_ID"),
        f"ORDER_{index}_PRODUCT_ID",
    )
    if returned_product_id != product_id:
        raise GateFailure("ORDER_PRODUCT_ID_MISMATCH", "IDENTITY")
    sender = _bytes32(
        _required(row, "sender", f"ORDER_{index}_SENDER"),
        f"ORDER_{index}_SENDER",
    )
    if sender != EXPECTED_SUBACCOUNT:
        raise GateFailure("ORDER_SENDER_IDENTITY_MISMATCH", "IDENTITY")
    price = _int_text(
        _required(row, "price_x18", f"ORDER_{index}_PRICE"),
        f"ORDER_{index}_PRICE",
    )
    amount = _int_text(
        _required(row, "amount", f"ORDER_{index}_AMOUNT"),
        f"ORDER_{index}_AMOUNT",
    )
    expiration = _uint_text(
        _required(row, "expiration", f"ORDER_{index}_EXPIRATION"),
        f"ORDER_{index}_EXPIRATION",
    )
    nonce = _uint_text(
        _required(row, "nonce", f"ORDER_{index}_NONCE"),
        f"ORDER_{index}_NONCE",
    )
    unfilled = _int_text(
        _required(row, "unfilled_amount", f"ORDER_{index}_UNFILLED"),
        f"ORDER_{index}_UNFILLED",
    )
    if unfilled == "0":
        raise GateFailure("OPEN_ORDER_ZERO_UNFILLED", "SCHEMA")
    digest = _bytes32(
        _required(row, "digest", f"ORDER_{index}_DIGEST"),
        f"ORDER_{index}_DIGEST",
    )
    placed_at_value = _required(row, "placed_at", f"ORDER_{index}_PLACED_AT")
    if type(placed_at_value) is int and not isinstance(placed_at_value, bool):
        placed_at = _uint(placed_at_value, f"ORDER_{index}_PLACED_AT")
    else:
        placed_at = _uint(
            _required(row, "placed_at", f"ORDER_{index}_PLACED_AT"),
            f"ORDER_{index}_PLACED_AT",
        )
    appendix = _uint_text(
        _required(row, "appendix", f"ORDER_{index}_APPENDIX"),
        f"ORDER_{index}_APPENDIX",
    )
    order_type = _text(
        _required(row, "order_type", f"ORDER_{index}_TYPE"),
        f"ORDER_{index}_TYPE",
    )
    if order_type not in {"default", "ioc", "fok", "post_only"}:
        raise GateFailure("ORDER_TYPE_INVALID", "SCHEMA")
    appendix_value = int(appendix, 10)
    return {
        "amount_x18": amount,
        "appendix": appendix,
        "digest": digest,
        "expiration": expiration,
        "isolated": bool((appendix_value >> 8) & 1),
        "nonce": nonce,
        "order_type": order_type,
        "placed_at": placed_at,
        "price_x18": price,
        "product_id": product_id,
        "sender": EXPECTED_SUBACCOUNT,
        "unfilled_amount_x18": unfilled,
    }


def _decode_subaccount_orders(
    data: Any, product_id: int
) -> tuple[Mapping[str, Any], ...]:
    sender = _bytes32(
        _required(data, "sender", "SUBACCOUNT_ORDERS_SENDER"),
        "SUBACCOUNT_ORDERS_SENDER",
    )
    if sender != EXPECTED_SUBACCOUNT:
        raise GateFailure("ORDERS_SENDER_IDENTITY_MISMATCH", "IDENTITY")
    rows = _required(data, "orders", "SUBACCOUNT_ORDERS")
    if type(rows) is not list or len(rows) > MAX_ORDERS_PER_PRODUCT:
        raise GateFailure("SUBACCOUNT_ORDERS_INVALID", "SCHEMA")
    result = tuple(
        _decode_order(row, product_id, index) for index, row in enumerate(rows)
    )
    if len({item["digest"] for item in result}) != len(result):
        raise GateFailure("ORDER_DIGEST_REPEATED", "SCHEMA")
    return result


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


def _coerce_reply(value: Any) -> GetReply:
    if isinstance(value, GetReply):
        reply = value
    elif isinstance(value, Mapping):
        required = {"status", "final_url", "body"}
        if not required.issubset(value):
            raise GateFailure("TRANSPORT_REPLY_SCHEMA_INVALID", "SCHEMA")
        reply = GetReply(
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
    body = _strict_json_value(reply.body)
    encoded_size = len(_json(body).encode("utf-8"))
    if encoded_size > MAX_RESPONSE_BYTES:
        raise GateFailure("RESPONSE_TOO_LARGE", "SCHEMA")
    return GetReply(reply.status, reply.final_url, body, True, encoded_size)


async def _call(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


class MainnetGetTransport:
    """Direct TLS GET-only transport for the fixed Nado Gateway query URL."""

    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None

    async def _session_for_get(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
            connector = aiohttp.TCPConnector(ssl=True, limit=1)
            self._session = aiohttp.ClientSession(
                connector=connector,
                headers={"Accept-Encoding": "gzip, br, deflate"},
                raise_for_status=False,
                timeout=timeout,
                trust_env=False,
            )
        return self._session

    async def get(self, request: GetRequest) -> GetReply:
        if (
            not isinstance(request, GetRequest)
            or request.method != HTTP_METHOD
            or request.body is not None
            or request.url.split("?", 1)[0] != MAINNET_QUERY_URL
        ):
            raise GateFailure("GET_TRANSPORT_REQUEST_INVALID", "SAFETY")
        session = await self._session_for_get()
        try:
            async with session.get(
                MAINNET_QUERY_URL,
                allow_redirects=False,
                params=request.params,
            ) as response:
                if response.status != 200:
                    # The body of a non-success response is not needed for a
                    # safe classification and is never copied into evidence.
                    return GetReply(
                        response.status,
                        str(response.url),
                        None,
                        True,
                        None,
                    )
                chunks: list[bytes] = []
                size = 0
                async for chunk in response.content.iter_chunked(65_536):
                    size += len(chunk)
                    if size > MAX_RESPONSE_BYTES:
                        raise GateFailure("RESPONSE_TOO_LARGE", "SCHEMA")
                    chunks.append(bytes(chunk))
                body = _strict_json_bytes(b"".join(chunks))
                return GetReply(
                    response.status,
                    str(response.url),
                    body,
                    True,
                    size,
                )
        except GateFailure:
            raise
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            if _is_transport_failure(exc):
                raise TransportInterruption() from None
            raise GateFailure("UNCLASSIFIED_TRANSPORT_FAILURE", "SAFETY") from None

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()


@dataclass
class _ObservationClock:
    clock_ms: Callable[[], int]
    last: int = -1

    def now(self) -> int:
        value = self.clock_ms()
        if type(value) is not int or value < 0 or value < self.last:
            raise GateFailure("OBSERVATION_CLOCK_INVALID", "SAFETY")
        self.last = value
        return value


async def _get_decoded(
    *,
    store: RunStore,
    transport: Any,
    surface: str,
    query_type: str,
    params: tuple[tuple[str, str], ...],
    expected_request_type: str,
    decoder: Callable[[Any], Any],
    account_binding: str,
    observation_clock: _ObservationClock,
) -> tuple[Any, QueryEvidence]:
    for attempt in (1, 2):
        store.attempt(surface, retry=attempt == 2)
        request = GetRequest(query_type, params, attempt)
        try:
            getter = getattr(transport, "get", None)
            if not callable(getter):
                raise GateFailure("GET_TRANSPORT_METHOD_MISSING", "SAFETY")
            reply = _coerce_reply(await _call(getter(request)))
            if reply.final_url != request.url:
                raise GateFailure("REDIRECT_FORBIDDEN", "SAFETY")
            if reply.status in {401, 403}:
                raise GateFailure("UNSIGNED_QUERY_AUTH_REJECTED", "AUTH")
            if reply.status != 200:
                raise GateFailure("HTTP_STATUS_UNACCEPTED", "HTTP")
            data = _decode_envelope(reply.body, expected_request_type)
            decoded = decoder(data)
            observed_at_ms = observation_clock.now()
            store.complete(surface)
            attempts = store.counters()[surface + "_attempts"]
            return decoded, QueryEvidence(
                surface,
                expected_request_type,
                attempts,
                observed_at_ms,
                _digest(reply.body),
                account_binding,
            )
        except asyncio.CancelledError:
            raise
        except TransportInterruption:
            if attempt == 1:
                continue
            raise GateFailure("TRANSPORT_RETRY_EXHAUSTED", "TRANSPORT") from None
        except GateFailure:
            raise
        except BaseException as exc:
            if _is_transport_failure(exc):
                if attempt == 1:
                    continue
                raise GateFailure("TRANSPORT_RETRY_EXHAUSTED", "TRANSPORT") from None
            raise GateFailure("UNCLASSIFIED_FAILURE", "SAFETY") from None
    raise GateFailure("TRANSPORT_RETRY_EXHAUSTED", "TRANSPORT")


def _funding_summary(info: SubaccountInfo | None) -> dict[str, Any]:
    if info is None:
        return _empty_funding()
    markers = [
        {
            "last_cumulative_funding_x18": item["last_cumulative_funding_x18"],
            "product_id": item["product_id"],
        }
        for item in info.perp_balances
    ]
    return {
        "account_attribution": "CURRENT_PERP_BALANCE_MARKERS_ONLY",
        "current_cumulative_markers": markers,
        "historical_payments": "UNKNOWN",
        "payment_status": "BLOCKED",
        "reason": "ARCHIVE_FUNDING_POST_ONLY",
    }


def _orders_summary(
    orders: Sequence[Mapping[str, Any]] | None,
    product_count: int,
    *,
    account_exists: bool,
) -> dict[str, Any]:
    if orders is None:
        return {
            "isolated_open": None,
            "open_digests_digest": None,
            "open_total": None,
            "products_queried": 0,
            "regular_open": None,
            "status": (
                "ACCOUNT_NOT_CREATED"
                if not account_exists
                else "NOT_OBSERVED"
            ),
        }
    digests = sorted(str(item["digest"]) for item in orders)
    isolated = sum(bool(item["isolated"]) for item in orders)
    regular = len(orders) - isolated
    return {
        "isolated_open": isolated,
        "open_digests_digest": _digest(digests),
        "open_total": len(orders),
        "products_queried": product_count,
        "regular_open": regular,
        "status": "OBSERVED",
    }


def _account_snapshot(
    info: SubaccountInfo,
    catalog: ProductCatalog,
    *,
    fees: Mapping[str, Any] | None = None,
    linked_signer: Mapping[str, Any] | None = None,
    isolated_positions: Sequence[Mapping[str, Any]] | None = None,
    orders: Sequence[Mapping[str, Any]] | None = None,
    orders_queried: bool = False,
) -> dict[str, Any]:
    quote_balance = next(
        (
            item["amount_x18"]
            for item in info.spot_balances
            if item["product_id"] == 0
        ),
        "0" if info.exists else None,
    )
    regular_positions = [
        {
            "amount_x18": item["amount_x18"],
            "last_cumulative_funding_x18": item["last_cumulative_funding_x18"],
            "product_id": item["product_id"],
            "v_quote_balance_x18": item["v_quote_balance_x18"],
        }
        for item in info.perp_balances
        if item["amount_x18"] != "0" or item["v_quote_balance_x18"] != "0"
    ]
    isolated_list = [] if isolated_positions is None else [dict(item) for item in isolated_positions]
    nonquote_spot = [
        dict(item)
        for item in info.spot_balances
        if item["product_id"] != 0 and item["amount_x18"] != "0"
    ]
    perp_residues = [
        dict(item)
        for item in info.perp_balances
        if item["amount_x18"] != "0" or item["v_quote_balance_x18"] != "0"
    ]
    isolated_residues = [
        dict(item)
        for item in isolated_list
        if item["base_amount_x18"] != "0"
        or item["base_v_quote_balance_x18"] != "0"
        or item["quote_amount_x18"] != "0"
    ]
    if isolated_positions is None:
        flatness_status = "UNKNOWN"
    elif regular_positions or isolated_list or nonquote_spot:
        flatness_status = "NOT_FLAT"
    else:
        flatness_status = "EXACT_FLAT"
    all_orders = None if not orders_queried else orders
    orders_summary = _orders_summary(
        all_orders,
        len(catalog.orderbook_ids) if orders_queried else 0,
        account_exists=info.exists,
    )
    return {
        "collateral": {
            "healths": [
                dict(item) for item in info.summary["healths"]
            ],
            "quote_balance_x18": quote_balance,
            "quote_product_id": 0,
            "spot_balances": [dict(item) for item in info.spot_balances],
            "status": "OBSERVED" if info.exists else "ACCOUNT_NOT_CREATED",
        },
        "fee_rates": None if fees is None else dict(fees),
        "flatness": {
            "complete_balance_vectors": True,
            "health_contribution_coverage": "MAX_PRODUCT_ID_PLUS_ONE",
            "isolated_residue_count": len(isolated_residues),
            "perp_residue_count": len(perp_residues),
            "spot_nonquote_nonzero_count": len(nonquote_spot),
            "status": flatness_status,
        },
        "isolated_positions": {
            "count": None if isolated_positions is None else len(isolated_list),
            "items": isolated_list,
            "status": "OBSERVED" if isolated_positions is not None else "NOT_OBSERVED",
        },
        "linked_signer": None if linked_signer is None else dict(linked_signer),
        "orders": orders_summary,
        "product_catalog": {
            "count": len(catalog.ids),
            "product_ids_digest": _digest(list(catalog.ids)),
        },
        "regular_positions": {
            "count": len(regular_positions),
            "items": regular_positions,
            "status": "OBSERVED",
        },
        "subaccount": EXPECTED_SUBACCOUNT,
        "subaccount_exists": info.exists,
    }


def _account_state_blockers(account: Mapping[str, Any] | None) -> tuple[str, ...]:
    if account is None:
        return ()
    blockers: list[str] = []
    orders = account["orders"]
    if orders["open_total"]:
        blockers.append("OPEN_ORDERS_PRESENT")
    if account["regular_positions"]["count"] or account["isolated_positions"]["count"]:
        blockers.append("POSITIONS_NOT_FLAT")
    flatness = account["flatness"]
    if flatness["spot_nonquote_nonzero_count"]:
        blockers.append("NONQUOTE_SPOT_BALANCE_PRESENT")
    if flatness["perp_residue_count"]:
        blockers.append("PERP_V_QUOTE_OR_POSITION_RESIDUE_PRESENT")
    if flatness["isolated_residue_count"]:
        blockers.append("ISOLATED_POSITION_RESIDUE_PRESENT")
    return tuple(blockers)


def _failure_query_evidence(
    surface: str,
    account_binding: str,
    failure: GateFailure,
    counters: Mapping[str, int],
) -> dict[str, Any]:
    return {
        "account_binding": account_binding,
        "attempts": counters.get(surface + "_attempts", 0),
        "failure_class": failure.failure_class,
        "method": HTTP_METHOD,
        "path": QUERY_PATH,
        "reason": failure.reason,
        "status": "BLOCKED",
        "surface": surface,
    }


def _dedupe(items: Sequence[str]) -> tuple[str, ...]:
    result: list[str] = []
    seen: set[str] = set()
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return tuple(result)


async def _run_gate(
    *,
    store: RunStore,
    identity: Any,
    transport: Any,
    clock_ms: Callable[[], int],
) -> ReadResult:
    try:
        identity_metadata = _canonical_identity(identity)
    except GateFailure as failure:
        prior = store.claim()
        if prior is not None:
            return prior
        result = _blocked_result(
            invocation_id=store.invocation_id,
            identity=None,
            reason=failure.reason,
            failure_class=failure.failure_class,
            counters=store.counters(),
            blockers=_dedupe((failure.reason, *PRIVATE_ONLY_BLOCKERS)),
        )
        return store.terminal(result)

    prior = store.claim()
    if prior is not None:
        return prior
    queries: list[Mapping[str, Any]] = []
    current_surface = ""
    current_binding = "NETWORK_BINDING"
    info: SubaccountInfo | None = None
    catalog: ProductCatalog | None = None
    fees: Mapping[str, Any] | None = None
    linked_signer: Mapping[str, Any] | None = None
    isolated_positions: tuple[Mapping[str, Any], ...] | None = None
    orders: list[Mapping[str, Any]] = []
    orders_queried = False
    observation_clock = _ObservationClock(clock_ms)

    def remember(evidence: QueryEvidence) -> None:
        queries.append(evidence.as_dict())

    def partial_account() -> Mapping[str, Any] | None:
        if info is None or catalog is None:
            return None
        return _account_snapshot(
            info,
            catalog,
            fees=fees,
            linked_signer=linked_signer,
            isolated_positions=isolated_positions,
            orders=orders,
            orders_queried=orders_queried,
        )

    try:
        current_surface = "contracts"
        current_binding = "NETWORK_BINDING"
        _contracts, evidence = await _get_decoded(
            store=store,
            transport=transport,
            surface=current_surface,
            query_type="contracts",
            params=(("type", "contracts"),),
            expected_request_type="query_contracts",
            decoder=_decode_contracts,
            account_binding=current_binding,
            observation_clock=observation_clock,
        )
        remember(evidence)

        current_surface = "all_products"
        current_binding = "PUBLIC_CATALOG"
        catalog, evidence = await _get_decoded(
            store=store,
            transport=transport,
            surface=current_surface,
            query_type="all_products",
            params=(("type", "all_products"),),
            expected_request_type="query_all_products",
            decoder=_decode_all_products,
            account_binding=current_binding,
            observation_clock=observation_clock,
        )
        remember(evidence)

        current_surface = "subaccount_info"
        current_binding = "EXACT_SUBACCOUNT"
        info, evidence = await _get_decoded(
            store=store,
            transport=transport,
            surface=current_surface,
            query_type="subaccount_info",
            params=(
                ("type", "subaccount_info"),
                ("subaccount", EXPECTED_SUBACCOUNT),
            ),
            expected_request_type="query_subaccount_info",
            decoder=lambda data: _decode_subaccount_info(data, catalog),
            account_binding=current_binding,
            observation_clock=observation_clock,
        )
        remember(evidence)
        if not info.exists:
            result = _blocked_result(
                invocation_id=store.invocation_id,
                identity=identity_metadata,
                reason="EXACT_SUBACCOUNT_NOT_CREATED",
                failure_class=None,
                counters=store.counters(),
                queries=queries,
                account=_account_snapshot(info, catalog),
                funding=_funding_summary(info),
                blockers=_dedupe(("EXACT_SUBACCOUNT_NOT_CREATED", *PRIVATE_ONLY_BLOCKERS)),
                read_complete=False,
            )
            return store.terminal(result)

        current_surface = "fee_rates"
        current_binding = "EXACT_SUBACCOUNT"
        fees, evidence = await _get_decoded(
            store=store,
            transport=transport,
            surface=current_surface,
            query_type="fee_rates",
            params=(("type", "fee_rates"), ("sender", EXPECTED_SUBACCOUNT)),
            expected_request_type="query_fee_rates",
            decoder=lambda data: _decode_fee_rates(data, catalog),
            account_binding=current_binding,
            observation_clock=observation_clock,
        )
        remember(evidence)

        current_surface = "linked_signer"
        current_binding = "EXACT_SUBACCOUNT"
        linked_signer, evidence = await _get_decoded(
            store=store,
            transport=transport,
            surface=current_surface,
            query_type="linked_signer",
            params=(
                ("type", "linked_signer"),
                ("subaccount", EXPECTED_SUBACCOUNT),
            ),
            expected_request_type="query_linked_signer",
            decoder=_decode_linked_signer,
            account_binding=current_binding,
            observation_clock=observation_clock,
        )
        remember(evidence)

        current_surface = "isolated_positions"
        current_binding = "EXACT_SUBACCOUNT"
        isolated_positions, evidence = await _get_decoded(
            store=store,
            transport=transport,
            surface=current_surface,
            query_type="isolated_positions",
            params=(
                ("type", "isolated_positions"),
                ("subaccount", EXPECTED_SUBACCOUNT),
            ),
            expected_request_type="query_isolated_positions",
            decoder=lambda data: _decode_isolated_positions(data, catalog),
            account_binding=current_binding,
            observation_clock=observation_clock,
        )
        remember(evidence)

        current_binding = "EXACT_SUBACCOUNT_AND_PRODUCT"
        for product_id in catalog.orderbook_ids:
            current_surface = f"subaccount_orders_{product_id}"
            decoded_orders, evidence = await _get_decoded(
                store=store,
                transport=transport,
                surface=current_surface,
                query_type="subaccount_orders",
                params=(
                    ("type", "subaccount_orders"),
                    ("sender", EXPECTED_SUBACCOUNT),
                    ("product_id", str(product_id)),
                ),
                expected_request_type="query_subaccount_orders",
                decoder=lambda data, product_id=product_id: _decode_subaccount_orders(
                    data, product_id
                ),
                account_binding=current_binding,
                observation_clock=observation_clock,
            )
            orders.extend(decoded_orders)
            remember(evidence)
        orders_queried = True

        account = _account_snapshot(
            info,
            catalog,
            fees=fees,
            linked_signer=linked_signer,
            isolated_positions=isolated_positions,
            orders=orders,
            orders_queried=orders_queried,
        )
        extra_blockers: list[str] = [
            *PRIVATE_ONLY_BLOCKERS,
            *_account_state_blockers(account),
        ]
        result = _blocked_result(
            invocation_id=store.invocation_id,
            identity=identity_metadata,
            reason="UNSIGNED_GET_COMPLETE_FUNDING_BLOCKED",
            failure_class=None,
            counters=store.counters(),
            queries=queries,
            account=account,
            funding=_funding_summary(info),
            blockers=_dedupe(extra_blockers),
            read_complete=True,
        )
        return store.terminal(result)
    except StoreFailure:
        raise
    except asyncio.CancelledError:
        result = _blocked_result(
            invocation_id=store.invocation_id,
            identity=identity_metadata,
            reason="READ_CANCELLED",
            failure_class="SAFETY",
            counters=store.counters(),
            queries=queries,
            account=partial_account(),
            funding=_funding_summary(info),
            blockers=_dedupe(("READ_CANCELLED", *PRIVATE_ONLY_BLOCKERS)),
        )
        return store.terminal(result)
    except GateFailure as failure:
        counters = store.counters()
        if current_surface and not any(
            item.get("surface") == current_surface for item in queries
        ):
            queries.append(
                _failure_query_evidence(
                    current_surface,
                    current_binding,
                    failure,
                    counters,
                )
            )
        partial = partial_account()
        result = _blocked_result(
            invocation_id=store.invocation_id,
            identity=identity_metadata,
            reason=failure.reason,
            failure_class=failure.failure_class,
            counters=counters,
            queries=queries,
            account=partial,
            funding=_funding_summary(info),
            blockers=_dedupe(
                (failure.reason, *_account_state_blockers(partial), *PRIVATE_ONLY_BLOCKERS)
            ),
        )
        return store.terminal(result)


async def _production_run() -> ReadResult:
    _ensure_run_directory(RUN_DIRECTORY)
    store = RunStore(RUN_STORE_PATH, _new_invocation_id())
    try:
        identity = onboarding.discover_public_identity()
    except onboarding.OnboardingViolation:
        identity = None
    transport = MainnetGetTransport()
    try:
        return await _run_gate(
            store=store,
            identity=identity,
            transport=transport,
            clock_ms=lambda: int(time.time() * 1000),
        )
    finally:
        await transport.close()


def _new_invocation_id() -> str:
    return "nado-mainnet-unsigned-read-" + secrets.token_hex(16)


async def run_fixture(
    *,
    store_path: Path | str,
    invocation_id: str,
    identity: Any,
    transport: Any,
    clock_ms: Callable[[], int] | None = None,
) -> ReadResult:
    """Synthetic seam for deterministic conformance tests; never production CLI."""

    store = RunStore(store_path, invocation_id)
    return await _run_gate(
        store=store,
        identity=identity,
        transport=transport,
        clock_ms=(lambda: int(time.time() * 1000)) if clock_ms is None else clock_ms,
    )


def main() -> int:
    if len(sys.argv) != 1:
        print(
            _json(
                {
                    "failure_class": "SAFETY",
                    "mainnet_write_authority": NO_MAINNET_WRITE_AUTHORITY,
                    "reason": "ARGUMENTS_FORBIDDEN",
                    "status": STATUS_BLOCKED,
                    "write_ready": False,
                }
            )
        )
        return 2
    try:
        result = asyncio.run(_production_run())
    except KeyboardInterrupt:
        print(
            _json(
                {
                    "failure_class": "SAFETY",
                    "mainnet_write_authority": NO_MAINNET_WRITE_AUTHORITY,
                    "reason": "READ_CANCELLED",
                    "status": STATUS_BLOCKED,
                    "write_ready": False,
                }
            )
        )
        return 1
    except GateFailure as failure:
        print(
            _json(
                {
                    "failure_class": failure.failure_class,
                    "mainnet_write_authority": NO_MAINNET_WRITE_AUTHORITY,
                    "reason": failure.reason,
                    "status": STATUS_BLOCKED,
                    "write_ready": False,
                }
            )
        )
        return 1
    except Exception:
        print(
            _json(
                {
                    "failure_class": "SAFETY",
                    "mainnet_write_authority": NO_MAINNET_WRITE_AUTHORITY,
                    "reason": "UNCLASSIFIED_FAILURE",
                    "status": STATUS_BLOCKED,
                    "write_ready": False,
                }
            )
        )
        return 1
    print(result.evidence())
    return 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "BLOCKED",
    "CONFIG_HASH",
    "EXPECTED_SUBACCOUNT",
    "EXPECTED_SUBACCOUNT_NAME",
    "EXPECTED_WALLET_ADDRESS",
    "FAILURE_CLASSES",
    "GetReply",
    "GetRequest",
    "GateFailure",
    "HTTP_METHOD",
    "MAINNET_GATEWAY_BASE_URL",
    "MAINNET_QUERY_URL",
    "MainnetGetTransport",
    "NO_MAINNET_WRITE_AUTHORITY",
    "PRIVATE_ONLY_BLOCKERS",
    "ReadResult",
    "RunStore",
    "StoreFailure",
    "TransportInterruption",
    "_decode_all_products",
    "_decode_contracts",
    "_decode_envelope",
    "_decode_isolated_positions",
    "_decode_subaccount_info",
    "_decode_subaccount_orders",
    "_new_invocation_id",
    "main",
    "run_fixture",
]
