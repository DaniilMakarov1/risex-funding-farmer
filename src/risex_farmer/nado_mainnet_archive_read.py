"""Nado mainnet public Archive read-only account-history gate.

This module is isolated from the paper product and from every private or
write-capable Nado path.  It binds all four documented Archive query bodies to
the one exact public wallet/default-subaccount identity and uses one fixed
mainnet Archive URL.  The only network method exposed by the transport is the
documented Archive ``POST`` query.

The implementation was checked against the current official Nado Archive
documentation and the official indexer SDK types/client:

* https://docs.nado.xyz/developer-resources/api/endpoints
* https://docs.nado.xyz/developer-resources/api/archive-indexer
* https://docs.nado.xyz/developer-resources/api/archive-indexer/orders
* https://docs.nado.xyz/developer-resources/api/archive-indexer/matches
* https://docs.nado.xyz/developer-resources/api/subscriptions/events
* https://raw.githubusercontent.com/nadohq/nado-typescript-sdk/refs/heads/main/packages/indexer-client/src/IndexerBaseClient.ts
* https://raw.githubusercontent.com/nadohq/nado-typescript-sdk/refs/heads/main/packages/indexer-client/src/IndexerClient.ts
* https://raw.githubusercontent.com/nadohq/nado-typescript-sdk/refs/heads/main/packages/indexer-client/src/types/serverModelTypes.ts

The result is always ``BLOCKED`` and always reports
``NO_MAINNET_WRITE_AUTHORITY``.  Archive evidence never becomes order
readiness.
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

import aiohttp

from . import nado_mainnet_onboarding as onboarding


VENUE = "Nado"
ENVIRONMENT = "MAINNET"
STATUS_BLOCKED = "BLOCKED"
BLOCKED = STATUS_BLOCKED
NO_MAINNET_WRITE_AUTHORITY = onboarding.NO_MAINNET_WRITE_AUTHORITY
NADO_MAINNET_CHAIN_ID = onboarding.NADO_MAINNET_CHAIN_ID

MAINNET_ARCHIVE_BASE_URL = "https://archive.prod.nado.xyz"
MAINNET_ARCHIVE_URL = MAINNET_ARCHIVE_BASE_URL + "/v1"
ARCHIVE_PATH = "/v1"
HTTP_METHOD = "POST"

EXPECTED_WALLET_ADDRESS = "0xf3c1b239f2978856839c3b676f22682c04500ac4"
EXPECTED_SUBACCOUNT_NAME = "default"
EXPECTED_SUBACCOUNT = (
    "0xf3c1b239f2978856839c3b676f22682c04500ac"
    "464656661756c740000000000"
)

# The accepted Nado public catalog contains product ids 0 through 93.  The
# official interest_and_funding query requires an explicit product_ids list;
# it has no documented "all products" sentinel.  Binding that observed
# catalog range into this source identity is safer than inventing an empty-list
# meaning or silently omitting a product.  A future catalog change therefore
# needs a fresh bounded review rather than implicit production drift.
ARCHIVE_FUNDING_PRODUCT_IDS = tuple(range(94))
ARCHIVE_FUNDING_PRODUCT_SCOPE = "ACCEPTED_CURRENT_CATALOG_0_TO_93"

REQUEST_TIMEOUT_SECONDS = 5.0
MAX_RESPONSE_BYTES = 1_048_576
ARCHIVE_PAGE_LIMIT = 100
ARCHIVE_REQUEST_LIMIT = ARCHIVE_PAGE_LIMIT + 1
MAX_ARCHIVE_PAGES = 64
MAX_ARCHIVE_ROWS = 10_000
SCHEMA_VERSION = 1

RUN_DIRECTORY = (
    Path.home() / ".config" / "risex-farmer" / "nado-mainnet-archive-read"
)
RUN_STORE_BASENAME = "runs-v1.sqlite3"
RUN_STORE_PATH = RUN_DIRECTORY / RUN_STORE_BASENAME
RUN_DIRECTORY_MODE = 0o700
RUN_STORE_MODE = 0o600

FAILURE_CLASSES = frozenset(
    {"TRANSPORT", "HTTP", "SCHEMA", "AUTH", "IDENTITY", "SAFETY"}
)

ARCHIVE_SURFACES = (
    "archive_order_history",
    "archive_match_history",
    "archive_event_history",
    "archive_funding_payments",
)

PERMANENT_BLOCKERS = (
    "ARCHIVE_READ_ONLY_NO_MAINNET_WRITE_AUTHORITY",
    "ARCHIVE_PRODUCT_SCOPE_FIXED_TO_ACCEPTED_CATALOG",
    "TRIGGER_ORDER_LIST_SIGNED_POST_ONLY",
    "PRIVATE_ORDER_FILL_POSITION_STREAM_SIGNED_AUTH_REQUIRED",
    "LINKED_SIGNER_PROVISIONING_FORBIDDEN",
    "ORDER_PAYLOAD_PREPARATION_FORBIDDEN",
    "ORDER_SIGNING_FORBIDDEN",
    "ORDER_DISPATCH_FORBIDDEN",
    "ALL_MAINNET_WRITES_FORBIDDEN",
)

ARCHIVE_QUERY_TYPES = {
    "orders": "archive_order_history",
    "matches": "archive_match_history",
    "events": "archive_event_history",
    "interest_and_funding": "archive_funding_payments",
}


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


def _bytes32(value: Any, field: str) -> str:
    if (
        type(value) is not str
        or len(value) != 66
        or not value.startswith("0x")
        or any(char not in "0123456789abcdefABCDEF" for char in value[2:])
    ):
        raise GateFailure(f"BYTES32_INVALID_{field}", "SCHEMA")
    return value.lower()


def _address(value: Any, field: str) -> str:
    if (
        type(value) is not str
        or len(value) != 42
        or not value.startswith("0x")
        or any(char not in "0123456789abcdefABCDEF" for char in value[2:])
    ):
        raise GateFailure(f"ADDRESS_INVALID_{field}", "SCHEMA")
    return value.lower()


def _bool(value: Any, field: str) -> bool:
    if type(value) is not bool:
        raise GateFailure(f"BOOL_INVALID_{field}", "SCHEMA")
    return value


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
        or exported.get("mainnet_write_authority") != NO_MAINNET_WRITE_AUTHORITY
        or exported.get("write_ready") is not False
    ):
        raise GateFailure("EXACT_SUBACCOUNT_IDENTITY_MISMATCH", "IDENTITY")
    identity_tag = hashlib.sha256(
        (EXPECTED_WALLET_ADDRESS + "\0" + EXPECTED_SUBACCOUNT).encode("ascii")
    ).hexdigest()[:16]
    return {
        "chain_id": NADO_MAINNET_CHAIN_ID,
        "environment": ENVIRONMENT,
        "identity_source": onboarding.NADO_PUBLIC_IDENTITY_SOURCE,
        "identity_tag": identity_tag,
        "mainnet_write_authority": NO_MAINNET_WRITE_AUTHORITY,
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
                "archive_url": MAINNET_ARCHIVE_URL,
                "environment": ENVIRONMENT,
                "funding_product_ids": ARCHIVE_FUNDING_PRODUCT_IDS,
                "identity": EXPECTED_SUBACCOUNT,
                "page_limit": ARCHIVE_PAGE_LIMIT,
                "schema_version": SCHEMA_VERSION,
                "surfaces": ARCHIVE_SURFACES,
                "venue": VENUE,
            }
        ).encode("utf-8")
    ).hexdigest()


CONFIG_HASH = _config_hash()


@dataclass(frozen=True)
class ArchiveRequest:
    """One closed-world unsigned public Archive query."""

    query_type: str
    body: Mapping[str, Any]
    attempt: int

    @property
    def method(self) -> str:
        return HTTP_METHOD

    @property
    def url(self) -> str:
        return MAINNET_ARCHIVE_URL

    def metadata(self, *, page: int, cursor: int | None) -> dict[str, Any]:
        return {
            "account_binding": "EXACT_SUBACCOUNT",
            "attempt": self.attempt,
            "body_digest": _digest(self.body),
            "body_keys": sorted(self.body.keys()),
            "method": HTTP_METHOD,
            "page": page,
            "path": ARCHIVE_PATH,
            "query_type": self.query_type,
            "cursor": cursor,
        }


@dataclass(frozen=True)
class ArchiveReply:
    status: int
    final_url: str
    body: Any
    complete: bool = True
    body_bytes: int | None = None


@dataclass(frozen=True)
class QueryEvidence:
    surface: str
    query_type: str
    page: int
    attempts: int
    observed_at_ms: int
    response_digest: str
    account_binding: str
    rows: int
    cursor_in: int | None
    cursor_out: int | None
    high_water: int | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "account_binding": self.account_binding,
            "attempts": self.attempts,
            "cursor_in": self.cursor_in,
            "cursor_out": self.cursor_out,
            "high_water": self.high_water,
            "method": HTTP_METHOD,
            "observed_at_ms": self.observed_at_ms,
            "page": self.page,
            "path": ARCHIVE_PATH,
            "request_type": self.query_type,
            "response_digest": self.response_digest,
            "rows": self.rows,
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
    history: Mapping[str, Any]
    funding: Mapping[str, Any]
    cross_agreement: Mapping[str, Any]
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
            "blockers": list(self.blockers),
            "config_hash": self.config_hash,
            "counters": dict(self.counters),
            "cross_agreement": dict(self.cross_agreement),
            "failure_class": self.failure_class,
            "funding": dict(self.funding),
            "history": dict(self.history),
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


def _empty_funding(*, partial_rows: int = 0) -> dict[str, Any]:
    return {
        "account_attribution": "UNKNOWN",
        "amount_digest": None,
        "funding_payment_count": partial_rows,
        "funding_product_scope": ARCHIVE_FUNDING_PRODUCT_SCOPE,
        "high_water_idx": None,
        "historical_payments": "UNKNOWN",
        "interest_payment_count": 0,
        "negative_count": 0,
        "pages": 0,
        "payment_status": "UNKNOWN",
        "positive_count": 0,
        "reason": "ARCHIVE_FUNDING_UNRESOLVED",
        "zero_count": 0,
    }


def _empty_history() -> dict[str, Any]:
    return {
        "events": {
            "count": 0,
            "high_water_submission_idx": None,
            "pages": 0,
            "status": "UNKNOWN",
        },
        "matches": {
            "count": 0,
            "high_water_submission_idx": None,
            "pages": 0,
            "status": "UNKNOWN",
        },
        "orders": {
            "count": 0,
            "high_water_submission_idx": None,
            "pages": 0,
            "status": "UNKNOWN",
        },
    }


def _empty_cross_agreement() -> dict[str, Any]:
    return {
        "event_submission_count": 0,
        "event_match_submission_count": 0,
        "match_digest_count": 0,
        "order_digest_count": 0,
        "status": "UNKNOWN",
    }


def _empty_unrelated_state() -> dict[str, Any]:
    return {
        "identity_bound_records_rejected": 0,
        "status": "ONLY_EXACT_SUBACCOUNT_SURFACES_CHECKED",
    }


def _surface_inventory() -> tuple[dict[str, Any], ...]:
    return (
        {
            "authentication": "NONE",
            "method": "POST",
            "path": ARCHIVE_PATH,
            "status": "IMPLEMENTED_READ_ONLY",
            "surface": "archive_order_history",
        },
        {
            "authentication": "NONE",
            "method": "POST",
            "path": ARCHIVE_PATH,
            "status": "IMPLEMENTED_READ_ONLY",
            "surface": "archive_match_history",
        },
        {
            "authentication": "NONE",
            "method": "POST",
            "path": ARCHIVE_PATH,
            "status": "IMPLEMENTED_READ_ONLY",
            "surface": "archive_event_history",
        },
        {
            "authentication": "NONE",
            "method": "POST",
            "path": ARCHIVE_PATH,
            "status": "IMPLEMENTED_READ_ONLY",
            "surface": "archive_funding_payments",
        },
        {
            "authentication": "SIGNED",
            "method": "POST",
            "reason": "TRIGGER_ORDER_LIST_SIGNED_POST_ONLY",
            "status": "BLOCKED",
            "surface": "trigger_order_list",
        },
        {
            "authentication": "SIGNED_AUTH",
            "method": "WEBSOCKET",
            "reason": "PRIVATE_ORDER_FILL_POSITION_STREAM_SIGNED_AUTH_REQUIRED",
            "status": "BLOCKED",
            "surface": "private_order_fill_position_stream",
        },
        {
            "authentication": "SIGNED",
            "method": "POST",
            "reason": "LINKED_SIGNER_PROVISIONING_FORBIDDEN",
            "status": "BLOCKED",
            "surface": "linked_signer_provisioning",
        },
        {
            "authentication": "SIGNED",
            "method": "POST",
            "reason": "ORDER_PAYLOAD_PREPARATION_FORBIDDEN",
            "status": "BLOCKED",
            "surface": "order_payload_preparation",
        },
        {
            "authentication": "SIGNED",
            "method": "POST",
            "reason": "ORDER_SIGNING_FORBIDDEN",
            "status": "BLOCKED",
            "surface": "order_signing",
        },
        {
            "authentication": "SIGNED",
            "method": "POST",
            "reason": "ORDER_DISPATCH_FORBIDDEN",
            "status": "BLOCKED",
            "surface": "order_dispatch",
        },
        {
            "authentication": "NONE",
            "method": "POST",
            "reason": "ALL_MAINNET_WRITES_FORBIDDEN",
            "status": "BLOCKED",
            "surface": "all_mainnet_writes",
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
    history: Mapping[str, Any] | None = None,
    funding: Mapping[str, Any] | None = None,
    cross_agreement: Mapping[str, Any] | None = None,
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
        dict(_empty_history() if history is None else history),
        dict(_empty_funding() if funding is None else funding),
        dict(_empty_cross_agreement() if cross_agreement is None else cross_agreement),
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

    _TABLE = "nado_mainnet_archive_read_runs"
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
                blockers=("INTERRUPTED_RUNNING_INVOCATION", *PERMANENT_BLOCKERS),
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
            result.history,
            result.funding,
            result.cross_agreement,
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
        "blockers",
        "config_hash",
        "counters",
        "cross_agreement",
        "failure_class",
        "funding",
        "history",
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
        or type(value["history"]) is not dict
        or type(value["funding"]) is not dict
        or type(value["cross_agreement"]) is not dict
        or type(value["unrelated_state"]) is not dict
        or type(value["identity"]) is not dict
        and value["identity"] is not None
        or value["failure_class"] is not None
        and value["failure_class"] not in FAILURE_CLASSES
        or type(value["reason"]) is not str
    ):
        raise StoreFailure("DURABLE_EVIDENCE_INVALID", "SAFETY")
    counters = _decode_counters(value["counters"])
    identity = value["identity"]
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
        value["history"],
        value["funding"],
        value["cross_agreement"],
        tuple(value["blockers"]),
        value["unrelated_state"],
        tuple(dict(item) for item in value["surface_inventory"]),
        value["read_complete"],
    )


def _validate_identity_fields(value: Mapping[str, Any], field_prefix: str) -> None:
    for key in ("subaccount", "sender", "owner"):
        if key in value:
            candidate = _bytes32(value[key], f"{field_prefix}_{key.upper()}")
            if candidate != EXPECTED_SUBACCOUNT:
                raise GateFailure(f"EXACT_SUBACCOUNT_MISMATCH_{field_prefix}", "IDENTITY")
    for key in ("wallet", "wallet_address"):
        if key in value:
            candidate = _address(value[key], f"{field_prefix}_{key.upper()}")
            if candidate != EXPECTED_WALLET_ADDRESS:
                raise GateFailure(f"EXACT_WALLET_MISMATCH_{field_prefix}", "IDENTITY")


def _parse_order(row: Any, index: int) -> dict[str, Any]:
    field = f"ORDER_{index}"
    if not isinstance(row, Mapping):
        raise GateFailure(f"ROW_INVALID_{field}", "SCHEMA")
    _validate_identity_fields(row, field)
    digest = _bytes32(_required(row, "digest", f"{field}_DIGEST"), f"{field}_DIGEST")
    subaccount = _bytes32(
        _required(row, "subaccount", f"{field}_SUBACCOUNT"),
        f"{field}_SUBACCOUNT",
    )
    if subaccount != EXPECTED_SUBACCOUNT:
        raise GateFailure("EXACT_SUBACCOUNT_MISMATCH_ORDER", "IDENTITY")
    product_id = _uint(
        _required(row, "product_id", f"{field}_PRODUCT_ID"),
        f"{field}_PRODUCT_ID",
    )
    submission_idx = _uint_text(
        _required(row, "submission_idx", f"{field}_SUBMISSION_IDX"),
        f"{field}_SUBMISSION_IDX",
    )
    last_fill_submission_idx = _uint_text(
        _required(row, "last_fill_submission_idx", f"{field}_LAST_FILL_IDX"),
        f"{field}_LAST_FILL_IDX",
    )
    if int(last_fill_submission_idx) < int(submission_idx):
        raise GateFailure("ORDER_FILL_RANGE_INVALID", "SCHEMA")
    isolated = _bool(_required(row, "isolated", f"{field}_ISOLATED"), f"{field}_ISOLATED")
    for key in (
        "amount",
        "price_x18",
        "base_filled",
        "quote_filled",
        "fee",
        "builder_fee",
        "realized_pnl",
        "closed_amount",
        "closed_net_entry",
        "closed_margin",
    ):
        if key in row:
            _int_text(row[key], f"{field}_{key.upper()}")
    for key in ("expiration", "nonce", "first_fill_timestamp", "last_fill_timestamp"):
        if key in row:
            _uint_text(row[key], f"{field}_{key.upper()}")
    if "appendix" in row:
        _uint(row["appendix"], f"{field}_APPENDIX", maximum=2**32 - 1)
    return {
        "digest": digest,
        "isolated": isolated,
        "last_fill_submission_idx": int(last_fill_submission_idx),
        "product_id": product_id,
        "submission_idx": int(submission_idx),
        "subaccount": subaccount,
    }


def _extract_match_product_id(row: Mapping[str, Any], field: str) -> int:
    balance = _required(row, "pre_balance", f"{field}_PRE_BALANCE")
    if not isinstance(balance, Mapping):
        raise GateFailure(f"BALANCE_INVALID_{field}", "SCHEMA")
    base = _required(balance, "base", f"{field}_BASE_BALANCE")
    if not isinstance(base, Mapping):
        raise GateFailure(f"BASE_BALANCE_INVALID_{field}", "SCHEMA")
    market_keys = [key for key in ("perp", "spot") if key in base]
    if len(market_keys) != 1 or not isinstance(base[market_keys[0]], Mapping):
        raise GateFailure(f"BASE_MARKET_INVALID_{field}", "SCHEMA")
    product_id = _uint(
        _required(base[market_keys[0]], "product_id", f"{field}_PRODUCT_ID"),
        f"{field}_PRODUCT_ID",
    )
    if "product_id" in row and _uint(row["product_id"], f"{field}_TOP_PRODUCT_ID") != product_id:
        raise GateFailure("MATCH_PRODUCT_CONTRADICTION", "SCHEMA")
    return product_id


def _parse_match(row: Any, index: int) -> dict[str, Any]:
    field = f"MATCH_{index}"
    if not isinstance(row, Mapping):
        raise GateFailure(f"ROW_INVALID_{field}", "SCHEMA")
    _validate_identity_fields(row, field)
    digest = _bytes32(_required(row, "digest", f"{field}_DIGEST"), f"{field}_DIGEST")
    submission_idx = _uint_text(
        _required(row, "submission_idx", f"{field}_SUBMISSION_IDX"),
        f"{field}_SUBMISSION_IDX",
    )
    nested_order = _required(row, "order", f"{field}_ORDER")
    if not isinstance(nested_order, Mapping):
        raise GateFailure(f"ORDER_INVALID_{field}", "SCHEMA")
    _validate_identity_fields(nested_order, f"{field}_ORDER")
    sender = _bytes32(
        _required(nested_order, "sender", f"{field}_ORDER_SENDER"),
        f"{field}_ORDER_SENDER",
    )
    if sender != EXPECTED_SUBACCOUNT:
        raise GateFailure("EXACT_SUBACCOUNT_MISMATCH_MATCH", "IDENTITY")
    if "digest" in nested_order:
        nested_digest = _bytes32(nested_order["digest"], f"{field}_ORDER_DIGEST")
        if nested_digest != digest:
            raise GateFailure("MATCH_ORDER_DIGEST_CONTRADICTION", "SCHEMA")
    product_id = _extract_match_product_id(row, field)
    for key in ("base_filled", "quote_filled", "base_fee", "quote_fee", "price"):
        if key in row:
            _int_text(row[key], f"{field}_{key.upper()}")
    if "is_taker" in row:
        _bool(row["is_taker"], f"{field}_IS_TAKER")
    return {
        "digest": digest,
        "product_id": product_id,
        "sender": sender,
        "submission_idx": int(submission_idx),
    }


def _parse_tx(row: Any, index: int, kind: str) -> dict[str, Any]:
    field = f"{kind.upper()}_TX_{index}"
    if not isinstance(row, Mapping):
        raise GateFailure(f"ROW_INVALID_{field}", "SCHEMA")
    submission_idx = _uint_text(
        _required(row, "submission_idx", f"{field}_SUBMISSION_IDX"),
        f"{field}_SUBMISSION_IDX",
    )
    if "timestamp" in row:
        timestamp = _uint_text(row["timestamp"], f"{field}_TIMESTAMP")
        if int(timestamp) <= 0:
            raise GateFailure(f"TIMESTAMP_INVALID_{field}", "SCHEMA")
    if "tx" in row:
        tx = row["tx"]
        if not isinstance(tx, Mapping):
            raise GateFailure(f"TX_BODY_INVALID_{field}", "SCHEMA")
        _validate_identity_fields(tx, field)
    _validate_identity_fields(row, field)
    return {"submission_idx": int(submission_idx)}


def _parse_event(row: Any, index: int) -> dict[str, Any]:
    field = f"EVENT_{index}"
    if not isinstance(row, Mapping):
        raise GateFailure(f"ROW_INVALID_{field}", "SCHEMA")
    _validate_identity_fields(row, field)
    subaccount = _bytes32(
        _required(row, "subaccount", f"{field}_SUBACCOUNT"),
        f"{field}_SUBACCOUNT",
    )
    if subaccount != EXPECTED_SUBACCOUNT:
        raise GateFailure("EXACT_SUBACCOUNT_MISMATCH_EVENT", "IDENTITY")
    submission_idx = _uint_text(
        _required(row, "submission_idx", f"{field}_SUBMISSION_IDX"),
        f"{field}_SUBMISSION_IDX",
    )
    event_type = _text(
        _required(row, "event_type", f"{field}_EVENT_TYPE"),
        f"{field}_EVENT_TYPE",
    )
    product_id = _uint(
        _required(row, "product_id", f"{field}_PRODUCT_ID"),
        f"{field}_PRODUCT_ID",
    )
    if "isolated" in row:
        _bool(row["isolated"], f"{field}_ISOLATED")
    if "isolated_product_id" in row and row["isolated_product_id"] is not None:
        _uint(row["isolated_product_id"], f"{field}_ISOLATED_PRODUCT_ID")
    return {
        "event_type": event_type,
        "product_id": product_id,
        "subaccount": subaccount,
        "submission_idx": int(submission_idx),
    }


def _parse_payment(row: Any, index: int, kind: str) -> dict[str, Any]:
    field = f"{kind.upper()}_{index}"
    if not isinstance(row, Mapping):
        raise GateFailure(f"ROW_INVALID_{field}", "SCHEMA")
    required = (
        "product_id",
        "idx",
        "timestamp",
        "amount",
        "balance_amount",
        "rate_x18",
        "oracle_price_x18",
    )
    for key in required:
        _required(row, key, f"{field}_{key.upper()}")
    product_id = row["product_id"]
    if type(product_id) is not int or isinstance(product_id, bool):
        raise GateFailure(f"PRODUCT_ID_INVALID_{field}", "SCHEMA")
    if product_id not in ARCHIVE_FUNDING_PRODUCT_IDS:
        raise GateFailure("FUNDING_PRODUCT_SCOPE_MISMATCH", "SCHEMA")
    idx = _uint_text(row["idx"], f"{field}_IDX")
    timestamp = _uint_text(row["timestamp"], f"{field}_TIMESTAMP")
    if int(timestamp) <= 0:
        raise GateFailure(f"TIMESTAMP_INVALID_{field}", "SCHEMA")
    amount = _int_text(row["amount"], f"{field}_AMOUNT")
    balance_amount = _int_text(row["balance_amount"], f"{field}_BALANCE_AMOUNT")
    rate_x18 = _int_text(row["rate_x18"], f"{field}_RATE_X18")
    oracle_price_x18 = _int_text(row["oracle_price_x18"], f"{field}_ORACLE_PRICE_X18")
    if int(oracle_price_x18) <= 0:
        raise GateFailure(f"ORACLE_PRICE_INVALID_{field}", "SCHEMA")
    _validate_identity_fields(row, field)
    if "isolated" in row:
        _bool(row["isolated"], f"{field}_ISOLATED")
    if "isolated_product_id" in row and row["isolated_product_id"] is not None:
        _uint(row["isolated_product_id"], f"{field}_ISOLATED_PRODUCT_ID")
    return {
        "amount": amount,
        "idx": int(idx),
        "kind": kind,
        "product_id": product_id,
    }


def _archive_payload(body: Any) -> Mapping[str, Any]:
    if not isinstance(body, Mapping):
        raise GateFailure("ARCHIVE_RESPONSE_SCHEMA_INVALID", "SCHEMA")
    if body.get("status") in {"failure", "error"}:
        raise GateFailure("ARCHIVE_QUERY_FAILURE", "HTTP")
    if "error" in body and body["error"] not in (None, ""):
        raise GateFailure("ARCHIVE_QUERY_FAILURE", "HTTP")
    return body


def _decoded_high_water(query_type: str, decoded: Any) -> int | None:
    if query_type == "orders":
        rows = decoded
        return None if not rows else rows[0]["submission_idx"]
    if query_type in {"matches", "events"}:
        rows = decoded.txs
        return None if not rows else rows[0]["submission_idx"]
    rows = (*decoded.interest, *decoded.funding)
    return None if not rows else max(row["idx"] for row in rows)


@dataclass(frozen=True)
class _RecordsPage:
    records: tuple[dict[str, Any], ...]
    txs: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class _FundingPage:
    interest: tuple[dict[str, Any], ...]
    funding: tuple[dict[str, Any], ...]
    next_idx: int | None


def _decode_orders(body: Any) -> tuple[dict[str, Any], ...]:
    payload = _archive_payload(body)
    rows = _required(payload, "orders", "ORDERS")
    if type(rows) is not list or len(rows) > ARCHIVE_REQUEST_LIMIT:
        raise GateFailure("ORDERS_PAGE_UNBOUNDED", "SCHEMA")
    parsed = tuple(_parse_order(row, index) for index, row in enumerate(rows))
    if len({row["digest"] for row in parsed}) != len(parsed):
        raise GateFailure("ORDER_DIGEST_REPEATED", "SCHEMA")
    return parsed


def _decode_matches(body: Any) -> _RecordsPage:
    payload = _archive_payload(body)
    raw_matches = _required(payload, "matches", "MATCHES")
    raw_txs = _required(payload, "txs", "MATCH_TXS")
    if (
        type(raw_matches) is not list
        or type(raw_txs) is not list
        or len(raw_matches) > MAX_ARCHIVE_ROWS
        or len(raw_txs) > ARCHIVE_REQUEST_LIMIT
    ):
        raise GateFailure("MATCHES_PAGE_UNBOUNDED", "SCHEMA")
    matches = tuple(_parse_match(row, index) for index, row in enumerate(raw_matches))
    txs = tuple(_parse_tx(row, index, "match") for index, row in enumerate(raw_txs))
    if len({row["submission_idx"] for row in txs}) != len(txs):
        raise GateFailure("MATCH_TX_INDEX_REPEATED", "SCHEMA")
    if {row["submission_idx"] for row in matches} != {
        row["submission_idx"] for row in txs
    }:
        raise GateFailure("MATCH_TX_AGREEMENT_INVALID", "SCHEMA")
    if len({(row["digest"], row["submission_idx"]) for row in matches}) != len(matches):
        raise GateFailure("MATCH_ROW_REPEATED", "SCHEMA")
    return _RecordsPage(matches, txs)


def _decode_events(body: Any) -> _RecordsPage:
    payload = _archive_payload(body)
    raw_events = _required(payload, "events", "EVENTS")
    raw_txs = _required(payload, "txs", "EVENT_TXS")
    if (
        type(raw_events) is not list
        or type(raw_txs) is not list
        or len(raw_events) > MAX_ARCHIVE_ROWS
        or len(raw_txs) > ARCHIVE_REQUEST_LIMIT
    ):
        raise GateFailure("EVENTS_PAGE_UNBOUNDED", "SCHEMA")
    events = tuple(_parse_event(row, index) for index, row in enumerate(raw_events))
    txs = tuple(_parse_tx(row, index, "event") for index, row in enumerate(raw_txs))
    if len({row["submission_idx"] for row in txs}) != len(txs):
        raise GateFailure("EVENT_TX_INDEX_REPEATED", "SCHEMA")
    if {row["submission_idx"] for row in events} != {
        row["submission_idx"] for row in txs
    }:
        raise GateFailure("EVENT_TX_AGREEMENT_INVALID", "SCHEMA")
    event_keys = {
        (row["submission_idx"], row["event_type"], row["product_id"])
        for row in events
    }
    if len(event_keys) != len(events):
        raise GateFailure("EVENT_ROW_REPEATED", "SCHEMA")
    return _RecordsPage(events, txs)


def _decode_funding(body: Any) -> _FundingPage:
    payload = _archive_payload(body)
    raw_interest = _required(payload, "interest_payments", "INTEREST_PAYMENTS")
    raw_funding = _required(payload, "funding_payments", "FUNDING_PAYMENTS")
    raw_next = _required(payload, "next_idx", "FUNDING_NEXT_IDX")
    if (
        type(raw_interest) is not list
        or type(raw_funding) is not list
        or len(raw_interest) > ARCHIVE_PAGE_LIMIT
        or len(raw_funding) > ARCHIVE_PAGE_LIMIT
    ):
        raise GateFailure("FUNDING_PAGE_UNBOUNDED", "SCHEMA")
    interest = tuple(
        _parse_payment(row, index, "interest")
        for index, row in enumerate(raw_interest)
    )
    funding = tuple(
        _parse_payment(row, index, "funding")
        for index, row in enumerate(raw_funding)
    )
    if raw_next is None:
        next_idx = None
    else:
        next_idx = int(_uint_text(raw_next, "FUNDING_NEXT_IDX"))
    for kind, rows in (("interest", interest), ("funding", funding)):
        previous: int | None = None
        seen: set[tuple[int, int]] = set()
        for row in rows:
            idx = row["idx"]
            if previous is not None and idx > previous:
                raise GateFailure(f"FUNDING_PAGE_REORDERED_{kind.upper()}", "SCHEMA")
            previous = idx
            key = (row["product_id"], idx)
            if key in seen:
                raise GateFailure(f"FUNDING_ROW_REPEATED_{kind.upper()}", "SCHEMA")
            seen.add(key)
    return _FundingPage(interest, funding, next_idx)


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


def _coerce_reply(value: Any) -> ArchiveReply:
    if isinstance(value, ArchiveReply):
        reply = value
    elif isinstance(value, Mapping):
        required = {"status", "final_url", "body"}
        if not required.issubset(value):
            raise GateFailure("TRANSPORT_REPLY_SCHEMA_INVALID", "SCHEMA")
        reply = ArchiveReply(
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
    if reply.status != 200:
        return ArchiveReply(reply.status, reply.final_url, None, True, reply.body_bytes)
    body = _strict_json_value(reply.body)
    encoded_size = len(_json(body).encode("utf-8"))
    if encoded_size > MAX_RESPONSE_BYTES:
        raise GateFailure("RESPONSE_TOO_LARGE", "SCHEMA")
    return ArchiveReply(reply.status, reply.final_url, body, True, encoded_size)


async def _call(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _expected_body(query_type: str, cursor: int | None) -> dict[str, Any]:
    if query_type == "orders":
        params: dict[str, Any] = {
            "subaccounts": [EXPECTED_SUBACCOUNT],
            "limit": ARCHIVE_REQUEST_LIMIT,
        }
        if cursor is not None:
            params["idx"] = str(cursor)
    elif query_type == "matches":
        params = {
            "subaccounts": [EXPECTED_SUBACCOUNT],
            "limit": ARCHIVE_REQUEST_LIMIT,
        }
        if cursor is not None:
            params["idx"] = str(cursor)
    elif query_type == "events":
        params = {
            "subaccounts": [EXPECTED_SUBACCOUNT],
            "desc": True,
            "limit": {"txs": ARCHIVE_REQUEST_LIMIT},
        }
        if cursor is not None:
            params["idx"] = str(cursor)
    elif query_type == "interest_and_funding":
        params = {
            "subaccount": EXPECTED_SUBACCOUNT,
            "product_ids": list(ARCHIVE_FUNDING_PRODUCT_IDS),
            "limit": ARCHIVE_PAGE_LIMIT,
        }
        if cursor is not None:
            params["max_idx"] = str(cursor)
    else:
        raise GateFailure("ARCHIVE_QUERY_TYPE_INVALID", "SAFETY")
    return {query_type: params}


def _validate_archive_request(request: ArchiveRequest) -> None:
    if (
        not isinstance(request, ArchiveRequest)
        or request.method != HTTP_METHOD
        or request.url != MAINNET_ARCHIVE_URL
        or type(request.attempt) is not int
        or request.attempt not in {1, 2}
    ):
        raise GateFailure("ARCHIVE_TRANSPORT_REQUEST_INVALID", "SAFETY")
    if request.query_type not in ARCHIVE_QUERY_TYPES or type(request.body) is not dict:
        raise GateFailure("ARCHIVE_TRANSPORT_REQUEST_INVALID", "SAFETY")
    if set(request.body) != {request.query_type}:
        raise GateFailure("ARCHIVE_BODY_NOT_CLOSED", "SAFETY")
    params = request.body[request.query_type]
    if not isinstance(params, Mapping):
        raise GateFailure("ARCHIVE_BODY_NOT_CLOSED", "SAFETY")
    for forbidden in (
        "txns",
        "pre_state",
        "signature",
        "authorization",
        "private_key",
        "cancel",
        "transfer",
        "withdrawal",
        "execute",
        "trigger",
    ):
        if forbidden in request.body or forbidden in params:
            raise GateFailure("ARCHIVE_BODY_UNSAFE_FIELD", "SAFETY")
    cursor: int | None = None
    if request.query_type in {"orders", "matches", "events"}:
        expected_keys = {"subaccounts", "limit"}
        if request.query_type == "events":
            expected_keys.add("desc")
        if "idx" in params:
            expected_keys.add("idx")
            cursor = _uint_text(params["idx"], "ARCHIVE_IDX") and int(params["idx"])
        if set(params) != expected_keys:
            raise GateFailure("ARCHIVE_BODY_NOT_CLOSED", "SAFETY")
        if params.get("subaccounts") != [EXPECTED_SUBACCOUNT]:
            raise GateFailure("ARCHIVE_SUBACCOUNT_BINDING_INVALID", "IDENTITY")
        if params.get("limit") != ARCHIVE_REQUEST_LIMIT:
            raise GateFailure("ARCHIVE_LIMIT_INVALID", "SAFETY")
        if request.query_type == "events" and params.get("desc") is not True:
            raise GateFailure("ARCHIVE_EVENT_ORDER_INVALID", "SAFETY")
    else:
        expected_keys = {"subaccount", "product_ids", "limit"}
        if "max_idx" in params:
            expected_keys.add("max_idx")
            cursor = _uint_text(params["max_idx"], "ARCHIVE_MAX_IDX") and int(params["max_idx"])
        if set(params) != expected_keys:
            raise GateFailure("ARCHIVE_BODY_NOT_CLOSED", "SAFETY")
        if params.get("subaccount") != EXPECTED_SUBACCOUNT:
            raise GateFailure("ARCHIVE_SUBACCOUNT_BINDING_INVALID", "IDENTITY")
        if params.get("product_ids") != list(ARCHIVE_FUNDING_PRODUCT_IDS):
            raise GateFailure("ARCHIVE_PRODUCT_SCOPE_INVALID", "SAFETY")
        if params.get("limit") != ARCHIVE_PAGE_LIMIT:
            raise GateFailure("ARCHIVE_LIMIT_INVALID", "SAFETY")
    expected = _expected_body(request.query_type, cursor)
    if dict(request.body) != expected:
        raise GateFailure("ARCHIVE_BODY_NOT_CLOSED", "SAFETY")


class MainnetArchiveTransport:
    """Direct TLS transport for the fixed public Archive query URL."""

    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None

    async def _session_for_post(self) -> aiohttp.ClientSession:
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

    async def post(self, request: ArchiveRequest) -> ArchiveReply:
        _validate_archive_request(request)
        session = await self._session_for_post()
        try:
            async with session.post(
                MAINNET_ARCHIVE_URL,
                json=request.body,
                allow_redirects=False,
            ) as response:
                if response.status != 200:
                    return ArchiveReply(
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
                return ArchiveReply(response.status, str(response.url), body, True, size)
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


def _failure_query_evidence(
    *,
    surface: str,
    query_type: str,
    page: int,
    account_binding: str,
    failure: GateFailure,
    counters: Mapping[str, int],
    cursor_in: int | None,
) -> dict[str, Any]:
    return {
        "account_binding": account_binding,
        "attempts": counters.get(surface + "_attempts", 0),
        "cursor_in": cursor_in,
        "cursor_out": None,
        "failure_class": failure.failure_class,
        "high_water": None,
        "method": HTTP_METHOD,
        "page": page,
        "path": ARCHIVE_PATH,
        "reason": failure.reason,
        "request_type": query_type,
        "rows": 0,
        "status": "BLOCKED",
        "surface": surface,
    }


async def _archive_query(
    *,
    store: RunStore,
    transport: Any,
    query_type: str,
    page: int,
    cursor: int | None,
    decoder: Callable[[Any], Any],
    row_count: Callable[[Any], int],
    queries: list[Mapping[str, Any]],
    observation_clock: _ObservationClock,
) -> tuple[Any, QueryEvidence]:
    surface = f"{ARCHIVE_QUERY_TYPES[query_type]}_page_{page}"
    body = _expected_body(query_type, cursor)
    for attempt in (1, 2):
        store.attempt(surface, retry=attempt == 2)
        request = ArchiveRequest(query_type, body, attempt)
        try:
            poster = getattr(transport, "post", None)
            if not callable(poster):
                raise GateFailure("ARCHIVE_TRANSPORT_METHOD_MISSING", "SAFETY")
            reply = _coerce_reply(await _call(poster(request)))
            if reply.final_url != request.url:
                raise GateFailure("REDIRECT_FORBIDDEN", "SAFETY")
            if 300 <= reply.status < 400:
                raise GateFailure("REDIRECT_FORBIDDEN", "SAFETY")
            if reply.status in {401, 403}:
                raise GateFailure("ARCHIVE_QUERY_AUTH_REJECTED", "AUTH")
            if reply.status != 200:
                raise GateFailure("HTTP_STATUS_UNACCEPTED", "HTTP")
            decoded = decoder(reply.body)
            observed_at_ms = observation_clock.now()
            store.complete(surface)
            attempts = store.counters()[surface + "_attempts"]
            evidence = QueryEvidence(
                surface=surface,
                query_type=query_type,
                page=page,
                attempts=attempts,
                observed_at_ms=observed_at_ms,
                response_digest=_digest(reply.body),
                account_binding="EXACT_SUBACCOUNT",
                rows=row_count(decoded),
                cursor_in=cursor,
                cursor_out=None,
                high_water=_decoded_high_water(query_type, decoded),
            )
            return decoded, evidence
        except asyncio.CancelledError:
            raise
        except TransportInterruption:
            if attempt == 1:
                continue
            failure = GateFailure("TRANSPORT_RETRY_EXHAUSTED", "TRANSPORT")
            queries.append(
                _failure_query_evidence(
                    surface=surface,
                    query_type=query_type,
                    page=page,
                    account_binding="EXACT_SUBACCOUNT",
                    failure=failure,
                    counters=store.counters(),
                    cursor_in=cursor,
                )
            )
            raise failure
        except GateFailure as failure:
            queries.append(
                _failure_query_evidence(
                    surface=surface,
                    query_type=query_type,
                    page=page,
                    account_binding="EXACT_SUBACCOUNT",
                    failure=failure,
                    counters=store.counters(),
                    cursor_in=cursor,
                )
            )
            raise
        except BaseException as exc:
            if _is_transport_failure(exc):
                if attempt == 1:
                    continue
                failure = GateFailure("TRANSPORT_RETRY_EXHAUSTED", "TRANSPORT")
                queries.append(
                    _failure_query_evidence(
                        surface=surface,
                        query_type=query_type,
                        page=page,
                        account_binding="EXACT_SUBACCOUNT",
                        failure=failure,
                        counters=store.counters(),
                        cursor_in=cursor,
                    )
                )
                raise failure
            failure = GateFailure("UNCLASSIFIED_FAILURE", "SAFETY")
            queries.append(
                _failure_query_evidence(
                    surface=surface,
                    query_type=query_type,
                    page=page,
                    account_binding="EXACT_SUBACCOUNT",
                    failure=failure,
                    counters=store.counters(),
                    cursor_in=cursor,
                )
            )
            raise failure
    raise GateFailure("TRANSPORT_RETRY_EXHAUSTED", "TRANSPORT")


def _validate_desc(rows: Sequence[Mapping[str, Any]], field: str) -> None:
    previous: int | None = None
    for row in rows:
        value = row[field]
        if previous is not None and value > previous:
            raise GateFailure("ARCHIVE_PAGE_REORDERED", "SCHEMA")
        previous = value


def _page_rows(
    *,
    raw_rows: Sequence[dict[str, Any]],
    cursor: int | None,
    boundary: tuple[Any, dict[str, Any]] | None,
    seen: dict[Any, dict[str, Any]],
    key: Callable[[dict[str, Any]], Any],
    index: Callable[[dict[str, Any]], int],
) -> tuple[tuple[dict[str, Any], ...], int | None, tuple[Any, dict[str, Any]] | None]:
    if len(raw_rows) > ARCHIVE_REQUEST_LIMIT:
        raise GateFailure("ARCHIVE_PAGE_UNBOUNDED", "SCHEMA")
    all_rows = list(raw_rows)
    _validate_desc(all_rows, "submission_idx")
    next_cursor = (
        index(all_rows[ARCHIVE_PAGE_LIMIT])
        if len(all_rows) > ARCHIVE_PAGE_LIMIT
        else None
    )
    next_boundary = (
        (key(all_rows[ARCHIVE_PAGE_LIMIT]), all_rows[ARCHIVE_PAGE_LIMIT])
        if len(all_rows) > ARCHIVE_PAGE_LIMIT
        else None
    )
    # The SDK's requestedLimit+1 convention makes the first omitted row the
    # next cursor. Keep only the first requestedLimit rows even when the
    # server repeats the prior boundary inclusively.
    rows = all_rows[:ARCHIVE_PAGE_LIMIT]
    if boundary is not None and rows and key(rows[0]) == boundary[0]:
        if rows[0] != boundary[1]:
            raise GateFailure("ARCHIVE_PAGE_REPLAY_CONTRADICTORY", "SCHEMA")
        rows.pop(0)
        boundary = None
    if cursor is not None and any(index(row) >= cursor for row in rows):
        raise GateFailure("ARCHIVE_CURSOR_NOT_RESPECTED", "SCHEMA")
    for row in rows:
        row_key = key(row)
        if row_key in seen:
            raise GateFailure("ARCHIVE_PAGE_REPEATED", "SCHEMA")
        seen[row_key] = row
    if next_cursor is not None:
        if cursor is not None and next_cursor >= cursor:
            raise GateFailure("ARCHIVE_CURSOR_NOT_ADVANCING", "SCHEMA")
        if next_cursor < 0:
            raise GateFailure("ARCHIVE_CURSOR_INVALID", "SCHEMA")
    return tuple(rows), next_cursor, next_boundary


async def _read_orders(
    *,
    store: RunStore,
    transport: Any,
    queries: list[Mapping[str, Any]],
    observation_clock: _ObservationClock,
) -> tuple[tuple[dict[str, Any], ...], dict[str, Any]]:
    cursor: int | None = None
    boundary: tuple[Any, dict[str, Any]] | None = None
    seen: dict[Any, dict[str, Any]] = {}
    result: list[dict[str, Any]] = []
    pages = 0
    high_water: int | None = None
    while pages < MAX_ARCHIVE_PAGES:
        page, evidence = await _archive_query(
            store=store,
            transport=transport,
            query_type="orders",
            page=pages,
            cursor=cursor,
            decoder=_decode_orders,
            row_count=len,
            queries=queries,
            observation_clock=observation_clock,
        )
        visible, next_cursor, next_boundary = _page_rows(
            raw_rows=page,
            cursor=cursor,
            boundary=boundary,
            seen=seen,
            key=lambda row: row["digest"],
            index=lambda row: row["submission_idx"],
        )
        page_high_water = None if not page else page[0]["submission_idx"]
        if page_high_water is not None:
            if high_water is None:
                high_water = page_high_water
            elif page_high_water > high_water:
                raise GateFailure("ARCHIVE_HIGH_WATER_MOVED", "SCHEMA")
        evidence_dict = evidence.as_dict()
        evidence_dict["cursor_out"] = next_cursor
        queries.append(evidence_dict)
        result.extend(visible)
        pages += 1
        if len(result) > MAX_ARCHIVE_ROWS:
            raise GateFailure("ARCHIVE_HISTORY_TOO_LARGE", "SCHEMA")
        if next_cursor is None:
            return tuple(result), {
                "count": len(result),
                "digests_digest": _digest(sorted(seen)),
                "high_water_submission_idx": high_water,
                "pages": pages,
                "status": "OBSERVED_COMPLETE",
                "terminal_cursor": None,
            }
        cursor, boundary = next_cursor, next_boundary
    raise GateFailure("ARCHIVE_PAGINATION_NOT_BOUNDED", "SCHEMA")


async def _read_records(
    *,
    query_type: str,
    store: RunStore,
    transport: Any,
    queries: list[Mapping[str, Any]],
    observation_clock: _ObservationClock,
) -> tuple[tuple[dict[str, Any], ...], dict[str, Any]]:
    cursor: int | None = None
    boundary: tuple[Any, dict[str, Any]] | None = None
    seen_tx: dict[Any, dict[str, Any]] = {}
    seen_records: dict[Any, dict[str, Any]] = {}
    result: list[dict[str, Any]] = []
    pages = 0
    high_water: int | None = None
    while pages < MAX_ARCHIVE_PAGES:
        page, evidence = await _archive_query(
            store=store,
            transport=transport,
            query_type=query_type,
            page=pages,
            cursor=cursor,
            decoder=_decode_matches if query_type == "matches" else _decode_events,
            row_count=lambda item: len(item.records),
            queries=queries,
            observation_clock=observation_clock,
        )
        txs = page.txs
        _validate_desc(txs, "submission_idx")
        page_high_water = None if not txs else txs[0]["submission_idx"]
        if page_high_water is not None:
            if high_water is None:
                high_water = page_high_water
            elif page_high_water > high_water:
                raise GateFailure("ARCHIVE_HIGH_WATER_MOVED", "SCHEMA")
        raw_tx_keys = [tx["submission_idx"] for tx in txs]
        if len(raw_tx_keys) > ARCHIVE_REQUEST_LIMIT:
            raise GateFailure("ARCHIVE_PAGE_UNBOUNDED", "SCHEMA")
        next_cursor = raw_tx_keys[ARCHIVE_PAGE_LIMIT] if len(raw_tx_keys) > ARCHIVE_PAGE_LIMIT else None
        next_boundary = (
            (raw_tx_keys[ARCHIVE_PAGE_LIMIT], txs[ARCHIVE_PAGE_LIMIT])
            if next_cursor is not None
            else None
        )
        # As with order pages, the first omitted transaction is the next
        # cursor; the response's extra transaction is never admitted to the
        # current result set.
        tx_rows = list(txs[:ARCHIVE_PAGE_LIMIT])
        if boundary is not None and tx_rows and tx_rows[0]["submission_idx"] == boundary[0]:
            if tx_rows[0] != boundary[1]:
                raise GateFailure("ARCHIVE_PAGE_REPLAY_CONTRADICTORY", "SCHEMA")
            tx_rows.pop(0)
            boundary = None
        if cursor is not None and any(tx["submission_idx"] >= cursor for tx in tx_rows):
            raise GateFailure("ARCHIVE_CURSOR_NOT_RESPECTED", "SCHEMA")
        visible_tx_indices = {tx["submission_idx"] for tx in tx_rows}
        for tx in tx_rows:
            tx_idx = tx["submission_idx"]
            if tx_idx in seen_tx:
                raise GateFailure("ARCHIVE_PAGE_REPEATED", "SCHEMA")
            seen_tx[tx_idx] = tx
        records = [
            row for row in page.records if row["submission_idx"] in visible_tx_indices
        ]
        if len(records) != len({
            (row["submission_idx"], row.get("digest", row.get("event_type")), row["product_id"])
            for row in records
        }):
            raise GateFailure("ARCHIVE_PAGE_REPEATED", "SCHEMA")
        for row in records:
            if query_type == "matches":
                row_key = (row["digest"], row["submission_idx"])
            else:
                row_key = (row["submission_idx"], row["event_type"], row["product_id"])
            if row_key in seen_records:
                raise GateFailure("ARCHIVE_PAGE_REPEATED", "SCHEMA")
            seen_records[row_key] = row
        evidence_dict = evidence.as_dict()
        evidence_dict["cursor_out"] = next_cursor
        queries.append(evidence_dict)
        result.extend(records)
        pages += 1
        if len(result) > MAX_ARCHIVE_ROWS:
            raise GateFailure("ARCHIVE_HISTORY_TOO_LARGE", "SCHEMA")
        if next_cursor is None:
            return tuple(result), {
                "count": len(result),
                "high_water_submission_idx": high_water,
                "pages": pages,
                "records_digest": _digest(
                    sorted(_json(row) for row in result)
                ),
                "status": "OBSERVED_COMPLETE",
                "terminal_cursor": None,
                "transaction_count": len(seen_tx),
            }
        if cursor is not None and next_cursor >= cursor:
            raise GateFailure("ARCHIVE_CURSOR_NOT_ADVANCING", "SCHEMA")
        cursor, boundary = next_cursor, next_boundary
    raise GateFailure("ARCHIVE_PAGINATION_NOT_BOUNDED", "SCHEMA")


async def _read_funding(
    *,
    store: RunStore,
    transport: Any,
    queries: list[Mapping[str, Any]],
    observation_clock: _ObservationClock,
) -> tuple[tuple[dict[str, Any], ...], tuple[dict[str, Any], ...], dict[str, Any]]:
    cursor: int | None = None
    seen: dict[tuple[str, int, int], dict[str, Any]] = {}
    funding_rows: list[dict[str, Any]] = []
    interest_rows: list[dict[str, Any]] = []
    pages = 0
    high_water: int | None = None
    while pages < MAX_ARCHIVE_PAGES:
        page, evidence = await _archive_query(
            store=store,
            transport=transport,
            query_type="interest_and_funding",
            page=pages,
            cursor=cursor,
            decoder=_decode_funding,
            row_count=lambda item: len(item.interest) + len(item.funding),
            queries=queries,
            observation_clock=observation_clock,
        )
        all_rows = (*page.interest, *page.funding)
        page_high_water = None if not all_rows else max(row["idx"] for row in all_rows)
        if page_high_water is not None:
            if high_water is None:
                high_water = page_high_water
            elif page_high_water > high_water:
                raise GateFailure("ARCHIVE_HIGH_WATER_MOVED", "SCHEMA")
        if cursor is not None and any(row["idx"] > cursor for row in all_rows):
            raise GateFailure("FUNDING_CURSOR_NOT_RESPECTED", "SCHEMA")
        for kind, rows in (("interest", page.interest), ("funding", page.funding)):
            for row in rows:
                row_key = (kind, row["product_id"], row["idx"])
                previous = seen.get(row_key)
                if previous is not None:
                    if previous != row or row["idx"] != cursor:
                        raise GateFailure("FUNDING_ROW_CONTRADICTORY", "SCHEMA")
                    continue
                seen[row_key] = row
                (interest_rows if kind == "interest" else funding_rows).append(row)
        if page.next_idx is not None:
            if cursor is not None and page.next_idx >= cursor:
                raise GateFailure("FUNDING_CURSOR_NOT_ADVANCING", "SCHEMA")
        evidence_dict = evidence.as_dict()
        evidence_dict["cursor_out"] = page.next_idx
        queries.append(evidence_dict)
        pages += 1
        if len(funding_rows) + len(interest_rows) > MAX_ARCHIVE_ROWS:
            raise GateFailure("ARCHIVE_HISTORY_TOO_LARGE", "SCHEMA")
        if page.next_idx is None:
            return tuple(funding_rows), tuple(interest_rows), {
                "high_water_idx": high_water,
                "pages": pages,
                "status": "OBSERVED_COMPLETE",
                "terminal_cursor": None,
            }
        cursor = page.next_idx
    raise GateFailure("ARCHIVE_PAGINATION_NOT_BOUNDED", "SCHEMA")


def _funding_summary(
    funding_rows: Sequence[Mapping[str, Any]],
    interest_rows: Sequence[Mapping[str, Any]],
    pages: int,
    high_water: int | None,
) -> dict[str, Any]:
    amounts = [str(row["amount"]) for row in funding_rows]
    zero_count = sum(amount == "0" for amount in amounts)
    negative_count = sum(amount.startswith("-") for amount in amounts)
    positive_count = sum(amount != "0" and not amount.startswith("-") for amount in amounts)
    if not amounts or (zero_count == len(amounts)):
        payment_status = "ACTUAL_ZERO"
    else:
        payment_status = "OBSERVED_NONZERO"
    amount_projection = [
        {
            "amount": row["amount"],
            "idx": row["idx"],
            "product_id": row["product_id"],
        }
        for row in funding_rows
    ]
    return {
        "account_attribution": "REQUEST_BOUND_EXACT_SUBACCOUNT",
        "amount_digest": _digest(sorted(_json(item) for item in amount_projection)),
        "funding_payment_count": len(funding_rows),
        "funding_product_scope": ARCHIVE_FUNDING_PRODUCT_SCOPE,
        "high_water_idx": high_water,
        "historical_payments": "OBSERVED_COMPLETE",
        "interest_payment_count": len(interest_rows),
        "negative_count": negative_count,
        "pages": pages,
        "payment_status": payment_status,
        "positive_count": positive_count,
        "reason": "ARCHIVE_FUNDING_HISTORY_COMPLETE",
        "zero_count": zero_count,
    }


def _cross_agreement(
    orders: Sequence[Mapping[str, Any]],
    matches: Sequence[Mapping[str, Any]],
    events: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    order_by_digest = {row["digest"]: row for row in orders}
    match_digests = {row["digest"] for row in matches}
    if set(order_by_digest) != match_digests:
        raise GateFailure("HISTORY_CROSS_AGREEMENT_MISMATCH", "SCHEMA")
    match_indices = {row["submission_idx"] for row in matches}
    event_indices = {row["submission_idx"] for row in events}
    if not match_indices <= event_indices:
        raise GateFailure("HISTORY_EVENT_MATCH_AGREEMENT_MISMATCH", "SCHEMA")
    for match in matches:
        order = order_by_digest[match["digest"]]
        if not (
            order["submission_idx"]
            <= match["submission_idx"]
            <= order["last_fill_submission_idx"]
        ):
            raise GateFailure("HISTORY_FILL_RANGE_MISMATCH", "SCHEMA")
    match_products = {
        row["submission_idx"]: row["product_id"] for row in matches
    }
    for match_idx, product_id in match_products.items():
        if not any(
            event["submission_idx"] == match_idx
            and event["product_id"] == product_id
            for event in events
        ):
            raise GateFailure("HISTORY_EVENT_PRODUCT_MISMATCH", "SCHEMA")
    return {
        "event_match_submission_count": len(match_indices & event_indices),
        "event_submission_count": len(event_indices),
        "match_digest_count": len(match_digests),
        "match_submission_count": len(match_indices),
        "order_digest_count": len(order_by_digest),
        "status": "AGREE_EXACT_ACCOUNT_HISTORY",
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
    prior = store.claim()
    if prior is not None:
        return prior
    queries: list[Mapping[str, Any]] = []
    history = _empty_history()
    funding = _empty_funding()
    cross = _empty_cross_agreement()
    observation_clock = _ObservationClock(clock_ms)
    identity_metadata: Mapping[str, Any] | None = None
    current_surface = ""
    current_query_type = ""
    current_page = 0
    current_cursor: int | None = None
    try:
        identity_metadata = _canonical_identity(identity)

        current_surface = "archive_order_history_page_0"
        current_query_type = "orders"
        current_page = 0
        orders, order_summary = await _read_orders(
            store=store,
            transport=transport,
            queries=queries,
            observation_clock=observation_clock,
        )
        history["orders"] = order_summary

        current_surface = "archive_match_history_page_0"
        current_query_type = "matches"
        current_page = 0
        matches, match_summary = await _read_records(
            query_type="matches",
            store=store,
            transport=transport,
            queries=queries,
            observation_clock=observation_clock,
        )
        history["matches"] = match_summary

        current_surface = "archive_event_history_page_0"
        current_query_type = "events"
        current_page = 0
        events, event_summary = await _read_records(
            query_type="events",
            store=store,
            transport=transport,
            queries=queries,
            observation_clock=observation_clock,
        )
        history["events"] = event_summary

        current_surface = "archive_cross_agreement"
        current_query_type = "local_cross_agreement"
        cross = _cross_agreement(orders, matches, events)

        current_surface = "archive_funding_payments_page_0"
        current_query_type = "interest_and_funding"
        current_page = 0
        funding_rows, interest_rows, funding_page_summary = await _read_funding(
            store=store,
            transport=transport,
            queries=queries,
            observation_clock=observation_clock,
        )
        funding = _funding_summary(
            funding_rows,
            interest_rows,
            funding_page_summary["pages"],
            funding_page_summary["high_water_idx"],
        )
        result = _blocked_result(
            invocation_id=store.invocation_id,
            identity=identity_metadata,
            reason="ARCHIVE_READ_COMPLETE_NO_MAINNET_WRITE_AUTHORITY",
            failure_class=None,
            counters=store.counters(),
            queries=queries,
            history=history,
            funding=funding,
            cross_agreement=cross,
            blockers=_dedupe(PERMANENT_BLOCKERS),
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
            history=history,
            funding=funding,
            cross_agreement=cross,
            blockers=_dedupe(("READ_CANCELLED", *PERMANENT_BLOCKERS)),
        )
        return store.terminal(result)
    except GateFailure as failure:
        counters = store.counters()
        if current_surface and not any(
            item.get("surface") == current_surface for item in queries
        ):
            queries.append(
                _failure_query_evidence(
                    surface=current_surface,
                    query_type=current_query_type or "identity",
                    page=current_page,
                    account_binding="EXACT_SUBACCOUNT",
                    failure=failure,
                    counters=counters,
                    cursor_in=current_cursor,
                )
            )
        result = _blocked_result(
            invocation_id=store.invocation_id,
            identity=identity_metadata,
            reason=failure.reason,
            failure_class=failure.failure_class,
            counters=counters,
            queries=queries,
            history=history,
            funding=funding,
            cross_agreement=cross,
            blockers=_dedupe((failure.reason, *PERMANENT_BLOCKERS)),
        )
        return store.terminal(result)
    except Exception:
        failure = GateFailure("UNCLASSIFIED_FAILURE", "SAFETY")
        counters = store.counters()
        if current_surface and not any(
            item.get("surface") == current_surface for item in queries
        ):
            queries.append(
                _failure_query_evidence(
                    surface=current_surface,
                    query_type=current_query_type or "unknown",
                    page=current_page,
                    account_binding="EXACT_SUBACCOUNT",
                    failure=failure,
                    counters=counters,
                    cursor_in=current_cursor,
                )
            )
        result = _blocked_result(
            invocation_id=store.invocation_id,
            identity=identity_metadata,
            reason=failure.reason,
            failure_class=failure.failure_class,
            counters=counters,
            queries=queries,
            history=history,
            funding=funding,
            cross_agreement=cross,
            blockers=_dedupe((failure.reason, *PERMANENT_BLOCKERS)),
        )
        return store.terminal(result)


async def _production_run() -> ReadResult:
    _ensure_run_directory(RUN_DIRECTORY)
    store = RunStore(RUN_STORE_PATH, _new_invocation_id())
    try:
        try:
            identity = onboarding.discover_public_identity()
        except onboarding.OnboardingViolation:
            identity = None
        transport = MainnetArchiveTransport()
        try:
            return await _run_gate(
                store=store,
                identity=identity,
                transport=transport,
                clock_ms=lambda: int(time.time() * 1000),
            )
        finally:
            await transport.close()
    except Exception:
        raise


def _new_invocation_id() -> str:
    return "nado-mainnet-archive-read-" + secrets.token_hex(16)


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
    "ARCHIVE_FUNDING_PRODUCT_IDS",
    "ARCHIVE_FUNDING_PRODUCT_SCOPE",
    "ARCHIVE_PAGE_LIMIT",
    "ARCHIVE_PATH",
    "ArchiveReply",
    "ArchiveRequest",
    "BLOCKED",
    "CONFIG_HASH",
    "EXPECTED_SUBACCOUNT",
    "EXPECTED_SUBACCOUNT_NAME",
    "EXPECTED_WALLET_ADDRESS",
    "FAILURE_CLASSES",
    "GateFailure",
    "HTTP_METHOD",
    "MAINNET_ARCHIVE_URL",
    "MainnetArchiveTransport",
    "NO_MAINNET_WRITE_AUTHORITY",
    "PERMANENT_BLOCKERS",
    "ReadResult",
    "RunStore",
    "StoreFailure",
    "TransportInterruption",
    "_decode_events",
    "_decode_funding",
    "_decode_matches",
    "_decode_orders",
    "_expected_body",
    "_validate_archive_request",
    "run_fixture",
]
