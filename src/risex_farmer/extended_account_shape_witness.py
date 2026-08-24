"""Bounded, redacted Extended account-response shape witness.

The module is deliberately absent from normal Farmer startup and exposes no
production launcher.  A later operational gate may bind its fixture seam to a
fixed credential source and direct transport; this candidate performs neither.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import sqlite3
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping


SCHEMA_VERSION = 1
ACCOUNT_INFO_URL = (
    "https://api.starknet.sepolia.extended.exchange/api/v1/user/account/info"
)
BODY_MAX_BYTES = 65_536
SHAPE_MAX_DEPTH = 3
SHAPE_MAX_KEYS = 16
DESCRIPTOR_MAX_BYTES = 4_096
ALLOWED_FIELD_NAMES = frozenset(
    {
        "status",
        "data",
        "error",
        "pagination",
        "accountId",
        "id",
        "description",
        "accountIndex",
        "accountIndexForKeyGeneration",
        "l2Key",
        "l2Vault",
        "bridgeStarknetAddress",
    }
)
ALLOWED_TYPE_CLASSES = frozenset(
    {"object", "array", "string", "integer", "number", "boolean", "null"}
)
EFFECTS = ("loader", "account_info", "terminal")


class WitnessViolation(Exception):
    """A bounded public contract or redaction boundary was violated."""


def _empty_counters() -> dict[str, int]:
    return {
        f"{effect}_{suffix}": 0
        for effect in EFFECTS
        for suffix in ("attempts", "completions")
    }


def _decode_counters(raw: Any) -> dict[str, int]:
    try:
        value = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise WitnessViolation("DURABLE_COUNTERS_INVALID") from exc
    if (
        type(value) is not dict
        or set(value) != set(_empty_counters())
        or any(type(item) is not int or item not in {0, 1} for item in value.values())
        or any(
            value[f"{effect}_completions"] > value[f"{effect}_attempts"]
            for effect in EFFECTS
        )
    ):
        raise WitnessViolation("DURABLE_COUNTERS_INVALID")
    return value


def _type_class(value: Any) -> str:
    if value is None:
        return "null"
    if type(value) is bool:
        return "boolean"
    if type(value) is int:
        return "integer"
    if type(value) is float and math.isfinite(value):
        return "number"
    if type(value) is str:
        return "string"
    if type(value) is list:
        return "array"
    if type(value) is dict:
        return "object"
    raise WitnessViolation("SHAPE_TYPE_INVALID")


def _describe_body(body: Any) -> dict[str, Any]:
    """Return only approved field names, closed types, and unknown-name counts."""

    keys_seen = 0

    def walk(value: Any, depth: int, *, emit: bool) -> dict[str, Any]:
        nonlocal keys_seen
        if depth > SHAPE_MAX_DEPTH:
            raise WitnessViolation("SHAPE_DEPTH_EXCEEDED")
        kind = _type_class(value)
        if kind == "object":
            keys_seen += len(value)
            if keys_seen > SHAPE_MAX_KEYS:
                raise WitnessViolation("SHAPE_KEYS_EXCEEDED")
            known: dict[str, Any] = {}
            unknown = 0
            for key, item in value.items():
                if type(key) is not str:
                    raise WitnessViolation("SHAPE_TYPE_INVALID")
                if key in ALLOWED_FIELD_NAMES:
                    child = walk(item, depth + 1, emit=emit)
                    if emit:
                        known[key] = child
                else:
                    unknown += 1
                    walk(item, depth + 1, emit=False)
            if not emit:
                return {"type": "object"}
            return {
                "type": "object",
                "fields": {key: known[key] for key in sorted(known)},
                "unknown_fields": unknown,
            }
        if kind == "array":
            for item in value:
                walk(item, depth + 1, emit=False)
            return {"type": "array"}
        return {"type": kind}

    descriptor = walk(body, 0, emit=True)
    encoded = json.dumps(descriptor, sort_keys=True, separators=(",", ":"))
    if len(encoded.encode("ascii")) > DESCRIPTOR_MAX_BYTES:
        raise WitnessViolation("DESCRIPTOR_TOO_LARGE")
    return descriptor


@dataclass(frozen=True)
class AccountInfoRequest:
    method: str
    url: str
    headers: Mapping[str, str]


@dataclass(frozen=True)
class WitnessResult:
    status: str
    reason: str
    phase: str
    counters: Mapping[str, int]
    descriptor: Mapping[str, Any] | None
    schema_version: int = SCHEMA_VERSION

    def evidence(self) -> str:
        return json.dumps(
            {
                "counters": dict(self.counters),
                "descriptor": self.descriptor,
                "phase": self.phase,
                "reason": self.reason,
                "schema_version": self.schema_version,
                "status": self.status,
            },
            sort_keys=True,
            separators=(",", ":"),
        )


def _decode_result(raw: Any) -> WitnessResult:
    try:
        value = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise WitnessViolation("DURABLE_EVIDENCE_INVALID") from exc
    if type(value) is not dict or set(value) != {
        "counters", "descriptor", "phase", "reason", "schema_version", "status"
    }:
        raise WitnessViolation("DURABLE_EVIDENCE_INVALID")
    counters = _decode_counters(json.dumps(value["counters"]))
    if (
        value["schema_version"] != SCHEMA_VERSION
        or value["status"] not in {"CAPTURED", "BLOCKED", "UNKNOWN"}
        or type(value["reason"]) is not str
        or not value["reason"]
        or type(value["phase"]) is not str
        or not value["phase"]
        or (value["descriptor"] is not None and type(value["descriptor"]) is not dict)
        or (
            value["status"] == "CAPTURED"
            and (
                value["reason"] != "ACCOUNT_SHAPE_CAPTURED"
                or value["phase"] != "TERMINAL"
                or value["descriptor"] is None
                or set(counters.values()) != {1}
            )
        )
    ):
        raise WitnessViolation("DURABLE_EVIDENCE_INVALID")
    if value["descriptor"] is not None:
        try:
            _validate_descriptor(value["descriptor"])
            encoded = json.dumps(
                value["descriptor"], sort_keys=True, separators=(",", ":")
            )
        except (TypeError, UnicodeEncodeError, WitnessViolation) as exc:
            raise WitnessViolation("DURABLE_EVIDENCE_INVALID") from exc
        if len(encoded.encode("ascii")) > DESCRIPTOR_MAX_BYTES:
            raise WitnessViolation("DURABLE_EVIDENCE_INVALID")
    return WitnessResult(
        value["status"], value["reason"], value["phase"], counters,
        value["descriptor"],
    )


def _validate_descriptor(descriptor: Any) -> None:
    """Reject durable evidence that is not exactly the redacted grammar."""

    keys_seen = 0

    def walk(value: Any, depth: int) -> None:
        nonlocal keys_seen
        if depth > SHAPE_MAX_DEPTH or type(value) is not dict:
            raise WitnessViolation("DESCRIPTOR_INVALID")
        kind = value.get("type")
        if kind not in ALLOWED_TYPE_CLASSES:
            raise WitnessViolation("DESCRIPTOR_INVALID")
        if kind != "object":
            if set(value) != {"type"}:
                raise WitnessViolation("DESCRIPTOR_INVALID")
            return
        if set(value) != {"type", "fields", "unknown_fields"}:
            raise WitnessViolation("DESCRIPTOR_INVALID")
        fields = value["fields"]
        unknown = value["unknown_fields"]
        if (
            type(fields) is not dict
            or not set(fields) <= ALLOWED_FIELD_NAMES
            or type(unknown) is not int
            or unknown < 0
        ):
            raise WitnessViolation("DESCRIPTOR_INVALID")
        keys_seen += len(fields) + unknown
        if keys_seen > SHAPE_MAX_KEYS:
            raise WitnessViolation("DESCRIPTOR_INVALID")
        for child in fields.values():
            walk(child, depth + 1)

    walk(descriptor, 0)


class _WitnessStore:
    """Schema-one, FULL-synchronous, one-shot witness ledger."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._ensure_file()
        try:
            with self._connect() as connection:
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS extended_account_shape_witness (
                        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                        schema_version INTEGER NOT NULL,
                        state TEXT NOT NULL,
                        phase TEXT NOT NULL,
                        counters TEXT NOT NULL,
                        evidence TEXT
                    )
                    """
                )
                columns = tuple(
                    row[1] for row in connection.execute(
                        "PRAGMA table_info(extended_account_shape_witness)"
                    )
                )
                if columns != (
                    "singleton", "schema_version", "state", "phase", "counters",
                    "evidence",
                ):
                    raise WitnessViolation("DURABLE_SCHEMA_INVALID")
                catalog = tuple(
                    connection.execute(
                        """
                        SELECT type,name,tbl_name FROM sqlite_master
                        WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name
                        """
                    )
                )
                if catalog != ((
                    "table", "extended_account_shape_witness",
                    "extended_account_shape_witness",
                ),):
                    raise WitnessViolation("DURABLE_SCHEMA_INVALID")
        except sqlite3.DatabaseError as exc:
            raise WitnessViolation("DURABLE_STORE_INVALID") from exc

    def _ensure_file(self) -> None:
        try:
            before = self.path.lstat()
        except FileNotFoundError:
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            descriptor = os.open(self.path, flags, 0o600)
            os.close(descriptor)
            before = self.path.lstat()
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_uid != os.getuid()
        ):
            raise WitnessViolation("DURABLE_FILE_INVALID")

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def claim(self) -> WitnessResult | None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT schema_version,state,phase,counters,evidence
                FROM extended_account_shape_witness WHERE singleton=1
                """
            ).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO extended_account_shape_witness VALUES (1,?,?,?,?,NULL)",
                    (
                        SCHEMA_VERSION, "RUNNING", "STARTED",
                        json.dumps(_empty_counters(), sort_keys=True),
                    ),
                )
                return None
        schema, state, phase, raw_counters, evidence = row
        if schema != SCHEMA_VERSION:
            raise WitnessViolation("DURABLE_SCHEMA_INVALID")
        counters = _decode_counters(raw_counters)
        if state == "TERMINAL" and type(evidence) is str:
            result = _decode_result(evidence)
            if dict(result.counters) != counters or result.phase != phase:
                raise WitnessViolation("DURABLE_EVIDENCE_INVALID")
            return result
        if state == "RUNNING" and evidence is None:
            return WitnessResult(
                "UNKNOWN", "INTERRUPTED_RUNNING", phase, counters, None,
            )
        raise WitnessViolation("DURABLE_STATE_INVALID")

    def increment(self, effect: str, suffix: str) -> None:
        if effect not in EFFECTS or suffix not in {"attempts", "completions"}:
            raise WitnessViolation("DURABLE_COUNTERS_INVALID")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT state,counters FROM extended_account_shape_witness WHERE singleton=1"
            ).fetchone()
            if row is None or row[0] != "RUNNING":
                raise WitnessViolation("DURABLE_STATE_CONFLICT")
            counters = _decode_counters(row[1])
            key = f"{effect}_{suffix}"
            if suffix == "attempts" and counters[key] != 0:
                raise WitnessViolation("EFFECT_REPLAY_FORBIDDEN")
            if suffix == "completions" and (
                counters[key] != 0 or counters[f"{effect}_attempts"] != 1
            ):
                raise WitnessViolation("DURABLE_COUNTERS_INVALID")
            counters[key] += 1
            connection.execute(
                """
                UPDATE extended_account_shape_witness SET phase=?,counters=?
                WHERE singleton=1
                """,
                (effect.upper(), json.dumps(counters, sort_keys=True)),
            )

    def snapshot(self) -> tuple[str, dict[str, int]]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT phase,counters FROM extended_account_shape_witness WHERE singleton=1"
            ).fetchone()
        if row is None:
            raise WitnessViolation("DURABLE_STATE_INVALID")
        return row[0], _decode_counters(row[1])

    def terminal(
        self,
        result: WitnessResult,
        hook: Callable[[str, str], None] | None = None,
    ) -> WitnessResult:
        if hook is not None:
            hook("terminal", "before_attempt")
        self.increment("terminal", "attempts")
        if hook is not None:
            hook("terminal", "after_attempt")
        if hook is not None:
            hook("terminal", "before_completion")
        _, counters = self.snapshot()
        counters["terminal_completions"] = 1
        terminal = WitnessResult(
            result.status, result.reason, "TERMINAL", counters, result.descriptor,
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            changed = connection.execute(
                """
                UPDATE extended_account_shape_witness
                SET state='TERMINAL',phase='TERMINAL',counters=?,evidence=?
                WHERE singleton=1 AND state='RUNNING'
                """,
                (json.dumps(counters, sort_keys=True), terminal.evidence()),
            ).rowcount
            if changed != 1:
                raise WitnessViolation("DURABLE_STATE_CONFLICT")
        if hook is not None:
            hook("terminal", "after_completion")
        return terminal


class _Ledger:
    def __init__(
        self,
        store: _WitnessStore,
        hook: Callable[[str, str], None] | None,
    ):
        self.store = store
        self.hook = hook

    def _hook(self, effect: str, point: str) -> None:
        if self.hook is not None:
            self.hook(effect, point)

    def attempt(self, effect: str) -> None:
        self._hook(effect, "before_attempt")
        self.store.increment(effect, "attempts")
        self._hook(effect, "after_attempt")

    def observed(self, effect: str) -> None:
        self._hook(effect, "after_effect")

    def complete(self, effect: str) -> None:
        self._hook(effect, "before_completion")
        self.store.increment(effect, "completions")
        self._hook(effect, "after_completion")


def _validate_transport(metadata: Any) -> None:
    if type(metadata) is not dict or set(metadata) != {
        "actual_url", "method", "direct_tls", "trust_env", "proxy",
        "redirects", "retries",
    }:
        raise WitnessViolation("TRANSPORT_CONTRACT_INVALID")
    if (
        metadata["actual_url"] != ACCOUNT_INFO_URL
        or metadata["method"] != "GET"
        or metadata["direct_tls"] is not True
        or metadata["trust_env"] is not False
        or metadata["proxy"] is not None
        or type(metadata["redirects"]) is not int
        or metadata["redirects"] != 0
        or type(metadata["retries"]) is not int
        or metadata["retries"] != 0
    ):
        raise WitnessViolation("TRANSPORT_CONTRACT_INVALID")


async def _close_capability(capability: Any) -> None:
    if capability is None:
        return
    closer = getattr(capability, "close", None)
    if not callable(closer):
        raise WitnessViolation("CREDENTIAL_CAPABILITY_INVALID")
    result = closer()
    if asyncio.iscoroutine(result):
        await result


async def _run_fixture_account_shape_witness(
    *,
    store: _WitnessStore,
    credential_source: Any,
    transport: Any,
    _effect_hook: Callable[[str, str], None] | None = None,
) -> WitnessResult:
    """Fixture-only seam for a later separately gated sealed operation."""

    existing = store.claim()
    if existing is not None:
        return existing
    ledger = _Ledger(store, _effect_hook)
    capability: Any = None
    result: WitnessResult | None = None
    cancelled = False
    try:
        ledger.attempt("loader")
        capability = credential_source.open()
        if asyncio.iscoroutine(capability):
            capability = await capability
        header_method = getattr(capability, "x_api_key_header_value", None)
        if not callable(header_method) or not callable(getattr(capability, "close", None)):
            raise WitnessViolation("CREDENTIAL_CAPABILITY_INVALID")
        api_key = header_method()
        if type(api_key) is not str or not api_key:
            raise WitnessViolation("CREDENTIAL_INVALID")
        ledger.observed("loader")
        ledger.complete("loader")

        ledger.attempt("account_info")
        reply = await transport.get(
            AccountInfoRequest("GET", ACCOUNT_INFO_URL, {"X-Api-Key": api_key})
        )
        ledger.observed("account_info")
        if type(reply) is not dict or set(reply) != {"body", "body_bytes", "transport"}:
            raise WitnessViolation("RESPONSE_CONTRACT_INVALID")
        body_bytes = reply["body_bytes"]
        if type(body_bytes) is not int or body_bytes < 1:
            raise WitnessViolation("RESPONSE_CONTRACT_INVALID")
        if body_bytes > BODY_MAX_BYTES:
            raise WitnessViolation("BODY_TOO_LARGE")
        _validate_transport(reply["transport"])
        descriptor = _describe_body(reply["body"])
        ledger.complete("account_info")
        result = WitnessResult(
            "CAPTURED", "ACCOUNT_SHAPE_CAPTURED", store.snapshot()[0],
            store.snapshot()[1], descriptor,
        )
    except asyncio.CancelledError:
        cancelled = True
        phase, counters = store.snapshot()
        result = WitnessResult("BLOCKED", "CANCELLED", phase, counters, None)
    except WitnessViolation as exc:
        phase, counters = store.snapshot()
        result = WitnessResult("BLOCKED", str(exc), phase, counters, None)
    except Exception:
        phase, counters = store.snapshot()
        result = WitnessResult(
            "BLOCKED", "ACCOUNT_INFO_UNRESOLVED", phase, counters, None,
        )
    finally:
        try:
            await _close_capability(capability)
        except Exception:
            if result is None or result.status == "CAPTURED":
                phase, counters = store.snapshot()
                result = WitnessResult(
                    "BLOCKED", "CAPABILITY_CLOSE_FAILED", phase, counters, None,
                )
    if result is None:
        raise WitnessViolation("ACCOUNT_INFO_UNRESOLVED")
    terminal = store.terminal(result, _effect_hook)
    if cancelled:
        raise asyncio.CancelledError
    return terminal
