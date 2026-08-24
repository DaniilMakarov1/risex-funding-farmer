"""Explicit one-shot RISEx private-read operational launcher.

This module is intentionally absent from normal startup.  Production owns one
fixed invocation and accepts no path, identity, transport, URL, retry, or write
override.  Tests enter only through the private fixture factory at the bottom.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import math
import os
from pathlib import Path
import pwd
import sqlite3
import ssl
import stat
import sys
import time
from typing import Any, Callable, Mapping

import aiohttp

from . import testnet_risex_signer as _signer_storage
from .testnet_risex_private_read_operational import LifecycleClearBinding
from .testnet_risex_private_read_preflight import (
    ACCOUNT,
    HttpResponse,
    MAX_AGE_SECONDS,
    PrivateReadPreflight,
    SIGNER,
    WS_ORIGIN,
    expected_url,
)


INVOCATION_ID = "risex-private-read-20260824-new-op-003"
STORE_BASENAME = ".risex-funding-farmer-risex-private-read-20260824-new-op-003.sqlite3"
FIXED_STORE_PATH = Path(
    "/Users/daniilmakarov/.risex-funding-farmer-risex-private-read-20260824-new-op-003.sqlite3"
)
SCHEMA_VERSION = 2
_APPLICATION_ID = 0x52585052
_MAX_BYTES = 1_048_576
_DEADLINE_SECONDS = 5
_AUTH_SHAPE_SAFE_KEYS = frozenset({
    "method", "status", "data", "result", "response", "payload", "type",
    "error", "code",
})
_AUTH_SHAPE_MAX_DEPTH = 3
_AUTH_SHAPE_MAX_NODES = 32
_AUTH_SHAPE_MAX_FIELDS = 9
_AUTH_SHAPE_MAX_BYTES = 512
_AUTH_SHAPE_LIMIT = '{"shape":"limit_exceeded"}'

_PUBLIC_REQUESTS = tuple(PrivateReadPreflight._REQUESTS)
_PUBLIC_A_COUNTERS = tuple(
    f"public_a_{index:02d}" for index in range(1, len(_PUBLIC_REQUESTS) + 1)
)
_PRIVATE_COUNTERS = (
    "source_load",
    "capability_open",
    "signer_derive",
    "nonce_get",
    "register_v2_construct",
    "sign_register_v2",
    "auth_v2_dispatch",
    "auth_v2_receive",
    "auth_v2_parse",
    "auth_v2_validate",
    "auth_v2_status",
    "orders_subscribe",
    "orders_snapshot",
    "positions_subscribe",
    "positions_snapshot",
    "capability_close",
)
_PUBLIC_B_COUNTERS = tuple(
    f"public_b_{index:02d}" for index in range(1, len(_PUBLIC_REQUESTS) + 1)
)
_COUNTER_NAMES = (
    *_PUBLIC_A_COUNTERS,
    *_PRIVATE_COUNTERS,
    *_PUBLIC_B_COUNTERS,
    "final_agreement",
    "terminal_persist",
)
_TERMINAL_STATES = frozenset({"PASSED", "BLOCKED", "UNKNOWN"})
_REASON_VALUES = frozenset({
    "complete",
    "validation_failed",
    "cancelled",
    "interrupted_nonterminal",
    "store_rejected",
    "auth_v2_timeout",
    "auth_v2_close",
    "auth_v2_binary",
    "auth_v2_malformed",
    "auth_v2_schema_invalid",
    "auth_v2_error",
})
_LEDGER_SCHEMA = (
    "CREATE TABLE run ("
    "singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
    "schema_version INTEGER NOT NULL CHECK(schema_version=2),"
    "invocation_id TEXT NOT NULL CHECK(length(invocation_id)>0),"
    "store_path_sha256 TEXT NOT NULL CHECK(length(store_path_sha256)=64),"
    "state TEXT NOT NULL CHECK(state IN "
    "('NEW','CLAIMED','OBSERVED','PASSED','BLOCKED','UNKNOWN'))"
    ",barrier_a_fingerprint TEXT,"
    "barrier_b_fingerprint TEXT,"
    "started_at_ns INTEGER NOT NULL,"
    "finished_at_ns INTEGER,"
    "reason TEXT,"
    "auth_v2_shape TEXT,"
    "auth_v2_shape_sha256 TEXT);"
    "CREATE TABLE phase_counter ("
    "name TEXT PRIMARY KEY,"
    "attempts INTEGER NOT NULL CHECK(attempts IN (0,1)),"
    "completions INTEGER NOT NULL CHECK(completions IN (0,1)),"
    "CHECK(completions<=attempts));"
)


class Result(str, Enum):
    PASSED = "PASSED"
    BLOCKED = "BLOCKED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class OperationalReport:
    schema_version: int
    result: Result
    invocation_id: str
    store_path_sha256: str
    state: str
    counters: Mapping[str, Mapping[str, int]]
    barrier_a_fingerprint: str | None
    barrier_b_fingerprint: str | None
    reason: str
    auth_v2_shape: str | None
    auth_v2_shape_sha256: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "result": self.result.value,
            "invocation_id": self.invocation_id,
            "store_path_sha256": self.store_path_sha256,
            "state": self.state,
            "counters": {name: dict(value) for name, value in self.counters.items()},
            "barrier_a_fingerprint": self.barrier_a_fingerprint,
            "barrier_b_fingerprint": self.barrier_b_fingerprint,
            "reason": self.reason,
            "auth_v2_shape": self.auth_v2_shape,
            "auth_v2_shape_sha256": self.auth_v2_shape_sha256,
        }


class _StoreRejected(Exception):
    pass


class _AuthV2Failure(Exception):
    """Fixed redacted auth outcome; never carries server-controlled data."""

    def __init__(self, reason: str) -> None:
        if reason not in _REASON_VALUES or not reason.startswith("auth_v2_"):
            raise ValueError("auth_v2 failure reason")
        self.reason = reason
        super().__init__(reason)


class _SimulatedProcessDeath(BaseException):
    """Fixture-only abrupt-stop sentinel; production never constructs it."""


def _path_hash(path: Path) -> str:
    return hashlib.sha256(os.fsencode(str(path))).hexdigest()


def _safe_existing_file(path: Path) -> bool:
    try:
        details = path.lstat()
    except OSError:
        return False
    return (
        path.is_absolute()
        and stat.S_ISREG(details.st_mode)
        and not path.is_symlink()
        and details.st_uid == os.getuid()
        and stat.S_IMODE(details.st_mode) == 0o600
        and details.st_nlink == 1
    )


def _fsync_parent(path: Path) -> None:
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _sqlite_catalog(connection: sqlite3.Connection) -> tuple[tuple[Any, ...], ...]:
    return tuple(connection.execute(
        "SELECT type,name,tbl_name,COALESCE(sql,'') FROM sqlite_master "
        "ORDER BY type,name,tbl_name,sql"
    ))


def _expected_ledger_catalog() -> tuple[tuple[Any, ...], ...]:
    expected = sqlite3.connect(":memory:")
    try:
        expected.executescript(_LEDGER_SCHEMA)
        return _sqlite_catalog(expected)
    finally:
        expected.close()


def _sequential_prefix(
    names: tuple[str, ...], counters: Mapping[str, tuple[int, int]],
) -> bool:
    stopped = False
    for name in names:
        value = counters[name]
        if stopped:
            if value != (0, 0):
                return False
        elif value == (1, 1):
            continue
        elif value in {(0, 0), (1, 0)}:
            stopped = True
        else:
            return False
    return True


def _private_prefix(counters: Mapping[str, tuple[int, int]]) -> bool:
    close = counters["capability_close"]
    core = _PRIVATE_COUNTERS[:-1]
    if close == (0, 0):
        return _sequential_prefix(core, counters)
    return (
        close in {(1, 0), (1, 1)}
        and counters["capability_open"] == (1, 1)
        and _sequential_prefix(core, counters)
    )


def _reachable_stage(
    row: tuple[Any, ...], counters: Mapping[str, tuple[int, int]],
) -> str | None:
    fingerprint_a, fingerprint_b = row[4], row[5]
    public_a_complete = all(counters[name] == (1, 1) for name in _PUBLIC_A_COUNTERS)
    private_complete = all(counters[name] == (1, 1) for name in _PRIVATE_COUNTERS)
    public_b_complete = all(counters[name] == (1, 1) for name in _PUBLIC_B_COUNTERS)
    private_any = any(counters[name] != (0, 0) for name in _PRIVATE_COUNTERS)
    public_b_any = any(counters[name] != (0, 0) for name in _PUBLIC_B_COUNTERS)
    final = counters["final_agreement"]

    if public_b_any or fingerprint_b is not None or final != (0, 0):
        if (
            not public_a_complete
            or fingerprint_a is None
            or not private_complete
            or not _sequential_prefix((*_PUBLIC_B_COUNTERS, "final_agreement"), counters)
            or (fingerprint_b is not None and not public_b_complete)
            or (final != (0, 0) and (not public_b_complete or fingerprint_b is None))
        ):
            return None
        return "OBSERVED"
    if private_any or fingerprint_a is not None:
        if (
            not public_a_complete
            or fingerprint_a is None
            or fingerprint_b is not None
            or not _private_prefix(counters)
        ):
            return None
        return "CLAIMED"
    if (
        fingerprint_a is not None
        or fingerprint_b is not None
        or not _sequential_prefix(_PUBLIC_A_COUNTERS, counters)
        or any(
            counters[name] != (0, 0)
            for name in (*_PRIVATE_COUNTERS, *_PUBLIC_B_COUNTERS, "final_agreement")
        )
    ):
        return None
    return "NEW"


class DurableCounterLedger:
    """Single-use FULL-synchronous ledger with one counter row per effect."""

    def __init__(self, path: Path, invocation_id: str) -> None:
        self.path = Path(path)
        self.invocation_id = invocation_id
        self.created = False
        if not self.path.is_absolute() or not self.path.parent.is_dir():
            raise _StoreRejected("store_rejected")
        if not self.path.exists():
            try:
                fd = os.open(
                    self.path,
                    os.O_CREAT | os.O_EXCL | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                )
                os.fchmod(fd, 0o600)
                os.fsync(fd)
                os.close(fd)
                _fsync_parent(self.path)
                self.created = True
            except OSError:
                raise _StoreRejected("store_rejected") from None
        if not _safe_existing_file(self.path):
            raise _StoreRejected("store_rejected")
        try:
            self._db = sqlite3.connect(self.path)
            self._db.execute("PRAGMA journal_mode=DELETE")
            self._db.execute("PRAGMA synchronous=FULL")
            if self.created:
                self._initialize()
            self._validate()
        except (sqlite3.DatabaseError, ValueError, TypeError):
            try:
                self._db.close()
            except Exception:
                pass
            raise _StoreRejected("store_rejected") from None

    def _initialize(self) -> None:
        now = time.time_ns()
        with self._db:
            self._db.executescript(_LEDGER_SCHEMA)
            self._db.execute(f"PRAGMA application_id={_APPLICATION_ID}")
            self._db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
            self._db.execute(
                "INSERT INTO run VALUES(1,?,?,?,?,?,?,?,NULL,NULL,NULL,NULL)",
                (
                    SCHEMA_VERSION,
                    self.invocation_id,
                    _path_hash(self.path),
                    "NEW",
                    None,
                    None,
                    now,
                ),
            )
            self._db.executemany(
                "INSERT INTO phase_counter VALUES(?,0,0)",
                ((name,) for name in _COUNTER_NAMES),
            )

    def _validate(self) -> None:
        if self._db.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
            raise ValueError("integrity")
        if self._db.execute("PRAGMA application_id").fetchone() != (_APPLICATION_ID,):
            raise ValueError("application")
        if self._db.execute("PRAGMA user_version").fetchone() != (SCHEMA_VERSION,):
            raise ValueError("version")
        if (
            self._db.execute("PRAGMA foreign_key_check").fetchall()
            or _sqlite_catalog(self._db) != _expected_ledger_catalog()
        ):
            raise ValueError("schema")
        run_columns = tuple(row[1] for row in self._db.execute("PRAGMA table_info(run)"))
        counter_columns = tuple(
            row[1] for row in self._db.execute("PRAGMA table_info(phase_counter)")
        )
        if run_columns != (
            "singleton", "schema_version", "invocation_id", "store_path_sha256",
            "state", "barrier_a_fingerprint", "barrier_b_fingerprint",
            "started_at_ns", "finished_at_ns", "reason", "auth_v2_shape",
            "auth_v2_shape_sha256",
        ) or counter_columns != ("name", "attempts", "completions"):
            raise ValueError("schema")
        if self._db.execute("SELECT COUNT(*) FROM run").fetchone() != (1,):
            raise ValueError("cardinality")
        if self._db.execute("SELECT COUNT(*) FROM phase_counter").fetchone() != (
            len(_COUNTER_NAMES),
        ):
            raise ValueError("cardinality")
        row = self._row()
        if (
            row[0] != SCHEMA_VERSION
            or row[1] != self.invocation_id
            or row[2] != _path_hash(self.path)
            or row[3] not in {"NEW", "CLAIMED", "OBSERVED", *_TERMINAL_STATES}
            or (row[4] is not None and not _fingerprint(row[4]))
            or (row[5] is not None and not _fingerprint(row[5]))
            or type(row[6]) is not int or not 0 < row[6] < 2 ** 63
            or (row[7] is not None and (
                type(row[7]) is not int or not row[6] <= row[7] < 2 ** 63
            ))
            or (row[8] is not None and row[8] not in _REASON_VALUES)
            or (row[3] in _TERMINAL_STATES) != (row[7] is not None and row[8] is not None)
            or ((row[9] is None) != (row[10] is None))
            or (
                row[9] is not None
                and not _valid_auth_v2_shape(str(row[9]), str(row[10]))
            )
        ):
            raise ValueError("identity")
        counters = self._counter_rows()
        if set(counters) != set(_COUNTER_NAMES):
            raise ValueError("counters")
        for attempts, completions in counters.values():
            if (
                type(attempts) is not int
                or type(completions) is not int
                or attempts not in (0, 1)
                or completions not in (0, 1)
                or completions > attempts
            ):
                raise ValueError("counter")
        stage = _reachable_stage(row, counters)
        if stage is None:
            raise ValueError("counter state")
        terminal = row[3] in _TERMINAL_STATES
        terminal_counter = counters["terminal_persist"]
        shape_present = row[9] is not None
        if terminal:
            if shape_present != (row[8] == "auth_v2_schema_invalid"):
                raise ValueError("auth_v2 shape terminal")
        elif shape_present and not (
            row[3] == "CLAIMED"
            and counters["auth_v2_receive"] == (1, 1)
            and counters["auth_v2_parse"] == (1, 1)
            and counters["auth_v2_validate"] == (1, 0)
            and counters["auth_v2_status"] == (0, 0)
        ):
            raise ValueError("auth_v2 shape stage")
        if counters["final_agreement"] == (1, 1) and row[4] != row[5]:
            raise ValueError("agreement invariant")
        if terminal:
            if terminal_counter != (1, 1):
                raise ValueError("terminal counter")
        elif terminal_counter not in {(0, 0), (1, 0)} or row[3] != stage:
            raise ValueError("nonterminal state")
        if row[3] == "PASSED" and (
            row[8] != "complete"
            or stage != "OBSERVED"
            or row[4] is None
            or row[4] != row[5]
            or any(counters[name] != (1, 1) for name in _COUNTER_NAMES)
        ):
            raise ValueError("passed invariant")
        if row[3] == "BLOCKED" and (
            row[8] != "validation_failed"
            or all(
                counters[name] == (1, 1)
                for name in _COUNTER_NAMES
                if name != "terminal_persist"
            )
        ):
            raise ValueError("blocked invariant")
        if row[3] == "UNKNOWN" and row[8] not in {
            "validation_failed", "cancelled", "interrupted_nonterminal",
            "auth_v2_timeout", "auth_v2_close", "auth_v2_binary",
            "auth_v2_malformed", "auth_v2_schema_invalid", "auth_v2_error",
        }:
            raise ValueError("unknown invariant")

    def _row(self) -> tuple[Any, ...]:
        row = self._db.execute(
            "SELECT schema_version,invocation_id,store_path_sha256,state,"
            "barrier_a_fingerprint,barrier_b_fingerprint,started_at_ns,"
            "finished_at_ns,reason,auth_v2_shape,auth_v2_shape_sha256 "
            "FROM run WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise ValueError("missing run")
        return row

    def _counter_rows(self) -> dict[str, tuple[int, int]]:
        return {
            name: (attempts, completions)
            for name, attempts, completions in self._db.execute(
                "SELECT name,attempts,completions FROM phase_counter"
            )
        }

    @property
    def state(self) -> str:
        return str(self._row()[3])

    def attempt(self, name: str) -> None:
        if name not in _COUNTER_NAMES:
            raise ValueError("unknown phase")
        with self._db:
            changed = self._db.execute(
                "UPDATE phase_counter SET attempts=1 WHERE name=? "
                "AND attempts=0 AND completions=0",
                (name,),
            ).rowcount
        if changed != 1:
            raise ValueError("phase already attempted")

    def complete(self, name: str) -> None:
        with self._db:
            changed = self._db.execute(
                "UPDATE phase_counter SET completions=1 WHERE name=? "
                "AND attempts=1 AND completions=0",
                (name,),
            ).rowcount
        if changed != 1:
            raise ValueError("phase not attempted")

    def set_claimed(self, fingerprint: str) -> None:
        if not _fingerprint(fingerprint):
            raise ValueError("fingerprint")
        with self._db:
            changed = self._db.execute(
                "UPDATE run SET state='CLAIMED',barrier_a_fingerprint=? "
                "WHERE singleton=1 AND state='NEW'",
                (fingerprint,),
            ).rowcount
        if changed != 1:
            raise ValueError("state")

    def set_observed(self) -> None:
        with self._db:
            changed = self._db.execute(
                "UPDATE run SET state='OBSERVED' WHERE singleton=1 AND state='CLAIMED'",
            ).rowcount
        if changed != 1:
            raise ValueError("state")

    def set_barrier_b(self, fingerprint: str) -> None:
        if not _fingerprint(fingerprint):
            raise ValueError("fingerprint")
        with self._db:
            changed = self._db.execute(
                "UPDATE run SET barrier_b_fingerprint=? "
                "WHERE singleton=1 AND state='OBSERVED' AND barrier_b_fingerprint IS NULL",
                (fingerprint,),
            ).rowcount
        if changed != 1:
            raise ValueError("state")

    def has_mismatch(self) -> bool:
        return any(a != c for a, c in self._counter_rows().values())

    def record_auth_v2_shape(self, descriptor: str, digest: str) -> None:
        if not _valid_auth_v2_shape(descriptor, digest):
            raise ValueError("auth_v2 shape")
        counters = self._counter_rows()
        if (
            counters["auth_v2_receive"] != (1, 1)
            or counters["auth_v2_parse"] != (1, 1)
            or counters["auth_v2_validate"] != (1, 0)
            or counters["auth_v2_status"] != (0, 0)
        ):
            raise ValueError("auth_v2 shape stage")
        with self._db:
            changed = self._db.execute(
                "UPDATE run SET auth_v2_shape=?,auth_v2_shape_sha256=? "
                "WHERE singleton=1 AND state='CLAIMED' "
                "AND auth_v2_shape IS NULL AND auth_v2_shape_sha256 IS NULL",
                (descriptor, digest),
            ).rowcount
        if changed != 1:
            raise ValueError("auth_v2 shape state")
        self._validate()

    def finalize(
        self,
        result: Result,
        reason: str,
        crash_hook: Callable[[str, str], None] | None = None,
    ) -> OperationalReport:
        if result.value not in _TERMINAL_STATES or reason not in _REASON_VALUES:
            raise ValueError("terminal")
        counters = self._counter_rows()
        row = self._row()
        if result is Result.PASSED and (
            row[3] != "OBSERVED"
            or row[4] is None
            or row[4] != row[5]
            or any(
                counters[name] != (1, 1)
                for name in _COUNTER_NAMES
                if name != "terminal_persist"
            )
        ):
            result = Result.UNKNOWN
            reason = "validation_failed"
        if reason == "auth_v2_schema_invalid" and not (
            row[9] is not None
            and row[10] is not None
            and _valid_auth_v2_shape(str(row[9]), str(row[10]))
        ):
            result = Result.UNKNOWN
            reason = "validation_failed"
        attempts, completions = self._counter_rows()["terminal_persist"]
        if attempts == 0:
            self.attempt("terminal_persist")
            if crash_hook is not None:
                crash_hook("after_attempt", "terminal_persist")
        elif not (attempts == 1 and completions == 0):
            if self.state in _TERMINAL_STATES and completions == 1:
                return self.report()
            raise ValueError("terminal counter")
        if crash_hook is not None:
            crash_hook("before_completion", "terminal_persist")
        with self._db:
            self._db.execute(
                "UPDATE run SET state=?,finished_at_ns=?,reason=?,"
                "auth_v2_shape=CASE WHEN ?='auth_v2_schema_invalid' "
                "THEN auth_v2_shape ELSE NULL END,"
                "auth_v2_shape_sha256=CASE WHEN ?='auth_v2_schema_invalid' "
                "THEN auth_v2_shape_sha256 ELSE NULL END WHERE singleton=1",
                (result.value, time.time_ns(), reason, reason, reason),
            )
            changed = self._db.execute(
                "UPDATE phase_counter SET completions=1 WHERE name='terminal_persist' "
                "AND attempts=1 AND completions=0"
            ).rowcount
            if changed != 1:
                raise ValueError("terminal counter")
        self._validate()
        return self.report()

    def report(self) -> OperationalReport:
        row = self._row()
        state = str(row[3])
        if state not in _TERMINAL_STATES:
            raise ValueError("nonterminal")
        return OperationalReport(
            schema_version=SCHEMA_VERSION,
            result=Result(state),
            invocation_id=self.invocation_id,
            store_path_sha256=_path_hash(self.path),
            state=state,
            counters={
                name: {"attempts": values[0], "completions": values[1]}
                for name, values in sorted(self._counter_rows().items())
            },
            barrier_a_fingerprint=row[4],
            barrier_b_fingerprint=row[5],
            reason=str(row[8]),
            auth_v2_shape=None if row[9] is None else str(row[9]),
            auth_v2_shape_sha256=None if row[10] is None else str(row[10]),
        )

    def close(self) -> None:
        self._db.close()


def _fingerprint(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


class _AuthShapeLimit(Exception):
    pass


def _auth_v2_shape(value: Any) -> tuple[str, str]:
    nodes = 0

    def walk(current: Any, depth: int) -> Any:
        nonlocal nodes
        nodes += 1
        if depth > _AUTH_SHAPE_MAX_DEPTH or nodes > _AUTH_SHAPE_MAX_NODES:
            raise _AuthShapeLimit
        if current is None:
            return "null"
        if type(current) is bool:
            return "boolean"
        if type(current) in {int, float}:
            return "number"
        if type(current) is str:
            return "string"
        if type(current) is list:
            return "array"
        if type(current) is not dict or len(current) > _AUTH_SHAPE_MAX_FIELDS:
            raise _AuthShapeLimit
        safe = sorted(key for key in current if key in _AUTH_SHAPE_SAFE_KEYS)
        shaped: dict[str, Any] = {
            "object": [[key, walk(current[key], depth + 1)] for key in safe],
        }
        if len(safe) != len(current):
            shaped["other_key"] = "present"
        return shaped

    try:
        shaped = walk(value, 0)
        descriptor = json.dumps(
            shaped, ensure_ascii=True, sort_keys=True, separators=(",", ":"),
        )
        if len(descriptor.encode("ascii")) > _AUTH_SHAPE_MAX_BYTES:
            raise _AuthShapeLimit
    except _AuthShapeLimit:
        descriptor = _AUTH_SHAPE_LIMIT
    return descriptor, hashlib.sha256(descriptor.encode("ascii")).hexdigest()


def _valid_auth_v2_shape(descriptor: str, digest: str) -> bool:
    if (
        type(descriptor) is not str
        or not _fingerprint(digest)
        or len(descriptor.encode("utf-8")) > _AUTH_SHAPE_MAX_BYTES
        or hashlib.sha256(descriptor.encode("utf-8")).hexdigest() != digest
    ):
        return False
    if descriptor == _AUTH_SHAPE_LIMIT:
        return True
    try:
        parsed = json.loads(descriptor)
        if descriptor != json.dumps(
            parsed, ensure_ascii=True, sort_keys=True, separators=(",", ":"),
        ):
            return False
    except Exception:
        return False

    nodes = 0

    def valid_shape(current: Any, depth: int) -> bool:
        nonlocal nodes
        nodes += 1
        if depth > _AUTH_SHAPE_MAX_DEPTH or nodes > _AUTH_SHAPE_MAX_NODES:
            return False
        if type(current) is str and current in {
            "null", "boolean", "number", "string", "array",
        }:
            return True
        if type(current) is not dict or set(current) not in (
            {"object"}, {"object", "other_key"},
        ):
            return False
        if current.get("other_key", "present") != "present":
            return False
        fields = current.get("object")
        if type(fields) is not list or len(fields) > _AUTH_SHAPE_MAX_FIELDS:
            return False
        keys: list[str] = []
        for field in fields:
            if (
                type(field) is not list
                or len(field) != 2
                or type(field[0]) is not str
                or field[0] not in _AUTH_SHAPE_SAFE_KEYS
                or not valid_shape(field[1], depth + 1)
            ):
                return False
            keys.append(field[0])
        return keys == sorted(set(keys))

    return valid_shape(parsed, 0)


class _SignOnlyCapability:
    """Opaque closeable handle; its only public operations are the exact two uses."""

    def __init__(self, secret: bytearray) -> None:
        if not isinstance(secret, bytearray) or len(secret) != 32:
            raise ValueError("credential capability rejected")
        self._secret = secret
        self._closed = False

    def derive_signer_address(self) -> str:
        if self._closed:
            raise ValueError("credential capability rejected")
        return _signer_storage._derive_address(bytes(self._secret)).lower()

    def sign_register_v2(self, canonical_typed_data: Mapping[str, Any]) -> str:
        if self._closed:
            raise ValueError("credential capability rejected")
        try:
            nonce = canonical_typed_data["message"]["nonce"]
            canonical_nonce = PrivateReadPreflight._nonce(nonce)
            canonical = PrivateReadPreflight._typed_data(canonical_nonce)
        except Exception:
            raise ValueError("credential capability rejected") from None
        if nonce != canonical_nonce or canonical_typed_data != canonical:
            raise ValueError("credential capability rejected")
        secret = bytes(self._secret)
        try:
            return _signer_storage._sign_typed_data(secret, dict(canonical_typed_data))
        finally:
            secret = b""

    def close(self) -> None:
        for index in range(len(self._secret)):
            self._secret[index] = 0
        self._secret.clear()
        self._closed = True


class PasswdHomeSessionSignerCapabilitySource:
    """Chief-designated fixed passwd-home source for one sign-only handle."""

    def __init__(self) -> None:
        self._secret: bytearray | None = None
        self._loaded = False

    def load(self) -> None:
        if self._loaded:
            raise ValueError("credential capability rejected")
        home_fd = _signer_storage._open_home()
        secret = bytearray()
        try:
            record = _signer_storage._load_record(home_fd)
            loaded = _signer_storage._load_credential(home_fd)
            secret = bytearray(loaded)
            loaded = b""
        finally:
            os.close(home_fd)
        if (
            record.state is not _signer_storage.SignerState.ACTIVE
            or record.signer != SIGNER
            or len(secret) != 32
        ):
            for index in range(len(secret)):
                secret[index] = 0
            secret.clear()
            raise ValueError("credential capability rejected")
        self._secret = secret
        self._loaded = True

    def open(self) -> _SignOnlyCapability:
        if not self._loaded or self._secret is None:
            raise ValueError("credential capability rejected")
        secret = self._secret
        self._secret = None
        self._loaded = False
        return _SignOnlyCapability(secret)

    def close(self) -> None:
        if self._secret is not None:
            for index in range(len(self._secret)):
                self._secret[index] = 0
            self._secret.clear()
        self._secret = None


def _strict_json(raw: bytes) -> Any:
    def reject_constant(_value: str) -> Any:
        raise ValueError("private transport rejected")

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("private transport rejected")
            result[key] = value
        return result

    def finite_float(value: str) -> float:
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError("private transport rejected")
        return parsed

    try:
        return json.loads(
            raw.decode("utf-8", errors="strict"),
            parse_constant=reject_constant,
            parse_float=finite_float,
            object_pairs_hook=unique_object,
        )
    except Exception:
        raise ValueError("private transport rejected") from None


def _parse_auth_v2(raw: Any) -> Any:
    if type(raw) is not str:
        raise _AuthV2Failure("auth_v2_malformed")
    try:
        encoded = raw.encode("utf-8", errors="strict")
        return _strict_json(encoded)
    except _AuthV2Failure:
        raise
    except Exception:
        raise _AuthV2Failure("auth_v2_malformed") from None


def _validate_auth_v2_schema(value: Any) -> Mapping[str, Any]:
    if (
        not isinstance(value, Mapping)
        or set(value) != {"method", "status"}
        or value.get("method") != "auth_v2"
        or value.get("status") not in {"success", "error"}
    ):
        raise _AuthV2Failure("auth_v2_schema_invalid")
    return value


def _require_auth_v2_success(value: Mapping[str, Any]) -> Mapping[str, Any]:
    if value.get("status") == "error":
        raise _AuthV2Failure("auth_v2_error")
    try:
        PrivateReadPreflight._validate_auth_frame(value)
    except Exception:
        raise _AuthV2Failure("auth_v2_schema_invalid") from None
    return value


async def _abort_redirect(_session: Any, _context: Any, _params: Any) -> None:
    raise ValueError("private transport redirect rejected")


async def _guard_single_dispatch(_session: Any, context: Any, _params: Any) -> None:
    if getattr(context, "risex_request_dispatched", False):
        raise ValueError("private transport repeated dispatch rejected")
    context.risex_request_dispatched = True


class FixedRisexPrivateReadTransport:
    REST_ORIGIN = "https://api.testnet.rise.trade"
    WS_URL = "wss://api.testnet.rise.trade/ws/"
    TRUST_ENV = False
    ALLOW_REDIRECTS = False
    DEADLINE_SECONDS = _DEADLINE_SECONDS
    PUBLIC_REQUEST_COUNT = 9

    def __init__(self) -> None:
        redirect_guard = aiohttp.TraceConfig()
        redirect_guard.on_request_redirect.append(_abort_redirect)
        redirect_guard.on_request_headers_sent.append(_guard_single_dispatch)
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=5, connect=2, sock_read=3),
            trust_env=False,
            connector=aiohttp.TCPConnector(ssl=ssl.create_default_context()),
            trace_configs=[redirect_guard],
        )
        if hasattr(self._session, "_retry_connection"):
            self._session._retry_connection = False
        self._socket_context: Any = None
        self._socket: Any = None

    async def _get(self, path: str, query: tuple[tuple[str, str], ...]) -> HttpResponse:
        target = expected_url(path, query)
        async with self._session.get(
            target, allow_redirects=False, proxy=None,
        ) as response:
            if response.history or str(response.url) != target:
                raise ValueError("private transport rejected")
            if response.content_length is not None and response.content_length > _MAX_BYTES:
                raise ValueError("private transport rejected")
            raw = await response.content.read(_MAX_BYTES + 1)
            if len(raw) > _MAX_BYTES:
                raise ValueError("private transport rejected")
            return HttpResponse(
                response.status,
                str(response.url),
                _strict_json(raw),
                time.time(),
                False,
            )

    async def public_get(self, index: int) -> HttpResponse:
        if type(index) is not int or not 0 <= index < len(_PUBLIC_REQUESTS):
            raise ValueError("private transport rejected")
        path, query = _PUBLIC_REQUESTS[index]
        return await self._get(path, query)

    async def nonce_get(self) -> HttpResponse:
        return await self._get("/v1/auth/nonce", (("account", ACCOUNT),))

    async def auth_v2_dispatch(self, frame: Mapping[str, Any]) -> None:
        if self._socket is not None or set(frame) != {"method", "params"}:
            raise ValueError("private transport rejected")
        params = frame.get("params")
        if not isinstance(params, Mapping) or set(params) != {
            "account", "signer", "message", "nonce", "expiration", "signature",
        }:
            raise ValueError("private transport rejected")
        if (
            frame.get("method") != "auth_v2"
            or params.get("account") != ACCOUNT
            or params.get("signer") != SIGNER
            or params.get("message") != "sign in with RISEx"
            or PrivateReadPreflight._nonce(params.get("nonce")) != params.get("nonce")
            or type(params.get("expiration")) is not int
            or params["expiration"] <= 0
        ):
            raise ValueError("private transport rejected")
        _validate_signature(params.get("signature"))
        self._socket_context = self._session.ws_connect(
            WS_ORIGIN,
            ssl=ssl.create_default_context(),
            proxy=None,
            autoclose=False,
            autoping=False,
            max_msg_size=_MAX_BYTES,
        )
        self._socket = await self._socket_context.__aenter__()
        response = getattr(self._socket, "_response", None)
        if (
            response is None
            or str(getattr(response, "url", "")) != self.WS_URL
            or bool(getattr(response, "history", ()))
        ):
            raise ValueError("private transport rejected")
        await self._socket.send_json(dict(frame))

    async def _receive(self) -> Any:
        if self._socket is None:
            raise ValueError("private transport rejected")
        incoming = await self._socket.receive(timeout=_DEADLINE_SECONDS)
        if incoming.type is not aiohttp.WSMsgType.TEXT:
            raise ValueError("private transport rejected")
        return _strict_json(incoming.data.encode("utf-8"))

    async def auth_v2_receive(self) -> str:
        if self._socket is None:
            raise ValueError("private transport rejected")
        try:
            incoming = await self._socket.receive(timeout=_DEADLINE_SECONDS)
        except asyncio.TimeoutError:
            raise _AuthV2Failure("auth_v2_timeout") from None
        if incoming.type is aiohttp.WSMsgType.BINARY:
            raise _AuthV2Failure("auth_v2_binary")
        if incoming.type in {
            aiohttp.WSMsgType.CLOSE,
            aiohttp.WSMsgType.CLOSING,
            aiohttp.WSMsgType.CLOSED,
            aiohttp.WSMsgType.ERROR,
        }:
            raise _AuthV2Failure("auth_v2_close")
        if incoming.type is not aiohttp.WSMsgType.TEXT or type(incoming.data) is not str:
            raise _AuthV2Failure("auth_v2_malformed")
        return incoming.data

    async def orders_subscribe(self) -> None:
        if self._socket is None:
            raise ValueError("private transport rejected")
        await self._socket.send_json(
            {"method": "subscribe", "params": {"channel": "orders"}}
        )

    async def orders_snapshot(self) -> Any:
        return await self._receive()

    async def positions_subscribe(self) -> None:
        if self._socket is None:
            raise ValueError("private transport rejected")
        await self._socket.send_json(
            {"method": "subscribe", "params": {"channel": "positions"}}
        )

    async def positions_snapshot(self) -> Any:
        value = await self._receive()
        try:
            extra = await self._socket.receive(timeout=0.01)
        except asyncio.TimeoutError:
            extra = None
        if extra is not None:
            raise ValueError("private transport rejected")
        return value

    async def close(self) -> None:
        if self._socket is not None:
            await self._socket.close()
            await self._socket_context.__aexit__(None, None, None)
            self._socket = None
            self._socket_context = None
        await self._session.close()


def _public_fingerprint(validated_sweep: tuple[Any, ...]) -> str:
    if (
        not isinstance(validated_sweep, tuple)
        or len(validated_sweep) != 4
        or not isinstance(validated_sweep[2], tuple)
        or not isinstance(validated_sweep[3], tuple)
    ):
        raise ValueError("public sweep fingerprint rejected")
    best_bid, best_ask, bids, asks = validated_sweep

    def levels(value: tuple[Any, ...]) -> list[list[str]]:
        result = []
        for level in value:
            if not isinstance(level, tuple) or len(level) != 2:
                raise ValueError("public sweep fingerprint rejected")
            result.append([str(level[0]), str(level[1])])
        return result

    canonical = {
        "contract": "strict-risex-public-zero-flat-v1",
        "account_sha256": hashlib.sha256(ACCOUNT.encode()).hexdigest(),
        "signer_sha256": hashlib.sha256(SIGNER.encode()).hexdigest(),
        "request_count": len(_PUBLIC_REQUESTS),
        "validated_sweep": {
            "best_bid": str(best_bid),
            "best_ask": str(best_ask),
            "bids": levels(bids),
            "asks": levels(asks),
        },
    }
    return hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _capability_surface_is_exact(capability: Any) -> bool:
    expected = {"derive_signer_address", "sign_register_v2", "close"}
    exposed = {
        name for name in vars(type(capability))
        if not name.startswith("_")
    }
    return exposed == expected and all(callable(getattr(capability, name, None)) for name in expected)


async def _maybe_await(value: Any) -> Any:
    if asyncio.iscoroutine(value) or isinstance(value, asyncio.Future):
        return await value
    return value


@dataclass(frozen=True)
class _Dependencies:
    path: Path
    invocation_id: str
    source_factory: Callable[[], Any]
    transport_factory: Callable[[], Any]
    clock: Callable[[], float]
    lifecycle_clear: Callable[[], bool]
    crash_hook: Callable[[str, str], None] | None = None


class OperationalPrivateRead:
    """Production binding: constructor deliberately accepts no arguments."""

    def __init__(self) -> None:
        home = Path(pwd.getpwuid(os.getuid()).pw_dir)
        path = home / STORE_BASENAME
        if path != FIXED_STORE_PATH:
            raise RuntimeError("fixed production identity unavailable")
        self._dependencies = _Dependencies(
            path=path,
            invocation_id=INVOCATION_ID,
            source_factory=PasswdHomeSessionSignerCapabilitySource,
            transport_factory=FixedRisexPrivateReadTransport,
            clock=time.time,
            lifecycle_clear=LifecycleClearBinding(),
        )

    async def run(self) -> OperationalReport:
        return await _run(self._dependencies)


async def _phase(
    ledger: DurableCounterLedger,
    name: str,
    effect: Callable[[], Any],
    validate: Callable[[Any], Any],
    crash_hook: Callable[[str, str], None] | None,
) -> Any:
    ledger.attempt(name)
    if crash_hook is not None:
        crash_hook("after_attempt", name)
    value = await _maybe_await(effect())
    if crash_hook is not None:
        crash_hook("before_completion", name)
    validated = validate(value)
    ledger.complete(name)
    return validated


def _identity(value: Any) -> Any:
    return value


def _none(value: Any) -> None:
    if value is not None:
        raise ValueError("effect result rejected")
    return None


async def _public_barrier(
    ledger: DurableCounterLedger,
    validator: PrivateReadPreflight,
    transport: Any,
    prefix: str,
    crash_hook: Callable[[str, str], None] | None,
) -> str:
    responses: dict[str, Any] = {}
    observations: list[float] = []
    for index, (path, query) in enumerate(_PUBLIC_REQUESTS):
        raw_response: HttpResponse | None = None

        def validate_response(value: Any) -> Any:
            nonlocal raw_response
            validated = validator._validate_response(path, query, value)
            raw_response = value
            return validated

        response = await _phase(
            ledger,
            f"public_{prefix}_{index + 1:02d}",
            lambda index=index: transport.public_get(index),
            validate_response,
            crash_hook,
        )
        assert raw_response is not None
        observations.append(raw_response.observed_at)
        responses[path] = response
    validated_sweep = validator._validate_sweep(responses)
    finished_at = validator._now()
    if any(
        finished_at - observed_at < 0
        or finished_at - observed_at > MAX_AGE_SECONDS
        for observed_at in observations
    ):
        raise ValueError("public barrier stale")
    fingerprint = _public_fingerprint(validated_sweep)
    if not _fingerprint(fingerprint):
        raise ValueError("fingerprint rejected")
    return fingerprint


def _validate_signature(value: Any) -> str:
    if (
        not isinstance(value, str)
        or not value.startswith("0x")
        or len(value) != 132
    ):
        raise ValueError("signature rejected")
    try:
        int(value[2:], 16)
    except ValueError:
        raise ValueError("signature rejected") from None
    return value


async def _execute(
    dependencies: _Dependencies,
    ledger: DurableCounterLedger,
    source: Any,
    transport: Any,
) -> None:
    if dependencies.lifecycle_clear() is not True:
        raise ValueError("lifecycle rejected")
    validator = PrivateReadPreflight(
        ledger,  # validation helpers do not access the legacy store
        clock=dependencies.clock,
        public_get=lambda *_args: None,
        lifecycle_clear=lambda: True,
    )
    fingerprint_a = await _public_barrier(
        ledger, validator, transport, "a", dependencies.crash_hook,
    )
    ledger.set_claimed(fingerprint_a)
    private_started = validator._now()

    await _phase(
        ledger, "source_load", source.load, _none, dependencies.crash_hook,
    )
    capability = await _phase(
        ledger,
        "capability_open",
        lambda: _open_exact_capability(source),
        _identity,
        dependencies.crash_hook,
    )
    abrupt = False
    try:
        signer = await _phase(
            ledger,
            "signer_derive",
            capability.derive_signer_address,
            lambda value: value if isinstance(value, str) and value.lower() == SIGNER else (
                _raise("credential capability rejected")
            ),
            dependencies.crash_hook,
        )
        del signer
        nonce_response = await _phase(
            ledger,
            "nonce_get",
            transport.nonce_get,
            lambda value: validator._validate_response(
                "/v1/auth/nonce", (("account", ACCOUNT),), value,
            ),
            dependencies.crash_hook,
        )
        nonce = PrivateReadPreflight._nonce(
            _exact_mapping(nonce_response, {"nonce"})["nonce"]
        )
        typed_data = await _phase(
            ledger,
            "register_v2_construct",
            lambda: PrivateReadPreflight._typed_data(nonce),
            lambda value: value if value == PrivateReadPreflight._typed_data(nonce) else (
                _raise("typed data rejected")
            ),
            dependencies.crash_hook,
        )
        signature = await _phase(
            ledger,
            "sign_register_v2",
            lambda: capability.sign_register_v2(typed_data),
            _validate_signature,
            dependencies.crash_hook,
        )
        auth_frame = {
            "method": "auth_v2",
            "params": {
                "account": ACCOUNT,
                "signer": SIGNER,
                "message": "sign in with RISEx",
                "nonce": nonce,
                "expiration": int(dependencies.clock()) + 365 * 24 * 60 * 60,
                "signature": signature,
            },
        }
        await _phase(
            ledger,
            "auth_v2_dispatch",
            lambda: transport.auth_v2_dispatch(auth_frame),
            _none,
            dependencies.crash_hook,
        )
        auth_raw = await _phase(
            ledger,
            "auth_v2_receive",
            transport.auth_v2_receive,
            _identity,
            dependencies.crash_hook,
        )
        auth_parsed = await _phase(
            ledger,
            "auth_v2_parse",
            lambda: _parse_auth_v2(auth_raw),
            _identity,
            dependencies.crash_hook,
        )
        try:
            auth_validated = await _phase(
                ledger,
                "auth_v2_validate",
                lambda: _validate_auth_v2_schema(auth_parsed),
                _identity,
                dependencies.crash_hook,
            )
        except _AuthV2Failure as failure:
            if failure.reason == "auth_v2_schema_invalid":
                ledger.record_auth_v2_shape(*_auth_v2_shape(auth_parsed))
            raise
        await _phase(
            ledger,
            "auth_v2_status",
            lambda: _require_auth_v2_success(auth_validated),
            _identity,
            dependencies.crash_hook,
        )
        await _phase(
            ledger,
            "orders_subscribe",
            transport.orders_subscribe,
            _none,
            dependencies.crash_hook,
        )
        await _phase(
            ledger,
            "orders_snapshot",
            transport.orders_snapshot,
            lambda value: (
                PrivateReadPreflight._validate_private_snapshot(
                    value,
                    channel="orders",
                    count_field="order_count",
                    now=dependencies.clock(),
                ),
                value,
            )[1],
            dependencies.crash_hook,
        )
        await _phase(
            ledger,
            "positions_subscribe",
            transport.positions_subscribe,
            _none,
            dependencies.crash_hook,
        )
        await _phase(
            ledger,
            "positions_snapshot",
            transport.positions_snapshot,
            lambda value: _validate_positions_snapshot(
                value, validator._now(), private_started,
            ),
            dependencies.crash_hook,
        )
    except _SimulatedProcessDeath:
        abrupt = True
        raise
    finally:
        if not abrupt:
            await _phase(
                ledger,
                "capability_close",
                capability.close,
                _none,
                dependencies.crash_hook,
            )
    ledger.set_observed()

    fingerprint_b = await _public_barrier(
        ledger, validator, transport, "b", dependencies.crash_hook,
    )
    ledger.set_barrier_b(fingerprint_b)
    await _phase(
        ledger,
        "final_agreement",
        lambda: (fingerprint_a, fingerprint_b),
        lambda value: value if value[0] == value[1] else _raise("barrier disagreement"),
        dependencies.crash_hook,
    )


def _raise(message: str) -> Any:
    raise ValueError(message)


def _validate_positions_snapshot(value: Any, now: float, started_at: float) -> Any:
    PrivateReadPreflight._validate_private_snapshot(
        value,
        channel="positions",
        count_field="position_count",
        now=now,
    )
    elapsed = now - started_at
    if not math.isfinite(elapsed) or elapsed < 0 or elapsed > MAX_AGE_SECONDS:
        raise ValueError("private observation stale")
    return value


def _open_exact_capability(source: Any) -> Any:
    capability = source.open()
    if _capability_surface_is_exact(capability):
        return capability
    try:
        capability.close()
    except Exception:
        pass
    raise ValueError("credential capability rejected")


def _exact_mapping(value: Any, keys: set[str]) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != keys:
        raise ValueError("response rejected")
    return value


async def _run(dependencies: _Dependencies) -> OperationalReport:
    try:
        ledger = DurableCounterLedger(dependencies.path, dependencies.invocation_id)
    except _StoreRejected:
        return OperationalReport(
            SCHEMA_VERSION,
            Result.UNKNOWN,
            dependencies.invocation_id,
            _path_hash(dependencies.path),
            Result.UNKNOWN.value,
            {},
            None,
            None,
            "store_rejected",
            None,
            None,
        )
    source: Any = None
    transport: Any = None
    try:
        if not ledger.created:
            if ledger.state in _TERMINAL_STATES:
                return ledger.report()
            return ledger.finalize(Result.UNKNOWN, "interrupted_nonterminal")
        result = Result.UNKNOWN
        reason = "validation_failed"
        abrupt = False
        try:
            source = dependencies.source_factory()
            transport = dependencies.transport_factory()
            await _execute(dependencies, ledger, source, transport)
            result = Result.PASSED
            reason = "complete"
        except _SimulatedProcessDeath:
            abrupt = True
            raise
        except asyncio.CancelledError:
            result = Result.UNKNOWN
            reason = "cancelled"
        except _AuthV2Failure as failure:
            result = Result.UNKNOWN
            reason = failure.reason
        except Exception:
            result = Result.UNKNOWN if ledger.has_mismatch() else Result.BLOCKED
            reason = "validation_failed"
        finally:
            if source is not None and not abrupt:
                try:
                    source.close()
                except Exception:
                    result = Result.UNKNOWN
                    reason = "validation_failed"
            if transport is not None and not abrupt:
                try:
                    await _maybe_await(transport.close())
                except Exception:
                    result = Result.UNKNOWN
                    reason = "validation_failed"
        return ledger.finalize(result, reason, dependencies.crash_hook)
    finally:
        ledger.close()


def _fixture_operational_private_read(
    *,
    path: Path,
    invocation_id: str,
    source_factory: Callable[[], Any],
    transport_factory: Callable[[], Any],
    clock: Callable[[], float],
    lifecycle_clear: Callable[[], bool] = lambda: True,
    crash_hook: Callable[[str, str], None] | None = None,
) -> _Dependencies:
    if Path(path) == FIXED_STORE_PATH:
        raise ValueError("production store rejected in fixture")
    return _Dependencies(
        path=Path(path),
        invocation_id=invocation_id,
        source_factory=source_factory,
        transport_factory=transport_factory,
        clock=clock,
        lifecycle_clear=lifecycle_clear,
        crash_hook=crash_hook,
    )


async def _run_fixture(dependencies: _Dependencies) -> OperationalReport:
    return await _run(dependencies)


def main() -> int:
    if len(sys.argv) != 1:
        print('{"result":"BLOCKED","reason":"arguments_rejected"}')
        return 1
    report = asyncio.run(OperationalPrivateRead().run())
    print(json.dumps(report.as_dict(), sort_keys=True, separators=(",", ":")))
    return 0 if report.result is Result.PASSED else 1


if __name__ == "__main__":
    raise SystemExit(main())
