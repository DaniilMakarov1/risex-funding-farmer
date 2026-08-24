"""Sealed operational binding for the Extended account-shape witness.

Normal Farmer startup does not import this module.  ``main`` is deliberately a
metadata-only pre-arm; the separately gated operation is the zero-argument
``run`` coroutine and has no path, URL, credential, or transport override.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
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
from typing import Any, Callable, Mapping

import aiohttp

from . import extended_account_shape_witness as core


INVOCATION_ID = "extended-account-shape-witness-20260824-new-op-003"
STORE_BASENAME = (
    ".risex-funding-farmer-extended-account-shape-witness-"
    "20260824-new-op-003.sqlite3"
)
API_KEY_BASENAME = ".risex-funding-farmer-extended-api-key-v1"
EXPECTED_STORE_PATH_SHA256 = (
    "c4c769e78cbfb76ae807510b5d6efbd5f393b15ac8dea4c1f468522fc68cbcdf"
)
REDACTED_STORE_PATH = "<passwd-home>/" + STORE_BASENAME
SCHEMA_VERSION = 1
TIMEOUT_SECONDS = 10
MAX_API_KEY_BYTES = 512
_TABLE = "extended_account_shape_witness_operation"


def _home() -> Path:
    return Path(pwd.getpwuid(os.getuid()).pw_dir)


def _store_path() -> Path:
    path = _home() / STORE_BASENAME
    if hashlib.sha256(os.fsencode(path)).hexdigest() != EXPECTED_STORE_PATH_SHA256:
        raise core.WitnessViolation("FIXED_STORE_IDENTITY_MISMATCH")
    return path


def _file_is_capability(details: os.stat_result) -> bool:
    return (
        stat.S_ISREG(details.st_mode)
        and details.st_uid == os.getuid()
        and stat.S_IMODE(details.st_mode) == 0o600
        and details.st_nlink == 1
        and 0 < details.st_size <= MAX_API_KEY_BYTES
    )


def _open_home_directory() -> int:
    try:
        descriptor = os.open(
            _home(), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        )
        details = os.fstat(descriptor)
        if not stat.S_ISDIR(details.st_mode) or details.st_uid != os.getuid():
            raise core.WitnessViolation("PASSWD_HOME_INVALID")
        return descriptor
    except core.WitnessViolation:
        raise
    except OSError:
        raise core.WitnessViolation("PASSWD_HOME_INVALID") from None


def _credential_metadata() -> None:
    """Inspect the fixed capability without opening or reading its contents."""

    directory = _open_home_directory()
    try:
        try:
            details = os.stat(
                API_KEY_BASENAME, dir_fd=directory, follow_symlinks=False
            )
        except OSError:
            raise core.WitnessViolation("CREDENTIAL_CAPABILITY_UNAVAILABLE") from None
        if not _file_is_capability(details):
            raise core.WitnessViolation("CREDENTIAL_CAPABILITY_INVALID")
    finally:
        os.close(directory)


class _ApiKeyCapability:
    def __init__(self, value: bytearray):
        self._value = value
        self._closed = False

    def x_api_key_header_value(self) -> str:
        if self._closed:
            raise core.WitnessViolation("CREDENTIAL_CLOSED")
        try:
            value = self._value.decode("ascii")
        except UnicodeDecodeError:
            raise core.WitnessViolation("CREDENTIAL_INVALID") from None
        if (
            not value
            or any(ord(character) < 33 or ord(character) > 126 for character in value)
            or value.encode("ascii") != self._value
        ):
            raise core.WitnessViolation("CREDENTIAL_INVALID")
        return value

    def close(self) -> None:
        for index in range(len(self._value)):
            self._value[index] = 0
        self._value.clear()
        self._closed = True


class _PasswdHomeApiKeySource:
    """Open only the fixed API-key basename through the passwd-home dirfd."""

    def open(self) -> _ApiKeyCapability:
        directory = _open_home_directory()
        descriptor: int | None = None
        value = bytearray(MAX_API_KEY_BYTES + 1)
        try:
            try:
                descriptor = os.open(
                    API_KEY_BASENAME,
                    os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=directory,
                )
            except OSError:
                raise core.WitnessViolation(
                    "CREDENTIAL_CAPABILITY_UNAVAILABLE"
                ) from None
            details = os.fstat(descriptor)
            if not _file_is_capability(details):
                raise core.WitnessViolation("CREDENTIAL_CAPABILITY_INVALID")
            count = os.readv(descriptor, [value])
            if count != details.st_size or count > MAX_API_KEY_BYTES:
                raise core.WitnessViolation("CREDENTIAL_CAPABILITY_INVALID")
            del value[count:]
            capability = _ApiKeyCapability(value)
            capability.x_api_key_header_value()
            return capability
        except BaseException:
            for index in range(len(value)):
                value[index] = 0
            value.clear()
            raise
        finally:
            if descriptor is not None:
                os.close(descriptor)
            os.close(directory)


def _strict_json(raw: bytes) -> Any:
    def reject_constant(_: str) -> Any:
        raise ValueError

    def finite_float(value: str) -> float:
        result = float(value)
        if not math.isfinite(result):
            raise ValueError
        return result

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError
            result[key] = value
        return result

    try:
        return json.loads(
            raw.decode("utf-8", errors="strict"),
            parse_constant=reject_constant,
            parse_float=finite_float,
            object_pairs_hook=unique_object,
        )
    except (UnicodeError, ValueError, json.JSONDecodeError):
        raise core.WitnessViolation("STRICT_JSON_INVALID") from None


class _DirectTransport:
    """One verified-TLS, non-redirecting, non-proxied account-info GET."""

    def __init__(self) -> None:
        self._used = False
        trace = aiohttp.TraceConfig()

        async def reject_redirect(*_: Any) -> None:
            raise core.WitnessViolation("HTTP_RESPONSE_INVALID")

        async def reject_second_dispatch(
            _session: Any, context: Any, _params: Any,
        ) -> None:
            if getattr(context, "account_shape_dispatched", False):
                raise core.WitnessViolation("TRANSPORT_REUSE_FORBIDDEN")
            context.account_shape_dispatched = True

        trace.on_request_redirect.append(reject_redirect)
        trace.on_request_headers_sent.append(reject_second_dispatch)
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=TIMEOUT_SECONDS),
            trust_env=False,
            connector=aiohttp.TCPConnector(ssl=ssl.create_default_context()),
            trace_configs=[trace],
        )
        if hasattr(self._session, "_retry_connection"):
            self._session._retry_connection = False

    async def get(self, request: core.AccountInfoRequest) -> dict[str, Any]:
        if self._used:
            raise core.WitnessViolation("TRANSPORT_REUSE_FORBIDDEN")
        self._used = True
        if (
            type(request) is not core.AccountInfoRequest
            or request.method != "GET"
            or request.url != core.ACCOUNT_INFO_URL
            or set(request.headers) != {"X-Api-Key"}
        ):
            raise core.WitnessViolation("REQUEST_CONTRACT_INVALID")
        async with self._session.get(
            core.ACCOUNT_INFO_URL,
            headers=dict(request.headers),
            allow_redirects=False,
            proxy=None,
            ssl=True,
        ) as response:
            if (
                response.status != 200
                or response.history
                or str(response.url) != core.ACCOUNT_INFO_URL
            ):
                raise core.WitnessViolation("HTTP_RESPONSE_INVALID")
            if (
                response.content_length is not None
                and response.content_length > core.BODY_MAX_BYTES
            ):
                raise core.WitnessViolation("BODY_TOO_LARGE")
            raw = await response.content.read(core.BODY_MAX_BYTES + 1)
            if not raw:
                raise core.WitnessViolation("STRICT_JSON_INVALID")
            if len(raw) > core.BODY_MAX_BYTES:
                raise core.WitnessViolation("BODY_TOO_LARGE")
            body = _strict_json(raw)
            return {
                "body": body,
                "body_bytes": len(raw),
                "transport": {
                    "actual_url": str(response.url),
                    "method": "GET",
                    "direct_tls": True,
                    "trust_env": False,
                    "proxy": None,
                    "redirects": 0,
                    "retries": 0,
                },
            }

    async def close(self) -> None:
        await self._session.close()


@dataclass(frozen=True)
class OperationalResult:
    status: str
    reason: str
    counters: Mapping[str, int]
    descriptor: Mapping[str, Any] | None
    invocation_id: str = INVOCATION_ID
    path: str = REDACTED_STORE_PATH
    schema_version: int = SCHEMA_VERSION

    def evidence(self) -> str:
        return json.dumps(
            {
                "counters": dict(self.counters),
                "descriptor": self.descriptor,
                "invocation_id": self.invocation_id,
                "path": self.path,
                "reason": self.reason,
                "schema_version": self.schema_version,
                "status": self.status,
            },
            sort_keys=True,
            separators=(",", ":"),
        )


def _decode_operational_result(raw: Any) -> OperationalResult:
    try:
        value = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        raise core.WitnessViolation("DURABLE_EVIDENCE_INVALID") from None
    expected = {
        "counters", "descriptor", "invocation_id", "path", "reason",
        "schema_version", "status",
    }
    if type(value) is not dict or set(value) != expected:
        raise core.WitnessViolation("DURABLE_EVIDENCE_INVALID")
    counters = core._decode_counters(json.dumps(value["counters"]))
    if (
        value["schema_version"] != SCHEMA_VERSION
        or value["invocation_id"] != INVOCATION_ID
        or value["path"] != REDACTED_STORE_PATH
        or value["status"] not in {"CAPTURED", "BLOCKED", "UNKNOWN"}
        or type(value["reason"]) is not str
        or not value["reason"]
        or (value["descriptor"] is not None and type(value["descriptor"]) is not dict)
    ):
        raise core.WitnessViolation("DURABLE_EVIDENCE_INVALID")
    if value["descriptor"] is not None:
        try:
            core._validate_descriptor(value["descriptor"])
        except core.WitnessViolation:
            raise core.WitnessViolation("DURABLE_EVIDENCE_INVALID") from None
    if value["status"] == "CAPTURED" and (
        value["reason"] != "ACCOUNT_SHAPE_CAPTURED"
        or value["descriptor"] is None
        or set(counters.values()) != {1}
    ):
        raise core.WitnessViolation("DURABLE_EVIDENCE_INVALID")
    return OperationalResult(
        value["status"], value["reason"], counters, value["descriptor"]
    )


class _OperationalStore:
    """Invocation-bound, FULL-synchronous, never-resumable effect ledger."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._fresh_claim_available = self._prepare_file()
        try:
            with self._connect() as connection:
                connection.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {_TABLE} (
                        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                        schema_version INTEGER NOT NULL,
                        invocation_id TEXT NOT NULL,
                        state TEXT NOT NULL,
                        phase TEXT NOT NULL,
                        counters TEXT NOT NULL,
                        evidence TEXT
                    )
                    """
                )
                columns = tuple(
                    row[1] for row in connection.execute(f"PRAGMA table_info({_TABLE})")
                )
                if columns != (
                    "singleton", "schema_version", "invocation_id", "state",
                    "phase", "counters", "evidence",
                ):
                    raise core.WitnessViolation("DURABLE_SCHEMA_INVALID")
                catalog = tuple(connection.execute(
                    "SELECT type,name,tbl_name FROM sqlite_master "
                    "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
                ))
                if catalog != (("table", _TABLE, _TABLE),):
                    raise core.WitnessViolation("DURABLE_SCHEMA_INVALID")
        except sqlite3.DatabaseError:
            raise core.WitnessViolation("DURABLE_STORE_INVALID") from None

    def _prepare_file(self) -> bool:
        created = False
        try:
            descriptor = os.open(
                self.path,
                os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
        except FileExistsError:
            descriptor = None
        except OSError:
            raise core.WitnessViolation("DURABLE_FILE_INVALID") from None
        else:
            created = True
            try:
                os.fchmod(descriptor, 0o600)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            directory = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        self._validate_file()
        return created

    def _validate_file(self) -> os.stat_result:
        try:
            details = self.path.lstat()
        except OSError:
            raise core.WitnessViolation("DURABLE_FILE_INVALID") from None
        if (
            self.path.is_symlink()
            or not stat.S_ISREG(details.st_mode)
            or details.st_uid != os.getuid()
            or stat.S_IMODE(details.st_mode) != 0o600
            or details.st_nlink != 1
        ):
            raise core.WitnessViolation("DURABLE_FILE_INVALID")
        return details

    def _connect(self) -> sqlite3.Connection:
        before = self._validate_file()
        try:
            anchor = os.open(
                self.path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
            )
        except OSError:
            raise core.WitnessViolation("DURABLE_FILE_INVALID") from None
        connection: sqlite3.Connection | None = None
        try:
            anchored = os.fstat(anchor)
            if (anchored.st_dev, anchored.st_ino) != (before.st_dev, before.st_ino):
                raise core.WitnessViolation("DURABLE_FILE_INVALID")
            connection = sqlite3.connect(self.path)
            after = self._validate_file()
            if (after.st_dev, after.st_ino) != (anchored.st_dev, anchored.st_ino):
                raise core.WitnessViolation("DURABLE_FILE_INVALID")
            connection.execute("PRAGMA journal_mode=DELETE")
            connection.execute("PRAGMA synchronous=FULL")
            return connection
        except BaseException:
            if connection is not None:
                connection.close()
            raise
        finally:
            os.close(anchor)

    def claim(self) -> core.WitnessResult | None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                f"SELECT schema_version,invocation_id,state,phase,counters,evidence "
                f"FROM {_TABLE} WHERE singleton=1"
            ).fetchone()
            if row is None:
                if not self._fresh_claim_available:
                    counters = core._empty_counters()
                    result = OperationalResult(
                        "UNKNOWN", "INTERRUPTED_BEFORE_CLAIM", counters, None
                    )
                    connection.execute(
                        f"INSERT INTO {_TABLE} VALUES (1,?,?,?,?,?,?)",
                        (
                            SCHEMA_VERSION, INVOCATION_ID, "TERMINAL", "TERMINAL",
                            json.dumps(counters, sort_keys=True), result.evidence(),
                        ),
                    )
                    return core.WitnessResult(
                        result.status, result.reason, "TERMINAL", counters, None
                    )
                self._fresh_claim_available = False
                connection.execute(
                    f"INSERT INTO {_TABLE} VALUES (1,?,?,?,?,?,NULL)",
                    (
                        SCHEMA_VERSION, INVOCATION_ID, "RUNNING", "STARTED",
                        json.dumps(core._empty_counters(), sort_keys=True),
                    ),
                )
                return None
        schema, invocation, state, phase, raw_counters, evidence = row
        if schema != SCHEMA_VERSION or invocation != INVOCATION_ID:
            raise core.WitnessViolation("DURABLE_IDENTITY_MISMATCH")
        counters = core._decode_counters(raw_counters)
        if state == "TERMINAL" and type(evidence) is str:
            result = _decode_operational_result(evidence)
            if dict(result.counters) != counters or phase != "TERMINAL":
                raise core.WitnessViolation("DURABLE_EVIDENCE_INVALID")
            return core.WitnessResult(
                result.status, result.reason, "TERMINAL", counters, result.descriptor
            )
        if state == "RUNNING" and evidence is None:
            return core.WitnessResult(
                "UNKNOWN", "INTERRUPTED_RUNNING", phase, counters, None
            )
        raise core.WitnessViolation("DURABLE_STATE_INVALID")

    def increment(self, effect: str, suffix: str) -> None:
        if effect not in core.EFFECTS or suffix not in {"attempts", "completions"}:
            raise core.WitnessViolation("DURABLE_COUNTERS_INVALID")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                f"SELECT state,counters FROM {_TABLE} WHERE singleton=1"
            ).fetchone()
            if row is None or row[0] != "RUNNING":
                raise core.WitnessViolation("DURABLE_STATE_CONFLICT")
            counters = core._decode_counters(row[1])
            key = f"{effect}_{suffix}"
            if suffix == "attempts" and counters[key] != 0:
                raise core.WitnessViolation("EFFECT_REPLAY_FORBIDDEN")
            if suffix == "completions" and (
                counters[key] != 0 or counters[f"{effect}_attempts"] != 1
            ):
                raise core.WitnessViolation("DURABLE_COUNTERS_INVALID")
            counters[key] += 1
            connection.execute(
                f"UPDATE {_TABLE} SET phase=?,counters=? WHERE singleton=1",
                (effect.upper(), json.dumps(counters, sort_keys=True)),
            )

    def snapshot(self) -> tuple[str, dict[str, int]]:
        with self._connect() as connection:
            row = connection.execute(
                f"SELECT phase,counters FROM {_TABLE} WHERE singleton=1"
            ).fetchone()
        if row is None:
            raise core.WitnessViolation("DURABLE_STATE_INVALID")
        return row[0], core._decode_counters(row[1])

    def terminal(
        self,
        result: core.WitnessResult,
        hook: Callable[[str, str], None] | None = None,
    ) -> core.WitnessResult:
        if hook is not None:
            hook("terminal", "before_attempt")
        self.increment("terminal", "attempts")
        if hook is not None:
            hook("terminal", "after_attempt")
            hook("terminal", "before_completion")
        _, counters = self.snapshot()
        counters["terminal_completions"] = 1
        operational = OperationalResult(
            result.status, result.reason, counters, result.descriptor
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            changed = connection.execute(
                f"UPDATE {_TABLE} SET state='TERMINAL',phase='TERMINAL',"
                "counters=?,evidence=? WHERE singleton=1 AND state='RUNNING'",
                (json.dumps(counters, sort_keys=True), operational.evidence()),
            ).rowcount
            if changed != 1:
                raise core.WitnessViolation("DURABLE_STATE_CONFLICT")
        if hook is not None:
            hook("terminal", "after_completion")
        return core.WitnessResult(
            result.status, result.reason, "TERMINAL", counters, result.descriptor
        )


def _as_operational(result: core.WitnessResult) -> OperationalResult:
    return OperationalResult(
        result.status, result.reason, result.counters, result.descriptor
    )


async def _fixture_run(
    *, store: _OperationalStore, credential_source: Any, transport: Any,
    _effect_hook: Callable[[str, str], None] | None = None,
) -> OperationalResult:
    """Synthetic seam; the production constructor exposes no substitutions."""

    result = await core._run_fixture_account_shape_witness(
        store=store,
        credential_source=credential_source,
        transport=transport,
        _effect_hook=_effect_hook,
    )
    return _as_operational(result)


async def run() -> OperationalResult:
    """Execute the fixed one-shot operation; requires a separate Chief gate."""

    store = _OperationalStore(_store_path())
    transport = _DirectTransport()
    try:
        return await _fixture_run(
            store=store,
            credential_source=_PasswdHomeApiKeySource(),
            transport=transport,
        )
    finally:
        await transport.close()


def prearm() -> OperationalResult:
    """Verify fixed metadata only; read no credential and create no store."""

    path = _store_path()
    try:
        path.lstat()
    except FileNotFoundError:
        pass
    except OSError:
        raise core.WitnessViolation("DURABLE_STORE_UNAVAILABLE") from None
    else:
        raise core.WitnessViolation("DURABLE_STORE_ALREADY_EXISTS")
    _credential_metadata()
    return OperationalResult(
        "READY", "PREARM_READY", core._empty_counters(), None
    )


def main() -> int:
    if len(sys.argv) != 1:
        print(OperationalResult(
            "BLOCKED", "ARGUMENTS_FORBIDDEN", core._empty_counters(), None
        ).evidence())
        return 2
    try:
        result = prearm()
    except core.WitnessViolation as exc:
        result = OperationalResult(
            "BLOCKED", str(exc), core._empty_counters(), None
        )
    except BaseException:
        result = OperationalResult(
            "BLOCKED", "PREARM_UNRESOLVED", core._empty_counters(), None
        )
    print(result.evidence())
    return 0 if result.status == "READY" else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["INVOCATION_ID", "prearm", "run", "main"]
