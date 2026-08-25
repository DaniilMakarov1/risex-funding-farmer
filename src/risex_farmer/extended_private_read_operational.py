"""Explicit one-shot Extended Sepolia private-read operational binding.

This module is intentionally absent from normal package startup.  Its production
entry has no configuration arguments and allocates a fresh runtime run identity
in one fixed protected journal.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import pwd
import secrets
import sqlite3
import stat
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

import aiohttp

from .extended_private_read_preflight import (
    REST_BASE_URL,
    STREAM_URL,
    PreflightResult,
    PreflightViolation,
    _Counters,
    _execute,
)


STORE_BASENAME = ".risex-funding-farmer-extended-private-read-runs-v1.sqlite3"
API_KEY_BASENAME = ".risex-funding-farmer-extended-api-key-v1"
IDENTITY_BASENAME = ".risex-funding-farmer-extended-identity-v1.json"
SCHEMA_VERSION = 3
_TIMEOUT_SECONDS = 10
_MAX_JSON_BYTES = 1_048_576
_IDENTITY_KEYS = {"id", "accountIndex", "l2Key", "l2Vault"}
_EFFECTS = (
    "loader",
    "rest_a_info", "rest_a_orders", "rest_a_positions",
    "stream_open", "stream_upgrade",
    "rest_b_info", "rest_b_orders", "rest_b_positions",
    "barrier_request", "barrier_validation", "stream_close",
    "terminal_persistence",
)


def _new_runtime_run_id() -> str:
    return "extended-read-" + secrets.token_hex(16)


def _config_hash(invocation_id: str) -> str:
    return hashlib.sha256(
        json.dumps(
            {
                "invocation": invocation_id,
                "rest_base": REST_BASE_URL,
                "rest_paths": [
                    "/user/account/info", "/user/orders", "/user/positions",
                ],
                "stream": STREAM_URL,
                "timeout_seconds": _TIMEOUT_SECONDS,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def _empty_counters() -> dict[str, int]:
    return {
        f"{effect}_{suffix}": 0
        for effect in _EFFECTS
        for suffix in ("attempts", "completions")
    }


def _decode_counters(raw: Any) -> dict[str, int]:
    try:
        value = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise PreflightViolation("DURABLE_COUNTERS_INVALID") from exc
    expected = set(_empty_counters())
    if (
        type(value) is not dict
        or set(value) != expected
        or any(type(item) is not int or item not in {0, 1} for item in value.values())
        or any(
            value[f"{effect}_completions"] > value[f"{effect}_attempts"]
            for effect in _EFFECTS
        )
    ):
        raise PreflightViolation("DURABLE_COUNTERS_INVALID")
    return value


@dataclass(frozen=True)
class OperationalResult:
    status: str
    reason: str
    phase: str
    counters: Mapping[str, int]
    rest_calls: int
    stream_frames: int
    clock_calls: int
    identity_verified: bool
    invocation_id: str
    config_hash: str

    def evidence(self) -> str:
        return json.dumps(
            {
                "clock_calls": self.clock_calls,
                "config_hash": self.config_hash,
                "counters": dict(self.counters),
                "identity_verified": self.identity_verified,
                "invocation_id": self.invocation_id,
                "phase": self.phase,
                "reason": self.reason,
                "rest_calls": self.rest_calls,
                "status": self.status,
                "stream_frames": self.stream_frames,
            },
            sort_keys=True,
            separators=(",", ":"),
        )


def _decode_result(
    raw: Any, *, invocation_id: str, config_hash: str,
) -> OperationalResult:
    try:
        value = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise PreflightViolation("DURABLE_EVIDENCE_INVALID") from exc
    keys = {
        "clock_calls", "config_hash", "counters", "identity_verified",
        "invocation_id", "phase", "reason", "rest_calls", "status",
        "stream_frames",
    }
    if type(value) is not dict or set(value) != keys:
        raise PreflightViolation("DURABLE_EVIDENCE_INVALID")
    counters = _decode_counters(json.dumps(value["counters"]))
    if (
        value["invocation_id"] != invocation_id
        or value["config_hash"] != config_hash
        or value["status"] not in {"READY", "BLOCKED", "UNKNOWN"}
        or type(value["reason"]) is not str or not value["reason"]
        or type(value["phase"]) is not str or not value["phase"]
        or type(value["identity_verified"]) is not bool
        or any(
            type(value[key]) is not int or value[key] < 0
            for key in ("rest_calls", "stream_frames", "clock_calls")
        )
        or (
            value["status"] == "READY"
            and (
                value["reason"] != "OPERATIONAL_CONTRACT_PROVED"
                or value["rest_calls"] != 6 or value["clock_calls"] != 7
                or value["identity_verified"] is not True
                or set(counters.values()) != {1}
            )
        )
        or (value["status"] == "BLOCKED" and value["identity_verified"] is not False)
    ):
        raise PreflightViolation("DURABLE_EVIDENCE_INVALID")
    return OperationalResult(
        status=value["status"], reason=value["reason"], phase=value["phase"],
        counters=counters, rest_calls=value["rest_calls"],
        stream_frames=value["stream_frames"], clock_calls=value["clock_calls"],
        identity_verified=value["identity_verified"],
        invocation_id=invocation_id, config_hash=config_hash,
    )


def _validate_private_path(path: Path, *, may_create: bool) -> None:
    parent = path.parent
    try:
        parent_stat = parent.stat()
    except OSError as exc:
        raise PreflightViolation("STORE_PARENT_INVALID") from exc
    if (
        not stat.S_ISDIR(parent_stat.st_mode)
        or parent.is_symlink()
        or parent_stat.st_uid != os.getuid()
    ):
        raise PreflightViolation("STORE_PARENT_INVALID")
    try:
        item = path.lstat()
    except FileNotFoundError:
        if not may_create:
            raise PreflightViolation("SECURE_FILE_MISSING")
        return
    if (
        not stat.S_ISREG(item.st_mode)
        or stat.S_IMODE(item.st_mode) != 0o600
        or item.st_uid != os.getuid()
    ):
        raise PreflightViolation("STORE_FILE_INVALID")


class _OperationalStore:
    """Versioned FULL-synchronous, never-resumable effect ledger."""

    def __init__(self, path: str | Path, invocation_id: str = "extended-read-fixture"):
        self.path = Path(path)
        if type(invocation_id) is not str or not invocation_id:
            raise PreflightViolation("DURABLE_IDENTITY_INVALID")
        self.invocation_id = invocation_id
        self.config_hash = _config_hash(invocation_id)
        _validate_private_path(self.path, may_create=True)
        if not self.path.exists():
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            descriptor = os.open(self.path, flags, 0o600)
            os.close(descriptor)
        _validate_private_path(self.path, may_create=False)
        try:
            with self._connect() as connection:
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS extended_private_read_operation (
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
                    row[1] for row in connection.execute(
                        "PRAGMA table_info(extended_private_read_operation)"
                    )
                )
                if columns != (
                    "invocation_id", "schema_version", "config_hash",
                    "state", "phase", "counters", "evidence",
                ):
                    raise PreflightViolation("DURABLE_SCHEMA_INVALID")
                catalog = tuple(
                    connection.execute(
                        """
                        SELECT type,name,tbl_name FROM sqlite_master
                        WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name
                        """
                    )
                )
                if catalog != ((
                    "table", "extended_private_read_operation",
                    "extended_private_read_operation",
                ),):
                    raise PreflightViolation("DURABLE_SCHEMA_INVALID")
        except sqlite3.DatabaseError as exc:
            raise PreflightViolation("DURABLE_STORE_INVALID") from exc

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def claim(self) -> OperationalResult | None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT schema_version, invocation_id, config_hash, state, phase,
                       counters, evidence
                FROM extended_private_read_operation WHERE invocation_id=?
                """,
                (self.invocation_id,),
            ).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO extended_private_read_operation VALUES (?,?,?,?,?,?,NULL)",
                    (
                        self.invocation_id, SCHEMA_VERSION, self.config_hash, "RUNNING",
                        "STARTED", json.dumps(_empty_counters(), sort_keys=True),
                    ),
                )
                return None
        schema, invocation, config, state, phase, raw_counters, evidence = row
        if schema != SCHEMA_VERSION:
            raise PreflightViolation("DURABLE_SCHEMA_INVALID")
        if invocation != self.invocation_id or config != self.config_hash:
            raise PreflightViolation("DURABLE_IDENTITY_MISMATCH")
        counters = _decode_counters(raw_counters)
        if state == "TERMINAL" and type(evidence) is str:
            result = _decode_result(
                evidence, invocation_id=self.invocation_id,
                config_hash=self.config_hash,
            )
            if dict(result.counters) != counters or result.phase != phase:
                raise PreflightViolation("DURABLE_EVIDENCE_INVALID")
            return result
        if state == "RUNNING" and evidence is None:
            return OperationalResult(
                "UNKNOWN", "INTERRUPTED_RUNNING", phase, counters, 0, 0, 0, False,
                self.invocation_id, self.config_hash,
            )
        raise PreflightViolation("DURABLE_STATE_INVALID")

    def increment(self, effect: str, suffix: str) -> None:
        if effect not in _EFFECTS or suffix not in {"attempts", "completions"}:
            raise PreflightViolation("DURABLE_COUNTERS_INVALID")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT state,counters FROM extended_private_read_operation "
                "WHERE invocation_id=?",
                (self.invocation_id,),
            ).fetchone()
            if row is None or row[0] != "RUNNING":
                raise PreflightViolation("DURABLE_STATE_CONFLICT")
            counters = _decode_counters(row[1])
            key = f"{effect}_{suffix}"
            if suffix == "attempts" and counters[key] != 0:
                raise PreflightViolation("EFFECT_REPLAY_FORBIDDEN")
            if suffix == "completions" and (
                counters[key] != 0 or counters[f"{effect}_attempts"] != 1
            ):
                raise PreflightViolation("DURABLE_COUNTERS_INVALID")
            counters[key] += 1
            connection.execute(
                "UPDATE extended_private_read_operation SET phase=?,counters=? "
                "WHERE invocation_id=?",
                (effect.upper(), json.dumps(counters, sort_keys=True), self.invocation_id),
            )

    def snapshot(self) -> tuple[str, dict[str, int]]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT phase,counters FROM extended_private_read_operation "
                "WHERE invocation_id=?",
                (self.invocation_id,),
            ).fetchone()
        if row is None:
            raise PreflightViolation("DURABLE_STATE_INVALID")
        return row[0], _decode_counters(row[1])

    def terminal(
        self, result: OperationalResult,
        hook: Callable[[str, str], None] | None = None,
    ) -> OperationalResult:
        if hook is not None:
            hook("terminal_persistence", "before_attempt")
        self.increment("terminal_persistence", "attempts")
        if hook is not None:
            hook("terminal_persistence", "after_attempt")
        phase, counters = self.snapshot()
        terminal = OperationalResult(
            result.status, result.reason, "TERMINAL", counters,
            result.rest_calls, result.stream_frames, result.clock_calls,
            result.identity_verified, self.invocation_id, self.config_hash,
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if hook is not None:
                hook("terminal_persistence", "before_completion")
            counters["terminal_persistence_completions"] = 1
            terminal = OperationalResult(
                terminal.status, terminal.reason, "TERMINAL", counters,
                terminal.rest_calls, terminal.stream_frames, terminal.clock_calls,
                terminal.identity_verified, self.invocation_id, self.config_hash,
            )
            changed = connection.execute(
                """
                UPDATE extended_private_read_operation
                SET state='TERMINAL',phase='TERMINAL',counters=?,evidence=?
                WHERE invocation_id=? AND state='RUNNING'
                """,
                (
                    json.dumps(counters, sort_keys=True), terminal.evidence(),
                    self.invocation_id,
                ),
            ).rowcount
            if changed != 1:
                raise PreflightViolation("DURABLE_STATE_CONFLICT")
        if hook is not None:
            hook("terminal_persistence", "after_completion")
        return terminal


class _EffectLedger:
    def __init__(
        self, store: _OperationalStore,
        hook: Callable[[str, str], None] | None = None,
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

    def complete(self, effect: str) -> None:
        self._hook(effect, "before_completion")
        self.store.increment(effect, "completions")
        self._hook(effect, "after_completion")

    def observed(self, effect: str) -> None:
        self._hook(effect, "after_effect")


class _LoadedSource:
    def __init__(self, source: Any):
        self.source = source
        self.capability: Any = None

    async def load(self) -> str:
        capability = self.source.open()
        if asyncio.iscoroutine(capability):
            capability = await capability
        required = (
            "x_api_key_header_value", "matches_account",
            "matches_spot_account_id", "close",
        )
        if capability is None or any(not callable(getattr(capability, item, None)) for item in required):
            raise PreflightViolation("CREDENTIAL_CAPABILITY_INVALID")
        self.capability = capability
        value = capability.x_api_key_header_value()
        if type(value) is not str or not value:
            raise PreflightViolation("CREDENTIAL_INVALID")
        return value

    def matches_account(self, account: Mapping[str, Any]) -> bool:
        return self.capability is not None and self.capability.matches_account(account) is True

    def matches_spot_account_id(self, account_id: Any) -> bool:
        return (
            self.capability is not None
            and self.capability.matches_spot_account_id(account_id) is True
        )

    async def close(self) -> None:
        if self.capability is None:
            return
        value = self.capability.close()
        if asyncio.iscoroutine(value):
            await value


async def _run_fixture_operational_private_read(
    *, store: _OperationalStore, credential_source: Any, transport: Any,
    clock_ms: Callable[[], int],
    _effect_hook: Callable[[str, str], None] | None = None,
) -> OperationalResult:
    """Private fixture seam; production construction below exposes no overrides."""

    existing = store.claim()
    if existing is not None:
        return existing
    ledger = _EffectLedger(store, _effect_hook)
    loaded = _LoadedSource(credential_source)
    counters = _Counters()
    result: OperationalResult | None = None
    cancelled = False
    try:
        proved: PreflightResult = await _execute(
            loaded.load, transport, clock_ms, counters, loaded, ledger
        )
        result = OperationalResult(
            "READY", "OPERATIONAL_CONTRACT_PROVED", "FINALIZING",
            store.snapshot()[1], proved.rest_calls, proved.stream_frames,
            proved.clock_calls, True, store.invocation_id, store.config_hash,
        )
    except asyncio.CancelledError:
        cancelled = True
        result = OperationalResult(
            "BLOCKED", "CANCELLED", store.snapshot()[0], store.snapshot()[1],
            counters.rest_calls, counters.stream_frames, counters.clock_calls, False,
            store.invocation_id, store.config_hash,
        )
    except PreflightViolation as exc:
        result = OperationalResult(
            "BLOCKED", str(exc), store.snapshot()[0], store.snapshot()[1],
            counters.rest_calls, counters.stream_frames, counters.clock_calls, False,
            store.invocation_id, store.config_hash,
        )
    except Exception:
        result = OperationalResult(
            "BLOCKED", "UNEXPECTED_FAILURE", store.snapshot()[0], store.snapshot()[1],
            counters.rest_calls, counters.stream_frames, counters.clock_calls, False,
            store.invocation_id, store.config_hash,
        )
    finally:
        try:
            await loaded.close()
        except Exception:
            if result is None or result.status == "READY":
                result = OperationalResult(
                    "BLOCKED", "CAPABILITY_CLOSE_FAILED", store.snapshot()[0],
                    store.snapshot()[1], counters.rest_calls, counters.stream_frames,
                    counters.clock_calls, False, store.invocation_id,
                    store.config_hash,
                )
    if result is None:
        raise PreflightViolation("UNEXPECTED_FAILURE")
    terminal = store.terminal(result, _effect_hook)
    if cancelled:
        raise asyncio.CancelledError
    return terminal


def _passwd_home() -> Path:
    return Path(pwd.getpwuid(os.getuid()).pw_dir)


def _read_secure_file(path: Path, maximum: int) -> bytearray:
    if path.parent != _passwd_home():
        raise PreflightViolation("SECURE_FILE_LOCATION_INVALID")
    try:
        before = path.lstat()
    except FileNotFoundError as exc:
        raise PreflightViolation("SECURE_FILE_MISSING") from exc
    if (
        not stat.S_ISREG(before.st_mode) or stat.S_IMODE(before.st_mode) != 0o600
        or before.st_uid != os.getuid()
    ):
        raise PreflightViolation("SECURE_FILE_INVALID")
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise PreflightViolation("SECURE_FILE_INVALID") from exc
    try:
        current = os.fstat(descriptor)
        if (
            not stat.S_ISREG(current.st_mode)
            or stat.S_IMODE(current.st_mode) != 0o600
            or current.st_uid != os.getuid()
            or (current.st_dev, current.st_ino) != (before.st_dev, before.st_ino)
        ):
            raise PreflightViolation("SECURE_FILE_INVALID")
        value = os.read(descriptor, maximum + 1)
        if not value or len(value) > maximum or os.read(descriptor, 1):
            raise PreflightViolation("SECURE_FILE_INVALID")
        return bytearray(value)
    finally:
        os.close(descriptor)


class _LocalCapability:
    def __init__(self, key: bytearray, identity: Mapping[str, Any]):
        self._key = key
        self._identity = dict(identity)
        self._closed = False

    def x_api_key_header_value(self) -> str:
        if self._closed:
            raise PreflightViolation("CREDENTIAL_CLOSED")
        try:
            value = bytes(self._key).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise PreflightViolation("CREDENTIAL_INVALID") from exc
        if not value or value.strip() != value or any(ord(char) < 33 or ord(char) > 126 for char in value):
            raise PreflightViolation("CREDENTIAL_INVALID")
        return value

    def matches_account(self, account: Mapping[str, Any]) -> bool:
        return (
            not self._closed
            and account.get("id") == self._identity["id"]
            and account.get("accountIndex") == self._identity["accountIndex"]
            and account.get("l2Key") == self._identity["l2Key"]
            and account.get("l2Vault") == self._identity["l2Vault"]
        )

    def matches_spot_account_id(self, account_id: Any) -> bool:
        return not self._closed and account_id == self._identity["id"]

    def close(self) -> None:
        for index in range(len(self._key)):
            self._key[index] = 0
        self._identity.clear()
        self._closed = True


class _PasswdHomeCredentialSource:
    """The sole production source; it accepts no path or identity arguments."""

    def open(self) -> _LocalCapability:
        home = _passwd_home()
        raw_identity = _read_secure_file(home / IDENTITY_BASENAME, 2048)
        try:
            identity = json.loads(raw_identity)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PreflightViolation("IDENTITY_FILE_INVALID") from exc
        finally:
            for index in range(len(raw_identity)):
                raw_identity[index] = 0
        if (
            type(identity) is not dict or set(identity) != _IDENTITY_KEYS
            or type(identity["id"]) is not int or identity["id"] < 0
            or type(identity["accountIndex"]) is not int or identity["accountIndex"] < 0
            or type(identity["l2Key"]) is not str or not identity["l2Key"]
            or type(identity["l2Vault"]) is not int or identity["l2Vault"] < 0
        ):
            raise PreflightViolation("IDENTITY_FILE_INVALID")
        key = _read_secure_file(home / API_KEY_BASENAME, 512)
        return _LocalCapability(key, identity)


def _transport_metadata(
    url: str, *, header_names: tuple[str, ...] | None = None
) -> dict[str, Any]:
    reported_header_names = (
        list(header_names)
        if header_names is not None
        else ["Accept", "Content-Type", "User-Agent", "X-Api-Key"]
    )
    return {
        "actual_url": url, "method": "GET",
        "header_names": reported_header_names,
        "direct_tls": True, "trust_env": False, "proxy": None,
        "redirects": 0, "retries": 0, "fallbacks": 0,
        "api_key_header_count": 1, "authorization_present": False,
        "credential_in_query": False, "credential_in_body": False,
        "application_frames_sent": False,
    }


class _DirectStream:
    _END = object()

    def __init__(
        self,
        socket: aiohttp.ClientWebSocketResponse,
        request_header_names: tuple[str, ...],
    ):
        self._socket = socket
        self._queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=64)
        self._reader = asyncio.create_task(self._read())
        self._request_header_names = request_header_names
        self.upgrade_metadata = _transport_metadata(
            STREAM_URL, header_names=request_header_names
        )
        self.outbound_frames: list[Any] = []
        self.reconnect_count = 0
        self.closed = False
        self.barrier_complete = False
        self._barrier_payload: bytes | None = None
        self._barrier_pong = asyncio.Event()

    async def _read(self) -> None:
        try:
            while True:
                message = await self._socket.receive(timeout=_TIMEOUT_SECONDS)
                if message.type == aiohttp.WSMsgType.TEXT:
                    if len(message.data.encode()) > _MAX_JSON_BYTES:
                        raise PreflightViolation("STREAM_MALFORMED_FRAME")
                    await self._queue.put(json.loads(message.data))
                elif message.type == aiohttp.WSMsgType.PING:
                    await self._socket.pong(message.data)
                elif (
                    message.type == aiohttp.WSMsgType.PONG
                    and self._barrier_payload is not None
                    and message.data == self._barrier_payload
                ):
                    self._barrier_pong.set()
                elif message.type in {
                    aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.ERROR,
                }:
                    raise ConnectionError("stream ended")
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            try:
                self._queue.put_nowait(exc)
            except asyncio.QueueFull:
                pass

    async def recv(self) -> Any:
        item = await self._queue.get()
        if item is self._END:
            raise StopAsyncIteration
        if isinstance(item, BaseException):
            raise item
        return item

    async def final_barrier(self) -> dict[str, Any]:
        self._barrier_payload = os.urandom(16)
        await self._socket.ping(self._barrier_payload)
        await asyncio.wait_for(self._barrier_pong.wait(), timeout=_TIMEOUT_SECONDS)
        self.barrier_complete = True
        await self._queue.put(self._END)
        return {
            "connected": not self._socket.closed,
            "same_connection": True,
            "outbound_frames": [], "reconnect_count": 0, "frames": [],
            "transport": _transport_metadata(
                STREAM_URL, header_names=self._request_header_names
            ),
            "observed_at_ms": int(time.time() * 1000),
        }

    async def close(self) -> None:
        self.closed = True
        if not self._reader.done():
            self._reader.cancel()
        await asyncio.gather(self._reader, return_exceptions=True)
        await self._socket.close()


class _DirectTransport:
    def __init__(self):
        timeout = aiohttp.ClientTimeout(total=_TIMEOUT_SECONDS)
        trace = aiohttp.TraceConfig()

        async def reject_redirect(*_: Any) -> None:
            raise PreflightViolation("REDIRECT_FORBIDDEN")

        trace.on_request_redirect.append(reject_redirect)
        self._session = aiohttp.ClientSession(
            timeout=timeout, trust_env=False, trace_configs=[trace]
        )

    async def get(self, request: Any) -> dict[str, Any]:
        async with self._session.get(
            request.url, headers=dict(request.headers), allow_redirects=False,
            proxy=None,
        ) as response:
            raw = await response.content.read(_MAX_JSON_BYTES + 1)
            if len(raw) > _MAX_JSON_BYTES:
                raise PreflightViolation("REST_REPLY_MALFORMED")
            try:
                body = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise PreflightViolation("REST_REPLY_MALFORMED") from exc
            return {
                "status": response.status, "final_url": str(response.url),
                "observed_at_ms": int(time.time() * 1000), "body": body,
                "transport": _transport_metadata(str(response.url)),
            }

    async def open_stream(self, request: Any) -> _DirectStream:
        socket = await self._session.ws_connect(
            request.url, headers=dict(request.headers), proxy=None,
            timeout=_TIMEOUT_SECONDS, autoclose=False, autoping=False,
            max_msg_size=_MAX_JSON_BYTES,
        )
        if str(socket._response.url) != request.url:
            await socket.close()
            raise PreflightViolation("STREAM_REDIRECT_FORBIDDEN")
        return _DirectStream(socket, tuple(request.headers))

    async def close(self) -> None:
        await self._session.close()


async def _production_run() -> OperationalResult:
    home = _passwd_home()
    store_path = home / STORE_BASENAME
    if str(store_path) != "/Users/daniilmakarov/" + STORE_BASENAME:
        raise PreflightViolation("PRODUCTION_HOME_MISMATCH")
    store = _OperationalStore(store_path, _new_runtime_run_id())
    transport = _DirectTransport()
    try:
        return await _run_fixture_operational_private_read(
            store=store, credential_source=_PasswdHomeCredentialSource(),
            transport=transport, clock_ms=lambda: int(time.time() * 1000),
        )
    finally:
        await transport.close()


def main() -> int:
    if len(sys.argv) != 1:
        print("BLOCKED: ARGUMENTS_FORBIDDEN")
        return 2
    try:
        result = asyncio.run(_production_run())
    except Exception as exc:
        reason = str(exc) if isinstance(exc, PreflightViolation) else "UNEXPECTED_FAILURE"
        print(json.dumps({"status": "BLOCKED", "reason": reason}, sort_keys=True))
        return 1
    print(result.evidence())
    return 0 if result.status == "READY" else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["main"]
