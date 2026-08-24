"""Sealed, one-shot operational binding for the accepted RISEx private read.

This module is intentionally absent from normal startup and exposes no trading API.
Its operational constructor has no path, URL, session, proxy, or credential override.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
import os
from pathlib import Path
import pwd
import ssl
import stat
import tempfile
import time
from typing import Any, Awaitable, Callable, Sequence

import aiohttp

from .testnet_risex_order_lifecycle import DurableIntentStore
from .testnet_risex_private_read_preflight import (
    ACCOUNT, AUTHORIZATION, HttpResponse, Outcome, PreflightResult,
    PrivateReadPreflight, PrivateReadStore, REST_ORIGIN, ROUTER, SIGNER,
    SyntheticCredential, WS_ORIGIN, expected_url,
)
from . import testnet_risex_signer as _signer


_LIFECYCLE = ".risex-funding-farmer-testnet-order-lifecycle-v1.sqlite"
_ATTEMPT = ".risex-funding-farmer-risex-private-read-attempt-v1.json"
_PREFLIGHT = ".risex-funding-farmer-risex-private-read-v1.sqlite"


def _home() -> Path:
    return Path(pwd.getpwuid(os.getuid()).pw_dir)


def _safe_file(path: Path) -> bool:
    try:
        details = path.lstat()
    except OSError:
        return False
    return (
        stat.S_ISREG(details.st_mode) and not path.is_symlink()
        and details.st_uid == os.getuid() and stat.S_IMODE(details.st_mode) == 0o600
        and details.st_nlink == 1
    )


class SealedTransport:
    REST_ORIGIN = REST_ORIGIN
    WS_URL = WS_ORIGIN
    TRUST_ENV = False
    ALLOW_REDIRECTS = False
    MAX_BYTES = 1_048_576
    MAX_FRAMES = 3
    DEADLINE_SECONDS = 5

    @staticmethod
    def _allowed_url(path: str, query: Sequence[tuple[str, str]]) -> str:
        allowed = {
            expected_url(request_path, request_query)
            for request_path, request_query in PrivateReadPreflight._REQUESTS
        }
        allowed.add(expected_url("/v1/auth/nonce", (("account", ACCOUNT),)))
        target = expected_url(path, query)
        if target not in allowed:
            raise ValueError("HTTP request surface rejected")
        return target

    def __init__(self) -> None:
        timeout = aiohttp.ClientTimeout(total=self.DEADLINE_SECONDS,
                                        connect=2, sock_read=3)
        self._session = aiohttp.ClientSession(
            timeout=timeout, trust_env=False,
            connector=aiohttp.TCPConnector(ssl=ssl.create_default_context()),
        )

    async def public_get(self, path: str, query: Sequence[tuple[str, str]]) -> HttpResponse:
        target = self._allowed_url(path, query)
        async with self._session.get(target, allow_redirects=False, proxy=None) as response:
            final = str(response.url)
            redirected = bool(response.history) or final != target
            if redirected:
                raise ValueError("redirect or final URL rejected")
            body = await self._bounded_body(response)
            return HttpResponse(response.status, final, body, time.time(), redirected)

    async def _bounded_body(self, response: aiohttp.ClientResponse) -> Any:
        declared = response.content_length
        if declared is not None and declared > self.MAX_BYTES:
            raise ValueError("bounded response rejected")
        raw = await response.content.read(self.MAX_BYTES + 1)
        if len(raw) > self.MAX_BYTES:
            raise ValueError("bounded response rejected")
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            raise ValueError("strict JSON rejected") from None

    async def _private_exchange(self, url: str, outbound: Sequence[dict[str, Any]]) -> tuple[Any, ...]:
        if url != self.WS_URL or len(outbound) != self.MAX_FRAMES:
            raise ValueError("sealed websocket rejected")
        if (
            set(outbound[0]) != {"method", "params"}
            or outbound[0].get("method") != "auth_v2"
            or outbound[1:] != (
                {"method": "subscribe", "params": {"channel": "orders"}},
                {"method": "subscribe", "params": {"channel": "positions"}},
            )
        ):
            raise ValueError("websocket sequence rejected")
        async with self._session.ws_connect(
            self.WS_URL, ssl=ssl.create_default_context(), proxy=None,
            autoclose=False, autoping=False, max_msg_size=self.MAX_BYTES,
        ) as socket:
            frames = []
            for sent in outbound:
                await socket.send_json(sent)
                incoming = await socket.receive(timeout=self.DEADLINE_SECONDS)
                if incoming.type is not aiohttp.WSMsgType.TEXT:
                    raise ValueError("websocket frame rejected")
                try:
                    frames.append(json.loads(incoming.data))
                except Exception:
                    raise ValueError("websocket JSON rejected") from None
            try:
                extra = await socket.receive(timeout=0.01)
            except asyncio.TimeoutError:
                extra = None
            if extra is not None:
                raise ValueError("extra websocket frame rejected")
            await socket.close()
            return tuple(frames)

    async def close(self) -> None:
        await self._session.close()


class LifecycleClearBinding:
    """Read-only pristine predicate after one canonical empty initialization."""

    def __init__(self) -> None:
        self._path = _home() / _LIFECYCLE
        self._allow_initialize = True

    @classmethod
    def _fixture(cls, path: Path) -> "LifecycleClearBinding":
        value = object.__new__(cls)
        value._path = Path(path)
        value._allow_initialize = True
        return value

    def __call__(self) -> bool:
        existed = self._path.exists()
        if not existed and not self._allow_initialize:
            return False
        try:
            if not existed:
                fd = os.open(self._path, os.O_CREAT | os.O_EXCL | os.O_RDWR
                             | getattr(os, "O_NOFOLLOW", 0), 0o600)
                os.fchmod(fd, 0o600); os.fsync(fd); os.close(fd)
                directory = os.open(self._path.parent, os.O_RDONLY)
                try: os.fsync(directory)
                finally: os.close(directory)
            self._allow_initialize = False
            if not _safe_file(self._path):
                return False
            store = DurableIntentStore(self._path)
            try:
                if not existed:
                    store._bind_identities(ACCOUNT, SIGNER, ROUTER, AUTHORIZATION)
                tables = {
                    row[0] for row in store.connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                if tables != {"intents", "cancels", "terminal"}:
                    return False
                intents = store.connection.execute("SELECT COUNT(*) FROM intents").fetchone()[0]
                cancels = store.connection.execute("SELECT COUNT(*) FROM cancels").fetchone()[0]
                terminal = dict(store.connection.execute("SELECT key,value FROM terminal"))
                return intents == 0 and cancels == 0 and terminal == {
                    "account": ACCOUNT, "signer": SIGNER,
                    "router": ROUTER, "authorization": AUTHORIZATION,
                }
            finally:
                store.close()
        except Exception:
            self._allow_initialize = False
            return False


class SessionSignerCredential(SyntheticCredential):
    def __init__(self, signer: str, secret: bytes) -> None:
        super().__init__(signer, b"")
        self._secret = bytearray(secret)

    def sign_register_v2(self, typed_data: dict[str, Any]) -> str:
        if self.closed or typed_data.get("primaryType") != "RegisterV2":
            raise ValueError("signer operation rejected")
        return _signer._sign_typed_data(bytes(self._secret), typed_data)

    def close(self) -> None:
        for index in range(len(self._secret)):
            self._secret[index] = 0
        self._secret.clear()
        super().close()


def _load_session_signer_only() -> SessionSignerCredential:
    home_fd = _signer._open_home()
    try:
        record = _signer._load_record(home_fd)
        secret = _signer._load_credential(home_fd)
    finally:
        os.close(home_fd)
    if record.state is not _signer.SignerState.ACTIVE or record.signer != SIGNER:
        secret = b""
        raise ValueError("session signer rejected")
    return _credential_from_secret(secret, SIGNER)


def _credential_from_secret(secret: bytes, expected: str) -> SessionSignerCredential:
    if not isinstance(secret, bytes) or len(secret) != 32:
        raise ValueError("session signer rejected")
    if _signer._derive_address(secret) != expected:
        raise ValueError("session signer rejected")
    return SessionSignerCredential(expected, secret)


class OperationalJournal:
    def __init__(self) -> None:
        self._path = _home() / _ATTEMPT

    @classmethod
    def _fixture(cls, path: Path) -> "OperationalJournal":
        value = object.__new__(cls); value._path = Path(path); return value

    def claim_blocked(self) -> bool:
        payload = b'{"schema_version":1,"result":"PREFLIGHT_BLOCKED"}\n'
        try:
            fd = os.open(self._path, os.O_WRONLY | os.O_CREAT | os.O_EXCL
                         | getattr(os, "O_NOFOLLOW", 0), 0o600)
        except OSError:
            return False
        try:
            os.fchmod(fd, 0o600); os.write(fd, payload); os.fsync(fd)
            directory = os.open(self._path.parent, os.O_RDONLY)
            try: os.fsync(directory)
            finally: os.close(directory)
            return True
        finally:
            os.close(fd)

    def finish(self, passed: bool) -> None:
        if not passed or not _safe_file(self._path):
            return
        payload = b'{"schema_version":1,"result":"PREFLIGHT_PASSED"}\n'
        fd, temp_name = tempfile.mkstemp(prefix=self._path.name + ".", dir=self._path.parent)
        try:
            os.fchmod(fd, 0o600); os.write(fd, payload); os.fsync(fd); os.close(fd)
            os.replace(temp_name, self._path)
            directory = os.open(self._path.parent, os.O_RDONLY)
            try: os.fsync(directory)
            finally: os.close(directory)
        finally:
            try: os.close(fd)
            except OSError: pass
            try: os.unlink(temp_name)
            except OSError: pass


@dataclass
class _FixtureDependencies:
    journal: OperationalJournal
    runner: Callable[[], Awaitable[PreflightResult]]


class OperationalAttempt:
    def __init__(self) -> None:
        self._clock = time.time
        self._fixture: _FixtureDependencies | None = None

    async def run(self) -> PreflightResult:
        journal = self._fixture.journal if self._fixture else OperationalJournal()
        if not journal.claim_blocked():
            return PreflightResult(Outcome.BLOCKED, {})
        try:
            result = (await self._fixture.runner()) if self._fixture else await self._run_production()
            if not isinstance(result, PreflightResult) or result.outcome is not Outcome.PASSED:
                return PreflightResult(Outcome.BLOCKED, {})
            journal.finish(True)
            return result
        except asyncio.CancelledError:
            raise
        except BaseException:
            return PreflightResult(Outcome.BLOCKED, {})

    async def _run_production(self) -> PreflightResult:
        transport = SealedTransport()
        preflight_path = _home() / _PREFLIGHT
        if not _prepare_sqlite_file(preflight_path):
            await transport.close()
            return PreflightResult(Outcome.BLOCKED, {})
        store = PrivateReadStore(preflight_path)
        lifecycle = LifecycleClearBinding()
        if lifecycle() is not True:
            store.close()
            await transport.close()
            return PreflightResult(Outcome.BLOCKED, {})
        controller = PrivateReadPreflight(
            store, clock=self._clock, public_get=transport.public_get,
            lifecycle_clear=lifecycle,
        )
        try:
            barrier = await controller.run_public_barrier()
            if barrier is None:
                return PreflightResult(Outcome.BLOCKED, {})
            return await controller.run_private_proof(
                barrier, signer_loader=_load_session_signer_only,
                nonce_get=transport.public_get,
                sign_register_v2=lambda credential, typed: credential.sign_register_v2(typed),
                private_exchange=transport._private_exchange,
            )
        finally:
            store.close()
            await transport.close()


def fixture_adapter(*, journal: OperationalJournal,
                    runner: Callable[[], Awaitable[PreflightResult]]) -> OperationalAttempt:
    value = OperationalAttempt()
    value._fixture = _FixtureDependencies(journal, runner)
    return value


def _prepare_sqlite_file(path: Path) -> bool:
    if path.exists():
        return _safe_file(path)
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR
                     | getattr(os, "O_NOFOLLOW", 0), 0o600)
        os.fchmod(fd, 0o600); os.fsync(fd); os.close(fd)
        directory = os.open(path.parent, os.O_RDONLY)
        try: os.fsync(directory)
        finally: os.close(directory)
        return _safe_file(path)
    except OSError:
        return False
