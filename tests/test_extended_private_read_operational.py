import asyncio
import copy
import importlib
import importlib.util
import inspect
import json
import os
from pathlib import Path
import sqlite3
import stat
import subprocess
import sys
from types import SimpleNamespace

import pytest
from aiohttp import WSMsgType
import risex_farmer.extended_private_read_operational as operational

from risex_farmer.extended_private_read_preflight import (
    ACCOUNT_ID,
    ACCOUNT_INDEX,
    L2_KEY,
    L2_VAULT,
    REST_BASE_URL,
    STREAM_URL,
    PreflightStore,
)
from risex_farmer.extended_private_read_operational import (
    _OperationalStore,
    _PasswdHomeCredentialSource,
    _DirectStream,
    _run_fixture_operational_private_read,
)


def test_opt_in_operational_launcher_exists() -> None:
    assert importlib.util.find_spec(
        "risex_farmer.extended_private_read_operational"
    ) is not None


def test_interrupted_running_has_authoritative_counter_recovery(tmp_path) -> None:
    path = tmp_path / "interrupted.sqlite3"
    store = PreflightStore(path)
    assert hasattr(store, "recover_interrupted")
    with sqlite3.connect(path) as connection:
        columns = {
            row[1]
            for row in connection.execute(
                "PRAGMA table_info(extended_private_read_preflight)"
            )
        }
    assert {"schema_version", "phase", "counters"} <= columns


def _wrapped(data, *, count=None):
    pagination = None if count is None else {"cursor": None, "count": count}
    return {"status": "OK", "data": data, "error": None, "pagination": pagination}


def _account():
    return {
        "id": ACCOUNT_ID, "description": "synthetic", "accountIndex": ACCOUNT_INDEX,
        "status": "ACTIVE", "l2Key": L2_KEY, "l2Vault": L2_VAULT,
        "bridgeStarknetAddress": None,
    }


def _account_response():
    return {
        "status": "OK",
        "data": {
            "accountId": ACCOUNT_ID,
            "accountIndex": ACCOUNT_INDEX,
            "accountIndexForKeyGeneration": ACCOUNT_INDEX,
            "bridgeStarknetAddress": "0xabc123",
            "description": "synthetic",
            "l2Key": L2_KEY,
            "l2Vault": str(L2_VAULT),
            "status": "ACTIVE",
        },
    }


def _meta(url):
    return {
        "actual_url": url, "method": "GET",
        "header_names": (
            ["User-Agent", "X-Api-Key"] if url == STREAM_URL
            else ["Accept", "Content-Type", "User-Agent", "X-Api-Key"]
        ),
        "direct_tls": True, "trust_env": False, "proxy": None,
        "redirects": 0, "retries": 0, "fallbacks": 0,
        "api_key_header_count": 1, "authorization_present": False,
        "credential_in_query": False, "credential_in_body": False,
        "application_frames_sent": False,
    }


class _Stream:
    def __init__(self, timeline, *, close_error=None, silent_close=False):
        self.timeline = timeline
        self.close_error = close_error
        self.silent_close = silent_close
        self.upgrade_metadata = _meta(STREAM_URL)
        self.outbound_frames = []
        self.reconnect_count = 0
        self.closed = False
        self.barrier_complete = False
        self._done = asyncio.Event()

    async def recv(self):
        self.timeline.append("RECV")
        await self._done.wait()
        raise StopAsyncIteration

    async def final_barrier(self):
        self.timeline.append("BARRIER")
        self.barrier_complete = True
        self._done.set()
        return {
            "connected": True, "same_connection": True,
            "outbound_frames": [], "reconnect_count": 0, "frames": [],
            "transport": _meta(STREAM_URL), "observed_at_ms": 1770000000300,
        }

    async def close(self):
        self.timeline.append("CLOSE")
        if self.close_error:
            raise self.close_error
        if not self.silent_close:
            self.closed = True


class _Transport:
    def __init__(
        self, *, identity_offset=0, fail_b_path=None, close_error=None,
        silent_close=False,
    ):
        self.timeline = []
        self.get_calls = []
        self.open_calls = []
        self.identity_offset = identity_offset
        self.fail_b_path = fail_b_path
        self.stream = _Stream(
            self.timeline, close_error=close_error, silent_close=silent_close
        )

    async def get(self, request):
        self.timeline.append(f"GET_{request.round_name}_{request.path}")
        self.get_calls.append(request)
        if request.round_name == "B" and request.path == self.fail_b_path:
            raise ConnectionError("synthetic private response")
        if request.path == "/user/account/info":
            payload = _account_response()
            payload["data"]["accountId"] += self.identity_offset
        else:
            payload = _wrapped([], count=0)
        return {
            "status": 200, "final_url": request.url,
            "observed_at_ms": 1770000000000 + (200 if request.round_name == "B" else 0),
            "body": payload, "transport": _meta(request.url),
        }

    async def open_stream(self, request):
        self.timeline.append("OPEN")
        self.open_calls.append(request)
        return self.stream


class _FailingWebSocketSession:
    def __init__(self, error):
        self.error = error

    async def ws_connect(self, *args, **kwargs):
        raise self.error


class _DirectOpenFailureTransport(_Transport):
    def __init__(self, error):
        super().__init__()
        self.direct = object.__new__(operational._DirectTransport)
        self.direct._session = _FailingWebSocketSession(error)

    async def open_stream(self, request):
        self.timeline.append("OPEN")
        self.open_calls.append(request)
        return await self.direct.open_stream(request)


class _Capability:
    def __init__(self, *, account_id=ACCOUNT_ID, secret="synthetic-secret-only"):
        self.account_id = account_id
        self.secret = secret
        self.closed = False
        self.header_calls = 0

    def x_api_key_header_value(self):
        self.header_calls += 1
        return self.secret

    def matches_account(self, account):
        return (
            account["id"] == self.account_id
            and account["accountIndex"] == ACCOUNT_INDEX
            and account["l2Key"] == L2_KEY
            and account["l2Vault"] == L2_VAULT
        )

    def matches_spot_account_id(self, account_id):
        return account_id == self.account_id

    def close(self):
        self.closed = True
        self.secret = ""


class _Source:
    def __init__(self, capability=None):
        self.capability = capability or _Capability()
        self.calls = 0

    def open(self):
        self.calls += 1
        return self.capability


class _Clock:
    def __init__(self):
        self.calls = 0

    def __call__(self):
        self.calls += 1
        return 1770000000100 if self.calls <= 3 else 1770000000300


async def _run(tmp_path, *, source=None, transport=None, hook=None):
    source = source or _Source()
    transport = transport or _Transport()
    clock = _Clock()
    result = await _run_fixture_operational_private_read(
        store=_OperationalStore(tmp_path / "operational.sqlite3"),
        credential_source=source, transport=transport, clock_ms=clock,
        _effect_hook=hook,
    )
    return result, source, transport, clock


@pytest.mark.asyncio
async def test_success_has_exact_sequence_and_all_effect_counters(tmp_path):
    result, source, transport, clock = await _run(tmp_path)
    assert (result.status, result.reason, result.phase) == (
        "READY", "OPERATIONAL_CONTRACT_PROVED", "TERMINAL"
    )
    assert source.calls == 1 and source.capability.header_calls == 1
    assert source.capability.closed
    assert len(transport.get_calls) == 6 and len(transport.open_calls) == 1
    assert [call.round_name for call in transport.get_calls] == ["A"] * 3 + ["B"] * 3
    assert [call.path for call in transport.get_calls] == [
        "/user/account/info", "/user/orders", "/user/positions",
    ] * 2
    assert transport.timeline.index("OPEN") > transport.timeline.index("GET_A_/user/positions")
    assert transport.timeline.index("OPEN") < transport.timeline.index("GET_B_/user/account/info")
    assert transport.timeline[-2:] == ["BARRIER", "CLOSE"]
    assert result.rest_calls == 6 and result.clock_calls == 7
    assert set(result.counters.values()) == {1}
    assert all(call.method == "GET" for call in transport.get_calls)
    assert all(call.headers == {"X-Api-Key": "synthetic-secret-only"} for call in transport.get_calls)
    stream_request = transport.open_calls[0]
    assert stream_request.headers == {
        "User-Agent": "X10PythonTradingClient/2.5.0",
        "X-Api-Key": "synthetic-secret-only",
    }
    assert transport.stream.upgrade_metadata["header_names"] == list(
        stream_request.headers
    )


@pytest.mark.asyncio
async def test_terminal_restart_is_identical_and_has_zero_effects(tmp_path):
    first, _, _, _ = await _run(tmp_path)
    source, transport, clock = _Source(), _Transport(), _Clock()
    second = await _run_fixture_operational_private_read(
        store=_OperationalStore(tmp_path / "operational.sqlite3"),
        credential_source=source, transport=transport, clock_ms=clock,
    )
    assert second == first
    assert source.calls == 0 and transport.get_calls == [] and transport.open_calls == []
    assert clock.calls == 0


@pytest.mark.asyncio
async def test_coherently_corrupted_ready_counters_never_return_ready(tmp_path):
    await _run(tmp_path)
    path = tmp_path / "operational.sqlite3"
    with sqlite3.connect(path) as connection:
        raw_counters, raw_evidence = connection.execute(
            "SELECT counters,evidence FROM extended_private_read_operation"
        ).fetchone()
        counters = json.loads(raw_counters)
        evidence = json.loads(raw_evidence)
        counters["rest_b_positions_completions"] = 0
        evidence["counters"] = counters
        connection.execute(
            "UPDATE extended_private_read_operation SET counters=?,evidence=?",
            (json.dumps(counters), json.dumps(evidence)),
        )
    with pytest.raises(Exception, match="DURABLE_EVIDENCE_INVALID"):
        await _run_fixture_operational_private_read(
            store=_OperationalStore(path), credential_source=_Source(),
            transport=_Transport(), clock_ms=_Clock(),
        )


@pytest.mark.asyncio
async def test_interrupted_running_recovers_exact_counters_with_zero_effects(tmp_path):
    store = _OperationalStore(tmp_path / "operational.sqlite3")
    assert store.claim() is None
    store.increment("loader", "attempts")
    source, transport, clock = _Source(), _Transport(), _Clock()
    result = await _run_fixture_operational_private_read(
        store=_OperationalStore(store.path), credential_source=source,
        transport=transport, clock_ms=clock,
    )
    assert (result.status, result.reason, result.phase) == (
        "UNKNOWN", "INTERRUPTED_RUNNING", "LOADER"
    )
    assert result.counters["loader_attempts"] == 1
    assert result.counters["loader_completions"] == 0
    assert source.calls == 0 and transport.get_calls == [] and clock.calls == 0


@pytest.mark.asyncio
async def test_identity_mismatch_fails_closed_and_redacts(tmp_path):
    secret = "do-not-persist-this-synthetic-key"
    source = _Source(_Capability(account_id=ACCOUNT_ID + 1, secret=secret))
    result, source, transport, _ = await _run(tmp_path, source=source)
    assert (result.status, result.reason) == ("BLOCKED", "ACCOUNT_IDENTITY_MISMATCH")
    assert source.capability.closed and len(transport.get_calls) == 1
    durable = (tmp_path / "operational.sqlite3").read_bytes()
    assert secret.encode() not in durable
    assert b"synthetic private response" not in durable


class _ProcessDeath(BaseException):
    pass


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "effect",
    [
        "loader", "rest_a_info", "rest_a_orders", "rest_a_positions",
        "stream_open", "stream_upgrade", "rest_b_info", "rest_b_orders",
        "rest_b_positions", "barrier_request", "barrier_validation",
        "stream_close", "terminal_persistence",
    ],
)
@pytest.mark.parametrize("point", ["after_attempt", "before_completion"])
async def test_process_death_at_each_effect_is_never_resumed(tmp_path, effect, point):
    path = tmp_path / f"{effect}-{point}.sqlite3"

    def hook(current, current_point):
        if (current, current_point) == (effect, point):
            raise _ProcessDeath

    with pytest.raises(_ProcessDeath):
        await _run_fixture_operational_private_read(
            store=_OperationalStore(path), credential_source=_Source(),
            transport=_Transport(), clock_ms=_Clock(), _effect_hook=hook,
        )
    source, transport, clock = _Source(), _Transport(), _Clock()
    recovered = await _run_fixture_operational_private_read(
        store=_OperationalStore(path), credential_source=source,
        transport=transport, clock_ms=clock,
    )
    assert recovered.status == "UNKNOWN"
    assert recovered.counters[f"{effect}_attempts"] == 1
    assert recovered.counters[f"{effect}_completions"] == 0
    assert source.calls == 0 and transport.get_calls == [] and clock.calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "effect",
    [
        "loader", "rest_a_info", "rest_a_orders", "rest_a_positions",
        "stream_open", "rest_b_info", "rest_b_orders", "rest_b_positions",
        "barrier_request", "stream_close",
    ],
)
async def test_process_death_immediately_after_external_effect_is_unknown(tmp_path, effect):
    def hook(current, point):
        if (current, point) == (effect, "after_effect"):
            raise _ProcessDeath

    with pytest.raises(_ProcessDeath):
        await _run(tmp_path, hook=hook)
    recovered = await _run_fixture_operational_private_read(
        store=_OperationalStore(tmp_path / "operational.sqlite3"),
        credential_source=_Source(), transport=_Transport(), clock_ms=_Clock(),
    )
    assert recovered.status == "UNKNOWN"
    assert recovered.counters[f"{effect}_attempts"] == 1
    assert recovered.counters[f"{effect}_completions"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "effect",
    [
        "loader", "rest_a_info", "rest_a_orders", "rest_a_positions",
        "stream_open", "stream_upgrade", "rest_b_info", "rest_b_orders",
        "rest_b_positions", "barrier_request", "barrier_validation",
        "stream_close",
    ],
)
async def test_process_death_after_nonterminal_completion_is_unknown(tmp_path, effect):
    def hook(current, point):
        if (current, point) == (effect, "after_completion"):
            raise _ProcessDeath

    with pytest.raises(_ProcessDeath):
        await _run(tmp_path, hook=hook)
    source, transport, clock = _Source(), _Transport(), _Clock()
    recovered = await _run_fixture_operational_private_read(
        store=_OperationalStore(tmp_path / "operational.sqlite3"),
        credential_source=source, transport=transport, clock_ms=clock,
    )
    assert recovered.status == "UNKNOWN"
    assert recovered.counters[f"{effect}_attempts"] == 1
    assert recovered.counters[f"{effect}_completions"] == 1
    assert source.calls == 0 and transport.get_calls == [] and clock.calls == 0


@pytest.mark.asyncio
async def test_process_death_after_terminal_completion_replays_terminal_only(tmp_path):
    def hook(current, point):
        if (current, point) == ("terminal_persistence", "after_completion"):
            raise _ProcessDeath

    with pytest.raises(_ProcessDeath):
        await _run(tmp_path, hook=hook)
    source, transport, clock = _Source(), _Transport(), _Clock()
    recovered = await _run_fixture_operational_private_read(
        store=_OperationalStore(tmp_path / "operational.sqlite3"),
        credential_source=source, transport=transport, clock_ms=clock,
    )
    assert recovered.status == "READY"
    assert recovered.counters["terminal_persistence_attempts"] == 1
    assert recovered.counters["terminal_persistence_completions"] == 1
    assert source.calls == 0 and transport.get_calls == [] and clock.calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "effect,point",
    [
        *((effect, "after_attempt") for effect in (
            "loader", "rest_a_info", "rest_a_orders", "rest_a_positions",
            "stream_open", "stream_upgrade", "rest_b_info", "rest_b_orders",
            "rest_b_positions", "barrier_request", "barrier_validation",
            "stream_close",
        )),
        *((effect, "after_effect") for effect in (
            "loader", "rest_a_info", "rest_a_orders", "rest_a_positions",
            "stream_open", "rest_b_info", "rest_b_orders", "rest_b_positions",
            "barrier_request", "stream_close",
        )),
    ],
)
async def test_cancellation_around_each_effect_is_terminal_and_not_replayed(
    tmp_path, effect, point
):
    def hook(current, current_point):
        if (current, current_point) == (effect, point):
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await _run(tmp_path, hook=hook)
    source, transport, clock = _Source(), _Transport(), _Clock()
    result = await _run_fixture_operational_private_read(
        store=_OperationalStore(tmp_path / "operational.sqlite3"),
        credential_source=source, transport=transport, clock_ms=clock,
    )
    assert (result.status, result.reason) == ("BLOCKED", "CANCELLED")
    assert source.calls == 0 and transport.get_calls == [] and clock.calls == 0


@pytest.mark.asyncio
async def test_incomplete_round_b_and_ambiguous_close_are_fail_closed(tmp_path):
    (tmp_path / "round-b").mkdir(mode=0o700)
    result, _, _, _ = await _run(
        tmp_path / "round-b",
        transport=_Transport(fail_b_path="/user/orders"),
    )
    assert (result.status, result.reason) == ("BLOCKED", "UNEXPECTED_FAILURE")
    assert result.counters["rest_b_orders_attempts"] == 1
    assert result.counters["rest_b_orders_completions"] == 0
    (tmp_path / "close").mkdir(mode=0o700)
    result, _, _, _ = await _run(
        tmp_path / "close", transport=_Transport(close_error=OSError("private close"))
    )
    assert (result.status, result.reason) == ("BLOCKED", "STREAM_CLOSE_FAILED")
    assert result.counters["stream_close_attempts"] == 1
    assert result.counters["stream_close_completions"] == 0
    (tmp_path / "silent-close").mkdir(mode=0o700)
    result, _, _, _ = await _run(
        tmp_path / "silent-close", transport=_Transport(silent_close=True)
    )
    assert (result.status, result.reason) == ("BLOCKED", "STREAM_CLOSE_FAILED")
    assert result.counters["stream_close_attempts"] == 1
    assert result.counters["stream_close_completions"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error,expected_class",
    [
        (
            operational.aiohttp.WSServerHandshakeError(
                None, (), status=503, message="private handshake response"
            ),
            "HTTP",
        ),
        (
            operational.aiohttp.ServerDisconnectedError(
                "private pre-upgrade disconnect"
            ),
            "TRANSPORT",
        ),
        (TimeoutError("private pre-upgrade timeout"), "TRANSPORT"),
    ],
)
async def test_stream_open_failures_persist_sanitized_class(
    tmp_path, error, expected_class
):
    result, _, _, _ = await _run(
        tmp_path, transport=_DirectOpenFailureTransport(error)
    )
    assert (result.status, result.reason) == ("BLOCKED", expected_class)
    assert result.counters["stream_open_attempts"] == 1
    assert result.counters["stream_open_completions"] == 0
    assert json.loads(result.evidence())["reason"] == expected_class
    with sqlite3.connect(tmp_path / "operational.sqlite3") as connection:
        durable = connection.execute(
            "SELECT evidence FROM extended_private_read_operation"
        ).fetchone()[0]
    assert json.loads(durable)["reason"] == expected_class
    assert "private" not in durable


@pytest.mark.asyncio
async def test_direct_stream_barrier_requires_matching_protocol_pong_and_closes():
    class Socket:
        def __init__(self):
            self.closed = False
            self.payload = None
            self.ping_sent = asyncio.Event()

        async def ping(self, payload):
            self.payload = payload
            self.ping_sent.set()

        async def receive(self, timeout):
            await self.ping_sent.wait()
            if self.payload is not None:
                payload, self.payload = self.payload, None
                return SimpleNamespace(type=WSMsgType.PONG, data=payload)
            await asyncio.Event().wait()

        async def pong(self, payload):
            raise AssertionError("fixture server did not send PING")

        async def close(self):
            self.closed = True

    socket = Socket()
    request_headers = {
        "User-Agent": "X10PythonTradingClient/2.5.0",
        "X-Api-Key": "direct-stream-secret",
    }
    stream = _DirectStream(socket, tuple(request_headers))
    assert stream.upgrade_metadata["header_names"] == list(request_headers)
    assert "direct-stream-secret" not in json.dumps(stream.upgrade_metadata)
    barrier = await stream.final_barrier()
    assert barrier["transport"]["header_names"] == list(request_headers)
    assert "direct-stream-secret" not in json.dumps(barrier)
    assert barrier["connected"] and barrier["same_connection"]
    assert barrier["outbound_frames"] == []
    with pytest.raises(StopAsyncIteration):
        await stream.recv()
    await asyncio.wait_for(stream.close(), timeout=1)
    assert socket.closed and stream.closed


def _identity_document():
    return {
        "id": ACCOUNT_ID, "accountIndex": ACCOUNT_INDEX,
        "l2Key": L2_KEY, "l2Vault": L2_VAULT,
    }


def _write_source_files(home, *, key=b"fixture-key-only", identity=None):
    key_path = home / operational.API_KEY_BASENAME
    identity_path = home / operational.IDENTITY_BASENAME
    key_path.write_bytes(key)
    identity_path.write_text(json.dumps(identity or _identity_document()))
    key_path.chmod(0o600)
    identity_path.chmod(0o600)
    return key_path, identity_path


def test_passwd_home_source_uses_exact_files_once_nofollow_and_zeroizes(
    tmp_path, monkeypatch
):
    home = tmp_path / "passwd-home"
    home.mkdir(mode=0o755)
    home.chmod(0o755)
    key_path, identity_path = _write_source_files(home)
    monkeypatch.setattr(operational, "_passwd_home", lambda: home)
    original_open = operational.os.open
    opened = []

    def recording_open(path, flags, *args):
        opened.append((Path(path), flags))
        return original_open(path, flags, *args)

    monkeypatch.setattr(operational.os, "open", recording_open)
    capability = _PasswdHomeCredentialSource().open()
    assert [item[0] for item in opened] == [identity_path, key_path]
    assert all(item[1] & os.O_NOFOLLOW for item in opened)
    assert capability.x_api_key_header_value() == "fixture-key-only"
    assert capability.matches_account(_account())
    assert capability.matches_spot_account_id(ACCOUNT_ID)
    key_buffer = capability._key
    capability.close()
    assert set(key_buffer) == {0}
    assert capability._identity == {}
    with pytest.raises(Exception, match="CREDENTIAL_CLOSED"):
        capability.x_api_key_header_value()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "defect",
    [
        "missing_identity", "missing_key", "identity_mode", "key_mode",
        "identity_symlink", "key_symlink", "identity_directory", "key_directory",
        "identity_extra", "identity_missing", "identity_type", "identity_json",
        "identity_oversize", "key_oversize", "key_empty", "key_whitespace",
        "key_non_utf8",
    ],
)
async def test_passwd_home_source_adverse_files_block_before_transport(
    tmp_path, monkeypatch, defect
):
    home = tmp_path / "passwd-home"
    home.mkdir(mode=0o755)
    home.chmod(0o755)
    key_path, identity_path = _write_source_files(home)
    if defect == "missing_identity":
        identity_path.unlink()
    elif defect == "missing_key":
        key_path.unlink()
    elif defect == "identity_mode":
        identity_path.chmod(0o644)
    elif defect == "key_mode":
        key_path.chmod(0o644)
    elif defect == "identity_symlink":
        identity_path.unlink()
        target = home / "identity-target"
        target.write_text(json.dumps(_identity_document()))
        target.chmod(0o600)
        identity_path.symlink_to(target)
    elif defect == "key_symlink":
        key_path.unlink()
        target = home / "key-target"
        target.write_bytes(b"fixture-key-only")
        target.chmod(0o600)
        key_path.symlink_to(target)
    elif defect == "identity_directory":
        identity_path.unlink()
        identity_path.mkdir(mode=0o700)
    elif defect == "key_directory":
        key_path.unlink()
        key_path.mkdir(mode=0o700)
    elif defect == "identity_extra":
        value = _identity_document()
        value["extra"] = 1
        identity_path.write_text(json.dumps(value))
    elif defect == "identity_missing":
        value = _identity_document()
        value.pop("l2Vault")
        identity_path.write_text(json.dumps(value))
    elif defect == "identity_type":
        value = _identity_document()
        value["accountIndex"] = True
        identity_path.write_text(json.dumps(value))
    elif defect == "identity_json":
        identity_path.write_text("not-json")
    elif defect == "identity_oversize":
        value = _identity_document()
        value["l2Key"] = "x" * 3000
        identity_path.write_text(json.dumps(value))
    elif defect == "key_oversize":
        key_path.write_bytes(b"x" * 513)
    elif defect == "key_empty":
        key_path.write_bytes(b"")
    elif defect == "key_whitespace":
        key_path.write_bytes(b" fixture-key-only")
    elif defect == "key_non_utf8":
        key_path.write_bytes(b"\xff")
    monkeypatch.setattr(operational, "_passwd_home", lambda: home)
    transport = _Transport()
    result = await _run_fixture_operational_private_read(
        store=_OperationalStore(tmp_path / "operation.sqlite3"),
        credential_source=_PasswdHomeCredentialSource(), transport=transport,
        clock_ms=_Clock(),
    )
    assert result.status == "BLOCKED"
    assert transport.get_calls == [] and transport.open_calls == []
    durable = (tmp_path / "operation.sqlite3").read_bytes()
    assert b"fixture-key-only" not in durable
    assert str(home).encode() not in durable


def test_store_accepts_owned_0755_home_but_rejects_bad_file_and_symlink(tmp_path):
    passwd_home = tmp_path / "passwd-home"
    passwd_home.mkdir(mode=0o755)
    passwd_home.chmod(0o755)
    store = _OperationalStore(passwd_home / "store.sqlite3")
    assert stat.S_IMODE(store.path.stat().st_mode) == 0o600
    bad_mode = passwd_home / "bad-mode.sqlite3"
    bad_mode.write_bytes(b"")
    bad_mode.chmod(0o644)
    with pytest.raises(Exception, match="STORE_FILE_INVALID"):
        _OperationalStore(bad_mode)
    target = tmp_path / "target.sqlite3"
    target.write_bytes(b"")
    target.chmod(0o600)
    link = tmp_path / "link.sqlite3"
    link.symlink_to(target)
    with pytest.raises(Exception, match="STORE_FILE_INVALID"):
        _OperationalStore(link)


def test_store_rejects_schema_corruption(tmp_path):
    path = tmp_path / "corrupt.sqlite3"
    store = _OperationalStore(path)
    with sqlite3.connect(store.path) as connection:
        connection.execute("DROP TABLE extended_private_read_operation")
        connection.execute("CREATE TABLE extended_private_read_operation (singleton INTEGER)")
    with pytest.raises(Exception, match="DURABLE_SCHEMA_INVALID"):
        _OperationalStore(path)


def test_runtime_production_binding_and_normal_startup_isolation():
    assert operational.STORE_BASENAME == (
        ".risex-funding-farmer-extended-private-read-runs-v1.sqlite3"
    )
    assert list(inspect.signature(_PasswdHomeCredentialSource).parameters) == []
    module = importlib.import_module("risex_farmer.extended_private_read_operational")
    assert module.__all__ == ["main"]
    assert list(inspect.signature(module._production_run).parameters) == []
    source = Path(module.__file__).read_text()
    assert REST_BASE_URL == "https://api.starknet.sepolia.extended.exchange/api/v1"
    assert STREAM_URL == (
        "wss://api.starknet.sepolia.extended.exchange/"
        "stream.extended.exchange/v1/account"
    )
    assert "REST_BASE_URL" in source and "STREAM_URL" in source
    assert "def post" not in source.lower()
    assert "allow_redirects=False" in source
    assert "trust_env=False" in source
    completed = subprocess.run(
        [
            sys.executable, "-c", "import sys, risex_farmer.cli; "
            "assert 'risex_farmer.extended_private_read_operational' not in sys.modules; "
            "assert 'risex_farmer.extended_private_read_preflight' not in sys.modules",
        ],
        check=False, capture_output=True, text=True,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).parents[1] / "src")},
    )
    assert completed.returncode == 0, completed.stderr


def test_runtime_run_id_allocator_is_fresh_and_source_milestone_free():
    run_ids = {operational._new_runtime_run_id() for _ in range(32)}
    assert len(run_ids) == 32
    assert all(run_id.startswith("extended-read-") for run_id in run_ids)
    source = Path(operational.__file__).read_text()
    assert not hasattr(operational, "INVOCATION_ID")
    for consumed in ("new-op-004", "20260824"):
        assert consumed not in operational.STORE_BASENAME
        assert consumed not in source


@pytest.mark.asyncio
async def test_fresh_runtime_rows_are_durable_and_historical_row_is_immutable(tmp_path):
    path = tmp_path / "runtime-runs.sqlite3"
    first = await _run_fixture_operational_private_read(
        store=_OperationalStore(path, "extended-read-a"),
        credential_source=_Source(), transport=_Transport(), clock_ms=_Clock(),
    )
    with sqlite3.connect(path) as connection:
        historical = connection.execute(
            "SELECT * FROM extended_private_read_operation WHERE invocation_id=?",
            ("extended-read-a",),
        ).fetchone()
    second = await _run_fixture_operational_private_read(
        store=_OperationalStore(path, "extended-read-b"),
        credential_source=_Source(), transport=_Transport(), clock_ms=_Clock(),
    )
    with sqlite3.connect(path) as connection:
        rows = connection.execute(
            "SELECT invocation_id FROM extended_private_read_operation ORDER BY rowid"
        ).fetchall()
        preserved = connection.execute(
            "SELECT * FROM extended_private_read_operation WHERE invocation_id=?",
            ("extended-read-a",),
        ).fetchone()
    assert first.invocation_id == "extended-read-a"
    assert second.invocation_id == "extended-read-b"
    assert rows == [("extended-read-a",), ("extended-read-b",)]
    assert preserved == historical
