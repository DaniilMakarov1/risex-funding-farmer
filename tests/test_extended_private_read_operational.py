import asyncio
import copy
import importlib
import importlib.util
import inspect
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
from types import SimpleNamespace

import pytest
from aiohttp import WSMsgType

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
    INVOCATION_ID,
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
    def __init__(self, timeline, *, close_error=None):
        self.timeline = timeline
        self.close_error = close_error
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
        self.closed = True
        if self.close_error:
            raise self.close_error


class _Transport:
    def __init__(self, *, identity_offset=0, fail_b_path=None, close_error=None):
        self.timeline = []
        self.get_calls = []
        self.open_calls = []
        self.identity_offset = identity_offset
        self.fail_b_path = fail_b_path
        self.stream = _Stream(self.timeline, close_error=close_error)

    async def get(self, request):
        self.timeline.append(f"GET_{request.round_name}_{request.path}")
        self.get_calls.append(request)
        if request.round_name == "B" and request.path == self.fail_b_path:
            raise ConnectionError("synthetic private response")
        if request.path == "/user/account/info":
            body = _account()
            body["id"] += self.identity_offset
            payload = _wrapped(body)
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
    stream = _DirectStream(socket)
    barrier = await stream.final_barrier()
    assert barrier["connected"] and barrier["same_connection"]
    assert barrier["outbound_frames"] == []
    with pytest.raises(StopAsyncIteration):
        await stream.recv()
    await asyncio.wait_for(stream.close(), timeout=1)
    assert socket.closed and stream.closed


def test_store_rejects_permissions_symlink_and_schema_corruption(tmp_path):
    bad_parent = tmp_path / "wide"
    bad_parent.mkdir(mode=0o755)
    with pytest.raises(Exception, match="STORE_PARENT_INVALID"):
        _OperationalStore(bad_parent / "store.sqlite3")
    target = tmp_path / "target.sqlite3"
    target.write_bytes(b"")
    target.chmod(0o600)
    link = tmp_path / "link.sqlite3"
    link.symlink_to(target)
    with pytest.raises(Exception, match="STORE_FILE_INVALID"):
        _OperationalStore(link)
    path = tmp_path / "corrupt.sqlite3"
    store = _OperationalStore(path)
    with sqlite3.connect(store.path) as connection:
        connection.execute("DROP TABLE extended_private_read_operation")
        connection.execute("CREATE TABLE extended_private_read_operation (singleton INTEGER)")
    with pytest.raises(Exception, match="DURABLE_SCHEMA_INVALID"):
        _OperationalStore(path)


def test_fixed_production_binding_and_normal_startup_isolation():
    assert INVOCATION_ID == "extended-private-read-20260824-new-op-001"
    assert list(inspect.signature(_PasswdHomeCredentialSource).parameters) == []
    module = importlib.import_module("risex_farmer.extended_private_read_operational")
    assert module.__all__ == ["INVOCATION_ID", "main"]
    assert list(inspect.signature(module._production_run).parameters) == []
    source = Path(module.__file__).read_text()
    assert REST_BASE_URL == "https://api.starknet.sepolia.extended.exchange/api/v1"
    assert STREAM_URL.endswith("/stream.extended.exchange/v1/account")
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
