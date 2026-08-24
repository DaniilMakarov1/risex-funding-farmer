from __future__ import annotations

import asyncio
import copy
import hashlib
import inspect
import json
import os
from pathlib import Path
import sqlite3

import pytest

import risex_farmer.risex_private_read_operational as operational
from risex_farmer.testnet_risex_private_read_preflight import (
    ACCOUNT,
    HttpResponse,
    PrivateReadPreflight,
    SIGNER,
    expected_url,
)
from risex_farmer.risex_private_read_operational import (
    FIXED_STORE_PATH,
    INVOCATION_ID,
    STORE_BASENAME,
    DurableCounterLedger,
    FixedRisexPrivateReadTransport,
    OperationalPrivateRead,
    PasswdHomeSessionSignerCapabilitySource,
    Result,
    _COUNTER_NAMES,
    _APPLICATION_ID,
    _SignOnlyCapability,
    _SimulatedProcessDeath,
    _fixture_operational_private_read,
    _run_fixture,
)
from tests.test_testnet_risex_private_read_preflight import NOW, public_bodies


SIGNATURE = "0x" + "11" * 65


class SyntheticSignOnlyCapability:
    def __init__(self, calls: list[str], signer: str = SIGNER) -> None:
        self._calls = calls
        self._signer = signer
        self._closed = False

    def derive_signer_address(self) -> str:
        self._calls.append("derive")
        return self._signer

    def sign_register_v2(self, canonical_typed_data):
        self._calls.append("sign")
        assert canonical_typed_data == PrivateReadPreflight._typed_data("0x0001")
        return SIGNATURE

    def close(self) -> None:
        self._calls.append("capability_close")
        self._closed = True


class SyntheticSource:
    def __init__(self, calls: list[str], signer: str = SIGNER, secret: str = "") -> None:
        self._calls = calls
        self._signer = signer
        self._secret = secret

    def load(self) -> None:
        self._calls.append("source_load")

    def open(self) -> SyntheticSignOnlyCapability:
        self._calls.append("capability_open")
        return SyntheticSignOnlyCapability(self._calls, self._signer)

    def close(self) -> None:
        self._calls.append("source_close")
        self._secret = ""


class SyntheticTransport:
    def __init__(self, calls: list[str], *, public_mutator=None) -> None:
        self._calls = calls
        self._public_index = 0
        self._public_mutator = public_mutator

    async def public_get(self, index: int) -> HttpResponse:
        round_name = "a" if self._public_index < 9 else "b"
        self._public_index += 1
        self._calls.append(f"public_{round_name}_{index + 1:02d}")
        path, query = PrivateReadPreflight._REQUESTS[index]
        body = copy.deepcopy(public_bodies()[path])
        if self._public_mutator is not None:
            self._public_mutator(round_name, path, body)
        return HttpResponse(200, expected_url(path, query), body, NOW, False)

    async def nonce_get(self) -> HttpResponse:
        self._calls.append("nonce_get")
        path = "/v1/auth/nonce"
        query = (("account", ACCOUNT),)
        return HttpResponse(
            200,
            expected_url(path, query),
            {"data": {"nonce": "0x0001"}, "request_id": "fixture"},
            NOW,
            False,
        )

    async def auth_v2_dispatch(self, frame) -> None:
        self._calls.append("auth_v2_dispatch")
        assert frame["method"] == "auth_v2"
        assert frame["params"]["account"] == ACCOUNT
        assert frame["params"]["signer"] == SIGNER

    async def auth_v2_ack(self):
        self._calls.append("auth_v2_ack")
        return {"method": "auth_v2", "status": "success"}

    async def orders_subscribe(self) -> None:
        self._calls.append("orders_subscribe")

    async def orders_snapshot(self):
        self._calls.append("orders_snapshot")
        return {
            "method": "snapshot",
            "channel": "orders",
            "type": "snapshot",
            "data": [],
            "order_count": 0,
            "worker_timestamp": str(int(NOW * 1_000_000_000)),
        }

    async def positions_subscribe(self) -> None:
        self._calls.append("positions_subscribe")

    async def positions_snapshot(self):
        self._calls.append("positions_snapshot")
        return {
            "method": "snapshot",
            "channel": "positions",
            "type": "snapshot",
            "data": [],
            "position_count": 0,
            "worker_timestamp": str(int(NOW * 1_000_000_000)),
        }

    async def close(self) -> None:
        self._calls.append("transport_close")


class _Body:
    def __init__(self, value: bytes) -> None:
        self._value = value

    async def read(self, _limit: int) -> bytes:
        return self._value


class _Response:
    status = 200
    content_length = None
    history = ()

    def __init__(self, url: str, body: bytes) -> None:
        self.url = url
        self.content = _Body(body)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None


class _GetSession:
    def __init__(self, response: _Response) -> None:
        self.response = response
        self.call = None

    def get(self, *args, **kwargs):
        self.call = (args, kwargs)
        return self.response


def dependencies(tmp_path, calls, **changes):
    values = {
        "path": tmp_path / "fixture.sqlite3",
        "invocation_id": "synthetic-risex-private-read",
        "source_factory": lambda: SyntheticSource(calls),
        "transport_factory": lambda: SyntheticTransport(calls),
        "clock": lambda: NOW,
    }
    values.update(changes)
    return _fixture_operational_private_read(**values)


def test_new_fixed_identity_launcher_and_normal_startup_isolation():
    assert INVOCATION_ID == "risex-private-read-20260824-new-op-001"
    assert STORE_BASENAME == (
        ".risex-funding-farmer-risex-private-read-20260824-new-op-001.sqlite3"
    )
    assert str(FIXED_STORE_PATH) == (
        "/Users/daniilmakarov/.risex-funding-farmer-"
        "risex-private-read-20260824-new-op-001.sqlite3"
    )
    assert not inspect.signature(OperationalPrivateRead).parameters
    module_source = Path(__file__).parents[1] / "src/risex_farmer/risex_private_read_operational.py"
    assert 'if __name__ == "__main__"' in module_source.read_text()
    for normal in ("src/risex_farmer/__init__.py", "src/risex_farmer/cli.py"):
        assert "risex_private_read_operational" not in (
            Path(__file__).parents[1] / normal
        ).read_text()


def test_production_transport_and_capability_surfaces_are_narrow():
    public_transport = {
        name for name in vars(FixedRisexPrivateReadTransport)
        if not name.startswith("_")
    }
    assert public_transport == {
        "REST_ORIGIN", "WS_URL", "TRUST_ENV", "ALLOW_REDIRECTS",
        "DEADLINE_SECONDS", "PUBLIC_REQUEST_COUNT", "public_get", "nonce_get",
        "auth_v2_dispatch", "auth_v2_ack", "orders_subscribe", "orders_snapshot",
        "positions_subscribe", "positions_snapshot", "close",
    }
    source = Path(__file__).parents[1] / "src/risex_farmer/risex_private_read_operational.py"
    text = source.read_text()
    assert "def retry" not in text.lower()
    assert "def reconnect" not in text.lower()
    for forbidden in ("place_order", "cancel_order", "close_position", "deposit"):
        assert forbidden not in text
    assert {
        name for name in vars(_SignOnlyCapability) if not name.startswith("_")
    } == {"derive_signer_address", "sign_register_v2", "close"}
    assert {
        name for name in vars(PasswdHomeSessionSignerCapabilitySource)
        if not name.startswith("_")
    } == {"load", "open", "close"}
    orchestration = inspect.getsource(__import__(
        "risex_farmer.risex_private_read_operational", fromlist=["_execute"]
    )._execute)
    assert "_signer_storage" not in orchestration
    assert "passwd" not in orchestration.lower()
    assert "path" not in orchestration.lower()


@pytest.mark.asyncio
async def test_fixed_transport_owns_exact_public_url_proxy_and_redirect_policy():
    path, query = PrivateReadPreflight._REQUESTS[0]
    target = expected_url(path, query)
    response = _Response(
        target,
        json.dumps(public_bodies()[path], separators=(",", ":")).encode(),
    )
    session = _GetSession(response)
    transport = object.__new__(FixedRisexPrivateReadTransport)
    transport._session = session
    result = await transport.public_get(0)
    assert result.final_url == target
    assert session.call == ((target,), {"allow_redirects": False, "proxy": None})
    with pytest.raises(ValueError, match="transport"):
        await transport.public_get("/v1/system/config")


@pytest.mark.asyncio
async def test_fixed_transport_rejects_noncanonical_auth_before_socket_access():
    transport = object.__new__(FixedRisexPrivateReadTransport)
    transport._session = object()
    transport._socket = None
    transport._socket_context = None
    with pytest.raises(ValueError, match="transport"):
        await transport.auth_v2_dispatch({"method": "auth_v2", "params": {}})


@pytest.mark.asyncio
async def test_session_redirect_signal_aborts_before_follow_or_send(monkeypatch):
    captured = {}
    effects: list[str] = []

    class Session:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        async def close(self):
            effects.append("close")

    monkeypatch.setattr(operational.aiohttp, "ClientSession", Session)
    monkeypatch.setattr(
        operational.aiohttp, "TCPConnector", lambda **_kwargs: object(),
    )
    monkeypatch.setattr(
        operational.ssl, "create_default_context", lambda: object(),
    )
    transport = FixedRisexPrivateReadTransport()
    trace_configs = captured["trace_configs"]
    assert len(trace_configs) == 1
    callbacks = tuple(trace_configs[0].on_request_redirect)
    assert callbacks == (operational._abort_redirect,)

    async def redirect_flow():
        await callbacks[0](None, None, object())
        effects.append("follow")
        effects.append("send")

    with pytest.raises(ValueError, match="redirect rejected"):
        await redirect_flow()
    assert effects == []
    await transport.close()
    assert effects == ["close"]


@pytest.mark.asyncio
async def test_fixed_transport_disconnect_does_not_repeat_physical_get(monkeypatch):
    dispatches: list[str] = []

    class Protocol:
        def set_response_params(self, **_kwargs):
            return None

    class Connection:
        protocol = Protocol()

        def close(self):
            return None

    class DisconnectingRequest:
        def __init__(self, method, url, **kwargs):
            self.method = method
            self.url = url
            self._traces = kwargs["traces"]

        async def send(self, _connection):
            for trace in self._traces:
                await trace.send_request_headers(self.method, self.url, {})
            dispatches.append(str(self.url))
            raise operational.aiohttp.ServerDisconnectedError()

    transport = FixedRisexPrivateReadTransport()
    transport._session._request_class = DisconnectingRequest

    async def connect(_request, **_kwargs):
        return Connection()

    monkeypatch.setattr(transport._session._connector, "connect", connect)
    try:
        with pytest.raises(operational.aiohttp.ServerDisconnectedError):
            await transport.public_get(0)
    finally:
        await transport.close()

    assert dispatches == [expected_url(*PrivateReadPreflight._REQUESTS[0])]


@pytest.mark.asyncio
async def test_success_has_exact_sequence_full_counters_and_agreeing_barriers(tmp_path):
    calls: list[str] = []
    path = tmp_path / "fixture.sqlite3"
    report = await _run_fixture(dependencies(tmp_path, calls))
    assert report.result is Result.PASSED
    assert report.barrier_a_fingerprint == report.barrier_b_fingerprint
    assert set(report.counters) == set(_COUNTER_NAMES)
    assert all(value == {"attempts": 1, "completions": 1} for value in report.counters.values())
    assert calls[:9] == [f"public_a_{index:02d}" for index in range(1, 10)]
    assert calls[9:21] == [
        "source_load", "capability_open", "derive", "nonce_get", "sign",
        "auth_v2_dispatch", "auth_v2_ack", "orders_subscribe", "orders_snapshot",
        "positions_subscribe", "positions_snapshot", "capability_close",
    ]
    assert calls[21:30] == [f"public_b_{index:02d}" for index in range(1, 10)]
    assert calls[-2:] == ["source_close", "transport_close"]
    assert path.stat().st_mode & 0o777 == 0o600
    persisted = path.read_bytes().decode("latin1")
    for forbidden in (ACCOUNT, SIGNER, SIGNATURE):
        assert forbidden not in persisted
    database = sqlite3.connect(path)
    try:
        assert database.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert database.execute("PRAGMA user_version").fetchone() == (1,)
    finally:
        database.close()


@pytest.mark.asyncio
async def test_terminal_restart_returns_same_report_with_zero_effects(tmp_path):
    calls: list[str] = []
    first = await _run_fixture(dependencies(tmp_path, calls))
    calls.clear()
    second = await _run_fixture(dependencies(
        tmp_path,
        calls,
        source_factory=lambda: (_ for _ in ()).throw(AssertionError("source")),
        transport_factory=lambda: (_ for _ in ()).throw(AssertionError("transport")),
    ))
    assert second == first
    assert calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("factory", ("source", "transport"))
async def test_fixture_factory_failure_is_terminal_and_redacted(tmp_path, factory):
    calls: list[str] = []
    secret = "factory-secret-must-not-survive"

    def fail():
        raise RuntimeError(secret)

    changes = {f"{factory}_factory": fail}
    report = await _run_fixture(dependencies(tmp_path, calls, **changes))
    assert report.result is Result.BLOCKED
    assert secret not in json.dumps(report.as_dict(), sort_keys=True)
    assert report.counters["terminal_persist"] == {"attempts": 1, "completions": 1}


@pytest.mark.asyncio
async def test_signer_identity_mismatch_fails_closed_and_closes(tmp_path):
    calls: list[str] = []
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        source_factory=lambda: SyntheticSource(calls, "0x" + "22" * 20),
    ))
    assert report.result is Result.UNKNOWN
    assert calls.count("derive") == calls.count("capability_close") == 1
    assert "nonce_get" not in calls
    assert report.counters["signer_derive"] == {"attempts": 1, "completions": 0}


@pytest.mark.asyncio
async def test_nonflat_public_barrier_b_blocks_after_private_observation(tmp_path):
    calls: list[str] = []

    def mutate(round_name, path, body):
        if round_name == "b" and path == "/v1/orders/open":
            body["data"]["orders"] = [{"redacted": True}]

    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        transport_factory=lambda: SyntheticTransport(calls, public_mutator=mutate),
    ))
    assert report.result is Result.BLOCKED
    assert calls.count("sign") == 1
    assert report.barrier_b_fingerprint is None
    assert report.counters["public_b_09"] == {"attempts": 1, "completions": 1}
    assert report.counters["final_agreement"] == {"attempts": 0, "completions": 0}


@pytest.mark.asyncio
async def test_barrier_fingerprint_disagreement_is_terminal_unknown(tmp_path):
    calls: list[str] = []

    def mutate(round_name, path, body):
        if round_name == "b" and path == "/v1/orderbook":
            body["data"]["bids"][0]["price"] = "77963.2"
            body["data"]["asks"][0]["price"] = "77963.5"

    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        transport_factory=lambda: SyntheticTransport(calls, public_mutator=mutate),
    ))
    assert report.result is Result.UNKNOWN
    assert report.barrier_a_fingerprint != report.barrier_b_fingerprint
    assert report.counters["final_agreement"] == {"attempts": 1, "completions": 0}


_NONTERMINAL_PHASES = tuple(name for name in _COUNTER_NAMES if name != "terminal_persist")


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ("after_attempt", "before_completion"))
@pytest.mark.parametrize("phase", _NONTERMINAL_PHASES)
async def test_cancellation_at_every_effect_boundary_is_redacted_unknown(
    tmp_path, phase, boundary,
):
    calls: list[str] = []

    def cancel(where, name):
        if (where, name) == (boundary, phase):
            raise asyncio.CancelledError

    secret = "fixture-secret-must-not-survive"
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        crash_hook=cancel,
        source_factory=lambda: SyntheticSource(calls, secret=secret),
    ))
    assert report.result is Result.UNKNOWN
    encoded = json.dumps(report.as_dict(), sort_keys=True)
    assert secret not in encoded and secret not in (tmp_path / "fixture.sqlite3").read_bytes().decode(
        "latin1"
    )
    assert report.counters[phase]["attempts"] == 1
    expected_completion = 0 if boundary == "after_attempt" else 0
    assert report.counters[phase]["completions"] == expected_completion


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ("after_attempt", "before_completion"))
@pytest.mark.parametrize("phase", _NONTERMINAL_PHASES)
async def test_failure_at_every_effect_boundary_is_redacted_and_never_retried(
    tmp_path, phase, boundary,
):
    calls: list[str] = []
    secret = "failure-secret-must-not-survive"

    def fail(where, name):
        if (where, name) == (boundary, phase):
            raise RuntimeError(secret)

    report = await _run_fixture(dependencies(tmp_path, calls, crash_hook=fail))
    assert report.result is Result.UNKNOWN
    assert report.counters[phase] == {"attempts": 1, "completions": 0}
    assert secret not in json.dumps(report.as_dict(), sort_keys=True)
    restart_calls: list[str] = []
    restarted = await _run_fixture(dependencies(
        tmp_path,
        restart_calls,
        source_factory=lambda: (_ for _ in ()).throw(AssertionError("source")),
        transport_factory=lambda: (_ for _ in ()).throw(AssertionError("transport")),
    ))
    assert restarted == report
    assert restart_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ("after_attempt", "before_completion"))
@pytest.mark.parametrize("phase", _COUNTER_NAMES)
async def test_process_death_recovers_exact_counters_and_restart_has_zero_effects(
    tmp_path, phase, boundary,
):
    calls: list[str] = []

    def die(where, name):
        if (where, name) == (boundary, phase):
            raise _SimulatedProcessDeath

    with pytest.raises(_SimulatedProcessDeath):
        await _run_fixture(dependencies(tmp_path, calls, crash_hook=die))

    before = sqlite3.connect(tmp_path / "fixture.sqlite3")
    try:
        exact = dict(before.execute(
            "SELECT name,attempts || ':' || completions FROM phase_counter"
        ))
    finally:
        before.close()
    restart_calls: list[str] = []
    report = await _run_fixture(dependencies(
        tmp_path,
        restart_calls,
        source_factory=lambda: (_ for _ in ()).throw(AssertionError("source")),
        transport_factory=lambda: (_ for _ in ()).throw(AssertionError("transport")),
    ))
    assert report.result is Result.UNKNOWN
    assert restart_calls == []
    for name, value in exact.items():
        attempts, completions = map(int, value.split(":"))
        if name == "terminal_persist":
            assert report.counters[name] == {"attempts": 1, "completions": 1}
        else:
            assert report.counters[name] == {
                "attempts": attempts, "completions": completions,
            }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    ("counter", "passed_missing_phase", "schema", "version", "mode", "symlink", "bytes"),
)
async def test_store_counter_schema_path_and_file_corruption_fail_without_effects(
    tmp_path, mutation,
):
    calls: list[str] = []
    path = tmp_path / "fixture.sqlite3"
    assert (await _run_fixture(dependencies(tmp_path, calls))).result is Result.PASSED
    if mutation in {"counter", "passed_missing_phase", "schema", "version"}:
        database = sqlite3.connect(path)
        if mutation == "counter":
            database.execute("PRAGMA ignore_check_constraints=ON")
            database.execute(
                "UPDATE phase_counter SET attempts=2 WHERE name='nonce_get'"
            )
        elif mutation == "passed_missing_phase":
            database.execute(
                "UPDATE phase_counter SET attempts=0,completions=0 "
                "WHERE name='nonce_get'"
            )
        elif mutation == "schema":
            database.execute("ALTER TABLE run ADD COLUMN unexpected TEXT")
        else:
            database.execute("PRAGMA user_version=2")
        database.commit()
        database.close()
    elif mutation == "mode":
        path.chmod(0o644)
    elif mutation == "symlink":
        target = tmp_path / "target.sqlite3"
        path.rename(target)
        path.symlink_to(target)
    else:
        path.write_bytes(b"not sqlite")
        path.chmod(0o600)
    calls.clear()
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        source_factory=lambda: (_ for _ in ()).throw(AssertionError("source")),
        transport_factory=lambda: (_ for _ in ()).throw(AssertionError("transport")),
    ))
    assert report.result is Result.UNKNOWN and report.reason == "store_rejected"
    assert calls == []


@pytest.mark.asyncio
async def test_spoofed_noncanonical_schema_and_forged_passed_row_are_rejected(tmp_path):
    path = tmp_path / "fixture.sqlite3"
    invocation_id = "synthetic-risex-private-read"
    path_hash = hashlib.sha256(os.fsencode(str(path))).hexdigest()
    database = sqlite3.connect(path)
    database.executescript(
        "CREATE TABLE run ("
        "singleton INTEGER,schema_version INTEGER,invocation_id TEXT,"
        "store_path_sha256 TEXT,state TEXT,barrier_a_fingerprint TEXT,"
        "barrier_b_fingerprint TEXT,started_at_ns INTEGER,finished_at_ns INTEGER,"
        "reason TEXT);"
        "CREATE TABLE phase_counter (name TEXT,attempts INTEGER,completions INTEGER);"
    )
    database.execute(f"PRAGMA application_id={_APPLICATION_ID}")
    database.execute("PRAGMA user_version=1")
    database.execute(
        "INSERT INTO run VALUES(1,1,?,?,?, ?,?,1,2,'complete')",
        (invocation_id, path_hash, "PASSED", "1" * 64, "1" * 64),
    )
    database.executemany(
        "INSERT INTO phase_counter VALUES(?,1,1)",
        ((name,) for name in _COUNTER_NAMES),
    )
    database.commit()
    database.close()
    path.chmod(0o600)
    calls: list[str] = []
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        source_factory=lambda: (_ for _ in ()).throw(AssertionError("source")),
        transport_factory=lambda: (_ for _ in ()).throw(AssertionError("transport")),
    ))
    assert report.result is Result.UNKNOWN and report.reason == "store_rejected"
    assert calls == []


def test_fixture_rejects_production_path_without_inspecting_it():
    with pytest.raises(ValueError, match="production store"):
        _fixture_operational_private_read(
            path=FIXED_STORE_PATH,
            invocation_id="synthetic",
            source_factory=lambda: None,
            transport_factory=lambda: None,
            clock=lambda: NOW,
        )


def test_ledger_rejects_relative_path_and_no_override_is_public():
    with pytest.raises(Exception):
        DurableCounterLedger(Path("relative.sqlite3"), "synthetic")
    assert set(inspect.signature(OperationalPrivateRead).parameters) == set()
    assert set(inspect.signature(OperationalPrivateRead.run).parameters) == {"self"}
