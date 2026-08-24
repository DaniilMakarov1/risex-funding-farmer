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
OFFICIAL_CLOSED_POSITION = {
    "account": ACCOUNT,
    "market_id": "1",
    "size": "0",
    "quote_amount": "0",
    "side": "SELL",
    "margin_mode": 0,
    "leverage": "11",
    "avg_entry_price": "0",
    "isolated_usdc_balance": "0",
    "last_funding_payment": "-1.25",
    "unsettled_funding": "0",
    "block_number": "0",
    "log_index": "0",
    "worker_timestamp": "0",
}


def positions_frame(*rows, **changes):
    value = {
        "method": "snapshot",
        "channel": "positions",
        "type": "snapshot",
        "data": list(rows),
        "position_count": len(rows),
        "worker_timestamp": str(int(NOW * 1_000_000_000)),
    }
    value.update(changes)
    return json.dumps(value, separators=(",", ":"))


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
    def __init__(
        self, calls: list[str], *, public_mutator=None,
        auth_response='{"method":"auth_v2","status":"success"}',
        positions_ack_response=None,
        positions_response=None,
    ) -> None:
        self._calls = calls
        self._public_index = 0
        self._public_mutator = public_mutator
        self._auth_response = auth_response
        self._positions_ack_response = positions_ack_response
        self._positions_response = positions_response

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

    async def auth_v2_receive(self):
        self._calls.append("auth_v2_receive")
        return self._auth_response

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

    async def positions_ack_receive(self):
        self._calls.append("positions_ack_receive")
        value = self._positions_ack_response
        if value is not None:
            return value
        return json.dumps({
            "method": "subscribe",
            "status": "success",
            "data": {},
            "channel": "empirical-channel",
            "type": "empirical-type",
        }, separators=(",", ":"))

    async def positions_snapshot_receive(self):
        self._calls.append("positions_snapshot_receive")
        value = self._positions_response
        if value is not None:
            return value
        return json.dumps({
            "method": "snapshot",
            "channel": "positions",
            "type": "snapshot",
            "data": [],
            "position_count": 0,
            "worker_timestamp": str(int(NOW * 1_000_000_000)),
        }, separators=(",", ":"))

    async def close(self) -> None:
        self._calls.append("transport_close")


class _AuthReceiveSocket:
    def __init__(self, message=None, *, timeout=False) -> None:
        self._message = message
        self._timeout = timeout
        self.receives = 0

    async def receive(self, **_kwargs):
        self.receives += 1
        if self._timeout:
            raise asyncio.TimeoutError
        return self._message


class _AuthReceiveTransport(SyntheticTransport):
    def __init__(self, calls, socket) -> None:
        super().__init__(calls)
        self._auth_socket = socket

    async def auth_v2_receive(self):
        self._calls.append("auth_v2_receive")
        transport = object.__new__(FixedRisexPrivateReadTransport)
        transport._socket = self._auth_socket
        return await transport.auth_v2_receive()


class _SequenceSocket:
    def __init__(self, *outcomes) -> None:
        self._outcomes = list(outcomes)
        self.receives = 0

    async def receive(self, **_kwargs):
        self.receives += 1
        outcome = self._outcomes.pop(0)
        if outcome is asyncio.TimeoutError:
            raise asyncio.TimeoutError
        return outcome


class _PositionsSocketTransport(SyntheticTransport):
    def __init__(self, calls, socket) -> None:
        super().__init__(calls)
        self._positions_socket = socket

    def _fixed(self):
        transport = object.__new__(FixedRisexPrivateReadTransport)
        transport._socket = self._positions_socket
        return transport

    async def positions_snapshot_receive(self):
        self._calls.append("positions_snapshot_receive")
        return await self._fixed().positions_snapshot_receive()

class _PositionsAckSocketTransport(SyntheticTransport):
    def __init__(self, calls, socket) -> None:
        super().__init__(calls)
        self._positions_ack_socket = socket

    async def positions_ack_receive(self):
        self._calls.append("positions_ack_receive")
        transport = object.__new__(FixedRisexPrivateReadTransport)
        transport._socket = self._positions_ack_socket
        return await transport.positions_ack_receive()


class _PositionsSequenceSocketTransport(_PositionsSocketTransport):
    async def positions_ack_receive(self):
        self._calls.append("positions_ack_receive")
        return await self._fixed().positions_ack_receive()


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
    assert INVOCATION_ID == "risex-private-read-20260824-new-op-010"
    assert STORE_BASENAME == (
        ".risex-funding-farmer-risex-private-read-20260824-new-op-010.sqlite3"
    )
    assert str(FIXED_STORE_PATH) == (
        "/Users/daniilmakarov/.risex-funding-farmer-"
        "risex-private-read-20260824-new-op-010.sqlite3"
    )
    assert hashlib.sha256(os.fsencode(str(FIXED_STORE_PATH))).hexdigest() == (
        "b1ae54ded6896f14e533a820a65faf78011506d3be0810f16ba5a86bc7ab007b"
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
        "auth_v2_dispatch", "auth_v2_receive", "orders_subscribe", "orders_snapshot",
        "positions_subscribe", "positions_ack_receive", "positions_snapshot_receive",
        "close",
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
    assert report.schema_version == 7
    assert report.auth_v2_shape is None
    assert report.auth_v2_shape_sha256 is None
    assert report.positions_schema_classifier is None
    assert report.positions_shape is None
    assert report.positions_shape_sha256 is None
    assert report.positions_method_class is None
    assert report.positions_channel_class is None
    assert report.positions_type_class is None
    assert report.positions_status_class is None
    assert report.barrier_a_fingerprint == report.barrier_b_fingerprint
    assert set(report.counters) == set(_COUNTER_NAMES)
    assert len(_COUNTER_NAMES) == 43
    assert len(operational._PRIVATE_COUNTERS) == 23
    assert all(value == {"attempts": 1, "completions": 1} for value in report.counters.values())
    assert calls[:9] == [f"public_a_{index:02d}" for index in range(1, 10)]
    assert calls[9:22] == [
        "source_load", "capability_open", "derive", "nonce_get", "sign",
        "auth_v2_dispatch", "auth_v2_receive", "orders_subscribe", "orders_snapshot",
        "positions_subscribe", "positions_ack_receive", "positions_snapshot_receive",
        "capability_close",
    ]
    assert calls[22:31] == [f"public_b_{index:02d}" for index in range(1, 10)]
    assert calls[-2:] == ["source_close", "transport_close"]
    assert path.stat().st_mode & 0o777 == 0o600
    persisted = path.read_bytes().decode("latin1")
    for forbidden in (ACCOUNT, SIGNER, SIGNATURE):
        assert forbidden not in persisted
    database = sqlite3.connect(path)
    try:
        assert database.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert database.execute("PRAGMA user_version").fetchone() == (7,)
    finally:
        database.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("raw", "reason", "failed_phase"),
    (
        ('{"method":"auth_v2",', "auth_v2_malformed", "auth_v2_parse"),
        ('{"method":"auth_v2","status":"success","status":"error"}',
         "auth_v2_malformed", "auth_v2_parse"),
        ('{"method":"auth","status":"success"}',
         "auth_v2_schema_invalid", "auth_v2_validate"),
        ('{"method":"auth_v2","status":"unknown"}',
         "auth_v2_schema_invalid", "auth_v2_validate"),
        ('{"status":"success","extra":true}',
         "auth_v2_schema_invalid", "auth_v2_validate"),
        ('{"method":"auth_v2","extra":true}',
         "auth_v2_schema_invalid", "auth_v2_validate"),
        ('{"method":7,"status":"success","extra":true}',
         "auth_v2_schema_invalid", "auth_v2_validate"),
        ('{"method":"auth_v2","status":true,"extra":true}',
         "auth_v2_schema_invalid", "auth_v2_validate"),
        ('{"method":"auth_v2","status":"error"}',
         "auth_v2_error", "auth_v2_status"),
    ),
)
async def test_auth_ack_failures_are_durable_redacted_and_never_subscribe(
    tmp_path, raw, reason, failed_phase,
):
    calls: list[str] = []
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        transport_factory=lambda: SyntheticTransport(calls, auth_response=raw),
    ))
    assert report.result is Result.UNKNOWN and report.reason == reason
    assert report.counters["auth_v2_receive"] == {"attempts": 1, "completions": 1}
    assert report.counters[failed_phase] == {"attempts": 1, "completions": 0}
    assert "orders_subscribe" not in calls and "positions_subscribe" not in calls
    if reason == "auth_v2_schema_invalid":
        assert report.auth_v2_shape is not None
        assert report.auth_v2_shape_sha256 is not None
    else:
        assert report.auth_v2_shape is None
        assert report.auth_v2_shape_sha256 is None
    assert report.positions_method_class is None
    assert report.positions_channel_class is None
    assert report.positions_type_class is None
    assert report.positions_status_class is None
    persisted = (tmp_path / "fixture.sqlite3").read_bytes().decode("latin1")
    assert raw not in persisted

    restart_calls: list[str] = []
    restarted = await _run_fixture(dependencies(
        tmp_path,
        restart_calls,
        source_factory=lambda: (_ for _ in ()).throw(AssertionError("source")),
        transport_factory=lambda: (_ for _ in ()).throw(AssertionError("transport")),
    ))
    assert restarted == report and restart_calls == []


@pytest.mark.asyncio
async def test_official_handler_parity_accepts_observed_envelope_and_drops_extras(
    tmp_path, monkeypatch,
):
    calls: list[str] = []
    extra_key = "server-controlled-extra-key"
    extra_value = "server-controlled-extra-value"
    raw = json.dumps({
        "method": "auth_v2",
        "status": "success",
        "data": {extra_key: extra_value},
        "request_id": "server-controlled-request-id",
    }, separators=(",", ":"))
    downstream = []
    original_validate = PrivateReadPreflight._validate_auth_frame

    def capture_canonical(frame):
        downstream.append(frame)
        return original_validate(frame)

    monkeypatch.setattr(
        PrivateReadPreflight, "_validate_auth_frame", staticmethod(capture_canonical),
    )
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        transport_factory=lambda: SyntheticTransport(calls, auth_response=raw),
    ))
    assert report.result is Result.PASSED and report.reason == "complete"
    assert report.auth_v2_shape is None and report.auth_v2_shape_sha256 is None
    assert "orders_subscribe" in calls and "positions_subscribe" in calls
    canonical = operational._validate_auth_v2_schema(json.loads(raw))
    assert canonical == {"method": "auth_v2", "status": "success"}
    assert set(canonical) == {"method", "status"}
    assert downstream == [{"method": "auth_v2", "status": "success"}]
    serialized = json.dumps(report.as_dict(), sort_keys=True)
    persisted = (tmp_path / "fixture.sqlite3").read_bytes().decode("latin1")
    for forbidden in (
        raw, extra_key, extra_value, "request_id", "server-controlled-request-id",
    ):
        assert forbidden not in serialized
        assert forbidden not in persisted


def test_auth_validator_does_not_iterate_or_traverse_extra_fields():
    class NoIteration(dict):
        def __iter__(self):
            raise AssertionError("frame iterated")

        def keys(self):
            raise AssertionError("frame keys traversed")

        def items(self):
            raise AssertionError("frame items traversed")

        def values(self):
            raise AssertionError("frame values traversed")

    extra = object()
    frame = NoIteration(
        method="auth_v2", status="success", data=extra, request_id=extra,
    )
    canonical = operational._validate_auth_v2_schema(frame)
    assert canonical == {"method": "auth_v2", "status": "success"}
    assert extra is frame.get("data") and extra is frame.get("request_id")


@pytest.mark.asyncio
async def test_official_error_with_extras_is_unknown_without_witness_or_subscribe(
    tmp_path,
):
    calls: list[str] = []
    secret = "server-controlled-error-extra"
    raw = json.dumps({
        "method": "auth_v2",
        "status": "error",
        "data": {"error": secret},
        "request_id": "server-controlled-request-id",
    }, separators=(",", ":"))
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        transport_factory=lambda: SyntheticTransport(calls, auth_response=raw),
    ))
    assert report.result is Result.UNKNOWN and report.reason == "auth_v2_error"
    assert report.counters["auth_v2_validate"] == {"attempts": 1, "completions": 1}
    assert report.counters["auth_v2_status"] == {"attempts": 1, "completions": 0}
    assert report.auth_v2_shape is None and report.auth_v2_shape_sha256 is None
    assert "orders_subscribe" not in calls and "positions_subscribe" not in calls
    serialized = json.dumps(report.as_dict(), sort_keys=True)
    persisted = (tmp_path / "fixture.sqlite3").read_bytes().decode("latin1")
    for forbidden in (raw, secret, "request_id", "server-controlled-request-id"):
        assert forbidden not in serialized and forbidden not in persisted

    restart_calls: list[str] = []
    restarted = await _run_fixture(dependencies(
        tmp_path,
        restart_calls,
        source_factory=lambda: (_ for _ in ()).throw(AssertionError("source")),
        transport_factory=lambda: (_ for _ in ()).throw(AssertionError("transport")),
    ))
    assert restarted == report and restart_calls == []


@pytest.mark.asyncio
async def test_schema_invalid_auth_ack_persists_only_bounded_structural_witness(
    tmp_path,
):
    calls: list[str] = []
    raw = '{"method":"auth_v2","status":"unknown","data":{"code":7}}'
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        transport_factory=lambda: SyntheticTransport(calls, auth_response=raw),
    ))
    assert report.result is Result.UNKNOWN
    assert report.reason == "auth_v2_schema_invalid"
    assert report.auth_v2_shape == (
        '{"object":[["data",{"object":[["code","number"]]}],'
        '["method","string"],["status","string"]]}'
    )
    assert report.auth_v2_shape_sha256 == hashlib.sha256(
        report.auth_v2_shape.encode("ascii")
    ).hexdigest()
    assert raw not in (tmp_path / "fixture.sqlite3").read_bytes().decode("latin1")
    assert "auth_v2" not in report.auth_v2_shape

    restart_calls: list[str] = []
    restarted = await _run_fixture(dependencies(
        tmp_path,
        restart_calls,
        source_factory=lambda: (_ for _ in ()).throw(AssertionError("source")),
        transport_factory=lambda: (_ for _ in ()).throw(AssertionError("transport")),
    ))
    assert restarted == report and restart_calls == []


@pytest.mark.asyncio
async def test_auth_shape_masks_unknown_keys_values_arrays_and_lengths(tmp_path):
    assert operational._AUTH_SHAPE_SAFE_KEYS == {
        "method", "status", "data", "result", "response", "payload", "type",
        "error", "code",
    }
    assert (
        operational._AUTH_SHAPE_MAX_DEPTH,
        operational._AUTH_SHAPE_MAX_NODES,
        operational._AUTH_SHAPE_MAX_FIELDS,
        operational._AUTH_SHAPE_MAX_BYTES,
    ) == (3, 32, 9, 512)
    calls: list[str] = []
    secrets = {
        "unknown-key-containing-server-value": "unknown-value-secret",
        "request_id": "request-id-secret",
    }
    raw = json.dumps({
        "method": "method-value-secret",
        "status": "status-value-secret",
        "data": {
            "code": "code-value-secret",
            "payload": ["array-secret-1", "array-secret-2"],
            **secrets,
        },
        "key-name-is-a-secret-value": {"result": "nested-secret"},
    }, separators=(",", ":"))
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        transport_factory=lambda: SyntheticTransport(calls, auth_response=raw),
    ))
    assert report.reason == "auth_v2_schema_invalid"
    assert report.auth_v2_shape == (
        '{"object":[["data",{"object":[["code","string"],'
        '["payload","array"]],"other_key":"present"}],'
        '["method","string"],["status","string"]],"other_key":"present"}'
    )
    serialized = json.dumps(report.as_dict(), sort_keys=True)
    persisted = (tmp_path / "fixture.sqlite3").read_bytes().decode("latin1")
    for forbidden in (
        raw, "unknown-key-containing-server-value", "unknown-value-secret",
        "request_id", "request-id-secret", "method-value-secret",
        "status-value-secret", "code-value-secret", "array-secret-1",
        "array-secret-2", "key-name-is-a-secret-value", "nested-secret",
    ):
        assert forbidden not in report.auth_v2_shape
        assert forbidden not in serialized
        assert forbidden not in persisted


@pytest.mark.asyncio
@pytest.mark.parametrize("value", (
    {f"unknown-{index}": None for index in range(10)},
    {"data": {"data": {"data": {"data": None}}}},
    {key: {"code": None, "error": None}
     for key in operational._AUTH_SHAPE_SAFE_KEYS},
    {key: {nested: None for nested in operational._AUTH_SHAPE_SAFE_KEYS}
     for key in operational._AUTH_SHAPE_SAFE_KEYS},
))
async def test_auth_shape_limits_collapse_to_one_constant_witness(tmp_path, value):
    calls: list[str] = []
    raw = json.dumps(value, separators=(",", ":"))
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        transport_factory=lambda: SyntheticTransport(calls, auth_response=raw),
    ))
    assert report.result is Result.UNKNOWN
    assert report.reason == "auth_v2_schema_invalid"
    assert report.auth_v2_shape == '{"shape":"limit_exceeded"}'
    assert len(report.auth_v2_shape.encode("ascii")) <= 512
    assert raw not in (tmp_path / "fixture.sqlite3").read_bytes().decode("latin1")


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ("shape", "digest", "forbidden_grammar"))
async def test_auth_shape_corruption_rejects_store_without_effects(tmp_path, mutation):
    calls: list[str] = []
    raw = '{"method":"auth_v2","status":"unknown","extra":true}'
    first = await _run_fixture(dependencies(
        tmp_path,
        calls,
        transport_factory=lambda: SyntheticTransport(calls, auth_response=raw),
    ))
    assert first.reason == "auth_v2_schema_invalid"
    database = sqlite3.connect(tmp_path / "fixture.sqlite3")
    if mutation == "shape":
        database.execute(
            "UPDATE run SET auth_v2_shape='\"string\"' WHERE singleton=1"
        )
    elif mutation == "digest":
        database.execute(
            "UPDATE run SET auth_v2_shape_sha256=? WHERE singleton=1", ("0" * 64,)
        )
    else:
        forbidden = '{"object":[["server-secret-key","string"]]}'
        database.execute(
            "UPDATE run SET auth_v2_shape=?,auth_v2_shape_sha256=? WHERE singleton=1",
            (forbidden, hashlib.sha256(forbidden.encode()).hexdigest()),
        )
    database.commit()
    database.close()
    calls.clear()
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        source_factory=lambda: (_ for _ in ()).throw(AssertionError("source")),
        transport_factory=lambda: (_ for _ in ()).throw(AssertionError("transport")),
    ))
    assert report.result is Result.UNKNOWN and report.reason == "store_rejected"
    assert report.auth_v2_shape is None and report.auth_v2_shape_sha256 is None
    assert calls == []


@pytest.mark.asyncio
async def test_nonterminal_shape_restart_clears_witness_without_effects(tmp_path):
    path = tmp_path / "fixture.sqlite3"
    ledger = DurableCounterLedger(path, "synthetic-risex-private-read")
    for name in operational._PUBLIC_A_COUNTERS:
        ledger.attempt(name)
        ledger.complete(name)
    ledger.set_claimed("1" * 64)
    for name in operational._PRIVATE_COUNTERS:
        ledger.attempt(name)
        if name == "auth_v2_validate":
            break
        ledger.complete(name)
    descriptor, digest = operational._auth_v2_shape({"extra": "secret-value"})
    ledger.record_auth_v2_shape(descriptor, digest)
    ledger.close()

    calls: list[str] = []
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        source_factory=lambda: (_ for _ in ()).throw(AssertionError("source")),
        transport_factory=lambda: (_ for _ in ()).throw(AssertionError("transport")),
    ))
    assert report.result is Result.UNKNOWN
    assert report.reason == "interrupted_nonterminal"
    assert report.auth_v2_shape is None and report.auth_v2_shape_sha256 is None
    assert "secret-value" not in path.read_bytes().decode("latin1")
    assert calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("message_type", "timeout", "reason"),
    (
        (None, True, "positions_ack_timeout"),
        (operational.aiohttp.WSMsgType.CLOSE, False, "positions_ack_close"),
        (operational.aiohttp.WSMsgType.ERROR, False, "positions_ack_close"),
        (operational.aiohttp.WSMsgType.BINARY, False, "positions_ack_binary"),
        (operational.aiohttp.WSMsgType.PING, False, "positions_ack_malformed"),
    ),
)
async def test_positions_ack_receive_outcomes_are_redacted_and_stop_sequence(
    tmp_path, message_type, timeout, reason,
):
    calls: list[str] = []
    secret = "ack-server-controlled-frame-data"
    outcome = asyncio.TimeoutError if timeout else type(
        "Message", (), {"type": message_type, "data": secret},
    )()
    socket = _SequenceSocket(outcome)
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        transport_factory=lambda: _PositionsAckSocketTransport(calls, socket),
    ))
    assert report.result is Result.UNKNOWN and report.reason == reason
    assert report.counters["positions_ack_receive"] == {
        "attempts": 1, "completions": 0,
    }
    assert report.counters["positions_ack_parse"] == {
        "attempts": 0, "completions": 0,
    }
    assert report.counters["positions_snapshot_receive"] == {
        "attempts": 0, "completions": 0,
    }
    assert socket.receives == 1
    persisted = (tmp_path / "fixture.sqlite3").read_bytes().decode("latin1")
    assert secret not in persisted and secret not in json.dumps(report.as_dict())
    assert not any(call.startswith("public_b_") for call in calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("message_type", "timeout", "reason"),
    (
        (None, True, "auth_v2_timeout"),
        (operational.aiohttp.WSMsgType.CLOSE, False, "auth_v2_close"),
        (operational.aiohttp.WSMsgType.ERROR, False, "auth_v2_close"),
        (operational.aiohttp.WSMsgType.BINARY, False, "auth_v2_binary"),
    ),
)
async def test_auth_receive_outcomes_are_durable_redacted_and_terminal(
    tmp_path, message_type, timeout, reason,
):
    calls: list[str] = []
    secret = "server-controlled-frame-data"
    message = None if timeout else type(
        "Message", (), {"type": message_type, "data": secret},
    )()
    socket = _AuthReceiveSocket(message, timeout=timeout)
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        transport_factory=lambda: _AuthReceiveTransport(calls, socket),
    ))
    assert report.result is Result.UNKNOWN and report.reason == reason
    assert report.auth_v2_shape is None
    assert report.auth_v2_shape_sha256 is None
    assert report.counters["auth_v2_receive"] == {"attempts": 1, "completions": 0}
    assert socket.receives == 1
    assert "orders_subscribe" not in calls and "positions_subscribe" not in calls
    persisted = (tmp_path / "fixture.sqlite3").read_bytes().decode("latin1")
    assert secret not in persisted and secret not in json.dumps(report.as_dict())

    restart_calls: list[str] = []
    restarted = await _run_fixture(dependencies(
        tmp_path,
        restart_calls,
        source_factory=lambda: (_ for _ in ()).throw(AssertionError("source")),
        transport_factory=lambda: (_ for _ in ()).throw(AssertionError("transport")),
    ))
    assert restarted == report and restart_calls == []


@pytest.mark.asyncio
async def test_official_closed_position_row_completes_split_positions_phases(tmp_path):
    calls: list[str] = []
    raw = positions_frame(OFFICIAL_CLOSED_POSITION)
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        transport_factory=lambda: SyntheticTransport(
            calls, positions_response=raw,
        ),
    ))
    assert report.result is Result.PASSED and report.reason == "complete"
    for phase in (
        "positions_ack_receive", "positions_ack_parse", "positions_ack_validate",
        "positions_snapshot_receive", "positions_snapshot_parse",
        "positions_snapshot_schema",
        "positions_flat", "positions_freshness",
    ):
        assert report.counters[phase] == {"attempts": 1, "completions": 1}
    persisted = (tmp_path / "fixture.sqlite3").read_bytes().decode("latin1")
    assert raw not in persisted
    assert ACCOUNT not in persisted


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("raw", "reason", "failed_phase"),
    (
        ('{"method":"snapshot",', "positions_snapshot_malformed", "positions_snapshot_parse"),
        (positions_frame(method="update"),
         "positions_snapshot_schema_invalid", "positions_snapshot_schema"),
        (positions_frame(position_count=1),
         "positions_snapshot_schema_invalid", "positions_snapshot_schema"),
        (positions_frame(extra=True),
         "positions_snapshot_schema_invalid", "positions_snapshot_schema"),
        (positions_frame(worker_timestamp="0"),
         "positions_snapshot_schema_invalid", "positions_snapshot_schema"),
        (positions_frame({**OFFICIAL_CLOSED_POSITION, "account": "0x" + "22" * 20}),
         "positions_snapshot_schema_invalid", "positions_snapshot_schema"),
        (positions_frame({**OFFICIAL_CLOSED_POSITION, "market_id": "0"}),
         "positions_snapshot_schema_invalid", "positions_snapshot_schema"),
        (positions_frame({**OFFICIAL_CLOSED_POSITION, "size": "not-decimal"}),
         "positions_snapshot_schema_invalid", "positions_snapshot_schema"),
        (positions_frame({**OFFICIAL_CLOSED_POSITION, "size": "0.000001"}),
         "positions_not_flat", "positions_flat"),
        (positions_frame(worker_timestamp="1"),
         "positions_stale", "positions_freshness"),
        (positions_frame(worker_timestamp=str(int((NOW + 6) * 1_000_000_000))),
         "positions_stale", "positions_freshness"),
    ),
)
async def test_positions_failures_are_split_redacted_and_terminal(
    tmp_path, raw, reason, failed_phase,
):
    calls: list[str] = []
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        transport_factory=lambda: SyntheticTransport(calls, positions_response=raw),
    ))
    assert report.result is Result.UNKNOWN and report.reason == reason
    assert report.counters[failed_phase] == {"attempts": 1, "completions": 0}
    ordered = (
        "positions_ack_receive", "positions_ack_parse", "positions_ack_validate",
        "positions_snapshot_receive", "positions_snapshot_parse",
        "positions_snapshot_schema",
        "positions_flat", "positions_freshness",
    )
    for phase in ordered[ordered.index(failed_phase) + 1:]:
        assert report.counters[phase] == {"attempts": 0, "completions": 0}
    assert report.counters["capability_close"] == {"attempts": 1, "completions": 1}
    assert report.counters["terminal_persist"] == {"attempts": 1, "completions": 1}
    assert report.auth_v2_shape is None and report.auth_v2_shape_sha256 is None
    if reason == "positions_snapshot_schema_invalid":
        assert report.positions_schema_classifier in {
            "top_envelope", "first_or_later_row",
        }
        assert report.positions_shape is not None
        assert report.positions_shape_sha256 == hashlib.sha256(
            report.positions_shape.encode("ascii")
        ).hexdigest()
        assert report.positions_method_class in operational._POSITIONS_METHOD_CLASSES
        assert report.positions_channel_class in operational._POSITIONS_CHANNEL_CLASSES
        assert report.positions_type_class in operational._POSITIONS_TYPE_CLASSES
        assert report.positions_status_class in operational._POSITIONS_STATUS_CLASSES
    else:
        assert report.positions_schema_classifier is None
        assert report.positions_shape is None
        assert report.positions_shape_sha256 is None
        assert report.positions_method_class is None
        assert report.positions_channel_class is None
        assert report.positions_type_class is None
        assert report.positions_status_class is None
    assert not any(call.startswith("public_b_") for call in calls)
    persisted = (tmp_path / "fixture.sqlite3").read_bytes().decode("latin1")
    assert raw not in persisted
    assert ACCOUNT not in persisted

    restart_calls: list[str] = []
    restarted = await _run_fixture(dependencies(
        tmp_path,
        restart_calls,
        source_factory=lambda: (_ for _ in ()).throw(AssertionError("source")),
        transport_factory=lambda: (_ for _ in ()).throw(AssertionError("transport")),
    ))
    assert restarted == report and restart_calls == []


@pytest.mark.asyncio
async def test_positions_top_extra_is_not_accepted_and_persists_only_safe_shape(
    tmp_path,
):
    assert operational._POSITIONS_SHAPE_TOP_KEYS == {
        "method", "channel", "type", "data", "position_count",
        "worker_timestamp",
    }
    assert operational._POSITIONS_SHAPE_ROW_KEYS == set(OFFICIAL_CLOSED_POSITION)
    assert (
        operational._POSITIONS_SHAPE_MAX_DEPTH,
        operational._POSITIONS_SHAPE_MAX_NODES,
        operational._POSITIONS_SHAPE_MAX_FIELDS,
        operational._POSITIONS_SHAPE_MAX_BYTES,
    ) == (3, 32, 20, 768)
    calls: list[str] = []
    raw = positions_frame(
        OFFICIAL_CLOSED_POSITION,
        **{"unknown-key-secret": "unknown-value-secret"},
    )
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        transport_factory=lambda: SyntheticTransport(calls, positions_response=raw),
    ))
    assert report.result is Result.UNKNOWN
    assert report.reason == "positions_snapshot_schema_invalid"
    assert report.positions_schema_classifier == "top_envelope"
    assert report.positions_shape is not None
    assert report.positions_shape.endswith('],"other_key":"present"}')
    assert report.counters["positions_snapshot_schema"] == {"attempts": 1, "completions": 0}
    assert report.counters["positions_flat"] == {"attempts": 0, "completions": 0}
    serialized = json.dumps(report.as_dict(), sort_keys=True)
    persisted = (tmp_path / "fixture.sqlite3").read_bytes().decode("latin1")
    for forbidden in (raw, "unknown-key-secret", "unknown-value-secret", ACCOUNT):
        assert forbidden not in report.positions_shape
        assert forbidden not in serialized
        assert forbidden not in persisted


@pytest.mark.asyncio
async def test_positions_ack_is_canonicalized_and_extras_are_never_persisted(
    tmp_path,
):
    assert "status" not in operational._POSITIONS_SHAPE_TOP_KEYS
    assert operational._POSITIONS_METHOD_CLASSES == {"snapshot", "subscribe", "other"}
    assert operational._POSITIONS_CHANNEL_CLASSES == {"positions", "other"}
    assert operational._POSITIONS_TYPE_CLASSES == {
        "snapshot", "update", "success", "error", "other",
    }
    assert operational._POSITIONS_STATUS_CLASSES == {
        "absent", "success", "error", "other",
    }
    calls: list[str] = []
    raw = json.dumps({
        "method": "subscribe",
        "channel": "positions",
        "type": "success",
        "status": "success",
        "data": {"server-payload-secret": "server-value-secret"},
        "server-unknown-key-secret": "server-unknown-value-secret",
    }, separators=(",", ":"))
    assert operational._validate_positions_ack(
        operational._parse_positions_ack(raw)
    ) == {"method": "subscribe", "status": "success"}
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        transport_factory=lambda: SyntheticTransport(
            calls, positions_ack_response=raw,
        ),
    ))
    assert report.result is Result.PASSED and report.reason == "complete"
    assert report.positions_schema_classifier is None
    assert report.positions_shape is None
    assert report.counters["positions_ack_validate"] == {
        "attempts": 1, "completions": 1,
    }
    assert report.counters["positions_snapshot_schema"] == {
        "attempts": 1, "completions": 1,
    }
    serialized = json.dumps(report.as_dict(), sort_keys=True)
    persisted = (tmp_path / "fixture.sqlite3").read_bytes().decode("latin1")
    for forbidden in (
        raw, "server-payload-secret", "server-value-secret",
        "server-unknown-key-secret", "server-unknown-value-secret",
    ):
        assert forbidden not in serialized
        assert forbidden not in persisted


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("raw", "reason", "failed_phase"),
    (
        ('{"method":"subscribe",', "positions_ack_malformed", "positions_ack_parse"),
        ('{"method":"subscribe","method":"subscribe","status":"success",'
         '"data":{},"channel":"x","type":"y"}',
         "positions_ack_malformed", "positions_ack_parse"),
        ('{"method":"subscribe","status":NaN,"data":{},'
         '"channel":"x","type":"y"}',
         "positions_ack_malformed", "positions_ack_parse"),
        (json.dumps({
            "method": "snapshot", "status": "success", "data": {},
            "channel": "positions", "type": "snapshot",
        }), "positions_ack_schema_invalid", "positions_ack_validate"),
        (json.dumps({
            "method": "subscribe", "status": "success", "data": [],
            "channel": "positions", "type": "success",
        }), "positions_ack_schema_invalid", "positions_ack_validate"),
        (json.dumps({
            "method": "subscribe", "status": "success", "data": {},
            "channel": 7, "type": "success",
        }), "positions_ack_schema_invalid", "positions_ack_validate"),
        (json.dumps({
            "method": "subscribe", "status": "success", "data": {},
            "channel": "positions", "type": None,
        }), "positions_ack_schema_invalid", "positions_ack_validate"),
        (json.dumps({
            "method": "subscribe", "status": "error", "data": {},
            "channel": "server-channel-secret", "type": "server-type-secret",
        }), "positions_ack_error", "positions_ack_validate"),
        (json.dumps({
            "method": "subscribe", "status": "error",
            "server-error-secret": "server-error-value-secret",
        }), "positions_ack_error", "positions_ack_validate"),
    ),
)
async def test_positions_ack_failures_are_terminal_redacted_and_never_read_snapshot(
    tmp_path, raw, reason, failed_phase,
):
    calls: list[str] = []
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        transport_factory=lambda: SyntheticTransport(
            calls, positions_ack_response=raw,
        ),
    ))
    assert report.result is Result.UNKNOWN and report.reason == reason
    assert report.counters[failed_phase] == {"attempts": 1, "completions": 0}
    assert report.counters["positions_snapshot_receive"] == {
        "attempts": 0, "completions": 0,
    }
    assert "positions_snapshot_receive" not in calls
    assert report.positions_schema_classifier is None
    assert report.positions_shape is None and report.positions_shape_sha256 is None
    serialized = json.dumps(report.as_dict(), sort_keys=True)
    persisted = (tmp_path / "fixture.sqlite3").read_bytes().decode("latin1")
    for forbidden in (
        raw, "server-channel-secret", "server-type-secret",
        "server-error-secret", "server-error-value-secret",
    ):
        assert forbidden not in serialized and forbidden not in persisted

    restarted = await _run_fixture(dependencies(
        tmp_path,
        [],
        source_factory=lambda: (_ for _ in ()).throw(AssertionError("source")),
        transport_factory=lambda: (_ for _ in ()).throw(AssertionError("transport")),
    ))
    assert restarted == report


def test_positions_ack_parser_enforces_existing_fixed_frame_bound(monkeypatch):
    monkeypatch.setattr(operational, "_MAX_BYTES", 32)
    with pytest.raises(operational._PositionsFailure) as failure:
        operational._parse_positions_ack("x" * 33)
    assert failure.value.reason == "positions_ack_malformed"


@pytest.mark.asyncio
async def test_valid_ack_followed_by_second_ack_is_snapshot_schema_invalid(tmp_path):
    calls: list[str] = []
    second = json.dumps({
        "method": "subscribe", "status": "success", "data": {},
        "channel": "second-channel-secret", "type": "second-type-secret",
    }, separators=(",", ":"))
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        transport_factory=lambda: SyntheticTransport(
            calls, positions_response=second,
        ),
    ))
    assert report.result is Result.UNKNOWN
    assert report.reason == "positions_snapshot_schema_invalid"
    assert report.counters["positions_ack_validate"] == {
        "attempts": 1, "completions": 1,
    }
    assert report.counters["positions_snapshot_schema"] == {
        "attempts": 1, "completions": 0,
    }
    assert report.positions_schema_classifier == "top_envelope"
    serialized = json.dumps(report.as_dict(), sort_keys=True)
    persisted = (tmp_path / "fixture.sqlite3").read_bytes().decode("latin1")
    for forbidden in (second, "second-channel-secret", "second-type-secret"):
        assert forbidden not in serialized and forbidden not in persisted


@pytest.mark.parametrize(
    ("value", "expected"),
    (
        ({"method": "snapshot"}, ("snapshot", "other", "other", "absent")),
        ({"method": "subscribe"}, ("subscribe", "other", "other", "absent")),
        ({"method": None}, ("other", "other", "other", "absent")),
        ({"channel": "positions"}, ("other", "positions", "other", "absent")),
        ({"channel": 7}, ("other", "other", "other", "absent")),
        ({"type": "snapshot"}, ("other", "other", "snapshot", "absent")),
        ({"type": "update"}, ("other", "other", "update", "absent")),
        ({"type": "success"}, ("other", "other", "success", "absent")),
        ({"type": "error"}, ("other", "other", "error", "absent")),
        ({"type": []}, ("other", "other", "other", "absent")),
        ({"status": "success"}, ("other", "other", "other", "success")),
        ({"status": "error"}, ("other", "other", "other", "error")),
        ({"status": "absent"}, ("other", "other", "other", "other")),
        ({"status": False}, ("other", "other", "other", "other")),
        ([], ("other", "other", "other", "absent")),
    ),
)
def test_positions_semantic_classes_are_exact_fixed_enums(value, expected):
    assert operational._positions_semantic_classes(value) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("frame", "expected_fragment"),
    (
        (
            {
                "method": "snapshot", "type": "snapshot", "data": [],
                "position_count": 0, "worker_timestamp": "1",
            },
            '["data",{"array":"empty"}]',
        ),
        (
            {
                "method": "snapshot", "channel": "positions", "type": "snapshot",
                "data": {}, "position_count": 0, "worker_timestamp": "1",
            },
            '["data","object"]',
        ),
        (
            {
                "method": "wrong-value-secret", "channel": "positions",
                "type": "snapshot", "data": [], "position_count": 0,
                "worker_timestamp": "1",
            },
            '["method","string"]',
        ),
    ),
)
async def test_positions_top_missing_type_and_empty_shape_are_bounded(
    tmp_path, frame, expected_fragment,
):
    calls: list[str] = []
    raw = json.dumps(frame, separators=(",", ":"))
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        transport_factory=lambda: SyntheticTransport(calls, positions_response=raw),
    ))
    assert report.reason == "positions_snapshot_schema_invalid"
    assert report.positions_schema_classifier == "top_envelope"
    assert expected_fragment in report.positions_shape
    assert "wrong-value-secret" not in report.positions_shape
    assert len(report.positions_shape.encode("ascii")) <= 768


@pytest.mark.asyncio
async def test_positions_missing_first_row_field_is_structural_only(tmp_path):
    calls: list[str] = []
    row = dict(OFFICIAL_CLOSED_POSITION)
    row.pop("size")
    raw = positions_frame(row)
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        transport_factory=lambda: SyntheticTransport(calls, positions_response=raw),
    ))
    assert report.reason == "positions_snapshot_schema_invalid"
    assert report.positions_schema_classifier == "first_or_later_row"
    assert '["size",' not in report.positions_shape
    assert raw not in (tmp_path / "fixture.sqlite3").read_bytes().decode("latin1")


@pytest.mark.asyncio
async def test_positions_row_classifier_uses_first_row_shape_without_index_or_values(
    tmp_path,
):
    calls: list[str] = []
    later = {
        **OFFICIAL_CLOSED_POSITION,
        "market_id": "2",
        "size": "later-size-value-secret",
        "later-unknown-key-secret": "later-unknown-value-secret",
    }
    raw = positions_frame(OFFICIAL_CLOSED_POSITION, later)
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        transport_factory=lambda: SyntheticTransport(calls, positions_response=raw),
    ))
    assert report.result is Result.UNKNOWN
    assert report.reason == "positions_snapshot_schema_invalid"
    assert report.positions_schema_classifier == "first_or_later_row"
    expected, digest = operational._positions_shape(json.loads(
        positions_frame(OFFICIAL_CLOSED_POSITION)
    ))
    assert report.positions_shape == expected
    assert report.positions_shape_sha256 == digest
    serialized = json.dumps(report.as_dict(), sort_keys=True)
    persisted = (tmp_path / "fixture.sqlite3").read_bytes().decode("latin1")
    for forbidden in (
        raw, "later-size-value-secret", "later-unknown-key-secret",
        "later-unknown-value-secret", ACCOUNT, "market_id\":\"2",
    ):
        assert forbidden not in report.positions_shape
        assert forbidden not in serialized
        assert forbidden not in persisted
    assert "index" not in report.positions_schema_classifier


@pytest.mark.asyncio
async def test_positions_first_row_type_tags_are_value_free_and_containers_opaque(
    tmp_path,
):
    calls: list[str] = []
    row = {
        **OFFICIAL_CLOSED_POSITION,
        "account": "account-value-secret",
        "market_id": {"nested-market-value-secret": ["array-content-secret"]},
        "size": ["size-array-content-secret"],
        "row-unknown-key-secret": "row-unknown-value-secret",
    }
    raw = positions_frame(row)
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        transport_factory=lambda: SyntheticTransport(calls, positions_response=raw),
    ))
    assert report.positions_schema_classifier == "first_or_later_row"
    assert '["account","string"]' in report.positions_shape
    assert '["market_id","object"]' in report.positions_shape
    assert '["size","array"]' in report.positions_shape
    assert '"other_key":"present"' in report.positions_shape
    serialized = json.dumps(report.as_dict(), sort_keys=True)
    persisted = (tmp_path / "fixture.sqlite3").read_bytes().decode("latin1")
    for forbidden in (
        raw, "account-value-secret", "nested-market-value-secret",
        "array-content-secret", "size-array-content-secret",
        "row-unknown-key-secret", "row-unknown-value-secret",
    ):
        assert forbidden not in report.positions_shape
        assert forbidden not in serialized
        assert forbidden not in persisted


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("limit_name", "limit_value"),
    (
        ("_POSITIONS_SHAPE_MAX_DEPTH", 1),
        ("_POSITIONS_SHAPE_MAX_NODES", 2),
        ("_POSITIONS_SHAPE_MAX_FIELDS", 5),
        ("_POSITIONS_SHAPE_MAX_BYTES", 32),
    ),
)
async def test_positions_shape_limits_collapse_to_constant_witness(
    tmp_path, monkeypatch, limit_name, limit_value,
):
    monkeypatch.setattr(operational, limit_name, limit_value)
    calls: list[str] = []
    raw = positions_frame({**OFFICIAL_CLOSED_POSITION, "size": []})
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        transport_factory=lambda: SyntheticTransport(calls, positions_response=raw),
    ))
    assert report.reason == "positions_snapshot_schema_invalid"
    assert report.positions_schema_classifier == "first_or_later_row"
    assert report.positions_shape == '{"shape":"limit_exceeded"}'
    assert report.positions_shape_sha256 == hashlib.sha256(
        report.positions_shape.encode("ascii")
    ).hexdigest()
    assert raw not in (tmp_path / "fixture.sqlite3").read_bytes().decode("latin1")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation", (
        "classifier", "shape", "digest", "grammar", "method", "channel",
        "type", "status", "partial_shape", "partial_semantic",
    ),
)
async def test_positions_shape_corruption_rejects_store_without_effects(
    tmp_path, mutation,
):
    calls: list[str] = []
    raw = positions_frame({**OFFICIAL_CLOSED_POSITION, "size": []})
    first = await _run_fixture(dependencies(
        tmp_path,
        calls,
        transport_factory=lambda: SyntheticTransport(calls, positions_response=raw),
    ))
    assert first.reason == "positions_snapshot_schema_invalid"
    database = sqlite3.connect(tmp_path / "fixture.sqlite3")
    if mutation == "classifier":
        database.execute(
            "UPDATE run SET positions_schema_classifier='row-7' WHERE singleton=1"
        )
    elif mutation == "shape":
        database.execute("UPDATE run SET positions_shape='\"object\"' WHERE singleton=1")
    elif mutation == "digest":
        database.execute(
            "UPDATE run SET positions_shape_sha256=? WHERE singleton=1", ("0" * 64,)
        )
    elif mutation == "grammar":
        forbidden = '{"object":[["server-secret-key","string"]]}'
        database.execute(
            "UPDATE run SET positions_shape=?,positions_shape_sha256=? "
            "WHERE singleton=1",
            (forbidden, hashlib.sha256(forbidden.encode()).hexdigest()),
        )
    elif mutation in {"method", "channel", "type", "status"}:
        database.execute(
            f"UPDATE run SET positions_{mutation}_class='server-secret' "
            "WHERE singleton=1"
        )
    elif mutation == "partial_shape":
        database.execute("UPDATE run SET positions_shape=NULL WHERE singleton=1")
    else:
        database.execute(
            "UPDATE run SET positions_status_class=NULL WHERE singleton=1"
        )
    database.commit()
    database.close()
    calls.clear()
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        source_factory=lambda: (_ for _ in ()).throw(AssertionError("source")),
        transport_factory=lambda: (_ for _ in ()).throw(AssertionError("transport")),
    ))
    assert report.result is Result.UNKNOWN and report.reason == "store_rejected"
    assert report.positions_schema_classifier is None
    assert report.positions_shape is None and report.positions_shape_sha256 is None
    assert report.positions_method_class is None
    assert report.positions_channel_class is None
    assert report.positions_type_class is None
    assert report.positions_status_class is None
    assert calls == []


@pytest.mark.asyncio
async def test_nonterminal_positions_shape_restart_clears_witness_without_effects(
    tmp_path,
):
    path = tmp_path / "fixture.sqlite3"
    ledger = DurableCounterLedger(path, "synthetic-risex-private-read")
    for name in operational._PUBLIC_A_COUNTERS:
        ledger.attempt(name)
        ledger.complete(name)
    ledger.set_claimed("1" * 64)
    for name in operational._PRIVATE_COUNTERS:
        ledger.attempt(name)
        if name == "positions_snapshot_schema":
            break
        ledger.complete(name)
    descriptor, digest = operational._positions_shape({
        "method": "server-secret", "data": [{"size": "server-secret"}],
    })
    ledger.record_positions_shape(
        "top_envelope", descriptor, digest,
        *operational._positions_semantic_classes({
            "method": "server-secret", "data": [{"size": "server-secret"}],
        }),
    )
    ledger.close()

    calls: list[str] = []
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        source_factory=lambda: (_ for _ in ()).throw(AssertionError("source")),
        transport_factory=lambda: (_ for _ in ()).throw(AssertionError("transport")),
    ))
    assert report.result is Result.UNKNOWN
    assert report.reason == "interrupted_nonterminal"
    assert report.positions_schema_classifier is None
    assert report.positions_shape is None and report.positions_shape_sha256 is None
    assert report.positions_method_class is None
    assert report.positions_channel_class is None
    assert report.positions_type_class is None
    assert report.positions_status_class is None
    assert "server-secret" not in path.read_bytes().decode("latin1")
    assert calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("message_type", "timeout", "reason"),
    (
        (None, True, "positions_snapshot_timeout"),
        (operational.aiohttp.WSMsgType.CLOSE, False, "positions_snapshot_close"),
        (operational.aiohttp.WSMsgType.ERROR, False, "positions_snapshot_close"),
        (operational.aiohttp.WSMsgType.BINARY, False, "positions_snapshot_binary"),
        (operational.aiohttp.WSMsgType.PING, False, "positions_snapshot_malformed"),
    ),
)
async def test_positions_receive_outcomes_are_fixed_redacted_and_terminal(
    tmp_path, message_type, timeout, reason,
):
    calls: list[str] = []
    secret = "positions-server-controlled-frame-data"
    outcome = asyncio.TimeoutError if timeout else type(
        "Message", (), {"type": message_type, "data": secret},
    )()
    socket = _SequenceSocket(outcome)
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        transport_factory=lambda: _PositionsSocketTransport(calls, socket),
    ))
    assert report.result is Result.UNKNOWN and report.reason == reason
    assert report.counters["positions_snapshot_receive"] == {"attempts": 1, "completions": 0}
    assert report.counters["positions_snapshot_parse"] == {"attempts": 0, "completions": 0}
    assert report.positions_schema_classifier is None
    assert report.positions_shape is None and report.positions_shape_sha256 is None
    assert report.positions_method_class is None
    assert report.positions_channel_class is None
    assert report.positions_type_class is None
    assert report.positions_status_class is None
    assert socket.receives == 1
    persisted = (tmp_path / "fixture.sqlite3").read_bytes().decode("latin1")
    assert secret not in persisted and secret not in json.dumps(report.as_dict())
    assert not any(call.startswith("public_b_") for call in calls)

    restart_calls: list[str] = []
    restarted = await _run_fixture(dependencies(
        tmp_path,
        restart_calls,
        source_factory=lambda: (_ for _ in ()).throw(AssertionError("source")),
        transport_factory=lambda: (_ for _ in ()).throw(AssertionError("transport")),
    ))
    assert restarted == report and restart_calls == []


@pytest.mark.asyncio
async def test_fixed_positions_leaves_official_queued_update_unconsumed(tmp_path):
    calls: list[str] = []
    ack = json.dumps({
        "method": "subscribe", "status": "success", "data": {},
        "channel": "empirical-channel", "type": "empirical-type",
    }, separators=(",", ":"))
    raw = positions_frame(OFFICIAL_CLOSED_POSITION)
    ack_message = type(
        "Message", (), {"type": operational.aiohttp.WSMsgType.TEXT, "data": ack},
    )()
    snapshot_message = type(
        "Message", (), {"type": operational.aiohttp.WSMsgType.TEXT, "data": raw},
    )()
    update_raw = json.dumps({
        "channel": "positions",
        "type": "update",
        "market_id": "1",
        "data": [{
            **OFFICIAL_CLOSED_POSITION,
            "block_number": "123",
            "log_index": "1",
            "worker_timestamp": str(int(NOW * 1_000_000_000)),
        }],
        "block_number": 123,
        "log_index": 1,
        "worker_timestamp": str(int(NOW * 1_000_000_000)),
    }, separators=(",", ":"))
    update_message = type(
        "Message", (), {
            "type": operational.aiohttp.WSMsgType.TEXT, "data": update_raw,
        },
    )()
    socket = _SequenceSocket(ack_message, snapshot_message, update_message)
    report = await _run_fixture(dependencies(
        tmp_path,
        calls,
        transport_factory=lambda: _PositionsSequenceSocketTransport(calls, socket),
    ))
    assert report.result is Result.PASSED and report.reason == "complete"
    assert socket.receives == 2
    assert update_raw not in json.dumps(report.as_dict(), sort_keys=True)
    assert update_raw not in (
        tmp_path / "fixture.sqlite3"
    ).read_bytes().decode("latin1")


def test_positions_end_to_end_freshness_has_fixed_redacted_failure():
    current = NOW + 6
    worker_timestamp = str(int(current * 1_000_000_000))
    with pytest.raises(operational._PositionsFailure) as failure:
        operational._validate_positions_freshness(
            ((), worker_timestamp), current, NOW,
        )
    assert failure.value.reason == "positions_stale"


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
            database.execute("PRAGMA user_version=8")
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
