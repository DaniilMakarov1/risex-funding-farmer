import asyncio
import copy
import importlib
import json
from pathlib import Path
import subprocess
import sys
import sqlite3

import pytest

from risex_farmer.extended_private_read_preflight import (
    ACCOUNT_ID,
    REST_BASE_URL,
    STREAM_URL,
    PreflightStore,
    run_preflight,
)


FIXTURE = Path(__file__).parent / "fixtures/extended_private_read_preflight/official_contract.json"
API_KEY = "synthetic-api-key-never-persisted"
_EOF = object()
_BARRIER = object()
_UNSET = object()


def contract():
    return json.loads(FIXTURE.read_text())


def test_pinned_provenance_includes_response_semantic_owners():
    provenance = contract()["provenance"]
    assert provenance["repository"] == "https://github.com/x10xchange/python_sdk"
    assert provenance["commit"] == "2130cdb1cd6e7b1867db83bd3af036572d258739"
    assert {
        "x10/models/account.py",
        "x10/models/http.py",
    } <= set(provenance["sources"])


def wrapped(data, *, count=None):
    pagination = None if count is None else {"cursor": None, "count": count}
    return {"status": "OK", "data": data, "error": None, "pagination": pagination}


def transport_meta(url):
    headers = (
        ["User-Agent", "X-Api-Key"]
        if url == STREAM_URL
        else ["Accept", "Content-Type", "User-Agent", "X-Api-Key"]
    )
    return {
        "actual_url": url, "method": "GET", "header_names": headers,
        "direct_tls": True, "trust_env": False, "proxy": None, "redirects": 0,
        "retries": 0, "fallbacks": 0,
        "api_key_header_count": 1, "authorization_present": False,
        "credential_in_query": False, "credential_in_body": False,
        "application_frames_sent": False,
    }


def balance():
    return {
        "collateralName": "USD", "balance": "1000", "equity": "1000",
        "availableForTrade": "900", "availableForWithdrawal": "900",
        "unrealisedPnl": "0", "initialMargin": "0", "marginRatio": "0",
        "updatedTime": 1770000000100, "spotEquity": "0",
        "spotEquityForAvailableForTrade": "0", "collateralReservedForSpotOrders": "0",
    }


def frame(seq, *, kind="BALANCE", orders=_UNSET, positions=_UNSET, trades=_UNSET):
    data = {"orders": None, "positions": None, "trades": None, "balance": None, "spotBalances": None}
    if kind == "BALANCE":
        data["balance"] = balance()
    elif kind == "SPOT_BALANCE":
        data["spotBalances"] = []
    elif kind == "ORDER":
        data["orders"] = [] if orders is _UNSET else orders
    elif kind == "POSITION":
        data["positions"] = [] if positions is _UNSET else positions
    elif kind == "TRADE":
        data["trades"] = [] if trades is _UNSET else trades
    if orders is not _UNSET:
        data["orders"] = orders
    if positions is not _UNSET:
        data["positions"] = positions
    if trades is not _UNSET:
        data["trades"] = trades
    return {
        "type": kind,
        "data": data,
        "error": None,
        "ts": 1770000000100 + seq,
        "seq": seq,
    }


class FixtureStream:
    def __init__(self, frames, timeline, *, outbound=None, reconnects=0, barrier_override=None, metadata_mutation=None):
        self.queue = asyncio.Queue()
        for item in frames:
            self.queue.put_nowait(item)
        self.timeline = timeline
        self.outbound_frames = [] if outbound is None else list(outbound)
        self.reconnect_count = reconnects
        self.closed = False
        self.upgrade_metadata = transport_meta(STREAM_URL)
        self.barrier_override = barrier_override
        self.metadata_mutation = metadata_mutation

    async def recv(self):
        self.timeline.append("RECV")
        await asyncio.sleep(0)
        item = await self.queue.get()
        if item is _BARRIER:
            self.barrier_complete = True
            raise StopAsyncIteration
        if item is _EOF:
            self.closed = True
            raise StopAsyncIteration
        if isinstance(item, BaseException):
            self.closed = True
            raise item
        return copy.deepcopy(item)

    async def final_barrier(self):
        self.timeline.append("BARRIER")
        if isinstance(self.barrier_override, BaseException):
            raise self.barrier_override
        self.queue.put_nowait(_BARRIER)
        value = {
            "connected": True,
            "same_connection": True,
            "outbound_frames": copy.deepcopy(self.outbound_frames),
            "reconnect_count": self.reconnect_count,
            "frames": [],
            "transport": transport_meta(STREAM_URL),
            "observed_at_ms": 1770000000300,
        }
        if self.barrier_override:
            value.update(copy.deepcopy(self.barrier_override))
        if self.metadata_mutation:
            self.metadata_mutation(value["transport"])
        return value

    def push(self, item):
        self.queue.put_nowait(item)

    async def close(self):
        self.timeline.append("CLOSE")
        self.closed = True


class FixtureTransport:
    def __init__(
        self, *, frames=None, outbound=None, reconnects=0, mutation=None,
        inject_at=None, inject_item=None, barrier_override=None, metadata_mutation=None,
        observed_mutation=None,
    ):
        data = contract()
        self.timeline = []
        self.open_calls = []
        self.get_calls = []
        self.stream = FixtureStream(
            [] if frames is None else frames,
            self.timeline,
            outbound=outbound, reconnects=reconnects, barrier_override=barrier_override,
            metadata_mutation=metadata_mutation,
        )
        self.responses = {
            "/user/account/info": data["accountResponse"],
            "/user/orders": wrapped([], count=0),
            "/user/positions": wrapped([], count=0),
        }
        self.mutation = mutation
        self.inject_at = inject_at
        self.inject_item = inject_item
        self.metadata_mutation = metadata_mutation
        self.observed_mutation = observed_mutation
        if metadata_mutation:
            metadata_mutation(self.stream.upgrade_metadata)

    async def get(self, request):
        self.timeline.append(f"GET_{request.round_name}_{request.path}")
        self.get_calls.append(request)
        if self.inject_at == (request.round_name, request.path):
            self.stream.push(self.inject_item)
            await asyncio.Event().wait()
        body = copy.deepcopy(self.responses[request.path])
        if self.mutation:
            body = self.mutation(request, body)
        reply = {
            "status": 200,
            "final_url": request.url,
            "observed_at_ms": 1770000000000 + (0 if request.round_name == "A" else 200),
            "body": body,
            "transport": transport_meta(request.url),
        }
        if self.observed_mutation:
            reply["observed_at_ms"] = self.observed_mutation(request, reply["observed_at_ms"])
        if self.metadata_mutation:
            self.metadata_mutation(reply["transport"])
        return reply

    async def open_stream(self, request):
        self.timeline.append("OPEN")
        self.open_calls.append(request)
        return self.stream


class Loader:
    def __init__(self):
        self.calls = 0

    def __call__(self):
        self.calls += 1
        return API_KEY


class Clock:
    def __init__(self, values=None):
        self.values = iter(values or (
            [1770000000100] * 3 + [1770000000300] * 3 + [1770000000400]
        ))
        self.calls = 0

    def __call__(self):
        self.calls += 1
        return next(self.values)


async def execute(tmp_path, transport=None, loader=None, clock=None):
    transport = transport or FixtureTransport()
    loader = loader or Loader()
    clock = clock or Clock()
    result = await run_preflight(
        store=PreflightStore(tmp_path / "preflight.sqlite"),
        credential_loader=loader,
        transport=transport,
        clock_ms=clock,
    )
    return result, transport, loader


@pytest.mark.asyncio
async def test_ready_uses_exact_v1_path_header_and_no_application_frames(tmp_path):
    result, transport, loader = await execute(tmp_path)
    assert result.status == "READY_FIXTURE"
    assert loader.calls == 1
    assert len(transport.open_calls) == 1
    request = transport.open_calls[0]
    assert request.url == STREAM_URL == contract()["contract"]["streamUrl"]
    assert request.url == (
        "wss://starknet.sepolia.extended.exchange/"
        "stream.extended.exchange/v1/account"
    )
    assert not request.url.startswith("wss://api.starknet.sepolia.extended.exchange/")
    assert request.headers == {
        "User-Agent": "X10PythonTradingClient/2.5.0",
        "X-Api-Key": API_KEY,
    }
    assert transport.stream.upgrade_metadata["header_names"] == list(request.headers)
    assert request.direct_tls and not request.trust_env and not request.allow_redirects
    assert transport.stream.outbound_frames == []
    assert transport.stream.reconnect_count == 0
    assert result.stream_frames == 0
    assert result.clock_calls == 7
    assert len(transport.get_calls) == 6
    assert all(call.method == "GET" for call in transport.get_calls)
    assert all(
        call.headers == {"X-Api-Key": API_KEY}
        and call.direct_tls
        and not call.trust_env
        and not call.allow_redirects
        and call.retry_count == 0
        for call in transport.get_calls
    )


@pytest.mark.asyncio
async def test_captured_account_envelope_exact_shape_is_accepted(tmp_path):
    assert contract()["accountResponse"] == {
        "status": "OK",
        "data": {
            "accountId": 7001,
            "accountIndex": 3,
            "accountIndexForKeyGeneration": 3,
            "bridgeStarknetAddress": "0xabc123",
            "description": "synthetic fixture account",
            "l2Key": "0x12345",
            "l2Vault": "7001003",
            "status": "ACTIVE",
        },
    }
    result, _, _ = await execute(tmp_path)
    assert result.status == "READY_FIXTURE"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "defect",
    [
        "unknown_wrapper", "error_member", "pagination_member",
        "missing_account_index_for_key_generation", "unknown_account",
        "account_id_bool", "account_index_string", "key_generation_null",
        "bridge_null", "description_null", "l2_key_null", "l2_vault_integer",
        "status_null",
    ],
)
async def test_captured_account_envelope_unknown_fields_and_types_block(
    tmp_path, defect,
):
    def mutate(request, body):
        if request.path != "/user/account/info":
            return body
        if defect == "unknown_wrapper":
            body["unknown"] = None
        elif defect == "error_member":
            body["error"] = None
        elif defect == "pagination_member":
            body["pagination"] = None
        elif defect == "missing_account_index_for_key_generation":
            body["data"].pop("accountIndexForKeyGeneration")
        elif defect == "unknown_account":
            body["data"]["unknown"] = None
        else:
            field, value = {
                "account_id_bool": ("accountId", True),
                "account_index_string": ("accountIndex", "3"),
                "key_generation_null": ("accountIndexForKeyGeneration", None),
                "bridge_null": ("bridgeStarknetAddress", None),
                "description_null": ("description", None),
                "l2_key_null": ("l2Key", None),
                "l2_vault_integer": ("l2Vault", 7001003),
                "status_null": ("status", None),
            }[defect]
            body["data"][field] = value
        return body

    result, _, _ = await execute(tmp_path, FixtureTransport(mutation=mutate))
    expected_reason = (
        "REST_WRAPPER_MALFORMED"
        if defect in {"unknown_wrapper", "error_member", "pagination_member"}
        else "REST_ACCOUNT_MALFORMED"
    )
    assert (result.status, result.reason) == ("BLOCKED", expected_reason)


@pytest.mark.asyncio
@pytest.mark.parametrize("pagination_member", [_UNSET, None])
async def test_official_zero_list_wrappers_accept_absent_or_null_optionals(
    tmp_path, pagination_member,
):
    def mutate(request, body):
        if request.path == "/user/account/info":
            return body
        body.pop("error")
        if pagination_member is _UNSET:
            body.pop("pagination")
        else:
            body["pagination"] = pagination_member
        return body

    result, _, _ = await execute(tmp_path, FixtureTransport(mutation=mutate))
    assert result.status == "READY_FIXTURE"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "defect,reason",
    [
        ("missing_status", "REST_WRAPPER_MALFORMED"),
        ("missing_data", "REST_WRAPPER_MALFORMED"),
        ("unknown_wrapper", "REST_WRAPPER_MALFORMED"),
        ("nonnull_error", "REST_WRAPPER_MALFORMED"),
        ("nonnull_account_pagination", "REST_WRAPPER_MALFORMED"),
        ("missing_alias", "REST_ACCOUNT_MALFORMED"),
        ("duplicate_equal_alias", "REST_ACCOUNT_MALFORMED"),
        ("duplicate_conflicting_alias", "REST_ACCOUNT_MALFORMED"),
        ("description_null", "REST_ACCOUNT_MALFORMED"),
        ("bridge_empty", "REST_ACCOUNT_MALFORMED"),
        ("bridge_nonstring", "REST_ACCOUNT_MALFORMED"),
        ("inactive", "ACCOUNT_INACTIVE"),
        ("identity_mismatch", "ACCOUNT_IDENTITY_MISMATCH"),
    ],
)
async def test_account_wrapper_and_identity_contradictions_block(
    tmp_path, defect, reason,
):
    def mutate(request, body):
        if request.path != "/user/account/info":
            return body
        if defect == "missing_status":
            body.pop("status")
        elif defect == "missing_data":
            body.pop("data")
        elif defect == "unknown_wrapper":
            body["unknown"] = None
        elif defect == "nonnull_error":
            body["error"] = {"code": 1, "message": "synthetic"}
        elif defect == "nonnull_account_pagination":
            body["pagination"] = {"cursor": None, "count": 1}
        elif defect == "missing_alias":
            body["data"].pop("accountId")
        elif defect == "duplicate_equal_alias":
            body["data"]["id"] = body["data"]["accountId"]
        elif defect == "duplicate_conflicting_alias":
            body["data"]["id"] = body["data"]["accountId"] + 1
        elif defect == "description_null":
            body["data"]["description"] = None
        elif defect == "bridge_empty":
            body["data"]["bridgeStarknetAddress"] = ""
        elif defect == "bridge_nonstring":
            body["data"]["bridgeStarknetAddress"] = 1
        elif defect == "inactive":
            body["data"]["status"] = "INACTIVE"
        elif defect == "identity_mismatch":
            body["data"]["accountId"] += 1
        return body

    result, _, _ = await execute(tmp_path, FixtureTransport(mutation=mutate))
    assert (result.status, result.reason) == ("BLOCKED", reason)


@pytest.mark.asyncio
async def test_round_a_finishes_before_one_stream_spans_round_b_and_final_barrier(tmp_path):
    result, transport, _ = await execute(tmp_path)
    assert result.status == "READY_FIXTURE"
    timeline = transport.timeline
    assert timeline.index("OPEN") > max(i for i, item in enumerate(timeline) if item.startswith("GET_A_"))
    assert timeline.index("OPEN") < min(i for i, item in enumerate(timeline) if item.startswith("GET_B_"))
    assert timeline.count("OPEN") == 1
    assert timeline.count("RECV") == 1
    assert timeline.count("BARRIER") == 1
    assert timeline[-1] == "CLOSE"


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", [FixtureTransport(frames=[_EOF]), FixtureTransport(inject_at=("B", "/user/orders"), inject_item=_EOF)])
async def test_early_stream_end_before_or_during_round_b_blocks(tmp_path, transport):
    result, _, _ = await execute(tmp_path, transport)
    assert result.status == "BLOCKED"
    assert result.reason == "STREAM_ENDED_EARLY"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_frame,reason",
    [
        (frame(42, kind="ORDER", orders=[{"id": 1}]), "STREAM_ORDER_ACTIVITY"),
        (frame(42, kind="POSITION", positions=[{"id": 2}]), "STREAM_POSITION_ACTIVITY"),
        (frame(42, kind="TRADE", trades=[{"id": 3}]), "STREAM_TRADE_ACTIVITY"),
        (frame(43), "STREAM_SEQUENCE_GAP"),
        (frame(41), "STREAM_SEQUENCE_DUPLICATE"),
        (frame(40), "STREAM_SEQUENCE_REGRESSION"),
        ({"type": "MYSTERY", "data": {}, "error": None, "ts": 1, "seq": 42}, "STREAM_UNKNOWN_FRAME"),
        ({"type": "BALANCE", "data": {}, "error": None, "ts": 1, "seq": 42}, "STREAM_MALFORMED_FRAME"),
        ({"id": 1, "result": {"subscription": "account.7001"}}, "STREAM_MALFORMED_FRAME"),
    ],
)
async def test_transient_activity_sequence_and_bad_frames_block(tmp_path, bad_frame, reason):
    frames = [frame(40), frame(41), bad_frame, frame(43), frame(44)]
    result, _, _ = await execute(tmp_path, FixtureTransport(frames=frames))
    assert result.status == "BLOCKED"
    assert result.reason == reason


@pytest.mark.asyncio
async def test_server_frame_ts_is_typed_but_not_rest_freshness_authority(tmp_path):
    server_frame = frame(40)
    server_frame["ts"] = 9999999999999
    result, _, _ = await execute(tmp_path, FixtureTransport(frames=[server_frame]))
    assert result.status == "READY_FIXTURE"
    server_frame["ts"] = True
    result, _, _ = await execute(tmp_path / "bool", FixtureTransport(frames=[server_frame]))
    assert (result.status, result.reason) == ("BLOCKED", "STREAM_MALFORMED_FRAME")


@pytest.mark.asyncio
async def test_activity_injected_during_slow_round_b_is_counted_and_terminal(tmp_path):
    transport = FixtureTransport(
        inject_at=("B", "/user/orders"),
        inject_item=frame(40, kind="POSITION", positions=[{"id": 99}]),
    )
    store = PreflightStore(tmp_path / "preflight.sqlite")
    loader = Loader()
    result = await run_preflight(store=store, credential_loader=loader, transport=transport, clock_ms=Clock())
    assert (result.status, result.reason) == ("BLOCKED", "STREAM_POSITION_ACTIVITY")
    assert (result.rest_calls, result.stream_frames) == (5, 1)
    assert result.clock_calls == 4
    replay_loader, replay_transport = Loader(), FixtureTransport()
    replay_clock = Clock()
    replay = await run_preflight(store=store, credential_loader=replay_loader, transport=replay_transport, clock_ms=replay_clock)
    assert replay == result
    assert replay_loader.calls == 0 and replay_transport.get_calls == []
    assert replay_clock.calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad",
    [
        frame(40, kind="ORDER", orders=None),
        frame(40, kind="POSITION", positions=None),
        frame(40, kind="TRADE", trades=None),
        frame(40, kind="BALANCE", orders=[]),
        frame(40, kind="ORDER", positions=[]),
        {"type": "DEPOSIT", "data": {"orders": None, "positions": None, "trades": None, "balance": None, "spotBalances": None}, "error": None, "ts": 1, "seq": 40},
    ],
)
async def test_type_payload_contradictions_and_unproven_types_block(tmp_path, bad):
    result, _, _ = await execute(tmp_path, FixtureTransport(frames=[bad]))
    assert result.status == "BLOCKED"
    assert result.reason in {"STREAM_TYPE_PAYLOAD_MISMATCH", "STREAM_UNKNOWN_FRAME"}


@pytest.mark.asyncio
async def test_balance_and_spot_balance_are_exhaustively_decoded(tmp_path):
    bad_balance = frame(40)
    bad_balance["data"]["balance"].pop("equity")
    extra_balance = frame(40)
    extra_balance["data"]["balance"]["extra"] = "0"
    nan_balance = frame(40)
    nan_balance["data"]["balance"]["balance"] = "NaN"
    spot = frame(40, kind="SPOT_BALANCE")
    spot["data"]["spotBalances"] = [{
        "accountId": ACCOUNT_ID + 1, "asset": "USDC", "balance": "1", "indexPrice": "1",
        "notionalValue": "1", "contributionFactor": "1", "equityContribution": "1",
        "availableToWithdraw": None, "absolutePnl": None, "pnlPercentage": None,
        "averageEntryPrice": None, "updatedAt": 1770000000100,
    }]
    for index, bad in enumerate((bad_balance, extra_balance, nan_balance, spot)):
        result, _, _ = await execute(tmp_path / str(index), FixtureTransport(frames=[bad]))
        assert result.status == "BLOCKED"
        assert result.reason in {"STREAM_BALANCE_MALFORMED", "STREAM_SPOT_BALANCE_MALFORMED"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "key,value",
    [
        ("actual_url", "wss://wrong.invalid/v1/account"), ("method", "POST"),
        ("header_names", ["Authorization"]), ("header_names", []),
        ("header_names", ["User-Agent", "X-Api-Key", "Extra"]),
        ("api_key_header_count", 0), ("api_key_header_count", 2),
        ("authorization_present", True), ("credential_in_query", True),
        ("credential_in_body", True), ("application_frames_sent", True),
        ("direct_tls", False),
        ("trust_env", True), ("proxy", "http://proxy.invalid"),
        ("redirects", 1), ("retries", 1), ("fallbacks", 1),
        ("redirects", False), ("retries", False), ("fallbacks", False),
    ],
)
async def test_actual_transport_metadata_mismatch_blocks(tmp_path, key, value):
    def mutate(meta):
        meta[key] = value
    result, _, _ = await execute(tmp_path, FixtureTransport(metadata_mutation=mutate))
    assert (result.status, result.reason) == ("BLOCKED", "TRANSPORT_CONTRACT_MISMATCH")


@pytest.mark.asyncio
@pytest.mark.parametrize("round_name,mode", [("A", "stale"), ("A", "future"), ("B", "stale"), ("B", "future")])
async def test_each_rest_round_rejects_stale_or_future_observations(tmp_path, round_name, mode):
    def mutate(request, observed):
        if request.round_name != round_name:
            return observed
        return 1769999990000 if mode == "stale" else 1770000001000
    result, _, _ = await execute(tmp_path, FixtureTransport(observed_mutation=mutate))
    assert result.status == "BLOCKED"
    assert result.reason == ("EVIDENCE_STALE" if mode == "stale" else "EVIDENCE_FROM_FUTURE")


@pytest.mark.asyncio
@pytest.mark.parametrize("observed", [True, "1770000000000"])
async def test_non_integer_rest_observation_blocks(tmp_path, observed):
    result, _, _ = await execute(
        tmp_path, FixtureTransport(observed_mutation=lambda request, value: observed)
    )
    assert (result.status, result.reason) == ("BLOCKED", "CLOCK_EVIDENCE_INVALID")


@pytest.mark.asyncio
@pytest.mark.parametrize("current", [True, "1770000000100"])
async def test_non_integer_current_clock_blocks(tmp_path, current):
    result, _, _ = await execute(tmp_path, clock=Clock([current]))
    assert (result.status, result.reason) == ("BLOCKED", "CLOCK_EVIDENCE_INVALID")


@pytest.mark.asyncio
async def test_current_clock_regression_blocks(tmp_path):
    values = [1770000000100] * 3 + [1770000000050]
    result, _, _ = await execute(tmp_path, clock=Clock(values))
    assert (result.status, result.reason) == ("BLOCKED", "CLOCK_REGRESSION")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "override,reason,initial",
    [
        (StopAsyncIteration(), "STREAM_ENDED_EARLY", []),
        (ConnectionError("end"), "STREAM_DISCONNECTED", []),
        ({"connected": False}, "STREAM_ENDED_EARLY", []),
        ({"same_connection": False}, "STREAM_ENDED_EARLY", []),
        ({"extra": 1}, "STREAM_BARRIER_UNVERIFIABLE", []),
        ({"outbound_frames": [{"method": "subscribe"}]}, "STREAM_OUTBOUND_FORBIDDEN", []),
        ({"reconnect_count": 1}, "STREAM_RECONNECT_FORBIDDEN", []),
        ({"observed_at_ms": 1769999990000}, "EVIDENCE_STALE", []),
        ({"observed_at_ms": 1770000001000}, "EVIDENCE_FROM_FUTURE", []),
        ({"observed_at_ms": True}, "CLOCK_EVIDENCE_INVALID", []),
        ({"observed_at_ms": 1770000000100}, "STREAM_BARRIER_TIME_INVALID", []),
        ({"frames": [frame(41, kind="POSITION", positions=[{"id": 1}])]}, "STREAM_POSITION_ACTIVITY", []),
        ({"frames": [frame(42)]}, "STREAM_SEQUENCE_GAP", [frame(40)]),
        ({"frames": [frame(40)]}, "STREAM_SEQUENCE_DUPLICATE", [frame(40)]),
        ({"frames": [frame(39)]}, "STREAM_SEQUENCE_REGRESSION", [frame(40)]),
    ],
)
async def test_final_barrier_adverse_contracts_block(tmp_path, override, reason, initial):
    result, _, _ = await execute(tmp_path, FixtureTransport(frames=initial, barrier_override=override))
    assert (result.status, result.reason) == ("BLOCKED", reason)


@pytest.mark.asyncio
async def test_corrupt_terminal_evidence_never_replays_or_returns_ready(tmp_path):
    store = PreflightStore(tmp_path / "preflight.sqlite")
    first, _, _ = await execute(tmp_path)
    assert first.status == "READY_FIXTURE"
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "UPDATE extended_private_read_preflight SET evidence=? WHERE singleton=1",
            ('{"identity_verified":"yes","reason":"FIXTURE_CONTRACT_PROVED","rest_calls":6,"status":"READY_FIXTURE","stream_frames":0}',),
        )
    loader, transport = Loader(), FixtureTransport()
    corrupt_clock = Clock()
    with pytest.raises(Exception, match="DURABLE_EVIDENCE_INVALID"):
        await run_preflight(store=store, credential_loader=loader, transport=transport, clock_ms=corrupt_clock)
    assert loader.calls == 0 and transport.get_calls == [] and transport.open_calls == []
    assert corrupt_clock.calls == 0


@pytest.mark.asyncio
async def test_disconnect_and_reconnect_or_outbound_protocol_frames_block(tmp_path):
    disconnected = FixtureTransport(frames=[frame(40), ConnectionError("fixture disconnect")])
    result, _, _ = await execute(tmp_path / "disconnect", disconnected)
    assert (result.status, result.reason) == ("BLOCKED", "STREAM_DISCONNECTED")
    for name, transport, reason in (
        ("reconnect", FixtureTransport(reconnects=1), "STREAM_RECONNECT_FORBIDDEN"),
        ("outbound", FixtureTransport(outbound=[{"method": "subscribe"}]), "STREAM_OUTBOUND_FORBIDDEN"),
    ):
        result, _, _ = await execute(tmp_path / name, transport)
        assert (result.status, result.reason) == ("BLOCKED", reason)


@pytest.mark.asyncio
async def test_rounds_are_fresh_exhaustive_exact_identity_zero_and_flat(tmp_path):
    def mutate(request, body):
        if request.round_name == "B" and request.path == "/user/orders":
            body["pagination"]["cursor"] = 99
        return body
    result, _, _ = await execute(tmp_path, FixtureTransport(mutation=mutate))
    assert (result.status, result.reason) == ("BLOCKED", "REST_PAGINATION_INCOMPLETE")

    def wrong_identity(request, body):
        if request.path == "/user/account/info":
            identity_key = "accountId" if "accountId" in body["data"] else "id"
            body["data"][identity_key] = ACCOUNT_ID + 1
        return body
    result, _, _ = await execute(tmp_path / "identity", FixtureTransport(mutation=wrong_identity))
    assert (result.status, result.reason) == ("BLOCKED", "ACCOUNT_IDENTITY_MISMATCH")

    for endpoint, reason in (("/user/orders", "REST_OPEN_ORDER_PRESENT"), ("/user/positions", "REST_POSITION_PRESENT")):
        def active(request, body, endpoint=endpoint):
            if request.round_name == "B" and request.path == endpoint:
                body["data"] = [{"id": 1}]
                body["pagination"]["count"] = 1
            return body
        result, _, _ = await execute(tmp_path / endpoint.rsplit('/', 1)[-1], FixtureTransport(mutation=active))
        assert (result.status, result.reason) == ("BLOCKED", reason)


@pytest.mark.asyncio
async def test_terminal_restart_makes_zero_loader_or_transport_calls_and_evidence_is_redacted(tmp_path):
    store = PreflightStore(tmp_path / "preflight.sqlite")
    loader = Loader()
    transport = FixtureTransport()
    first = await run_preflight(store=store, credential_loader=loader, transport=transport, clock_ms=Clock())
    assert first.status == "READY_FIXTURE"
    second_loader = Loader()
    second_transport = FixtureTransport()
    second_clock = Clock()
    second = await run_preflight(store=store, credential_loader=second_loader, transport=second_transport, clock_ms=second_clock)
    assert second == first
    assert second_loader.calls == 0 and second_transport.get_calls == [] and second_transport.open_calls == []
    assert second_clock.calls == 0
    evidence = (tmp_path / "preflight.sqlite").read_bytes()
    assert API_KEY.encode() not in evidence
    assert b"synthetic fixture account" not in evidence


@pytest.mark.asyncio
async def test_cancellation_is_durably_terminal_and_cannot_replay(tmp_path):
    class BlockingTransport(FixtureTransport):
        def __init__(self):
            super().__init__()
            self.started = asyncio.Event()

        async def get(self, request):
            self.started.set()
            await asyncio.Event().wait()

    store = PreflightStore(tmp_path / "preflight.sqlite")
    loader = Loader()
    transport = BlockingTransport()
    task = asyncio.create_task(
        run_preflight(store=store, credential_loader=loader, transport=transport, clock_ms=Clock())
    )
    await transport.started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    replay_loader = Loader()
    replay_transport = FixtureTransport()
    replay_clock = Clock()
    result = await run_preflight(
        store=store, credential_loader=replay_loader, transport=replay_transport, clock_ms=replay_clock
    )
    assert (result.status, result.reason) == ("BLOCKED", "CANCELLED")
    assert (result.rest_calls, result.stream_frames) == (1, 0)
    assert result.clock_calls == 0
    assert replay_loader.calls == 0 and replay_transport.get_calls == [] and replay_transport.open_calls == []
    assert replay_clock.calls == 0


def test_module_is_not_imported_by_normal_startup():
    assert REST_BASE_URL.startswith("https://")
    assert importlib.import_module("risex_farmer.extended_private_read_preflight")
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys, risex_farmer.cli; "
            "assert 'risex_farmer.extended_private_read_preflight' not in sys.modules",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
