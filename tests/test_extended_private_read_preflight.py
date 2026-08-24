import asyncio
import copy
import importlib
import json
from pathlib import Path
import subprocess
import sys

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


def contract():
    return json.loads(FIXTURE.read_text())


def wrapped(data, *, count=None):
    pagination = None if count is None else {"cursor": None, "count": count}
    return {"status": "OK", "data": data, "error": None, "pagination": pagination}


def frame(seq, *, kind="BALANCE", orders=None, positions=None, trades=None):
    return {
        "type": kind,
        "data": {
            "orders": [] if orders is None else orders,
            "positions": [] if positions is None else positions,
            "trades": [] if trades is None else trades,
            "balance": None,
            "spotBalances": None,
        },
        "error": None,
        "ts": 1770000000100 + seq,
        "seq": seq,
    }


class FixtureStream:
    def __init__(self, frames, timeline, *, outbound=None, reconnects=0):
        self.frames = list(frames)
        self.timeline = timeline
        self.outbound_frames = [] if outbound is None else list(outbound)
        self.reconnect_count = reconnects
        self.closed = False

    async def recv(self):
        self.timeline.append("RECV")
        await asyncio.sleep(0)
        if not self.frames:
            self.closed = True
            raise StopAsyncIteration
        item = self.frames.pop(0)
        if isinstance(item, BaseException):
            self.closed = True
            raise item
        return copy.deepcopy(item)

    async def final_barrier(self):
        self.timeline.append("BARRIER")
        await asyncio.sleep(0)
        if not self.frames:
            self.closed = True
            raise StopAsyncIteration
        remaining, self.frames = self.frames, []
        if isinstance(remaining[0], BaseException):
            self.closed = True
            raise remaining[0]
        return {
            "connected": True,
            "same_connection": True,
            "outbound_frames": copy.deepcopy(self.outbound_frames),
            "reconnect_count": self.reconnect_count,
            "frames": copy.deepcopy(remaining),
        }

    async def close(self):
        self.timeline.append("CLOSE")
        self.closed = True


class FixtureTransport:
    def __init__(self, *, frames=None, outbound=None, reconnects=0, mutation=None):
        data = contract()
        self.timeline = []
        self.open_calls = []
        self.get_calls = []
        self.stream = FixtureStream(
            [frame(n) for n in range(40, 45)] if frames is None else frames,
            self.timeline,
            outbound=outbound, reconnects=reconnects,
        )
        self.responses = {
            "/user/account/info": wrapped(data["account"]),
            "/user/orders": wrapped([], count=0),
            "/user/positions": wrapped([], count=0),
        }
        self.mutation = mutation

    async def get(self, request):
        self.timeline.append(f"GET_{request.round_name}_{request.path}")
        self.get_calls.append(request)
        body = copy.deepcopy(self.responses[request.path])
        if self.mutation:
            body = self.mutation(request, body)
        return {
            "status": 200,
            "final_url": request.url,
            "observed_at_ms": 1770000000000 + (0 if request.round_name == "A" else 200),
            "body": body,
        }

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


async def execute(tmp_path, transport=None, loader=None):
    transport = transport or FixtureTransport()
    loader = loader or Loader()
    result = await run_preflight(
        store=PreflightStore(tmp_path / "preflight.sqlite"),
        credential_loader=loader,
        transport=transport,
        now_ms=1770000000500,
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
    assert request.headers == {"X-Api-Key": API_KEY}
    assert request.direct_tls and not request.trust_env and not request.allow_redirects
    assert transport.stream.outbound_frames == []
    assert transport.stream.reconnect_count == 0
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
async def test_round_a_finishes_before_one_stream_spans_round_b_and_final_barrier(tmp_path):
    result, transport, _ = await execute(tmp_path)
    assert result.status == "READY_FIXTURE"
    timeline = transport.timeline
    assert timeline.index("OPEN") > max(i for i, item in enumerate(timeline) if item.startswith("GET_A_"))
    assert timeline.index("OPEN") < min(i for i, item in enumerate(timeline) if item.startswith("GET_B_"))
    assert timeline.count("OPEN") == 1
    assert timeline.count("RECV") == 4
    assert timeline.count("BARRIER") == 1
    assert timeline[-1] == "CLOSE"


@pytest.mark.asyncio
@pytest.mark.parametrize("frame_count", [0, 1, 3, 4])
async def test_early_stream_end_before_during_round_b_or_final_barrier_blocks(tmp_path, frame_count):
    result, _, _ = await execute(tmp_path, FixtureTransport(frames=[frame(n) for n in range(40, 40 + frame_count)]))
    assert result.status == "BLOCKED"
    assert result.reason == "STREAM_ENDED_EARLY"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_frame,reason",
    [
        (frame(42, orders=[{"id": 1}]), "STREAM_ORDER_ACTIVITY"),
        (frame(42, positions=[{"id": 2}]), "STREAM_POSITION_ACTIVITY"),
        (frame(42, trades=[{"id": 3}]), "STREAM_TRADE_ACTIVITY"),
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
async def test_final_barrier_drains_every_intermediate_frame(tmp_path):
    frames = [frame(n) for n in range(40, 46)]
    frames.append(frame(46, positions=[{"id": 99}]))
    result, _, _ = await execute(tmp_path, FixtureTransport(frames=frames))
    assert (result.status, result.reason) == ("BLOCKED", "STREAM_POSITION_ACTIVITY")


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
            body["data"]["id"] = ACCOUNT_ID + 1
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
    first = await run_preflight(store=store, credential_loader=loader, transport=transport, now_ms=1770000000500)
    assert first.status == "READY_FIXTURE"
    second_loader = Loader()
    second_transport = FixtureTransport()
    second = await run_preflight(store=store, credential_loader=second_loader, transport=second_transport, now_ms=1770000000600)
    assert second == first
    assert second_loader.calls == 0 and second_transport.get_calls == [] and second_transport.open_calls == []
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
        run_preflight(store=store, credential_loader=loader, transport=transport, now_ms=1770000000500)
    )
    await transport.started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    replay_loader = Loader()
    replay_transport = FixtureTransport()
    result = await run_preflight(
        store=store, credential_loader=replay_loader, transport=replay_transport, now_ms=1770000000600
    )
    assert (result.status, result.reason) == ("BLOCKED", "CANCELLED")
    assert replay_loader.calls == 0 and replay_transport.get_calls == [] and replay_transport.open_calls == []


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
