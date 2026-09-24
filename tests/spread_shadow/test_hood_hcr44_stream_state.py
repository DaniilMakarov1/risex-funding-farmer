from __future__ import annotations

import asyncio
from decimal import Decimal
import json
import time
from types import SimpleNamespace

import pytest

from risex_spread_shadow.hood_handoff.stream_measurement import StreamIdentity
from risex_spread_shadow.hood_handoff.engine import HandoffEngine
from risex_spread_shadow.hood_handoff.contracts import HandoffConfig
from risex_spread_shadow.hood_handoff.sdk import LighterSdkClient, StaticSecretProvider
from risex_spread_shadow.hood_handoff.stream_state import (
    ReadStreamSession, ReadStreamState, StreamReadLimits,
)


def identity() -> StreamIdentity:
    return StreamIdentity.from_config({
        "market_id": 1, "market_symbol": "BTC", "source_account_index": 27331,
        "receiver_account_index": 27337, "api_key_index": 4,
        "environment": "robinhood", "api_base_url": "https://api.rh.lighter.xyz",
        "chain_id": 466324,
    })


def frame(value: dict) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode()


def book(nonce: int, *, begin: int | None = None, snapshot: bool = False) -> bytes:
    return frame({"type": "subscribed/order_book" if snapshot else "update/order_book",
                  "channel": "order_book:1",
                  "order_book": {"nonce": nonce, "begin_nonce": begin, "bids": [], "asks": []}})


def private_snapshot(account: int) -> bytes:
    return frame({"type": "subscribed/account_all_orders",
                  "channel": f"account_all_orders:{account}", "orders": {}})


def terminal(account: int = 27337, client: int = 99, order_id: str = "123") -> bytes:
    return frame({"type": "update/account_all_orders",
                  "channel": f"account_all_orders:{account}",
                  "orders": {"1": [{"owner_account_index": account,
                                    "market_index": 1, "client_order_index": client,
                                    "order_id": order_id, "status": "filled",
                                    "filled_base_amount": "0.0002",
                                    "remaining_base_amount": "0"}]}})


@pytest.mark.parametrize("override", [
    {"lifetime_seconds": 571}, {"max_frames": 15001},
    {"max_bytes": 20 * 1024 * 1024 + 1},
    {"max_frame_bytes": 8 * 1024 * 1024 + 1},
    {"max_fresh_age_seconds": 2.1},
    {"max_fresh_age_seconds": float("nan")},
])
def test_stream_read_limits_are_finite_and_bounded(override):
    with pytest.raises(ValueError):
        StreamReadLimits(**override)


def test_book_and_two_private_snapshots_ready_only_when_fresh_and_connected():
    async def scenario():
        state = ReadStreamState(identity())
        now = time.monotonic()
        await state.connected_now()
        await state.feed(book(10, snapshot=True), now)
        assert not state.subscription_ready(now)
        await state.feed(private_snapshot(27331), now + 0.1)
        assert not state.subscription_ready(now + 0.1)
        await state.feed(private_snapshot(27337), now + 0.2)
        assert state.subscription_ready(now + 0.2)
        assert not state.subscription_ready(now + 2.1)
        await state.feed(book(11, begin=10), now + 2.2)
        assert state.subscription_ready(now + 2.2)
        await state.feed(book(12, begin=0), now + 2.3)
        assert not state.subscription_ready(now + 2.3)
        await state.disconnected()
        assert not state.subscription_ready(now + 2.3)
        assert state.private_snapshot_at == {}
        await state.connected_now()
        assert not state.subscription_ready(now + 2.3)

    asyncio.run(scenario())


def test_exact_terminal_hint_wakes_only_matching_current_epoch_and_still_is_hint():
    async def scenario():
        state = ReadStreamState(identity())
        await state.connected_now()
        now = time.monotonic()
        waiter = asyncio.create_task(state.wait_terminal_hint(27337, 99, "123", 0.2))
        await asyncio.sleep(0)
        await state.feed(terminal(order_id="124"), now)
        assert not state.exact_terminal_hint(27337, 99, "123", now)
        await state.feed(terminal(order_id="123"), now + 0.01)
        # A conflicting identity does not become a hint in the same epoch.
        assert not await waiter
        await state.disconnected()
        await state.connected_now()
        await state.feed(terminal(order_id="123"), time.monotonic())
        assert await state.wait_terminal_hint(27337, 99, "123", 0.1)
        assert not await state.wait_terminal_hint(27337, 99, "123", 0.01)
        assert not state.exact_terminal_hint(27331, 99, "123", time.monotonic())
        assert not state.exact_terminal_hint(27337, 99, "124", time.monotonic())
        assert not state.exact_terminal_hint(27337, 99, "123", time.monotonic() + 3)
        await state.disconnected()
        assert not state.exact_terminal_hint(27337, 99, "123", time.monotonic())

    asyncio.run(scenario())


def test_disconnect_wakes_waiter_without_misreporting_terminal():
    async def scenario():
        state = ReadStreamState(identity())
        await state.connected_now()
        waiter = asyncio.create_task(state.wait_terminal_hint(27337, 99, None, 1))
        await asyncio.sleep(0)
        await state.disconnected()
        assert await waiter is False

    asyncio.run(scenario())


def test_session_keeps_one_read_only_socket_until_stop_and_invalidates(monkeypatch):
    sent = []
    options = {}
    stop = asyncio.Event()
    frames = [book(10, snapshot=True), private_snapshot(27331), private_snapshot(27337)]

    class Socket:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return None

        async def send(self, value):
            sent.append(json.loads(value))

        async def recv(self):
            value = frames.pop(0)
            if not frames:
                stop.set()
            return value

    async def fake_tokens(*_):
        return {27331: "PRIVATE_TOKEN_1", 27337: "PRIVATE_TOKEN_2"}

    def fake_connect(*_, **kwargs):
        options.update(kwargs)
        return Socket()

    monkeypatch.setattr("risex_spread_shadow.hood_handoff.stream_state._auth_tokens", fake_tokens)
    monkeypatch.setattr("websockets.asyncio.client.connect", fake_connect)

    async def scenario():
        state = ReadStreamState(identity())
        result = await ReadStreamSession(state).run(object(), stop)
        assert result == "stopped"
        assert not state.connected
        assert not state.subscription_ready(time.monotonic())
        assert state.observer.epoch == 1
        assert state.observer.frames == 3
        assert state.observer.private_snapshot_accounts == set()

    asyncio.run(scenario())
    assert [value["channel"] for value in sent] == [
        "order_book/1", "account_all_orders/27331", "account_all_orders/27337",
        "account_tx/27331", "account_tx/27337",
    ]
    assert all(value["type"] == "subscribe" for value in sent)
    assert "auth" not in sent[0]
    assert options["max_size"] == 8 * 1024 * 1024
    assert options["max_queue"] == 1 and options["ping_interval"] == 30


def test_session_masks_transport_error_and_never_retains_auth(monkeypatch):
    class Socket:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return None

        async def send(self, _value):
            return None

        async def recv(self):
            raise RuntimeError("PRIVATE_TOKEN_NEVER_SAVE")

    async def fake_tokens(*_):
        return {27331: "PRIVATE_TOKEN_1", 27337: "PRIVATE_TOKEN_2"}

    monkeypatch.setattr("risex_spread_shadow.hood_handoff.stream_state._auth_tokens", fake_tokens)
    monkeypatch.setattr("websockets.asyncio.client.connect", lambda *_, **__: Socket())

    async def scenario():
        state = ReadStreamState(identity())
        result = await ReadStreamSession(state).run(object(), asyncio.Event())
        assert result == "transport_error"
        assert not state.connected and state.observer.epoch == 1
        assert "PRIVATE_TOKEN" not in repr(state) + repr(result)

    asyncio.run(scenario())


def test_session_stopped_before_connect_does_not_request_credentials(monkeypatch):
    async def forbidden_tokens(*_):
        raise AssertionError("credentials were requested")

    monkeypatch.setattr("risex_spread_shadow.hood_handoff.stream_state._auth_tokens", forbidden_tokens)

    async def scenario():
        state = ReadStreamState(identity())
        stop = asyncio.Event()
        stop.set()
        assert await ReadStreamSession(state).run(object(), stop) == "stopped_before_connect"
        assert not state.connected and state.observer.frames == 0

    asyncio.run(scenario())


def test_session_stopped_during_auth_does_not_open_socket(monkeypatch):
    stop = asyncio.Event()

    async def tokens_then_stop(*_):
        stop.set()
        return {27331: "PRIVATE_TOKEN_1", 27337: "PRIVATE_TOKEN_2"}

    def forbidden_connect(*_, **__):
        raise AssertionError("socket opened after stop")

    monkeypatch.setattr("risex_spread_shadow.hood_handoff.stream_state._auth_tokens", tokens_then_stop)
    monkeypatch.setattr("websockets.asyncio.client.connect", forbidden_connect)

    async def scenario():
        state = ReadStreamState(identity())
        assert await ReadStreamSession(state).run(object(), stop) == "stopped_before_connect"
        assert not state.connected and state.observer.frames == 0

    asyncio.run(scenario())


def test_session_masks_auth_failure_before_socket(monkeypatch):
    async def failed_tokens(*_):
        raise RuntimeError("PRIVATE_TOKEN_NEVER_SAVE")

    monkeypatch.setattr("risex_spread_shadow.hood_handoff.stream_state._auth_tokens", failed_tokens)

    async def scenario():
        state = ReadStreamState(identity())
        assert await ReadStreamSession(state).run(object(), asyncio.Event()) == "transport_error"
        assert not state.connected
        assert "PRIVATE_TOKEN" not in repr(state)

    asyncio.run(scenario())


@pytest.mark.parametrize("rest_terminal", [False, True])
def test_terminal_stream_hint_only_wakes_exact_rest_proof(rest_terminal):
    class Client:
        async def wait_terminal_hint(self, account, client, order_id, timeout):
            assert (account, client, order_id) == (27337, 99, "123")
            assert 0 < timeout <= 0.1
            return True

    class Journal:
        def __init__(self):
            self.events = []

        def append(self, name, payload, **_):
            self.events.append((name, payload))

    async def scenario():
        engine = HandoffEngine(Client())
        engine._configured_freshness = 5
        engine._configured_poll_limit = 2
        engine._configured_poll_interval = 0.1
        engine._configured_order_timeout = 1
        lookups = []
        sleeps = []

        async def lookup(*_args, **_kwargs):
            lookups.append(True)
            if len(lookups) == 1 or not rest_terminal:
                return None
            return SimpleNamespace(account_index=27337, market_id=1,
                                   order_id="123", status="filled", filled_quantity="0.0002",
                                   remaining_quantity="0", observed_at=time.time(),
                                   terminal=True, active=False)

        async def sleep(seconds):
            sleeps.append(seconds)

        engine._lookup_order = lookup
        engine._sleep = sleep
        plan = SimpleNamespace(account_index=27337, market_id=1,
                               client_order_index=99, side="BUY")
        journal = Journal()
        result = await engine._poll_order(plan, "123", journal, "run", require_terminal=True)
        assert len(lookups) == 2 and sleeps == []
        assert (result is not None) is rest_terminal
        assert [name for name, _ in journal.events] == (
            ["PRIVATE_TERMINAL_HINT", "ORDER_OBSERVED"] if rest_terminal
            else ["PRIVATE_TERMINAL_HINT"])

    asyncio.run(scenario())


@pytest.mark.parametrize("hint_behavior", ["false", "error"])
def test_terminal_hint_failure_keeps_poll_interval_and_rest_authority(hint_behavior):
    class Client:
        async def wait_terminal_hint(self, *_):
            if hint_behavior == "error":
                raise RuntimeError("private stream unavailable")
            return False

    async def scenario():
        engine = HandoffEngine(Client())
        engine._configured_freshness = 5
        engine._configured_poll_limit = 2
        engine._configured_poll_interval = 0.1
        engine._configured_order_timeout = 1
        reads = []
        sleeps = []

        async def lookup(*_args, **_kwargs):
            reads.append(True)
            return None

        async def sleep(seconds):
            sleeps.append(seconds)

        engine._lookup_order = lookup
        engine._sleep = sleep
        plan = SimpleNamespace(account_index=27337, market_id=1,
                               client_order_index=99, side="BUY")
        journal = SimpleNamespace(append=lambda *_args, **_kwargs: None)
        assert await engine._poll_order(plan, "123", journal, "run", require_terminal=True) is None
        assert len(reads) == 2 and len(sleeps) == 1
        assert 0 < sleeps[0] <= 0.1

    asyncio.run(scenario())


def test_sdk_opt_in_stream_lifecycle_is_warmed_once_and_closes(monkeypatch, tmp_path):
    config = HandoffConfig(
        market_id=1, market_symbol="BTC", environment="robinhood",
        api_base_url="https://api.rh.lighter.xyz", chain_id=466324,
        direction="LONG", quantity=Decimal("0.0002"),
        source_limit_price=Decimal("100"), receiver_worst_price=Decimal("101"),
        freshness_seconds=5, request_timeout_seconds=1,
        order_timeout_seconds=1, reconcile_timeout_seconds=1,
        poll_interval_seconds=0.1, max_poll_count=2,
        source_order_lifetime_seconds=300, client_order_prefix="stream-test",
        journal_path=str(tmp_path / "unused.jsonl"), api_key_index=4,
        operator_execution_opt_in=True,
    )
    client = LighterSdkClient(
        config, source_account_index=27331, receiver_account_index=27337,
        secrets=StaticSecretProvider({27331: "synthetic-one", 27337: "synthetic-two"}),
        market_evidence={},
    )
    starts = []

    async def fake_run(self, _provider, stop):
        starts.append(True)
        await self.state.connected_now()
        await self.state.feed(book(10, snapshot=True), time.monotonic())
        await self.state.feed(private_snapshot(27331), time.monotonic())
        await self.state.feed(private_snapshot(27337), time.monotonic())
        await self.state.feed(terminal(), time.monotonic())
        await stop.wait()
        await self.state.disconnected()
        return "stopped"

    monkeypatch.setattr(ReadStreamSession, "run", fake_run)

    async def scenario():
        assert await client.start_read_stream(ready_timeout=0.5)
        summary = client.read_stream_summary()
        assert summary["ready"] is True and summary["order_events"] == 1
        assert set(summary) == {
            "connected", "ready", "frames", "bytes", "book_gaps",
            "private_subscription_controls", "order_events", "order_conflicts",
            "malformed", "stopped_reason", "ws_price_reads", "ws_order_reads",
            "transaction_events", "transaction_invalid",
        }
        assert not await client.start_read_stream(ready_timeout=0.5)
        assert await client.wait_terminal_hint(27337, 99, "123", 0.1)
        assert not await client.wait_terminal_hint(27337, 99, "123", 0.01)
        await client.aclose()
        assert not await client.wait_terminal_hint(27337, 99, "123", 0.01)

    asyncio.run(scenario())
    assert len(starts) == 1
