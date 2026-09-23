from __future__ import annotations

import asyncio
import json

import pytest

from risex_spread_shadow.hood_handoff.offline_observer import ObserverLimits
from risex_spread_shadow.hood_handoff.stream_measurement import (
    MAX_DECOMPRESSED_FRAME_BYTES, MAX_SESSION_BYTES, MAX_SESSION_FRAMES,
    MAX_SESSION_SECONDS, StreamIdentity, StreamProjection, collect_once,
)


def config(**changes):
    value = {"market_id": 1, "market_symbol": "BTC", "source_account_index": 27331,
             "receiver_account_index": 27337, "api_key_index": 4,
             "environment": "robinhood", "api_base_url": "https://api.rh.lighter.xyz",
             "chain_id": 466324}
    value.update(changes)
    return value


def encoded(value):
    return json.dumps(value, separators=(",", ":")).encode()


def book(kind, nonce, begin=None):
    return encoded({"type": kind, "channel": "order_book:1",
                    "order_book": {"nonce": nonce, "begin_nonce": begin,
                                   "asks": [{"price": "100", "size": "0.2"}]}})


def private(status="open", *, filled="0", remaining="0.0002", client=99,
            account=27331, market=1, owner=None, order_id="12345"):
    return encoded({"type": "update/account_all_orders",
                    "channel": f"account_all_orders:{account}",
                    "auth": "PRIVATE_TOKEN_NEVER_SAVE",
                    "orders": {"1": [{"owner_account_index": account if owner is None else owner,
                                      "market_index": market, "client_order_index": client,
                                      "order_id": order_id, "status": status,
                                      "filled_base_amount": filled,
                                      "remaining_base_amount": remaining,
                                      "Sig": "PRIVATE_SIGNATURE_NEVER_SAVE"}]}})


@pytest.mark.parametrize("change", [
    {"market_id": True}, {"market_id": 0}, {"market_symbol": "ETH"},
    {"source_account_index": True}, {"source_account_index": 27337},
    {"receiver_account_index": 27337.0}, {"api_key_index": False},
    {"chain_id": 4663}, {"environment": "mainnet"},
    {"api_base_url": "https://mainnet.zklighter.elliot.ai"},
])
def test_config_enforces_exact_robinhood_btc_two_account_gate(change):
    with pytest.raises(ValueError):
        StreamIdentity.from_config(config(**change))


def test_book_requires_subscription_snapshot_and_exact_continuity():
    observer = StreamProjection(StreamIdentity.from_config(config()))
    assert observer.feed(book("update/order_book", 11, 10), 1)[0]["kind"] == "book_unanchored"
    assert not observer.book_snapshot
    assert observer.feed(book("subscribed/order_book", 12), 2)[0]["kind"] == "book_snapshot"
    assert observer.feed(book("update/order_book", 13, 12), 3)[0]["kind"] == "book_update"
    assert observer.feed(book("update/order_book", 13, 12), 4)[0]["kind"] == "book_gap"
    observer.reconnect()
    assert observer.feed(book("update/order_book", 14, 13), 5)[0]["kind"] == "book_unanchored"
    assert observer.epoch == 1
    assert observer.book_gaps == 3


def test_private_partial_duplicate_terminal_reorder_and_redaction():
    observer = StreamProjection(StreamIdentity.from_config(config()))
    first = observer.feed(private(), 1)[0]
    assert first["kind"] == "order" and first["filled_base_amount"] == "0"
    partial = observer.feed(private(filled="0.0001", remaining="0.0001"), 2)[0]
    assert partial["filled_base_amount"] == "0.0001"
    assert observer.feed(private(filled="0.0001", remaining="0.0001"), 3) == []
    assert observer.feed(private(filled="0", remaining="0.0002"), 4)[0]["kind"] == "order_conflict"
    terminal = observer.feed(private("filled", filled="0.0002", remaining="0"), 5)[0]
    assert terminal["terminal"] is True and terminal["stream_complete"] is False
    assert observer.feed(private("open", filled="0.0002", remaining="0"), 6)[0]["kind"] == "order_conflict"
    assert observer.order_duplicates == 1 and observer.order_conflicts == 2
    assert "PRIVATE_TOKEN" not in repr(observer) + repr([first, partial, terminal])
    assert "PRIVATE_SIGNATURE" not in repr(observer) + repr([first, partial, terminal])


@pytest.mark.parametrize("raw", [
    private(owner=True), private(market=True), private(client=True),
    private(owner=27337), private(market=2), private(client=-1),
    private(filled="NaN"), private(remaining="-1"),
    private(order_id="PRIVATE_TOKEN_NEVER_SAVE"),
])
def test_private_invalid_identity_or_quantity_cannot_emit_order(raw):
    observer = StreamProjection(StreamIdentity.from_config(config()))
    assert observer.feed(raw, 1) == []
    assert observer.order_events == 0


def test_other_market_and_account_do_not_become_observations():
    observer = StreamProjection(StreamIdentity.from_config(config()))
    assert observer.feed(private(account=999), 1) == []
    assert observer.feed(encoded({"type": "update/order_book", "channel": "order_book:2",
                                  "order_book": {"nonce": 1}}), 2) == []
    assert observer.order_events == 0 and observer.book_gaps == 0


def test_raw_venue_times_are_kept_separate_from_local_receipt_clock():
    observer = StreamProjection(StreamIdentity.from_config(config()))
    snapshot = json.loads(book("subscribed/order_book", 4))
    snapshot["timestamp"] = 123
    snapshot["order_book"]["last_updated_at"] = 456
    event = observer.feed(encoded(snapshot), 10.5)[0]
    assert event["received_at"] == 10.5
    assert event["venue_timestamp_raw"] == 123
    assert event["venue_last_updated_at_raw"] == 456
    order = json.loads(private())
    order["orders"]["1"][0]["timestamp"] = 789
    order["orders"]["1"][0]["transaction_time"] = True
    result = observer.feed(encoded(order), 11)[0]
    assert result["venue_timestamp_raw"] == 789
    assert "venue_transaction_time_raw" not in result


def test_limits_reconnect_chronology_and_malformed_frame():
    identity = StreamIdentity.from_config(config())
    raw = private()
    observer = StreamProjection(identity, ObserverLimits(max_seconds=2, max_frames=2,
                                                          max_bytes=2 * len(raw), max_frame_bytes=len(raw)))
    assert observer.feed(b"{", 10) == []
    assert observer.malformed == 1
    observer.reconnect()
    assert observer.feed(raw, 11)[0]["kind"] == "order"
    assert observer.feed(raw, 10.5) == []
    assert observer.stopped_reason == "receipt_time_regression"
    assert observer.frames == 2
    duration = StreamProjection(identity, ObserverLimits(max_seconds=1))
    duration.feed(raw, 10)
    duration.reconnect()
    assert duration.feed(raw, 12) == [] and duration.stopped_reason == "duration_limit"
    too_big = StreamProjection(identity, ObserverLimits(max_frame_bytes=len(raw)-1))
    assert too_big.feed(raw, 1) == [] and too_big.stopped_reason == "frame_limit_bytes"


def test_connection_and_server_error_controls_save_no_payload():
    observer = StreamProjection(StreamIdentity.from_config(config()))
    assert observer.feed(encoded({"type": "connected", "session": "PRIVATE_TOKEN"}), 1) == []
    assert observer.connected_controls == 1 and observer.malformed == 0
    assert observer.feed(encoded({"type": "error", "message": "PRIVATE_TOKEN"}), 2) == []
    assert observer.stopped_reason == "server_error_control"
    assert observer.server_error_controls == 1
    assert "PRIVATE_TOKEN" not in repr(observer.summary())


def test_connection_sends_only_three_read_subscriptions_and_saves_projections(monkeypatch, tmp_path):
    identity = StreamIdentity.from_config(config())
    frames = [book("subscribed/order_book", 10), private("filled", filled="0.0002", remaining="0")]
    sent = []

    class Socket:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return None

        async def send(self, value):
            sent.append(json.loads(value))

        async def recv(self):
            return frames.pop(0)

    async def fake_tokens(*_):
        return {27331: "PRIVATE_TOKEN_1", 27337: "PRIVATE_TOKEN_2"}

    monkeypatch.setattr("risex_spread_shadow.hood_handoff.stream_measurement._auth_tokens", fake_tokens)
    socket_options = {}

    def connect(*_, **options):
        socket_options.update(options)
        return Socket()

    monkeypatch.setattr("websockets.asyncio.client.connect", connect)
    output = tmp_path / "events.jsonl"
    result = asyncio.run(collect_once(identity, output, object(),
                                      limits=ObserverLimits(max_seconds=5, max_frames=2,
                                                            max_bytes=MAX_SESSION_BYTES,
                                                            max_frame_bytes=MAX_DECOMPRESSED_FRAME_BYTES)))
    assert result["stopped_reason"] == "frame_count_limit"
    assert [x["channel"] for x in sent] == ["order_book/1", "account_all_orders/27331", "account_all_orders/27337"]
    assert all(x["type"] == "subscribe" for x in sent)
    assert socket_options["max_size"] == MAX_DECOMPRESSED_FRAME_BYTES
    assert socket_options["max_queue"] == 1
    assert "auth" not in sent[0]
    saved = output.read_text()
    assert "PRIVATE_TOKEN" not in saved and "PRIVATE_SIGNATURE" not in saved
    assert [json.loads(line)["kind"] for line in saved.splitlines()] == ["book_snapshot", "order"]
    assert output.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize("limits", [
    ObserverLimits(max_seconds=MAX_SESSION_SECONDS + 1, max_frames=MAX_SESSION_FRAMES,
                   max_bytes=MAX_SESSION_BYTES),
    ObserverLimits(max_seconds=MAX_SESSION_SECONDS, max_frames=MAX_SESSION_FRAMES + 1,
                   max_bytes=MAX_SESSION_BYTES),
    ObserverLimits(max_seconds=MAX_SESSION_SECONDS, max_frames=MAX_SESSION_FRAMES,
                   max_bytes=MAX_SESSION_BYTES + 1),
    ObserverLimits(max_seconds=MAX_SESSION_SECONDS, max_frames=MAX_SESSION_FRAMES,
                   max_bytes=MAX_SESSION_BYTES,
                   max_frame_bytes=MAX_DECOMPRESSED_FRAME_BYTES + 1),
])
def test_collection_gate_rejects_expanded_limits_before_socket(tmp_path, limits):
    identity = StreamIdentity.from_config(config())
    with pytest.raises(ValueError):
        asyncio.run(collect_once(identity, tmp_path / "events.jsonl", object(),
                                 limits=limits))


def test_larger_snapshot_is_projected_without_retaining_raw_payload(monkeypatch, tmp_path):
    raw = encoded({"type": "subscribed/order_book", "channel": "order_book:1",
                   "order_book": {"nonce": 17, "asks": []},
                   "private_padding": "PRIVATE_SIGNATURE_NEVER_SAVE" * 4000})
    assert 65536 < len(raw) < MAX_DECOMPRESSED_FRAME_BYTES

    class Socket:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return None

        async def send(self, _value):
            return None

        async def recv(self):
            return raw

    async def fake_tokens(*_):
        return {27331: "PRIVATE_TOKEN_1", 27337: "PRIVATE_TOKEN_2"}

    monkeypatch.setattr("risex_spread_shadow.hood_handoff.stream_measurement._auth_tokens", fake_tokens)
    monkeypatch.setattr("websockets.asyncio.client.connect", lambda *_, **__: Socket())
    output = tmp_path / "events.jsonl"
    result = asyncio.run(collect_once(StreamIdentity.from_config(config()), output,
                                      object(), limits=ObserverLimits(max_seconds=1, max_frames=1,
                                                                    max_bytes=MAX_SESSION_BYTES,
                                                                    max_frame_bytes=MAX_DECOMPRESSED_FRAME_BYTES)))
    assert result["stopped_reason"] == "frame_count_limit"
    assert result["book_snapshot"] is True
    assert result["bytes"] == len(raw)
    saved = output.read_text()
    assert [json.loads(line)["kind"] for line in saved.splitlines()] == ["book_snapshot"]
    assert "PRIVATE_SIGNATURE" not in saved + repr(result)


def test_transport_close_records_only_safe_code_not_reason(monkeypatch, tmp_path):
    from websockets.exceptions import ConnectionClosedError
    from websockets.frames import Close

    async def fake_tokens(*_):
        return {27331: "PRIVATE_TOKEN_1", 27337: "PRIVATE_TOKEN_2"}

    class Socket:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return None

        async def send(self, _value):
            return None

        async def recv(self):
            raise ConnectionClosedError(Close(1009, "PRIVATE_TOKEN_NEVER_SAVE"), None)

    monkeypatch.setattr("risex_spread_shadow.hood_handoff.stream_measurement._auth_tokens", fake_tokens)
    monkeypatch.setattr("websockets.asyncio.client.connect", lambda *_, **__: Socket())
    output = tmp_path / "events.jsonl"
    result = asyncio.run(collect_once(StreamIdentity.from_config(config()), output,
                                      object(), limits=ObserverLimits(max_seconds=1, max_frames=2,
                                                                    max_bytes=MAX_SESSION_BYTES)))
    assert result["transport_error_class"] == "connection_closed"
    assert result["transport_close_code"] == 1009
    assert result["transport_sent_close_code"] is None
    assert "PRIVATE_TOKEN" not in repr(result) + output.read_text()


def test_local_oversize_close_code_is_recorded_without_reason(monkeypatch, tmp_path):
    from websockets.exceptions import ConnectionClosedError
    from websockets.frames import Close

    async def fake_tokens(*_):
        return {27331: "PRIVATE_TOKEN_1", 27337: "PRIVATE_TOKEN_2"}

    class Socket:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return None

        async def send(self, _value):
            return None

        async def recv(self):
            raise ConnectionClosedError(None, Close(1009, "PRIVATE_SIGNATURE_NEVER_SAVE"))

    monkeypatch.setattr("risex_spread_shadow.hood_handoff.stream_measurement._auth_tokens", fake_tokens)
    monkeypatch.setattr("websockets.asyncio.client.connect", lambda *_, **__: Socket())
    output = tmp_path / "events.jsonl"
    result = asyncio.run(collect_once(StreamIdentity.from_config(config()), output,
                                      object(), limits=ObserverLimits(max_seconds=1, max_frames=2,
                                                                    max_bytes=MAX_SESSION_BYTES)))
    assert result["transport_close_code"] is None
    assert result["transport_sent_close_code"] == 1009
    assert "PRIVATE_SIGNATURE" not in repr(result) + output.read_text()
