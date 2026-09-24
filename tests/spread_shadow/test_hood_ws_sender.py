from __future__ import annotations

import asyncio
import json
import time

import pytest

from risex_spread_shadow.hood_handoff.ws_sender import WarmTxSender


class Socket:
    def __init__(self):
        self.incoming = asyncio.Queue()
        self.sent = []
        self.closed = False

    async def send(self, raw):
        self.sent.append(json.loads(raw))

    async def recv(self):
        value = await self.incoming.get()
        if isinstance(value, Exception):
            raise value
        return value

    async def close(self):
        self.closed = True


async def wait_sent(socket: Socket, count: int):
    for _ in range(100):
        if len(socket.sent) >= count:
            return
        await asyncio.sleep(0.001)
    raise AssertionError("sender did not write")


@pytest.mark.asyncio
async def test_warmed_sender_requires_exact_echo_and_ignores_mixed_controls():
    socket = Socket()
    sender = WarmTxSender(connect_factory=lambda: asyncio.sleep(0, result=socket))
    await sender.start()
    tx_hash = "0x" + "a" * 64
    pending = asyncio.create_task(sender.send(14, '{"Sig":"PRIVATE"}', tx_hash,
                                              deadline=time.monotonic() + 1))
    await wait_sent(socket, 1)
    request = socket.sent[0]
    assert request["type"] == "jsonapi/sendtx"
    assert request["data"]["tx_info"] == {"Sig": "PRIVATE"}
    request_id = request["data"]["id"]
    await socket.incoming.put('{"type":"connected"}')
    await socket.incoming.put('{"type":"ping"}')
    await socket.incoming.put(json.dumps({"type": "jsonapi/sendtx", "data": {
        "id": "wrong", "code": 200, "tx_hash": tx_hash,
    }}))
    await asyncio.sleep(0)
    assert not pending.done()
    await socket.incoming.put(json.dumps({"type": "jsonapi/sendtx", "data": {
        "id": request_id, "code": 200, "tx_hash": tx_hash,
    }}))
    result = await pending
    assert result["code"] == 200
    assert result["_transport_timing"]["ws_write_seconds"] >= 0
    assert any(message == {"type": "pong"} for message in socket.sent)
    await sender.close()
    assert socket.closed


@pytest.mark.asyncio
async def test_ws_sender_rejects_hash_conflict_and_missing_reply_is_unknown():
    socket = Socket()
    sender = WarmTxSender(connect_factory=lambda: asyncio.sleep(0, result=socket))
    await sender.start()
    tx_hash = "0x" + "a" * 64
    pending = asyncio.create_task(sender.send(14, "{}", tx_hash,
                                              deadline=time.monotonic() + 1))
    await wait_sent(socket, 1)
    request_id = socket.sent[0]["data"]["id"]
    await socket.incoming.put(json.dumps({"data": {"id": request_id,
                                                     "code": 200,
                                                     "tx_hash": "0x" + "b" * 64}}))
    with pytest.raises(RuntimeError, match="hash conflicts"):
        await pending
    with pytest.raises(RuntimeError, match="already attempted"):
        await sender.send(14, "{}", tx_hash, deadline=time.monotonic() + 1)
    missing = asyncio.create_task(sender.send(14, "{}", "0x" + "b" * 64,
                                              deadline=time.monotonic() + 0.02))
    await wait_sent(socket, 2)
    with pytest.raises(TimeoutError):
        await missing
    assert len([x for x in socket.sent if x.get("type") == "jsonapi/sendtx"]) == 2
    await sender.close()


@pytest.mark.asyncio
async def test_ws_sender_disconnect_after_write_and_canceled_wait_remain_unknown():
    socket = Socket()
    sender = WarmTxSender(connect_factory=lambda: asyncio.sleep(0, result=socket))
    await sender.start()
    tx_hash = "0x" + "c" * 64
    pending = asyncio.create_task(sender.send(15, "{}", tx_hash,
                                              deadline=time.monotonic() + 1))
    await wait_sent(socket, 1)
    await socket.incoming.put(ConnectionError("secret details never returned"))
    with pytest.raises(ConnectionError, match="before exact ACK"):
        await pending
    with pytest.raises(ConnectionError, match="not ready"):
        await sender.send(15, "{}", tx_hash, deadline=time.monotonic() + 1)
    await sender.close()

    other = Socket()
    sender = WarmTxSender(connect_factory=lambda: asyncio.sleep(0, result=other))
    await sender.start()
    awaiting = asyncio.create_task(sender.send(14, "{}", tx_hash,
                                               deadline=time.monotonic() + 1))
    await wait_sent(other, 1)
    awaiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await awaiting
    assert len(other.sent) == 1
    await sender.close()


@pytest.mark.asyncio
async def test_ws_sender_serializes_distinct_transactions_and_explicit_reject():
    socket = Socket()
    sender = WarmTxSender(connect_factory=lambda: asyncio.sleep(0, result=socket))
    await sender.start()
    first = asyncio.create_task(sender.send(14, "{}", "0x" + "d" * 64,
                                            deadline=time.monotonic() + 1))
    second = asyncio.create_task(sender.send(15, "{}", "0x" + "e" * 64,
                                             deadline=time.monotonic() + 1))
    await wait_sent(socket, 1)
    assert len(socket.sent) == 1
    first_id = socket.sent[0]["data"]["id"]
    await socket.incoming.put(json.dumps({"data": {"id": first_id, "code": 200}}))
    assert (await first)["code"] == 200
    await wait_sent(socket, 2)
    second_id = socket.sent[1]["data"]["id"]
    assert second_id != first_id
    await socket.incoming.put(json.dumps({"data": {"id": second_id, "code": 400}}))
    assert (await second)["code"] == 400
    await sender.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("error,extra,decidable", [
    ({"code": 21602, "message": "PRIVATE_NEVER_RETURN"}, {}, True),
    ({"code": True}, {}, False),
    ({"code": "21602"}, {}, False),
    ({"code": 500}, {}, False),
    ({"code": 200}, {}, False),
    ({"code": 99999}, {}, False),
    ({"code": 21602}, {"code": 200}, False),
    ({"code": 21602}, {"tx_hash": "0x" + "a" * 64}, False),
])
async def test_observed_robinhood_rejection_is_correlated_and_never_replayed(error, extra, decidable):
    socket = Socket()
    sender = WarmTxSender(connect_factory=lambda: asyncio.sleep(0, result=socket))
    await sender.start()
    tx_hash = "0x" + "a" * 64
    pending = asyncio.create_task(sender.send(14, "{}", tx_hash, deadline=time.monotonic() + 1))
    await wait_sent(socket, 1)
    request_id = socket.sent[0]["data"]["id"]
    await socket.incoming.put(json.dumps({"id": "wrong", "error": error, **extra}))
    await asyncio.sleep(0)
    assert not pending.done()
    await socket.incoming.put(json.dumps({"id": request_id, "error": error, **extra}))
    if decidable:
        result = await pending
        assert result["code"] == 21602
        assert "PRIVATE_NEVER_RETURN" not in repr(result)
    else:
        with pytest.raises(RuntimeError, match="undecidable"):
            await pending
    with pytest.raises(RuntimeError, match="already attempted"):
        await sender.send(14, "{}", tx_hash, deadline=time.monotonic() + 1)
    assert len(socket.sent) == 1
    await sender.close()
