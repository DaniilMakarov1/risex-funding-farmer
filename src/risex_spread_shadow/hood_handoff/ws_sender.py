"""Inactive-by-default Robinhood WS sendTx transport for prepared mutations.

Only an exact echoed request ID with a decidable application code is an ACK.
It is never an order/fill proof, and an uncertain send is never replayed.
"""

from __future__ import annotations

import asyncio
import json
import math
import re
import time
import uuid
from typing import Any, Callable


ROBINHOOD_TX_WS_URL = "wss://api.rh.lighter.xyz/stream"
_TX_HASH = re.compile(r"(?:0x)?[0-9a-fA-F]{8,128}\Z")


class WarmTxSender:
    def __init__(self, *, connect_factory: Callable[[], Any] | None = None) -> None:
        self._connect_factory = connect_factory
        self._socket: Any | None = None
        self._reader: asyncio.Task[None] | None = None
        self._send_lock = asyncio.Lock()
        self._pending_id: str | None = None
        self._pending_hash: str | None = None
        self._pending: asyncio.Future[dict[str, Any]] | None = None
        self._started_at: float | None = None
        self._closed = False
        self._sent_hashes: set[str] = set()

    async def start(self) -> None:
        if self._closed or self._socket is not None:
            raise RuntimeError("WS sender is already used")
        if self._connect_factory is None:
            from websockets.asyncio.client import connect
            self._socket = await connect(ROBINHOOD_TX_WS_URL, max_size=1024 * 1024,
                                         max_queue=1, ping_interval=30,
                                         open_timeout=10, close_timeout=3)
        else:
            self._socket = await self._connect_factory()
        self._started_at = time.monotonic()
        self._reader = asyncio.create_task(self._read())

    async def _read(self) -> None:
        try:
            while not self._closed:
                raw = await self._socket.recv()
                if not isinstance(raw, (str, bytes)) or len(raw) > 1024 * 1024:
                    raise RuntimeError("WS sender received an invalid frame")
                try:
                    frame = json.loads(raw)
                except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
                    continue
                if not isinstance(frame, dict):
                    continue
                kind = frame.get("type")
                if kind == "ping":
                    await self._socket.send('{"type":"pong"}')
                    continue
                if kind in {"connected", "pong"}:
                    continue
                pending = self._pending
                if pending is None or pending.done():
                    continue
                data = frame.get("data")
                response = data if isinstance(data, dict) else frame
                if response.get("id") != self._pending_id:
                    continue
                # Robinhood's observed unsigned rejection is top-level
                # {id, error: {code: 21602, message: ...}}. Only this verified
                # validation code is decidable; unknown/server errors remain
                # ambiguous and never authorize replay. Never copy messages.
                if "error" in response:
                    error = response["error"]
                    if (isinstance(error, dict) and type(error.get("code")) is int
                            and error["code"] == 21602 and "code" not in response
                            and "tx_hash" not in response):
                        pending.set_result({"code": 21602, "tx_hash": self._pending_hash})
                    else:
                        pending.set_exception(RuntimeError("WS transaction response is undecidable"))
                    continue
                code = response.get("code")
                if type(code) is not int or code < 100 or code >= 500:
                    pending.set_exception(RuntimeError("WS transaction response is undecidable"))
                    continue
                tx_hash = response.get("tx_hash")
                if tx_hash is not None and (
                    not isinstance(tx_hash, str) or tx_hash.lower() != self._pending_hash
                ):
                    pending.set_exception(RuntimeError("WS transaction hash conflicts"))
                    continue
                pending.set_result({"code": code, "tx_hash": self._pending_hash})
        except asyncio.CancelledError:
            raise
        except Exception:
            pending = self._pending
            if pending is not None and not pending.done():
                pending.set_exception(ConnectionError("WS sender connection ended before exact ACK"))

    async def send(self, tx_type: int, tx_info: str, tx_hash: str,
                   *, deadline: float) -> dict[str, Any]:
        if (type(tx_type) is not int or not isinstance(tx_info, str) or not tx_info
                or not isinstance(tx_hash, str) or not _TX_HASH.fullmatch(tx_hash)
                or type(deadline) not in {int, float} or not math.isfinite(deadline)):
            raise ValueError("WS prepared transaction identity is invalid")
        if self._socket is None or self._reader is None or self._reader.done() or self._closed:
            raise ConnectionError("WS sender is not ready")
        if self._started_at is None or time.monotonic() - self._started_at > 570:
            raise TimeoutError("WS sender lifetime expired")
        try:
            signed = json.loads(tx_info)
        except (json.JSONDecodeError, ValueError):
            raise ValueError("signed transaction is not JSON") from None
        async with self._send_lock:
            if time.monotonic() >= deadline:
                raise TimeoutError("WS send deadline expired")
            if tx_hash.lower() in self._sent_hashes:
                raise RuntimeError("signed WS transaction was already attempted")
            # Mark before entering socket.send: a failed or canceled write can
            # have reached the peer and must never be replayed by this sender.
            self._sent_hashes.add(tx_hash.lower())
            request_id = uuid.uuid4().hex
            future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
            self._pending_id, self._pending_hash, self._pending = (
                request_id, tx_hash.lower(), future,
            )
            body = json.dumps({"type": "jsonapi/sendtx", "data": {
                "id": request_id, "tx_type": tx_type, "tx_info": signed,
            }}, separators=(",", ":"))
            started = time.perf_counter()
            try:
                await asyncio.wait_for(self._socket.send(body), timeout=max(0, deadline - time.monotonic()))
                written = time.perf_counter()
                response = await asyncio.wait_for(future, timeout=max(0, deadline - time.monotonic()))
                response["_transport_timing"] = {
                    "ws_write_seconds": written - started,
                    "ws_ack_wait_seconds": time.perf_counter() - written,
                    "ws_total_seconds": time.perf_counter() - started,
                }
                return response
            finally:
                if not future.done():
                    future.cancel()
                self._pending_id = self._pending_hash = self._pending = None

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._pending is not None and not self._pending.done():
            self._pending.set_exception(ConnectionError("WS sender closed before exact ACK"))
        if self._reader is not None:
            self._reader.cancel()
            await asyncio.gather(self._reader, return_exceptions=True)
        if self._socket is not None:
            await self._socket.close()
        self._socket = None
