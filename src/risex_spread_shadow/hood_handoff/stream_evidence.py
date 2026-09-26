"""Bounded, owner-only causal projections from the read stream.

The queue never holds a raw frame or an authentication field.  A missing
terminal record means that the file may end at a process/crash boundary.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping


_EVENT_KINDS = frozenset({"book_top", "order", "order_conflict", "order_duplicate", "book_gap", "book_unanchored",
                          "private_orders_snapshot", "transaction", "transaction_subscription"})
_MILESTONES = frozenset({
    "prepare_done", "send_entered", "ack_parsed", "exact_rest_observed", "exact_ws_observed",
    "admission_decision", "receiver_terminal", "cancel_send_entered",
    "cancel_prepare_done", "cancel_decision", "cancel_ack_parsed", "cancel_reconciled",
})


def _safe_projection(item: Mapping[str, object], context: Mapping[str, object] | None) -> dict[str, object] | None:
    kind = item.get("kind")
    epoch, received_at = item.get("epoch"), item.get("received_at")
    if (not isinstance(kind, str) or kind not in _EVENT_KINDS
            or type(epoch) is not int or epoch < 0
            or type(received_at) not in {int, float} or not math.isfinite(received_at)
            or received_at < 0):
        return None
    row: dict[str, object] = {"kind": kind, "connection_epoch": epoch,
                              "receive_monotonic_seconds": float(received_at),
                              "clock_basis": "time.monotonic/process-local", "stream_complete": False}
    if kind in {"order", "order_conflict", "order_duplicate", "private_orders_snapshot", "transaction", "transaction_subscription"}:
        account = item.get("account_index")
        if type(account) is not int or account < 0:
            return None
        row["account_index"] = account
    if kind == "book_top":
        for name in ("bid_price", "bid_quantity", "ask_price", "ask_quantity"):
            value = item.get(name)
            if not isinstance(value, str) or len(value) > 40:
                return None
            try:
                parsed = Decimal(value)
            except InvalidOperation:
                return None
            if not parsed.is_finite() or parsed <= 0:
                return None
            row[name] = value
        index = item.get("trace_index")
        if type(index) is not int or not 1 <= index <= 32:
            return None
        row.update(trace_index=index, trace_limit=32, trace_window_seconds=2.0,
                   identity_or_fifo_proof=False)
    if kind == "transaction":
        tx_hash, status, nonce = item.get("tx_hash"), item.get("transaction_status"), item.get("nonce")
        if (not isinstance(tx_hash, str) or re.fullmatch(r"(?:0x)?[0-9a-fA-F]{8,128}", tx_hash) is None
                or type(status) is not int or not 0 <= status <= 2**63 - 1
                or type(nonce) is not int or not 0 <= nonce <= 2**63 - 1):
            return None
        row.update(tx_hash=tx_hash.lower(), transaction_status=status, nonce=nonce,
                   execution_proof=False, account_scope=True)
    if kind in {"order", "order_conflict", "order_duplicate"}:
        client = item.get("client_order_index")
        if type(client) is not int or client < 0:
            return None
        row["client_order_index"] = client
    if kind == "order":
        market, order_id, status = item.get("market_id"), item.get("order_id"), item.get("status")
        filled, remaining = item.get("filled_base_amount"), item.get("remaining_base_amount")
        if (type(market) is not int or market < 0 or not isinstance(order_id, str)
                or not order_id.isascii() or not order_id.isdecimal() or len(order_id) > 20
                or not isinstance(status, str) or not status.isascii() or len(status) > 48
                or not isinstance(filled, str) or not isinstance(remaining, str)
                or len(filled) > 40 or len(remaining) > 40):
            return None
        try:
            amounts = (Decimal(filled), Decimal(remaining))
        except InvalidOperation:
            return None
        if any(not amount.is_finite() or amount < 0 for amount in amounts):
            return None
        row.update(market_id=market, order_id=order_id, status=status,
                   filled_base_amount=filled, remaining_base_amount=remaining,
                   terminal=item.get("terminal") is True)
    if kind in {"order", "transaction"}:
        fields = (("timestamp", "created_at", "updated_at", "transaction_time", "block_height")
                  if kind == "order" else
                  ("queued_at", "executed_at", "transaction_time", "sequence_index", "block_height"))
        for name in fields:
            value = item.get(f"venue_{name}_raw")
            if type(value) is int and 0 <= value <= 2**63 - 1:
                row[f"venue_{name}_raw"] = value
        row["venue_clock_basis"] = "raw/units-and-clock-offset-unverified"
    if context is not None:
        # The engine owns these fields.  Do not copy arbitrary context keys.
        for name in ("run_id", "phase", "role"):
            value = context.get(name)
            if isinstance(value, str) and value.isascii() and len(value) <= 64:
                row[name] = value
        attempt = context.get("attempt_index")
        if type(attempt) is int and 0 <= attempt <= 100000:
            row["attempt_index"] = attempt
    return row


class StreamEvidenceJournal:
    """Bounded diagnostics: nonblocking producer, batched writer, fsync at close.

    These observations never authorize mutations. Without a durable session_end
    the file is incomplete; order intents live in DurableJournal separately.
    """

    def __init__(self, path: Path, *, market_id: int, accounts: tuple[int, ...],
                 max_records: int = 512, queue_size: int = 64) -> None:
        # Two accounts for 1 -> 1; up to 17 for a fan-out cycle (source + 16 receivers).
        if (type(market_id) is not int or market_id <= 0
                or type(accounts) is not tuple or not 2 <= len(accounts) <= 17
                or len(set(accounts)) != len(accounts)
                or any(type(account) is not int or account <= 0 for account in accounts)
                or type(max_records) is not int or not 1 <= max_records <= 512
                or type(queue_size) is not int or not 1 <= queue_size <= 64):
            raise ValueError("stream evidence identity or bounds are invalid")
        self.path = Path(path)
        self.market_id = market_id
        self.accounts = accounts
        self.max_records = max_records
        self._queue: asyncio.Queue[dict[str, object] | None] = asyncio.Queue(queue_size)
        self._fd: int | None = None
        self._writer: asyncio.Task[None] | None = None
        self.accepted = 0
        self.dropped = 0
        self.failed = False
        self.closed = False

    @staticmethod
    def _write(fd: int, row: Mapping[str, object]) -> None:
        blob = (json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        offset = 0
        while offset < len(blob):
            count = os.write(fd, blob[offset:])
            if count <= 0:
                raise OSError("stream evidence write did not progress")
            offset += count

    async def _write_rows(self, rows, *, durable=False) -> None:
        fd = self._fd
        assert fd is not None

        def write():
            for row in rows:
                self._write(fd, row)
            if durable:
                os.fsync(fd)

        task = asyncio.create_task(asyncio.to_thread(write))
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            # Drain uncancellable disk IO before close can reuse the descriptor.
            try:
                await task
            finally:
                raise

    async def start(self) -> None:
        if self._fd is not None or self._writer is not None or self.closed:
            raise RuntimeError("stream evidence already started")
        parent = self.path.parent
        info = parent.stat()
        if info.st_uid != os.geteuid() or info.st_mode & 0o077 or self.path.is_symlink():
            raise RuntimeError("stream evidence requires an owner-only directory")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        self._fd = os.open(self.path, flags, 0o600)
        try:
            await self._write_rows([{
                "kind": "session_start", "schema": "hood-stream-evidence-v1",
                "market_id": self.market_id, "accounts": list(self.accounts),
                "clock_basis": "time.monotonic/process-local",
                "missing_terminal_means_incomplete": True,
            }], durable=True)
        except BaseException:
            os.close(self._fd)
            self._fd = None
            raise
        self._writer = asyncio.create_task(self._drain())

    def offer(self, item: Mapping[str, object], context: Mapping[str, object] | None = None) -> None:
        kind = item.get("kind")
        if not isinstance(kind, str) or kind not in _EVENT_KINDS:
            return
        if self.closed or self.failed or self._writer is None:
            self.dropped += 1
            return
        row = _safe_projection(item, context)
        if row is None:
            self.dropped += 1
            return
        if ("account_index" in row and row["account_index"] not in self.accounts
                or "market_id" in row and row["market_id"] != self.market_id):
            self.dropped += 1
            return
        if kind not in {"transaction", "transaction_subscription"}:
            row.setdefault("market_id", self.market_id)
        self._offer_row(row)

    def offer_milestone(self, milestone: str, *, account: int, client: int,
                        order_id: str | None, at: float,
                        context: Mapping[str, object] | None = None) -> None:
        if (milestone not in _MILESTONES or type(account) is not int
                or account not in self.accounts or type(client) is not int or client < 0
                or type(at) not in {int, float} or not math.isfinite(at) or at < 0
                or (order_id is not None and (not isinstance(order_id, str)
                                              or not order_id.isascii()
                                              or not order_id.isdecimal() or len(order_id) > 20))):
            self.dropped += 1
            return
        row: dict[str, object] = {
            "kind": "local_milestone", "milestone": milestone,
            "account_index": account, "market_id": self.market_id,
            "client_order_index": client,
            "receive_monotonic_seconds": float(at),
            "clock_basis": "time.monotonic/process-local",
        }
        if order_id is not None:
            row["order_id"] = order_id
        if context is not None:
            for name in ("run_id", "phase", "role"):
                value = context.get(name)
                if isinstance(value, str) and value.isascii() and len(value) <= 64:
                    row[name] = value
            attempt = context.get("attempt_index")
            if type(attempt) is int and 0 <= attempt <= 100000:
                row["attempt_index"] = attempt
        self._offer_row(row)

    def _offer_row(self, row: dict[str, object]) -> None:
        if self.closed or self.failed or self._writer is None:
            self.dropped += 1
            return
        if self.accepted >= self.max_records or self._queue.full():
            self.dropped += 1
            return
        self._queue.put_nowait(row)
        self.accepted += 1

    async def _drain(self) -> None:
        assert self._fd is not None
        try:
            while True:
                row = await self._queue.get()
                if row is None:
                    self._queue.task_done()
                    break
                batch = [row]
                ended = False
                while len(batch) < 64 and not self._queue.empty():
                    item = self._queue.get_nowait()
                    if item is None:
                        self._queue.task_done()
                        ended = True
                        break
                    batch.append(item)
                await self._write_rows(batch)
                for _ in batch:
                    self._queue.task_done()
                if ended:
                    break
        except BaseException:
            self.failed = True
            raise

    async def close(self, *, reason: str) -> dict[str, object]:
        if self.closed:
            return self.summary()
        self.closed = True
        writer, fd = self._writer, self._fd
        if writer is not None and fd is not None:
            try:
                if not writer.done():
                    await asyncio.wait_for(self._queue.put(None), timeout=3)
                await asyncio.wait_for(writer, timeout=5)
                if not self.failed:
                    safe_reason = reason if reason in {"stopped", "duration_limit", "observer_stopped",
                                                        "transport_error", "task_cancelled",
                                                        "stopped_before_connect"} else "other"
                    await self._write_rows([{
                        "kind": "session_end", "reason": safe_reason,
                        "accepted": self.accepted, "dropped": self.dropped,
                        "complete": self.dropped == 0 and safe_reason == "stopped",
                    }], durable=True)
            except BaseException:
                self.failed = True
                raise
            finally:
                if not writer.done():
                    writer.cancel()
                    await asyncio.gather(writer, return_exceptions=True)
                os.close(fd)
                self._fd = None
                self._writer = None
        return self.summary()

    def summary(self) -> dict[str, object]:
        return {"path": str(self.path), "accepted": self.accepted,
                "dropped": self.dropped, "write_failed": self.failed,
                "closed": self.closed}
