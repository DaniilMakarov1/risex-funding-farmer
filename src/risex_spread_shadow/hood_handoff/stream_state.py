"""Ephemeral, read-only stream state for one paired-operation lifetime.

The state is a wake-up hint, never an execution or public-admission proof.
No raw frame, authentication token, or signed transaction is retained.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import json
import math
import time

from .stream_reads import StreamReads
from .offline_observer import ObserverLimits
from .stream_evidence import StreamEvidenceJournal
from .stream_measurement import StreamIdentity, StreamProjection, WS_URL, _auth_tokens


@dataclass(frozen=True, slots=True)
class StreamReadLimits:
    lifetime_seconds: float = 570.0
    max_frames: int = 15000
    max_bytes: int = 20 * 1024 * 1024
    max_frame_bytes: int = 8 * 1024 * 1024
    max_fresh_age_seconds: float = 2.0

    def __post_init__(self) -> None:
        if (isinstance(self.lifetime_seconds, bool)
                or not isinstance(self.lifetime_seconds, (int, float))
                or not math.isfinite(self.lifetime_seconds)
                or not 0 < self.lifetime_seconds <= 570
                or any(type(value) is not int or value <= 0 for value in
                       (self.max_frames, self.max_bytes, self.max_frame_bytes))
                or self.max_frames > 15000 or self.max_bytes > 20 * 1024 * 1024
                or self.max_frame_bytes > 8 * 1024 * 1024
                or isinstance(self.max_fresh_age_seconds, bool)
                or not isinstance(self.max_fresh_age_seconds, (int, float))
                or not math.isfinite(self.max_fresh_age_seconds)
                or not 0 < self.max_fresh_age_seconds <= 2):
            raise ValueError("read stream limits are outside the bounded lifecycle")


@dataclass(slots=True)
class ReadStreamState:
    identity: StreamIdentity
    limits: StreamReadLimits = field(default_factory=StreamReadLimits)
    observer: StreamProjection = field(init=False)
    reads: StreamReads = field(init=False)
    connected: bool = False
    book_received_at: float | None = None
    private_snapshot_at: dict[int, float] = field(default_factory=dict)
    exact_orders: dict[tuple[int, int], dict[str, object]] = field(default_factory=dict)
    order_versions: dict[tuple[int, int], int] = field(default_factory=dict)
    consumed_hint_versions: dict[tuple[int, int], int] = field(default_factory=dict)
    evidence: StreamEvidenceJournal | None = None
    order_contexts: dict[tuple[int, int], dict[str, object]] = field(default_factory=dict)
    _condition: asyncio.Condition = field(default_factory=asyncio.Condition, repr=False)

    def __post_init__(self) -> None:
        self.reads = StreamReads(self.identity.market_id, self.identity.market_symbol, self.identity.accounts)
        self.observer = StreamProjection(
            self.identity,
            ObserverLimits(max_seconds=self.limits.lifetime_seconds,
                           max_frames=self.limits.max_frames,
                           max_bytes=self.limits.max_bytes,
                           max_frame_bytes=self.limits.max_frame_bytes),
        )

    async def connected_now(self) -> None:
        if self.connected:
            raise RuntimeError("read stream connection already active")
        self.connected = True
        async with self._condition:
            self._condition.notify_all()

    async def disconnected(self) -> None:
        """Invalidate all channel and exact-order hints before any reconnect."""
        self.connected = False
        self.reads.clear()
        self.observer.reconnect()
        self.book_received_at = None
        self.private_snapshot_at.clear()
        self.exact_orders.clear()
        self.order_versions.clear()
        self.consumed_hint_versions.clear()
        async with self._condition:
            self._condition.notify_all()

    def bind_order(self, account: int, market: int, client: int,
                   *, run_id: str, phase: str, role: str,
                   attempt_index: int | None) -> None:
        """Associate a planned exact order before its mutation can be sent."""
        if (type(account) is not int or account not in self.identity.accounts
                or type(market) is not int or market != self.identity.market_id
                or type(client) is not int or client < 0
                or not isinstance(run_id, str) or not run_id.isascii() or len(run_id) > 64
                or phase not in {"PAIRED_OPENING", "PAIRED_CLOSING"}
                or role not in {"source", "receiver"}
                or (attempt_index is not None and (type(attempt_index) is not int
                                                   or not 0 <= attempt_index <= 100000))):
            raise ValueError("read-stream order context is invalid")
        self.order_contexts[(account, client)] = {
            "run_id": run_id, "phase": phase, "role": role,
            **({"attempt_index": attempt_index} if attempt_index is not None else {}),
        }

    async def feed(self, raw: bytes, received_at: float) -> list[dict[str, object]]:
        if not self.connected:
            raise RuntimeError("read stream is not connected")
        projections = self.observer.feed(raw, received_at)
        try:
            frame = json.loads(raw)
            if isinstance(frame, dict):
                self.reads.feed(frame, projections, received_at, time.time() - (time.monotonic() - received_at))
            else:
                self.reads.clear()
        except (ValueError, TypeError, RecursionError):
            self.reads.clear()
        if self.observer.stopped_reason is not None:
            await self.disconnected()
            return []
        for item in projections:
            kind = item.get("kind")
            if kind in {"book_snapshot", "book_update"}:
                self.book_received_at = received_at
            elif kind in {"book_gap", "book_unanchored"}:
                self.book_received_at = None
            elif kind == "private_orders_snapshot":
                account = item["account_index"]
                if type(account) is int:
                    self.private_snapshot_at[account] = received_at
            elif kind == "order":
                account, client = item["account_index"], item["client_order_index"]
                if type(account) is int and type(client) is int:
                    self.exact_orders[(account, client)] = item
                    self.order_versions[(account, client)] = self.order_versions.get((account, client), 0) + 1
            elif kind == "order_conflict":
                account, client = item["account_index"], item["client_order_index"]
                if type(account) is int and type(client) is int:
                    self.exact_orders.pop((account, client), None)
                    self.order_versions.pop((account, client), None)
                    self.consumed_hint_versions.pop((account, client), None)
            if self.evidence is not None:
                account, client = item.get("account_index"), item.get("client_order_index")
                context = self.order_contexts.get((account, client)) if type(account) is int and type(client) is int else None
                self.evidence.offer(item, context)
        if self.evidence is not None:
            for account, client in self.observer.last_duplicate_keys:
                self.evidence.offer({"kind": "order_duplicate", "epoch": self.observer.epoch,
                                     "received_at": received_at, "account_index": account,
                                     "client_order_index": client},
                                    self.order_contexts.get((account, client)))
        async with self._condition:
            self._condition.notify_all()
        return projections

    def subscription_ready(self, now: float) -> bool:
        """Snapshot/fresh-book health only; never an order-admission decision."""
        return bool(
            self.connected
            and self.observer.stopped_reason is None
            and self.observer.book_snapshot
            and self.book_received_at is not None
            and type(now) in {int, float} and math.isfinite(now)
            and 0 <= now - self.book_received_at <= self.limits.max_fresh_age_seconds
            and self.observer.private_snapshot_accounts == set(self.identity.accounts)
            and (self.evidence is None or not self.evidence.failed)
        )

    async def wait_subscription_ready(self, timeout: float) -> bool:
        if (isinstance(timeout, bool) or not isinstance(timeout, (int, float))
                or not math.isfinite(timeout) or timeout <= 0):
            return False
        deadline = time.monotonic() + timeout
        async with self._condition:
            while True:
                if self.subscription_ready(time.monotonic()):
                    return True
                if self.observer.stopped_reason is not None:
                    return False
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                try:
                    await asyncio.wait_for(self._condition.wait(), remaining)
                except asyncio.TimeoutError:
                    return False

    async def wait_order_observation(self, account: int, market: int, client: int,
                                     order_id: str | None, timeout: float, *, terminal_only: bool):
        if type(timeout) not in {int, float} or not math.isfinite(timeout) or timeout <= 0:
            return None
        deadline = time.monotonic() + timeout
        async with self._condition:
            while self.connected and self.observer.stopped_reason is None:
                now = time.monotonic()
                order = self.reads.order(account, market, client, order_id, now,
                                         self.limits.max_fresh_age_seconds, terminal_only=terminal_only)
                if order is not None:
                    return order
                remaining = deadline - now
                if remaining <= 0:
                    break
                try:
                    await asyncio.wait_for(self._condition.wait(), remaining)
                except asyncio.TimeoutError:
                    break
        return None

    def exact_terminal_hint(self, account: int, client: int, order_id: str | None,
                            now: float) -> bool:
        """A recent exact private event can wake REST polling, not replace it."""
        if (not self.connected or type(account) is not int or type(client) is not int
                or account not in self.identity.accounts or type(now) not in {int, float}
                or not math.isfinite(now)):
            return False
        item = self.exact_orders.get((account, client))
        if (item is None or item.get("terminal") is not True
                or item.get("epoch") != self.observer.epoch
                or (order_id is not None and item.get("order_id") != order_id)):
            return False
        received_at = item.get("received_at")
        return bool(type(received_at) in {int, float}
                    and 0 <= now - received_at <= self.limits.max_fresh_age_seconds)

    async def wait_terminal_hint(self, account: int, client: int,
                                 order_id: str | None, timeout: float) -> bool:
        """Wait only up to the caller's current poll interval."""
        if (isinstance(timeout, bool) or not isinstance(timeout, (int, float))
                or not math.isfinite(timeout) or timeout <= 0):
            return False
        deadline = time.monotonic() + timeout
        key = (account, client)
        async with self._condition:
            while True:
                version = self.order_versions.get(key, 0)
                if (version > self.consumed_hint_versions.get(key, 0)
                        and self.exact_terminal_hint(account, client, order_id, time.monotonic())):
                    self.consumed_hint_versions[key] = version
                    return True
                if not self.connected or self.observer.stopped_reason is not None:
                    return False
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                try:
                    await asyncio.wait_for(self._condition.wait(), remaining)
                except asyncio.TimeoutError:
                    return False


@dataclass(slots=True)
class ReadStreamSession:
    """One heartbeat-maintained, read-only socket for a bounded cycle lifetime."""

    state: ReadStreamState

    async def run(self, provider: object, stop: asyncio.Event) -> str:
        from websockets.asyncio.client import connect

        if stop.is_set():
            return "stopped_before_connect"
        tokens: dict[int, str] = {}
        result = "duration_limit"
        cancelled = False
        try:
            if self.state.evidence is not None:
                await self.state.evidence.start()
            stop_at = time.monotonic() + self.state.limits.lifetime_seconds
            tokens = await asyncio.wait_for(_auth_tokens(self.state.identity, provider), timeout=10)
            if stop.is_set() or time.monotonic() >= stop_at:
                return "stopped_before_connect"
            async with connect(WS_URL, max_size=self.state.limits.max_frame_bytes,
                               max_queue=1, ping_interval=30, open_timeout=10,
                               close_timeout=3) as socket:
                await self.state.connected_now()
                channels = [f"order_book/{self.state.identity.market_id}"]
                channels.extend(f"account_all_orders/{account}" for account in self.state.identity.accounts)
                # Passive timing evidence; never a readiness/admission gate.
                channels.extend(f"account_tx/{account}" for account in self.state.identity.accounts)
                for channel in channels:
                    value = {"type": "subscribe", "channel": channel}
                    if channel.startswith(("account_all_orders/", "account_tx/")):
                        value["auth"] = tokens[int(channel.rsplit("/", 1)[1])]
                    await socket.send(json.dumps(value, separators=(",", ":")))
                    value.clear()
                tokens.clear()
                while not stop.is_set():
                    remaining = stop_at - time.monotonic()
                    if remaining <= 0:
                        break
                    try:
                        incoming = await asyncio.wait_for(socket.recv(), min(remaining, 1.0))
                    except asyncio.TimeoutError:
                        continue
                    raw = incoming.encode("utf-8") if isinstance(incoming, str) else incoming
                    received_at = time.monotonic()
                    await self.state.feed(raw, received_at)
                    try:
                        control = json.loads(raw)
                        if isinstance(control, dict) and control.get("type") == "ping":
                            await socket.send('{"type":"pong"}')
                    except (ValueError, TypeError):
                        pass
                    if self.state.observer.stopped_reason is not None:
                        result = "observer_stopped"
                        break
                else:
                    result = "stopped"
        except asyncio.CancelledError:
            result = "task_cancelled"
            cancelled = True
        except Exception:
            # Never surface exception text: WebSocket errors may contain auth.
            result = "transport_error"
        finally:
            tokens.clear()
            if self.state.connected:
                await self.state.disconnected()
            if self.state.evidence is not None:
                try:
                    await self.state.evidence.close(reason=result)
                except Exception:
                    result = "evidence_flush_error"
        if cancelled:
            raise asyncio.CancelledError
        return result
