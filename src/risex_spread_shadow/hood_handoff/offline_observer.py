"""Bounded, offline-only observation of sanitized Lighter-shaped stream fixtures.

This module has no socket, credential, or trading adapter. It records local
receipt milestones, never source frames, and never authorizes a mutation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import math
from typing import Mapping


TERMINAL = frozenset({
    "filled", "canceled", "canceled-post-only", "canceled-reduce-only",
    "canceled-position-not-allowed", "canceled-margin-not-allowed",
    "canceled-too-much-slippage", "canceled-not-enough-liquidity",
    "canceled-self-trade", "canceled-expired", "canceled-oco",
    "canceled-child", "canceled-liquidation", "canceled-invalid-balance",
})


@dataclass(frozen=True, slots=True)
class ObserverLimits:
    max_seconds: float = 600.0
    max_frames: int = 15000
    max_bytes: int = 20 * 1024 * 1024
    max_frame_bytes: int = 65536

    def __post_init__(self) -> None:
        if (isinstance(self.max_seconds, bool)
                or not isinstance(self.max_seconds, (int, float))
                or not math.isfinite(self.max_seconds) or self.max_seconds <= 0
                or any(type(value) is not int or value <= 0 for value in
                       (self.max_frames, self.max_bytes, self.max_frame_bytes))):
            raise ValueError("observer limits must be finite and positive")


@dataclass(slots=True)
class OfflineObserver:
    market_id: int
    target_client_orders: Mapping[int, int]
    limits: ObserverLimits = field(default_factory=ObserverLimits)
    start_at: float | None = None
    last_received_at: float | None = None
    epoch: int | None = None
    book_nonce: int | None = None
    book_ready: bool = False
    private_snapshot_proved: bool = False
    stopped_reason: str | None = None
    frames: int = 0
    bytes_seen: int = 0
    malformed: int = 0
    gaps: int = 0
    private_duplicates: int = 0
    private_conflicts: int = 0
    private_last: dict[tuple[int, int], tuple[str, str]] = field(default_factory=dict)
    observations: list[dict[str, object]] = field(default_factory=list)

    def __post_init__(self) -> None:
        if type(self.market_id) is not int or self.market_id < 0:
            raise ValueError("market identity is invalid")
        if not self.target_client_orders or any(
            type(account) is not int or account < 0 or type(index) is not int or index < 0
            for account, index in self.target_client_orders.items()
        ):
            raise ValueError("exact account and client order identities are required")

    def reconnect(self, epoch: int) -> None:
        """Invalidate both channels; a new connection needs new snapshots."""
        if type(epoch) is not int or epoch < 0 or (self.epoch is not None and epoch <= self.epoch):
            raise ValueError("connection epoch must increase")
        self.epoch = epoch
        self.book_nonce = None
        self.book_ready = False
        self.private_snapshot_proved = False
        self.private_last.clear()

    def feed(self, raw: bytes, *, received_at: float, epoch: int) -> dict[str, object] | None:
        """Consume one bounded fixture frame; return only a redacted projection."""
        if self.stopped_reason is not None:
            return None
        if (type(raw) is not bytes or isinstance(received_at, bool)
                or not isinstance(received_at, (int, float))
                or not math.isfinite(received_at) or received_at < 0):
            self.stopped_reason = "invalid_input"
            return None
        if type(epoch) is not int or epoch < 0 or self.epoch != epoch:
            self.stopped_reason = "connection_epoch_mismatch"
            return None
        if self.last_received_at is not None and received_at < self.last_received_at:
            self.stopped_reason = "receipt_time_regression"
            return None
        if self.start_at is None:
            self.start_at = received_at
        if received_at < self.start_at or received_at - self.start_at > self.limits.max_seconds:
            self.stopped_reason = "duration_limit"
            return None
        if len(raw) > self.limits.max_frame_bytes or self.bytes_seen + len(raw) > self.limits.max_bytes:
            self.stopped_reason = "byte_limit"
            return None
        if self.frames >= self.limits.max_frames:
            self.stopped_reason = "frame_limit"
            return None
        self.frames += 1
        self.bytes_seen += len(raw)
        self.last_received_at = received_at
        try:
            frame = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError):
            self.malformed += 1
            return None
        if not isinstance(frame, dict):
            self.malformed += 1
            return None
        channel = frame.get("channel")
        kind = frame.get("type")
        if not isinstance(channel, str) or not isinstance(kind, str):
            self.malformed += 1
            return None
        if channel == f"order_book:{self.market_id}":
            return self._book(frame, kind, received_at)
        for account, target in self.target_client_orders.items():
            if channel == f"account_all_orders:{account}":
                return self._private_order(frame, kind, account, target, received_at)
        return None

    def _record(self, value: dict[str, object]) -> dict[str, object]:
        # At most one output per accepted input frame, itself bounded by max_frames.
        self.observations.append(value)
        return value

    def _book(self, frame: dict, kind: str, received_at: float) -> dict[str, object] | None:
        book = frame.get("order_book")
        if not isinstance(book, dict):
            self.malformed += 1
            self.book_ready = False
            return None
        nonce = book.get("nonce")
        if type(nonce) is not int or nonce < 0:
            self.malformed += 1
            self.book_ready = False
            return None
        if kind == "subscribed/order_book":
            self.book_nonce = nonce
            self.book_ready = True
            return self._record({"kind": "book_snapshot", "epoch": self.epoch,
                                 "nonce": nonce, "received_at": received_at})
        if kind != "update/order_book":
            return None
        begin = book.get("begin_nonce")
        if type(begin) is not int or not self.book_ready or begin != self.book_nonce or nonce <= begin:
            self.book_ready = False
            self.book_nonce = None
            self.gaps += 1
            return self._record({"kind": "book_gap", "epoch": self.epoch,
                                 "received_at": received_at})
        self.book_nonce = nonce
        return self._record({"kind": "book_update", "epoch": self.epoch,
                             "nonce": nonce, "received_at": received_at})

    def _private_order(self, frame: dict, kind: str, account: int, target: int,
                       received_at: float) -> dict[str, object] | None:
        # The general docs name update/account_all_orders but do not establish
        # a private sequence or snapshot barrier. Keep completeness unproved.
        if kind not in {"subscribed/account_all_orders", "update/account_all_orders"}:
            return None
        orders = frame.get("orders")
        if not isinstance(orders, dict):
            self.malformed += 1
            return None
        market_orders = orders.get(str(self.market_id))
        if not isinstance(market_orders, list):
            return None
        matched = []
        for order in market_orders:
            if not isinstance(order, dict):
                continue
            owner = order.get("owner_account_index")
            market = order.get("market_index")
            client_index = order.get("client_order_index")
            if (type(owner) is not int or owner != account
                    or type(market) is not int or market != self.market_id
                    or type(client_index) is not int or client_index != target):
                continue
            order_id, status = order.get("order_id"), order.get("status")
            if (not isinstance(order_id, str) or not order_id.isascii()
                    or not order_id.isdecimal() or len(order_id) > 20
                    or not isinstance(status, str)
                    or status not in TERMINAL | {"open", "pending", "in-progress"}):
                self.malformed += 1
                continue
            matched.append((order_id, status))
        if len(matched) != 1:
            if len(matched) > 1:
                self.malformed += 1
            return None
        order_id, status = matched[0]
        prior = self.private_last.get((account, target))
        if prior == (order_id, status):
            self.private_duplicates += 1
            return None
        if prior is not None and (prior[0] != order_id or (prior[1] in TERMINAL and status != prior[1])):
            self.private_conflicts += 1
            self.private_snapshot_proved = False
            return self._record({"kind": "private_conflict", "epoch": self.epoch,
                                 "account_index": account, "client_order_index": target,
                                 "received_at": received_at})
        self.private_last[(account, target)] = (order_id, status)
        return self._record({"kind": "exact_private_order", "epoch": self.epoch,
                             "account_index": account, "market_id": self.market_id,
                             "client_order_index": target, "order_id": order_id,
                             "status": status, "terminal": status in TERMINAL,
                             "received_at": received_at,
                             "stream_complete": False})

    def summary(self) -> dict[str, object]:
        return {"schema": "hcr43-offline-observer-v1", "market_id": self.market_id,
                "epoch": self.epoch, "frames": self.frames, "bytes": self.bytes_seen,
                "start_at": self.start_at, "last_received_at": self.last_received_at,
                "malformed": self.malformed, "gaps": self.gaps,
                "private_duplicates": self.private_duplicates,
                "private_conflicts": self.private_conflicts,
                "book_ready": self.book_ready,
                "private_snapshot_proved": self.private_snapshot_proved,
                "stopped_reason": self.stopped_reason,
                "observations": list(self.observations)}
