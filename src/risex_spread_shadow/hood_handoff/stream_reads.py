"""Validated positive WS observations; absence never proves empty inventory.

L2 depth never proves public owner/queue priority. The explicit ws_confirmed
mode uses exact private observations and L2 vetoes for admission, accepting
unobserved external changes; strict admission retains its REST proofs.
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from dataclasses import replace
import math
import heapq
from typing import Any

from .contracts import OrderSnapshot
from .series import DepthLevel, OrderBookSnapshot


def amount(value: Any, *, positive: bool = False) -> Decimal:
    if not isinstance(value, str) or len(value) > 64:
        raise ValueError("expected decimal text")
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("invalid decimal") from exc
    if not number.is_finite() or number < 0 or (positive and number == 0):
        raise ValueError("invalid amount")
    return number


class StreamReads:
    def __init__(self, market: int, symbol: str, accounts: tuple[int, ...]) -> None:
        self.market, self.symbol, self.accounts = market, symbol, accounts
        self.clear()

    def clear(self) -> None:
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self.book_at: float | None = None
        self.book_wall: float | None = None
        self.invalid_orders: set[tuple[int, int]] = set()
        self.orders: dict[tuple[int, int], tuple[OrderSnapshot, float]] = {}

    def feed(self, frame: dict, projections: list[dict], at: float, wall: float) -> None:
        if not math.isfinite(at) or not math.isfinite(wall):
            self.clear()
            return
        labels = {p.get("kind") for p in projections}
        if frame.get("channel") == f"order_book:{self.market}":
            if labels & {"book_gap", "book_unanchored"} or not labels & {"book_snapshot", "book_update"}:
                self.bids.clear(); self.asks.clear(); self.book_at = None
                return
            try:
                book = frame["order_book"]
                changes = {}
                for side in ("bids", "asks"):
                    raw = book[side]
                    if not isinstance(raw, list):
                        raise ValueError("missing depth")
                    parsed = {}
                    for level in raw:
                        price = amount(level["price"], positive=True)
                        size = amount(level["size"])
                        if price in parsed:
                            raise ValueError("duplicate price")
                        parsed[price] = size
                    changes[side] = parsed
                snapshot = "book_snapshot" in labels
                if not snapshot and self.book_at is None:
                    return  # A valid nonce alone cannot repair invalid depth.
                bids = {} if snapshot else dict(self.bids)
                asks = {} if snapshot else dict(self.asks)
                for side, target in (("bids", bids), ("asks", asks)):
                    for price, size in changes[side].items():
                        if size == 0:
                            target.pop(price, None)
                        else:
                            target[price] = size
                if len(bids) + len(asks) > 50000 or (bids and asks and max(bids) >= min(asks)):
                    raise ValueError("invalid depth bounds")
                self.bids, self.asks, self.book_at, self.book_wall = bids, asks, at, wall
            except (KeyError, TypeError, ValueError):
                self.bids.clear(); self.asks.clear(); self.book_at = None
            return
        channel = frame.get("channel")
        for account in self.accounts:
            if channel != f"account_all_orders:{account}":
                continue
            if frame.get("type") not in {"subscribed/account_all_orders", "update/account_all_orders"}:
                return
            if frame.get("type") == "subscribed/account_all_orders":
                self.orders = {k: v for k, v in self.orders.items() if k[0] != account}
            # Invalidate each affected cache entry BEFORE parsing its new state.
            values = frame.get("orders", {}).get(str(self.market), []) if isinstance(frame.get("orders"), dict) else []
            if not isinstance(values, list):
                self.orders = {k: v for k, v in self.orders.items() if k[0] != account}
                return
            conflicts = {(p.get("account_index"), p.get("client_order_index")) for p in projections if p.get("kind") == "order_conflict"}
            accepted = {(p.get("account_index"), p.get("client_order_index")) for p in projections if p.get("kind") == "order"}
            seen: set[int] = set()
            for item in values:
                if not isinstance(item, dict) or type(item.get("client_order_index")) is not int:
                    self.orders = {k: v for k, v in self.orders.items() if k[0] != account}
                    continue
                client = item["client_order_index"]
                key = (account, client)
                prior = self.orders.pop(key, None)
                if key in self.invalid_orders:
                    continue
                try:
                    if client in seen or (account, client) in conflicts:
                        raise ValueError("conflicting event")
                    seen.add(client)
                    if type(item.get("owner_account_index")) is not int or item["owner_account_index"] != account or type(item.get("market_index")) is not int or item["market_index"] != self.market:
                        raise ValueError("wrong identity")
                    if type(item.get("is_ask")) is not bool or type(item.get("reduce_only")) is not bool:
                        raise ValueError("missing flags")
                    # Require full venue fields; never infer missing quantities or side.
                    selected = {k: item[k] for k in ("owner_account_index", "market_index", "order_id", "client_order_index", "status", "type", "time_in_force", "reduce_only", "initial_base_amount", "remaining_base_amount", "filled_base_amount", "price")}
                    oid = selected["order_id"]
                    if not isinstance(oid, str) or not oid.isascii() or not oid.isdecimal() or len(oid) > 20:
                        raise ValueError("invalid order ID")
                    for name in ("initial_base_amount", "remaining_base_amount", "filled_base_amount", "price"):
                        amount(selected[name], positive=name in {"initial_base_amount", "price"})
                    selected.update(side="SELL" if item["is_ask"] else "BUY", observed_at=wall)
                    parsed = OrderSnapshot.from_mapping(selected)
                    if key not in accepted:
                        if prior and replace(parsed, observed_at=prior[0].observed_at) == prior[0]:
                            self.orders[key] = prior  # Duplicate must not renew observation age.
                            continue
                        raise ValueError("no accepted private event")
                    if parsed.status not in {"open", "filled", "canceled"} and not parsed.terminal:
                        raise ValueError("non-resting event")
                    if parsed.terminal and parsed.remaining_quantity != 0:
                        raise ValueError("terminal remaining")
                    if parsed.status == "filled" and parsed.filled_quantity != parsed.initial_quantity:
                        raise ValueError("incomplete fill")
                    if prior and (prior[0].order_id != parsed.order_id or prior[0].filled_quantity > parsed.filled_quantity or (prior[0].terminal and parsed != prior[0] and (parsed.status != prior[0].status or parsed.filled_quantity != prior[0].filled_quantity))):
                        raise ValueError("regression")
                    if len(self.orders) >= 2048:
                        self.orders.clear()
                        return
                    self.orders[(account, client)] = (parsed, at)
                except (KeyError, TypeError, ValueError):
                    self.invalid_orders.add(key)
                    if len(self.invalid_orders) > 2048:
                        self.clear()
                        return

    def book(self, now: float, max_age: float) -> OrderBookSnapshot | None:
        if self.book_at is None or not 0 <= now - self.book_at <= max_age or not self.bids or not self.asks:
            return None
        return OrderBookSnapshot(self.market, self.symbol,
            tuple(DepthLevel(p, self.bids[p]) for p in heapq.nlargest(250, self.bids)),
            tuple(DepthLevel(p, self.asks[p]) for p in heapq.nsmallest(250, self.asks)), self.book_wall)

    def order(self, account: int, market: int, client: int, order_id: str | None,
              now: float, max_age: float, *, terminal_only: bool) -> OrderSnapshot | None:
        if type(account) is not int or type(client) is not int or type(market) is not int or market != self.market:
            return None
        value = self.orders.get((account, client))
        if value is None:
            return None
        order, at = value
        if not 0 <= now - at <= max_age or (order_id is not None and order.order_id != order_id) or (terminal_only and not order.terminal):
            return None
        return order
