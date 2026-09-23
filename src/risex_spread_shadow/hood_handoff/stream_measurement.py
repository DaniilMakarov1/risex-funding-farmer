"""One bounded Robinhood read-only stream session; no trading adapter."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
import json
import os
from pathlib import Path
import time
from typing import Any

from .contracts import OFFICIAL_ROBINHOOD_API_URL, OFFICIAL_ROBINHOOD_CHAIN_ID
from .keychain import KeychainSecretProvider
from .offline_observer import ObserverLimits, TERMINAL


WS_URL = "wss://api.rh.lighter.xyz/stream?readonly=true"
CONFIG_FIELDS = frozenset({"market_id", "market_symbol", "source_account_index",
                           "receiver_account_index", "api_key_index", "environment",
                           "api_base_url", "chain_id"})
ORDER_STATUSES = TERMINAL | {"pending", "in-progress", "open"}


def _integer(value: object) -> bool:
    return type(value) is int and value >= 0


def _amount(value: object) -> str | None:
    if not isinstance(value, str) or len(value) > 32:
        return None
    try:
        amount = Decimal(value)
    except InvalidOperation:
        return None
    if not amount.is_finite() or amount < 0:
        return None
    return value


@dataclass(frozen=True, slots=True)
class StreamIdentity:
    market_id: int
    accounts: tuple[int, int]
    api_key_index: int
    api_base_url: str
    chain_id: int
    environment: str
    market_symbol: str

    @classmethod
    def from_config(cls, value: dict[str, Any]) -> "StreamIdentity":
        if not CONFIG_FIELDS <= value.keys():
            raise ValueError("configured identity is incomplete")
        market, source, receiver, key = (value[name] for name in
                                         ("market_id", "source_account_index",
                                          "receiver_account_index", "api_key_index"))
        if (not all(_integer(x) for x in (market, source, receiver, key))
                or source == receiver or key < 4 or key > 254
                or market != 1 or value["market_symbol"] != "BTC"
                or value["api_base_url"].rstrip("/") != OFFICIAL_ROBINHOOD_API_URL
                or value["chain_id"] != OFFICIAL_ROBINHOOD_CHAIN_ID
                or value["environment"] != "robinhood"):
            raise ValueError("configuration is outside the Robinhood BTC read-only gate")
        return cls(market, (source, receiver), key, OFFICIAL_ROBINHOOD_API_URL,
                   OFFICIAL_ROBINHOOD_CHAIN_ID, "robinhood", "BTC")


@dataclass(slots=True)
class StreamProjection:
    identity: StreamIdentity
    limits: ObserverLimits = field(default_factory=ObserverLimits)
    start_at: float | None = None
    last_at: float | None = None
    epoch: int = 0
    frames: int = 0
    bytes_seen: int = 0
    malformed: int = 0
    book_nonce: int | None = None
    book_snapshot: bool = False
    book_gaps: int = 0
    order_events: int = 0
    order_duplicates: int = 0
    order_conflicts: int = 0
    stopped_reason: str | None = None
    last_orders: dict[tuple[int, int], tuple[str, str, str, str]] = field(default_factory=dict)

    def reconnect(self) -> None:
        """Invalidate stream continuity; budgets and local chronology persist."""
        self.epoch += 1
        self.book_nonce = None
        self.book_snapshot = False
        self.last_orders.clear()

    def feed(self, raw: bytes, received_at: float) -> list[dict[str, object]]:
        if self.stopped_reason is not None:
            return []
        if (type(raw) is not bytes or isinstance(received_at, bool)
                or not isinstance(received_at, (int, float))
                or not 0 <= received_at < float("inf")):
            self.stopped_reason = "invalid_input"
            return []
        if self.last_at is not None and received_at < self.last_at:
            self.stopped_reason = "receipt_time_regression"
            return []
        if self.start_at is None:
            self.start_at = received_at
        if received_at - self.start_at > self.limits.max_seconds:
            self.stopped_reason = "duration_limit"
            return []
        if len(raw) > self.limits.max_frame_bytes:
            self.stopped_reason = "frame_limit_bytes"
            return []
        if self.bytes_seen + len(raw) > self.limits.max_bytes:
            self.stopped_reason = "total_byte_limit"
            return []
        if self.frames >= self.limits.max_frames:
            self.stopped_reason = "frame_count_limit"
            return []
        self.frames += 1
        self.bytes_seen += len(raw)
        self.last_at = received_at
        try:
            frame = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError):
            self.malformed += 1
            return []
        if not isinstance(frame, dict):
            self.malformed += 1
            return []
        channel, kind = frame.get("channel"), frame.get("type")
        if not isinstance(channel, str) or not isinstance(kind, str):
            self.malformed += 1
            return []
        if channel == f"order_book:{self.identity.market_id}":
            return self._book(frame, kind, received_at)
        for account in self.identity.accounts:
            if channel == f"account_all_orders:{account}":
                return self._orders(frame, kind, account, received_at)
        return []

    def _book(self, frame: dict, kind: str, at: float) -> list[dict[str, object]]:
        if kind not in {"subscribed/order_book", "update/order_book"}:
            return []
        value = frame.get("order_book")
        if not isinstance(value, dict) or not _integer(value.get("nonce")):
            self.malformed += 1
            self.book_snapshot = False
            return []
        nonce = value["nonce"]
        if kind == "subscribed/order_book":
            self.book_nonce = nonce
            self.book_snapshot = True
            label = "book_snapshot"
        elif not self.book_snapshot:
            # The documented update is not proof of a subscription snapshot.
            self.book_gaps += 1
            label = "book_unanchored"
        elif not _integer(value.get("begin_nonce")) or value["begin_nonce"] != self.book_nonce or nonce <= self.book_nonce:
            self.book_snapshot = False
            self.book_nonce = None
            self.book_gaps += 1
            label = "book_gap"
        else:
            self.book_nonce = nonce
            label = "book_update"
        result: dict[str, object] = {"kind": label, "epoch": self.epoch, "received_at": at}
        if label in {"book_snapshot", "book_update"}:
            result["nonce"] = nonce
        if _integer(frame.get("timestamp")):
            result["venue_timestamp_raw"] = frame["timestamp"]
        if _integer(value.get("last_updated_at")):
            result["venue_last_updated_at_raw"] = value["last_updated_at"]
        return [result]

    def _orders(self, frame: dict, kind: str, account: int, at: float) -> list[dict[str, object]]:
        if kind not in {"subscribed/account_all_orders", "update/account_all_orders"}:
            return []
        orders = frame.get("orders")
        if not isinstance(orders, dict):
            self.malformed += 1
            return []
        selected = orders.get(str(self.identity.market_id), [])
        if not isinstance(selected, list):
            self.malformed += 1
            return []
        output = []
        for item in selected:
            if not isinstance(item, dict):
                self.malformed += 1
                continue
            if (type(item.get("owner_account_index")) is not int
                    or item["owner_account_index"] != account
                    or type(item.get("market_index")) is not int
                    or item["market_index"] != self.identity.market_id
                    or not _integer(item.get("client_order_index"))):
                self.malformed += 1
                continue
            order_id, status = item.get("order_id"), item.get("status")
            filled, remaining = _amount(item.get("filled_base_amount")), _amount(item.get("remaining_base_amount"))
            if (not isinstance(order_id, str) or not order_id.isascii()
                    or not order_id.isdecimal() or len(order_id) > 20
                    or status not in ORDER_STATUSES or filled is None or remaining is None):
                self.malformed += 1
                continue
            client = item["client_order_index"]
            prior = self.last_orders.get((account, client))
            state = (order_id, status, filled, remaining)
            if prior == state:
                self.order_duplicates += 1
                continue
            if prior is not None and (prior[0] != order_id or prior[1] in TERMINAL and status != prior[1]
                                      or Decimal(filled) < Decimal(prior[2])):
                self.order_conflicts += 1
                output.append({"kind": "order_conflict", "epoch": self.epoch,
                               "account_index": account, "client_order_index": client,
                               "received_at": at})
                continue
            self.last_orders[(account, client)] = state
            self.order_events += 1
            projection: dict[str, object] = {
                "kind": "order", "epoch": self.epoch, "received_at": at,
                "account_index": account, "market_id": self.identity.market_id,
                "client_order_index": client, "order_id": order_id,
                "status": status, "filled_base_amount": filled,
                "remaining_base_amount": remaining,
                "terminal": status in TERMINAL, "stream_complete": False,
            }
            for name in ("timestamp", "created_at", "updated_at", "transaction_time"):
                if _integer(item.get(name)):
                    projection[f"venue_{name}_raw"] = item[name]
            output.append(projection)
        return output

    def summary(self) -> dict[str, object]:
        return {"schema": "hcr44-stream-v1", "market_id": self.identity.market_id,
                "accounts": list(self.identity.accounts), "epoch": self.epoch,
                "start_at": self.start_at, "last_at": self.last_at,
                "frames": self.frames, "bytes": self.bytes_seen,
                "malformed": self.malformed, "book_snapshot": self.book_snapshot,
                "book_gaps": self.book_gaps, "order_events": self.order_events,
                "order_duplicates": self.order_duplicates,
                "order_conflicts": self.order_conflicts,
                "stopped_reason": self.stopped_reason}


async def _auth_tokens(identity: StreamIdentity, provider: KeychainSecretProvider) -> dict[int, str]:
    """Generate short-lived read tokens in memory, without a mutation nonce manager."""
    import lighter

    nonce_type = lighter.nonce_manager.NonceManagerType.NONE
    tokens = {}
    for account in identity.accounts:
        signer = lighter.SignerClient(url=identity.api_base_url, account_index=account,
                                      api_private_keys={identity.api_key_index: provider.private_key(account, identity.api_key_index)},
                                      chain_id=identity.chain_id, nonce_management_type=nonce_type)
        try:
            result = signer.create_auth_token_with_expiry(deadline=600, api_key_index=identity.api_key_index)
            if asyncio.iscoroutine(result):
                raise RuntimeError("unexpected asynchronous token interface")
            token = result[0] if isinstance(result, tuple) else result
            if isinstance(result, tuple) and len(result) > 1 and result[1]:
                raise RuntimeError("auth token creation failed")
            if not isinstance(token, str) or not token:
                raise RuntimeError("auth token creation failed")
            tokens[account] = token
        finally:
            await signer.close()
    return tokens


async def collect_once(identity: StreamIdentity, output: Path, provider: KeychainSecretProvider,
                       *, limits: ObserverLimits = ObserverLimits()) -> dict[str, object]:
    """One connection; no automatic retry/reconnect or trading message type."""
    from websockets.asyncio.client import connect

    if (limits.max_seconds > 600 or limits.max_frames > 15000
            or limits.max_bytes > 20 * 1024 * 1024 or limits.max_frame_bytes > 65536):
        raise ValueError("collection exceeds the owner gate")
    if output.exists() or output.is_symlink():
        raise FileExistsError("measurement output already exists")
    tokens = await _auth_tokens(identity, provider)
    observer = StreamProjection(identity, limits)
    started = time.monotonic()
    stop_at = started + limits.max_seconds
    # The file contains only allowlisted projections. Never serialize raw frames.
    with open(output, "x", encoding="utf-8", opener=lambda path, flags: os.open(path, flags, 0o600)) as sink:
        try:
            async with connect(WS_URL, max_size=limits.max_frame_bytes,
                               max_queue=16, ping_interval=30, open_timeout=10,
                               close_timeout=3) as socket:
                subscriptions = [f"order_book/{identity.market_id}"]
                subscriptions += [f"account_all_orders/{account}" for account in identity.accounts]
                for channel in subscriptions:
                    value = {"type": "subscribe", "channel": channel}
                    if channel.startswith("account_all_orders/"):
                        value["auth"] = tokens[int(channel.rsplit("/", 1)[1])]
                    await socket.send(json.dumps(value, separators=(",", ":")))
                tokens.clear()
                while observer.stopped_reason is None:
                    remaining = stop_at - time.monotonic()
                    if remaining <= 0:
                        observer.stopped_reason = "duration_limit"
                        break
                    try:
                        incoming = await asyncio.wait_for(socket.recv(), remaining)
                    except asyncio.TimeoutError:
                        observer.stopped_reason = "duration_limit"
                        break
                    raw = incoming.encode("utf-8") if isinstance(incoming, str) else incoming
                    for projection in observer.feed(raw, time.monotonic()):
                        sink.write(json.dumps(projection, separators=(",", ":")) + "\n")
                    if observer.frames >= limits.max_frames:
                        observer.stopped_reason = "frame_count_limit"
                    elif observer.bytes_seen >= limits.max_bytes:
                        observer.stopped_reason = "total_byte_limit"
        except Exception:
            # Exception text may contain the WebSocket auth message or raw frame.
            observer.stopped_reason = observer.stopped_reason or "connection_or_protocol_error"
        finally:
            tokens.clear()
            sink.flush()
    summary = observer.summary()
    summary["duration_seconds"] = time.monotonic() - started
    return summary


def main() -> int:
    """Offline-safe CLI until called with a pre-existing protected config path."""
    import argparse
    parser = argparse.ArgumentParser(description="One bounded read-only Robinhood BTC stream session")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.config.is_symlink() or not args.config.is_file():
            raise ValueError("protected config unavailable")
        value = json.loads(args.config.read_text())
        identity = StreamIdentity.from_config(value)
        if (not args.output.parent.is_dir() or args.output.parent.is_symlink()
                or args.output.parent.stat().st_uid != os.geteuid()
                or args.output.parent.stat().st_mode & 0o077
                or args.output.with_suffix(".summary.json").exists()):
            raise ValueError("owner-only output directory unavailable")
        provider = KeychainSecretProvider.from_config(
            identity, identity.accounts,
            prompt=lambda _message: (_ for _ in ()).throw(RuntimeError("keychain record unavailable")))
        try:
            # Leave time for handshake and close inside the owner's 10-minute cap.
            summary = asyncio.run(collect_once(identity, args.output, provider,
                                               limits=ObserverLimits(max_seconds=570)))
        finally:
            provider.close()
        summary_path = args.output.with_suffix(".summary.json")
        with open(summary_path, "x", encoding="utf-8", opener=lambda path, flags: os.open(path, flags, 0o600)) as sink:
            json.dump(summary, sink, sort_keys=True, indent=2)
            sink.write("\n")
        print(json.dumps({"status": summary["stopped_reason"], "frames": summary["frames"],
                          "bytes": summary["bytes"], "order_events": summary["order_events"]}))
        return 0
    except Exception:
        # Never render underlying exceptions: library errors can contain auth.
        print('{"status":"setup_or_access_blocked"}')
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
