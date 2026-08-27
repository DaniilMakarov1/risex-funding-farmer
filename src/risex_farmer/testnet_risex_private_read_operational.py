"""Sealed, one-shot operational binding for the accepted RISEx private read.

This module is intentionally absent from normal startup and exposes no trading API.
Its operational constructor has no path, URL, session, proxy, or credential override.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import os
from pathlib import Path
import pwd
import sqlite3
import ssl
import stat
import tempfile
import time
from typing import Any, Awaitable, Callable, Sequence
import uuid

import aiohttp

from .testnet_risex_order_lifecycle import (
    DurableIntentStore, _SCHEMA, _valid_order_id, encode_place_action,
    pack_order_data,
)
from .testnet_risex_private_read_preflight import (
    ACCOUNT, AUTHORIZATION, HttpResponse, Outcome, PreflightResult,
    MARKET_ID, MINIMUM, PrivateReadPreflight, PrivateReadStore, REST_ORIGIN,
    ROUTER, SIGNER, STEP, TICK, SyntheticCredential, WS_ORIGIN, expected_url,
)
from . import testnet_risex_signer as _signer


_LIFECYCLE = ".risex-funding-farmer-testnet-order-lifecycle-v1.sqlite"
_ATTEMPT = ".risex-funding-farmer-risex-private-read-attempt-v1.json"
_PREFLIGHT = ".risex-funding-farmer-risex-private-read-v1.sqlite"


def _home() -> Path:
    return Path(pwd.getpwuid(os.getuid()).pw_dir)


def _safe_file(path: Path) -> bool:
    try:
        details = path.lstat()
    except OSError:
        return False
    return (
        stat.S_ISREG(details.st_mode) and not path.is_symlink()
        and details.st_uid == os.getuid() and stat.S_IMODE(details.st_mode) == 0o600
        and details.st_nlink == 1
    )


def _strict_json(raw: str) -> Any:
    def reject_constant(_value: str) -> Any:
        raise ValueError("non-finite JSON number rejected")

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON object key rejected")
            result[key] = value
        return result

    def finite_float(value: str) -> float:
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError("non-finite JSON conversion rejected")
        return parsed

    try:
        return json.loads(
            raw, parse_constant=reject_constant, parse_float=finite_float,
            object_pairs_hook=unique_object,
        )
    except Exception:
        raise ValueError("strict JSON rejected") from None


class SealedTransport:
    REST_ORIGIN = REST_ORIGIN
    WS_URL = WS_ORIGIN
    TRUST_ENV = False
    ALLOW_REDIRECTS = False
    MAX_BYTES = 1_048_576
    MAX_FRAMES = 3
    DEADLINE_SECONDS = 5

    @staticmethod
    def _allowed_url(path: str, query: Sequence[tuple[str, str]]) -> str:
        allowed = {
            expected_url(request_path, request_query)
            for request_path, request_query in PrivateReadPreflight._REQUESTS
        }
        allowed.add(expected_url("/v1/auth/nonce", (("account", ACCOUNT),)))
        target = expected_url(path, query)
        if target not in allowed:
            raise ValueError("HTTP request surface rejected")
        return target

    def __init__(self) -> None:
        timeout = aiohttp.ClientTimeout(total=self.DEADLINE_SECONDS,
                                        connect=2, sock_read=3)
        self._session = aiohttp.ClientSession(
            timeout=timeout, trust_env=False,
            connector=aiohttp.TCPConnector(ssl=ssl.create_default_context()),
        )

    async def public_get(self, path: str, query: Sequence[tuple[str, str]]) -> HttpResponse:
        target = self._allowed_url(path, query)
        async with self._session.get(target, allow_redirects=False, proxy=None) as response:
            final = str(response.url)
            redirected = bool(response.history) or final != target
            if redirected:
                raise ValueError("redirect or final URL rejected")
            body = await self._bounded_body(response)
            return HttpResponse(response.status, final, body, time.time(), redirected)

    async def _bounded_body(self, response: aiohttp.ClientResponse) -> Any:
        declared = response.content_length
        if declared is not None and declared > self.MAX_BYTES:
            raise ValueError("bounded response rejected")
        raw = await response.content.read(self.MAX_BYTES + 1)
        if len(raw) > self.MAX_BYTES:
            raise ValueError("bounded response rejected")
        try:
            return _strict_json(raw.decode("utf-8", errors="strict"))
        except Exception:
            raise ValueError("strict JSON rejected") from None

    async def _private_exchange(self, url: str, outbound: Sequence[dict[str, Any]]) -> tuple[Any, ...]:
        if url != self.WS_URL or len(outbound) != self.MAX_FRAMES:
            raise ValueError("sealed websocket rejected")
        if (
            set(outbound[0]) != {"method", "params"}
            or outbound[0].get("method") != "auth_v2"
            or outbound[1:] != (
                {"method": "subscribe", "params": {"channel": "orders"}},
                {"method": "subscribe", "params": {"channel": "positions"}},
            )
        ):
            raise ValueError("websocket sequence rejected")
        async with self._session.ws_connect(
            self.WS_URL, ssl=ssl.create_default_context(), proxy=None,
            autoclose=False, autoping=False, max_msg_size=self.MAX_BYTES,
        ) as socket:
            frames = []
            for sent in outbound:
                await socket.send_json(sent)
                incoming = await socket.receive(timeout=self.DEADLINE_SECONDS)
                if incoming.type is not aiohttp.WSMsgType.TEXT:
                    raise ValueError("websocket frame rejected")
                try:
                    frames.append(_strict_json(incoming.data))
                except Exception:
                    raise ValueError("websocket JSON rejected") from None
            try:
                extra = await socket.receive(timeout=0.01)
            except asyncio.TimeoutError:
                extra = None
            if extra is not None:
                raise ValueError("extra websocket frame rejected")
            await socket.close()
            return tuple(frames)

    async def close(self) -> None:
        await self._session.close()


class LifecycleClearBinding:
    """Read-only predicate for an empty or terminally safe RISEx history."""

    def __init__(self) -> None:
        self._path = _home() / _LIFECYCLE
        self._allow_initialize = True

    @classmethod
    def _fixture(cls, path: Path) -> "LifecycleClearBinding":
        value = object.__new__(cls)
        value._path = Path(path)
        value._allow_initialize = True
        return value

    def __call__(self) -> bool:
        try:
            pair_paths = _two_account_journal_paths(self._path.parent)
            legacy_present = _path_present(self._path)
            pair_present = tuple(_path_present(path) for path in pair_paths)
            if any(pair_present) and (
                not all(pair_present) or not legacy_present
            ):
                self._allow_initialize = False
                return False
            if not legacy_present:
                if not self._allow_initialize:
                    return False
                _initialize_legacy_lifecycle_database(self._path)
            self._allow_initialize = False
            if not _legacy_lifecycle_safe(self._path):
                return False
            if not any(pair_present):
                pair_present = tuple(_path_present(path) for path in pair_paths)
                if not any(pair_present):
                    return True
                if not all(pair_present):
                    self._allow_initialize = False
                    return False
            return _two_account_journals_safe(*pair_paths)
        except Exception:
            self._allow_initialize = False
            return False


@dataclass(frozen=True)
class _ValidatedPairJournal:
    role: str
    meta: dict[str, str]
    intents: tuple[dict[str, Any], ...]
    terminal: dict[str, str]


def _path_present(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    except OSError:
        return True
    return True


def _initialize_legacy_lifecycle_database(path: Path) -> None:
    fd: int | None = None
    try:
        fd = os.open(
            path,
            os.O_CREAT | os.O_EXCL | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        os.fchmod(fd, 0o600)
        os.fsync(fd)
    finally:
        if fd is not None:
            os.close(fd)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    if not _safe_file(path):
        raise ValueError("legacy lifecycle path rejected")
    store = DurableIntentStore(path)
    try:
        store._bind_identities(ACCOUNT, SIGNER, ROUTER, AUTHORIZATION)
    finally:
        store.close()
    _fsync_file_and_parent(path)


def _read_only_sqlite(path: Path) -> sqlite3.Connection:
    if not _safe_file(path):
        raise ValueError("SQLite history path rejected")
    connection = sqlite3.connect(f"{path.absolute().as_uri()}?mode=ro", uri=True)
    connection.execute("PRAGMA query_only=ON")
    return connection


def _hex_digest(value: Any) -> bool:
    return (
        isinstance(value, str) and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _canonical_uint_text(value: Any, *, maximum: int, minimum: int = 0) -> int | None:
    if (
        not isinstance(value, str) or not value.isascii() or not value.isdecimal()
        or (len(value) > 1 and value.startswith("0"))
    ):
        return None
    try:
        parsed = int(value)
    except ValueError:
        return None
    return parsed if minimum <= parsed <= maximum else None


def _canonical_decimal_text(
    value: Any, *, positive: bool = False, nonnegative: bool = False,
) -> Decimal | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite() or str(parsed) != value:
        return None
    if positive and parsed <= 0:
        return None
    if nonnegative and parsed < 0:
        return None
    return parsed


def _canonical_uuid(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError):
        return False
    return parsed.version == 4 and str(parsed) == value


def _order_identity(value: Any) -> tuple[int, int] | None:
    if not isinstance(value, str) or not _valid_order_id(value):
        return None
    try:
        wide = int(value[2:18], 16)
    except ValueError:
        return None
    if not 0 <= wide < 2**64:
        return None
    return wide, wide >> 1


def _canonical_history_order(row: Any, expected_account: str) -> bool:
    if not isinstance(row, list) or len(row) != 15:
        return False
    (
        order_id, wide, resting, client, market_id, account, side, order_type,
        time_in_force, status, size, filled_size, price, post_only, reduce_only,
    ) = row
    identity = _order_identity(order_id)
    size_value = _canonical_decimal_text(size, positive=True)
    filled_value = _canonical_decimal_text(filled_size, nonnegative=True)
    price_value = _canonical_decimal_text(price, nonnegative=True)
    return (
        identity is not None
        and type(wide) is int and type(resting) is int
        and identity == (wide, resting)
        and 0 <= wide < 2**64 and 0 <= resting < 2**64
        and type(client) is int and 0 < client < 2**64
        and market_id == MARKET_ID
        and account == expected_account
        and side in {"BUY", "SELL"}
        and order_type in {"MARKET", "LIMIT"}
        and time_in_force in {"GTC", "IOC"}
        and status in {"OPEN", "FILLED", "CANCELLED"}
        and size_value is not None and size_value % STEP == 0
        and filled_value is not None and filled_value <= size_value
        and price_value is not None and price_value % TICK == 0
        and (order_type != "LIMIT" or price_value > 0)
        and type(post_only) is bool and type(reduce_only) is bool
    )


def _canonical_history_trade(row: Any, expected_account: str) -> bool:
    if not isinstance(row, list) or len(row) != 8:
        return False
    trade_id, order_id, client, market_id, account, side, size, price = row
    trade_parts = trade_id.split("-") if isinstance(trade_id, str) else ()
    size_value = _canonical_decimal_text(size, positive=True)
    price_value = _canonical_decimal_text(price, positive=True)
    return (
        isinstance(trade_id, str) and len(trade_parts) == 2
        and all(_order_identity(part) is not None for part in trade_parts)
        and _order_identity(order_id) is not None
        and type(client) is int and 0 < client < 2**64
        and market_id == MARKET_ID and account == expected_account
        and side in {"BUY", "SELL"}
        and size_value is not None and size_value % STEP == 0
        and price_value is not None and price_value % TICK == 0
    )


def _canonical_history_payload(token: Any, digest: Any, expected_account: str) -> bool:
    if (
        not isinstance(token, str) or len(token) > 1_048_576
        or not _hex_digest(digest)
    ):
        return False
    try:
        payload = _strict_json(token)
        canonical = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), allow_nan=False,
        )
    except Exception:
        return False
    if canonical != token or hashlib.sha256(token.encode("utf-8")).hexdigest() != digest:
        return False
    if (
        not isinstance(payload, dict) or set(payload) != {"orders", "trades"}
        or not isinstance(payload["orders"], list)
        or not isinstance(payload["trades"], list)
        or len(payload["orders"]) + len(payload["trades"]) > 256
    ):
        return False
    orders = payload["orders"]
    trades = payload["trades"]
    return (
        all(_canonical_history_order(row, expected_account) for row in orders)
        and all(_canonical_history_trade(row, expected_account) for row in trades)
        and len({row[0] for row in orders}) == len(orders)
        and len({row[0] for row in trades}) == len(trades)
        and [row[0] for row in orders] == sorted(row[0] for row in orders)
        and [row[0] for row in trades] == sorted(row[0] for row in trades)
    )


def _legacy_identity(terminal: dict[str, str]) -> bool:
    return terminal == {
        "account": ACCOUNT,
        "signer": SIGNER,
        "router": ROUTER,
        "authorization": AUTHORIZATION,
    }


def _legacy_intent_safe(row: tuple[Any, ...]) -> tuple[bool, str | None, str]:
    if len(row) != 24:
        return False, None, ""
    (
        intent_id, ordinal, kind, client_order_id, nonce, nonce_bitmap,
        payload_digest, bbo_digest, state, side, order_type, time_in_force,
        reduce_only, post_only, market_id, size, size_steps, price, price_ticks,
        source_position, expires_at, dispatch_count, order_id, reconciled,
    ) = row
    client = _canonical_uint_text(client_order_id, maximum=2**64 - 1, minimum=1)
    price_value = _canonical_decimal_text(price, positive=True)
    source_value = _canonical_decimal_text(source_position, nonnegative=True)
    order_identity = _order_identity(order_id) if order_id is not None else None
    safe = (
        _canonical_uuid(intent_id) and ordinal == 1 and kind == "OPEN"
        and client is not None and type(nonce) is int and 0 <= nonce < 2**48
        and type(nonce_bitmap) is int and 0 <= nonce_bitmap <= 207
        and _hex_digest(payload_digest) and _hex_digest(bbo_digest)
        and state == "TERMINAL" and side == "BUY" and order_type == "MARKET"
        and time_in_force == "IOC" and reduce_only == 0 and post_only == 0
        and market_id == MARKET_ID and size == str(MINIMUM)
        and size_steps == int(MINIMUM / STEP) and price_value is not None
        and price_value % TICK == 0 and price_ticks == int(price_value / TICK)
        and 0 < price_ticks < 2**24 and source_value == 0
        and type(expires_at) is int and 0 < expires_at < 2**32
        and dispatch_count == 1 and reconciled == 1
        and (order_id is None or order_identity is not None)
    )
    if not safe:
        return False, None, str(intent_id)
    try:
        order_data = pack_order_data(
            market_id=market_id, size_steps=size_steps, price_ticks=price_ticks,
            side=side, post_only=False, reduce_only=False,
            order_type=order_type, time_in_force=time_in_force,
        )
        _encoded, action_hash = encode_place_action(
            order_data=order_data, client_order_id=client,
        )
    except Exception:
        return False, None, str(intent_id)
    return payload_digest == action_hash.hex(), order_id, str(intent_id)


def _snapshot_window_safe(value: Any) -> bool:
    if not isinstance(value, str) or value.count(":") != 1:
        return False
    start, end = value.split(":")
    start_value = _canonical_uint_text(start, maximum=2**63 - 1)
    end_value = _canonical_uint_text(end, maximum=2**63 - 1)
    return (
        start_value is not None and end_value is not None
        and start_value <= end_value <= start_value + 5
    )


def _legacy_lifecycle_safe(path: Path) -> bool:
    connection = _read_only_sqlite(path)
    try:
        if not _canonical_lifecycle_database(connection):
            return False
        terminal_rows = connection.execute(
            "SELECT key,value FROM terminal ORDER BY key"
        ).fetchall()
        if any(type(key) is not str or type(value) is not str for key, value in terminal_rows):
            return False
        terminal = dict(terminal_rows)
        intents = connection.execute("SELECT * FROM intents ORDER BY ordinal").fetchall()
        cancels = connection.execute("SELECT * FROM cancels").fetchall()
        if not cancels:
            if not intents and _legacy_identity(terminal):
                return True
            if len(intents) != 1:
                return False
            valid, order_id, intent_id = _legacy_intent_safe(intents[0])
            if not valid or terminal.get("outcome") != "COMPLETED_NO_FILL_FLAT":
                return False
            if terminal.get(f"position:{intent_id}") != "0":
                return False
            if not _snapshot_window_safe(terminal.get(f"snapshot:{intent_id}")):
                return False
            if terminal.get(f"place_result:{intent_id}") != "ACCEPTED":
                return False
            if terminal.get(f"place_failure:{intent_id}") != "ORDER_ID_ACCEPTED":
                return False
            expected = {
                "account": ACCOUNT, "signer": SIGNER, "router": ROUTER,
                "authorization": AUTHORIZATION, "outcome": terminal["outcome"],
                f"position:{intent_id}": "0",
                f"snapshot:{intent_id}": terminal[f"snapshot:{intent_id}"],
                f"place_result:{intent_id}": "ACCEPTED",
                f"place_failure:{intent_id}": "ORDER_ID_ACCEPTED",
            }
            if order_id is not None:
                wide, resting = _order_identity(order_id)  # type: ignore[misc]
                expected[f"wide:{intent_id}"] = str(wide)
                expected[f"resting:{intent_id}"] = str(resting)
            return terminal == expected
        return False
    except (sqlite3.DatabaseError, TypeError, ValueError, KeyError):
        return False
    finally:
        connection.close()


def _two_account_journal_module() -> Any:
    from . import testnet_risex_two_account_coordinator as coordinator
    return coordinator


def _two_account_journal_paths(directory: Path) -> tuple[Path, Path]:
    coordinator = _two_account_journal_module()
    return (
        directory / coordinator.PRIMARY_JOURNAL,
        directory / coordinator.COUNTERPARTY_JOURNAL,
    )


def _pair_meta_safe(
    rows: list[tuple[Any, Any]], *, role: str, account: str, signer: str,
) -> dict[str, str] | None:
    if (
        len(rows) != 6
        or any(type(key) is not str or type(value) is not str for key, value in rows)
    ):
        return None
    meta = dict(rows)
    if set(meta) != {"role", "account", "signer", "run_id", "phase", "outcome"}:
        return None
    prefix = f"risex-two-{role.lower()}-"
    run_id = meta["run_id"]
    suffix = run_id[len(prefix):] if run_id.startswith(prefix) else ""
    return meta if (
        meta["role"] == role and meta["account"] == account
        and meta["signer"] == signer
        and len(suffix) == 32
        and all(character in "0123456789abcdef" for character in suffix)
        and meta["phase"] == "COMPLETE" and meta["outcome"] == "COMPLETE"
    ) else None


def _pair_intent_safe(
    row: tuple[Any, ...], *, role: str, ordinal: int,
) -> dict[str, Any] | None:
    if len(row) != 23:
        return None
    (
        intent_id, row_ordinal, step, client_order_id, nonce_anchor, nonce_bitmap,
        payload_digest, bbo_digest, state, side, order_type, time_in_force,
        reduce_only, post_only, market_id, size, price, source_position,
        expires_at, dispatch_count, order_id, filled_size, reconciled,
    ) = row
    expected = {
        "PRIMARY": (
            ("ENTRY_TAKER", "BUY", "MARKET", "IOC", 0, 0, "0"),
            ("EXIT_TAKER", "SELL", "MARKET", "IOC", 1, 0, "0.1"),
        ),
        "COUNTERPARTY": (
            ("ENTRY_MAKER", "SELL", "LIMIT", "GTC", 0, 1, "0"),
            ("EXIT_MAKER", "BUY", "LIMIT", "GTC", 1, 1, "-0.1"),
        ),
    }[role][ordinal - 1]
    step_expected, side_expected, type_expected, tif_expected, reduce_expected, post_expected, source_expected = expected
    client = _canonical_uint_text(client_order_id, maximum=2**64 - 1, minimum=1)
    price_value = _canonical_decimal_text(price, positive=True)
    source_value = _canonical_decimal_text(source_position)
    filled_value = _canonical_decimal_text(filled_size, positive=True)
    identity = _order_identity(order_id)
    safe = (
        _canonical_uuid(intent_id) and row_ordinal == ordinal
        and step == step_expected and client is not None
        and type(nonce_anchor) is int and 0 <= nonce_anchor < 2**48
        and type(nonce_bitmap) is int and 0 <= nonce_bitmap <= 207
        and _hex_digest(payload_digest) and _hex_digest(bbo_digest)
        and state == "TERMINAL" and side == side_expected
        and order_type == type_expected and time_in_force == tif_expected
        and reduce_only == reduce_expected and post_only == post_expected
        and market_id == MARKET_ID and size == str(MINIMUM)
        and price_value is not None and price_value % TICK == 0
        and source_value is not None and str(source_value) == source_expected
        and type(expires_at) is int and 0 < expires_at < 2**32
        and dispatch_count == 1 and identity is not None
        and filled_size == str(MINIMUM) and filled_value == MINIMUM
        and reconciled == 1
    )
    if not safe:
        return None
    action_data = {
        "step": step, "side": side, "order_type": order_type,
        "time_in_force": time_in_force, "reduce_only": bool(reduce_only),
        "post_only": bool(post_only), "market_id": market_id,
        "size": size, "price": price, "source_position": source_position,
        "client_order_id": client, "nonce_anchor": nonce_anchor,
        "nonce_bitmap": nonce_bitmap, "expires_at": expires_at,
    }
    payload = hashlib.sha256(
        json.dumps(action_data, sort_keys=True, separators=(",", ":"), allow_nan=False)
        .encode("utf-8")
    ).hexdigest()
    if payload != payload_digest:
        return None
    return {
        "intent_id": intent_id, "step": step, "client_order_id": client,
        "nonce_anchor": nonce_anchor, "nonce_bitmap": nonce_bitmap,
        "payload_digest": payload_digest, "price": price,
        "price_value": price_value,
        "source_position": source_position, "order_id": order_id,
    }


def _same_account_intents_safe(intents: Sequence[dict[str, Any]]) -> bool:
    return all(
        len({item[field] for item in intents}) == len(intents)
        for field in ("intent_id", "client_order_id", "order_id")
    ) and len({
        (item["nonce_anchor"], item["nonce_bitmap"]) for item in intents
    }) == len(intents)


def _pair_journal_safe(
    path: Path, *, role: str, account: str, signer: str,
) -> _ValidatedPairJournal | None:
    connection = _read_only_sqlite(path)
    try:
        coordinator = _two_account_journal_module()
        expected = sqlite3.connect(":memory:")
        try:
            expected.executescript(coordinator._JOURNAL_SCHEMA)
            expected_catalog = _sqlite_catalog(expected)
        finally:
            expected.close()
        if not _canonical_sqlite_database(connection, expected_catalog):
            return None
        meta = _pair_meta_safe(
            connection.execute("SELECT key,value FROM meta ORDER BY key").fetchall(),
            role=role, account=account, signer=signer,
        )
        if meta is None:
            return None
        if connection.execute("SELECT 1 FROM cancels LIMIT 1").fetchone() is not None:
            return None
        rows = connection.execute("SELECT * FROM intents ORDER BY ordinal").fetchall()
        if len(rows) != 2:
            return None
        intents = tuple(
            item for index, row in enumerate(rows, 1)
            if (item := _pair_intent_safe(row, role=role, ordinal=index)) is not None
        )
        if len(intents) != 2 or not _same_account_intents_safe(intents):
            return None
        terminal_rows = connection.execute(
            "SELECT key,value FROM terminal ORDER BY key"
        ).fetchall()
        if any(type(key) is not str or type(value) is not str or not key or not value
               for key, value in terminal_rows):
            return None
        terminal = dict(terminal_rows)
        intent_ids = {item["intent_id"] for item in intents}
        required = {
            "baseline_history", "baseline_history_digest", "trade:ENTRY",
            "price:ENTRY", "trade:EXIT", "price:EXIT", "final_round_one",
            "final_round_one_id", "final_round_two",
            *(f"place:{intent_id}" for intent_id in intent_ids),
        }
        optional = {
            key for key in terminal
            if key.startswith("place_resample:")
        }
        if set(terminal) - required - optional or not required <= set(terminal):
            return None
        if any(
            not _canonical_uuid(key.split(":", 1)[1])
            or terminal[key] != "USED"
            for key in optional
        ):
            return None
        if not _canonical_history_payload(
            terminal["baseline_history"], terminal["baseline_history_digest"], account,
        ):
            return None
        if not all(_hex_digest(terminal[key]) for key in (
            "final_round_one", "final_round_two",
        )):
            return None
        if _canonical_uint_text(
            terminal["final_round_one_id"], maximum=2**63 - 1, minimum=1,
        ) is None:
            return None
        if any(terminal[f"place:{item['intent_id']}"] != "ACCEPTED" for item in intents):
            return None
        return _ValidatedPairJournal(role, meta, intents, terminal)
    except (ImportError, sqlite3.DatabaseError, TypeError, ValueError, KeyError):
        return None
    finally:
        connection.close()


def _two_account_journals_safe(primary_path: Path, counterparty_path: Path) -> bool:
    coordinator = _two_account_journal_module()
    primary = _pair_journal_safe(
        primary_path, role="PRIMARY", account=coordinator.PRIMARY_ACCOUNT,
        signer=coordinator.PRIMARY_SIGNER,
    )
    counterparty = _pair_journal_safe(
        counterparty_path, role="COUNTERPARTY", account=coordinator.COUNTERPARTY_ACCOUNT,
        signer=coordinator.COUNTERPARTY_SIGNER,
    )
    if primary is None or counterparty is None:
        return False
    if (
        primary.meta["run_id"] == counterparty.meta["run_id"]
        or primary.meta["account"] == counterparty.meta["account"]
        or primary.meta["signer"] == counterparty.meta["signer"]
    ):
        return False
    if (
        not _same_account_intents_safe(primary.intents)
        or not _same_account_intents_safe(counterparty.intents)
    ):
        return False
    all_intents = (*primary.intents, *counterparty.intents)
    if any(
        len({item[field] for item in all_intents}) != len(all_intents)
        for field in ("intent_id", "client_order_id", "order_id")
    ):
        return False
    primary_by_step = {item["step"]: item for item in primary.intents}
    counter_by_step = {item["step"]: item for item in counterparty.intents}
    intent_ids = {
        item["intent_id"]
        for item in (*primary.intents, *counterparty.intents)
    }
    if set(primary_by_step) != {"ENTRY_TAKER", "EXIT_TAKER"} or set(counter_by_step) != {
        "ENTRY_MAKER", "EXIT_MAKER",
    }:
        return False
    for stage, maker_step, taker_step in (
        ("ENTRY", "ENTRY_MAKER", "ENTRY_TAKER"),
        ("EXIT", "EXIT_MAKER", "EXIT_TAKER"),
    ):
        maker = counter_by_step[maker_step]
        taker = primary_by_step[taker_step]
        if (
            maker["order_id"] == taker["order_id"]
            or (
                stage == "ENTRY" and maker["price_value"] > taker["price_value"]
            )
            or (
                stage == "EXIT" and maker["price_value"] < taker["price_value"]
            )
        ):
            return False
        trade_id = f"{maker['order_id']}-{taker['order_id']}"
        if (
            primary.terminal[f"trade:{stage}"] != trade_id
            or counterparty.terminal[f"trade:{stage}"] != trade_id
            or primary.terminal[f"price:{stage}"] != maker["price"]
            or counterparty.terminal[f"price:{stage}"] != maker["price"]
            or maker["client_order_id"] == taker["client_order_id"]
        ):
            return False
    for key in ("final_round_one", "final_round_one_id", "final_round_two"):
        if primary.terminal[key] != counterparty.terminal[key]:
            return False
    resamples = {
        key for key in primary.terminal if key.startswith("place_resample:")
    }
    if resamples != {
        key for key in counterparty.terminal if key.startswith("place_resample:")
    } or any(
        key.split(":", 1)[1]
        not in intent_ids
        for key in resamples
    ):
        return False
    return True


class SessionSignerCredential(SyntheticCredential):
    def __init__(self, signer: str, secret: bytes) -> None:
        super().__init__(signer, b"")
        self._secret = bytearray(secret)

    def sign_register_v2(self, typed_data: dict[str, Any]) -> str:
        try:
            nonce = typed_data["message"]["nonce"]
            canonical_nonce = PrivateReadPreflight._nonce(nonce)
            canonical = PrivateReadPreflight._typed_data(canonical_nonce)
        except Exception:
            raise ValueError("signer operation rejected") from None
        if self.closed or nonce != canonical_nonce or typed_data != canonical:
            raise ValueError("signer operation rejected")
        return _signer._sign_typed_data(bytes(self._secret), typed_data)

    def close(self) -> None:
        for index in range(len(self._secret)):
            self._secret[index] = 0
        self._secret.clear()
        super().close()


def _load_session_signer_only() -> SessionSignerCredential:
    home_fd = _signer._open_home()
    try:
        record = _signer._load_record(home_fd)
        secret = _signer._load_credential(home_fd)
    finally:
        os.close(home_fd)
    if record.state is not _signer.SignerState.ACTIVE or record.signer != SIGNER:
        secret = b""
        raise ValueError("session signer rejected")
    return _credential_from_secret(secret, SIGNER)


def _credential_from_secret(secret: bytes, expected: str) -> SessionSignerCredential:
    if not isinstance(secret, bytes) or len(secret) != 32:
        raise ValueError("session signer rejected")
    if _signer._derive_address(secret) != expected:
        raise ValueError("session signer rejected")
    return SessionSignerCredential(expected, secret)


class OperationalJournal:
    def __init__(self) -> None:
        self._path = _home() / _ATTEMPT

    @classmethod
    def _fixture(cls, path: Path) -> "OperationalJournal":
        value = object.__new__(cls); value._path = Path(path); return value

    def claim_blocked(self) -> bool:
        payload = b'{"schema_version":1,"result":"PREFLIGHT_BLOCKED"}\n'
        try:
            fd = os.open(self._path, os.O_WRONLY | os.O_CREAT | os.O_EXCL
                         | getattr(os, "O_NOFOLLOW", 0), 0o600)
        except OSError:
            return False
        try:
            os.fchmod(fd, 0o600); os.write(fd, payload); os.fsync(fd)
            directory = os.open(self._path.parent, os.O_RDONLY)
            try: os.fsync(directory)
            finally: os.close(directory)
            return True
        finally:
            os.close(fd)

    def finish(self, passed: bool) -> None:
        if not passed or not _safe_file(self._path):
            return
        payload = b'{"schema_version":1,"result":"PREFLIGHT_PASSED"}\n'
        fd, temp_name = tempfile.mkstemp(prefix=self._path.name + ".", dir=self._path.parent)
        try:
            os.fchmod(fd, 0o600); os.write(fd, payload); os.fsync(fd); os.close(fd)
            os.replace(temp_name, self._path)
            directory = os.open(self._path.parent, os.O_RDONLY)
            try: os.fsync(directory)
            finally: os.close(directory)
        finally:
            try: os.close(fd)
            except OSError: pass
            try: os.unlink(temp_name)
            except OSError: pass


@dataclass
class _FixtureDependencies:
    journal: OperationalJournal
    runner: Callable[[], Awaitable[PreflightResult]]


class OperationalAttempt:
    def __init__(self) -> None:
        self._clock = time.time
        self._fixture: _FixtureDependencies | None = None

    async def run(self) -> PreflightResult:
        journal = self._fixture.journal if self._fixture else OperationalJournal()
        if not journal.claim_blocked():
            return PreflightResult(Outcome.BLOCKED, {})
        try:
            result = (await self._fixture.runner()) if self._fixture else await self._run_production()
            if not isinstance(result, PreflightResult) or result.outcome is not Outcome.PASSED:
                return PreflightResult(Outcome.BLOCKED, {})
            journal.finish(True)
            return result
        except asyncio.CancelledError:
            raise
        except BaseException:
            return PreflightResult(Outcome.BLOCKED, {})

    async def _run_production(self) -> PreflightResult:
        transport = SealedTransport()
        preflight_path = _home() / _PREFLIGHT
        if not _prepare_sqlite_file(preflight_path):
            await transport.close()
            return PreflightResult(Outcome.BLOCKED, {})
        store = PrivateReadStore(preflight_path)
        lifecycle = LifecycleClearBinding()
        if lifecycle() is not True:
            store.close()
            await transport.close()
            return PreflightResult(Outcome.BLOCKED, {})
        controller = PrivateReadPreflight(
            store, clock=self._clock, public_get=transport.public_get,
            lifecycle_clear=lifecycle,
        )
        try:
            barrier = await controller.run_public_barrier()
            if barrier is None:
                return PreflightResult(Outcome.BLOCKED, {})
            return await controller.run_private_proof(
                barrier, signer_loader=_load_session_signer_only,
                nonce_get=transport.public_get,
                sign_register_v2=lambda credential, typed: credential.sign_register_v2(typed),
                private_exchange=transport._private_exchange,
            )
        finally:
            store.close()
            await transport.close()


def fixture_adapter(*, journal: OperationalJournal,
                    runner: Callable[[], Awaitable[PreflightResult]]) -> OperationalAttempt:
    value = OperationalAttempt()
    value._fixture = _FixtureDependencies(journal, runner)
    return value


def _prepare_sqlite_file(path: Path) -> bool:
    if path.exists():
        return _safe_file(path)
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR
                     | getattr(os, "O_NOFOLLOW", 0), 0o600)
        os.fchmod(fd, 0o600); os.fsync(fd); os.close(fd)
        directory = os.open(path.parent, os.O_RDONLY)
        try: os.fsync(directory)
        finally: os.close(directory)
        return _safe_file(path)
    except OSError:
        return False


def _sqlite_catalog(connection: sqlite3.Connection) -> tuple[tuple[Any, ...], ...]:
    return tuple(connection.execute(
        "SELECT type,name,tbl_name,COALESCE(sql,'') FROM sqlite_master "
        "ORDER BY type,name,tbl_name,sql"
    ))


def _expected_lifecycle_catalog() -> tuple[tuple[Any, ...], ...]:
    expected = sqlite3.connect(":memory:")
    try:
        expected.executescript(_SCHEMA)
        return _sqlite_catalog(expected)
    finally:
        expected.close()


def _canonical_lifecycle_database(connection: sqlite3.Connection) -> bool:
    try:
        return _canonical_sqlite_database(
            connection, _expected_lifecycle_catalog(),
        )
    except sqlite3.DatabaseError:
        return False


def _canonical_sqlite_database(
    connection: sqlite3.Connection,
    expected_catalog: tuple[tuple[Any, ...], ...],
) -> bool:
    return (
        connection.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        and not connection.execute("PRAGMA foreign_key_check").fetchall()
        and connection.execute("PRAGMA user_version").fetchone() == (0,)
        and connection.execute("PRAGMA application_id").fetchone() == (0,)
        and _sqlite_catalog(connection) == expected_catalog
    )


def _fsync_file_and_parent(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
