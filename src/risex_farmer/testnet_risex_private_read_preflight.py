"""Opt-in, injected-only RISEx private-read preflight.

This module deliberately owns no transport and is not imported by normal startup.
It proves a narrowly bounded fixture contract and exposes no executing operation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from enum import Enum
import inspect
import json
import math
from pathlib import Path
import sqlite3
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import urlencode


REST_ORIGIN = "https://api.testnet.rise.trade"
WS_ORIGIN = "wss://api.testnet.rise.trade/ws/"
CHAIN_ID = 11155931
DOMAIN_NAME = "RISEx"
DOMAIN_VERSION = "1"
AUTHORIZATION = "0x6da86f486b5e6536358f5b122dbe184522ca0ee3"
ROUTER = "0x980b8621b8e03c3f396e1dc34c00b14d84f2a20f"
ACCOUNT = "0x20f9153e2eeba0ff7880fb5a23e976e8b2af56ee"
SIGNER = "0x6274d6d9f628ba89c36de4b71efa2c602b7f783b"
REGISTERED_AT = "2026-08-23T16:40:21Z"
SIGNER_EXPIRATION = "1790092010"
SIGNER_LABEL = "RISEx Funding Farmer testnet probe"

MAX_AGE_SECONDS = 5
MAX_NOTIONAL = Decimal("500")
MAX_BOUND_FRACTION = Decimal("0.003")
MINIMUM = Decimal("0.0001")
STEP = Decimal("0.000001")
TICK = Decimal("0.1")


class Outcome(str, Enum):
    CLAIMED = "CLAIMED"
    PASSED = "PASSED"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True)
class HttpResponse:
    status: int
    final_url: str
    body: Any
    observed_at: float
    redirected: bool


@dataclass
class SyntheticCredential:
    signer: str
    material: bytes
    closed: bool = False

    def close(self) -> None:
        self.material = b""
        self.closed = True


@dataclass(frozen=True)
class PreflightResult:
    outcome: Outcome
    evidence: Mapping[str, Any]


@dataclass(frozen=True)
class _Barrier:
    owner: object
    issued_at: float


def expected_url(path: str, query: Sequence[tuple[str, str]]) -> str:
    suffix = "?" + urlencode(query) if query else ""
    return REST_ORIGIN + path + suffix


class PrivateReadStore:
    """One-shot durable state containing only redacted counters and booleans."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._db = sqlite3.connect(self.path)
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS private_read_state ("
            "singleton INTEGER PRIMARY KEY CHECK (singleton=1), "
            "outcome TEXT NOT NULL, evidence TEXT NOT NULL)"
        )
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS private_read_public_attempt ("
            "singleton INTEGER PRIMARY KEY CHECK (singleton=1))"
        )
        self._db.commit()

    def start_public_attempt(self) -> bool:
        try:
            with self._db:
                self._db.execute("INSERT INTO private_read_public_attempt VALUES (1)")
            return True
        except sqlite3.IntegrityError:
            return False

    def outcome(self) -> Outcome | None:
        row = self._db.execute(
            "SELECT outcome FROM private_read_state WHERE singleton=1"
        ).fetchone()
        return None if row is None else Outcome(row[0])

    def evidence(self) -> dict[str, Any]:
        row = self._db.execute(
            "SELECT evidence FROM private_read_state WHERE singleton=1"
        ).fetchone()
        return {} if row is None else json.loads(row[0])

    def result(self) -> PreflightResult | None:
        outcome = self.outcome()
        return None if outcome is None else PreflightResult(outcome, self.evidence())

    def _insert_claim(self) -> bool:
        try:
            with self._db:
                self._db.execute(
                    "INSERT INTO private_read_state VALUES (1, ?, ?)",
                    (Outcome.CLAIMED.value, json.dumps({"public_get_count": 18})),
                )
            return True
        except sqlite3.IntegrityError:
            return False

    def _terminal(self, outcome: Outcome, evidence: Mapping[str, Any]) -> None:
        count_keys = {
            "public_get_count", "private_nonce_count", "signature_count",
            "auth_send_count", "orders_snapshot_count", "positions_snapshot_count",
        }
        flat_keys = {"public_flat", "private_flat"}
        safe = {}
        for key, value in evidence.items():
            if key in count_keys and type(value) is int and value >= 0:
                safe[key] = value
            elif key in flat_keys and type(value) is bool:
                safe[key] = value
        current = self.outcome()
        if current in {Outcome.PASSED, Outcome.BLOCKED}:
            return
        with self._db:
            if current is None:
                self._db.execute(
                    "INSERT INTO private_read_state VALUES (1, ?, ?)",
                    (outcome.value, json.dumps(safe, sort_keys=True)),
                )
            else:
                self._db.execute(
                    "UPDATE private_read_state SET outcome=?, evidence=? "
                    "WHERE singleton=1 AND outcome=?",
                    (outcome.value, json.dumps(safe, sort_keys=True),
                     Outcome.CLAIMED.value),
                )

    def block(self, **evidence: Any) -> None:
        self._terminal(Outcome.BLOCKED, evidence)

    def pass_(self, **evidence: Any) -> None:
        if self.outcome() != Outcome.CLAIMED:
            raise RuntimeError("one-shot claim missing")
        self._terminal(Outcome.PASSED, evidence)

    def close(self) -> None:
        self._db.close()


async def _await(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


def _mapping(value: Any, keys: set[str] | None = None) -> Mapping[str, Any]:
    if not isinstance(value, dict) or (keys is not None and set(value) != keys):
        raise ValueError("invalid schema")
    return value


def _list(value: Any) -> list[Any]:
    if not isinstance(value, list):
        raise ValueError("invalid schema")
    return value


def _decimal(value: Any) -> Decimal:
    if not isinstance(value, str):
        raise ValueError("invalid decimal")
    try:
        result = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("invalid decimal") from exc
    if not result.is_finite():
        raise ValueError("invalid decimal")
    return result


def _canonical_uint(value: Any, *, bits: int = 64) -> int:
    if (
        not isinstance(value, str) or not value
        or (value != "0" and value.startswith("0")) or not value.isascii()
        or not value.isdecimal()
    ):
        raise ValueError("invalid unsigned integer")
    result = int(value)
    if result >= 2**bits:
        raise ValueError("invalid unsigned integer")
    return result


def _address(value: Any) -> str:
    if (
        not isinstance(value, str) or len(value) != 42 or not value.startswith("0x")
        or any(character not in "0123456789abcdefABCDEF" for character in value[2:])
    ):
        raise ValueError("invalid address")
    return value.lower()


def _finite_time(value: Any) -> float:
    if type(value) not in {int, float}:
        raise ValueError("invalid time scalar")
    try:
        normalized = float(value)
    except (OverflowError, ValueError) as exc:
        raise ValueError("invalid time scalar") from exc
    if not math.isfinite(normalized):
        raise ValueError("invalid time scalar")
    return normalized


def _utc_timestamp(value: str) -> float:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError("invalid timestamp")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError("invalid timestamp") from exc
    return parsed.timestamp()


class PrivateReadPreflight:
    _SIGNER_KEYS = {"expiration", "label", "registered_at", "signer", "status"}
    _MARKET_KEYS = {
        "accumulated_funding", "active", "base_asset_symbol", "change_24h",
        "config", "current_funding_rate", "display_base_asset_symbol",
        "display_name", "funding_interval", "funding_rate_8h", "high_24h",
        "index_price", "last_cumulative_funding", "last_price", "low_24h",
        "mark_price", "market_id", "max_position_size", "next_funding_time",
        "open_interest", "post_only", "predicted_funding_rate",
        "quote_asset_symbol", "quote_volume_24h", "underlying",
    }
    _MARKET_CONFIG_KEYS = {
        "maintenance_margin_factor", "max_leverage", "min_order_size", "name",
        "open_interest_limit", "quote", "step_price", "step_size", "unlocked",
    }
    _REQUESTS = (
        ("/v1/system/config", ()),
        ("/v1/auth/eip712-domain", ()),
        ("/v1/auth/session-key-status", (("account", ACCOUNT), ("signer", SIGNER))),
        ("/v1/auth/signers", (("account", ACCOUNT),)),
        ("/v1/markets", (("force_refresh", "true"), ("market_ids", "1"))),
        ("/v1/orderbook", (("market_id", "1"),)),
        ("/v1/orders/open", (("account", ACCOUNT),)),
        ("/v1/account/position", (("account", ACCOUNT), ("market_id", "1"))),
        ("/v1/positions", (("account", ACCOUNT),)),
    )

    def __init__(
        self,
        store: PrivateReadStore,
        *,
        clock: Callable[[], float],
        public_get: Callable[..., Any],
        lifecycle_clear: Callable[[], bool],
    ) -> None:
        self._store = store
        self._clock = clock
        self._public_get = public_get
        self._lifecycle_clear = lifecycle_clear
        self._owner = object()
        self._barrier: _Barrier | None = None

    def _now(self) -> float:
        return _finite_time(self._clock())

    def _block(self, count: int = 0) -> None:
        self._barrier = None
        self._store.block(public_get_count=count)

    async def run_public_barrier(self) -> _Barrier | None:
        prior = self._store.outcome()
        if prior is not None:
            if prior == Outcome.CLAIMED:
                self._block(18)
            return None
        count = 0
        observations: list[float] = []
        try:
            if not self._store.start_public_attempt():
                raise ValueError("public attempt already consumed")
            if self._lifecycle_clear() is not True:
                raise ValueError("unresolved lifecycle")
            sweeps = []
            for _ in range(2):
                responses = {}
                for path, query in self._REQUESTS:
                    count += 1
                    response = await _await(self._public_get(path, query))
                    responses[path] = self._validate_response(path, query, response)
                    observations.append(_finite_time(response.observed_at))
                sweeps.append(self._validate_sweep(responses))
            if (
                sweeps[0] != sweeps[1] or self._lifecycle_clear() is not True
                or any(
                    self._now() - observed < 0
                    or self._now() - observed > MAX_AGE_SECONDS
                    for observed in observations
                )
            ):
                raise ValueError("inconsistent sweeps")
            self._barrier = _Barrier(self._owner, self._now())
            return self._barrier
        except Exception:
            self._block(count)
            return None

    def _validate_response(
        self, path: str, query: Sequence[tuple[str, str]], response: Any,
    ) -> Any:
        if not isinstance(response, HttpResponse):
            raise ValueError("invalid response")
        observed_at = _finite_time(response.observed_at)
        age = self._now() - observed_at
        if (
            type(response.status) is not int or response.status != 200
            or type(response.redirected) is not bool or response.redirected is not False
            or response.final_url != expected_url(path, query)
            or age < 0 or age > MAX_AGE_SECONDS
        ):
            raise ValueError("untrusted response")
        envelope = _mapping(response.body, {"data", "request_id"})
        if not isinstance(envelope["request_id"], str) or not envelope["request_id"]:
            raise ValueError("invalid envelope")
        return envelope["data"]

    def _validate_sweep(self, values: Mapping[str, Any]) -> tuple[Any, ...]:
        config = _mapping(values["/v1/system/config"])
        chain = _mapping(config.get("chain"))
        addresses = _mapping(config.get("addresses"))
        if (
            chain.get("chain_id") != str(CHAIN_ID)
            or _address(addresses.get("auth")) != AUTHORIZATION
            or _address(addresses.get("router")) != ROUTER
            or config.get("is_maintenance_mode") is not False
        ):
            raise ValueError("config mismatch")

        domain = _mapping(values["/v1/auth/eip712-domain"])
        if (
            set(domain) != {"name", "version", "chain_id", "verifying_contract"}
            or domain.get("name") != DOMAIN_NAME
            or domain.get("version") != DOMAIN_VERSION
            or domain.get("chain_id") != str(CHAIN_ID)
            or _address(domain.get("verifying_contract")) != AUTHORIZATION
        ):
            raise ValueError("domain mismatch")

        status = _mapping(
            values["/v1/auth/session-key-status"],
            {"status", "status_description"},
        )
        if (
            type(status["status"]) is not int or status["status"] != 1
            or status["status_description"] != "Active"
        ):
            raise ValueError("signer inactive")
        now = self._now()
        self._validate_signers(values["/v1/auth/signers"], now)
        self._validate_market(values["/v1/markets"], now)
        bids, asks = self._validate_book(values["/v1/orderbook"], now)
        if (
            any(left[0] <= right[0] for left, right in zip(bids, bids[1:]))
            or any(left[0] >= right[0] for left, right in zip(asks, asks[1:]))
        ):
            raise ValueError("unordered book")
        best_bid, best_ask = bids[0][0], asks[0][0]
        if best_bid >= best_ask or best_ask > best_bid * (Decimal(1) + MAX_BOUND_FRACTION):
            raise ValueError("crossed book")
        adverse_ask = (
            (best_ask * (Decimal(1) + MAX_BOUND_FRACTION) / TICK)
            .to_integral_value(rounding=ROUND_CEILING) * TICK
        )
        if MINIMUM * adverse_ask > MAX_NOTIONAL:
            raise ValueError("notional cap")
        lower = best_bid * (Decimal(1) - MAX_BOUND_FRACTION)
        upper = best_ask * (Decimal(1) + MAX_BOUND_FRACTION)
        bid_depth = sum(q for p, q in bids if p >= lower)
        ask_depth = sum(q for p, q in asks if p <= upper)
        if bid_depth < MINIMUM or ask_depth < MINIMUM:
            raise ValueError("insufficient bounded depth")

        orders = _mapping(values["/v1/orders/open"])
        if _list(orders.get("orders")):
            raise ValueError("open orders")
        point = _mapping(values["/v1/account/position"])
        position = _mapping(point.get("position"))
        if position.get("size") != "0":
            raise ValueError("nonflat point position")
        positions = _mapping(values["/v1/positions"])
        if _list(positions.get("positions")):
            raise ValueError("nonflat positions")
        return (best_bid, best_ask, tuple(bids), tuple(asks))

    @staticmethod
    def _validate_signers(value: Any, now: float) -> tuple[str, int]:
        signer_data = _mapping(value, {"signers"})
        signers = _list(signer_data["signers"])
        if len(signers) != 1:
            raise ValueError("signer mismatch")
        signer = _mapping(signers[0], PrivateReadPreflight._SIGNER_KEYS)
        if (
            _address(signer["signer"]) != SIGNER
            or signer["label"] != SIGNER_LABEL
            or signer["status"] != "Active"
            or signer["registered_at"] != REGISTERED_AT
            or signer["expiration"] != SIGNER_EXPIRATION
        ):
            raise ValueError("signer mismatch")
        registered_at = _utc_timestamp(signer["registered_at"])
        expiration = _canonical_uint(signer["expiration"])
        if not registered_at <= _finite_time(now) < expiration:
            raise ValueError("signer outside active interval")
        return signer["signer"], expiration

    @staticmethod
    def _validate_market(value: Any, now: float) -> Mapping[str, Any]:
        del now
        market_data = _mapping(value, {"cached_at", "markets"})
        if _canonical_uint(market_data["cached_at"]) == 0:
            raise ValueError("invalid market cache time")
        markets = _list(market_data["markets"])
        if len(markets) != 1:
            raise ValueError("market mismatch")
        market = _mapping(markets[0], PrivateReadPreflight._MARKET_KEYS)
        config = _mapping(market["config"], PrivateReadPreflight._MARKET_CONFIG_KEYS)
        string_fields = PrivateReadPreflight._MARKET_KEYS - {
            "active", "config", "post_only",
        }
        config_string_fields = PrivateReadPreflight._MARKET_CONFIG_KEYS - {"unlocked"}
        if (
            any(not isinstance(market[field], str) for field in string_fields)
            or any(not isinstance(config[field], str) for field in config_string_fields)
            or type(market["active"]) is not bool or market["active"] is not True
            or type(market["post_only"]) is not bool
            or type(config["unlocked"]) is not bool or config["unlocked"] is not True
        ):
            raise ValueError("market field type mismatch")
        full_symbol = "BTC/USDC"
        if (
            market["market_id"] != "1"
            or market["quote_asset_symbol"] != "USDC" or not config["quote"]
            or any(market[field] != full_symbol for field in {
                "base_asset_symbol", "display_base_asset_symbol", "display_name",
                "underlying",
            })
            or config["name"] != full_symbol
            or config["step_size"] != "0.000001"
            or config["step_price"] != "0.1"
            or config["min_order_size"] != "0.0001"
        ):
            raise ValueError("market mismatch")
        signed_decimal_fields = {
            "accumulated_funding", "change_24h", "current_funding_rate",
            "funding_rate_8h", "last_cumulative_funding", "predicted_funding_rate",
        }
        positive_decimal_fields = {
            "high_24h", "index_price", "last_price", "low_24h", "mark_price",
            "max_position_size",
        }
        nonnegative_decimal_fields = {"open_interest", "quote_volume_24h"}
        for field in signed_decimal_fields:
            _decimal(market[field])
        if any(_decimal(market[field]) <= 0 for field in positive_decimal_fields):
            raise ValueError("nonpositive market field")
        if any(_decimal(market[field]) < 0 for field in nonnegative_decimal_fields):
            raise ValueError("negative market field")
        if (
            _canonical_uint(market["funding_interval"]) == 0
            or _canonical_uint(market["next_funding_time"]) == 0
        ):
            raise ValueError("invalid market time")
        for field in {
            "maintenance_margin_factor", "max_leverage", "min_order_size",
            "open_interest_limit", "step_price", "step_size",
        }:
            if _decimal(config[field]) <= 0:
                raise ValueError("nonpositive market config")
        return market

    @staticmethod
    def _validate_book(
        value: Any, now: float,
    ) -> tuple[tuple[tuple[Decimal, Decimal], ...], tuple[tuple[Decimal, Decimal], ...]]:
        del now
        book = _mapping(
            value, {"asks", "bids", "market_id", "total_asks", "total_bids"},
        )
        if book["market_id"] != "1":
            raise ValueError("book identity mismatch")
        bids = PrivateReadPreflight._levels(book["bids"])
        asks = PrivateReadPreflight._levels(book["asks"])
        if (
            _canonical_uint(book["total_bids"]) < len(bids)
            or _canonical_uint(book["total_asks"]) < len(asks)
        ):
            raise ValueError("book total mismatch")
        return bids, asks

    @staticmethod
    def _levels(value: Any) -> tuple[tuple[Decimal, Decimal], ...]:
        levels = _list(value)
        parsed = []
        for raw in levels:
            level = _mapping(raw, {"order_count", "price", "quantity"})
            if (
                isinstance(level["order_count"], bool)
                or not isinstance(level["order_count"], int)
                or level["order_count"] <= 0
            ):
                raise ValueError("invalid order count")
            price, quantity = _decimal(level["price"]), _decimal(level["quantity"])
            if price <= 0 or quantity <= 0 or price % TICK or quantity % STEP:
                raise ValueError("invalid book grid")
            parsed.append((price, quantity))
        if not parsed:
            raise ValueError("empty book")
        return tuple(parsed)

    async def run_private_proof(
        self,
        barrier: _Barrier,
        *,
        signer_loader: Callable[[], SyntheticCredential],
        nonce_get: Callable[..., Any],
        sign_register_v2: Callable[..., str],
        private_exchange: Callable[..., Any],
    ) -> PreflightResult:
        try:
            barrier_age = self._now() - _finite_time(barrier.issued_at)
            gate_valid = (
                isinstance(barrier, _Barrier)
                and barrier is self._barrier and barrier.owner is self._owner
                and 0 <= barrier_age <= MAX_AGE_SECONDS
                and self._store.outcome() is None
                and self._lifecycle_clear() is True
            )
        except Exception:
            gate_valid = False
        if not gate_valid:
            self._block(18)
            return self._store.result()  # type: ignore[return-value]
        self._barrier = None
        private_started = self._now()
        if not self._store._insert_claim():
            self._block(18)
            return self._store.result()  # type: ignore[return-value]

        credential: SyntheticCredential | None = None
        counts = {
            "public_get_count": 18, "private_nonce_count": 0,
            "signature_count": 0, "auth_send_count": 0,
            "orders_snapshot_count": 0, "positions_snapshot_count": 0,
            "public_flat": True, "private_flat": False,
        }
        try:
            credential = signer_loader()
            if (
                not isinstance(credential, SyntheticCredential)
                or _address(credential.signer) != SIGNER
            ):
                raise ValueError("signer mismatch")

            nonce_path = "/v1/auth/nonce"
            nonce_query = (("account", ACCOUNT),)
            counts["private_nonce_count"] = 1
            nonce_response = await _await(nonce_get(nonce_path, nonce_query))
            nonce_data = self._validate_response(nonce_path, nonce_query, nonce_response)
            nonce = self._nonce(_mapping(nonce_data, {"nonce"})["nonce"])
            typed_data = self._typed_data(nonce)

            counts["signature_count"] = 1
            signature = sign_register_v2(credential, typed_data)
            if (
                not isinstance(signature, str) or not signature.startswith("0x")
                or len(signature) != 132
            ):
                raise ValueError("invalid signature")
            int(signature[2:], 16)
            frame = {
                "method": "auth_v2",
                "params": {
                    "account": ACCOUNT, "signer": SIGNER,
                    "message": "sign in with RISEx", "nonce": nonce,
                    "expiration": int(self._now()) + 365 * 24 * 60 * 60,
                    "signature": signature,
                },
            }
            counts["auth_send_count"] = 1
            outbound_plan = (
                frame,
                {"method": "subscribe", "params": {"channel": "orders"}},
                {"method": "subscribe", "params": {"channel": "positions"}},
            )
            frames = await _await(private_exchange(WS_ORIGIN, outbound_plan))
            self._validate_private_frames(frames, self._now())
            private_age = self._now() - private_started
            if private_age < 0 or private_age > MAX_AGE_SECONDS:
                raise ValueError("private proof stale")
            counts["orders_snapshot_count"] = 1
            counts["positions_snapshot_count"] = 1
            counts["private_flat"] = True
            self._store.pass_(**counts)
        except Exception:
            self._store.block(**counts)
        finally:
            if credential is not None:
                credential.close()
        return self._store.result()  # type: ignore[return-value]

    @staticmethod
    def _nonce(value: Any) -> str:
        if not isinstance(value, str) or not value:
            raise ValueError("invalid authentication value")
        text = value[2:] if value.startswith("0x") else value
        if (
            not text or len(text) > 64
            or any(character not in "0123456789abcdefABCDEF" for character in text)
        ):
            raise ValueError("invalid authentication value")
        number = int(text, 16)
        if number >= 2 ** 256:
            raise ValueError("invalid authentication value")
        return "0x" + text.lower().zfill(4)

    @staticmethod
    def _typed_data(nonce: str) -> dict[str, Any]:
        return {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                "RegisterV2": [
                    {"name": "signer", "type": "address"},
                    {"name": "message", "type": "string"},
                    {"name": "nonce", "type": "uint256"},
                ],
            },
            "primaryType": "RegisterV2",
            "domain": {
                "name": DOMAIN_NAME, "version": DOMAIN_VERSION,
                "chainId": CHAIN_ID, "verifyingContract": AUTHORIZATION,
            },
            "message": {
                "signer": SIGNER, "message": "sign in with RISEx", "nonce": nonce,
            },
        }

    @staticmethod
    def _validate_private_frames(frames: Any, now: float) -> None:
        now = _finite_time(now)
        if not isinstance(frames, (tuple, list)) or len(frames) != 3:
            raise ValueError("invalid private frames")
        auth, orders, positions = frames
        if _mapping(auth, {"method", "status"}) != {
            "method": "auth_v2", "status": "success",
        }:
            raise ValueError("authentication failed")
        expected_orders = _mapping(
            orders,
            {"method", "channel", "type", "data", "order_count", "worker_timestamp"},
        )
        expected_positions = _mapping(
            positions,
            {"method", "channel", "type", "data", "position_count", "worker_timestamp"},
        )
        if (
            expected_orders.get("method") != "snapshot"
            or expected_orders.get("channel") != "orders"
            or expected_orders.get("type") != "snapshot"
            or expected_positions.get("method") != "snapshot"
            or expected_positions.get("channel") != "positions"
            or expected_positions.get("type") != "snapshot"
        ):
            raise ValueError("private identity mismatch")
        order_data = _list(expected_orders.get("data"))
        position_data = _list(expected_positions.get("data"))
        order_count = expected_orders.get("order_count")
        position_count = expected_positions.get("position_count")
        if (
            isinstance(order_count, bool) or not isinstance(order_count, int)
            or isinstance(position_count, bool) or not isinstance(position_count, int)
            or order_count != len(order_data) or position_count != len(position_data)
        ):
            raise ValueError("private count mismatch")
        for snapshot in (expected_orders, expected_positions):
            worker_timestamp = snapshot.get("worker_timestamp")
            if (
                not isinstance(worker_timestamp, str) or not worker_timestamp.isdecimal()
                or int(worker_timestamp) <= 0
            ):
                raise ValueError("invalid worker timestamp")
            age_ns = Decimal(str(now)) * Decimal(1_000_000_000) - Decimal(
                worker_timestamp
            )
            if age_ns < 0 or age_ns > Decimal(MAX_AGE_SECONDS * 1_000_000_000):
                raise ValueError("stale worker timestamp")
        if order_data or position_data:
            raise ValueError("private state not flat")
