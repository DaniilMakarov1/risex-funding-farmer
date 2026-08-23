"""Opt-in, injected-only RISEx private-read preflight.

This module deliberately owns no transport and is not imported by normal startup.
It proves a narrowly bounded fixture contract and exposes no executing operation.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from enum import Enum
import inspect
import json
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
SIGNER_EXPIRATION = "2026-09-22T15:46:50Z"

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
        safe = {
            key: value for key, value in evidence.items()
            if key in {
                "public_get_count", "private_nonce_count", "signature_count",
                "auth_send_count", "orders_snapshot_count",
                "positions_snapshot_count", "public_flat", "private_flat",
            } and isinstance(value, (bool, int))
        }
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


def _address(value: Any) -> str:
    if (
        not isinstance(value, str) or len(value) != 42 or not value.startswith("0x")
        or any(character not in "0123456789abcdefABCDEF" for character in value[2:])
    ):
        raise ValueError("invalid address")
    return value.lower()


class PrivateReadPreflight:
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
            if not self._lifecycle_clear():
                raise ValueError("unresolved lifecycle")
            sweeps = []
            for _ in range(2):
                responses = {}
                for path, query in self._REQUESTS:
                    count += 1
                    response = await _await(self._public_get(path, query))
                    responses[path] = self._validate_response(path, query, response)
                    observations.append(response.observed_at)
                sweeps.append(self._validate_sweep(responses))
            if (
                sweeps[0] != sweeps[1] or not self._lifecycle_clear()
                or any(
                    self._clock() - observed < 0
                    or self._clock() - observed > MAX_AGE_SECONDS
                    for observed in observations
                )
            ):
                raise ValueError("inconsistent sweeps")
            self._barrier = _Barrier(self._owner, self._clock())
            return self._barrier
        except Exception:
            self._block(count)
            return None

    def _validate_response(
        self, path: str, query: Sequence[tuple[str, str]], response: Any,
    ) -> Any:
        if not isinstance(response, HttpResponse):
            raise ValueError("invalid response")
        age = self._clock() - response.observed_at
        if (
            response.status != 200 or response.redirected
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

        status = _mapping(values["/v1/auth/session-key-status"])
        if status != {"status": 1, "status_description": "Active"}:
            raise ValueError("signer inactive")
        signer_data = _mapping(values["/v1/auth/signers"])
        signers = _list(signer_data.get("signers"))
        expected_signer = {
            "signer": SIGNER, "status": "Active", "registered_at": REGISTERED_AT,
            "expiration": SIGNER_EXPIRATION,
        }
        if len(signers) != 1:
            raise ValueError("signer mismatch")
        signer_row = _mapping(signers[0])
        if (
            set(signer_row) != set(expected_signer)
            or _address(signer_row.get("signer")) != SIGNER
            or {key: signer_row.get(key) for key in expected_signer if key != "signer"}
            != {key: value for key, value in expected_signer.items() if key != "signer"}
        ):
            raise ValueError("signer mismatch")

        market_data = _mapping(values["/v1/markets"])
        markets = _list(market_data.get("markets"))
        if len(markets) != 1:
            raise ValueError("market mismatch")
        market = _mapping(markets[0])
        market_config = _mapping(market.get("config"))
        if (
            market.get("market_id") != "1" or market.get("active") is not True
            or market.get("base_asset_symbol") != "BTC"
            or market.get("quote_asset_symbol") != "USDC"
            or market_config != {
                "name": "BTC/USDC", "unlocked": True,
                "step_size": "0.000001", "step_price": "0.1",
                "min_order_size": "0.0001",
            }
        ):
            raise ValueError("market mismatch")

        book = _mapping(values["/v1/orderbook"])
        if book.get("market_id") != "1":
            raise ValueError("book identity mismatch")
        bids = self._levels(book.get("bids"))
        asks = self._levels(book.get("asks"))
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
    def _levels(value: Any) -> tuple[tuple[Decimal, Decimal], ...]:
        levels = _list(value)
        parsed = []
        for raw in levels:
            level = _mapping(raw, {"price", "quantity"})
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
        if (
            barrier is not self._barrier or barrier.owner is not self._owner
            or self._clock() - barrier.issued_at > MAX_AGE_SECONDS
            or self._store.outcome() is not None
            or not self._lifecycle_clear()
        ):
            self._block(18)
            return self._store.result()  # type: ignore[return-value]
        self._barrier = None
        private_started = self._clock()
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
                    "expiration": int(self._clock()) + 60,
                    "signature": signature,
                },
            }
            counts["auth_send_count"] = 1
            frames = await _await(private_exchange(WS_ORIGIN, frame))
            self._validate_private_frames(frames)
            private_age = self._clock() - private_started
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
    def _validate_private_frames(frames: Any) -> None:
        if not isinstance(frames, (tuple, list)) or len(frames) != 3:
            raise ValueError("invalid private frames")
        auth, orders, positions = frames
        if _mapping(auth, {"method", "status"}) != {
            "method": "auth_v2", "status": "success",
        }:
            raise ValueError("authentication failed")
        expected_orders = _mapping(orders, {"method", "channel", "account", "data"})
        expected_positions = _mapping(
            positions, {"method", "channel", "account", "data"},
        )
        if (
            expected_orders.get("method") != "snapshot"
            or expected_orders.get("channel") != "orders"
            or _address(expected_orders.get("account")) != ACCOUNT
            or expected_positions.get("method") != "snapshot"
            or expected_positions.get("channel") != "positions"
            or _address(expected_positions.get("account")) != ACCOUNT
        ):
            raise ValueError("private identity mismatch")
        order_data = _mapping(expected_orders.get("data"), {"orders"})
        position_data = _mapping(expected_positions.get("data"), {"positions"})
        if _list(order_data["orders"]) or _list(position_data["positions"]):
            raise ValueError("private state not flat")
