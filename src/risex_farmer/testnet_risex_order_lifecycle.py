"""Fixture-only RISEx bounded lifecycle contract.

This module deliberately has no network client, credential implementation, CLI, or
normal Farmer import.  Its callables are synthetic boundaries for deterministic
tests; enabling real signing or transport requires a later governance gate.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from enum import Enum
import hashlib
import json
from pathlib import Path
import sqlite3
from typing import Any, Callable
import uuid


OFFICIAL_HOST = "api.testnet.rise.trade"
OFFICIAL_CHAIN_ID = 11_155_931
OFFICIAL_DOMAIN_NAME = "RISEx"
OFFICIAL_DOMAIN_VERSION = "1"
MAX_NOTIONAL_USD = Decimal("500")
BOUND_BPS = Decimal("30")
MAX_AGE_SECONDS = 5
MAX_PERMIT_SECONDS = 60
HEADER_FLAGS = 0x05
PLACE_ACTION = "RISE_PERPS_PLACE_ORDER_V1"
VERIFY_WITNESS_TYPE = (
    "VerifyWitness(address account,address target,bytes32 hash,uint48 nonceAnchor,"
    "uint8 nonceBitmap,uint32 deadline)"
)


class LifecycleSafetyError(RuntimeError):
    """A bounded, identity-free fail-closed rejection."""


class Outcome(str, Enum):
    ACTIVE = "ACTIVE"
    COMPLETED_NO_FILL_FLAT = "COMPLETED_NO_FILL_FLAT"
    SUCCESS_CLOSED_FLAT = "SUCCESS_CLOSED_FLAT"
    FAILED_HALTED_MANUAL_RECOVERY = "FAILED_HALTED_MANUAL_RECOVERY"


@dataclass(frozen=True)
class MarketState:
    host: str
    chain_id: int
    domain_name: str
    domain_version: str
    router: str
    authorization: str
    market_id: int
    symbol: str
    active: bool
    unlocked: bool
    tick: Decimal
    step: Decimal
    minimum: Decimal
    observed_at: int


@dataclass(frozen=True)
class AccountState:
    account: str
    signer: str
    signer_status: str
    position: Decimal
    open_order_ids: tuple[str, ...]
    repeated_open_order_ids: tuple[str, ...] = ()
    repeated_position: Decimal | None = None
    unexplained: bool = False
    observed_at: int = 0


@dataclass(frozen=True)
class BBO:
    bid: Decimal
    ask: Decimal
    bid_depth: Decimal
    ask_depth: Decimal
    observed_at: int


@dataclass(frozen=True)
class Preflight:
    market: MarketState
    account: AccountState
    bbo: BBO
    size: Decimal
    buy_bound: Decimal


@dataclass(frozen=True)
class Intent:
    intent_id: str
    ordinal: int
    kind: str
    client_order_id: int
    nonce: int
    nonce_bitmap: int
    payload_digest: str
    bbo_digest: str
    state: str
    side: str
    order_type: str
    time_in_force: str
    reduce_only: bool
    post_only: bool
    market_id: int
    size: Decimal
    size_steps: int
    price: Decimal
    price_ticks: int
    source_position: Decimal
    expires_at: int
    dispatch_count: int = 0
    order_id: str | None = None
    reconciled: bool = False


@dataclass(frozen=True)
class SyntheticSigner:
    signer: str
    fixture_only: bool = True


@dataclass(frozen=True)
class Evidence:
    account: str
    signer: str
    signer_status: str
    order_id: str | None
    client_order_id: int
    terminal: bool
    filled_size: Decimal
    position: Decimal
    open_order_ids: tuple[str, ...]
    observed_at: int
    by_id_order_id: str | None = None
    history_order_ids: tuple[str, ...] = ()
    history_client_order_ids: tuple[int, ...] = ()
    trade_order_ids: tuple[str, ...] = ()
    trade_client_order_ids: tuple[int, ...] = ()

def _keccak(value: bytes) -> bytes:
    try:
        from eth_utils import keccak
    except Exception:
        raise LifecycleSafetyError("RISEx encoding dependency unavailable") from None
    return bytes(keccak(value))


def _uint(value: int, bits: int, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value < 2**bits:
        raise LifecycleSafetyError(f"RISEx {name} width rejected")
    return value


def _valid_order_id(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _address(value: str) -> str:
    if not isinstance(value, str) or len(value) != 42 or not value.startswith("0x"):
        raise LifecycleSafetyError("RISEx address rejected")
    try:
        int(value[2:], 16)
    except ValueError:
        raise LifecycleSafetyError("RISEx address rejected") from None
    return value


def _abi_word(value: int | bytes, bits: int | None = None) -> bytes:
    if isinstance(value, bytes):
        if len(value) != 32:
            raise LifecycleSafetyError("RISEx bytes32 rejected")
        return value
    assert bits is not None
    return _uint(value, bits, "ABI word").to_bytes(32, "big")


def pack_order_data(
    *, market_id: int, size_steps: int, price_ticks: int, side: str,
    post_only: bool, reduce_only: bool, order_type: str, time_in_force: str,
) -> int:
    if not isinstance(post_only, bool) or not isinstance(reduce_only, bool):
        raise LifecycleSafetyError("RISEx order flag rejected")
    side_code = {"BUY": 0, "SELL": 1}.get(side)
    type_code = {"MARKET": 0, "LIMIT": 1}.get(order_type)
    tif_code = {"GTC": 0, "GTT": 1, "FOK": 2, "IOC": 3}.get(time_in_force)
    if side_code is None or type_code is None or tif_code is None:
        raise LifecycleSafetyError("RISEx order enum rejected")
    market_id = _uint(market_id, 16, "market_id")
    size_steps = _uint(size_steps, 32, "size_steps")
    price_ticks = _uint(price_ticks, 24, "price_ticks")
    if size_steps == 0 or price_ticks == 0:
        raise LifecycleSafetyError("RISEx zero order field rejected")
    flags = (
        side_code | (int(post_only) << 1) | (int(reduce_only) << 2)
        | (type_code << 5) | (tif_code << 6)
    )
    value = (
        (market_id << 70) | (size_steps << 38) | (price_ticks << 14)
        | (flags << 6) | (1 << 1)
    )
    return _uint(value, 88, "order_data")


def encode_place_action(
    *, order_data: int, client_order_id: int, builder_id: int = 0,
    ttl_units: int = 0,
) -> tuple[bytes, bytes]:
    if builder_id != 0 or ttl_units != 0:
        raise LifecycleSafetyError("RISEx fixed header rejected")
    client_order_id = _uint(client_order_id, 64, "client_order_id")
    if client_order_id == 0:
        raise LifecycleSafetyError("RISEx client_order_id rejected")
    order_data = _uint(order_data, 88, "order_data")
    if order_data >= 2**86:
        raise LifecycleSafetyError("RISEx reserved order_data bits rejected")
    encoded = b"".join((
        _abi_word(_keccak(PLACE_ACTION.encode())),
        _abi_word(HEADER_FLAGS, 8),
        _abi_word(order_data, 88),
        _abi_word(builder_id, 16),
        _abi_word(client_order_id, 64),
        _abi_word(ttl_units, 16),
    ))
    return encoded, _keccak(encoded)


def verify_witness_typed_data(
    *, account: str, market: MarketState, action_hash: bytes,
    nonce_anchor: int, nonce_bitmap: int, deadline: int,
) -> dict[str, Any]:
    nonce_anchor = _uint(nonce_anchor, 48, "nonce_anchor")
    nonce_bitmap = _uint(nonce_bitmap, 8, "nonce_bitmap")
    if nonce_bitmap > 207:
        raise LifecycleSafetyError("RISEx nonce bitmap rejected")
    deadline = _uint(deadline, 32, "deadline")
    account = _address(account)
    _address(market.router)
    _address(market.authorization)
    if len(action_hash) != 32:
        raise LifecycleSafetyError("RISEx action hash rejected")
    return {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            "VerifyWitness": [
                {"name": "account", "type": "address"},
                {"name": "target", "type": "address"},
                {"name": "hash", "type": "bytes32"},
                {"name": "nonceAnchor", "type": "uint48"},
                {"name": "nonceBitmap", "type": "uint8"},
                {"name": "deadline", "type": "uint32"},
            ],
        },
        "primaryType": "VerifyWitness",
        "domain": {
            "name": market.domain_name, "version": market.domain_version,
            "chainId": market.chain_id, "verifyingContract": market.authorization,
        },
        "message": {
            "account": account, "target": market.router,
            "hash": "0x" + action_hash.hex(), "nonceAnchor": nonce_anchor,
            "nonceBitmap": nonce_bitmap, "deadline": deadline,
        },
    }


_SCHEMA = """
CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    ordinal INTEGER NOT NULL,
    kind TEXT NOT NULL,
    client_order_id TEXT NOT NULL UNIQUE,
    nonce INTEGER NOT NULL,
    nonce_bitmap INTEGER NOT NULL,
    payload_digest TEXT NOT NULL UNIQUE,
    bbo_digest TEXT NOT NULL,
    state TEXT NOT NULL,
    side TEXT NOT NULL,
    order_type TEXT NOT NULL,
    time_in_force TEXT NOT NULL,
    reduce_only INTEGER NOT NULL,
    post_only INTEGER NOT NULL,
    market_id INTEGER NOT NULL,
    size TEXT NOT NULL,
    size_steps INTEGER NOT NULL,
    price TEXT NOT NULL,
    price_ticks INTEGER NOT NULL,
    source_position TEXT NOT NULL,
    expires_at INTEGER NOT NULL,
    dispatch_count INTEGER NOT NULL DEFAULT 0,
    order_id TEXT UNIQUE,
    reconciled INTEGER NOT NULL DEFAULT 0,
    UNIQUE(nonce, nonce_bitmap)
);
CREATE TABLE IF NOT EXISTS cancels (
    order_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    dispatch_count INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS terminal (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class DurableIntentStore:
    """Small dedicated journal; it stores canonical digests, never payloads."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.connection = sqlite3.connect(self.path)
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.executescript(_SCHEMA)
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    def _add(self, intent: Intent) -> None:
        try:
            with self.connection:
                self.connection.execute(
                    "INSERT INTO intents VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        intent.intent_id, intent.ordinal, intent.kind,
                        str(intent.client_order_id), intent.nonce, intent.nonce_bitmap,
                        intent.payload_digest, intent.bbo_digest, intent.state,
                        intent.side, intent.order_type,
                        intent.time_in_force, int(intent.reduce_only),
                        int(intent.post_only), intent.market_id, str(intent.size),
                        intent.size_steps, str(intent.price), intent.price_ticks,
                        str(intent.source_position), intent.expires_at,
                        intent.dispatch_count, intent.order_id, int(intent.reconciled),
                    ),
                )
        except sqlite3.IntegrityError:
            raise LifecycleSafetyError("RISEx lifecycle identity rejected") from None

    def get(self, intent_id: str) -> Intent:
        row = self.connection.execute(
            "SELECT * FROM intents WHERE intent_id = ?", (intent_id,)
        ).fetchone()
        if row is None:
            raise LifecycleSafetyError("RISEx lifecycle intent rejected")
        return _intent(row)

    def all(self) -> list[Intent]:
        return [_intent(row) for row in self.connection.execute("SELECT * FROM intents ORDER BY ordinal")]

    def _update_state(self, intent_id: str, state: str, *, increment: bool = False, order_id: str | None = None) -> None:
        with self.connection:
            if increment:
                self.connection.execute(
                    "UPDATE intents SET state=?, dispatch_count=dispatch_count+1 WHERE intent_id=?",
                    (state, intent_id),
                )
            elif order_id is not None:
                self.connection.execute(
                    "UPDATE intents SET state=?, order_id=? WHERE intent_id=?",
                    (state, order_id, intent_id),
                )
            else:
                self.connection.execute("UPDATE intents SET state=? WHERE intent_id=?", (state, intent_id))

    def _reconcile_intent(
        self, intent_id: str, order_id: str | None, resulting_position: Decimal,
    ) -> None:
        with self.connection:
            self.connection.execute(
                "UPDATE intents SET state='TERMINAL', order_id=COALESCE(order_id, ?), "
                "reconciled=1 WHERE intent_id=?",
                (order_id, intent_id),
            )
            self.connection.execute(
                "INSERT INTO terminal VALUES (?, ?)",
                (f"position:{intent_id}", str(resulting_position)),
            )

    def latest_reconciled_position(self) -> Decimal | None:
        row = self.connection.execute(
            "SELECT terminal.value FROM intents LEFT JOIN terminal "
            "ON terminal.key = 'position:' || intents.intent_id "
            "WHERE intents.state='TERMINAL' AND intents.reconciled=1 "
            "ORDER BY intents.ordinal DESC LIMIT 1"
        ).fetchone()
        if row is None or row[0] is None:
            return None
        try:
            return Decimal(str(row[0]))
        except (InvalidOperation, ValueError):
            return None

    def _record_open_known(self, intent_id: str, order_id: str) -> None:
        with self.connection:
            self.connection.execute(
                "UPDATE intents SET state='OPEN_KNOWN', order_id=? WHERE intent_id=?",
                (order_id, intent_id),
            )

    def known_order(self, order_id: str) -> bool:
        return self.connection.execute(
            "SELECT 1 FROM intents WHERE order_id=?", (order_id,)
        ).fetchone() is not None

    def order_state(self, order_id: str) -> str | None:
        row = self.connection.execute(
            "SELECT state FROM intents WHERE order_id=?", (order_id,)
        ).fetchone()
        return None if row is None else str(row[0])

    def _reserve_cancel(self, order_id: str) -> None:
        try:
            with self.connection:
                self.connection.execute(
                    "INSERT INTO cancels VALUES (?, 'DISPATCHING', 1)", (order_id,)
                )
        except sqlite3.IntegrityError:
            raise LifecycleSafetyError("RISEx cancel replay rejected") from None

    def _set_cancel_state(self, order_id: str, state: str) -> None:
        with self.connection:
            self.connection.execute("UPDATE cancels SET state=? WHERE order_id=?", (state, order_id))

    def cancel_count(self, order_id: str) -> int:
        row = self.connection.execute(
            "SELECT dispatch_count FROM cancels WHERE order_id=?", (order_id,)
        ).fetchone()
        return 0 if row is None else int(row[0])

    def cancel_states(self) -> list[str]:
        return [row[0] for row in self.connection.execute("SELECT state FROM cancels")]

    def persist_outcome(self, outcome: Outcome) -> None:
        with self.connection:
            self.connection.execute(
                "INSERT OR REPLACE INTO terminal VALUES ('outcome', ?)", (outcome.value,)
            )

    def load_outcome(self) -> Outcome:
        row = self.connection.execute(
            "SELECT value FROM terminal WHERE key='outcome'"
        ).fetchone()
        return Outcome.ACTIVE if row is None else Outcome(row[0])

    def _set_opening_fill_observed(self) -> None:
        with self.connection:
            self.connection.execute(
                "INSERT OR REPLACE INTO terminal VALUES ('opening_fill', '1')"
            )

    def opening_fill_observed(self) -> bool:
        return self.connection.execute(
            "SELECT 1 FROM terminal WHERE key='opening_fill'"
        ).fetchone() is not None

    def _record_snapshot_window(
        self, intent_id: str, valid_from: int, valid_until: int,
    ) -> None:
        with self.connection:
            self.connection.execute(
                "INSERT INTO terminal VALUES (?, ?)",
                (f"snapshot:{intent_id}", f"{valid_from}:{valid_until}"),
            )

    def snapshot_window(self, intent_id: str) -> tuple[int, int] | None:
        row = self.connection.execute(
            "SELECT value FROM terminal WHERE key=?", (f"snapshot:{intent_id}",)
        ).fetchone()
        if row is None:
            return None
        valid_from, valid_until = str(row[0]).split(":", 1)
        return int(valid_from), int(valid_until)

    def _bind_identities(self, account: str, signer: str) -> None:
        with self.connection:
            rows = dict(self.connection.execute(
                "SELECT key, value FROM terminal WHERE key IN ('account', 'signer')"
            ))
            if rows and rows != {"account": account, "signer": signer}:
                raise LifecycleSafetyError("RISEx lifecycle identity rejected")
            self.connection.execute(
                "INSERT OR IGNORE INTO terminal VALUES ('account', ?)", (account,)
            )
            self.connection.execute(
                "INSERT OR IGNORE INTO terminal VALUES ('signer', ?)", (signer,)
            )

    def redacted_evidence(self) -> dict[str, Any]:
        rows = self.all()
        return {
            "intent_count": len(rows),
            "states": [row.state for row in rows],
            "identities": ["[REDACTED]" for _ in rows],
        }


def _intent(row: tuple[Any, ...]) -> Intent:
    return Intent(
        intent_id=row[0], ordinal=row[1], kind=row[2], client_order_id=int(row[3]),
        nonce=row[4], nonce_bitmap=row[5], payload_digest=row[6],
        bbo_digest=row[7], state=row[8], side=row[9], order_type=row[10],
        time_in_force=row[11], reduce_only=bool(row[12]), post_only=bool(row[13]),
        market_id=row[14], size=Decimal(row[15]), size_steps=row[16],
        price=Decimal(row[17]), price_ticks=row[18],
        source_position=Decimal(row[19]), expires_at=row[20],
        dispatch_count=row[21], order_id=row[22], reconciled=bool(row[23]),
    )


def _aligned(value: Decimal, grid: Decimal) -> bool:
    return grid > 0 and value > 0 and value % grid == 0


def _bound(price: Decimal, tick: Decimal, side: str) -> Decimal:
    factor = Decimal("1") + (BOUND_BPS / Decimal("10000") if side == "BUY" else -BOUND_BPS / Decimal("10000"))
    rounding = ROUND_CEILING if side == "BUY" else ROUND_FLOOR
    return (price * factor / tick).to_integral_value(rounding=rounding) * tick


def _evidence_digest(fields: dict[str, Any]) -> str:
    canonical = json.dumps(fields, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


class Lifecycle:
    def __init__(
        self, store: DurableIntentStore, *, now: Callable[[], int], router: str,
        authorization: str, expected_account: str, expected_signer: str,
    ) -> None:
        self.store = store
        self._now = now
        self._router = _address(router).lower()
        self._authorization = _address(authorization).lower()
        self._expected_account = _address(expected_account).lower()
        self._expected_signer = _address(expected_signer).lower()
        store._bind_identities(self._expected_account, self._expected_signer)
        self.outcome = store.load_outcome()
        self.observed_opening_fill = store.opening_fill_observed()
        self._halted = self.outcome != Outcome.ACTIVE
        self._issued_preflight: Preflight | None = None

    @property
    def close_count(self) -> int:
        return sum(intent.kind == "CLOSE" for intent in self.store.all())

    def _reject(self, *, halt: bool = False) -> None:
        if halt:
            self._halted = True
            self.outcome = Outcome.FAILED_HALTED_MANUAL_RECOVERY
            self.store.persist_outcome(self.outcome)
        raise LifecycleSafetyError("RISEx lifecycle safety check rejected")

    def _fresh(self, observed_at: int) -> bool:
        return 0 <= self._now() - observed_at <= MAX_AGE_SECONDS

    def _account_valid(self, value: AccountState) -> bool:
        try:
            account = _address(value.account).lower()
            signer = _address(value.signer).lower()
        except LifecycleSafetyError:
            return False
        return not (
            account != self._expected_account
            or signer != self._expected_signer
            or value.signer_status != "ACTIVE"
            or not self._fresh(value.observed_at)
            or value.position != value.repeated_position
            or value.open_order_ids != value.repeated_open_order_ids
            or any(not _valid_order_id(item) for item in value.open_order_ids)
            or value.unexplained
        )

    def _validate_account(self, value: AccountState) -> None:
        if not self._account_valid(value):
            self._reject(halt=bool(self.store.all()))

    def _validate_market(self, value: MarketState) -> None:
        router = _address(value.router).lower()
        authorization = _address(value.authorization).lower()
        if (
            value.host != OFFICIAL_HOST or value.chain_id != OFFICIAL_CHAIN_ID
            or value.domain_name != OFFICIAL_DOMAIN_NAME
            or value.domain_version != OFFICIAL_DOMAIN_VERSION
            or router != self._router or value.market_id != 1
            or authorization != self._authorization
            or value.symbol != "BTC/USDC" or not value.active or not value.unlocked
            or not self._fresh(value.observed_at) or value.tick <= 0 or value.step <= 0
            or not _aligned(value.minimum, value.step)
        ):
            self._reject()
        _uint(value.market_id, 16, "market_id")

    def _validate_bbo(self, market: MarketState, value: BBO, side: str, size: Decimal) -> Decimal:
        if (
            not self._fresh(value.observed_at) or value.bid <= 0 or value.ask <= value.bid
            or value.bid % market.tick or value.ask % market.tick
        ):
            self._reject()
        depth = value.ask_depth if side == "BUY" else value.bid_depth
        reference = value.ask if side == "BUY" else value.bid
        if depth < size:
            self._reject()
        price = _bound(reference, market.tick, side)
        if size * price > MAX_NOTIONAL_USD:
            self._reject()
        return price

    def preflight(self, market: MarketState, account: AccountState, bbo: BBO) -> Preflight:
        self._issued_preflight = None
        if self._halted or self.store.all():
            self._reject()
        self._validate_market(market)
        self._validate_account(account)
        if account.position != 0 or account.open_order_ids:
            self._reject()
        if bbo.bid_depth < market.minimum or bbo.ask_depth < market.minimum:
            self._reject()
        price = self._validate_bbo(market, bbo, "BUY", market.minimum)
        result = Preflight(market, account, bbo, market.minimum, price)
        self._issued_preflight = result
        return result

    def _prepare(
        self, *, kind: str, side: str, order_type: str, time_in_force: str,
        reduce_only: bool, market: MarketState, account: AccountState, bbo: BBO,
        size: Decimal, price: Decimal, source_position: Decimal,
        client_order_id: int, nonce_anchor: int, nonce_bitmap: int, expires_at: int,
    ) -> Intent:
        if (
            self._halted or expires_at <= self._now()
        ):
            self._reject()
        expires_at = _uint(expires_at, 32, "permit deadline")
        if expires_at > self._now() + MAX_PERMIT_SECONDS:
            self._reject()
        client_order_id = _uint(client_order_id, 64, "client_order_id")
        nonce_anchor = _uint(nonce_anchor, 48, "nonce_anchor")
        nonce_bitmap = _uint(nonce_bitmap, 8, "nonce_bitmap")
        if client_order_id == 0 or nonce_bitmap > 207:
            self._reject()
        ordinal = len(self.store.all()) + 1
        if size % market.step or price % market.tick:
            self._reject()
        size_steps = _uint(int(size / market.step), 32, "size_steps")
        price_ticks = _uint(int(price / market.tick), 24, "price_ticks")
        order_data = pack_order_data(
            market_id=market.market_id, size_steps=size_steps,
            price_ticks=price_ticks, side=side, post_only=False,
            reduce_only=reduce_only, order_type=order_type,
            time_in_force=time_in_force,
        )
        _encoded, action_hash = encode_place_action(
            order_data=order_data, client_order_id=client_order_id,
        )
        snapshot_digest = _evidence_digest({
            "market": repr(market), "account": repr(account), "bbo": repr(bbo),
        })
        intent = Intent(
            str(uuid.uuid4()), ordinal, kind, client_order_id, nonce_anchor,
            nonce_bitmap, action_hash.hex(), snapshot_digest, "PREPARED", side, order_type,
            time_in_force, reduce_only, False, market.market_id, size, size_steps,
            price, price_ticks, source_position, expires_at,
        )
        self.store._add(intent)
        observed_times = (market.observed_at, account.observed_at, bbo.observed_at)
        self.store._record_snapshot_window(
            intent.intent_id, max(observed_times),
            min(observed_times) + MAX_AGE_SECONDS,
        )
        return intent

    def prepare_open(
        self, preflight: Preflight, client_order_id: int, nonce_anchor: int,
        nonce_bitmap: int, expires_at: int,
    ) -> Intent:
        issued_preflight = self._issued_preflight
        self._issued_preflight = None
        if (
            preflight is not issued_preflight
            or any(item.kind == "OPEN" for item in self.store.all())
        ):
            self._reject()
        self._validate_market(preflight.market)
        self._validate_account(preflight.account)
        if preflight.account.position != 0 or preflight.account.open_order_ids:
            self._reject()
        exact_size = preflight.market.minimum
        exact_bound = self._validate_bbo(
            preflight.market, preflight.bbo, "BUY", exact_size,
        )
        if preflight.size != exact_size or preflight.buy_bound != exact_bound:
            self._reject()
        intent = self._prepare(
            kind="OPEN", side="BUY", order_type="MARKET", time_in_force="FOK",
            reduce_only=False, market=preflight.market, account=preflight.account,
            bbo=preflight.bbo,
            size=exact_size, price=exact_bound,
            source_position=Decimal("0"), client_order_id=client_order_id,
            nonce_anchor=nonce_anchor, nonce_bitmap=nonce_bitmap,
            expires_at=expires_at,
        )
        return intent

    def prepare_close(
        self, market: MarketState, account: AccountState, bbo: BBO,
        client_order_id: int, nonce_anchor: int, nonce_bitmap: int,
        expires_at: int,
    ) -> Intent:
        if self._halted or not self.observed_opening_fill or self.close_count >= 3:
            self._reject(halt=self.close_count >= 3)
        self._validate_market(market)
        self._validate_account(account)
        position = account.position
        if account.open_order_ids:
            self._reject(halt=True)
        latest_position = self.store.latest_reconciled_position()
        if latest_position is None or position != latest_position:
            self._reject(halt=True)
        if position <= 0 or not _aligned(position, market.step):
            self._reject(halt=True)
        prior = [item for item in self.store.all() if item.kind == "CLOSE"]
        unfinished = [item for item in self.store.all() if item.state != "TERMINAL"]
        if unfinished or any(
            state != "TERMINAL" for state in self.store.cancel_states()
        ):
            self._reject(halt=True)
        if prior and (prior[-1].state != "TERMINAL" or position > prior[-1].source_position):
            self._reject(halt=True)
        if not prior and position > market.minimum:
            self._reject(halt=True)
        side = "SELL"
        price = self._validate_bbo(market, bbo, side, position)
        number = len(prior) + 1
        if number == 3 and position == prior[-1].source_position:
            self._reject(halt=True)
        return self._prepare(
            kind="CLOSE", side=side,
            order_type="MARKET" if number == 1 else "LIMIT",
            time_in_force="FOK" if number == 1 else "IOC", reduce_only=True,
            market=market, account=account, bbo=bbo, size=position, price=price,
            source_position=position,
            client_order_id=client_order_id, nonce_anchor=nonce_anchor,
            nonce_bitmap=nonce_bitmap, expires_at=expires_at,
        )

    def run_open(
        self, market: MarketState, account: AccountState, bbo: BBO,
        client_order_id: int, nonce_anchor: int, nonce_bitmap: int,
        expires_at: int,
        *, signer_loader: Callable[[], Any], dispatch: Callable[[dict[str, Any]], Any],
    ) -> Intent:
        intent = self.prepare_open(
            self.preflight(market, account, bbo), client_order_id,
            nonce_anchor, nonce_bitmap, expires_at,
        )
        synthetic_or_later_gated_signer = signer_loader()
        self.dispatch(intent, synthetic_or_later_gated_signer, dispatch)
        return intent

    def dispatch(
        self, intent: Intent, synthetic_signer: Any,
        execute: Callable[[dict[str, Any]], str],
    ) -> None:
        current = self.store.get(intent.intent_id)
        snapshot_window = self.store.snapshot_window(intent.intent_id)
        if (
            self._halted or intent != current or current.state != "PREPARED"
            or current.dispatch_count or self._now() >= current.expires_at
            or snapshot_window is None
            or not snapshot_window[0] <= self._now() <= snapshot_window[1]
        ):
            self._reject()
        if (
            not isinstance(synthetic_signer, SyntheticSigner)
            or not synthetic_signer.fixture_only
            or _address(synthetic_signer.signer).lower() != self._expected_signer
        ):
            self._reject()
        self.store._update_state(intent.intent_id, "DISPATCHING", increment=True)
        try:
            order_id = execute({"intent": "[REDACTED]", "digest": current.payload_digest})
        except Exception:
            self.store._update_state(intent.intent_id, "AMBIGUOUS")
            return
        if not _valid_order_id(order_id):
            self.store._update_state(intent.intent_id, "AMBIGUOUS")
            return
        self.store._update_state(intent.intent_id, "DISPATCHED", order_id=order_id)

    def unsigned_action(self, intent_id: str) -> dict[str, Any]:
        """Return the exact synthetic-boundary fields used by official place encoding."""
        value = self.store.get(intent_id)
        return {
            "action": PLACE_ACTION,
            "market_id": value.market_id,
            "side": value.side,
            "order_type": value.order_type,
            "time_in_force": value.time_in_force,
            "reduce_only": value.reduce_only,
            "post_only": value.post_only,
            "size_steps": value.size_steps,
            "price_ticks": value.price_ticks,
            "client_order_id": value.client_order_id,
            "nonce_anchor": value.nonce,
            "nonce_bitmap": value.nonce_bitmap,
            "permit_deadline": value.expires_at,
        }

    def unsigned_request(
        self, intent_id: str, *, market: MarketState,
    ) -> dict[str, Any]:
        self._validate_market(market)
        value = self.store.get(intent_id)
        order_data = pack_order_data(
            market_id=value.market_id, size_steps=value.size_steps,
            price_ticks=value.price_ticks, side=value.side,
            post_only=value.post_only, reduce_only=value.reduce_only,
            order_type=value.order_type, time_in_force=value.time_in_force,
        )
        encoded, action_hash = encode_place_action(
            order_data=order_data, client_order_id=value.client_order_id,
        )
        if action_hash.hex() != value.payload_digest:
            self._reject(halt=True)
        witness = verify_witness_typed_data(
            account=self._expected_account, market=market, action_hash=action_hash,
            nonce_anchor=value.nonce, nonce_bitmap=value.nonce_bitmap,
            deadline=value.expires_at,
        )
        return {
            "header_flags": HEADER_FLAGS,
            "order_data": order_data,
            "abi_encoded": encoded,
            "action_hash": action_hash,
            "permit": witness,
            "body": {
                "market_id": value.market_id,
                "size_steps": value.size_steps,
                "price_ticks": value.price_ticks,
                "side": {"BUY": 0, "SELL": 1}[value.side],
                "order_type": {"MARKET": 0, "LIMIT": 1}[value.order_type],
                "time_in_force": {
                    "GTC": 0, "GTT": 1, "FOK": 2, "IOC": 3,
                }[value.time_in_force],
                "post_only": value.post_only,
                "reduce_only": value.reduce_only,
                "stp_mode": 0,
                "client_order_id": value.client_order_id,
                "account": self._expected_account,
                "signer": self._expected_signer,
                "nonce_anchor": str(value.nonce),
                "nonce_bitmap_index": value.nonce_bitmap,
                "deadline": value.expires_at,
            },
            "signature": None,
            "dispatchable": False,
        }

    def reconcile(self, intent_id: str, evidence: Evidence) -> Outcome:
        current = self.store.get(intent_id)
        if current.dispatch_count != 1 or current.state not in {
            "DISPATCHING", "DISPATCHED", "AMBIGUOUS", "OPEN_KNOWN",
        }:
            self._reject()
        try:
            evidence_account = _address(evidence.account).lower()
            evidence_signer = _address(evidence.signer).lower()
        except LifecycleSafetyError:
            self._reject(halt=True)
        if (
            evidence_account != self._expected_account
            or evidence_signer != self._expected_signer
            or evidence.signer_status != "ACTIVE"
            or evidence.client_order_id != current.client_order_id
            or not self._fresh(evidence.observed_at)
        ):
            self._reject(halt=True)
        scalar_ids = (evidence.order_id, evidence.by_id_order_id)
        tuple_ids = (
            *evidence.history_order_ids, *evidence.trade_order_ids,
            *evidence.open_order_ids,
        )
        if (
            any(value is not None and not _valid_order_id(value) for value in scalar_ids)
            or any(not _valid_order_id(value) for value in tuple_ids)
        ):
            self._reject(halt=True)
        observed_ids = {
            value for value in (
                evidence.order_id, evidence.by_id_order_id,
                *evidence.history_order_ids, *evidence.trade_order_ids,
            ) if value is not None
        }
        if any(value != current.client_order_id for value in evidence.trade_client_order_ids):
            self._reject(halt=True)
        if any(value != current.client_order_id for value in evidence.history_client_order_ids):
            self._reject(halt=True)
        expected = current.order_id
        if expected is not None and (observed_ids - {expected}):
            self._reject(halt=True)
        if expected is None and observed_ids:
            if len(observed_ids) != 1:
                self._reject(halt=True)
            expected = next(iter(observed_ids))
        if evidence.open_order_ids:
            open_known = (
                expected is not None
                and evidence.open_order_ids == (expected,)
                and not evidence.terminal
                and evidence.by_id_order_id == expected
                and evidence.history_order_ids == (expected,)
                and evidence.history_client_order_ids == (current.client_order_id,)
                and evidence.filled_size == 0
                and not evidence.trade_order_ids
                and not evidence.trade_client_order_ids
                and evidence.position == (
                    Decimal("0") if current.kind == "OPEN"
                    else current.source_position
                )
            )
            if not open_known:
                self._reject(halt=True)
            self.store._record_open_known(intent_id, expected)
            return self.outcome
        if expected is None:
            safe_no_identity = (
                self._now() > current.expires_at and evidence.terminal
                and evidence.filled_size == 0 and evidence.position == 0
                and not evidence.open_order_ids and not evidence.history_order_ids
                and not evidence.history_client_order_ids
                and not evidence.trade_order_ids
                and not evidence.trade_client_order_ids
            )
            if not safe_no_identity:
                self._reject()
        else:
            if not evidence.terminal or expected in evidence.open_order_ids:
                self._reject()
            if evidence.by_id_order_id != expected or expected not in evidence.history_order_ids:
                self._reject(halt=True)
            if current.client_order_id not in evidence.history_client_order_ids:
                self._reject(halt=True)
            if evidence.filled_size > 0 and expected not in evidence.trade_order_ids:
                self._reject(halt=True)
            if evidence.filled_size > 0 and current.client_order_id not in evidence.trade_client_order_ids:
                self._reject(halt=True)
            if evidence.filled_size == 0 and (
                evidence.trade_order_ids or evidence.trade_client_order_ids
            ):
                self._reject(halt=True)
        if evidence.filled_size < 0 or evidence.position < 0:
            self._reject(halt=True)
        if current.kind == "OPEN":
            if evidence.filled_size not in {Decimal("0"), current.size}:
                self._reject(halt=True)
            expected_position = current.size if evidence.filled_size else Decimal("0")
            if evidence.position != expected_position:
                self._reject(halt=True)
        else:
            expected_position = current.source_position - evidence.filled_size
            if evidence.filled_size > current.source_position or evidence.position != expected_position:
                self._reject(halt=True)
        self.store._reconcile_intent(intent_id, expected, evidence.position)
        if current.kind == "OPEN" and evidence.filled_size == 0 and evidence.position == 0 and not evidence.open_order_ids:
            self.outcome = Outcome.COMPLETED_NO_FILL_FLAT
            self.store.persist_outcome(self.outcome)
            self._halted = True
            return self.outcome
        if evidence.filled_size > 0 or evidence.position > 0:
            self.observed_opening_fill = True
            if current.kind == "OPEN":
                self.store._set_opening_fill_observed()
        return self.outcome

    def cancel_known(self, order_id: str, execute: Callable[[str], Any]) -> None:
        if not _valid_order_id(order_id):
            self._reject(halt=bool(self.store.all()))
        if (
            self._halted or not self.store.known_order(order_id)
            or self.store.order_state(order_id) != "OPEN_KNOWN"
        ):
            self._reject()
        self.store._reserve_cancel(order_id)
        try:
            execute(order_id)
        except Exception:
            self.store._set_cancel_state(order_id, "AMBIGUOUS")
            return
        self.store._set_cancel_state(order_id, "PENDING_RECONCILIATION")

    def reconcile_cancel(self, order_id: str, account: AccountState) -> bool:
        if not _valid_order_id(order_id):
            self._reject(halt=bool(self.store.all()))
        if (
            not self.store.known_order(order_id) or self.store.cancel_count(order_id) != 1
        ):
            self._reject()
        self._validate_account(account)
        if account.open_order_ids:
            if account.open_order_ids == (order_id,):
                return False
            self._reject(halt=True)
        self.store._set_cancel_state(order_id, "TERMINAL")
        return True

    def halt_manual(self, account: AccountState, reason: str) -> dict[str, Any]:
        self._halted = True
        self.outcome = Outcome.FAILED_HALTED_MANUAL_RECOVERY
        self.store.persist_outcome(self.outcome)
        safe_reason = reason if reason in {"connectivity_lost", "attempts_exhausted", "state_conflict"} else "safety_halt"
        return {
            "outcome": self.outcome.value,
            "reason": safe_reason,
            "position": str(account.position),
            "order_ids": ["[REDACTED]" for _ in account.open_order_ids],
            "manual_recovery": (
                "Use the official RISEx testnet UI to inspect and full-close the exact "
                "remaining position, then verify zero open orders and exact flatness."
            ),
        }

    def finalize(self, account: AccountState) -> Outcome:
        if self.outcome in {
            Outcome.COMPLETED_NO_FILL_FLAT, Outcome.SUCCESS_CLOSED_FLAT,
            Outcome.FAILED_HALTED_MANUAL_RECOVERY,
        }:
            return self.outcome
        intents = self.store.all()
        latest_position = self.store.latest_reconciled_position()
        if (
            self.observed_opening_fill and self._account_valid(account)
            and latest_position == Decimal("0")
            and account.position == latest_position
            and not account.open_order_ids and not account.unexplained
            and self._fresh(account.observed_at)
            and account.repeated_position == account.position
            and account.repeated_open_order_ids == account.open_order_ids
            and any(item.kind == "OPEN" for item in intents)
            and any(item.kind == "CLOSE" for item in intents)
            and all(
                item.state == "TERMINAL" and item.dispatch_count == 1
                and item.reconciled for item in intents
            )
            and all(state == "TERMINAL" for state in self.store.cancel_states())
        ):
            self.outcome = Outcome.SUCCESS_CLOSED_FLAT
            self.store.persist_outcome(self.outcome)
            self._halted = True
            return self.outcome
        self._halted = True
        self.outcome = Outcome.FAILED_HALTED_MANUAL_RECOVERY
        self.store.persist_outcome(self.outcome)
        return self.outcome
