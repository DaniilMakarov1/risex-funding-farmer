"""Fixture-only Nado testnet lifecycle contract.

This module deliberately contains no network transport, credential loader, CLI,
or normal Farmer import.  Its only execution boundary is an injected fixture
callable used to prove PREPARED-before-dispatch ordering.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Callable, Mapping

from eth_hash.auto import keccak
from eth_keys import keys
from eth_keys.datatypes import Signature


X18 = 10**18
MAX_NOTIONAL_X18 = 500 * X18
MAX_CLOSE_ATTEMPTS = 3
UINT32_MAX = 2**32 - 1
_ONLY_SYNTHETIC_PRIVATE_KEY = bytes.fromhex("00" * 31 + "01")
SOURCE_PINS = {
    "typescript_sdk": "315e4f23dadefeb2f86f713e423241e81467d4c3",
    "rust_sdk": "e54118786b171a4325871d5bd17e5abae0e90c5a",
    "contracts": "11c27b2851999f1b4f8cb4a7fbfcc9320253f12f",
}

ACTIVE_PERP = "PERP"
ENTRY = "ENTRY"
CANCEL_ALL = "CANCEL_ALL"
CLOSE = "CLOSE"
HALTED = "HALTED"
COMPLETE = "COMPLETE"
RUNNING = "RUNNING"

# Pinned SDK appendix packing: version=1, order type in bits 9..10,
# reduce-only in bit 11.
POST_ONLY_APPENDIX = 1 | (3 << 9)
IOC_REDUCE_ONLY_APPENDIX = 1 | (1 << 9) | (1 << 11)


class NadoContractError(ValueError):
    """A fail-closed contract or evidence violation."""


class FixedEnvironment:
    chain_id = 763373
    domain_name = "Nado"
    domain_version = "0.0.1"
    endpoint = "0x698D87105274292B5673367DEC81874Ce3633Ac2"
    gateway = "https://gateway.test.nado.xyz/v1"
    gateway_ws = "wss://gateway.test.nado.xyz/v1/ws"
    archive = "https://archive.test.nado.xyz/v1"
    trigger = "https://trigger.test.nado.xyz/v1"

    @classmethod
    def as_dict(cls) -> dict[str, object]:
        return {
            "chain_id": cls.chain_id,
            "domain_name": cls.domain_name,
            "domain_version": cls.domain_version,
            "endpoint": cls.endpoint,
            "gateway": cls.gateway,
            "gateway_ws": cls.gateway_ws,
            "archive": cls.archive,
            "trigger": cls.trigger,
        }

    @classmethod
    def assert_exact(cls, *, chain_id: int, endpoint: str) -> None:
        if chain_id != cls.chain_id or endpoint != cls.endpoint:
            raise NadoContractError("Nado testnet environment identity mismatch")


def product_verifier(product_id: int) -> str:
    if not 0 <= product_id <= UINT32_MAX:
        raise NadoContractError("product id is outside uint32")
    return f"0x{product_id:040x}"


def build_order_nonce(recv_time: int, salt: int) -> int:
    if not 0 <= recv_time < 2**44:
        raise NadoContractError("recv_time is outside 44 bits")
    if not 0 <= salt < 2**20:
        raise NadoContractError("order salt is outside 20 bits")
    return (recv_time << 20) | salt


def unpack_order_nonce(nonce: int) -> tuple[int, int]:
    if not 0 <= nonce < 2**64:
        raise NadoContractError("nonce is outside uint64")
    return nonce >> 20, nonce & ((1 << 20) - 1)


def _address_bytes(address: str) -> bytes:
    if not isinstance(address, str) or not address.startswith("0x"):
        raise NadoContractError("address must be 0x-prefixed")
    try:
        raw = bytes.fromhex(address[2:])
    except ValueError as exc:
        raise NadoContractError("address is not hex") from exc
    if len(raw) != 20:
        raise NadoContractError("address must contain 20 bytes")
    return raw


def encode_subaccount(owner: str, subaccount_name: str) -> str:
    try:
        name = subaccount_name.encode("ascii")
    except UnicodeEncodeError as exc:
        raise NadoContractError("subaccount name must be ASCII") from exc
    if not name or len(name) > 12 or b"\0" in name:
        raise NadoContractError("subaccount name must contain 1..12 non-NUL bytes")
    return "0x" + (_address_bytes(owner) + name.ljust(12, b"\0")).hex()


def canonical_payload(payload: Mapping[str, object]) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")


def _hex_bytes(value: str, length: int) -> bytes:
    if not value.startswith("0x"):
        raise NadoContractError("hex value must be 0x-prefixed")
    try:
        raw = bytes.fromhex(value[2:])
    except ValueError as exc:
        raise NadoContractError("invalid hex value") from exc
    if len(raw) != length:
        raise NadoContractError(f"hex value must contain {length} bytes")
    return raw


def _uint256(value: int) -> bytes:
    if not 0 <= value < 2**256:
        raise NadoContractError("unsigned EIP-712 value is outside uint256")
    return value.to_bytes(32, "big")


def _signed256(value: int, bits: int) -> bytes:
    if not -(2 ** (bits - 1)) <= value < 2 ** (bits - 1):
        raise NadoContractError(f"signed EIP-712 value is outside int{bits}")
    encoded = value if value >= 0 else (1 << 256) + value
    return encoded.to_bytes(32, "big")


def _domain_separator(verifying_contract: str) -> bytes:
    domain_type = keccak(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
    )
    encoded = b"".join(
        (
            domain_type,
            keccak(FixedEnvironment.domain_name.encode("ascii")),
            keccak(FixedEnvironment.domain_version.encode("ascii")),
            _uint256(FixedEnvironment.chain_id),
            _address_bytes(verifying_contract).rjust(32, b"\0"),
        )
    )
    return keccak(encoded)


@dataclass(frozen=True)
class SyntheticOrderVector:
    owner: str
    subaccount_name: str
    sender: str
    product_id: int
    price_x18: int
    amount_x18: int
    expiration: int
    recv_time: int
    salt: int
    nonce: int
    appendix: int

    @classmethod
    def from_fixture(cls, fixture: Mapping[str, object]) -> "SyntheticOrderVector":
        order = cls(
            owner=str(fixture["owner"]),
            subaccount_name=str(fixture["subaccount_name"]),
            sender=str(fixture["sender"]),
            product_id=int(fixture["product_id"]),
            price_x18=int(fixture["price_x18"]),
            amount_x18=int(fixture["amount_x18"]),
            expiration=int(fixture["expiration"]),
            recv_time=int(fixture["recv_time"]),
            salt=int(fixture["salt"]),
            nonce=int(fixture["nonce"]),
            appendix=int(fixture["appendix"]),
        )
        order.assert_contract()
        return order

    def assert_contract(self) -> None:
        if encode_subaccount(self.owner, self.subaccount_name).lower() != self.sender.lower():
            raise NadoContractError("sender does not encode owner and subaccount")
        if build_order_nonce(self.recv_time, self.salt) != self.nonce:
            raise NadoContractError("nonce does not encode recv_time and salt")
        product_verifier(self.product_id)
        _signed256(self.price_x18, 128)
        _signed256(self.amount_x18, 128)
        if not 0 <= self.expiration < 2**64:
            raise NadoContractError("expiration is outside uint64")
        if not 0 <= self.appendix < 2**128:
            raise NadoContractError("appendix is outside uint128")

    def as_payload(self) -> dict[str, object]:
        self.assert_contract()
        return {
            "place_order": {
                "product_id": self.product_id,
                "order": {
                    "sender": self.sender.lower(),
                    "priceX18": str(self.price_x18),
                    "amount": str(self.amount_x18),
                    "expiration": str(self.expiration),
                    "nonce": str(self.nonce),
                    "appendix": str(self.appendix),
                },
            }
        }


def order_digest(order: SyntheticOrderVector) -> str:
    order.assert_contract()
    order_type = keccak(
        b"Order(bytes32 sender,int128 priceX18,int128 amount,uint64 expiration,uint64 nonce,uint128 appendix)"
    )
    struct_hash = keccak(
        b"".join(
            (
                order_type,
                _hex_bytes(order.sender, 32),
                _signed256(order.price_x18, 128),
                _signed256(order.amount_x18, 128),
                _uint256(order.expiration),
                _uint256(order.nonce),
                _uint256(order.appendix),
            )
        )
    )
    digest = keccak(
        b"\x19\x01" + _domain_separator(product_verifier(order.product_id)) + struct_hash
    )
    return "0x" + digest.hex()


def sign_synthetic_order(order: SyntheticOrderVector, synthetic_private_key: str) -> tuple[str, str]:
    """Sign a fixed synthetic vector; there is intentionally no key loader."""
    private_key = _hex_bytes(synthetic_private_key, 32)
    if private_key != _ONLY_SYNTHETIC_PRIVATE_KEY:
        raise NadoContractError("only the fixed public synthetic test key is permitted")
    digest = order_digest(order)
    signature = keys.PrivateKey(private_key).sign_msg_hash(_hex_bytes(digest, 32))
    encoded = (
        signature.r.to_bytes(32, "big")
        + signature.s.to_bytes(32, "big")
        + bytes((signature.v + 27,))
    )
    return digest, "0x" + encoded.hex()


def verify_signed_validation(
    order: SyntheticOrderVector,
    *,
    signature: str,
    validation_digest: str,
    validation_signature: str,
    validation_valid: bool,
) -> bool:
    digest = order_digest(order)
    if not validation_valid or validation_digest.lower() != digest.lower():
        raise NadoContractError("validate_order did not affirm the exact digest")
    if validation_signature.lower() != signature.lower():
        raise NadoContractError("validate_order signature identity mismatch")
    raw = _hex_bytes(signature, 65)
    if raw[64] not in (27, 28):
        raise NadoContractError("signature recovery id must be 27 or 28")
    recovered = Signature(
        vrs=(raw[64] - 27, int.from_bytes(raw[:32], "big"), int.from_bytes(raw[32:64], "big"))
    ).recover_public_key_from_msg_hash(_hex_bytes(digest, 32))
    if recovered.to_canonical_address() != _address_bytes(order.owner):
        raise NadoContractError("signature is not from the dedicated owner EOA")
    return True


@dataclass(frozen=True)
class Product:
    product_id: int
    symbol: str
    product_type: str
    active: bool
    tick_x18: int
    step_x18: int
    minimum_amount_x18: int
    minimum_notional_x18: int

    def assert_contract(self) -> None:
        product_verifier(self.product_id)
        if not self.symbol or min(
            self.tick_x18,
            self.step_x18,
            self.minimum_amount_x18,
            self.minimum_notional_x18,
        ) <= 0:
            raise NadoContractError("product grid and minimums must be positive")
        if self.minimum_amount_x18 % self.step_x18:
            raise NadoContractError("minimum amount is off the product step")


@dataclass(frozen=True)
class CatalogSnapshot:
    products: tuple[Product, ...]
    complete: bool
    observed_at_ms: int

    def by_id(self) -> dict[int, Product]:
        if not self.complete or not self.products:
            raise NadoContractError("complete dynamic product catalog is required")
        result: dict[int, Product] = {}
        for product in self.products:
            product.assert_contract()
            if product.product_id in result:
                raise NadoContractError("duplicate product in dynamic catalog")
            result[product.product_id] = product
        return result


@dataclass(frozen=True)
class AccountSnapshot:
    chain_id: int
    endpoint: str
    owner: str
    subaccount_name: str
    observed_at_ms: int
    fresh: bool
    authoritative_source: str
    regular_orders_by_product: Mapping[int, tuple[str, ...]]
    cross_perp_amounts_x18: Mapping[int, int]
    isolated_positions: tuple[str, ...]
    snapshot_id: str = "engine-snapshot"
    contradictions: tuple[str, ...] = ()


@dataclass(frozen=True)
class TriggerSnapshot:
    owner: str
    subaccount_name: str
    observed_at_ms: int
    fresh: bool
    authoritative_source: str
    active_digests: tuple[str, ...]
    snapshot_id: str = "trigger-snapshot"
    contradictions: tuple[str, ...] = ()


def _assert_authoritative_account(
    catalog: CatalogSnapshot,
    account: AccountSnapshot,
    *,
    now_ms: int,
    require_flat: bool,
) -> dict[int, Product]:
    products = catalog.by_id()
    FixedEnvironment.assert_exact(chain_id=account.chain_id, endpoint=account.endpoint)
    _address_bytes(account.owner)
    encode_subaccount(account.owner, account.subaccount_name)
    if not account.fresh or account.authoritative_source != "engine":
        raise NadoContractError("fresh authoritative engine account evidence is required")
    if account.observed_at_ms > now_ms:
        raise NadoContractError("account evidence is from the future")
    if account.contradictions:
        raise NadoContractError("contradictory engine evidence")
    if not account.snapshot_id:
        raise NadoContractError("engine snapshot identity is required")
    catalog_ids = set(products)
    if set(account.regular_orders_by_product) != catalog_ids:
        raise NadoContractError("regular-order evidence does not cover the complete catalog")
    perp_ids = {
        product_id
        for product_id, product in products.items()
        if product.product_type == ACTIVE_PERP
    }
    if set(account.cross_perp_amounts_x18) != perp_ids:
        raise NadoContractError("cross-perp evidence does not cover the complete catalog")
    if require_flat and any(account.cross_perp_amounts_x18.values()):
        raise NadoContractError("cross-perp amount is not exactly zero")
    if require_flat and account.isolated_positions:
        raise NadoContractError("isolated position is present")
    return products


def _assert_trigger_snapshot(
    account: AccountSnapshot,
    triggers: TriggerSnapshot,
    *,
    now_ms: int,
) -> None:
    if (
        not triggers.fresh
        or triggers.authoritative_source != "trigger"
        or triggers.observed_at_ms > now_ms
        or not triggers.snapshot_id
        or triggers.contradictions
    ):
        raise NadoContractError("fresh authoritative trigger-service evidence is required")
    if (
        triggers.owner.lower() != account.owner.lower()
        or triggers.subaccount_name != account.subaccount_name
    ):
        raise NadoContractError("trigger-service subaccount identity mismatch")


def _notional_x18(price_x18: int, amount_x18: int) -> int:
    if price_x18 <= 0 or amount_x18 == 0:
        raise NadoContractError("price and amount must define positive notional")
    numerator = abs(price_x18) * abs(amount_x18)
    if numerator % X18:
        raise NadoContractError("notional is not exactly representable at x18")
    return numerator // X18


@dataclass(frozen=True)
class EntryPlan:
    product_id: int
    amount_x18: int
    entry_price_x18: int
    worst_close_price_x18: int
    entry_notional_x18: int
    close_notional_x18: int
    appendix: int = POST_ONLY_APPENDIX


def validate_entry_preflight(
    *,
    catalog: CatalogSnapshot,
    account: AccountSnapshot,
    triggers: TriggerSnapshot,
    product_id: int,
    entry_price_x18: int,
    worst_close_price_x18: int,
    now_ms: int,
) -> EntryPlan:
    products = _assert_authoritative_account(catalog, account, now_ms=now_ms, require_flat=True)
    _assert_trigger_snapshot(account, triggers, now_ms=now_ms)
    if any(account.regular_orders_by_product.values()):
        raise NadoContractError("regular order exists in the complete catalog")
    if triggers.active_digests:
        raise NadoContractError("active trigger order blocks preflight")
    product = products.get(product_id)
    if product is None or not product.active or product.product_type != ACTIVE_PERP:
        raise NadoContractError("target must be an active perpetual")
    for price in (entry_price_x18, worst_close_price_x18):
        if price <= 0 or price % product.tick_x18:
            raise NadoContractError("price is off the x18 product tick")
    amount = product.minimum_amount_x18
    if amount % product.step_x18:
        raise NadoContractError("minimum amount is off the x18 product step")
    entry_notional = _notional_x18(entry_price_x18, amount)
    close_notional = _notional_x18(worst_close_price_x18, amount)
    if min(entry_notional, close_notional) < product.minimum_notional_x18:
        raise NadoContractError("order is below product minimum notional")
    if max(entry_notional, close_notional) > MAX_NOTIONAL_X18:
        raise NadoContractError("entry or recovery exceeds USD 500")
    return EntryPlan(
        product_id=product_id,
        amount_x18=amount,
        entry_price_x18=entry_price_x18,
        worst_close_price_x18=worst_close_price_x18,
        entry_notional_x18=entry_notional,
        close_notional_x18=close_notional,
    )


@dataclass(frozen=True)
class OrderIntent:
    kind: str
    product_id: int | None
    nonce: int
    recv_time: int
    digest: str
    payload: bytes
    amount_x18: int = 0
    appendix: int = 0
    notional_x18: int = 0
    clamp_expected: bool = False
    snapshot_id: str | None = None
    snapshot_observed_at_ms: int | None = None
    starting_position_x18: int = 0


@dataclass(frozen=True)
class OrderEvidence:
    digest: str
    product_id: int
    nonce: int
    amount_x18: int
    status: str


@dataclass(frozen=True)
class FillEvidence:
    digest: str
    product_id: int
    amount_x18: int


@dataclass(frozen=True)
class EngineEvidence:
    account: AccountSnapshot
    triggers: TriggerSnapshot
    orders: tuple[OrderEvidence, ...]
    fills: tuple[FillEvidence, ...]
    observed_at_ms: int
    exact_rejection_digest: str | None = None
    duplicate_digest: bool = False
    exact_cancel_digest: str | None = None
    terminal_digest: str | None = None
    terminal_status: str | None = None
    archive_digests: tuple[str, ...] = ()


class Reconciliation(str, Enum):
    RESTING = "RESTING"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"
    AMBIGUOUS = "AMBIGUOUS"
    CONTRADICTORY = "CONTRADICTORY"


class IntentStore:
    """One durable owner for immutable signed-write intent identities."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._connection = sqlite3.connect(self.path)
        self._connection.execute("PRAGMA journal_mode=DELETE")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS nado_intents (
                digest TEXT PRIMARY KEY,
                nonce TEXT NOT NULL UNIQUE,
                recv_time INTEGER NOT NULL,
                kind TEXT NOT NULL,
                product_id INTEGER,
                payload BLOB NOT NULL,
                amount_x18 TEXT NOT NULL,
                appendix TEXT NOT NULL,
                notional_x18 TEXT NOT NULL,
                clamp_expected INTEGER NOT NULL,
                snapshot_id TEXT,
                snapshot_observed_at_ms INTEGER,
                starting_position_x18 TEXT NOT NULL,
                state TEXT NOT NULL
            )
            """
        )
        self._connection.commit()

    def prepare(self, intent: OrderIntent) -> None:
        if unpack_order_nonce(intent.nonce)[0] != intent.recv_time:
            raise NadoContractError("intent nonce and recv_time disagree")
        _hex_bytes(intent.digest, 32)
        if canonical_payload(json.loads(intent.payload)) != intent.payload:
            raise NadoContractError("intent payload is not canonical")
        try:
            with self._connection:
                self._connection.execute(
                    """
                    INSERT INTO nado_intents (
                        digest, nonce, recv_time, kind, product_id, payload,
                        amount_x18, appendix, notional_x18, clamp_expected,
                        snapshot_id, snapshot_observed_at_ms,
                        starting_position_x18, state
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PREPARED')
                    """,
                    (
                        intent.digest.lower(),
                        str(intent.nonce),
                        intent.recv_time,
                        intent.kind,
                        intent.product_id,
                        intent.payload,
                        str(intent.amount_x18),
                        str(intent.appendix),
                        str(intent.notional_x18),
                        int(intent.clamp_expected),
                        intent.snapshot_id,
                        intent.snapshot_observed_at_ms,
                        str(intent.starting_position_x18),
                    ),
                )
        except sqlite3.IntegrityError as exc:
            raise NadoContractError("digest or nonce was already consumed") from exc

    def get(self, digest: str) -> OrderIntent:
        row = self._connection.execute(
            """
            SELECT kind, product_id, nonce, recv_time, digest, payload,
                   amount_x18, appendix, notional_x18, clamp_expected,
                   snapshot_id, snapshot_observed_at_ms, starting_position_x18
            FROM nado_intents WHERE digest = ?
            """,
            (digest.lower(),),
        ).fetchone()
        if row is None:
            raise NadoContractError("unknown intent digest")
        return OrderIntent(
            kind=row[0],
            product_id=row[1],
            nonce=int(row[2]),
            recv_time=row[3],
            digest=row[4],
            payload=row[5],
            amount_x18=int(row[6]),
            appendix=int(row[7]),
            notional_x18=int(row[8]),
            clamp_expected=bool(row[9]),
            snapshot_id=row[10],
            snapshot_observed_at_ms=row[11],
            starting_position_x18=int(row[12]),
        )

    def _mark(self, digest: str, state: str) -> None:
        with self._connection:
            cursor = self._connection.execute(
                "UPDATE nado_intents SET state = ? WHERE digest = ?",
                (state, digest.lower()),
            )
        if cursor.rowcount != 1:
            raise NadoContractError("unknown intent digest")

    def state(self, digest: str) -> str:
        row = self._connection.execute(
            "SELECT state FROM nado_intents WHERE digest = ?", (digest.lower(),)
        ).fetchone()
        if row is None:
            raise NadoContractError("unknown intent digest")
        return str(row[0])

    def mark_ambiguous(self, digest: str) -> None:
        self._mark(digest, "AMBIGUOUS")

    def mark_reconciled(self, digest: str) -> None:
        self._mark(digest, "RECONCILED")

    def mark_rejected(self, digest: str) -> None:
        self._mark(digest, "REJECTED")

    def replace_payload(self, digest: str, payload: bytes) -> None:
        del digest, payload
        raise NadoContractError("persisted payload and digest are immutable")

    def prepare_then_fixture_dispatch(
        self,
        intent: OrderIntent,
        fixture_dispatch: Callable[[OrderIntent], str],
    ) -> str:
        self.prepare(intent)
        try:
            result = fixture_dispatch(intent)
        except BaseException:
            self.mark_ambiguous(intent.digest)
            raise
        self._mark(intent.digest, "DISPATCHED")
        return result

    def intents(self) -> tuple[tuple[OrderIntent, str], ...]:
        rows = self._connection.execute(
            "SELECT digest, state FROM nado_intents ORDER BY rowid"
        ).fetchall()
        return tuple((self.get(row[0]), str(row[1])) for row in rows)

    def count_kind(self, kind: str) -> int:
        return int(
            self._connection.execute(
                "SELECT COUNT(*) FROM nado_intents WHERE kind = ?", (kind,)
            ).fetchone()[0]
        )

    def close(self) -> None:
        self._connection.close()

    def snapshot_used(self, snapshot_id: str) -> bool:
        return bool(
            self._connection.execute(
                "SELECT 1 FROM nado_intents WHERE snapshot_id = ? LIMIT 1", (snapshot_id,)
            ).fetchone()
        )

    def latest_close_snapshot_time(self) -> int | None:
        return self._connection.execute(
            "SELECT MAX(snapshot_observed_at_ms) FROM nado_intents WHERE kind = ?",
            (CLOSE,),
        ).fetchone()[0]

    def latest_recv_time(self) -> int | None:
        return self._connection.execute("SELECT MAX(recv_time) FROM nado_intents").fetchone()[0]

    def reconcile(self, digest: str, evidence: EngineEvidence) -> Reconciliation:
        intent = self.get(digest)
        account = evidence.account
        if (
            not account.fresh
            or account.authoritative_source != "engine"
            or account.contradictions
            or evidence.observed_at_ms < account.observed_at_ms
        ):
            return Reconciliation.CONTRADICTORY
        try:
            _assert_trigger_snapshot(account, evidence.triggers, now_ms=evidence.observed_at_ms)
        except NadoContractError:
            return Reconciliation.CONTRADICTORY
        matching_orders = [order for order in evidence.orders if order.digest.lower() == digest.lower()]
        matching_fills = [fill for fill in evidence.fills if fill.digest.lower() == digest.lower()]
        if any(
            order.product_id != intent.product_id or order.nonce != intent.nonce
            for order in matching_orders
        ) or any(fill.product_id != intent.product_id for fill in matching_fills):
            return Reconciliation.CONTRADICTORY
        if len(matching_orders) > 1:
            return Reconciliation.CONTRADICTORY
        filled = sum(fill.amount_x18 for fill in matching_fills)
        position = account.cross_perp_amounts_x18.get(intent.product_id or -1, 0)
        if intent.kind == CANCEL_ALL:
            if matching_fills or matching_orders:
                return Reconciliation.CONTRADICTORY
            all_zero = not any(account.regular_orders_by_product.values())
            exact_cancel = (
                evidence.exact_cancel_digest is not None
                and evidence.exact_cancel_digest.lower() == digest.lower()
            )
            if all_zero and (exact_cancel or evidence.observed_at_ms > intent.recv_time):
                self.mark_reconciled(digest)
                return Reconciliation.CANCELLED
            self.mark_ambiguous(digest)
            return Reconciliation.AMBIGUOUS
        if matching_fills:
            expected_position = filled
            if intent.kind == CLOSE:
                expected_position = intent.starting_position_x18 + filled
            if position != expected_position:
                return Reconciliation.CONTRADICTORY
            if intent.kind == CLOSE and position == 0:
                self.mark_reconciled(digest)
                return Reconciliation.FILLED
            if intent.kind == ENTRY and abs(filled) >= abs(intent.amount_x18):
                self.mark_reconciled(digest)
                return Reconciliation.FILLED
            self._mark(digest, "PARTIAL")
            return Reconciliation.PARTIAL
        if matching_orders:
            if matching_orders[0].status != "OPEN":
                return Reconciliation.CONTRADICTORY
            self._mark(digest, "RESTING")
            return Reconciliation.RESTING
        if evidence.exact_rejection_digest and evidence.exact_rejection_digest.lower() == digest.lower():
            expected_position = intent.starting_position_x18 if intent.kind == CLOSE else 0
            if position != expected_position:
                return Reconciliation.CONTRADICTORY
            self.mark_rejected(digest)
            return Reconciliation.REJECTED
        if evidence.terminal_digest and evidence.terminal_digest.lower() == digest.lower():
            terminal = evidence.terminal_status
            if terminal not in {"CANCELLED", "EXPIRED", "REJECTED"}:
                return Reconciliation.CONTRADICTORY
            expected_position = intent.starting_position_x18 if intent.kind == CLOSE else 0
            if position != expected_position:
                return Reconciliation.CONTRADICTORY
            self.mark_rejected(digest)
            return Reconciliation(terminal)
        if evidence.duplicate_digest:
            self.mark_ambiguous(digest)
            return Reconciliation.AMBIGUOUS
        self.mark_ambiguous(digest)
        return Reconciliation.AMBIGUOUS

    def write_allowed(self, digest: str, *, now_ms: int, evidence: EngineEvidence) -> bool:
        intent = self.get(digest)
        return (
            self.state(digest) in {"RECONCILED", "REJECTED"}
            and now_ms > intent.recv_time
            and evidence.account.fresh
            and evidence.account.authoritative_source == "engine"
            and not evidence.account.contradictions
            and evidence.triggers.fresh
            and evidence.triggers.authoritative_source == "trigger"
            and not evidence.triggers.contradictions
        )


def cancel_all_payload(*, sender: str, tx_nonce: int) -> dict[str, object]:
    _hex_bytes(sender, 32)
    unpack_order_nonce(tx_nonce)
    return {
        "cancel_product_orders": {
            "tx": {
                "sender": sender.lower(),
                "productIds": [],
                "nonce": str(tx_nonce),
            }
        }
    }


def _cancel_all_digest(sender: str, nonce: int) -> str:
    type_hash = keccak(b"CancellationProducts(bytes32 sender,uint32[] productIds,uint64 nonce)")
    empty_array_hash = keccak(b"")
    struct_hash = keccak(type_hash + _hex_bytes(sender, 32) + empty_array_hash + _uint256(nonce))
    digest = keccak(
        b"\x19\x01" + _domain_separator(FixedEnvironment.endpoint) + struct_hash
    )
    return "0x" + digest.hex()


class LifecycleCore:
    def __init__(self, store: IntentStore) -> None:
        self.store = store
        self._halted = False

    @property
    def status(self) -> str:
        if self._halted or self.store.count_kind(CLOSE) >= MAX_CLOSE_ATTEMPTS:
            return HALTED
        return RUNNING

    def prepare_cancel_all(
        self,
        *,
        catalog: CatalogSnapshot,
        account: AccountSnapshot,
        triggers: TriggerSnapshot,
        sender: str,
        recv_time: int,
        salt: int,
        now_ms: int,
    ) -> OrderIntent:
        self._assert_next_state_write(recv_time=recv_time, now_ms=now_ms)
        _assert_authoritative_account(catalog, account, now_ms=now_ms, require_flat=False)
        _assert_trigger_snapshot(account, triggers, now_ms=now_ms)
        if triggers.active_digests or triggers.contradictions:
            raise NadoContractError("trigger state blocks cancel-all")
        if not any(account.regular_orders_by_product.values()):
            raise NadoContractError("cancel-all requires an unresolved regular order")
        if sender.lower() != encode_subaccount(account.owner, account.subaccount_name).lower():
            raise NadoContractError("cancel-all sender identity mismatch")
        nonce = build_order_nonce(recv_time, salt)
        payload = canonical_payload(cancel_all_payload(sender=sender, tx_nonce=nonce))
        intent = OrderIntent(
            kind=CANCEL_ALL,
            product_id=None,
            nonce=nonce,
            recv_time=recv_time,
            digest=_cancel_all_digest(sender, nonce),
            payload=payload,
        )
        self.store.prepare(intent)
        return intent

    def prepare_entry(
        self,
        *,
        order: SyntheticOrderVector,
        catalog: CatalogSnapshot,
        account: AccountSnapshot,
        triggers: TriggerSnapshot,
        worst_close_price_x18: int,
        signature: str,
        validation_digest: str,
        validation_signature: str,
        validation_valid: bool,
        now_ms: int,
    ) -> OrderIntent:
        if order.appendix != POST_ONLY_APPENDIX:
            raise NadoContractError("entry must be post-only")
        self._assert_next_state_write(recv_time=order.recv_time, now_ms=now_ms)
        verify_signed_validation(
            order,
            signature=signature,
            validation_digest=validation_digest,
            validation_signature=validation_signature,
            validation_valid=validation_valid,
        )
        plan = validate_entry_preflight(
            catalog=catalog,
            account=account,
            triggers=triggers,
            product_id=order.product_id,
            entry_price_x18=order.price_x18,
            worst_close_price_x18=worst_close_price_x18,
            now_ms=now_ms,
        )
        if order.amount_x18 != plan.amount_x18:
            raise NadoContractError("entry amount is not the preflight minimum")
        intent = OrderIntent(
            kind=ENTRY,
            product_id=order.product_id,
            nonce=order.nonce,
            recv_time=order.recv_time,
            digest=order_digest(order),
            payload=canonical_payload(order.as_payload()),
            amount_x18=order.amount_x18,
            appendix=order.appendix,
            notional_x18=plan.entry_notional_x18,
        )
        self.store.prepare(intent)
        return intent

    def _assert_next_state_write(self, *, recv_time: int, now_ms: int) -> None:
        if self.status == HALTED:
            raise NadoContractError("lifecycle is halted at the manual gate")
        prior_recv_time = self.store.latest_recv_time()
        if prior_recv_time is not None and now_ms <= prior_recv_time:
            raise NadoContractError("preceding signed recv_time has not elapsed")
        if recv_time < now_ms or recv_time > now_ms + 100_000:
            raise NadoContractError("recv_time is outside the documented receive window")

    def prepare_close(
        self,
        *,
        catalog: CatalogSnapshot,
        product: Product,
        account: AccountSnapshot,
        triggers: TriggerSnapshot,
        worst_price_x18: int,
        recv_time: int,
        salt: int,
        now_ms: int,
    ) -> OrderIntent:
        try:
            self._assert_next_state_write(recv_time=recv_time, now_ms=now_ms)
            if self.store.count_kind(CLOSE) >= MAX_CLOSE_ATTEMPTS:
                raise NadoContractError("three close attempts are exhausted")
            products = _assert_authoritative_account(
                catalog, account, now_ms=now_ms, require_flat=False
            )
            _assert_trigger_snapshot(account, triggers, now_ms=now_ms)
            if products.get(product.product_id) != product:
                raise NadoContractError("close product does not match the dynamic catalog")
            if self.store.snapshot_used(account.snapshot_id):
                raise NadoContractError("authoritative position snapshot was already consumed")
            latest_snapshot_time = self.store.latest_close_snapshot_time()
            if latest_snapshot_time is not None and account.observed_at_ms <= latest_snapshot_time:
                raise NadoContractError("close snapshot is not newer than the previous attempt")
            if any(account.regular_orders_by_product.values()):
                raise NadoContractError("regular orders must be zero before close")
            if triggers.active_digests or account.isolated_positions:
                raise NadoContractError("trigger or isolated state blocks close")
            position = account.cross_perp_amounts_x18.get(product.product_id)
            if position is None or position == 0:
                raise NadoContractError("nonzero authoritative cross position is required")
            if any(
                amount
                for other_product_id, amount in account.cross_perp_amounts_x18.items()
                if other_product_id != product.product_id
            ):
                raise NadoContractError("unexpected cross-perp exposure in another product")
            absolute_position = abs(position)
            if absolute_position % product.step_x18:
                raise NadoContractError("authoritative residual is off the amount step")
            if worst_price_x18 <= 0 or worst_price_x18 % product.tick_x18:
                raise NadoContractError("aggressive limit is off the price tick")
            clamp_expected = absolute_position < product.minimum_amount_x18
            submitted_amount = max(absolute_position, product.minimum_amount_x18)
            notional = _notional_x18(worst_price_x18, submitted_amount)
            if notional < product.minimum_notional_x18 or notional > MAX_NOTIONAL_X18:
                raise NadoContractError("close notional violates minimum or USD 500 cap")
            signed_amount = -submitted_amount if position > 0 else submitted_amount
            nonce = build_order_nonce(recv_time, salt)
            order = SyntheticOrderVector(
                owner=account.owner,
                subaccount_name=account.subaccount_name,
                sender=encode_subaccount(account.owner, account.subaccount_name),
                product_id=product.product_id,
                price_x18=worst_price_x18,
                amount_x18=signed_amount,
                expiration=UINT32_MAX,
                recv_time=recv_time,
                salt=salt,
                nonce=nonce,
                appendix=IOC_REDUCE_ONLY_APPENDIX,
            )
            intent = OrderIntent(
                kind=CLOSE,
                product_id=product.product_id,
                nonce=nonce,
                recv_time=recv_time,
                digest=order_digest(order),
                payload=canonical_payload(order.as_payload()),
                amount_x18=signed_amount,
                appendix=IOC_REDUCE_ONLY_APPENDIX,
                notional_x18=notional,
                clamp_expected=clamp_expected,
                snapshot_id=account.snapshot_id,
                snapshot_observed_at_ms=account.observed_at_ms,
                starting_position_x18=position,
            )
            self.store.prepare(intent)
            return intent
        except NadoContractError:
            self._halted = True
            raise


def completion_barrier(
    *,
    store: IntentStore,
    catalog: CatalogSnapshot,
    evidence: EngineEvidence,
    now_ms: int,
) -> bool:
    try:
        _assert_authoritative_account(catalog, evidence.account, now_ms=now_ms, require_flat=True)
        _assert_trigger_snapshot(evidence.account, evidence.triggers, now_ms=now_ms)
    except NadoContractError:
        return False
    if evidence.observed_at_ms < evidence.account.observed_at_ms:
        return False
    if any(evidence.account.regular_orders_by_product.values()):
        return False
    if evidence.triggers.active_digests or evidence.orders:
        return False
    intents = store.intents()
    known_intents = {intent.digest.lower(): intent for intent, _ in intents}
    net_fills: dict[int, int] = {}
    filled_by_digest: dict[str, int] = {}
    for fill in evidence.fills:
        intent = known_intents.get(fill.digest.lower())
        if intent is None or fill.product_id != intent.product_id:
            return False
        if fill.amount_x18 == 0 or (fill.amount_x18 > 0) != (intent.amount_x18 > 0):
            return False
        filled_by_digest[intent.digest.lower()] = (
            filled_by_digest.get(intent.digest.lower(), 0) + fill.amount_x18
        )
        net_fills[fill.product_id] = net_fills.get(fill.product_id, 0) + fill.amount_x18
    if any(net_fills.values()):
        return False
    if any(
        abs(amount) > abs(known_intents[digest].amount_x18)
        for digest, amount in filled_by_digest.items()
    ):
        return False
    for intent, state in intents:
        if state != "RECONCILED" or now_ms <= intent.recv_time:
            return False
    return True
