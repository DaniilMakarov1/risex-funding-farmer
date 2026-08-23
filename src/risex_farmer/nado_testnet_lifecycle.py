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
    def assert_exact(
        cls,
        *,
        chain_id: int,
        domain_name: str,
        domain_version: str,
        endpoint: str,
        gateway: str,
        gateway_ws: str,
        archive: str,
        trigger: str,
    ) -> None:
        if {
            "chain_id": chain_id,
            "domain_name": domain_name,
            "domain_version": domain_version,
            "endpoint": endpoint,
            "gateway": gateway,
            "gateway_ws": gateway_ws,
            "archive": archive,
            "trigger": trigger,
        } != cls.as_dict():
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


def decode_subaccount(sender: str) -> tuple[str, str]:
    raw = _hex_bytes(sender, 32)
    name_bytes = raw[20:]
    name_raw, separator, padding = name_bytes.partition(b"\0")
    if not name_raw or (separator and any(padding)):
        raise NadoContractError("sender contains a noncanonical subaccount name")
    try:
        name = name_raw.decode("ascii")
    except UnicodeDecodeError as exc:
        raise NadoContractError("sender subaccount name is not ASCII") from exc
    owner = "0x" + raw[:20].hex()
    if encode_subaccount(owner, name).lower() != sender.lower():
        raise NadoContractError("sender encoding is not canonical")
    return owner, name


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
    validation_product_id: int,
    validation_valid: bool,
) -> bool:
    digest = order_digest(order)
    if validation_valid is not True:
        raise NadoContractError("validate_order did not return valid true")
    if type(validation_product_id) is not int or validation_product_id != order.product_id:
        raise NadoContractError("validate_order product identity mismatch")
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
    fresh: bool
    authoritative_source: str

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

    def assert_authoritative(
        self, *, now_ms: int, after_ms: int | None = None
    ) -> dict[int, Product]:
        products = self.by_id()
        if (
            not self.fresh
            or self.authoritative_source != "engine"
            or self.observed_at_ms > now_ms
            or (after_ms is not None and self.observed_at_ms <= after_ms)
        ):
            raise NadoContractError(
                "fresh authoritative dynamic catalog evidence is required"
            )
        return products


@dataclass(frozen=True)
class AccountSnapshot:
    chain_id: int
    domain_name: str
    domain_version: str
    endpoint: str
    gateway: str
    gateway_ws: str
    archive: str
    trigger: str
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
    after_ms: int | None = None,
) -> dict[int, Product]:
    products = catalog.assert_authoritative(now_ms=now_ms, after_ms=after_ms)
    FixedEnvironment.assert_exact(
        chain_id=account.chain_id,
        domain_name=account.domain_name,
        domain_version=account.domain_version,
        endpoint=account.endpoint,
        gateway=account.gateway,
        gateway_ws=account.gateway_ws,
        archive=account.archive,
        trigger=account.trigger,
    )
    _address_bytes(account.owner)
    encode_subaccount(account.owner, account.subaccount_name)
    if not account.fresh or account.authoritative_source != "engine":
        raise NadoContractError("fresh authoritative engine account evidence is required")
    if account.observed_at_ms > now_ms:
        raise NadoContractError("account evidence is from the future")
    if after_ms is not None and account.observed_at_ms <= after_ms:
        raise NadoContractError("account evidence must postdate the prior write")
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
    sender: str = ""
    owner: str = ""
    subaccount_name: str = ""


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
    submission_idx: int


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


def _assert_intent_evidence_contract(
    intent: OrderIntent,
    catalog: CatalogSnapshot,
    evidence: EngineEvidence,
    *,
    now_ms: int,
) -> dict[int, Product]:
    products = _assert_authoritative_account(
        catalog, evidence.account, now_ms=now_ms, require_flat=False
    )
    _assert_trigger_snapshot(evidence.account, evidence.triggers, now_ms=now_ms)
    expected_sender = encode_subaccount(
        evidence.account.owner, evidence.account.subaccount_name
    )
    if (
        intent.sender.lower() != expected_sender.lower()
        or intent.owner.lower() != evidence.account.owner.lower()
        or intent.subaccount_name != evidence.account.subaccount_name
        or evidence.triggers.owner.lower() != intent.owner.lower()
        or evidence.triggers.subaccount_name != intent.subaccount_name
    ):
        raise NadoContractError("authoritative evidence lifecycle identity mismatch")
    if (
        evidence.observed_at_ms < evidence.account.observed_at_ms
        or evidence.observed_at_ms < evidence.triggers.observed_at_ms
    ):
        raise NadoContractError("reconciliation envelope predates authoritative evidence")
    if (
        evidence.account.observed_at_ms <= intent.recv_time
        or evidence.triggers.observed_at_ms <= intent.recv_time
        or evidence.observed_at_ms <= intent.recv_time
    ):
        raise NadoContractError("authoritative evidence does not postdate signed recv_time")
    if any(order.product_id not in products for order in evidence.orders):
        raise NadoContractError("order evidence references a product outside the catalog")
    if any(fill.product_id not in products for fill in evidence.fills):
        raise NadoContractError("fill evidence references a product outside the catalog")
    return products


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
                sender TEXT NOT NULL,
                owner TEXT NOT NULL,
                subaccount_name TEXT NOT NULL,
                state TEXT NOT NULL
            )
            """
        )
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS nado_lifecycle_identity (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                sender TEXT NOT NULL,
                owner TEXT NOT NULL,
                subaccount_name TEXT NOT NULL
            )
            """
        )
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS nado_lifecycle_state (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                status TEXT NOT NULL
            )
            """
        )
        self._connection.execute(
            """
            INSERT OR IGNORE INTO nado_lifecycle_state (singleton, status)
            VALUES (1, 'RUNNING')
            """
        )
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS nado_fill_evidence (
                order_digest TEXT NOT NULL,
                submission_idx TEXT NOT NULL,
                product_id INTEGER NOT NULL,
                amount_x18 TEXT NOT NULL,
                PRIMARY KEY (order_digest, submission_idx)
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
        payload = json.loads(intent.payload)
        try:
            if intent.kind in {ENTRY, CLOSE}:
                place = payload["place_order"]
                order_payload = place["order"]
                payload_sender = order_payload["sender"]
                payload_nonce = int(order_payload["nonce"])
                recv_time, salt = unpack_order_nonce(payload_nonce)
                payload_order = SyntheticOrderVector(
                    owner=decode_subaccount(payload_sender)[0],
                    subaccount_name=decode_subaccount(payload_sender)[1],
                    sender=payload_sender,
                    product_id=int(place["product_id"]),
                    price_x18=int(order_payload["priceX18"]),
                    amount_x18=int(order_payload["amount"]),
                    expiration=int(order_payload["expiration"]),
                    recv_time=recv_time,
                    salt=salt,
                    nonce=payload_nonce,
                    appendix=int(order_payload["appendix"]),
                )
                if (
                    intent.product_id != payload_order.product_id
                    or intent.amount_x18 != payload_order.amount_x18
                    or intent.appendix != payload_order.appendix
                    or intent.digest.lower() != order_digest(payload_order).lower()
                ):
                    raise NadoContractError("intent fields do not match signed order payload")
            elif intent.kind == CANCEL_ALL:
                tx = payload["cancel_product_orders"]["tx"]
                payload_sender = tx["sender"]
                payload_nonce = int(tx["nonce"])
                if (
                    intent.product_id is not None
                    or tx["productIds"] != []
                    or intent.digest.lower()
                    != _cancel_all_digest(payload_sender, payload_nonce).lower()
                ):
                    raise NadoContractError("intent fields do not match cancel-all payload")
            else:
                raise NadoContractError("unsupported intent kind")
        except (KeyError, TypeError, ValueError) as exc:
            raise NadoContractError("intent payload contract is invalid") from exc
        if not isinstance(payload_sender, str) or payload_nonce != intent.nonce:
            raise NadoContractError("intent payload sender or nonce mismatch")
        owner, subaccount_name = decode_subaccount(payload_sender)
        if intent.sender and intent.sender.lower() != payload_sender.lower():
            raise NadoContractError("intent sender differs from canonical payload")
        if intent.owner and intent.owner.lower() != owner.lower():
            raise NadoContractError("intent owner differs from canonical sender")
        if intent.subaccount_name and intent.subaccount_name != subaccount_name:
            raise NadoContractError("intent subaccount differs from canonical sender")
        try:
            with self._connection:
                states = self.intent_states()
                halted_resting_cancel = (
                    intent.kind == CANCEL_ALL
                    and self.lifecycle_status() == HALTED
                    and not any(existing.kind == CANCEL_ALL for existing, _ in states)
                    and sum(existing.kind == ENTRY for existing, _ in states) == 1
                    and any(
                        existing.kind == ENTRY and state == "RESTING"
                        for existing, state in states
                    )
                )
                if self.lifecycle_status() != RUNNING and not halted_resting_cancel:
                    raise NadoContractError("lifecycle is at a durable manual gate")
                if intent.kind == ENTRY and self.count_kind(ENTRY):
                    raise NadoContractError("one lifecycle permits exactly one entry")
                if intent.kind == CANCEL_ALL and (
                    sum(existing.kind == ENTRY for existing, _ in states) != 1
                    or any(existing.kind == CANCEL_ALL for existing, _ in states)
                    or not any(
                        existing.kind == ENTRY
                        and state in {"RESTING", "PARTIAL"}
                        for existing, state in states
                    )
                ):
                    raise NadoContractError("cancel-all intent ordering is invalid")
                if intent.kind == CLOSE and (
                    sum(existing.kind == ENTRY for existing, _ in states) != 1
                    or any(state != "RECONCILED" for _, state in states)
                    or sum(existing.kind == CLOSE for existing, _ in states)
                    >= MAX_CLOSE_ATTEMPTS
                ):
                    raise NadoContractError("close intent ordering is invalid")
                self._connection.execute(
                    """
                    INSERT OR IGNORE INTO nado_lifecycle_identity (
                        singleton, sender, owner, subaccount_name
                    ) VALUES (1, ?, ?, ?)
                    """,
                    (payload_sender.lower(), owner.lower(), subaccount_name),
                )
                existing_identity = self._connection.execute(
                    """
                    SELECT sender, owner, subaccount_name
                    FROM nado_lifecycle_identity WHERE singleton = 1
                    """
                ).fetchone()
                if existing_identity != (
                    payload_sender.lower(), owner.lower(), subaccount_name
                ):
                    raise NadoContractError(
                        "lifecycle subaccount identity is immutable"
                    )
                self._connection.execute(
                    """
                    INSERT INTO nado_intents (
                        digest, nonce, recv_time, kind, product_id, payload,
                        amount_x18, appendix, notional_x18, clamp_expected,
                        snapshot_id, snapshot_observed_at_ms,
                        starting_position_x18, sender, owner, subaccount_name, state
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PREPARED')
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
                        payload_sender.lower(),
                        owner.lower(),
                        subaccount_name,
                    ),
                )
        except sqlite3.IntegrityError as exc:
            raise NadoContractError("digest or nonce was already consumed") from exc

    def get(self, digest: str) -> OrderIntent:
        row = self._connection.execute(
            """
            SELECT kind, product_id, nonce, recv_time, digest, payload,
                   amount_x18, appendix, notional_x18, clamp_expected,
                   snapshot_id, snapshot_observed_at_ms, starting_position_x18,
                   sender, owner, subaccount_name
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
            sender=row[13],
            owner=row[14],
            subaccount_name=row[15],
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

    def _mark_ambiguous(self, digest: str) -> None:
        self._mark(digest, "AMBIGUOUS")
        self.halt()

    def _mark_reconciled(self, digest: str) -> None:
        self._mark(digest, "RECONCILED")

    def _mark_rejected(self, digest: str) -> None:
        self._mark(digest, "REJECTED")
        if self.get(digest).kind == ENTRY:
            self.halt()

    def lifecycle_status(self) -> str:
        row = self._connection.execute(
            "SELECT status FROM nado_lifecycle_state WHERE singleton = 1"
        ).fetchone()
        if row is None:
            raise NadoContractError("durable lifecycle status is missing")
        return str(row[0])

    def halt(self) -> None:
        with self._connection:
            self._connection.execute(
                """
                UPDATE nado_lifecycle_state SET status = 'HALTED'
                WHERE singleton = 1 AND status != 'COMPLETE'
                """
            )

    def _mark_complete(self) -> None:
        with self._connection:
            cursor = self._connection.execute(
                """
                UPDATE nado_lifecycle_state SET status = 'COMPLETE'
                WHERE singleton = 1 AND status = 'RUNNING'
                """
            )
        if cursor.rowcount != 1:
            raise NadoContractError("halted lifecycle cannot become complete")

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
            self._mark_ambiguous(intent.digest)
            raise
        self._mark(intent.digest, "DISPATCHED")
        return result

    def intents(self) -> tuple[tuple[OrderIntent, str], ...]:
        rows = self._connection.execute(
            "SELECT digest, state FROM nado_intents ORDER BY rowid"
        ).fetchall()
        return tuple((self.get(row[0]), str(row[1])) for row in rows)

    def intent_states(self) -> tuple[tuple[OrderIntent, str], ...]:
        return self.intents()

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

    @staticmethod
    def _canonical_fill_map(
        fills: tuple[FillEvidence, ...],
    ) -> dict[tuple[str, int], FillEvidence]:
        result: dict[tuple[str, int], FillEvidence] = {}
        for fill in fills:
            _hex_bytes(fill.digest, 32)
            if type(fill.submission_idx) is not int or not 0 <= fill.submission_idx < 2**64:
                raise NadoContractError("fill submission_idx is outside uint64")
            if fill.amount_x18 == 0:
                raise NadoContractError("fill amount must be nonzero")
            identity = (fill.digest.lower(), fill.submission_idx)
            if identity in result:
                raise NadoContractError("duplicate fill identity in authoritative evidence")
            result[identity] = fill
        return result

    def _record_fill_evidence(self, fills: tuple[FillEvidence, ...]) -> None:
        canonical = self._validate_global_fills(fills)
        combined = self.persisted_fill_map()
        for identity, fill in canonical.items():
            expected = (fill.product_id, fill.amount_x18)
            existing = combined.get(identity)
            if existing is not None and existing != expected:
                raise NadoContractError("persisted fill identity is contradictory")
            combined[identity] = expected
        self._validate_fill_map(combined)
        with self._connection:
            for (digest, submission_idx), fill in canonical.items():
                self._connection.execute(
                    """
                    INSERT OR IGNORE INTO nado_fill_evidence (
                        order_digest, submission_idx, product_id, amount_x18
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (digest, str(submission_idx), fill.product_id, str(fill.amount_x18)),
                )

    def _validate_fill_map(
        self, fills: Mapping[tuple[str, int], tuple[int, int]]
    ) -> None:
        intents = {intent.digest.lower(): intent for intent, _ in self.intents()}
        cumulative: dict[str, int] = {}
        for (digest, _), (product_id, amount_x18) in fills.items():
            intent = intents.get(digest)
            if (
                intent is None
                or intent.product_id != product_id
                or amount_x18 == 0
                or (amount_x18 > 0) != (intent.amount_x18 > 0)
            ):
                raise NadoContractError("fill does not belong to a lifecycle intent")
            cumulative[digest] = cumulative.get(digest, 0) + amount_x18
        if any(
            abs(amount) > abs(intents[digest].amount_x18)
            for digest, amount in cumulative.items()
        ):
            raise NadoContractError("cumulative fill exceeds signed intent amount")

    def _validate_global_fills(
        self, fills: tuple[FillEvidence, ...]
    ) -> dict[tuple[str, int], FillEvidence]:
        canonical = self._canonical_fill_map(fills)
        self._validate_fill_map(
            {
                identity: (fill.product_id, fill.amount_x18)
                for identity, fill in canonical.items()
            }
        )
        return canonical

    def _validate_order_evidence(self, evidence: EngineEvidence) -> None:
        regular: dict[str, int] = {}
        for product_id, digests in evidence.account.regular_orders_by_product.items():
            for digest in digests:
                normalized = digest.lower()
                _hex_bytes(normalized, 32)
                if normalized in regular:
                    raise NadoContractError("duplicate regular-order identity")
                regular[normalized] = product_id
        intents = {intent.digest.lower(): intent for intent, _ in self.intents()}
        observed: dict[str, OrderEvidence] = {}
        persisted_fills = self.persisted_fill_map()
        for order in evidence.orders:
            normalized = order.digest.lower()
            _hex_bytes(normalized, 32)
            if normalized in observed:
                raise NadoContractError("duplicate open-order evidence identity")
            intent = intents.get(normalized)
            filled = sum(
                amount
                for (fill_digest, _), (_, amount) in persisted_fills.items()
                if fill_digest == normalized
            )
            if (
                intent is None
                or intent.kind != ENTRY
                or order.status != "OPEN"
                or order.product_id != intent.product_id
                or order.nonce != intent.nonce
                or regular.get(normalized) != intent.product_id
                or order.amount_x18 == 0
                or (order.amount_x18 > 0) != (intent.amount_x18 > 0)
                or filled + order.amount_x18 != intent.amount_x18
            ):
                raise NadoContractError(
                    "open-order evidence does not match a lifecycle entry"
                )
            observed[normalized] = order
        if set(observed) != set(regular):
            raise NadoContractError(
                "engine open-order representations disagree"
            )

    def persisted_fill_map(self) -> dict[tuple[str, int], tuple[int, int]]:
        rows = self._connection.execute(
            """
            SELECT order_digest, submission_idx, product_id, amount_x18
            FROM nado_fill_evidence
            """
        ).fetchall()
        return {
            (str(row[0]), int(row[1])): (int(row[2]), int(row[3]))
            for row in rows
        }

    def _contradictory(self) -> Reconciliation:
        self.halt()
        return Reconciliation.CONTRADICTORY

    def reconcile(
        self,
        digest: str,
        *,
        catalog: CatalogSnapshot,
        evidence: EngineEvidence,
    ) -> Reconciliation:
        intent = self.get(digest)
        account = evidence.account
        try:
            _assert_intent_evidence_contract(
                intent, catalog, evidence, now_ms=evidence.observed_at_ms
            )
            self._record_fill_evidence(evidence.fills)
            self._validate_order_evidence(evidence)
        except NadoContractError:
            return self._contradictory()
        matching_orders = [order for order in evidence.orders if order.digest.lower() == digest.lower()]
        matching_fills = [fill for fill in evidence.fills if fill.digest.lower() == digest.lower()]
        if any(
            order.product_id != intent.product_id or order.nonce != intent.nonce
            for order in matching_orders
        ) or any(fill.product_id != intent.product_id for fill in matching_fills):
            return self._contradictory()
        if len(matching_orders) > 1:
            return self._contradictory()
        filled = sum(fill.amount_x18 for fill in matching_fills)
        position = account.cross_perp_amounts_x18.get(intent.product_id or -1, 0)
        if intent.kind == CANCEL_ALL:
            if matching_fills or matching_orders:
                return self._contradictory()
            all_zero = not any(account.regular_orders_by_product.values())
            exact_cancel = (
                evidence.exact_cancel_digest is not None
                and evidence.exact_cancel_digest.lower() == digest.lower()
            )
            if all_zero and (exact_cancel or evidence.observed_at_ms > intent.recv_time):
                self._mark_reconciled(digest)
                return Reconciliation.CANCELLED
            self._mark_ambiguous(digest)
            return Reconciliation.AMBIGUOUS
        if matching_orders and matching_orders[0].status != "OPEN":
            return self._contradictory()
        if matching_orders:
            if intent.kind == CLOSE:
                return self._contradictory()
            open_order = matching_orders[0]
            regular_digests = {
                order_digest.lower()
                for order_digests in account.regular_orders_by_product.values()
                for order_digest in order_digests
            }
            if (
                open_order.amount_x18 == 0
                or (open_order.amount_x18 > 0) != (intent.amount_x18 > 0)
                or filled + open_order.amount_x18 != intent.amount_x18
                or intent.digest.lower() not in regular_digests
            ):
                return self._contradictory()
        exact_terminal = (
            evidence.terminal_digest is not None
            and evidence.terminal_digest.lower() == digest.lower()
        )
        if exact_terminal and evidence.terminal_status not in {
            "CANCELLED", "EXPIRED", "REJECTED"
        }:
            return self._contradictory()
        if exact_terminal and matching_orders:
            return self._contradictory()
        if matching_fills:
            if any(
                (fill.amount_x18 > 0) != (intent.amount_x18 > 0)
                for fill in matching_fills
            ) or abs(filled) > abs(intent.amount_x18):
                return self._contradictory()
            if exact_terminal and evidence.terminal_status == "REJECTED":
                return self._contradictory()
            expected_position = filled
            if intent.kind == CLOSE:
                expected_position = intent.starting_position_x18 + filled
            if position != expected_position:
                return self._contradictory()
            if intent.kind == CLOSE and position == 0:
                self._mark_reconciled(digest)
                return Reconciliation.FILLED
            if intent.kind == ENTRY and abs(filled) >= abs(intent.amount_x18):
                self._mark_reconciled(digest)
                return Reconciliation.FILLED
            if intent.kind == ENTRY and exact_terminal:
                self._mark_reconciled(digest)
                return Reconciliation(evidence.terminal_status)
            if intent.kind == CLOSE and not matching_orders:
                self._mark_reconciled(digest)
                self.halt()
                return Reconciliation.PARTIAL
            self._mark(digest, "PARTIAL")
            if not (intent.kind == ENTRY and matching_orders):
                self.halt()
            return Reconciliation.PARTIAL
        if matching_orders:
            self._mark(digest, "RESTING")
            return Reconciliation.RESTING
        if evidence.exact_rejection_digest and evidence.exact_rejection_digest.lower() == digest.lower():
            expected_position = intent.starting_position_x18 if intent.kind == CLOSE else 0
            if position != expected_position:
                return self._contradictory()
            if intent.kind == CLOSE:
                self._mark_reconciled(digest)
                self.halt()
            else:
                self._mark_rejected(digest)
            return Reconciliation.REJECTED
        if evidence.terminal_digest and evidence.terminal_digest.lower() == digest.lower():
            terminal = evidence.terminal_status
            if terminal not in {"CANCELLED", "EXPIRED", "REJECTED"}:
                return self._contradictory()
            expected_position = intent.starting_position_x18 if intent.kind == CLOSE else 0
            if position != expected_position:
                return self._contradictory()
            if terminal in {"CANCELLED", "EXPIRED"} or intent.kind == CLOSE:
                self._mark_reconciled(digest)
                if (
                    intent.kind == CLOSE
                    and terminal in {"CANCELLED", "EXPIRED"}
                    and self.count_kind(CLOSE) >= MAX_CLOSE_ATTEMPTS
                    and position != 0
                ):
                    self.halt()
            else:
                self._mark_rejected(digest)
            if intent.kind == CLOSE and terminal == "REJECTED":
                self.halt()
            return Reconciliation(terminal)
        if evidence.duplicate_digest:
            self._mark_ambiguous(digest)
            return Reconciliation.AMBIGUOUS
        self._mark_ambiguous(digest)
        return Reconciliation.AMBIGUOUS

    def write_allowed(
        self,
        digest: str,
        *,
        catalog: CatalogSnapshot,
        now_ms: int,
        evidence: EngineEvidence,
    ) -> bool:
        intent = self.get(digest)
        if self.lifecycle_status() != RUNNING:
            return False
        latest_recv_time = self.latest_recv_time()
        if latest_recv_time is None:
            return False
        try:
            _assert_intent_evidence_contract(intent, catalog, evidence, now_ms=now_ms)
            _assert_authoritative_account(
                catalog, evidence.account, now_ms=now_ms, require_flat=False,
                after_ms=latest_recv_time,
            )
            if (
                evidence.triggers.observed_at_ms <= latest_recv_time
                or evidence.observed_at_ms <= latest_recv_time
            ):
                return False
        except NadoContractError:
            return False
        try:
            canonical_fills = self._validate_global_fills(evidence.fills)
            self._validate_order_evidence(evidence)
        except NadoContractError:
            self.halt()
            return False
        if {
            identity: (fill.product_id, fill.amount_x18)
            for identity, fill in canonical_fills.items()
        } != self.persisted_fill_map():
            return False
        return (
            self.state(digest) == "RECONCILED"
            and all(state == "RECONCILED" for _, state in self.intent_states())
            and self.count_kind(CLOSE) < MAX_CLOSE_ATTEMPTS
            and now_ms > intent.recv_time
            and not any(evidence.account.regular_orders_by_product.values())
            and not evidence.triggers.active_digests
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

    @property
    def status(self) -> str:
        durable = self.store.lifecycle_status()
        if durable == COMPLETE:
            return COMPLETE
        if durable == HALTED:
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
        entry_records = [
            (intent, state)
            for intent, state in self.store.intent_states()
            if intent.kind == ENTRY
        ]
        halted_resting_cancel = (
            self.status == HALTED
            and len(entry_records) == 1
            and entry_records[0][1] == "RESTING"
            and not self.store.count_kind(CANCEL_ALL)
        )
        if halted_resting_cancel:
            self._assert_recv_fence(recv_time=recv_time, now_ms=now_ms)
        else:
            self._assert_next_state_write(recv_time=recv_time, now_ms=now_ms)
        if self.store.count_kind(CANCEL_ALL):
            self.store.halt()
            raise NadoContractError("parallel or repeated cancel-all is prohibited")
        if len(entry_records) != 1 or entry_records[0][1] not in {
            "RESTING", "PARTIAL"
        }:
            self.store.halt()
            raise NadoContractError("cancel-all requires one unresolved entry")
        prior_recv_time = self.store.latest_recv_time()
        _assert_authoritative_account(
            catalog, account, now_ms=now_ms, require_flat=False,
            after_ms=prior_recv_time,
        )
        _assert_trigger_snapshot(account, triggers, now_ms=now_ms)
        if prior_recv_time is None or triggers.observed_at_ms <= prior_recv_time:
            raise NadoContractError("cancel-all evidence does not postdate prior write")
        if triggers.active_digests or triggers.contradictions:
            raise NadoContractError("trigger state blocks cancel-all")
        regular_digests = {
            digest.lower()
            for digests in account.regular_orders_by_product.values()
            for digest in digests
        }
        if entry_records[0][0].digest.lower() not in regular_digests:
            raise NadoContractError("cancel-all requires the exact unresolved entry order")
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
            sender=sender.lower(),
            owner=account.owner.lower(),
            subaccount_name=account.subaccount_name,
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
        validation_product_id: int,
        validation_valid: bool,
        now_ms: int,
    ) -> OrderIntent:
        if order.appendix != POST_ONLY_APPENDIX:
            raise NadoContractError("entry must be post-only")
        expected_sender = encode_subaccount(account.owner, account.subaccount_name)
        if (
            order.sender.lower() != expected_sender.lower()
            or order.owner.lower() != account.owner.lower()
            or order.subaccount_name != account.subaccount_name
        ):
            raise NadoContractError("signed order and preflight subaccount identity mismatch")
        self._assert_next_state_write(recv_time=order.recv_time, now_ms=now_ms)
        verify_signed_validation(
            order,
            signature=signature,
            validation_product_id=validation_product_id,
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
            sender=order.sender.lower(),
            owner=order.owner.lower(),
            subaccount_name=order.subaccount_name,
        )
        self.store.prepare(intent)
        return intent

    def _assert_next_state_write(self, *, recv_time: int, now_ms: int) -> None:
        if self.status == HALTED:
            raise NadoContractError("lifecycle is halted at the manual gate")
        if self.status == COMPLETE:
            raise NadoContractError("lifecycle is already complete")
        self._assert_recv_fence(recv_time=recv_time, now_ms=now_ms)

    def _assert_recv_fence(self, *, recv_time: int, now_ms: int) -> None:
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
            states = self.store.intent_states()
            if sum(intent.kind == ENTRY for intent, _ in states) != 1:
                raise NadoContractError("close requires exactly one lifecycle entry")
            if any(state != "RECONCILED" for _, state in states):
                raise NadoContractError("all prior lifecycle intents must be reconciled")
            if self.store.count_kind(CLOSE) >= MAX_CLOSE_ATTEMPTS:
                raise NadoContractError("three close attempts are exhausted")
            products = _assert_authoritative_account(
                catalog, account, now_ms=now_ms, require_flat=False,
                after_ms=self.store.latest_recv_time(),
            )
            _assert_trigger_snapshot(account, triggers, now_ms=now_ms)
            if triggers.observed_at_ms <= (self.store.latest_recv_time() or -1):
                raise NadoContractError("close evidence does not postdate prior write")
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
                sender=order.sender.lower(),
                owner=order.owner.lower(),
                subaccount_name=order.subaccount_name,
            )
            self.store.prepare(intent)
            return intent
        except NadoContractError:
            self.store.halt()
            raise


def completion_barrier(
    *,
    store: IntentStore,
    catalog: CatalogSnapshot,
    evidence: EngineEvidence,
    now_ms: int,
) -> bool:
    if store.lifecycle_status() != RUNNING:
        return False
    intents = store.intents()
    if sum(intent.kind == ENTRY for intent, _ in intents) != 1:
        return False
    latest_recv_time = max(intent.recv_time for intent, _ in intents)
    try:
        _assert_authoritative_account(
            catalog, evidence.account, now_ms=now_ms, require_flat=True,
            after_ms=latest_recv_time,
        )
        _assert_trigger_snapshot(evidence.account, evidence.triggers, now_ms=now_ms)
    except NadoContractError:
        return False
    if (
        evidence.observed_at_ms < evidence.account.observed_at_ms
        or evidence.account.observed_at_ms <= latest_recv_time
        or evidence.triggers.observed_at_ms <= latest_recv_time
        or evidence.observed_at_ms <= latest_recv_time
    ):
        return False
    if any(evidence.account.regular_orders_by_product.values()):
        return False
    if evidence.triggers.active_digests or evidence.orders:
        return False
    try:
        for intent, _ in intents:
            _assert_intent_evidence_contract(
                intent, catalog, evidence, now_ms=now_ms
            )
    except NadoContractError:
        return False
    known_intents = {intent.digest.lower(): intent for intent, _ in intents}
    try:
        canonical_fills = store._validate_global_fills(evidence.fills)
        store._validate_order_evidence(evidence)
    except NadoContractError:
        store.halt()
        return False
    observed_fill_map = {
        identity: (fill.product_id, fill.amount_x18)
        for identity, fill in canonical_fills.items()
    }
    if observed_fill_map != store.persisted_fill_map():
        return False
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
    try:
        store._mark_complete()
    except NadoContractError:
        return False
    return True
