"""Fixture-only Nado testnet lifecycle contract.

This module deliberately contains no network transport, credential loader, CLI,
or normal Farmer import.  Its only execution boundary is an injected fixture
callable used to prove PREPARED-before-dispatch ordering.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Callable, Mapping

from eth_hash.auto import keccak
from eth_keys import keys
from eth_keys.datatypes import Signature


X18 = 10**18
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

RISEX_VENUE = "RISEX"
NADO_VENUE = "NADO"
LONG = "LONG"
SHORT = "SHORT"
FUNDING_APPLIED = "APPLIED"
FUNDING_SKIPPED_POSITION_NOT_OPEN = "SKIPPED_POSITION_NOT_OPEN"
FUNDING_SKIPPED_POSITION_CLOSED = "SKIPPED_POSITION_CLOSED"
FUNDING_UNRESOLVED = "UNRESOLVED"
# These are the repository's existing normalized settlement statuses; this
# Nado contract does not invent a venue-specific funding status.
FUNDING_STATUSES = frozenset({
    FUNDING_APPLIED,
    FUNDING_SKIPPED_POSITION_NOT_OPEN,
    FUNDING_SKIPPED_POSITION_CLOSED,
    FUNDING_UNRESOLVED,
})
FUNDING_COMPLETION_STATUSES = frozenset({
    FUNDING_APPLIED,
})

# Pinned SDK appendix packing: version=1, order type in bits 9..10,
# reduce-only in bit 11.
POST_ONLY_APPENDIX = 1 | (3 << 9)
IOC_APPENDIX = 1 | (1 << 9)
IOC_REDUCE_ONLY_APPENDIX = 1 | (1 << 9) | (1 << 11)

EXECUTE_VENUE_REJECTION = "VENUE_REJECTION"
EXECUTE_TRANSPORT_AMBIGUITY = "TRANSPORT_AMBIGUITY"
EXECUTE_RESPONSE_AMBIGUITY = "RESPONSE_AMBIGUITY"
EXECUTE_FAILURE_CLASSES = {
    EXECUTE_VENUE_REJECTION,
    EXECUTE_TRANSPORT_AMBIGUITY,
    EXECUTE_RESPONSE_AMBIGUITY,
}


class NadoContractError(ValueError):
    """A fail-closed contract or evidence violation."""


class ExecuteFailure(RuntimeError):
    """Sanitized execute outcome safe to retain beside a durable intent."""

    def __init__(self, failure_class: str, venue_code: int | None = None) -> None:
        if failure_class not in EXECUTE_FAILURE_CLASSES:
            raise NadoContractError("execute failure class rejected")
        if venue_code is not None and (
            type(venue_code) is not int or venue_code < 0
        ):
            raise NadoContractError("execute venue code rejected")
        if failure_class == EXECUTE_VENUE_REJECTION and venue_code is None:
            raise NadoContractError("terminal venue rejection requires a code")
        if failure_class != EXECUTE_VENUE_REJECTION and venue_code is not None:
            raise NadoContractError("ambiguous execute failure cannot retain a venue code")
        self.failure_class = failure_class
        self.venue_code = venue_code
        super().__init__(f"execute failure: {failure_class}")


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
    perp_last_cumulative_funding_x18: Mapping[int, int] = field(default_factory=dict)


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


def smallest_executable_amount(
    product: Product, *, prices_x18: tuple[int, ...]
) -> int:
    """Return the least step-aligned amount executable at every supplied price."""
    product.assert_contract()
    if product.minimum_amount_x18 % product.step_x18:
        raise NadoContractError("minimum amount is off the x18 product step")
    if not prices_x18:
        raise NadoContractError("at least one execution price is required")
    for price_x18 in prices_x18:
        if price_x18 <= 0 or price_x18 % product.tick_x18:
            raise NadoContractError("price is off the x18 product tick")
    limiting_price = min(prices_x18)
    required_for_notional = (
        product.minimum_notional_x18 * X18 + limiting_price - 1
    ) // limiting_price
    required = max(product.minimum_amount_x18, required_for_notional)
    return ((required + product.step_x18 - 1) // product.step_x18) * product.step_x18


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
    amount = smallest_executable_amount(
        product, prices_x18=(entry_price_x18, worst_close_price_x18)
    )
    entry_notional = _notional_x18(entry_price_x18, amount)
    close_notional = _notional_x18(worst_close_price_x18, amount)
    if min(entry_notional, close_notional) < product.minimum_notional_x18:
        raise NadoContractError("order is below product minimum notional")
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


def _exact_decimal(value: object, label: str, *, positive: bool = False) -> Decimal:
    if isinstance(value, bool) or isinstance(value, float) or not isinstance(
        value, (str, int, Decimal)
    ):
        raise NadoContractError(f"{label} must be an exact decimal")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise NadoContractError(f"{label} must be an exact decimal") from None
    if not parsed.is_finite() or (positive and parsed <= 0):
        raise NadoContractError(f"{label} must be finite and positive")
    return parsed


def _decimal_text(value: object, label: str, *, positive: bool = False) -> str:
    parsed = _exact_decimal(value, label, positive=positive)
    return format(parsed.normalize(), "f")


def _funding_quantity_x18(value: object, label: str = "funding quantity") -> int:
    parsed = _exact_decimal(value, label, positive=True)
    scaled = parsed * X18
    if scaled != scaled.to_integral_value():
        raise NadoContractError(f"{label} is not exactly representable in x18")
    return int(scaled)


def _safe_identifier(value: object, label: str) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise NadoContractError(f"{label} is invalid")
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
        raise NadoContractError(f"{label} is invalid")
    return value


def _hash_text(value: object, label: str) -> str:
    if type(value) is not str or not value.startswith("0x"):
        raise NadoContractError(f"{label} is invalid")
    try:
        raw = bytes.fromhex(value[2:])
    except ValueError:
        raise NadoContractError(f"{label} is invalid") from None
    if len(raw) != 32:
        raise NadoContractError(f"{label} is invalid")
    return "0x" + raw.hex()


def _strict_int(value: object, label: str, *, positive: bool = False) -> int:
    if type(value) is not int or (positive and value <= 0):
        raise NadoContractError(f"{label} is invalid")
    return value


@dataclass(frozen=True)
class FundingLegBinding:
    """One route leg with an explicit, exact canonical quantity."""

    venue: str
    market: str
    direction: str
    raw_quantity: object
    base_multiplier: object
    canonical_quantity: object

    def assert_contract(self) -> None:
        if self.venue not in {RISEX_VENUE, NADO_VENUE}:
            raise NadoContractError("funding leg venue is invalid")
        _safe_identifier(self.market, "funding leg market")
        if self.direction not in {LONG, SHORT}:
            raise NadoContractError("funding leg direction is invalid")
        raw = _exact_decimal(self.raw_quantity, "funding leg raw quantity", positive=True)
        multiplier = _exact_decimal(
            self.base_multiplier, "funding leg base multiplier", positive=True
        )
        canonical = _exact_decimal(
            self.canonical_quantity, "funding leg canonical quantity", positive=True
        )
        if raw * multiplier != canonical:
            raise NadoContractError(
                "funding leg canonical quantity is not bound to raw quantity"
            )


@dataclass(frozen=True)
class FundingRouteBinding:
    """Immutable cross-venue route identity persisted before funding reads."""

    canonical_asset: str
    risex_leg: FundingLegBinding
    nado_leg: FundingLegBinding
    nado_product_id: int
    settlement_at_ms: int

    def assert_contract(self) -> None:
        _safe_identifier(self.canonical_asset, "funding canonical asset")
        self.risex_leg.assert_contract()
        self.nado_leg.assert_contract()
        if self.risex_leg.venue != RISEX_VENUE or self.nado_leg.venue != NADO_VENUE:
            raise NadoContractError("funding route must contain RISEx and Nado legs")
        if self.risex_leg.direction == self.nado_leg.direction:
            raise NadoContractError("funding legs must have opposite directions")
        if _exact_decimal(
            self.risex_leg.canonical_quantity, "RISEx canonical quantity", positive=True
        ) != _exact_decimal(
            self.nado_leg.canonical_quantity, "Nado canonical quantity", positive=True
        ):
            raise NadoContractError(
                "funding legs must bind one exact canonical quantity"
            )
        product_verifier(_strict_int(self.nado_product_id, "Nado product id"))
        _strict_int(self.settlement_at_ms, "funding settlement timestamp", positive=True)

    @property
    def canonical_quantity(self) -> Decimal:
        self.assert_contract()
        return _exact_decimal(
            self.risex_leg.canonical_quantity,
            "funding canonical quantity",
            positive=True,
        )


@dataclass(frozen=True)
class JournalIdentity:
    """Immutable pre-dispatch identity of one venue lifecycle journal."""

    venue: str
    run_id: str
    store_identity: str
    account_id: str

    def assert_contract(self) -> None:
        if self.venue not in {RISEX_VENUE, NADO_VENUE}:
            raise NadoContractError("journal venue is invalid")
        _safe_identifier(self.run_id, "journal run identity")
        _safe_identifier(self.store_identity, "journal store identity")
        _safe_identifier(self.account_id, "journal account identity")


@dataclass(frozen=True)
class TerminalEvidence:
    """Authoritative zero-order/flat terminal proof bound to its journal."""

    journal: JournalIdentity
    status: str
    observed_at_ms: int
    journal_content_sha256: str
    zero_regular_orders: bool
    zero_trigger_orders: bool
    exact_flat: bool
    unresolved_write_identities: tuple[str, ...]
    evidence_digest: str
    authoritative: bool = True

    def assert_contract(self) -> None:
        self.journal.assert_contract()
        if self.status != COMPLETE:
            raise NadoContractError("terminal evidence is not complete")
        _strict_int(self.observed_at_ms, "terminal evidence timestamp", positive=True)
        _hash_text(self.journal_content_sha256, "terminal journal content digest")
        if (
            self.zero_regular_orders is not True
            or self.zero_trigger_orders is not True
            or self.exact_flat is not True
        ):
            raise NadoContractError("terminal evidence does not prove zero and flat state")
        if type(self.unresolved_write_identities) is not tuple or any(
            not _safe_identifier(item, "unresolved write identity")
            for item in self.unresolved_write_identities
        ):
            raise NadoContractError("terminal evidence has unresolved writes")
        if self.authoritative is not True:
            raise NadoContractError("terminal evidence is not authoritative")
        if _hash_text(self.evidence_digest, "terminal evidence digest") != (
            "0x" + terminal_evidence_digest(self)
        ):
            raise NadoContractError("terminal evidence digest does not bind its contents")


@dataclass(frozen=True)
class RisexTerminalEvidence:
    """The only external final-evidence seam accepted by the Nado runner."""

    route: FundingRouteBinding
    journal: JournalIdentity
    terminal: TerminalEvidence
    round_index: int

    def assert_contract(self) -> None:
        if (
            type(self.route) is not FundingRouteBinding
            or type(self.journal) is not JournalIdentity
            or type(self.terminal) is not TerminalEvidence
            or type(self.round_index) is not int
            or self.round_index not in {1, 2}
        ):
            raise NadoContractError("RISEx terminal provenance is invalid")
        try:
            self.route.assert_contract()
            self.journal.assert_contract()
            self.terminal.assert_contract()
        except NadoContractError:
            raise
        if self.terminal.journal != self.journal:
            raise NadoContractError("RISEx terminal journal provenance is invalid")


def _funding_leg_payload(leg: FundingLegBinding) -> dict[str, object]:
    return {
        "venue": leg.venue,
        "market": leg.market,
        "direction": leg.direction,
        "raw_quantity": _decimal_text(leg.raw_quantity, "funding leg raw quantity", positive=True),
        "base_multiplier": _decimal_text(
            leg.base_multiplier, "funding leg base multiplier", positive=True
        ),
        "canonical_quantity": _decimal_text(
            leg.canonical_quantity, "funding leg canonical quantity", positive=True
        ),
    }


def _funding_route_payload(route: FundingRouteBinding) -> dict[str, object]:
    route.assert_contract()
    return {
        "canonical_asset": route.canonical_asset,
        "risex_leg": _funding_leg_payload(route.risex_leg),
        "nado_leg": _funding_leg_payload(route.nado_leg),
        "nado_product_id": route.nado_product_id,
        "settlement_at_ms": route.settlement_at_ms,
    }


def _journal_payload(journal: JournalIdentity) -> dict[str, object]:
    journal.assert_contract()
    return {
        "venue": journal.venue,
        "run_id": journal.run_id,
        "store_identity": journal.store_identity,
        "account_id": journal.account_id,
    }


def _terminal_payload(evidence: TerminalEvidence) -> dict[str, object]:
    return {
        "journal": _journal_payload(evidence.journal),
        "status": evidence.status,
        "observed_at_ms": evidence.observed_at_ms,
        "journal_content_sha256": _hash_text(
            evidence.journal_content_sha256,
            "terminal journal content digest",
        ),
        "zero_regular_orders": evidence.zero_regular_orders,
        "zero_trigger_orders": evidence.zero_trigger_orders,
        "exact_flat": evidence.exact_flat,
        "unresolved_write_identities": list(evidence.unresolved_write_identities),
        "authoritative": evidence.authoritative,
    }


def terminal_evidence_digest(evidence: TerminalEvidence) -> str:
    return hashlib.sha256(canonical_payload(_terminal_payload(evidence))).hexdigest()


@dataclass(frozen=True)
class FundingBoundaryBinding:
    route: FundingRouteBinding
    risex_journal: JournalIdentity
    nado_journal: JournalIdentity

    def assert_contract(self) -> None:
        self.route.assert_contract()
        self.risex_journal.assert_contract()
        self.nado_journal.assert_contract()
        if (
            self.risex_journal.venue != RISEX_VENUE
            or self.nado_journal.venue != NADO_VENUE
        ):
            raise NadoContractError("funding journals must bind RISEx and Nado")


@dataclass(frozen=True)
class CrossRunAttestation:
    """Both venue terminal proofs, never a free-standing summary."""

    route: FundingRouteBinding
    risex_journal: JournalIdentity
    nado_journal: JournalIdentity
    risex_terminal: TerminalEvidence
    nado_terminal: TerminalEvidence
    attestation_digest: str

    def assert_contract(self) -> None:
        self.route.assert_contract()
        self.risex_journal.assert_contract()
        self.nado_journal.assert_contract()
        self.risex_terminal.assert_contract()
        self.nado_terminal.assert_contract()
        if (
            self.risex_journal.venue != RISEX_VENUE
            or self.nado_journal.venue != NADO_VENUE
            or self.risex_terminal.journal != self.risex_journal
            or self.nado_terminal.journal != self.nado_journal
        ):
            raise NadoContractError("cross-run attestation journal binding is invalid")
        if _hash_text(self.attestation_digest, "cross-run attestation digest") != (
            "0x" + cross_run_attestation_digest(self)
        ):
            raise NadoContractError("cross-run attestation digest does not bind its contents")


def _attestation_payload(attestation: CrossRunAttestation) -> dict[str, object]:
    return {
        "route": _funding_route_payload(attestation.route),
        "risex_journal": _journal_payload(attestation.risex_journal),
        "nado_journal": _journal_payload(attestation.nado_journal),
        "risex_terminal_digest": _hash_text(
            attestation.risex_terminal.evidence_digest, "RISEx terminal evidence digest"
        ),
        "nado_terminal_digest": _hash_text(
            attestation.nado_terminal.evidence_digest, "Nado terminal evidence digest"
        ),
    }


def cross_run_attestation_digest(attestation: CrossRunAttestation) -> str:
    return hashlib.sha256(canonical_payload(_attestation_payload(attestation))).hexdigest()


FUNDING_BOUNDARY_INTERVAL_MS = 3_600_000
FUNDING_BLOCKED_MISSING = "POST_BOUNDARY_FUNDING_EVIDENCE_MISSING"
FUNDING_BLOCKED_CONTRADICTORY = "POST_BOUNDARY_FUNDING_EVIDENCE_CONTRADICTORY"
FUNDING_BLOCKER_REASONS = frozenset({
    FUNDING_BLOCKED_MISSING,
    FUNDING_BLOCKED_CONTRADICTORY,
})

# These are durable coordination facts, not funding outcomes.  The sequence is
# intentionally a closed-world prefix so a reopened journal can tell which
# read-only boundary transition was last proven without retaining a payload.
FUNDING_PROGRESSION_PUBLIC_EVENT_WAIT_STARTED = "PUBLIC_EVENT_WAIT_STARTED"
FUNDING_PROGRESSION_PUBLIC_EVENT_ACCEPTED = "PUBLIC_EVENT_ACCEPTED"
FUNDING_PROGRESSION_RELAY_PUBLICATION_STARTED = "RELAY_PUBLICATION_STARTED"
FUNDING_PROGRESSION_RELAY_PUBLICATION_ACCEPTED = "RELAY_PUBLICATION_ACCEPTED"
FUNDING_PROGRESSION_ACCOUNT_HISTORY_READ_STARTED = "ACCOUNT_HISTORY_READ_STARTED"
FUNDING_PROGRESSION_ACCOUNT_HISTORY_READ_COMPLETED = "ACCOUNT_HISTORY_READ_COMPLETED"
FUNDING_PROGRESSION_REQUIRED = "REQUIRED"
FUNDING_PROGRESSION_TOKENS = frozenset({
    FUNDING_PROGRESSION_PUBLIC_EVENT_WAIT_STARTED,
    FUNDING_PROGRESSION_PUBLIC_EVENT_ACCEPTED,
    FUNDING_PROGRESSION_RELAY_PUBLICATION_STARTED,
    FUNDING_PROGRESSION_RELAY_PUBLICATION_ACCEPTED,
    FUNDING_PROGRESSION_ACCOUNT_HISTORY_READ_STARTED,
    FUNDING_PROGRESSION_ACCOUNT_HISTORY_READ_COMPLETED,
})
FUNDING_PROGRESSION_RELAY_KINDS = frozenset({"RELEASED", "CANCELLED"})


def _funding_integer(value: object, label: str, *, nonnegative: bool = False) -> int:
    if type(value) is not int or (nonnegative and value < 0):
        raise NadoContractError(f"{label} must be an integer")
    return value


@dataclass(frozen=True)
class NadoFundingBaseline:
    """Durable pre-entry state for one exact Nado funding boundary.

    The history cursor is an archive cursor, not an account cash value.  The
    explicit empty terminal flag records the authoritative ``next_idx=null``
    baseline observed for a previously unused subaccount.
    """

    journal: JournalIdentity
    owner: str
    subaccount_name: str
    product_id: int
    boundary_at_ms: int
    history_high_water_idx: int | None
    history_empty_terminal: bool
    position_x18: int
    v_quote_balance_x18: int
    position_observed_at_ms: int
    position_snapshot_id: str
    cumulative_funding_long_x18: int
    cumulative_funding_short_x18: int
    open_interest_x18: int
    public_observed_at_ms: int
    baseline_digest: str
    authoritative: bool = True

    @property
    def target_boundary_at_ms(self) -> int:
        return self.boundary_at_ms

    def assert_contract(self) -> None:
        self.journal.assert_contract()
        if self.journal.venue != NADO_VENUE:
            raise NadoContractError("Nado funding baseline journal is not Nado")
        _address_bytes(self.owner)
        if self.journal.account_id.lower() != encode_subaccount(
            self.owner, self.subaccount_name
        ).lower():
            raise NadoContractError("Nado funding baseline journal identity mismatch")
        product_verifier(_strict_int(self.product_id, "Nado funding baseline product id"))
        _strict_int(self.boundary_at_ms, "Nado funding baseline boundary", positive=True)
        if self.history_high_water_idx is not None:
            _funding_integer(
                self.history_high_water_idx,
                "Nado funding history high-water index",
                nonnegative=True,
            )
        if type(self.history_empty_terminal) is not bool:
            raise NadoContractError("Nado funding baseline empty marker is invalid")
        if self.history_empty_terminal != (self.history_high_water_idx is None):
            raise NadoContractError("Nado funding baseline cursor/empty marker disagree")
        _funding_integer(self.position_x18, "Nado funding baseline position")
        _funding_integer(self.v_quote_balance_x18, "Nado funding baseline v_quote")
        _strict_int(
            self.position_observed_at_ms,
            "Nado funding baseline position timestamp",
            positive=True,
        )
        _safe_identifier(self.position_snapshot_id, "Nado funding baseline snapshot identity")
        _funding_integer(
            self.cumulative_funding_long_x18,
            "Nado funding baseline long cumulative",
        )
        _funding_integer(
            self.cumulative_funding_short_x18,
            "Nado funding baseline short cumulative",
        )
        _funding_integer(
            self.open_interest_x18,
            "Nado funding baseline open interest",
            nonnegative=True,
        )
        _strict_int(
            self.public_observed_at_ms,
            "Nado funding baseline public timestamp",
            positive=True,
        )
        if (
            self.position_observed_at_ms >= self.boundary_at_ms
            or self.public_observed_at_ms >= self.boundary_at_ms
        ):
            raise NadoContractError(
                "Nado funding baseline observations must precede the boundary"
            )
        if self.authoritative is not True:
            raise NadoContractError("Nado funding baseline is not authoritative")
        if _hash_text(self.baseline_digest, "Nado funding baseline digest") != (
            "0x" + nado_funding_baseline_digest(self)
        ):
            raise NadoContractError("Nado funding baseline digest does not bind its contents")


def _nado_baseline_payload(baseline: NadoFundingBaseline) -> dict[str, object]:
    return {
        "journal": _journal_payload(baseline.journal),
        "owner": baseline.owner.lower(),
        "subaccount_name": baseline.subaccount_name,
        "product_id": baseline.product_id,
        "boundary_at_ms": baseline.boundary_at_ms,
        "history_high_water_idx": baseline.history_high_water_idx,
        "history_empty_terminal": baseline.history_empty_terminal,
        "position_x18": baseline.position_x18,
        "v_quote_balance_x18": baseline.v_quote_balance_x18,
        "position_observed_at_ms": baseline.position_observed_at_ms,
        "position_snapshot_id": baseline.position_snapshot_id,
        "cumulative_funding_long_x18": baseline.cumulative_funding_long_x18,
        "cumulative_funding_short_x18": baseline.cumulative_funding_short_x18,
        "open_interest_x18": baseline.open_interest_x18,
        "public_observed_at_ms": baseline.public_observed_at_ms,
        "authoritative": baseline.authoritative,
    }


def nado_funding_baseline_digest(baseline: NadoFundingBaseline) -> str:
    return hashlib.sha256(canonical_payload(_nado_baseline_payload(baseline))).hexdigest()


@dataclass(frozen=True)
class NadoFundingExposure:
    """Durable authoritative signed exposure observed after entry and before settlement."""

    journal: JournalIdentity
    owner: str
    subaccount_name: str
    product_id: int
    direction: str
    signed_position_x18: int
    route_quantity_x18: int
    observed_at_ms: int
    snapshot_id: str
    cumulative_side: str
    cumulative_funding_x18: int
    exposure_digest: str
    authoritative: bool = True

    def assert_contract(self) -> None:
        self.journal.assert_contract()
        if self.journal.venue != NADO_VENUE:
            raise NadoContractError("Nado funding exposure journal is not Nado")
        _address_bytes(self.owner)
        if self.journal.account_id.lower() != encode_subaccount(
            self.owner, self.subaccount_name
        ).lower():
            raise NadoContractError("Nado funding exposure journal identity mismatch")
        product_verifier(_strict_int(self.product_id, "Nado funding exposure product id"))
        if self.direction not in {LONG, SHORT}:
            raise NadoContractError("Nado funding exposure direction is invalid")
        _funding_integer(self.signed_position_x18, "Nado funding signed position")
        if self.signed_position_x18 == 0:
            raise NadoContractError("Nado funding exposure position is not open")
        _funding_integer(
            self.route_quantity_x18,
            "Nado funding route quantity",
        )
        if self.route_quantity_x18 <= 0:
            raise NadoContractError("Nado funding route quantity is not positive")
        expected_sign = 1 if self.direction == LONG else -1
        if (
            abs(self.signed_position_x18) != self.route_quantity_x18
            or (self.signed_position_x18 > 0) != (expected_sign > 0)
        ):
            raise NadoContractError("Nado funding exposure sign or quantity mismatch")
        _strict_int(self.observed_at_ms, "Nado funding exposure timestamp", positive=True)
        _safe_identifier(self.snapshot_id, "Nado funding exposure snapshot identity")
        if self.cumulative_side not in {LONG, SHORT}:
            raise NadoContractError("Nado funding exposure cumulative side is invalid")
        if self.cumulative_side != self.direction:
            raise NadoContractError("Nado funding exposure cumulative side mismatch")
        _funding_integer(
            self.cumulative_funding_x18,
            "Nado funding exposure cumulative state",
        )
        if self.authoritative is not True:
            raise NadoContractError("Nado funding exposure is not authoritative")
        if _hash_text(self.exposure_digest, "Nado funding exposure digest") != (
            "0x" + nado_funding_exposure_digest(self)
        ):
            raise NadoContractError("Nado funding exposure digest does not bind its contents")


def _nado_exposure_payload(exposure: NadoFundingExposure) -> dict[str, object]:
    return {
        "journal": _journal_payload(exposure.journal),
        "owner": exposure.owner.lower(),
        "subaccount_name": exposure.subaccount_name,
        "product_id": exposure.product_id,
        "direction": exposure.direction,
        "signed_position_x18": exposure.signed_position_x18,
        "route_quantity_x18": exposure.route_quantity_x18,
        "observed_at_ms": exposure.observed_at_ms,
        "snapshot_id": exposure.snapshot_id,
        "cumulative_side": exposure.cumulative_side,
        "cumulative_funding_x18": exposure.cumulative_funding_x18,
        "authoritative": exposure.authoritative,
    }


def nado_funding_exposure_digest(exposure: NadoFundingExposure) -> str:
    return hashlib.sha256(canonical_payload(_nado_exposure_payload(exposure))).hexdigest()


@dataclass(frozen=True)
class NadoFundingEvent:
    """Authoritative public product-level hourly funding payment.

    This intentionally contains no account quantity, account cash, or account
    status.  ``payment_amount`` is the aggregate product payment from Nado's
    public event contract.
    """

    product_id: int
    timestamp: int
    payment_amount: int
    open_interest: int
    cumulative_funding_long_x18: int
    cumulative_funding_short_x18: int
    dt: int
    event_digest: str
    authoritative: bool = True

    @property
    def event_id(self) -> str:
        return f"nado-funding-{self.product_id}-{self.timestamp}"

    @property
    def timestamp_ns(self) -> int:
        return self.timestamp

    @property
    def payment_amount_x18(self) -> int:
        return self.payment_amount

    @property
    def open_interest_x18(self) -> int:
        return self.open_interest

    @property
    def dt_ns(self) -> int:
        return self.dt

    def assert_contract(self) -> None:
        product_verifier(_strict_int(self.product_id, "Nado funding product id"))
        _strict_int(self.timestamp, "Nado funding event timestamp", positive=True)
        _funding_integer(self.payment_amount, "Nado funding aggregate payment")
        _funding_integer(self.open_interest, "Nado funding open interest", nonnegative=True)
        _funding_integer(
            self.cumulative_funding_long_x18,
            "Nado funding cumulative long value",
        )
        _funding_integer(
            self.cumulative_funding_short_x18,
            "Nado funding cumulative short value",
        )
        _strict_int(self.dt, "Nado funding event dt", positive=True)
        if self.authoritative is not True:
            raise NadoContractError("Nado funding event is not authoritative")
        if _hash_text(self.event_digest, "Nado funding event digest") != (
            "0x" + nado_funding_event_digest(self)
        ):
            raise NadoContractError("Nado funding event digest does not bind its contents")


NadoPublicFundingEvent = NadoFundingEvent


def _nado_event_payload(event: NadoFundingEvent) -> dict[str, object]:
    return {
        "product_id": event.product_id,
        "timestamp": event.timestamp,
        "payment_amount": event.payment_amount,
        "open_interest": event.open_interest,
        "cumulative_funding_long_x18": event.cumulative_funding_long_x18,
        "cumulative_funding_short_x18": event.cumulative_funding_short_x18,
        "dt": event.dt,
        "authoritative": event.authoritative,
    }


def nado_funding_event_digest(event: NadoFundingEvent) -> str:
    return hashlib.sha256(canonical_payload(_nado_event_payload(event))).hexdigest()


@dataclass(frozen=True)
class NadoAccountFunding:
    """One official account-scoped funding-payment row.

    These fields mirror the pinned Python SDK ``IndexerPayment`` contract.
    ``amount`` is individual account cash and is deliberately independent of
    the public product event's aggregate ``payment_amount``.
    """

    journal: JournalIdentity
    owner: str
    subaccount_name: str
    product_id: int
    idx: int
    timestamp: int
    amount: int
    balance_amount: int
    rate_x18: int
    oracle_price_x18: int
    evidence_digest: str
    payment_kind: str = "funding"
    authoritative: bool = True

    @property
    def amount_x18(self) -> int:
        return self.amount

    @property
    def balance_amount_x18(self) -> int:
        return self.balance_amount

    @property
    def timestamp_s(self) -> int:
        return self.timestamp

    def assert_contract(self) -> None:
        self.journal.assert_contract()
        if self.journal.venue != NADO_VENUE:
            raise NadoContractError("Nado account funding journal is not Nado")
        _address_bytes(self.owner)
        if self.journal.account_id.lower() != encode_subaccount(
            self.owner, self.subaccount_name
        ).lower():
            raise NadoContractError("Nado account funding journal identity mismatch")
        product_verifier(_strict_int(self.product_id, "Nado account funding product id"))
        _funding_integer(self.idx, "Nado account funding index", nonnegative=True)
        _strict_int(self.timestamp, "Nado account funding timestamp", positive=True)
        _funding_integer(self.amount, "Nado account funding amount")
        _funding_integer(self.balance_amount, "Nado account funding balance")
        _strict_int(self.rate_x18, "Nado account funding rate")
        _strict_int(
            self.oracle_price_x18,
            "Nado account funding oracle price",
            positive=True,
        )
        if self.payment_kind != "funding":
            raise NadoContractError("Nado account row is not a funding payment")
        if self.authoritative is not True:
            raise NadoContractError("Nado account funding is not authoritative")
        if _hash_text(self.evidence_digest, "Nado account funding digest") != (
            "0x" + nado_account_funding_digest(self)
        ):
            raise NadoContractError(
                "Nado account funding digest does not bind its contents"
            )


NadoAccountFundingRow = NadoAccountFunding


def _nado_account_funding_payload(account: NadoAccountFunding) -> dict[str, object]:
    return {
        "journal": _journal_payload(account.journal),
        "owner": account.owner.lower(),
        "subaccount_name": account.subaccount_name,
        "product_id": account.product_id,
        "idx": account.idx,
        "timestamp": account.timestamp,
        "amount": account.amount,
        "balance_amount": account.balance_amount,
        "rate_x18": account.rate_x18,
        "oracle_price_x18": account.oracle_price_x18,
        "payment_kind": account.payment_kind,
        "authoritative": account.authoritative,
    }


def nado_account_funding_digest(account: NadoAccountFunding) -> str:
    return hashlib.sha256(canonical_payload(_nado_account_funding_payload(account))).hexdigest()


@dataclass(frozen=True)
class NadoFundingBoundaryResult:
    event_id: str | None
    market: str
    settlement_at_ms: int
    status: str
    rate_x18: int | None
    cash_x18: int | None
    completion_eligible: bool
    blocked: bool
    aggregate_payment_x18: int | None = None
    account_idx: int | None = None
    reason: str | None = None


def _funding_event_interval_ms(
    boundary_at_ms: int, event: NadoFundingEvent,
) -> tuple[int, int]:
    del event
    return boundary_at_ms, boundary_at_ms + FUNDING_BOUNDARY_INTERVAL_MS


def _account_timestamp_ms(account: NadoAccountFunding) -> int:
    # The archive contract documents payment timestamps as Unix epoch seconds.
    return account.timestamp * 1_000


def _assert_funding_identity(
    *,
    binding: FundingBoundaryBinding,
    baseline: NadoFundingBaseline,
    attestation: CrossRunAttestation,
    event: NadoFundingEvent,
    account_funding: NadoAccountFunding,
    exposure: NadoFundingExposure,
) -> None:
    if _funding_route_payload(attestation.route) != _funding_route_payload(binding.route):
        raise NadoContractError("cross-run attestation route is not the persisted route")
    if _journal_payload(attestation.risex_journal) != _journal_payload(binding.risex_journal):
        raise NadoContractError("RISEx journal identity is not the persisted journal")
    if _journal_payload(attestation.nado_journal) != _journal_payload(binding.nado_journal):
        raise NadoContractError("Nado journal identity is not the persisted journal")
    for name, journal in (
        ("baseline", baseline.journal),
        ("Nado account funding", account_funding.journal),
        ("Nado funding exposure", exposure.journal),
    ):
        if _journal_payload(journal) != _journal_payload(binding.nado_journal):
            raise NadoContractError(f"{name} is not bound to the persisted journal")
    route = binding.route
    if (
        baseline.product_id != route.nado_product_id
        or baseline.boundary_at_ms != route.settlement_at_ms
        or event.product_id != route.nado_product_id
        or account_funding.product_id != route.nado_product_id
        or exposure.product_id != route.nado_product_id
    ):
        raise NadoContractError("Nado funding evidence is not bound to the persisted product")
    if baseline.owner.lower() != account_funding.owner.lower():
        raise NadoContractError("Nado account funding owner differs from baseline")
    if baseline.subaccount_name != account_funding.subaccount_name:
        raise NadoContractError("Nado account funding subaccount differs from baseline")
    if baseline.owner.lower() != exposure.owner.lower():
        raise NadoContractError("Nado funding exposure owner differs from baseline")
    if baseline.subaccount_name != exposure.subaccount_name:
        raise NadoContractError("Nado funding exposure subaccount differs from baseline")


def _solidity_divide_toward_zero(numerator: int, denominator: int) -> int:
    if type(numerator) is not int or type(denominator) is not int or denominator <= 0:
        raise NadoContractError("Nado funding settlement division is invalid")
    magnitude = abs(numerator) // denominator
    return -magnitude if numerator < 0 else magnitude


def nado_funding_account_amount_x18(
    cumulative_delta_x18: int, signed_position_x18: int,
) -> int:
    """Apply PerpEngineState._updateBalance with vQuoteDelta equal to zero.

    MathSD21x18.mul uses signed integer division by 1e18, which Solidity
    truncates toward zero.  The account archive amount is the resulting
    deltaQuote, not the public product aggregate payment.
    """
    _funding_integer(cumulative_delta_x18, "Nado funding cumulative delta")
    _funding_integer(signed_position_x18, "Nado funding signed position")
    return -_solidity_divide_toward_zero(
        cumulative_delta_x18 * signed_position_x18, X18
    )


def validate_nado_funding_boundary(
    *,
    binding: FundingBoundaryBinding,
    baseline: NadoFundingBaseline | None,
    attestation: CrossRunAttestation | None,
    event: NadoFundingEvent | None,
    account_funding: NadoAccountFunding | None,
    exposure: NadoFundingExposure | None,
) -> NadoFundingBoundaryResult:
    """Validate product and account evidence for one funding boundary.

    The public event supplies the product-level cumulative transition.  The
    signed exposure selects the account's long or short cumulative side, and
    the account amount must equal the exact PerpEngineState settlement delta.
    The archive rate and oracle fields are retained as strict metadata only.
    """
    binding.assert_contract()
    if baseline is None:
        raise NadoContractError("pre-entry funding baseline is incomplete")
    if (
        attestation is None
        or event is None
        or account_funding is None
        or exposure is None
    ):
        raise NadoContractError("post-boundary funding evidence is incomplete")
    attestation.assert_contract()
    baseline.assert_contract()
    event.assert_contract()
    account_funding.assert_contract()
    exposure.assert_contract()
    _assert_funding_identity(
        binding=binding,
        baseline=baseline,
        attestation=attestation,
        event=event,
        account_funding=account_funding,
        exposure=exposure,
    )
    if baseline.position_x18 != 0 or baseline.v_quote_balance_x18 != 0:
        raise NadoContractError("pre-entry funding baseline is not exactly flat")
    start_ms, end_ms = _funding_event_interval_ms(binding.route.settlement_at_ms, event)
    event_ms = event.timestamp // 1_000_000
    account_ms = _account_timestamp_ms(account_funding)
    if not start_ms <= event_ms < end_ms:
        raise NadoContractError("public funding event is outside the persisted boundary")
    if not start_ms <= account_ms < end_ms:
        raise NadoContractError("account funding row is outside the persisted boundary")
    if event.dt != FUNDING_BOUNDARY_INTERVAL_MS * 1_000_000:
        raise NadoContractError("public funding event interval does not match the boundary")
    if baseline.history_high_water_idx is not None:
        if account_funding.idx <= baseline.history_high_water_idx:
            raise NadoContractError("account funding row is stale or prior to the baseline")
    elif not baseline.history_empty_terminal:
        raise NadoContractError("account funding baseline cursor is invalid")
    expected_route_quantity = _funding_quantity_x18(
        binding.route.nado_leg.canonical_quantity,
        "Nado route canonical quantity",
    )
    expected_position = (
        expected_route_quantity
        if binding.route.nado_leg.direction == LONG
        else -expected_route_quantity
    )
    if (
        exposure.direction != binding.route.nado_leg.direction
        or exposure.route_quantity_x18 != expected_route_quantity
        or exposure.signed_position_x18 != expected_position
        or exposure.observed_at_ms <= baseline.position_observed_at_ms
        or exposure.observed_at_ms >= binding.route.settlement_at_ms
    ):
        raise NadoContractError("Nado funding exposure is not the fresh exact route position")
    baseline_cumulative = (
        baseline.cumulative_funding_long_x18
        if exposure.cumulative_side == LONG
        else baseline.cumulative_funding_short_x18
    )
    if exposure.cumulative_funding_x18 != baseline_cumulative:
        raise NadoContractError("Nado funding exposure cumulative state is stale")
    if account_funding.balance_amount != exposure.signed_position_x18:
        raise NadoContractError("account funding balance does not equal exact exposure")
    delta_long = (
        event.cumulative_funding_long_x18
        - baseline.cumulative_funding_long_x18
    )
    delta_short = (
        event.cumulative_funding_short_x18
        - baseline.cumulative_funding_short_x18
    )
    if delta_long != delta_short:
        raise NadoContractError("public cumulative funding sides disagree")
    relevant_after = (
        event.cumulative_funding_long_x18
        if exposure.cumulative_side == LONG
        else event.cumulative_funding_short_x18
    )
    expected_amount = nado_funding_account_amount_x18(
        relevant_after - exposure.cumulative_funding_x18,
        exposure.signed_position_x18,
    )
    if account_funding.amount != expected_amount:
        raise NadoContractError(
            "account funding amount does not match exact cumulative settlement"
        )
    return NadoFundingBoundaryResult(
        event.event_id,
        binding.route.nado_leg.market,
        binding.route.settlement_at_ms,
        FUNDING_APPLIED,
        account_funding.rate_x18,
        account_funding.amount,
        True,
        False,
        event.payment_amount,
        account_funding.idx,
        None,
    )


def _funding_boundary_payload(
    binding: FundingBoundaryBinding,
) -> dict[str, object]:
    binding.assert_contract()
    return {
        "route": _funding_route_payload(binding.route),
        "risex_journal": _journal_payload(binding.risex_journal),
        "nado_journal": _journal_payload(binding.nado_journal),
    }


def _funding_boundary_digest(binding: FundingBoundaryBinding) -> str:
    return hashlib.sha256(
        canonical_payload(_funding_boundary_payload(binding))
    ).hexdigest()


@dataclass(frozen=True)
class _FundingProgressionRecord:
    step: int
    token: str
    observed_at_ms: int
    binding_digest: str
    relay_kind: str | None


def _validate_funding_progression_records(
    records: tuple[_FundingProgressionRecord, ...],
    *,
    binding_digest: str,
    require_nonempty: bool,
) -> None:
    """Validate one exact, append-only post-exposure progression prefix."""
    if type(binding_digest) is not str or len(binding_digest) != 64:
        raise NadoContractError("funding progression binding digest is invalid")
    try:
        int(binding_digest, 16)
    except ValueError:
        raise NadoContractError("funding progression binding digest is invalid") from None
    if require_nonempty and not records:
        raise NadoContractError("funding progression is missing after exposure")
    previous: _FundingProgressionRecord | None = None
    for expected_step, record in enumerate(records, start=1):
        if (
            type(record.step) is not int
            or record.step != expected_step
            or type(record.token) is not str
            or record.token not in FUNDING_PROGRESSION_TOKENS
            or type(record.observed_at_ms) is not int
            or record.observed_at_ms <= 0
            or record.binding_digest != binding_digest
            or (
                previous is not None
                and record.observed_at_ms < previous.observed_at_ms
            )
        ):
            raise NadoContractError("funding progression record is invalid")
        if record.token in {
            FUNDING_PROGRESSION_RELAY_PUBLICATION_STARTED,
            FUNDING_PROGRESSION_RELAY_PUBLICATION_ACCEPTED,
        }:
            if (
                type(record.relay_kind) is not str
                or record.relay_kind not in FUNDING_PROGRESSION_RELAY_KINDS
            ):
                raise NadoContractError("funding progression relay kind is invalid")
        elif record.relay_kind is not None:
            raise NadoContractError("funding progression relay kind is unexpected")

        if previous is None:
            allowed = record.token == FUNDING_PROGRESSION_PUBLIC_EVENT_WAIT_STARTED
        elif previous.token == FUNDING_PROGRESSION_PUBLIC_EVENT_WAIT_STARTED:
            allowed = record.token in {
                FUNDING_PROGRESSION_PUBLIC_EVENT_ACCEPTED,
                FUNDING_PROGRESSION_RELAY_PUBLICATION_STARTED,
            }
            if record.token == FUNDING_PROGRESSION_RELAY_PUBLICATION_STARTED:
                allowed = record.relay_kind == "CANCELLED"
        elif previous.token == FUNDING_PROGRESSION_PUBLIC_EVENT_ACCEPTED:
            allowed = record.token == FUNDING_PROGRESSION_ACCOUNT_HISTORY_READ_STARTED
            if record.token == FUNDING_PROGRESSION_RELAY_PUBLICATION_STARTED:
                allowed = record.relay_kind in FUNDING_PROGRESSION_RELAY_KINDS
        elif previous.token == FUNDING_PROGRESSION_RELAY_PUBLICATION_STARTED:
            allowed = (
                record.token == FUNDING_PROGRESSION_RELAY_PUBLICATION_ACCEPTED
                and record.relay_kind == previous.relay_kind
            )
        elif previous.token == FUNDING_PROGRESSION_RELAY_PUBLICATION_ACCEPTED:
            allowed = (
                previous.relay_kind == "RELEASED"
                and record.token == FUNDING_PROGRESSION_ACCOUNT_HISTORY_READ_STARTED
            )
        elif previous.token == FUNDING_PROGRESSION_ACCOUNT_HISTORY_READ_STARTED:
            allowed = record.token == FUNDING_PROGRESSION_ACCOUNT_HISTORY_READ_COMPLETED
        else:
            allowed = False
        if not allowed:
            raise NadoContractError("funding progression ordering is invalid")
        previous = record


def _mapping_payload(value: object, label: str, keys: set[str]) -> dict[str, object]:
    if type(value) is not dict or set(value) != keys:
        raise NadoContractError(f"{label} persistence schema is invalid")
    return value


def _leg_from_payload(value: object) -> FundingLegBinding:
    payload = _mapping_payload(
        value,
        "funding leg",
        {"venue", "market", "direction", "raw_quantity", "base_multiplier", "canonical_quantity"},
    )
    leg = FundingLegBinding(
        payload["venue"], payload["market"], payload["direction"],
        Decimal(payload["raw_quantity"]), Decimal(payload["base_multiplier"]),
        Decimal(payload["canonical_quantity"]),
    )
    leg.assert_contract()
    return leg


def _route_from_payload(value: object) -> FundingRouteBinding:
    payload = _mapping_payload(
        value,
        "funding route",
        {"canonical_asset", "risex_leg", "nado_leg", "nado_product_id", "settlement_at_ms"},
    )
    route = FundingRouteBinding(
        payload["canonical_asset"],
        _leg_from_payload(payload["risex_leg"]),
        _leg_from_payload(payload["nado_leg"]),
        payload["nado_product_id"],
        payload["settlement_at_ms"],
    )
    route.assert_contract()
    return route


def _journal_from_payload(value: object) -> JournalIdentity:
    payload = _mapping_payload(
        value, "journal", {"venue", "run_id", "store_identity", "account_id"}
    )
    journal = JournalIdentity(
        payload["venue"], payload["run_id"], payload["store_identity"],
        payload["account_id"],
    )
    journal.assert_contract()
    return journal


def _terminal_from_payload(value: object) -> TerminalEvidence:
    payload = _mapping_payload(
        value,
        "terminal evidence",
        {
            "journal", "status", "observed_at_ms", "zero_regular_orders",
            "zero_trigger_orders", "exact_flat", "unresolved_write_identities",
            "authoritative", "journal_content_sha256", "evidence_digest",
        },
    )
    unresolved = payload["unresolved_write_identities"]
    if type(unresolved) is not list:
        raise NadoContractError("terminal evidence persistence schema is invalid")
    evidence = TerminalEvidence(
        _journal_from_payload(payload["journal"]), payload["status"],
        payload["observed_at_ms"], payload["journal_content_sha256"],
        payload["zero_regular_orders"],
        payload["zero_trigger_orders"], payload["exact_flat"],
        tuple(unresolved), payload["evidence_digest"], payload["authoritative"],
    )
    evidence.assert_contract()
    return evidence


def _attestation_from_payload(value: object) -> CrossRunAttestation:
    payload = _mapping_payload(
        value,
        "cross-run attestation",
        {
            "route", "risex_journal", "nado_journal", "risex_terminal",
            "nado_terminal", "attestation_digest",
        },
    )
    attestation = CrossRunAttestation(
        _route_from_payload(payload["route"]),
        _journal_from_payload(payload["risex_journal"]),
        _journal_from_payload(payload["nado_journal"]),
        _terminal_from_payload(payload["risex_terminal"]),
        _terminal_from_payload(payload["nado_terminal"]),
        payload["attestation_digest"],
    )
    attestation.assert_contract()
    return attestation


def _event_from_payload(value: object) -> NadoFundingEvent:
    payload = _mapping_payload(
        value,
        "Nado funding event",
        {
            "product_id", "timestamp", "payment_amount", "open_interest",
            "cumulative_funding_long_x18", "cumulative_funding_short_x18", "dt",
            "event_digest", "authoritative",
        },
    )
    event = NadoFundingEvent(
        payload["product_id"], payload["timestamp"], payload["payment_amount"],
        payload["open_interest"], payload["cumulative_funding_long_x18"],
        payload["cumulative_funding_short_x18"], payload["dt"],
        payload["event_digest"], payload["authoritative"],
    )
    event.assert_contract()
    return event


def _account_funding_from_payload(value: object) -> NadoAccountFunding:
    payload = _mapping_payload(
        value,
        "Nado account funding",
        {
            "journal", "owner", "subaccount_name", "product_id", "idx",
            "timestamp", "amount", "balance_amount", "rate_x18",
            "oracle_price_x18", "payment_kind", "evidence_digest", "authoritative",
        },
    )
    account = NadoAccountFunding(
        _journal_from_payload(payload["journal"]), payload["owner"],
        payload["subaccount_name"], payload["product_id"], payload["idx"],
        payload["timestamp"], payload["amount"], payload["balance_amount"],
        payload["rate_x18"], payload["oracle_price_x18"],
        payload["evidence_digest"], payload["payment_kind"], payload["authoritative"],
    )
    account.assert_contract()
    return account


def _baseline_from_payload(value: object) -> NadoFundingBaseline:
    payload = _mapping_payload(
        value,
        "Nado funding baseline",
        {
            "journal", "owner", "subaccount_name", "product_id", "boundary_at_ms",
            "history_high_water_idx", "history_empty_terminal", "position_x18",
            "v_quote_balance_x18", "position_observed_at_ms", "position_snapshot_id",
            "cumulative_funding_long_x18", "cumulative_funding_short_x18",
            "open_interest_x18", "public_observed_at_ms", "baseline_digest",
            "authoritative",
        },
    )
    baseline = NadoFundingBaseline(
        _journal_from_payload(payload["journal"]), payload["owner"],
        payload["subaccount_name"], payload["product_id"], payload["boundary_at_ms"],
        payload["history_high_water_idx"], payload["history_empty_terminal"],
        payload["position_x18"], payload["v_quote_balance_x18"],
        payload["position_observed_at_ms"], payload["position_snapshot_id"],
        payload["cumulative_funding_long_x18"],
        payload["cumulative_funding_short_x18"], payload["open_interest_x18"],
        payload["public_observed_at_ms"], payload["baseline_digest"],
        payload["authoritative"],
    )
    baseline.assert_contract()
    return baseline


def _exposure_from_payload(value: object) -> NadoFundingExposure:
    payload = _mapping_payload(
        value,
        "Nado funding exposure",
        {
            "journal", "owner", "subaccount_name", "product_id", "direction",
            "signed_position_x18", "route_quantity_x18", "observed_at_ms",
            "snapshot_id", "cumulative_side", "cumulative_funding_x18",
            "exposure_digest", "authoritative",
        },
    )
    exposure = NadoFundingExposure(
        _journal_from_payload(payload["journal"]), payload["owner"],
        payload["subaccount_name"], payload["product_id"], payload["direction"],
        payload["signed_position_x18"], payload["route_quantity_x18"],
        payload["observed_at_ms"], payload["snapshot_id"],
        payload["cumulative_side"], payload["cumulative_funding_x18"],
        payload["exposure_digest"], payload["authoritative"],
    )
    exposure.assert_contract()
    return exposure


def _funding_evidence_payload(
    attestation: CrossRunAttestation,
    baseline: NadoFundingBaseline,
    event: NadoFundingEvent,
    account_funding: NadoAccountFunding,
    exposure: NadoFundingExposure,
) -> dict[str, object]:
    return {
        "attestation": _stored_attestation_payload(attestation),
        "baseline": _nado_baseline_payload(baseline) | {
            "baseline_digest": _hash_text(
                baseline.baseline_digest, "Nado funding baseline digest"
            ),
        },
        "event": _nado_event_payload(event) | {
            "event_digest": _hash_text(event.event_digest, "Nado funding event digest"),
        },
        "account_funding": _nado_account_funding_payload(account_funding) | {
            "evidence_digest": _hash_text(
                account_funding.evidence_digest, "Nado account funding digest"
            ),
        },
        "exposure": _nado_exposure_payload(exposure) | {
            "exposure_digest": _hash_text(
                exposure.exposure_digest, "Nado funding exposure digest"
            ),
        },
    }


def _stored_attestation_payload(
    attestation: CrossRunAttestation,
) -> dict[str, object]:
    return {
        "route": _funding_route_payload(attestation.route),
        "risex_journal": _journal_payload(attestation.risex_journal),
        "nado_journal": _journal_payload(attestation.nado_journal),
        "attestation_digest": _hash_text(
            attestation.attestation_digest, "cross-run attestation digest"
        ),
        "risex_terminal": _terminal_payload(attestation.risex_terminal) | {
            "evidence_digest": _hash_text(
                attestation.risex_terminal.evidence_digest,
                "RISEx terminal evidence digest",
            )
        },
        "nado_terminal": _terminal_payload(attestation.nado_terminal) | {
            "evidence_digest": _hash_text(
                attestation.nado_terminal.evidence_digest,
                "Nado terminal evidence digest",
            )
        },
    }


def _funding_evidence_from_payload(
    value: object,
) -> tuple[
    CrossRunAttestation, NadoFundingBaseline, NadoFundingEvent,
    NadoAccountFunding, NadoFundingExposure
]:
    payload = _mapping_payload(
        value,
        "funding evidence",
        {"attestation", "baseline", "event", "account_funding", "exposure"},
    )
    return (
        _attestation_from_payload(payload["attestation"]),
        _baseline_from_payload(payload["baseline"]),
        _event_from_payload(payload["event"]),
        _account_funding_from_payload(payload["account_funding"]),
        _exposure_from_payload(payload["exposure"]),
    )


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
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS nado_execute_failures (
                digest TEXT PRIMARY KEY,
                failure_class TEXT NOT NULL,
                venue_code INTEGER
            )
            """
        )
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS nado_funding_boundary (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                binding_json BLOB NOT NULL,
                binding_digest TEXT NOT NULL
            )
            """
        )
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS nado_funding_evidence (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                evidence_json BLOB NOT NULL,
                evidence_digest TEXT NOT NULL
            )
            """
        )
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS nado_funding_baseline (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                baseline_json BLOB NOT NULL,
                baseline_digest TEXT NOT NULL
            )
            """
        )
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS nado_funding_exposure (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                exposure_json BLOB NOT NULL,
                exposure_digest TEXT NOT NULL
            )
            """
        )
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS nado_funding_blocker (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                reason TEXT NOT NULL,
                attestation_json BLOB NOT NULL,
                blocker_digest TEXT NOT NULL
            )
            """
        )
        self._connection.commit()
        self._validate_funding_progression()

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
                if (
                    self.funding_boundary_binding() is not None
                    and self.funding_boundary_baseline() is None
                ):
                    raise NadoContractError(
                        "funding baseline must be persisted before intent preparation"
                    )
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

    def _record_execute_failure(
        self, digest: str, failure: ExecuteFailure
    ) -> None:
        state = (
            "REJECTED"
            if failure.failure_class == EXECUTE_VENUE_REJECTION
            else "AMBIGUOUS"
        )
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO nado_execute_failures
                    (digest, failure_class, venue_code)
                VALUES (?, ?, ?)
                """,
                (digest.lower(), failure.failure_class, failure.venue_code),
            )
            changed = self._connection.execute(
                """
                UPDATE nado_intents SET state = ?
                WHERE digest = ? AND state = 'PREPARED'
                """,
                (state, digest.lower()),
            )
            if changed.rowcount != 1:
                raise NadoContractError("execute failure intent state rejected")
            self._connection.execute(
                """
                UPDATE nado_lifecycle_state SET status = 'HALTED'
                WHERE singleton = 1 AND status != 'COMPLETE'
                """
            )

    def execute_failure(self, digest: str) -> tuple[str, int | None] | None:
        row = self._connection.execute(
            """
            SELECT failure_class, venue_code FROM nado_execute_failures
            WHERE digest = ?
            """,
            (digest.lower(),),
        ).fetchone()
        if row is None:
            return None
        return str(row[0]), None if row[1] is None else int(row[1])

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

    def _funding_progression_table_exists(self) -> bool:
        return self._connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            ("nado_funding_progression",),
        ).fetchone() is not None

    def _funding_progression_activation_table_exists(self) -> bool:
        return self._connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            ("nado_funding_progression_activation",),
        ).fetchone() is not None

    def _ensure_funding_progression_activation_schema(self) -> None:
        expected_columns = {"singleton", "requirement", "binding_digest"}
        try:
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS nado_funding_progression_activation (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    requirement TEXT NOT NULL,
                    binding_digest TEXT NOT NULL
                )
                """
            )
            columns = {
                str(row[1])
                for row in self._connection.execute(
                    "PRAGMA table_info(nado_funding_progression_activation)"
                )
            }
        except sqlite3.DatabaseError:
            raise NadoContractError(
                "funding progression activation schema is unavailable"
            ) from None
        if columns != expected_columns:
            raise NadoContractError(
                "funding progression activation schema is invalid"
            )

    def _funding_progression_activation_required(self) -> bool:
        if not self._funding_progression_activation_table_exists():
            return False
        self._ensure_funding_progression_activation_schema()
        try:
            rows = self._connection.execute(
                """
                SELECT singleton, requirement, binding_digest
                FROM nado_funding_progression_activation
                ORDER BY singleton
                """
            ).fetchall()
        except sqlite3.DatabaseError:
            raise NadoContractError(
                "funding progression activation could not be read"
            ) from None
        if (
            len(rows) != 1
            or len(rows[0]) != 3
            or type(rows[0][0]) is not int
            or rows[0][0] != 1
            or type(rows[0][1]) is not str
            or rows[0][1] != FUNDING_PROGRESSION_REQUIRED
            or type(rows[0][2]) is not str
        ):
            raise NadoContractError("funding progression activation is invalid")
        binding = self.funding_boundary_binding()
        if binding is None or rows[0][2] != _funding_boundary_digest(binding):
            raise NadoContractError(
                "funding progression activation binding is invalid"
            )
        return True

    def _ensure_funding_progression_schema(self) -> None:
        expected_columns = {
            "step", "token", "observed_at_ms", "binding_digest", "relay_kind",
        }
        try:
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS nado_funding_progression (
                    step INTEGER PRIMARY KEY,
                    token TEXT NOT NULL,
                    observed_at_ms INTEGER NOT NULL,
                    binding_digest TEXT NOT NULL,
                    relay_kind TEXT
                )
                """
            )
            columns = {
                str(row[1])
                for row in self._connection.execute(
                    "PRAGMA table_info(nado_funding_progression)"
                )
            }
        except sqlite3.DatabaseError:
            raise NadoContractError("funding progression schema is unavailable") from None
        if columns != expected_columns:
            raise NadoContractError("funding progression schema is invalid")

    def _funding_progression_rows(
        self,
    ) -> tuple[_FundingProgressionRecord, ...] | None:
        if not self._funding_progression_table_exists():
            return None
        self._ensure_funding_progression_schema()
        try:
            raw_rows = self._connection.execute(
                """
                SELECT step, token, observed_at_ms, binding_digest, relay_kind
                FROM nado_funding_progression ORDER BY step
                """
            ).fetchall()
        except sqlite3.DatabaseError:
            raise NadoContractError("funding progression could not be read") from None
        records: list[_FundingProgressionRecord] = []
        for row in raw_rows:
            if (
                len(row) != 5
                or type(row[0]) is not int
                or type(row[1]) is not str
                or type(row[2]) is not int
                or type(row[3]) is not str
                or (row[4] is not None and type(row[4]) is not str)
            ):
                raise NadoContractError("funding progression record is invalid")
            records.append(
                _FundingProgressionRecord(
                    row[0], row[1], row[2], row[3], row[4],
                )
            )
        return tuple(records)

    def _validate_funding_progression(
        self, *, require_nonempty: bool | None = None,
    ) -> None:
        activation_required = self._funding_progression_activation_required()
        records = self._funding_progression_rows()
        if not activation_required and records is not None:
            raise NadoContractError(
                "funding progression activation is missing"
            )
        if records is None:
            if activation_required:
                raise NadoContractError("funding progression table is missing")
            # Historical/direct stores predate this table and remain
            # read-only compatible without an in-place schema migration.
            return
        binding = self.funding_boundary_binding()
        if binding is None:
            raise NadoContractError("funding progression is unbound")
        if require_nonempty is None:
            require_nonempty = self.funding_boundary_exposure() is not None
        _validate_funding_progression_records(
            records,
            binding_digest=_funding_boundary_digest(binding),
            require_nonempty=require_nonempty,
        )
        try:
            evidence_exists = self._connection.execute(
                "SELECT 1 FROM nado_funding_evidence WHERE singleton = 1"
            ).fetchone() is not None
        except sqlite3.DatabaseError:
            raise NadoContractError(
                "funding progression evidence state is unavailable"
            ) from None
        if evidence_exists and (
            not records
            or records[-1].token != FUNDING_PROGRESSION_ACCOUNT_HISTORY_READ_COMPLETED
        ):
            raise NadoContractError(
                "funding progression is incomplete for funding evidence"
            )

    def record_funding_progression(
        self,
        token: str,
        *,
        observed_at_ms: int,
        relay_kind: str | None = None,
    ) -> None:
        """Append exactly one allowlisted post-exposure progression marker."""
        if type(token) is not str or token not in FUNDING_PROGRESSION_TOKENS:
            raise NadoContractError("funding progression token is not bounded")
        if type(observed_at_ms) is not int or observed_at_ms <= 0:
            raise NadoContractError("funding progression timestamp is invalid")
        if token in {
            FUNDING_PROGRESSION_RELAY_PUBLICATION_STARTED,
            FUNDING_PROGRESSION_RELAY_PUBLICATION_ACCEPTED,
        }:
            if (
                type(relay_kind) is not str
                or relay_kind not in FUNDING_PROGRESSION_RELAY_KINDS
            ):
                raise NadoContractError("funding progression relay kind is invalid")
        elif relay_kind is not None:
            raise NadoContractError("funding progression relay kind is unexpected")
        binding = self.funding_boundary_binding()
        if binding is None or self.funding_boundary_exposure() is None:
            raise NadoContractError("funding progression requires durable exposure")
        if not self._funding_progression_activation_required():
            raise NadoContractError("funding progression activation is missing")
        if not self._funding_progression_table_exists():
            raise NadoContractError("funding progression schema is unavailable")
        binding_digest = _funding_boundary_digest(binding)
        try:
            with self._connection:
                # The empty table is the valid pre-marker state immediately
                # after the durable exposure insert.  The candidate below
                # must still be a non-empty valid prefix.
                self._validate_funding_progression(require_nonempty=False)
                records = self._funding_progression_rows()
                assert records is not None
                candidate = records + (
                    _FundingProgressionRecord(
                        len(records) + 1,
                        token,
                        observed_at_ms,
                        binding_digest,
                        relay_kind,
                    ),
                )
                _validate_funding_progression_records(
                    candidate,
                    binding_digest=binding_digest,
                    require_nonempty=True,
                )
                self._connection.execute(
                    """
                    INSERT INTO nado_funding_progression
                        (step, token, observed_at_ms, binding_digest, relay_kind)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        candidate[-1].step, candidate[-1].token,
                        candidate[-1].observed_at_ms, candidate[-1].binding_digest,
                        candidate[-1].relay_kind,
                    ),
                )
        except sqlite3.DatabaseError:
            raise NadoContractError("funding progression persistence failed") from None

    def funding_boundary_progression(self) -> tuple[str, ...]:
        """Return only fixed sanitized tokens from the durable progression."""
        self._validate_funding_progression()
        records = self._funding_progression_rows()
        return () if records is None else tuple(record.token for record in records)

    def replace_payload(self, digest: str, payload: bytes) -> None:
        del digest, payload
        raise NadoContractError("persisted payload and digest are immutable")

    def bind_funding_boundary(
        self,
        binding: FundingBoundaryBinding,
        baseline: NadoFundingBaseline | None = None,
    ) -> None:
        """Persist the exact route and both venue journal identities once."""
        encoded = canonical_payload(_funding_boundary_payload(binding))
        digest = hashlib.sha256(encoded).hexdigest()
        try:
            with self._connection:
                row = self._connection.execute(
                    "SELECT binding_json, binding_digest "
                    "FROM nado_funding_boundary WHERE singleton = 1"
                ).fetchone()
                if row is not None:
                    stored = bytes(row[0])
                    if stored != encoded or str(row[1]) != digest:
                        raise NadoContractError(
                            "persisted funding boundary identity is immutable"
                        )
                    if baseline is not None:
                        self.bind_funding_baseline(baseline)
                    return
                if self.intents():
                    raise NadoContractError(
                        "funding boundary must be bound before intent preparation"
                    )
                if self.lifecycle_status() != RUNNING:
                    raise NadoContractError("funding boundary requires a running lifecycle")
                self._connection.execute(
                    "INSERT INTO nado_funding_boundary "
                    "(singleton, binding_json, binding_digest) VALUES (1, ?, ?)",
                    (encoded, digest),
                )
                if baseline is not None:
                    self.bind_funding_baseline(baseline)
        except sqlite3.DatabaseError:
            raise NadoContractError("funding boundary persistence failed") from None

    def bind_funding_baseline(self, baseline: NadoFundingBaseline) -> None:
        """Persist the exact empty/high-water and public pre-entry baseline."""
        binding = self.funding_boundary_binding()
        if binding is None:
            raise NadoContractError("funding baseline requires a persisted boundary")
        baseline.assert_contract()
        if (
            baseline.journal != binding.nado_journal
            or baseline.product_id != binding.route.nado_product_id
            or baseline.boundary_at_ms != binding.route.settlement_at_ms
        ):
            raise NadoContractError("funding baseline is not bound to the persisted route")
        encoded = canonical_payload(_nado_baseline_payload(baseline) | {
            "baseline_digest": _hash_text(
                baseline.baseline_digest, "Nado funding baseline digest"
            ),
        })
        digest = hashlib.sha256(encoded).hexdigest()
        try:
            with self._connection:
                row = self._connection.execute(
                    "SELECT baseline_json, baseline_digest "
                    "FROM nado_funding_baseline WHERE singleton = 1"
                ).fetchone()
                if row is not None:
                    if bytes(row[0]) != encoded or str(row[1]) != digest:
                        raise NadoContractError(
                            "persisted funding baseline is immutable"
                        )
                    return
                if self.intents():
                    raise NadoContractError(
                        "funding baseline must be persisted before intent preparation"
                    )
                if self.lifecycle_status() != RUNNING:
                    raise NadoContractError(
                        "funding baseline requires a running lifecycle"
                    )
                self._connection.execute(
                    "INSERT INTO nado_funding_baseline "
                    "(singleton, baseline_json, baseline_digest) VALUES (1, ?, ?)",
                    (encoded, digest),
                )
        except sqlite3.DatabaseError:
            raise NadoContractError("funding baseline persistence failed") from None

    def bind_funding_exposure(
        self, exposure: NadoFundingExposure, *, track_progression: bool = False,
    ) -> None:
        """Persist the exact fresh signed route exposure before funding wait."""
        binding = self.funding_boundary_binding()
        baseline = self.funding_boundary_baseline()
        if binding is None or baseline is None:
            raise NadoContractError("funding exposure requires a persisted baseline")
        if type(track_progression) is not bool:
            raise NadoContractError("funding progression tracking flag is invalid")
        exposure.assert_contract()
        expected_route_quantity = _funding_quantity_x18(
            binding.route.nado_leg.canonical_quantity,
            "Nado route canonical quantity",
        )
        if (
            exposure.journal != binding.nado_journal
            or exposure.product_id != binding.route.nado_product_id
            or exposure.direction != binding.route.nado_leg.direction
            or exposure.route_quantity_x18 != expected_route_quantity
            or exposure.observed_at_ms <= baseline.position_observed_at_ms
            or exposure.observed_at_ms >= binding.route.settlement_at_ms
        ):
            raise NadoContractError("funding exposure is not bound to the persisted route")
        baseline_cumulative = (
            baseline.cumulative_funding_long_x18
            if exposure.cumulative_side == LONG
            else baseline.cumulative_funding_short_x18
        )
        if exposure.cumulative_funding_x18 != baseline_cumulative:
            raise NadoContractError("funding exposure cumulative state is not the baseline")
        entries = [intent for intent, _state in self.intents() if intent.kind == ENTRY]
        if (
            len(entries) != 1
            or self.state(entries[0].digest) != "RECONCILED"
            or self.count_kind(CLOSE) != 0
            or self.lifecycle_status() != RUNNING
        ):
            raise NadoContractError(
                "funding exposure must bind after reconciled entry and before close"
            )
        encoded = canonical_payload(_nado_exposure_payload(exposure) | {
            "exposure_digest": _hash_text(
                exposure.exposure_digest, "Nado funding exposure digest"
            ),
        })
        digest = hashlib.sha256(encoded).hexdigest()
        try:
            with self._connection:
                row = self._connection.execute(
                    "SELECT exposure_json, exposure_digest "
                    "FROM nado_funding_exposure WHERE singleton = 1"
                ).fetchone()
                if row is not None:
                    if bytes(row[0]) != encoded or str(row[1]) != digest:
                        raise NadoContractError(
                            "persisted funding exposure is immutable"
                        )
                    if track_progression:
                        if not self._funding_progression_activation_required():
                            raise NadoContractError(
                                "funding progression activation is missing"
                            )
                        if not self._funding_progression_table_exists():
                            raise NadoContractError(
                                "funding progression schema is unavailable"
                            )
                        self._validate_funding_progression()
                    return
                if track_progression:
                    # Activation is durable with the exposure row, so an
                    # interrupted first marker cannot reopen as an untracked
                    # post-exposure lifecycle.
                    self._ensure_funding_progression_activation_schema()
                    self._ensure_funding_progression_schema()
                    self._connection.execute(
                        """
                        INSERT INTO nado_funding_progression_activation
                            (singleton, requirement, binding_digest)
                        VALUES (1, ?, ?)
                        """,
                        (
                            FUNDING_PROGRESSION_REQUIRED,
                            _funding_boundary_digest(binding),
                        ),
                    )
                self._connection.execute(
                    "INSERT INTO nado_funding_exposure "
                    "(singleton, exposure_json, exposure_digest) VALUES (1, ?, ?)",
                    (encoded, digest),
                )
        except sqlite3.DatabaseError:
            raise NadoContractError("funding exposure persistence failed") from None

    def funding_boundary_binding(self) -> FundingBoundaryBinding | None:
        row = self._connection.execute(
            "SELECT binding_json, binding_digest FROM nado_funding_boundary "
            "WHERE singleton = 1"
        ).fetchone()
        if row is None:
            return None
        try:
            encoded = bytes(row[0])
            if hashlib.sha256(encoded).hexdigest() != str(row[1]):
                raise NadoContractError("persisted funding boundary digest is invalid")
            binding_payload = json.loads(encoded.decode("ascii"))
            payload = _mapping_payload(
                binding_payload,
                "funding boundary",
                {"route", "risex_journal", "nado_journal"},
            )
            binding = FundingBoundaryBinding(
                _route_from_payload(payload["route"]),
                _journal_from_payload(payload["risex_journal"]),
                _journal_from_payload(payload["nado_journal"]),
            )
            if canonical_payload(_funding_boundary_payload(binding)) != encoded:
                raise NadoContractError("persisted funding boundary is not canonical")
            return binding
        except (
            KeyError, UnicodeDecodeError, json.JSONDecodeError,
            InvalidOperation, TypeError, ValueError,
        ):
            raise NadoContractError("persisted funding boundary is invalid") from None

    def funding_boundary_baseline(self) -> NadoFundingBaseline | None:
        row = self._connection.execute(
            "SELECT baseline_json, baseline_digest FROM nado_funding_baseline "
            "WHERE singleton = 1"
        ).fetchone()
        if row is None:
            return None
        try:
            encoded = bytes(row[0])
            if hashlib.sha256(encoded).hexdigest() != str(row[1]):
                raise NadoContractError("persisted funding baseline digest is invalid")
            baseline = _baseline_from_payload(json.loads(encoded.decode("ascii")))
            canonical = canonical_payload(_nado_baseline_payload(baseline) | {
                "baseline_digest": _hash_text(
                    baseline.baseline_digest, "Nado funding baseline digest"
                ),
            })
            if canonical != encoded:
                raise NadoContractError("persisted funding baseline is not canonical")
            return baseline
        except (
            KeyError, UnicodeDecodeError, json.JSONDecodeError,
            InvalidOperation, TypeError, ValueError,
        ):
            raise NadoContractError("persisted funding baseline is invalid") from None

    def funding_boundary_exposure(self) -> NadoFundingExposure | None:
        row = self._connection.execute(
            "SELECT exposure_json, exposure_digest FROM nado_funding_exposure "
            "WHERE singleton = 1"
        ).fetchone()
        if row is None:
            return None
        try:
            encoded = bytes(row[0])
            if hashlib.sha256(encoded).hexdigest() != str(row[1]):
                raise NadoContractError("persisted funding exposure digest is invalid")
            exposure = _exposure_from_payload(json.loads(encoded.decode("ascii")))
            canonical = canonical_payload(_nado_exposure_payload(exposure) | {
                "exposure_digest": _hash_text(
                    exposure.exposure_digest, "Nado funding exposure digest"
                ),
            })
            if canonical != encoded:
                raise NadoContractError("persisted funding exposure is not canonical")
            return exposure
        except (
            KeyError, UnicodeDecodeError, json.JSONDecodeError,
            InvalidOperation, TypeError, ValueError,
        ):
            raise NadoContractError("persisted funding exposure is invalid") from None

    def _assert_stored_attestation(
        self,
        binding: FundingBoundaryBinding,
        attestation: CrossRunAttestation,
    ) -> None:
        attestation.assert_contract()
        if _funding_route_payload(attestation.route) != _funding_route_payload(
            binding.route
        ):
            raise NadoContractError("funding blocker route is not persisted")
        if attestation.risex_journal != binding.risex_journal:
            raise NadoContractError("funding blocker RISEx journal is not persisted")
        if attestation.nado_journal != binding.nado_journal:
            raise NadoContractError("funding blocker Nado journal is not persisted")

    def record_nado_funding_blocker(
        self,
        *,
        attestation: CrossRunAttestation | None,
        reason: str,
    ) -> NadoFundingBoundaryResult:
        """Durably block on missing/contradictory post-boundary evidence."""
        binding = self.funding_boundary_binding()
        baseline = self.funding_boundary_baseline()
        if binding is None or baseline is None:
            self.halt()
            raise NadoContractError("funding blocker lacks a persisted baseline")
        if reason not in FUNDING_BLOCKER_REASONS:
            self.halt()
            raise NadoContractError("funding blocker reason is not bounded")
        if attestation is None:
            self.halt()
            raise NadoContractError("funding blocker lacks final attestation")
        try:
            self._assert_stored_attestation(binding, attestation)
            payload = {
                "reason": reason,
                "attestation": _stored_attestation_payload(attestation),
            }
            encoded = canonical_payload(payload)
            digest = hashlib.sha256(encoded).hexdigest()
            with self._connection:
                existing = self._connection.execute(
                    "SELECT reason, attestation_json, blocker_digest "
                    "FROM nado_funding_blocker WHERE singleton = 1"
                ).fetchone()
                if existing is not None:
                    if (
                        str(existing[0]) != reason
                        or bytes(existing[1]) != encoded
                        or str(existing[2]) != digest
                    ):
                        raise NadoContractError(
                            "persisted funding blocker contradicts the first observation"
                        )
                else:
                    if self.lifecycle_status() != RUNNING:
                        raise NadoContractError(
                            "funding blocker requires a running lifecycle"
                        )
                    self._connection.execute(
                        "INSERT INTO nado_funding_blocker "
                        "(singleton, reason, attestation_json, blocker_digest) "
                        "VALUES (1, ?, ?, ?)",
                        (reason, encoded, digest),
                    )
                self._connection.execute(
                    "UPDATE nado_lifecycle_state SET status = 'HALTED' "
                    "WHERE singleton = 1 AND status != 'COMPLETE'"
                )
            return NadoFundingBoundaryResult(
                None,
                binding.route.nado_leg.market,
                binding.route.settlement_at_ms,
                FUNDING_UNRESOLVED,
                None,
                None,
                False,
                True,
                None,
                None,
                reason,
            )
        except NadoContractError:
            self.halt()
            raise
        except sqlite3.DatabaseError:
            self.halt()
            raise NadoContractError("funding blocker persistence failed") from None

    def record_nado_funding_boundary(
        self,
        *,
        attestation: CrossRunAttestation | None,
        event: NadoFundingEvent | None,
        account_funding: NadoAccountFunding | None,
    ) -> NadoFundingBoundaryResult:
        """Validate and durably record one exact Nado funding settlement."""
        binding = self.funding_boundary_binding()
        baseline = self.funding_boundary_baseline()
        exposure = self.funding_boundary_exposure()
        if binding is None or baseline is None:
            self.halt()
            raise NadoContractError("funding boundary baseline was not persisted")
        if event is None or account_funding is None or exposure is None:
            return self.record_nado_funding_blocker(
                attestation=attestation, reason=FUNDING_BLOCKED_MISSING
            )
        try:
            self._validate_funding_progression()
            result = validate_nado_funding_boundary(
                binding=binding,
                baseline=baseline,
                attestation=attestation,
                event=event,
                account_funding=account_funding,
                exposure=exposure,
            )
            if attestation is None:
                raise NadoContractError("funding boundary evidence is incomplete")
            encoded = canonical_payload(
                _funding_evidence_payload(
                    attestation, baseline, event, account_funding, exposure
                )
            )
            digest = hashlib.sha256(encoded).hexdigest()
            with self._connection:
                row = self._connection.execute(
                    "SELECT evidence_json, evidence_digest "
                    "FROM nado_funding_evidence WHERE singleton = 1"
                ).fetchone()
                if row is not None:
                    if bytes(row[0]) != encoded or str(row[1]) != digest:
                        raise NadoContractError(
                            "persisted funding evidence contradicts the first observation"
                        )
                    return result
                if self.lifecycle_status() != RUNNING:
                    raise NadoContractError(
                        "funding evidence requires a running lifecycle"
                    )
                self._connection.execute(
                    "INSERT INTO nado_funding_evidence "
                    "(singleton, evidence_json, evidence_digest) VALUES (1, ?, ?)",
                    (encoded, digest),
                )
            return result
        except NadoContractError:
            try:
                if attestation is not None:
                    self._assert_stored_attestation(binding, attestation)
                    return self.record_nado_funding_blocker(
                        attestation=attestation,
                        reason=FUNDING_BLOCKED_CONTRADICTORY,
                    )
            except NadoContractError:
                pass
            self.halt()
            raise
        except sqlite3.DatabaseError:
            self.halt()
            raise NadoContractError("funding evidence persistence failed") from None

    def nado_funding_boundary_evidence(
        self,
    ) -> tuple[
        CrossRunAttestation, NadoFundingBaseline, NadoFundingEvent,
        NadoAccountFunding, NadoFundingExposure
    ] | None:
        row = self._connection.execute(
            "SELECT evidence_json, evidence_digest FROM nado_funding_evidence "
            "WHERE singleton = 1"
        ).fetchone()
        if row is None:
            return None
        try:
            encoded = bytes(row[0])
            if hashlib.sha256(encoded).hexdigest() != str(row[1]):
                raise NadoContractError("persisted funding evidence digest is invalid")
            evidence = _funding_evidence_from_payload(
                json.loads(encoded.decode("ascii"))
            )
            if canonical_payload(_funding_evidence_payload(*evidence)) != encoded:
                raise NadoContractError("persisted funding evidence is not canonical")
            return evidence
        except (
            KeyError, UnicodeDecodeError, json.JSONDecodeError,
            InvalidOperation, TypeError, ValueError,
        ):
            raise NadoContractError("persisted funding evidence is invalid") from None

    def funding_boundary_blocker(self) -> str | None:
        row = self._connection.execute(
            "SELECT reason, attestation_json, blocker_digest "
            "FROM nado_funding_blocker WHERE singleton = 1"
        ).fetchone()
        if row is None:
            return None
        try:
            encoded = bytes(row[1])
            if hashlib.sha256(encoded).hexdigest() != str(row[2]):
                raise NadoContractError("persisted funding blocker digest is invalid")
            payload = _mapping_payload(
                json.loads(encoded.decode("ascii")),
                "funding blocker",
                {"reason", "attestation"},
            )
            if payload["reason"] != row[0] or payload["reason"] not in FUNDING_BLOCKER_REASONS:
                raise NadoContractError("persisted funding blocker reason is invalid")
            binding = self.funding_boundary_binding()
            if binding is None:
                raise NadoContractError("persisted funding blocker is unbound")
            attestation = _attestation_from_payload(payload["attestation"])
            self._assert_stored_attestation(binding, attestation)
            if canonical_payload(payload) != encoded:
                raise NadoContractError("persisted funding blocker is not canonical")
            return str(row[0])
        except (
            KeyError, UnicodeDecodeError, json.JSONDecodeError,
            InvalidOperation, TypeError, ValueError,
        ):
            raise NadoContractError("persisted funding blocker is invalid") from None

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

    def dispatch_prepared(
        self,
        digest: str,
        dispatch: Callable[[OrderIntent], str],
    ) -> str:
        """Dispatch one already-durable intent exactly once.

        The operational binding deliberately prepares through ``LifecycleCore``
        before obtaining a signature.  A complete venue rejection is terminal;
        every ambiguous outcome permanently closes the automatic gate.
        """
        intent = self.get(digest)
        if self.state(digest) != "PREPARED" or self.lifecycle_status() != RUNNING:
            raise NadoContractError("only one prepared intent may be dispatched")
        try:
            result = dispatch(intent)
        except ExecuteFailure as error:
            self._record_execute_failure(digest, error)
            raise
        except BaseException:
            self._record_execute_failure(
                digest, ExecuteFailure(EXECUTE_TRANSPORT_AMBIGUITY)
            )
            raise
        self._mark(digest, "DISPATCHED")
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
        direction: str = LONG,
    ) -> OrderIntent:
        if order.appendix not in {POST_ONLY_APPENDIX, IOC_APPENDIX}:
            raise NadoContractError("entry must use an accepted bounded order type")
        if direction not in {LONG, SHORT}:
            raise NadoContractError("entry direction is invalid")
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
        expected_amount = plan.amount_x18 if direction == LONG else -plan.amount_x18
        if order.amount_x18 != expected_amount:
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
            executable_minimum = smallest_executable_amount(
                product, prices_x18=(worst_price_x18,)
            )
            submitted_amount = max(absolute_position, executable_minimum)
            clamp_expected = absolute_position < submitted_amount
            if submitted_amount % product.step_x18:
                raise NadoContractError("close amount is off the x18 product step")
            notional = _notional_x18(worst_price_x18, submitted_amount)
            if notional < product.minimum_notional_x18:
                raise NadoContractError("close notional violates minimum")
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
    mark_complete: bool = True,
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
    if mark_complete:
        try:
            store._mark_complete()
        except NadoContractError:
            return False
    return True
