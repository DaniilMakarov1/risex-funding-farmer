"""Disarmed, fixture-driven Nado private-read preflight contract.

There is no ambient credential, filesystem, HTTP, CLI, or normal Farmer import
surface here.  All reads and the one signed observation are supplied by the
caller.  CI uses the synthetic observer path; the operational boundary is an
injected object and is deliberately unreachable from ``run_fixture_preflight``.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping, Protocol


NEW = "NEW"
CLAIMED = "CLAIMED"
OBSERVED = "OBSERVED"
FINALIZED = "FINALIZED"
ZERO_ADDRESS = "0x" + "00" * 20
MIN_COLLATERAL_X18 = 5 * 10**18
MAX_FRESHNESS_MS = 30_000

SOURCE_PINS = {
    "typescript_sdk": "315e4f23dadefeb2f86f713e423241e81467d4c3",
    "rust_sdk": "e54118786b171a4325871d5bd17e5abae0e90c5a",
    "contracts": "11c27b2851999f1b4f8cb4a7fbfcc9320253f12f",
}


class NadoPreflightError(RuntimeError):
    """A fail-closed identity, evidence, or one-shot-state violation."""


class FixedPreflightIdentity:
    chain_id = 763373
    domain_name = "Nado"
    domain_version = "0.0.1"
    endpoint = "0x698D87105274292B5673367DEC81874Ce3633Ac2"
    gateway = "https://gateway.test.nado.xyz/v1"
    trigger = "https://trigger.test.nado.xyz/v1"
    gateway_query = gateway + "/query"
    trigger_query = trigger + "/query"

    @classmethod
    def as_dict(cls) -> dict[str, object]:
        return {
            "chain_id": cls.chain_id,
            "domain_name": cls.domain_name,
            "domain_version": cls.domain_version,
            "endpoint": cls.endpoint,
            "gateway": cls.gateway,
            "trigger": cls.trigger,
        }


def _address_bytes(address: str) -> bytes:
    if not isinstance(address, str) or not address.startswith("0x"):
        raise NadoPreflightError("owner address is invalid")
    try:
        raw = bytes.fromhex(address[2:])
    except ValueError as exc:
        raise NadoPreflightError("owner address is invalid") from exc
    if len(raw) != 20 or raw == b"\0" * 20:
        raise NadoPreflightError("owner address is invalid")
    return raw


def _bytes32(value: str) -> bytes:
    if not isinstance(value, str) or not value.startswith("0x"):
        raise NadoPreflightError("subaccount identity is invalid")
    try:
        raw = bytes.fromhex(value[2:])
    except ValueError as exc:
        raise NadoPreflightError("subaccount identity is invalid") from exc
    if len(raw) != 32:
        raise NadoPreflightError("subaccount identity is invalid")
    return raw


def encode_subaccount(owner: str, subaccount_name: str) -> str:
    try:
        name = subaccount_name.encode("ascii")
    except (AttributeError, UnicodeEncodeError) as exc:
        raise NadoPreflightError("subaccount name is invalid") from exc
    if not 1 <= len(name) <= 12 or b"\0" in name:
        raise NadoPreflightError("subaccount name is invalid")
    return "0x" + (_address_bytes(owner) + name.ljust(12, b"\0")).hex()


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _redacted_identity(sender: str) -> str:
    return _identity_hash(sender)[:16]


def _identity_hash(sender: str) -> str:
    return hashlib.sha256(_bytes32(sender)).hexdigest()


@dataclass(frozen=True)
class PreflightConfig:
    owner: str
    subaccount_name: str
    sender: str
    invocation_id: str
    exclusive_owner_lease: bool
    direct_owner_eoa: bool
    now_ms: int

    def __post_init__(self) -> None:
        if (
            encode_subaccount(self.owner, self.subaccount_name).lower()
            != self.sender.lower()
        ):
            raise NadoPreflightError("subaccount identity mismatch")
        if not self.invocation_id or not self.exclusive_owner_lease:
            raise NadoPreflightError("exclusive invocation identity is required")
        if len(self.invocation_id) > 64 or not all(
            character.isascii() and (character.isalnum() or character in "._-")
            for character in self.invocation_id
        ):
            raise NadoPreflightError("invocation identity is invalid")
        if not self.direct_owner_eoa:
            raise NadoPreflightError("direct owner EOA is required")
        if type(self.now_ms) is not int or self.now_ms <= 0:
            raise NadoPreflightError("reference time is invalid")


class PublicReader(Protocol):
    gateway_url: str
    trust_env: bool
    allow_redirects: bool
    tls_verified: bool
    timeout_ms: int
    max_response_bytes: int

    def read(self, operation: str) -> Mapping[str, object]: ...


class SyntheticSignedObserver(Protocol):
    server_time_ms: int
    trigger_url: str
    trust_env: bool
    allow_redirects: bool
    tls_verified: bool
    timeout_ms: int
    max_response_bytes: int

    def observe(
        self, request: dict[str, object], typed_data: dict[str, object],
    ) -> Mapping[str, object]: ...


@dataclass(frozen=True)
class OperationalSignedObserver:
    """Disarmed injected operational boundary, never selected by fixture runs."""

    credential_loader: Callable[[], object]
    derive_owner: Callable[[object], str]
    signer: Callable[[object, Mapping[str, object]], object]
    server_time: Callable[[], int]
    private_post: Callable[[Mapping[str, object], object], Mapping[str, object]]
    trigger_url: str = FixedPreflightIdentity.trigger_query
    trust_env: bool = False
    allow_redirects: bool = False
    tls_verified: bool = True
    timeout_ms: int = 5_000
    max_response_bytes: int = 65_536

    def __post_init__(self) -> None:
        if (
            self.trigger_url != FixedPreflightIdentity.trigger_query
            or self.trust_env
            or self.allow_redirects
            or not self.tls_verified
            or type(self.timeout_ms) is not int
            or not 1 <= self.timeout_ms <= MAX_FRESHNESS_MS
            or type(self.max_response_bytes) is not int
            or not 1 <= self.max_response_bytes <= 1_048_576
        ):
            raise NadoPreflightError("signed transport policy mismatch")

    def observe(self, *, owner: str, sender: str) -> Mapping[str, object]:
        try:
            credential = self.credential_loader()
            if self.derive_owner(credential).lower() != owner.lower():
                raise NadoPreflightError("credential owner identity mismatch")
            server_ms = self.server_time()
            if type(server_ms) is not int or server_ms <= 0:
                raise NadoPreflightError("server time is invalid")
            request = _trigger_request(sender, server_ms)
            typed = list_trigger_orders_typed_data(sender, int(request["recv_time"]))
            signature = self.signer(credential, typed)
            return self.private_post(request, signature)
        except NadoPreflightError:
            raise
        except BaseException:
            raise NadoPreflightError("signed observation boundary failed") from None


class OneShotStore:
    """Durable claim/observe/finalize ledger with no re-arm operation."""

    def __init__(self, path: Path) -> None:
        self._connection = sqlite3.connect(path)
        self._connection.execute("PRAGMA synchronous=FULL")
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS nado_preflight_one_shot (
                invocation_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                identity_tag TEXT NOT NULL,
                round_a_hash TEXT NOT NULL,
                round_a_observed_ms INTEGER NOT NULL,
                trigger_hash TEXT,
                trigger_observed_ms INTEGER
            )
            """
        )
        self._connection.commit()

    def state(self, invocation_id: str) -> str:
        row = self._connection.execute(
            "SELECT state FROM nado_preflight_one_shot WHERE invocation_id = ?",
            (invocation_id,),
        ).fetchone()
        return NEW if row is None else str(row[0])

    def claim(
        self, invocation_id: str, identity_tag: str, round_a_hash: str,
        round_a_observed_ms: int,
    ) -> None:
        try:
            with self._connection:
                self._connection.execute(
                    "INSERT INTO nado_preflight_one_shot VALUES (?, ?, ?, ?, ?, NULL, NULL)",
                    (invocation_id, CLAIMED, identity_tag, round_a_hash, round_a_observed_ms),
                )
        except sqlite3.IntegrityError:
            raise NadoPreflightError("signed observation is already claimed") from None

    def observe(self, invocation_id: str, trigger_hash: str, observed_at_ms: int) -> None:
        with self._connection:
            cursor = self._connection.execute(
                """UPDATE nado_preflight_one_shot
                   SET state = ?, trigger_hash = ?, trigger_observed_ms = ?
                   WHERE invocation_id = ? AND state = ?""",
                (OBSERVED, trigger_hash, observed_at_ms, invocation_id, CLAIMED),
            )
        if cursor.rowcount != 1:
            raise NadoPreflightError("one-shot claim cannot be observed")

    def evidence(
        self, invocation_id: str,
    ) -> tuple[str, str, str, int, str | None, int | None] | None:
        row = self._connection.execute(
            """SELECT state, identity_tag, round_a_hash, round_a_observed_ms,
                      trigger_hash, trigger_observed_ms
               FROM nado_preflight_one_shot WHERE invocation_id = ?""",
            (invocation_id,),
        ).fetchone()
        return None if row is None else (
            str(row[0]), str(row[1]), str(row[2]), int(row[3]),
            None if row[4] is None else str(row[4]),
            None if row[5] is None else int(row[5]),
        )

    def finalize(self, invocation_id: str, round_b_hash: str) -> None:
        with self._connection:
            cursor = self._connection.execute(
                """UPDATE nado_preflight_one_shot SET state = ?
                   WHERE invocation_id = ? AND state = ? AND round_a_hash = ?
                         AND trigger_hash IS NOT NULL""",
                (FINALIZED, invocation_id, OBSERVED, round_b_hash),
            )
        if cursor.rowcount != 1:
            raise NadoPreflightError("public rounds disagree or observation is incomplete")


@dataclass(frozen=True)
class PreflightResult:
    status: str
    identity_tag: str
    zero_regular_orders: bool
    exact_flat: bool
    zero_trigger_history: bool


@dataclass(frozen=True)
class _RoundEvidence:
    fingerprint: str
    first_observed_ms: int
    last_observed_ms: int


def _exact_keys(value: Mapping[str, object], expected: set[str], label: str) -> None:
    if set(value) != expected:
        raise NadoPreflightError(f"{label} schema mismatch")


def _envelope(
    response: Mapping[str, object], *, operation: str | None, expected_url: str,
    now_ms: int, max_response_bytes: int,
) -> tuple[Mapping[str, object], int]:
    try:
        encoded_size = len(_canonical(response))
    except (TypeError, ValueError):
        raise NadoPreflightError("transport body schema mismatch") from None
    if encoded_size > max_response_bytes:
        raise NadoPreflightError("transport response size exceeded")
    keys = {"url", "final_url", "status", "observed_at_ms", "body"}
    if operation is not None:
        keys.add("op")
    _exact_keys(response, keys, "transport")
    if operation is not None and response["op"] != operation:
        raise NadoPreflightError("public operation identity mismatch")
    if response["url"] != expected_url or response["final_url"] != expected_url:
        raise NadoPreflightError("transport host or redirect mismatch")
    if response["status"] != 200:
        raise NadoPreflightError("public transport rejected")
    observed = response["observed_at_ms"]
    if (
        type(observed) is not int
        or observed > now_ms
        or now_ms - observed > MAX_FRESHNESS_MS
    ):
        raise NadoPreflightError("transport observation is not fresh")
    body = response["body"]
    if not isinstance(body, Mapping):
        raise NadoPreflightError("transport body schema mismatch")
    return body, observed


def _read(
    reader: PublicReader, operation: str, *, now_ms: int,
) -> tuple[Mapping[str, object], int]:
    try:
        response = reader.read(operation)
    except NadoPreflightError:
        raise
    except Exception:
        raise NadoPreflightError("public read failed") from None
    return _envelope(
        response, operation=operation,
        expected_url=FixedPreflightIdentity.gateway_query, now_ms=now_ms,
        max_response_bytes=reader.max_response_bytes,
    )


def _reader_policy(reader: PublicReader) -> None:
    if (
        reader.gateway_url != FixedPreflightIdentity.gateway_query
        or reader.trust_env
        or reader.allow_redirects
        or not reader.tls_verified
        or type(reader.timeout_ms) is not int
        or reader.timeout_ms <= 0
        or type(reader.max_response_bytes) is not int
        or not 1 <= reader.max_response_bytes <= 1_048_576
    ):
        raise NadoPreflightError("public transport policy mismatch")


def _observer_policy(observer: SyntheticSignedObserver) -> None:
    if (
        observer.trigger_url != FixedPreflightIdentity.trigger_query
        or observer.trust_env
        or observer.allow_redirects
        or not observer.tls_verified
        or type(getattr(observer, "timeout_ms", None)) is not int
        or not 1 <= getattr(observer, "timeout_ms", 0) <= MAX_FRESHNESS_MS
        or type(observer.max_response_bytes) is not int
        or not 1 <= observer.max_response_bytes <= 1_048_576
    ):
        raise NadoPreflightError("signed transport policy mismatch")


def _temporal(observed: list[int]) -> None:
    if not observed or any(later <= earlier for earlier, later in zip(observed, observed[1:])):
        raise NadoPreflightError("public evidence temporal order mismatch")


def _validate_contracts(body: Mapping[str, object]) -> None:
    _exact_keys(body, {"chain_id", "endpoint"}, "contracts")
    if (
        body["chain_id"] != FixedPreflightIdentity.chain_id
        or str(body["endpoint"]).lower() != FixedPreflightIdentity.endpoint.lower()
    ):
        raise NadoPreflightError("contracts identity mismatch")


def _validate_status(body: Mapping[str, object]) -> None:
    _exact_keys(body, {"status"}, "status")
    if body["status"] != "active":
        raise NadoPreflightError("engine is not active")


def _catalog(body: Mapping[str, object]) -> tuple[tuple[int, str, str], ...]:
    _exact_keys(body, {"complete", "products"}, "catalog")
    if body["complete"] is not True or not isinstance(body["products"], list):
        raise NadoPreflightError("complete catalog is required")
    products: dict[int, tuple[int, str, str]] = {}
    for raw in body["products"]:
        if not isinstance(raw, Mapping):
            raise NadoPreflightError("catalog schema mismatch")
        _exact_keys(raw, {"product_id", "symbol", "product_type"}, "product")
        product_id = raw["product_id"]
        symbol = raw["symbol"]
        product_type = raw["product_type"]
        if (
            type(product_id) is not int
            or not 0 <= product_id < 2**32
            or not isinstance(symbol, str)
            or not symbol
            or product_type not in ("spot", "perp")
            or product_id in products
        ):
            raise NadoPreflightError("duplicate or invalid catalog product")
        products[product_id] = (product_id, symbol, str(product_type))
    if not products or products.get(0) != (0, "USDT0", "spot"):
        raise NadoPreflightError("official collateral product identity mismatch")
    return tuple(products[key] for key in sorted(products))


def _sender(body: Mapping[str, object], expected: str) -> None:
    if str(body.get("sender", "")).lower() != expected.lower():
        raise NadoPreflightError("subaccount identity mismatch")


def _linked_signer(body: Mapping[str, object], sender: str) -> None:
    _exact_keys(body, {"sender", "linked_signer"}, "linked signer")
    _sender(body, sender)
    if str(body["linked_signer"]).lower() != ZERO_ADDRESS:
        raise NadoPreflightError("linked signer is not zero")


def _integer(value: object, label: str) -> int:
    if isinstance(value, bool):
        raise NadoPreflightError(f"{label} is invalid")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise NadoPreflightError(f"{label} is invalid") from exc
    if str(parsed) != str(value):
        raise NadoPreflightError(f"{label} is noncanonical")
    return parsed


def _product_key_map(value: object, label: str) -> dict[int, object]:
    if not isinstance(value, Mapping):
        raise NadoPreflightError(f"{label} coverage is unexplained")
    normalized: dict[int, object] = {}
    for key, item in value.items():
        if (
            type(key) is not str
            or not key
            or not key.isascii()
            or not key.isdecimal()
            or (len(key) > 1 and key.startswith("0"))
        ):
            raise NadoPreflightError(f"{label} product identity is noncanonical")
        product_id = int(key)
        if str(product_id) != key or product_id in normalized:
            raise NadoPreflightError(f"{label} product identity is noncanonical")
        normalized[product_id] = item
    return normalized


def _account(
    body: Mapping[str, object], sender: str,
    catalog: tuple[tuple[int, str, str], ...],
) -> dict[str, object]:
    _exact_keys(
        body,
        {"sender", "exists", "health", "spot_balances", "perp_balances", "perp_count"},
        "account",
    )
    _sender(body, sender)
    if body["exists"] is not True:
        raise NadoPreflightError("subaccount does not exist")
    health = _integer(body["health"], "health")
    if health <= 0:
        raise NadoPreflightError("health is not positive")
    spot_ids = {product_id for product_id, _, kind in catalog if kind == "spot"}
    perp_ids = {product_id for product_id, _, kind in catalog if kind == "perp"}
    spots = body["spot_balances"]
    perps = body["perp_balances"]
    keyed_spots = _product_key_map(spots, "spot balance")
    if set(keyed_spots) != spot_ids:
        raise NadoPreflightError("spot balance coverage is unexplained")
    normalized_spots = {
        key: _integer(value, "spot balance") for key, value in keyed_spots.items()
    }
    if any(value < 0 for value in normalized_spots.values()):
        raise NadoPreflightError("negative spot balance")
    if normalized_spots.get(0, -1) < MIN_COLLATERAL_X18:
        raise NadoPreflightError("collateral floor is not met")
    if any(value for key, value in normalized_spots.items() if key != 0):
        raise NadoPreflightError("unexplained spot balance")
    keyed_perps = _product_key_map(perps, "cross-perp")
    if set(keyed_perps) != perp_ids:
        raise NadoPreflightError("cross-perp coverage is incomplete")
    normalized_perps: dict[int, tuple[int, int]] = {}
    for key, raw in keyed_perps.items():
        if not isinstance(raw, Mapping):
            raise NadoPreflightError("cross-perp schema mismatch")
        _exact_keys(raw, {"amount", "v_quote_balance"}, "cross-perp")
        amount = _integer(raw["amount"], "cross-perp amount")
        v_quote = _integer(raw["v_quote_balance"], "v_quote")
        if amount != 0:
            raise NadoPreflightError("cross-perp is not exactly flat")
        if v_quote != 0:
            raise NadoPreflightError("unexplained v_quote balance")
        normalized_perps[key] = (amount, v_quote)
    return {
        "health": health,
        "spots": normalized_spots,
        "perps": normalized_perps,
    }


def _orders(body: Mapping[str, object], sender: str, product_id: int) -> None:
    _exact_keys(body, {"sender", "product_id", "orders"}, "regular orders")
    _sender(body, sender)
    if body["product_id"] != product_id:
        raise NadoPreflightError("regular-order product identity mismatch")
    if body["orders"] != []:
        raise NadoPreflightError("regular order exists")


def _isolated(body: Mapping[str, object], sender: str) -> None:
    _exact_keys(body, {"sender", "positions"}, "isolated positions")
    _sender(body, sender)
    if body["positions"] != []:
        raise NadoPreflightError("isolated position exists")


def _round_a(reader: PublicReader, config: PreflightConfig) -> _RoundEvidence:
    observed: list[int] = []
    contracts, at = _read(reader, "contracts", now_ms=config.now_ms)
    observed.append(at)
    _validate_contracts(contracts)
    status, at = _read(reader, "status", now_ms=config.now_ms)
    observed.append(at)
    _validate_status(status)
    products_body, at = _read(reader, "all_products", now_ms=config.now_ms)
    observed.append(at)
    products = _catalog(products_body)
    linked, at = _read(reader, "linked_signer", now_ms=config.now_ms)
    observed.append(at)
    _linked_signer(linked, config.sender)
    account_body, at = _read(reader, "subaccount_info", now_ms=config.now_ms)
    observed.append(at)
    account = _account(account_body, config.sender, products)
    for product_id, _, _ in products:
        orders, at = _read(reader, f"open_orders:{product_id}", now_ms=config.now_ms)
        observed.append(at)
        _orders(orders, config.sender, product_id)
    isolated, at = _read(reader, "isolated_positions", now_ms=config.now_ms)
    observed.append(at)
    _isolated(isolated, config.sender)
    _temporal(observed)
    return _RoundEvidence(
        _digest({"catalog": products, "account": account}), observed[0], observed[-1]
    )


def _round_b(reader: PublicReader, config: PreflightConfig) -> _RoundEvidence:
    observed: list[int] = []
    products_body, at = _read(reader, "all_products", now_ms=config.now_ms)
    observed.append(at)
    products = _catalog(products_body)
    for product_id, _, _ in products:
        orders, at = _read(reader, f"open_orders:{product_id}", now_ms=config.now_ms)
        observed.append(at)
        _orders(orders, config.sender, product_id)
    account_body, at = _read(reader, "subaccount_info", now_ms=config.now_ms)
    observed.append(at)
    account = _account(account_body, config.sender, products)
    isolated, at = _read(reader, "isolated_positions", now_ms=config.now_ms)
    observed.append(at)
    _isolated(isolated, config.sender)
    for product_id, _, _ in products:
        orders, at = _read(reader, f"open_orders:{product_id}", now_ms=config.now_ms)
        observed.append(at)
        _orders(orders, config.sender, product_id)
    contracts, at = _read(reader, "contracts", now_ms=config.now_ms)
    observed.append(at)
    _validate_contracts(contracts)
    status, at = _read(reader, "status", now_ms=config.now_ms)
    observed.append(at)
    _validate_status(status)
    final_products_body, at = _read(reader, "all_products", now_ms=config.now_ms)
    observed.append(at)
    if _catalog(final_products_body) != products:
        raise NadoPreflightError("round-B catalog changed")
    linked, at = _read(reader, "linked_signer", now_ms=config.now_ms)
    observed.append(at)
    _linked_signer(linked, config.sender)
    _temporal(observed)
    return _RoundEvidence(
        _digest({"catalog": products, "account": account}), observed[0], observed[-1]
    )


def _trigger_request(sender: str, server_time_ms: int) -> dict[str, object]:
    if type(server_time_ms) is not int or server_time_ms <= 0:
        raise NadoPreflightError("server time is invalid")
    return {
        "type": "list_trigger_orders",
        "sender": sender,
        "recv_time": server_time_ms + 30_000,
        "limit": 1,
    }


def list_trigger_orders_typed_data(sender: str, recv_time: int) -> dict[str, object]:
    _bytes32(sender)
    if type(recv_time) is not int or not 0 <= recv_time < 2**64:
        raise NadoPreflightError("trigger receive time is invalid")
    return {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            "ListTriggerOrders": [
                {"name": "sender", "type": "bytes32"},
                {"name": "recvTime", "type": "uint64"},
            ],
        },
        "primaryType": "ListTriggerOrders",
        "domain": {
            "name": FixedPreflightIdentity.domain_name,
            "version": FixedPreflightIdentity.domain_version,
            "chainId": FixedPreflightIdentity.chain_id,
            "verifyingContract": FixedPreflightIdentity.endpoint,
        },
        "message": {"sender": sender, "recvTime": recv_time},
    }


def _trigger_zero(
    response: Mapping[str, object], config: PreflightConfig, *, max_response_bytes: int,
) -> tuple[str, int]:
    body, observed = _envelope(
        response, operation=None, expected_url=FixedPreflightIdentity.trigger_query,
        now_ms=config.now_ms, max_response_bytes=max_response_bytes,
    )
    _exact_keys(body, {"sender", "orders"}, "trigger response")
    _sender(body, config.sender)
    if body["orders"] != []:
        raise NadoPreflightError("trigger history is not zero")
    return (
        _digest({"identity": _redacted_identity(config.sender), "orders_empty": True}),
        observed,
    )


def run_fixture_preflight(
    *,
    config: PreflightConfig,
    public_reader: PublicReader,
    synthetic_observer: SyntheticSignedObserver,
    operational_observer: OperationalSignedObserver | None,
    store: OneShotStore,
) -> PreflightResult:
    """Run only the deterministic fixture path; operational observer stays inert."""
    del operational_observer
    identity_tag = _redacted_identity(config.sender)
    durable_identity_hash = _identity_hash(config.sender)
    _reader_policy(public_reader)
    _observer_policy(synthetic_observer)
    evidence = store.evidence(config.invocation_id)
    if evidence is None:
        round_a = _round_a(public_reader, config)
        store.claim(
            config.invocation_id, durable_identity_hash, round_a.fingerprint,
            round_a.last_observed_ms,
        )
        try:
            if (
                synthetic_observer.server_time_ms > config.now_ms
                or config.now_ms - synthetic_observer.server_time_ms > MAX_FRESHNESS_MS
                or synthetic_observer.server_time_ms <= round_a.last_observed_ms
            ):
                raise NadoPreflightError("server time is not fresh")
            request = _trigger_request(config.sender, synthetic_observer.server_time_ms)
            typed_data = list_trigger_orders_typed_data(
                config.sender, int(request["recv_time"])
            )
            response = synthetic_observer.observe(request, typed_data)
            trigger_hash, trigger_observed_ms = _trigger_zero(
                response, config,
                max_response_bytes=synthetic_observer.max_response_bytes,
            )
            if (
                trigger_observed_ms < synthetic_observer.server_time_ms
                or trigger_observed_ms <= round_a.last_observed_ms
            ):
                raise NadoPreflightError("signed observation temporal order mismatch")
        except BaseException:
            raise NadoPreflightError("signed observation is ambiguous and cannot be retried") from None
        store.observe(config.invocation_id, trigger_hash, trigger_observed_ms)
        evidence = store.evidence(config.invocation_id)
    if evidence is None:
        raise NadoPreflightError("durable one-shot evidence is unavailable")
    (
        state, stored_identity, round_a_hash, round_a_observed_ms,
        trigger_hash, trigger_observed_ms,
    ) = evidence
    if stored_identity != durable_identity_hash:
        raise NadoPreflightError("durable one-shot identity mismatch")
    if state == CLAIMED:
        raise NadoPreflightError("signed observation is already claimed and ambiguous")
    if state == FINALIZED:
        raise NadoPreflightError("preflight invocation is already finalized")
    if state != OBSERVED or trigger_hash is None or trigger_observed_ms is None:
        raise NadoPreflightError("durable one-shot state is invalid")
    expected_trigger_hash = _digest(
        {"identity": identity_tag, "orders_empty": True}
    )
    if trigger_hash != expected_trigger_hash:
        raise NadoPreflightError("durable trigger evidence is invalid")
    if trigger_observed_ms <= round_a_observed_ms:
        raise NadoPreflightError("durable temporal evidence is invalid")
    if (
        round_a_observed_ms > config.now_ms
        or trigger_observed_ms > config.now_ms
        or config.now_ms - round_a_observed_ms > MAX_FRESHNESS_MS
    ):
        raise NadoPreflightError("durable temporal evidence is not fresh")
    round_b = _round_b(public_reader, config)
    if round_b.first_observed_ms <= trigger_observed_ms:
        raise NadoPreflightError("public evidence temporal barrier mismatch")
    if round_b.last_observed_ms - round_a_observed_ms > MAX_FRESHNESS_MS:
        raise NadoPreflightError("public evidence temporal barrier is stale")
    if round_b.fingerprint != round_a_hash:
        raise NadoPreflightError("public rounds disagree")
    store.finalize(config.invocation_id, round_b.fingerprint)
    return PreflightResult(FINALIZED, identity_tag, True, True, True)
