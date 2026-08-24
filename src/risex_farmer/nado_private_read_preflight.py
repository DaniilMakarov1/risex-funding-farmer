"""Disarmed, injected Nado testnet private-read preflight.

The module owns transport identity and policy but contains no HTTP, credential,
or signing implementation.  Every external action is an injected callback.
It is not imported by Farmer startup and CI supplies only synthetic callbacks.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping


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
    """A sanitized fail-closed contract or durable-state violation."""


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


def _address_bytes(address: object) -> bytes:
    if type(address) is not str or not address.startswith("0x"):
        raise NadoPreflightError("owner address is invalid")
    try:
        raw = bytes.fromhex(address[2:])
    except ValueError:
        raise NadoPreflightError("owner address is invalid") from None
    if len(raw) != 20 or raw == b"\0" * 20:
        raise NadoPreflightError("owner address is invalid")
    return raw


def _bytes32(value: object) -> bytes:
    if type(value) is not str or not value.startswith("0x"):
        raise NadoPreflightError("subaccount identity is invalid")
    try:
        raw = bytes.fromhex(value[2:])
    except ValueError:
        raise NadoPreflightError("subaccount identity is invalid") from None
    if len(raw) != 32:
        raise NadoPreflightError("subaccount identity is invalid")
    return raw


def encode_subaccount(owner: str, subaccount_name: str) -> str:
    try:
        name = subaccount_name.encode("ascii")
    except (AttributeError, UnicodeEncodeError):
        raise NadoPreflightError("subaccount name is invalid") from None
    if not 1 <= len(name) <= 12 or b"\0" in name:
        raise NadoPreflightError("subaccount name is invalid")
    return "0x" + (_address_bytes(owner) + name.ljust(12, b"\0")).hex()


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError):
        raise NadoPreflightError("external response is not canonical JSON") from None


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _identity_hash(sender: str) -> str:
    return hashlib.sha256(_bytes32(sender)).hexdigest()


def _identity_tag(sender: str) -> str:
    return _identity_hash(sender)[:16]


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
        if type(self.exclusive_owner_lease) is not bool or not self.exclusive_owner_lease:
            raise NadoPreflightError("exclusive owner lease is required")
        if type(self.direct_owner_eoa) is not bool or not self.direct_owner_eoa:
            raise NadoPreflightError("direct owner EOA is required")
        if (
            type(self.invocation_id) is not str
            or not self.invocation_id
            or len(self.invocation_id) > 64
            or not all(
                character.isascii()
                and (character.isalnum() or character in "._-")
                for character in self.invocation_id
            )
        ):
            raise NadoPreflightError("invocation identity is invalid")
        if type(self.now_ms) is not int or self.now_ms <= 0:
            raise NadoPreflightError("reference time is invalid")


@dataclass(frozen=True)
class ObservedResponse:
    """Injected observation metadata plus an unmodified official wire payload."""

    url: object
    final_url: object
    http_status: object
    observed_at_ms: object
    payload: object


@dataclass(frozen=True)
class TransportPolicy:
    tls_verified: bool
    trust_env: bool
    allow_redirects: bool
    timeout_ms: int
    max_response_bytes: int


def _invoke(callback: Callable[..., object], *args: object) -> tuple[bool, object | None]:
    try:
        return True, callback(*args)
    except BaseException:
        return False, None


@dataclass(frozen=True)
class _SealedTransport:
    callback: Callable[..., object]
    trust_env: bool = False
    allow_redirects: bool = False
    tls_verified: bool = True
    timeout_ms: int = 5_000
    max_response_bytes: int = 65_536

    expected_url = ""
    failure_label = "external transport failed"

    def __post_init__(self) -> None:
        if (
            not callable(self.callback)
            or type(self.trust_env) is not bool
            or self.trust_env
            or type(self.allow_redirects) is not bool
            or self.allow_redirects
            or type(self.tls_verified) is not bool
            or not self.tls_verified
            or type(self.timeout_ms) is not int
            or not 1 <= self.timeout_ms <= MAX_FRESHNESS_MS
            or type(self.max_response_bytes) is not int
            or not 1 <= self.max_response_bytes <= 1_048_576
        ):
            raise NadoPreflightError("transport policy mismatch")

    def send(self, request: Mapping[str, object]) -> ObservedResponse:
        if not isinstance(request, Mapping):
            raise NadoPreflightError("request schema mismatch")
        policy = TransportPolicy(
            self.tls_verified, self.trust_env, self.allow_redirects,
            self.timeout_ms, self.max_response_bytes,
        )
        ok, raw = _invoke(self.callback, self.expected_url, dict(request), policy)
        if not ok:
            raise NadoPreflightError(self.failure_label)
        if type(raw) is not ObservedResponse:
            raise NadoPreflightError("transport observation schema mismatch")
        if raw.url != self.expected_url or raw.final_url != self.expected_url:
            raise NadoPreflightError("transport host or redirect mismatch")
        if type(raw.http_status) is not int or raw.http_status != 200:
            raise NadoPreflightError("transport HTTP status rejected")
        if type(raw.observed_at_ms) is not int or raw.observed_at_ms <= 0:
            raise NadoPreflightError("transport observation time is invalid")
        if len(_canonical(raw.payload)) > self.max_response_bytes:
            raise NadoPreflightError("transport response size exceeded")
        return raw


@dataclass(frozen=True)
class SealedPublicTransport(_SealedTransport):
    expected_url = FixedPreflightIdentity.gateway_query
    failure_label = "public transport callback failed"


@dataclass(frozen=True)
class SealedSignedTransport(_SealedTransport):
    expected_url = FixedPreflightIdentity.trigger_query
    failure_label = "signed transport callback failed"


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
        self, invocation_id: str, identity_hash: str, round_a_hash: str,
        round_a_observed_ms: int,
    ) -> None:
        try:
            with self._connection:
                self._connection.execute(
                    "INSERT INTO nado_preflight_one_shot VALUES (?, ?, ?, ?, ?, NULL, NULL)",
                    (invocation_id, CLAIMED, identity_hash, round_a_hash, round_a_observed_ms),
                )
        except sqlite3.IntegrityError:
            raise NadoPreflightError("signed observation is already claimed") from None

    def observe(self, invocation_id: str, trigger_hash: str, observed_ms: int) -> None:
        with self._connection:
            cursor = self._connection.execute(
                """UPDATE nado_preflight_one_shot
                   SET state = ?, trigger_hash = ?, trigger_observed_ms = ?
                   WHERE invocation_id = ? AND state = ?""",
                (OBSERVED, trigger_hash, observed_ms, invocation_id, CLAIMED),
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
        if row is None:
            return None
        return (
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


def _wire_data(observation: ObservedResponse, now_ms: int) -> tuple[Mapping[str, object], int]:
    observed = observation.observed_at_ms
    if observed > now_ms or now_ms - observed > MAX_FRESHNESS_MS:
        raise NadoPreflightError("transport observation is not fresh")
    payload = observation.payload
    if type(payload) is not dict:
        raise NadoPreflightError("wire envelope schema mismatch")
    _exact_keys(payload, {"status", "data"}, "wire envelope")
    if type(payload["status"]) is not str or payload["status"] != "success":
        raise NadoPreflightError("wire status is not success")
    data = payload["data"]
    if type(data) is not dict:
        raise NadoPreflightError("wire data schema mismatch")
    return data, observed


def _query(
    transport: SealedPublicTransport, request: Mapping[str, object], now_ms: int,
) -> tuple[Mapping[str, object], int]:
    return _wire_data(transport.send(request), now_ms)


def _strict_uint(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise NadoPreflightError(f"{label} is invalid")
    return value


def _decimal(value: object, label: str, *, signed: bool = True) -> int:
    if type(value) is not str or not value or not value.isascii():
        raise NadoPreflightError(f"{label} is invalid")
    negative = value.startswith("-")
    digits = value[1:] if negative else value
    if (
        not digits
        or not all("0" <= character <= "9" for character in digits)
        or (len(digits) > 1 and digits.startswith("0"))
        or (negative and (not signed or digits == "0"))
    ):
        raise NadoPreflightError(f"{label} is noncanonical")
    return int(value)


def _contracts(data: Mapping[str, object]) -> None:
    _exact_keys(data, {"chain_id", "endpoint_addr"}, "contracts")
    chain = _decimal(data["chain_id"], "chain id", signed=False)
    endpoint = data["endpoint_addr"]
    if chain != FixedPreflightIdentity.chain_id or (
        type(endpoint) is not str
        or endpoint.lower() != FixedPreflightIdentity.endpoint.lower()
    ):
        raise NadoPreflightError("contracts identity mismatch")


def _status(data: Mapping[str, object]) -> None:
    _exact_keys(data, {"status"}, "engine status")
    if type(data["status"]) is not str or data["status"] != "active":
        raise NadoPreflightError("engine is not active")


def _catalog(data: Mapping[str, object]) -> tuple[tuple[int, str, str], ...]:
    _exact_keys(data, {"spot_products", "perp_products"}, "all products")
    products: dict[int, tuple[int, str, str]] = {}
    for kind, field in (("spot", "spot_products"), ("perp", "perp_products")):
        raw_products = data[field]
        if type(raw_products) is not list:
            raise NadoPreflightError("product catalog schema mismatch")
        for raw in raw_products:
            if type(raw) is not dict:
                raise NadoPreflightError("product schema mismatch")
            _exact_keys(raw, {"product_id", "symbol"}, "product")
            product_id = _strict_uint(raw["product_id"], "product id")
            symbol = raw["symbol"]
            if type(symbol) is not str or not symbol or product_id in products:
                raise NadoPreflightError("product identity is invalid or duplicate")
            products[product_id] = (product_id, symbol, kind)
    if not products or products.get(0) != (0, "USDT0", "spot"):
        raise NadoPreflightError("official collateral identity mismatch")
    return tuple(products[key] for key in sorted(products))


def _linked(data: Mapping[str, object]) -> None:
    _exact_keys(data, {"linked_signer"}, "linked signer")
    signer = data["linked_signer"]
    if type(signer) is not str or signer.lower() != ZERO_ADDRESS:
        raise NadoPreflightError("linked signer is not zero")


def _balance_entries(
    raw: object, label: str, expected_ids: set[int], *, perp: bool,
) -> dict[int, tuple[int, int]]:
    if type(raw) is not list:
        raise NadoPreflightError(f"{label} array is required")
    parsed: dict[int, tuple[int, int]] = {}
    for entry in raw:
        if type(entry) is not dict:
            raise NadoPreflightError(f"{label} schema mismatch")
        _exact_keys(entry, {"product_id", "balance"}, label)
        product_id = _strict_uint(entry["product_id"], f"{label} product id")
        if product_id in parsed:
            raise NadoPreflightError(f"{label} product is duplicate")
        balance = entry["balance"]
        if type(balance) is not dict:
            raise NadoPreflightError(f"{label} balance schema mismatch")
        expected = {"amount", "v_quote_balance"} if perp else {"amount"}
        _exact_keys(balance, expected, f"{label} balance")
        amount = _decimal(balance["amount"], f"{label} amount")
        v_quote = _decimal(balance["v_quote_balance"], "v_quote") if perp else 0
        parsed[product_id] = (amount, v_quote)
    if set(parsed) != expected_ids:
        raise NadoPreflightError(f"{label} coverage is incomplete")
    return parsed


def _account(
    data: Mapping[str, object], sender: str,
    catalog: tuple[tuple[int, str, str], ...],
) -> dict[str, object]:
    _exact_keys(
        data,
        {"subaccount", "exists", "health", "spot_balances", "perp_balances", "perp_count"},
        "subaccount info",
    )
    subaccount = data["subaccount"]
    if type(subaccount) is not str or subaccount.lower() != sender.lower():
        raise NadoPreflightError("subaccount identity mismatch")
    if type(data["exists"]) is not bool or not data["exists"]:
        raise NadoPreflightError("subaccount does not exist")
    health = _decimal(data["health"], "health")
    if health <= 0:
        raise NadoPreflightError("health is not positive")
    spot_ids = {item[0] for item in catalog if item[2] == "spot"}
    perp_ids = {item[0] for item in catalog if item[2] == "perp"}
    spots = _balance_entries(data["spot_balances"], "spot balance", spot_ids, perp=False)
    perps = _balance_entries(data["perp_balances"], "cross-perp", perp_ids, perp=True)
    if spots.get(0, (-1, 0))[0] < MIN_COLLATERAL_X18:
        raise NadoPreflightError("collateral floor is not met")
    if any(amount < 0 for amount, _ in spots.values()):
        raise NadoPreflightError("negative spot balance")
    if any(amount for product_id, (amount, _) in spots.items() if product_id != 0):
        raise NadoPreflightError("unexplained spot balance")
    if any(amount != 0 for amount, _ in perps.values()):
        raise NadoPreflightError("cross-perp is not exactly flat")
    if any(v_quote != 0 for _, v_quote in perps.values()):
        raise NadoPreflightError("unexplained v_quote balance")
    perp_count = _decimal(data["perp_count"], "perp count", signed=False)
    if perp_count != len(perps):
        raise NadoPreflightError("perp count contradicts complete vector")
    return {"health": health, "spots": spots, "perps": perps}


def _orders(data: Mapping[str, object]) -> None:
    _exact_keys(data, {"orders"}, "regular orders")
    if type(data["orders"]) is not list or data["orders"]:
        raise NadoPreflightError("regular order exists or response is invalid")


def _isolated(data: Mapping[str, object]) -> None:
    _exact_keys(data, {"isolated_positions"}, "isolated positions")
    if type(data["isolated_positions"]) is not list or data["isolated_positions"]:
        raise NadoPreflightError("isolated position exists or response is invalid")


def _temporal(observed: list[int]) -> None:
    if not observed or any(later <= earlier for earlier, later in zip(observed, observed[1:])):
        raise NadoPreflightError("public evidence temporal order mismatch")


def _round_a(transport: SealedPublicTransport, config: PreflightConfig) -> _RoundEvidence:
    observed: list[int] = []
    data, at = _query(transport, {"type": "contracts"}, config.now_ms)
    observed.append(at); _contracts(data)
    data, at = _query(transport, {"type": "status"}, config.now_ms)
    observed.append(at); _status(data)
    data, at = _query(transport, {"type": "all_products"}, config.now_ms)
    observed.append(at); products = _catalog(data)
    data, at = _query(
        transport, {"type": "linked_signer", "sender": config.sender}, config.now_ms,
    )
    observed.append(at); _linked(data)
    data, at = _query(
        transport, {"type": "subaccount_info", "subaccount": config.sender}, config.now_ms,
    )
    observed.append(at); account = _account(data, config.sender, products)
    for product_id, _, _ in products:
        data, at = _query(
            transport,
            {"type": "open_orders", "sender": config.sender, "product_id": product_id},
            config.now_ms,
        )
        observed.append(at); _orders(data)
    data, at = _query(
        transport, {"type": "isolated_positions", "subaccount": config.sender},
        config.now_ms,
    )
    observed.append(at); _isolated(data)
    _temporal(observed)
    return _RoundEvidence(
        _digest({"catalog": products, "account": account}), observed[0], observed[-1]
    )


def _round_b(transport: SealedPublicTransport, config: PreflightConfig) -> _RoundEvidence:
    observed: list[int] = []
    data, at = _query(transport, {"type": "all_products"}, config.now_ms)
    observed.append(at); products = _catalog(data)
    for product_id, _, _ in products:
        data, at = _query(
            transport,
            {"type": "open_orders", "sender": config.sender, "product_id": product_id},
            config.now_ms,
        )
        observed.append(at); _orders(data)
    data, at = _query(
        transport, {"type": "subaccount_info", "subaccount": config.sender}, config.now_ms,
    )
    observed.append(at); account = _account(data, config.sender, products)
    data, at = _query(
        transport, {"type": "isolated_positions", "subaccount": config.sender},
        config.now_ms,
    )
    observed.append(at); _isolated(data)
    for product_id, _, _ in products:
        data, at = _query(
            transport,
            {"type": "open_orders", "sender": config.sender, "product_id": product_id},
            config.now_ms,
        )
        observed.append(at); _orders(data)
    data, at = _query(transport, {"type": "contracts"}, config.now_ms)
    observed.append(at); _contracts(data)
    data, at = _query(transport, {"type": "status"}, config.now_ms)
    observed.append(at); _status(data)
    data, at = _query(transport, {"type": "all_products"}, config.now_ms)
    observed.append(at)
    if _catalog(data) != products:
        raise NadoPreflightError("round-B catalog changed")
    data, at = _query(
        transport, {"type": "linked_signer", "sender": config.sender}, config.now_ms,
    )
    observed.append(at); _linked(data)
    _temporal(observed)
    return _RoundEvidence(
        _digest({"catalog": products, "account": account}), observed[0], observed[-1]
    )


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


def _callback_value(
    callback: Callable[..., object], *args: object, label: str,
) -> object:
    ok, value = _invoke(callback, *args)
    if not ok:
        raise NadoPreflightError(f"{label} callback failed")
    return value


def _signature(value: object) -> str:
    if type(value) is not str or not value.startswith("0x"):
        raise NadoPreflightError("signature is invalid")
    try:
        raw = bytes.fromhex(value[2:])
    except ValueError:
        raise NadoPreflightError("signature is invalid") from None
    if len(raw) != 65:
        raise NadoPreflightError("signature is invalid")
    return value


def _trigger_zero(
    observation: ObservedResponse, config: PreflightConfig,
) -> tuple[str, int]:
    data, observed = _wire_data(observation, config.now_ms)
    _exact_keys(data, {"orders"}, "trigger orders")
    if type(data["orders"]) is not list or data["orders"]:
        raise NadoPreflightError("trigger history is not zero")
    return _digest({"identity": _identity_tag(config.sender), "orders_empty": True}), observed


def run_private_read_preflight(
    *,
    config: PreflightConfig,
    public_transport: SealedPublicTransport,
    credential_loader: Callable[[], object],
    derive_owner: Callable[[object], str],
    server_time: Callable[[], int],
    signer: Callable[[object, dict[str, object]], str],
    signed_transport: SealedSignedTransport,
    store: OneShotStore,
) -> PreflightResult:
    """Run the injected one-shot private-read barrier; no callback is ambient."""
    if type(public_transport) is not SealedPublicTransport:
        raise NadoPreflightError("public transport boundary mismatch")
    if type(signed_transport) is not SealedSignedTransport:
        raise NadoPreflightError("signed transport boundary mismatch")
    identity_hash = _identity_hash(config.sender)
    identity_tag = _identity_tag(config.sender)
    evidence = store.evidence(config.invocation_id)
    if evidence is None:
        round_a = _round_a(public_transport, config)
        store.claim(
            config.invocation_id, identity_hash, round_a.fingerprint,
            round_a.last_observed_ms,
        )
        credential = _callback_value(credential_loader, label="credential loader")
        derived = _callback_value(derive_owner, credential, label="owner derivation")
        if (
            type(derived) is not str
            or _address_bytes(derived) != _address_bytes(config.owner)
        ):
            raise NadoPreflightError("credential owner identity mismatch")
        server_ms = _callback_value(server_time, label="server time")
        if (
            type(server_ms) is not int
            or server_ms <= round_a.last_observed_ms
            or server_ms > config.now_ms
            or config.now_ms - server_ms > MAX_FRESHNESS_MS
        ):
            raise NadoPreflightError("server time is invalid or out of order")
        recv_time = server_ms + MAX_FRESHNESS_MS
        typed_data = list_trigger_orders_typed_data(config.sender, recv_time)
        signature = _signature(
            _callback_value(signer, credential, typed_data, label="signer")
        )
        request = {
            "type": "list_trigger_orders",
            "tx": {"sender": config.sender, "recvTime": recv_time},
            "signature": signature,
            "limit": 1,
        }
        observation = signed_transport.send(request)
        trigger_hash, trigger_observed_ms = _trigger_zero(observation, config)
        if trigger_observed_ms < server_ms or trigger_observed_ms <= round_a.last_observed_ms:
            raise NadoPreflightError("signed observation temporal order mismatch")
        store.observe(config.invocation_id, trigger_hash, trigger_observed_ms)
        evidence = store.evidence(config.invocation_id)
    if evidence is None:
        raise NadoPreflightError("durable one-shot evidence is unavailable")
    (
        state, stored_identity, round_a_hash, round_a_observed_ms,
        trigger_hash, trigger_observed_ms,
    ) = evidence
    if stored_identity != identity_hash:
        raise NadoPreflightError("durable one-shot identity mismatch")
    if state == CLAIMED:
        raise NadoPreflightError("signed observation is claimed and cannot be retried")
    if state == FINALIZED:
        raise NadoPreflightError("preflight invocation is already finalized")
    if state != OBSERVED or trigger_hash is None or trigger_observed_ms is None:
        raise NadoPreflightError("durable one-shot state is invalid")
    expected_trigger_hash = _digest({"identity": identity_tag, "orders_empty": True})
    if trigger_hash != expected_trigger_hash:
        raise NadoPreflightError("durable trigger evidence is invalid")
    if (
        trigger_observed_ms <= round_a_observed_ms
        or round_a_observed_ms > config.now_ms
        or trigger_observed_ms > config.now_ms
        or config.now_ms - round_a_observed_ms > MAX_FRESHNESS_MS
    ):
        raise NadoPreflightError("durable temporal evidence is invalid or stale")
    round_b = _round_b(public_transport, config)
    if round_b.first_observed_ms <= trigger_observed_ms:
        raise NadoPreflightError("public evidence temporal barrier mismatch")
    if round_b.last_observed_ms - round_a_observed_ms > MAX_FRESHNESS_MS:
        raise NadoPreflightError("public evidence temporal barrier is stale")
    if round_b.fingerprint != round_a_hash:
        raise NadoPreflightError("public rounds disagree")
    store.finalize(config.invocation_id, round_b.fingerprint)
    return PreflightResult(FINALIZED, identity_tag, True, True, True)
