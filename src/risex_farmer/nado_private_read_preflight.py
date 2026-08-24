"""Disarmed Nado testnet private-read preflight.

The deterministic core accepts only sealed fixture transports.  The additive
operational entry point owns its exact HTTP transports and system clock, but is
not imported by Farmer startup.  Credentials and signing remain explicit
injected capabilities and CI supplies synthetic callbacks only.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import ssl
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Generator, Mapping

import aiohttp


NEW = "NEW"
CLAIMED = "CLAIMED"
OBSERVED = "OBSERVED"
FINALIZED = "FINALIZED"
UNKNOWN = "UNKNOWN"
ZERO_ADDRESS = "0x" + "00" * 20
MIN_COLLATERAL_X18 = 5 * 10**18
MAX_FRESHNESS_MS = 30_000
HTTP_TIMEOUT_SECONDS = 5.0
MAX_RESPONSE_BYTES = 65_536
LEDGER_SCHEMA_VERSION = 2
COUNTER_PHASES = (
    "public_a", "loader", "derive", "server_time", "sign", "recover",
    "trigger_dispatch", "trigger_observation", "public_b",
)

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
    gateway_edge_query = gateway + "/edge/query"
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

    def __post_init__(self) -> None:
        if type(self.sender) is not str:
            raise NadoPreflightError("subaccount identity is invalid")
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
        if (
            type(raw.url) is not str
            or type(raw.final_url) is not str
            or raw.url != self.expected_url
            or raw.final_url != self.expected_url
        ):
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


@dataclass(frozen=True)
class SealedTimeTransport(_SealedTransport):
    expected_url = FixedPreflightIdentity.gateway_edge_query
    failure_label = "time transport callback failed"


def _system_clock_ms() -> int:
    return time.time_ns() // 1_000_000


def _durable_now_ms() -> int:
    return time.time_ns() // 1_000_000


def _strict_json(raw: bytes) -> object:
    try:
        text = raw.decode("utf-8", errors="strict")

        def reject_constant(value: str) -> object:
            raise ValueError(value)

        def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
            result: dict[str, object] = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError("duplicate key")
                result[key] = value
            return result

        return json.loads(
            text, parse_constant=reject_constant, object_pairs_hook=unique_object,
        )
    except (UnicodeError, json.JSONDecodeError, ValueError):
        raise NadoPreflightError("transport response is not strict JSON") from None


def _new_aiohttp_session() -> aiohttp.ClientSession:
    return aiohttp.ClientSession(
        trust_env=False,
        timeout=aiohttp.ClientTimeout(total=HTTP_TIMEOUT_SECONDS),
    )


class _OperationalFixedHostTransport:
    expected_url = ""
    failure_label = "operational transport failed"

    async def send_async(self, request: Mapping[str, object]) -> ObservedResponse:
        if not isinstance(request, Mapping):
            raise NadoPreflightError("request schema mismatch")
        failed = False
        try:
            request_body = dict(request)
            encoded_request = _canonical(request_body)
            tls = ssl.create_default_context()
            async with asyncio.timeout(HTTP_TIMEOUT_SECONDS):
                async with _new_aiohttp_session() as session:
                    async with session.post(
                        self.expected_url,
                        data=encoded_request,
                        headers={"Content-Type": "application/json"},
                        allow_redirects=False,
                        proxy=None,
                        ssl=tls,
                    ) as response:
                        if type(response.status) is not int or response.status != 200:
                            raise NadoPreflightError("transport HTTP status rejected")
                        if str(response.url) != self.expected_url:
                            raise NadoPreflightError("transport host or redirect mismatch")
                        raw = bytearray()
                        while True:
                            remaining = MAX_RESPONSE_BYTES + 1 - len(raw)
                            chunk = await response.content.read(min(16_384, remaining))
                            raw.extend(chunk)
                            if len(raw) > MAX_RESPONSE_BYTES:
                                raise NadoPreflightError("transport response size exceeded")
                            if not chunk:
                                break
                        payload = _strict_json(bytes(raw))
                        observed_at_ms = _system_clock_ms()
        except asyncio.CancelledError:
            raise
        except BaseException:
            failed = True
        if failed:
            raise NadoPreflightError(self.failure_label)
        if type(observed_at_ms) is not int or observed_at_ms <= 0:
            raise NadoPreflightError("transport observation time is invalid")
        return ObservedResponse(
            url=self.expected_url,
            final_url=self.expected_url,
            http_status=200,
            observed_at_ms=observed_at_ms,
            payload=payload,
        )

class _OperationalGatewayTransport(_OperationalFixedHostTransport):
    expected_url = FixedPreflightIdentity.gateway_query
    failure_label = "operational gateway transport failed"


class _OperationalTimeTransport(_OperationalFixedHostTransport):
    expected_url = FixedPreflightIdentity.gateway_edge_query
    failure_label = "operational time transport failed"


class _OperationalTriggerTransport(_OperationalFixedHostTransport):
    expected_url = FixedPreflightIdentity.trigger_query
    failure_label = "operational trigger transport failed"


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
                round_a_hash TEXT,
                round_a_observed_ms INTEGER,
                trigger_hash TEXT,
                trigger_observed_ms INTEGER,
                schema_version INTEGER NOT NULL DEFAULT 2,
                path_hash TEXT NOT NULL DEFAULT '',
                product_count INTEGER,
                round_b_hash TEXT,
                reason TEXT NOT NULL DEFAULT '',
                counters_json TEXT NOT NULL DEFAULT '{}',
                terminal_ms INTEGER
            )
            """
        )
        self._connection.commit()

    @staticmethod
    def _empty_counters() -> dict[str, int]:
        return {
            f"{phase}_{suffix}": 0
            for phase in COUNTER_PHASES for suffix in ("attempts", "completions")
        }

    def begin(self, invocation_id: str, identity_hash: str, path_hash: str) -> None:
        counters = json.dumps(self._empty_counters(), sort_keys=True, separators=(",", ":"))
        try:
            with self._connection:
                self._connection.execute(
                    """INSERT INTO nado_preflight_one_shot
                       (invocation_id,state,identity_tag,round_a_hash,
                        round_a_observed_ms,trigger_hash,trigger_observed_ms,
                        schema_version,path_hash,product_count,round_b_hash,
                        reason,counters_json,terminal_ms)
                       VALUES (?, ?, ?, NULL, NULL, NULL, NULL, ?, ?, NULL, NULL, '', ?, NULL)""",
                    (invocation_id, NEW, identity_hash, LEDGER_SCHEMA_VERSION,
                     path_hash, counters),
                )
        except sqlite3.IntegrityError:
            raise NadoPreflightError("one-shot invocation already exists") from None

    def count(self, invocation_id: str, phase: str, completion: bool = False) -> None:
        if phase not in COUNTER_PHASES:
            raise NadoPreflightError("counter phase is invalid")
        key = f"{phase}_{'completions' if completion else 'attempts'}"
        with self._connection:
            row = self._connection.execute(
                "SELECT counters_json FROM nado_preflight_one_shot WHERE invocation_id = ?",
                (invocation_id,),
            ).fetchone()
            if row is None:
                raise NadoPreflightError("durable counter identity is unavailable")
            try:
                counters = json.loads(str(row[0]))
            except (TypeError, ValueError, json.JSONDecodeError):
                raise NadoPreflightError("durable counters are corrupt") from None
            if counters != {name: counters.get(name) for name in self._empty_counters()}:
                raise NadoPreflightError("durable counter schema mismatch")
            if any(type(value) is not int or value < 0 for value in counters.values()):
                raise NadoPreflightError("durable counter is invalid")
            if completion and counters[key] >= counters[f"{phase}_attempts"]:
                raise NadoPreflightError("counter completion has no attempt")
            counters[key] += 1
            self._connection.execute(
                "UPDATE nado_preflight_one_shot SET counters_json = ? WHERE invocation_id = ?",
                (json.dumps(counters, sort_keys=True, separators=(",", ":")), invocation_id),
            )

    def counters(self, invocation_id: str) -> dict[str, int]:
        row = self._connection.execute(
            "SELECT schema_version,counters_json FROM nado_preflight_one_shot WHERE invocation_id = ?",
            (invocation_id,),
        ).fetchone()
        if row is None or row[0] != LEDGER_SCHEMA_VERSION:
            raise NadoPreflightError("durable counter schema mismatch")
        try:
            counters = json.loads(str(row[1]))
        except (TypeError, ValueError, json.JSONDecodeError):
            raise NadoPreflightError("durable counters are corrupt") from None
        expected = self._empty_counters()
        if set(counters) != set(expected) or any(
            type(value) is not int or value < 0 for value in counters.values()
        ):
            raise NadoPreflightError("durable counter schema mismatch")
        if any(
            counters[f"{phase}_completions"] > counters[f"{phase}_attempts"]
            for phase in COUNTER_PHASES
        ):
            raise NadoPreflightError("durable counter ordering is invalid")
        return dict(counters)

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
                    """INSERT INTO nado_preflight_one_shot
                       (invocation_id,state,identity_tag,round_a_hash,
                        round_a_observed_ms,trigger_hash,trigger_observed_ms)
                       VALUES (?, ?, ?, ?, ?, NULL, NULL)""",
                    (invocation_id, CLAIMED, identity_hash, round_a_hash, round_a_observed_ms),
                )
        except sqlite3.IntegrityError:
            raise NadoPreflightError("signed observation is already claimed") from None

    def claim_started(
        self, invocation_id: str, round_a_hash: str, round_a_observed_ms: int,
        product_count: int,
    ) -> None:
        with self._connection:
            cursor = self._connection.execute(
                """UPDATE nado_preflight_one_shot
                   SET state=?,round_a_hash=?,round_a_observed_ms=?,product_count=?
                   WHERE invocation_id=? AND state=?""",
                (CLAIMED, round_a_hash, round_a_observed_ms, product_count,
                 invocation_id, NEW),
            )
        if cursor.rowcount != 1:
            raise NadoPreflightError("one-shot claim cannot be recorded")

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
                """UPDATE nado_preflight_one_shot
                   SET state = ?, round_b_hash = ?, terminal_ms = ?
                   WHERE invocation_id = ? AND state = ? AND round_a_hash = ?
                         AND trigger_hash IS NOT NULL""",
                (FINALIZED, round_b_hash, _durable_now_ms(), invocation_id,
                 OBSERVED, round_b_hash),
            )
        if cursor.rowcount != 1:
            raise NadoPreflightError("public rounds disagree or observation is incomplete")

    def terminalize_unknown(self, invocation_id: str, reason: str) -> None:
        bounded = reason if reason in {
            "INTERRUPTED", "CANCELLED", "VALIDATION_FAILED", "AMBIGUOUS_DISPATCH",
        } else "VALIDATION_FAILED"
        with self._connection:
            cursor = self._connection.execute(
                """UPDATE nado_preflight_one_shot SET state=?,reason=?,terminal_ms=?
                   WHERE invocation_id=? AND state IN (?,?,?)""",
                (UNKNOWN, bounded, _durable_now_ms(), invocation_id,
                 NEW, CLAIMED, OBSERVED),
            )
        if cursor.rowcount != 1:
            raise NadoPreflightError("terminal report cannot be persisted")

    def terminal_report(self, invocation_id: str) -> dict[str, object] | None:
        row = self._connection.execute(
            """SELECT state,identity_tag,path_hash,product_count,reason,
                      round_a_hash,trigger_hash,round_b_hash,terminal_ms
               FROM nado_preflight_one_shot WHERE invocation_id=?""",
            (invocation_id,),
        ).fetchone()
        if row is None:
            return None
        if str(row[0]) not in {NEW, CLAIMED, OBSERVED, FINALIZED, UNKNOWN}:
            raise NadoPreflightError("durable state is invalid")
        if str(row[0]) in {FINALIZED, UNKNOWN} and (
            type(row[8]) is not int or row[8] <= 0
        ):
            raise NadoPreflightError("durable terminal time is invalid")
        if row[3] is not None and (type(row[3]) is not int or row[3] <= 0):
            raise NadoPreflightError("durable product count is invalid")
        counters = self.counters(invocation_id)
        return {
            "schema_version": LEDGER_SCHEMA_VERSION,
            "invocation_id": invocation_id,
            "status": str(row[0]),
            "identity_tag": str(row[1])[:16],
            "path_hash": str(row[2]),
            "product_count": row[3],
            "reason": str(row[4]),
            "round_a_tag": None if row[5] is None else str(row[5])[:16],
            "trigger_tag": None if row[6] is None else str(row[6])[:16],
            "round_b_tag": None if row[7] is None else str(row[7])[:16],
            "terminal_ms": row[8],
            "counters": counters,
        }

    def close(self) -> None:
        self._connection.close()


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
    product_count: int


def _exact_keys(value: Mapping[str, object], expected: set[str], label: str) -> None:
    if set(value) != expected:
        raise NadoPreflightError(f"{label} schema mismatch")


def _clock(clock_ms: Callable[[], int]) -> int:
    ok, value = _invoke(clock_ms)
    if not ok or type(value) is not int or value <= 0:
        raise NadoPreflightError("owned clock failed")
    return value


def _fresh_observation(
    observation: ObservedResponse, clock_ms: Callable[[], int],
) -> int:
    observed = observation.observed_at_ms
    now_ms = _clock(clock_ms)
    if observed > now_ms or now_ms - observed > MAX_FRESHNESS_MS:
        raise NadoPreflightError("transport observation is not fresh")
    return observed


def _wire_data(
    observation: ObservedResponse, clock_ms: Callable[[], int],
) -> tuple[object, int]:
    observed = _fresh_observation(observation, clock_ms)
    payload = observation.payload
    if type(payload) is not dict:
        raise NadoPreflightError("wire envelope schema mismatch")
    _exact_keys(payload, {"status", "data"}, "wire envelope")
    if type(payload["status"]) is not str or payload["status"] != "success":
        raise NadoPreflightError("wire status is not success")
    return payload["data"], observed


def _query(
    transport: object, request: Mapping[str, object], clock_ms: Callable[[], int],
) -> tuple[object, int]:
    return _wire_data(transport.send(request), clock_ms)


def _server_time(
    transport: object, clock_ms: Callable[[], int],
) -> int:
    return _server_time_observation(transport.send({"type": "time"}), clock_ms)


def _server_time_observation(
    observation: ObservedResponse, clock_ms: Callable[[], int],
) -> int:
    _fresh_observation(observation, clock_ms)
    payload = _object(observation.payload, "time envelope")
    _exact_keys(payload, {"status", "method", "server_time"}, "time envelope")
    if payload["status"] != "success" or type(payload["status"]) is not str:
        raise NadoPreflightError("time status is not success")
    if payload["method"] != "time" or type(payload["method"]) is not str:
        raise NadoPreflightError("time method mismatch")
    server_ms = _decimal(payload["server_time"], "server time", signed=False)
    now_ms = _clock(clock_ms)
    if server_ms > now_ms or now_ms - server_ms > MAX_FRESHNESS_MS:
        raise NadoPreflightError("server time is not fresh")
    return server_ms


def _strict_uint(value: object, label: str) -> int:
    if type(value) is not int or not 0 <= value < 2**32:
        raise NadoPreflightError(f"{label} is invalid")
    return value


def _object(value: object, label: str) -> dict[str, object]:
    if type(value) is not dict:
        raise NadoPreflightError(f"{label} schema mismatch")
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


def _contracts(raw_data: object) -> None:
    data = _object(raw_data, "contracts")
    _exact_keys(data, {"chain_id", "endpoint_addr"}, "contracts")
    chain = _decimal(data["chain_id"], "chain id", signed=False)
    endpoint = data["endpoint_addr"]
    if chain != FixedPreflightIdentity.chain_id or (
        type(endpoint) is not str
        or endpoint.lower() != FixedPreflightIdentity.endpoint.lower()
    ):
        raise NadoPreflightError("contracts identity mismatch")


def _status(data: object) -> None:
    if type(data) is not str or data != "active":
        raise NadoPreflightError("engine is not active")


_RISK_KEYS = {
    "long_weight_initial_x18", "short_weight_initial_x18",
    "long_weight_maintenance_x18", "short_weight_maintenance_x18",
    "large_position_penalty_x18",
}
_BOOK_KEYS = {"size_increment", "price_increment_x18", "min_size", "collected_fees"}
_SPOT_CONFIG_KEYS = {
    "token", "interest_inflection_util_x18", "interest_floor_x18",
    "interest_small_cap_x18", "interest_large_cap_x18", "min_deposit_rate_x18",
}
_SPOT_STATE_KEYS = {
    "cumulative_deposits_multiplier_x18", "cumulative_borrows_multiplier_x18",
    "total_deposits_normalized", "total_borrows_normalized",
}
_PERP_STATE_KEYS = {
    "cumulative_funding_long_x18", "cumulative_funding_short_x18",
    "available_settle", "open_interest",
}


def _decimal_object(raw: object, keys: set[str], label: str) -> dict[str, object]:
    value = _object(raw, label)
    _exact_keys(value, keys, label)
    for key in keys:
        _decimal(value[key], f"{label} {key}")
    return value


def _product(raw: object, kind: str) -> tuple[int, str, str]:
    product = _object(raw, f"{kind} product")
    common = {"product_id", "oracle_price_x18", "risk", "state", "book_info"}
    expected = common | ({"config"} if kind == "spot" else {"index_price_x18"})
    _exact_keys(product, expected, f"{kind} product")
    product_id = _strict_uint(product["product_id"], "product id")
    if _decimal(product["oracle_price_x18"], "oracle price", signed=False) <= 0:
        raise NadoPreflightError("oracle price is not positive")
    _decimal_object(product["risk"], _RISK_KEYS, "risk")
    _decimal_object(product["book_info"], _BOOK_KEYS, "book info")
    if kind == "spot":
        config = _object(product["config"], "spot config")
        _exact_keys(config, _SPOT_CONFIG_KEYS, "spot config")
        _address_bytes(config["token"])
        for key in _SPOT_CONFIG_KEYS - {"token"}:
            _decimal(config[key], f"spot config {key}")
        _decimal_object(product["state"], _SPOT_STATE_KEYS, "spot state")
    else:
        if _decimal(product["index_price_x18"], "index price", signed=False) <= 0:
            raise NadoPreflightError("index price is not positive")
        _decimal_object(product["state"], _PERP_STATE_KEYS, "perp state")
    return product_id, kind, _digest(product)


def _catalog(raw_data: object) -> tuple[tuple[int, str, str], ...]:
    data = _object(raw_data, "all products")
    _exact_keys(data, {"spot_products", "perp_products"}, "all products")
    products: dict[int, tuple[int, str, str]] = {}
    for kind, field in (("spot", "spot_products"), ("perp", "perp_products")):
        raw_products = data[field]
        if type(raw_products) is not list:
            raise NadoPreflightError("product catalog schema mismatch")
        for raw in raw_products:
            parsed = _product(raw, kind)
            product_id = parsed[0]
            if product_id in products:
                raise NadoPreflightError("product identity is invalid or duplicate")
            products[product_id] = parsed
    if not products or products.get(0, (None, None, None))[1] != "spot":
        raise NadoPreflightError("official collateral identity mismatch")
    if set(products) != set(range(len(products))):
        raise NadoPreflightError("product catalog coverage is not contiguous")
    return tuple(products[key] for key in sorted(products))


def _linked(raw_data: object) -> None:
    data = _object(raw_data, "linked signer")
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
        expected = (
            {"amount", "v_quote_balance", "last_cumulative_funding_x18"}
            if perp else {"amount"}
        )
        _exact_keys(balance, expected, f"{label} balance")
        amount = _decimal(balance["amount"], f"{label} amount")
        v_quote = _decimal(balance["v_quote_balance"], "v_quote") if perp else 0
        if perp:
            _decimal(
                balance["last_cumulative_funding_x18"],
                "last cumulative funding",
            )
        parsed[product_id] = (amount, v_quote)
    if set(parsed) != expected_ids:
        raise NadoPreflightError(f"{label} coverage is incomplete")
    return parsed


def _account(
    raw_data: object, sender: str,
    catalog: tuple[tuple[int, str, str], ...],
) -> dict[str, object]:
    data = _object(raw_data, "subaccount info")
    _exact_keys(
        data,
        {
            "exists", "subaccount", "spot_count", "perp_count", "healths",
            "health_contributions", "spot_balances", "perp_balances",
            "spot_products", "perp_products",
        },
        "subaccount info",
    )
    subaccount = data["subaccount"]
    if type(subaccount) is not str or subaccount.lower() != sender.lower():
        raise NadoPreflightError("subaccount identity mismatch")
    if type(data["exists"]) is not bool or not data["exists"]:
        raise NadoPreflightError("subaccount does not exist")
    if _catalog({
        "spot_products": data["spot_products"],
        "perp_products": data["perp_products"],
    }) != catalog:
        raise NadoPreflightError("embedded product catalog disagrees")
    healths = data["healths"]
    if type(healths) is not list or len(healths) != 3:
        raise NadoPreflightError("health breakdown triple is required")
    normalized_healths: list[tuple[int, int, int]] = []
    for raw_health in healths:
        health = _object(raw_health, "health breakdown")
        _exact_keys(health, {"health", "assets", "liabilities"}, "health breakdown")
        parsed = (
            _decimal(health["health"], "health"),
            _decimal(health["assets"], "health assets"),
            _decimal(health["liabilities"], "health liabilities"),
        )
        if parsed[0] <= 0 or parsed[1] < 0 or parsed[2] < 0:
            raise NadoPreflightError("health breakdown is not positive")
        normalized_healths.append(parsed)
    contributions = data["health_contributions"]
    if type(contributions) is not list or len(contributions) != len(catalog):
        raise NadoPreflightError("health contribution coverage is incomplete")
    normalized_contributions: list[tuple[int, int, int]] = []
    for raw_contribution in contributions:
        if type(raw_contribution) is not list or len(raw_contribution) != 3:
            raise NadoPreflightError("health contribution triple is required")
        normalized_contributions.append(tuple(
            _decimal(value, "health contribution") for value in raw_contribution
        ))
    spot_ids = {item[0] for item in catalog if item[1] == "spot"}
    perp_ids = {item[0] for item in catalog if item[1] == "perp"}
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
    spot_count = _strict_uint(data["spot_count"], "spot count")
    if spot_count != len(spots):
        raise NadoPreflightError("balance counts contradict complete vectors")
    return {
        "healths": normalized_healths,
        "health_contributions": normalized_contributions,
        "spots": spots,
        "perps": perps,
    }


def _orders(raw_data: object, sender: str, product_id: int) -> None:
    data = _object(raw_data, "subaccount orders")
    _exact_keys(data, {"sender", "product_id", "orders"}, "subaccount orders")
    echoed_sender = data["sender"]
    echoed_product = _strict_uint(data["product_id"], "order product id")
    if (
        type(echoed_sender) is not str
        or echoed_sender.lower() != sender.lower()
        or echoed_product != product_id
    ):
        raise NadoPreflightError("subaccount orders identity mismatch")
    if type(data["orders"]) is not list or data["orders"]:
        raise NadoPreflightError("regular order exists or response is invalid")


def _isolated(raw_data: object) -> None:
    data = _object(raw_data, "isolated positions")
    _exact_keys(data, {"isolated_positions"}, "isolated positions")
    if type(data["isolated_positions"]) is not list or data["isolated_positions"]:
        raise NadoPreflightError("isolated position exists or response is invalid")


def _temporal(observed: list[int]) -> None:
    if not observed or any(later <= earlier for earlier, later in zip(observed, observed[1:])):
        raise NadoPreflightError("public evidence temporal order mismatch")


def _round_a_contract(
    config: PreflightConfig,
) -> Generator[dict[str, object], tuple[object, int], _RoundEvidence]:
    observed: list[int] = []
    data, at = yield {"type": "contracts"}
    observed.append(at); _contracts(data)
    data, at = yield {"type": "status"}
    observed.append(at); _status(data)
    data, at = yield {"type": "all_products"}
    observed.append(at); products = _catalog(data)
    data, at = yield {"type": "linked_signer", "subaccount": config.sender}
    observed.append(at); _linked(data)
    data, at = yield {"type": "subaccount_info", "subaccount": config.sender}
    observed.append(at); account = _account(data, config.sender, products)
    for product_id, _, _ in products:
        data, at = yield {
            "type": "subaccount_orders", "sender": config.sender,
            "product_id": product_id,
        }
        observed.append(at); _orders(data, config.sender, product_id)
    data, at = yield {"type": "isolated_positions", "subaccount": config.sender}
    observed.append(at); _isolated(data)
    _temporal(observed)
    return _RoundEvidence(
        _digest({"catalog": products, "account": account}), observed[0], observed[-1],
        len(products),
    )


def _round_b_contract(
    config: PreflightConfig,
) -> Generator[dict[str, object], tuple[object, int], _RoundEvidence]:
    observed: list[int] = []
    data, at = yield {"type": "all_products"}
    observed.append(at); products = _catalog(data)
    for product_id, _, _ in products:
        data, at = yield {
            "type": "subaccount_orders", "sender": config.sender,
            "product_id": product_id,
        }
        observed.append(at); _orders(data, config.sender, product_id)
    data, at = yield {"type": "subaccount_info", "subaccount": config.sender}
    observed.append(at); account = _account(data, config.sender, products)
    data, at = yield {"type": "isolated_positions", "subaccount": config.sender}
    observed.append(at); _isolated(data)
    for product_id, _, _ in products:
        data, at = yield {
            "type": "subaccount_orders", "sender": config.sender,
            "product_id": product_id,
        }
        observed.append(at); _orders(data, config.sender, product_id)
    data, at = yield {"type": "contracts"}
    observed.append(at); _contracts(data)
    data, at = yield {"type": "status"}
    observed.append(at); _status(data)
    data, at = yield {"type": "all_products"}
    observed.append(at)
    if _catalog(data) != products:
        raise NadoPreflightError("round-B catalog changed")
    data, at = yield {"type": "linked_signer", "subaccount": config.sender}
    observed.append(at); _linked(data)
    _temporal(observed)
    return _RoundEvidence(
        _digest({"catalog": products, "account": account}), observed[0], observed[-1],
        len(products),
    )


def _round(
    contract: Generator[dict[str, object], tuple[object, int], _RoundEvidence],
    transport: SealedPublicTransport,
    clock_ms: Callable[[], int],
) -> _RoundEvidence:
    try:
        request = next(contract)
        while True:
            request = contract.send(_query(transport, request, clock_ms))
    except StopIteration as completed:
        return completed.value


def _round_a(
    transport: SealedPublicTransport, config: PreflightConfig,
    clock_ms: Callable[[], int],
) -> _RoundEvidence:
    return _round(_round_a_contract(config), transport, clock_ms)


def _round_b(
    transport: SealedPublicTransport, config: PreflightConfig,
    clock_ms: Callable[[], int],
) -> _RoundEvidence:
    return _round(_round_b_contract(config), transport, clock_ms)


async def _operational_round(
    contract: Generator[dict[str, object], tuple[object, int], _RoundEvidence],
    transport: _OperationalGatewayTransport,
    before: Callable[[], None] | None = None,
    after: Callable[[], None] | None = None,
) -> _RoundEvidence:
    try:
        request = next(contract)
        while True:
            if before is not None:
                before()
                await asyncio.sleep(0)
            observation = await transport.send_async(request)
            await asyncio.sleep(0)
            decoded = _wire_data(observation, _system_clock_ms)
            try:
                request = contract.send(decoded)
            except StopIteration:
                if after is not None:
                    after()
                    await asyncio.sleep(0)
                raise
            else:
                if after is not None:
                    after()
                    await asyncio.sleep(0)
    except StopIteration as completed:
        return completed.value


async def _operational_round_a(
    transport: _OperationalGatewayTransport, config: PreflightConfig,
    before: Callable[[], None] | None = None,
    after: Callable[[], None] | None = None,
) -> _RoundEvidence:
    return await _operational_round(_round_a_contract(config), transport, before, after)


async def _operational_round_b(
    transport: _OperationalGatewayTransport, config: PreflightConfig,
    before: Callable[[], None] | None = None,
    after: Callable[[], None] | None = None,
) -> _RoundEvidence:
    return await _operational_round(_round_b_contract(config), transport, before, after)


def list_trigger_orders_typed_data(sender: str, recv_time: str) -> dict[str, object]:
    _bytes32(sender)
    parsed_recv_time = _decimal(recv_time, "trigger receive time", signed=False)
    if parsed_recv_time >= 2**64:
        raise NadoPreflightError("trigger receive time is invalid")
    return {
        "types": {
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


def _counted_callback(
    callback: Callable[..., object], *args: object, label: str,
) -> object:
    try:
        return callback(*args)
    except asyncio.CancelledError:
        raise
    except BaseException:
        raise NadoPreflightError(f"{label} callback failed") from None


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
    clock_ms: Callable[[], int],
) -> tuple[str, int]:
    raw_data, observed = _wire_data(observation, clock_ms)
    data = _object(raw_data, "trigger orders")
    _exact_keys(data, {"orders"}, "trigger orders")
    if type(data["orders"]) is not list or data["orders"]:
        raise NadoPreflightError("trigger history is not zero")
    return _digest({"identity": _identity_tag(config.sender), "orders_empty": True}), observed


def run_fixture_preflight(
    *,
    config: PreflightConfig,
    public_transport: object,
    time_transport: object,
    credential_loader: Callable[[], object],
    derive_owner: Callable[[object], str],
    signer: Callable[[object, dict[str, object]], str],
    recover_owner: Callable[[dict[str, object], str], str],
    signed_transport: object,
    clock_ms: Callable[[], int],
    store: OneShotStore,
) -> PreflightResult:
    """Run the deterministic fixture barrier; never an operational readiness claim."""
    if type(public_transport) is not SealedPublicTransport:
        raise NadoPreflightError("public transport boundary mismatch")
    if type(time_transport) is not SealedTimeTransport:
        raise NadoPreflightError("time transport boundary mismatch")
    if type(signed_transport) is not SealedSignedTransport:
        raise NadoPreflightError("signed transport boundary mismatch")
    identity_hash = _identity_hash(config.sender)
    identity_tag = _identity_tag(config.sender)
    evidence = store.evidence(config.invocation_id)
    if evidence is None:
        round_a = _round_a(public_transport, config, clock_ms)
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
        server_ms = _server_time(time_transport, clock_ms)
        if server_ms <= round_a.last_observed_ms:
            raise NadoPreflightError("server time is invalid or out of order")
        recv_time = str(server_ms + MAX_FRESHNESS_MS)
        typed_data = list_trigger_orders_typed_data(config.sender, recv_time)
        signature = _signature(
            _callback_value(signer, credential, typed_data, label="signer")
        )
        recovered = _callback_value(
            recover_owner, typed_data, signature, label="signature recovery"
        )
        if (
            type(recovered) is not str
            or _address_bytes(recovered) != _address_bytes(config.owner)
        ):
            raise NadoPreflightError("signature owner identity mismatch")
        request = {
            "type": "list_trigger_orders",
            "tx": {"sender": config.sender, "recvTime": recv_time},
            "signature": signature,
            "limit": 1,
        }
        observation = signed_transport.send(request)
        trigger_hash, trigger_observed_ms = _trigger_zero(observation, config, clock_ms)
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
    now_ms = _clock(clock_ms)
    if (
        trigger_observed_ms <= round_a_observed_ms
        or round_a_observed_ms > now_ms
        or trigger_observed_ms > now_ms
        or now_ms - round_a_observed_ms > MAX_FRESHNESS_MS
    ):
        raise NadoPreflightError("durable temporal evidence is invalid or stale")
    round_b = _round_b(public_transport, config, clock_ms)
    if round_b.first_observed_ms <= trigger_observed_ms:
        raise NadoPreflightError("public evidence temporal barrier mismatch")
    if round_b.last_observed_ms - round_a_observed_ms > MAX_FRESHNESS_MS:
        raise NadoPreflightError("public evidence temporal barrier is stale")
    if round_b.fingerprint != round_a_hash:
        raise NadoPreflightError("public rounds disagree")
    store.finalize(config.invocation_id, round_b.fingerprint)
    return PreflightResult(FINALIZED, identity_tag, True, True, True)


async def run_operational_private_read_preflight(
    *,
    config: PreflightConfig,
    credential_loader: Callable[[], object],
    derive_owner: Callable[[object], str],
    signer: Callable[[object, dict[str, object]], str],
    recover_owner: Callable[[dict[str, object], str], str],
    store: OneShotStore,
) -> PreflightResult:
    """Explicit disarmed entry point; no startup module imports or invokes it."""
    public_transport = _OperationalGatewayTransport()
    time_transport = _OperationalTimeTransport()
    signed_transport = _OperationalTriggerTransport()
    identity_hash = _identity_hash(config.sender)
    identity_tag = _identity_tag(config.sender)
    evidence = store.evidence(config.invocation_id)
    if evidence is None:
        round_a = await _operational_round_a(public_transport, config)
        await asyncio.sleep(0)
        store.claim(
            config.invocation_id, identity_hash, round_a.fingerprint,
            round_a.last_observed_ms,
        )
        await asyncio.sleep(0)
        credential = _callback_value(credential_loader, label="credential loader")
        await asyncio.sleep(0)
        derived = _callback_value(derive_owner, credential, label="owner derivation")
        if (
            type(derived) is not str
            or _address_bytes(derived) != _address_bytes(config.owner)
        ):
            raise NadoPreflightError("credential owner identity mismatch")
        await asyncio.sleep(0)
        time_observation = await time_transport.send_async({"type": "time"})
        await asyncio.sleep(0)
        server_ms = _server_time_observation(time_observation, _system_clock_ms)
        if server_ms <= round_a.last_observed_ms:
            raise NadoPreflightError("server time is invalid or out of order")
        recv_time = str(server_ms + MAX_FRESHNESS_MS)
        typed_data = list_trigger_orders_typed_data(config.sender, recv_time)
        await asyncio.sleep(0)
        signature = _signature(
            _callback_value(signer, credential, typed_data, label="signer")
        )
        await asyncio.sleep(0)
        recovered = _callback_value(
            recover_owner, typed_data, signature, label="signature recovery"
        )
        if (
            type(recovered) is not str
            or _address_bytes(recovered) != _address_bytes(config.owner)
        ):
            raise NadoPreflightError("signature owner identity mismatch")
        await asyncio.sleep(0)
        request = {
            "type": "list_trigger_orders",
            "tx": {"sender": config.sender, "recvTime": recv_time},
            "signature": signature,
            "limit": 1,
        }
        observation = await signed_transport.send_async(request)
        await asyncio.sleep(0)
        trigger_hash, trigger_observed_ms = _trigger_zero(
            observation, config, _system_clock_ms,
        )
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
    if trigger_hash != _digest({"identity": identity_tag, "orders_empty": True}):
        raise NadoPreflightError("durable trigger evidence is invalid")
    now_ms = _clock(_system_clock_ms)
    if (
        trigger_observed_ms <= round_a_observed_ms
        or round_a_observed_ms > now_ms
        or trigger_observed_ms > now_ms
        or now_ms - round_a_observed_ms > MAX_FRESHNESS_MS
    ):
        raise NadoPreflightError("durable temporal evidence is invalid or stale")
    round_b = await _operational_round_b(public_transport, config)
    if round_b.first_observed_ms <= trigger_observed_ms:
        raise NadoPreflightError("public evidence temporal barrier mismatch")
    if round_b.last_observed_ms - round_a_observed_ms > MAX_FRESHNESS_MS:
        raise NadoPreflightError("public evidence temporal barrier is stale")
    if round_b.fingerprint != round_a_hash:
        raise NadoPreflightError("public rounds disagree")
    store.finalize(config.invocation_id, round_b.fingerprint)
    return PreflightResult(FINALIZED, identity_tag, True, True, True)


async def _run_counted_operational_private_read(
    *,
    config: PreflightConfig,
    capability_loader: Callable[[], object],
    recover_owner: Callable[[dict[str, object], str], str],
    store: OneShotStore,
    path_hash: str,
    _transports: tuple[object, object, object] | None = None,
) -> dict[str, object]:
    """Private adapter seam for the sealed production launcher and fixtures.

    Existing durable identity always produces a zero-effect report.  It is never
    resumed, repaired, or rearmed.
    """
    invocation_id = config.invocation_id
    existing_state = store.state(invocation_id)
    if existing_state != NEW or store.terminal_report(invocation_id) is not None:
        if existing_state in {NEW, CLAIMED, OBSERVED}:
            counters = store.counters(invocation_id)
            reason = (
                "AMBIGUOUS_DISPATCH"
                if counters["trigger_dispatch_attempts"]
                > counters["trigger_observation_completions"]
                else "INTERRUPTED"
            )
            store.terminalize_unknown(invocation_id, reason)
        report = store.terminal_report(invocation_id)
        if report is None:
            raise NadoPreflightError("durable report is unavailable")
        return report

    store.begin(invocation_id, _identity_hash(config.sender), path_hash)
    public_transport, time_transport, trigger_transport = _transports or (
        _OperationalGatewayTransport(), _OperationalTimeTransport(),
        _OperationalTriggerTransport(),
    )
    handle: object | None = None
    try:
        round_a = await _operational_round_a(
            public_transport, config,
            lambda: store.count(invocation_id, "public_a"),
            lambda: store.count(invocation_id, "public_a", True),
        )
        store.claim_started(
            invocation_id, round_a.fingerprint, round_a.last_observed_ms,
            round_a.product_count,
        )

        store.count(invocation_id, "loader")
        await asyncio.sleep(0)
        handle = _counted_callback(capability_loader, label="credential loader")
        await asyncio.sleep(0)
        if not callable(getattr(handle, "derive_owner", None)) or not callable(
            getattr(handle, "sign_list_trigger_orders", None)
        ) or not callable(getattr(handle, "close", None)):
            raise NadoPreflightError("credential capability is invalid")
        store.count(invocation_id, "loader", True)
        await asyncio.sleep(0)

        store.count(invocation_id, "derive")
        await asyncio.sleep(0)
        derived = _counted_callback(handle.derive_owner, label="owner derivation")
        await asyncio.sleep(0)
        if type(derived) is not str or _address_bytes(derived) != _address_bytes(config.owner):
            raise NadoPreflightError("credential owner identity mismatch")
        store.count(invocation_id, "derive", True)
        await asyncio.sleep(0)

        store.count(invocation_id, "server_time")
        await asyncio.sleep(0)
        time_observation = await time_transport.send_async({"type": "time"})
        await asyncio.sleep(0)
        server_ms = _server_time_observation(time_observation, _system_clock_ms)
        if server_ms <= round_a.last_observed_ms:
            raise NadoPreflightError("server time is invalid or out of order")
        store.count(invocation_id, "server_time", True)
        await asyncio.sleep(0)

        recv_time = str(server_ms + MAX_FRESHNESS_MS)
        typed_data = list_trigger_orders_typed_data(config.sender, recv_time)
        store.count(invocation_id, "sign")
        await asyncio.sleep(0)
        signature = _signature(_counted_callback(
            handle.sign_list_trigger_orders, typed_data, label="signer",
        ))
        await asyncio.sleep(0)
        store.count(invocation_id, "sign", True)
        await asyncio.sleep(0)

        store.count(invocation_id, "recover")
        await asyncio.sleep(0)
        recovered = _counted_callback(
            recover_owner, typed_data, signature, label="signature recovery",
        )
        await asyncio.sleep(0)
        if type(recovered) is not str or _address_bytes(recovered) != _address_bytes(config.owner):
            raise NadoPreflightError("signature owner identity mismatch")
        store.count(invocation_id, "recover", True)
        await asyncio.sleep(0)

        request = {
            "type": "list_trigger_orders",
            "tx": {"sender": config.sender, "recvTime": recv_time},
            "signature": signature,
            "limit": 1,
        }
        store.count(invocation_id, "trigger_dispatch")
        await asyncio.sleep(0)
        observation = await trigger_transport.send_async(request)
        await asyncio.sleep(0)
        store.count(invocation_id, "trigger_dispatch", True)
        await asyncio.sleep(0)
        store.count(invocation_id, "trigger_observation")
        await asyncio.sleep(0)
        trigger_hash, trigger_observed_ms = _trigger_zero(
            observation, config, _system_clock_ms,
        )
        await asyncio.sleep(0)
        if trigger_observed_ms < server_ms or trigger_observed_ms <= round_a.last_observed_ms:
            raise NadoPreflightError("signed observation temporal order mismatch")
        store.count(invocation_id, "trigger_observation", True)
        await asyncio.sleep(0)
        store.observe(invocation_id, trigger_hash, trigger_observed_ms)
        await asyncio.sleep(0)

        round_b = await _operational_round_b(
            public_transport, config,
            lambda: store.count(invocation_id, "public_b"),
            lambda: store.count(invocation_id, "public_b", True),
        )
        if (
            round_b.first_observed_ms <= trigger_observed_ms
            or round_b.last_observed_ms - round_a.last_observed_ms > MAX_FRESHNESS_MS
            or round_b.fingerprint != round_a.fingerprint
            or round_b.product_count != round_a.product_count
        ):
            raise NadoPreflightError("public rounds disagree or temporal barrier failed")
        counters = store.counters(invocation_id)
        expected_a = round_a.product_count + 6
        expected_b = 2 * round_a.product_count + 7
        if (
            counters["public_a_attempts"] != expected_a
            or counters["public_a_completions"] != expected_a
            or counters["public_b_attempts"] != expected_b
            or counters["public_b_completions"] != expected_b
            or any(
                counters[f"{phase}_{suffix}"] != 1
                for phase in COUNTER_PHASES[1:-1]
                for suffix in ("attempts", "completions")
            )
        ):
            raise NadoPreflightError("durable counter totals disagree")
        store.finalize(invocation_id, round_b.fingerprint)
    except asyncio.CancelledError:
        counters = store.counters(invocation_id)
        reason = (
            "AMBIGUOUS_DISPATCH"
            if counters["trigger_dispatch_attempts"]
            > counters["trigger_observation_completions"]
            else "CANCELLED"
        )
        store.terminalize_unknown(invocation_id, reason)
        raise
    except BaseException:
        try:
            counters = store.counters(invocation_id)
            reason = (
                "AMBIGUOUS_DISPATCH"
                if counters["trigger_dispatch_attempts"]
                > counters["trigger_dispatch_completions"]
                else "VALIDATION_FAILED"
            )
            store.terminalize_unknown(invocation_id, reason)
        except BaseException:
            pass
    finally:
        if handle is not None:
            try:
                handle.close()
            except BaseException:
                pass
    report = store.terminal_report(invocation_id)
    if report is None:
        raise NadoPreflightError("durable report is unavailable")
    return report
