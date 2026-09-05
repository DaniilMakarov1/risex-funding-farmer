"""Opt-in, fail-closed RISEx mainnet owner-fee read.

The public Spread Shadow command never imports this module.  The separate
entry point is a Level-B boundary for one exact owner account: it checks the
fixed local RISEx identity/session-signer files, verifies public session-key
readiness, signs the official ``Login`` EIP-712 message with a locally entered
owner key, and performs one caller-owned ``GET /v1/user/fees`` read.

The module deliberately has no order, position, balance, collateral, transfer,
withdrawal, deposit, strategy, or write surface.  It accepts no secret-bearing
arguments or environment values.  The owner key is collected with hidden local
input, used only in memory, and zeroized before return.  Tests inject synthetic
capabilities and transports; the production runner is not invoked by the test
suite.

Frozen contract provenance supplied for SS-001K on 2026-09-05:

* mainnet REST origin: ``https://api.rise.trade``;
* EIP-712 domain: ``RISEx`` / ``1`` / chain ``4153`` / the exact mainnet
  authorization contract below;
* login domain prerequisite: ``GET /v1/auth/eip712-domain`` from the fixed
  origin, with the returned name, version, chain, and verifying contract
  validated before any readiness, nonce, signing, login, or fee step;
* nonce: ``GET /v1/auth/nonce?account=<exact wallet>``; the accepted
  ``data.nonce`` wire value is either the observed 64-character unprefixed
  hexadecimal form or a documented ``0x``-prefixed 1..64-character
  hexadecimal form.  Both forms are parsed base-16 for EIP-712 while the
  exact received string is preserved in the login request;
* login: the account signs ``Login(address account,uint256 nonce,uint32
  deadline)`` and sends ``POST /v1/auth/login``;
* session-key status is public readiness only;
* fees are read only through caller-owned ``GET /v1/user/fees``;
* Lighter Standard research inputs are zero maker/taker bps, 300 ms taker,
  200 ms maker, and 300 ms cancel latency.
"""

from __future__ import annotations

import asyncio
import base64
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import errno
import getpass
import hashlib
import json
import math
from pathlib import Path
import os
import ssl
import stat
import sys
import time
from typing import Any, Callable, Mapping, Sequence

import aiohttp
from yarl import URL


MAINNET_REST_ORIGIN = URL("https://api.rise.trade")
MAINNET_CHAIN_ID = 4153
MAINNET_DOMAIN_NAME = "RISEx"
MAINNET_DOMAIN_VERSION = "1"
MAINNET_AUTH_CONTRACT = "0x0d919daa3f12ae715744eb648c00066c5dbd66f0"

NONCE_PATH = "/v1/auth/nonce"
DOMAIN_PATH = "/v1/auth/eip712-domain"
SESSION_KEY_STATUS_PATH = "/v1/auth/session-key-status"
LOGIN_PATH = "/v1/auth/login"
FEES_PATH = "/v1/user/fees"

ALLOWED_ENDPOINTS = (
    ("GET", DOMAIN_PATH),
    ("GET", NONCE_PATH),
    ("GET", SESSION_KEY_STATUS_PATH),
    ("POST", LOGIN_PATH),
    ("GET", FEES_PATH),
)
_ALLOWED_ENDPOINT_SET = frozenset(ALLOWED_ENDPOINTS)

PROTECTED_DIRECTORY = Path.home() / ".config" / "risex-farmer"
SESSION_KEY_FILENAME = "risex-mainnet-session-signer-v1.key"
IDENTITY_FILENAME = "risex-mainnet-identity-v1.json"
REGISTRATION_INTENT_FILENAME = "risex-mainnet-register-signer-v1.json"
REGISTRATION_SPENT_FILENAME = "risex-mainnet-register-signer-v1.spent"

OBSERVED_ON = "2026-09-05"
OFFICIAL_SOURCES = {
    "risex_domain": "https://developer.rise.trade/reference/authservice_geteip712domain",
    "risex_nonce": "https://developer.rise.trade/reference/authservice_getloginnonce",
    "risex_login": "https://developer.rise.trade/reference/authservice_login",
    "risex_session_key_status": "https://developer.rise.trade/reference/authservice_getsessionkeystatus",
    "risex_fees": "https://developer.rise.trade/reference/feetierservice_getuserfees",
    "lighter_standard_fees": "https://docs.lighter.xyz/trading/trading-fees",
}
LIGHTER_STANDARD_INPUTS = {
    "checked_on": OBSERVED_ON,
    "maker_bps": 0,
    "taker_bps": 0,
    "taker_latency_ms": 300,
    "maker_latency_ms": 200,
    "cancel_latency_ms": 300,
    "source": OFFICIAL_SOURCES["lighter_standard_fees"],
}

_EIP712_DOMAIN_FIELDS = (
    {"name": "name", "type": "string"},
    {"name": "version", "type": "string"},
    {"name": "chainId", "type": "uint256"},
    {"name": "verifyingContract", "type": "address"},
)
_LOGIN_FIELDS = (
    {"name": "account", "type": "address"},
    {"name": "nonce", "type": "uint256"},
    {"name": "deadline", "type": "uint32"},
)

_FAILURE_CLASSES = frozenset(
    {"TRANSPORT", "HTTP", "SCHEMA", "AUTH", "IDENTITY", "SAFETY"}
)
_SAFE_REASONS = frozenset(
    {
        "FEE_READ_COMPLETE",
        "ARGUMENTS_REJECTED",
        "PROTECTED_CREDENTIALS_UNAVAILABLE",
        "PROTECTED_PATH_INVALID",
        "IDENTITY_FILE_INVALID",
        "IDENTITY_BINDING_MISMATCH",
        "SESSION_SIGNER_IDENTITY_MISMATCH",
        "SESSION_STATUS_INVALID",
        "SESSION_KEY_NOT_ACTIVE",
        "NONCE_INVALID",
        "DOMAIN_RESPONSE_INVALID",
        "DOMAIN_BINDING_MISMATCH",
        "OWNER_INPUT_CANCELLED",
        "OWNER_INPUT_UNAVAILABLE",
        "OWNER_KEY_INVALID",
        "OWNER_KEY_IDENTITY_MISMATCH",
        "LOGIN_SIGNATURE_INVALID",
        "LOGIN_SIGNATURE_IDENTITY_MISMATCH",
        "LOGIN_RESPONSE_INVALID",
        "FEE_RESPONSE_INVALID",
        "AUTH_RESPONSE_REJECTED",
        "HTTP_RESPONSE_REJECTED",
        "TRANSPORT_RETRY_EXHAUSTED",
        "ENDPOINT_NOT_ALLOWED",
        "HOST_MISMATCH",
        "REDIRECT_REJECTED",
        "PROTECTED_INPUT_REJECTED",
        "UNCLASSIFIED_EXCEPTION",
    }
)

_DIRECTORY_MODE = 0o700
_FILE_MODE = 0o600
_MAX_IDENTITY_BYTES = 4_096
_MAX_SESSION_KEY_BYTES = 32
_MAX_RESPONSE_BYTES = 1_048_576
_MAX_TOKEN_CHARS = 16_384
_MAX_DECIMAL_CHARS = 128
_MAX_DECIMAL_ADJUSTED = 64
_MAX_SCHEDULE_ROWS = 64
_UINT32_MAX = 2**32 - 1
_UINT256_MAX = 2**256 - 1
_PROMPT = "RISEx owner main-wallet private key (hidden; used once, not persisted): "

READY = "READY"
BLOCKED = "BLOCKED"


class _FeeReadFailure(Exception):
    """Fixed terminal failure that never carries venue or secret text."""

    def __init__(self, reason: str, failure_class: str) -> None:
        if reason not in _SAFE_REASONS or failure_class not in _FAILURE_CLASSES:
            reason, failure_class = "UNCLASSIFIED_EXCEPTION", "SAFETY"
        self.reason = reason
        self.failure_class = failure_class
        super().__init__(reason)


def _failure(reason: str, failure_class: str = "SAFETY") -> _FeeReadFailure:
    return _FeeReadFailure(reason, failure_class)


@dataclass(frozen=True, slots=True)
class HttpObservation:
    status: int
    final_url: str
    body: Any


@dataclass(frozen=True, slots=True)
class _LoginTokens:
    access_token: str
    refresh_token: str
    expires_in: int


@dataclass(frozen=True, slots=True)
class ProvisionedIdentity:
    wallet_address: str
    session_signer_address: str
    expiration: int
    environment: str = "MAINNET"
    chain_id: int = MAINNET_CHAIN_ID
    verifying_contract: str = MAINNET_AUTH_CONTRACT
    venue: str = "RISEx"
    registration_status: str = "NOT_PREPARED"
    schema_version: int = 1


@dataclass(frozen=True, slots=True)
class ProtectedPathState:
    name: str
    path: str
    present: bool
    protected: bool
    reason: str
    size: int | None = None


@dataclass(frozen=True, slots=True)
class ProtectedFiles:
    session_key: ProtectedPathState
    identity: ProtectedPathState
    registration_intent: ProtectedPathState
    registration_spent: ProtectedPathState

    @property
    def all_required_protected(self) -> bool:
        return self.session_key.protected and self.identity.protected

    def for_name(self, name: str) -> ProtectedPathState:
        return {
            SESSION_KEY_FILENAME: self.session_key,
            IDENTITY_FILENAME: self.identity,
            REGISTRATION_INTENT_FILENAME: self.registration_intent,
            REGISTRATION_SPENT_FILENAME: self.registration_spent,
        }[name]


@dataclass(frozen=True, slots=True)
class FeeReadReport:
    """Only sanitized fee evidence and a fixed terminal classification."""

    status: str
    terminal_classification: str
    reason: str
    observed_at: str
    provenance: Mapping[str, Any]
    account_fingerprint: str | None = None
    session_signer_status: str | None = None
    tier: int | None = None
    maker_bps: str | None = None
    taker_bps: str | None = None
    schedule: tuple[Mapping[str, Any], ...] | None = None
    trial_tier: int | None = None
    trial_ends_at: str | None = None
    earned_tier: int | None = None

    def as_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "account_fingerprint": self.account_fingerprint,
            "earned_tier": self.earned_tier,
            "maker_bps": self.maker_bps,
            "observed_at": self.observed_at,
            "provenance": _copy_json_value(self.provenance),
            "reason": self.reason,
            "schedule": (
                None
                if self.schedule is None
                else [dict(entry) for entry in self.schedule]
            ),
            "session_signer_status": self.session_signer_status,
            "status": self.status,
            "taker_bps": self.taker_bps,
            "terminal_classification": self.terminal_classification,
            "tier": self.tier,
            "trial_ends_at": self.trial_ends_at,
            "trial_tier": self.trial_tier,
        }
        return result

    def evidence(self) -> str:
        return json.dumps(self.as_dict(), sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True, slots=True)
class _Dependencies:
    inspect_files: Callable[[], Any]
    read_identity: Callable[[], Any]
    owner_capability_factory: Callable[[], Any]
    transport_factory: Callable[[], Any]
    clock: Callable[[], float]
    read_session_signer: Callable[[], str] | None = None


def _copy_json_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _copy_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_copy_json_value(item) for item in value]
    return value


def _now_iso(clock: Callable[[], float]) -> str:
    try:
        value = clock()
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError
        value = float(value)
        if not math.isfinite(value) or value < 0:
            raise ValueError
        return datetime.fromtimestamp(value, tz=timezone.utc).isoformat(
            timespec="milliseconds"
        ).replace("+00:00", "Z")
    except Exception:
        return "UNKNOWN"


def _provenance(observed_endpoints: Sequence[str]) -> dict[str, Any]:
    return {
        "api_origin": str(MAINNET_REST_ORIGIN),
        "observed_endpoints": list(observed_endpoints),
        "risex": {
            "chain_id": MAINNET_CHAIN_ID,
            "domain_name": MAINNET_DOMAIN_NAME,
            "domain_version": MAINNET_DOMAIN_VERSION,
            "verifying_contract": MAINNET_AUTH_CONTRACT,
        },
        "official_sources": _copy_json_value(OFFICIAL_SOURCES),
        "lighter_standard": _copy_json_value(LIGHTER_STANDARD_INPUTS),
    }


def _report(
    *,
    status: str,
    terminal_classification: str,
    reason: str,
    clock: Callable[[], float],
    observed_endpoints: Sequence[str] = (),
    account_fingerprint: str | None = None,
    session_signer_status: str | None = None,
    **fields: Any,
) -> FeeReadReport:
    if status not in {READY, BLOCKED}:
        status, terminal_classification, reason = (
            BLOCKED,
            "SAFETY",
            "UNCLASSIFIED_EXCEPTION",
        )
    if terminal_classification not in _FAILURE_CLASSES | {"COMPLETE"}:
        terminal_classification, reason = "SAFETY", "UNCLASSIFIED_EXCEPTION"
    if reason not in _SAFE_REASONS:
        reason = "UNCLASSIFIED_EXCEPTION"
    if status == READY and terminal_classification != "COMPLETE":
        status, terminal_classification, reason = (
            BLOCKED,
            "SAFETY",
            "UNCLASSIFIED_EXCEPTION",
        )
    if status == BLOCKED and terminal_classification == "COMPLETE":
        terminal_classification, reason = "SAFETY", "UNCLASSIFIED_EXCEPTION"
    allowed = {
        "tier",
        "maker_bps",
        "taker_bps",
        "schedule",
        "trial_tier",
        "trial_ends_at",
        "earned_tier",
    }
    clean_fields = {key: value for key, value in fields.items() if key in allowed}
    return FeeReadReport(
        status=status,
        terminal_classification=terminal_classification,
        reason=reason,
        observed_at=_now_iso(clock),
        provenance=_provenance(observed_endpoints),
        account_fingerprint=account_fingerprint,
        session_signer_status=session_signer_status,
        **clean_fields,
    )


def _strict_json(raw: bytes | bytearray) -> Any:
    def reject_constant(_value: str) -> Any:
        raise ValueError

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError
            result[key] = value
        return result

    if not isinstance(raw, (bytes, bytearray)) or len(raw) > _MAX_RESPONSE_BYTES:
        raise _failure("FEE_RESPONSE_INVALID", "SCHEMA")
    try:
        return json.loads(
            bytes(raw).decode("utf-8", errors="strict"),
            parse_constant=reject_constant,
            object_pairs_hook=unique_object,
        )
    except Exception:
        raise _failure("FEE_RESPONSE_INVALID", "SCHEMA") from None


def _response_data(value: Any, reason: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or "data" not in value:
        raise _failure(reason, "SCHEMA")
    data = value["data"]
    if not isinstance(data, Mapping):
        raise _failure(reason, "SCHEMA")
    return data


def _normalize_address(value: Any, reason: str = "IDENTITY_BINDING_MISMATCH") -> str:
    if (
        type(value) is not str
        or len(value) != 42
        or not value.startswith("0x")
        or any(character not in "0123456789abcdefABCDEF" for character in value[2:])
    ):
        raise _failure(reason, "IDENTITY")
    return value.lower()


def _strict_uint(value: Any, *, maximum: int, reason: str) -> int:
    if type(value) is int:
        parsed = value
    elif (
        type(value) is str
        and value
        and value == value.strip()
        and value.isascii()
        and value.isdecimal()
        and (value == "0" or not value.startswith("0"))
    ):
        try:
            parsed = int(value, 10)
        except ValueError:
            raise _failure(reason, "SCHEMA") from None
    else:
        raise _failure(reason, "SCHEMA")
    if parsed < 0 or parsed > maximum:
        raise _failure(reason, "SAFETY")
    return parsed


def _parse_nonce(value: Any) -> tuple[str, int]:
    if isinstance(value, Mapping) and "nonce" in value:
        raise _failure("NONCE_INVALID", "SCHEMA")
    data = _response_data(value, "NONCE_INVALID")
    raw = data.get("nonce")
    if type(raw) is not str:
        raise _failure("NONCE_INVALID", "SCHEMA")

    if raw.startswith("0x"):
        digits = raw[2:]
        if not 1 <= len(digits) <= 64:
            raise _failure("NONCE_INVALID", "SCHEMA")
    else:
        if len(raw) != 64:
            raise _failure("NONCE_INVALID", "SCHEMA")
        digits = raw

    if any(character not in "0123456789abcdefABCDEF" for character in digits):
        raise _failure("NONCE_INVALID", "SCHEMA") from None
    return raw, int(digits, 16)


def _parse_domain(value: Any) -> None:
    data = _response_data(value, "DOMAIN_RESPONSE_INVALID")
    required = {"name", "version", "chain_id", "verifying_contract"}
    if not required <= set(data):
        raise _failure("DOMAIN_RESPONSE_INVALID", "SCHEMA")

    if type(data["name"]) is not str or data["name"] != MAINNET_DOMAIN_NAME:
        raise _failure("DOMAIN_BINDING_MISMATCH", "IDENTITY")
    if type(data["version"]) is not str or data["version"] != MAINNET_DOMAIN_VERSION:
        raise _failure("DOMAIN_BINDING_MISMATCH", "IDENTITY")

    chain_id = _strict_uint(
        data["chain_id"], maximum=_UINT256_MAX, reason="DOMAIN_RESPONSE_INVALID"
    )
    if chain_id != MAINNET_CHAIN_ID:
        raise _failure("DOMAIN_BINDING_MISMATCH", "IDENTITY")

    verifying_contract = _normalize_address(
        data["verifying_contract"], "DOMAIN_RESPONSE_INVALID"
    )
    if verifying_contract != MAINNET_AUTH_CONTRACT:
        raise _failure("DOMAIN_BINDING_MISMATCH", "IDENTITY")


def _validate_session_status(
    value: Any, *, expected_account: str, expected_signer: str
) -> str:
    data = _response_data(value, "SESSION_STATUS_INVALID")
    if "status" not in data or "status_description" not in data:
        raise _failure("SESSION_STATUS_INVALID", "SCHEMA")
    for field, expected in (
        ("account", expected_account),
        ("signer", expected_signer),
    ):
        if field in data:
            observed = _normalize_address(data[field])
            if observed != expected:
                raise _failure("IDENTITY_BINDING_MISMATCH", "IDENTITY")
    status = _strict_uint(data["status"], maximum=2, reason="SESSION_STATUS_INVALID")
    descriptions = {0: "NotExist", 1: "Active", 2: "Revoked"}
    description = data["status_description"]
    if type(description) is not str or description != descriptions[status]:
        raise _failure("SESSION_STATUS_INVALID", "SCHEMA")
    if status != 1:
        raise _failure("SESSION_KEY_NOT_ACTIVE", "AUTH")
    return description


def _validate_token(value: Any, reason: str) -> str:
    if (
        type(value) is not str
        or not value
        or len(value) > _MAX_TOKEN_CHARS
        or value != value.strip()
        or any(ord(character) < 0x21 or ord(character) > 0x7E for character in value)
    ):
        raise _failure(reason, "SCHEMA")
    return value


def _decode_jwt_segment(segment: str, reason: str) -> bytes:
    if not segment or len(segment) > _MAX_TOKEN_CHARS or any(
        character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-"
        for character in segment
    ):
        raise _failure(reason, "SCHEMA")
    try:
        decoded = base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))
    except Exception:
        raise _failure(reason, "SCHEMA") from None
    if base64.urlsafe_b64encode(decoded).decode("ascii").rstrip("=") != segment:
        raise _failure(reason, "SCHEMA")
    return decoded


def _validate_jwt(value: Any, reason: str) -> str:
    token = _validate_token(value, reason)
    parts = token.split(".")
    if len(parts) != 3:
        raise _failure(reason, "SCHEMA")
    header_raw = _decode_jwt_segment(parts[0], reason)
    payload_raw = _decode_jwt_segment(parts[1], reason)
    signature_raw = _decode_jwt_segment(parts[2], reason)
    if not signature_raw:
        raise _failure(reason, "SCHEMA")
    try:
        header = _strict_json(header_raw)
        payload = _strict_json(payload_raw)
    except _FeeReadFailure:
        raise _failure(reason, "SCHEMA") from None
    if not isinstance(header, Mapping) or not isinstance(payload, Mapping):
        raise _failure(reason, "SCHEMA")
    algorithm = header.get("alg")
    if type(algorithm) is not str or not algorithm or algorithm == "none":
        raise _failure(reason, "SAFETY")
    return token


def _parse_login_response(value: Any) -> _LoginTokens:
    data = _response_data(value, "LOGIN_RESPONSE_INVALID")
    required = {"access_token", "refresh_token", "expires_in", "token_type"}
    if not required <= set(data):
        raise _failure("LOGIN_RESPONSE_INVALID", "SCHEMA")
    access = _validate_jwt(data["access_token"], "LOGIN_RESPONSE_INVALID")
    refresh = _validate_token(data["refresh_token"], "LOGIN_RESPONSE_INVALID")
    expires = _strict_uint(
        data["expires_in"], maximum=_UINT32_MAX, reason="LOGIN_RESPONSE_INVALID"
    )
    if expires <= 0:
        raise _failure("LOGIN_RESPONSE_INVALID", "SAFETY")
    if data["token_type"] != "Bearer":
        raise _failure("LOGIN_RESPONSE_INVALID", "SCHEMA")
    return _LoginTokens(access, refresh, expires)


def _decimal(value: Any, *, nonnegative: bool, reason: str) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise _failure(reason, "SCHEMA")
    if isinstance(value, float) and not math.isfinite(value):
        raise _failure(reason, "SCHEMA")
    text = str(value)
    if not text or len(text) > _MAX_DECIMAL_CHARS or text != text.strip():
        raise _failure(reason, "SCHEMA")
    try:
        parsed = Decimal(text)
    except (InvalidOperation, ValueError):
        raise _failure(reason, "SCHEMA") from None
    if not parsed.is_finite() or abs(parsed.adjusted()) > _MAX_DECIMAL_ADJUSTED:
        raise _failure(reason, "SCHEMA")
    if nonnegative and parsed < 0:
        raise _failure(reason, "SAFETY")
    normalized = format(parsed, "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized if normalized not in {"", "-0", "-"} else "0"


def _decimal_string(value: Any, *, nonnegative: bool, reason: str) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise _failure(reason, "SCHEMA")
    return _decimal(value, nonnegative=nonnegative, reason=reason)


def _parse_tier(value: Any, reason: str) -> int:
    return _strict_uint(value, maximum=_UINT32_MAX, reason=reason)


def _parse_rfc3339(value: Any, reason: str) -> str | None:
    if type(value) is not str or len(value) > 128:
        raise _failure(reason, "SCHEMA")
    if value == "":
        return None
    if not value.endswith("Z"):
        raise _failure(reason, "SCHEMA")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        raise _failure(reason, "SCHEMA") from None
    if parsed.tzinfo is None:
        raise _failure(reason, "SCHEMA")
    return parsed.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def _parse_schedule_entry(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise _failure("FEE_RESPONSE_INVALID", "SCHEMA")
    required = {"tier", "threshold_usd", "taker_bps", "maker_bps"}
    if not required <= set(value):
        raise _failure("FEE_RESPONSE_INVALID", "SCHEMA")
    return {
        "maker_bps": _decimal(
            value["maker_bps"], nonnegative=False, reason="FEE_RESPONSE_INVALID"
        ),
        "taker_bps": _decimal(
            value["taker_bps"], nonnegative=True, reason="FEE_RESPONSE_INVALID"
        ),
        "threshold_usd": _decimal_string(
            value["threshold_usd"], nonnegative=True, reason="FEE_RESPONSE_INVALID"
        ),
        "tier": _parse_tier(value["tier"], "FEE_RESPONSE_INVALID"),
    }


def _parse_fees(value: Any) -> dict[str, Any]:
    data = _response_data(value, "FEE_RESPONSE_INVALID")
    required = {"tier", "taker_bps", "maker_bps"}
    if not required <= set(data):
        raise _failure("FEE_RESPONSE_INVALID", "SCHEMA")
    schedule: tuple[Mapping[str, Any], ...] | None = None
    if "schedule" in data:
        raw_schedule = data["schedule"]
        if not isinstance(raw_schedule, list) or len(raw_schedule) > _MAX_SCHEDULE_ROWS:
            raise _failure("FEE_RESPONSE_INVALID", "SCHEMA")
        entries = [_parse_schedule_entry(item) for item in raw_schedule]
        if len({entry["tier"] for entry in entries}) != len(entries):
            raise _failure("FEE_RESPONSE_INVALID", "SAFETY")
        schedule = tuple(sorted(entries, key=lambda item: item["tier"]))

    result: dict[str, Any] = {
        "tier": _parse_tier(data["tier"], "FEE_RESPONSE_INVALID"),
        "taker_bps": _decimal(
            data["taker_bps"], nonnegative=True, reason="FEE_RESPONSE_INVALID"
        ),
        "maker_bps": _decimal(
            data["maker_bps"], nonnegative=False, reason="FEE_RESPONSE_INVALID"
        ),
        "schedule": schedule,
        "trial_tier": None,
        "trial_ends_at": None,
        "earned_tier": None,
    }
    if "trial_tier" in data:
        result["trial_tier"] = _parse_tier(data["trial_tier"], "FEE_RESPONSE_INVALID")
    if "trial_ends_at" in data:
        result["trial_ends_at"] = _parse_rfc3339(
            data["trial_ends_at"], "FEE_RESPONSE_INVALID"
        )
    if "earned_tier" in data:
        result["earned_tier"] = _parse_tier(data["earned_tier"], "FEE_RESPONSE_INVALID")
    return result


def _login_typed_data(account: str, nonce: int, deadline: int) -> dict[str, Any]:
    account_address = _normalize_address(account, "IDENTITY_BINDING_MISMATCH")
    nonce_value = _strict_uint(nonce, maximum=_UINT256_MAX, reason="NONCE_INVALID")
    deadline_value = _strict_uint(
        deadline, maximum=_UINT32_MAX, reason="LOGIN_SIGNATURE_INVALID"
    )
    return {
        "types": {
            "EIP712Domain": [dict(field) for field in _EIP712_DOMAIN_FIELDS],
            "Login": [dict(field) for field in _LOGIN_FIELDS],
        },
        "primaryType": "Login",
        "domain": {
            "name": MAINNET_DOMAIN_NAME,
            "version": MAINNET_DOMAIN_VERSION,
            "chainId": MAINNET_CHAIN_ID,
            "verifyingContract": MAINNET_AUTH_CONTRACT,
        },
        "message": {
            "account": account_address,
            "nonce": nonce_value,
            "deadline": deadline_value,
        },
    }


def _valid_signature(value: Any) -> bool:
    return (
        type(value) is str
        and len(value) == 132
        and value.startswith("0x")
        and all(character in "0123456789abcdefABCDEF" for character in value[2:])
    )


def _sign_login(secret: bytearray, typed_data: Mapping[str, Any]) -> str:
    try:
        from eth_account import Account
        from eth_account.messages import encode_typed_data

        signed = Account.sign_message(
            encode_typed_data(full_message=dict(typed_data)), bytes(secret)
        )
        signature = "0x" + bytes(signed.signature).hex()
        if not _valid_signature(signature):
            raise ValueError
        return signature
    except _FeeReadFailure:
        raise
    except Exception:
        raise _failure("LOGIN_SIGNATURE_INVALID", "SAFETY") from None


def _recover_login(typed_data: Mapping[str, Any], signature: str) -> str:
    try:
        from eth_account import Account
        from eth_account.messages import encode_typed_data

        return Account.recover_message(
            encode_typed_data(full_message=dict(typed_data)), signature=signature
        ).lower()
    except Exception:
        raise _failure("LOGIN_SIGNATURE_INVALID", "SAFETY") from None


def _derive_address(secret: bytes | bytearray) -> str:
    try:
        from eth_account import Account

        address = Account.from_key(bytes(secret)).address
    except Exception:
        raise _failure("OWNER_KEY_INVALID", "IDENTITY") from None
    try:
        return _normalize_address(address, "OWNER_KEY_INVALID")
    except _FeeReadFailure:
        raise _failure("OWNER_KEY_INVALID", "IDENTITY") from None


def _zeroize(secret: bytearray | None) -> None:
    if secret is None:
        return
    for index in range(len(secret)):
        secret[index] = 0
    secret.clear()


def _parse_main_key(value: Any) -> bytearray:
    if type(value) is not str or not value or len(value) > 66:
        raise _failure("OWNER_KEY_INVALID", "IDENTITY")
    digits = value[2:] if value.startswith("0x") else value
    if len(digits) != 64 or any(
        character not in "0123456789abcdefABCDEF" for character in digits
    ):
        raise _failure("OWNER_KEY_INVALID", "IDENTITY")
    try:
        secret = bytearray.fromhex(digits)
    except ValueError:
        raise _failure("OWNER_KEY_INVALID", "IDENTITY") from None
    if len(secret) != _MAX_SESSION_KEY_BYTES:
        _zeroize(secret)
        raise _failure("OWNER_KEY_INVALID", "IDENTITY")
    return secret


class _OwnerLoginCapability:
    """Short-lived hidden-input owner signing capability."""

    def __init__(self, input_fn: Callable[[str], str]) -> None:
        supplied: str | None = None
        secret: bytearray | None = None
        try:
            try:
                supplied = input_fn(_PROMPT)
            except (EOFError, KeyboardInterrupt):
                raise _failure("OWNER_INPUT_CANCELLED", "SAFETY") from None
            except Exception:
                raise _failure("OWNER_INPUT_UNAVAILABLE", "SAFETY") from None
            secret = _parse_main_key(supplied)
            self._secret = secret
            secret = None
            self._closed = False
        finally:
            supplied = None
            _zeroize(secret)

    def _check(self) -> None:
        if getattr(self, "_closed", True):
            raise _failure("UNCLASSIFIED_EXCEPTION", "SAFETY")

    def wallet_address(self) -> str:
        self._check()
        return _derive_address(self._secret)

    def sign_login(self, typed_data: Mapping[str, Any]) -> str:
        self._check()
        return _sign_login(self._secret, typed_data)

    def close(self) -> None:
        if getattr(self, "_closed", True):
            return
        _zeroize(self._secret)
        self._closed = True


def _is_transport_exception(error: BaseException) -> bool:
    return isinstance(
        error,
        (
            aiohttp.ClientConnectionError,
            aiohttp.ClientPayloadError,
            asyncio.TimeoutError,
            ConnectionError,
            OSError,
        ),
    )


def _validate_login_body(body: Any) -> None:
    try:
        if not isinstance(body, Mapping) or set(body) != {
            "account",
            "nonce",
            "deadline",
            "signature",
        }:
            raise ValueError
        _normalize_address(body["account"], "ENDPOINT_NOT_ALLOWED")
        _parse_nonce({"data": {"nonce": body["nonce"]}})
        _strict_uint(
            body["deadline"], maximum=_UINT32_MAX, reason="ENDPOINT_NOT_ALLOWED"
        )
        if not _valid_signature(body["signature"]):
            raise ValueError
    except (_FeeReadFailure, KeyError, TypeError, ValueError):
        raise _failure("ENDPOINT_NOT_ALLOWED", "SAFETY")


def _validate_bearer_token(token: Any) -> str:
    try:
        return _validate_jwt(token, "ENDPOINT_NOT_ALLOWED")
    except _FeeReadFailure:
        raise _failure("ENDPOINT_NOT_ALLOWED", "SAFETY") from None


class FixedRisexFeeReadTransport:
    """Fixed-origin, no-redirect, no-automatic-retry transport."""

    REST_ORIGIN = MAINNET_REST_ORIGIN
    TIMEOUT_SECONDS = 15

    def __init__(self) -> None:
        try:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15, connect=5, sock_read=10),
                trust_env=False,
                connector=aiohttp.TCPConnector(ssl=ssl.create_default_context()),
            )
            if hasattr(self._session, "_retry_connection"):
                self._session._retry_connection = False
        except Exception:
            raise _failure("PROTECTED_CREDENTIALS_UNAVAILABLE", "SAFETY") from None

    @staticmethod
    def _target(
        method: str,
        path: str,
        query: tuple[tuple[str, str], ...] = (),
    ) -> URL:
        if (
            type(method) is not str
            or type(path) is not str
            or type(query) is not tuple
            or (method, path) not in _ALLOWED_ENDPOINT_SET
        ):
            raise _failure("ENDPOINT_NOT_ALLOWED", "SAFETY")
        if any(
            type(pair) is not tuple
            or len(pair) != 2
            or type(pair[0]) is not str
            or type(pair[1]) is not str
            for pair in query
        ):
            raise _failure("ENDPOINT_NOT_ALLOWED", "SAFETY")
        if path == NONCE_PATH:
            if query.__len__() != 1 or query[0][0] != "account":
                raise _failure("ENDPOINT_NOT_ALLOWED", "SAFETY")
            _normalize_address(query[0][1], "ENDPOINT_NOT_ALLOWED")
        elif path == SESSION_KEY_STATUS_PATH:
            if tuple(name for name, _ in query) != ("account", "signer"):
                raise _failure("ENDPOINT_NOT_ALLOWED", "SAFETY")
            account = _normalize_address(query[0][1], "ENDPOINT_NOT_ALLOWED")
            signer = _normalize_address(query[1][1], "ENDPOINT_NOT_ALLOWED")
            if account == signer:
                raise _failure("ENDPOINT_NOT_ALLOWED", "SAFETY")
        elif query:
            raise _failure("ENDPOINT_NOT_ALLOWED", "SAFETY")
        return MAINNET_REST_ORIGIN.with_path(path).with_query(query)

    async def request(
        self,
        method: str,
        path: str,
        *,
        query: tuple[tuple[str, str], ...] = (),
        body: Mapping[str, Any] | None = None,
        bearer_token: str | None = None,
    ) -> HttpObservation:
        target = self._target(method, path, query)
        if path == LOGIN_PATH:
            if bearer_token is not None:
                raise _failure("ENDPOINT_NOT_ALLOWED", "SAFETY")
            _validate_login_body(body)
        elif path in {DOMAIN_PATH, NONCE_PATH, SESSION_KEY_STATUS_PATH}:
            if body is not None or bearer_token is not None:
                raise _failure("ENDPOINT_NOT_ALLOWED", "SAFETY")
        elif path == FEES_PATH:
            if body is not None or type(bearer_token) is not str:
                raise _failure("ENDPOINT_NOT_ALLOWED", "SAFETY")
            _validate_bearer_token(bearer_token)

        headers = {"Accept": "application/json"}
        if path == FEES_PATH:
            headers["Authorization"] = "Bearer " + str(bearer_token)
        kwargs: dict[str, Any] = {
            "allow_redirects": False,
            "headers": headers,
            "proxy": None,
        }
        if body is not None:
            kwargs["json"] = dict(body)
        try:
            async with self._session.request(method, target, **kwargs) as response:
                if response.history or str(response.url) != str(target):
                    raise _failure("HOST_MISMATCH", "SAFETY")
                if type(response.status) is not int:
                    raise _failure("HTTP_RESPONSE_REJECTED", "HTTP")
                if 300 <= response.status < 400:
                    raise _failure("REDIRECT_REJECTED", "SAFETY")
                if response.status < 200 or response.status >= 300:
                    failure_class = "AUTH" if response.status in {401, 403} else "HTTP"
                    raise _failure(
                        "AUTH_RESPONSE_REJECTED"
                        if failure_class == "AUTH"
                        else "HTTP_RESPONSE_REJECTED",
                        failure_class,
                    )
                raw = await response.content.read(_MAX_RESPONSE_BYTES + 1)
                return HttpObservation(
                    status=response.status,
                    final_url=str(response.url),
                    body=_strict_json(raw),
                )
        except asyncio.CancelledError:
            raise
        except _FeeReadFailure:
            raise
        except Exception as error:
            if _is_transport_exception(error):
                raise _failure("TRANSPORT_RETRY_EXHAUSTED", "TRANSPORT") from None
            raise _failure("UNCLASSIFIED_EXCEPTION", "SAFETY") from None

    async def close(self) -> None:
        try:
            await self._session.close()
        except Exception:
            return


def _validate_protected_files(files: Any) -> None:
    try:
        if not bool(files.all_required_protected):
            raise _failure("PROTECTED_PATH_INVALID", "SAFETY")
        for name in (REGISTRATION_INTENT_FILENAME, REGISTRATION_SPENT_FILENAME):
            state = files.for_name(name)
            if state.present and not state.protected:
                raise _failure("PROTECTED_PATH_INVALID", "SAFETY")
    except _FeeReadFailure:
        raise
    except Exception:
        raise _failure("PROTECTED_PATH_INVALID", "SAFETY") from None


def _validate_identity(identity: Any, now: float) -> tuple[str, str]:
    try:
        if (
            identity.environment != "MAINNET"
            or identity.chain_id != MAINNET_CHAIN_ID
            or str(identity.verifying_contract).lower() != MAINNET_AUTH_CONTRACT
            or identity.venue != "RISEx"
            or identity.registration_status != "NOT_PREPARED"
            or identity.schema_version != 1
        ):
            raise ValueError
        wallet = _normalize_address(identity.wallet_address, "IDENTITY_FILE_INVALID")
        signer = _normalize_address(
            identity.session_signer_address, "IDENTITY_FILE_INVALID"
        )
        if wallet == signer:
            raise ValueError
        expiration = _strict_uint(
            identity.expiration, maximum=_UINT32_MAX, reason="IDENTITY_FILE_INVALID"
        )
        if expiration <= int(now):
            raise ValueError
        return wallet, signer
    except _FeeReadFailure:
        raise _failure("IDENTITY_FILE_INVALID", "IDENTITY") from None
    except Exception:
        raise _failure("IDENTITY_FILE_INVALID", "IDENTITY") from None


async def _request_with_transport_retry(
    transport: Any,
    method: str,
    path: str,
    *,
    query: tuple[tuple[str, str], ...] = (),
    body: Mapping[str, Any] | None = None,
    bearer_token: str | None = None,
) -> HttpObservation:
    for attempt in range(2):
        try:
            observation = await transport.request(
                method,
                path,
                query=query,
                body=body,
                bearer_token=bearer_token,
            )
            if not isinstance(observation, HttpObservation):
                raise _failure("UNCLASSIFIED_EXCEPTION", "SAFETY")
            if type(observation.status) is not int:
                raise _failure("HTTP_RESPONSE_REJECTED", "HTTP")
            if observation.status < 200 or observation.status >= 300:
                if 300 <= observation.status < 400:
                    raise _failure("REDIRECT_REJECTED", "SAFETY")
                failure_class = "AUTH" if observation.status in {401, 403} else "HTTP"
                raise _failure(
                    "AUTH_RESPONSE_REJECTED"
                    if failure_class == "AUTH"
                    else "HTTP_RESPONSE_REJECTED",
                    failure_class,
                )
            expected_url = str(FixedRisexFeeReadTransport._target(method, path, query))
            if observation.final_url != expected_url:
                raise _failure("HOST_MISMATCH", "SAFETY")
            return observation
        except asyncio.CancelledError:
            raise
        except _FeeReadFailure as error:
            if error.failure_class == "TRANSPORT" and attempt == 0:
                continue
            if error.failure_class == "TRANSPORT":
                raise _failure("TRANSPORT_RETRY_EXHAUSTED", "TRANSPORT") from None
            raise
        except Exception as error:
            if _is_transport_exception(error) and attempt == 0:
                continue
            if _is_transport_exception(error):
                raise _failure("TRANSPORT_RETRY_EXHAUSTED", "TRANSPORT") from None
            raise _failure("UNCLASSIFIED_EXCEPTION", "SAFETY") from None
    raise _failure("TRANSPORT_RETRY_EXHAUSTED", "TRANSPORT")


async def _run_with_dependencies(dependencies: _Dependencies) -> FeeReadReport:
    transport: Any = None
    capability: Any = None
    access_token: str | None = None
    fingerprint: str | None = None
    session_status: str | None = None
    observed_endpoints: list[str] = []
    try:
        files = dependencies.inspect_files()
        _validate_protected_files(files)
        now = dependencies.clock()
        if isinstance(now, bool) or not isinstance(now, (int, float)):
            raise _failure("IDENTITY_FILE_INVALID", "IDENTITY")
        now = float(now)
        if not math.isfinite(now) or now < 0:
            raise _failure("IDENTITY_FILE_INVALID", "IDENTITY")
        identity = dependencies.read_identity()
        wallet, signer = _validate_identity(identity, now)
        fingerprint = "sha256:" + hashlib.sha256(
            ("RISEx-owner-account-v1:" + wallet).encode("ascii")
        ).hexdigest()

        if dependencies.read_session_signer is not None:
            observed_signer = _normalize_address(
                dependencies.read_session_signer(), "SESSION_SIGNER_IDENTITY_MISMATCH"
            )
            if observed_signer != signer:
                raise _failure("SESSION_SIGNER_IDENTITY_MISMATCH", "IDENTITY")

        transport = dependencies.transport_factory()
        observed_endpoints.append(DOMAIN_PATH)
        domain_observation = await _request_with_transport_retry(
            transport,
            "GET",
            DOMAIN_PATH,
        )
        _parse_domain(domain_observation.body)

        observed_endpoints.append(SESSION_KEY_STATUS_PATH)
        status_observation = await _request_with_transport_retry(
            transport,
            "GET",
            SESSION_KEY_STATUS_PATH,
            query=(("account", wallet), ("signer", signer)),
        )
        session_status = _validate_session_status(
            status_observation.body,
            expected_account=wallet,
            expected_signer=signer,
        )

        observed_endpoints.append(NONCE_PATH)
        nonce_observation = await _request_with_transport_retry(
            transport,
            "GET",
            NONCE_PATH,
            query=(("account", wallet),),
        )
        nonce_wire, nonce = _parse_nonce(nonce_observation.body)

        capability = dependencies.owner_capability_factory()
        owner_wallet = _normalize_address(
            capability.wallet_address(), "OWNER_KEY_IDENTITY_MISMATCH"
        )
        if owner_wallet != wallet:
            raise _failure("OWNER_KEY_IDENTITY_MISMATCH", "IDENTITY")
        deadline = int(now) + 300
        if deadline <= 0 or deadline > _UINT32_MAX:
            raise _failure("LOGIN_SIGNATURE_INVALID", "SAFETY")
        typed_data = _login_typed_data(wallet, nonce, deadline)
        signature = capability.sign_login(typed_data)
        if not _valid_signature(signature):
            raise _failure("LOGIN_SIGNATURE_INVALID", "SAFETY")
        if _recover_login(typed_data, signature) != wallet:
            raise _failure("LOGIN_SIGNATURE_IDENTITY_MISMATCH", "IDENTITY")

        observed_endpoints.append(LOGIN_PATH)
        login_observation = await _request_with_transport_retry(
            transport,
            "POST",
            LOGIN_PATH,
            body={
                "account": wallet,
                "nonce": nonce_wire,
                "deadline": deadline,
                "signature": signature,
            },
        )
        tokens = _parse_login_response(login_observation.body)
        access_token = tokens.access_token

        observed_endpoints.append(FEES_PATH)
        fees_observation = await _request_with_transport_retry(
            transport,
            "GET",
            FEES_PATH,
            bearer_token=access_token,
        )
        parsed_fees = _parse_fees(fees_observation.body)
        return _report(
            status=READY,
            terminal_classification="COMPLETE",
            reason="FEE_READ_COMPLETE",
            clock=dependencies.clock,
            observed_endpoints=observed_endpoints,
            account_fingerprint=fingerprint,
            session_signer_status=session_status,
            **parsed_fees,
        )
    except asyncio.CancelledError:
        raise
    except _FeeReadFailure as error:
        return _report(
            status=BLOCKED,
            terminal_classification=error.failure_class,
            reason=error.reason,
            clock=dependencies.clock,
            observed_endpoints=observed_endpoints,
            account_fingerprint=fingerprint,
            session_signer_status=session_status,
        )
    except Exception:
        return _report(
            status=BLOCKED,
            terminal_classification="SAFETY",
            reason="UNCLASSIFIED_EXCEPTION",
            clock=dependencies.clock,
            observed_endpoints=observed_endpoints,
            account_fingerprint=fingerprint,
            session_signer_status=session_status,
        )
    finally:
        access_token = None
        if capability is not None:
            try:
                capability.close()
            except Exception:
                pass
        if transport is not None:
            try:
                await transport.close()
            except Exception:
                pass


def _directory_open_flags() -> int:
    directory = getattr(os, "O_DIRECTORY", 0)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    if not directory or not nofollow:
        raise _failure("PROTECTED_PATH_INVALID", "SAFETY")
    return os.O_RDONLY | directory | nofollow | getattr(os, "O_CLOEXEC", 0)


def _fixed_components(path: Path) -> tuple[str, ...]:
    if not isinstance(path, Path) or not path.is_absolute():
        raise _failure("PROTECTED_PATH_INVALID", "SAFETY")
    parts = path.parts
    if not parts or parts[0] != os.sep or any(part in {"", ".", ".."} for part in parts[1:]):
        raise _failure("PROTECTED_PATH_INVALID", "SAFETY")
    return parts[1:]


def _validate_directory_fd(fd: int, *, final: bool) -> None:
    try:
        info = os.fstat(fd)
    except OSError:
        raise _failure("PROTECTED_PATH_INVALID", "SAFETY") from None
    if not stat.S_ISDIR(info.st_mode):
        raise _failure("PROTECTED_PATH_INVALID", "SAFETY")
    if info.st_uid not in {0, os.getuid()}:
        raise _failure("PROTECTED_PATH_INVALID", "SAFETY")
    if final and stat.S_IMODE(info.st_mode) != _DIRECTORY_MODE:
        raise _failure("PROTECTED_PATH_INVALID", "SAFETY")


def _walk_directory(*, create_missing: bool) -> int:
    components = _fixed_components(PROTECTED_DIRECTORY)
    flags = _directory_open_flags()
    try:
        current = os.open(os.sep, flags)
    except OSError:
        raise _failure("PROTECTED_PATH_INVALID", "SAFETY") from None
    try:
        _validate_directory_fd(current, final=False)
        for index, component in enumerate(components):
            created = False
            try:
                child = os.open(component, flags, dir_fd=current)
            except OSError as error:
                if error.errno != errno.ENOENT or not create_missing:
                    raise _failure("PROTECTED_PATH_INVALID", "SAFETY") from None
                try:
                    os.mkdir(component, _DIRECTORY_MODE, dir_fd=current)
                    created = True
                    child = os.open(component, flags, dir_fd=current)
                except (OSError, FileExistsError):
                    raise _failure("PROTECTED_PATH_INVALID", "SAFETY") from None
            try:
                if created:
                    os.fchmod(child, _DIRECTORY_MODE)
                _validate_directory_fd(child, final=index == len(components) - 1)
            except Exception:
                os.close(child)
                raise
            os.close(current)
            current = child
        result = current
        current = -1
        return result
    finally:
        if current != -1:
            os.close(current)


def _path_limit(filename: str) -> int:
    if filename == SESSION_KEY_FILENAME:
        return _MAX_SESSION_KEY_BYTES
    if filename == IDENTITY_FILENAME:
        return _MAX_IDENTITY_BYTES
    return _MAX_IDENTITY_BYTES


def _state_for(directory_fd: int | None, filename: str) -> ProtectedPathState:
    path = PROTECTED_DIRECTORY / filename
    if directory_fd is None:
        return ProtectedPathState(filename, str(path), False, False, "DIRECTORY_UNAVAILABLE")
    try:
        info = os.stat(filename, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return ProtectedPathState(filename, str(path), False, False, "FILE_MISSING")
    except OSError:
        return ProtectedPathState(filename, str(path), False, False, "FILE_UNREADABLE")
    reason = "FILE_OK"
    if stat.S_ISLNK(info.st_mode):
        reason = "FILE_SYMLINK"
    elif not stat.S_ISREG(info.st_mode):
        reason = "FILE_NOT_REGULAR"
    elif info.st_uid != os.getuid():
        reason = "FILE_OWNER_INVALID"
    elif info.st_nlink != 1:
        reason = "FILE_HARDLINK"
    elif stat.S_IMODE(info.st_mode) != _FILE_MODE:
        reason = "FILE_MODE_INVALID"
    elif info.st_size <= 0 or info.st_size > _path_limit(filename):
        reason = "FILE_SIZE_INVALID"
    elif filename == SESSION_KEY_FILENAME and info.st_size != _MAX_SESSION_KEY_BYTES:
        reason = "SESSION_KEY_SIZE_INVALID"
    return ProtectedPathState(
        filename,
        str(path),
        True,
        reason == "FILE_OK",
        reason,
        info.st_size,
    )


def inspect_protected_files() -> ProtectedFiles:
    directory_fd: int | None = None
    try:
        directory_fd = _walk_directory(create_missing=False)
    except _FeeReadFailure:
        pass
    try:
        return ProtectedFiles(
            session_key=_state_for(directory_fd, SESSION_KEY_FILENAME),
            identity=_state_for(directory_fd, IDENTITY_FILENAME),
            registration_intent=_state_for(directory_fd, REGISTRATION_INTENT_FILENAME),
            registration_spent=_state_for(directory_fd, REGISTRATION_SPENT_FILENAME),
        )
    finally:
        if directory_fd is not None:
            os.close(directory_fd)


def _read_fixed_file(filename: str, maximum: int) -> bytearray:
    directory_fd = _walk_directory(create_missing=False)
    descriptor: int | None = None
    raw = bytearray()
    try:
        descriptor = os.open(
            filename,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
            dir_fd=directory_fd,
        )
        initial = os.fstat(descriptor)
        state = _state_for(directory_fd, filename)
        if not state.protected or initial.st_size > maximum:
            raise _failure("PROTECTED_PATH_INVALID", "SAFETY")
        while True:
            chunk = os.read(descriptor, maximum + 1 - len(raw))
            if not chunk:
                break
            raw.extend(chunk)
            if len(raw) > maximum:
                raise _failure("PROTECTED_PATH_INVALID", "SAFETY")
        final = os.fstat(descriptor)
        if final.st_size != len(raw) or final.st_ino != initial.st_ino or final.st_dev != initial.st_dev:
            raise _failure("PROTECTED_PATH_INVALID", "SAFETY")
        return raw
    except _FeeReadFailure:
        _zeroize(raw)
        raise
    except Exception:
        _zeroize(raw)
        raise _failure("PROTECTED_PATH_INVALID", "SAFETY") from None
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(directory_fd)


def _parse_identity_bytes(raw: bytes | bytearray) -> ProvisionedIdentity:
    try:
        value = _strict_json(raw)
        if not isinstance(value, Mapping):
            raise ValueError
        required = {
            "chain_id",
            "environment",
            "expiration",
            "registration_status",
            "schema_version",
            "session_signer_address",
            "venue",
            "verifying_contract",
            "wallet_address",
        }
        if set(value) != required:
            raise ValueError
        if (
            value["chain_id"] != MAINNET_CHAIN_ID
            or value["environment"] != "MAINNET"
            or value["registration_status"] != "NOT_PREPARED"
            or value["schema_version"] != 1
            or value["venue"] != "RISEx"
            or str(value["verifying_contract"]).lower() != MAINNET_AUTH_CONTRACT
        ):
            raise ValueError
        wallet = _normalize_address(value["wallet_address"], "IDENTITY_FILE_INVALID")
        signer = _normalize_address(
            value["session_signer_address"], "IDENTITY_FILE_INVALID"
        )
        expiration = _strict_uint(
            value["expiration"], maximum=_UINT32_MAX, reason="IDENTITY_FILE_INVALID"
        )
        if wallet == signer or expiration == 0:
            raise ValueError
        return ProvisionedIdentity(wallet, signer, expiration)
    except _FeeReadFailure:
        raise _failure("IDENTITY_FILE_INVALID", "IDENTITY") from None
    except Exception:
        raise _failure("IDENTITY_FILE_INVALID", "IDENTITY") from None


def read_provisioned_identity() -> ProvisionedIdentity:
    raw = _read_fixed_file(IDENTITY_FILENAME, _MAX_IDENTITY_BYTES)
    try:
        return _parse_identity_bytes(raw)
    finally:
        _zeroize(raw)


def read_session_signer_address() -> str:
    raw = _read_fixed_file(SESSION_KEY_FILENAME, _MAX_SESSION_KEY_BYTES)
    try:
        return _derive_address(raw)
    except _FeeReadFailure:
        raise _failure("SESSION_SIGNER_IDENTITY_MISMATCH", "IDENTITY") from None
    finally:
        _zeroize(raw)


def _production_dependencies() -> _Dependencies:
    return _Dependencies(
        inspect_files=inspect_protected_files,
        read_identity=read_provisioned_identity,
        read_session_signer=read_session_signer_address,
        owner_capability_factory=lambda: _OwnerLoginCapability(getpass.getpass),
        transport_factory=FixedRisexFeeReadTransport,
        clock=time.time,
    )


async def run_fee_read() -> FeeReadReport:
    """Run one fixed, caller-owned Level-B fee read."""

    return await _run_with_dependencies(_production_dependencies())


def main(argv: Sequence[str] | None = None) -> int:
    """Print one sanitized report and reject every process argument."""

    args = list(sys.argv[1:] if argv is None else argv)
    if args:
        report = _report(
            status=BLOCKED,
            terminal_classification="SAFETY",
            reason="ARGUMENTS_REJECTED",
            clock=time.time,
        )
    else:
        try:
            report = asyncio.run(run_fee_read())
        except Exception:
            report = _report(
                status=BLOCKED,
                terminal_classification="SAFETY",
                reason="UNCLASSIFIED_EXCEPTION",
                clock=time.time,
            )
    print(report.evidence())
    return 0 if report.status == READY else 2


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ALLOWED_ENDPOINTS",
    "BLOCKED",
    "DOMAIN_PATH",
    "FEES_PATH",
    "FeeReadReport",
    "FixedRisexFeeReadTransport",
    "IDENTITY_FILENAME",
    "LOGIN_PATH",
    "LIGHTER_STANDARD_INPUTS",
    "MAINNET_AUTH_CONTRACT",
    "MAINNET_CHAIN_ID",
    "MAINNET_DOMAIN_NAME",
    "MAINNET_DOMAIN_VERSION",
    "MAINNET_REST_ORIGIN",
    "NONCE_PATH",
    "OFFICIAL_SOURCES",
    "PROTECTED_DIRECTORY",
    "ProvisionedIdentity",
    "REGISTRATION_INTENT_FILENAME",
    "REGISTRATION_SPENT_FILENAME",
    "READY",
    "SESSION_KEY_FILENAME",
    "SESSION_KEY_STATUS_PATH",
    "_Dependencies",
    "_FeeReadFailure",
    "_OwnerLoginCapability",
    "_login_typed_data",
    "_parse_fees",
    "_run_with_dependencies",
    "_strict_json",
    "main",
    "run_fee_read",
]
