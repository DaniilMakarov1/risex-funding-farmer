"""One-shot, explicitly bound RISEx mainnet session-signer registration.

This module is outside the normal Farmer import graph.  It is the narrow
operational boundary for one named registration: it reads the protected
identity and session key, obtains the official domain/nonce/signer
observations, persists a fresh runtime identity plus a distinct write intent
before signing, and permits one registration POST.

The main-wallet key is accepted only through the hidden local prompt, used to
produce the account signature, and zeroized from its local bytearray.  A
claimed intent is never re-armed; an ambiguous dispatch is reconciled by reads
only.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum
import getpass
import json
import os
import ssl
import sys
import time
from typing import Any, Callable, Mapping

import aiohttp
from yarl import URL

from . import risex_mainnet_onboarding as _onboarding


MAINNET_REST_ORIGIN = URL("https://api.rise.trade")
MAINNET_CHAIN_ID = 4153
MAINNET_AUTH_CONTRACT = "0x0d919daa3f12ae715744eb648c00066c5dbd66f0"
AUTHORIZED_WALLET = "0xb13c2bbe1f07f58efbbbdf86d948b49da2e0a56f"
AUTHORIZED_SESSION_SIGNER = "0x9c904d9145a45fbe2d5645cd9226def5efc9c5de"
AUTHORIZED_EXPIRATION = 1790689990
DOMAIN_NAME = "RISEx"
DOMAIN_VERSION = "1"
REGISTER_MESSAGE = "RISEx session key"

DOMAIN_PATH = "/v1/auth/eip712-domain"
NONCE_STATE_PATH = "/v1/nonce-state/"
SIGNERS_PATH = "/v1/auth/signers"
REGISTER_PATH = "/v1/auth/register-signer"

_MAX_RESPONSE_BYTES = 1_048_576
_TIMEOUT_SECONDS = 15
_MAX_NONCE_ANCHOR = 2**48 - 1
_MAX_NONCE_BITMAP_INDEX = 207
_FULL_NONCE_BITMAP_INDEX = 208
_FULL_NONCE_BITMAP_MASK = (1 << (_MAX_NONCE_BITMAP_INDEX + 1)) - 1
_MAX_UINT32 = 2**32 - 1
_PROMPT = "RISEx main-wallet private key (hidden; used once, not persisted): "

REGISTERED = "REGISTERED"
BLOCKED = "BLOCKED"
SPENT_UNKNOWN = "SPENT_UNKNOWN"
ONE_EXACT_REGISTRATION = "ONE_EXACT_RISEX_SESSION_SIGNER_REGISTRATION"

_FAILURE_CLASSES = frozenset({
    "TRANSPORT", "HTTP", "SCHEMA", "AUTH", "IDENTITY", "SAFETY",
})
_SAFE_REASONS = frozenset({
    "COMPLETE",
    "ALREADY_REGISTERED",
    "PROTECTED_CREDENTIALS_UNAVAILABLE",
    "PROTECTED_PATH_INVALID",
    "IDENTITY_BINDING_MISMATCH",
    "DOMAIN_MISMATCH",
    "NONCE_STATE_INVALID",
    "NONCE_INDEX_ALREADY_USED",
    "NONCE_BITMAP_FULL_INVALID",
    "NONCE_CHANGED_BEFORE_CLAIM",
    "REGISTRATION_INTENT_ALREADY_EXISTS",
    "REGISTRATION_INTENT_INVALID",
    "REGISTRATION_RUNTIME_ID_FAILED",
    "REGISTRATION_IDENTITY_COLLISION",
    "RUNTIME_ID_MISMATCH",
    "MAIN_KEY_INVALID",
    "SESSION_KEY_IDENTITY_MISMATCH",
    "SIGNATURE_INVALID",
    "SIGNATURE_RECOVERY_MISMATCH",
    "CLAIM_FAILED",
    "REGISTRATION_INTENT_ALREADY_SPENT",
    "POST_REJECTED",
    "POST_RESPONSE_INVALID",
    "POST_RECONCILIATION_FAILED",
    "POST_RECONCILIATION_UNAVAILABLE",
    "NONCE_RECONCILIATION_FAILED",
    "RECONCILED_AFTER_AMBIGUOUS_POST",
    "PROTECTED_INPUT_CANCELLED",
    "PROTECTED_INPUT_UNAVAILABLE",
    "ARGUMENTS_REJECTED",
    "OPERATION_FAILED",
})


@dataclass(frozen=True)
class AuthorizationBinding:
    environment: str
    wallet_address: str
    session_signer_address: str
    chain_id: int
    verifying_contract: str
    expiration: int
    message: str


AUTHORIZED_BINDING = AuthorizationBinding(
    environment="MAINNET",
    wallet_address=AUTHORIZED_WALLET,
    session_signer_address=AUTHORIZED_SESSION_SIGNER,
    chain_id=MAINNET_CHAIN_ID,
    verifying_contract=MAINNET_AUTH_CONTRACT,
    expiration=AUTHORIZED_EXPIRATION,
    message=REGISTER_MESSAGE,
)


class RegistrationState(str, Enum):
    REGISTERED = REGISTERED
    BLOCKED = BLOCKED
    SPENT_UNKNOWN = SPENT_UNKNOWN


@dataclass(frozen=True)
class RegistrationReport:
    state: RegistrationState
    reason: str
    failure_class: str | None = None
    wallet_address: str | None = AUTHORIZED_WALLET
    session_signer_address: str | None = AUTHORIZED_SESSION_SIGNER
    chain_id: int = MAINNET_CHAIN_ID
    expiration: int = AUTHORIZED_EXPIRATION
    runtime_id: str | None = None
    intent_id: str | None = None
    observed_nonce_anchor: int | None = None
    observed_nonce_bitmap_index: int | None = None
    post_nonce_anchor: int | None = None
    post_nonce_bitmap_index: int | None = None
    dispatch_count: int = 0
    reconciliation: str = "NOT_ATTEMPTED"

    def as_dict(self) -> dict[str, Any]:
        """Return only sanitized identity and bounded operational evidence."""

        return {
            "chain_id": self.chain_id,
            "dispatch_count": self.dispatch_count,
            "environment": "MAINNET",
            "expiration": self.expiration,
            "failure_class": self.failure_class,
            "intent_id": self.intent_id,
            "mainnet_write_scope": ONE_EXACT_REGISTRATION,
            "observed_nonce_anchor": self.observed_nonce_anchor,
            "observed_nonce_bitmap_index": self.observed_nonce_bitmap_index,
            "post_nonce_anchor": self.post_nonce_anchor,
            "post_nonce_bitmap_index": self.post_nonce_bitmap_index,
            "reconciliation": self.reconciliation,
            "reason": self.reason,
            "runtime_id": self.runtime_id,
            "session_signer_address": self.session_signer_address,
            "state": self.state.value,
            "wallet_address": self.wallet_address,
        }

    def evidence(self) -> str:
        return json.dumps(self.as_dict(), sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class NonceObservation:
    anchor: int
    current_bitmap_index: int
    bitmap: int

    @property
    def signed_anchor(self) -> int:
        if self.current_bitmap_index == _FULL_NONCE_BITMAP_INDEX:
            return self.anchor + 1
        return self.anchor

    @property
    def signed_bitmap_index(self) -> int:
        if self.current_bitmap_index == _FULL_NONCE_BITMAP_INDEX:
            return 0
        return self.current_bitmap_index


@dataclass(frozen=True)
class SignerObservation:
    active: bool
    expiration: int | None = None
    status: str | None = None


@dataclass(frozen=True)
class HttpObservation:
    status: int
    url: str
    body: Any


class _RegistrationFailure(Exception):
    """Sanitized operational failure; never carries venue-controlled text."""

    def __init__(self, reason: str, failure_class: str) -> None:
        if reason not in _SAFE_REASONS or failure_class not in _FAILURE_CLASSES:
            reason, failure_class = "OPERATION_FAILED", "SAFETY"
        self.reason = reason
        self.failure_class = failure_class
        super().__init__(reason)


def _failure(reason: str, failure_class: str = "SAFETY") -> _RegistrationFailure:
    return _RegistrationFailure(reason, failure_class)


def _identity_values(identity: Any) -> tuple[str, str, int]:
    return (
        getattr(identity, "wallet_address", AUTHORIZED_WALLET),
        getattr(identity, "session_signer_address", AUTHORIZED_SESSION_SIGNER),
        getattr(identity, "expiration", AUTHORIZED_EXPIRATION),
    )


def _blocked_report(
    reason: str,
    *,
    failure_class: str = "SAFETY",
    identity: Any = None,
    intent: Any = None,
    intent_id: str | None = None,
    runtime_id: str | None = None,
    nonce: NonceObservation | None = None,
    dispatch_count: int = 0,
    reconciliation: str = "NOT_ATTEMPTED",
) -> RegistrationReport:
    wallet, signer, expiration = _identity_values(identity)
    if intent is not None:
        intent_id = getattr(intent, "intent_id", intent_id)
        runtime_id = getattr(intent, "runtime_id", runtime_id)
    return RegistrationReport(
        state=RegistrationState.BLOCKED,
        reason=reason if reason in _SAFE_REASONS else "OPERATION_FAILED",
        failure_class=failure_class if failure_class in _FAILURE_CLASSES else "SAFETY",
        wallet_address=wallet,
        session_signer_address=signer,
        expiration=expiration,
        runtime_id=runtime_id,
        intent_id=intent_id,
        observed_nonce_anchor=None if nonce is None else nonce.anchor,
        observed_nonce_bitmap_index=None if nonce is None else nonce.current_bitmap_index,
        dispatch_count=dispatch_count,
        reconciliation=reconciliation,
    )


def _spent_report(
    reason: str,
    *,
    identity: Any,
    intent: Any,
    failure_class: str = "SAFETY",
    post_nonce: NonceObservation | None = None,
    dispatch_count: int = 1,
    reconciliation: str = "REQUIRED",
) -> RegistrationReport:
    wallet, signer, expiration = _identity_values(identity)
    return RegistrationReport(
        state=RegistrationState.SPENT_UNKNOWN,
        reason=reason if reason in _SAFE_REASONS else "OPERATION_FAILED",
        failure_class=failure_class if failure_class in _FAILURE_CLASSES else "SAFETY",
        wallet_address=wallet,
        session_signer_address=signer,
        expiration=expiration,
        runtime_id=getattr(intent, "runtime_id", None),
        intent_id=getattr(intent, "intent_id", None),
        observed_nonce_anchor=getattr(intent, "observed_nonce_anchor", None),
        observed_nonce_bitmap_index=getattr(intent, "observed_bitmap_index", None),
        post_nonce_anchor=None if post_nonce is None else post_nonce.anchor,
        post_nonce_bitmap_index=None if post_nonce is None else post_nonce.current_bitmap_index,
        dispatch_count=dispatch_count,
        reconciliation=reconciliation,
    )


def _registered_report(
    reason: str,
    *,
    identity: Any,
    intent: Any = None,
    post_nonce: NonceObservation | None = None,
    dispatch_count: int = 0,
    reconciliation: str = "PROVEN",
) -> RegistrationReport:
    wallet, signer, expiration = _identity_values(identity)
    return RegistrationReport(
        state=RegistrationState.REGISTERED,
        reason=reason if reason in _SAFE_REASONS else "OPERATION_FAILED",
        wallet_address=wallet,
        session_signer_address=signer,
        expiration=expiration,
        runtime_id=None if intent is None else getattr(intent, "runtime_id", None),
        intent_id=None if intent is None else getattr(intent, "intent_id", None),
        observed_nonce_anchor=None if intent is None else intent.observed_nonce_anchor,
        observed_nonce_bitmap_index=None if intent is None else intent.observed_bitmap_index,
        post_nonce_anchor=None if post_nonce is None else post_nonce.anchor,
        post_nonce_bitmap_index=None if post_nonce is None else post_nonce.current_bitmap_index,
        dispatch_count=dispatch_count,
        reconciliation=reconciliation,
    )


def _strict_json(raw: bytes) -> Any:
    def reject_constant(_value: str) -> Any:
        raise ValueError

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError
            result[key] = value
        return result

    try:
        return json.loads(
            raw.decode("utf-8", errors="strict"),
            parse_constant=reject_constant,
            object_pairs_hook=unique_object,
        )
    except Exception:
        raise _failure("POST_RESPONSE_INVALID", "SCHEMA") from None


def _normalize_address(value: object) -> str:
    try:
        return _onboarding._normalize_address(value)
    except Exception:
        raise _failure("IDENTITY_BINDING_MISMATCH", "IDENTITY") from None


def _unsigned(value: object, maximum: int, reason: str) -> int:
    if type(value) is int:
        parsed = value
    elif (
        type(value) is str
        and value
        and value == value.strip()
        and len(value) <= 78
        and all(character in "0123456789" for character in value)
    ):
        try:
            parsed = int(value, 10)
        except ValueError:
            raise _failure(reason, "SCHEMA") from None
    else:
        raise _failure(reason, "SCHEMA")
    if not 0 <= parsed <= maximum:
        raise _failure(reason, "SAFETY")
    return parsed


def _nonce_bitmap(value: object) -> int:
    if type(value) is not str or not value.startswith("0x"):
        raise _failure("NONCE_STATE_INVALID", "SCHEMA")
    digits = value[2:]
    if not 1 <= len(digits) <= 64 or any(
        character not in "0123456789abcdefABCDEF" for character in digits
    ):
        raise _failure("NONCE_STATE_INVALID", "SCHEMA")
    parsed = int(digits, 16)
    if parsed >= 2**256:
        raise _failure("NONCE_STATE_INVALID", "SAFETY")
    return parsed


def _data_envelope(value: object) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _failure("POST_RESPONSE_INVALID", "SCHEMA")
    if "data" not in value or "request_id" not in value:
        raise _failure("POST_RESPONSE_INVALID", "SCHEMA")
    if type(value["request_id"]) is not str or not value["request_id"]:
        raise _failure("POST_RESPONSE_INVALID", "SCHEMA")
    data = value["data"]
    if not isinstance(data, Mapping):
        raise _failure("POST_RESPONSE_INVALID", "SCHEMA")
    return data


def _parse_domain(value: object) -> None:
    data = _data_envelope(value)
    required = {"name", "version", "chain_id", "verifying_contract"}
    if not required <= set(data):
        raise _failure("DOMAIN_MISMATCH", "SCHEMA")
    try:
        verifier = _normalize_address(data["verifying_contract"])
    except _RegistrationFailure:
        raise _failure("DOMAIN_MISMATCH", "IDENTITY") from None
    if (
        data["name"] != DOMAIN_NAME
        or data["version"] != DOMAIN_VERSION
        or _unsigned(data["chain_id"], MAINNET_CHAIN_ID, "DOMAIN_MISMATCH")
        != MAINNET_CHAIN_ID
        or verifier != MAINNET_AUTH_CONTRACT
    ):
        raise _failure("DOMAIN_MISMATCH", "IDENTITY")


def _parse_nonce(value: object) -> NonceObservation:
    data = _data_envelope(value)
    required = {"nonce_anchor", "current_bitmap_index", "bitmap"}
    if not required <= set(data):
        raise _failure("NONCE_STATE_INVALID", "SCHEMA")
    anchor = _unsigned(data["nonce_anchor"], _MAX_NONCE_ANCHOR, "NONCE_STATE_INVALID")
    index = _unsigned(
        data["current_bitmap_index"],
        _FULL_NONCE_BITMAP_INDEX,
        "NONCE_STATE_INVALID",
    )
    bitmap = _nonce_bitmap(data["bitmap"])
    if index == _FULL_NONCE_BITMAP_INDEX:
        if anchor >= _MAX_NONCE_ANCHOR or (
            bitmap & _FULL_NONCE_BITMAP_MASK
        ) != _FULL_NONCE_BITMAP_MASK:
            raise _failure("NONCE_BITMAP_FULL_INVALID", "SAFETY")
    elif bitmap & (1 << index):
        raise _failure("NONCE_INDEX_ALREADY_USED", "SAFETY")
    return NonceObservation(anchor, index, bitmap)


def _expected_post_nonce(observed: NonceObservation) -> NonceObservation:
    if observed.current_bitmap_index > _FULL_NONCE_BITMAP_INDEX:
        raise _failure("NONCE_STATE_INVALID", "SAFETY")
    selected = 1 << observed.signed_bitmap_index
    if observed.current_bitmap_index < _MAX_NONCE_BITMAP_INDEX:
        return NonceObservation(
            observed.anchor,
            observed.current_bitmap_index + 1,
            observed.bitmap | selected,
        )
    if observed.current_bitmap_index == _MAX_NONCE_BITMAP_INDEX:
        return NonceObservation(
            observed.anchor,
            _FULL_NONCE_BITMAP_INDEX,
            observed.bitmap | selected,
        )
    return NonceObservation(observed.anchor + 1, 1, 1)


def _nonce_consumed(observed: NonceObservation, current: NonceObservation) -> bool:
    return current == _expected_post_nonce(observed)


def _parse_signers(value: object) -> SignerObservation:
    data = _data_envelope(value)
    signers = data.get("signers")
    if not isinstance(signers, list):
        raise _failure("POST_RESPONSE_INVALID", "SCHEMA")
    match: Mapping[str, Any] | None = None
    for row in signers:
        if not isinstance(row, Mapping) or "signer" not in row:
            raise _failure("POST_RESPONSE_INVALID", "SCHEMA")
        signer = _normalize_address(row["signer"])
        if signer != AUTHORIZED_SESSION_SIGNER:
            continue
        if match is not None:
            raise _failure("IDENTITY_BINDING_MISMATCH", "IDENTITY")
        match = row
    if match is None:
        return SignerObservation(False)
    if "status" not in match or "expiration" not in match:
        raise _failure("POST_RESPONSE_INVALID", "SCHEMA")
    status = match["status"]
    if type(status) is not str or status not in {"Active", "Expired", "Revoked"}:
        raise _failure("POST_RESPONSE_INVALID", "SCHEMA")
    expiration = _unsigned(match["expiration"], _MAX_UINT32, "POST_RESPONSE_INVALID")
    if expiration != AUTHORIZED_EXPIRATION:
        raise _failure("IDENTITY_BINDING_MISMATCH", "IDENTITY")
    return SignerObservation(status == "Active", expiration, status)


def _parse_register_success(value: object) -> None:
    data = _data_envelope(value)
    if data.get("success") is not True:
        raise _failure("POST_REJECTED", "HTTP")
    if "transaction_hash" in data:
        transaction_hash = data["transaction_hash"]
        if type(transaction_hash) is not str:
            raise _failure("POST_RESPONSE_INVALID", "SCHEMA")
        if transaction_hash and (
            len(transaction_hash) != 66
            or not transaction_hash.startswith("0x")
            or any(character not in "0123456789abcdefABCDEF" for character in transaction_hash[2:])
        ):
            raise _failure("POST_RESPONSE_INVALID", "SCHEMA")


def _is_transport_exception(error: BaseException) -> bool:
    return isinstance(error, (
        aiohttp.ClientConnectionError,
        aiohttp.ClientPayloadError,
        asyncio.TimeoutError,
        ConnectionError,
        OSError,
    ))


async def _abort_redirect(_session: Any, _context: Any, _params: Any) -> None:
    raise _failure("POST_RESPONSE_INVALID", "SAFETY")


class FixedRisexMainnetRegistrationTransport:
    """Fixed-origin transport with no automatic retry or redirect behavior."""

    REST_ORIGIN = MAINNET_REST_ORIGIN
    TIMEOUT_SECONDS = _TIMEOUT_SECONDS

    def __init__(self) -> None:
        redirect_guard = aiohttp.TraceConfig()
        redirect_guard.on_request_redirect.append(_abort_redirect)
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=_TIMEOUT_SECONDS, connect=5, sock_read=10),
            trust_env=False,
            connector=aiohttp.TCPConnector(ssl=ssl.create_default_context()),
            trace_configs=[redirect_guard],
        )
        if hasattr(self._session, "_retry_connection"):
            self._session._retry_connection = False
        self._post_used = False

    @staticmethod
    def _target(path: str, query: tuple[tuple[str, str], ...] = ()) -> URL:
        if not path.startswith("/v1/") or "?" in path or "#" in path:
            raise _failure("POST_RESPONSE_INVALID", "SAFETY")
        return MAINNET_REST_ORIGIN.with_path(path).with_query(query)

    async def _request(
        self,
        method: str,
        path: str,
        *,
        query: tuple[tuple[str, str], ...] = (),
        body: Mapping[str, Any] | None = None,
    ) -> HttpObservation:
        method = method.upper()
        if method == "POST":
            if self._post_used:
                raise _failure("REGISTRATION_INTENT_ALREADY_SPENT", "SAFETY")
            self._post_used = True
        target = self._target(path, query)
        kwargs: dict[str, Any] = {
            "allow_redirects": False,
            "proxy": None,
            "ssl": ssl.create_default_context(),
        }
        if query:
            kwargs["params"] = query
        if body is not None:
            kwargs["json"] = dict(body)
        try:
            async with self._session.request(
                method, MAINNET_REST_ORIGIN.with_path(path), **kwargs
            ) as response:
                if response.history or str(response.url) != str(target):
                    raise _failure("POST_RESPONSE_INVALID", "SAFETY")
                if not 200 <= response.status < 300:
                    failure_class = "AUTH" if response.status in {401, 403} else "HTTP"
                    raise _failure("POST_REJECTED", failure_class)
                raw = await response.read()
                if len(raw) > _MAX_RESPONSE_BYTES:
                    raise _failure("POST_RESPONSE_INVALID", "SCHEMA")
                return HttpObservation(response.status, str(response.url), _strict_json(raw))
        except asyncio.CancelledError:
            raise
        except _RegistrationFailure:
            raise
        except Exception as error:
            if _is_transport_exception(error):
                raise _failure("POST_RECONCILIATION_UNAVAILABLE", "TRANSPORT") from None
            raise _failure("OPERATION_FAILED", "SAFETY") from None

    async def get(
        self, path: str, query: tuple[tuple[str, str], ...] = ()
    ) -> HttpObservation:
        return await self._request("GET", path, query=query)

    async def register(self, request: Mapping[str, Any]) -> HttpObservation:
        if set(request) != {
            "account", "signer", "message", "nonce_anchor", "nonce_bitmap_index",
            "expiration", "account_signature", "signer_signature",
        }:
            raise _failure("POST_RESPONSE_INVALID", "SAFETY")
        return await self._request("POST", REGISTER_PATH, body=request)

    async def close(self) -> None:
        await self._session.close()


class _RegistrationSigningCapability:
    """Closeable pair of local bytearrays used only for this registration."""

    def __init__(self, main_secret: bytearray, session_secret: bytearray) -> None:
        self._main_secret = main_secret
        self._session_secret = session_secret
        self._closed = False

    def _check(self) -> None:
        if self._closed:
            raise _failure("OPERATION_FAILED", "SAFETY")

    def wallet_address(self) -> str:
        self._check()
        try:
            return _onboarding._derive_address(self._main_secret)
        except Exception:
            raise _failure("MAIN_KEY_INVALID", "IDENTITY") from None

    def session_signer_address(self) -> str:
        self._check()
        try:
            return _onboarding._derive_address(self._session_secret)
        except Exception:
            raise _failure("SESSION_KEY_IDENTITY_MISMATCH", "IDENTITY") from None

    def sign_register(self, typed_data: Mapping[str, Any]) -> str:
        self._check()
        return _sign_typed_data(self._main_secret, typed_data)

    def sign_verify(self, typed_data: Mapping[str, Any]) -> str:
        self._check()
        return _sign_typed_data(self._session_secret, typed_data)

    def close(self) -> None:
        if self._closed:
            return
        _zeroize(self._main_secret)
        _zeroize(self._session_secret)
        self._closed = True


def _zeroize(secret: bytearray) -> None:
    for index in range(len(secret)):
        secret[index] = 0
    secret.clear()


def _open_session_secret(identity: Any) -> bytearray:
    directory_fd: int | None = None
    secret: bytearray | None = None
    try:
        directory_fd = _onboarding._open_directory()
        value = _onboarding._read_secure_file(
            directory_fd, _onboarding.SESSION_KEY_FILENAME
        )
        secret = bytearray(value)
        if _onboarding._derive_address(secret) != identity.session_signer_address:
            raise _failure("SESSION_KEY_IDENTITY_MISMATCH", "IDENTITY")
        result = secret
        secret = None
        return result
    except _RegistrationFailure:
        raise
    except Exception:
        raise _failure("PROTECTED_CREDENTIALS_UNAVAILABLE", "SAFETY") from None
    finally:
        if secret is not None:
            _zeroize(secret)
        if directory_fd is not None:
            os.close(directory_fd)


def _parse_main_secret(input_fn: Callable[[str], str]) -> bytearray:
    supplied: str | None = None
    try:
        try:
            supplied = input_fn(_PROMPT)
        except (EOFError, KeyboardInterrupt):
            raise _failure("PROTECTED_INPUT_CANCELLED", "SAFETY") from None
        except Exception:
            raise _failure("PROTECTED_INPUT_UNAVAILABLE", "SAFETY") from None
        try:
            return _onboarding._parse_main_private_key(supplied)
        except Exception:
            raise _failure("MAIN_KEY_INVALID", "IDENTITY") from None
    finally:
        supplied = None


def _sign_typed_data(secret: bytearray, typed_data: Mapping[str, Any]) -> str:
    try:
        from eth_account import Account
        from eth_account.messages import encode_typed_data

        signed = Account.sign_message(
            encode_typed_data(full_message=dict(typed_data)), bytes(secret)
        )
        signature = bytes(signed.signature)
        if len(signature) != 65:
            raise ValueError
        value = "0x" + signature.hex()
        if not _valid_signature(value):
            raise ValueError
        return value
    except Exception:
        raise _failure("SIGNATURE_INVALID", "SAFETY") from None


def _recover_typed_data(typed_data: Mapping[str, Any], signature: str) -> str:
    try:
        from eth_account import Account
        from eth_account.messages import encode_typed_data

        return Account.recover_message(
            encode_typed_data(full_message=dict(typed_data)), signature=signature
        ).lower()
    except Exception:
        raise _failure("SIGNATURE_RECOVERY_MISMATCH", "SAFETY") from None


def _valid_signature(value: object) -> bool:
    return (
        type(value) is str
        and len(value) == 132
        and value.startswith("0x")
        and all(character in "0123456789abcdefABCDEF" for character in value[2:])
    )


def _validate_identity(identity: Any) -> None:
    expected = AuthorizationBinding(
        environment="MAINNET",
        wallet_address=AUTHORIZED_WALLET,
        session_signer_address=AUTHORIZED_SESSION_SIGNER,
        chain_id=MAINNET_CHAIN_ID,
        verifying_contract=MAINNET_AUTH_CONTRACT,
        expiration=AUTHORIZED_EXPIRATION,
        message=REGISTER_MESSAGE,
    )
    if (
        AUTHORIZED_BINDING != expected
        or identity.environment != expected.environment
        or identity.wallet_address != expected.wallet_address
        or identity.session_signer_address != expected.session_signer_address
        or identity.chain_id != expected.chain_id
        or identity.verifying_contract != expected.verifying_contract
        or identity.expiration != expected.expiration
        or identity.registration_status != _onboarding.REGISTRATION_NOT_PREPARED
    ):
        raise _failure("IDENTITY_BINDING_MISMATCH", "IDENTITY")


def _validate_protected_files() -> Any:
    files = _onboarding.inspect_protected_files()
    if not files.all_required_protected:
        raise _failure("PROTECTED_CREDENTIALS_UNAVAILABLE", "SAFETY")
    for name in (
        _onboarding.REGISTRATION_INTENT_FILENAME,
        _onboarding.REGISTRATION_SPENT_FILENAME,
    ):
        state = files.for_name(name)
        if state.present and not state.protected:
            raise _failure("PROTECTED_PATH_INVALID", "SAFETY")
    return files


async def _read_with_one_transport_retry(effect: Callable[[], Any]) -> Any:
    for attempt in range(2):
        try:
            return await effect()
        except asyncio.CancelledError:
            raise
        except _RegistrationFailure as error:
            if error.failure_class == "TRANSPORT" and attempt == 0:
                continue
            raise
    raise _failure("POST_RECONCILIATION_UNAVAILABLE", "TRANSPORT")


async def _read_domain(transport: Any) -> None:
    observation = await _read_with_one_transport_retry(
        lambda: transport.get(DOMAIN_PATH)
    )
    _parse_domain(observation.body)


async def _read_nonce(transport: Any, wallet: str) -> NonceObservation:
    observation = await _read_with_one_transport_retry(
        lambda: transport.get(NONCE_STATE_PATH + wallet)
    )
    return _parse_nonce(observation.body)


async def _read_signers(transport: Any, wallet: str) -> SignerObservation:
    observation = await _read_with_one_transport_retry(
        lambda: transport.get(SIGNERS_PATH, (("account", wallet),))
    )
    return _parse_signers(observation.body)


async def _reconcile(
    transport: Any,
    identity: Any,
    intent: Any,
    *,
    post_failure: _RegistrationFailure | None = None,
    dispatch_count: int = 1,
) -> RegistrationReport:
    try:
        signers = await _read_signers(transport, identity.wallet_address)
        post_nonce = await _read_nonce(transport, identity.wallet_address)
    except asyncio.CancelledError:
        raise
    except _RegistrationFailure as error:
        return _spent_report(
            "POST_RECONCILIATION_UNAVAILABLE",
            identity=identity,
            intent=intent,
            failure_class=error.failure_class,
            dispatch_count=dispatch_count,
        )
    if not signers.active:
        return _spent_report(
            "POST_RECONCILIATION_FAILED",
            identity=identity,
            intent=intent,
            failure_class="IDENTITY",
            post_nonce=post_nonce,
            dispatch_count=dispatch_count,
            reconciliation="FAILED",
        )
    if not _nonce_consumed(
        NonceObservation(
            intent.observed_nonce_anchor,
            intent.observed_bitmap_index,
            intent.observed_bitmap,
        ),
        post_nonce,
    ):
        return _spent_report(
            "NONCE_RECONCILIATION_FAILED",
            identity=identity,
            intent=intent,
            failure_class="SAFETY",
            post_nonce=post_nonce,
            dispatch_count=dispatch_count,
            reconciliation="FAILED",
        )
    if post_failure is not None and post_failure.failure_class != "TRANSPORT":
        return _spent_report(
            post_failure.reason,
            identity=identity,
            intent=intent,
            failure_class=post_failure.failure_class,
            post_nonce=post_nonce,
            dispatch_count=dispatch_count,
            reconciliation="FAILED",
        )
    reason = (
        "RECONCILED_AFTER_AMBIGUOUS_POST" if post_failure is not None else "COMPLETE"
    )
    return _registered_report(
        reason,
        identity=identity,
        intent=intent,
        post_nonce=post_nonce,
        dispatch_count=dispatch_count,
        reconciliation="PROVEN",
    )


@dataclass(frozen=True)
class _Dependencies:
    input_fn: Callable[[str], str]
    transport_factory: Callable[[], Any]
    clock: Callable[[], float]


def _same_intent(left: Any, right: Any) -> bool:
    fields = (
        "intent_id",
        "runtime_id",
        "wallet_address",
        "session_signer_address",
        "expiration",
        "observed_nonce_anchor",
        "observed_bitmap_index",
        "observed_bitmap",
        "nonce_anchor",
        "nonce_bitmap_index",
    )
    return all(getattr(left, field, None) == getattr(right, field, None) for field in fields)


async def _run_with_dependencies(dependencies: _Dependencies) -> RegistrationReport:
    identity: Any = None
    capability: _RegistrationSigningCapability | None = None
    transport: Any = None
    intent: Any = None
    observed_nonce: NonceObservation | None = None
    session_secret: bytearray | None = None
    main_secret: bytearray | None = None
    dispatch_count = 0
    claim_started = False
    claimed = False
    try:
        try:
            _validate_protected_files()
            identity = _onboarding.read_provisioned_identity()
            _validate_identity(identity)
        except _RegistrationFailure as error:
            return _blocked_report(
                error.reason, failure_class=error.failure_class, identity=identity
            )
        except Exception:
            return _blocked_report("PROTECTED_PATH_INVALID", identity=identity)

        try:
            transport = dependencies.transport_factory()
            await _read_domain(transport)
            observed_nonce = await _read_nonce(transport, identity.wallet_address)

            existing_intent: Any = None
            files = _onboarding.inspect_protected_files()
            if files.registration_intent.present or files.registration_spent.present:
                try:
                    existing_intent = _onboarding.load_registration_intent()
                except Exception:
                    return _blocked_report(
                        "REGISTRATION_INTENT_INVALID",
                        failure_class="SAFETY",
                        identity=identity,
                        nonce=observed_nonce,
                    )
                if existing_intent.state == _onboarding.REGISTRATION_SPENT_UNKNOWN:
                    return await _reconcile(
                        transport,
                        identity,
                        existing_intent,
                        dispatch_count=1,
                    )
                return _blocked_report(
                    "REGISTRATION_INTENT_ALREADY_EXISTS",
                    identity=identity,
                    intent=existing_intent,
                    nonce=observed_nonce,
                )

            signers = await _read_signers(transport, identity.wallet_address)
            if signers.active:
                return _registered_report(
                    "ALREADY_REGISTERED", identity=identity, dispatch_count=0
                )

            session_secret = _open_session_secret(identity)
            try:
                main_secret = _parse_main_secret(dependencies.input_fn)
                capability = _RegistrationSigningCapability(main_secret, session_secret)
                main_secret = None
                session_secret = None
                if capability.wallet_address() != AUTHORIZED_WALLET:
                    raise _failure("IDENTITY_BINDING_MISMATCH", "IDENTITY")
                if capability.session_signer_address() != AUTHORIZED_SESSION_SIGNER:
                    raise _failure("SESSION_KEY_IDENTITY_MISMATCH", "IDENTITY")
                now = int(dependencies.clock())
                if not 0 < now < AUTHORIZED_EXPIRATION <= _MAX_UINT32:
                    raise _failure("IDENTITY_BINDING_MISMATCH", "SAFETY")

                intent = _onboarding.prepare_registration_intent(
                    nonce_anchor=observed_nonce.anchor,
                    current_bitmap_index=observed_nonce.current_bitmap_index,
                    bitmap="0x" + format(observed_nonce.bitmap, "x"),
                    allow_full_bitmap=True,
                )
                if not intent.runtime_id or intent.runtime_id == intent.intent_id:
                    raise _failure("RUNTIME_ID_MISMATCH", "SAFETY")
                register_typed = intent.typed_register_data
                verify_typed = intent.typed_verify_data
                account_signature = capability.sign_register(register_typed)
                signer_signature = capability.sign_verify(verify_typed)
                if (
                    _recover_typed_data(register_typed, account_signature)
                    != AUTHORIZED_WALLET
                    or _recover_typed_data(verify_typed, signer_signature)
                    != AUTHORIZED_SESSION_SIGNER
                ):
                    raise _failure("SIGNATURE_RECOVERY_MISMATCH", "SAFETY")
                request = _onboarding.build_register_signer_request(
                    intent, account_signature, signer_signature
                )
            finally:
                if main_secret is not None:
                    _zeroize(main_secret)
                    main_secret = None
                if session_secret is not None:
                    _zeroize(session_secret)
                    session_secret = None

            confirmed_nonce = await _read_nonce(transport, identity.wallet_address)
            if confirmed_nonce != observed_nonce:
                raise _failure("NONCE_CHANGED_BEFORE_CLAIM", "SAFETY")

            claim_started = True
            try:
                claimed_intent = _onboarding.claim_registration_intent()
            except _onboarding.OnboardingViolation as error:
                if error.reason == "REGISTRATION_INTENT_ALREADY_SPENT":
                    return _spent_report(
                        "REGISTRATION_INTENT_ALREADY_SPENT",
                        identity=identity,
                        intent=intent,
                        dispatch_count=0,
                    )
                raise _failure("CLAIM_FAILED", "SAFETY") from None
            if (
                not isinstance(claimed_intent, _onboarding.RegistrationIntent)
                or claimed_intent.state != _onboarding.REGISTRATION_SPENT_UNKNOWN
                or not _same_intent(claimed_intent, intent)
            ):
                raise _failure("CLAIM_FAILED", "SAFETY")
            claimed = True
            dispatch_count = 1

            post_failure: _RegistrationFailure | None = None
            try:
                response = await transport.register(request)
                _parse_register_success(response.body)
            except asyncio.CancelledError:
                raise
            except _RegistrationFailure as error:
                post_failure = error
            except Exception as error:
                post_failure = _failure(
                    "POST_RECONCILIATION_UNAVAILABLE"
                    if _is_transport_exception(error)
                    else "OPERATION_FAILED",
                    "TRANSPORT" if _is_transport_exception(error) else "SAFETY",
                )
            return await _reconcile(
                transport,
                identity,
                intent,
                post_failure=post_failure,
                dispatch_count=dispatch_count,
            )
        except asyncio.CancelledError:
            raise
        except _RegistrationFailure as error:
            if claimed or claim_started:
                return _spent_report(
                    error.reason,
                    identity=identity,
                    intent=intent,
                    failure_class=error.failure_class,
                    dispatch_count=dispatch_count,
                )
            return _blocked_report(
                error.reason,
                failure_class=error.failure_class,
                identity=identity,
                intent=intent,
                nonce=observed_nonce,
            )
        except Exception:
            if claimed or claim_started:
                return _spent_report(
                    "OPERATION_FAILED",
                    identity=identity,
                    intent=intent,
                    failure_class="SAFETY",
                    dispatch_count=dispatch_count,
                )
            return _blocked_report(
                "OPERATION_FAILED",
                identity=identity,
                intent=intent,
                nonce=observed_nonce,
            )
        finally:
            if transport is not None:
                try:
                    await transport.close()
                except Exception:
                    pass
    finally:
        if main_secret is not None:
            _zeroize(main_secret)
        if session_secret is not None:
            _zeroize(session_secret)
        if capability is not None:
            capability.close()


async def run() -> RegistrationReport:
    """Run the fixed one-shot mainnet registration operation."""

    return await _run_with_dependencies(
        _Dependencies(
            input_fn=getpass.getpass,
            transport_factory=FixedRisexMainnetRegistrationTransport,
            clock=time.time,
        )
    )


def _fixture_dependencies(
    *,
    input_fn: Callable[[str], str],
    transport_factory: Callable[[], Any],
    clock: Callable[[], float] = time.time,
) -> _Dependencies:
    """Test-only dependency seam; production ``run`` accepts no overrides."""

    return _Dependencies(input_fn, transport_factory, clock)


async def _run_fixture(dependencies: _Dependencies) -> RegistrationReport:
    return await _run_with_dependencies(dependencies)


def main() -> int:
    if len(sys.argv) != 1:
        report = _blocked_report("ARGUMENTS_REJECTED", failure_class="SAFETY")
        print(report.evidence())
        return 1
    try:
        report = asyncio.run(run())
    except KeyboardInterrupt:
        report = _blocked_report("PROTECTED_INPUT_CANCELLED", failure_class="SAFETY")
    except Exception:
        report = _blocked_report("OPERATION_FAILED", failure_class="SAFETY")
    print(report.evidence())
    return 0 if report.state is RegistrationState.REGISTERED else 1


__all__ = [
    "AUTHORIZED_BINDING",
    "AUTHORIZED_EXPIRATION",
    "AUTHORIZED_SESSION_SIGNER",
    "AUTHORIZED_WALLET",
    "BLOCKED",
    "DOMAIN_NAME",
    "DOMAIN_PATH",
    "DOMAIN_VERSION",
    "FixedRisexMainnetRegistrationTransport",
    "HttpObservation",
    "MAINNET_AUTH_CONTRACT",
    "MAINNET_CHAIN_ID",
    "MAINNET_REST_ORIGIN",
    "NonceObservation",
    "NONCE_STATE_PATH",
    "ONE_EXACT_REGISTRATION",
    "REGISTERED",
    "REGISTER_MESSAGE",
    "REGISTER_PATH",
    "RegistrationReport",
    "RegistrationState",
    "SIGNERS_PATH",
    "SPENT_UNKNOWN",
    "run",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
