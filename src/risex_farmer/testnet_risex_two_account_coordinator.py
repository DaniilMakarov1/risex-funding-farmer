"""Sealed, venue-local RISEx two-account ETH/USDC testnet coordinator.

The normal Farmer never imports this module.  The pure coordinator accepts only
injected observations and write effects, while the zero-argument production
entry point binds the two fixed testnet roles, protected credential files, and
the official REST/WebSocket surfaces.  The primary account and the one
counterparty each have an independent durable journal; a journal never stores
payloads, signatures, secrets, or raw venue responses.
"""

from __future__ import annotations

import asyncio
import base64
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from enum import Enum
import hashlib
import json
import math
import os
from pathlib import Path
import pwd
import secrets
import sqlite3
import stat
import ssl
import sys
import time
from typing import Any, Callable, Mapping, Protocol, Sequence
from urllib.parse import urlencode
import uuid

import aiohttp

from . import testnet_risex_signer as _signer
from .testnet_risex_order_lifecycle import (
    CANCEL_ACTION,
    HEADER_FLAGS,
    OFFICIAL_CHAIN_ID,
    OFFICIAL_DOMAIN_NAME,
    OFFICIAL_DOMAIN_VERSION,
    OFFICIAL_HOST,
    OFFICIAL_MARKET_ID,
    OFFICIAL_MARKET_MINIMUM,
    OFFICIAL_MARKET_STEP,
    OFFICIAL_MARKET_SYMBOL,
    OFFICIAL_MARKET_TICK,
    PLACE_ACTION,
    _address,
    _bound,
    _uint,
    _valid_order_id,
    encode_cancel_action,
    encode_place_action,
    pack_order_data,
    verify_witness_typed_data,
)
from .testnet_risex_private_read_preflight import (
    ACCOUNT as PRIMARY_ACCOUNT,
    AUTHORIZATION,
    CHAIN_ID,
    DOMAIN_NAME,
    DOMAIN_VERSION,
    PrivateReadPreflight,
    REST_ORIGIN,
    ROUTER,
    SIGNER as PRIMARY_SIGNER,
    WS_ORIGIN,
)
from .risex_private_read_operational import (
    _parse_auth_v2,
    _require_auth_v2_success,
    _validate_auth_v2_schema,
)


MAX_AGE_SECONDS = 5
MAX_PERMIT_SECONDS = 60
BOUND_FRACTION = Decimal("0.003")
MARKET_ID = OFFICIAL_MARKET_ID
MARKET_SYMBOL = OFFICIAL_MARKET_SYMBOL
MARKET_TICK = OFFICIAL_MARKET_TICK
MARKET_STEP = OFFICIAL_MARKET_STEP
MARKET_MINIMUM = OFFICIAL_MARKET_MINIMUM

# The counterparty account is intentionally not a selectable CLI/config value.
# Its fixed public account is derived from the one protected wallet source and
# its fixed public signer from the one protected session-key source.  The two
# independent journals are lifecycle/write-identity domains; the protected
# signer-registration JSON is the durable setup marker.  No extra role-record
# credential is required or invented here.
COUNTERPARTY_WALLET_KEY = ".risex-funding-farmer-risex-counterparty-wallet-v1.key"
COUNTERPARTY_SESSION_KEY = ".risex-funding-farmer-risex-counterparty-session-signer-v1.key"
COUNTERPARTY_SIGNER_MARKER = ".risex-funding-farmer-risex-counterparty-signer-registration-v1.json"
PRIMARY_JOURNAL = ".risex-funding-farmer-risex-two-account-primary-v1.sqlite3"
COUNTERPARTY_JOURNAL = ".risex-funding-farmer-risex-two-account-counterparty-v1.sqlite3"
COUNTERPARTY_ACCOUNT = "0xa2e5355fe89ae005054371b31ce8ccb5e1a18377"
COUNTERPARTY_SIGNER = "0x3ffe4d22ea3ced440576643efbeb8a315b0be7c4"
PLACE_PATH = "/v1/orders/place"
CANCEL_PATH = "/v1/orders/cancel"
HISTORY_PATH = "/v1/orders"
TRADES_PATH = "/v1/trade-history"
ORDER_LOOKUP_PATH_TEMPLATE = "/v1/orders/by-id/{order_id}"
POSITION_PATH = "/v1/account/position"
POSITIONS_PATH = "/v1/positions"
PORTFOLIO_PATH = "/v1/portfolio/details"
NONCE_PATH_TEMPLATE = "/v1/nonce-state/{account}"
MAX_RESPONSE_BYTES = 1_048_576
ADAPTER_DEADLINE_SECONDS = 5
PAGE_LIMIT = 100
MAX_PAGINATION_PAGES = 64
MAX_BASELINE_HISTORY_ROWS = 256
COUNTERPARTY_REGISTRATION_EXPIRATION = 1819294121


class CoordinatorSafetyError(RuntimeError):
    """Sanitized fail-closed coordinator rejection."""


class AccountRole(str, Enum):
    PRIMARY = "PRIMARY"
    COUNTERPARTY = "COUNTERPARTY"


class CoordinatorResult(str, Enum):
    COMPLETE = "COMPLETE"
    BLOCKED_BEFORE_WRITE = "BLOCKED_BEFORE_WRITE"
    FAILED_HALTED_MANUAL_RECOVERY = "FAILED_HALTED_MANUAL_RECOVERY"


class WriteResultClass(str, Enum):
    ACCEPTED = "ACCEPTED"
    TERMINAL_VENUE_REJECTION = "TERMINAL_VENUE_REJECTION"
    TRANSPORT_AMBIGUITY = "TRANSPORT_AMBIGUITY"
    RESPONSE_AMBIGUITY = "RESPONSE_AMBIGUITY"
    LOCAL_FAILURE = "LOCAL_FAILURE"


class Phase(str, Enum):
    START = "START"
    INITIAL_ZERO = "INITIAL_ZERO"
    ENTRY_MAKER_PREPARED = "ENTRY_MAKER_PREPARED"
    ENTRY_MAKER_DISPATCHED = "ENTRY_MAKER_DISPATCHED"
    ENTRY_MAKER_RESTING = "ENTRY_MAKER_RESTING"
    ENTRY_PREPARED = "ENTRY_PREPARED"
    ENTRY_DISPATCHED = "ENTRY_DISPATCHED"
    ENTRY_RESIDUE_CANCEL_PREPARED = "ENTRY_RESIDUE_CANCEL_PREPARED"
    ENTRY_RECONCILED = "ENTRY_RECONCILED"
    EXIT_MAKER_PREPARED = "EXIT_MAKER_PREPARED"
    EXIT_MAKER_DISPATCHED = "EXIT_MAKER_DISPATCHED"
    EXIT_MAKER_RESTING = "EXIT_MAKER_RESTING"
    EXIT_PREPARED = "EXIT_PREPARED"
    EXIT_DISPATCHED = "EXIT_DISPATCHED"
    EXIT_RESIDUE_CANCEL_PREPARED = "EXIT_RESIDUE_CANCEL_PREPARED"
    EXIT_RECONCILED = "EXIT_RECONCILED"
    FINAL_ROUND_ONE = "FINAL_ROUND_ONE"
    COMPLETE = "COMPLETE"
    HALTED = "HALTED"


@dataclass(frozen=True)
class WriteResult:
    result_class: WriteResultClass
    failure_code: str | None = None
    order_id: str | None = None

    @classmethod
    def accepted(cls, order_id: str | None = None) -> "WriteResult":
        return cls(WriteResultClass.ACCEPTED, order_id=order_id)

    @classmethod
    def rejected(cls, code: str = "VENUE_REJECTED") -> "WriteResult":
        return cls(WriteResultClass.TERMINAL_VENUE_REJECTION, code)

    @classmethod
    def ambiguous(cls, code: str = "AMBIGUOUS_WRITE") -> "WriteResult":
        return cls(WriteResultClass.TRANSPORT_AMBIGUITY, code)


@dataclass(frozen=True)
class RoleIdentity:
    role: AccountRole
    account: str
    signer: str
    credential_key_name: str
    setup_marker_name: str
    journal_name: str

    def validate(self) -> None:
        if not isinstance(self.role, AccountRole):
            raise CoordinatorSafetyError("RISEx role identity rejected")
        try:
            account = _address(self.account).lower()
            signer = _address(self.signer).lower()
        except Exception:
            raise CoordinatorSafetyError("RISEx role identity rejected") from None
        if (
            account != self.account.lower()
            or signer != self.signer.lower()
            or account == signer
            or not self.credential_key_name
            or not self.setup_marker_name
            or not self.journal_name
        ):
            raise CoordinatorSafetyError("RISEx role identity rejected")


@dataclass(frozen=True)
class NonceState:
    anchor: int
    bitmap_index: int

    def validate(self) -> None:
        if (
            type(self.anchor) is not int
            or not 0 <= self.anchor < 2**48
            or type(self.bitmap_index) is not int
            or not 0 <= self.bitmap_index <= 207
        ):
            raise CoordinatorSafetyError("RISEx nonce state rejected")


@dataclass(frozen=True)
class BookLevel:
    price: Decimal
    quantity: Decimal
    order_count: int


@dataclass(frozen=True)
class BookObservation:
    bid: Decimal
    ask: Decimal
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    observed_at: int


@dataclass(frozen=True)
class MarketObservation:
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
    book: BookObservation


@dataclass(frozen=True)
class RestOrder:
    order_id: str
    wide_order_id: int
    resting_order_id: int
    client_order_id: int
    market_id: int
    account: str
    side: str
    order_type: str
    time_in_force: str
    status: str
    size: Decimal
    filled_size: Decimal
    price: Decimal
    post_only: bool
    reduce_only: bool
    observed_at: int = 0


@dataclass(frozen=True)
class RestTrade:
    trade_id: str
    order_id: str
    client_order_id: int
    market_id: int
    account: str
    side: str
    size: Decimal
    price: Decimal
    observed_at: int


@dataclass(frozen=True)
class PortfolioState:
    account: str
    usdc_balance: Decimal
    free_collateral: Decimal
    total_account_value: Decimal
    in_liquidation: bool
    risk_level: str
    observed_at: int


@dataclass(frozen=True)
class PrivateEventEvidence:
    account: str
    auth_status: str
    orders_snapshot: tuple[RestOrder, ...]
    positions_snapshot: tuple[tuple[int, Decimal], ...]
    orders_updates: tuple[RestOrder, ...]
    positions_updates: tuple[tuple[int, Decimal], ...]
    observed_at: int


@dataclass(frozen=True)
class AccountSnapshot:
    role: AccountRole
    account: str
    signer: str
    signer_status: str
    position: Decimal
    open_orders: tuple[RestOrder, ...]
    history_orders: tuple[RestOrder, ...] = ()
    trades: tuple[RestTrade, ...] = ()
    portfolio: PortfolioState | None = None
    private: PrivateEventEvidence | None = None
    observed_at: int = 0
    source: str = "REST"
    unexplained: bool = False


@dataclass(frozen=True)
class VenueObservation:
    market: MarketObservation
    accounts: Mapping[AccountRole, AccountSnapshot]
    nonces: Mapping[AccountRole, NonceState] | None = None
    rest_round: int = 0


@dataclass(frozen=True)
class CoordinatorReport:
    run_id: str
    primary_run_id: str
    counterparty_run_id: str
    result: CoordinatorResult
    phase: Phase
    primary_intents: int
    counterparty_intents: int
    primary_dispatches: int
    counterparty_dispatches: int
    counterparty_cancels: int
    final_rounds: int
    failure_code: str | None = None

    def sanitized(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "primary_run_id": self.primary_run_id,
            "counterparty_run_id": self.counterparty_run_id,
            "result": self.result.value,
            "phase": self.phase.value,
            "primary_intents": self.primary_intents,
            "counterparty_intents": self.counterparty_intents,
            "primary_dispatches": self.primary_dispatches,
            "counterparty_dispatches": self.counterparty_dispatches,
            "counterparty_cancels": self.counterparty_cancels,
            "final_rounds": self.final_rounds,
            "failure_code": self.failure_code,
        }


class VenueAdapter(Protocol):
    async def observe(self) -> VenueObservation: ...

    async def rest_round(self) -> VenueObservation: ...

    async def place(self, role: AccountRole, request: Mapping[str, Any]) -> WriteResult: ...

    async def cancel(self, role: AccountRole, request: Mapping[str, Any]) -> WriteResult: ...

    async def close(self) -> None: ...


_EIP712_DOMAIN_TYPES = [
    {"name": "name", "type": "string"},
    {"name": "version", "type": "string"},
    {"name": "chainId", "type": "uint256"},
    {"name": "verifyingContract", "type": "address"},
]
_REGISTER_V2_TYPES = [
    {"name": "signer", "type": "address"},
    {"name": "message", "type": "string"},
    {"name": "nonce", "type": "uint256"},
]
_VERIFY_WITNESS_TYPES = [
    {"name": "account", "type": "address"},
    {"name": "target", "type": "address"},
    {"name": "hash", "type": "bytes32"},
    {"name": "nonceAnchor", "type": "uint48"},
    {"name": "nonceBitmap", "type": "uint8"},
    {"name": "deadline", "type": "uint32"},
]


def _register_v2_typed_data(signer: str, nonce: str) -> dict[str, Any]:
    return {
        "types": {
            "EIP712Domain": _EIP712_DOMAIN_TYPES,
            "RegisterV2": _REGISTER_V2_TYPES,
        },
        "primaryType": "RegisterV2",
        "domain": {
            "name": DOMAIN_NAME,
            "version": DOMAIN_VERSION,
            "chainId": CHAIN_ID,
            "verifyingContract": AUTHORIZATION,
        },
        "message": {
            "signer": signer,
            "message": "sign in with RISEx",
            "nonce": nonce,
        },
    }


def _compact_signature(signature: str) -> str:
    try:
        raw = bytes.fromhex(signature[2:])
        if len(raw) != 65 or raw[64] not in {27, 28}:
            raise ValueError
        compact_s = bytearray(raw[32:64])
        if raw[64] == 28:
            compact_s[0] |= 0x80
        return base64.b64encode(raw[:32] + bytes(compact_s)).decode("ascii")
    except Exception:
        raise CoordinatorSafetyError("RISEx signature encoding rejected") from None


class _ScopedCredential:
    """Short-lived signer handle; secret bytes are zeroized on close."""

    def __init__(self, identity: RoleIdentity, secret: bytes) -> None:
        if not isinstance(secret, bytes) or len(secret) != 32:
            raise CoordinatorSafetyError("RISEx credential source rejected")
        if _signer._derive_address(secret).lower() != identity.signer:
            raise CoordinatorSafetyError("RISEx signer identity rejected")
        self.signer = identity.signer
        self._secret = bytearray(secret)
        self._closed = False

    def _require_open(self) -> bytes:
        if self._closed or len(self._secret) != 32:
            raise CoordinatorSafetyError("RISEx credential handle rejected")
        return bytes(self._secret)

    def sign_register_v2(self, typed_data: Mapping[str, Any]) -> str:
        try:
            if (
                set(typed_data) != {"types", "primaryType", "domain", "message"}
                or typed_data["types"] != {
                    "EIP712Domain": _EIP712_DOMAIN_TYPES,
                    "RegisterV2": _REGISTER_V2_TYPES,
                }
                or typed_data["primaryType"] != "RegisterV2"
                or typed_data["domain"] != {
                    "name": DOMAIN_NAME,
                    "version": DOMAIN_VERSION,
                    "chainId": CHAIN_ID,
                    "verifyingContract": AUTHORIZATION,
                }
                or typed_data["message"]["signer"] != self.signer
                or typed_data["message"]["message"] != "sign in with RISEx"
                or PrivateReadPreflight._nonce(typed_data["message"]["nonce"])
                != typed_data["message"]["nonce"]
            ):
                raise ValueError
            return _signer._sign_typed_data(self._require_open(), dict(typed_data))
        except CoordinatorSafetyError:
            raise
        except Exception:
            raise CoordinatorSafetyError("RISEx auth_v2 signing rejected") from None

    def sign_permit(self, typed_data: Mapping[str, Any], identity: RoleIdentity) -> str:
        try:
            if identity.signer != self.signer:
                raise ValueError
            if (
                set(typed_data) != {"types", "primaryType", "domain", "message"}
                or typed_data["types"] != {
                    "EIP712Domain": _EIP712_DOMAIN_TYPES,
                    "VerifyWitness": _VERIFY_WITNESS_TYPES,
                }
                or typed_data["primaryType"] != "VerifyWitness"
                or typed_data["domain"] != {
                    "name": DOMAIN_NAME,
                    "version": DOMAIN_VERSION,
                    "chainId": CHAIN_ID,
                    "verifyingContract": AUTHORIZATION,
                }
            ):
                raise ValueError
            message = typed_data["message"]
            if (
                set(message) != {
                    "account", "target", "hash", "nonceAnchor", "nonceBitmap", "deadline",
                }
                or _address(message["account"]).lower() != identity.account
                or _address(message["target"]).lower() != ROUTER.lower()
                or not isinstance(message["hash"], str)
                or len(message["hash"]) != 66
                or not message["hash"].startswith("0x")
                or type(message["nonceAnchor"]) is not int
                or not 0 <= message["nonceAnchor"] < 2**48
                or type(message["nonceBitmap"]) is not int
                or not 0 <= message["nonceBitmap"] <= 207
                or type(message["deadline"]) is not int
                or not 0 < message["deadline"] < 2**32
            ):
                raise ValueError
            int(message["hash"][2:], 16)
            return _signer._sign_typed_data(self._require_open(), dict(typed_data))
        except CoordinatorSafetyError:
            raise
        except Exception:
            raise CoordinatorSafetyError("RISEx permit signing rejected") from None

    def close(self) -> None:
        for index in range(len(self._secret)):
            self._secret[index] = 0
        self._secret.clear()
        self._closed = True


def _read_protected_file(name: str) -> bytes:
    if not isinstance(name, str) or not name or "/" in name or "\\" in name:
        raise CoordinatorSafetyError("RISEx credential source rejected")
    home_fd: int | None = None
    try:
        home_fd = _signer._open_home()
        value = _signer._read_file(home_fd, name)
        if value is None:
            raise CoordinatorSafetyError("RISEx credential source rejected")
        return bytes(value)
    except CoordinatorSafetyError:
        raise
    except Exception:
        raise CoordinatorSafetyError("RISEx credential source rejected") from None
    finally:
        if home_fd is not None:
            os.close(home_fd)


def _read_fixed_secret(name: str) -> bytes:
    value = _read_protected_file(name)
    if len(value) != 32:
        raise CoordinatorSafetyError("RISEx credential source rejected")
    return value


def _decode_counterparty_secret(value: bytes) -> bytes:
    if (
        not isinstance(value, bytes)
        or len(value) != 67
        or value[:2] != b"0x"
        or value[-1:] != b"\n"
        or any(byte not in b"0123456789abcdef" for byte in value[2:66])
    ):
        raise CoordinatorSafetyError("RISEx counterparty credential source rejected")
    try:
        decoded = bytes.fromhex(value[2:66].decode("ascii"))
    except Exception:
        raise CoordinatorSafetyError("RISEx counterparty credential source rejected") from None
    if len(decoded) != 32:
        raise CoordinatorSafetyError("RISEx counterparty credential source rejected")
    return decoded


def _read_counterparty_secret(name: str) -> bytes:
    return _decode_counterparty_secret(_read_protected_file(name))


def _load_primary_identity() -> tuple[RoleIdentity, Callable[[], _ScopedCredential]]:
    home_fd: int | None = None
    secret = bytearray()
    try:
        home_fd = _signer._open_home()
        record = _signer._load_record(home_fd)
        loaded = _signer._load_credential(home_fd)
        secret.extend(loaded)
        if (
            record.state is not _signer.SignerState.ACTIVE
            or record.signer != PRIMARY_SIGNER
            or record.expiration <= int(time.time())
            or _signer._derive_address(bytes(secret)).lower() != PRIMARY_SIGNER
        ):
            raise CoordinatorSafetyError("RISEx primary identity rejected")
    except CoordinatorSafetyError:
        raise
    except Exception:
        raise CoordinatorSafetyError("RISEx primary identity rejected") from None
    finally:
        if home_fd is not None:
            os.close(home_fd)
    identity = RoleIdentity(
        AccountRole.PRIMARY, PRIMARY_ACCOUNT.lower(), PRIMARY_SIGNER.lower(),
        _signer._CREDENTIAL, _signer._RECORD, PRIMARY_JOURNAL,
    )
    identity.validate()
    for index in range(len(secret)):
        secret[index] = 0
    secret.clear()

    def loader() -> _ScopedCredential:
        return _ScopedCredential(identity, _read_fixed_secret(_signer._CREDENTIAL))

    return identity, loader


def _load_counterparty_registration_marker() -> None:
    home_fd: int | None = None
    try:
        home_fd = _signer._open_home()
        raw = _signer._read_file(home_fd, COUNTERPARTY_SIGNER_MARKER)
        if raw is None:
            raise CoordinatorSafetyError("RISEx counterparty registration rejected")
        marker = _strict_json_bytes(raw)
        expected_keys = {
            "account", "chain_id", "expiration", "host", "operation",
            "schema_version", "signer", "state", "venue",
        }
        if (
            not isinstance(marker, Mapping)
            or set(marker) != expected_keys
            or marker["account"] != COUNTERPARTY_ACCOUNT
            or marker["chain_id"] != CHAIN_ID
            or marker["expiration"] != COUNTERPARTY_REGISTRATION_EXPIRATION
            or marker["host"] != OFFICIAL_HOST
            or marker["operation"] != "COUNTERPARTY_REGISTER_SIGNER"
            or marker["schema_version"] != 1
            or marker["signer"] != COUNTERPARTY_SIGNER
            or marker["state"] != "ACTIVE"
            or marker["venue"] != "RISEx"
            or marker["expiration"] <= int(time.time())
        ):
            raise CoordinatorSafetyError("RISEx counterparty registration rejected")
    except CoordinatorSafetyError:
        raise
    except Exception:
        raise CoordinatorSafetyError("RISEx counterparty registration rejected") from None
    finally:
        if home_fd is not None:
            os.close(home_fd)


def _load_counterparty_identity() -> tuple[RoleIdentity, Callable[[], _ScopedCredential]]:
    wallet = bytearray(_read_counterparty_secret(COUNTERPARTY_WALLET_KEY))
    try:
        account = _signer._derive_address(bytes(wallet)).lower()
    except Exception:
        raise CoordinatorSafetyError("RISEx counterparty wallet identity rejected") from None
    finally:
        for index in range(len(wallet)):
            wallet[index] = 0
        wallet.clear()
    session = bytearray(_read_counterparty_secret(COUNTERPARTY_SESSION_KEY))
    try:
        signer = _signer._derive_address(session).lower()
    except Exception:
        raise CoordinatorSafetyError("RISEx counterparty signer identity rejected") from None
    finally:
        for index in range(len(session)):
            session[index] = 0
        session.clear()
    if account != COUNTERPARTY_ACCOUNT or signer != COUNTERPARTY_SIGNER:
        raise CoordinatorSafetyError("RISEx counterparty fixed identity rejected")
    _load_counterparty_registration_marker()
    identity = RoleIdentity(
        AccountRole.COUNTERPARTY, account, signer,
        COUNTERPARTY_SESSION_KEY, COUNTERPARTY_SIGNER_MARKER, COUNTERPARTY_JOURNAL,
    )
    identity.validate()

    def loader() -> _ScopedCredential:
        secret = bytearray(_read_counterparty_secret(COUNTERPARTY_SESSION_KEY))
        try:
            return _ScopedCredential(identity, bytes(secret))
        finally:
            for index in range(len(secret)):
                secret[index] = 0
            secret.clear()

    return identity, loader


@dataclass(frozen=True)
class _HTTPObservation:
    status: int
    final_url: str
    body: Any
    observed_at: int


class _RetryableGetFailure(Exception):
    """Internal marker for one eligible GET transport/body failure."""

    def __init__(self, kind: str) -> None:
        super().__init__(kind)
        self.kind = kind


def _strict_json_loads(raw: bytes) -> Any:
    def reject_constant(_value: str) -> Any:
        raise ValueError

    def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError
            result[key] = value
        return result

    def finite_float(value: str) -> float:
        parsed = float(value)
        if not math.isfinite(parsed):
            raise ValueError
        return parsed

    return json.loads(
        raw.decode("utf-8", errors="strict"),
        parse_constant=reject_constant,
        parse_float=finite_float,
        object_pairs_hook=unique_object,
    )


def _strict_json_bytes(raw: bytes) -> Any:
    try:
        return _strict_json_loads(raw)
    except Exception:
        raise CoordinatorSafetyError("RISEx response JSON rejected") from None


def _json_decode_error_is_incomplete(error: json.JSONDecodeError) -> bool:
    document = error.doc
    significant_end = len(document.rstrip(" \t\r\n"))
    if significant_end == 0:
        return True
    return error.pos >= significant_end or error.msg.startswith("Unterminated string")


def _strict_get_json_bytes(raw: bytes) -> Any:
    try:
        return _strict_json_loads(raw)
    except json.JSONDecodeError as error:
        if _json_decode_error_is_incomplete(error):
            raise _RetryableGetFailure("json") from None
        raise CoordinatorSafetyError("RISEx response JSON rejected") from None
    except Exception:
        raise CoordinatorSafetyError("RISEx response JSON rejected") from None


class FixedRisexTwoAccountTransport:
    """Fixed direct-TLS REST and auth_v2 transport with bounded GET recovery."""

    REST_ORIGIN = REST_ORIGIN
    WS_ORIGIN = WS_ORIGIN
    TRUST_ENV = False
    ALLOW_REDIRECTS = False
    MAX_BYTES = MAX_RESPONSE_BYTES
    DEADLINE_SECONDS = ADAPTER_DEADLINE_SECONDS

    def __init__(self, accounts: Sequence[str]) -> None:
        normalized = tuple(_address(value).lower() for value in accounts)
        if len(normalized) != 2 or len(set(normalized)) != 2:
            raise CoordinatorSafetyError("RISEx transport account set rejected")
        self._accounts = frozenset(normalized)
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.DEADLINE_SECONDS, connect=2, sock_read=3),
            trust_env=False,
            connector=aiohttp.TCPConnector(ssl=ssl.create_default_context()),
        )

    def _target(self, path: str, query: Sequence[tuple[str, str]]) -> str:
        query = tuple(query)
        fixed = {
            ("/v1/system/config", ()),
            ("/v1/auth/eip712-domain", ()),
            ("/v1/markets", (("force_refresh", "true"), ("market_ids", str(MARKET_ID)))),
            ("/v1/orderbook", (("market_id", str(MARKET_ID)),)),
        }
        allowed = path in {
            "/v1/auth/session-key-status", "/v1/auth/signers", "/v1/orders/open",
            "/v1/auth/nonce", HISTORY_PATH, TRADES_PATH, POSITION_PATH,
            POSITIONS_PATH, PORTFOLIO_PATH,
        } or path.startswith("/v1/nonce-state/") or path.startswith("/v1/orders/by-id/")
        if not (path, query) in fixed and not allowed:
            raise CoordinatorSafetyError("RISEx REST read surface rejected")
        if path.startswith("/v1/orders/by-id/"):
            order_id = path.removeprefix("/v1/orders/by-id/")
            if not _valid_order_id(order_id) or query:
                raise CoordinatorSafetyError("RISEx exact order read surface rejected")
        elif path.startswith("/v1/nonce-state/"):
            account = path.rsplit("/", 1)[-1].lower()
            if account not in self._accounts or query:
                raise CoordinatorSafetyError("RISEx nonce read surface rejected")
        elif path in {"/v1/auth/nonce"}:
            if (
                len(query) != 1
                or query[0][0] != "account"
                or query[0][1].lower() not in self._accounts
            ):
                raise CoordinatorSafetyError("RISEx auth nonce read surface rejected")
        elif path in {"/v1/auth/session-key-status"}:
            if len(query) != 2 or query[0][0] != "account" or query[1][0] != "signer":
                raise CoordinatorSafetyError("RISEx signer read surface rejected")
            if query[0][1].lower() not in self._accounts:
                raise CoordinatorSafetyError("RISEx signer read surface rejected")
        elif path in {"/v1/auth/signers", "/v1/orders/open", POSITIONS_PATH, PORTFOLIO_PATH}:
            if (
                len(query) != 1
                or query[0][0] != "account"
                or query[0][1].lower() not in self._accounts
            ):
                raise CoordinatorSafetyError("RISEx account read surface rejected")
        elif path in {HISTORY_PATH, TRADES_PATH}:
            if (
                len(query) != 4
                or query[0][0] != "account"
                or query[1] != ("market_id", str(MARKET_ID))
                or query[2][0] != "page"
                or query[3] != ("limit", str(PAGE_LIMIT))
                or query[0][1].lower() not in self._accounts
            ):
                raise CoordinatorSafetyError("RISEx paged read surface rejected")
            try:
                page = int(query[2][1])
            except (TypeError, ValueError):
                raise CoordinatorSafetyError("RISEx paged read surface rejected") from None
            if page <= 0 or page > MAX_PAGINATION_PAGES:
                raise CoordinatorSafetyError("RISEx paged read surface rejected")
        elif path == POSITION_PATH:
            if len(query) != 2 or query[0][0] != "account" or query[1] != ("market_id", str(MARKET_ID)):
                raise CoordinatorSafetyError("RISEx position read surface rejected")
            if query[0][1].lower() not in self._accounts:
                raise CoordinatorSafetyError("RISEx position read surface rejected")
        return self.REST_ORIGIN + path + ("?" + urlencode(query) if query else "")

    @staticmethod
    def _is_retryable_get_transport_error(error: Exception) -> bool:
        if isinstance(error, (
            aiohttp.ClientResponseError, aiohttp.InvalidURL,
            aiohttp.ClientSSLError, ssl.SSLError,
        )) or isinstance(getattr(error, "os_error", None), ssl.SSLError):
            return False
        return isinstance(error, (
            aiohttp.ClientError, asyncio.IncompleteReadError,
            TimeoutError, OSError, EOFError,
        ))

    async def _get_attempt(self, target: str) -> _HTTPObservation:
        response_status: int | None = None
        try:
            async with self._session.get(target, allow_redirects=False, proxy=None) as response:
                response_status = response.status
                if response.history or str(response.url) != target:
                    raise CoordinatorSafetyError("RISEx REST redirect rejected")
                if response.content_length is not None and response.content_length > self.MAX_BYTES:
                    raise CoordinatorSafetyError("RISEx REST response bound rejected")
                raw = await response.content.read(self.MAX_BYTES + 1)
                if len(raw) > self.MAX_BYTES:
                    raise CoordinatorSafetyError("RISEx REST response bound rejected")
                body = (
                    _strict_get_json_bytes(raw)
                    if response.status == 200
                    else _strict_json_bytes(raw)
                )
                return _HTTPObservation(response.status, str(response.url), body, int(time.time()))
        except _RetryableGetFailure:
            raise
        except CoordinatorSafetyError:
            raise
        except Exception as error:
            if response_status not in {None, 200} or not self._is_retryable_get_transport_error(error):
                raise CoordinatorSafetyError("RISEx REST transport failed") from None
            raise _RetryableGetFailure("transport") from None

    async def get(self, path: str, query: Sequence[tuple[str, str]] = ()) -> _HTTPObservation:
        target = self._target(path, query)
        for attempt in range(2):
            try:
                return await self._get_attempt(target)
            except _RetryableGetFailure as failure:
                if attempt == 1:
                    message = (
                        "RISEx response JSON rejected"
                        if failure.kind == "json"
                        else "RISEx REST transport failed"
                    )
                    raise CoordinatorSafetyError(message) from None

    async def post(self, path: str, body: Mapping[str, Any]) -> _HTTPObservation:
        if path not in {PLACE_PATH, CANCEL_PATH} or not isinstance(body, Mapping):
            raise CoordinatorSafetyError("RISEx REST write surface rejected")
        try:
            encoded = json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
            async with self._session.post(
                self.REST_ORIGIN + path,
                data=encoded,
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                allow_redirects=False,
                proxy=None,
            ) as response:
                if response.history or str(response.url) != self.REST_ORIGIN + path:
                    raise CoordinatorSafetyError("RISEx REST write redirect rejected")
                if response.content_length is not None and response.content_length > self.MAX_BYTES:
                    raise CoordinatorSafetyError("RISEx REST write response bound rejected")
                raw = await response.content.read(self.MAX_BYTES + 1)
                if len(raw) > self.MAX_BYTES:
                    raise CoordinatorSafetyError("RISEx REST write response bound rejected")
                return _HTTPObservation(response.status, str(response.url), _strict_json_bytes(raw), int(time.time()))
        except CoordinatorSafetyError:
            raise
        except Exception:
            raise CoordinatorSafetyError("RISEx REST write transport ambiguous") from None

    async def auth_v2_frames(
        self, frame: Mapping[str, Any], *, account: str, signer: str,
    ) -> tuple[Any, tuple[Any, ...], tuple[Any, ...]]:
        if account.lower() not in self._accounts or _address(signer).lower() == account.lower():
            raise CoordinatorSafetyError("RISEx auth_v2 identity rejected")
        try:
            async with self._session.ws_connect(
                self.WS_ORIGIN,
                ssl=ssl.create_default_context(),
                proxy=None,
                autoclose=False,
                autoping=False,
                max_msg_size=self.MAX_BYTES,
            ) as socket:
                await socket.send_json(dict(frame))
                auth_raw = await self._receive_text(socket)
                auth = _parse_auth_v2(auth_raw)
                _require_auth_v2_success(_validate_auth_v2_schema(auth))
                await socket.send_json({"method": "subscribe", "params": {"channel": "orders"}})
                await socket.send_json({"method": "subscribe", "params": {"channel": "positions"}})
                orders: list[Any] = []
                positions: list[Any] = []
                for _ in range(8):
                    value = _strict_json_bytes((await self._receive_text(socket)).encode("utf-8"))
                    if isinstance(value, Mapping) and value.get("method") == "subscribe":
                        if value.get("status") != "success" or value.get("channel") not in {"orders", "positions"}:
                            raise CoordinatorSafetyError("RISEx auth_v2 subscribe rejected")
                        continue
                    if not isinstance(value, Mapping) or value.get("channel") not in {"orders", "positions"}:
                        raise CoordinatorSafetyError("RISEx auth_v2 unrelated frame rejected")
                    channel = str(value["channel"])
                    if value.get("type") not in {"snapshot", "update"}:
                        raise CoordinatorSafetyError("RISEx auth_v2 frame type rejected")
                    (orders if channel == "orders" else positions).append(value)
                    if any(item.get("type") == "snapshot" for item in orders) and any(item.get("type") == "snapshot" for item in positions):
                        return auth, tuple(orders), tuple(positions)
                raise CoordinatorSafetyError("RISEx auth_v2 snapshots missing")
        except CoordinatorSafetyError:
            raise
        except Exception:
            raise CoordinatorSafetyError("RISEx auth_v2 transport failed") from None

    async def _receive_text(self, socket: Any) -> str:
        try:
            incoming = await socket.receive(timeout=self.DEADLINE_SECONDS)
        except Exception:
            raise CoordinatorSafetyError("RISEx auth_v2 receive failed") from None
        if incoming.type is not aiohttp.WSMsgType.TEXT or type(incoming.data) is not str:
            raise CoordinatorSafetyError("RISEx auth_v2 frame rejected")
        return incoming.data

    async def close(self) -> None:
        await self._session.close()


def _now_int(value: Any) -> int:
    if type(value) is not int or value <= 0:
        raise CoordinatorSafetyError("RISEx observation clock rejected")
    return value


def _fresh(observed_at: Any, now: int) -> bool:
    return type(observed_at) is int and 0 <= now - observed_at <= MAX_AGE_SECONDS


def _decimal(value: Any, *, positive: bool = False, nonnegative: bool = False) -> Decimal:
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise CoordinatorSafetyError("RISEx decimal observation rejected") from None
    if not parsed.is_finite() or (positive and parsed <= 0) or (nonnegative and parsed < 0):
        raise CoordinatorSafetyError("RISEx decimal observation rejected")
    return parsed


def _aligned(value: Decimal, step: Decimal) -> bool:
    return step > 0 and value > 0 and value % step == 0


def _private_market_id(value: Any) -> int:
    if type(value) is int:
        market_id = value
    elif (
        isinstance(value, str) and value.isascii() and value.isdecimal()
        and (value == "0" or not value.startswith("0"))
    ):
        market_id = int(value)
    else:
        raise CoordinatorSafetyError("RISEx private position identity rejected")
    if not 0 < market_id < 2**16:
        raise CoordinatorSafetyError("RISEx private position identity rejected")
    return market_id


def _wide_order_id(order_id: str) -> int:
    if not _valid_order_id(order_id):
        raise CoordinatorSafetyError("RISEx order identity rejected")
    return int(order_id[2:18], 16)


def _canonical_digest(value: Mapping[str, Any]) -> str:
    try:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except Exception:
        raise CoordinatorSafetyError("RISEx canonical identity rejected") from None
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _validate_book(book: BookObservation, market: MarketObservation, now: int) -> None:
    if not _fresh(book.observed_at, now):
        raise CoordinatorSafetyError("RISEx book stale")
    if (
        book.bid <= 0
        or book.ask <= book.bid
        or book.bid % market.tick
        or book.ask % market.tick
        or not book.bids
        or not book.asks
        or book.bids[0].price != book.bid
        or book.asks[0].price != book.ask
    ):
        raise CoordinatorSafetyError("RISEx uncrossed BBO rejected")
    bid_prices: set[Decimal] = set()
    ask_prices: set[Decimal] = set()
    for levels, descending, seen in (
        (book.bids, True, bid_prices), (book.asks, False, ask_prices),
    ):
        previous: Decimal | None = None
        for level in levels:
            quantity = _decimal(level.quantity, positive=True)
            price = _decimal(level.price, positive=True)
            if (
                price % market.tick
                or quantity % market.step
                or type(level.order_count) is not int
                or level.order_count <= 0
                or price in seen
                or (previous is not None and ((descending and price >= previous) or (not descending and price <= previous)))
            ):
                raise CoordinatorSafetyError("RISEx book grid or ordering rejected")
            seen.add(price)
            previous = price
    if bid_prices & ask_prices:
        raise CoordinatorSafetyError("RISEx book level contradiction")


def _validate_market(market: MarketObservation, now: int) -> None:
    try:
        router = _address(market.router).lower()
        authorization = _address(market.authorization).lower()
    except Exception:
        raise CoordinatorSafetyError("RISEx market identity rejected") from None
    if (
        market.host != OFFICIAL_HOST
        or market.chain_id != OFFICIAL_CHAIN_ID
        or market.domain_name != OFFICIAL_DOMAIN_NAME
        or market.domain_version != OFFICIAL_DOMAIN_VERSION
        or router != ROUTER.lower()
        or authorization != AUTHORIZATION.lower()
        or market.market_id != MARKET_ID
        or market.symbol != MARKET_SYMBOL
        or not market.active
        or not market.unlocked
        or market.tick != MARKET_TICK
        or market.step != MARKET_STEP
        or market.minimum != MARKET_MINIMUM
        or not _fresh(market.observed_at, now)
    ):
        raise CoordinatorSafetyError("RISEx market contract rejected")
    _validate_book(market.book, market, now)


def maker_price(market: MarketObservation, side: str) -> Decimal:
    """Return the one-tick-inside unique maker price for the fixed market."""
    if side not in {"BUY", "SELL"}:
        raise CoordinatorSafetyError("RISEx maker side rejected")
    candidate = (
        market.book.bid + market.tick if side == "SELL"
        else market.book.ask - market.tick
    )
    if not market.book.bids or not market.book.asks:
        raise CoordinatorSafetyError("RISEx maker book rejected")
    if (
        not market.book.bid < candidate < market.book.ask
        or candidate % market.tick
        or candidate in {level.price for level in (*market.book.bids, *market.book.asks)}
    ):
        raise CoordinatorSafetyError("RISEx unique inside maker price unavailable")
    if side == "SELL" and candidate <= max(level.price for level in market.book.bids):
        raise CoordinatorSafetyError("RISEx maker ask is not best")
    if side == "BUY" and candidate >= min(level.price for level in market.book.asks):
        raise CoordinatorSafetyError("RISEx maker bid is not best")
    return candidate


def _validate_order(order: RestOrder, identity: RoleIdentity, now: int) -> None:
    if (
        not _valid_order_id(order.order_id)
        or _wide_order_id(order.order_id) != order.wide_order_id
        or order.resting_order_id != order.wide_order_id >> 1
        or type(order.client_order_id) is not int
        or not 0 < order.client_order_id < 2**64
        or order.market_id != MARKET_ID
        or order.account.lower() != identity.account
        or order.side not in {"BUY", "SELL"}
        or order.order_type not in {"MARKET", "LIMIT"}
        or order.time_in_force not in {"GTC", "IOC"}
        or order.status not in {"OPEN", "FILLED", "CANCELLED"}
        or type(order.post_only) is not bool
        or type(order.reduce_only) is not bool
    ):
        raise CoordinatorSafetyError("RISEx order observation rejected")
    size = _decimal(order.size, positive=True)
    filled = _decimal(order.filled_size, nonnegative=True)
    price = _decimal(order.price, nonnegative=True)
    if (
        filled > size
        or not _aligned(size, MARKET_STEP)
        or not _fresh(order.observed_at, now)
        or price % MARKET_TICK
        or (order.order_type == "LIMIT" and price <= 0)
    ):
        raise CoordinatorSafetyError("RISEx order grid rejected")


def _validate_trade(trade: RestTrade, identity: RoleIdentity, now: int) -> None:
    trade_parts = trade.trade_id.split("-")
    if (
        not trade.trade_id
        or len(trade_parts) != 2
        or not all(_valid_order_id(part) for part in trade_parts)
        or not _valid_order_id(trade.order_id)
        or type(trade.client_order_id) is not int
        or trade.client_order_id <= 0
        or trade.market_id != MARKET_ID
        or trade.account.lower() != identity.account
        or trade.side not in {"BUY", "SELL"}
        or not _fresh(trade.observed_at, now)
    ):
        raise CoordinatorSafetyError("RISEx trade observation rejected")
    size = _decimal(trade.size, positive=True)
    price = _decimal(trade.price, positive=True)
    if not _aligned(size, MARKET_STEP) or price % MARKET_TICK:
        raise CoordinatorSafetyError("RISEx trade grid rejected")


def _validate_private_position_rows(
    rows: Sequence[tuple[int, Decimal]], *,
    allow_unrelated_zero: bool, require_unique: bool,
) -> None:
    seen: dict[int, Decimal] = {}
    try:
        iterator = iter(rows)
        for row in iterator:
            if not isinstance(row, (tuple, list)) or len(row) != 2:
                raise CoordinatorSafetyError("RISEx private position schema rejected")
            market_id, raw_size = row
            if type(market_id) is not int or not 0 < market_id < 2**16:
                raise CoordinatorSafetyError("RISEx private position identity rejected")
            size = _decimal(raw_size, nonnegative=False)
            if size % MARKET_STEP:
                raise CoordinatorSafetyError("RISEx private position grid rejected")
            previous = seen.get(market_id)
            if previous is not None:
                if require_unique or previous != size:
                    raise CoordinatorSafetyError("RISEx private position contradiction")
                continue
            if market_id != MARKET_ID and (
                not allow_unrelated_zero or size != 0
            ):
                raise CoordinatorSafetyError("RISEx unrelated private position rejected")
            seen[market_id] = size
    except CoordinatorSafetyError:
        raise
    except Exception:
        raise CoordinatorSafetyError("RISEx private position schema rejected") from None


def _validate_private(value: PrivateEventEvidence, identity: RoleIdentity, now: int) -> None:
    if (
        value.account.lower() != identity.account
        or value.auth_status != "success"
        or not _fresh(value.observed_at, now)
    ):
        raise CoordinatorSafetyError("RISEx private event evidence rejected")
    for order in (*value.orders_snapshot, *value.orders_updates):
        _validate_order(order, identity, now)
    _validate_private_position_rows(
        value.positions_snapshot, allow_unrelated_zero=True, require_unique=True,
    )
    _validate_private_position_rows(
        value.positions_updates, allow_unrelated_zero=False, require_unique=False,
    )


def _validate_account(value: AccountSnapshot, identity: RoleIdentity, now: int, *, private: bool = True) -> None:
    if (
        value.role is not identity.role
        or value.account.lower() != identity.account
        or value.signer.lower() != identity.signer
        or value.signer_status != "ACTIVE"
        or value.source != "REST"
        or not _fresh(value.observed_at, now)
        or value.unexplained
        or value.portfolio is None
    ):
        raise CoordinatorSafetyError("RISEx account identity or freshness rejected")
    position = _decimal(value.position, nonnegative=False)
    if not position.is_finite() or position % MARKET_STEP:
        raise CoordinatorSafetyError("RISEx position grid rejected")
    portfolio = value.portfolio
    if (
        portfolio.account.lower() != identity.account
        or not _fresh(portfolio.observed_at, now)
        or portfolio.in_liquidation is not False
        or portfolio.risk_level != "NORMAL"
        or not _decimal(portfolio.usdc_balance, positive=True).is_finite()
        or not _decimal(portfolio.free_collateral, positive=True).is_finite()
        or not _decimal(portfolio.total_account_value, positive=True).is_finite()
    ):
        raise CoordinatorSafetyError("RISEx portfolio risk state rejected")
    for collection in (value.open_orders, value.history_orders):
        seen_orders: set[str] = set()
        for order in collection:
            _validate_order(order, identity, now)
            if order.order_id in seen_orders:
                raise CoordinatorSafetyError("RISEx duplicate order observation")
            seen_orders.add(order.order_id)
    open_by_id = {order.order_id: order for order in value.open_orders}
    for order in value.history_orders:
        overlap = open_by_id.get(order.order_id)
        if overlap is not None and _order_history_evidence(overlap) != _order_history_evidence(order):
            raise CoordinatorSafetyError("RISEx overlapping order disagreement")
    seen_trades: set[str] = set()
    for trade in value.trades:
        _validate_trade(trade, identity, now)
        if trade.trade_id in seen_trades:
            raise CoordinatorSafetyError("RISEx duplicate trade observation")
        seen_trades.add(trade.trade_id)
    if private:
        if value.private is None:
            raise CoordinatorSafetyError("RISEx private event evidence missing")
        _validate_private(value.private, identity, now)
        rest_open = {order.order_id: order for order in value.open_orders}
        private_open = {
            order.order_id: order for order in value.private.orders_snapshot
        }
        if set(rest_open) != set(private_open):
            raise CoordinatorSafetyError("RISEx private order snapshot disagrees")
        for order_id, order in rest_open.items():
            if _order_history_evidence(order) != _order_history_evidence(private_open[order_id]):
                raise CoordinatorSafetyError("RISEx private order snapshot disagrees")
        private_positions = [
            size for market_id, size in value.private.positions_snapshot
            if market_id == MARKET_ID
        ]
        if len(private_positions) > 1:
            raise CoordinatorSafetyError("RISEx private position snapshot disagrees")
        if (private_positions[0] if private_positions else Decimal("0")) != position:
            raise CoordinatorSafetyError("RISEx private position snapshot disagrees")
        current_orders = {
            item.order_id: item for item in (*value.open_orders, *value.history_orders)
        }
        for update in value.private.orders_updates:
            current = current_orders.get(update.order_id)
            if current is not None and _order_immutable_evidence(current) != _order_immutable_evidence(update):
                raise CoordinatorSafetyError("RISEx private order update disagrees")
        for market_id, size in value.private.positions_updates:
            if market_id == MARKET_ID and size != position:
                raise CoordinatorSafetyError("RISEx private position update disagrees")


def _all_orders(value: AccountSnapshot) -> tuple[RestOrder, ...]:
    unique: dict[str, RestOrder] = {}
    for order in (*value.open_orders, *value.history_orders):
        previous = unique.get(order.order_id)
        if previous is not None and _order_history_evidence(previous) != _order_history_evidence(order):
            raise CoordinatorSafetyError("RISEx overlapping order disagreement")
        unique[order.order_id] = order
    return tuple(unique.values())


def _account_zero(value: AccountSnapshot) -> bool:
    return value.position == 0 and not value.open_orders


def _order_for(value: AccountSnapshot, client_order_id: int) -> RestOrder | None:
    matches = tuple(item for item in _all_orders(value) if item.client_order_id == client_order_id)
    if len(matches) > 1:
        raise CoordinatorSafetyError("RISEx order identity duplicated")
    return matches[0] if matches else None


def _trades_for(value: AccountSnapshot, order: RestOrder) -> tuple[RestTrade, ...]:
    return tuple(
        trade for trade in value.trades
        if trade.order_id == order.order_id or trade.client_order_id == order.client_order_id
    )


def _order_history_evidence(order: RestOrder) -> tuple[Any, ...]:
    return (
        order.order_id, order.wide_order_id, order.resting_order_id,
        order.client_order_id, order.market_id, order.account,
        order.side, order.order_type, order.time_in_force, order.status,
        str(order.size), str(order.filled_size), str(order.price),
        order.post_only, order.reduce_only,
    )


def _order_immutable_evidence(order: RestOrder) -> tuple[Any, ...]:
    return (
        order.order_id, order.wide_order_id, order.resting_order_id,
        order.client_order_id, order.market_id, order.account,
        order.side, order.order_type, order.time_in_force,
        str(order.size), str(order.price), order.post_only, order.reduce_only,
    )


def _trade_history_evidence(trade: RestTrade) -> tuple[Any, ...]:
    return (
        trade.trade_id, trade.order_id, trade.client_order_id, trade.market_id,
        trade.account, trade.side, str(trade.size), str(trade.price),
    )


def _history_payload(account: AccountSnapshot) -> dict[str, list[list[Any]]]:
    if len(account.history_orders) + len(account.trades) > MAX_BASELINE_HISTORY_ROWS:
        raise CoordinatorSafetyError("RISEx baseline history bound rejected")
    return {
        "orders": [
            list(item) for item in sorted(
                (_order_history_evidence(order) for order in account.history_orders),
                key=lambda item: str(item[0]),
            )
        ],
        "trades": [
            list(item) for item in sorted(
                (_trade_history_evidence(trade) for trade in account.trades),
                key=lambda item: str(item[0]),
            )
        ],
    }


def _history_token(payload: Mapping[str, Any]) -> str:
    try:
        return json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except Exception:
        raise CoordinatorSafetyError("RISEx history fingerprint rejected") from None


def _history_digest(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(_history_token(payload).encode("utf-8")).hexdigest()


def _decode_history_token(token: str) -> Mapping[str, list[list[Any]]]:
    try:
        value = json.loads(token)
        if (
            not isinstance(value, Mapping)
            or set(value) != {"orders", "trades"}
            or not isinstance(value["orders"], list)
            or not isinstance(value["trades"], list)
            or len(value["orders"]) + len(value["trades"])
            > MAX_BASELINE_HISTORY_ROWS
            or not all(isinstance(item, list) for item in (*value["orders"], *value["trades"]))
        ):
            raise ValueError
        return value  # type: ignore[return-value]
    except Exception:
        raise CoordinatorSafetyError("RISEx baseline history rejected") from None


def _response_data(response: _HTTPObservation) -> Any:
    if response.status != 200 or not isinstance(response.body, Mapping):
        raise CoordinatorSafetyError("RISEx REST authoritative read rejected")
    if (
        set(response.body) != {"data", "request_id"}
        or not isinstance(response.body["request_id"], str)
        or not response.body["request_id"]
    ):
        raise CoordinatorSafetyError("RISEx REST envelope rejected")
    return response.body["data"]


def _require_recent(response: _HTTPObservation, now: int, label: str) -> None:
    if not isinstance(response, _HTTPObservation) or not _fresh(response.observed_at, now):
        raise CoordinatorSafetyError(f"RISEx {label} response stale")


async def _paged_rows(
    transport: Any, path: str, identity: RoleIdentity, key: str,
    now: Callable[[], int],
) -> tuple[tuple[Mapping[str, Any], ...], int]:
    rows: list[Mapping[str, Any]] = []
    oldest: int | None = None
    for page in range(1, MAX_PAGINATION_PAGES + 1):
        response = await transport.get(
            path,
            (
                ("account", identity.account),
                ("market_id", str(MARKET_ID)),
                ("page", str(page)),
                ("limit", str(PAGE_LIMIT)),
            ),
        )
        _require_recent(response, now(), "paged")
        data = _response_data(response)
        if (
            not isinstance(data, Mapping)
            or not isinstance(data.get(key), list)
            or type(data.get("page")) is not int
            or data["page"] != page
            or type(data.get("has_next_page")) is not bool
            or len(data[key]) > PAGE_LIMIT
        ):
            raise CoordinatorSafetyError("RISEx paged response schema rejected")
        oldest = response.observed_at if oldest is None else min(oldest, response.observed_at)
        page_rows = data[key]
        if not all(isinstance(row, Mapping) for row in page_rows):
            raise CoordinatorSafetyError("RISEx paged response row rejected")
        rows.extend(page_rows)
        if not data["has_next_page"]:
            if oldest is None:
                raise CoordinatorSafetyError("RISEx paged response timestamp rejected")
            return tuple(rows), oldest
    raise CoordinatorSafetyError("RISEx paged response bound rejected")


def _mapped_enum(value: Any, values: Mapping[Any, str], label: str) -> str:
    if value in values:
        return values[value]
    if isinstance(value, str):
        upper = value.upper()
        if upper in values.values():
            return upper
        for key, mapped in values.items():
            if isinstance(key, str) and (upper == key.upper() or upper.endswith(key.upper())):
                return mapped
    raise CoordinatorSafetyError(f"RISEx {label} rejected")


def _compact_uint(value: Any, *, bits: int, label: str) -> int:
    if type(value) is int:
        parsed = value
    elif (
        isinstance(value, str)
        and value.isascii()
        and value.isdecimal()
        and (value == "0" or not value.startswith("0"))
    ):
        parsed = int(value)
    else:
        raise CoordinatorSafetyError(f"RISEx compact {label} rejected")
    if not 0 <= parsed < 2**bits:
        raise CoordinatorSafetyError(f"RISEx compact {label} rejected")
    return parsed


def _compact_enum(value: Any, values: Mapping[int, str], label: str) -> str:
    parsed = _compact_uint(value, bits=8, label=label)
    mapped = values.get(parsed)
    if mapped is not None:
        return mapped
    raise CoordinatorSafetyError(f"RISEx compact {label} rejected")


def _parse_book_rows(value: Any, observed_at: int) -> BookObservation:
    if not isinstance(value, Mapping) or str(value.get("market_id")) != str(MARKET_ID):
        raise CoordinatorSafetyError("RISEx REST book identity rejected")
    levels: dict[str, list[BookLevel]] = {"bids": [], "asks": []}
    for side in levels:
        raw_levels = value.get(side)
        if not isinstance(raw_levels, list) or not raw_levels:
            raise CoordinatorSafetyError("RISEx REST book schema rejected")
        for raw in raw_levels:
            if not isinstance(raw, Mapping) or not {"order_count", "price", "quantity"} <= set(raw):
                raise CoordinatorSafetyError("RISEx REST book level rejected")
            if type(raw["order_count"]) is not int or raw["order_count"] <= 0:
                raise CoordinatorSafetyError("RISEx REST book count rejected")
            levels[side].append(BookLevel(
                _decimal(raw["price"], positive=True),
                _decimal(raw["quantity"], positive=True),
                raw["order_count"],
            ))
    return BookObservation(
        bid=levels["bids"][0].price,
        ask=levels["asks"][0].price,
        bids=tuple(levels["bids"]), asks=tuple(levels["asks"]), observed_at=observed_at,
    )


_COMPACT_OPEN_ORDER_FIELDS = {
    "account", "client_order_id", "market_id", "order_id", "order_type",
    "post_only", "price_ticks", "reduce_only", "resting_order_id", "side",
    "size_steps", "time_in_force", "wide_order_id",
}


def _parse_compact_open_order_row(
    row: Mapping[str, Any], identity: RoleIdentity, observed_at: int,
) -> RestOrder:
    if not isinstance(row, Mapping) or not _COMPACT_OPEN_ORDER_FIELDS <= set(row):
        raise CoordinatorSafetyError("RISEx compact open order schema rejected")
    try:
        order_id = row["order_id"]
        if not isinstance(order_id, str) or not _valid_order_id(order_id):
            raise ValueError
        wide = _compact_uint(row["wide_order_id"], bits=64, label="wide order identity")
        resting = _compact_uint(
            row["resting_order_id"], bits=64, label="resting order identity",
        )
        client = _compact_uint(
            row["client_order_id"], bits=64, label="client order identity",
        )
        market_id = _compact_uint(row["market_id"], bits=16, label="market identity")
        account = _address(row["account"]).lower()
        if account != identity.account.lower() or market_id != MARKET_ID:
            raise CoordinatorSafetyError("RISEx compact open order identity rejected")
        if _wide_order_id(order_id) != wide or resting != wide >> 1:
            raise CoordinatorSafetyError("RISEx compact open order composite rejected")
        side = _compact_enum(row["side"], {0: "BUY", 1: "SELL"}, "order side")
        order_type = _compact_enum(
            row["order_type"], {0: "MARKET", 1: "LIMIT"}, "order type",
        )
        tif = _compact_enum(
            row["time_in_force"], {0: "GTC", 1: "GTT", 2: "FOK", 3: "IOC"},
            "order time-in-force",
        )
        size_steps = _compact_uint(row["size_steps"], bits=32, label="size")
        price_ticks = _compact_uint(row["price_ticks"], bits=24, label="price")
        if type(row["post_only"]) is not bool or type(row["reduce_only"]) is not bool:
            raise ValueError
        size = Decimal(format((Decimal(size_steps) * MARKET_STEP).normalize(), "f"))
        price = Decimal(format((Decimal(price_ticks) * MARKET_TICK).normalize(), "f"))
        order = RestOrder(
            order_id=order_id, wide_order_id=wide, resting_order_id=resting,
            client_order_id=client, market_id=market_id, account=account,
            side=side, order_type=order_type, time_in_force=tif, status="OPEN",
            size=size, filled_size=Decimal("0"), price=price,
            post_only=row["post_only"], reduce_only=row["reduce_only"],
            observed_at=observed_at,
        )
    except CoordinatorSafetyError:
        raise
    except Exception:
        raise CoordinatorSafetyError("RISEx compact open order value rejected") from None
    _validate_order(order, identity, observed_at)
    return order


def _parse_open_orders(
    response: _HTTPObservation, identity: RoleIdentity,
) -> tuple[RestOrder, ...]:
    data = _response_data(response)
    if (
        not isinstance(data, Mapping)
        or not {"account", "market_id", "orders", "total_orders"} <= set(data)
        or not isinstance(data["orders"], list)
    ):
        raise CoordinatorSafetyError("RISEx compact open orders schema rejected")
    try:
        account = _address(data["account"]).lower()
    except Exception:
        raise CoordinatorSafetyError("RISEx compact open orders identity rejected") from None
    if account != identity.account.lower():
        raise CoordinatorSafetyError("RISEx compact open orders identity rejected")
    if _compact_uint(data["market_id"], bits=16, label="open market") != 0:
        raise CoordinatorSafetyError("RISEx compact open orders market rejected")
    total_orders = _compact_uint(data["total_orders"], bits=32, label="open count")
    if total_orders > MAX_BASELINE_HISTORY_ROWS or len(data["orders"]) > MAX_BASELINE_HISTORY_ROWS:
        raise CoordinatorSafetyError("RISEx compact open orders bound rejected")
    return tuple(
        _parse_compact_open_order_row(row, identity, response.observed_at)
        for row in data["orders"]
    )


def _parse_order_row(row: Mapping[str, Any], identity: RoleIdentity, observed_at: int) -> RestOrder:
    required = {
        "id", "wide_order_id", "resting_order_id", "client_order_id", "market_id",
        "sender", "side", "type", "time_in_force", "status", "size", "filled_size",
        "post_only", "reduce_only", "is_liquidation",
    }
    if not required <= set(row) or row.get("is_liquidation") is not False:
        raise CoordinatorSafetyError("RISEx REST order schema rejected")
    try:
        order_id = str(row["id"])
        wide = int(row["wide_order_id"])
        resting = int(row["resting_order_id"])
        client = int(row["client_order_id"])
        market_id = int(row["market_id"])
        account = _address(row["sender"]).lower()
        side = _mapped_enum(row["side"], {0: "BUY", 1: "SELL"}, "order side")
        order_type = _mapped_enum(row["type"], {0: "MARKET", 1: "LIMIT"}, "order type")
        tif = _mapped_enum(row["time_in_force"], {0: "GTC", 1: "GTT", 2: "FOK", 3: "IOC"}, "order time-in-force")
        status = _mapped_enum(
            row["status"],
            {
                "ORDER_STATUS_OPEN": "OPEN",
                "ORDER_STATUS_FILLED": "FILLED",
                "ORDER_STATUS_CANCELLED": "CANCELLED",
            },
            "order status",
        )
        size = _decimal(row["size"], positive=True)
        filled = _decimal(row["filled_size"], nonnegative=True)
        if "price" in row:
            price = _decimal(row["price"], nonnegative=True)
        elif "price_ticks" in row and type(row["price_ticks"]) is int:
            price = Decimal(row["price_ticks"]) * MARKET_TICK
        else:
            raise ValueError
        post_only = row["post_only"]
        reduce_only = row["reduce_only"]
    except CoordinatorSafetyError:
        raise
    except Exception:
        raise CoordinatorSafetyError("RISEx REST order value rejected") from None
    order = RestOrder(
        order_id=order_id, wide_order_id=wide, resting_order_id=resting,
        client_order_id=client, market_id=market_id, account=account,
        side=side, order_type=order_type, time_in_force=tif, status=status,
        size=size, filled_size=filled, price=price, post_only=post_only,
        reduce_only=reduce_only, observed_at=observed_at,
    )
    _validate_order(order, identity, observed_at)
    return order


def _parse_trade_row(row: Mapping[str, Any], identity: RoleIdentity, observed_at: int) -> RestTrade:
    try:
        trade_id = row.get("id")
        order_id = row.get("order_id")
        if trade_id is None or order_id is None or "match_id" in row:
            raise ValueError
        trade = RestTrade(
            trade_id=str(trade_id), order_id=str(order_id),
            client_order_id=int(row["client_order_id"]), market_id=int(row["market_id"]),
            account=_address(
                row.get("wallet_address", row.get("sender", row.get("account")))
            ).lower(),
            side=_mapped_enum(row["side"], {0: "BUY", 1: "SELL"}, "trade side"),
            size=_decimal(row["size"], positive=True),
            price=_decimal(row["price"], positive=True), observed_at=observed_at,
        )
    except CoordinatorSafetyError:
        raise
    except Exception:
        raise CoordinatorSafetyError("RISEx REST trade value rejected") from None
    _validate_trade(trade, identity, observed_at)
    return trade


def _position_rows(value: Any, identity: RoleIdentity) -> tuple[tuple[int, Decimal], ...]:
    if isinstance(value, Mapping) and isinstance(value.get("positions"), list):
        rows = value["positions"]
    elif isinstance(value, list):
        rows = value
    else:
        raise CoordinatorSafetyError("RISEx REST positions schema rejected")
    result: list[tuple[int, Decimal]] = []
    seen: set[int] = set()
    for row in rows:
        if not isinstance(row, Mapping) or not {"account", "market_id", "size"} <= set(row):
            raise CoordinatorSafetyError("RISEx REST position row rejected")
        if _address(row["account"]).lower() != identity.account:
            raise CoordinatorSafetyError("RISEx REST position identity rejected")
        market_id = int(row["market_id"])
        size = _decimal(row["size"], nonnegative=False)
        if market_id <= 0 or market_id in seen or size % MARKET_STEP:
            raise CoordinatorSafetyError("RISEx REST position contradiction")
        seen.add(market_id)
        result.append((market_id, size))
    return tuple(result)


def _point_position(value: Any, identity: RoleIdentity) -> tuple[int, Decimal]:
    if isinstance(value, Mapping) and isinstance(value.get("position"), Mapping):
        value = value["position"]
    if not isinstance(value, Mapping) or not {"market_id", "size"} <= set(value):
        raise CoordinatorSafetyError("RISEx REST point position schema rejected")
    if "account" in value and _address(value["account"]).lower() != identity.account:
        raise CoordinatorSafetyError("RISEx REST point position identity rejected")
    try:
        market_id = int(value["market_id"])
    except Exception:
        raise CoordinatorSafetyError("RISEx REST point position schema rejected") from None
    size = _decimal(value["size"], nonnegative=False)
    if size % MARKET_STEP:
        raise CoordinatorSafetyError("RISEx REST point position grid rejected")
    if market_id == 0 and size == 0:
        return MARKET_ID, Decimal("0")
    if market_id != MARKET_ID:
        raise CoordinatorSafetyError("RISEx REST point position market rejected")
    return market_id, size


def _portfolio_position_rows(value: Any) -> tuple[tuple[int, Decimal], ...]:
    if not isinstance(value, list):
        raise CoordinatorSafetyError("RISEx portfolio positions schema rejected")
    result: list[tuple[int, Decimal]] = []
    seen: set[int] = set()
    for row in value:
        if not isinstance(row, Mapping) or not {"market_id", "size"} <= set(row):
            raise CoordinatorSafetyError("RISEx portfolio position row rejected")
        try:
            market_id = int(row["market_id"])
        except Exception:
            raise CoordinatorSafetyError("RISEx portfolio position row rejected") from None
        size = _decimal(row["size"], nonnegative=False)
        if market_id <= 0 or market_id in seen or size % MARKET_STEP:
            raise CoordinatorSafetyError("RISEx portfolio position row rejected")
        seen.add(market_id)
        result.append((market_id, size))
    return tuple(result)


def _normalize_position_map(
    rows: Sequence[tuple[int, Decimal]],
) -> dict[int, Decimal]:
    normalized: dict[int, Decimal] = {}
    for market_id, size in rows:
        if market_id <= 0 or market_id in normalized or not size.is_finite() or size % MARKET_STEP:
            raise CoordinatorSafetyError("RISEx position map rejected")
        normalized[market_id] = size
    return normalized


def _position_maps_agree(
    first: Mapping[int, Decimal], second: Mapping[int, Decimal],
) -> bool:
    return all(
        first.get(market_id, Decimal("0")) == second.get(market_id, Decimal("0"))
        for market_id in set(first) | set(second)
    )


def _parse_portfolio(
    value: Any, identity: RoleIdentity, observed_at: int,
) -> tuple[PortfolioState, tuple[tuple[int, Decimal], ...]]:
    if (
        not isinstance(value, Mapping)
        or value.get("account") != identity.account
        or not isinstance(value.get("summary"), Mapping)
    ):
        raise CoordinatorSafetyError("RISEx portfolio identity schema rejected")
    summary = value["summary"]
    required = {
        "usdc_balance", "free_collateral", "total_account_value",
        "in_liquidation", "risk_level",
    }
    if (
        not required <= set(summary)
        or summary["in_liquidation"] is not False
        or summary["risk_level"] != "NORMAL"
    ):
        raise CoordinatorSafetyError("RISEx portfolio risk state rejected")
    try:
        usdc_balance = _decimal(summary["usdc_balance"], positive=True)
        free_collateral = _decimal(summary["free_collateral"], positive=True)
        total_account_value = _decimal(summary["total_account_value"], positive=True)
    except CoordinatorSafetyError:
        raise
    positions_value = value.get("positions", summary.get("positions"))
    positions = _portfolio_position_rows(positions_value)
    portfolio = PortfolioState(
        account=identity.account, usdc_balance=usdc_balance,
        free_collateral=free_collateral, total_account_value=total_account_value,
        in_liquidation=False, risk_level="NORMAL", observed_at=observed_at,
    )
    return portfolio, positions


def _parse_private_snapshot(
    frame: Mapping[str, Any], *, channel: str, count_field: str,
    identity: RoleIdentity, observed_at: int,
) -> tuple[RestOrder, ...] | tuple[tuple[int, Decimal], ...]:
    if (
        frame.get("method") != "snapshot" or frame.get("channel") != channel
        or frame.get("type") != "snapshot" or not isinstance(frame.get("data"), list)
        or frame.get(count_field) != len(frame["data"])
        or not isinstance(frame.get("worker_timestamp"), str)
    ):
        raise CoordinatorSafetyError("RISEx private snapshot schema rejected")
    try:
        timestamp_ns = int(frame["worker_timestamp"])
    except Exception:
        raise CoordinatorSafetyError("RISEx private snapshot timestamp rejected") from None
    timestamp_seconds = timestamp_ns // 1_000_000_000
    if (
        timestamp_ns <= 0
        or timestamp_seconds > observed_at
        or observed_at - timestamp_seconds > MAX_AGE_SECONDS
    ):
        raise CoordinatorSafetyError("RISEx private snapshot stale")
    if channel == "orders":
        return tuple(_parse_order_row(row, identity, observed_at) for row in frame["data"])
    positions: list[tuple[int, Decimal]] = []
    seen: set[int] = set()
    for row in frame["data"]:
        if not isinstance(row, Mapping) or not {"account", "market_id", "size"} <= set(row):
            raise CoordinatorSafetyError("RISEx private position schema rejected")
        if _address(row["account"]).lower() != identity.account:
            raise CoordinatorSafetyError("RISEx private position identity rejected")
        market_id = _private_market_id(row["market_id"])
        if market_id <= 0 or market_id in seen:
            raise CoordinatorSafetyError("RISEx private position contradiction")
        seen.add(market_id)
        positions.append((market_id, _decimal(row["size"], nonnegative=False)))
    _validate_private_position_rows(
        tuple(positions), allow_unrelated_zero=True, require_unique=True,
    )
    return tuple(positions)


def _parse_private_update(
    frame: Mapping[str, Any], *, channel: str, identity: RoleIdentity, observed_at: int,
) -> tuple[RestOrder, ...] | tuple[tuple[int, Decimal], ...]:
    if (
        frame.get("channel") != channel or frame.get("type") != "update"
        or not isinstance(frame.get("data"), list)
        or not isinstance(frame.get("worker_timestamp"), str)
    ):
        raise CoordinatorSafetyError("RISEx private update schema rejected")
    if _private_market_id(frame.get("market_id")) != MARKET_ID:
        raise CoordinatorSafetyError("RISEx private update schema rejected")
    try:
        timestamp_ns = int(frame["worker_timestamp"])
    except Exception:
        raise CoordinatorSafetyError("RISEx private update timestamp rejected") from None
    timestamp_seconds = timestamp_ns // 1_000_000_000
    if (
        timestamp_ns <= 0
        or timestamp_seconds > observed_at
        or observed_at - timestamp_seconds > MAX_AGE_SECONDS
    ):
        raise CoordinatorSafetyError("RISEx private update stale")
    if channel == "orders":
        return tuple(_parse_order_row(row, identity, observed_at) for row in frame["data"])
    positions: list[tuple[int, Decimal]] = []
    for row in frame["data"]:
        if not isinstance(row, Mapping) or not {"account", "market_id", "size"} <= set(row):
            raise CoordinatorSafetyError("RISEx private position update rejected")
        if _address(row["account"]).lower() != identity.account:
            raise CoordinatorSafetyError("RISEx private position identity rejected")
        market_id = _private_market_id(row["market_id"])
        if market_id != MARKET_ID:
            raise CoordinatorSafetyError("RISEx private position market rejected")
        positions.append((market_id, _decimal(row["size"], nonnegative=False)))
    _validate_private_position_rows(
        tuple(positions), allow_unrelated_zero=False, require_unique=True,
    )
    return tuple(positions)


@dataclass(frozen=True)
class IntentSpec:
    step: str
    side: str
    order_type: str
    time_in_force: str
    reduce_only: bool
    post_only: bool
    market_id: int
    size: Decimal
    price: Decimal
    source_position: Decimal
    client_order_id: int
    nonce_anchor: int
    nonce_bitmap: int
    expires_at: int
    bbo_digest: str


@dataclass(frozen=True)
class DurableIntent:
    intent_id: str
    ordinal: int
    step: str
    client_order_id: int
    nonce_anchor: int
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
    price: Decimal
    source_position: Decimal
    expires_at: int
    dispatch_count: int
    order_id: str | None
    filled_size: Decimal | None
    reconciled: bool


@dataclass(frozen=True)
class DurableCancel:
    cancel_id: str
    intent_id: str
    order_id: str
    market_id: int
    resting_order_id: int
    nonce_anchor: int
    nonce_bitmap: int
    payload_digest: str
    expires_at: int
    state: str
    dispatch_count: int


_JOURNAL_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS intents (
    intent_id TEXT PRIMARY KEY,
    ordinal INTEGER NOT NULL UNIQUE,
    step TEXT NOT NULL UNIQUE,
    client_order_id TEXT NOT NULL UNIQUE,
    nonce_anchor INTEGER NOT NULL,
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
    price TEXT NOT NULL,
    source_position TEXT NOT NULL,
    expires_at INTEGER NOT NULL,
    dispatch_count INTEGER NOT NULL DEFAULT 0,
    order_id TEXT UNIQUE,
    filled_size TEXT,
    reconciled INTEGER NOT NULL DEFAULT 0,
    UNIQUE(nonce_anchor, nonce_bitmap)
);
CREATE TABLE IF NOT EXISTS cancels (
    cancel_id TEXT PRIMARY KEY,
    intent_id TEXT NOT NULL UNIQUE,
    order_id TEXT NOT NULL UNIQUE,
    market_id INTEGER NOT NULL,
    resting_order_id INTEGER NOT NULL,
    nonce_anchor INTEGER NOT NULL,
    nonce_bitmap INTEGER NOT NULL,
    payload_digest TEXT NOT NULL UNIQUE,
    expires_at INTEGER NOT NULL,
    state TEXT NOT NULL,
    dispatch_count INTEGER NOT NULL DEFAULT 0,
    UNIQUE(nonce_anchor, nonce_bitmap)
);
CREATE TABLE IF NOT EXISTS terminal (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def _safe_file(path: Path) -> bool:
    try:
        details = path.lstat()
    except OSError:
        return False
    return (
        path.is_absolute()
        and stat.S_ISREG(details.st_mode)
        and not path.is_symlink()
        and details.st_uid == os.getuid()
        and stat.S_IMODE(details.st_mode) == 0o600
        and details.st_nlink == 1
    )


def _fsync_file_and_parent(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _ensure_sqlite_file(path: Path) -> None:
    if not path.is_absolute() or not path.parent.is_dir():
        raise CoordinatorSafetyError("RISEx journal path rejected")
    if not path.exists():
        try:
            fd = os.open(
                path,
                os.O_CREAT | os.O_EXCL | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            os.fchmod(fd, 0o600)
            os.fsync(fd)
            os.close(fd)
            directory = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except OSError:
            raise CoordinatorSafetyError("RISEx journal path rejected") from None
    if not _safe_file(path):
        raise CoordinatorSafetyError("RISEx journal path rejected")


class PairJournal:
    """One role's durable run and write-identity domain."""

    def __init__(self, path: str | Path, identity: RoleIdentity) -> None:
        identity.validate()
        self.path = Path(path)
        _ensure_sqlite_file(self.path)
        self._db = sqlite3.connect(self.path)
        self._db.execute("PRAGMA foreign_keys=ON")
        self._db.execute("PRAGMA journal_mode=DELETE")
        self._db.execute("PRAGMA synchronous=FULL")
        try:
            self._db.executescript(_JOURNAL_SCHEMA)
            self._bind(identity)
            self._validate_schema()
        except (sqlite3.DatabaseError, CoordinatorSafetyError):
            self._db.close()
            raise CoordinatorSafetyError("RISEx journal rejected") from None

    def close(self) -> None:
        self._db.close()

    def _bind(self, identity: RoleIdentity) -> None:
        expected = {
            "role": identity.role.value,
            "account": identity.account,
            "signer": identity.signer,
        }
        rows = dict(self._db.execute("SELECT key,value FROM meta"))
        if not rows:
            run_id = f"risex-two-{identity.role.value.lower()}-{secrets.token_hex(16)}"
            with self._db:
                self._db.executemany(
                    "INSERT INTO meta(key,value) VALUES (?,?)",
                    (*expected.items(), ("run_id", run_id), ("phase", Phase.START.value),
                     ("outcome", "ACTIVE")),
                )
            _fsync_file_and_parent(self.path)
            return
        if any(rows.get(key) != value for key, value in expected.items()):
            raise CoordinatorSafetyError("RISEx journal identity rejected")
        if set(rows) != {"role", "account", "signer", "run_id", "phase", "outcome"}:
            raise CoordinatorSafetyError("RISEx journal metadata rejected")
        try:
            AccountRole(rows["role"])
            Phase(rows["phase"])
            if rows["outcome"] not in {"ACTIVE", "COMPLETE", "HALTED"}:
                raise ValueError
        except ValueError:
            raise CoordinatorSafetyError("RISEx journal metadata rejected") from None

    def _validate_schema(self) -> None:
        if self._db.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
            raise CoordinatorSafetyError("RISEx journal integrity rejected")
        if self._db.execute("PRAGMA foreign_key_check").fetchall():
            raise CoordinatorSafetyError("RISEx journal foreign key rejected")
        tables = tuple(self._db.execute(
            "SELECT type,name,tbl_name,COALESCE(sql,'') FROM sqlite_master "
            "WHERE type IN ('table','index') ORDER BY type,name,tbl_name,sql"
        ))
        expected = sqlite3.connect(":memory:")
        try:
            expected.executescript(_JOURNAL_SCHEMA)
            expected_tables = tuple(expected.execute(
                "SELECT type,name,tbl_name,COALESCE(sql,'') FROM sqlite_master "
                "WHERE type IN ('table','index') ORDER BY type,name,tbl_name,sql"
            ))
        finally:
            expected.close()
        if tables != expected_tables:
            raise CoordinatorSafetyError("RISEx journal schema rejected")

    @property
    def identity(self) -> RoleIdentity:
        rows = dict(self._db.execute("SELECT key,value FROM meta"))
        return RoleIdentity(
            AccountRole(rows["role"]), rows["account"], rows["signer"],
            "", "", "",
        )

    @property
    def run_id(self) -> str:
        row = self._db.execute("SELECT value FROM meta WHERE key='run_id'").fetchone()
        if row is None or not row[0]:
            raise CoordinatorSafetyError("RISEx journal run identity rejected")
        return str(row[0])

    @property
    def phase(self) -> Phase:
        row = self._db.execute("SELECT value FROM meta WHERE key='phase'").fetchone()
        if row is None:
            raise CoordinatorSafetyError("RISEx journal phase rejected")
        try:
            return Phase(row[0])
        except ValueError:
            raise CoordinatorSafetyError("RISEx journal phase rejected") from None

    @property
    def outcome(self) -> str:
        row = self._db.execute("SELECT value FROM meta WHERE key='outcome'").fetchone()
        if row is None or row[0] not in {"ACTIVE", "COMPLETE", "HALTED"}:
            raise CoordinatorSafetyError("RISEx journal outcome rejected")
        return str(row[0])

    def set_phase(self, phase: Phase) -> None:
        if not isinstance(phase, Phase):
            raise CoordinatorSafetyError("RISEx journal phase rejected")
        with self._db:
            self._db.execute("UPDATE meta SET value=? WHERE key='phase'", (phase.value,))
        _fsync_file_and_parent(self.path)

    def set_outcome(self, outcome: str) -> None:
        if outcome not in {"ACTIVE", "COMPLETE", "HALTED"}:
            raise CoordinatorSafetyError("RISEx journal outcome rejected")
        with self._db:
            self._db.execute("UPDATE meta SET value=? WHERE key='outcome'", (outcome,))
        _fsync_file_and_parent(self.path)

    def terminal(self, key: str) -> str | None:
        row = self._db.execute("SELECT value FROM terminal WHERE key=?", (key,)).fetchone()
        return None if row is None else str(row[0])

    def set_terminal(self, key: str, value: str) -> None:
        if not key or not value:
            raise CoordinatorSafetyError("RISEx terminal evidence rejected")
        current = self.terminal(key)
        if current is not None and current != value:
            raise CoordinatorSafetyError("RISEx terminal evidence contradiction")
        if current == value:
            return
        with self._db:
            self._db.execute(
                "INSERT INTO terminal(key,value) VALUES (?,?)",
                (key, value),
            )
        _fsync_file_and_parent(self.path)

    def intents(self) -> tuple[DurableIntent, ...]:
        rows = self._db.execute("SELECT * FROM intents ORDER BY ordinal").fetchall()
        return tuple(_intent_from_row(row) for row in rows)

    def intent(self, intent_id: str) -> DurableIntent:
        row = self._db.execute("SELECT * FROM intents WHERE intent_id=?", (intent_id,)).fetchone()
        if row is None:
            raise CoordinatorSafetyError("RISEx intent identity rejected")
        return _intent_from_row(row)

    def by_step(self, step: str) -> DurableIntent | None:
        row = self._db.execute("SELECT * FROM intents WHERE step=?", (step,)).fetchone()
        return None if row is None else _intent_from_row(row)

    def prepare(self, spec: IntentSpec) -> DurableIntent:
        spec = _validate_intent_spec(spec)
        if self.by_step(spec.step) is not None:
            raise CoordinatorSafetyError("RISEx intent replay rejected")
        ordinal = len(self.intents()) + 1
        action_data = {
            "step": spec.step,
            "side": spec.side,
            "order_type": spec.order_type,
            "time_in_force": spec.time_in_force,
            "reduce_only": spec.reduce_only,
            "post_only": spec.post_only,
            "market_id": spec.market_id,
            "size": str(spec.size),
            "price": str(spec.price),
            "source_position": str(spec.source_position),
            "client_order_id": spec.client_order_id,
            "nonce_anchor": spec.nonce_anchor,
            "nonce_bitmap": spec.nonce_bitmap,
            "expires_at": spec.expires_at,
        }
        payload_digest = _canonical_digest(action_data)
        intent = DurableIntent(
            intent_id=str(uuid.uuid4()), ordinal=ordinal, step=spec.step,
            client_order_id=spec.client_order_id, nonce_anchor=spec.nonce_anchor,
            nonce_bitmap=spec.nonce_bitmap, payload_digest=payload_digest,
            bbo_digest=spec.bbo_digest, state="PREPARED", side=spec.side,
            order_type=spec.order_type, time_in_force=spec.time_in_force,
            reduce_only=spec.reduce_only, post_only=spec.post_only,
            market_id=spec.market_id, size=spec.size, price=spec.price,
            source_position=spec.source_position, expires_at=spec.expires_at,
            dispatch_count=0, order_id=None, filled_size=None, reconciled=False,
        )
        try:
            with self._db:
                self._db.execute(
                    "INSERT INTO intents VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        intent.intent_id, intent.ordinal, intent.step,
                        str(intent.client_order_id), intent.nonce_anchor,
                        intent.nonce_bitmap, intent.payload_digest, intent.bbo_digest,
                        intent.state, intent.side, intent.order_type,
                        intent.time_in_force, int(intent.reduce_only), int(intent.post_only),
                        intent.market_id, str(intent.size), str(intent.price),
                        str(intent.source_position), intent.expires_at,
                        intent.dispatch_count, intent.order_id, None, int(intent.reconciled),
                    ),
                )
        except sqlite3.IntegrityError:
            raise CoordinatorSafetyError("RISEx intent identity rejected") from None
        _fsync_file_and_parent(self.path)
        return intent

    def mark_dispatching(self, intent_id: str) -> DurableIntent:
        with self._db:
            changed = self._db.execute(
                "UPDATE intents SET state='DISPATCHING',dispatch_count=1 "
                "WHERE intent_id=? AND state='PREPARED' AND dispatch_count=0",
                (intent_id,),
            ).rowcount
        if changed != 1:
            raise CoordinatorSafetyError("RISEx write replay rejected")
        _fsync_file_and_parent(self.path)
        return self.intent(intent_id)

    def record_place_result(self, intent_id: str, result: WriteResult) -> None:
        current = self.intent(intent_id)
        if current.dispatch_count != 1 or current.state != "DISPATCHING":
            raise CoordinatorSafetyError("RISEx place result replay rejected")
        if not isinstance(result, WriteResult):
            raise CoordinatorSafetyError("RISEx place result rejected")
        if result.result_class is WriteResultClass.ACCEPTED:
            if result.order_id is None or not _valid_order_id(result.order_id):
                raise CoordinatorSafetyError("RISEx place order identity rejected")
            state = "DISPATCHED"
        elif result.result_class is WriteResultClass.TERMINAL_VENUE_REJECTION:
            state = "VENUE_REJECTED"
        else:
            state = "AMBIGUOUS"
        if result.failure_code is not None and (
            not result.failure_code
            or len(result.failure_code) > 64
            or any(character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ_0123456789" for character in result.failure_code)
        ):
            raise CoordinatorSafetyError("RISEx place result code rejected")
        with self._db:
            self._db.execute(
                "UPDATE intents SET state=?,order_id=? WHERE intent_id=? AND state='DISPATCHING'",
                (state, result.order_id, intent_id),
            )
            self._db.execute(
                "INSERT OR REPLACE INTO terminal(key,value) VALUES (?,?)",
                (f"place:{intent_id}", result.result_class.value),
            )
            if result.failure_code is not None:
                self._db.execute(
                    "INSERT OR REPLACE INTO terminal(key,value) VALUES (?,?)",
                    (f"place_failure:{intent_id}", result.failure_code),
                )
        _fsync_file_and_parent(self.path)

    def reconcile_intent(self, intent_id: str, *, filled_size: Decimal, state: str = "TERMINAL") -> DurableIntent:
        current = self.intent(intent_id)
        if current.dispatch_count != 1:
            raise CoordinatorSafetyError("RISEx intent reconciliation rejected")
        if state not in {"RESTING", "TERMINAL"}:
            raise CoordinatorSafetyError("RISEx intent reconciliation rejected")
        filled_size = _decimal(filled_size, nonnegative=True)
        if filled_size > current.size:
            raise CoordinatorSafetyError("RISEx intent fill rejected")
        if current.state == "TERMINAL":
            if state != "TERMINAL" or current.filled_size != filled_size:
                raise CoordinatorSafetyError("RISEx intent reconciliation contradiction")
            return current
        if current.state not in {"DISPATCHED", "DISPATCHING", "RESTING"}:
            raise CoordinatorSafetyError("RISEx intent reconciliation rejected")
        with self._db:
            self._db.execute(
                "UPDATE intents SET state=?,filled_size=?,reconciled=? WHERE intent_id=?",
                (state, str(filled_size), int(state == "TERMINAL"), intent_id),
            )
        _fsync_file_and_parent(self.path)
        return self.intent(intent_id)

    def cancels(self) -> tuple[DurableCancel, ...]:
        return tuple(_cancel_from_row(row) for row in self._db.execute(
            "SELECT * FROM cancels ORDER BY rowid"
        ))

    def prepare_cancel(
        self, intent: DurableIntent, *, nonce_anchor: int, nonce_bitmap: int,
        expires_at: int,
    ) -> DurableCancel:
        if intent.order_id is None or intent.state not in {"RESTING", "TERMINAL"}:
            raise CoordinatorSafetyError("RISEx residue cancel identity rejected")
        _uint(nonce_anchor, 48, "nonce_anchor")
        _uint(nonce_bitmap, 8, "nonce_bitmap")
        if nonce_bitmap > 207 or expires_at <= 0:
            raise CoordinatorSafetyError("RISEx residue cancel identity rejected")
        if self.cancels():
            raise CoordinatorSafetyError("RISEx residue cancel replay rejected")
        record = DurableCancel(
            cancel_id=str(uuid.uuid4()), intent_id=intent.intent_id,
            order_id=intent.order_id, market_id=intent.market_id,
            resting_order_id=intent.order_id and (_wide_order_id(intent.order_id) >> 1),
            nonce_anchor=nonce_anchor, nonce_bitmap=nonce_bitmap,
            payload_digest=_canonical_digest({
                "action": CANCEL_ACTION, "order_id": intent.order_id,
                "market_id": intent.market_id, "nonce_anchor": nonce_anchor,
                "nonce_bitmap": nonce_bitmap, "expires_at": expires_at,
            }),
            expires_at=expires_at, state="PREPARED", dispatch_count=0,
        )
        try:
            with self._db:
                self._db.execute(
                    "INSERT INTO cancels VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        record.cancel_id, record.intent_id, record.order_id,
                        record.market_id, record.resting_order_id,
                        record.nonce_anchor, record.nonce_bitmap,
                        record.payload_digest, record.expires_at,
                        record.state, record.dispatch_count,
                    ),
                )
        except sqlite3.IntegrityError:
            raise CoordinatorSafetyError("RISEx residue cancel identity rejected") from None
        _fsync_file_and_parent(self.path)
        return record

    def mark_cancel_dispatching(self, cancel_id: str) -> DurableCancel:
        with self._db:
            changed = self._db.execute(
                "UPDATE cancels SET state='DISPATCHING',dispatch_count=1 "
                "WHERE cancel_id=? AND state='PREPARED' AND dispatch_count=0",
                (cancel_id,),
            ).rowcount
        if changed != 1:
            raise CoordinatorSafetyError("RISEx cancel replay rejected")
        _fsync_file_and_parent(self.path)
        return next(item for item in self.cancels() if item.cancel_id == cancel_id)

    def record_cancel_result(self, cancel_id: str, result: WriteResult) -> None:
        current = next((item for item in self.cancels() if item.cancel_id == cancel_id), None)
        if current is None or current.dispatch_count != 1 or current.state != "DISPATCHING":
            raise CoordinatorSafetyError("RISEx cancel result rejected")
        state = "DISPATCHED" if result.result_class is WriteResultClass.ACCEPTED else "AMBIGUOUS"
        if result.result_class is WriteResultClass.TERMINAL_VENUE_REJECTION:
            state = "VENUE_REJECTED"
        with self._db:
            self._db.execute("UPDATE cancels SET state=? WHERE cancel_id=?", (state, cancel_id))
            self._db.execute(
                "INSERT OR REPLACE INTO terminal(key,value) VALUES (?,?)",
                (f"cancel:{cancel_id}", result.result_class.value),
            )
        _fsync_file_and_parent(self.path)

    def reconcile_cancel(self, cancel_id: str) -> DurableCancel:
        current = next((item for item in self.cancels() if item.cancel_id == cancel_id), None)
        if current is None or current.dispatch_count != 1 or current.state not in {"DISPATCHED", "DISPATCHING"}:
            raise CoordinatorSafetyError("RISEx cancel reconciliation rejected")
        with self._db:
            self._db.execute("UPDATE cancels SET state='TERMINAL' WHERE cancel_id=?", (cancel_id,))
        _fsync_file_and_parent(self.path)
        return next(item for item in self.cancels() if item.cancel_id == cancel_id)

    def pending_writes(self) -> tuple[DurableIntent | DurableCancel, ...]:
        values: list[DurableIntent | DurableCancel] = [
            item for item in self.intents() if item.state in {"DISPATCHING", "AMBIGUOUS"}
        ]
        values.extend(item for item in self.cancels() if item.state in {"DISPATCHING", "AMBIGUOUS"})
        return tuple(values)

    def dispatch_count(self) -> int:
        return sum(item.dispatch_count for item in self.intents())


def _intent_from_row(row: Sequence[Any]) -> DurableIntent:
    return DurableIntent(
        intent_id=str(row[0]), ordinal=int(row[1]), step=str(row[2]),
        client_order_id=int(row[3]), nonce_anchor=int(row[4]), nonce_bitmap=int(row[5]),
        payload_digest=str(row[6]), bbo_digest=str(row[7]), state=str(row[8]),
        side=str(row[9]), order_type=str(row[10]), time_in_force=str(row[11]),
        reduce_only=bool(row[12]), post_only=bool(row[13]), market_id=int(row[14]),
        size=Decimal(str(row[15])), price=Decimal(str(row[16])),
        source_position=Decimal(str(row[17])), expires_at=int(row[18]),
        dispatch_count=int(row[19]), order_id=None if row[20] is None else str(row[20]),
        filled_size=None if row[21] is None else Decimal(str(row[21])),
        reconciled=bool(row[22]),
    )


def _cancel_from_row(row: Sequence[Any]) -> DurableCancel:
    return DurableCancel(
        cancel_id=str(row[0]), intent_id=str(row[1]), order_id=str(row[2]),
        market_id=int(row[3]), resting_order_id=int(row[4]), nonce_anchor=int(row[5]),
        nonce_bitmap=int(row[6]), payload_digest=str(row[7]), expires_at=int(row[8]),
        state=str(row[9]), dispatch_count=int(row[10]),
    )


def _validate_intent_spec(spec: IntentSpec) -> IntentSpec:
    if (
        not spec.step
        or spec.side not in {"BUY", "SELL"}
        or spec.order_type not in {"MARKET", "LIMIT"}
        or spec.time_in_force not in {"GTC", "IOC"}
        or type(spec.reduce_only) is not bool
        or type(spec.post_only) is not bool
        or spec.market_id != MARKET_ID
        or not _aligned(spec.size, MARKET_STEP)
        or spec.size < MARKET_MINIMUM
        or spec.price <= 0
        or spec.price % MARKET_TICK
        or type(spec.client_order_id) is not int
        or not 0 < spec.client_order_id < 2**64
        or type(spec.nonce_anchor) is not int
        or not 0 <= spec.nonce_anchor < 2**48
        or type(spec.nonce_bitmap) is not int
        or not 0 <= spec.nonce_bitmap <= 207
        or type(spec.expires_at) is not int
        or spec.expires_at <= 0
        or not isinstance(spec.bbo_digest, str)
        or len(spec.bbo_digest) != 64
    ):
        raise CoordinatorSafetyError("RISEx intent contract rejected")
    if spec.order_type == "MARKET" and spec.post_only:
        raise CoordinatorSafetyError("RISEx market post-only rejected")
    if spec.order_type == "LIMIT" and not spec.post_only and spec.time_in_force == "GTC":
        raise CoordinatorSafetyError("RISEx limit liquidity contract rejected")
    if spec.reduce_only and spec.source_position == 0:
        raise CoordinatorSafetyError("RISEx reduce-only source rejected")
    return spec


def _permit_typed_data(identity: RoleIdentity, market: MarketObservation, action_hash: bytes,
                       nonce_anchor: int, nonce_bitmap: int, deadline: int) -> dict[str, Any]:
    return verify_witness_typed_data(
        account=identity.account,
        market=market_for_contract(market),
        action_hash=action_hash,
        nonce_anchor=nonce_anchor,
        nonce_bitmap=nonce_bitmap,
        deadline=deadline,
    )


def market_for_contract(market: MarketObservation) -> Any:
    """Adapt the new observation to the accepted pure contract primitive."""
    from .testnet_risex_order_lifecycle import MarketState

    return MarketState(
        host=market.host,
        chain_id=market.chain_id,
        domain_name=market.domain_name,
        domain_version=market.domain_version,
        router=market.router,
        authorization=market.authorization,
        market_id=market.market_id,
        symbol=market.symbol,
        active=market.active,
        unlocked=market.unlocked,
        tick=market.tick,
        step=market.step,
        minimum=market.minimum,
        observed_at=market.observed_at,
    )


def _fixed_contract_market() -> Any:
    from .testnet_risex_order_lifecycle import MarketState

    return MarketState(
        host=OFFICIAL_HOST,
        chain_id=OFFICIAL_CHAIN_ID,
        domain_name=OFFICIAL_DOMAIN_NAME,
        domain_version=OFFICIAL_DOMAIN_VERSION,
        router=ROUTER,
        authorization=AUTHORIZATION,
        market_id=MARKET_ID,
        symbol=MARKET_SYMBOL,
        active=True,
        unlocked=True,
        tick=MARKET_TICK,
        step=MARKET_STEP,
        minimum=MARKET_MINIMUM,
        observed_at=0,
    )


def _expected_permit(
    identity: RoleIdentity, action_hash: bytes,
    nonce_anchor: int, nonce_bitmap: int, deadline: int,
) -> dict[str, Any]:
    try:
        return verify_witness_typed_data(
            account=identity.account,
            market=_fixed_contract_market(),
            action_hash=action_hash,
            nonce_anchor=nonce_anchor,
            nonce_bitmap=nonce_bitmap,
            deadline=deadline,
        )
    except Exception:
        raise CoordinatorSafetyError("RISEx permit binding rejected") from None


def _wire_uint(value: Any, bits: int, label: str) -> int:
    try:
        return _uint(value, bits, label)
    except Exception:
        raise CoordinatorSafetyError("RISEx wire integer rejected") from None


def _wire_nonce_anchor(value: Any) -> int:
    if not isinstance(value, str) or not value or str(int(value)) != value:
        raise CoordinatorSafetyError("RISEx nonce anchor binding rejected")
    return _wire_uint(int(value), 48, "nonce_anchor")


def _wire_deadline(value: Any) -> int:
    deadline = _wire_uint(value, 32, "deadline")
    if deadline == 0:
        raise CoordinatorSafetyError("RISEx deadline binding rejected")
    return deadline


def unsigned_place_request(
    intent: DurableIntent, *, identity: RoleIdentity, market: MarketObservation,
) -> dict[str, Any]:
    if intent.order_id is not None or intent.state not in {"PREPARED", "DISPATCHING"}:
        raise CoordinatorSafetyError("RISEx unsigned place request rejected")
    order_data = pack_order_data(
        market_id=intent.market_id,
        size_steps=int(intent.size / market.step),
        price_ticks=int(intent.price / market.tick),
        side=intent.side,
        post_only=intent.post_only,
        reduce_only=intent.reduce_only,
        order_type=intent.order_type,
        time_in_force=intent.time_in_force,
    )
    encoded, action_hash = encode_place_action(
        order_data=order_data, client_order_id=intent.client_order_id,
    )
    if action_hash.hex() != intent.payload_digest[:64]:
        # The journal digest is over the complete intent; retaining an explicit
        # action digest in the request still lets the signer/transport bind the
        # exact pure encoding without storing the payload durably.
        action_digest = action_hash.hex()
    else:
        action_digest = action_hash.hex()
    permit = _permit_typed_data(
        identity, market, action_hash, intent.nonce_anchor,
        intent.nonce_bitmap, intent.expires_at,
    )
    body = {
        "market_id": intent.market_id,
        "size_steps": int(intent.size / market.step),
        "price_ticks": int(intent.price / market.tick),
        "side": {"BUY": 0, "SELL": 1}[intent.side],
        "order_type": {"MARKET": 0, "LIMIT": 1}[intent.order_type],
        "time_in_force": {"GTC": 0, "IOC": 3}[intent.time_in_force],
        "post_only": intent.post_only,
        "reduce_only": intent.reduce_only,
        "stp_mode": 0,
        "client_order_id": intent.client_order_id,
        "account": identity.account,
        "signer": identity.signer,
        "nonce_anchor": str(intent.nonce_anchor),
        "nonce_bitmap_index": intent.nonce_bitmap,
        "deadline": intent.expires_at,
    }
    return {
        "action": PLACE_ACTION,
        "header_flags": HEADER_FLAGS,
        "order_data": order_data,
        "abi_encoded": encoded,
        "action_hash": action_hash,
        "action_digest": action_digest,
        "permit": permit,
        "body": body,
        "signature": None,
        "dispatchable": False,
    }


def unsigned_cancel_request(
    cancel: DurableCancel, *, identity: RoleIdentity, market: MarketObservation,
) -> dict[str, Any]:
    encoded, action_hash = encode_cancel_action(
        market_id=cancel.market_id, resting_order_id=cancel.resting_order_id,
    )
    permit = _permit_typed_data(
        identity, market, action_hash, cancel.nonce_anchor,
        cancel.nonce_bitmap, cancel.expires_at,
    )
    return {
        "action": CANCEL_ACTION,
        "market_id": cancel.market_id,
        "resting_order_id": cancel.resting_order_id,
        "abi_encoded": encoded,
        "action_hash": action_hash,
        "permit": permit,
        "body": {
            "market_id": cancel.market_id,
            "order_id": cancel.order_id,
            "permit": {
                "account": identity.account,
                "signer": identity.signer,
                "nonce_anchor": str(cancel.nonce_anchor),
                "nonce_bitmap_index": cancel.nonce_bitmap,
                "deadline": cancel.expires_at,
                "signature": None,
            },
        },
        "signature": None,
        "dispatchable": False,
    }


def _validate_unsigned_place(request: Mapping[str, Any], identity: RoleIdentity) -> None:
    try:
        if (
            set(request) != {
                "action", "header_flags", "order_data", "abi_encoded", "action_hash",
                "action_digest", "permit", "body", "signature", "dispatchable",
            }
            or request["action"] != PLACE_ACTION
            or request["header_flags"] != HEADER_FLAGS
            or request["signature"] is not None
            or request["dispatchable"] is not False
            or type(request["order_data"]) is not int
            or type(request["abi_encoded"]) is not bytes
            or type(request["action_hash"]) is not bytes
            or len(request["action_hash"]) != 32
            or type(request["action_digest"]) is not str
            or request["action_digest"] != request["action_hash"].hex()
        ):
            raise CoordinatorSafetyError("RISEx place request binding rejected")
        body = request["body"]
        if (
            not isinstance(body, Mapping)
            or set(body) != {
                "market_id", "size_steps", "price_ticks", "side", "order_type",
                "time_in_force", "post_only", "reduce_only", "stp_mode",
                "client_order_id", "account", "signer", "nonce_anchor",
                "nonce_bitmap_index", "deadline",
            }
            or type(body["market_id"]) is not int
            or body["market_id"] != MARKET_ID
            or type(body["size_steps"]) is not int
            or type(body["price_ticks"]) is not int
            or type(body["side"]) is not int
            or type(body["order_type"]) is not int
            or type(body["time_in_force"]) is not int
            or type(body["post_only"]) is not bool
            or type(body["reduce_only"]) is not bool
            or type(body["stp_mode"]) is not int
            or body["stp_mode"] != 0
            or type(body["client_order_id"]) is not int
            or type(body["account"]) is not str
            or body["account"] != identity.account
            or type(body["signer"]) is not str
            or body["signer"] != identity.signer
        ):
            raise CoordinatorSafetyError("RISEx place identity binding rejected")
        size_steps = _wire_uint(body["size_steps"], 32, "size_steps")
        if size_steps < int(MARKET_MINIMUM / MARKET_STEP):
            raise CoordinatorSafetyError("RISEx place size binding rejected")
        price_ticks = _wire_uint(body["price_ticks"], 24, "price_ticks")
        if price_ticks == 0:
            raise CoordinatorSafetyError("RISEx place price binding rejected")
        side = {0: "BUY", 1: "SELL"}.get(body["side"])
        order_type = {0: "MARKET", 1: "LIMIT"}.get(body["order_type"])
        time_in_force = {0: "GTC", 3: "IOC"}.get(body["time_in_force"])
        if side is None or order_type is None or time_in_force is None:
            raise CoordinatorSafetyError("RISEx place enum binding rejected")
        if (
            (order_type == "MARKET" and (time_in_force != "IOC" or body["post_only"]))
            or (order_type == "LIMIT" and (time_in_force != "GTC" or not body["post_only"]))
        ):
            raise CoordinatorSafetyError("RISEx place liquidity binding rejected")
        client_order_id = _wire_uint(body["client_order_id"], 64, "client_order_id")
        if client_order_id == 0:
            raise CoordinatorSafetyError("RISEx place client binding rejected")
        nonce_anchor = _wire_nonce_anchor(body["nonce_anchor"])
        nonce_bitmap = _wire_uint(body["nonce_bitmap_index"], 8, "nonce_bitmap")
        if nonce_bitmap > 207:
            raise CoordinatorSafetyError("RISEx place nonce binding rejected")
        deadline = _wire_deadline(body["deadline"])
        order_data = pack_order_data(
            market_id=MARKET_ID, size_steps=size_steps, price_ticks=price_ticks,
            side=side, post_only=body["post_only"], reduce_only=body["reduce_only"],
            order_type=order_type, time_in_force=time_in_force,
        )
        encoded, action_hash = encode_place_action(
            order_data=order_data, client_order_id=client_order_id,
        )
        expected_permit = _expected_permit(
            identity, action_hash, nonce_anchor, nonce_bitmap, deadline,
        )
        if (
            request["order_data"] != order_data
            or request["abi_encoded"] != encoded
            or request["action_hash"] != action_hash
            or request["permit"] != expected_permit
            or body["nonce_anchor"] != str(expected_permit["message"]["nonceAnchor"])
            or body["nonce_bitmap_index"] != expected_permit["message"]["nonceBitmap"]
            or body["deadline"] != expected_permit["message"]["deadline"]
        ):
            raise CoordinatorSafetyError("RISEx place canonical binding rejected")
    except CoordinatorSafetyError:
        raise
    except Exception:
        raise CoordinatorSafetyError("RISEx place request binding rejected") from None


def _validate_unsigned_cancel(request: Mapping[str, Any], identity: RoleIdentity) -> None:
    try:
        if (
            set(request) != {
                "action", "market_id", "resting_order_id", "abi_encoded", "action_hash",
                "permit", "body", "signature", "dispatchable",
            }
            or request["action"] != CANCEL_ACTION
            or request["signature"] is not None
            or request["dispatchable"] is not False
            or type(request["market_id"]) is not int
            or type(request["resting_order_id"]) is not int
            or type(request["abi_encoded"]) is not bytes
            or type(request["action_hash"]) is not bytes
            or len(request["action_hash"]) != 32
        ):
            raise CoordinatorSafetyError("RISEx cancel request binding rejected")
        market_id = _wire_uint(request["market_id"], 16, "market_id")
        if market_id != MARKET_ID:
            raise CoordinatorSafetyError("RISEx cancel market binding rejected")
        resting_order_id = _wire_uint(request["resting_order_id"], 64, "resting_order_id")
        body = request["body"]
        permit = body.get("permit") if isinstance(body, Mapping) else None
        if (
            not isinstance(body, Mapping)
            or set(body) != {"market_id", "order_id", "permit"}
            or body["market_id"] != market_id
            or not isinstance(body["order_id"], str)
            or not _valid_order_id(body["order_id"])
            or not isinstance(permit, Mapping)
            or set(permit) != {
                "account", "signer", "nonce_anchor", "nonce_bitmap_index",
                "deadline", "signature",
            }
            or permit["account"] != identity.account
            or permit["signer"] != identity.signer
            or permit["signature"] is not None
        ):
            raise CoordinatorSafetyError("RISEx cancel identity binding rejected")
        if _wide_order_id(body["order_id"]) >> 1 != resting_order_id:
            raise CoordinatorSafetyError("RISEx cancel resting identity rejected")
        nonce_anchor = _wire_nonce_anchor(permit["nonce_anchor"])
        nonce_bitmap = _wire_uint(permit["nonce_bitmap_index"], 8, "nonce_bitmap")
        if nonce_bitmap > 207:
            raise CoordinatorSafetyError("RISEx cancel nonce binding rejected")
        deadline = _wire_deadline(permit["deadline"])
        encoded, action_hash = encode_cancel_action(
            market_id=market_id, resting_order_id=resting_order_id,
        )
        expected_permit = _expected_permit(
            identity, action_hash, nonce_anchor, nonce_bitmap, deadline,
        )
        if (
            request["market_id"] != market_id
            or request["resting_order_id"] != resting_order_id
            or request["abi_encoded"] != encoded
            or request["action_hash"] != action_hash
            or request["permit"] != expected_permit
            or permit["nonce_anchor"] != str(expected_permit["message"]["nonceAnchor"])
            or permit["nonce_bitmap_index"] != expected_permit["message"]["nonceBitmap"]
            or permit["deadline"] != expected_permit["message"]["deadline"]
        ):
            raise CoordinatorSafetyError("RISEx cancel canonical binding rejected")
    except CoordinatorSafetyError:
        raise
    except Exception:
        raise CoordinatorSafetyError("RISEx cancel request binding rejected") from None


def _validate_unsigned_auth_frame(frame: Mapping[str, Any], identity: RoleIdentity) -> None:
    if set(frame) != {"method", "params"} or frame.get("method") != "auth_v2":
        raise CoordinatorSafetyError("RISEx auth_v2 frame binding rejected")
    params = frame.get("params")
    if (
        not isinstance(params, Mapping)
        or set(params) != {"account", "signer", "message", "nonce", "expiration", "signature"}
        or params["account"] != identity.account
        or params["signer"] != identity.signer
        or params["message"] != "sign in with RISEx"
        or type(params["expiration"]) is not int
        or params["expiration"] <= 0
        or not isinstance(params["signature"], str)
        or len(params["signature"]) != 132
        or not params["signature"].startswith("0x")
    ):
        raise CoordinatorSafetyError("RISEx auth_v2 frame binding rejected")
    try:
        int(params["signature"][2:], 16)
        if PrivateReadPreflight._nonce(params["nonce"]) != params["nonce"]:
            raise ValueError
    except Exception:
        raise CoordinatorSafetyError("RISEx auth_v2 frame binding rejected") from None


class FixedRisexTwoAccountVenue:
    """Production adapter for exactly the fixed primary and counterparty roles."""

    def __init__(
        self,
        *,
        identities: Mapping[AccountRole, RoleIdentity],
        credential_loaders: Mapping[AccountRole, Callable[[], _ScopedCredential]],
        transport: FixedRisexTwoAccountTransport,
        now: Callable[[], int] = lambda: int(time.time()),
        initial_rest_round: int = 0,
        known_order_ids: Mapping[AccountRole, Sequence[str]] | None = None,
    ) -> None:
        if set(identities) != {AccountRole.PRIMARY, AccountRole.COUNTERPARTY}:
            raise CoordinatorSafetyError("RISEx adapter identity set rejected")
        if set(credential_loaders) != set(identities):
            raise CoordinatorSafetyError("RISEx adapter credential set rejected")
        for identity in identities.values():
            identity.validate()
        if identities[AccountRole.PRIMARY].account == identities[AccountRole.COUNTERPARTY].account:
            raise CoordinatorSafetyError("RISEx adapter account isolation rejected")
        if (
            identities[AccountRole.PRIMARY].account != PRIMARY_ACCOUNT.lower()
            or identities[AccountRole.PRIMARY].signer != PRIMARY_SIGNER.lower()
            or identities[AccountRole.COUNTERPARTY].account != COUNTERPARTY_ACCOUNT
            or identities[AccountRole.COUNTERPARTY].signer != COUNTERPARTY_SIGNER
        ):
            raise CoordinatorSafetyError("RISEx adapter fixed identity rejected")
        if type(initial_rest_round) is not int or initial_rest_round < 0:
            raise CoordinatorSafetyError("RISEx REST round seed rejected")
        supplied = known_order_ids or {}
        if set(supplied) - set(identities):
            raise CoordinatorSafetyError("RISEx known order role rejected")
        bound: dict[AccountRole, set[str]] = {
            AccountRole.PRIMARY: set(), AccountRole.COUNTERPARTY: set(),
        }
        for role, values in supplied.items():
            for value in values:
                if not _valid_order_id(value):
                    raise CoordinatorSafetyError("RISEx known order identity rejected")
                if value in bound[role]:
                    raise CoordinatorSafetyError("RISEx known order identity duplicated")
                bound[role].add(value)
        self._identities = dict(identities)
        self._credential_loaders = dict(credential_loaders)
        self._transport = transport
        self._now = now
        self._rest_round = initial_rest_round
        self._known_order_ids = bound

    def bind_accepted_order(self, role: AccountRole, order_id: str) -> None:
        if role not in self._known_order_ids or not _valid_order_id(order_id):
            raise CoordinatorSafetyError("RISEx accepted order identity rejected")
        if order_id in self._known_order_ids[role]:
            return
        self._known_order_ids[role].add(order_id)

    @staticmethod
    def _nonce_value(value: Any) -> str:
        try:
            return PrivateReadPreflight._nonce(value)
        except Exception:
            raise CoordinatorSafetyError("RISEx auth nonce rejected") from None

    @staticmethod
    def _active_signer(status_data: Any, signers_data: Any, identity: RoleIdentity) -> None:
        if (
            not isinstance(status_data, Mapping)
            or status_data.get("status") != 1
            or status_data.get("status_description") != "Active"
        ):
            raise CoordinatorSafetyError("RISEx session signer is not active")
        if not isinstance(signers_data, Mapping) or not isinstance(signers_data.get("signers"), list):
            raise CoordinatorSafetyError("RISEx signer list schema rejected")
        rows = signers_data["signers"]
        if len(rows) != 1 or not isinstance(rows[0], Mapping):
            raise CoordinatorSafetyError("RISEx signer list identity rejected")
        row = rows[0]
        if (
            _address(row.get("signer")).lower() != identity.signer
            or row.get("status") != "Active"
        ):
            raise CoordinatorSafetyError("RISEx signer list identity rejected")

    async def _market(self) -> MarketObservation:
        (
            system_response, domain_response, market_response, book_response,
        ) = await asyncio.gather(
            self._transport.get("/v1/system/config"),
            self._transport.get("/v1/auth/eip712-domain"),
            self._transport.get(
                "/v1/markets", (("force_refresh", "true"), ("market_ids", str(MARKET_ID))),
            ),
            self._transport.get(
                "/v1/orderbook", (("market_id", str(MARKET_ID)),),
            ),
        )
        _require_recent(book_response, self._now(), "book")
        system = _response_data(system_response)
        domain = _response_data(domain_response)
        market_data = _response_data(market_response)
        book_data = _response_data(book_response)
        if not isinstance(system, Mapping) or not isinstance(system.get("chain"), Mapping) or not isinstance(system.get("addresses"), Mapping):
            raise CoordinatorSafetyError("RISEx system config schema rejected")
        chain = system["chain"]
        addresses = system["addresses"]
        if (
            str(chain.get("chain_id")) != str(CHAIN_ID)
            or _address(addresses.get("auth")).lower() != AUTHORIZATION.lower()
            or _address(addresses.get("router")).lower() != ROUTER.lower()
            or system.get("is_maintenance_mode") is not False
        ):
            raise CoordinatorSafetyError("RISEx system contract rejected")
        if (
            not isinstance(domain, Mapping)
            or domain.get("name") != DOMAIN_NAME
            or domain.get("version") != DOMAIN_VERSION
            or str(domain.get("chain_id")) != str(CHAIN_ID)
            or _address(domain.get("verifying_contract")).lower() != AUTHORIZATION.lower()
        ):
            raise CoordinatorSafetyError("RISEx EIP712 domain rejected")
        try:
            parsed = PrivateReadPreflight._validate_market(market_data, float(self._now()))
        except Exception:
            raise CoordinatorSafetyError("RISEx market response rejected") from None
        observed_at = min(int(market_response.observed_at), int(book_response.observed_at))
        book = _parse_book_rows(book_data, observed_at)
        value = MarketObservation(
            host=REST_ORIGIN.removeprefix("https://"), chain_id=CHAIN_ID,
            domain_name=DOMAIN_NAME, domain_version=DOMAIN_VERSION,
            router=ROUTER, authorization=AUTHORIZATION,
            market_id=MARKET_ID, symbol=MARKET_SYMBOL,
            active=parsed["active"], unlocked=parsed["config"]["unlocked"],
            tick=Decimal(parsed["config"]["step_price"]),
            step=Decimal(parsed["config"]["step_size"]),
            minimum=Decimal(parsed["config"]["min_order_size"]),
            observed_at=observed_at, book=book,
        )
        _validate_market(value, self._now())
        return value

    async def _nonce(self, identity: RoleIdentity) -> tuple[NonceState, int]:
        response = await self._transport.get(NONCE_PATH_TEMPLATE.format(account=identity.account))
        _require_recent(response, self._now(), "nonce")
        data = _response_data(response)
        if not isinstance(data, Mapping) or not {"nonce_anchor", "current_bitmap_index", "bitmap"} <= set(data):
            raise CoordinatorSafetyError("RISEx nonce response schema rejected")
        try:
            anchor = int(data["nonce_anchor"])
            index = int(data["current_bitmap_index"])
            bitmap_text = data["bitmap"]
            if not isinstance(bitmap_text, str) or not bitmap_text.startswith("0x"):
                raise ValueError
            bitmap = int(bitmap_text[2:], 16)
        except Exception:
            raise CoordinatorSafetyError("RISEx nonce response rejected") from None
        nonce = NonceState(anchor, index)
        nonce.validate()
        if bitmap < 0 or bitmap >= 2**256 or bitmap & (1 << index):
            raise CoordinatorSafetyError("RISEx nonce is already consumed")
        return nonce, response.observed_at

    async def _lookup_order(self, identity: RoleIdentity, order_id: str) -> RestOrder:
        if order_id not in self._known_order_ids[identity.role]:
            raise CoordinatorSafetyError("RISEx unbound exact order lookup")
        path = ORDER_LOOKUP_PATH_TEMPLATE.format(order_id=order_id)
        response = await self._transport.get(path)
        _require_recent(response, self._now(), "exact order")
        data = _response_data(response)
        if not isinstance(data, Mapping) or not isinstance(data.get("order"), Mapping):
            raise CoordinatorSafetyError("RISEx exact order response schema rejected")
        order = _parse_order_row(data["order"], identity, response.observed_at)
        if order.order_id != order_id:
            raise CoordinatorSafetyError("RISEx exact order identity rejected")
        return order

    async def _account(
        self, role: AccountRole, *, include_private: bool,
    ) -> tuple[AccountSnapshot, NonceState, int]:
        identity = self._identities[role]
        account_query = (("account", identity.account),)
        status_task = self._transport.get(
            "/v1/auth/session-key-status", (("account", identity.account), ("signer", identity.signer)),
        )
        signers_task = self._transport.get("/v1/auth/signers", account_query)
        open_task = self._transport.get("/v1/orders/open", account_query)
        history_task = _paged_rows(
            self._transport, HISTORY_PATH, identity, "orders", self._now,
        )
        trades_task = _paged_rows(
            self._transport, TRADES_PATH, identity, "trades", self._now,
        )
        point_task = self._transport.get(
            POSITION_PATH, (("account", identity.account), ("market_id", str(MARKET_ID))),
        )
        positions_task = self._transport.get(POSITIONS_PATH, account_query)
        portfolio_task = self._transport.get(PORTFOLIO_PATH, account_query)
        nonce_task = self._nonce(identity)
        private_task = self._private(identity) if include_private else None
        gathered = [
            status_task, signers_task, open_task, history_task, trades_task,
            point_task, positions_task, portfolio_task, nonce_task,
        ]
        if private_task is not None:
            gathered.append(private_task)
        values = await asyncio.gather(*gathered)
        (
            status_response, signers_response, open_response,
            (history_rows, history_at), (trade_rows, trades_at),
            point_response, positions_response, portfolio_response, (nonce, nonce_at),
            *private_values,
        ) = values
        _require_recent(status_response, self._now(), "session signer")
        _require_recent(signers_response, self._now(), "signer list")
        status = _response_data(status_response)
        signers = _response_data(signers_response)
        self._active_signer(status, signers, identity)
        _require_recent(open_response, self._now(), "open orders")
        open_orders = _parse_open_orders(open_response, identity)
        history_orders = tuple(
            _parse_order_row(row, identity, history_at) for row in history_rows
        )
        trades = tuple(
            _parse_trade_row(row, identity, trades_at) for row in trade_rows
        )
        listed_by_id: dict[str, RestOrder] = {}
        for item in (*open_orders, *history_orders):
            existing = listed_by_id.get(item.order_id)
            if existing is not None:
                if _order_history_evidence(existing) != _order_history_evidence(item):
                    raise CoordinatorSafetyError("RISEx overlapping order disagreement")
                continue
            listed_by_id[item.order_id] = item
        lookup_order_ids = sorted(self._known_order_ids[role])
        lookup_values = await asyncio.gather(*(
            self._lookup_order(identity, order_id) for order_id in lookup_order_ids
        ))
        lookup_by_id: dict[str, RestOrder] = {}
        for order_id, exact in zip(lookup_order_ids, lookup_values):
            listed = listed_by_id.get(order_id)
            if listed is None or _order_history_evidence(listed) != _order_history_evidence(exact):
                raise CoordinatorSafetyError("RISEx exact order/list disagreement")
            lookup_by_id[order_id] = exact
        _require_recent(point_response, self._now(), "point position")
        point = _point_position(
            _response_data(point_response), identity,
        )
        _require_recent(positions_response, self._now(), "positions")
        positions = _position_rows(
            _response_data(positions_response), identity,
        )
        _require_recent(portfolio_response, self._now(), "portfolio")
        portfolio, portfolio_positions = _parse_portfolio(
            _response_data(portfolio_response), identity, portfolio_response.observed_at,
        )
        point_map = _normalize_position_map((point,))
        position_map = _normalize_position_map(positions)
        portfolio_map = _normalize_position_map(portfolio_positions)
        if position_map.get(MARKET_ID, Decimal("0")) != point_map[MARKET_ID]:
            raise CoordinatorSafetyError("RISEx position reads disagree")
        if not _position_maps_agree(position_map, portfolio_map):
            raise CoordinatorSafetyError("RISEx portfolio positions disagree")
        merged_positions = {**position_map, **portfolio_map}
        if any(
            market_id != MARKET_ID and size != 0
            for market_id, size in merged_positions.items()
        ):
            raise CoordinatorSafetyError("RISEx unrelated position rejected")
        private = private_values[0] if private_values else None
        observed_at = min(
            status_response.observed_at, signers_response.observed_at,
            open_response.observed_at, history_at, trades_at,
            point_response.observed_at, positions_response.observed_at,
            portfolio_response.observed_at,
            nonce_at, *(item.observed_at for item in lookup_by_id.values()),
        )
        _require_recent(
            _HTTPObservation(200, "", {}, observed_at), self._now(), "account",
        )
        return AccountSnapshot(
            role=role, account=identity.account, signer=identity.signer,
            signer_status="ACTIVE", position=point[1], open_orders=open_orders,
            history_orders=history_orders, trades=trades, portfolio=portfolio,
            private=private,
            observed_at=observed_at, source="REST",
            unexplained=any(
                market_id != MARKET_ID and size != 0
                for market_id, size in merged_positions.items()
            ),
        ), nonce, nonce_at

    async def _private(self, identity: RoleIdentity) -> PrivateEventEvidence:
        response = await self._transport.get(
            "/v1/auth/nonce", (("account", identity.account),),
        )
        _require_recent(response, self._now(), "auth nonce")
        data = _response_data(response)
        if not isinstance(data, Mapping) or set(data) != {"nonce"}:
            raise CoordinatorSafetyError("RISEx auth nonce schema rejected")
        nonce = self._nonce_value(data["nonce"])
        credential = self._credential_loaders[identity.role]()
        try:
            signature = credential.sign_register_v2(_register_v2_typed_data(identity.signer, nonce))
        finally:
            credential.close()
        frame = {
            "method": "auth_v2",
            "params": {
                "account": identity.account, "signer": identity.signer,
                "message": "sign in with RISEx", "nonce": nonce,
                "expiration": self._now() + 365 * 24 * 60 * 60,
                "signature": signature,
            },
        }
        _validate_unsigned_auth_frame(frame, identity)
        _auth, order_frames, position_frames = await self._transport.auth_v2_frames(
            frame, account=identity.account, signer=identity.signer,
        )
        now = self._now()
        order_snapshots = [
            _parse_private_snapshot(
                item, channel="orders", count_field="order_count",
                identity=identity, observed_at=now,
            )
            for item in order_frames if item.get("type") == "snapshot"
        ]
        position_snapshots = [
            _parse_private_snapshot(
                item, channel="positions", count_field="position_count",
                identity=identity, observed_at=now,
            )
            for item in position_frames if item.get("type") == "snapshot"
        ]
        if len(order_snapshots) != 1 or len(position_snapshots) != 1:
            raise CoordinatorSafetyError("RISEx private snapshot cardinality rejected")
        order_updates: list[RestOrder] = []
        for item in order_frames:
            if item.get("type") == "update":
                parsed = _parse_private_update(
                    item, channel="orders", identity=identity, observed_at=now,
                )
                order_updates.extend(parsed)  # type: ignore[arg-type]
        position_updates: list[tuple[int, Decimal]] = []
        for item in position_frames:
            if item.get("type") == "update":
                parsed = _parse_private_update(
                    item, channel="positions", identity=identity, observed_at=now,
                )
                position_updates.extend(parsed)  # type: ignore[arg-type]
        return PrivateEventEvidence(
            account=identity.account, auth_status="success",
            orders_snapshot=tuple(order_snapshots[0]),
            positions_snapshot=tuple(position_snapshots[0]),
            orders_updates=tuple(order_updates),
            positions_updates=tuple(position_updates), observed_at=now,
        )

    async def _observation(self, *, include_private: bool, rest_round: int = 0) -> VenueObservation:
        market_task = self._market()
        account_tasks = {
            role: self._account(role, include_private=include_private)
            for role in (AccountRole.PRIMARY, AccountRole.COUNTERPARTY)
        }
        market, *account_values = await asyncio.gather(
            market_task, *(account_tasks[role] for role in account_tasks),
        )
        accounts: dict[AccountRole, AccountSnapshot] = {}
        nonces: dict[AccountRole, NonceState] = {}
        for role, (account, nonce, _nonce_at) in zip(account_tasks, account_values):
            accounts[role] = account
            nonces[role] = nonce
        return VenueObservation(
            market=market, accounts=accounts, nonces=nonces, rest_round=rest_round,
        )

    async def observe(self) -> VenueObservation:
        return await self._observation(include_private=True)

    async def rest_round(self) -> VenueObservation:
        self._rest_round += 1
        return await self._observation(include_private=False, rest_round=self._rest_round)

    @staticmethod
    def _permit_body(
        typed: Mapping[str, Any], identity: RoleIdentity, signature: str,
    ) -> dict[str, Any]:
        message = typed["message"]
        return {
            "account": identity.account, "signer": identity.signer,
            "nonce_anchor": str(message["nonceAnchor"]),
            "nonce_bitmap_index": message["nonceBitmap"],
            "deadline": message["deadline"], "signature": signature,
        }

    async def place(self, role: AccountRole, request: Mapping[str, Any]) -> WriteResult:
        identity = self._identities[role]
        _validate_unsigned_place(request, identity)
        body = request["body"]
        credential = self._credential_loaders[role]()
        try:
            signature = _compact_signature(credential.sign_permit(request["permit"], identity))
        finally:
            credential.close()
        wire = {
            key: body[key]
            for key in (
                "market_id", "size_steps", "price_ticks", "side", "post_only",
                "reduce_only", "stp_mode", "order_type", "time_in_force", "client_order_id",
            )
        }
        wire["permit"] = self._permit_body(request["permit"], identity, signature)
        try:
            response = await self._transport.post(PLACE_PATH, wire)
            if 200 <= response.status < 300:
                data = _response_data(response)
                if isinstance(data, Mapping) and _valid_order_id(data.get("order_id")):
                    order_id = str(data["order_id"])
                    self.bind_accepted_order(role, order_id)
                    return WriteResult.accepted(order_id)
                return WriteResult(WriteResultClass.RESPONSE_AMBIGUITY, "MISSING_ORDER_ID")
            if 400 <= response.status < 500 and response.status != 408:
                return WriteResult.rejected(f"HTTP_{response.status}")
            return WriteResult.ambiguous(f"HTTP_{response.status}")
        except CoordinatorSafetyError:
            return WriteResult.ambiguous("WRITE_TRANSPORT_FAILURE")

    async def cancel(self, role: AccountRole, request: Mapping[str, Any]) -> WriteResult:
        identity = self._identities[role]
        _validate_unsigned_cancel(request, identity)
        credential = self._credential_loaders[role]()
        try:
            signature = _compact_signature(credential.sign_permit(request["permit"], identity))
        finally:
            credential.close()
        inner = request["body"]["permit"]
        wire = {
            "market_id": request["body"]["market_id"],
            "order_id": request["body"]["order_id"],
            "permit": dict(inner, signature=signature),
        }
        try:
            response = await self._transport.post(CANCEL_PATH, wire)
            if 200 <= response.status < 300:
                data = _response_data(response)
                if isinstance(data, Mapping):
                    return WriteResult.accepted()
                return WriteResult(WriteResultClass.RESPONSE_AMBIGUITY, "CANCEL_RESPONSE_SCHEMA")
            if 400 <= response.status < 500 and response.status != 408:
                return WriteResult.rejected(f"HTTP_{response.status}")
            return WriteResult.ambiguous(f"HTTP_{response.status}")
        except CoordinatorSafetyError:
            return WriteResult.ambiguous("CANCEL_TRANSPORT_FAILURE")

    async def close(self) -> None:
        await self._transport.close()

class IdentityFactory(Protocol):
    def __call__(
        self, role: AccountRole, step: str, observation: VenueObservation,
    ) -> tuple[int, int, int, int]: ...


def _default_identity_factory(
    role: AccountRole, step: str, observation: VenueObservation,
) -> tuple[int, int, int, int]:
    del step
    if observation.nonces is None or role not in observation.nonces:
        raise CoordinatorSafetyError("RISEx nonce evidence missing")
    nonce = observation.nonces[role]
    nonce.validate()
    return (
        secrets.randbelow(2**64 - 1) + 1,
        nonce.anchor,
        nonce.bitmap_index,
        int(time.time()) + 60,
    )


async def _maybe_await(value: Any) -> Any:
    if asyncio.iscoroutine(value) or isinstance(value, asyncio.Future):
        return await value
    return value


class TwoAccountCoordinator:
    """The fixed, strictly sequential two-account lifecycle."""

    def __init__(
        self,
        *,
        venue: VenueAdapter,
        primary_identity: RoleIdentity,
        counterparty_identity: RoleIdentity,
        primary_journal: PairJournal,
        counterparty_journal: PairJournal,
        now: Callable[[], int] = lambda: int(time.time()),
        identity_factory: IdentityFactory = _default_identity_factory,
    ) -> None:
        if primary_identity.role is not AccountRole.PRIMARY or counterparty_identity.role is not AccountRole.COUNTERPARTY:
            raise CoordinatorSafetyError("RISEx two-account roles rejected")
        primary_identity.validate()
        counterparty_identity.validate()
        if primary_identity.account == counterparty_identity.account:
            raise CoordinatorSafetyError("RISEx account isolation rejected")
        if primary_journal.identity.account != primary_identity.account or counterparty_journal.identity.account != counterparty_identity.account:
            raise CoordinatorSafetyError("RISEx journal role binding rejected")
        self._venue = venue
        self._identities = {
            AccountRole.PRIMARY: primary_identity,
            AccountRole.COUNTERPARTY: counterparty_identity,
        }
        self._journals = {
            AccountRole.PRIMARY: primary_journal,
            AccountRole.COUNTERPARTY: counterparty_journal,
        }
        self._now = now
        self._identity_factory = identity_factory
        self._last_failure: str | None = None

    @classmethod
    def _fixture(
        cls,
        *,
        venue: VenueAdapter,
        primary_journal: str | Path | PairJournal,
        counterparty_journal: str | Path | PairJournal,
        now: Callable[[], int] = lambda: int(time.time()),
        identity_factory: IdentityFactory | None = None,
        primary_account: str = PRIMARY_ACCOUNT,
        primary_signer: str = PRIMARY_SIGNER,
        counterparty_account: str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        counterparty_signer: str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    ) -> "TwoAccountCoordinator":
        primary = RoleIdentity(
            AccountRole.PRIMARY, primary_account.lower(), primary_signer.lower(),
            _signer._CREDENTIAL, _signer._RECORD, PRIMARY_JOURNAL,
        )
        counterparty = RoleIdentity(
            AccountRole.COUNTERPARTY, counterparty_account.lower(), counterparty_signer.lower(),
            COUNTERPARTY_SESSION_KEY, COUNTERPARTY_SIGNER_MARKER, COUNTERPARTY_JOURNAL,
        )
        p = primary_journal if isinstance(primary_journal, PairJournal) else PairJournal(primary_journal, primary)
        c = counterparty_journal if isinstance(counterparty_journal, PairJournal) else PairJournal(counterparty_journal, counterparty)
        return cls(
            venue=venue, primary_identity=primary, counterparty_identity=counterparty,
            primary_journal=p, counterparty_journal=c, now=now,
            identity_factory=identity_factory or _default_identity_factory,
        )

    @property
    def phase(self) -> Phase:
        primary = self._journals[AccountRole.PRIMARY].phase
        counterparty = self._journals[AccountRole.COUNTERPARTY].phase
        if primary != counterparty:
            raise CoordinatorSafetyError("RISEx cross-account phase contradiction")
        return primary

    def _set_phase(self, phase: Phase) -> None:
        current = self.phase
        if current is Phase.COMPLETE and phase is not Phase.COMPLETE:
            raise CoordinatorSafetyError("RISEx completed lifecycle replay rejected")
        if current is Phase.HALTED and phase is not Phase.HALTED:
            raise CoordinatorSafetyError("RISEx halted lifecycle replay rejected")
        # A phase is written to both independent domains.  A crash between the
        # writes is deliberately a contradiction on restart, never repaired by
        # guessing which side was newer.
        self._journals[AccountRole.PRIMARY].set_phase(phase)
        self._journals[AccountRole.COUNTERPARTY].set_phase(phase)

    def _set_outcome(self, outcome: str) -> None:
        self._journals[AccountRole.PRIMARY].set_outcome(outcome)
        self._journals[AccountRole.COUNTERPARTY].set_outcome(outcome)

    def _identity(self, role: AccountRole, step: str, observation: VenueObservation) -> tuple[int, int, int, int]:
        try:
            values = self._identity_factory(role, step, observation)
            client, anchor, bitmap, expires = values
        except CoordinatorSafetyError:
            raise
        except Exception:
            raise CoordinatorSafetyError("RISEx write identity unavailable") from None
        if (
            type(client) is not int or not 0 < client < 2**64
            or type(anchor) is not int or not 0 <= anchor < 2**48
            or type(bitmap) is not int or not 0 <= bitmap <= 207
            or type(expires) is not int or expires <= self._now()
            or expires > self._now() + MAX_PERMIT_SECONDS
        ):
            raise CoordinatorSafetyError("RISEx write identity rejected")
        journal = self._journals[role]
        if any(item.nonce_anchor == anchor and item.nonce_bitmap == bitmap for item in journal.intents()):
            raise CoordinatorSafetyError("RISEx nonce replay rejected")
        if any(item.nonce_anchor == anchor and item.nonce_bitmap == bitmap for item in journal.cancels()):
            raise CoordinatorSafetyError("RISEx cancel nonce replay rejected")
        return client, anchor, bitmap, expires

    def _validate_observation(self, observation: VenueObservation, *, private: bool = True) -> None:
        if not isinstance(observation, VenueObservation):
            raise CoordinatorSafetyError("RISEx venue observation rejected")
        now = _now_int(self._now())
        _validate_market(observation.market, now)
        if set(observation.accounts) != {AccountRole.PRIMARY, AccountRole.COUNTERPARTY}:
            raise CoordinatorSafetyError("RISEx account set rejected")
        for role, identity in self._identities.items():
            value = observation.accounts.get(role)
            if not isinstance(value, AccountSnapshot):
                raise CoordinatorSafetyError("RISEx account observation rejected")
            _validate_account(value, identity, now, private=private)
        if observation.nonces is not None:
            if set(observation.nonces) != {AccountRole.PRIMARY, AccountRole.COUNTERPARTY}:
                raise CoordinatorSafetyError("RISEx nonce account set rejected")
            for value in observation.nonces.values():
                value.validate()

    async def _observe(self, *, private: bool = True) -> VenueObservation:
        observation = await _maybe_await(self._venue.observe())
        self._validate_observation(observation, private=private)
        for role in (AccountRole.PRIMARY, AccountRole.COUNTERPARTY):
            if self._journals[role].terminal("baseline_history") is not None:
                self._validate_history_scope(role, observation.accounts[role])
        return observation

    def _bind_baseline_history(self, role: AccountRole, account: AccountSnapshot) -> None:
        journal = self._journals[role]
        payload = _history_payload(account)
        token = _history_token(payload)
        current = journal.terminal("baseline_history")
        if current is None:
            journal.set_terminal("baseline_history", token)
            journal.set_terminal("baseline_history_digest", _history_digest(payload))
            return
        if (
            current != token
            or journal.terminal("baseline_history_digest") != _history_digest(payload)
        ):
            raise CoordinatorSafetyError("RISEx baseline history contradiction")

    def _baseline_payload(self, role: AccountRole) -> Mapping[str, list[list[Any]]]:
        journal = self._journals[role]
        token = journal.terminal("baseline_history")
        digest = journal.terminal("baseline_history_digest")
        if token is None or digest is None:
            raise CoordinatorSafetyError("RISEx baseline history missing")
        payload = _decode_history_token(token)
        if _history_digest(payload) != digest:
            raise CoordinatorSafetyError("RISEx baseline history digest rejected")
        return payload

    def _validate_history_scope(self, role: AccountRole, account: AccountSnapshot) -> None:
        baseline = self._baseline_payload(role)
        baseline_orders = {str(item[0]): item for item in baseline["orders"]}
        baseline_trades = {str(item[0]): item for item in baseline["trades"]}
        journal = self._journals[role]
        known_orders = {
            intent.order_id for intent in journal.intents() if intent.order_id is not None
        }
        known_trades = {
            value for stage in ("ENTRY", "EXIT")
            if (value := journal.terminal(f"trade:{stage}")) is not None
        }
        observed_orders = {order.order_id: list(_order_history_evidence(order)) for order in account.history_orders}
        observed_trades = {trade.trade_id: list(_trade_history_evidence(trade)) for trade in account.trades}
        if any(order.order_id not in known_orders for order in account.open_orders):
            raise CoordinatorSafetyError("RISEx unrelated open order state rejected")
        candidate_trade_ids = {
            trade.trade_id for trade in account.trades
            if trade.order_id in known_orders
        }
        if (
            not set(baseline_orders) <= set(observed_orders)
            or not set(baseline_trades) <= set(observed_trades)
            or set(observed_orders) - set(baseline_orders) - known_orders
            or set(observed_trades) - set(baseline_trades) - known_trades - candidate_trade_ids
        ):
            raise CoordinatorSafetyError("RISEx unrelated history state rejected")
        for identity, expected in baseline_orders.items():
            if observed_orders[identity] != expected:
                raise CoordinatorSafetyError("RISEx baseline order changed")
        for identity, expected in baseline_trades.items():
            if observed_trades[identity] != expected:
                raise CoordinatorSafetyError("RISEx baseline trade changed")

    def _zero_state(self, observation: VenueObservation) -> None:
        for role in (AccountRole.PRIMARY, AccountRole.COUNTERPARTY):
            value = observation.accounts[role]
            if not _account_zero(value) or value.unexplained:
                raise CoordinatorSafetyError("RISEx initial state is not exact zero")
            self._bind_baseline_history(role, value)

    def _journal_intent(self, role: AccountRole, step: str) -> DurableIntent:
        intent = self._journals[role].by_step(step)
        if intent is None:
            raise CoordinatorSafetyError("RISEx lifecycle intent missing")
        return intent

    def _ensure_order_matches(self, intent: DurableIntent, order: RestOrder, role: AccountRole) -> None:
        identity = self._identities[role]
        _validate_order(order, identity, _now_int(self._now()))
        if (
            intent.order_id != order.order_id
            or order.client_order_id != intent.client_order_id
            or order.side != intent.side
            or order.order_type != intent.order_type
            or order.time_in_force != intent.time_in_force
            or order.reduce_only != intent.reduce_only
            or order.post_only != intent.post_only
            or order.market_id != intent.market_id
            or order.size != intent.size
            or (order.order_type == "LIMIT" and order.price != intent.price)
            or (
                order.order_type == "MARKET"
                and order.price not in {Decimal("0"), intent.price}
            )
        ):
            raise CoordinatorSafetyError("RISEx exact order binding rejected")

    async def _dispatch_place(self, role: AccountRole, intent: DurableIntent, market: MarketObservation) -> DurableIntent:
        journal = self._journals[role]
        current = journal.intent(intent.intent_id)
        if current.state == "PREPARED":
            _validate_market(market, _now_int(self._now()))
            current = journal.mark_dispatching(current.intent_id)
            request = unsigned_place_request(current, identity=self._identities[role], market=market)
            try:
                result = await _maybe_await(self._venue.place(role, request))
            except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
                # Leave DISPATCHING durable.  A fresh process can reconcile an
                # authoritative order identity, but it can never re-dispatch.
                raise
            except Exception:
                result = WriteResult.ambiguous("LOCAL_WRITE_FAILURE")
            if not isinstance(result, WriteResult):
                result = WriteResult(WriteResultClass.RESPONSE_AMBIGUITY, "INVALID_WRITE_RESULT")
            journal.record_place_result(current.intent_id, result)
            if result.result_class is not WriteResultClass.ACCEPTED:
                raise CoordinatorSafetyError("RISEx place requires reconciliation")
            binder = getattr(self._venue, "bind_accepted_order", None)
            if callable(binder):
                binder(role, result.order_id)
            current = journal.intent(current.intent_id)
        elif current.state in {"DISPATCHING", "AMBIGUOUS"}:
            raise CoordinatorSafetyError("RISEx ambiguous place requires authoritative recovery")
        elif current.state not in {"DISPATCHED", "RESTING", "TERMINAL"}:
            raise CoordinatorSafetyError("RISEx place replay rejected")
        return current

    def _prove_resting(self, role: AccountRole, intent: DurableIntent, observation: VenueObservation) -> RestOrder:
        """Prove the exact stage-specific counterparty maker contract."""
        if role is not AccountRole.COUNTERPARTY:
            raise CoordinatorSafetyError("RISEx maker role rejected")
        expected_side = "SELL" if intent.step == "ENTRY_MAKER" else "BUY"
        expected_reduce_only = intent.step == "EXIT_MAKER"
        expected_price = (
            observation.market.book.ask if expected_side == "SELL"
            else observation.market.book.bid
        )
        account = observation.accounts[role]
        matches = tuple(item for item in account.open_orders if item.client_order_id == intent.client_order_id)
        if len(matches) != 1:
            raise CoordinatorSafetyError("RISEx resting order proof rejected")
        order = matches[0]
        self._ensure_order_matches(intent, order, role)
        if order.status != "OPEN" or order.filled_size != 0:
            raise CoordinatorSafetyError("RISEx resting order state rejected")
        levels = observation.market.book.asks if expected_side == "SELL" else observation.market.book.bids
        if (
            order.side != expected_side
            or order.order_type != "LIMIT"
            or order.time_in_force != "GTC"
            or not order.post_only
            or order.reduce_only != expected_reduce_only
            or order.price != expected_price
            or intent.price != expected_price
            or sum(item.order_count for item in levels if item.price == expected_price) != 1
        ):
            proof = "ask" if expected_side == "SELL" else "bid"
            raise CoordinatorSafetyError(f"RISEx unique best {proof} proof rejected")
        self._journals[role].reconcile_intent(intent.intent_id, filled_size=Decimal("0"), state="RESTING")
        return order

    def _prove_bid_resting(self, intent: DurableIntent, observation: VenueObservation) -> RestOrder:
        return self._prove_resting(AccountRole.COUNTERPARTY, intent, observation)

    def _pair_fill(
        self, maker_role: AccountRole, maker_intent: DurableIntent,
        taker_role: AccountRole, taker_intent: DurableIntent,
        observation: VenueObservation, *, maker_position: Decimal, taker_position: Decimal,
    ) -> RestOrder | None:
        maker_account = observation.accounts[maker_role]
        taker_account = observation.accounts[taker_role]
        maker_order = _order_for(maker_account, maker_intent.client_order_id)
        taker_order = _order_for(taker_account, taker_intent.client_order_id)
        if maker_order is None or taker_order is None:
            raise CoordinatorSafetyError("RISEx mutual order identity missing")
        self._ensure_order_matches(maker_intent, maker_order, maker_role)
        self._ensure_order_matches(taker_intent, taker_order, taker_role)
        if (
            taker_order.status != "FILLED"
            or taker_order.filled_size != taker_intent.size
            or maker_order.filled_size != maker_intent.size
            or maker_order.filled_size != taker_order.filled_size
            or observation.accounts[maker_role].position != maker_position
            or observation.accounts[taker_role].position != taker_position
        ):
            raise CoordinatorSafetyError("RISEx partial or contradictory fill rejected")
        maker_trades = _trades_for(maker_account, maker_order)
        taker_trades = _trades_for(taker_account, taker_order)
        if len(maker_trades) != 1 or len(taker_trades) != 1:
            raise CoordinatorSafetyError("RISEx exact trade evidence missing")
        maker_trade, taker_trade = maker_trades[0], taker_trades[0]
        expected_trade_id = f"{maker_order.order_id}-{taker_order.order_id}"
        if (
            maker_trade.trade_id != taker_trade.trade_id
            or maker_trade.trade_id != expected_trade_id
            or maker_trade.size != maker_intent.size
            or taker_trade.size != taker_intent.size
            or maker_trade.size != taker_trade.size
            or maker_trade.price != taker_trade.price
            or maker_trade.price != maker_intent.price
            or (
                taker_intent.side == "BUY"
                and maker_trade.price > taker_intent.price
            )
            or (
                taker_intent.side == "SELL"
                and maker_trade.price < taker_intent.price
            )
            or maker_trade.order_id != maker_order.order_id
            or taker_trade.order_id != taker_order.order_id
            or maker_trade.client_order_id != maker_intent.client_order_id
            or taker_trade.client_order_id != taker_intent.client_order_id
            or maker_trade.side != maker_intent.side
            or taker_trade.side != taker_intent.side
            or maker_trade.side == taker_trade.side
            or maker_trade.market_id != taker_intent.market_id
            or taker_trade.market_id != taker_intent.market_id
        ):
            raise CoordinatorSafetyError("RISEx mutual trade identity rejected")
        stage = "ENTRY" if maker_intent.step == "ENTRY_MAKER" else "EXIT"
        for role, trade in ((maker_role, maker_trade), (taker_role, taker_trade)):
            journal = self._journals[role]
            journal.set_terminal(f"trade:{stage}", trade.trade_id)
            journal.set_terminal(f"price:{stage}", str(trade.price))
        residue = maker_order if maker_order.status == "OPEN" else None
        if residue is None and maker_order.status not in {"FILLED", "CANCELLED"}:
            raise CoordinatorSafetyError("RISEx maker terminal state rejected")
        self._journals[maker_role].reconcile_intent(
            maker_intent.intent_id, filled_size=maker_order.filled_size,
        )
        self._journals[taker_role].reconcile_intent(
            taker_intent.intent_id, filled_size=taker_order.filled_size,
        )
        return residue

    async def _cancel_residue(
        self, role: AccountRole, intent: DurableIntent, observation: VenueObservation,
        *, step: str,
    ) -> VenueObservation:
        _validate_market(observation.market, _now_int(self._now()))
        order = _order_for(observation.accounts[role], intent.client_order_id)
        if order is None or order.status != "OPEN" or order.order_id != intent.order_id:
            raise CoordinatorSafetyError("RISEx residue identity rejected")
        nonce = observation.nonces.get(role) if observation.nonces else None
        if nonce is None:
            raise CoordinatorSafetyError("RISEx cancel nonce evidence missing")
        client, anchor, bitmap, deadline = self._identity(role, step, observation)
        del client
        cancel = self._journals[role].prepare_cancel(
            intent, nonce_anchor=anchor, nonce_bitmap=bitmap, expires_at=deadline,
        )
        self._set_phase(
            Phase.ENTRY_RESIDUE_CANCEL_PREPARED if step.startswith("ENTRY")
            else Phase.EXIT_RESIDUE_CANCEL_PREPARED
        )
        current = self._journals[role].cancels()[-1]
        self._journals[role].mark_cancel_dispatching(current.cancel_id)
        request = unsigned_cancel_request(
            current, identity=self._identities[role], market=observation.market,
        )
        try:
            result = await _maybe_await(self._venue.cancel(role, request))
        except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
            raise
        except Exception:
            result = WriteResult.ambiguous("LOCAL_CANCEL_FAILURE")
        if not isinstance(result, WriteResult):
            result = WriteResult(WriteResultClass.RESPONSE_AMBIGUITY, "INVALID_CANCEL_RESULT")
        self._journals[role].record_cancel_result(current.cancel_id, result)
        if result.result_class is not WriteResultClass.ACCEPTED:
            raise CoordinatorSafetyError("RISEx residue cancel requires recovery")
        next_observation = await self._observe()
        cancelled = _order_for(next_observation.accounts[role], intent.client_order_id)
        if cancelled is not None and cancelled.status == "OPEN":
            raise CoordinatorSafetyError("RISEx residue cancel unresolved")
        if cancelled is not None and cancelled.status != "CANCELLED":
            raise CoordinatorSafetyError("RISEx residue cancel contradiction")
        self._journals[role].reconcile_cancel(current.cancel_id)
        return next_observation

    def _prepare(
        self, role: AccountRole, observation: VenueObservation, *, step: str,
        side: str, order_type: str, time_in_force: str, reduce_only: bool,
        post_only: bool, size: Decimal, price: Decimal, source_position: Decimal,
    ) -> DurableIntent:
        client, anchor, bitmap, deadline = self._identity(role, step, observation)
        spec = IntentSpec(
            step=step, side=side, order_type=order_type,
            time_in_force=time_in_force, reduce_only=reduce_only,
            post_only=post_only, market_id=MARKET_ID, size=size, price=price,
            source_position=source_position, client_order_id=client,
            nonce_anchor=anchor, nonce_bitmap=bitmap, expires_at=deadline,
            bbo_digest=_canonical_digest({
                "bid": str(observation.market.book.bid),
                "ask": str(observation.market.book.ask),
                "observed_at": observation.market.book.observed_at,
            }),
        )
        return self._journals[role].prepare(spec)

    async def _entry_maker(self, observation: VenueObservation) -> VenueObservation:
        if self.phase is Phase.INITIAL_ZERO:
            price = maker_price(observation.market, "SELL")
            intent = self._prepare(
                AccountRole.COUNTERPARTY, observation, step="ENTRY_MAKER",
                side="SELL", order_type="LIMIT", time_in_force="GTC",
                reduce_only=False, post_only=True, size=MARKET_MINIMUM,
                price=price, source_position=Decimal("0"),
            )
            self._set_phase(Phase.ENTRY_MAKER_PREPARED)
        else:
            intent = self._journal_intent(AccountRole.COUNTERPARTY, "ENTRY_MAKER")
        if self.phase is Phase.ENTRY_MAKER_PREPARED:
            intent = await self._dispatch_place(AccountRole.COUNTERPARTY, intent, observation.market)
            self._set_phase(Phase.ENTRY_MAKER_DISPATCHED)
        observation = await self._observe()
        intent = self._journal_intent(AccountRole.COUNTERPARTY, "ENTRY_MAKER")
        self._prove_resting(AccountRole.COUNTERPARTY, intent, observation)
        self._set_phase(Phase.ENTRY_MAKER_RESTING)
        return observation

    async def _entry_taker(self, observation: VenueObservation) -> VenueObservation:
        intent = self._journal_intent(AccountRole.COUNTERPARTY, "ENTRY_MAKER")
        if self.phase is Phase.ENTRY_MAKER_RESTING:
            if intent.order_id is None:
                raise CoordinatorSafetyError("RISEx maker order identity missing")
            if observation.accounts[AccountRole.PRIMARY].position != 0:
                raise CoordinatorSafetyError("RISEx primary pre-entry state rejected")
            price = _bound(observation.market.book.ask, observation.market.tick, "BUY")
            if price < intent.price:
                raise CoordinatorSafetyError("RISEx entry crossing bound rejected")
            taker = self._prepare(
                AccountRole.PRIMARY, observation, step="ENTRY_TAKER",
                side="BUY", order_type="MARKET", time_in_force="IOC",
                reduce_only=False, post_only=False, size=MARKET_MINIMUM,
                price=price, source_position=Decimal("0"),
            )
            self._set_phase(Phase.ENTRY_PREPARED)
        else:
            taker = self._journal_intent(AccountRole.PRIMARY, "ENTRY_TAKER")
        if self.phase is Phase.ENTRY_PREPARED:
            await self._dispatch_place(AccountRole.PRIMARY, taker, observation.market)
            self._set_phase(Phase.ENTRY_DISPATCHED)
        observation = await self._observe()
        taker = self._journal_intent(AccountRole.PRIMARY, "ENTRY_TAKER")
        maker = self._journal_intent(AccountRole.COUNTERPARTY, "ENTRY_MAKER")
        residue = self._pair_fill(
            AccountRole.COUNTERPARTY, maker,
            AccountRole.PRIMARY, taker, observation,
            maker_position=Decimal("-0.1"), taker_position=Decimal("0.1"),
        )
        if residue is not None:
            observation = await self._cancel_residue(
                AccountRole.COUNTERPARTY, maker, observation, step="ENTRY_RESIDUE_CANCEL",
            )
            self._pair_fill(
                AccountRole.COUNTERPARTY, maker,
                AccountRole.PRIMARY, taker, observation,
                maker_position=Decimal("-0.1"), taker_position=Decimal("0.1"),
            )
        self._set_phase(Phase.ENTRY_RECONCILED)
        return observation

    async def _exit_maker(self, observation: VenueObservation) -> VenueObservation:
        if self.phase is Phase.ENTRY_RECONCILED:
            if (
                observation.accounts[AccountRole.PRIMARY].position != Decimal("0.1")
                or observation.accounts[AccountRole.COUNTERPARTY].position != Decimal("-0.1")
                or observation.accounts[AccountRole.PRIMARY].open_orders
                or observation.accounts[AccountRole.COUNTERPARTY].open_orders
            ):
                raise CoordinatorSafetyError("RISEx pre-close state rejected")
            price = maker_price(observation.market, "BUY")
            intent = self._prepare(
                AccountRole.COUNTERPARTY, observation, step="EXIT_MAKER",
                side="BUY", order_type="LIMIT", time_in_force="GTC",
                reduce_only=True, post_only=True, size=MARKET_MINIMUM,
                price=price, source_position=Decimal("-0.1"),
            )
            self._set_phase(Phase.EXIT_MAKER_PREPARED)
        else:
            intent = self._journal_intent(AccountRole.COUNTERPARTY, "EXIT_MAKER")
        if self.phase is Phase.EXIT_MAKER_PREPARED:
            await self._dispatch_place(AccountRole.COUNTERPARTY, intent, observation.market)
            self._set_phase(Phase.EXIT_MAKER_DISPATCHED)
        observation = await self._observe()
        intent = self._journal_intent(AccountRole.COUNTERPARTY, "EXIT_MAKER")
        self._prove_bid_resting(intent, observation)
        self._set_phase(Phase.EXIT_MAKER_RESTING)
        return observation

    async def _exit_taker(self, observation: VenueObservation) -> VenueObservation:
        maker = self._journal_intent(AccountRole.COUNTERPARTY, "EXIT_MAKER")
        if self.phase is Phase.EXIT_MAKER_RESTING:
            if maker.order_id is None:
                raise CoordinatorSafetyError("RISEx close maker identity missing")
            if (
                observation.accounts[AccountRole.PRIMARY].position != Decimal("0.1")
                or observation.accounts[AccountRole.COUNTERPARTY].position != Decimal("-0.1")
            ):
                raise CoordinatorSafetyError("RISEx close position state rejected")
            price = _bound(observation.market.book.bid, observation.market.tick, "SELL")
            if price > maker.price or price <= 0:
                raise CoordinatorSafetyError("RISEx close crossing bound rejected")
            taker = self._prepare(
                AccountRole.PRIMARY, observation, step="EXIT_TAKER",
                side="SELL", order_type="MARKET", time_in_force="IOC",
                reduce_only=True, post_only=False, size=MARKET_MINIMUM,
                price=price, source_position=Decimal("0.1"),
            )
            self._set_phase(Phase.EXIT_PREPARED)
        else:
            taker = self._journal_intent(AccountRole.PRIMARY, "EXIT_TAKER")
        if self.phase is Phase.EXIT_PREPARED:
            await self._dispatch_place(AccountRole.PRIMARY, taker, observation.market)
            self._set_phase(Phase.EXIT_DISPATCHED)
        observation = await self._observe()
        taker = self._journal_intent(AccountRole.PRIMARY, "EXIT_TAKER")
        residue = self._pair_fill(
            AccountRole.COUNTERPARTY, maker,
            AccountRole.PRIMARY, taker, observation,
            maker_position=Decimal("0"), taker_position=Decimal("0"),
        )
        if residue is not None:
            observation = await self._cancel_residue(
                AccountRole.COUNTERPARTY, maker, observation, step="EXIT_RESIDUE_CANCEL",
            )
            self._pair_fill(
                AccountRole.COUNTERPARTY, maker,
                AccountRole.PRIMARY, taker, observation,
                maker_position=Decimal("0"), taker_position=Decimal("0"),
            )
        self._set_phase(Phase.EXIT_RECONCILED)
        return observation

    def _prove_final_account(self, role: AccountRole, account: AccountSnapshot) -> None:
        """Bind final REST history to the four durable lifecycle identities."""
        journal = self._journals[role]
        intents = journal.intents()
        if len(intents) != 2 or any(intent.order_id is None for intent in intents):
            raise CoordinatorSafetyError("RISEx final lifecycle identity missing")
        if account.open_orders:
            raise CoordinatorSafetyError("RISEx final zero-order barrier rejected")
        baseline = self._baseline_payload(role)
        baseline_orders = {str(item[0]): item for item in baseline["orders"]}
        expected_orders = {
            **baseline_orders,
            **{intent.order_id: intent for intent in intents},
        }
        if len(account.history_orders) != len(expected_orders):
            raise CoordinatorSafetyError("RISEx final order history count rejected")
        observed_orders = {order.order_id: order for order in account.history_orders}
        if set(observed_orders) != set(expected_orders):
            raise CoordinatorSafetyError("RISEx unrelated final order rejected")
        for order_id, intent in expected_orders.items():
            order = observed_orders[order_id]
            if order_id in baseline_orders:
                if list(_order_history_evidence(order)) != baseline_orders[order_id]:
                    raise CoordinatorSafetyError("RISEx baseline final order changed")
            else:
                if not isinstance(intent, DurableIntent):
                    raise CoordinatorSafetyError("RISEx final order identity rejected")
                self._ensure_order_matches(intent, order, role)
                if order.status not in {"FILLED", "CANCELLED"}:
                    raise CoordinatorSafetyError("RISEx final order terminal state rejected")
                if intent.filled_size is None or order.filled_size != intent.filled_size:
                    raise CoordinatorSafetyError("RISEx final fill size rejected")

        baseline_trades = {str(item[0]): item for item in baseline["trades"]}
        expected_trade_ids = set(baseline_trades)
        expected_trade_ids.update(
            value for stage in ("ENTRY", "EXIT")
            if (value := journal.terminal(f"trade:{stage}")) is not None
        )
        expected_prices = {
            "ENTRY": journal.terminal("price:ENTRY"),
            "EXIT": journal.terminal("price:EXIT"),
        }
        if (
            None in expected_prices
            or journal.terminal("trade:ENTRY") is None
            or journal.terminal("trade:EXIT") is None
            or len(account.trades) != len(expected_trade_ids)
            or {trade.trade_id for trade in account.trades} != expected_trade_ids
        ):
            raise CoordinatorSafetyError("RISEx final trade identity rejected")
        observed_trades = {trade.trade_id: trade for trade in account.trades}
        for trade_id, expected in baseline_trades.items():
            if list(_trade_history_evidence(observed_trades[trade_id])) != expected:
                raise CoordinatorSafetyError("RISEx baseline final trade changed")
        seen_stages: set[str] = set()
        for trade in account.trades:
            if trade.trade_id in baseline_trades:
                continue
            order = observed_orders.get(trade.order_id)
            if order is None or trade.client_order_id != order.client_order_id:
                raise CoordinatorSafetyError("RISEx final trade order binding rejected")
            if trade.side != order.side or trade.size != order.filled_size:
                raise CoordinatorSafetyError("RISEx final trade economics rejected")
            stage = "ENTRY" if trade.trade_id == journal.terminal("trade:ENTRY") else "EXIT"
            if stage in seen_stages or str(trade.price) != expected_prices[stage]:
                raise CoordinatorSafetyError("RISEx final trade price rejected")
            seen_stages.add(stage)
        if seen_stages != {"ENTRY", "EXIT"}:
            raise CoordinatorSafetyError("RISEx final trade stages rejected")

    async def _final_barrier(self) -> None:
        previous_fingerprint: str | None = None
        previous_round_id: int | None = None
        if self.phase is Phase.FINAL_ROUND_ONE:
            previous_fingerprint = self._journals[AccountRole.PRIMARY].terminal("final_round_one")
            if previous_fingerprint is None or previous_fingerprint != self._journals[AccountRole.COUNTERPARTY].terminal("final_round_one"):
                raise CoordinatorSafetyError("RISEx final round journal mismatch")
            raw_round_id = self._journals[AccountRole.PRIMARY].terminal("final_round_one_id")
            if raw_round_id is None:
                raise CoordinatorSafetyError("RISEx final round sequence missing")
            try:
                previous_round_id = int(raw_round_id)
            except ValueError:
                raise CoordinatorSafetyError("RISEx final round sequence rejected") from None
        for round_number in (1, 2):
            if round_number == 1 and previous_fingerprint is not None:
                continue
            observation = await _maybe_await(self._venue.rest_round())
            self._validate_observation(observation, private=False)
            if observation.rest_round <= 0:
                raise CoordinatorSafetyError("RISEx REST round identity rejected")
            if previous_round_id is not None and observation.rest_round <= previous_round_id:
                raise CoordinatorSafetyError("RISEx REST rounds are not ordered")
            for role in (AccountRole.PRIMARY, AccountRole.COUNTERPARTY):
                account = observation.accounts[role]
                if not _account_zero(account):
                    raise CoordinatorSafetyError("RISEx final zero-flat barrier rejected")
                self._prove_final_account(role, account)
            fingerprint = _canonical_digest({
                "accounts": {
                    role.value: {
                        "account": observation.accounts[role].account,
                        "signer": observation.accounts[role].signer,
                        "signer_status": observation.accounts[role].signer_status,
                        "position": str(observation.accounts[role].position),
                        "open_orders": tuple(sorted(
                            _order_history_evidence(item)
                            for item in observation.accounts[role].open_orders
                        )),
                        "history_orders": tuple(sorted(
                            _order_history_evidence(item)
                            for item in observation.accounts[role].history_orders
                        )),
                        "trades": tuple(sorted(
                            _trade_history_evidence(item)
                            for item in observation.accounts[role].trades
                        )),
                        "portfolio": {
                            "in_liquidation": observation.accounts[role].portfolio.in_liquidation,
                            "risk_level": observation.accounts[role].portfolio.risk_level,
                        },
                    }
                    for role in (AccountRole.PRIMARY, AccountRole.COUNTERPARTY)
                },
            })
            if previous_fingerprint is not None and fingerprint != previous_fingerprint:
                raise CoordinatorSafetyError("RISEx final REST rounds disagree")
            if round_number == 1:
                self._journals[AccountRole.PRIMARY].set_terminal("final_round_one", fingerprint)
                self._journals[AccountRole.COUNTERPARTY].set_terminal("final_round_one", fingerprint)
                self._journals[AccountRole.PRIMARY].set_terminal("final_round_one_id", str(observation.rest_round))
                self._journals[AccountRole.COUNTERPARTY].set_terminal("final_round_one_id", str(observation.rest_round))
                self._set_phase(Phase.FINAL_ROUND_ONE)
                previous_fingerprint = fingerprint
                previous_round_id = observation.rest_round
            else:
                self._journals[AccountRole.PRIMARY].set_terminal("final_round_two", fingerprint)
                self._journals[AccountRole.COUNTERPARTY].set_terminal("final_round_two", fingerprint)
        self._set_outcome("COMPLETE")
        self._set_phase(Phase.COMPLETE)

    async def run(self) -> CoordinatorReport:
        try:
            current = self.phase
            if current is Phase.COMPLETE or any(
                journal.outcome == "COMPLETE" for journal in self._journals.values()
            ):
                self._require_complete_evidence()
                return self._report(CoordinatorResult.COMPLETE)
            if self._journals[AccountRole.PRIMARY].outcome == "HALTED" or current is Phase.HALTED:
                return self._report(CoordinatorResult.FAILED_HALTED_MANUAL_RECOVERY)
            if self._journals[AccountRole.PRIMARY].pending_writes() or self._journals[AccountRole.COUNTERPARTY].pending_writes():
                raise CoordinatorSafetyError("RISEx pending ambiguous write requires recovery")
            observation = await self._observe()
            if current is Phase.START:
                self._zero_state(observation)
                self._set_phase(Phase.INITIAL_ZERO)
                current = Phase.INITIAL_ZERO
            if current in {Phase.INITIAL_ZERO, Phase.ENTRY_MAKER_PREPARED, Phase.ENTRY_MAKER_DISPATCHED}:
                observation = await self._entry_maker(observation)
                current = self.phase
            if current in {Phase.ENTRY_MAKER_RESTING, Phase.ENTRY_PREPARED, Phase.ENTRY_DISPATCHED, Phase.ENTRY_RESIDUE_CANCEL_PREPARED}:
                observation = await self._entry_taker(observation)
                current = self.phase
            if current in {Phase.ENTRY_RECONCILED, Phase.EXIT_MAKER_PREPARED, Phase.EXIT_MAKER_DISPATCHED}:
                observation = await self._exit_maker(observation)
                current = self.phase
            if current in {Phase.EXIT_MAKER_RESTING, Phase.EXIT_PREPARED, Phase.EXIT_DISPATCHED, Phase.EXIT_RESIDUE_CANCEL_PREPARED}:
                observation = await self._exit_taker(observation)
            if self.phase is Phase.EXIT_RECONCILED or self.phase is Phase.FINAL_ROUND_ONE:
                await self._final_barrier()
            self._require_complete_evidence()
            return self._report(CoordinatorResult.COMPLETE)
        except CoordinatorSafetyError as error:
            self._last_failure = str(error)
            try:
                self._set_outcome("HALTED")
                self._set_phase(Phase.HALTED)
            except CoordinatorSafetyError:
                pass
            result = (
                CoordinatorResult.FAILED_HALTED_MANUAL_RECOVERY
                if any(j.dispatch_count() or j.cancels() for j in self._journals.values())
                else CoordinatorResult.BLOCKED_BEFORE_WRITE
            )
            return self._report(result)
        finally:
            closer = getattr(self._venue, "close", None)
            if callable(closer):
                await _maybe_await(closer())

    def _report(self, result: CoordinatorResult) -> CoordinatorReport:
        primary = self._journals[AccountRole.PRIMARY]
        counterparty = self._journals[AccountRole.COUNTERPARTY]
        final_rounds = int(primary.terminal("final_round_one") is not None)
        final_rounds += int(primary.terminal("final_round_two") is not None)
        return CoordinatorReport(
            run_id=_canonical_digest({"primary": primary.run_id, "counterparty": counterparty.run_id}),
            primary_run_id=primary.run_id,
            counterparty_run_id=counterparty.run_id,
            result=result,
            phase=self.phase,
            primary_intents=len(primary.intents()),
            counterparty_intents=len(counterparty.intents()),
            primary_dispatches=primary.dispatch_count(),
            counterparty_dispatches=counterparty.dispatch_count(),
            counterparty_cancels=len(counterparty.cancels()),
            final_rounds=final_rounds,
            failure_code=self._last_failure,
        )

    def _require_complete_evidence(self) -> None:
        if (
            self.phase is not Phase.COMPLETE
            or any(journal.outcome != "COMPLETE" for journal in self._journals.values())
        ):
            raise CoordinatorSafetyError("RISEx completion state incomplete")
        for key in ("final_round_one", "final_round_two"):
            primary = self._journals[AccountRole.PRIMARY].terminal(key)
            counterparty = self._journals[AccountRole.COUNTERPARTY].terminal(key)
            if primary is None or primary != counterparty:
                raise CoordinatorSafetyError("RISEx final evidence incomplete")


async def build_risex_two_account_coordinator() -> TwoAccountCoordinator:
    """Bind only the fixed testnet identities, journals, and REST/auth adapter."""
    primary_identity, primary_loader = _load_primary_identity()
    counterparty_identity, counterparty_loader = _load_counterparty_identity()
    try:
        home = Path(pwd.getpwuid(os.getuid()).pw_dir)
        primary_path = home / PRIMARY_JOURNAL
        counterparty_path = home / COUNTERPARTY_JOURNAL
        primary_journal = PairJournal(primary_path, primary_identity)
        try:
            counterparty_journal = PairJournal(counterparty_path, counterparty_identity)
        except Exception:
            primary_journal.close()
            raise
        try:
            known_order_ids = {
                AccountRole.PRIMARY: tuple(
                    item.order_id for item in primary_journal.intents()
                    if item.order_id is not None
                ),
                AccountRole.COUNTERPARTY: tuple(
                    item.order_id for item in counterparty_journal.intents()
                    if item.order_id is not None
                ),
            }
            initial_round = 0
            raw_round = primary_journal.terminal("final_round_one_id")
            if raw_round is not None:
                initial_round = int(raw_round)
            transport = FixedRisexTwoAccountTransport(
                (primary_identity.account, counterparty_identity.account),
            )
            try:
                venue = FixedRisexTwoAccountVenue(
                    identities={
                        AccountRole.PRIMARY: primary_identity,
                        AccountRole.COUNTERPARTY: counterparty_identity,
                    },
                    credential_loaders={
                        AccountRole.PRIMARY: primary_loader,
                        AccountRole.COUNTERPARTY: counterparty_loader,
                    },
                    transport=transport,
                    initial_rest_round=initial_round,
                    known_order_ids=known_order_ids,
                )
            except Exception:
                await transport.close()
                raise
            return TwoAccountCoordinator(
                venue=venue,
                primary_identity=primary_identity,
                counterparty_identity=counterparty_identity,
                primary_journal=primary_journal,
                counterparty_journal=counterparty_journal,
            )
        except Exception:
            counterparty_journal.close()
            primary_journal.close()
            raise
    except CoordinatorSafetyError:
        raise
    except Exception:
        raise CoordinatorSafetyError("RISEx production binding rejected") from None


async def run_risex_two_account_coordinator() -> CoordinatorReport:
    """Zero-argument operational entry point; normal startup never calls it."""
    coordinator = await build_risex_two_account_coordinator()
    try:
        return await coordinator.run()
    finally:
        for journal in coordinator._journals.values():
            journal.close()


def main() -> int:
    try:
        report = asyncio.run(run_risex_two_account_coordinator())
    except CoordinatorSafetyError:
        print(json.dumps({
            "result": CoordinatorResult.BLOCKED_BEFORE_WRITE.value,
            "failure_code": "PRODUCTION_BINDING_REJECTED",
        }, sort_keys=True))
        return 1
    except Exception:
        print(json.dumps({
            "result": CoordinatorResult.FAILED_HALTED_MANUAL_RECOVERY.value,
            "failure_code": "PRODUCTION_ENTRY_FAILED",
        }, sort_keys=True))
        return 1
    print(json.dumps(report.sanitized(), sort_keys=True))
    return 0 if report.result is CoordinatorResult.COMPLETE else 1


if __name__ == "__main__":
    raise SystemExit(main())
