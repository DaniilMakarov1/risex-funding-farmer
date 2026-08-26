"""Sealed Extended Sepolia Level-C operational binding.

The normal Farmer never imports this module.  The production entry point fixes
the Extended testnet, account files, market, REST transport, and journal.  The
fixture seam below is intentionally dependency-injected so the contract can be
tested without credentials, sockets, or authenticated calls.

Order construction is delegated to the pinned official ``x10`` SDK at the
production boundary.  REST reads and writes stay explicit here because the
SDK's convenience methods do not expose the bounded pagination and exact
identity barriers required by the lifecycle contract.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
import hashlib
import http.client
import json
import os
from pathlib import Path
import secrets
import sqlite3
import ssl
import stat
import sys
from threading import Lock
import time
from typing import Any, Callable, Mapping, Protocol, Sequence
from urllib.parse import quote, urlencode, urlsplit
import uuid

from .extended_private_read_operational import (
    API_KEY_BASENAME,
    IDENTITY_BASENAME,
    _passwd_home,
    _read_secure_file,
)
from .extended_testnet_lifecycle import MAX_NOTIONAL_USD, TESTNET_CONTRACT


RUN_STORE_BASENAME = ".risex-funding-farmer-extended-level-c-v1.sqlite3"
REDACTED_STORE_PATH = "<passwd-home>/" + RUN_STORE_BASENAME
STARK_PRIVATE_KEY_BASENAME = ".risex-funding-farmer-extended-stark-private-key-v1"
TARGET_MARKET = "BTC-USD"
SDK_PACKAGE = "x10-python-trading-starknet"
SDK_VERSION = "2.5.0"
SDK_PROVENANCE = {
    "repository": "https://github.com/x10xchange/python_sdk",
    "commit": "2130cdb1cd6e7b1867db83bd3af036572d258739",
}
# Stark private scalars are accepted by the pinned SDK's crypto dependency in
# the half-open range [1, EC_ORDER).  Keep the bound local so malformed values
# never reach the native key-derivation routine, which can panic on them.
STARK_EC_ORDER = 0x800000000000010FFFFFFFFFFFFFFFFB781126DCAE7B2321E66A241ADC64D2F
_MAX_STARK_PRIVATE_KEY_HEX_DIGITS = (STARK_EC_ORDER.bit_length() + 3) // 4

MAX_REST_PAGE_ITEMS = 256
MAX_REST_PAGES = 16
MAX_REST_RESPONSE_BYTES = 1_048_576
MAX_FRESHNESS_MS = 5_000
SHORT_EXPIRY_MS = 15_000
HTTP_TIMEOUT_SECONDS = 10.0
MAX_RECONCILE_READS = 5
RECONCILE_SLEEP_SECONDS = 0.20
_REQUIRED_REST_RECEIPTS = frozenset({
    "account", "balance", "market", "book", "fees", "leverage",
    "open_orders", "positions", "order_history", "trades",
})

OK = "OK"
ACTIVE = "ACTIVE"
PERPETUAL = "PERPETUAL"
IOC = "IOC"
LIMIT = "LIMIT"
TRADE = "TRADE"
FILLED = "FILLED"
NEW = "NEW"
PARTIALLY_FILLED = "PARTIALLY_FILLED"
CANCELLED = "CANCELLED"
EXPIRED = "EXPIRED"
REJECTED = "REJECTED"

_ORDER_TERMINAL = frozenset({FILLED, CANCELLED, EXPIRED, REJECTED})
_ORDER_OPEN = frozenset({NEW, PARTIALLY_FILLED, "UNTRIGGERED"})
_FAILURE_CLASSES = frozenset({
    "TRANSPORT", "HTTP", "SCHEMA", "AUTH", "IDENTITY", "SAFETY",
    "AMBIGUOUS_WRITE", "VENUE_REJECTION", "RESTART",
})
_RUNTIME_STAGES = frozenset({
    "RUNNER_STARTUP", "INITIAL_REST", "ENTRY_PREPARATION",
    "ENTRY_SIGNATURE", "ENTRY_DISPATCH", "ENTRY_RECONCILIATION",
    "CANCEL_PREPARATION", "CANCEL_DISPATCH", "CANCEL_RECONCILIATION",
    "CLOSE_PREPARATION", "CLOSE_SIGNATURE", "CLOSE_DISPATCH",
    "CLOSE_RECONCILIATION", "FINAL_BARRIER", "OUTER",
})


class OperationalSafetyError(RuntimeError):
    """Sanitized terminal failure; raw responses and secrets never escape."""

    def __init__(self, code: str, *, failure_class: str = "SAFETY") -> None:
        if type(code) is not str or not code or any(ord(char) < 33 for char in code):
            raise ValueError("invalid operational failure code")
        if failure_class not in _FAILURE_CLASSES:
            raise ValueError("invalid operational failure class")
        self.code = code
        self.failure_class = failure_class
        super().__init__(code)


class AmbiguousWrite(OperationalSafetyError):
    """The write outcome is not known; dispatch must never be repeated."""

    def __init__(self, code: str = "WRITE_OUTCOME_AMBIGUOUS") -> None:
        super().__init__(code, failure_class="AMBIGUOUS_WRITE")


class VenueRejection(OperationalSafetyError):
    """The venue returned a complete, explicit negative write response."""

    def __init__(self, code: str = "VENUE_REJECTED") -> None:
        super().__init__(code, failure_class="VENUE_REJECTION")


def _fail(code: str, failure_class: str = "SAFETY") -> None:
    raise OperationalSafetyError(code, failure_class=failure_class)


def _decimal(value: Any, code: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        _fail(code, "SCHEMA")
    if not result.is_finite():
        _fail(code, "SCHEMA")
    return result


def _positive_int(value: Any, code: str) -> int:
    if type(value) is not int or value <= 0:
        _fail(code, "SCHEMA")
    return value


def _integer(value: Any, code: str) -> int:
    if type(value) is int:
        result = value
    elif type(value) is str and value and value.isdecimal():
        result = int(value)
    else:
        _fail(code, "SCHEMA")
    return result


def _venue_order_id(value: Any, code: str) -> str:
    if type(value) is int:
        if value <= 0:
            _fail(code, "IDENTITY")
        return str(value)
    if type(value) is str and value.isdecimal() and int(value) > 0:
        return value
    _fail(code, "IDENTITY")


def _string(value: Any, code: str) -> str:
    if type(value) is not str or not value:
        _fail(code, "SCHEMA")
    return value


def _same_hex(left: Any, right: Any) -> bool:
    return type(left) is str and type(right) is str and left.lower() == right.lower()


def _normalize_observed_l2_vault(value: Any) -> int | None:
    """Normalize Extended's canonical non-negative decimal-string field."""
    if type(value) is not str or not value:
        return None
    if any(char not in "0123456789" for char in value):
        return None
    if len(value) > 1 and value[0] == "0":
        return None
    try:
        normalized = int(value, 10)
    except ValueError:
        return None
    return normalized if str(normalized) == value else None


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def canonical_digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


def _zeroize(value: bytearray) -> None:
    for index in range(len(value)):
        value[index] = 0
    value.clear()


def _prepare_file(path: Path) -> None:
    """Create or validate a protected operational SQLite file."""
    try:
        descriptor = os.open(
            path,
            os.O_CREAT | os.O_EXCL | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
    except FileExistsError:
        try:
            details = path.lstat()
        except OSError:
            _fail("STORE_UNAVAILABLE", "TRANSPORT")
        if (
            path.is_symlink()
            or not stat.S_ISREG(details.st_mode)
            or details.st_uid != os.getuid()
            or stat.S_IMODE(details.st_mode) != 0o600
            or details.st_nlink != 1
        ):
            _fail("STORE_INVALID")
    except OSError:
        _fail("STORE_UNAVAILABLE", "TRANSPORT")
    else:
        os.fchmod(descriptor, 0o600)
        os.fsync(descriptor)
        os.close(descriptor)
        _fsync(path)


def _fsync(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except OSError:
        _fail("STORE_SYNC_FAILED", "TRANSPORT")


@dataclass(frozen=True)
class ExtendedIdentity:
    account_id: int
    account_index: int
    l2_key: str
    l2_vault: int

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ExtendedIdentity":
        if not isinstance(value, Mapping) or set(value) != {"id", "accountIndex", "l2Key", "l2Vault"}:
            _fail("IDENTITY_SCHEMA", "SCHEMA")
        if type(value["id"]) is not int or value["id"] < 0:
            _fail("IDENTITY_SCHEMA", "SCHEMA")
        if type(value["accountIndex"]) is not int or value["accountIndex"] < 0:
            _fail("IDENTITY_SCHEMA", "SCHEMA")
        if type(value["l2Key"]) is not str or not value["l2Key"].startswith("0x"):
            _fail("IDENTITY_SCHEMA", "SCHEMA")
        if type(value["l2Vault"]) is not int or value["l2Vault"] < 0:
            _fail("IDENTITY_SCHEMA", "SCHEMA")
        try:
            int(value["l2Key"], 16)
        except ValueError:
            _fail("IDENTITY_SCHEMA", "SCHEMA")
        return cls(
            value["id"], value["accountIndex"], value["l2Key"], value["l2Vault"],
        )

    def matches_account(self, value: Mapping[str, Any]) -> bool:
        if not isinstance(value, Mapping):
            return False
        account_id = value.get("id", value.get("accountId"))
        l2_vault = _normalize_observed_l2_vault(value.get("l2Vault"))
        return (
            account_id == self.account_id
            and value.get("accountIndex") == self.account_index
            and _same_hex(value.get("l2Key"), self.l2_key)
            and l2_vault is not None
            and l2_vault == self.l2_vault
        )


def _validate_stark_private_key(value: str, identity: ExtendedIdentity) -> None:
    """Validate the pinned SDK key form and bind it to the configured L2 key."""
    if (
        type(value) is not str
        or not value.startswith("0x")
        or not value[2:]
        or len(value[2:]) > _MAX_STARK_PRIVATE_KEY_HEX_DIGITS
        or any(char not in "0123456789abcdefABCDEF" for char in value[2:])
    ):
        _fail("PRIVATE_KEY_INVALID", "AUTH")
    try:
        scalar = int(value[2:], 16)
    except ValueError:
        _fail("PRIVATE_KEY_INVALID", "AUTH")
    if not 0 < scalar < STARK_EC_ORDER:
        _fail("PRIVATE_KEY_INVALID", "AUTH")

    if (
        type(identity.l2_key) is not str
        or not identity.l2_key.startswith("0x")
        or not identity.l2_key[2:]
        or any(char not in "0123456789abcdefABCDEF" for char in identity.l2_key[2:])
    ):
        _fail("ACCOUNT_IDENTITY_MISMATCH", "IDENTITY")
    try:
        configured_public_key = int(identity.l2_key[2:], 16)
    except ValueError:
        _fail("ACCOUNT_IDENTITY_MISMATCH", "IDENTITY")
    try:
        # x10 2.5.0 pins fast-stark-crypto 0.5.0, whose SDK public-key
        # derivation is the authoritative binding for StarkPerpetualAccount.
        from fast_stark_crypto import get_public_key
        derived_public_key = get_public_key(scalar)
    except (ImportError, ModuleNotFoundError):
        _fail("OFFICIAL_SDK_UNAVAILABLE", "AUTH")
    except BaseException:
        # Native crypto failures must not expose exception text or key data.
        _fail("STARK_PUBLIC_KEY_DERIVATION_FAILED", "AUTH")
    if type(derived_public_key) is not int or derived_public_key != configured_public_key:
        _fail("ACCOUNT_IDENTITY_MISMATCH", "IDENTITY")


class ExtendedCredentialCapability:
    """Short-lived protected credential handle used only by the sealed runner."""

    def __init__(
        self,
        api_key: bytearray,
        private_key: bytearray,
        identity: ExtendedIdentity,
    ) -> None:
        self._api_key = api_key
        self._private_key = private_key
        self.identity = identity
        self._closed = False

    def api_key(self) -> str:
        if self._closed:
            _fail("CREDENTIAL_CLOSED", "AUTH")
        try:
            value = bytes(self._api_key).decode("ascii")
        except UnicodeDecodeError:
            _fail("API_KEY_INVALID", "AUTH")
        if (
            not value
            or value.strip() != value
            or any(ord(char) < 33 or ord(char) > 126 for char in value)
        ):
            _fail("API_KEY_INVALID", "AUTH")
        return value

    def _private_key_text(self) -> str:
        if self._closed:
            _fail("CREDENTIAL_CLOSED", "AUTH")
        try:
            value = bytes(self._private_key).decode("ascii")
        except UnicodeDecodeError:
            _fail("PRIVATE_KEY_INVALID", "AUTH")
        _validate_stark_private_key(value, self.identity)
        return value

    def sign_order(
        self,
        intent: "OrderIntent",
        market: Mapping[str, Any],
    ) -> "SignedOrder":
        """Create the exact REST payload through the pinned official SDK."""
        if self._closed:
            _fail("CREDENTIAL_CLOSED", "AUTH")
        if intent.external_id is None or intent.nonce is None:
            _fail("WRITE_IDENTITY_MISSING")
        try:
            # The import is deliberately inside the credential boundary.  The
            # normal paper process therefore has no SDK/signing side effects.
            from x10.config import TESTNET_CONFIG
            from x10.core.stark_account import StarkPerpetualAccount
            from x10.models.market import MarketModel
            from x10.models.order import (
                OrderSide,
                OrderType,
                SelfTradeProtectionLevel,
                TimeInForce,
            )
            from x10.signing.order_object import create_order_object
            from x10.signing.order_object_settlement import (
                SettlementDataCtx,
                create_order_settlement_data,
            )
        except (ImportError, ModuleNotFoundError):
            _fail("OFFICIAL_SDK_UNAVAILABLE", "AUTH")
        try:
            if not isinstance(market, Mapping):
                _fail("MARKET_SCHEMA", "SCHEMA")
            sdk_market = MarketModel.model_validate(dict(market))
            account = StarkPerpetualAccount(
                self.identity.l2_vault,
                self._private_key_text(),
                self.identity.l2_key,
                self.api_key(),
            )
            expiry = datetime.fromtimestamp(intent.expiry_ms / 1000, tz=UTC)
            # The official settlement helper is also the source of the
            # durable settlement hash.  It is computed from the same exact
            # inputs as create_order_object; no locally invented hash is used.
            settlement_data = create_order_settlement_data(
                side=OrderSide(intent.side),
                synthetic_amount=intent.qty,
                price=intent.price,
                ctx=SettlementDataCtx(
                    market=sdk_market,
                    taker_fee=intent.fee,
                    builder_fee=None,
                    nonce=intent.nonce,
                    collateral_position_id=self.identity.l2_vault,
                    expire_time=expiry,
                    signer=account.sign,
                    public_key=account.public_key,
                    starknet_domain=TESTNET_CONFIG.signing.starknet_domain,
                ),
            )
            order = create_order_object(
                account=account,
                market=sdk_market,
                amount_of_synthetic=intent.qty,
                price=intent.price,
                side=OrderSide(intent.side),
                starknet_domain=TESTNET_CONFIG.signing.starknet_domain,
                order_type=OrderType.LIMIT,
                post_only=False,
                expire_time=expiry,
                order_external_id=intent.external_id,
                time_in_force=TimeInForce.IOC,
                self_trade_protection_level=SelfTradeProtectionLevel.ACCOUNT,
                nonce=intent.nonce,
                taker_fee=intent.fee,
                reduce_only=intent.reduce_only,
            )
            payload = order.to_api_request_json(exclude_none=True)
            if not isinstance(payload, dict):
                _fail("SDK_PAYLOAD_SCHEMA", "SCHEMA")
            # The SDK order id is the durable external identity when supplied.
            if payload.get("id") != intent.external_id:
                _fail("SDK_ORDER_ID_MISMATCH")
            return SignedOrder(payload, intent.external_id, str(settlement_data.order_hash))
        except OperationalSafetyError:
            raise
        except BaseException:
            # Do not expose SDK exception text; it can contain key material.
            _fail("SDK_SIGNING_FAILED", "AUTH")

    def close(self) -> None:
        _zeroize(self._api_key)
        _zeroize(self._private_key)
        self._closed = True


class _PasswdHomeCredentialSource:
    """The only production credential source; paths are not configurable."""

    def open(self) -> ExtendedCredentialCapability:
        home = _passwd_home()
        try:
            raw_identity = _read_secure_file(home / IDENTITY_BASENAME, 2048)
        except BaseException:
            _fail("IDENTITY_FILE_UNAVAILABLE", "AUTH")
        try:
            try:
                identity_value = json.loads(bytes(raw_identity))
            except (UnicodeDecodeError, json.JSONDecodeError):
                _fail("IDENTITY_FILE_INVALID", "SCHEMA")
            identity = ExtendedIdentity.from_mapping(identity_value)
        finally:
            _zeroize(raw_identity)
        api_key = bytearray()
        private_key = bytearray()
        try:
            api_key = _read_secure_file(home / API_KEY_BASENAME, 512)
            private_key = _read_secure_file(home / STARK_PRIVATE_KEY_BASENAME, 128)
        except BaseException:
            _zeroize(api_key)
            _zeroize(private_key)
            _fail("CREDENTIAL_FILE_UNAVAILABLE", "AUTH")
        try:
            # Validate before returning the handle while keeping all raw files
            # out of exceptions and durable state.
            try:
                api_text = bytes(api_key).decode("ascii")
            except UnicodeDecodeError:
                _fail("API_KEY_INVALID", "AUTH")
            if (
                not api_text
                or api_text.strip() != api_text
                or any(ord(char) < 33 or ord(char) > 126 for char in api_text)
            ):
                _fail("API_KEY_INVALID", "AUTH")
            try:
                private_text = bytes(private_key).decode("ascii")
            except UnicodeDecodeError:
                _fail("PRIVATE_KEY_INVALID", "AUTH")
            _validate_stark_private_key(private_text, identity)
            return ExtendedCredentialCapability(api_key, private_key, identity)
        except BaseException:
            _zeroize(api_key)
            _zeroize(private_key)
            raise


@dataclass(frozen=True)
class SignedOrder:
    payload: Mapping[str, Any]
    external_id: str
    settlement_hash: str | None


@dataclass(frozen=True)
class WriteReceipt:
    accepted: bool
    order_id: str | None = None
    external_id: str | None = None


@dataclass(frozen=True)
class OrderIntent:
    id: str
    kind: str
    state: str
    nonce: int
    external_id: str
    settlement_identity: str
    expiry_ms: int
    account_id: int
    l2_key: str
    market: str
    side: str
    qty: Decimal
    price: Decimal
    fee: Decimal
    reduce_only: bool
    target_id: str | None = None
    target_external_id: str | None = None
    venue_order_id: str | None = None
    payload: Mapping[str, Any] | None = None
    payload_digest: str | None = None
    dispatch_count: int = 0


def _reject_secret_keys(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized_key = "".join(char for char in key.lower() if char.isalnum()) if type(key) is str else ""
            if any(
                token in normalized_key
                for token in ("private", "secret", "password", "apikey", "authorization")
            ):
                _fail("SECRET_PERSISTENCE_FORBIDDEN")
            _reject_secret_keys(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _reject_secret_keys(item)


class OperationalIntentStore:
    """SQLite journal for one sealed Level-C lifecycle."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        _prepare_file(self.path)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS extended_level_c_intents (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    state TEXT NOT NULL,
                    nonce INTEGER NOT NULL UNIQUE,
                    external_id TEXT NOT NULL UNIQUE,
                    settlement_identity TEXT NOT NULL,
                    expiry_ms INTEGER NOT NULL,
                    account_id INTEGER NOT NULL,
                    l2_key TEXT NOT NULL,
                    market TEXT NOT NULL,
                    side TEXT NOT NULL,
                    qty TEXT NOT NULL,
                    price TEXT NOT NULL,
                    fee TEXT NOT NULL,
                    reduce_only INTEGER NOT NULL,
                    target_id TEXT,
                    target_external_id TEXT,
                    venue_order_id TEXT,
                    payload_json TEXT,
                    payload_digest TEXT,
                    dispatch_count INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS extended_level_c_lifecycle (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    state TEXT NOT NULL
                )
                """
            )
            connection.execute(
                "INSERT OR IGNORE INTO extended_level_c_lifecycle(singleton, state) VALUES (1, 'FLAT')"
            )
            columns = {
                str(row[1])
                for row in connection.execute(
                    "PRAGMA table_info(extended_level_c_intents)"
                )
            }
            required = {
                "id", "kind", "state", "nonce", "external_id", "settlement_identity",
                "expiry_ms", "account_id", "l2_key", "market", "side", "qty", "price",
                "fee", "reduce_only", "target_id", "target_external_id", "venue_order_id",
                "payload_json", "payload_digest", "dispatch_count",
            }
            if not required <= columns:
                _fail("STORE_SCHEMA")
            connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS extended_level_c_settlement_identity "
                "ON extended_level_c_intents(settlement_identity)"
            )

    @staticmethod
    def _from_row(row: sqlite3.Row) -> OrderIntent:
        payload = None if row["payload_json"] is None else json.loads(row["payload_json"])
        if payload is not None:
            _reject_secret_keys(payload)
        return OrderIntent(
            id=row["id"], kind=row["kind"], state=row["state"], nonce=int(row["nonce"]),
            external_id=row["external_id"], settlement_identity=row["settlement_identity"],
            expiry_ms=int(row["expiry_ms"]), account_id=int(row["account_id"]),
            l2_key=row["l2_key"], market=row["market"], side=row["side"],
            qty=Decimal(row["qty"]), price=Decimal(row["price"]), fee=Decimal(row["fee"]),
            reduce_only=bool(row["reduce_only"]), target_id=row["target_id"],
            target_external_id=row["target_external_id"], venue_order_id=row["venue_order_id"],
            payload=payload, payload_digest=row["payload_digest"],
            dispatch_count=int(row["dispatch_count"]),
        )

    def lifecycle_state(self) -> str:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT state FROM extended_level_c_lifecycle WHERE singleton=1"
            ).fetchone()
        if row is None:
            _fail("LIFECYCLE_SCHEMA")
        return str(row[0])

    def _set_lifecycle(self, state: str) -> None:
        if type(state) is not str or not state:
            _fail("LIFECYCLE_SCHEMA")
        with self._connect() as connection:
            changed = connection.execute(
                "UPDATE extended_level_c_lifecycle SET state=? WHERE singleton=1",
                (state,),
            )
            if changed.rowcount != 1:
                _fail("LIFECYCLE_SCHEMA")
        _fsync(self.path)

    def prepare(
        self,
        *,
        kind: str,
        nonce: int,
        external_id: str,
        expiry_ms: int,
        account_id: int,
        l2_key: str,
        market: str,
        side: str,
        qty: Decimal,
        price: Decimal,
        fee: Decimal,
        reduce_only: bool,
        target_id: str | None = None,
        target_external_id: str | None = None,
        settlement_identity: str | None = None,
        expected_lifecycle: str,
        next_lifecycle: str,
    ) -> OrderIntent:
        if (
            type(kind) is not str or kind not in {"ENTRY", "CANCEL", "CLOSE"}
            or type(nonce) is not int or not 0 < nonce < 2**31
            or type(external_id) is not str or not external_id
            or type(expiry_ms) is not int or expiry_ms <= 0
            or type(account_id) is not int or account_id < 0
            or type(l2_key) is not str or type(market) is not str
            or type(side) is not str or side not in {"BUY", "SELL", "NONE"}
            or type(reduce_only) is not bool
            or self.lifecycle_state() != expected_lifecycle
        ):
            _fail("INTENT_PREPARATION_REJECTED")
        if settlement_identity is None:
            settlement_identity = external_id
        if type(settlement_identity) is not str or not settlement_identity:
            _fail("SETTLEMENT_IDENTITY_MISSING")
        intent = OrderIntent(
            id=f"{kind.lower()}-{uuid.uuid4().hex}", kind=kind, state="PREPARED",
            nonce=nonce, external_id=external_id,
            settlement_identity=settlement_identity, expiry_ms=expiry_ms,
            account_id=account_id, l2_key=l2_key, market=market, side=side,
            qty=qty, price=price, fee=fee, reduce_only=reduce_only,
            target_id=target_id, target_external_id=target_external_id,
        )
        try:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO extended_level_c_intents(
                        id, kind, state, nonce, external_id, settlement_identity,
                        expiry_ms, account_id, l2_key, market, side, qty, price, fee,
                        reduce_only, target_id, target_external_id
                    ) VALUES (?, ?, 'PREPARED', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        intent.id, kind, nonce, external_id, settlement_identity,
                        expiry_ms, account_id, l2_key, market, side, str(qty), str(price),
                        str(fee), int(reduce_only), target_id, target_external_id,
                    ),
                )
                changed = connection.execute(
                    "UPDATE extended_level_c_lifecycle SET state=? WHERE singleton=1 AND state=?",
                    (next_lifecycle, expected_lifecycle),
                )
                if changed.rowcount != 1:
                    _fail("LIFECYCLE_CONFLICT")
        except sqlite3.IntegrityError:
            _fail("DURABLE_IDENTITY_REUSE")
        _fsync(self.path)
        return intent

    def get(self, intent_id: str) -> OrderIntent:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM extended_level_c_intents WHERE id=?", (intent_id,)
            ).fetchone()
        if row is None:
            _fail("INTENT_NOT_FOUND")
        return self._from_row(row)

    def all(self) -> tuple[OrderIntent, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM extended_level_c_intents ORDER BY rowid"
            ).fetchall()
        return tuple(self._from_row(row) for row in rows)

    def claim(self, intent_id: str, *, expected_lifecycle: str, next_lifecycle: str) -> OrderIntent:
        with self._connect() as connection:
            current = connection.execute(
                "SELECT state FROM extended_level_c_lifecycle WHERE singleton=1"
            ).fetchone()
            if current is None or current[0] != expected_lifecycle:
                _fail("LIFECYCLE_CONFLICT")
            changed = connection.execute(
                """
                UPDATE extended_level_c_intents
                SET state='CLAIMED', dispatch_count=dispatch_count+1
                WHERE id=? AND state='PREPARED'
                """,
                (intent_id,),
            )
            if changed.rowcount != 1:
                _fail("INTENT_NOT_PREPARED")
            changed = connection.execute(
                "UPDATE extended_level_c_lifecycle SET state=? WHERE singleton=1 AND state=?",
                (next_lifecycle, expected_lifecycle),
            )
            if changed.rowcount != 1:
                _fail("LIFECYCLE_CONFLICT")
        _fsync(self.path)
        return self.get(intent_id)

    def bind_signed(
        self,
        intent_id: str,
        *,
        payload: Mapping[str, Any],
        payload_digest: str,
        settlement_identity: str | None = None,
    ) -> OrderIntent:
        _reject_secret_keys(payload)
        if type(payload_digest) is not str or len(payload_digest) != 64:
            _fail("PAYLOAD_DIGEST_INVALID")
        if payload_digest != canonical_digest(payload):
            _fail("PAYLOAD_DIGEST_MISMATCH", "IDENTITY")
        intent = self.get(intent_id)
        if intent.state != "CLAIMED" or intent.payload is not None:
            _fail("SIGNED_PAYLOAD_STATE_INVALID")
        identity = settlement_identity or intent.settlement_identity
        if type(identity) is not str or not identity:
            _fail("SETTLEMENT_IDENTITY_INVALID", "IDENTITY")
        try:
            with self._connect() as connection:
                changed = connection.execute(
                    """
                    UPDATE extended_level_c_intents
                    SET payload_json=?, payload_digest=?, settlement_identity=?
                    WHERE id=? AND state='CLAIMED' AND payload_json IS NULL
                    """,
                    (_json(payload), payload_digest, identity, intent_id),
                )
                if changed.rowcount != 1:
                    _fail("SIGNED_PAYLOAD_STATE_INVALID")
        except sqlite3.IntegrityError:
            _fail("SETTLEMENT_IDENTITY_REUSE", "IDENTITY")
        _fsync(self.path)
        return self.get(intent_id)

    def mark_accepted(self, intent_id: str, order_id: str, external_id: str) -> OrderIntent:
        order_id = _venue_order_id(order_id, "WRITE_RESPONSE_ORDER_ID_INVALID")
        if not external_id:
            _fail("WRITE_RESPONSE_IDENTITY_MISSING", "SCHEMA")
        intent = self.get(intent_id)
        if intent.state not in {"CLAIMED", "AMBIGUOUS"} or intent.payload is None:
            _fail("WRITE_RESPONSE_STATE_INVALID")
        if external_id != intent.external_id:
            _fail("WRITE_RESPONSE_EXTERNAL_MISMATCH", "IDENTITY")
        with self._connect() as connection:
            changed = connection.execute(
                """
                UPDATE extended_level_c_intents
                SET state='ACCEPTED', venue_order_id=?
                WHERE id=? AND state IN ('CLAIMED', 'AMBIGUOUS') AND venue_order_id IS NULL
                """,
                (order_id, intent_id),
            )
            if changed.rowcount != 1:
                _fail("WRITE_RESPONSE_STATE_INVALID")
        _fsync(self.path)
        return self.get(intent_id)

    def mark(self, intent_id: str, state: str, *, lifecycle: str | None = None) -> OrderIntent:
        if type(state) is not str or not state:
            _fail("INTENT_STATE_INVALID")
        with self._connect() as connection:
            changed = connection.execute(
                "UPDATE extended_level_c_intents SET state=? WHERE id=?", (state, intent_id)
            )
            if changed.rowcount != 1:
                _fail("INTENT_NOT_FOUND")
            if lifecycle is not None:
                changed = connection.execute(
                    "UPDATE extended_level_c_lifecycle SET state=? WHERE singleton=1",
                    (lifecycle,),
                )
                if changed.rowcount != 1:
                    _fail("LIFECYCLE_SCHEMA")
        _fsync(self.path)
        return self.get(intent_id)

    def mark_ambiguous(self, intent_id: str) -> OrderIntent:
        intent = self.get(intent_id)
        lifecycle = {
            "ENTRY": "ENTRY_AMBIGUOUS",
            "CLOSE": "CLOSE_AMBIGUOUS",
            "CANCEL": "CANCEL_AMBIGUOUS",
        }.get(intent.kind)
        if lifecycle is None:
            _fail("INTENT_KIND_INVALID")
        return self.mark(intent_id, "AMBIGUOUS", lifecycle=lifecycle)

    def mark_rejected(self, intent_id: str) -> OrderIntent:
        intent = self.get(intent_id)
        lifecycle = {
            "ENTRY": "ENTRY_AMBIGUOUS",
            "CANCEL": "CANCEL_AMBIGUOUS",
            "CLOSE": "CLOSE_AMBIGUOUS",
        }.get(intent.kind)
        if lifecycle is None:
            _fail("INTENT_KIND_INVALID")
        return self.mark(intent_id, "REJECTED", lifecycle=lifecycle)

    def mark_reconciled(self, intent_id: str, *, close: bool = False) -> OrderIntent:
        state = "CLOSE_RECONCILED" if close else "ENTRY_RECONCILED"
        lifecycle = "EXPOSED" if not close else "EXPOSED"
        return self.mark(intent_id, state, lifecycle=lifecycle)

    def mark_no_fill(self, intent_id: str) -> OrderIntent:
        return self.mark(intent_id, "ENTRY_RECONCILED_NO_FILL", lifecycle="FLAT_PENDING_EXPIRY")

    def dispatch_count(self, intent_id: str) -> int:
        return self.get(intent_id).dispatch_count

    def terminal(self, state: str) -> None:
        self._set_lifecycle(state)


class RuntimeRunJournal:
    """One durable runtime identity, separate from the historical Level-B DB."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @staticmethod
    def _validate_schema(connection: sqlite3.Connection) -> None:
        columns = {
            str(row[1])
            for row in connection.execute(
                "PRAGMA table_info(extended_level_c_runtime)"
            )
        }
        required = {
            "singleton", "run_id", "created_at_ms", "state", "failure_class",
            "stage", "report_json",
        }
        if not required <= columns:
            _fail("RUNTIME_SCHEMA")

    def begin(self, created_at_ms: int) -> str:
        if type(created_at_ms) is not int or created_at_ms <= 0:
            _fail("RUNTIME_CLOCK_INVALID")
        _prepare_file(self.path)
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS extended_level_c_runtime (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    run_id TEXT NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    failure_class TEXT,
                    stage TEXT,
                    report_json TEXT
                )
                """
            )
            self._validate_schema(connection)
            existing = connection.execute(
                "SELECT state FROM extended_level_c_runtime WHERE singleton=1"
            ).fetchone()
            if existing is not None:
                _fail("RUNTIME_ALREADY_TERMINAL" if existing[0] != "STARTED" else "RUNTIME_RESTART_REQUIRED", "RESTART")
            run_id = "extended-level-c-" + uuid.uuid4().hex
            connection.execute(
                """
                INSERT INTO extended_level_c_runtime
                (singleton, run_id, created_at_ms, state, failure_class, stage, report_json)
                VALUES (1, ?, ?, 'STARTED', NULL, NULL, NULL)
                """,
                (run_id, created_at_ms),
            )
        _fsync(self.path)
        return run_id

    def terminalize(
        self,
        run_id: str,
        *,
        state: str,
        failure_class: str | None = None,
        stage: str,
        report: Mapping[str, Any] | None = None,
    ) -> None:
        if state not in {"COMPLETE", "BLOCKED"} or stage not in _RUNTIME_STAGES:
            _fail("RUNTIME_TERMINAL_INVALID")
        if failure_class is not None and failure_class not in _FAILURE_CLASSES:
            _fail("RUNTIME_FAILURE_INVALID")
        if report is not None:
            _reject_secret_keys(report)
        with self._connect() as connection:
            changed = connection.execute(
                """
                UPDATE extended_level_c_runtime
                SET state=?, failure_class=?, stage=?, report_json=?
                WHERE singleton=1 AND run_id=? AND state='STARTED'
                """,
                (
                    state, failure_class, stage,
                    None if report is None else _json(report), run_id,
                ),
            )
            if changed.rowcount != 1:
                _fail("RUNTIME_TERMINAL_CONFLICT")
        _fsync(self.path)

    def snapshot(self) -> Mapping[str, Any] | None:
        with self._connect() as connection:
            table = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='extended_level_c_runtime'"
            ).fetchone()
            if table is None:
                return None
            self._validate_schema(connection)
            row = connection.execute(
                "SELECT * FROM extended_level_c_runtime WHERE singleton=1"
            ).fetchone()
        if row is None:
            return None
        return {
            "run_id": row["run_id"], "created_at_ms": row["created_at_ms"],
            "state": row["state"], "failure_class": row["failure_class"],
            "stage": row["stage"],
            "report": None if row["report_json"] is None else json.loads(row["report_json"]),
        }


@dataclass(frozen=True)
class RestObservation:
    observed_at_ms: int
    server_time_ms: int | None
    account: Mapping[str, Any]
    market: Mapping[str, Any]
    book: Mapping[str, Any]
    balance: Mapping[str, Any]
    fees: tuple[Mapping[str, Any], ...]
    leverage: tuple[Mapping[str, Any], ...]
    open_orders: tuple[Mapping[str, Any], ...]
    positions: tuple[Mapping[str, Any], ...]
    order_history: tuple[Mapping[str, Any], ...]
    trades: tuple[Mapping[str, Any], ...]
    exact_by_id: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    exact_by_external: Mapping[str, tuple[Mapping[str, Any], ...]] = field(default_factory=dict)
    stream_frames: int = 0
    fingerprint: str | None = None
    book_observed_at_ms: int | None = None
    receipt_times_ms: Mapping[str, int] = field(default_factory=dict)

    @property
    def semantic_fingerprint(self) -> str:
        if self.fingerprint is not None:
            return self.fingerprint
        return _observation_fingerprint(self)


def _row_identity(row: Mapping[str, Any], *, position: bool = False) -> dict[str, Any]:
    if position:
        keys = ("id", "accountId", "market", "status", "side", "size", "value", "openPrice", "markPrice", "leverage")
    else:
        keys = ("id", "accountId", "externalId", "market", "type", "side", "status", "price", "averagePrice", "qty", "filledQty", "cancelledQty", "reduceOnly", "postOnly", "timeInForce", "expiryTime", "expireTime")
    return {key: row.get(key) for key in keys if key in row}


def _trade_identity(row: Mapping[str, Any]) -> dict[str, Any]:
    external = row.get("externalId", row.get("externalOrderId"))
    return {
        "id": row.get("id"), "accountId": row.get("accountId"),
        "externalId": external, "market": row.get("market"),
        "orderId": row.get("orderId"), "side": row.get("side"),
        "price": row.get("price"), "qty": row.get("qty"),
        "value": row.get("value"), "fee": row.get("fee"),
    }


def _observation_fingerprint(observation: RestObservation) -> str:
    state = {
        "account": {
            "id": observation.account.get("id", observation.account.get("accountId")),
            "accountIndex": observation.account.get("accountIndex"),
            "l2Key": observation.account.get("l2Key"),
            "l2Vault": observation.account.get("l2Vault"),
            "status": observation.account.get("status"),
        },
        "open": sorted(
            (_row_identity(row) for row in observation.open_orders),
            key=lambda row: str((row.get("id"), row.get("externalId"))),
        ),
        "positions": sorted(
            (_row_identity(row, position=True) for row in observation.positions),
            key=lambda row: str(row.get("id")),
        ),
        "history": sorted(
            (_row_identity(row) for row in observation.order_history),
            key=lambda row: str((row.get("id"), row.get("externalId"))),
        ),
        "trades": sorted(
            (_trade_identity(row) for row in observation.trades),
            key=lambda row: str(row.get("id")),
        ),
    }
    return canonical_digest(state)


class VenueIO(Protocol):
    def now_ms(self) -> int: ...
    def observe(self, intents: Sequence[OrderIntent]) -> RestObservation: ...
    def place_order(self, intent: OrderIntent, payload: Mapping[str, Any]) -> WriteReceipt: ...
    def cancel_order(self, intent: OrderIntent, order_id: str) -> WriteReceipt: ...


class RestFailure(OperationalSafetyError):
    def __init__(
        self,
        code: str,
        *,
        ambiguous: bool = False,
        failure_class: str = "TRANSPORT",
    ) -> None:
        super().__init__(code, failure_class="AMBIGUOUS_WRITE" if ambiguous else failure_class)
        self.ambiguous = ambiguous


class ExtendedRestTransport:
    """Fixed direct-TLS REST transport with no proxy, redirect, or retry."""

    def __init__(self, api_key: str) -> None:
        if type(api_key) is not str or not api_key:
            _fail("API_KEY_INVALID", "AUTH")
        base = urlsplit(TESTNET_CONTRACT["apiBaseUrl"])
        if base.scheme != "https" or base.path != "/api/v1":
            _fail("TESTNET_ENDPOINT_INVALID", "IDENTITY")
        self._host = base.netloc
        self._base_path = base.path
        self._api_key = api_key
        self._connection_factory = lambda: http.client.HTTPSConnection(
            self._host, timeout=HTTP_TIMEOUT_SECONDS,
            context=ssl.create_default_context(),
        )

    @staticmethod
    def _headers(api_key: str) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": f"X10PythonTradingClient/{SDK_VERSION}",
            "X-Api-Key": api_key,
        }

    def request(
        self,
        method: str,
        path: str,
        *,
        query: Sequence[tuple[str, Any]] = (),
        body: Mapping[str, Any] | None = None,
        allow_404: bool = False,
    ) -> Mapping[str, Any] | None:
        if method not in {"GET", "POST", "DELETE"} or not path.startswith("/"):
            _fail("REST_REQUEST_INVALID")
        if any(type(key) is not str or not key for key, _ in query):
            _fail("REST_QUERY_INVALID")
        url_path = self._base_path + path
        encoded = urlencode(list(query), doseq=True)
        if encoded:
            url_path += "?" + encoded
        connection: http.client.HTTPSConnection | None = None
        try:
            connection = self._connection_factory()
            try:
                raw_body = None if body is None else _json(body).encode("utf-8")
            except (TypeError, ValueError, UnicodeError):
                _fail("REST_REQUEST_BODY", "SCHEMA")
            connection.request(
                method, url_path, body=raw_body, headers=self._headers(self._api_key)
            )
            response = connection.getresponse()
            raw = response.read(MAX_REST_RESPONSE_BYTES + 1)
            if len(raw) > MAX_REST_RESPONSE_BYTES:
                _fail("REST_RESPONSE_TOO_LARGE", "SCHEMA")
            if response.status == 404 and allow_404:
                return None
            try:
                value = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError):
                _fail("REST_RESPONSE_JSON", "SCHEMA")
            if not isinstance(value, Mapping):
                _fail("REST_RESPONSE_SCHEMA", "SCHEMA")
            if not 200 <= response.status < 300:
                raise RestFailure(
                    "REST_HTTP_STATUS",
                    ambiguous=method in {"POST", "DELETE"} and response.status >= 500,
                    failure_class="HTTP",
                )
            return value
        except OperationalSafetyError:
            raise
        except RestFailure:
            raise
        except (http.client.HTTPException, OSError, TimeoutError):
            # A write transport failure is always ambiguous; callers never
            # replay it.  Reads are also terminal but keep the same safe class.
            raise RestFailure(
                "REST_TRANSPORT", ambiguous=method in {"POST", "DELETE"},
                failure_class="TRANSPORT",
            ) from None
        finally:
            try:
                if connection is not None:
                    connection.close()
            except BaseException:
                pass


def _unwrap_object(value: Mapping[str, Any], code: str) -> Mapping[str, Any]:
    if value.get("status") != OK or "data" not in value or not isinstance(value["data"], Mapping):
        _fail(code, "SCHEMA")
    return value["data"]


def _validate_page_meta(value: Mapping[str, Any], length: int, *, nonempty: bool) -> int | None:
    if not nonempty:
        if "pagination" not in value or value["pagination"] is None:
            return None
        pagination = value["pagination"]
        if not isinstance(pagination, Mapping) or "count" not in pagination:
            _fail("PAGINATION_SCHEMA", "SCHEMA")
        if type(pagination["count"]) is not int:
            _fail("PAGINATION_COUNT_INVALID", "SCHEMA")
        if pagination.get("cursor") is not None or pagination["count"] != 0:
            _fail("PAGINATION_EMPTY_CONTRADICTION", "SAFETY")
        return None
    pagination = value.get("pagination")
    if not isinstance(pagination, Mapping) or not {"cursor", "count"} <= set(pagination):
        _fail("NONEMPTY_PAGINATION_REQUIRED", "SAFETY")
    count = pagination.get("count")
    cursor = pagination.get("cursor")
    if type(count) is not int or not 0 < count <= MAX_REST_PAGE_ITEMS or count != length:
        _fail("PAGINATION_COUNT_INVALID", "SCHEMA")
    if cursor is not None and (type(cursor) is not int or cursor <= 0):
        _fail("PAGINATION_CURSOR_INVALID", "SCHEMA")
    return cursor


def _list_data(
    value: Mapping[str, Any],
    code: str,
    *,
    allow_single_unpaginated: bool = False,
) -> tuple[list[Mapping[str, Any]], int | None]:
    if value.get("status") != OK or "data" not in value or not isinstance(value["data"], list):
        _fail(code, "SCHEMA")
    rows = value["data"]
    if any(not isinstance(row, Mapping) for row in rows):
        _fail(code, "SCHEMA")
    if allow_single_unpaginated and len(rows) == 1 and (
        "pagination" not in value or value["pagination"] is None
    ):
        return list(rows), None
    cursor = _validate_page_meta(value, len(rows), nonempty=bool(rows))
    return list(rows), cursor


def _limit_price_config(
    market: Mapping[str, Any],
) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    if not isinstance(market, Mapping):
        _fail("MARKET_SCHEMA", "SCHEMA")
    config = market.get("tradingConfig")
    stats = market.get("marketStats")
    if not isinstance(config, Mapping) or not isinstance(stats, Mapping):
        _fail("MARKET_SCHEMA", "SCHEMA")
    mark_price = _decimal(stats.get("markPrice"), "MARK_PRICE_SCHEMA")
    cap = _decimal(config.get("limitPriceCap"), "PRICE_CAP_SCHEMA")
    floor = _decimal(config.get("limitPriceFloor"), "PRICE_FLOOR_SCHEMA")
    tick = _decimal(config.get("minPriceChange"), "PRICE_TICK_SCHEMA")
    if mark_price <= 0:
        _fail("MARK_PRICE_INVALID")
    if not 0 <= cap <= 1:
        _fail("PRICE_CAP_INVALID")
    if not 0 <= floor <= 1:
        _fail("PRICE_FLOOR_INVALID")
    if tick <= 0:
        _fail("PRICE_TICK_INVALID")
    return mark_price, cap, floor, tick


def _validate_limit_price(
    observation: RestObservation,
    *,
    side: str,
    price: Decimal,
) -> None:
    mark_price, cap, floor, tick = _limit_price_config(observation.market)
    actual_price = _decimal(price, "PRICE_SCHEMA")
    try:
        tick_aligned = actual_price % tick == 0
    except (InvalidOperation, ValueError):
        tick_aligned = False
    if actual_price <= 0 or not tick_aligned:
        _fail("PRICE_OFF_GRID")
    try:
        if side == "BUY":
            bound = mark_price * (Decimal(1) + cap)
            invalid = actual_price > bound
        elif side == "SELL":
            bound = mark_price * (Decimal(1) - floor)
            invalid = actual_price < bound
        else:
            _fail("ORDER_SIDE_INVALID")
    except (InvalidOperation, OverflowError):
        _fail("PRICE_BOUND_INVALID")
    if not bound.is_finite():
        _fail("PRICE_BOUND_INVALID")
    if invalid:
        _fail("PRICE_BOUND_INVALID")


def _validate_fresh(observation: RestObservation, now_ms: int) -> None:
    server_fresh = (
        observation.server_time_ms is None
        or (
            type(observation.server_time_ms) is int
            and observation.server_time_ms > 0
            and 0 <= now_ms - observation.server_time_ms <= MAX_FRESHNESS_MS
        )
    )
    book_fresh = (
        observation.book_observed_at_ms is None
        or (
            type(observation.book_observed_at_ms) is int
            and observation.book_observed_at_ms > 0
            and 0 <= now_ms - observation.book_observed_at_ms <= MAX_FRESHNESS_MS
        )
    )
    receipt_times = observation.receipt_times_ms
    if receipt_times:
        receipts_fresh = isinstance(receipt_times, Mapping) and (
            _REQUIRED_REST_RECEIPTS <= set(receipt_times)
            and all(
                type(receipt) is int
                and receipt > 0
                and 0 <= now_ms - receipt <= MAX_FRESHNESS_MS
                for receipt in receipt_times.values()
            )
            and observation.book_observed_at_ms == receipt_times.get("book")
        )
    else:
        # Dependency-injected fixture IO has one aggregate observation clock;
        # production REST observations always carry the per-response map.
        receipts_fresh = True
    if (
        type(observation.observed_at_ms) is not int
        or observation.observed_at_ms <= 0
        or not 0 <= now_ms - observation.observed_at_ms <= MAX_FRESHNESS_MS
        or not server_fresh
        or not book_fresh
        or not receipts_fresh
        or observation.stream_frames != 0
    ):
        _fail("FRESH_REST_OBSERVATION_REQUIRED")


class OperationalVenueIO:
    """REST-only Extended testnet observer and write binding."""

    def __init__(
        self,
        capability: ExtendedCredentialCapability,
        *,
        transport: ExtendedRestTransport | None = None,
        clock: Callable[[], int] | None = None,
    ) -> None:
        self._capability = capability
        self._transport = transport or ExtendedRestTransport(capability.api_key())
        self._clock = clock or (lambda: time.time_ns() // 1_000_000)
        self._clock_lock = Lock()
        self.stream_frames = 0

    def now_ms(self) -> int:
        with self._clock_lock:
            return int(self._clock())

    def open_stream(self) -> None:
        _fail("STREAM_UNAVAILABLE", "SAFETY")

    def _request(
        self,
        method: str,
        path: str,
        *,
        query: Sequence[tuple[str, Any]] = (),
        body: Mapping[str, Any] | None = None,
        allow_404: bool = False,
    ) -> Mapping[str, Any] | None:
        try:
            return self._transport.request(
                method, path, query=query, body=body, allow_404=allow_404
            )
        except RestFailure as error:
            if error.ambiguous:
                raise AmbiguousWrite() from None
            _fail(error.code, error.failure_class)

    def _object(
        self,
        path: str,
        *,
        code: str,
        allow_404: bool = False,
        receipt: Callable[[int], None] | None = None,
    ) -> Mapping[str, Any] | None:
        value = self._request("GET", path, allow_404=allow_404)
        if receipt is not None:
            receipt(self.now_ms())
        if value is None:
            return None
        return _unwrap_object(value, code)

    def _list(
        self,
        path: str,
        *,
        query: Sequence[tuple[str, Any]] = (),
        code: str,
        allow_404: bool = False,
        receipt: Callable[[int], None] | None = None,
    ) -> tuple[Mapping[str, Any], ...]:
        rows: list[Mapping[str, Any]] = []
        cursor: int | None = None
        seen_cursors: set[int] = set()
        allow_single_unpaginated = (
            path in {"/info/markets", "/user/fees", "/user/leverage"}
            and tuple(query) == (("market", TARGET_MARKET),)
        )
        for page in range(MAX_REST_PAGES):
            page_query = list(query)
            page_query.append(("limit", MAX_REST_PAGE_ITEMS))
            if cursor is not None:
                page_query.append(("cursor", cursor))
            value = self._request(
                "GET", path, query=tuple(page_query), allow_404=allow_404
            )
            if receipt is not None:
                receipt(self.now_ms())
            if value is None:
                return ()
            page_rows, next_cursor = _list_data(
                value, code, allow_single_unpaginated=allow_single_unpaginated
            )
            rows.extend(page_rows)
            if len(rows) > MAX_REST_PAGE_ITEMS:
                _fail("PAGINATION_BOUND_EXCEEDED")
            if next_cursor is None:
                return tuple(rows)
            if next_cursor in seen_cursors:
                _fail("PAGINATION_CURSOR_REPLAY")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        _fail("PAGINATION_UNBOUNDED")

    @staticmethod
    def _timed_read(
        reader: Callable[[Callable[[int], None]], Any],
    ) -> tuple[Any, int]:
        receipts: list[int] = []
        value = reader(receipts.append)
        if not receipts:
            _fail("REST_RECEIPT_MISSING", "SAFETY")
        # A paginated response is only as fresh as its oldest page.
        return value, min(receipts)

    @staticmethod
    def _market(value: Mapping[str, Any]) -> Mapping[str, Any]:
        required = {
            "name", "type", "active", "isRfq", "isOffHours", "collateralAssetName",
            "tradingConfig", "marketStats", "l2Config",
        }
        if not required <= set(value):
            _fail("MARKET_SCHEMA", "SCHEMA")
        config = value["tradingConfig"]
        if not isinstance(config, Mapping):
            _fail("MARKET_SCHEMA", "SCHEMA")
        try:
            minimum = _decimal(config["minOrderSize"], "MARKET_SIZE_SCHEMA")
            step = _decimal(config["minOrderSizeChange"], "MARKET_SIZE_SCHEMA")
            tick = _decimal(config["minPriceChange"], "MARKET_PRICE_SCHEMA")
            max_leverage = _decimal(config["maxLeverage"], "MARKET_LEVERAGE_SCHEMA")
        except KeyError:
            _fail("MARKET_SCHEMA", "SCHEMA")
        if (
            value["name"] != TARGET_MARKET
            or value["type"] != PERPETUAL
            or value["active"] is not True
            or value["isRfq"] is not False
            or value["isOffHours"] is not False
            or minimum <= 0
            or step <= 0
            or minimum % step != 0
            or tick <= 0
            or max_leverage <= 0
        ):
            _fail("MARKET_SAFETY_GATE")
        _limit_price_config(value)
        return value

    @staticmethod
    def _book(value: Mapping[str, Any], market: Mapping[str, Any]) -> Mapping[str, Any]:
        # The pinned SDK accepts the compact REST aliases m/b/a; the fixture
        # contract uses the expanded names.  Both are observed official forms.
        market_name = value.get("market", value.get("m"))
        bids = value.get("bids", value.get("bid", value.get("b")))
        asks = value.get("asks", value.get("ask", value.get("a")))
        if market_name != TARGET_MARKET or not isinstance(bids, list) or not isinstance(asks, list):
            _fail("BOOK_SCHEMA", "SCHEMA")
        config = market["tradingConfig"]
        tick = _decimal(config["minPriceChange"], "BOOK_TICK_SCHEMA")
        step = _decimal(config["minOrderSizeChange"], "BOOK_STEP_SCHEMA")

        def levels(raw: list[Any], side: str) -> list[dict[str, str]]:
            result: list[dict[str, str]] = []
            for item in raw:
                if not isinstance(item, Mapping):
                    _fail("BOOK_SCHEMA", "SCHEMA")
                price = _decimal(item.get("price", item.get("p")), "BOOK_PRICE_SCHEMA")
                qty = _decimal(item.get("qty", item.get("q")), "BOOK_QTY_SCHEMA")
                if price <= 0 or qty <= 0 or price % tick != 0 or qty % step != 0:
                    _fail("BOOK_LEVEL_INVALID")
                result.append({"price": str(price), "qty": str(qty)})
            if not result:
                _fail("BOOK_DEPTH_INSUFFICIENT")
            prices = [Decimal(row["price"]) for row in result]
            if side == "bids" and any(left <= right for left, right in zip(prices, prices[1:])):
                _fail("BOOK_ORDER_INVALID")
            if side == "asks" and any(left >= right for left, right in zip(prices, prices[1:])):
                _fail("BOOK_ORDER_INVALID")
            return result

        normalized = {"market": TARGET_MARKET, "bids": levels(bids, "bids"), "asks": levels(asks, "asks")}
        if Decimal(normalized["bids"][0]["price"]) >= Decimal(normalized["asks"][0]["price"]):
            _fail("BOOK_CROSSED")
        return normalized

    def observe(self, intents: Sequence[OrderIntent]) -> RestObservation:
        account, account_receipt = self._timed_read(
            lambda receipt: self._object(
                "/user/account/info", code="ACCOUNT_SCHEMA", receipt=receipt,
            )
        )
        if account is None or not self._capability.identity.matches_account(account):
            _fail("ACCOUNT_IDENTITY_MISMATCH", "IDENTITY")
        if account.get("status") != ACTIVE:
            _fail("ACCOUNT_INACTIVE")
        readers: dict[str, Callable[[Callable[[int], None]], Any]] = {
            "balance": lambda receipt: self._object(
                "/user/balance", code="BALANCE_SCHEMA", receipt=receipt,
            ),
            "market": lambda receipt: self._list(
                "/info/markets", query=(("market", TARGET_MARKET),),
                code="MARKET_LIST_SCHEMA", receipt=receipt,
            ),
            "book": lambda receipt: self._object(
                f"/info/markets/{quote(TARGET_MARKET, safe='')}/orderbook",
                code="BOOK_SCHEMA", receipt=receipt,
            ),
            "fees": lambda receipt: self._list(
                "/user/fees", query=(("market", TARGET_MARKET),),
                code="FEE_LIST_SCHEMA", receipt=receipt,
            ),
            "leverage": lambda receipt: self._list(
                "/user/leverage", query=(("market", TARGET_MARKET),),
                code="LEVERAGE_LIST_SCHEMA", receipt=receipt,
            ),
            "open_orders": lambda receipt: self._list(
                "/user/orders", code="OPEN_ORDER_LIST_SCHEMA", receipt=receipt,
            ),
            "positions": lambda receipt: self._list(
                "/user/positions", code="POSITION_LIST_SCHEMA", receipt=receipt,
            ),
            "order_history": lambda receipt: self._list(
                "/user/orders/history", code="ORDER_HISTORY_LIST_SCHEMA", receipt=receipt,
            ),
            "trades": lambda receipt: self._list(
                "/user/trades", code="TRADE_LIST_SCHEMA", receipt=receipt,
            ),
        }
        for intent in intents:
            if intent.kind not in {"ENTRY", "CLOSE"}:
                continue
            if intent.venue_order_id is not None:
                order_id = intent.venue_order_id
                readers[f"exact_id:{order_id}"] = lambda receipt, order_id=order_id: self._object(
                    f"/user/orders/{quote(order_id, safe='')}",
                    code="EXACT_ORDER_SCHEMA", allow_404=True, receipt=receipt,
                )
            external_id = intent.external_id
            readers[f"exact_external:{external_id}"] = lambda receipt, external_id=external_id: self._list(
                f"/user/orders/external/{quote(external_id, safe='')}",
                code="EXACT_EXTERNAL_ORDER_SCHEMA", allow_404=True, receipt=receipt,
            )

        with ThreadPoolExecutor(max_workers=min(32, len(readers))) as executor:
            futures = {
                key: executor.submit(self._timed_read, reader)
                for key, reader in readers.items()
            }
            results = {key: future.result() for key, future in futures.items()}

        balance, balance_receipt = results["balance"]
        if balance is None:
            _fail("BALANCE_SCHEMA", "SCHEMA")
        market_rows, market_receipt = results["market"]
        if len(market_rows) != 1:
            _fail("TARGET_MARKET_UNIQUE")
        market = self._market(market_rows[0])
        raw_book, book_receipt = results["book"]
        if raw_book is None:
            _fail("BOOK_SCHEMA", "SCHEMA")
        book = self._book(raw_book, market)
        fees, fees_receipt = results["fees"]
        leverage, leverage_receipt = results["leverage"]
        open_orders, open_orders_receipt = results["open_orders"]
        positions, positions_receipt = results["positions"]
        history, history_receipt = results["order_history"]
        trades, trades_receipt = results["trades"]
        exact_by_id: dict[str, Mapping[str, Any]] = {}
        exact_by_external: dict[str, tuple[Mapping[str, Any], ...]] = {}
        receipt_times_ms = {
            "account": account_receipt,
            "balance": balance_receipt,
            "market": market_receipt,
            "book": book_receipt,
            "fees": fees_receipt,
            "leverage": leverage_receipt,
            "open_orders": open_orders_receipt,
            "positions": positions_receipt,
            "order_history": history_receipt,
            "trades": trades_receipt,
        }
        for intent in intents:
            if intent.kind not in {"ENTRY", "CLOSE"}:
                continue
            if intent.venue_order_id is not None:
                key = f"exact_id:{intent.venue_order_id}"
                exact, exact_receipt = results[key]
                receipt_times_ms[key] = exact_receipt
                if exact is not None:
                    exact_by_id[intent.venue_order_id] = exact
            key = f"exact_external:{intent.external_id}"
            external_rows, external_receipt = results[key]
            receipt_times_ms[key] = external_receipt
            exact_by_external[intent.external_id] = external_rows
        observed_at = self.now_ms()
        observation = RestObservation(
            observed_at_ms=observed_at,
            # This direct REST binding has no authoritative server-clock field;
            # keep the server timestamp absent instead of relabeling local time.
            server_time_ms=None,
            account=dict(account), market=dict(market), book=book,
            balance=dict(balance), fees=tuple(dict(row) for row in fees),
            leverage=tuple(dict(row) for row in leverage),
            open_orders=tuple(dict(row) for row in open_orders),
            positions=tuple(dict(row) for row in positions),
            order_history=tuple(dict(row) for row in history),
            trades=tuple(dict(row) for row in trades),
            exact_by_id=exact_by_id, exact_by_external=exact_by_external,
            stream_frames=0,
            book_observed_at_ms=book_receipt,
            receipt_times_ms=receipt_times_ms,
        )
        _validate_fresh(observation, observed_at)
        return observation

    @staticmethod
    def _write_response(value: Mapping[str, Any], *, code: str) -> WriteReceipt:
        if value.get("status") not in {OK, "SUCCESS"}:
            if value.get("status") in {"ERROR", "FAILURE"}:
                raise VenueRejection(code)
            raise AmbiguousWrite(code)
        data = value.get("data")
        if not isinstance(data, Mapping):
            raise AmbiguousWrite(code)
        order_id = data.get("id")
        primary_external = data.get("externalId")
        alternate_external = data.get("externalOrderId")
        if primary_external is not None and alternate_external is not None and primary_external != alternate_external:
            raise AmbiguousWrite(code)
        external = primary_external if primary_external is not None else alternate_external
        if order_id is None and external is None:
            return WriteReceipt(True)
        if order_id is None:
            raise AmbiguousWrite(code)
        try:
            order_id = _venue_order_id(order_id, "WRITE_RESPONSE_ORDER_ID_INVALID")
        except OperationalSafetyError:
            raise AmbiguousWrite(code) from None
        if external is not None and type(external) is not str:
            raise AmbiguousWrite(code)
        return WriteReceipt(True, order_id, external)

    def place_order(self, intent: OrderIntent, payload: Mapping[str, Any]) -> WriteReceipt:
        response = self._request("POST", "/user/order", body=payload)
        if response is None:
            raise AmbiguousWrite()
        return self._write_response(response, code="PLACE_RESPONSE_INVALID")

    def cancel_order(self, intent: OrderIntent, order_id: str) -> WriteReceipt:
        response = self._request(
            "DELETE", f"/user/order/{quote(str(order_id), safe='')}"
        )
        if response is None:
            raise AmbiguousWrite("CANCEL_RESPONSE_INVALID")
        return self._write_response(response, code="CANCEL_RESPONSE_INVALID")


def _validate_signed_payload(intent: OrderIntent, payload: Mapping[str, Any], identity: ExtendedIdentity) -> None:
    _reject_secret_keys(payload)
    required = {
        "id", "market", "type", "side", "qty", "price", "reduceOnly", "postOnly",
        "timeInForce", "expiryEpochMillis", "fee", "nonce",
        "selfTradeProtectionLevel", "settlement",
    }
    allowed = required | {
        "cancelId", "trigger", "tpSlType", "takeProfit", "stopLoss",
        "debuggingAmounts", "builderFee", "builderId",
    }
    if not required <= set(payload) or not set(payload) <= allowed:
        _fail("SIGNED_ORDER_SCHEMA", "SCHEMA")
    if any(
        key in payload
        for key in {"cancelId", "trigger", "tpSlType", "takeProfit", "stopLoss", "builderFee", "builderId"}
    ):
        _fail("SIGNED_ORDER_OPTIONAL_FIELDS", "SAFETY")
    if "debuggingAmounts" in payload:
        debugging = payload["debuggingAmounts"]
        if not isinstance(debugging, Mapping) or set(debugging) != {
            "collateralAmount", "feeAmount", "syntheticAmount"
        }:
            _fail("SIGNED_ORDER_DEBUG_SCHEMA", "SCHEMA")
    if (
        payload["id"] != intent.external_id
        or payload["market"] != intent.market
        or payload["type"] != LIMIT
        or payload["side"] != intent.side
        or _decimal(payload["qty"], "SIGNED_ORDER_QTY") != intent.qty
        or _decimal(payload["price"], "SIGNED_ORDER_PRICE") != intent.price
        or payload["reduceOnly"] is not intent.reduce_only
        or payload["postOnly"] is not False
        or payload["timeInForce"] != IOC
        or _integer(payload["expiryEpochMillis"], "SIGNED_ORDER_EXPIRY") != intent.expiry_ms
        or _decimal(payload["fee"], "SIGNED_ORDER_FEE") != intent.fee
        or _integer(payload["nonce"], "SIGNED_ORDER_NONCE") != intent.nonce
        or payload["selfTradeProtectionLevel"] != "ACCOUNT"
    ):
        _fail("SIGNED_ORDER_BINDING", "IDENTITY")
    settlement = payload["settlement"]
    if not isinstance(settlement, Mapping) or set(settlement) != {
        "signature", "starkKey", "collateralPosition"
    }:
        _fail("SIGNED_SETTLEMENT_SCHEMA", "SCHEMA")
    signature = settlement.get("signature")
    if (
        not isinstance(signature, Mapping) or set(signature) != {"r", "s"}
        or type(signature.get("r")) is not str
        or type(signature.get("s")) is not str
        or not signature["r"].startswith("0x")
        or not signature["s"].startswith("0x")
        or not _same_hex(settlement.get("starkKey"), identity.l2_key)
        or str(settlement.get("collateralPosition")) != str(identity.l2_vault)
    ):
        _fail("SIGNED_SETTLEMENT_BINDING", "IDENTITY")


def _account_identity_matches(observation: RestObservation, intent: OrderIntent | None = None) -> None:
    account_id = observation.account.get("id", observation.account.get("accountId"))
    if intent is not None and (
        account_id != intent.account_id or not _same_hex(observation.account.get("l2Key"), intent.l2_key)
    ):
        _fail("OBSERVATION_IDENTITY_MISMATCH", "IDENTITY")
    if observation.account.get("status") != ACTIVE:
        _fail("ACCOUNT_INACTIVE")


def _order_external(row: Mapping[str, Any]) -> str:
    primary = row.get("externalId")
    alternate = row.get("externalOrderId")
    value = primary if primary is not None else alternate
    if type(value) is not str or not value:
        _fail("ORDER_EXTERNAL_ID_MISSING", "IDENTITY")
    if primary is not None and alternate is not None and primary != alternate:
        _fail("ORDER_EXTERNAL_ID_CONTRADICTION", "IDENTITY")
    return value


def _validate_order_binding(
    intent: OrderIntent,
    row: Mapping[str, Any],
    identity: ExtendedIdentity,
) -> None:
    required = {"id", "accountId", "market", "type", "side", "status", "qty", "filledQty", "cancelledQty", "reduceOnly", "postOnly", "timeInForce"}
    if not required <= set(row):
        _fail("ORDER_ROW_SCHEMA", "SCHEMA")
    row_id = _venue_order_id(row["id"], "ORDER_ID_SCHEMA")
    if (
        row_id != str(intent.venue_order_id)
        or row["accountId"] != identity.account_id
        or _order_external(row) != intent.external_id
        or row["market"] != intent.market
        or row["type"] != LIMIT
        or row["side"] != intent.side
        or row["reduceOnly"] is not intent.reduce_only
        or row["postOnly"] is not False
        or row["timeInForce"] != IOC
    ):
        _fail("ORDER_IDENTITY_MISMATCH", "IDENTITY")
    if row["status"] not in _ORDER_TERMINAL | _ORDER_OPEN:
        _fail("ORDER_STATUS_UNKNOWN", "SCHEMA")
    if "price" not in row or row["price"] is None:
        _fail("ORDER_PRICE_MISSING", "SCHEMA")
    if _decimal(row["price"], "ORDER_PRICE_SCHEMA") != intent.price:
        _fail("ORDER_PRICE_MISMATCH", "IDENTITY")
    if _decimal(row["qty"], "ORDER_QTY_SCHEMA") != intent.qty:
        _fail("ORDER_QTY_MISMATCH", "IDENTITY")
    filled = _decimal(row["filledQty"], "ORDER_FILLED_SCHEMA")
    cancelled = _decimal(row["cancelledQty"], "ORDER_CANCELLED_SCHEMA")
    if filled < 0 or cancelled < 0 or filled + cancelled > intent.qty:
        _fail("ORDER_QUANTITY_CONTRADICTION")
    expiry = row.get("expiryTime")
    alternate_expiry = row.get("expireTime")
    if expiry is None:
        expiry = alternate_expiry
    if expiry is None:
        _fail("ORDER_EXPIRY_MISSING", "IDENTITY")
    if alternate_expiry is not None and _integer(alternate_expiry, "ORDER_EXPIRY_SCHEMA") != _integer(expiry, "ORDER_EXPIRY_SCHEMA"):
        _fail("ORDER_EXPIRY_CONTRADICTION", "IDENTITY")
    if _integer(expiry, "ORDER_EXPIRY_SCHEMA") != intent.expiry_ms:
        _fail("ORDER_EXPIRY_MISMATCH", "IDENTITY")


def _matching_trades(
    intent: OrderIntent,
    observation: RestObservation,
    identity: ExtendedIdentity,
) -> tuple[Mapping[str, Any], ...]:
    result: list[Mapping[str, Any]] = []
    ids: set[Any] = set()
    for row in observation.trades:
        if not isinstance(row, Mapping):
            _fail("TRADE_ROW_SCHEMA", "SCHEMA")
        if row.get("accountId") != identity.account_id or row.get("market") != intent.market:
            _fail("UNRELATED_TRADE", "IDENTITY")
        external = _order_external(row)
        row_order_id = _venue_order_id(row.get("orderId"), "TRADE_ORDER_ID_SCHEMA")
        if external == intent.external_id or row_order_id == str(intent.venue_order_id):
            if external != intent.external_id or row_order_id != str(intent.venue_order_id):
                _fail("TRADE_IDENTITY_MISMATCH", "IDENTITY")
            trade_id = row.get("id")
            if type(trade_id) is not int or trade_id <= 0 or trade_id in ids:
                _fail("TRADE_IDENTITY_DUPLICATE", "IDENTITY")
            ids.add(trade_id)
            if row.get("side") != intent.side or row.get("tradeType", TRADE) != TRADE:
                _fail("TRADE_BINDING_MISMATCH", "IDENTITY")
            if row.get("isTaker") is not True:
                _fail("TRADE_TAKER_REQUIRED")
            price = _decimal(row.get("price"), "TRADE_PRICE_SCHEMA")
            qty = _decimal(row.get("qty"), "TRADE_QTY_SCHEMA")
            value = _decimal(row.get("value"), "TRADE_VALUE_SCHEMA")
            fee = _decimal(row.get("fee"), "TRADE_FEE_SCHEMA")
            if price <= 0 or qty <= 0 or fee < 0 or value != price * qty:
                _fail("TRADE_ARITHMETIC_CONTRADICTION")
            if (intent.side == "BUY" and price > intent.price) or (intent.side == "SELL" and price < intent.price):
                _fail("TRADE_PRICE_BOUND_BREACH")
            result.append(row)
    return tuple(result)


def _matching_order(
    intent: OrderIntent,
    observation: RestObservation,
    identity: ExtendedIdentity,
) -> Mapping[str, Any] | None:
    if intent.venue_order_id is None:
        rows = observation.exact_by_external.get(intent.external_id, ())
        if len(rows) > 1:
            _fail("EXACT_EXTERNAL_ORDER_AMBIGUOUS", "IDENTITY")
        if not rows:
            return None
        return rows[0]
    by_id = observation.exact_by_id.get(str(intent.venue_order_id))
    by_external = observation.exact_by_external.get(intent.external_id, ())
    if by_id is None and not by_external:
        return None
    if by_id is None or len(by_external) != 1:
        _fail("EXACT_ORDER_RESOLUTION_INCOMPLETE", "IDENTITY")
    if str(by_external[0].get("id")) != str(by_id.get("id")):
        _fail("EXACT_ORDER_ID_CONTRADICTION", "IDENTITY")
    _validate_order_binding(intent, by_id, identity)
    _validate_order_binding(intent, by_external[0], identity)
    return by_id


def _validate_position(
    intent: OrderIntent,
    positions: Sequence[Mapping[str, Any]],
    identity: ExtendedIdentity,
    *,
    expected_side: str,
    expected_size: Decimal,
) -> Mapping[str, Any]:
    if len(positions) != 1:
        _fail("AUTHORITATIVE_POSITION_REQUIRED", "IDENTITY")
    position = positions[0]
    required = {"id", "accountId", "market", "status", "side", "size"}
    if not required <= set(position):
        _fail("POSITION_SCHEMA", "SCHEMA")
    _venue_order_id(position["id"], "POSITION_ID_SCHEMA")
    if (
        position["accountId"] != identity.account_id
        or position["market"] != intent.market
        or position["status"] != "OPENED"
        or position["side"] != expected_side
        or _decimal(position["size"], "POSITION_SIZE_SCHEMA") != expected_size
    ):
        _fail("POSITION_BINDING_MISMATCH", "IDENTITY")
    if expected_size <= 0:
        _fail("POSITION_SIZE_INVALID")
    return position


def _assert_no_unrelated(
    observation: RestObservation,
    identity: ExtendedIdentity,
    expected_external_ids: set[str],
) -> None:
    for row in observation.positions:
        if not isinstance(row, Mapping) or row.get("accountId") != identity.account_id or row.get("market") != TARGET_MARKET:
            _fail("UNRELATED_ACCOUNT_STATE", "IDENTITY")
        _venue_order_id(row.get("id"), "POSITION_ID_SCHEMA")
    for row in (*observation.open_orders, *observation.order_history):
        if not isinstance(row, Mapping) or row.get("accountId") != identity.account_id or row.get("market") != TARGET_MARKET:
            _fail("UNRELATED_ACCOUNT_STATE", "IDENTITY")
        _venue_order_id(row.get("id"), "ORDER_ID_SCHEMA")
        if _order_external(row) not in expected_external_ids:
            _fail("UNRELATED_ACCOUNT_STATE", "IDENTITY")
    for row in observation.trades:
        if not isinstance(row, Mapping) or row.get("accountId") != identity.account_id or row.get("market") != TARGET_MARKET:
            _fail("UNRELATED_ACCOUNT_STATE", "IDENTITY")
        _venue_order_id(row.get("id"), "TRADE_ID_SCHEMA")
        _venue_order_id(row.get("orderId"), "TRADE_ORDER_ID_SCHEMA")
        if _order_external(row) not in expected_external_ids:
            _fail("UNRELATED_ACCOUNT_STATE", "IDENTITY")


class SealedLifecycleRunner:
    """Single sequential Extended place/reconcile/cancel/close lifecycle."""

    def __init__(
        self,
        *,
        store: OperationalIntentStore,
        journal: RuntimeRunJournal,
        io: VenueIO,
        capability: ExtendedCredentialCapability | Any,
        identity: ExtendedIdentity,
    ) -> None:
        self.store = store
        self.journal = journal
        self.io = io
        self.capability = capability
        self.identity = identity
        self.stage = "RUNNER_STARTUP"
        self._last_observation: RestObservation | None = None
        self.run_id = journal.begin(io.now_ms())
        self.writes = 0

    def _all_order_intents(self) -> tuple[OrderIntent, ...]:
        return tuple(intent for intent in self.store.all() if intent.kind in {"ENTRY", "CLOSE"})

    def _observe(self) -> RestObservation:
        observation = self.io.observe(self._all_order_intents())
        _validate_fresh(observation, self.io.now_ms())
        _account_identity_matches(observation)
        _assert_no_unrelated(
            observation, self.identity,
            {intent.external_id for intent in self._all_order_intents()},
        )
        return observation

    def _new_order_identity(self) -> tuple[int, str]:
        used_nonces = {intent.nonce for intent in self.store.all()}
        for _ in range(16):
            nonce = secrets.randbelow(2**31 - 1) + 1
            if nonce not in used_nonces:
                break
        else:
            _fail("NONCE_GENERATION_EXHAUSTED")
        # A client-supplied external ID is durable before SDK signing and is
        # also the settlement identity carried by the signed order.
        external = str(secrets.randbelow(10**29) + 10**29)
        if any(intent.external_id == external for intent in self.store.all()):
            _fail("EXTERNAL_ID_GENERATION_COLLISION")
        return nonce, external

    @staticmethod
    def _book_prices(observation: RestObservation) -> tuple[Decimal, Decimal]:
        try:
            bids = observation.book["bids"]
            asks = observation.book["asks"]
            bid = _decimal(bids[0]["price"], "BOOK_PRICE_SCHEMA")
            ask = _decimal(asks[0]["price"], "BOOK_PRICE_SCHEMA")
        except (KeyError, IndexError, TypeError):
            _fail("BOOK_SCHEMA", "SCHEMA")
        if bid <= 0 or ask <= bid:
            _fail("BOOK_CROSSED")
        return bid, ask

    @staticmethod
    def _top_level_qty(levels: Any) -> Decimal:
        try:
            level = levels[0]
            value = level["qty"]
        except (KeyError, IndexError, TypeError):
            _fail("BOOK_SCHEMA", "SCHEMA")
        return _decimal(value, "BOOK_QTY_SCHEMA")

    def _prepare_entry(self, observation: RestObservation) -> OrderIntent:
        _validate_fresh(observation, self.io.now_ms())
        config = observation.market["tradingConfig"]
        qty = _decimal(config["minOrderSize"], "MINIMUM_SIZE_SCHEMA")
        fee_rows = tuple(row for row in observation.fees if row.get("market") == TARGET_MARKET)
        if len(fee_rows) != 1:
            _fail("TAKER_FEE_UNIQUE")
        fee = _decimal(fee_rows[0].get("takerFeeRate"), "TAKER_FEE_SCHEMA")
        bid, ask = self._book_prices(observation)
        price = ask
        if (
            self._top_level_qty(observation.book["asks"]) < qty
            or self._top_level_qty(observation.book["bids"]) < qty
        ):
            _fail("TOP_LEVEL_DEPTH_INSUFFICIENT")
        _validate_limit_price(observation, side="BUY", price=price)
        if qty * price > MAX_NOTIONAL_USD:
            _fail("NOTIONAL_CAP")
        if observation.balance.get("collateralName") != observation.market.get("collateralAssetName"):
            _fail("COLLATERAL_MISMATCH")
        available = _decimal(observation.balance.get("availableForTrade"), "BALANCE_SCHEMA")
        if available < qty * price * (Decimal(1) + fee) + qty * bid * (Decimal(1) + fee):
            _fail("BALANCE_INSUFFICIENT")
        leverage_rows = tuple(row for row in observation.leverage if row.get("market") == TARGET_MARKET)
        if len(leverage_rows) != 1 or _decimal(leverage_rows[0].get("leverage"), "LEVERAGE_SCHEMA") <= 0:
            _fail("LEVERAGE_INVALID")
        nonce, external = self._new_order_identity()
        expiry = self.io.now_ms() + SHORT_EXPIRY_MS
        intent = self.store.prepare(
            kind="ENTRY", nonce=nonce, external_id=external, expiry_ms=expiry,
            account_id=self.identity.account_id, l2_key=self.identity.l2_key,
            market=TARGET_MARKET, side="BUY", qty=qty, price=price, fee=fee,
            reduce_only=False, expected_lifecycle="FLAT", next_lifecycle="ENTRY_PREPARED",
            settlement_identity=external,
        )
        return intent

    def _dispatch_signed_order(self, intent: OrderIntent, *, close: bool = False) -> OrderIntent:
        expected = "CLOSE_PREPARED" if close else "ENTRY_PREPARED"
        next_state = "CLOSE_AMBIGUOUS" if close else "ENTRY_AMBIGUOUS"
        if self._last_observation is None:
            _fail("SIGNING_MARKET_MISSING")
        _validate_fresh(self._last_observation, self.io.now_ms())
        claimed = self.store.claim(intent.id, expected_lifecycle=expected, next_lifecycle=next_state)
        self.stage = "CLOSE_SIGNATURE" if close else "ENTRY_SIGNATURE"
        signed = self.capability.sign_order(claimed, self._last_observation.market)
        if not isinstance(signed, SignedOrder) or not isinstance(signed.payload, Mapping):
            _fail("SIGNER_RESULT_SCHEMA", "AUTH")
        _validate_signed_payload(claimed, signed.payload, self.identity)
        durable = self.store.bind_signed(
            claimed.id, payload=signed.payload,
            payload_digest=canonical_digest(signed.payload),
            settlement_identity=signed.settlement_hash or claimed.settlement_identity,
        )
        self.stage = "CLOSE_DISPATCH" if close else "ENTRY_DISPATCH"
        dispatch_now_ms = self.io.now_ms()
        if dispatch_now_ms >= durable.expiry_ms:
            _fail("WRITE_INTENT_EXPIRED")
        _validate_fresh(self._last_observation, dispatch_now_ms)
        self.writes += 1
        try:
            receipt = self.io.place_order(durable, signed.payload)
        except AmbiguousWrite as error:
            self.store.mark_ambiguous(durable.id)
            try:
                outcome, _, _ = self._reconcile(durable, close=close)
            except OperationalSafetyError:
                raise AmbiguousWrite(error.code) from None
            if outcome not in {"FILLED", "NO_FILL", "OPEN"}:
                raise AmbiguousWrite(error.code) from None
            return self.store.get(durable.id)
        except VenueRejection:
            self.store.mark_rejected(durable.id)
            return self.store.get(durable.id)
        except OperationalSafetyError as error:
            self.store.mark_ambiguous(durable.id)
            try:
                outcome, _, _ = self._reconcile(durable, close=close)
            except OperationalSafetyError:
                raise AmbiguousWrite(error.code) from None
            if outcome not in {"FILLED", "NO_FILL", "OPEN"}:
                raise AmbiguousWrite(error.code) from None
            return self.store.get(durable.id)
        if not isinstance(receipt, WriteReceipt) or receipt.accepted is not True:
            self.store.mark_ambiguous(durable.id)
            raise AmbiguousWrite("WRITE_RECEIPT_INVALID")
        if receipt.order_id is None or receipt.external_id != durable.external_id:
            self.store.mark_ambiguous(durable.id)
            _fail("WRITE_RESPONSE_IDENTITY_MISMATCH", "IDENTITY")
        return self.store.mark_accepted(durable.id, str(receipt.order_id), receipt.external_id)

    def _resolve_and_classify(
        self,
        intent: OrderIntent,
        observation: RestObservation,
        *,
        close: bool,
    ) -> tuple[str, RestObservation, Mapping[str, Any] | None]:
        current = self.store.get(intent.id)
        if current.venue_order_id is None:
            rows = observation.exact_by_external.get(current.external_id, ())
            if len(rows) == 1:
                order_id = rows[0].get("id")
                current = self.store.mark_accepted(
                    current.id, _venue_order_id(order_id, "EXACT_ORDER_ID_SCHEMA"), current.external_id
                )
                # The external lookup discovers the venue ID after an
                # ambiguous write; fetch the exact-ID endpoint again before
                # accepting any outcome.
                observation = self._observe()
            elif len(rows) > 1:
                _fail("EXACT_EXTERNAL_ORDER_AMBIGUOUS", "IDENTITY")
            else:
                return "UNSEEN", observation, None
        row = _matching_order(current, observation, self.identity)
        if row is None:
            return "UNSEEN", observation, None
        status = row["status"]
        trades = _matching_trades(current, observation, self.identity)
        filled_qty = sum((_decimal(item["qty"], "TRADE_QTY_SCHEMA") for item in trades), Decimal(0))
        declared = _decimal(row["filledQty"], "ORDER_FILLED_SCHEMA")
        if filled_qty != declared:
            _fail("ORDER_TRADE_QUANTITY_CONTRADICTION")
        if status == FILLED:
            if filled_qty != current.qty:
                _fail("FILLED_ORDER_QUANTITY_MISMATCH")
            if not trades:
                _fail("FILLED_TRADE_MISSING")
            if close:
                if observation.positions:
                    _fail("CLOSE_POSITION_NOT_FLAT")
                self.store.mark_reconciled(current.id, close=True)
                return "FILLED", observation, row
            expected_side = "LONG" if current.side == "BUY" else "SHORT"
            _validate_position(
                current, observation.positions, self.identity,
                expected_side=expected_side, expected_size=current.qty,
            )
            self.store.mark_reconciled(current.id)
            return "FILLED", observation, row
        if status in {CANCELLED, EXPIRED, REJECTED}:
            if filled_qty != 0 or observation.positions:
                _fail("TERMINAL_ORDER_POSITION_CONTRADICTION")
            if close:
                self.store.mark(current.id, "CLOSE_RECONCILED_NO_FILL", lifecycle="EXPOSED")
            else:
                self.store.mark_no_fill(current.id)
            return "NO_FILL", observation, row
        if status == PARTIALLY_FILLED:
            _fail("PARTIAL_FILL_REQUIRES_MANUAL_RECOVERY")
        return "OPEN", observation, row

    def _reconcile(self, intent: OrderIntent, *, close: bool = False) -> tuple[str, RestObservation, Mapping[str, Any] | None]:
        self.stage = "CLOSE_RECONCILIATION" if close else "ENTRY_RECONCILIATION"
        last: RestObservation | None = None
        last_outcome = "UNSEEN"
        for attempt in range(MAX_RECONCILE_READS):
            last = self._observe()
            outcome, observed, row = self._resolve_and_classify(intent, last, close=close)
            last_outcome = outcome
            if outcome in {"FILLED", "NO_FILL"}:
                return outcome, observed, row
            if attempt + 1 < MAX_RECONCILE_READS:
                time.sleep(RECONCILE_SLEEP_SECONDS)
        assert last is not None
        if last_outcome != "OPEN":
            _fail("ORDER_OUTCOME_NOT_VISIBLE", "TRANSPORT")
        return "OPEN", last, None

    def _prepare_cancel(self, entry: OrderIntent, observation: RestObservation) -> OrderIntent:
        current = self.store.get(entry.id)
        if current.venue_order_id is None:
            _fail("CANCEL_ORDER_ID_REQUIRED", "IDENTITY")
        nonce, external = self._new_order_identity()
        intent = self.store.prepare(
            kind="CANCEL", nonce=nonce, external_id=f"cancel-{external}",
            expiry_ms=self.io.now_ms() + SHORT_EXPIRY_MS,
            account_id=self.identity.account_id, l2_key=self.identity.l2_key,
            market=TARGET_MARKET, side="NONE", qty=Decimal(0), price=Decimal(0), fee=Decimal(0),
            reduce_only=False, target_id=current.id, target_external_id=current.external_id,
            expected_lifecycle="ENTRY_AMBIGUOUS", next_lifecycle="CANCEL_PREPARED",
            settlement_identity=f"cancel-{external}",
        )
        return intent

    def _dispatch_cancel(self, intent: OrderIntent, entry: OrderIntent) -> OrderIntent:
        self.store.claim(intent.id, expected_lifecycle="CANCEL_PREPARED", next_lifecycle="CANCEL_AMBIGUOUS")
        self.stage = "CANCEL_DISPATCH"
        target = self.store.get(entry.id)
        if target.venue_order_id is None:
            _fail("CANCEL_ORDER_ID_REQUIRED", "IDENTITY")
        if self.io.now_ms() >= intent.expiry_ms:
            _fail("CANCEL_INTENT_EXPIRED")
        self.writes += 1
        try:
            receipt = self.io.cancel_order(intent, target.venue_order_id)
        except AmbiguousWrite as error:
            self.store.mark_ambiguous(intent.id)
            try:
                outcome, _, _ = self._reconcile(target)
            except OperationalSafetyError:
                raise AmbiguousWrite(error.code) from None
            if outcome != "NO_FILL":
                raise AmbiguousWrite(error.code) from None
            self.store.mark(intent.id, "RECONCILED", lifecycle="FLAT_PENDING_EXPIRY")
            return self.store.get(intent.id)
        except VenueRejection:
            self.store.mark_rejected(intent.id)
            raise
        except OperationalSafetyError as error:
            self.store.mark_ambiguous(intent.id)
            try:
                outcome, _, _ = self._reconcile(target)
            except OperationalSafetyError:
                raise AmbiguousWrite(error.code) from None
            if outcome != "NO_FILL":
                raise AmbiguousWrite(error.code) from None
            self.store.mark(intent.id, "RECONCILED", lifecycle="FLAT_PENDING_EXPIRY")
            return self.store.get(intent.id)
        if not isinstance(receipt, WriteReceipt) or receipt.accepted is not True:
            self.store.mark_ambiguous(intent.id)
            raise AmbiguousWrite("CANCEL_RECEIPT_INVALID")
        if receipt.order_id is not None and str(receipt.order_id) != target.venue_order_id:
            self.store.mark_ambiguous(intent.id)
            _fail("CANCEL_RESPONSE_ORDER_ID_MISMATCH", "IDENTITY")
        if receipt.external_id is not None and receipt.external_id != target.external_id:
            self.store.mark_ambiguous(intent.id)
            _fail("CANCEL_RESPONSE_EXTERNAL_MISMATCH", "IDENTITY")
        return self.store.mark(intent.id, "ACCEPTED", lifecycle="CANCEL_AMBIGUOUS")

    def _prepare_close(self, entry: OrderIntent, observation: RestObservation) -> OrderIntent:
        current = self.store.get(entry.id)
        if current.state != "ENTRY_RECONCILED":
            _fail("ENTRY_NOT_RECONCILED")
        _validate_fresh(observation, self.io.now_ms())
        if observation.open_orders:
            _fail("OPEN_ORDER_PRESENT")
        position = _validate_position(
            current, observation.positions, self.identity,
            expected_side="LONG" if current.side == "BUY" else "SHORT",
            expected_size=current.qty,
        )
        size = _decimal(position["size"], "POSITION_SIZE_SCHEMA")
        bid, ask = self._book_prices(observation)
        price = bid if position["side"] == "LONG" else ask
        close_levels = observation.book["bids"] if position["side"] == "LONG" else observation.book["asks"]
        if self._top_level_qty(close_levels) < size:
            _fail("TOP_LEVEL_DEPTH_INSUFFICIENT")
        _validate_limit_price(
            observation,
            side="SELL" if position["side"] == "LONG" else "BUY",
            price=price,
        )
        if size * price > MAX_NOTIONAL_USD:
            _fail("NOTIONAL_CAP")
        fee_rows = tuple(row for row in observation.fees if row.get("market") == TARGET_MARKET)
        if len(fee_rows) != 1:
            _fail("TAKER_FEE_UNIQUE")
        fee = _decimal(fee_rows[0].get("takerFeeRate"), "TAKER_FEE_SCHEMA")
        nonce, external = self._new_order_identity()
        return self.store.prepare(
            kind="CLOSE", nonce=nonce, external_id=external,
            expiry_ms=self.io.now_ms() + SHORT_EXPIRY_MS,
            account_id=self.identity.account_id, l2_key=self.identity.l2_key,
            market=TARGET_MARKET, side="SELL" if position["side"] == "LONG" else "BUY",
            qty=size, price=price, fee=fee, reduce_only=True,
            target_id=current.id, target_external_id=current.external_id,
            expected_lifecycle="EXPOSED", next_lifecycle="CLOSE_PREPARED",
            settlement_identity=external,
        )

    def _final_barrier(self, *, no_fill: bool = False) -> None:
        self.stage = "FINAL_BARRIER"
        intents = self._all_order_intents()
        if not intents:
            _fail("FINAL_INTENTS_MISSING")
        maximum_expiry = max(intent.expiry_ms for intent in intents)
        for _ in range(256):
            if self.io.now_ms() > maximum_expiry:
                break
            time.sleep(RECONCILE_SLEEP_SECONDS)
        else:
            _fail("FINAL_EXPIRY_WAIT_UNBOUNDED", "TRANSPORT")
        first = self._observe()
        second = self._observe()
        if second.observed_at_ms <= first.observed_at_ms:
            _fail("FINAL_ROUNDS_NOT_ORDERED")
        if first.semantic_fingerprint != second.semantic_fingerprint:
            _fail("FINAL_ROUNDS_DISAGREE")
        if any(
            intent.state in {"PREPARED", "CLAIMED", "AMBIGUOUS"}
            for intent in self.store.all()
        ):
            _fail("FINAL_OUTSTANDING_INTENT")
        expected_external = {intent.external_id for intent in intents}

        def validate_round(observation: RestObservation) -> set[int]:
            if observation.open_orders or observation.positions:
                _fail("FINAL_ZERO_FLATNESS_FAILED")
            history_by_external: dict[str, Mapping[str, Any]] = {}
            for row in observation.order_history:
                external = _order_external(row)
                if external in history_by_external:
                    _fail("FINAL_HISTORY_DUPLICATE", "IDENTITY")
                history_by_external[external] = row
            expected_trade_ids: set[int] = set()
            for intent in intents:
                row = history_by_external.get(intent.external_id)
                if row is None:
                    _fail("FINAL_HISTORY_MISSING")
                if intent.state == "ENTRY_RECONCILED_NO_FILL":
                    if row["status"] not in {CANCELLED, EXPIRED, REJECTED} or _decimal(row["filledQty"], "ORDER_FILLED_SCHEMA") != 0:
                        _fail("FINAL_NO_FILL_HISTORY_MISMATCH")
                elif row["status"] != FILLED:
                    _fail("FINAL_FILL_HISTORY_MISMATCH")
                current = self.store.get(intent.id)
                exact = _matching_order(current, observation, self.identity)
                if exact is None:
                    _fail("FINAL_EXACT_ORDER_MISSING")
                for trade in _matching_trades(current, observation, self.identity):
                    expected_trade_ids.add(trade["id"])
            actual_trade_ids = {row.get("id") for row in observation.trades}
            if actual_trade_ids != expected_trade_ids:
                _fail("FINAL_TRADE_IDENTITY_MISMATCH", "IDENTITY")
            if not expected_external <= set(history_by_external):
                _fail("FINAL_HISTORY_MISSING")
            return expected_trade_ids

        first_trade_ids = validate_round(first)
        second_trade_ids = validate_round(second)
        if first_trade_ids != second_trade_ids:
            _fail("FINAL_TRADE_ROUNDS_DISAGREE", "IDENTITY")
        self.store.terminal("COMPLETE")

    def _report(self, status: str, reason: str | None = None) -> "OperationalReport":
        orders = self._all_order_intents()
        close = next((intent for intent in orders if intent.kind == "CLOSE"), None)
        report = OperationalReport(
            status=status, run_id=self.run_id, writes=self.writes,
            entry_order_id=next((intent.venue_order_id for intent in orders if intent.kind == "ENTRY"), None),
            close_order_id=None if close is None else close.venue_order_id,
            final_zero_orders=True, final_exact_flat=True, stream_frames=0,
            reason=reason,
        )
        self.journal.terminalize(
            self.run_id, state="COMPLETE", stage="FINAL_BARRIER", report=report.sanitized()
        )
        return report

    def run(self) -> "OperationalReport":
        if self.store.lifecycle_state() != "FLAT" or self.store.all():
            _fail("EXISTING_LIFECYCLE_REQUIRES_RECOVERY", "RESTART")
        self.stage = "INITIAL_REST"
        initial = self._observe()
        self._last_observation = initial
        self.stage = "ENTRY_PREPARATION"
        entry = self._prepare_entry(initial)
        self._last_observation = initial
        entry = self._dispatch_signed_order(entry)
        outcome, observation, _ = self._reconcile(entry)
        if outcome == "OPEN":
            self.stage = "CANCEL_PREPARATION"
            cancel = self._prepare_cancel(entry, observation)
            self._dispatch_cancel(cancel, entry)
            self.stage = "CANCEL_RECONCILIATION"
            for _ in range(MAX_RECONCILE_READS):
                observation = self._observe()
                cancel_outcome, observation, _ = self._resolve_and_classify(entry, observation, close=False)
                if cancel_outcome == "NO_FILL":
                    outcome = cancel_outcome
                    break
                time.sleep(RECONCILE_SLEEP_SECONDS)
            else:
                _fail("CANCEL_OUTCOME_UNRESOLVED")
            self.store.mark(cancel.id, "RECONCILED", lifecycle="FLAT_PENDING_EXPIRY")
        if outcome == "FILLED":
            self._last_observation = observation
            self.stage = "CLOSE_PREPARATION"
            close = self._prepare_close(entry, observation)
            self._last_observation = observation
            close = self._dispatch_signed_order(close, close=True)
            close_outcome, observation, _ = self._reconcile(close, close=True)
            if close_outcome != "FILLED":
                _fail("CLOSE_OUTCOME_UNRESOLVED")
            # A filled close must be followed by an exact flat position.
            if observation.positions:
                _fail("CLOSE_POSITION_NOT_FLAT")
        self._final_barrier(no_fill=outcome == "NO_FILL")
        return self._report("COMPLETED_NO_FILL_FLAT" if outcome == "NO_FILL" else "SUCCESS_CLOSED_FLAT")


@dataclass(frozen=True)
class OperationalReport:
    status: str
    run_id: str
    writes: int
    entry_order_id: str | None
    close_order_id: str | None
    final_zero_orders: bool
    final_exact_flat: bool
    stream_frames: int
    reason: str | None = None

    def sanitized(self) -> dict[str, Any]:
        return {
            "status": self.status, "run_id": self.run_id, "writes": self.writes,
            "entry_order_id": self.entry_order_id, "close_order_id": self.close_order_id,
            "final_zero_orders": self.final_zero_orders,
            "final_exact_flat": self.final_exact_flat, "stream_frames": self.stream_frames,
            "reason": self.reason, "path": REDACTED_STORE_PATH,
        }


def _persist_failure(runner: SealedLifecycleRunner, error: BaseException) -> None:
    failure_class = getattr(error, "failure_class", "SAFETY")
    if failure_class not in _FAILURE_CLASSES:
        failure_class = "SAFETY"
    try:
        runner.store.terminal("HALTED_" + str(getattr(error, "code", "FAILURE")))
    except BaseException:
        pass
    try:
        runner.journal.terminalize(
            runner.run_id, state="BLOCKED", failure_class=failure_class,
            stage=runner.stage if runner.stage in _RUNTIME_STAGES else "OUTER",
        )
    except BaseException:
        pass


def _fixture_run(
    *,
    path: Path,
    io: VenueIO,
    capability: Any,
    identity: ExtendedIdentity,
) -> OperationalReport:
    _prepare_file(path)
    store = OperationalIntentStore(path)
    journal = RuntimeRunJournal(path)
    runner: SealedLifecycleRunner | None = None
    try:
        runner = SealedLifecycleRunner(
            store=store, journal=journal, io=io, capability=capability, identity=identity,
        )
        return runner.run()
    except BaseException as error:
        if runner is not None:
            _persist_failure(runner, error)
        raise
    finally:
        close = getattr(capability, "close", None)
        if callable(close):
            close()


def _production_run() -> OperationalReport:
    home = _passwd_home()
    path = home / RUN_STORE_BASENAME
    # Inspect the protected runtime journal before opening either credential.
    # A terminal or interrupted prior run is never replayed and does not need
    # an authenticated transport or SDK handle to be diagnosed.
    _prepare_file(path)
    existing = RuntimeRunJournal(path).snapshot()
    if existing is not None:
        code = "RUNTIME_RESTART_REQUIRED" if existing["state"] == "STARTED" else "RUNTIME_ALREADY_TERMINAL"
        _fail(code, "RESTART")
    capability = _PasswdHomeCredentialSource().open()
    try:
        io = OperationalVenueIO(capability)
        return _fixture_run(
            path=path, io=io, capability=capability, identity=capability.identity,
        )
    finally:
        # _fixture_run closes the handle on its normal and exceptional paths.
        if not getattr(capability, "_closed", True):
            capability.close()


def run() -> dict[str, Any]:
    """Run exactly one sealed Extended-testnet lifecycle with no arguments."""
    return _production_run().sanitized()


def main() -> int:
    if len(sys.argv) != 1:
        print(json.dumps({"status": "BLOCKED", "reason": "ARGUMENTS_FORBIDDEN"}, sort_keys=True))
        return 2
    try:
        result = run()
    except OperationalSafetyError as error:
        print(json.dumps({"status": "BLOCKED", "reason": error.code}, sort_keys=True))
        return 1
    except BaseException:
        print(json.dumps({"status": "BLOCKED", "reason": "UNEXPECTED_FAILURE"}, sort_keys=True))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] in {"SUCCESS_CLOSED_FLAT", "COMPLETED_NO_FILL_FLAT"} else 1


__all__ = [
    "ExtendedCredentialCapability", "ExtendedIdentity", "ExtendedRestTransport",
    "OperationalIntentStore", "OperationalReport", "OperationalSafetyError",
    "OperationalVenueIO", "OrderIntent", "RestObservation", "RuntimeRunJournal",
    "SealedLifecycleRunner", "SignedOrder", "VenueRejection", "WriteReceipt",
    "_PasswdHomeCredentialSource", "_fixture_run", "main", "run",
]


if __name__ == "__main__":
    raise SystemExit(main())
