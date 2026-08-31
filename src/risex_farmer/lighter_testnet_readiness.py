"""Protected, read-only Lighter testnet account-readiness contract.

This module is deliberately separate from the normal paper adapter.  It does
not import the Lighter SDK, read a wallet secret, create an account, provision
an API key, build a transaction, or dispatch anything.  A later operational
gate can inject a narrowly scoped GET transport and a protected authorization
loader.

The fixed testnet wallet is inspected with ``lstat`` metadata only.  Its
contents are never opened or derived from here.
"""

from __future__ import annotations

import argparse
import json
import os
import stat
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence
from urllib.parse import urlencode


TESTNET_API_URL = "https://testnet.zklighter.elliot.ai"
TESTNET_WS_URL = "wss://testnet.zklighter.elliot.ai/stream"
TESTNET_CHAIN_ID = 300
TESTNET_WALLET_PATH = Path(
    "/Users/daniilmakarov/.config/risex-farmer/lighter-testnet/wallet-private-key"
)
EXPECTED_L1_ADDRESS = "0x84da822bbd7518b252dc74F0168afaACb562Ed9A"
USDC_SYMBOL = "USDC"
FIRST_USER_API_KEY_INDEX = 4
RESERVED_API_KEY_INDICES = frozenset({0, 1, 2, 3})
MAX_JSON_BYTES = 1_048_576
MAX_HISTORY_PAGES = 256
PAGE_LIMIT = 100

ACCOUNT_NOT_FOUND_CODE = 21100
API_KEY_NOT_FOUND_CODE = 21109

ACCOUNT_CREATION_BLOCKER = (
    "ACCOUNT_CREATION_REQUIRES_FUNDED_TESTNET_DEPOSIT_OR_VERIFIED_OFFICIAL_TESTNET_ONBOARDING_PATH"
)
NO_TESTNET_WRITE_AUTHORITY = "NO_TESTNET_WRITE_AUTHORITY"

OFFICIAL_SOURCES = {
    "sdk_endpoint_profile": (
        "https://raw.githubusercontent.com/elliottech/lighter-python/main/"
        "lighter/endpoint_profiles.py"
    ),
    "sdk_setup_example": (
        "https://raw.githubusercontent.com/elliottech/lighter-python/main/"
        "examples/system_setup.py"
    ),
    "openapi": "https://raw.githubusercontent.com/elliottech/lighter-python/main/openapi.json",
    "api_keys": "https://apidocs.lighter.xyz/docs/api-keys",
    "account_creation": (
        "https://apidocs.lighter.xyz/docs/create-accounts-programmatically"
    ),
    "websocket_reference": "https://apidocs.lighter.xyz/docs/websocket-reference",
    "fees": "https://docs.lighter.xyz/trading/trading-fees",
    "contracts": "https://docs.lighter.xyz/trading/contract-specifications",
}


class TransportInterruption(Exception):
    """A transport-level interruption eligible for the one permitted retry."""


class PrematureResponse(TransportInterruption):
    """The injected transport observed an incomplete response."""


@dataclass(frozen=True)
class WalletMetadata:
    """Non-secret metadata for the one fixed testnet wallet."""

    path: str
    address: str | None
    present: bool
    protected: bool
    reason: str

    @property
    def ready(self) -> bool:
        return self.present and self.protected and self.address is not None

    def evidence(self) -> dict[str, Any]:
        return {
            "address": self.address,
            "path": self.path,
            "present": self.present,
            "protected": self.protected,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class AccountCreationPlan:
    """The exact external prerequisite, without an invocation surface."""

    status: str
    wallet_address: str | None
    wallet_path: str
    prerequisite: str
    reason: str
    testnet_only: bool = True
    invocation_allowed: bool = False
    write_capable: bool = False

    def evidence(self) -> dict[str, Any]:
        return {
            "invocation_allowed": self.invocation_allowed,
            "prerequisite": self.prerequisite,
            "reason": self.reason,
            "status": self.status,
            "testnet_only": self.testnet_only,
            "wallet_address": self.wallet_address,
            "wallet_path": self.wallet_path,
            "write_capable": self.write_capable,
        }


@dataclass(frozen=True)
class ApiKeyProvisioningPlan:
    """A non-executing interface for the narrowest SDK key setup."""

    status: str
    account_index: int | None
    api_key_index: int
    reserved_indices: tuple[int, ...]
    key_generation: str
    association: str
    read_only_auth: str
    reason: str
    requires_wallet_authorization: bool = True
    requires_testnet_collateral: bool = True
    invocation_allowed: bool = False
    write_capable: bool = False

    def evidence(self) -> dict[str, Any]:
        return {
            "account_index": self.account_index,
            "api_key_index": self.api_key_index,
            "association": self.association,
            "invocation_allowed": self.invocation_allowed,
            "key_generation": self.key_generation,
            "read_only_auth": self.read_only_auth,
            "reason": self.reason,
            "reserved_indices": list(self.reserved_indices),
            "requires_testnet_collateral": self.requires_testnet_collateral,
            "requires_wallet_authorization": self.requires_wallet_authorization,
            "status": self.status,
            "write_capable": self.write_capable,
        }


@dataclass(frozen=True)
class AuthorizationCapability:
    """A protected header value paired with its non-secret account binding."""

    header_value: str = field(repr=False)
    account_index: int
    api_key_index: int

    def __post_init__(self) -> None:
        if not isinstance(self.header_value, str) or not self.header_value:
            raise ValueError("authorization header is unavailable")
        if (
            isinstance(self.account_index, bool)
            or not isinstance(self.account_index, int)
            or self.account_index < 0
        ):
            raise ValueError("authorization account index is invalid")
        if (
            isinstance(self.api_key_index, bool)
            or not isinstance(self.api_key_index, int)
            or self.api_key_index < 0
        ):
            raise ValueError("authorization API-key index is invalid")


def _address_is_valid(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 42
        and value[:2] == "0x"
        and all(character in "0123456789abcdefABCDEF" for character in value[2:])
    )


def _same_address(left: Any, right: str) -> bool:
    return _address_is_valid(left) and left.lower() == right.lower()


def canonical_testnet_address(value: Any) -> str:
    """Validate the operator-supplied public address without reading a key."""

    if not _same_address(value, EXPECTED_L1_ADDRESS):
        raise ValueError("Lighter testnet public address does not match the fixed identity")
    return EXPECTED_L1_ADDRESS


def _metadata_failure(path: Path, reason: str, *, present: bool = False) -> WalletMetadata:
    return WalletMetadata(
        path=str(path),
        address=None,
        present=present,
        protected=False,
        reason=reason,
    )


def inspect_protected_wallet(path: str | Path | None = None) -> WalletMetadata:
    """Inspect fixed-path permissions and link metadata, never file contents."""

    target = Path(TESTNET_WALLET_PATH if path is None else path)
    fixed = Path(TESTNET_WALLET_PATH)
    if target != fixed:
        return _metadata_failure(target, "FIXED_WALLET_PATH_REQUIRED")

    try:
        directory_stat = os.lstat(target.parent)
    except FileNotFoundError:
        return _metadata_failure(target, "WALLET_DIRECTORY_MISSING")
    except OSError:
        return _metadata_failure(target, "WALLET_DIRECTORY_METADATA_UNAVAILABLE")

    if not stat.S_ISDIR(directory_stat.st_mode):
        return _metadata_failure(target, "WALLET_DIRECTORY_NOT_REGULAR")
    if directory_stat.st_uid != os.getuid():
        return _metadata_failure(target, "WALLET_DIRECTORY_OWNER_MISMATCH")
    if stat.S_IMODE(directory_stat.st_mode) != 0o700:
        return _metadata_failure(target, "WALLET_DIRECTORY_NOT_PRIVATE")

    try:
        file_stat = os.lstat(target)
    except FileNotFoundError:
        return _metadata_failure(target, "WALLET_FILE_MISSING")
    except OSError:
        return _metadata_failure(target, "WALLET_FILE_METADATA_UNAVAILABLE")

    if not stat.S_ISREG(file_stat.st_mode):
        return _metadata_failure(target, "WALLET_FILE_NOT_REGULAR", present=True)
    if file_stat.st_uid != os.getuid():
        return _metadata_failure(target, "WALLET_FILE_OWNER_MISMATCH", present=True)
    if stat.S_IMODE(file_stat.st_mode) != 0o600:
        return _metadata_failure(target, "WALLET_FILE_NOT_PRIVATE", present=True)
    if file_stat.st_nlink != 1:
        return _metadata_failure(target, "WALLET_FILE_LINK_COUNT_INVALID", present=True)
    if file_stat.st_size < 1 or file_stat.st_size > 8192:
        return _metadata_failure(target, "WALLET_FILE_SIZE_INVALID", present=True)

    return WalletMetadata(
        path=str(target),
        address=EXPECTED_L1_ADDRESS,
        present=True,
        protected=True,
        reason="PROTECTED_FIXED_WALLET_METADATA_VALID",
    )


def build_account_creation_plan(
    wallet: WalletMetadata | None = None,
) -> AccountCreationPlan:
    wallet = wallet or inspect_protected_wallet()
    if not wallet.ready:
        reason = wallet.reason
    else:
        reason = ACCOUNT_CREATION_BLOCKER
    return AccountCreationPlan(
        status="BLOCKED",
        wallet_address=wallet.address,
        wallet_path=wallet.path,
        prerequisite=ACCOUNT_CREATION_BLOCKER,
        reason=reason,
    )


def build_api_key_provisioning_plan(
    account_index: int | None = None,
    *,
    api_key_index: int = FIRST_USER_API_KEY_INDEX,
) -> ApiKeyProvisioningPlan:
    if isinstance(api_key_index, bool) or not isinstance(api_key_index, int):
        raise ValueError("api_key_index must be an integer")
    if api_key_index in RESERVED_API_KEY_INDICES or not 0 <= api_key_index <= 254:
        raise ValueError("api_key_index is reserved or outside the official range")
    if account_index is not None and (
        isinstance(account_index, bool)
        or not isinstance(account_index, int)
        or account_index < 0
    ):
        raise ValueError("account_index must be a non-negative integer")

    reason = (
        "ACCOUNT_INDEX_REQUIRED"
        if account_index is None
        else "API_KEY_ASSOCIATION_REQUIRES_WALLET_AUTHORIZED_CHANGE_PUB_KEY"
    )
    return ApiKeyProvisioningPlan(
        status="BLOCKED",
        account_index=account_index,
        api_key_index=api_key_index,
        reserved_indices=tuple(sorted(RESERVED_API_KEY_INDICES)),
        key_generation="lighter.create_api_key (local generation only)",
        association="wallet-authorized ChangePubKey transaction type 8",
        read_only_auth="create_auth_token_with_expiry after key association",
        reason=reason,
    )


@dataclass(frozen=True)
class ReadRequest:
    """A fixed-host GET request; the authorization value is never represented."""

    path: str
    params: tuple[tuple[str, str], ...] = ()
    authorization: str | None = field(default=None, repr=False)
    method: str = field(default="GET", init=False)

    def __post_init__(self) -> None:
        if not self.path.startswith("/api/v1/"):
            raise ValueError("Lighter readiness path is outside the fixed API surface")
        if any(not isinstance(key, str) or not isinstance(value, str) for key, value in self.params):
            raise ValueError("Lighter readiness query parameters must be strings")

    @property
    def url(self) -> str:
        encoded = urlencode(self.params)
        return f"{TESTNET_API_URL}{self.path}" + (f"?{encoded}" if encoded else "")

    @property
    def headers(self) -> Mapping[str, str]:
        return {} if self.authorization is None else {"Authorization": self.authorization}


@dataclass(frozen=True)
class ReadResponse:
    """A response supplied by a later transport or a deterministic fixture."""

    status_code: int
    body: Any
    final_url: str | None = None


class ReadTransport(Protocol):
    def get(self, request: ReadRequest) -> ReadResponse:
        """Perform exactly one GET against the request's fixed testnet URL."""


@dataclass(frozen=True)
class MarketContract:
    market_id: int
    symbol: str
    market_type: str
    min_base_amount: Decimal
    min_quote_amount: Decimal
    size_decimals: int
    price_decimals: int
    quote_decimals: int
    status: str
    force_reduce_only: bool


@dataclass(frozen=True)
class LiquiditySnapshot:
    market_id: int
    bid_levels: int
    ask_levels: int
    best_bid: Decimal
    best_ask: Decimal
    two_sided: bool


def _schema() -> None:
    raise _GateFailure("SCHEMA", "RESPONSE_SCHEMA_INVALID")


def _require_mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        _schema()
    return value


def _require(value: Mapping[str, Any], key: str) -> Any:
    if key not in value:
        _schema()
    return value[key]


def _optional(value: Mapping[str, Any], key: str, default: Any = None) -> Any:
    return value[key] if key in value else default


def _list(value: Any) -> list[Any]:
    if not isinstance(value, list):
        _schema()
    return value


def _integer(value: Any, *, nonnegative: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        _schema()
    if nonnegative and value < 0:
        _schema()
    return value


def _string(value: Any, *, nonempty: bool = False) -> str:
    if not isinstance(value, str) or (nonempty and not value):
        _schema()
    return value


def _boolean(value: Any) -> bool:
    if not isinstance(value, bool):
        _schema()
    return value


def _decimal(value: Any, *, nonnegative: bool = False) -> Decimal:
    if not isinstance(value, str):
        _schema()
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError):
        _schema()
    if not parsed.is_finite() or (nonnegative and parsed < 0):
        _schema()
    return parsed


def _zero(value: Decimal) -> bool:
    return value == Decimal(0)


def _cursor(payload: Mapping[str, Any]) -> str:
    value = _optional(payload, "next_cursor", "")
    if value is None:
        return ""
    return _string(value)


@dataclass(frozen=True)
class _GateFailure(Exception):
    failure_class: str
    reason: str


@dataclass
class _Counters:
    requests: int = 0
    retries: int = 0


@dataclass
class _GateState:
    wallet_address: str
    account_index: int | None = None
    api_key_index: int = FIRST_USER_API_KEY_INDEX
    identity_verified: bool = False
    authorization_identity_verified: bool = False
    api_key_verified: bool = False
    collateral_positive: bool = False
    fees_verified: bool = False
    active_orders_zero: bool = False
    positions_flat: bool = False
    unrelated_state_clear: bool = False
    trades_read: bool = False
    funding_history_read: bool = False
    active_order_count: int = 0
    regular_order_count: int = 0
    trigger_order_count: int = 0
    trade_count: int = 0
    funding_count: int = 0
    asset_count: int = 0
    position_count: int = 0


@dataclass(frozen=True)
class ReadinessResult:
    status: str
    failure_class: str
    reason: str
    wallet_address: str
    account_index: int | None
    api_key_index: int
    requests: int
    retries: int
    identity_verified: bool
    authorization_identity_verified: bool
    api_key_verified: bool
    collateral_positive: bool
    fees_verified: bool
    active_orders_zero: bool
    positions_flat: bool
    unrelated_state_clear: bool
    trades_read: bool
    funding_history_read: bool
    active_order_count: int
    regular_order_count: int
    trigger_order_count: int
    trade_count: int
    funding_count: int
    asset_count: int
    position_count: int
    write_authority: str = NO_TESTNET_WRITE_AUTHORITY

    @property
    def write_capable(self) -> bool:
        return False

    def evidence(self) -> str:
        return json.dumps(
            {
                "account_creation_blocker": ACCOUNT_CREATION_BLOCKER,
                "account_index": self.account_index,
                "active_order_count": self.active_order_count,
                "api_key_index": self.api_key_index,
                "api_key_verified": self.api_key_verified,
                "asset_count": self.asset_count,
                "authorization_identity_verified": self.authorization_identity_verified,
                "collateral_positive": self.collateral_positive,
                "failure_class": self.failure_class,
                "fees_verified": self.fees_verified,
                "funding_count": self.funding_count,
                "funding_history_read": self.funding_history_read,
                "identity_verified": self.identity_verified,
                "position_count": self.position_count,
                "positions_flat": self.positions_flat,
                "reason": self.reason,
                "regular_order_count": self.regular_order_count,
                "requests": self.requests,
                "retries": self.retries,
                "status": self.status,
                "trades_read": self.trades_read,
                "trade_count": self.trade_count,
                "trigger_order_count": self.trigger_order_count,
                "unrelated_state_clear": self.unrelated_state_clear,
                "wallet_address": self.wallet_address,
                "write_authority": self.write_authority,
                "write_capable": self.write_capable,
            },
            sort_keys=True,
            separators=(",", ":"),
        )


def _result(
    state: _GateState,
    counters: _Counters,
    *,
    status: str,
    failure_class: str,
    reason: str,
) -> ReadinessResult:
    return ReadinessResult(
        status=status,
        failure_class=failure_class,
        reason=reason,
        wallet_address=state.wallet_address,
        account_index=state.account_index,
        api_key_index=state.api_key_index,
        requests=counters.requests,
        retries=counters.retries,
        identity_verified=state.identity_verified,
        authorization_identity_verified=state.authorization_identity_verified,
        api_key_verified=state.api_key_verified,
        collateral_positive=state.collateral_positive,
        fees_verified=state.fees_verified,
        active_orders_zero=state.active_orders_zero,
        positions_flat=state.positions_flat,
        unrelated_state_clear=state.unrelated_state_clear,
        trades_read=state.trades_read,
        funding_history_read=state.funding_history_read,
        active_order_count=state.active_order_count,
        regular_order_count=state.regular_order_count,
        trigger_order_count=state.trigger_order_count,
        trade_count=state.trade_count,
        funding_count=state.funding_count,
        asset_count=state.asset_count,
        position_count=state.position_count,
    )


def blocked_local_result(
    reason: str,
    *,
    failure_class: str = "SAFETY",
    wallet_address: str = EXPECTED_L1_ADDRESS,
    api_key_index: int = FIRST_USER_API_KEY_INDEX,
) -> ReadinessResult:
    state = _GateState(wallet_address=wallet_address, api_key_index=api_key_index)
    return _result(
        state,
        _Counters(),
        status="BLOCKED",
        failure_class=failure_class,
        reason=reason,
    )


@dataclass(frozen=True)
class _ResponseEnvelope:
    status_code: int
    payload: Mapping[str, Any]


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _decode_json(body: Any) -> Any:
    if isinstance(body, (bytes, bytearray)):
        if len(body) > MAX_JSON_BYTES:
            _schema()
        raw = bytes(body).decode("utf-8")
        try:
            return json.loads(
                raw,
                object_pairs_hook=_reject_duplicate_keys,
                parse_constant=lambda value: (_ for _ in ()).throw(
                    ValueError(f"invalid JSON constant: {value}")
                ),
            )
        except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
            _schema()
    if isinstance(body, str):
        if len(body.encode("utf-8")) > MAX_JSON_BYTES:
            _schema()
        try:
            return json.loads(
                body,
                object_pairs_hook=_reject_duplicate_keys,
                parse_constant=lambda value: (_ for _ in ()).throw(
                    ValueError(f"invalid JSON constant: {value}")
                ),
            )
        except (ValueError, json.JSONDecodeError):
            _schema()
    if isinstance(body, (Mapping, list, str, int, float, bool)) or body is None:
        try:
            encoded = json.dumps(body, allow_nan=False, separators=(",", ":"))
        except (TypeError, ValueError):
            _schema()
        if len(encoded.encode("utf-8")) > MAX_JSON_BYTES:
            _schema()
        return body
    _schema()


def _transport_get(transport: ReadTransport | Callable[[ReadRequest], ReadResponse], request: ReadRequest) -> ReadResponse:
    getter = getattr(transport, "get", None)
    if callable(getter):
        response = getter(request)
    elif callable(transport):
        response = transport(request)
    else:
        raise TypeError("read transport must expose GET")
    if not isinstance(response, ReadResponse):
        raise TypeError("read transport returned an invalid response")
    return response


def _get_json(
    transport: ReadTransport | Callable[[ReadRequest], ReadResponse],
    request: ReadRequest,
    counters: _Counters,
) -> _ResponseEnvelope:
    for attempt in range(2):
        counters.requests += 1
        try:
            response = _transport_get(transport, request)
        except (TransportInterruption, TimeoutError, ConnectionError):
            if attempt == 0:
                counters.retries += 1
                continue
            raise _GateFailure("TRANSPORT", "TRANSPORT_RETRY_EXHAUSTED")
        except Exception:
            raise _GateFailure("UNCLASSIFIED", "UNCLASSIFIED_TRANSPORT_FAILURE")

        if isinstance(response.status_code, bool) or not isinstance(response.status_code, int):
            raise _GateFailure("HTTP", "HTTP_STATUS_INVALID")
        if response.final_url is not None and response.final_url != request.url:
            raise _GateFailure("SAFETY", "FIXED_HOST_OR_REDIRECT_VIOLATION")
        try:
            payload = _require_mapping(_decode_json(response.body))
        except _GateFailure:
            raise
        except Exception:
            raise _GateFailure("SCHEMA", "RESPONSE_SCHEMA_INVALID")
        return _ResponseEnvelope(response.status_code, payload)
    raise _GateFailure("TRANSPORT", "TRANSPORT_RETRY_EXHAUSTED")


def _expect_success(envelope: _ResponseEnvelope) -> Mapping[str, Any]:
    code = _integer(_require(envelope.payload, "code"))
    if code == ACCOUNT_NOT_FOUND_CODE:
        raise _GateFailure("IDENTITY", "ACCOUNT_NOT_FOUND")
    if code == API_KEY_NOT_FOUND_CODE:
        raise _GateFailure("AUTH", "API_KEY_NOT_PROVISIONED")
    if envelope.status_code != 200 or code != 200:
        raise _GateFailure("HTTP", "LIGHTER_HTTP_OR_RESULT_CODE_FAILURE")
    return envelope.payload


def _request(
    transport: ReadTransport | Callable[[ReadRequest], ReadResponse],
    counters: _Counters,
    path: str,
    params: Sequence[tuple[str, str]],
    *,
    authorization: str | None = None,
) -> Mapping[str, Any]:
    request = ReadRequest(path, tuple(params), authorization)
    return _expect_success(_get_json(transport, request, counters))


def _parse_discovery_page(payload: Mapping[str, Any], expected_address: str) -> tuple[list[int], str]:
    root_address = _string(_require(payload, "l1_address"), nonempty=True)
    if not _same_address(root_address, expected_address):
        raise _GateFailure("IDENTITY", "DISCOVERY_ADDRESS_MISMATCH")
    accounts = _list(_require(payload, "sub_accounts"))
    indices: list[int] = []
    for account in accounts:
        row = _require_mapping(account)
        index = _integer(_require(row, "index"), nonnegative=True)
        address = _string(_require(row, "l1_address"), nonempty=True)
        if not _same_address(address, expected_address):
            raise _GateFailure("IDENTITY", "DISCOVERY_ACCOUNT_ADDRESS_MISMATCH")
        indices.append(index)
    return indices, _cursor(payload)


def _discover_account_index(
    transport: ReadTransport | Callable[[ReadRequest], ReadResponse],
    counters: _Counters,
    expected_address: str,
    expected_account_index: int | None,
) -> int:
    cursor = ""
    seen_cursors: set[str] = set()
    indices: list[int] = []
    for _ in range(MAX_HISTORY_PAGES):
        params: list[tuple[str, str]] = [("l1_address", expected_address)]
        if cursor:
            params.append(("cursor", cursor))
        payload = _request(transport, counters, "/api/v1/accountsByL1Address", params)
        page_indices, next_cursor = _parse_discovery_page(payload, expected_address)
        indices.extend(page_indices)
        if not next_cursor:
            break
        if next_cursor in seen_cursors:
            raise _GateFailure("SAFETY", "DISCOVERY_CURSOR_REPEATED")
        seen_cursors.add(next_cursor)
        cursor = next_cursor
    else:
        raise _GateFailure("SAFETY", "DISCOVERY_PAGINATION_LIMIT")

    if not indices or len(set(indices)) != len(indices):
        raise _GateFailure("IDENTITY", "DISCOVERY_ACCOUNT_INDEX_AMBIGUOUS")
    if expected_account_index is not None:
        if expected_account_index not in indices:
            raise _GateFailure("IDENTITY", "EXPECTED_ACCOUNT_INDEX_NOT_FOUND")
        return expected_account_index
    return min(indices)


def _parse_api_key(payload: Mapping[str, Any], account_index: int, api_key_index: int) -> None:
    keys = _list(_require(payload, "api_keys"))
    matches = 0
    for item in keys:
        row = _require_mapping(item)
        row_account = _integer(_require(row, "account_index"), nonnegative=True)
        row_index = _integer(_require(row, "api_key_index"), nonnegative=True)
        nonce = _integer(_require(row, "nonce"), nonnegative=True)
        public_key = _string(_require(row, "public_key"), nonempty=True)
        _integer(_require(row, "transaction_time"), nonnegative=True)
        if row_account == account_index and row_index == api_key_index:
            matches += 1
            if nonce < 0 or not public_key:
                raise _GateFailure("AUTH", "API_KEY_METADATA_INVALID")
    if matches != 1:
        raise _GateFailure("AUTH", "API_KEY_NOT_PROVISIONED")


def _parse_positions_and_assets(
    account: Mapping[str, Any],
    state: _GateState,
    quote_symbol: str,
) -> None:
    selected_account_index = state.account_index
    account_index = _integer(_require(account, "account_index"), nonnegative=True)
    account_row_index = _integer(_require(account, "index"), nonnegative=True)
    if (
        selected_account_index is None
        or account_index != selected_account_index
        or account_row_index != selected_account_index
    ):
        raise _GateFailure("IDENTITY", "ACCOUNT_INDEX_FIELDS_MISMATCH")
    address = _string(_require(account, "l1_address"), nonempty=True)
    if not _same_address(address, state.wallet_address):
        raise _GateFailure("IDENTITY", "ACCOUNT_ADDRESS_MISMATCH")
    _integer(_require(account, "account_type"), nonnegative=True)
    _integer(_require(account, "account_trading_mode"), nonnegative=True)
    _integer(_require(account, "status"), nonnegative=True)
    _integer(_require(account, "pending_order_count"), nonnegative=True)
    if _integer(_require(account, "pending_order_count"), nonnegative=True) != 0:
        raise _GateFailure("SAFETY", "ACCOUNT_PENDING_ORDERS_PRESENT")
    collateral = _decimal(_require(account, "collateral"), nonnegative=True)
    _decimal(_require(account, "available_balance"), nonnegative=True)
    total_asset_value = _decimal(_require(account, "total_asset_value"), nonnegative=True)
    cross_asset_value = _decimal(_require(account, "cross_asset_value"), nonnegative=True)
    if collateral <= 0 or total_asset_value <= 0 or cross_asset_value <= 0:
        raise _GateFailure("SAFETY", "COLLATERAL_NOT_POSITIVE")
    state.collateral_positive = True

    shares = _list(_require(account, "shares"))
    if shares:
        raise _GateFailure("SAFETY", "UNRELATED_POOL_SHARES_PRESENT")
    pool_info = _require(account, "pool_info")
    if pool_info is not None:
        raise _GateFailure("SAFETY", "UNRELATED_POOL_STATE_PRESENT")
    pending_unlocks = _list(_require(account, "pending_unlocks"))
    if pending_unlocks:
        raise _GateFailure("SAFETY", "UNRELATED_PENDING_UNLOCK_PRESENT")

    positions = _list(_require(account, "positions"))
    state.position_count = len(positions)
    for item in positions:
        position = _require_mapping(item)
        _integer(_require(position, "market_id"), nonnegative=True)
        _string(_require(position, "symbol"), nonempty=True)
        _decimal(_require(position, "initial_margin_fraction"), nonnegative=True)
        open_orders = _integer(_require(position, "open_order_count"), nonnegative=True)
        pending_orders = _integer(_require(position, "pending_order_count"), nonnegative=True)
        tied_orders = _integer(_require(position, "position_tied_order_count"), nonnegative=True)
        sign = _integer(_require(position, "sign"))
        position_size = _decimal(_require(position, "position"), nonnegative=True)
        average_price = _decimal(_require(position, "avg_entry_price"), nonnegative=True)
        position_value = _decimal(_require(position, "position_value"))
        unrealized = _decimal(_require(position, "unrealized_pnl"))
        _decimal(_require(position, "realized_pnl"))
        _decimal(_require(position, "liquidation_price"), nonnegative=True)
        _integer(_require(position, "margin_mode"), nonnegative=True)
        _integer(_require(position, "margin_set_flag"), nonnegative=True)
        allocated_margin = _decimal(_require(position, "allocated_margin"), nonnegative=True)
        _decimal(_require(position, "total_discount"))
        if (
            open_orders != 0
            or pending_orders != 0
            or tied_orders != 0
            or sign != 0
            or not _zero(position_size)
            or not _zero(average_price)
            or not _zero(position_value)
            or not _zero(unrealized)
            or not _zero(allocated_margin)
        ):
            raise _GateFailure("SAFETY", "POSITIONS_NOT_FLAT")
    state.positions_flat = True

    assets = _list(_require(account, "assets"))
    state.asset_count = len(assets)
    symbols: set[str] = set()
    asset_ids: set[int] = set()
    quote_asset_count = 0
    for item in assets:
        asset = _require_mapping(item)
        symbol = _string(_require(asset, "symbol"), nonempty=True)
        asset_id = _integer(_require(asset, "asset_id"), nonnegative=True)
        balance = _decimal(_require(asset, "balance"))
        locked_balance = _decimal(_require(asset, "locked_balance"))
        margin_balance = _decimal(_require(asset, "margin_balance"))
        margin_mode = _string(_require(asset, "margin_mode"), nonempty=True)
        if margin_mode not in {"enabled", "disabled"}:
            _schema()
        _decimal(_require(asset, "multiplier"))
        if symbol in symbols or asset_id in asset_ids:
            raise _GateFailure("SAFETY", "DUPLICATE_ACCOUNT_ASSET")
        symbols.add(symbol)
        asset_ids.add(asset_id)
        if symbol == quote_symbol:
            quote_asset_count += 1
            if balance <= 0 or margin_balance <= 0 or not _zero(locked_balance):
                raise _GateFailure("SAFETY", "QUOTE_COLLATERAL_STATE_INVALID")
        elif not (_zero(balance) and _zero(locked_balance) and _zero(margin_balance)):
            raise _GateFailure("SAFETY", "UNRELATED_ASSET_BALANCE_PRESENT")
    if quote_asset_count != 1:
        raise _GateFailure("SAFETY", "QUOTE_ASSET_NOT_UNIQUE")
    state.unrelated_state_clear = True


def _parse_detailed_account(
    payload: Mapping[str, Any],
    state: _GateState,
    quote_symbol: str,
) -> None:
    total = _integer(_require(payload, "total"), nonnegative=True)
    accounts = _list(_require(payload, "accounts"))
    if total != len(accounts) or len(accounts) != 1:
        raise _GateFailure("IDENTITY", "ACCOUNT_RESPONSE_NOT_EXACT")
    next_cursor = _cursor(payload)
    if next_cursor:
        raise _GateFailure("SAFETY", "ACCOUNT_RESPONSE_PAGINATED")
    _parse_positions_and_assets(_require_mapping(accounts[0]), state, quote_symbol)


_ORDER_TYPES = frozenset(
    {
        "limit",
        "market",
        "stop-loss",
        "stop-loss-limit",
        "take-profit",
        "take-profit-limit",
        "twap",
        "twap-sub",
        "liquidation",
    }
)
_TRIGGER_TYPES = frozenset(
    {"stop-loss", "stop-loss-limit", "take-profit", "take-profit-limit"}
)


def _parse_active_orders(payload: Mapping[str, Any], state: _GateState) -> None:
    orders = _list(_require(payload, "orders"))
    if _cursor(payload):
        raise _GateFailure("SAFETY", "ACTIVE_ORDER_RESPONSE_PAGINATED")
    state.active_order_count = len(orders)
    for item in orders:
        order = _require_mapping(item)
        owner = _integer(_require(order, "owner_account_index"), nonnegative=True)
        if state.account_index is None or owner != state.account_index:
            raise _GateFailure("IDENTITY", "ACTIVE_ORDER_ACCOUNT_MISMATCH")
        order_type = _string(_require(order, "type"), nonempty=True)
        if order_type not in _ORDER_TYPES:
            _schema()
        if order_type in _TRIGGER_TYPES:
            state.trigger_order_count += 1
        else:
            state.regular_order_count += 1
    if orders:
        raise _GateFailure("SAFETY", "ACTIVE_ORDERS_PRESENT")
    state.active_orders_zero = True


_TRADE_REQUIRED_INT_FIELDS = (
    "trade_id",
    "market_id",
    "ask_id",
    "bid_id",
    "ask_account_id",
    "bid_account_id",
    "ask_client_id",
    "bid_client_id",
    "block_height",
    "timestamp",
    "taker_initial_margin_fraction_before",
    "maker_initial_margin_fraction_before",
    "transaction_time",
    "integrator_taker_fee",
    "integrator_taker_fee_collector_index",
    "integrator_maker_fee",
    "integrator_maker_fee_collector_index",
    "taker_allocated_margin_usdc_before",
    "taker_allocated_margin_usdc_after",
    "maker_allocated_margin_usdc_before",
    "maker_allocated_margin_usdc_after",
    "ask_order_version",
    "bid_order_version",
)
_TRADE_REQUIRED_STRING_FIELDS = (
    "trade_id_str",
    "tx_hash",
    "size",
    "price",
    "usd_amount",
    "taker_position_size_before",
    "taker_entry_quote_before",
    "maker_position_size_before",
    "maker_entry_quote_before",
    "ask_account_pnl",
    "bid_account_pnl",
    "ask_client_id_str",
    "bid_client_id_str",
    "ask_id_str",
    "bid_id_str",
)
_TRADE_REQUIRED_BOOL_FIELDS = (
    "is_maker_ask",
    "taker_position_sign_changed",
    "maker_position_sign_changed",
)


def _parse_trade(item: Any, account_index: int) -> None:
    trade = _require_mapping(item)
    for key in _TRADE_REQUIRED_INT_FIELDS:
        _integer(_require(trade, key), nonnegative=True)
    trade_type = _string(_require(trade, "type"), nonempty=True)
    if trade_type not in {"trade", "liquidation", "deleverage", "market-settlement"}:
        _schema()
    for key in _TRADE_REQUIRED_STRING_FIELDS:
        _string(_require(trade, key), nonempty=key in {"trade_id_str", "tx_hash"})
    for key in _TRADE_REQUIRED_BOOL_FIELDS:
        _boolean(_require(trade, key))
    _decimal(_require(trade, "size"), nonnegative=True)
    if _decimal(_require(trade, "size"), nonnegative=True) <= 0:
        _schema()
    _decimal(_require(trade, "price"), nonnegative=True)
    _decimal(_require(trade, "usd_amount"), nonnegative=True)
    _decimal(_require(trade, "taker_position_size_before"), nonnegative=True)
    _decimal(_require(trade, "maker_position_size_before"), nonnegative=True)
    _decimal(_require(trade, "taker_entry_quote_before"))
    _decimal(_require(trade, "maker_entry_quote_before"))
    _decimal(_require(trade, "ask_account_pnl"))
    _decimal(_require(trade, "bid_account_pnl"))
    ask_account = _integer(_require(trade, "ask_account_id"), nonnegative=True)
    bid_account = _integer(_require(trade, "bid_account_id"), nonnegative=True)
    if account_index not in {ask_account, bid_account}:
        raise _GateFailure("IDENTITY", "TRADE_ACCOUNT_MISMATCH")


def _read_trades(
    transport: ReadTransport | Callable[[ReadRequest], ReadResponse],
    counters: _Counters,
    state: _GateState,
    authorization: str,
) -> None:
    cursor = ""
    seen_cursors: set[str] = set()
    for _ in range(MAX_HISTORY_PAGES):
        params = [
            ("market_id", "255"),
            ("market_type", "all"),
            ("account_index", str(state.account_index)),
            ("sort_by", "trade_id"),
            ("sort_dir", "desc"),
            ("limit", str(PAGE_LIMIT)),
            ("type", "all"),
        ]
        if cursor:
            params.append(("cursor", cursor))
        payload = _request(
            transport,
            counters,
            "/api/v1/trades",
            params,
            authorization=authorization,
        )
        trades = _list(_require(payload, "trades"))
        if len(trades) > PAGE_LIMIT:
            _schema()
        for item in trades:
            _parse_trade(item, state.account_index if state.account_index is not None else -1)
        state.trade_count += len(trades)
        next_cursor = _cursor(payload)
        if not next_cursor:
            state.trades_read = True
            return
        if next_cursor in seen_cursors:
            raise _GateFailure("SAFETY", "TRADES_CURSOR_REPEATED")
        seen_cursors.add(next_cursor)
        cursor = next_cursor
    raise _GateFailure("SAFETY", "TRADES_PAGINATION_LIMIT")


def _parse_position_funding(item: Any) -> None:
    funding = _require_mapping(item)
    _integer(_require(funding, "timestamp"), nonnegative=True)
    _integer(_require(funding, "market_id"), nonnegative=True)
    _integer(_require(funding, "funding_id"), nonnegative=True)
    _decimal(_require(funding, "change"))
    _decimal(_require(funding, "discount"))
    _decimal(_require(funding, "rate"))
    _decimal(_require(funding, "position_size"), nonnegative=True)
    position_side = _string(_require(funding, "position_side"), nonempty=True)
    if position_side not in {"long", "short"}:
        _schema()


def _read_position_funding(
    transport: ReadTransport | Callable[[ReadRequest], ReadResponse],
    counters: _Counters,
    state: _GateState,
    authorization: str,
) -> None:
    cursor = ""
    seen_cursors: set[str] = set()
    for _ in range(MAX_HISTORY_PAGES):
        params = [
            ("account_index", str(state.account_index)),
            ("limit", str(PAGE_LIMIT)),
            ("side", "all"),
        ]
        if cursor:
            params.append(("cursor", cursor))
        payload = _request(
            transport,
            counters,
            "/api/v1/positionFunding",
            params,
            authorization=authorization,
        )
        fundings = _list(_require(payload, "position_fundings"))
        if len(fundings) > PAGE_LIMIT:
            _schema()
        for item in fundings:
            _parse_position_funding(item)
        state.funding_count += len(fundings)
        next_cursor = _cursor(payload)
        if not next_cursor:
            state.funding_history_read = True
            return
        if next_cursor in seen_cursors:
            raise _GateFailure("SAFETY", "FUNDING_CURSOR_REPEATED")
        seen_cursors.add(next_cursor)
        cursor = next_cursor
    raise _GateFailure("SAFETY", "FUNDING_PAGINATION_LIMIT")


def _parse_account_limits(payload: Mapping[str, Any]) -> None:
    _integer(_require(payload, "max_llp_percentage"), nonnegative=True)
    _string(_require(payload, "user_tier"), nonempty=True)
    _boolean(_require(payload, "can_create_public_pool"))
    _decimal(_require(payload, "max_llp_amount"), nonnegative=True)
    _integer(_require(payload, "current_maker_fee_tick"), nonnegative=True)
    _integer(_require(payload, "current_taker_fee_tick"), nonnegative=True)
    _decimal(_require(payload, "effective_lit_stakes"), nonnegative=True)
    _decimal(_require(payload, "leased_lit"), nonnegative=True)
    _string(_require(payload, "user_tier_name"), nonempty=True)


def _parse_market_contract_row(row: Mapping[str, Any]) -> MarketContract:
    market_id = _integer(_require(row, "market_id"), nonnegative=True)
    symbol = _string(_require(row, "symbol"), nonempty=True)
    market_type = _string(_require(row, "market_type"), nonempty=True)
    if market_type not in {"perp", "spot"}:
        _schema()
    min_base = _decimal(_require(row, "min_base_amount"), nonnegative=True)
    min_quote = _decimal(_require(row, "min_quote_amount"), nonnegative=True)
    size_decimals = _integer(_require(row, "size_decimals"), nonnegative=True)
    price_decimals = _integer(_require(row, "price_decimals"), nonnegative=True)
    quote_decimals = _integer(_require(row, "supported_quote_decimals"), nonnegative=True)
    status = _string(_require(row, "status"), nonempty=True)
    config = _require_mapping(_require(row, "market_config"))
    force_reduce_only = _boolean(_require(config, "force_reduce_only"))
    return MarketContract(
        market_id=market_id,
        symbol=symbol,
        market_type=market_type,
        min_base_amount=min_base,
        min_quote_amount=min_quote,
        size_decimals=size_decimals,
        price_decimals=price_decimals,
        quote_decimals=quote_decimals,
        status=status,
        force_reduce_only=force_reduce_only,
    )


def parse_market_contract(
    payload: Mapping[str, Any],
    *,
    market_id: int,
) -> MarketContract:
    """Parse current minimum/grid metadata from official order-book details."""

    payload = _expect_success(_ResponseEnvelope(200, _require_mapping(payload)))
    rows = _list(_require(payload, "order_book_details"))
    matches = [_parse_market_contract_row(_require_mapping(row)) for row in rows]
    selected = [row for row in matches if row.market_id == market_id]
    if len(selected) != 1:
        raise ValueError("market metadata was not exact")
    return selected[0]


def parse_two_sided_liquidity(
    payload: Mapping[str, Any],
    *,
    market_id: int,
) -> LiquiditySnapshot:
    """Parse public book depth without preparing an order."""

    payload = _expect_success(_ResponseEnvelope(200, _require_mapping(payload)))
    bids = _list(_require(payload, "bids"))
    asks = _list(_require(payload, "asks"))

    def level(row: Any) -> tuple[Decimal, Decimal]:
        row = _require_mapping(row)
        price = _decimal(_require(row, "price"), nonnegative=True)
        quantity = _decimal(_require(row, "remaining_base_amount"), nonnegative=True)
        if price <= 0 or quantity <= 0:
            _schema()
        return price, quantity

    bid_levels = [level(row) for row in bids]
    ask_levels = [level(row) for row in asks]
    if not bid_levels or not ask_levels:
        raise ValueError("two-sided liquidity is absent")
    best_bid = max(price for price, _ in bid_levels)
    best_ask = min(price for price, _ in ask_levels)
    if best_bid >= best_ask:
        raise ValueError("public book is crossed or locked")
    return LiquiditySnapshot(
        market_id=market_id,
        bid_levels=len(bid_levels),
        ask_levels=len(ask_levels),
        best_bid=best_bid,
        best_ask=best_ask,
        two_sided=True,
    )


def run_level_b(
    transport: ReadTransport | Callable[[ReadRequest], ReadResponse],
    *,
    wallet: WalletMetadata | None = None,
    expected_account_index: int | None = None,
    api_key_index: int = FIRST_USER_API_KEY_INDEX,
    authorization_loader: Callable[[], AuthorizationCapability | None] | None = None,
    quote_symbol: str = USDC_SYMBOL,
) -> ReadinessResult:
    """Run a fail-closed account-only Level-B read gate.

    The transport is supplied by the caller.  Every request is GET-only,
    fixed-host, and made at most twice when the first attempt is interrupted.
    No request is made until fixed wallet metadata and the public identity are
    valid.  A successful result is read-only readiness evidence, never write
    authority.
    """

    metadata = wallet or inspect_protected_wallet()
    if not metadata.ready:
        reason = metadata.reason
    elif Path(metadata.path) != Path(TESTNET_WALLET_PATH):
        reason = "FIXED_WALLET_PATH_REQUIRED"
    else:
        reason = ""
    if reason:
        return blocked_local_result(
            reason,
            failure_class="SAFETY",
            wallet_address=EXPECTED_L1_ADDRESS,
            api_key_index=api_key_index,
        )
    try:
        address = canonical_testnet_address(metadata.address)
        plan = build_api_key_provisioning_plan(
            expected_account_index,
            api_key_index=api_key_index,
        )
        if plan.api_key_index != api_key_index:
            raise ValueError("API-key plan mismatch")
    except ValueError:
        return blocked_local_result(
            "READINESS_CONFIGURATION_INVALID",
            failure_class="SAFETY",
            wallet_address=EXPECTED_L1_ADDRESS,
            api_key_index=api_key_index,
        )

    state = _GateState(wallet_address=address, api_key_index=api_key_index)
    counters = _Counters()
    try:
        state.account_index = _discover_account_index(
            transport,
            counters,
            address,
            expected_account_index,
        )
        state.identity_verified = True

        if authorization_loader is None:
            raise _GateFailure("AUTH", "API_KEY_NOT_PROVISIONED")
        try:
            authorization = authorization_loader()
        except Exception:
            raise _GateFailure("UNCLASSIFIED", "UNCLASSIFIED_AUTHORIZATION_LOADER_FAILURE")
        if not isinstance(authorization, AuthorizationCapability):
            raise _GateFailure("AUTH", "AUTHORIZATION_IDENTITY_UNPROVEN")
        if (
            authorization.account_index != state.account_index
            or authorization.api_key_index != api_key_index
        ):
            raise _GateFailure("IDENTITY", "AUTHORIZATION_IDENTITY_MISMATCH")
        state.authorization_identity_verified = True

        payload = _request(
            transport,
            counters,
            "/api/v1/apikeys",
            [("account_index", str(state.account_index)), ("api_key_index", str(api_key_index))],
        )
        _parse_api_key(payload, state.account_index, api_key_index)
        state.api_key_verified = True

        payload = _request(
            transport,
            counters,
            "/api/v1/account",
            [
                ("by", "index"),
                ("value", str(state.account_index)),
                ("active_only", "false"),
            ],
            authorization=authorization.header_value,
        )
        _parse_detailed_account(payload, state, quote_symbol)

        payload = _request(
            transport,
            counters,
            "/api/v1/accountLimits",
            [("account_index", str(state.account_index))],
            authorization=authorization.header_value,
        )
        _parse_account_limits(payload)
        state.fees_verified = True

        payload = _request(
            transport,
            counters,
            "/api/v1/accountActiveOrders",
            [("account_index", str(state.account_index)), ("market_id", "255"), ("market_type", "all")],
            authorization=authorization.header_value,
        )
        _parse_active_orders(payload, state)

        _read_trades(transport, counters, state, authorization.header_value)
        _read_position_funding(transport, counters, state, authorization.header_value)
    except _GateFailure as failure:
        return _result(
            state,
            counters,
            status="BLOCKED",
            failure_class=failure.failure_class,
            reason=failure.reason,
        )
    except Exception:
        return _result(
            state,
            counters,
            status="BLOCKED",
            failure_class="UNCLASSIFIED",
            reason="UNCLASSIFIED_READINESS_FAILURE",
        )

    return _result(
        state,
        counters,
        status="READY",
        failure_class="NONE",
        reason="LEVEL_B_READ_ONLY_ACCOUNT_READINESS_PROVEN",
    )


class FixtureReadTransport:
    """Deterministic GET-only transport for local contract tests and fixtures."""

    def __init__(self, responses: Mapping[str, Any]):
        self._responses = dict(responses)
        self.requests: list[ReadRequest] = []

    def get(self, request: ReadRequest) -> ReadResponse:
        self.requests.append(request)
        value = self._responses.get(request.path)
        if value is None:
            raise PrematureResponse()
        if isinstance(value, list):
            if not value:
                raise PrematureResponse()
            value = value.pop(0)
        if isinstance(value, BaseException):
            raise value
        if isinstance(value, ReadResponse):
            return value
        if isinstance(value, Mapping) and "body" in value:
            return ReadResponse(
                status_code=value.get("status_code", 200),
                body=value["body"],
                final_url=value.get("final_url"),
            )
        return ReadResponse(status_code=200, body=value)

    def sanitized_requests(self) -> list[dict[str, Any]]:
        return [
            {
                "method": request.method,
                "params": list(request.params),
                "path": request.path,
                "authorization_present": request.authorization is not None,
            }
            for request in self.requests
        ]


def _load_fixture(
    path: Path,
) -> tuple[FixtureReadTransport, Callable[[], AuthorizationCapability | None] | None]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            fixture = json.load(handle)
    except (OSError, ValueError, json.JSONDecodeError):
        raise ValueError("FIXTURE_UNREADABLE")
    fixture = _require_mapping(fixture)
    _reject_sensitive_fixture_fields(fixture)
    responses = _require_mapping(_require(fixture, "responses"))
    auth_available = _optional(fixture, "authorization_available", False)
    if not isinstance(auth_available, bool):
        raise ValueError("FIXTURE_AUTHORIZATION_FLAG_INVALID")
    account_index = _optional(fixture, "authorization_account_index")
    api_key_index = _optional(fixture, "authorization_api_key_index")
    if auth_available:
        if (
            isinstance(account_index, bool)
            or not isinstance(account_index, int)
            or account_index < 0
            or isinstance(api_key_index, bool)
            or not isinstance(api_key_index, int)
            or api_key_index < 0
        ):
            raise ValueError("FIXTURE_AUTHORIZATION_IDENTITY_REQUIRED")
        loader = lambda: AuthorizationCapability("fixture", account_index, api_key_index)
    else:
        loader = None
    return FixtureReadTransport(responses), loader


_SENSITIVE_FIXTURE_FIELDS = frozenset(
    {
        "authorization",
        "auth_token",
        "access_token",
        "api_key",
        "password",
        "private_key",
        "secret",
        "wallet_private_key",
    }
)


def _reject_sensitive_fixture_fields(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if isinstance(key, str) and key.lower() in _SENSITIVE_FIXTURE_FIELDS:
                raise ValueError("FIXTURE_CONTAINS_SENSITIVE_FIELD")
            _reject_sensitive_fixture_fields(child)
    elif isinstance(value, list):
        for child in value:
            _reject_sensitive_fixture_fields(child)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Lighter testnet protected account-readiness diagnostic (GET fixtures only)"
    )
    parser.add_argument(
        "--fixture",
        type=Path,
        help="local JSON response fixture; no live transport is available",
    )
    args = parser.parse_args(argv)

    wallet = inspect_protected_wallet()
    if not wallet.ready:
        result = blocked_local_result(wallet.reason)
    elif args.fixture is None:
        result = blocked_local_result(ACCOUNT_CREATION_BLOCKER)
    else:
        try:
            transport, authorization_loader = _load_fixture(args.fixture)
            result = run_level_b(
                transport,
                wallet=wallet,
                authorization_loader=authorization_loader,
            )
        except (_GateFailure, ValueError):
            result = blocked_local_result("FIXTURE_SCHEMA_INVALID", failure_class="SCHEMA")
    print(result.evidence())
    return 0


__all__ = [
    "ACCOUNT_CREATION_BLOCKER",
    "AccountCreationPlan",
    "AuthorizationCapability",
    "ApiKeyProvisioningPlan",
    "EXPECTED_L1_ADDRESS",
    "FIRST_USER_API_KEY_INDEX",
    "FixtureReadTransport",
    "LiquiditySnapshot",
    "MarketContract",
    "NO_TESTNET_WRITE_AUTHORITY",
    "OFFICIAL_SOURCES",
    "PrematureResponse",
    "ReadRequest",
    "ReadResponse",
    "ReadinessResult",
    "TESTNET_API_URL",
    "TESTNET_CHAIN_ID",
    "TESTNET_WS_URL",
    "TESTNET_WALLET_PATH",
    "TransportInterruption",
    "WalletMetadata",
    "blocked_local_result",
    "build_account_creation_plan",
    "build_api_key_provisioning_plan",
    "canonical_testnet_address",
    "inspect_protected_wallet",
    "main",
    "parse_market_contract",
    "parse_two_sided_liquidity",
    "run_level_b",
]


if __name__ == "__main__":
    raise SystemExit(main())
