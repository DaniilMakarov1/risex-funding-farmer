"""Explicitly invoked, read-only Robinhood paired-opening readiness checks.

The HCR-5 path is deliberately separate from :mod:`engine` and
:mod:`series`.  It can resolve the current public catalog and book and read
both selected accounts, but it has no order, transfer, signing, nonce, or
mutation method.  A key is used only by the official auth-token read path
when the operator invokes this command from a local TTY.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace
from decimal import Decimal, InvalidOperation
import importlib
from importlib import metadata as importlib_metadata
import inspect
import math
import re
import time
from typing import Any, Callable, Mapping, Protocol, Sequence

from .contracts import (
    AccountMarginEvidence,
    AccountSnapshot,
    ContractError,
    Direction,
    MarketMetadata,
    OFFICIAL_ROBINHOOD_API_URL,
    OFFICIAL_ROBINHOOD_CHAIN_ID,
    OrderSnapshot,
    decimal_to_integer,
)
from .journal import sanitize, sanitize_exception
from .keychain import read_hidden_secret
from .sdk import (
    MissingSdkError,
    REQUIRED_LIGHTER_SDK_VERSION,
    SecretProvider,
    _await,
    _first_mapping,
    _model_dict,
    _order_snapshot_mapping,
    _require_success_code,
    _select_perp_market_by_symbol,
    ROBINHOOD_ORDER_BOOK_LIMIT,
)
from .series import OrderBookSnapshot


READINESS_STATUSES = frozenset({"PASS", "BLOCKED", "UNKNOWN", "UNSET"})


def _decimal(value: Any, name: str, *, positive: bool = False) -> Decimal:
    if isinstance(value, bool) or isinstance(value, float):
        raise ContractError(f"{name} must be an exact decimal string or Decimal")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ContractError(f"{name} must be a finite decimal") from exc
    if not parsed.is_finite() or (positive and parsed <= 0):
        qualifier = "positive " if positive else ""
        raise ContractError(f"{name} must be a finite {qualifier}decimal")
    return parsed


def _index(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ContractError(f"{name} must be a non-negative integer")
    return value


def _strict_integer(value: Any, name: str, *, minimum: int | None = None) -> int:
    """Parse an SDK identity only when its wire value is an exact integer."""

    if isinstance(value, bool):
        raise ContractError(f"{name} must be an exact integer")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and re.fullmatch(r"[+-]?[0-9]+", value):
        parsed = int(value)
    else:
        raise ContractError(f"{name} must be an exact integer")
    if minimum is not None and parsed < minimum:
        raise ContractError(f"{name} must be at least {minimum}")
    return parsed


def _finite_positive(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise ContractError(f"{name} must be a finite positive number")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ContractError(f"{name} must be a finite positive number") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise ContractError(f"{name} must be a finite positive number")
    return parsed


def _timestamp(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise ContractError(f"{name} must be a timestamp")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ContractError(f"{name} must be a timestamp") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise ContractError(f"{name} must be a finite non-negative timestamp")
    return parsed


@dataclass(frozen=True, slots=True)
class ReadinessMarketMarginEvidence:
    """Whitelisted market margin defaults from one catalog observation."""

    market_id: int
    symbol: str
    observed_at: float
    default_initial_margin_fraction: int | None
    minimum_initial_margin_fraction: int | None
    invalid_fields: tuple[str, ...] = ()
    source: str = "orderBookDetails response"
    sdk_version: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "market_id", _index(self.market_id, "market_id"))
        if not isinstance(self.symbol, str) or not self.symbol.strip():
            raise ContractError("symbol must be non-empty text")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        object.__setattr__(self, "observed_at", _timestamp(self.observed_at, "observed_at"))
        for value, name in (
            (self.default_initial_margin_fraction, "default_initial_margin_fraction"),
            (self.minimum_initial_margin_fraction, "minimum_initial_margin_fraction"),
        ):
            if value is not None:
                if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                    raise ContractError(f"{name} must be a non-negative integer or None")
        if not isinstance(self.invalid_fields, tuple) or any(
            not isinstance(item, str) or not item for item in self.invalid_fields
        ):
            raise ContractError("invalid_fields must be a tuple of non-empty names")
        if not isinstance(self.source, str) or not self.source.strip():
            raise ContractError("source must be non-empty text")
        object.__setattr__(self, "source", self.source.strip())
        if self.sdk_version is not None:
            if not isinstance(self.sdk_version, str) or not self.sdk_version.strip():
                raise ContractError("sdk_version must be non-empty text or None")
            object.__setattr__(self, "sdk_version", self.sdk_version.strip())

    @classmethod
    def from_response(
        cls,
        value: Mapping[str, Any],
        *,
        market_id: int,
        symbol: str,
        observed_at: float,
        source: str = "orderBookDetails response",
        sdk_version: str | None = None,
    ) -> "ReadinessMarketMarginEvidence":
        invalid: list[str] = []

        def raw_integer(name: str, *aliases: str) -> int | None:
            values: list[int] = []
            for candidate in (name, *aliases):
                if candidate not in value or value[candidate] is None:
                    continue
                raw = value[candidate]
                if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
                    invalid.append(name)
                    return None
                values.append(raw)
            if not values:
                return None
            if len(set(values)) != 1:
                invalid.append(name)
                return None
            return values[0]

        default_fraction = raw_integer("default_initial_margin_fraction")
        minimum_fraction = raw_integer(
            "minimum_initial_margin_fraction", "min_initial_margin_fraction"
        )
        if (
            default_fraction is not None
            and minimum_fraction is not None
            and default_fraction < minimum_fraction
        ):
            invalid.append("margin_fractions")

        return cls(
            market_id=market_id,
            symbol=symbol,
            observed_at=observed_at,
            default_initial_margin_fraction=default_fraction,
            minimum_initial_margin_fraction=minimum_fraction,
            invalid_fields=tuple(invalid),
            source=source,
            sdk_version=sdk_version,
        )

    @property
    def status(self) -> str:
        if self.source == "not returned":
            return "UNAVAILABLE"
        if self.invalid_fields:
            return "INVALID"
        if self.default_initial_margin_fraction is None or self.minimum_initial_margin_fraction is None:
            return "INCOMPLETE"
        return "OBSERVED"

    @staticmethod
    def _field(name: str, value: int | None, invalid_fields: tuple[str, ...]) -> dict[str, Any]:
        invalid = name in invalid_fields
        present = invalid or value is not None
        return {
            "present": present,
            "valid": False if invalid else (True if present else None),
            "raw": None if invalid else value,
            "units": "unverified",
        }

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "binding": {"market_id": self.market_id, "symbol": self.symbol},
            "provenance": {
                "source": self.source,
                "sdk_version": self.sdk_version,
                "observed_at": self.observed_at,
            },
            "default_initial_margin_fraction": self._field(
                "default_initial_margin_fraction", self.default_initial_margin_fraction, self.invalid_fields
            ),
            "minimum_initial_margin_fraction": self._field(
                "minimum_initial_margin_fraction", self.minimum_initial_margin_fraction, self.invalid_fields
            ),
            "invalid_fields": list(self.invalid_fields),
            "units_note": "Margin fraction response units are unverified; no signing input encoding was applied.",
        }


@dataclass(frozen=True, slots=True)
class ReadinessMarketMetadata:
    """Current catalog fields needed by readiness, without invented margin evidence."""

    market_id: int
    symbol: str
    status: str
    price_decimals: int
    size_decimals: int
    minimum_base_amount: Decimal
    minimum_quote_amount: Decimal
    observed_at: float
    market_type: str = "perp"
    venue: str = "robinhood"
    margin_evidence: ReadinessMarketMarginEvidence | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "market_id", _index(self.market_id, "market_id"))
        if not isinstance(self.symbol, str) or not self.symbol.strip():
            raise ContractError("symbol must be non-empty text")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        if not isinstance(self.status, str) or not self.status.strip():
            raise ContractError("status must be non-empty text")
        object.__setattr__(self, "status", self.status.strip())
        object.__setattr__(self, "price_decimals", _index(self.price_decimals, "price_decimals"))
        object.__setattr__(self, "size_decimals", _index(self.size_decimals, "size_decimals"))
        object.__setattr__(self, "minimum_base_amount", _decimal(self.minimum_base_amount, "minimum_base_amount", positive=True))
        object.__setattr__(self, "minimum_quote_amount", _decimal(self.minimum_quote_amount, "minimum_quote_amount", positive=True))
        object.__setattr__(self, "observed_at", _timestamp(self.observed_at, "observed_at"))
        if str(self.market_type).strip().lower() != "perp":
            raise ContractError("market_type must be perp")
        object.__setattr__(self, "market_type", "perp")
        object.__setattr__(self, "venue", str(self.venue).strip().lower() or "robinhood")
        if self.margin_evidence is not None:
            if not isinstance(self.margin_evidence, ReadinessMarketMarginEvidence):
                raise ContractError("margin_evidence must be ReadinessMarketMarginEvidence or None")
            if (
                self.margin_evidence.market_id != self.market_id
                or self.margin_evidence.symbol != self.symbol
                or self.margin_evidence.observed_at > self.observed_at
            ):
                raise ContractError("margin_evidence identity does not match market metadata")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ReadinessMarketMetadata":
        market_id = _index(value.get("market_id"), "market_id")
        symbol = str(value.get("symbol", value.get("market_symbol", "")))
        observed_at = _timestamp(value.get("observed_at"), "observed_at")
        return cls(
            market_id=market_id,
            symbol=symbol,
            status=str(value.get("status", "")),
            price_decimals=_index(value.get("price_decimals", value.get("supported_price_decimals")), "price_decimals"),
            size_decimals=_index(value.get("size_decimals", value.get("supported_size_decimals")), "size_decimals"),
            minimum_base_amount=_decimal(value.get("minimum_base_amount", value.get("min_base_amount")), "minimum_base_amount", positive=True),
            minimum_quote_amount=_decimal(value.get("minimum_quote_amount", value.get("min_quote_amount")), "minimum_quote_amount", positive=True),
            observed_at=observed_at,
            market_type=str(value.get("market_type", "perp")),
            venue=str(value.get("venue", "robinhood")),
            margin_evidence=ReadinessMarketMarginEvidence.from_response(
                value,
                market_id=market_id,
                symbol=symbol,
                observed_at=observed_at,
            ),
        )


@dataclass(frozen=True, slots=True)
class ReadinessConfig:
    """Minimal operator inputs for one non-mutating paired-opening check."""

    market_symbol: str
    quantity: Decimal
    direction: Direction | str
    source_account_index: int
    receiver_account_index: int
    api_key_index: int
    freshness_seconds: float
    request_timeout_seconds: float
    source_limit_price: Decimal | None = None
    receiver_worst_price: Decimal | None = None
    api_base_url: str = OFFICIAL_ROBINHOOD_API_URL
    chain_id: int = OFFICIAL_ROBINHOOD_CHAIN_ID
    auth_token_lifetime_seconds: int = 600

    def __post_init__(self) -> None:
        if not isinstance(self.market_symbol, str) or not self.market_symbol.strip():
            raise ContractError("market_symbol must be non-empty text")
        object.__setattr__(self, "market_symbol", self.market_symbol.strip().upper())
        try:
            direction = self.direction if isinstance(self.direction, Direction) else Direction(str(self.direction).upper())
        except (TypeError, ValueError) as exc:
            raise ContractError("direction must be LONG or SHORT") from exc
        object.__setattr__(self, "direction", direction)
        object.__setattr__(self, "quantity", _decimal(self.quantity, "quantity", positive=True))
        object.__setattr__(self, "source_account_index", _index(self.source_account_index, "source_account_index"))
        object.__setattr__(self, "receiver_account_index", _index(self.receiver_account_index, "receiver_account_index"))
        if self.source_account_index == self.receiver_account_index:
            raise ContractError("source and receiver accounts must differ")
        if isinstance(self.api_key_index, bool) or not isinstance(self.api_key_index, int) or not 4 <= self.api_key_index <= 254:
            raise ContractError("api_key_index must be in 4..254")
        object.__setattr__(self, "freshness_seconds", _finite_positive(self.freshness_seconds, "freshness_seconds"))
        object.__setattr__(self, "request_timeout_seconds", _finite_positive(self.request_timeout_seconds, "request_timeout_seconds"))
        for field_name in ("source_limit_price", "receiver_worst_price"):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, _decimal(value, field_name, positive=True))
        if self.api_base_url.rstrip("/") != OFFICIAL_ROBINHOOD_API_URL:
            raise ContractError("api_base_url must be the exact Robinhood Chain Lighter endpoint")
        object.__setattr__(self, "api_base_url", OFFICIAL_ROBINHOOD_API_URL)
        if self.chain_id != OFFICIAL_ROBINHOOD_CHAIN_ID:
            raise ContractError("chain_id must be Robinhood signing domain 466324")
        if isinstance(self.auth_token_lifetime_seconds, bool) or not isinstance(self.auth_token_lifetime_seconds, int):
            raise ContractError("auth_token_lifetime_seconds must be an integer")
        if not 60 <= self.auth_token_lifetime_seconds <= 8 * 60 * 60:
            raise ContractError("auth_token_lifetime_seconds must be in 60..28800")

    def as_dict(self) -> dict[str, Any]:
        return {
            "venue": "robinhood-chain",
            "market_symbol": self.market_symbol,
            "direction": self.direction.value,
            "source_opening_side": self.direction.source_side,
            "receiver_opening_side": self.direction.receiver_side,
            "quantity": format(self.quantity, "f"),
            "source_account_index": self.source_account_index,
            "receiver_account_index": self.receiver_account_index,
            "api_key_index": self.api_key_index,
            "freshness_seconds": self.freshness_seconds,
            "request_timeout_seconds": self.request_timeout_seconds,
            "source_limit_price": None if self.source_limit_price is None else format(self.source_limit_price, "f"),
            "receiver_worst_price": None if self.receiver_worst_price is None else format(self.receiver_worst_price, "f"),
            "trade_bounds": "UNSET" if self.source_limit_price is None or self.receiver_worst_price is None else "OPERATOR_SELECTED",
        }


@dataclass(frozen=True, slots=True)
class ReadinessCheck:
    name: str
    status: str
    code: str
    message: str
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.status not in READINESS_STATUSES:
            raise ContractError(f"unsupported readiness status {self.status!r}")

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "code": self.code,
            "message": self.message,
            "details": sanitize(dict(self.details)),
        }


@dataclass(frozen=True, slots=True)
class ReadinessResult:
    outcome: str
    config: ReadinessConfig
    checks: tuple[ReadinessCheck, ...]
    market: Mapping[str, Any] | None = None
    book: Mapping[str, Any] | None = None
    accounts: Mapping[str, Mapping[str, Any] | None] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.outcome not in {"READY", "BLOCKED", "UNKNOWN"}:
            raise ContractError(f"unsupported readiness outcome {self.outcome!r}")

    def as_dict(self) -> dict[str, Any]:
        return sanitize(
            {
                "outcome": self.outcome,
                "execution": "READ_ONLY",
                "config": self.config.as_dict(),
                "checks": [item.as_dict() for item in self.checks],
                "market": None if self.market is None else dict(self.market),
                "book": None if self.book is None else dict(self.book),
                "accounts": {
                    str(role): None if value is None else dict(value)
                    for role, value in self.accounts.items()
                },
                "trade_plan": {
                    "order_prices": "UNSET" if self.config.source_limit_price is None or self.config.receiver_worst_price is None else "OPERATOR_SELECTED_ONLY",
                    "automatic_price_selection": False,
                    "execution_authorized": False,
                },
            }
        )


class ReadinessClient(Protocol):
    source_account_index: int
    receiver_account_index: int

    async def resolve_market(self, symbol: str) -> ReadinessMarketMetadata: ...

    async def order_book_snapshot(self, market_id: int) -> OrderBookSnapshot: ...

    async def account_snapshot(self, account_index: int, market_id: int) -> AccountSnapshot: ...


class ReadinessSecretProvider:
    """Hidden, local, in-memory key input for the explicit operator check."""

    def __init__(self, account_indices: Sequence[int], api_key_index: int) -> None:
        self._account_indices = frozenset(account_indices)
        self._api_key_index = api_key_index
        self._values: dict[int, str] = {}

    def private_key(self, account_index: int, api_key_index: int) -> str:
        if api_key_index != self._api_key_index or account_index not in self._account_indices:
            raise ContractError("secret request does not match readiness account/key index")
        if account_index not in self._values:
            self._values[account_index] = read_hidden_secret(
                f"Lighter API key for account {account_index} (hidden input): "
            )
        return self._values[account_index]

    def close(self) -> None:
        self._values.clear()


class ReadOnlyLighterSdkClient:
    """Official SDK adapter exposing only public/account/auth read methods.

    This class intentionally does not inherit ``LighterSdkClient``.  The
    execution adapter and its mutation methods therefore cannot be reached by
    accidental method dispatch from the readiness engine.
    """

    def __init__(
        self,
        config: ReadinessConfig,
        *,
        source_account_index: int,
        receiver_account_index: int,
        secrets: SecretProvider,
        signer_factory: Callable[..., Any] | None = None,
        api_factory: Callable[..., Any] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if source_account_index == receiver_account_index:
            raise ContractError("source and receiver accounts must differ")
        self.config = config
        self.source_account_index = _index(source_account_index, "source_account_index")
        self.receiver_account_index = _index(receiver_account_index, "receiver_account_index")
        self.secrets = secrets
        self._signer_factory = signer_factory
        self._api_factory = api_factory
        self._clock = clock
        self._api_client: Any | None = None
        self._apis: dict[type[Any], Any] = {}
        self._auth_tokens: dict[int, tuple[str, float]] = {}
        self._auth_signers: dict[int, Any] = {}
        self.sdk_version = REQUIRED_LIGHTER_SDK_VERSION

    @staticmethod
    def verify_sdk() -> None:
        try:
            version = importlib_metadata.version("lighter-sdk")
        except importlib_metadata.PackageNotFoundError as exc:
            raise MissingSdkError("lighter-sdk is not installed; install the optional hood-handoff dependency") from exc
        if version != REQUIRED_LIGHTER_SDK_VERSION:
            raise ContractError(f"lighter-sdk {REQUIRED_LIGHTER_SDK_VERSION} is required, found {version}")

    def _lighter(self) -> Any:
        self.verify_sdk()
        try:
            return importlib.import_module("lighter")
        except ImportError as exc:
            raise MissingSdkError("lighter-sdk import failed") from exc

    def _generated_api_client(self, module: Any | None = None) -> Any:
        if self._api_client is not None:
            return self._api_client
        module = module or self._lighter()
        configuration = module.Configuration(host=self.config.api_base_url, retries=None)
        self._api_client = module.ApiClient(configuration)
        return self._api_client

    def _api(self, module: Any, api_type: str) -> Any:
        api_class = getattr(module, api_type)
        if api_class not in self._apis:
            factory = self._api_factory or api_class
            self._apis[api_class] = factory(self._generated_api_client(module))
        return self._apis[api_class]

    def _auth_signer(self, account_index: int) -> Any:
        if account_index in self._auth_signers:
            return self._auth_signers[account_index]
        module = self._lighter()
        key_index = self.config.api_key_index
        private_key = self.secrets.private_key(account_index, key_index)
        if not isinstance(private_key, str) or not private_key:
            raise ContractError("secret provider returned an empty API key")
        factory = self._signer_factory or module.SignerClient
        # Pin the SDK's explicit no-op nonce manager.  The pinned SDK creates
        # a nonce-manager object in every SignerClient constructor, but the
        # NONE variant cannot fetch or advance a mutation nonce.  Readiness
        # only requests an auth token and never signs an order/transaction.
        kwargs: dict[str, Any] = {
            "url": self.config.api_base_url,
            "account_index": account_index,
            "api_private_keys": {key_index: private_key},
            "chain_id": self.config.chain_id,
        }
        nonce_types = getattr(getattr(module, "nonce_manager", None), "NonceManagerType", None)
        if nonce_types is not None and hasattr(nonce_types, "NONE"):
            kwargs["nonce_management_type"] = nonce_types.NONE
        signer = factory(**kwargs)
        self._auth_signers[account_index] = signer
        return signer

    async def _authorization(self, account_index: int) -> str:
        cached = self._auth_tokens.get(account_index)
        if cached is not None and self._clock() < cached[1]:
            return cached[0]
        signer = self._auth_signer(account_index)
        deadline = time.monotonic() + self.config.request_timeout_seconds
        result = await self._bounded(
            _await(
                signer.create_auth_token_with_expiry(
                    deadline=int(self.config.auth_token_lifetime_seconds),
                    api_key_index=self.config.api_key_index,
                )
            ),
            deadline,
            "auth token acquisition",
        )
        token = result[0] if isinstance(result, tuple) else result
        if isinstance(result, tuple) and len(result) > 1 and result[1]:
            raise RuntimeError("Lighter auth token request was rejected")
        if not isinstance(token, str) or not token:
            raise RuntimeError("Lighter auth token was not returned")
        self._auth_tokens[account_index] = (
            token,
            self._clock() + max(1.0, self.config.auth_token_lifetime_seconds - 1.0),
        )
        return token

    async def resolve_market(self, symbol: str) -> ReadinessMarketMetadata:
        requested = str(symbol).strip().upper()
        if not requested:
            raise ContractError("market symbol is required")
        module = self._lighter()
        api = self._api(module, "OrderApi")
        raw = await self._bounded(
            _await(api.order_book_details(filter="perp", _request_timeout=self.config.request_timeout_seconds)),
            time.monotonic() + self.config.request_timeout_seconds,
            "Robinhood orderBookDetails catalog read",
        )
        payload = _model_dict(raw)
        _require_success_code(payload, "orderBookDetails")
        observed = _select_perp_market_by_symbol(payload, requested)
        # The SDK catalog response is the observation.  No external evidence
        # timestamp is accepted or refreshed here.
        values = dict(observed)
        values.setdefault("market_type", "perp")
        values.setdefault("venue", "robinhood")
        values.setdefault("observed_at", self._clock())
        aliases = {
            "status": "status",
            "price_decimals": "supported_price_decimals",
            "size_decimals": "supported_size_decimals",
            "minimum_base_amount": "min_base_amount",
            "minimum_quote_amount": "min_quote_amount",
        }
        for target, source in aliases.items():
            if target not in values and source in observed:
                values[target] = observed[source]
        metadata = ReadinessMarketMetadata.from_mapping(values)
        return replace(
            metadata,
            margin_evidence=ReadinessMarketMarginEvidence.from_response(
                observed,
                market_id=metadata.market_id,
                symbol=metadata.symbol,
                observed_at=metadata.observed_at,
                source="lighter-sdk.order_book_details response",
                sdk_version=self.sdk_version,
            ),
        )

    async def order_book_snapshot(self, market_id: int) -> OrderBookSnapshot:
        market_id = _strict_integer(market_id, "market_id", minimum=0)
        module = self._lighter()
        api = self._api(module, "OrderApi")
        raw = await self._bounded(
            _await(
                api.order_book_orders(
                    market_id=market_id,
                    limit=ROBINHOOD_ORDER_BOOK_LIMIT,
                    _request_timeout=self.config.request_timeout_seconds,
                )
            ),
            time.monotonic() + self.config.request_timeout_seconds,
            "Robinhood orderBookOrders read",
        )
        payload = _model_dict(raw)
        _require_success_code(payload, "orderBookOrders")
        return OrderBookSnapshot.from_mapping(
            payload,
            market_id=market_id,
            symbol=self.config.market_symbol,
            observed_at=self._clock(),
            venue="robinhood",
        )

    async def account_snapshot(self, account_index: int, market_id: int) -> AccountSnapshot:
        account_index = _strict_integer(account_index, "account_index", minimum=0)
        market_id = _strict_integer(market_id, "market_id", minimum=0)
        module = self._lighter()
        api = self._api(module, "AccountApi")
        raw = await self._bounded(
            _await(
                api.account(
                    by="index",
                    value=str(account_index),
                    active_only=False,
                    _request_timeout=self.config.request_timeout_seconds,
                )
            ),
            time.monotonic() + self.config.request_timeout_seconds,
            "account read",
        )
        # Preserve the account/position/balance observation before auth and
        # active-orders reads can take time or prompt for a hidden key.
        account_observed_at = _timestamp(self._clock(), "account observation time")
        payload = _model_dict(raw)
        _require_success_code(payload, "account")
        account = _first_mapping(raw, "accounts")
        required = {"index", "l1_address", "status", "positions", "available_balance"}
        if not required.issubset(account):
            raise ContractError("Lighter account response is missing required identity/state fields")
        returned_index = _strict_integer(account["index"], "Lighter account index", minimum=0)
        if returned_index != account_index:
            raise ContractError("Lighter account response identity does not match requested account")
        identity = account["l1_address"]
        if not isinstance(identity, str) or not identity.strip():
            raise ContractError("Lighter account response has no exact account identity")
        positions = account["positions"]
        if not isinstance(positions, (list, tuple)):
            raise ContractError("Lighter account positions field is malformed")
        position_rows: list[Mapping[str, Any]] = []
        matching_positions: list[Mapping[str, Any]] = []
        for item in positions:
            candidate = _model_dict(item)
            position_rows.append(candidate)
            if "market_id" not in candidate:
                raise ContractError("Lighter account position lacks market identity")
            candidate_market = _strict_integer(candidate["market_id"], "Lighter account position market_id", minimum=0)
            if candidate_market == market_id:
                if "position" not in candidate or "sign" not in candidate:
                    raise ContractError("Lighter account position lacks required sign/position fields")
                matching_positions.append(candidate)
        if len(matching_positions) > 1:
            raise ContractError("Lighter account positions contain duplicate selected market records")
        selected_position = matching_positions[0] if matching_positions else None
        position = selected_position or {"position": "0", "sign": 1}
        # Account state is public by index; auth is separately required for the
        # active-orders read.  This keeps auth permission distinct from trade
        # dispatch and never treats an auth failure as an empty order list.
        active_orders = await self._active_orders(account_index, market_id)
        active_orders_observed_at = _timestamp(self._clock(), "active orders observation time")
        margin_evidence = AccountMarginEvidence.from_response(
            account_index=account_index,
            market_id=market_id,
            source_identity=identity.strip(),
            observed_at=account_observed_at,
            selected_position=selected_position,
            account=account,
            position_rows=position_rows,
            source="lighter-sdk.account response",
            sdk_version=self.sdk_version,
        )
        status = account["status"]
        ready = status in (0, 1) or str(status).strip().lower() in {"active", "online"}
        return AccountSnapshot.from_mapping(
            {
                "account_index": account_index,
                "market_id": market_id,
                "position": position.get("position", "0"),
                "sign": position.get("sign", 1),
                "active_orders": active_orders,
                "observed_at": min(account_observed_at, active_orders_observed_at),
                "authorized": True,
                "ready": ready,
                "margin_available": account.get("available_balance"),
                "available_balance": account.get("available_balance"),
                "margin_required": account.get("cross_initial_margin_requirement"),
                "fee_rate": None,
                "source_identity": identity.strip(),
                # No account/market field here proves an incremental opening
                # margin delta.  Readiness reports this as UNKNOWN.
                "incremental_margin_required": None,
                "incremental_margin_evidence": "",
                "margin_evidence": margin_evidence,
            }
        )

    async def _active_orders(self, account_index: int, market_id: int) -> tuple[OrderSnapshot, ...]:
        account_index = _strict_integer(account_index, "account_index", minimum=0)
        market_id = _strict_integer(market_id, "market_id", minimum=0)
        module = self._lighter()
        api = self._api(module, "OrderApi")
        token = await self._authorization(account_index)
        raw = await self._bounded(
            _await(
                api.account_active_orders(
                    authorization=token,
                    account_index=account_index,
                    market_id=market_id,
                    _request_timeout=self.config.request_timeout_seconds,
                )
            ),
            time.monotonic() + self.config.request_timeout_seconds,
            "active orders read",
        )
        payload = _model_dict(raw)
        _require_success_code(payload, "accountActiveOrders")
        values = payload.get("orders")
        if not isinstance(values, (list, tuple)):
            raise ContractError("accountActiveOrders response lacks an orders list")
        result: list[OrderSnapshot] = []
        for item in values:
            parsed = OrderSnapshot.from_mapping(
                _order_snapshot_mapping(item, observed_at=self._clock())
            )
            if parsed.account_index != account_index or parsed.market_id != market_id:
                raise ContractError("active order response identity does not match requested account/market")
            result.append(parsed)
        return tuple(result)

    async def _bounded(self, awaitable: Any, deadline: float, label: str) -> Any:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"{label} exceeded configured request deadline")
        try:
            return await asyncio.wait_for(awaitable, timeout=remaining)
        except asyncio.TimeoutError as exc:
            raise TimeoutError(f"{label} exceeded configured request deadline") from exc

    async def aclose(self) -> None:
        """Close generated API resources and clear the in-memory auth cache."""

        resources: list[Any] = list(self._apis.values())
        if self._api_client is not None:
            resources.append(self._api_client)
        self._auth_tokens.clear()
        for resource in resources:
            close = getattr(resource, "close", None)
            if not callable(close):
                continue
            try:
                result = close()
                if inspect.isawaitable(result):
                    await result
            except Exception:
                # Cleanup must never print a secret-bearing SDK exception.
                continue
        for signer in tuple(self._auth_signers.values()):
            close = getattr(signer, "close", None)
            if callable(close):
                try:
                    result = close()
                    if inspect.isawaitable(result):
                        await result
                except Exception:
                    pass
            api_client = getattr(signer, "api_client", None)
            close_api = getattr(api_client, "close", None)
            if callable(close_api):
                try:
                    result = close_api()
                    if inspect.isawaitable(result):
                        await result
                except Exception:
                    pass
        self._auth_signers.clear()

    async def close(self) -> None:
        await self.aclose()


def _check(
    checks: list[ReadinessCheck],
    name: str,
    status: str,
    code: str,
    message: str,
    **details: Any,
) -> None:
    checks.append(ReadinessCheck(name, status, code, message, details))


def _read_failure_code(exc: BaseException) -> str:
    """Classify a read failure without exposing SDK text or secret material."""

    text = str(exc).lower()
    if "duplicate selected market" in text:
        return "ACCOUNT_POSITION_CONFLICT"
    if "identity" in text or "market_id" in text or "account index" in text or ("account" in text and "match" in text):
        return "ACCOUNT_IDENTITY_CONFLICT"
    if "auth" in text or "token" in text or "private key" in text:
        return "ACCOUNT_READ_AUTH_FAILED"
    if "active order" in text:
        return "ACTIVE_ORDERS_READ_FAILED"
    if "timeout" in text:
        return "ACCOUNT_READ_TIMEOUT"
    return "ACCOUNT_READ_FAILED"


def _account_details(snapshot: AccountSnapshot) -> dict[str, Any]:
    margin_evidence = snapshot.margin_evidence or AccountMarginEvidence.unavailable(
        account_index=snapshot.account_index,
        market_id=snapshot.market_id,
        source_identity=snapshot.source_identity,
        observed_at=snapshot.observed_at,
    )
    return {
        "account_index": snapshot.account_index,
        "market_id": snapshot.market_id,
        "source_identity": snapshot.source_identity,
        "signed_position": format(snapshot.signed_position, "f"),
        "active_orders": [
            {
                "order_id": order.order_id,
                "status": order.status,
                "side": order.side,
                "remaining_quantity": format(order.remaining_quantity, "f"),
            }
            for order in snapshot.active_orders
        ],
        "observed_at": snapshot.observed_at,
        "authorized": snapshot.authorized,
        "ready": snapshot.ready,
        "margin_available": None if snapshot.margin_available is None else format(snapshot.margin_available, "f"),
        "margin_required": None if snapshot.margin_required is None else format(snapshot.margin_required, "f"),
        "incremental_margin_required": None if snapshot.incremental_margin_required is None else format(snapshot.incremental_margin_required, "f"),
        "incremental_margin_evidence": snapshot.incremental_margin_evidence or None,
        "margin_evidence": margin_evidence.as_dict(),
    }


def _market_margin_details(metadata: ReadinessMarketMetadata | MarketMetadata) -> dict[str, Any]:
    evidence = getattr(metadata, "margin_evidence", None)
    if isinstance(evidence, ReadinessMarketMarginEvidence):
        return evidence.as_dict()
    return ReadinessMarketMarginEvidence(
        market_id=metadata.market_id,
        symbol=metadata.symbol,
        observed_at=metadata.observed_at,
        default_initial_margin_fraction=None,
        minimum_initial_margin_fraction=None,
        source="not returned",
    ).as_dict()


async def run_readiness(
    config: ReadinessConfig,
    client: ReadinessClient,
    *,
    now: float | None = None,
    clock: Callable[[], float] | None = None,
) -> ReadinessResult:
    """Run one bounded, non-mutating diagnostic against a read-only client."""

    if getattr(client, "source_account_index", None) != config.source_account_index:
        raise ContractError("readiness client source account does not match config")
    if getattr(client, "receiver_account_index", None) != config.receiver_account_index:
        raise ContractError("readiness client receiver account does not match config")
    checks: list[ReadinessCheck] = []
    if now is None:
        decision_clock = clock or time.time
    else:
        fixed_now = _timestamp(now, "now")
        decision_clock = lambda: fixed_now

    def current_decision_time() -> float:
        return _timestamp(decision_clock(), "decision time")

    metadata: ReadinessMarketMetadata | MarketMetadata | None = None
    book: OrderBookSnapshot | None = None
    snapshots: dict[str, AccountSnapshot | None] = {"source": None, "receiver": None}

    try:
        metadata = await client.resolve_market(config.market_symbol)
        if metadata.symbol.upper() != config.market_symbol:
            _check(checks, "market_identity", "BLOCKED", "WRONG_SYMBOL", "current catalog resolved a different symbol", observed_symbol=metadata.symbol)
        elif metadata.market_type.lower() != "perp":
            _check(checks, "market_identity", "BLOCKED", "WRONG_MARKET_TYPE", "current catalog resolved a non-perpetual market")
        elif metadata.venue and metadata.venue.lower() not in {"robinhood", "robinhood-chain"}:
            _check(checks, "market_identity", "BLOCKED", "WRONG_VENUE", "current catalog resolved the wrong venue", venue=metadata.venue)
        else:
            _check(checks, "market_identity", "PASS", "MARKET_IDENTITY_OK", "current Robinhood perpetual identity resolved", market_id=metadata.market_id, symbol=metadata.symbol, venue=metadata.venue or "robinhood")
        if metadata.status.lower() in {"active", "open", "online", "listed"}:
            _check(checks, "market_status", "PASS", "MARKET_ACTIVE", "current perpetual is active", market_status=metadata.status)
        else:
            _check(checks, "market_status", "BLOCKED", "MARKET_INACTIVE", "current perpetual is not active", market_status=metadata.status)

        margin_evidence = getattr(metadata, "margin_evidence", None)
        if isinstance(margin_evidence, ReadinessMarketMarginEvidence):
            if margin_evidence.invalid_fields:
                _check(
                    checks,
                    "market_margin_evidence",
                    "UNKNOWN",
                    "MARKET_MARGIN_EVIDENCE_INVALID",
                    "current market margin defaults contain invalid or conflicting fields",
                    invalid_fields=list(margin_evidence.invalid_fields),
                )
            elif margin_evidence.status == "INCOMPLETE":
                _check(
                    checks,
                    "market_margin_evidence",
                    "UNKNOWN",
                    "MARKET_MARGIN_EVIDENCE_INCOMPLETE",
                    "current market margin defaults are incomplete; no units or defaults are inferred",
                )
            else:
                _check(
                    checks,
                    "market_margin_evidence",
                    "PASS",
                    "MARKET_MARGIN_EVIDENCE_OBSERVED",
                    "current market margin defaults are retained for diagnosis only",
                )
    except Exception as exc:
        _check(checks, "market_identity", "UNKNOWN", "MARKET_READ_FAILED", "current market identity could not be established", reason=sanitize_exception(exc))
        _check(checks, "market_status", "UNKNOWN", "MARKET_READ_FAILED", "current market status could not be established", reason=sanitize_exception(exc))
        _check(checks, "market_metadata_freshness", "UNKNOWN", "MARKET_READ_FAILED", "current market metadata freshness is unavailable", reason=sanitize_exception(exc))

    if metadata is None:
        for name in ("quantity_grid", "quantity_base_minimum", "source_quote_minimum", "receiver_quote_minimum"):
            _check(checks, name, "UNKNOWN", "MARKET_CONSTRAINTS_UNKNOWN", "market precision/minimums are unavailable")
    else:
        try:
            quantity_int = decimal_to_integer(config.quantity, metadata.size_decimals, "quantity")
            _check(checks, "quantity_grid", "PASS", "QUANTITY_ON_SIZE_GRID", "quantity is on the current size grid", quantity_int=quantity_int, size_decimals=metadata.size_decimals)
        except ContractError as exc:
            _check(checks, "quantity_grid", "BLOCKED", "QUANTITY_OFF_GRID", str(exc), size_decimals=metadata.size_decimals)
        if config.quantity >= metadata.minimum_base_amount:
            _check(checks, "quantity_base_minimum", "PASS", "BASE_MINIMUM_OK", "quantity meets the current base minimum", minimum_base_amount=format(metadata.minimum_base_amount, "f"))
        else:
            _check(checks, "quantity_base_minimum", "BLOCKED", "BASE_MINIMUM_FAILED", "quantity is below the current base minimum", quantity=format(config.quantity, "f"), minimum_base_amount=format(metadata.minimum_base_amount, "f"))
        for role, bound_name, bound in (
            ("source", "source_quote_minimum", config.source_limit_price),
            ("receiver", "receiver_quote_minimum", config.receiver_worst_price),
        ):
            if bound is None:
                _check(checks, bound_name, "UNSET", "TRADE_BOUND_UNSET", f"{role} operator-selected price bound is UNSET; no live price was selected")
                continue
            try:
                price_int = decimal_to_integer(bound, metadata.price_decimals, f"{role}_price")
            except ContractError as exc:
                _check(checks, bound_name, "BLOCKED", "PRICE_OFF_GRID", str(exc), price_decimals=metadata.price_decimals)
                continue
            notional = config.quantity * bound
            if notional < metadata.minimum_quote_amount:
                _check(checks, bound_name, "BLOCKED", "QUOTE_MINIMUM_FAILED", f"{role} operator-selected bound produces notional below the current quote minimum", price_int=price_int, notional=format(notional, "f"), minimum_quote_amount=format(metadata.minimum_quote_amount, "f"))
            else:
                _check(checks, bound_name, "PASS", "QUOTE_MINIMUM_OK", f"{role} operator-selected bound meets the current quote minimum", price_int=price_int, notional=format(notional, "f"), minimum_quote_amount=format(metadata.minimum_quote_amount, "f"))

    if metadata is None:
        _check(checks, "public_book", "UNKNOWN", "BOOK_IDENTITY_UNKNOWN", "public book cannot be bound without the current market identity")
    else:
        try:
            book = await client.order_book_snapshot(metadata.market_id)
        except Exception as exc:
            _check(checks, "public_book", "UNKNOWN", "BOOK_READ_FAILED", "public book could not be read", reason=sanitize_exception(exc))

    for role, account_index in (("source", config.source_account_index), ("receiver", config.receiver_account_index)):
        prefix = f"{role}_account"
        if metadata is None:
            for suffix, message in (
                ("authorization", f"{role} read authorization is unavailable without a market identity"),
                ("identity", f"{role} account identity is unavailable without a market identity"),
                ("position", f"{role} selected-market position is unavailable without a market identity"),
                ("active_orders", f"{role} active orders are unavailable without a market identity"),
                ("margin", f"{role} margin readiness is unavailable without a market identity"),
            ):
                _check(checks, f"{prefix}_{suffix}", "UNKNOWN", "ACCOUNT_READ_SKIPPED", message, account_index=account_index)
            continue
        try:
            snapshot = await client.account_snapshot(account_index, metadata.market_id)
            snapshots[role] = snapshot
        except Exception as exc:
            reason = sanitize_exception(exc)
            failure_code = _read_failure_code(exc)
            failure_status = "BLOCKED" if failure_code in {"ACCOUNT_IDENTITY_CONFLICT", "ACCOUNT_POSITION_CONFLICT"} else "UNKNOWN"
            for suffix, message in (
                ("authorization", f"{role} read authorization is not established"),
                ("identity", f"{role} account identity is unavailable"),
                ("position", f"{role} selected-market position is unavailable"),
                ("active_orders", f"{role} active orders are unavailable"),
                ("margin", f"{role} margin readiness is unavailable"),
            ):
                _check(checks, f"{prefix}_{suffix}", failure_status, failure_code, message, reason=reason, account_index=account_index)
            continue
        if snapshot.account_index != account_index:
            _check(checks, f"{prefix}_identity", "BLOCKED", "ACCOUNT_IDENTITY_CONFLICT", f"{role} read returned a different account index", requested_account_index=account_index, returned_account_index=snapshot.account_index)
        elif snapshot.market_id != metadata.market_id:
            _check(checks, f"{prefix}_identity", "BLOCKED", "ACCOUNT_MARKET_CONFLICT", f"{role} read returned a different market", requested_market_id=metadata.market_id, returned_market_id=snapshot.market_id)
        elif not snapshot.source_identity:
            _check(checks, f"{prefix}_identity", "UNKNOWN", "ACCOUNT_IDENTITY_MISSING", f"{role} account identity is missing", account_index=account_index)
        else:
            _check(checks, f"{prefix}_identity", "PASS", "ACCOUNT_IDENTITY_OK", f"{role} account identity was returned", account_index=account_index, source_identity=snapshot.source_identity)
        if snapshot.authorized:
            _check(checks, f"{prefix}_authorization", "PASS", "ACCOUNT_READ_AUTHORIZED", f"{role} account read authorization succeeded", account_index=account_index)
        else:
            _check(checks, f"{prefix}_authorization", "UNKNOWN", "ACCOUNT_READ_UNAUTHORIZED", f"{role} account read authorization is unproven", account_index=account_index)
        if not snapshot.ready:
            _check(checks, f"{prefix}_status", "BLOCKED", "ACCOUNT_INACTIVE", f"{role} account is not active/online")
        else:
            _check(checks, f"{prefix}_status", "PASS", "ACCOUNT_ACTIVE", f"{role} account is active/online")
        margin_evidence = snapshot.margin_evidence
        if margin_evidence is not None and margin_evidence.selected_position_present is False:
            _check(
                checks,
                f"{prefix}_position",
                "UNKNOWN",
                "POSITION_ROW_ABSENT",
                f"{role} account response has no selected-market position row; flatness is not independently evidenced",
                signed_position="0",
            )
        elif snapshot.signed_position == 0:
            _check(checks, f"{prefix}_position", "PASS", "POSITION_FLAT", f"{role} selected-market position is flat for paired opening", signed_position="0")
        else:
            _check(checks, f"{prefix}_position", "BLOCKED", "POSITION_NOT_FLAT", f"{role} selected-market position is non-flat; paired opening requires flat accounts", signed_position=format(snapshot.signed_position, "f"))
        if snapshot.active_orders:
            _check(checks, f"{prefix}_active_orders", "BLOCKED", "ACTIVE_ORDERS_PRESENT", f"{role} has active orders on the selected market", count=len(snapshot.active_orders))
        else:
            _check(checks, f"{prefix}_active_orders", "PASS", "NO_ACTIVE_ORDERS", f"{role} has no active selected-market orders")
        if margin_evidence is not None:
            if margin_evidence.invalid_fields:
                _check(
                    checks,
                    f"{prefix}_margin_evidence",
                    "UNKNOWN",
                    "MARGIN_EVIDENCE_INVALID",
                    f"{role} returned margin/order evidence contains invalid or conflicting fields",
                    invalid_fields=list(margin_evidence.invalid_fields),
                )
            elif margin_evidence.selected_position_present is False:
                _check(
                    checks,
                    f"{prefix}_margin_evidence",
                    "UNKNOWN",
                    "MARGIN_EVIDENCE_POSITION_ROW_ABSENT",
                    f"{role} margin settings are unavailable because the selected-market position row is absent",
                )
            elif margin_evidence.incomplete:
                _check(
                    checks,
                    f"{prefix}_margin_evidence",
                    "UNKNOWN",
                    "MARGIN_EVIDENCE_INCOMPLETE",
                    f"{role} returned margin/order evidence is incomplete; no units or defaults are inferred",
                )
            else:
                _check(
                    checks,
                    f"{prefix}_margin_evidence",
                    "PASS",
                    "MARGIN_EVIDENCE_OBSERVED",
                    f"{role} returned margin/order evidence is retained for diagnosis only",
                )
        if snapshot.margin_available is None or snapshot.margin_required is None:
            missing = []
            if snapshot.margin_available is None:
                missing.append("available_balance")
            if snapshot.margin_required is None:
                missing.append("cross_initial_margin_requirement")
            _check(checks, f"{prefix}_margin", "UNKNOWN", "MARGIN_EVIDENCE_REQUIRED", f"{role} current margin fields are incomplete; incremental opening margin is UNKNOWN", missing_fields=missing)
        elif snapshot.margin_required > snapshot.margin_available:
            _check(checks, f"{prefix}_margin", "BLOCKED", "CURRENT_MARGIN_INSUFFICIENT", f"{role} current cross margin requirement exceeds available balance", margin_available=format(snapshot.margin_available, "f"), margin_required=format(snapshot.margin_required, "f"))
        elif snapshot.incremental_margin_required is None or not snapshot.incremental_margin_evidence:
            _check(checks, f"{prefix}_margin", "UNKNOWN", "MARGIN_EVIDENCE_REQUIRED", f"{role} incremental opening margin proof is unavailable; current balance alone is insufficient", margin_available=format(snapshot.margin_available, "f"), current_margin_required=format(snapshot.margin_required, "f"), missing_fields=["incremental_margin_required", "incremental_margin_evidence"])
        elif snapshot.incremental_margin_required > snapshot.margin_available:
            _check(checks, f"{prefix}_margin", "BLOCKED", "INCREMENTAL_MARGIN_INSUFFICIENT", f"{role} proven incremental opening margin exceeds available balance", margin_available=format(snapshot.margin_available, "f"), incremental_margin_required=format(snapshot.incremental_margin_required, "f"))
        else:
            _check(checks, f"{prefix}_margin", "PASS", "INCREMENTAL_MARGIN_PROVEN", f"{role} incremental opening margin is explicitly evidenced and available", margin_available=format(snapshot.margin_available, "f"), incremental_margin_required=format(snapshot.incremental_margin_required, "f"), evidence=snapshot.incremental_margin_evidence)

    # Freshness is evaluated against one final decision time after every
    # required read.  This keeps response observations coherent during normal
    # latency and lets an earlier market/account snapshot age while later
    # account reads or local hidden-key input complete.
    final_now = current_decision_time()
    if metadata is not None:
        metadata_age = final_now - metadata.observed_at
        if metadata.observed_at > final_now or metadata_age > config.freshness_seconds:
            _check(checks, "market_metadata_freshness", "BLOCKED", "MARKET_METADATA_STALE", "current market metadata is outside the explicit freshness bound", observed_at=metadata.observed_at, age_seconds=metadata_age, freshness_seconds=config.freshness_seconds)
        else:
            _check(checks, "market_metadata_freshness", "PASS", "MARKET_METADATA_FRESH", "current market metadata is fresh", observed_at=metadata.observed_at, age_seconds=metadata_age)
    if book is not None and metadata is not None:
        age = final_now - book.observed_at
        try:
            if book.market_id != metadata.market_id or book.symbol.upper() != metadata.symbol.upper() or book.market_type.lower() != "perp" or book.venue.lower() not in {"robinhood", "robinhood-chain"}:
                _check(checks, "public_book", "BLOCKED", "BOOK_IDENTITY_CONFLICT", "public book identity conflicts with the current market", book_market_id=book.market_id, book_symbol=book.symbol)
            elif book.observed_at > final_now or age > config.freshness_seconds:
                _check(checks, "public_book", "BLOCKED", "BOOK_STALE", "public book is outside the explicit freshness bound", observed_at=book.observed_at, age_seconds=age, freshness_seconds=config.freshness_seconds)
            elif not book.bids or not book.asks:
                _check(checks, "public_book", "UNKNOWN", "BOOK_SIDE_MISSING", "public book lacks a bid or ask side")
            else:
                best_bid = book.bids[0].price
                best_ask = book.asks[0].price
                _check(checks, "public_book", "PASS", "BOOK_FRESH", "bounded public book read is fresh; bid/ask are observations only", observed_at=book.observed_at, age_seconds=age, best_bid=format(best_bid, "f"), best_ask=format(best_ask, "f"), observed_source_notional=format(config.quantity * best_bid, "f"), observed_receiver_notional=format(config.quantity * best_ask, "f"))
        except Exception as exc:
            _check(checks, "public_book", "UNKNOWN", "BOOK_READ_FAILED", "public book could not be interpreted", reason=sanitize_exception(exc))
    for role, snapshot in snapshots.items():
        if snapshot is None:
            continue
        prefix = f"{role}_account"
        age = final_now - snapshot.observed_at
        if snapshot.observed_at > final_now or age > config.freshness_seconds:
            _check(checks, f"{prefix}_freshness", "BLOCKED", "ACCOUNT_STATE_STALE", f"{role} account state is outside the explicit freshness bound", observed_at=snapshot.observed_at, age_seconds=age, freshness_seconds=config.freshness_seconds)
        else:
            _check(checks, f"{prefix}_freshness", "PASS", "ACCOUNT_STATE_FRESH", f"{role} account state is fresh", observed_at=snapshot.observed_at, age_seconds=age)

    source = snapshots["source"]
    receiver = snapshots["receiver"]
    if source is None or receiver is None:
        _check(checks, "account_identity_pair", "UNKNOWN", "ACCOUNT_PAIR_UNKNOWN", "both account identities are required to establish distinct control")
    elif source.source_identity == receiver.source_identity:
        _check(checks, "account_identity_pair", "BLOCKED", "ACCOUNT_IDENTITY_CONFLICT", "source and receiver resolved to the same account identity")
    else:
        _check(checks, "account_identity_pair", "PASS", "ACCOUNT_IDENTITIES_DISTINCT", "source and receiver account identities are distinct")

    statuses = {item.status for item in checks}
    if "BLOCKED" in statuses:
        outcome = "BLOCKED"
    elif statuses.intersection({"UNKNOWN", "UNSET"}):
        outcome = "UNKNOWN"
    else:
        outcome = "READY"
    return ReadinessResult(
        outcome=outcome,
        config=config,
        checks=tuple(checks),
        market=None if metadata is None else {
            "market_id": metadata.market_id,
            "symbol": metadata.symbol,
            "market_type": metadata.market_type,
            "venue": metadata.venue or "robinhood",
            "status": metadata.status,
            "price_decimals": metadata.price_decimals,
            "size_decimals": metadata.size_decimals,
            "minimum_base_amount": format(metadata.minimum_base_amount, "f"),
            "minimum_quote_amount": format(metadata.minimum_quote_amount, "f"),
            "observed_at": metadata.observed_at,
            "margin_evidence": _market_margin_details(metadata),
        },
        book=None if book is None else book.as_dict(),
        accounts={
            role: None if snapshot is None else _account_details(snapshot)
            for role, snapshot in snapshots.items()
        },
    )


__all__ = [
    "ReadinessCheck",
    "ReadinessConfig",
    "ReadinessMarketMarginEvidence",
    "ReadinessMarketMetadata",
    "ReadinessResult",
    "ReadinessSecretProvider",
    "ReadOnlyLighterSdkClient",
    "run_readiness",
]
