"""Explicit, opt-in Lighter SDK/HTTP adapter for HCR-1.

No SDK module is imported at package import time.  Mutation calls use the
SDK's ``sign_*`` methods and one explicit ``sendTx`` form request.  The
combined ``create_order``/``cancel_order`` helpers are intentionally not used,
because their retry and response boundaries are not suitable for one-attempt
close/reopen semantics.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import importlib
from importlib import metadata as importlib_metadata
import inspect
import json
import math
import re
import time
from typing import Any, Callable, Mapping, Protocol

from .contracts import (
    AccountSnapshot,
    ContractError,
    HandoffConfig,
    HistoryPage,
    MarketMetadata,
    MutationReceipt,
    OrderPlan,
    OrderSnapshot,
    TradeReceipt,
    OFFICIAL_MAINNET_CHAIN_ID,
    _nonnegative,
)
from .journal import sanitize_exception


REQUIRED_LIGHTER_SDK_VERSION = "1.1.2"
# The official orderBookOrders endpoint requires a bounded page size.  This is
# a transport/read bound only; slice quantity remains operator/depth-driven.
ROBINHOOD_ORDER_BOOK_LIMIT = 250


class SecretProvider(Protocol):
    def private_key(self, account_index: int, api_key_index: int) -> str: ...


class MissingSdkError(RuntimeError):
    pass


class SdkVersionError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class _CachedToken:
    value: str
    expires_at: float


@dataclass(frozen=True, slots=True)
class StaticSecretProvider:
    """A caller-owned provider; the key is never placed in HCR config/journal."""

    values: Mapping[int, str]

    def private_key(self, account_index: int, api_key_index: int) -> str:
        value = self.values.get(account_index)
        if not isinstance(value, str) or not value:
            raise ContractError(f"private key for account {account_index} is unavailable")
        return value


async def _await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def _close_resource(value: Any) -> bool:
    """Best-effort close for SDK and injected synthetic resources."""

    if value is None:
        return True
    for name in ("aclose", "close"):
        method = getattr(value, name, None)
        if not callable(method):
            continue
        try:
            result = method()
            if inspect.isawaitable(result):
                await result
        except BaseException:
            # Teardown must not replace an already-observed execution result or
            # turn an interrupted attempt into a false terminal claim.
            return False
        return True
    return False


def _model_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if hasattr(value, "to_dict"):
        converted = value.to_dict()
        if isinstance(converted, Mapping):
            return dict(converted)
    result: dict[str, Any] = {}
    for name in dir(value):
        if name.startswith("_"):
            continue
        try:
            item = getattr(value, name)
        except Exception:
            continue
        if callable(item):
            continue
        if isinstance(item, (str, int, float, bool, type(None), list, tuple, dict)):
            result[name] = item
    return result


def _first_mapping(value: Any, *keys: str) -> dict[str, Any]:
    raw = _model_dict(value)
    for key in keys:
        nested = raw.get(key)
        if isinstance(nested, list) and nested:
            return _model_dict(nested[0])
        if isinstance(nested, Mapping):
            return dict(nested)
    return raw


def _order_snapshot_mapping(value: Any, *, observed_at: float) -> dict[str, Any]:
    """Normalize one official Order model without weakening its contract.

    The pinned SDK exposes ``is_ask`` as the authoritative side and labels its
    ``side`` field as legacy.  Its generated ``from_dict`` also supplies a
    default ``buy`` side when the wire response omits that legacy field.  The
    authoritative boolean is therefore required at this SDK boundary; a
    side-only response remains incomplete rather than being guessed.
    """

    mapped = _model_dict(value)
    if "is_ask" not in mapped:
        if "side" not in mapped:
            raise ContractError("order response lacks required is_ask/side")
        raise ContractError("order response lacks required is_ask")
    is_ask = mapped["is_ask"]
    if not isinstance(is_ask, bool):
        raise ContractError("order response is_ask must be bool")
    required_fields = {
        "account_index": ("account_index", "owner_account_index"),
        "market_id": ("market_id", "market_index"),
        "order_id": ("order_id",),
        "client_order_index": ("client_order_index",),
        "status": ("status",),
        "type": ("type", "order_type"),
        "time_in_force": ("time_in_force",),
        "reduce_only": ("reduce_only",),
        "initial_base_amount": ("initial_quantity", "initial_base_amount", "base_amount"),
        "remaining_base_amount": ("remaining_quantity", "remaining_base_amount"),
        "filled_base_amount": ("filled_quantity", "filled_base_amount"),
        "price": ("price", "base_price"),
    }
    for field, aliases in required_fields.items():
        if not any(alias in mapped and mapped[alias] is not None for alias in aliases):
            raise ContractError(f"order response lacks required {field}")
    mapped["side"] = "SELL" if is_ask else "BUY"
    mapped["observed_at"] = observed_at
    return mapped


def _trade_timestamp_seconds(value: Any) -> float:
    """Convert the documented integer millisecond timestamp to seconds."""

    if isinstance(value, bool) or not isinstance(value, int):
        raise ContractError("trade timestamp must be an integer millisecond value")
    if value < 0:
        raise ContractError("trade timestamp must be non-negative milliseconds")
    try:
        seconds = value / 1_000.0
    except OverflowError as exc:
        raise ContractError("trade timestamp must convert to finite seconds") from exc
    if not math.isfinite(seconds):
        raise ContractError("trade timestamp must convert to finite seconds")
    return seconds


class PlainAioHttp:
    """One-request HTTP helper; deliberately does not import aiohttp-retry."""

    def __init__(self, base_url: str, *, timeout_seconds: float) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = float(timeout_seconds)

    async def aclose(self) -> None:
        """Match the adapter lifecycle; request sessions are already scoped."""

        return None

    async def close(self) -> None:
        await self.aclose()

    async def get(self, path: str, *, params: Mapping[str, Any], authorization: str) -> dict[str, Any]:
        try:
            import aiohttp
        except ImportError as exc:
            raise MissingSdkError("aiohttp is required for explicit accountOrders transport") from exc
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                f"{self.base_url}/{path.lstrip('/')}",
                params=dict(params),
                headers={"Authorization": authorization},
                allow_redirects=False,
            ) as response:
                raw = await response.text()
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(f"Lighter response was not JSON (HTTP {response.status})") from exc
                if response.status < 200 or response.status >= 300:
                    code = payload.get("code") if isinstance(payload, Mapping) else None
                    raise RuntimeError(f"Lighter read rejected HTTP {response.status}, code={code}")
                if not isinstance(payload, Mapping):
                    raise RuntimeError("Lighter read response is not an object")
                return dict(payload)

    async def post_form(self, path: str, *, form: Mapping[str, Any]) -> dict[str, Any]:
        """Send exactly one mutation request without aiohttp-retry or SDK REST."""

        try:
            import aiohttp
        except ImportError as exc:
            raise MissingSdkError("aiohttp is required for the explicit mutation transport") from exc
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{self.base_url}/{path.lstrip('/')}",
                data=dict(form),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                allow_redirects=False,
            ) as response:
                raw = await response.text()
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(f"Lighter mutation response was not JSON (HTTP {response.status})") from exc
                if not isinstance(payload, Mapping):
                    raise RuntimeError("Lighter mutation response is not an object")
                return {**dict(payload), "_http_status": response.status}


class LighterSdkClient:
    """Production adapter, constructed only after explicit operator opt-in."""

    source_account_index: int
    receiver_account_index: int

    def __init__(
        self,
        config: HandoffConfig,
        *,
        source_account_index: int,
        receiver_account_index: int,
        secrets: SecretProvider,
        market_evidence: Mapping[str, Any],
        signer_factory: Callable[..., Any] | None = None,
        api_factory: Callable[..., Any] | None = None,
        http_factory: Callable[..., Any] | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if config.api_base_url is None:
            raise ContractError("api_base_url is required for the explicit live adapter")
        if config.api_key_index is None:
            raise ContractError("api_key_index is required for the explicit live adapter")
        if source_account_index == receiver_account_index:
            raise ContractError("source and receiver accounts must differ")
        self.config = config
        self.source_account_index = source_account_index
        self.receiver_account_index = receiver_account_index
        self.secrets = secrets
        self.market_evidence = dict(market_evidence)
        self._signer_factory = signer_factory
        self._api_factory = api_factory
        self._clock = clock
        self._signers: dict[int, Any] = {}
        self._apis: dict[int, Any] = {}
        self._tokens: dict[int, _CachedToken] = {}
        self._api_client: Any | None = None
        self._pending_mutation_deadline: float | None = None
        self._closed = False
        self.sdk_version = REQUIRED_LIGHTER_SDK_VERSION
        self._http = (http_factory or PlainAioHttp)(config.api_base_url, timeout_seconds=config.request_timeout_seconds)

    def set_mutation_deadline(self, deadline: float) -> None:
        """Bind the next cancellation attempt to the engine's evidence barrier."""

        try:
            value = float(deadline)
        except (TypeError, ValueError) as exc:
            raise ContractError("mutation deadline must be finite") from exc
        if value != value or value in {float("inf"), float("-inf")} or value <= 0:
            raise ContractError("mutation deadline must be finite and positive")
        self._pending_mutation_deadline = value

    @staticmethod
    def verify_sdk() -> None:
        try:
            version = importlib_metadata.version("lighter-sdk")
        except importlib_metadata.PackageNotFoundError as exc:
            raise MissingSdkError("lighter-sdk is not installed; install the optional hood-handoff dependency") from exc
        if version != REQUIRED_LIGHTER_SDK_VERSION:
            raise SdkVersionError(
                f"lighter-sdk {REQUIRED_LIGHTER_SDK_VERSION} is required, found {version}"
            )

    def _lighter(self) -> Any:
        self.verify_sdk()
        try:
            return importlib.import_module("lighter")
        except ImportError as exc:
            raise MissingSdkError("lighter-sdk import failed") from exc

    def _signer(self, account_index: int) -> Any:
        if account_index in self._signers:
            return self._signers[account_index]
        module = self._lighter()
        key_index = self.config.api_key_index
        assert key_index is not None
        private_key = self.secrets.private_key(account_index, key_index)
        if not isinstance(private_key, str) or not private_key:
            raise ContractError("secret provider returned an empty private key")
        factory = self._signer_factory or module.SignerClient
        kwargs: dict[str, Any] = {
            "url": self.config.api_base_url,
            "account_index": account_index,
            "api_private_keys": {key_index: private_key},
            "chain_id": self.config.chain_id,
        }
        nonce_types = getattr(getattr(module, "nonce_manager", None), "NonceManagerType", None)
        if nonce_types is not None:
            kwargs["nonce_management_type"] = nonce_types.API
        signer = factory(**kwargs)
        self._signers[account_index] = signer
        return signer

    def _api(self, account_index: int) -> Any:
        if account_index in self._apis:
            return self._apis[account_index]
        module = self._lighter()
        factory = self._api_factory or module.ApiClient
        api = factory(self._generated_api_client(module))
        self._apis[account_index] = api
        return api

    def _generated_api_client(self, module: Any | None = None) -> Any:
        if self._api_client is not None:
            return self._api_client
        module = module or self._lighter()
        # Generated API clients take ApiClient(Configuration), not a URL.  Keep
        # retries disabled (None) so reads and any accidental generated call
        # cannot silently replay; mutation transport is explicit below.
        configuration = module.Configuration(
            host=self.config.api_base_url,
            retries=None,
        )
        self._api_client = module.ApiClient(configuration)
        return self._api_client

    async def _authorization(self, account_index: int) -> str:
        cached = self._tokens.get(account_index)
        if cached is not None and self._clock() < cached.expires_at:
            return cached.value
        signer = self._signer(account_index)
        key_index = self.config.api_key_index
        assert key_index is not None
        lifetime = self.config.auth_token_lifetime_seconds
        auth_deadline = time.monotonic() + self.config.request_timeout_seconds
        result = await self._bounded(
            _await(
                signer.create_auth_token_with_expiry(
                    deadline=int(lifetime),
                    api_key_index=key_index,
                )
            ),
            auth_deadline,
            "auth token acquisition",
        )
        token: Any = result[0] if isinstance(result, tuple) else result
        if isinstance(result, tuple) and len(result) > 1 and result[1]:
            raise RuntimeError("Lighter auth token request was rejected")
        if not isinstance(token, str) or not token:
            raise RuntimeError("Lighter auth token was not returned")
        self._tokens[account_index] = _CachedToken(
            token,
            self._clock() + max(1.0, float(lifetime) - 1.0),
        )
        return token

    async def market_metadata(self, market_id: int) -> MarketMetadata:
        # Market metadata is explicitly supplied by the operator from current
        # orderBookDetails/account-limits evidence.  The SDK endpoint is still
        # queried once to bind this run to the requested market id.
        module = self._lighter()
        api = module.OrderApi(self._generated_api_client(module))
        details = await self._bounded(
            _await(
                api.order_book_details(
                    market_id=market_id,
                    filter="perp",
                    _request_timeout=self.config.request_timeout_seconds,
                )
            ),
            time.monotonic() + self.config.request_timeout_seconds,
            "orderBookDetails read",
        )
        raw_details = _model_dict(details)
        _require_success_code(raw_details, "orderBookDetails")
        observed = _select_perp_market(raw_details, market_id, expected_symbol=self.config.market_symbol)
        # Operator evidence supplies the durable market contract (minimums,
        # increments, fees and margin provenance), but its timestamp is not a
        # timestamp for this request.  orderBookDetails has no universally
        # usable observation timestamp, so bind this returned snapshot to the
        # completion of the actual read.  An explicitly timestamped fixture is
        # retained so stale/future saved responses still fail the normal
        # freshness checks.
        evidence = dict(self.market_evidence)
        try:
            evidence_market_id = int(evidence.get("market_id", market_id))
        except (TypeError, ValueError) as exc:
            raise ContractError("market evidence has no valid market_id") from exc
        if evidence_market_id != market_id:
            raise ContractError("market evidence market_id does not match config")
        if "market_id" in observed:
            try:
                observed_market_id = int(observed["market_id"])
            except (TypeError, ValueError) as exc:
                raise ContractError("orderBookDetails market_id is invalid") from exc
            if observed_market_id != market_id:
                raise ContractError("orderBookDetails market identity does not match config")
        if "symbol" in observed and "symbol" in evidence and str(observed["symbol"]).upper() != str(evidence["symbol"]).upper():
            raise ContractError("market evidence conflicts with orderBookDetails symbol")
        evidence.setdefault("market_id", market_id)
        evidence.setdefault("symbol", self.config.market_symbol)
        evidence.setdefault("market_type", "perp")
        if self.config.environment == "robinhood":
            evidence.setdefault("venue", "robinhood")
        observed_at = observed.get("observed_at")
        if observed_at is None:
            observed_at = self._clock()
        evidence["observed_at"] = observed_at
        # Do not infer status/fees/precision/minimums from undocumented SDK
        # attributes.  Merge only exact named fields supplied by the operator.
        observed_aliases = {
            "status": "status",
            "price_decimals": "supported_price_decimals",
            "size_decimals": "supported_size_decimals",
            "minimum_base_amount": "min_base_amount",
            "minimum_quote_amount": "min_quote_amount",
        }
        for target, source in observed_aliases.items():
            if source in observed:
                if target in evidence and str(evidence[target]) != str(observed[source]):
                    raise ContractError(f"market evidence conflicts with orderBookDetails {source}")
                evidence[target] = observed[source]
        return MarketMetadata.from_mapping(evidence)

    async def resolve_market(self, symbol: str) -> MarketMetadata:
        """Resolve the current Robinhood perp ID from the official catalog.

        The resolver is only available for the explicit Robinhood deployment;
        it never queries or falls back to ordinary Lighter mainnet.
        """

        requested = str(symbol).strip().upper()
        if self.config.environment != "robinhood":
            raise ContractError("symbol resolution is only available for Robinhood Chain")
        module = self._lighter()
        api = module.OrderApi(self._generated_api_client(module))
        details = await self._bounded(
            _await(
                api.order_book_details(
                    filter="perp",
                    _request_timeout=self.config.request_timeout_seconds,
                )
            ),
            time.monotonic() + self.config.request_timeout_seconds,
            "Robinhood orderBookDetails catalog read",
        )
        payload = _model_dict(details)
        _require_success_code(payload, "orderBookDetails")
        observed = _select_perp_market_by_symbol(payload, requested)
        evidence = dict(self.market_evidence)
        if "observed_at" not in evidence:
            raise ContractError("market evidence must include its original observed_at timestamp")
        if "market_id" in evidence:
            try:
                evidence_market_id = int(evidence["market_id"])
            except (TypeError, ValueError) as exc:
                raise ContractError("market evidence has no valid market_id") from exc
            if evidence_market_id != int(observed["market_id"]):
                raise ContractError("market evidence market_id conflicts with symbol resolution")
        if "symbol" in evidence and str(evidence["symbol"]).upper() != requested:
            raise ContractError("market evidence symbol conflicts with symbol resolution")
        evidence.setdefault("market_id", int(observed["market_id"]))
        evidence.setdefault("symbol", requested)
        evidence.setdefault("market_type", "perp")
        evidence.setdefault("venue", "robinhood")
        aliases = {
            "status": "status",
            "price_decimals": "supported_price_decimals",
            "size_decimals": "supported_size_decimals",
            "minimum_base_amount": "min_base_amount",
            "minimum_quote_amount": "min_quote_amount",
        }
        for target, source in aliases.items():
            if source in observed:
                if target in evidence and str(evidence[target]) != str(observed[source]):
                    raise ContractError(f"market evidence conflicts with orderBookDetails {source}")
                evidence[target] = observed[source]
        return MarketMetadata.from_mapping(evidence)

    async def resolve_perpetual_market(self, symbol: str) -> MarketMetadata:
        """Compatibility alias for callers that name the perp explicitly."""

        return await self.resolve_market(symbol)

    async def order_book(self, market_id: int) -> Any:
        """Read one official public orderBookOrders snapshot.

        The Robinhood response has no reliable venue timestamp, so freshness
        is bound to this request observation time.  ``transaction_time`` from
        individual orders is intentionally not used as a book timestamp.
        """

        if self.config.environment != "robinhood":
            raise ContractError("Robinhood order-book reads require the Robinhood deployment")
        module = self._lighter()
        api = module.OrderApi(self._generated_api_client(module))
        payload_raw = await self._bounded(
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
        payload = _model_dict(payload_raw)
        _require_success_code(payload, "orderBookOrders")
        from .series import OrderBookSnapshot

        return OrderBookSnapshot.from_mapping(
            payload,
            market_id=market_id,
            symbol=self.config.market_symbol,
            observed_at=self._clock(),
            venue="robinhood",
        )

    async def order_book_snapshot(self, market_id: int) -> Any:
        return await self.order_book(market_id)

    async def account_snapshot(self, account_index: int, market_id: int) -> AccountSnapshot:
        module = self._lighter()
        api = module.AccountApi(self._generated_api_client(module))
        raw_account = await self._bounded(
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
        raw_account_mapping = _model_dict(raw_account)
        _require_success_code(raw_account_mapping, "account")
        account = _first_mapping(raw_account, "accounts")
        if not {"index", "l1_address", "status", "positions", "available_balance"}.issubset(account):
            raise ContractError("Lighter account response is missing required identity/state fields")
        account_identity = account["l1_address"]
        if not isinstance(account_identity, str) or not account_identity.strip():
            raise ContractError("Lighter account response has no exact account identity")
        returned_index = account["index"]
        try:
            if int(returned_index) != account_index:
                raise ContractError("Lighter account response identity does not match requested account")
        except (TypeError, ValueError) as exc:
            raise ContractError("Lighter account response has no exact account identity") from exc
        positions = account["positions"]
        if not isinstance(positions, (list, tuple)):
            raise ContractError("Lighter account positions field is malformed")
        position: Mapping[str, Any] | None = None
        for candidate in positions:
            candidate_map = _model_dict(candidate)
            if "market_id" not in candidate_map:
                raise ContractError("Lighter account position lacks market identity")
            try:
                candidate_market_id = int(candidate_map["market_id"])
            except (TypeError, ValueError) as exc:
                raise ContractError("Lighter account position has invalid market identity") from exc
            if candidate_market_id == market_id:
                if "position" not in candidate_map or "sign" not in candidate_map:
                    raise ContractError("Lighter account position lacks required sign/position fields")
                position = candidate_map
                break
        position = position or {"position": "0", "sign": 1}
        active_orders = await self._active_orders(account_index, market_id)
        available = account.get("available_balance")
        margin_required = account.get("cross_initial_margin_requirement")
        now = self._clock()
        fee_rate_key = "source_fee_rate" if account_index == self.source_account_index else "receiver_fee_rate"
        incremental_key = (
            "source_incremental_margin_required"
            if account_index == self.source_account_index
            else "receiver_incremental_margin_required"
        )
        incremental_evidence_key = (
            "source_incremental_margin_evidence"
            if account_index == self.source_account_index
            else "receiver_incremental_margin_evidence"
        )
        status = account["status"]
        ready = status in (0, 1, "active", "online")
        if not ready:
            raise ContractError("Lighter account status is not an approved active value")
        fee_rate = self.market_evidence.get(fee_rate_key)
        incremental_margin = self.market_evidence.get(incremental_key)
        incremental_evidence = self.market_evidence.get(incremental_evidence_key, "")
        if incremental_margin is not None:
            # Validate a supplied estimate before missing provenance can turn
            # it into an apparently absent value under explicit deferral.
            _nonnegative(incremental_margin, incremental_key)
        if incremental_margin is None or not incremental_evidence:
            # Do not treat current cross margin as the requirement of adding Q.
            incremental_margin = None
        return AccountSnapshot.from_mapping(
            {
                "account_index": account_index,
                "market_id": market_id,
                "position": position.get("position", "0"),
                "sign": position.get("sign", 1),
                "active_orders": active_orders,
                "observed_at": now,
                "authorized": True,
                "ready": ready,
                "margin_available": available,
                "available_balance": available,
                "margin_required": margin_required,
                "fee_rate": fee_rate,
                "source_identity": account_identity.strip(),
                "incremental_margin_required": incremental_margin,
                "incremental_margin_evidence": incremental_evidence,
            }
        )

    async def _active_orders(self, account_index: int, market_id: int) -> tuple[OrderSnapshot, ...]:
        module = self._lighter()
        api = module.OrderApi(self._generated_api_client(module))
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
        raw_mapping = _model_dict(raw)
        _require_success_code(raw_mapping, "accountActiveOrders")
        if "orders" not in raw_mapping or not isinstance(raw_mapping["orders"], (list, tuple)):
            raise ContractError("accountActiveOrders response lacks an orders list")
        values = raw_mapping["orders"]
        result: list[OrderSnapshot] = []
        for item in values:
            parsed = OrderSnapshot.from_mapping(
                _order_snapshot_mapping(item, observed_at=self._clock())
            )
            if parsed.account_index != account_index or parsed.market_id != market_id:
                raise ContractError("active order response identity does not match requested account/market")
            result.append(parsed)
        return tuple(result)

    async def lookup_order(
        self,
        account_index: int,
        market_id: int,
        *,
        order_id: str | None = None,
        client_order_index: int | None = None,
    ) -> OrderSnapshot | None:
        if order_id is None and client_order_index is None:
            raise ContractError("lookup_order requires order_id or client_order_index")
        token = await self._authorization(account_index)
        params: dict[str, Any] = {"account_index": account_index}
        if client_order_index is None:
            raise ContractError("accountOrders lookup requires the exact client order index")
        params["client_order_indexes"] = str(client_order_index)
        payload = await self._bounded(
            self._http.get("api/v1/accountOrders", params=params, authorization=token),
            time.monotonic() + self.config.request_timeout_seconds,
            "accountOrders read",
        )
        _require_success_code(payload, "accountOrders")
        if "orders" not in payload or not isinstance(payload["orders"], (list, tuple)):
            raise ContractError("accountOrders response lacks an orders list")
        values = payload["orders"]
        for item in values:
            parsed = OrderSnapshot.from_mapping(
                _order_snapshot_mapping(item, observed_at=self._clock())
            )
            if parsed.account_index != account_index or parsed.market_id != market_id:
                raise ContractError("accountOrders response identity does not match requested account/market")
            if order_id is not None and parsed.order_id != str(order_id):
                continue
            if client_order_index is not None and str(parsed.client_order_index) != str(client_order_index):
                continue
            return parsed
        if payload.get("next_cursor"):
            raise ContractError("accountOrders history is paginated beyond the requested page")
        return None

    async def list_trades(
        self,
        account_index: int,
        market_id: int,
        *,
        order_id: str | None = None,
        cursor: str | None = None,
        limit: int = 100,
    ) -> HistoryPage:
        module = self._lighter()
        api = module.OrderApi(self._generated_api_client(module))
        token = await self._authorization(account_index)
        raw = await self._bounded(
            _await(
                api.trades(
                    sort_by="block_height",
                    limit=limit,
                    authorization=token,
                    market_id=market_id,
                    account_index=account_index,
                    order_index=None if order_id is None else int(order_id),
                    sort_dir="desc",
                    cursor=cursor,
                    market_type="perp",
                    type="all",
                    aggregate=False,
                    _request_timeout=self.config.request_timeout_seconds,
                )
            ),
            time.monotonic() + self.config.request_timeout_seconds,
            "trades read",
        )
        payload = _model_dict(raw)
        _require_success_code(payload, "trades")
        values = payload.get("trades")
        if not isinstance(values, (list, tuple)):
            raise ContractError("trades response lacks a trades list")
        trades: list[TradeReceipt] = []
        for item in values:
            mapped = _model_dict(item)
            required = (
                "trade_id",
                "trade_id_str",
                "market_id",
                "size",
                "price",
                "ask_id",
                "bid_id",
                "ask_client_id",
                "ask_client_id_str",
                "bid_client_id",
                "bid_client_id_str",
                "ask_account_id",
                "bid_account_id",
                "timestamp",
            )
            if any(key not in mapped for key in required):
                raise ContractError("trade receipt lacks required official identity/time fields")
            observed_at = _trade_timestamp_seconds(mapped["timestamp"])
            if (
                str(mapped["trade_id"]) != str(mapped["trade_id_str"])
                or str(mapped["ask_client_id"]) != str(mapped["ask_client_id_str"])
                or str(mapped["bid_client_id"]) != str(mapped["bid_client_id_str"])
            ):
                raise ContractError("trade receipt has conflicting numeric/string identities")
            try:
                receipt_market = int(mapped["market_id"])
                ask_id = str(mapped["ask_id"])
                bid_id = str(mapped["bid_id"])
                ask_account = int(mapped["ask_account_id"])
                bid_account = int(mapped["bid_account_id"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ContractError("trade receipt lacks exact ask/bid account identities") from exc
            if receipt_market != market_id:
                raise ContractError("trade receipt market identity does not match requested market")
            if account_index not in {ask_account, bid_account}:
                raise ContractError("trade receipt does not belong to requested account")
            is_ask = account_index == ask_account
            own_order_id = ask_id if is_ask else bid_id
            if order_id is not None and own_order_id != str(order_id):
                continue
            mapped["order_id"] = own_order_id
            mapped["side"] = "SELL" if is_ask else "BUY"
            mapped["quantity"] = mapped.get("size")
            mapped["counterparty_account_index"] = bid_account if is_ask else ask_account
            mapped["counterparty_order_id"] = bid_id if is_ask else ask_id
            mapped["counterparty_client_order_index"] = (
                mapped["bid_client_id_str"] if is_ask else mapped["ask_client_id_str"]
            )
            mapped["client_order_index"] = (
                mapped["ask_client_id_str"] if is_ask else mapped["bid_client_id_str"]
            )
            mapped["trade_id"] = mapped.get("trade_id_str", mapped.get("trade_id"))
            mapped["observed_at"] = observed_at
            trades.append(
                TradeReceipt.from_mapping(
                    {
                        **mapped,
                        "account_index": account_index,
                        "market_id": market_id,
                    }
                )
            )
        return HistoryPage(
            trades=tuple(trades),
            next_cursor=payload.get("next_cursor"),
            complete=not bool(payload.get("next_cursor")),
        )

    async def aclose(self) -> None:
        """Close all adapter-owned SDK/HTTP resources exactly once.

        The generated public client owns one HTTP session and each signer owns
        its own client/session.  A signer's ``close`` method owns its nested
        client, so that nested object is only closed directly when the signer
        does not expose a close method.  Cleanup errors are deliberately
        contained so a terminal handoff result is never replaced by teardown
        noise.
        """

        if self._closed:
            return
        self._closed = True
        seen: set[int] = set()
        self._tokens.clear()

        async def close_once(resource: Any) -> bool:
            if resource is None or id(resource) in seen:
                return True
            seen.add(id(resource))
            return await _close_resource(resource)

        await close_once(self._api_client)
        for signer in tuple(self._signers.values()):
            closed = await close_once(signer)
            if not closed:
                # A failing signer close can leave its private generated client
                # open.  Give that nested session one bounded fallback attempt.
                await close_once(getattr(signer, "api_client", None))
        await close_once(self._http)
        self._signers.clear()
        self._apis.clear()
        self._api_client = None

    async def close(self) -> None:
        await self.aclose()

    async def _bounded(self, awaitable: Any, deadline: float, label: str) -> Any:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"{label} exceeded configured request/freshness deadline")
        try:
            return await asyncio.wait_for(awaitable, timeout=remaining)
        except asyncio.TimeoutError as exc:
            raise TimeoutError(f"{label} exceeded configured request/freshness deadline") from exc

    async def _next_nonce(self, signer: Any, api_key_index: int, *, deadline: float | None = None) -> int:
        manager = getattr(signer, "nonce_manager", None)
        method = getattr(manager, "async_next_nonce", None)
        if method is None:
            raise ContractError("lighter-sdk nonce manager is unavailable; raw signing cannot use nonce=-1")
        if deadline is None:
            deadline = time.monotonic() + min(
                self.config.request_timeout_seconds,
                self.config.freshness_seconds,
            )
        result = await self._bounded(method(api_key_index), deadline, "nonce acquisition")
        if not isinstance(result, tuple) or len(result) != 2:
            raise ContractError("lighter-sdk nonce manager returned an unsupported shape")
        returned_key, nonce = result
        if (
            isinstance(returned_key, bool)
            or not isinstance(returned_key, int)
            or returned_key != api_key_index
            or isinstance(nonce, bool)
            or not isinstance(nonce, int)
            or nonce < 0
        ):
            raise ContractError("lighter-sdk nonce manager returned an invalid account/key nonce")
        return nonce

    async def _send_signed_tx(self, tx_type: Any, tx_info: Any) -> Mapping[str, Any]:
        if isinstance(tx_type, bool) or not isinstance(tx_type, int) or not isinstance(tx_info, str) or not tx_info:
            raise ContractError("lighter-sdk signer returned malformed transaction data")
        # PlainAioHttp performs exactly one POST to the documented sendTx form
        # endpoint.  The signed tx body is never journaled or included in errors.
        return await self._http.post_form(
            "api/v1/sendTx",
            form={"tx_type": tx_type, "tx_info": tx_info},
        )

    async def submit_order(self, plan: OrderPlan) -> MutationReceipt:
        signer = self._signer(plan.account_index)
        key_index = self.config.api_key_index
        assert key_index is not None
        try:
            deadline = plan.mutation_deadline_monotonic
            if deadline is None:
                deadline = time.monotonic() + min(
                    self.config.request_timeout_seconds,
                    self.config.freshness_seconds,
                )
            nonce = await self._next_nonce(signer, key_index, deadline=deadline)
            if time.monotonic() >= deadline:
                raise TimeoutError("nonce acquisition crossed the final mutation barrier")
            signer_type = type(signer)
            result = await self._bounded(
                _await(
                    signer.sign_create_order(
                        market_index=plan.market_id,
                        client_order_index=plan.client_order_index,
                        base_amount=plan.quantity_int,
                        price=plan.price_int,
                        is_ask=plan.side == "SELL",
                        order_type=getattr(signer_type, "ORDER_TYPE_LIMIT", 0) if plan.order_type == "LIMIT" else getattr(signer_type, "ORDER_TYPE_MARKET", 1),
                        time_in_force=getattr(signer_type, "ORDER_TIME_IN_FORCE_POST_ONLY", 2) if plan.time_in_force == "POST_ONLY" else getattr(signer_type, "ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL", 0),
                        reduce_only=plan.reduce_only,
                        order_expiry=plan.order_expiry_ms,
                        skip_nonce=getattr(signer_type, "SKIP_NONCE_OFF", 0),
                        nonce=nonce,
                        api_key_index=key_index,
                    )
                ),
                deadline,
                "order signing",
            )
            if not isinstance(result, tuple) or len(result) != 4:
                raise RuntimeError("lighter-sdk sign_create_order returned an unsupported shape")
            tx_type, tx_info, tx_hash, error = result
            if error:
                return MutationReceipt(False, None, _safe_text(tx_hash), sanitize_exception(ValueError(str(error))))
            if time.monotonic() >= deadline:
                raise TimeoutError("order signing crossed the final mutation barrier")
            response = await self._bounded(
                self._send_signed_tx(tx_type, tx_info),
                deadline,
                "order dispatch",
            )
            code = _response_code(response)
            return MutationReceipt(
                accepted=code == 200,
                order_id=None,
                tx_hash=_safe_text(tx_hash) or _safe_text(_model_dict(response).get("tx_hash")),
                error=None if code == 200 else f"send_tx response code {code}",
                response_code=code,
            )
        except Exception as exc:
            return MutationReceipt(False, None, None, sanitize_exception(exc))

    async def cancel_order(self, account_index: int, market_id: int, order_id: str) -> MutationReceipt:
        deadline = self._pending_mutation_deadline
        self._pending_mutation_deadline = None
        signer = self._signer(account_index)
        key_index = self.config.api_key_index
        assert key_index is not None
        try:
            if deadline is None:
                deadline = time.monotonic() + min(
                    self.config.request_timeout_seconds,
                    self.config.freshness_seconds,
                )
            nonce = await self._next_nonce(signer, key_index, deadline=deadline)
            if time.monotonic() >= deadline:
                raise TimeoutError("nonce acquisition crossed the final mutation barrier")
            signer_type = type(signer)
            result = await self._bounded(
                _await(
                    signer.sign_cancel_order(
                        market_index=market_id,
                        order_index=int(order_id),
                        skip_nonce=getattr(signer_type, "SKIP_NONCE_OFF", 0),
                        nonce=nonce,
                        api_key_index=key_index,
                    )
                ),
                deadline,
                "cancel signing",
            )
            if not isinstance(result, tuple) or len(result) != 4:
                raise RuntimeError("lighter-sdk sign_cancel_order returned an unsupported shape")
            tx_type, tx_info, tx_hash, error = result
            if error:
                return MutationReceipt(False, order_id, _safe_text(tx_hash), sanitize_exception(ValueError(str(error))))
            if time.monotonic() >= deadline:
                raise TimeoutError("cancel signing crossed the final mutation barrier")
            response = await self._bounded(
                self._send_signed_tx(tx_type, tx_info),
                deadline,
                "cancel dispatch",
            )
            code = _response_code(response)
            return MutationReceipt(
                accepted=code == 200,
                order_id=order_id,
                tx_hash=_safe_text(tx_hash) or _safe_text(_model_dict(response).get("tx_hash")),
                error=None if code == 200 else f"send_tx response code {code}",
                response_code=code,
            )
        except Exception as exc:
            return MutationReceipt(False, order_id, None, sanitize_exception(exc))

def _response_code(value: Any) -> int | None:
    mapping = _model_dict(value)
    code = mapping.get("code", mapping.get("_http_status"))
    if code is None:
        return None
    try:
        return int(code)
    except (TypeError, ValueError):
        return None


def _require_success_code(payload: Mapping[str, Any], label: str) -> None:
    code = payload.get("code")
    if code is None:
        raise ContractError(f"{label} response lacks required code")
    try:
        numeric = int(code)
    except (TypeError, ValueError) as exc:
        raise ContractError(f"{label} response code is malformed") from exc
    if numeric != 200:
        raise ContractError(f"{label} response was not successful")


def _select_perp_market(
    payload: Mapping[str, Any],
    market_id: int,
    *,
    expected_symbol: str = "HOOD",
) -> dict[str, Any]:
    values = payload.get("order_book_details")
    if not isinstance(values, (list, tuple)):
        raise ContractError("orderBookDetails response lacks order_book_details")
    for item in values:
        candidate = _model_dict(item)
        try:
            candidate_id = int(candidate.get("market_id"))
        except (TypeError, ValueError):
            continue
        if candidate_id == market_id:
            market_type = candidate.get("market_type")
            if not isinstance(market_type, str) or market_type.strip().lower() != "perp":
                raise ContractError("orderBookDetails identity is not a perpetual market")
            symbol = candidate.get("symbol")
            if not isinstance(symbol, str) or not symbol.strip():
                raise ContractError("orderBookDetails market identity lacks symbol")
            if symbol.strip().upper() != expected_symbol.strip().upper():
                raise ContractError(
                    f"orderBookDetails market identity is not {expected_symbol.strip().upper()}"
                )
            return candidate
    raise ContractError(
        f"orderBookDetails response lacks the requested {expected_symbol.strip().upper()} market"
    )


def _select_perp_market_by_symbol(payload: Mapping[str, Any], symbol: str) -> dict[str, Any]:
    values = payload.get("order_book_details")
    if not isinstance(values, (list, tuple)):
        raise ContractError("orderBookDetails response lacks order_book_details")
    expected = symbol.strip().upper()
    for item in values:
        candidate = _model_dict(item)
        candidate_symbol = candidate.get("symbol")
        if not isinstance(candidate_symbol, str) or candidate_symbol.strip().upper() != expected:
            continue
        market_type = candidate.get("market_type")
        if not isinstance(market_type, str) or market_type.strip().lower() != "perp":
            raise ContractError("orderBookDetails symbol resolved to a non-perpetual market")
        try:
            candidate["market_id"] = int(candidate["market_id"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ContractError("orderBookDetails market identity has no valid market_id") from exc
        return candidate
    raise ContractError(f"orderBookDetails response lacks the requested {expected} perpetual")


def _safe_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    if re.fullmatch(r"0x[0-9a-fA-F]{8,128}", text):
        return text
    return None


__all__ = [
    "LighterSdkClient",
    "MissingSdkError",
    "PlainAioHttp",
    "ROBINHOOD_ORDER_BOOK_LIMIT",
    "REQUIRED_LIGHTER_SDK_VERSION",
    "SecretProvider",
    "SdkVersionError",
    "StaticSecretProvider",
]
