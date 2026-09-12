"""Explicit, opt-in Lighter SDK/HTTP adapter for HCR-1.

No SDK module is imported at package import time.  Mutation calls use the
SDK's ``sign_*`` methods and one explicit ``TransactionApi.send_tx`` call.  The
combined ``create_order``/``cancel_order`` helpers are intentionally not used,
because their retry and response boundaries are not suitable for one-attempt
close/reopen semantics.
"""

from __future__ import annotations

from dataclasses import dataclass
import importlib
from importlib import metadata as importlib_metadata
import inspect
import json
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
)
from .journal import sanitize_exception


REQUIRED_LIGHTER_SDK_VERSION = "1.1.2"


class SecretProvider(Protocol):
    def private_key(self, account_index: int, api_key_index: int) -> str: ...


class MissingSdkError(RuntimeError):
    pass


class SdkVersionError(RuntimeError):
    pass


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


class PlainAioHttp:
    """One-request HTTP helper; deliberately does not import aiohttp-retry."""

    def __init__(self, base_url: str, *, timeout_seconds: float) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = float(timeout_seconds)

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
        self._tokens: dict[int, str] = {}
        self._api_client: Any | None = None
        self._http = PlainAioHttp(config.api_base_url, timeout_seconds=config.request_timeout_seconds)

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
        }
        if self.config.chain_id is not None:
            kwargs["chain_id"] = self.config.chain_id
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
        # Generated API clients take ApiClient(Configuration), not a URL.  Set
        # retries=0 explicitly so even read calls cannot silently replay and
        # mutation send_tx has one observable transport attempt.
        configuration = module.Configuration(
            host=self.config.api_base_url,
            retries=0,
        )
        self._api_client = module.ApiClient(configuration)
        return self._api_client

    async def _authorization(self, account_index: int) -> str:
        if account_index in self._tokens:
            return self._tokens[account_index]
        signer = self._signer(account_index)
        key_index = self.config.api_key_index
        assert key_index is not None
        result = await _await(
            signer.create_auth_token_with_expiry(
                deadline=int(self.config.request_timeout_seconds),
                api_key_index=key_index,
            )
        )
        token: Any = result[0] if isinstance(result, tuple) else result
        if not isinstance(token, str) or not token:
            raise RuntimeError("Lighter auth token was not returned")
        self._tokens[account_index] = token
        return token

    async def market_metadata(self, market_id: int) -> MarketMetadata:
        # Market metadata is explicitly supplied by the operator from current
        # orderBookDetails/account-limits evidence.  The SDK endpoint is still
        # queried once to bind this run to the requested market id.
        module = self._lighter()
        api = module.OrderApi(self._generated_api_client(module))
        details = await _await(
            api.order_book_details(
                market_id=market_id,
                filter="perp",
                _request_timeout=self.config.request_timeout_seconds,
            )
        )
        raw_details = _model_dict(details)
        observed = _select_perp_market(raw_details, market_id)
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
        evidence.setdefault("symbol", "HOOD")
        evidence.setdefault("observed_at", self._clock())
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

    async def account_snapshot(self, account_index: int, market_id: int) -> AccountSnapshot:
        module = self._lighter()
        api = module.AccountApi(self._generated_api_client(module))
        raw_account = await _await(
            api.account(
                by="index",
                value=str(account_index),
                active_only=False,
                _request_timeout=self.config.request_timeout_seconds,
            )
        )
        account = _first_mapping(raw_account, "accounts")
        returned_index = account.get("index", account.get("account_index", account_index))
        try:
            if int(returned_index) != account_index:
                raise ContractError("Lighter account response identity does not match requested account")
        except (TypeError, ValueError) as exc:
            raise ContractError("Lighter account response has no exact account identity") from exc
        positions = account.get("positions") or []
        position: Mapping[str, Any] | None = None
        for candidate in positions:
            candidate_map = _model_dict(candidate)
            if int(candidate_map.get("market_id", candidate_map.get("market_index", -1))) == market_id:
                position = candidate_map
                break
        position = position or {"position": "0", "sign": 1}
        active_orders = await self._active_orders(account_index, market_id)
        available = account.get("available_balance")
        margin_required = account.get("cross_initial_margin_requirement")
        now = self._clock()
        fee_rate_key = "source_fee_rate" if account_index == self.source_account_index else "receiver_fee_rate"
        fee_rate = self.market_evidence.get(fee_rate_key)
        return AccountSnapshot.from_mapping(
            {
                "account_index": account_index,
                "market_id": market_id,
                "position": position.get("position", "0"),
                "sign": position.get("sign", 1),
                "active_orders": active_orders,
                "observed_at": now,
                "authorized": True,
                "ready": bool(account.get("status", 0) in (0, 1, "active", "online")),
                "margin_available": available,
                "margin_required": margin_required,
                "fee_rate": fee_rate,
                "source_identity": str(account.get("l1_address", "")),
            }
        )

    async def _active_orders(self, account_index: int, market_id: int) -> tuple[OrderSnapshot, ...]:
        module = self._lighter()
        api = module.OrderApi(self._generated_api_client(module))
        token = await self._authorization(account_index)
        raw = await _await(
            api.account_active_orders(
                authorization=token,
                account_index=account_index,
                market_id=market_id,
                _request_timeout=self.config.request_timeout_seconds,
            )
        )
        values = _model_dict(raw).get("orders", [])
        result: list[OrderSnapshot] = []
        for item in values:
            result.append(
                OrderSnapshot.from_mapping(
                    {
                        **_model_dict(item),
                        "observed_at": self._clock(),
                    }
                )
            )
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
        payload = await self._http.get("api/v1/accountOrders", params=params, authorization=token)
        values = payload.get("orders", [])
        for item in values:
            parsed = OrderSnapshot.from_mapping({**_model_dict(item), "observed_at": self._clock()})
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
        raw = await _await(
            api.trades(
                sort_by="block_height",
                limit=limit,
                authorization=token,
                market_id=market_id,
                account_index=account_index,
                order_index=None if order_id is None else int(order_id),
                sort_dir="asc",
                cursor=cursor,
                market_type="perp",
                type="all",
                aggregate=False,
                _request_timeout=self.config.request_timeout_seconds,
            )
        )
        payload = _model_dict(raw)
        trades: list[TradeReceipt] = []
        for item in payload.get("trades", []):
            mapped = _model_dict(item)
            ask_id = mapped.get("ask_id", mapped.get("ask_id_str"))
            bid_id = mapped.get("bid_id", mapped.get("bid_id_str"))
            if order_id is not None:
                candidate_ids = {
                    str(mapped.get("order_id", "")),
                    str(mapped.get("order_index", "")),
                    str(mapped.get("ask_order_id", "")),
                    str(mapped.get("bid_order_id", "")),
                    str(ask_id),
                    str(bid_id),
                    str(mapped.get("ask_client_id", "")),
                    str(mapped.get("bid_client_id", "")),
                    str(mapped.get("ask_client_id_str", "")),
                    str(mapped.get("bid_client_id_str", "")),
                }
                if str(order_id) not in candidate_ids:
                    continue
            try:
                ask_account = int(mapped["ask_account_id"])
                bid_account = int(mapped["bid_account_id"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ContractError("trade receipt lacks exact ask/bid account identities") from exc
            if account_index not in {ask_account, bid_account}:
                raise ContractError("trade receipt does not belong to requested account")
            is_ask = account_index == ask_account
            mapped["order_id"] = str(order_id if order_id is not None else (ask_id if is_ask else bid_id))
            mapped["side"] = "SELL" if is_ask else "BUY"
            mapped["quantity"] = mapped.get("size")
            mapped["counterparty_account_index"] = bid_account if is_ask else ask_account
            mapped["trade_id"] = mapped.get("trade_id_str", mapped.get("trade_id"))
            trades.append(
                TradeReceipt.from_mapping(
                    {
                        **mapped,
                        "account_index": account_index,
                        "market_id": market_id,
                        "observed_at": self._clock(),
                    }
                )
            )
        return HistoryPage(
            trades=tuple(trades),
            next_cursor=payload.get("next_cursor"),
            complete=not bool(payload.get("next_cursor")) or "next_cursor" in payload,
        )

    async def submit_order(self, plan: OrderPlan) -> MutationReceipt:
        signer = self._signer(plan.account_index)
        key_index = self.config.api_key_index
        assert key_index is not None
        try:
            result = await _await(
                signer.sign_create_order(
                    market_index=plan.market_id,
                    client_order_index=plan.client_order_index,
                    base_amount=plan.quantity_int,
                    price=plan.price_int,
                    is_ask=plan.side == "SELL",
                    order_type=0 if plan.order_type == "LIMIT" else 1,
                    time_in_force=2 if plan.time_in_force == "POST_ONLY" else 0,
                    reduce_only=plan.reduce_only,
                    order_expiry=plan.order_expiry_ms,
                    api_key_index=key_index,
                )
            )
            if not isinstance(result, tuple) or len(result) != 4:
                raise RuntimeError("lighter-sdk sign_create_order returned an unsupported shape")
            tx_type, tx_info, tx_hash, error = result
            if error:
                return MutationReceipt(False, None, _safe_text(tx_hash), sanitize_exception(ValueError(str(error))))
            tx_api = self._tx_api()
            response = await _await(
                tx_api.send_tx(
                    tx_type=tx_type,
                    tx_info=tx_info,
                    _request_timeout=self.config.request_timeout_seconds,
                )
            )
            code = _response_code(response)
            return MutationReceipt(
                accepted=code == 200,
                order_id=None,
                tx_hash=_safe_text(tx_hash),
                error=None if code == 200 else f"send_tx response code {code}",
                response_code=code,
            )
        except Exception as exc:
            return MutationReceipt(False, None, None, sanitize_exception(exc))

    async def cancel_order(self, account_index: int, market_id: int, order_id: str) -> MutationReceipt:
        signer = self._signer(account_index)
        key_index = self.config.api_key_index
        assert key_index is not None
        try:
            result = await _await(
                signer.sign_cancel_order(
                    market_index=market_id,
                    order_index=int(order_id),
                    api_key_index=key_index,
                )
            )
            if not isinstance(result, tuple) or len(result) != 4:
                raise RuntimeError("lighter-sdk sign_cancel_order returned an unsupported shape")
            tx_type, tx_info, tx_hash, error = result
            if error:
                return MutationReceipt(False, order_id, _safe_text(tx_hash), sanitize_exception(ValueError(str(error))))
            response = await _await(
                self._tx_api().send_tx(
                    tx_type=tx_type,
                    tx_info=tx_info,
                    _request_timeout=self.config.request_timeout_seconds,
                )
            )
            code = _response_code(response)
            return MutationReceipt(
                accepted=code == 200,
                order_id=order_id,
                tx_hash=_safe_text(tx_hash),
                error=None if code == 200 else f"send_tx response code {code}",
                response_code=code,
            )
        except Exception as exc:
            return MutationReceipt(False, order_id, None, sanitize_exception(exc))

    def _tx_api(self) -> Any:
        module = self._lighter()
        return module.TransactionApi(self._generated_api_client(module))


def _response_code(value: Any) -> int | None:
    mapping = _model_dict(value)
    code = mapping.get("code")
    if code is None:
        return None
    try:
        return int(code)
    except (TypeError, ValueError):
        return None


def _select_perp_market(payload: Mapping[str, Any], market_id: int) -> dict[str, Any]:
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
            if str(candidate.get("market_type", "perp")).lower() != "perp":
                raise ContractError("orderBookDetails identity is not a perpetual market")
            return candidate
    raise ContractError("orderBookDetails response lacks the requested HOOD market")


def _safe_text(value: Any) -> str | None:
    if value is None:
        return None
    return sanitize_exception(ValueError(str(value)))


__all__ = [
    "LighterSdkClient",
    "MissingSdkError",
    "PlainAioHttp",
    "REQUIRED_LIGHTER_SDK_VERSION",
    "SecretProvider",
    "SdkVersionError",
    "StaticSecretProvider",
]
