"""Isolated fixture-only Extended testnet lifecycle contract.

This module deliberately contains no transport, signing, or credential access.
It persists only auth-free hypothetical request identities and reconciles them
from injected official-wire evidence.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from copy import deepcopy
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, Mapping, Sequence


MAX_NOTIONAL_USD = Decimal("500")
_MAX_AGE_MS = 30_000
_MAX_ORDER_LIFETIME_MS = 60_000

SDK_PROVENANCE = MappingProxyType({
    "repository": "https://github.com/x10xchange/python_sdk",
    "commit": "2130cdb1cd6e7b1867db83bd3af036572d258739",
    "tree": "30046661d08eb18187911cacc0925021ba00bf68",
})

TESTNET_CONTRACT = MappingProxyType({
    "apiBaseUrl": "https://api.starknet.sepolia.extended.exchange/api/v1",
    "apiBaseOrderManagementUrl": "https://api.starknet.sepolia.extended.exchange/api/v1",
    "streamUrl": "wss://starknet.sepolia.extended.exchange/stream.extended.exchange/v1",
    "streamRpcUrl": "wss://api.starknet.sepolia.extended.exchange/stream.extended.exchange/v2/rpc",
    "signingDomain": "starknet.sepolia.extended.exchange",
    "starknetDomain": MappingProxyType({
        "name": "Perpetuals",
        "version": "v0",
        "chainId": "SN_SEPOLIA",
        "revision": "1",
    }),
})


class ContractViolation(ValueError):
    pass


class EvidenceViolation(ValueError):
    pass


class NonceViolation(ContractViolation):
    pass


class IntentConflict(ContractViolation):
    pass


class LifecycleHalted(RuntimeError):
    pass


def _decimal(value: Any, field: str) -> Decimal:
    if isinstance(value, bool):
        raise EvidenceViolation(f"INVALID_DECIMAL:{field}")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise EvidenceViolation(f"INVALID_DECIMAL:{field}") from exc
    if not result.is_finite():
        raise EvidenceViolation(f"INVALID_DECIMAL:{field}")
    return result


def _strict(value: Any, required: set[str], optional: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise EvidenceViolation(f"WIRE_SCHEMA:{label}")
    keys = set(value)
    if not required <= keys or not keys <= required | optional:
        raise EvidenceViolation(f"WIRE_SCHEMA:{label}")
    return value


_SETTLEMENT_KEYS = {"signature", "starkKey", "collateralPosition"}
_ORDER_VECTOR_KEYS = {
    "externalId",
    "settlementHash",
    "nonce",
    "market",
    "side",
    "type",
    "qty",
    "price",
    "reduceOnly",
    "postOnly",
    "timeInForce",
    "expiryEpochMillis",
    "fee",
    "selfTradeProtectionLevel",
    "settlement",
}
_ORDER_KEYS = {
    "id",
    "accountId",
    "externalId",
    "market",
    "type",
    "side",
    "status",
    "statusReason",
    "price",
    "averagePrice",
    "qty",
    "filledQty",
    "cancelledQty",
    "reduceOnly",
    "postOnly",
    "payedFee",
    "createdTime",
    "updatedTime",
    "expiryTime",
    "timeInForce",
}
_FILL_KEYS = {
    "id",
    "accountId",
    "market",
    "orderId",
    "side",
    "price",
    "qty",
    "value",
    "fee",
    "isTaker",
    "tradeType",
    "createdTime",
}
_POSITION_KEYS = {
    "id",
    "accountId",
    "market",
    "status",
    "side",
    "leverage",
    "size",
    "value",
    "openPrice",
    "markPrice",
    "liquidationPrice",
    "unrealisedPnl",
    "realisedPnl",
    "tpPrice",
    "slPrice",
    "adl",
    "createdAt",
    "updatedAt",
}


def _validate_nonce(nonce: Any) -> int:
    if isinstance(nonce, bool) or not isinstance(nonce, int) or not 1 <= nonce <= 2**31 - 1:
        raise NonceViolation("NONCE_OUT_OF_RANGE")
    return nonce


def build_limit_ioc_order(vector: Mapping[str, Any], server_time_ms: int | None = None) -> dict[str, Any]:
    raw = _strict(dict(vector), _ORDER_VECTOR_KEYS, set(), "order_vector")
    nonce = _validate_nonce(raw["nonce"])
    external_id = str(raw["externalId"])
    if external_id != str(raw["settlementHash"]):
        raise ContractViolation("EXTERNAL_ID_SETTLEMENT_HASH_MISMATCH")
    if raw["type"] != "LIMIT" or raw["timeInForce"] != "IOC":
        raise ContractViolation("LIMIT_IOC_REQUIRED")
    if raw["postOnly"] is not False or not isinstance(raw["reduceOnly"], bool):
        raise ContractViolation("LIMIT_IOC_FLAGS_INVALID")
    if raw["price"] is None or _decimal(raw["price"], "price") <= 0:
        raise ContractViolation("PRICE_BOUND_REQUIRED")
    if _decimal(raw["qty"], "qty") <= 0 or _decimal(raw["fee"], "fee") < 0:
        raise ContractViolation("ORDER_AMOUNT_INVALID")
    expiry = raw["expiryEpochMillis"]
    if isinstance(expiry, bool) or not isinstance(expiry, int):
        raise ContractViolation("EXPIRY_INVALID")
    if server_time_ms is not None and not 0 < expiry - server_time_ms <= _MAX_ORDER_LIFETIME_MS:
        raise ContractViolation("SHORT_EXPIRY_REQUIRED")
    settlement = _strict(raw["settlement"], _SETTLEMENT_KEYS, set(), "settlement")
    signature = _strict(settlement["signature"], {"r", "s"}, set(), "signature")
    if not all(isinstance(signature[key], str) and signature[key].startswith("0x") for key in ("r", "s")):
        raise ContractViolation("SIGNATURE_WIRE_INVALID")
    if not isinstance(settlement["starkKey"], str) or not settlement["starkKey"].startswith("0x"):
        raise ContractViolation("STARK_KEY_WIRE_INVALID")
    if raw["side"] not in {"BUY", "SELL"} or raw["selfTradeProtectionLevel"] != "ACCOUNT":
        raise ContractViolation("ORDER_WIRE_INVALID")
    return {
        "id": external_id,
        "market": raw["market"],
        "type": "LIMIT",
        "side": raw["side"],
        "qty": str(raw["qty"]),
        "price": str(raw["price"]),
        "reduceOnly": raw["reduceOnly"],
        "postOnly": False,
        "timeInForce": "IOC",
        "expiryEpochMillis": expiry,
        "fee": str(raw["fee"]),
        "nonce": nonce,
        "selfTradeProtectionLevel": "ACCOUNT",
        "settlement": deepcopy(settlement),
    }


def canonical_payload_digest(payload: Mapping[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_cancel_by_external_id(external_id: str, api_key: str) -> dict[str, Any]:
    return {
        "method": "DELETE",
        "path": "/user/order",
        "query": {"externalId": external_id},
        "headers": {"X-Api-Key": api_key},
        "json": None,
    }


def build_mass_cancel(external_ids: Sequence[str], api_key: str) -> dict[str, Any]:
    return {
        "method": "POST",
        "path": "/user/order/massCancel",
        "query": {},
        "headers": {"X-Api-Key": api_key},
        "json": {"externalOrderIds": list(external_ids), "cancelAll": False},
    }


@dataclass(frozen=True)
class OfficialEvidence:
    now_ms: int
    server_time_ms: int
    account_observed_ms: int
    book_event_ms: int
    stream_event_ms: int
    stream_received_ms: int
    connected: bool
    gap_free: bool
    account_id: int
    account_status: str
    l2_key: str
    l2_vault: int
    market_name: str
    market: dict[str, Any]
    book: dict[str, Any]
    balance: dict[str, Any]
    fees: tuple[dict[str, Any], ...]
    leverage: tuple[dict[str, Any], ...]
    open_orders: tuple[dict[str, Any], ...]
    positions: tuple[dict[str, Any], ...]
    order_status: dict[str, Any] | None
    order_history: tuple[dict[str, Any], ...]
    fills: tuple[dict[str, Any], ...]
    stream_orders: tuple[dict[str, Any], ...]
    stream_positions: tuple[dict[str, Any], ...]
    stream_trades: tuple[dict[str, Any], ...]
    entry_vector: dict[str, Any]
    close_vector: dict[str, Any]
    position_template: dict[str, Any]

    @property
    def fresh(self) -> bool:
        stamps = (
            self.account_observed_ms,
            self.book_event_ms,
            self.stream_event_ms,
            self.stream_received_ms,
        )
        return all(0 <= self.now_ms - stamp <= _MAX_AGE_MS for stamp in stamps)

    def with_server_time(self, server_time: int, *, observed_at: int) -> "OfficialEvidence":
        return replace(self, now_ms=observed_at, server_time_ms=server_time, account_observed_ms=observed_at)

    def with_account_identity(self, *, account_id: int) -> "OfficialEvidence":
        return replace(self, account_id=account_id)

    def with_market_name(self, market_name: str) -> "OfficialEvidence":
        return replace(self, market_name=market_name)

    def with_open_orders(self, orders: Sequence[dict[str, Any]]) -> "OfficialEvidence":
        copied = tuple(deepcopy(list(orders)))
        return replace(self, open_orders=copied, stream_orders=copied)

    def with_position(self, *, size: Decimal, value: Decimal) -> "OfficialEvidence":
        position = deepcopy(self.position_template)
        position["size"] = str(size)
        position["value"] = str(value)
        return replace(self, positions=(position,), stream_positions=(deepcopy(position),))


def _validate_order_row(row: Any, label: str) -> dict[str, Any]:
    return deepcopy(_strict(row, _ORDER_KEYS, set(), label))


def _validate_fill_row(row: Any, label: str) -> dict[str, Any]:
    return deepcopy(_strict(row, _FILL_KEYS, set(), label))


def _validate_position_row(row: Any, label: str) -> dict[str, Any]:
    return deepcopy(_strict(row, _POSITION_KEYS, set(), label))


def _row_map(rows: Sequence[Mapping[str, Any]], key: str) -> dict[Any, Mapping[str, Any]]:
    mapped = {row[key]: row for row in rows}
    if len(mapped) != len(rows):
        raise EvidenceViolation("STREAM_REST_DISAGREE")
    return mapped


def normalize_official_evidence(raw: Mapping[str, Any], *, now_ms: int) -> OfficialEvidence:
    root_keys = {
        "provenance",
        "contract",
        "identity",
        "market",
        "book",
        "account",
        "stream",
        "entry",
        "close",
        "vectors",
        "filledOrder",
        "fill",
        "filledCloseOrder",
        "closeFill",
        "position",
    }
    data = _strict(dict(raw), root_keys, set(), "root")
    if data["provenance"] != SDK_PROVENANCE or data["contract"] != TESTNET_CONTRACT:
        raise ContractViolation("PINNED_OFFICIAL_CONTRACT_MISMATCH")

    identity = _strict(data["identity"], {"accountId", "accountIndex", "l2Key", "l2Vault", "apiKey"}, set(), "identity")
    market = _strict(
        data["market"],
        {
            "name",
            "type",
            "assetName",
            "assetPrecision",
            "collateralAssetName",
            "collateralAssetPrecision",
            "active",
            "isRfq",
            "isOffHours",
            "tradingHours",
            "marketStats",
            "tradingConfig",
            "l2Config",
        },
        set(),
        "market",
    )
    _strict(
        market["marketStats"],
        {
            "dailyVolume",
            "dailyVolumeBase",
            "dailyPriceChange",
            "dailyLow",
            "dailyHigh",
            "lastPrice",
            "askPrice",
            "bidPrice",
            "markPrice",
            "indexPrice",
            "fundingRate",
            "nextFundingRate",
            "openInterest",
            "openInterestBase",
        },
        set(),
        "marketStats",
    )
    config = _strict(
        market["tradingConfig"],
        {
            "minOrderSize",
            "minOrderSizeChange",
            "minPriceChange",
            "maxMarketOrderValue",
            "maxLimitOrderValue",
            "maxPositionValue",
            "maxLeverage",
            "maxNumOrders",
            "limitPriceCap",
            "limitPriceFloor",
            "riskFactorConfig",
        },
        set(),
        "tradingConfig",
    )
    for index, risk in enumerate(config["riskFactorConfig"]):
        _strict(risk, {"upperBound", "riskFactor"}, set(), f"riskFactorConfig[{index}]")
    _strict(
        market["l2Config"],
        {"type", "collateralId", "collateralResolution", "syntheticId", "syntheticResolution"},
        set(),
        "l2Config",
    )
    book = _strict(data["book"], {"market", "eventTime", "bids", "asks"}, set(), "book")
    for side in ("bids", "asks"):
        if not isinstance(book[side], list):
            raise EvidenceViolation(f"WIRE_SCHEMA:book.{side}")
        for index, level in enumerate(book[side]):
            _strict(level, {"price", "qty"}, set(), f"book.{side}[{index}]")

    account = _strict(
        data["account"],
        {"observedAt", "serverTime", "info", "balance", "fees", "leverage", "openOrders", "positions"},
        {"orderStatus", "orderHistory", "fills"},
        "account",
    )
    info = _strict(
        account["info"],
        {"id", "description", "accountIndex", "status", "l2Key", "l2Vault", "bridgeStarknetAddress"},
        set(),
        "account.info",
    )
    balance_keys = {
        "collateralName",
        "balance",
        "equity",
        "availableForTrade",
        "availableForWithdrawal",
        "unrealisedPnl",
        "initialMargin",
        "marginRatio",
        "updatedTime",
        "spotEquity",
        "spotEquityForAvailableForTrade",
        "collateralReservedForSpotOrders",
    }
    balance = deepcopy(_strict(account["balance"], balance_keys, set(), "balance"))
    fees = tuple(
        deepcopy(_strict(row, {"market", "makerFeeRate", "takerFeeRate", "builderFeeRate"}, set(), "fee"))
        for row in account["fees"]
    )
    leverage = tuple(
        deepcopy(_strict(row, {"market", "leverage"}, set(), "leverage")) for row in account["leverage"]
    )
    open_orders = tuple(_validate_order_row(row, "openOrder") for row in account["openOrders"])
    positions = tuple(_validate_position_row(row, "position") for row in account["positions"])
    order_status = (
        _validate_order_row(account["orderStatus"], "orderStatus") if account.get("orderStatus") is not None else None
    )
    history = tuple(_validate_order_row(row, "orderHistory") for row in account.get("orderHistory", []))
    fills = tuple(_validate_fill_row(row, "fill") for row in account.get("fills", []))

    stream = _strict(
        data["stream"],
        {
            "accountId",
            "l2Key",
            "sequence",
            "eventTime",
            "receivedAt",
            "gapFree",
            "connected",
            "orders",
            "positions",
            "trades",
            "balance",
        },
        set(),
        "stream",
    )
    stream_balance = deepcopy(_strict(stream["balance"], balance_keys, set(), "stream.balance"))
    stream_orders = tuple(_validate_order_row(row, "stream.order") for row in stream["orders"])
    stream_positions = tuple(_validate_position_row(row, "stream.position") for row in stream["positions"])
    stream_trades = tuple(_validate_fill_row(row, "stream.trade") for row in stream["trades"])

    entry = deepcopy(_strict(data["entry"], _ORDER_VECTOR_KEYS, set(), "entry"))
    close = deepcopy(_strict(data["close"], _ORDER_VECTOR_KEYS, set(), "close"))
    entry_payload = build_limit_ioc_order(entry)
    close_payload = build_limit_ioc_order(close)
    vectors = _strict(data["vectors"], {"canonicalPayloadDigest", "closeCanonicalPayloadDigest"}, set(), "vectors")
    _validate_order_row(data["filledOrder"], "filledOrder")
    _validate_fill_row(data["fill"], "fillVector")
    _validate_order_row(data["filledCloseOrder"], "filledCloseOrder")
    _validate_fill_row(data["closeFill"], "closeFillVector")
    position_template = _validate_position_row(data["position"], "positionVector")

    if identity["accountId"] != info["id"] or identity["accountId"] != stream["accountId"]:
        raise EvidenceViolation("ACCOUNT_IDENTITY_MISMATCH")
    if identity["l2Key"] != info["l2Key"] or identity["l2Key"] != stream["l2Key"]:
        raise EvidenceViolation("ACCOUNT_IDENTITY_MISMATCH")
    if any(vector["settlement"]["starkKey"] != identity["l2Key"] for vector in (entry, close)):
        raise EvidenceViolation("ACCOUNT_IDENTITY_MISMATCH")
    if info["l2Vault"] != identity["l2Vault"] or any(
        str(vector["settlement"]["collateralPosition"]) != str(identity["l2Vault"]) for vector in (entry, close)
    ):
        raise EvidenceViolation("ACCOUNT_IDENTITY_MISMATCH")
    if market["name"] != book["market"] or any(vector["market"] != market["name"] for vector in (entry, close)):
        raise EvidenceViolation("MARKET_IDENTITY_MISMATCH")

    minimum = _decimal(config["minOrderSize"], "minimum")
    step = _decimal(config["minOrderSizeChange"], "step")
    tick = _decimal(config["minPriceChange"], "tick")
    if minimum != step:
        raise EvidenceViolation("MINIMUM_STEP_MISMATCH")
    if _decimal(entry["qty"], "entry.qty") % step != 0:
        raise EvidenceViolation("QTY_OFF_GRID")
    if _decimal(entry["price"], "entry.price") % tick != 0:
        raise EvidenceViolation("PRICE_OFF_GRID")
    if canonical_payload_digest(entry_payload) != vectors["canonicalPayloadDigest"]:
        raise ContractViolation("ENTRY_DIGEST_MISMATCH")
    if canonical_payload_digest(close_payload) != vectors["closeCanonicalPayloadDigest"]:
        raise ContractViolation("CLOSE_DIGEST_MISMATCH")

    rest_orders = _row_map((*history, *open_orders), "externalId")
    if order_status is not None:
        matching_status = rest_orders.get(order_status["externalId"])
        if matching_status is None or matching_status != order_status:
            raise EvidenceViolation("STREAM_REST_DISAGREE")
    if (
        balance != stream_balance
        or rest_orders != _row_map(stream_orders, "externalId")
        or _row_map(positions, "id") != _row_map(stream_positions, "id")
        or _row_map(fills, "id") != _row_map(stream_trades, "id")
    ):
        raise EvidenceViolation("STREAM_REST_DISAGREE")
    if now_ms - book["eventTime"] > _MAX_AGE_MS:
        raise EvidenceViolation("STALE_BOOK")

    return OfficialEvidence(
        now_ms=now_ms,
        server_time_ms=account["serverTime"],
        account_observed_ms=account["observedAt"],
        book_event_ms=book["eventTime"],
        stream_event_ms=stream["eventTime"],
        stream_received_ms=stream["receivedAt"],
        connected=stream["connected"],
        gap_free=stream["gapFree"],
        account_id=identity["accountId"],
        account_status=info["status"],
        l2_key=identity["l2Key"],
        l2_vault=identity["l2Vault"],
        market_name=market["name"],
        market=deepcopy(market),
        book=deepcopy(book),
        balance=balance,
        fees=fees,
        leverage=leverage,
        open_orders=open_orders,
        positions=positions,
        order_status=order_status,
        order_history=history,
        fills=fills,
        stream_orders=stream_orders,
        stream_positions=stream_positions,
        stream_trades=stream_trades,
        entry_vector=entry,
        close_vector=close,
        position_template=position_template,
    )


@dataclass(frozen=True)
class Intent:
    id: str
    kind: str
    state: str
    nonce: int | None
    external_id: str
    payload_digest: str
    unsigned_api_payload: dict[str, Any]
    expiry_ms: int
    account_id: int
    l2_key: str
    market: str
    side: str | None
    qty: Decimal
    price: Decimal
    reduce_only: bool
    target_id: str | None = None
    target_external_id: str | None = None
    api_request: dict[str, Any] | None = None


@dataclass(frozen=True)
class StoreSnapshot:
    intent_states: dict[str, str]
    lifecycle_state: str


class IntentStore:
    def __init__(self, path: Path | str):
        self.path = Path(path)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS intents (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    state TEXT NOT NULL,
                    nonce INTEGER,
                    external_id TEXT NOT NULL UNIQUE,
                    payload_digest TEXT NOT NULL,
                    unsigned_api_payload TEXT NOT NULL,
                    expiry_ms INTEGER NOT NULL,
                    account_id INTEGER NOT NULL,
                    l2_key TEXT NOT NULL,
                    market TEXT NOT NULL,
                    side TEXT,
                    qty TEXT NOT NULL,
                    price TEXT NOT NULL,
                    reduce_only INTEGER NOT NULL,
                    target_id TEXT,
                    target_external_id TEXT,
                    api_request TEXT,
                    dispatch_count INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS intents_nonce ON intents(nonce) WHERE nonce IS NOT NULL")
            connection.execute(
                "CREATE TABLE IF NOT EXISTS lifecycle (singleton INTEGER PRIMARY KEY CHECK(singleton=1), state TEXT NOT NULL)"
            )
            connection.execute("INSERT OR IGNORE INTO lifecycle(singleton, state) VALUES (1, 'FLAT')")

    @staticmethod
    def _intent(row: sqlite3.Row) -> Intent:
        return Intent(
            id=row["id"],
            kind=row["kind"],
            state=row["state"],
            nonce=row["nonce"],
            external_id=row["external_id"],
            payload_digest=row["payload_digest"],
            unsigned_api_payload=json.loads(row["unsigned_api_payload"]),
            expiry_ms=row["expiry_ms"],
            account_id=row["account_id"],
            l2_key=row["l2_key"],
            market=row["market"],
            side=row["side"],
            qty=Decimal(row["qty"]),
            price=Decimal(row["price"]),
            reduce_only=bool(row["reduce_only"]),
            target_id=row["target_id"],
            target_external_id=row["target_external_id"],
            api_request=json.loads(row["api_request"]) if row["api_request"] is not None else None,
        )

    def _insert(self, intent: Intent, *, expected_lifecycle: str, next_lifecycle: str) -> None:
        try:
            with self._connect() as connection:
                lifecycle = str(
                    connection.execute("SELECT state FROM lifecycle WHERE singleton=1").fetchone()[0]
                )
                if lifecycle != expected_lifecycle:
                    raise LifecycleHalted("LIFECYCLE_STATE_MISMATCH")
                connection.execute(
                    """
                    INSERT INTO intents(
                        id, kind, state, nonce, external_id, payload_digest, unsigned_api_payload,
                        expiry_ms, account_id, l2_key, market, side, qty, price, reduce_only,
                        target_id, target_external_id, api_request
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        intent.id,
                        intent.kind,
                        intent.state,
                        intent.nonce,
                        intent.external_id,
                        intent.payload_digest,
                        json.dumps(intent.unsigned_api_payload, sort_keys=True, separators=(",", ":")),
                        intent.expiry_ms,
                        intent.account_id,
                        intent.l2_key,
                        intent.market,
                        intent.side,
                        str(intent.qty),
                        str(intent.price),
                        int(intent.reduce_only),
                        intent.target_id,
                        intent.target_external_id,
                        json.dumps(intent.api_request, sort_keys=True, separators=(",", ":"))
                        if intent.api_request is not None
                        else None,
                    ),
                )
                connection.execute(
                    "UPDATE lifecycle SET state=? WHERE singleton=1", (next_lifecycle,)
                )
        except sqlite3.IntegrityError as exc:
            message = str(exc).lower()
            if "nonce" in message:
                raise NonceViolation("NONCE_REUSE") from exc
            raise IntentConflict("EXTERNAL_ID_REUSE") from exc

    def _claim(
        self,
        intent_id: str,
        *,
        expected_lifecycle: str,
        next_lifecycle: str,
    ) -> None:
        with self._connect() as connection:
            lifecycle = str(
                connection.execute("SELECT state FROM lifecycle WHERE singleton=1").fetchone()[0]
            )
            if lifecycle != expected_lifecycle:
                raise LifecycleHalted("LIFECYCLE_STATE_MISMATCH")
            cursor = connection.execute(
                "UPDATE intents SET state='CLAIMED', dispatch_count=dispatch_count+1 WHERE id=? AND state='PREPARED'",
                (intent_id,),
            )
            if cursor.rowcount != 1:
                raise LifecycleHalted("INTENT_NOT_PREPARED")
            connection.execute(
                "UPDATE lifecycle SET state=? WHERE singleton=1", (next_lifecycle,)
            )

    def _set_states(self, states: Mapping[str, str], lifecycle_state: str) -> None:
        with self._connect() as connection:
            for intent_id, state in states.items():
                cursor = connection.execute("UPDATE intents SET state=? WHERE id=?", (state, intent_id))
                if cursor.rowcount != 1:
                    raise LifecycleHalted("INTENT_NOT_FOUND")
            connection.execute("UPDATE lifecycle SET state=? WHERE singleton=1", (lifecycle_state,))

    def get(self, intent_id: str) -> Intent:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM intents WHERE id=?", (intent_id,)).fetchone()
        if row is None:
            raise LifecycleHalted("INTENT_NOT_FOUND")
        return self._intent(row)

    def count(self) -> int:
        with self._connect() as connection:
            return int(connection.execute("SELECT COUNT(*) FROM intents").fetchone()[0])

    def dispatch_count(self, intent_id: str) -> int:
        with self._connect() as connection:
            row = connection.execute("SELECT dispatch_count FROM intents WHERE id=?", (intent_id,)).fetchone()
        if row is None:
            raise LifecycleHalted("INTENT_NOT_FOUND")
        return int(row[0])

    def snapshot(self) -> StoreSnapshot:
        with self._connect() as connection:
            states = {row["id"]: row["state"] for row in connection.execute("SELECT id, state FROM intents")}
            lifecycle_state = str(connection.execute("SELECT state FROM lifecycle WHERE singleton=1").fetchone()[0])
        return StoreSnapshot(states, lifecycle_state)

    def _all(self) -> tuple[Intent, ...]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM intents ORDER BY rowid").fetchall()
        return tuple(self._intent(row) for row in rows)


@dataclass(frozen=True)
class ReconciliationResult:
    lifecycle_state: str
    complete: bool
    next_write: None = None
    filled_qty: Decimal = Decimal(0)
    position_qty: Decimal = Decimal(0)
    reconciled_external_ids: frozenset[str] = frozenset()
    reconciled_fill_ids: frozenset[int] = frozenset()


class ExtendedLifecycle:
    def __init__(self, *, store: IntentStore, credential_loader: Callable[[], Any]):
        self.store = store
        self.__credential_loader = credential_loader

    @property
    def complete(self) -> bool:
        return self.store.snapshot().lifecycle_state == "COMPLETE"

    def _require_lifecycle(self, *allowed: str) -> str:
        state = self.store.snapshot().lifecycle_state
        if state.startswith("HALTED_"):
            raise LifecycleHalted("LIFECYCLE_HALTED")
        if state not in allowed:
            raise LifecycleHalted("LIFECYCLE_STATE_MISMATCH")
        return state

    @staticmethod
    def _grid(value: Decimal, step: Decimal) -> bool:
        return value > 0 and value % step == 0

    @staticmethod
    def _validate_common(evidence: OfficialEvidence) -> None:
        if not evidence.connected:
            raise EvidenceViolation("STREAM_DISCONNECTED")
        if not evidence.gap_free:
            raise EvidenceViolation("STREAM_GAP")
        if not evidence.fresh:
            raise EvidenceViolation("STALE_EVIDENCE")
        if not 0 <= evidence.now_ms - evidence.server_time_ms <= _MAX_AGE_MS:
            raise EvidenceViolation("TEMPORAL_DISAGREEMENT")

    @classmethod
    def _validated_book(
        cls,
        evidence: OfficialEvidence,
        *,
        tick: Decimal,
        step: Decimal,
    ) -> dict[str, tuple[tuple[Decimal, Decimal], ...]]:
        parsed: dict[str, tuple[tuple[Decimal, Decimal], ...]] = {}
        for side in ("bids", "asks"):
            levels = tuple(
                (
                    _decimal(level["price"], f"book.{side}.price"),
                    _decimal(level["qty"], f"book.{side}.qty"),
                )
                for level in evidence.book[side]
            )
            if not levels:
                raise EvidenceViolation("DEPTH_INSUFFICIENT")
            if any(level_qty == 0 for _, level_qty in levels):
                raise EvidenceViolation("DEPTH_INSUFFICIENT")
            if any(
                not cls._grid(level_price, tick) or not cls._grid(level_qty, step)
                for level_price, level_qty in levels
            ):
                raise EvidenceViolation("BOOK_LEVEL_INVALID")
            prices = tuple(level_price for level_price, _ in levels)
            if side == "bids" and any(left <= right for left, right in zip(prices, prices[1:])):
                raise EvidenceViolation("BOOK_ORDER_INVALID")
            if side == "asks" and any(left >= right for left, right in zip(prices, prices[1:])):
                raise EvidenceViolation("BOOK_ORDER_INVALID")
            parsed[side] = levels
        if parsed["bids"][0][0] >= parsed["asks"][0][0]:
            raise EvidenceViolation("BOOK_CROSSED")
        return parsed

    @staticmethod
    def _past_expiry(evidence: OfficialEvidence, expiry_ms: int, *, strict: bool = False) -> bool:
        clocks = (evidence.server_time_ms, evidence.stream_event_ms, evidence.now_ms)
        return all(clock > expiry_ms for clock in clocks) if strict else any(clock >= expiry_ms for clock in clocks)

    def _validate_order_gate(
        self,
        evidence: OfficialEvidence,
        *,
        vector: Mapping[str, Any],
        qty: Decimal,
        price: Decimal,
    ) -> Decimal:
        self._validate_common(evidence)
        market = evidence.market
        config = market["tradingConfig"]
        if not market["active"]:
            raise EvidenceViolation("MARKET_INACTIVE")
        if market["isRfq"]:
            raise EvidenceViolation("RFQ_FORBIDDEN")
        if market["isOffHours"]:
            raise EvidenceViolation("OFF_HOURS_FORBIDDEN")
        if market["type"] != "PERPETUAL":
            raise EvidenceViolation("PERPETUAL_REQUIRED")
        if evidence.account_status != "ACTIVE":
            raise EvidenceViolation("ACCOUNT_INACTIVE")
        if evidence.balance["collateralName"] != market["collateralAssetName"]:
            raise EvidenceViolation("COLLATERAL_MISMATCH")
        minimum = _decimal(config["minOrderSize"], "minimum")
        step = _decimal(config["minOrderSizeChange"], "step")
        tick = _decimal(config["minPriceChange"], "tick")
        if minimum <= 0 or step <= 0 or tick <= 0 or minimum != step:
            raise EvidenceViolation("MINIMUM_STEP_MISMATCH")
        if qty != minimum or _decimal(vector["qty"], "order.qty") != minimum:
            raise EvidenceViolation("ONE_STEP_QTY_REQUIRED")
        if not self._grid(price, tick):
            raise EvidenceViolation("PRICE_OFF_GRID")
        if price != _decimal(vector["price"], "order.price"):
            raise EvidenceViolation("ORDER_VECTOR_MISMATCH")
        build_limit_ioc_order(vector, evidence.server_time_ms)
        notional = qty * price
        if notional > MAX_NOTIONAL_USD:
            raise EvidenceViolation("NOTIONAL_CAP")
        book = self._validated_book(evidence, tick=tick, step=step)
        if any(
            sum((level_qty for _, level_qty in book[side]), Decimal(0)) < step
            for side in ("bids", "asks")
        ):
            raise EvidenceViolation("DEPTH_INSUFFICIENT")
        executable_side = "asks" if vector["side"] == "BUY" else "bids"
        executable_qty = sum(
            (
                level_qty
                for level_price, level_qty in book[executable_side]
                if (level_price <= price if vector["side"] == "BUY" else level_price >= price)
            ),
            Decimal(0),
        )
        if executable_qty < qty:
            raise EvidenceViolation("DEPTH_INSUFFICIENT")
        fee = next((row for row in evidence.fees if row["market"] == evidence.market_name), None)
        fee_rate = _decimal(fee["takerFeeRate"], "fee") if fee is not None else Decimal(-1)
        if fee is None or fee_rate < 0 or fee_rate != _decimal(vector["fee"], "order.fee"):
            raise EvidenceViolation("FEE_MISMATCH")
        leverage = next((row for row in evidence.leverage if row["market"] == evidence.market_name), None)
        leverage_value = _decimal(leverage["leverage"], "leverage") if leverage is not None else Decimal(0)
        if leverage is None or not 0 < leverage_value <= _decimal(config["maxLeverage"], "maxLeverage"):
            raise EvidenceViolation("LEVERAGE_INVALID")
        required_balance = notional * (Decimal(1) + fee_rate)
        if _decimal(evidence.balance["availableForTrade"], "availableForTrade") < required_balance:
            raise EvidenceViolation("BALANCE_INSUFFICIENT")
        if evidence.open_orders:
            raise EvidenceViolation("OPEN_ORDER_PRESENT")
        return fee_rate

    def _validate_entry(self, evidence: OfficialEvidence, *, qty: Decimal, price: Decimal) -> None:
        if evidence.open_orders or evidence.positions:
            raise EvidenceViolation("NOT_FLAT")
        entry_fee = self._validate_order_gate(
            evidence, vector=evidence.entry_vector, qty=qty, price=price
        )
        close_qty = _decimal(evidence.close_vector["qty"], "close.qty")
        close_price = _decimal(evidence.close_vector["price"], "close.price")
        close_fee = self._validate_order_gate(
            evidence,
            vector=evidence.close_vector,
            qty=close_qty,
            price=close_price,
        )
        if close_qty != qty:
            raise EvidenceViolation("ONE_STEP_QTY_REQUIRED")
        required_balance = qty * price * (Decimal(1) + entry_fee)
        required_balance += close_qty * close_price * (Decimal(1) + close_fee)
        if _decimal(evidence.balance["availableForTrade"], "availableForTrade") < required_balance:
            raise EvidenceViolation("BALANCE_INSUFFICIENT")

    @staticmethod
    def _assert_binding(intent: Intent, evidence: OfficialEvidence) -> None:
        if intent.account_id != evidence.account_id or intent.l2_key != evidence.l2_key:
            raise EvidenceViolation("ACCOUNT_IDENTITY_MISMATCH")
        if intent.market != evidence.market_name:
            raise EvidenceViolation("MARKET_IDENTITY_MISMATCH")

    @staticmethod
    def _validate_intent_vector(intent: Intent, evidence: OfficialEvidence) -> None:
        vector = evidence.entry_vector if intent.kind == "ENTRY" else evidence.close_vector
        payload = build_limit_ioc_order(vector, evidence.server_time_ms)
        if (
            vector["nonce"] != intent.nonce
            or vector["externalId"] != intent.external_id
            or vector["market"] != intent.market
            or vector["side"] != intent.side
            or _decimal(vector["qty"], "order.qty") != intent.qty
            or _decimal(vector["price"], "order.price") != intent.price
            or vector["expiryEpochMillis"] != intent.expiry_ms
            or vector["reduceOnly"] is not intent.reduce_only
            or payload != intent.unsigned_api_payload
            or canonical_payload_digest(payload) != intent.payload_digest
        ):
            raise ContractViolation("ORDER_VECTOR_MISMATCH")

    def prepare_order(
        self,
        *,
        nonce: int | None,
        settlement_hash: int,
        market: str,
        side: str,
        qty: Decimal,
        price: Decimal,
        expiry_ms: int,
        reduce_only: bool,
        evidence: OfficialEvidence,
    ) -> Intent:
        nonce_value = _validate_nonce(nonce)
        if qty * price > MAX_NOTIONAL_USD:
            raise EvidenceViolation("NOTIONAL_CAP")
        external_id = str(settlement_hash)
        for existing in self.store._all():
            if existing.nonce == nonce_value:
                raise NonceViolation("NONCE_REUSE")
            if existing.external_id == external_id:
                raise IntentConflict("EXTERNAL_ID_REUSE")
        if any(existing.kind == "ENTRY" for existing in self.store._all()):
            raise LifecycleHalted("ENTRY_ALREADY_EXISTS")
        self._require_lifecycle("FLAT")
        self._validate_entry(evidence, qty=qty, price=price)
        vector = evidence.entry_vector
        if (
            nonce_value != vector["nonce"]
            or external_id != vector["externalId"]
            or market != vector["market"]
            or side != vector["side"]
            or expiry_ms != vector["expiryEpochMillis"]
            or reduce_only is not False
        ):
            raise ContractViolation("ORDER_VECTOR_MISMATCH")
        payload = build_limit_ioc_order(vector, evidence.server_time_ms)
        intent = Intent(
            id=external_id,
            kind="ENTRY",
            state="PREPARED",
            nonce=nonce_value,
            external_id=external_id,
            payload_digest=canonical_payload_digest(payload),
            unsigned_api_payload=payload,
            expiry_ms=expiry_ms,
            account_id=evidence.account_id,
            l2_key=evidence.l2_key,
            market=market,
            side=side,
            qty=qty,
            price=price,
            reduce_only=False,
        )
        self.store._insert(
            intent,
            expected_lifecycle="FLAT",
            next_lifecycle="ENTRY_PREPARED",
        )
        return intent

    def claim_for_dispatch(self, intent_id: str, *, evidence: OfficialEvidence) -> Intent:
        intent = self.store.get(intent_id)
        if intent.state != "PREPARED":
            raise LifecycleHalted("INTENT_NOT_PREPARED")
        self._validate_common(evidence)
        if self._past_expiry(evidence, intent.expiry_ms):
            raise LifecycleHalted("EXPIRED")
        lifecycle_transition = {
            "ENTRY": ("ENTRY_PREPARED", "ENTRY_AMBIGUOUS"),
            "CANCEL": ("CANCEL_PREPARED", "CANCEL_AMBIGUOUS"),
            "MASS_CANCEL": ("CANCEL_PREPARED", "CANCEL_AMBIGUOUS"),
            "CLOSE": ("CLOSE_PREPARED", "CLOSE_AMBIGUOUS"),
        }
        if intent.kind not in lifecycle_transition:
            raise LifecycleHalted("INTENT_KIND_INVALID")
        expected_lifecycle, next_lifecycle = lifecycle_transition[intent.kind]
        self._require_lifecycle(expected_lifecycle)
        self._assert_binding(intent, evidence)
        if intent.kind == "ENTRY":
            self._validate_entry(evidence, qty=intent.qty, price=intent.price)
            self._validate_intent_vector(intent, evidence)
        elif intent.kind == "CLOSE":
            self._validate_close_evidence(intent, evidence)
        elif intent.kind in {"CANCEL", "MASS_CANCEL"}:
            target = self.store.get(intent.target_id or "")
            status, history = self._matching_order(target, evidence)
            if (
                status is not None
                and history is not None
                and status == history
                and history["status"] in {"FILLED", "CANCELLED"}
            ):
                raise LifecycleHalted("TERMINAL_EVIDENCE_REQUIRES_RECONCILIATION")
        self.store._claim(
            intent.id,
            expected_lifecycle=expected_lifecycle,
            next_lifecycle=next_lifecycle,
        )
        return self.store.get(intent.id)

    def prepare_cancellation(
        self, entry_id: str, *, kind: str, evidence: OfficialEvidence
    ) -> Intent:
        if kind not in {"CANCEL", "MASS_CANCEL"}:
            raise ContractViolation("CANCEL_KIND_INVALID")
        entry = self.store.get(entry_id)
        if entry.kind != "ENTRY" or entry.state != "CLAIMED":
            raise LifecycleHalted("ENTRY_NOT_CLAIMED")
        self._require_lifecycle("ENTRY_AMBIGUOUS")
        self._validate_common(evidence)
        self._assert_binding(entry, evidence)
        if any(
            existing.kind in {"CANCEL", "MASS_CANCEL"} and existing.target_id == entry.id
            for existing in self.store._all()
        ):
            raise IntentConflict("CANCELLATION_ALREADY_EXISTS")
        if kind == "CANCEL":
            request = {
                "method": "DELETE",
                "path": "/user/order",
                "query": {"externalId": entry.external_id},
                "json": None,
            }
        else:
            request = {
                "method": "POST",
                "path": "/user/order/massCancel",
                "query": {},
                "json": {"externalOrderIds": [entry.external_id], "cancelAll": False},
            }
        intent_id = f"{kind}:{entry.external_id}"
        intent = Intent(
            id=intent_id,
            kind=kind,
            state="PREPARED",
            nonce=None,
            external_id=intent_id,
            payload_digest=canonical_payload_digest(request),
            unsigned_api_payload=request,
            expiry_ms=entry.expiry_ms,
            account_id=entry.account_id,
            l2_key=entry.l2_key,
            market=entry.market,
            side=None,
            qty=Decimal(0),
            price=Decimal(0),
            reduce_only=False,
            target_id=entry.id,
            target_external_id=entry.external_id,
            api_request=request,
        )
        self.store._insert(
            intent,
            expected_lifecycle="ENTRY_AMBIGUOUS",
            next_lifecycle="CANCEL_PREPARED",
        )
        return intent

    def _position(self, evidence: OfficialEvidence) -> dict[str, Any]:
        if len(evidence.positions) != 1 or len(evidence.stream_positions) != 1:
            raise LifecycleHalted("AUTHORITATIVE_POSITION_REQUIRED")
        position = evidence.positions[0]
        if position != evidence.stream_positions[0]:
            raise LifecycleHalted("POSITION_DISAGREEMENT")
        return position

    def _validate_close_evidence(self, intent: Intent, evidence: OfficialEvidence) -> None:
        self._assert_binding(intent, evidence)
        if evidence.open_orders:
            raise LifecycleHalted("OPEN_ORDER_PRESENT")
        self._validate_order_gate(
            evidence,
            vector=evidence.close_vector,
            qty=intent.qty,
            price=intent.price,
        )
        self._validate_intent_vector(intent, evidence)
        position = self._position(evidence)
        entry = self.store.get(intent.target_id or "")
        self._validate_position_binding(entry, position, evidence)
        size = _decimal(position["size"], "position.size")
        if size != intent.qty:
            raise LifecycleHalted("POSITION_SIZE_MISMATCH")
        if abs(size * intent.price) > MAX_NOTIONAL_USD:
            raise LifecycleHalted("NOTIONAL_CAP")

    def prepare_close(
        self,
        *,
        entry_id: str,
        evidence: OfficialEvidence,
        nonce: int,
        settlement_hash: int,
        price: Decimal,
        expiry_ms: int,
    ) -> Intent:
        entry = self.store.get(entry_id)
        if entry.kind != "ENTRY" or entry.state != "ENTRY_RECONCILED":
            raise LifecycleHalted("ENTRY_NOT_RECONCILED")
        if any(intent.state in {"PREPARED", "CLAIMED"} for intent in self.store._all()):
            raise LifecycleHalted("OUTSTANDING_INTENT")
        if any(existing.kind == "CLOSE" for existing in self.store._all()):
            raise LifecycleHalted("CLOSE_ALREADY_EXISTS")
        self._require_lifecycle("EXPOSED")
        self._assert_binding(entry, evidence)
        self._validate_common(evidence)
        if evidence.open_orders:
            raise LifecycleHalted("OPEN_ORDER_PRESENT")
        position = self._position(evidence)
        qty = _decimal(position["size"], "position.size")
        if abs(qty * price) > MAX_NOTIONAL_USD:
            raise LifecycleHalted("NOTIONAL_CAP")
        self._validate_position_binding(entry, position, evidence)
        side = "SELL" if position["side"] == "LONG" else "BUY"
        nonce_value = _validate_nonce(nonce)
        external_id = str(settlement_hash)
        for existing in self.store._all():
            if existing.nonce == nonce_value:
                raise NonceViolation("NONCE_REUSE")
            if existing.external_id == external_id:
                raise IntentConflict("EXTERNAL_ID_REUSE")
        vector = evidence.close_vector
        if (
            nonce_value != vector["nonce"]
            or external_id != vector["externalId"]
            or side != vector["side"]
            or qty != _decimal(vector["qty"], "close.qty")
            or price != _decimal(vector["price"], "close.price")
            or expiry_ms != vector["expiryEpochMillis"]
            or vector["reduceOnly"] is not True
        ):
            raise ContractViolation("CLOSE_VECTOR_MISMATCH")
        self._validate_order_gate(evidence, vector=vector, qty=qty, price=price)
        payload = build_limit_ioc_order(vector, evidence.server_time_ms)
        intent = Intent(
            id=external_id,
            kind="CLOSE",
            state="PREPARED",
            nonce=nonce_value,
            external_id=external_id,
            payload_digest=canonical_payload_digest(payload),
            unsigned_api_payload=payload,
            expiry_ms=expiry_ms,
            account_id=evidence.account_id,
            l2_key=evidence.l2_key,
            market=evidence.market_name,
            side=side,
            qty=qty,
            price=price,
            reduce_only=True,
            target_id=entry.id,
            target_external_id=entry.external_id,
        )
        self.store._insert(
            intent,
            expected_lifecycle="EXPOSED",
            next_lifecycle="CLOSE_PREPARED",
        )
        return intent

    @staticmethod
    def _matching_order(intent: Intent, evidence: OfficialEvidence) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        status = evidence.order_status if evidence.order_status and evidence.order_status["externalId"] == intent.external_id else None
        history = next((row for row in evidence.order_history if row["externalId"] == intent.external_id), None)
        return status, history

    @staticmethod
    def _matching_fills(order: Mapping[str, Any], evidence: OfficialEvidence) -> tuple[dict[str, Any], ...]:
        return tuple(row for row in evidence.fills if row["orderId"] == order["id"])

    @staticmethod
    def _validate_order_binding(intent: Intent, order: Mapping[str, Any]) -> None:
        payload = intent.unsigned_api_payload
        expected = {
            "accountId": intent.account_id,
            "externalId": intent.external_id,
            "market": intent.market,
            "type": payload["type"],
            "side": intent.side,
            "price": str(intent.price),
            "qty": str(intent.qty),
            "reduceOnly": intent.reduce_only,
            "postOnly": payload["postOnly"],
            "expiryTime": intent.expiry_ms,
            "timeInForce": payload["timeInForce"],
        }
        if any(order[field] != value for field, value in expected.items()):
            raise LifecycleHalted("ORDER_BINDING_MISMATCH")

    def _validated_fills(
        self, intent: Intent, order: Mapping[str, Any], evidence: OfficialEvidence
    ) -> tuple[dict[str, Any], ...]:
        self._validate_order_binding(intent, order)
        fills = self._matching_fills(order, evidence)
        fee_rate = _decimal(intent.unsigned_api_payload["fee"], "order.fee")
        total_fee = Decimal(0)
        total_qty = Decimal(0)
        total_value = Decimal(0)
        for fill in fills:
            fill_price = _decimal(fill["price"], "fill.price")
            fill_qty = _decimal(fill["qty"], "fill.qty")
            fill_value = _decimal(fill["value"], "fill.value")
            fill_fee = _decimal(fill["fee"], "fill.fee")
            if (
                fill["accountId"] != intent.account_id
                or fill["market"] != intent.market
                or fill["side"] != intent.side
                or fill["orderId"] != order["id"]
                or fill_qty <= 0
                or fill_price <= 0
                or fill["isTaker"] is not True
                or fill["tradeType"] != "TRADE"
            ):
                raise LifecycleHalted("FILL_BINDING_MISMATCH")
            if (
                fill_value != fill_price * fill_qty
                or fill_fee < 0
                or fill_fee != fill_value * fee_rate
                or (intent.side == "BUY" and fill_price > intent.price)
                or (intent.side == "SELL" and fill_price < intent.price)
            ):
                raise LifecycleHalted("FILL_ARITHMETIC_MISMATCH")
            total_fee += fill_fee
            total_qty += fill_qty
            total_value += fill_value
        if _decimal(order["payedFee"], "order.payedFee") != total_fee:
            raise LifecycleHalted("FILL_ARITHMETIC_MISMATCH")
        filled_qty = _decimal(order["filledQty"], "order.filledQty")
        cancelled_qty = _decimal(order["cancelledQty"], "order.cancelledQty")
        average_price = (
            _decimal(order["averagePrice"], "order.averagePrice")
            if order["averagePrice"] is not None
            else None
        )
        if order["status"] == "FILLED":
            if (
                total_qty != intent.qty
                or filled_qty != intent.qty
                or cancelled_qty != 0
                or average_price is None
                or average_price != total_value / total_qty
            ):
                raise LifecycleHalted("ORDER_ARITHMETIC_MISMATCH")
        elif order["status"] == "PARTIALLY_FILLED":
            if (
                not 0 < total_qty <= intent.qty
                or filled_qty != total_qty
                or cancelled_qty < 0
                or filled_qty + cancelled_qty > intent.qty
                or average_price is None
                or average_price != total_value / total_qty
            ):
                raise LifecycleHalted("ORDER_ARITHMETIC_MISMATCH")
        elif order["status"] == "CANCELLED":
            if (
                fills
                or filled_qty != 0
                or cancelled_qty != intent.qty
                or average_price is not None
            ):
                raise LifecycleHalted("ORDER_ARITHMETIC_MISMATCH")
        return fills

    def _validate_position_binding(
        self,
        intent: Intent,
        position: Mapping[str, Any],
        evidence: OfficialEvidence,
    ) -> None:
        expected_side = "LONG" if intent.side == "BUY" else "SHORT"
        if (
            position["accountId"] != intent.account_id
            or position["market"] != intent.market
            or position["side"] != expected_side
            or position["status"] != "OPENED"
        ):
            raise LifecycleHalted("POSITION_BINDING_MISMATCH")
        size = _decimal(position["size"], "position.size")
        value = _decimal(position["value"], "position.value")
        open_price = _decimal(position["openPrice"], "position.openPrice")
        mark_price = _decimal(position["markPrice"], "position.markPrice")
        unrealised_pnl = _decimal(position["unrealisedPnl"], "position.unrealisedPnl")
        position_leverage = _decimal(position["leverage"], "position.leverage")
        account_leverage = next(
            (row for row in evidence.leverage if row["market"] == intent.market), None
        )
        if account_leverage is None:
            raise LifecycleHalted("POSITION_EVIDENCE_MISMATCH")
        history = next(
            (row for row in evidence.order_history if row["externalId"] == intent.external_id),
            None,
        )
        if history is None:
            raise LifecycleHalted("POSITION_EVIDENCE_MISMATCH")
        fills = self._validated_fills(intent, history, evidence)
        fill_qty = sum((_decimal(row["qty"], "fill.qty") for row in fills), Decimal(0))
        weighted_value = sum(
            (
                _decimal(row["price"], "fill.price")
                * _decimal(row["qty"], "fill.qty")
                for row in fills
            ),
            Decimal(0),
        )
        expected_unrealised_pnl = (
            size * (mark_price - open_price)
            if expected_side == "LONG"
            else size * (open_price - mark_price)
        )
        if (
            size <= 0
            or value <= 0
            or open_price <= 0
            or mark_price <= 0
            or position_leverage <= 0
            or not fills
            or size != fill_qty
            or value != abs(size * mark_price)
            or open_price != weighted_value / fill_qty
            or mark_price != _decimal(evidence.market["marketStats"]["markPrice"], "market.markPrice")
            or unrealised_pnl != expected_unrealised_pnl
            or position_leverage != _decimal(account_leverage["leverage"], "account.leverage")
        ):
            raise LifecycleHalted("POSITION_EVIDENCE_MISMATCH")
        if value > MAX_NOTIONAL_USD:
            raise LifecycleHalted("NOTIONAL_CAP")

    def _reconcile_entry(self, intent: Intent, evidence: OfficialEvidence) -> ReconciliationResult:
        if evidence.open_orders:
            raise LifecycleHalted("ORDER_OPEN_CONTRADICTION")
        status, history = self._matching_order(intent, evidence)
        if status is None and history is None and not evidence.fills and not evidence.positions:
            state = self.store.snapshot().lifecycle_state
            return ReconciliationResult(state, state == "COMPLETE")
        if status is None or history is None or status != history:
            raise LifecycleHalted("STATUS_HISTORY_CONTRADICTION")
        fills = self._validated_fills(intent, history, evidence)
        filled_qty = sum((_decimal(row["qty"], "fill.qty") for row in fills), Decimal(0))
        if history["status"] not in {"FILLED", "PARTIALLY_FILLED"} or not fills:
            raise LifecycleHalted("ORDER_FILL_CONTRADICTION")
        position = self._position(evidence)
        self._validate_position_binding(intent, position, evidence)
        position_qty = _decimal(position["size"], "position.size")
        if filled_qty != _decimal(history["filledQty"], "order.filledQty") or position_qty != filled_qty:
            raise LifecycleHalted("FILL_POSITION_CONTRADICTION")
        minimum = _decimal(evidence.market["tradingConfig"]["minOrderSize"], "minimum")
        step = _decimal(evidence.market["tradingConfig"]["minOrderSizeChange"], "step")
        if position_qty < minimum and position_qty * 2 == minimum:
            lifecycle_state = "HALTED_SUB_MINIMUM_RESIDUAL"
        elif not self._grid(position_qty, step):
            lifecycle_state = "HALTED_OFF_GRID_RESIDUAL"
        elif position_qty < minimum:
            lifecycle_state = "HALTED_SUB_MINIMUM_RESIDUAL"
        elif history["status"] == "PARTIALLY_FILLED":
            lifecycle_state = "HALTED_PARTIAL_FILL_UNCERTAIN"
        else:
            lifecycle_state = "EXPOSED"
        self.store._set_states({intent.id: "ENTRY_RECONCILED"}, lifecycle_state)
        return ReconciliationResult(
            lifecycle_state,
            False,
            filled_qty=filled_qty,
            position_qty=position_qty,
            reconciled_external_ids=frozenset({intent.external_id}),
            reconciled_fill_ids=frozenset(row["id"] for row in fills),
        )

    def _reconcile_cancel(self, intent: Intent, evidence: OfficialEvidence) -> ReconciliationResult:
        target = self.store.get(intent.target_id or "")
        superseded = intent.state == "PREPARED"
        status, history = self._matching_order(target, evidence)
        if status is None or history is None or status != history:
            raise LifecycleHalted("CANCEL_UNRESOLVED")
        fills = self._validated_fills(target, history, evidence)
        if history["status"] == "CANCELLED":
            if (
                fills
                or evidence.positions
                or evidence.open_orders
                or _decimal(history["filledQty"], "order.filledQty") != 0
                or _decimal(history["cancelledQty"], "order.cancelledQty") != target.qty
            ):
                raise LifecycleHalted("CANCEL_NO_FILL_CONTRADICTION")
            if intent.state in {"CLAIMED", "PREPARED"}:
                action_state = (
                    "SUPERSEDED_NOT_DISPATCHED"
                    if superseded
                    else "RECONCILED_CANCELLED_NO_FILL"
                )
                self.store._set_states(
                    {intent.id: action_state, target.id: "ENTRY_CANCELLED_NO_FILL"},
                    "FLAT_PENDING_EXPIRY",
                )
                return ReconciliationResult(
                    "FLAT_PENDING_EXPIRY", False, reconciled_external_ids=frozenset({target.external_id})
                )
            if (
                len(evidence.order_history) != 1
                or evidence.order_history[0]["externalId"] != target.external_id
                or evidence.order_status is None
                or evidence.order_status["externalId"] != target.external_id
            ):
                raise LifecycleHalted("FINAL_HISTORY_MISMATCH")
            all_intents = self.store._all()
            final_safe = (
                self._past_expiry(evidence, max(row.expiry_ms for row in all_intents), strict=True)
                and not evidence.fills
                and not evidence.positions
                and not evidence.open_orders
                and all(row.state not in {"PREPARED", "CLAIMED"} for row in all_intents)
            )
            lifecycle_state = "COMPLETE" if final_safe else "FLAT_PENDING_EXPIRY"
            if final_safe:
                self.store._set_states({}, lifecycle_state)
            return ReconciliationResult(
                lifecycle_state,
                final_safe,
                reconciled_external_ids=frozenset({target.external_id}),
            )
        if history["status"] != "FILLED":
            raise LifecycleHalted("CANCEL_UNRESOLVED")
        if not fills:
            raise LifecycleHalted("CANCEL_FILL_MISSING")
        position = self._position(evidence)
        self._validate_position_binding(target, position, evidence)
        filled_qty = sum((_decimal(row["qty"], "fill.qty") for row in fills), Decimal(0))
        position_qty = _decimal(position["size"], "position.size")
        if (
            filled_qty != _decimal(history["filledQty"], "order.filledQty")
            or filled_qty != position_qty
            or filled_qty != target.qty
        ):
            raise LifecycleHalted("CANCEL_FILL_POSITION_CONTRADICTION")
        self.store._set_states(
            {
                intent.id: "SUPERSEDED_NOT_DISPATCHED" if superseded else "RECONCILED_NO_CANCEL_EFFECT",
                target.id: "ENTRY_RECONCILED",
            },
            "EXPOSED",
        )
        return ReconciliationResult(
            "EXPOSED",
            False,
            filled_qty=filled_qty,
            position_qty=position_qty,
            reconciled_external_ids=frozenset({target.external_id}),
            reconciled_fill_ids=frozenset(row["id"] for row in fills),
        )

    def _final_identities(self, evidence: OfficialEvidence) -> tuple[frozenset[str], frozenset[int]]:
        order_intents = tuple(intent for intent in self.store._all() if intent.kind in {"ENTRY", "CLOSE"})
        expected_external = frozenset(intent.external_id for intent in order_intents)
        history_by_external = {row["externalId"]: row for row in evidence.order_history}
        if frozenset(history_by_external) != expected_external:
            raise LifecycleHalted("FINAL_HISTORY_MISMATCH")
        fill_ids: set[int] = set()
        for external_id in expected_external:
            order = history_by_external[external_id]
            intent = next(row for row in order_intents if row.external_id == external_id)
            matches = self._validated_fills(intent, order, evidence)
            if order["status"] != "FILLED" or not matches:
                raise LifecycleHalted("FINAL_FILL_MISMATCH")
            filled_qty = sum((_decimal(row["qty"], "fill.qty") for row in matches), Decimal(0))
            if filled_qty != _decimal(order["filledQty"], "order.filledQty") or filled_qty != intent.qty:
                raise LifecycleHalted("FINAL_FILL_MISMATCH")
            fill_ids.update(row["id"] for row in matches)
        if fill_ids != {row["id"] for row in evidence.fills}:
            raise LifecycleHalted("UNRECONCILED_FILL")
        return expected_external, frozenset(fill_ids)

    def _reconcile_close(self, intent: Intent, evidence: OfficialEvidence) -> ReconciliationResult:
        if evidence.open_orders:
            raise LifecycleHalted("CLOSE_ORDER_OPEN_CONTRADICTION")
        status, history = self._matching_order(intent, evidence)
        if status is None or history is None or status != history:
            raise LifecycleHalted("CLOSE_STATUS_HISTORY_CONTRADICTION")
        if history["status"] == "REJECTED":
            self.store._set_states({intent.id: "CLOSE_REJECTED"}, "HALTED_CLOSE_REJECTED")
            return ReconciliationResult("HALTED_CLOSE_REJECTED", False)
        if history["status"] != "FILLED":
            raise LifecycleHalted("CLOSE_NOT_FILLED")
        fills = self._validated_fills(intent, history, evidence)
        if not fills:
            raise LifecycleHalted("CLOSE_FILL_MISSING")
        close_qty = sum((_decimal(row["qty"], "fill.qty") for row in fills), Decimal(0))
        if close_qty != _decimal(history["filledQty"], "order.filledQty") or close_qty != intent.qty:
            raise LifecycleHalted("CLOSE_FILL_MISMATCH")
        target = self.store.get(intent.target_id or "")
        _, entry_history = self._matching_order(target, evidence)
        if entry_history is None:
            raise LifecycleHalted("ENTRY_HISTORY_MISSING")
        entry_fills = self._validated_fills(target, entry_history, evidence)
        entry_qty = sum((_decimal(row["qty"], "fill.qty") for row in entry_fills), Decimal(0))
        if (
            entry_qty != _decimal(entry_history["filledQty"], "entry.filledQty")
            or entry_qty != target.qty
            or entry_qty != close_qty
        ):
            raise LifecycleHalted("ENTRY_CLOSE_FILL_MISMATCH")
        if evidence.positions or evidence.stream_positions:
            raise LifecycleHalted("CLOSE_POSITION_NOT_FLAT")
        external_ids, fill_ids = self._final_identities(evidence)
        self.store._set_states({intent.id: "CLOSE_RECONCILED"}, "EXPOSED")
        all_intents = self.store._all()
        past_expiry = self._past_expiry(evidence, max(row.expiry_ms for row in all_intents), strict=True)
        final_safe = (
            past_expiry
            and evidence.connected
            and evidence.gap_free
            and evidence.fresh
            and not evidence.open_orders
            and not evidence.positions
            and all(row.state not in {"PREPARED", "CLAIMED"} for row in all_intents)
        )
        lifecycle_state = "COMPLETE" if final_safe else "EXPOSED"
        if lifecycle_state == "COMPLETE":
            self.store._set_states({}, "COMPLETE")
        return ReconciliationResult(
            lifecycle_state,
            lifecycle_state == "COMPLETE",
            reconciled_external_ids=external_ids,
            reconciled_fill_ids=fill_ids,
        )

    def _all_fills_bind_persisted_orders(self, evidence: OfficialEvidence) -> bool:
        persisted_external_ids = {
            intent.external_id for intent in self.store._all() if intent.kind in {"ENTRY", "CLOSE"}
        }
        orders_by_id: dict[Any, str] = {}
        observed_orders = (*evidence.open_orders, *evidence.order_history)
        if evidence.order_status is not None:
            observed_orders = (*observed_orders, evidence.order_status)
        for order in observed_orders:
            existing = orders_by_id.setdefault(order["id"], order["externalId"])
            if existing != order["externalId"]:
                return False
        return all(
            fill["orderId"] in orders_by_id
            and orders_by_id[fill["orderId"]] in persisted_external_ids
            for fill in evidence.fills
        )

    @staticmethod
    def _validate_reconciliation_route(intent: Intent, lifecycle_state: str) -> None:
        allowed_pairs = {
            ("ENTRY", "CLAIMED", "ENTRY_AMBIGUOUS"),
            ("CANCEL", "CLAIMED", "CANCEL_AMBIGUOUS"),
            ("MASS_CANCEL", "CLAIMED", "CANCEL_AMBIGUOUS"),
            ("CANCEL", "PREPARED", "CANCEL_PREPARED"),
            ("MASS_CANCEL", "PREPARED", "CANCEL_PREPARED"),
            ("CANCEL", "RECONCILED_CANCELLED_NO_FILL", "FLAT_PENDING_EXPIRY"),
            ("MASS_CANCEL", "RECONCILED_CANCELLED_NO_FILL", "FLAT_PENDING_EXPIRY"),
            ("CANCEL", "SUPERSEDED_NOT_DISPATCHED", "FLAT_PENDING_EXPIRY"),
            ("MASS_CANCEL", "SUPERSEDED_NOT_DISPATCHED", "FLAT_PENDING_EXPIRY"),
            ("CLOSE", "CLAIMED", "CLOSE_AMBIGUOUS"),
            ("CLOSE", "CLOSE_RECONCILED", "EXPOSED"),
        }
        if (intent.kind, intent.state, lifecycle_state) not in allowed_pairs:
            raise LifecycleHalted("ACTION_ROUTE_MISMATCH")

    def reconcile(self, intent_id: str, evidence: OfficialEvidence) -> ReconciliationResult:
        intent = self.store.get(intent_id)
        self._validate_common(evidence)
        current_lifecycle = self.store.snapshot().lifecycle_state
        if current_lifecycle.startswith("HALTED_"):
            raise LifecycleHalted("LIFECYCLE_HALTED")
        try:
            recheck_states = {"CLOSE_RECONCILED", "RECONCILED_CANCELLED_NO_FILL"}
            prepared_cancel = intent.kind in {"CANCEL", "MASS_CANCEL"} and intent.state == "PREPARED"
            superseded_cancel = (
                intent.kind in {"CANCEL", "MASS_CANCEL"}
                and intent.state == "SUPERSEDED_NOT_DISPATCHED"
            )
            if (
                intent.state != "CLAIMED"
                and intent.state not in recheck_states
                and not prepared_cancel
                and not superseded_cancel
            ):
                raise LifecycleHalted("INTENT_NOT_CLAIMED")
            self._validate_reconciliation_route(intent, current_lifecycle)
            self._assert_binding(intent, evidence)
            if not self._all_fills_bind_persisted_orders(evidence):
                self.store._set_states({}, "HALTED_UNEXPLAINED_FILL")
                raise LifecycleHalted("UNEXPLAINED_FILL")
            if intent.kind == "ENTRY":
                return self._reconcile_entry(intent, evidence)
            if intent.kind in {"CANCEL", "MASS_CANCEL"}:
                return self._reconcile_cancel(intent, evidence)
            if intent.kind == "CLOSE":
                return self._reconcile_close(intent, evidence)
            raise LifecycleHalted("INTENT_KIND_INVALID")
        except (ContractViolation, EvidenceViolation, LifecycleHalted) as exc:
            transient = {
                "INTENT_NOT_CLAIMED",
                "ACTION_ROUTE_MISMATCH",
                "CANCEL_UNRESOLVED",
                "CANCEL_FILL_MISSING",
                "CLOSE_NOT_FILLED",
            }
            state = self.store.snapshot().lifecycle_state
            if str(exc) not in transient and not state.startswith("HALTED_"):
                self.store._set_states({}, "HALTED_RECONCILIATION_CONTRADICTION")
            raise

    def next_write(self, evidence: OfficialEvidence) -> None:
        del evidence
        return None
