"""Unauthenticated Extended public market-data adapter."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from urllib.parse import quote

import aiohttp

from risex_farmer.config import EXTENDED_UNIVERSE_REQUEST_TIMEOUT_SECONDS
from risex_farmer.models import (
    BookLevel,
    BookDelta,
    CanonicalMarket,
    ContractType,
    FundingAccrualMethod,
    FundingCashQuote,
    FundingQuality,
    FundingSettlement,
    MarketType,
    MarketVolume,
    OrderBook,
    Side,
    SettlementStatus,
    TradeEvidence,
    Venue,
)

from .base import (
    PublicAdapter,
    PublicDataUnavailable,
    PublicHeartbeatAction,
    WebSocketFrameAction,
    decimal_value,
    require_list,
    require_mapping,
    timestamp,
)


class ExtendedAdapter(PublicAdapter):
    SOURCE = "https://api.docs.extended.exchange/"

    def __init__(self, session: Any) -> None:
        # Deliberately no API-key or Authorization support: PAPER-002 is public only.
        super().__init__(
            session,
            "https://api.starknet.extended.exchange",
            "wss://api.starknet.extended.exchange/stream.extended.exchange/v1",
        )

    async def fetch_markets(self) -> tuple[CanonicalMarket, ...]:
        markets, _ = await self.fetch_catalog()
        return markets

    async def fetch_catalog(
        self,
    ) -> tuple[tuple[CanonicalMarket, ...], tuple[MarketVolume, ...]]:
        """Normalize both catalog views from the venue's single public payload."""
        payload = await self._catalog_payload(
            timeout_seconds=EXTENDED_UNIVERSE_REQUEST_TIMEOUT_SECONDS
        )
        return self._normalize_catalog(payload)

    async def fetch_required_catalog(
        self, venue_symbols: tuple[str, ...]
    ) -> tuple[tuple[CanonicalMarket, ...], tuple[MarketVolume, ...]]:
        requested = tuple(dict.fromkeys(venue_symbols))
        if not requested:
            return (), ()
        payload = await self._catalog_payload(
            params=[("market", symbol) for symbol in requested]
        )
        markets, volumes = self._normalize_catalog(payload)
        returned = tuple(market.venue_symbol for market in markets)
        if len(set(returned)) != len(returned) or set(returned) != set(requested):
            raise ValueError("required Extended metadata response is incomplete")
        by_name = {market.venue_symbol: market for market in markets}
        volume_by_name = {row.canonical_market: row for row in volumes}
        return (
            tuple(by_name[symbol] for symbol in requested),
            tuple(volume_by_name[symbol] for symbol in requested),
        )

    async def _catalog_payload(
        self, *, params: list[tuple[str, str]] | None = None,
        timeout_seconds: int | None = None,
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {"params": params}
        if timeout_seconds is not None:
            kwargs["timeout"] = aiohttp.ClientTimeout(total=timeout_seconds)
        try:
            async with self._session.get(
                f"{self.rest_base}/api/v1/info/markets", **kwargs
            ) as response:
                response.raise_for_status()
                payload = json.loads(await response.text(), parse_float=Decimal)
        except aiohttp.ClientResponseError as exc:
            if exc.status in {401, 403}:
                self.public_data_available = False
                raise PublicDataUnavailable(
                    "venue rejected unauthenticated public market data"
                ) from exc
            raise
        if not isinstance(payload, dict):
            raise ValueError("public endpoint returned a non-object payload")
        self.public_data_available = True
        return payload

    def _normalize_catalog(
        self, payload: dict[str, Any]
    ) -> tuple[tuple[CanonicalMarket, ...], tuple[MarketVolume, ...]]:
        rows = require_list(payload.get("data"), "data")
        observed_at = datetime.now(UTC)
        return (
            tuple(self.normalize_market(require_mapping(row, "market")) for row in rows),
            tuple(self.normalize_volume(row, observed_at=observed_at) for row in rows),
        )

    async def fetch_volumes(self) -> tuple[MarketVolume, ...]:
        _, volumes = await self.fetch_catalog()
        return volumes

    def normalize_market(self, row: Any) -> CanonicalMarket:
        row = require_mapping(row, "market")
        config = require_mapping(row.get("tradingConfig"), "market.tradingConfig")
        is_perpetual = row.get("type") == "PERPETUAL"
        is_crypto = row.get("category") == "Crypto"
        status = str(row.get("status", ""))
        return CanonicalMarket(
            canonical_asset=str(row["assetName"]),
            venue=Venue.EXTENDED,
            venue_symbol=str(row["name"]),
            market_type=MarketType.PERPETUAL if is_perpetual else MarketType.SPOT,
            contract_type=(
                ContractType.LINEAR
                if is_perpetual and is_crypto
                else ContractType.OTHER
            ),
            base_multiplier=Decimal("1") if is_perpetual and is_crypto else None,
            quote_asset=str(row["collateralAssetName"]),
            settlement_asset=str(row["collateralAssetName"]),
            tick_size_raw=decimal_value(config["minPriceChange"], "tradingConfig.minPriceChange"),
            quantity_step_raw=decimal_value(
                config["minOrderSizeChange"], "tradingConfig.minOrderSizeChange"
            ),
            minimum_quantity_raw=decimal_value(config["minOrderSize"], "tradingConfig.minOrderSize"),
            minimum_notional_usd=Decimal("0"),
            minimum_fee_notional_usd=None,
            is_active=bool(row.get("active")) and status == "ACTIVE" and is_crypto,
            is_rfq=bool(row.get("isRfq")),
            is_off_hours=bool(row.get("isOffHours")),
        )

    def normalize_volume(self, row: Any, *, observed_at: datetime) -> MarketVolume:
        row = require_mapping(row, "market")
        stats = require_mapping(row.get("marketStats"), "market.marketStats")
        raw_volume = stats.get("dailyVolume")
        return MarketVolume(
            Venue.EXTENDED,
            str(row["name"]),
            None if raw_volume is None else decimal_value(raw_volume, "dailyVolume"),
            observed_at,
            self.SOURCE,
        )

    async def fetch_book(self, venue_symbol: str) -> OrderBook:
        payload = await self._get_json(
            f"/api/v1/info/markets/{quote(venue_symbol, safe='')}/orderbook"
        )
        return self.normalize_book(payload.get("data"), observed_at=datetime.now(UTC))

    def normalize_book(self, data: Any, *, observed_at: datetime, sequence: int | None = None) -> OrderBook:
        data = require_mapping(data, "orderbook")

        def levels(name: str) -> tuple[BookLevel, ...]:
            return tuple(
                BookLevel(
                    decimal_value(require_mapping(level, name)["price"], f"{name}.price"),
                    decimal_value(require_mapping(level, name)["qty"], f"{name}.qty"),
                )
                for level in require_list(data.get(name), name)
            )

        return OrderBook(
            Venue.EXTENDED,
            str(data["market"]),
            levels("bid"),
            levels("ask"),
            observed_at,
            sequence,
        )

    def normalize_book_message(self, payload: Any) -> OrderBook | BookDelta:
        message = require_mapping(payload, "orderbook message")
        data = require_mapping(message.get("data"), "orderbook message.data")
        observed_at = timestamp(message["ts"], "milliseconds")
        sequence = int(message["seq"])

        def levels(name: str) -> tuple[BookLevel, ...]:
            parsed: list[BookLevel] = []
            for raw_level in require_list(data.get(name, []), name):
                level = require_mapping(raw_level, name)
                quantity = level["c"] if "c" in level else level["q"]
                parsed.append(
                    BookLevel(
                        decimal_value(level["p"], f"{name}.p"),
                        decimal_value(quantity, f"{name}.c"),
                    )
                )
            return tuple(parsed)

        market = str(data["m"])
        bids = levels("b")
        asks = levels("a")
        if message.get("type") == "SNAPSHOT":
            return OrderBook(Venue.EXTENDED, market, bids, asks, observed_at, sequence)
        return BookDelta(Venue.EXTENDED, market, bids, asks, observed_at, sequence)

    def normalize_trade(
        self, payload: Any, *, received_at: datetime, session_id: str, ordinal: int
    ) -> TradeEvidence:
        trade = require_mapping(payload, "trade")
        market = str(trade["m"])
        price = decimal_value(trade["p"], "trade.p")
        quantity = decimal_value(trade["q"], "trade.q")
        raw_time = trade["T"]
        side = Side(str(trade["S"]))
        trade_id = trade.get("i")
        key = (
            f"EXTENDED|{market}|{trade_id}"
            if trade_id is not None
            else f"EXTENDED|{session_id}|{raw_time}|{market}|{price}|{quantity}|{side}|{ordinal}"
        )
        trade_type = str(trade.get("tT", ""))
        is_orderbook_match = {
            "TRADE": True,
            "LIQUIDATION": False,
            "DELEVERAGE": False,
        }.get(trade_type)
        return TradeEvidence(
            key,
            Venue.EXTENDED,
            market,
            timestamp(raw_time, "milliseconds"),
            received_at,
            raw_time,
            quantity,
            price,
            side,
            is_orderbook_match,
        )

    def normalize_trade_message(
        self, payload: Any, *, received_at: datetime, session_id: str,
        starting_ordinal: int,
    ) -> tuple[int, tuple[TradeEvidence, ...]]:
        """Normalize the documented ``{seq,data:[...]}`` public wrapper."""
        message = require_mapping(payload, "public trades message")
        rows = require_list(message.get("data"), "public trades message.data")
        return int(message["seq"]), tuple(
            self.normalize_trade(
                row, received_at=received_at, session_id=session_id,
                ordinal=starting_ordinal + index,
            )
            for index, row in enumerate(rows, 1)
        )

    def funding_quote(
        self,
        market: CanonicalMarket,
        *,
        funding_rate: Any,
        mark_price: Any,
        observed_at: datetime,
        assumed_open_at: datetime,
        settlement_at: datetime,
        quality: FundingQuality = FundingQuality.PREDICTED,
    ) -> FundingCashQuote:
        if (
            market.market_type is not MarketType.PERPETUAL
            or market.contract_type is not ContractType.LINEAR
            or market.base_multiplier is None
            or not market.is_active
            or market.is_rfq
            or market.is_off_hours
        ):
            return self.unknown_funding_quote(
                market,
                observed_at=observed_at,
                assumed_open_at=assumed_open_at,
            )
        rate = decimal_value(funding_rate, "funding_rate")
        mark = decimal_value(mark_price, "mark_price")
        cash = mark * rate
        eligible = assumed_open_at < settlement_at
        return FundingCashQuote(
            Venue.EXTENDED,
            market.venue_symbol,
            observed_at,
            assumed_open_at,
            settlement_at,
            quality,
            FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT,
            eligible,
            -cash if eligible else Decimal("0"),
            cash if eligible else Decimal("0"),
            self.SOURCE,
        )

    def normalize_applied_funding_message(
        self,
        payload: Any,
        market: CanonicalMarket,
    ) -> FundingSettlement | None:
        message = require_mapping(payload, "funding message")
        data = require_mapping(message.get("data"), "funding message.data")
        timestamp(message["ts"], "milliseconds")
        decimal_value(data["f"], "funding rate")
        if str(data.get("m")) != market.venue_symbol:
            return None
        return FundingSettlement(
            Venue.EXTENDED,
            market.venue_symbol,
            timestamp(data["T"], "milliseconds"),
            SettlementStatus.UNRESOLVED,
            None,
        )

    async def fetch_funding_quote(
        self, market: CanonicalMarket, *, assumed_open_at: datetime
    ) -> FundingCashQuote:
        payload = await self._get_json(
            f"/api/v1/info/markets/{quote(market.venue_symbol, safe='')}/stats"
        )
        data = require_mapping(payload.get("data"), "data")
        observed_at = datetime.now(UTC)
        try:
            settlement_at = timestamp(data["nextFundingRate"], "milliseconds")
            return self.funding_quote(
                market,
                funding_rate=data["fundingRate"],
                mark_price=data["markPrice"],
                observed_at=observed_at,
                assumed_open_at=assumed_open_at,
                settlement_at=settlement_at,
            )
        except (KeyError, TypeError, ValueError):
            return self.unknown_funding_quote(
                market, observed_at=observed_at, assumed_open_at=assumed_open_at
            )

    def unknown_funding_quote(
        self, market: CanonicalMarket, *, observed_at: datetime, assumed_open_at: datetime
    ) -> FundingCashQuote:
        return FundingCashQuote(
            Venue.EXTENDED,
            market.venue_symbol,
            observed_at,
            assumed_open_at,
            observed_at,
            FundingQuality.UNKNOWN,
            FundingAccrualMethod.UNKNOWN,
            False,
            None,
            None,
            self.SOURCE,
        )

    def orderbook_stream_url(self, venue_symbol: str) -> str:
        return f"{self.ws_base}/orderbooks/{quote(venue_symbol, safe='')}"

    def trades_stream_url(self, venue_symbol: str) -> str:
        return f"{self.ws_base}/publicTrades/{quote(venue_symbol, safe='')}"

    def funding_stream_url(self, venue_symbol: str) -> str:
        return f"{self.ws_base}/funding/{quote(venue_symbol, safe='')}"

    @staticmethod
    def handle_server_ping(payload: bytes) -> PublicHeartbeatAction:
        return PublicHeartbeatAction(WebSocketFrameAction.PONG, payload, True)
