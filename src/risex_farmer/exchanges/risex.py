"""RISEx public market-data normalization with explicit economics blockers."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from risex_farmer.models import (
    BookLevel,
    BookDelta,
    CanonicalMarket,
    ContractType,
    FundingAccrualMethod,
    FundingCashQuote,
    FundingQuality,
    MarketType,
    MarketVolume,
    OrderBook,
    Side,
    TradeEvidence,
    Venue,
)

from .base import PublicAdapter, decimal_value, require_list, require_mapping, timestamp


class RisexAdapter(PublicAdapter):
    MARKETS_SOURCE = "https://developer.rise.trade/reference/marketservice_getmarkets.md"
    FUNDING_SOURCE = "https://developer.rise.trade/reference/getfundingratehistory.md"

    def __init__(self, session: Any) -> None:
        super().__init__(session, "https://api.rise.trade", "wss://ws.rise.trade/ws")
        self._market_ids: dict[str, str] = {}
        self._symbols_by_id: dict[str, str] = {}

    async def fetch_markets(self) -> tuple[CanonicalMarket, ...]:
        payload = await self._get_json("/v1/markets", params={"force_refresh": "true"})
        data = require_mapping(payload.get("data"), "data")
        markets = require_list(data.get("markets"), "data.markets")
        normalized = tuple(self.normalize_market(require_mapping(row, "market")) for row in markets)
        return normalized

    def normalize_market(self, row: dict[str, Any] | Any) -> CanonicalMarket:
        row = require_mapping(row, "market")
        config = require_mapping(row.get("config"), "market.config")
        market_id = str(row["market_id"])
        symbol = str(config["name"])
        self._market_ids[symbol] = market_id
        self._symbols_by_id[market_id] = symbol
        return CanonicalMarket(
            canonical_asset=str(row.get("base_asset_symbol") or row.get("underlying") or symbol),
            venue=Venue.RISEX,
            venue_symbol=symbol,
            market_type=MarketType.PERPETUAL,
            # Official public metadata does not prove multiplier or linear parity.
            contract_type=ContractType.OTHER,
            base_multiplier=None,
            quote_asset=str(row.get("quote_asset_symbol") or "UNKNOWN"),
            settlement_asset=str(row.get("quote_asset_symbol") or "UNKNOWN"),
            tick_size_raw=decimal_value(config["step_price"], "config.step_price"),
            quantity_step_raw=decimal_value(config["step_size"], "config.step_size"),
            minimum_quantity_raw=decimal_value(config["min_order_size"], "config.min_order_size"),
            minimum_notional_usd=Decimal("0"),
            minimum_fee_notional_usd=None,
            is_active=bool(row.get("active", True)) and bool(config.get("unlocked", False)),
            is_rfq=False,
            is_off_hours=False,
        )

    def normalize_volume(self, row: Any, *, observed_at: datetime) -> MarketVolume:
        row = require_mapping(row, "market")
        config = require_mapping(row.get("config"), "market.config")
        raw_volume = row.get("quote_volume_24h")
        return MarketVolume(
            Venue.RISEX,
            str(config["name"]),
            None
            if raw_volume is None
            else decimal_value(raw_volume, "quote_volume_24h"),
            observed_at,
            self.MARKETS_SOURCE,
        )

    async def fetch_book(self, venue_symbol: str) -> OrderBook:
        try:
            market_id = self._market_ids[venue_symbol]
        except KeyError as exc:
            raise ValueError(f"unknown RISEx venue symbol: {venue_symbol}") from exc
        payload = await self._get_json("/v1/orderbook", params={"market_id": market_id, "limit": "250"})
        data = require_mapping(payload.get("data", payload), "orderbook")
        return self.normalize_book(data, observed_at=datetime.now(UTC))

    def normalize_book(self, data: Any, *, observed_at: datetime) -> OrderBook:
        data = require_mapping(data, "orderbook")
        market_id = str(data["market_id"])
        symbol = self._symbols_by_id.get(market_id, market_id)

        def levels(name: str) -> tuple[BookLevel, ...]:
            return tuple(
                BookLevel(
                    decimal_value(require_mapping(level, name)["price"], f"{name}.price"),
                    decimal_value(require_mapping(level, name)["quantity"], f"{name}.quantity"),
                )
                for level in require_list(data.get(name), name)
            )

        return OrderBook(Venue.RISEX, symbol, levels("bids"), levels("asks"), observed_at)

    def normalize_book_message(self, payload: Any) -> OrderBook | BookDelta:
        message = require_mapping(payload, "orderbook message")
        data = require_mapping(message.get("data"), "orderbook message.data")
        observed_at = timestamp(message["worker_timestamp"], "nanoseconds")
        if message.get("type") == "snapshot":
            return self.normalize_book(data, observed_at=observed_at)

        def levels(name: str) -> tuple[BookLevel, ...]:
            return tuple(
                BookLevel(
                    decimal_value(require_mapping(level, name)["price"], f"{name}.price"),
                    decimal_value(
                        require_mapping(level, name)["quantity"], f"{name}.quantity"
                    ),
                )
                for level in require_list(data.get(name, []), name)
            )

        market_id = str(message["market_id"])
        return BookDelta(
            Venue.RISEX,
            self._symbols_by_id.get(market_id, market_id),
            levels("bids"),
            levels("asks"),
            observed_at,
            checksum=int(message["checksum"]),
        )

    def normalize_trade(
        self, payload: Any, *, receipt_at: datetime, session_id: str, ordinal: int
    ) -> TradeEvidence:
        outer = require_mapping(payload, "trade message")
        trade = require_mapping(outer.get("data", outer), "trade")
        market_id = str(outer.get("market_id", trade.get("market_id", "UNKNOWN")))
        market = self._symbols_by_id.get(market_id, market_id)
        price = decimal_value(trade["price"], "trade.price")
        quantity = decimal_value(trade.get("size", trade.get("quantity")), "trade.size")
        maker_side = trade.get("maker_side")
        if maker_side in (0, "0", "BUY"):
            aggressor = Side.SELL
        elif maker_side in (1, "1", "SELL"):
            aggressor = Side.BUY
        else:
            aggressor = None
        raw_time = outer.get("block_timestamp", trade.get("time"))
        exchange_at = timestamp(raw_time, "nanoseconds") if raw_time is not None else None
        trade_id = trade.get("id")
        key = f"RISEX|{market}|{trade_id}" if trade_id is not None else (
            f"RISEX|{session_id}|{raw_time}|{market}|{price}|{quantity}|"
            f"{aggressor.value if aggressor is not None else 'UNKNOWN'}|{ordinal}"
        )
        return TradeEvidence(
            key,
            Venue.RISEX,
            market,
            exchange_at,
            receipt_at,
            raw_time,
            quantity,
            price,
            aggressor,
            True,
        )

    def unknown_funding_quote(
        self, market: CanonicalMarket, *, observed_at: datetime, assumed_open_at: datetime
    ) -> FundingCashQuote:
        return FundingCashQuote(
            Venue.RISEX,
            market.venue_symbol,
            observed_at,
            assumed_open_at,
            observed_at,
            FundingQuality.UNKNOWN,
            FundingAccrualMethod.UNKNOWN,
            False,
            None,
            None,
            self.FUNDING_SOURCE,
        )

    async def fetch_funding_quote(
        self, market: CanonicalMarket, *, assumed_open_at: datetime
    ) -> FundingCashQuote:
        # Public values do not establish hypothetical accrual or eligibility.
        return self.unknown_funding_quote(
            market, observed_at=datetime.now(UTC), assumed_open_at=assumed_open_at
        )

    @staticmethod
    def orderbook_subscription(market_ids: list[int]) -> dict[str, object]:
        return {"method": "subscribe", "params": {"channel": "orderbook", "market_ids": market_ids}}

    @staticmethod
    def trades_subscription(market_ids: list[int]) -> dict[str, object]:
        return {"method": "subscribe", "params": {"channel": "trades", "market_ids": market_ids}}
