"""Nado public market-data adapter with venue-owned funding conversion."""

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

from .base import (
    PublicAdapter,
    decimal_value,
    require_list,
    require_mapping,
    synthetic_trade_key,
    timestamp,
)


X18 = Decimal("1000000000000000000")


class NadoAdapter(PublicAdapter):
    SOURCE = "https://docs.nado.xyz/core/funding-rates.md"

    def __init__(self, session: Any) -> None:
        super().__init__(
            session,
            "https://archive.prod.nado.xyz",
            "wss://gateway.prod.nado.xyz/v1/subscribe",
        )
        self._product_ids: dict[str, int] = {}
        self._symbols_by_id: dict[int, str] = {}

    async def fetch_markets(self) -> tuple[CanonicalMarket, ...]:
        payload = await self._get_json("/v2/symbols", params={"product_type": "perp"})
        return tuple(self.normalize_market(row) for row in payload.values())

    def normalize_market(self, row: Any) -> CanonicalMarket:
        row = require_mapping(row, "symbol")
        symbol = str(row["symbol"])
        product_id = int(row["product_id"])
        self._product_ids[symbol] = product_id
        self._symbols_by_id[product_id] = symbol
        is_perpetual = row.get("type") == "perp"
        market_hours = row.get("market_hours")
        price_feed = require_mapping(row.get("price_feed", {}), "price_feed")
        spot_index = require_mapping(price_feed.get("spot_index", {}), "price_feed.spot_index")
        # Official docs identify Stork/24x7 products as crypto. Other products remain ineligible.
        is_crypto = market_hours is None and spot_index.get("primary") == "stork"
        canonical_asset = str(row.get("base_currency") or symbol.removesuffix("-PERP"))
        is_off_hours = bool(
            isinstance(market_hours, dict) and not market_hours.get("is_open", False)
        )
        eligible_contract = is_perpetual and is_crypto
        return CanonicalMarket(
            canonical_asset=canonical_asset,
            venue=Venue.NADO,
            venue_symbol=symbol,
            market_type=MarketType.PERPETUAL if is_perpetual else MarketType.SPOT,
            contract_type=ContractType.LINEAR if eligible_contract else ContractType.OTHER,
            base_multiplier=Decimal("1") if eligible_contract else None,
            quote_asset="USDT0",
            settlement_asset="USDT0",
            tick_size_raw=decimal_value(
                row["price_increment_x18"], "price_increment_x18", scale=X18
            ),
            quantity_step_raw=decimal_value(row["size_increment"], "size_increment", scale=X18),
            minimum_quantity_raw=decimal_value(
                row["size_increment"], "size_increment", scale=X18
            ),
            minimum_notional_usd=decimal_value(row["min_size"], "min_size", scale=X18),
            minimum_fee_notional_usd=decimal_value(row["min_size"], "min_size", scale=X18),
            is_active=row.get("trading_status") == "live" and eligible_contract,
            is_rfq=False,
            is_off_hours=is_off_hours,
        )

    def normalize_volume(self, row: Any, *, observed_at: datetime) -> MarketVolume:
        row = require_mapping(row, "ticker")
        raw_volume = row.get("quote_volume")
        return MarketVolume(
            Venue.NADO,
            str(row["base_currency"]),
            None if raw_volume is None else decimal_value(raw_volume, "quote_volume"),
            observed_at,
            "https://docs.nado.xyz/developer-resources/api/v2/tickers.md",
        )

    async def fetch_volumes(self) -> tuple[MarketVolume, ...]:
        payload = await self._get_json(
            "/v2/tickers", params={"market": "perp", "edge": "false"}
        )
        observed_at = datetime.now(UTC)
        return tuple(
            self.normalize_volume(row, observed_at=observed_at)
            for row in payload.values()
        )

    async def fetch_book(self, venue_symbol: str) -> OrderBook:
        try:
            product_id = self._product_ids[venue_symbol]
        except KeyError as exc:
            raise ValueError(f"unknown Nado venue symbol: {venue_symbol}") from exc
        payload = await self._get_json_at(
            "https://gateway.prod.nado.xyz/v1",
            "/query",
            params={"type": "market_liquidity", "product_id": str(product_id), "depth": "100"},
        )
        return self.normalize_book(payload.get("data"), observed_at=datetime.now(UTC))

    def normalize_book(self, data: Any, *, observed_at: datetime) -> OrderBook:
        data = require_mapping(data, "orderbook")
        product_id = int(data["product_id"])

        def levels(name: str) -> tuple[BookLevel, ...]:
            parsed: list[BookLevel] = []
            for raw_level in require_list(data.get(name), name):
                if not isinstance(raw_level, list) or len(raw_level) != 2:
                    raise ValueError(f"{name} level must be [price, quantity]")
                parsed.append(
                    BookLevel(
                        decimal_value(raw_level[0], f"{name}.price", scale=X18),
                        decimal_value(raw_level[1], f"{name}.quantity", scale=X18),
                    )
                )
            return tuple(parsed)

        raw_timestamp = data["timestamp"]
        return OrderBook(
            Venue.NADO,
            self._symbols_by_id.get(product_id, str(product_id)),
            levels("bids"),
            levels("asks"),
            timestamp(raw_timestamp, "nanoseconds"),
            int(raw_timestamp),
        )

    def normalize_book_message(self, payload: Any) -> BookDelta:
        message = require_mapping(payload, "book_depth")
        product_id = int(message["product_id"])

        def levels(name: str) -> tuple[BookLevel, ...]:
            parsed: list[BookLevel] = []
            for level in require_list(message.get(name, []), name):
                if not isinstance(level, list) or len(level) != 2:
                    raise ValueError(f"{name} level must be [price, quantity]")
                parsed.append(
                    BookLevel(
                        decimal_value(level[0], f"{name}.price", scale=X18),
                        decimal_value(level[1], f"{name}.quantity", scale=X18),
                    )
                )
            return tuple(parsed)

        max_timestamp = int(message["max_timestamp"])
        return BookDelta(
            Venue.NADO,
            self._symbols_by_id.get(product_id, str(product_id)),
            levels("bids"),
            levels("asks"),
            timestamp(max_timestamp, "nanoseconds"),
            max_timestamp,
            int(message["last_max_timestamp"]),
        )

    def normalize_trade(
        self, payload: Any, *, receipt_at: datetime, session_id: str, ordinal: int
    ) -> TradeEvidence:
        trade = require_mapping(payload, "trade")
        product_id = int(trade["product_id"])
        market = self._symbols_by_id.get(product_id, str(product_id))
        price = decimal_value(trade["price"], "trade.price", scale=X18)
        quantity = decimal_value(trade["taker_qty"], "trade.taker_qty", scale=X18)
        side = Side.BUY if bool(trade["is_taker_buyer"]) else Side.SELL
        raw_time = trade["timestamp"]
        key = synthetic_trade_key(
            "NADO", session_id, raw_time, market, price, quantity, side.value, ordinal
        )
        return TradeEvidence(
            key,
            Venue.NADO,
            market,
            timestamp(raw_time, "nanoseconds"),
            receipt_at,
            raw_time,
            quantity,
            price,
            side,
            True,
        )

    def predicted_funding_quote(
        self,
        market: CanonicalMarket,
        *,
        funding_rate_x18: Any,
        index_price_x18: Any,
        observed_at: datetime,
        assumed_open_at: datetime,
        settlement_at: datetime,
    ) -> FundingCashQuote:
        if market.base_multiplier is None or market.contract_type is not ContractType.LINEAR:
            return self.unknown_funding_quote(
                market, observed_at=observed_at, assumed_open_at=assumed_open_at
            )
        daily_rate = decimal_value(funding_rate_x18, "funding_rate_x18", scale=X18)
        index_price = decimal_value(index_price_x18, "index_price_x18", scale=X18)
        # Official 24h percentage -> hourly percentage -> cash/base. This is the
        # one and only price multiplication for predicted Nado funding.
        hourly_cash_per_base = daily_rate / Decimal("24") * index_price
        eligible = assumed_open_at < settlement_at
        return FundingCashQuote(
            Venue.NADO,
            market.venue_symbol,
            observed_at,
            assumed_open_at,
            settlement_at,
            FundingQuality.PREDICTED,
            FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT,
            eligible,
            -hourly_cash_per_base if eligible else Decimal("0"),
            hourly_cash_per_base if eligible else Decimal("0"),
            self.SOURCE,
        )

    def normalize_funding_rate_message(
        self,
        payload: Any,
        market: CanonicalMarket,
        *,
        index_price_x18: Any | None,
        assumed_open_at: datetime,
    ) -> FundingCashQuote:
        message = require_mapping(payload, "funding rate")
        observed_at = timestamp(message["timestamp"], "nanoseconds")
        if (
            self._symbols_by_id.get(int(message["product_id"]))
            != market.venue_symbol
            or index_price_x18 is None
        ):
            return self.unknown_funding_quote(
                market,
                observed_at=observed_at,
                assumed_open_at=assumed_open_at,
            )
        update_time = int(message["update_time"])
        settlement_at = datetime.fromtimestamp(
            update_time - (update_time % 3600) + 3600, tz=UTC
        )
        return self.predicted_funding_quote(
            market,
            funding_rate_x18=message["funding_rate_x18"],
            index_price_x18=index_price_x18,
            observed_at=observed_at,
            assumed_open_at=assumed_open_at,
            settlement_at=settlement_at,
        )

    async def fetch_funding_quote(
        self, market: CanonicalMarket, *, assumed_open_at: datetime
    ) -> FundingCashQuote:
        observed_at = datetime.now(UTC)
        try:
            product_id = self._product_ids[market.venue_symbol]
            rates = await self._post_json_at(
                "https://archive.prod.nado.xyz/v1",
                "",
                body={"funding_rates": {"product_ids": [product_id]}},
            )
            prices = await self._post_json_at(
                "https://archive.prod.nado.xyz/v1",
                "",
                body={"perp_prices": {"product_ids": [product_id]}},
            )
            rate = require_mapping(rates[str(product_id)], "funding rate")
            price = require_mapping(prices[str(product_id)], "perp price")
            update_time = int(rate["update_time"])
            settlement_at = datetime.fromtimestamp(
                update_time - (update_time % 3600) + 3600, tz=UTC
            )
            return self.predicted_funding_quote(
                market,
                funding_rate_x18=rate["funding_rate_x18"],
                index_price_x18=price["index_price_x18"],
                observed_at=observed_at,
                assumed_open_at=assumed_open_at,
                settlement_at=settlement_at,
            )
        except (KeyError, TypeError, ValueError):
            return self.unknown_funding_quote(
                market, observed_at=observed_at, assumed_open_at=assumed_open_at
            )

    def applied_cumulative_funding_quote(
        self,
        market: CanonicalMarket,
        *,
        previous_long_x18: Any,
        current_long_x18: Any,
        previous_short_x18: Any,
        current_short_x18: Any,
        observed_at: datetime,
        assumed_open_at: datetime,
        settlement_at: datetime,
        source: str = "https://docs.nado.xyz/developer-resources/api/archive-indexer/product-snapshots.md",
    ) -> FundingCashQuote:
        # Official cumulative values are already per-unit cash; never multiply by price.
        if market.base_multiplier is None or market.contract_type is not ContractType.LINEAR:
            return self.unknown_funding_quote(
                market,
                observed_at=observed_at,
                assumed_open_at=assumed_open_at,
            )
        long_cash = decimal_value(current_long_x18, "current_long_x18", scale=X18) - decimal_value(
            previous_long_x18, "previous_long_x18", scale=X18
        )
        short_cash = decimal_value(
            current_short_x18, "current_short_x18", scale=X18
        ) - decimal_value(previous_short_x18, "previous_short_x18", scale=X18)
        return FundingCashQuote(
            Venue.NADO,
            market.venue_symbol,
            observed_at,
            assumed_open_at,
            settlement_at,
            FundingQuality.APPLIED_RATE,
            FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT,
            True,
            long_cash if assumed_open_at < settlement_at else Decimal("0"),
            short_cash if assumed_open_at < settlement_at else Decimal("0"),
            source,
        )

    def normalize_funding_payment_message(
        self,
        payload: Any,
        market: CanonicalMarket,
        *,
        previous_long_x18: Any | None,
        previous_short_x18: Any | None,
        assumed_open_at: datetime,
    ) -> FundingCashQuote:
        message = require_mapping(payload, "funding payment")
        observed_at = timestamp(message["timestamp"], "nanoseconds")
        if (
            self._symbols_by_id.get(int(message["product_id"]))
            != market.venue_symbol
            or previous_long_x18 is None
            or previous_short_x18 is None
        ):
            return self.unknown_funding_quote(
                market,
                observed_at=observed_at,
                assumed_open_at=assumed_open_at,
            )
        return self.applied_cumulative_funding_quote(
            market,
            previous_long_x18=previous_long_x18,
            current_long_x18=message["cumulative_funding_long_x18"],
            previous_short_x18=previous_short_x18,
            current_short_x18=message["cumulative_funding_short_x18"],
            observed_at=observed_at,
            assumed_open_at=assumed_open_at,
            settlement_at=observed_at,
            source="https://docs.nado.xyz/developer-resources/api/subscriptions/events",
        )

    def unknown_funding_quote(
        self, market: CanonicalMarket, *, observed_at: datetime, assumed_open_at: datetime
    ) -> FundingCashQuote:
        return FundingCashQuote(
            Venue.NADO,
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

    @staticmethod
    def subscription(stream: str, product_id: int | None) -> dict[str, object]:
        return {
            "method": "subscribe",
            "stream": {"type": stream, "product_id": product_id},
        }
