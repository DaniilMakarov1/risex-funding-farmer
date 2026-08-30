"""Unauthenticated Lighter public market-data adapter for PAPER only.

The adapter deliberately stops at public market data.  It contains no account,
credential, order, signing, or dispatch surface.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from risex_farmer.models import (
    BookDelta,
    BookLevel,
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
    PublicHeartbeatAction,
    WebSocketFrameAction,
    decimal_value,
    require_list,
    require_mapping,
    timestamp,
)


class LighterAdapter(PublicAdapter):
    """Parse the current official Lighter public REST and WS contracts."""

    REST_SOURCE = "https://apidocs.lighter.xyz/reference/orderbookdetails"
    BOOK_SOURCE = "https://apidocs.lighter.xyz/reference/orderbookorders"
    WS_SOURCE = "https://apidocs.lighter.xyz/docs/websocket-reference"
    TRADES_SOURCE = "https://apidocs.lighter.xyz/reference/recenttrades"
    FUNDING_SOURCE = "https://apidocs.lighter.xyz/docs/funding"
    FEES_SOURCE = "https://docs.lighter.xyz/trading/trading-fees"
    CONTRACT_SOURCE = "https://docs.lighter.xyz/trading/contract-specifications"
    # Perpetual quote_asset_id=0 is the documented USDC collateral identity for
    # this public contract; it is not inferred from a generic asset registry.
    PERP_QUOTE_ASSET_ID = 0
    FUNDING_PERIOD = timedelta(hours=1)

    def __init__(self, session: Any) -> None:
        super().__init__(
            session,
            "https://mainnet.zklighter.elliot.ai",
            "wss://mainnet.zklighter.elliot.ai/stream",
        )
        self._market_ids: dict[str, int] = {}
        self._symbols_by_id: dict[int, str] = {}
        self._funding_quotes: dict[str, FundingCashQuote] = {}

    @staticmethod
    def _integer(value: Any, field: str) -> int:
        if isinstance(value, (bool, float)) or not isinstance(
            value, (str, int, Decimal)
        ):
            raise TypeError(f"{field} must be an integer")
        try:
            decimal = Decimal(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field} must be an integer") from exc
        if not decimal.is_finite() or decimal != decimal.to_integral_value():
            raise ValueError(f"{field} must be an integer")
        return int(decimal)

    @classmethod
    def _response_rows(cls, payload: Any, field: str) -> list[Any]:
        payload = require_mapping(payload, "Lighter public response")
        if cls._integer(payload.get("code"), "response.code") != 200:
            raise ValueError("Lighter public response was not successful")
        return require_list(payload.get(field), field)

    async def fetch_markets(self) -> tuple[CanonicalMarket, ...]:
        payload = await self._get_json("/api/v1/orderBookDetails", params={"filter": "perp"})
        # The official response has both arrays.  Requiring the spot array too
        # prevents a partial response from being mistaken for a complete catalog.
        rows = self._response_rows(payload, "order_book_details")
        require_list(require_mapping(payload, "response").get("spot_order_book_details"), "spot_order_book_details")
        row_ids = tuple(
            self._integer(require_mapping(row, "order_book_details row").get("market_id"), "market_id")
            for row in rows
        )
        if len(set(row_ids)) != len(row_ids):
            raise ValueError("Lighter market catalog contains duplicate market ids")
        markets = tuple(self.normalize_market(row) for row in rows)
        symbols = {market.venue_symbol for market in markets}
        if len(symbols) != len(markets):
            raise ValueError("Lighter market catalog contains duplicate symbols")
        return markets

    def normalize_market(self, row: Any) -> CanonicalMarket:
        row = require_mapping(row, "order_book_details row")
        symbol = str(row.get("symbol", "")).strip()
        if not symbol:
            raise ValueError("Lighter market symbol is empty")
        market_id = self._integer(row.get("market_id"), "market_id")
        market_type_raw = str(row.get("market_type", ""))
        market_type = (
            MarketType.PERPETUAL
            if market_type_raw == "perp"
            else MarketType.SPOT
            if market_type_raw == "spot"
            else MarketType.OTHER
        )
        config = require_mapping(row.get("market_config"), "market_config")
        base_asset_id = self._integer(row.get("base_asset_id"), "base_asset_id")
        quote_asset_id = self._integer(row.get("quote_asset_id"), "quote_asset_id")
        multiplier = decimal_value(row.get("multiplier"), "multiplier")
        size_decimals = self._integer(
            row.get("supported_size_decimals"), "supported_size_decimals"
        )
        price_decimals = self._integer(
            row.get("supported_price_decimals"), "supported_price_decimals"
        )
        quote_decimals = self._integer(
            row.get("supported_quote_decimals"), "supported_quote_decimals"
        )
        detail_size_decimals = self._integer(row.get("size_decimals"), "size_decimals")
        detail_price_decimals = self._integer(row.get("price_decimals"), "price_decimals")
        quote_multiplier = decimal_value(
            row.get("quote_multiplier"), "quote_multiplier"
        )
        minimum_quantity = decimal_value(row.get("min_base_amount"), "min_base_amount")
        minimum_notional = decimal_value(row.get("min_quote_amount"), "min_quote_amount")
        tick = Decimal("1").scaleb(-price_decimals)
        step = Decimal("1").scaleb(-size_decimals)
        trading_hours = config.get("trading_hours")
        hidden = config.get("hidden")
        force_reduce_only = config.get("force_reduce_only")
        if not isinstance(hidden, bool) or not isinstance(force_reduce_only, bool):
            raise TypeError("Lighter market_config booleans are invalid")
        # rfq_enabled is a market feature flag on ordinary perp books, not proof
        # that the market itself is RFQ-only.  The documented market_type/orderbook
        # contract is the eligibility identity used here.
        is_rfq = False if market_type is MarketType.PERPETUAL else True
        blockers: list[str] = []
        if market_type is not MarketType.PERPETUAL:
            blockers.append("LIGHTER_MARKET_NOT_PERPETUAL")
        if base_asset_id != 0:
            blockers.append("LIGHTER_BASE_ASSET_ID_UNPROVEN")
        if quote_asset_id != self.PERP_QUOTE_ASSET_ID:
            blockers.append("LIGHTER_QUOTE_ASSET_NOT_USDC")
        if multiplier != Decimal("1"):
            blockers.append("LIGHTER_MULTIPLIER_NOT_ONE")
        if quote_multiplier != Decimal("1"):
            blockers.append("LIGHTER_QUOTE_MULTIPLIER_NOT_ONE")
        if (
            size_decimals != detail_size_decimals
            or price_decimals != detail_price_decimals
        ):
            blockers.append("LIGHTER_GRID_DECIMAL_CONTRACT_MISMATCH")
        if minimum_quantity % step != 0:
            blockers.append("LIGHTER_MIN_BASE_AMOUNT_NOT_STEP_ALIGNED")
        if minimum_notional % Decimal("1").scaleb(-quote_decimals) != 0:
            blockers.append("LIGHTER_MIN_QUOTE_AMOUNT_NOT_GRID_ALIGNED")
        if (
            size_decimals < 0
            or price_decimals < 0
            or quote_decimals < 0
            or minimum_quantity <= 0
            or minimum_notional <= 0
        ):
            blockers.append("LIGHTER_GRID_OR_MINIMUM_NONPOSITIVE")
        if str(row.get("status", "")) != "active":
            blockers.append("LIGHTER_MARKET_INACTIVE")
        if hidden:
            blockers.append("LIGHTER_MARKET_HIDDEN")
        if force_reduce_only:
            blockers.append("LIGHTER_MARKET_FORCE_REDUCE_ONLY")
        if trading_hours not in (None, ""):
            blockers.append("LIGHTER_TRADING_HOURS_NOT_24X7_PROVEN")
        eligible_contract = not blockers
        self._market_ids[symbol] = market_id
        self._symbols_by_id[market_id] = symbol
        return CanonicalMarket(
            canonical_asset=symbol.upper(),
            venue=Venue.LIGHTER,
            venue_symbol=symbol,
            market_type=market_type,
            contract_type=ContractType.LINEAR if eligible_contract else ContractType.OTHER,
            base_multiplier=Decimal("1") if eligible_contract else None,
            quote_asset="USDC" if quote_asset_id == self.PERP_QUOTE_ASSET_ID else "UNKNOWN",
            settlement_asset="USDC" if quote_asset_id == self.PERP_QUOTE_ASSET_ID else "UNKNOWN",
            tick_size_raw=tick,
            quantity_step_raw=step,
            minimum_quantity_raw=minimum_quantity,
            minimum_notional_usd=minimum_notional,
            minimum_fee_notional_usd=None,
            is_active=(
                str(row.get("status", "")) == "active" and eligible_contract
            ),
            is_rfq=is_rfq,
            is_off_hours=trading_hours not in (None, ""),
            evidence_blockers=tuple(blockers),
        )

    def market_id(self, venue_symbol: str) -> int:
        try:
            return self._market_ids[venue_symbol]
        except KeyError as exc:
            raise ValueError(f"unknown Lighter venue symbol: {venue_symbol}") from exc

    def symbol_for_market(self, market_id: int) -> str:
        try:
            return self._symbols_by_id[market_id]
        except KeyError as exc:
            raise ValueError(f"unknown Lighter market id: {market_id}") from exc

    @classmethod
    def market_id_from_channel(cls, channel: Any) -> int:
        channel = str(channel or "")
        for separator in ("/", ":"):
            if separator in channel:
                channel = channel.rsplit(separator, 1)[-1]
                break
        return cls._integer(channel, "channel.market_id")

    def normalize_volume(self, row: Any, *, observed_at: datetime) -> MarketVolume:
        row = require_mapping(row, "order_book_details row")
        volume = decimal_value(row.get("daily_quote_token_volume"), "daily_quote_token_volume")
        if volume < 0:
            raise ValueError("Lighter quote volume cannot be negative")
        symbol = str(row.get("symbol", "")).strip()
        if not symbol:
            raise ValueError("Lighter volume symbol is empty")
        return MarketVolume(Venue.LIGHTER, symbol, volume, observed_at, self.REST_SOURCE)

    async def fetch_volumes(self) -> tuple[MarketVolume, ...]:
        payload = await self._get_json("/api/v1/orderBookDetails", params={"filter": "perp"})
        rows = self._response_rows(payload, "order_book_details")
        observed_at = datetime.now(UTC)
        volumes = tuple(
            self.normalize_volume(row, observed_at=observed_at) for row in rows
        )
        symbols = tuple(volume.canonical_market for volume in volumes)
        if len(set(symbols)) != len(symbols):
            raise ValueError("Lighter volume response contains duplicate symbols")
        return volumes

    @staticmethod
    def _levels(
        rows: Any, field: str, *, allow_zero: bool = False
    ) -> tuple[BookLevel, ...]:
        parsed: dict[Decimal, Decimal] = {}
        for raw in require_list(rows, field):
            level = require_mapping(raw, field)
            price = decimal_value(level.get("price"), f"{field}.price")
            quantity = decimal_value(
                level.get("size", level.get("remaining_base_amount")),
                f"{field}.size",
            )
            if price <= 0 or quantity < 0 or (quantity == 0 and not allow_zero):
                raise ValueError("Lighter order book levels must be non-negative")
            parsed[price] = parsed.get(price, Decimal("0")) + quantity
        ordered = sorted(parsed.items(), reverse=field == "bids")
        return tuple(BookLevel(price, quantity) for price, quantity in ordered)

    async def fetch_book(self, venue_symbol: str) -> OrderBook:
        market_id = self.market_id(venue_symbol)
        payload = await self._get_json(
            "/api/v1/orderBookOrders",
            params={"market_id": str(market_id), "limit": "250"},
        )
        response = require_mapping(payload, "order book orders response")
        if self._integer(response.get("code"), "response.code") != 200:
            raise ValueError("Lighter order book orders response was not successful")
        asks = self._levels(response.get("asks"), "asks")
        bids = self._levels(response.get("bids"), "bids")
        return OrderBook(
            Venue.LIGHTER,
            venue_symbol,
            bids,
            asks,
            datetime.now(UTC),
            None,
        )

    @classmethod
    def _book_event_levels(cls, data: Any) -> tuple[tuple[BookLevel, ...], tuple[BookLevel, ...]]:
        data = require_mapping(data, "order_book")
        if cls._integer(data.get("code"), "order_book.code") != 0:
            raise ValueError("Lighter order book event was not successful")
        return (
            cls._levels(data.get("bids"), "bids", allow_zero=True),
            cls._levels(data.get("asks"), "asks", allow_zero=True),
        )

    def normalize_book_message(
        self,
        payload: Any,
        *,
        received_at: datetime,
        initial: bool = False,
    ) -> OrderBook | BookDelta:
        message = require_mapping(payload, "Lighter order book message")
        if str(message.get("type", "")) != "update/order_book":
            raise ValueError("Lighter order book message type is invalid")
        data = require_mapping(message.get("order_book"), "order_book")
        channel = str(message.get("channel", ""))
        channel_market_id = (
            self.market_id_from_channel(channel) if channel else None
        )
        raw_market_id = message.get("market_id")
        if raw_market_id is None:
            if channel_market_id is None:
                raise ValueError("Lighter order book channel is missing")
            raw_market_id = channel_market_id
        market_id = self._integer(raw_market_id, "market_id")
        if channel_market_id is not None and channel_market_id != market_id:
            raise ValueError("Lighter order book channel identity mismatch")
        symbol = self.symbol_for_market(market_id)
        bids, asks = self._book_event_levels(data)
        if initial:
            bids = tuple(level for level in bids if level.canonical_quantity > 0)
            asks = tuple(level for level in asks if level.canonical_quantity > 0)
        nonce = self._integer(data.get("nonce"), "order_book.nonce")
        begin_nonce = self._integer(data.get("begin_nonce"), "order_book.begin_nonce")
        raw_time = message.get("timestamp")
        source_at = timestamp(raw_time, "milliseconds")
        observed_at = min(source_at, received_at)
        if initial:
            return OrderBook(Venue.LIGHTER, symbol, bids, asks, observed_at, nonce)
        return BookDelta(
            Venue.LIGHTER,
            symbol,
            bids,
            asks,
            observed_at,
            nonce,
            begin_nonce,
        )

    def normalize_trade(
        self, payload: Any, *, received_at: datetime, session_id: str, ordinal: int
    ) -> TradeEvidence:
        trade = require_mapping(payload, "Lighter trade")
        market_id = self._integer(trade.get("market_id"), "trade.market_id")
        symbol = self.symbol_for_market(market_id)
        if str(trade.get("type", "trade")) != "trade":
            raise ValueError("Lighter non-orderbook trade cannot prove maker fill")
        trade_id = self._integer(trade.get("trade_id"), "trade.trade_id")
        raw_time = trade.get("timestamp")
        exchange_at = timestamp(raw_time, "milliseconds")
        quantity = decimal_value(trade.get("size"), "trade.size")
        price = decimal_value(trade.get("price"), "trade.price")
        if quantity <= 0 or price <= 0:
            raise ValueError("Lighter public trade must be positive")
        maker_ask = trade.get("is_maker_ask")
        if not isinstance(maker_ask, bool):
            raise TypeError("Lighter is_maker_ask must be boolean")
        # A maker ask is lifted by an aggressive buyer; a maker bid is hit by
        # an aggressive seller, matching the official WS trade semantics.
        aggressor = Side.BUY if maker_ask else Side.SELL
        return TradeEvidence(
            f"LIGHTER|{symbol}|{trade_id}",
            Venue.LIGHTER,
            symbol,
            exchange_at,
            received_at,
            raw_time,
            quantity,
            price,
            aggressor,
            True,
            "OFFICIAL_PUBLIC",
            (),
        )

    def normalize_trade_message(
        self,
        payload: Any,
        *,
        received_at: datetime,
        session_id: str,
        starting_ordinal: int,
    ) -> tuple[int, tuple[TradeEvidence, ...]]:
        message = require_mapping(payload, "Lighter trade message")
        if str(message.get("type", "")) != "update/trade":
            raise ValueError("Lighter trade message type is invalid")
        channel_market_id = self.market_id_from_channel(message.get("channel"))
        nonce = self._integer(message.get("nonce"), "trade.nonce")
        rows = tuple(
            require_mapping(raw, "trade")
            for raw in require_list(message.get("trades"), "trades")
        )
        if any(
            self._integer(row.get("market_id"), "trade.market_id")
            != channel_market_id
            for row in rows
        ):
            raise ValueError("Lighter trade channel identity mismatch")
        trades = tuple(
            self.normalize_trade(
                row,
                received_at=received_at,
                session_id=session_id,
                ordinal=starting_ordinal + offset,
            )
            for offset, row in enumerate(rows)
            if str(row.get("type", "trade")) == "trade"
        )
        return nonce, trades

    def unknown_funding_quote(
        self,
        market: CanonicalMarket,
        *,
        observed_at: datetime,
        assumed_open_at: datetime,
    ) -> FundingCashQuote:
        return FundingCashQuote(
            Venue.LIGHTER,
            market.venue_symbol,
            observed_at,
            assumed_open_at,
            observed_at,
            FundingQuality.UNKNOWN,
            FundingAccrualMethod.UNKNOWN,
            False,
            None,
            None,
            f"{self.WS_SOURCE}#market-stats:future-funding-required",
        )

    def normalize_market_stats_message(
        self,
        payload: Any,
        market: CanonicalMarket,
        *,
        received_at: datetime,
        assumed_open_at: datetime,
    ) -> FundingCashQuote:
        message = require_mapping(payload, "Lighter market stats message")
        try:
            if str(message.get("type", "")) != "update/market_stats":
                raise ValueError("Lighter market stats message type is invalid")
            stats = require_mapping(message.get("market_stats"), "market_stats")
            market_id = self._integer(stats.get("market_id"), "market_stats.market_id")
            if self.market_id_from_channel(message.get("channel")) != market_id:
                raise ValueError("Lighter market stats channel identity mismatch")
            if self.symbol_for_market(market_id) != market.venue_symbol:
                raise ValueError("Lighter market stats identity mismatch")
            symbol = str(stats.get("symbol", "")).strip()
            if symbol != market.venue_symbol:
                raise ValueError("Lighter market stats symbol mismatch")
            if market.venue is not Venue.LIGHTER or market.contract_type is not ContractType.LINEAR:
                raise ValueError("Lighter market stats market is not linear")
            raw_time = message.get("timestamp")
            observed_at = min(timestamp(raw_time, "milliseconds"), received_at)
            last_funding_at = timestamp(stats.get("funding_timestamp"), "milliseconds")
            settlement_at = last_funding_at + self.FUNDING_PERIOD
            rate = decimal_value(stats.get("current_funding_rate"), "current_funding_rate")
            index = decimal_value(stats.get("index_price"), "index_price")
            if (
                index <= 0
                or not rate.is_finite()
                or last_funding_at.minute != 0
                or last_funding_at.second != 0
                or last_funding_at.microsecond != 0
                or last_funding_at > observed_at
                or settlement_at <= observed_at
            ):
                raise ValueError("Lighter future funding inputs are not usable")
            cash = index * rate
            eligible = assumed_open_at < settlement_at
            quote = FundingCashQuote(
                Venue.LIGHTER,
                market.venue_symbol,
                observed_at,
                assumed_open_at,
                settlement_at,
                FundingQuality.PREDICTED,
                FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT,
                True,
                -cash if eligible else Decimal("0"),
                cash if eligible else Decimal("0"),
                self.WS_SOURCE,
            )
        except (KeyError, TypeError, ValueError):
            return self.unknown_funding_quote(
                market, observed_at=received_at, assumed_open_at=assumed_open_at
            )
        self._funding_quotes[market.venue_symbol] = quote
        return quote

    async def fetch_funding_quote(
        self, market: CanonicalMarket, *, assumed_open_at: datetime
    ) -> FundingCashQuote:
        cached = self._funding_quotes.get(market.venue_symbol)
        if cached is None:
            return self.unknown_funding_quote(
                market,
                observed_at=datetime.now(UTC),
                assumed_open_at=assumed_open_at,
            )
        eligible = assumed_open_at < cached.settlement_at
        cash = abs(cached.long_cash_per_canonical_base_usd or cached.short_cash_per_canonical_base_usd or Decimal("0"))
        return replace(
            cached,
            assumed_or_actual_position_opened_at=assumed_open_at,
            eligibility_known=True,
            long_cash_per_canonical_base_usd=-cash if eligible else Decimal("0"),
            short_cash_per_canonical_base_usd=cash if eligible else Decimal("0"),
        )

    @staticmethod
    def subscription(kind: str, market_id: int) -> dict[str, object]:
        channels = {
            "book": "order_book",
            "order_book": "order_book",
            "trade": "trade",
            "market_stats": "market_stats",
        }
        try:
            channel = channels[kind]
        except KeyError as exc:
            raise ValueError(f"unsupported Lighter public channel: {kind}") from exc
        return {"type": "subscribe", "channel": f"{channel}/{market_id}"}

    @staticmethod
    def client_ping_action(payload: bytes = b'{"type":"ping"}') -> PublicHeartbeatAction:
        return PublicHeartbeatAction(WebSocketFrameAction.NONE, payload, False)

    @staticmethod
    def handle_server_pong(payload: bytes = b"") -> PublicHeartbeatAction:
        return PublicHeartbeatAction(WebSocketFrameAction.NONE, payload, True)
