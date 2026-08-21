"""RISEx public market-data normalization with explicit economics blockers."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
import re
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
from risex_farmer.config import RISEX_PAPER_FALLBACK_ASSUMPTIONS_ENABLED

from .base import (
    PublicAdapter,
    PublicHeartbeatAction,
    WebSocketFrameAction,
    decimal_value,
    require_list,
    require_mapping,
    timestamp,
)


class RisexAdapter(PublicAdapter):
    MARKETS_SOURCE = "https://developer.rise.trade/reference/marketservice_getmarkets.md"
    FUNDING_SOURCE = "https://developer.rise.trade/reference/getfundingratehistory.md"
    PAPER_ASSUMPTION_SOURCE = "PAPER_ASSUMPTION_CURRENT_NEXT_RATE"
    HISTORY_ASSUMPTION_SOURCE = "PAPER_ASSUMPTION_LAST_APPLIED_RATE"

    def __init__(self, session: Any) -> None:
        super().__init__(session, "https://api.rise.trade", "wss://ws.rise.trade/ws")
        self._market_ids: dict[str, str] = {}
        self._symbols_by_id: dict[str, str] = {}
        self._raw_markets: dict[str, dict[str, Any]] = {}
        self._metadata_consistent: set[str] = set()
        self._book_units_consistent: set[str] = set()
        self._trade_units_consistent: set[str] = set()
        self._unit_blockers: dict[str, tuple[str, ...]] = {}

    async def fetch_markets(self) -> tuple[CanonicalMarket, ...]:
        payload = await self._get_json("/v1/markets", params={"force_refresh": "true"})
        data = require_mapping(payload.get("data"), "data")
        markets = require_list(data.get("markets"), "data.markets")
        normalized = tuple(self.normalize_market(require_mapping(row, "market")) for row in markets)
        return normalized

    async def fetch_volumes(self) -> tuple[MarketVolume, ...]:
        payload = await self._get_json("/v1/markets", params={"force_refresh": "true"})
        data = require_mapping(payload.get("data"), "data")
        rows = require_list(data.get("markets"), "data.markets")
        observed_at = datetime.now(UTC)
        return tuple(self.normalize_volume(row, observed_at=observed_at) for row in rows)

    def normalize_market(self, row: dict[str, Any] | Any) -> CanonicalMarket:
        row = require_mapping(row, "market")
        config = require_mapping(row.get("config"), "market.config")
        market_id = str(row["market_id"])
        symbol = str(config["name"])
        self._market_ids[symbol] = market_id
        self._symbols_by_id[market_id] = symbol
        self._raw_markets[symbol] = dict(row)
        quote = str(row.get("quote_asset_symbol") or "UNKNOWN")
        raw_asset = str(row.get("base_asset_symbol") or row.get("underlying") or symbol)
        canonical_asset = raw_asset.split("/")[0].split("-")[0]
        grids = {name: decimal_value(config[name], f"config.{name}") for name in ("step_price", "step_size", "min_order_size")}
        exact_symbol = f"{canonical_asset}/{quote}"
        mapping_consistent = all(str(value) == exact_symbol for value in (
            symbol, raw_asset, row.get("underlying", exact_symbol),
            row.get("display_name", exact_symbol), row.get("display_base_asset_symbol", exact_symbol),
        ))
        synthetic = bool(re.match(r"^\d", canonical_asset)) or any(
            bool(row.get(name) or config.get(name)) for name in ("deprecated", "synthetic", "is_synthetic")
        )
        grids_positive = all(value > 0 for value in grids.values())
        multiple = (
            grids["min_order_size"] / grids["step_size"]
            if grids["step_size"] > 0 else None
        )
        multiplier = row.get("multiplier", config.get("multiplier"))
        metadata_consistent = (
            mapping_consistent and not synthetic and grids_positive
            and multiple is not None and multiple == multiple.to_integral_value()
            and (multiplier is None or decimal_value(multiplier, "multiplier") == 1)
        )
        metadata_blockers: list[str] = []
        if not mapping_consistent:
            metadata_blockers.append("RISEX_SYMBOL_MAPPING_INCONSISTENT")
        if synthetic:
            metadata_blockers.append("RISEX_SYNTHETIC_OR_DEPRECATED_PRODUCT")
        if not grids_positive:
            metadata_blockers.append("RISEX_GRID_OR_MINIMUM_NONPOSITIVE")
        if multiple is not None and multiple != multiple.to_integral_value():
            metadata_blockers.append("RISEX_MINIMUM_NOT_STEP_ALIGNED")
        if multiplier is not None and decimal_value(multiplier, "multiplier") != 1:
            metadata_blockers.append("RISEX_MULTIPLIER_NOT_ONE")
        (self._metadata_consistent.add if metadata_consistent else self._metadata_consistent.discard)(symbol)
        fallback_consistent = (
            RISEX_PAPER_FALLBACK_ASSUMPTIONS_ENABLED
            and bool(row.get("active", True))
            and bool(config.get("unlocked", False))
            and quote in {"USD", "USDC", "USDT", "USDT0"}
            and bool(canonical_asset)
            and metadata_consistent
            and symbol in self._book_units_consistent
            and symbol in self._trade_units_consistent
        )
        evidence_blockers = tuple(dict.fromkeys((
            *metadata_blockers,
            *(() if symbol in self._book_units_consistent else self._unit_blockers.get(
                f"book:{symbol}", ("RISEX_BOOK_UNIT_EVIDENCE_MISSING",)
            )),
            *(() if symbol in self._trade_units_consistent else self._unit_blockers.get(
                f"trade:{symbol}", ("RISEX_TRADE_UNIT_EVIDENCE_MISSING",)
            )),
        )))
        return CanonicalMarket(
            canonical_asset=canonical_asset,
            venue=Venue.RISEX,
            venue_symbol=symbol,
            market_type=MarketType.PERPETUAL,
            # Explicitly authorized paper fallback, never official contract evidence.
            contract_type=ContractType.LINEAR if fallback_consistent else ContractType.OTHER,
            base_multiplier=Decimal("1") if fallback_consistent else None,
            quote_asset=quote,
            settlement_asset=quote,
            tick_size_raw=decimal_value(config["step_price"], "config.step_price"),
            quantity_step_raw=decimal_value(config["step_size"], "config.step_size"),
            minimum_quantity_raw=decimal_value(config["min_order_size"], "config.min_order_size"),
            minimum_notional_usd=Decimal("0"),
            minimum_fee_notional_usd=None,
            is_active=bool(row.get("active", True)) and bool(config.get("unlocked", False)),
            is_rfq=False,
            is_off_hours=False,
            evidence_blockers=evidence_blockers,
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

    async def prime_recent_trade_evidence(
        self, market: CanonicalMarket, *, limit: int = 20
    ) -> CanonicalMarket:
        """Prove public trade quantity units, then rebuild the immutable market."""
        market_id = self._market_ids[market.venue_symbol]
        payload = await self._get_json(
            f"/v1/markets/id/{market_id}/trade-history",
            params={"limit": str(limit)},
        )
        trades = require_list(
            require_mapping(payload.get("data"), "data").get("trades"),
            "data.trades",
        )
        if not trades:
            self._trade_units_consistent.discard(market.venue_symbol)
        all_valid = bool(trades)
        failure_blockers: list[str] = []
        for ordinal, raw in enumerate(trades, 1):
            trade = require_mapping(raw, "trade history row")
            self.normalize_trade(
                {
                    "market_id": market_id,
                    "worker_timestamp": trade["time"],
                    "data": trade,
                },
                received_at=datetime.now(UTC),
                session_id="REST_TRADE_HISTORY",
                ordinal=ordinal,
            )
            row_valid = market.venue_symbol in self._trade_units_consistent
            if not row_valid:
                failure_blockers.extend(self._unit_blockers.get(
                    f"trade:{market.venue_symbol}", ()
                ))
            all_valid = all_valid and row_valid
        if not all_valid:
            self._trade_units_consistent.discard(market.venue_symbol)
            self._unit_blockers[f"trade:{market.venue_symbol}"] = tuple(
                dict.fromkeys(failure_blockers or ["RISEX_TRADE_UNIT_EVIDENCE_EMPTY"])
            )
        return self.normalize_market(self._raw_markets[market.venue_symbol])

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

        bids, asks = levels("bids"), levels("asks")
        raw = self._raw_markets.get(symbol, {})
        try:
            config = require_mapping(raw.get("config"), "market.config")
            step = decimal_value(config["step_size"], "step_size")
            price_step = decimal_value(config["step_price"], "step_price")
            levels_all = bids + asks
            blockers: list[str] = []
            if not levels_all:
                blockers.append("RISEX_BOOK_UNIT_EVIDENCE_EMPTY")
            if any(level.canonical_quantity <= 0 for level in levels_all):
                blockers.append("RISEX_BOOK_QUANTITY_NONPOSITIVE")
            if any(level.canonical_quantity / step != (level.canonical_quantity / step).to_integral_value() for level in levels_all):
                blockers.append("RISEX_BOOK_QUANTITY_OFF_STEP")
            if any(level.canonical_price <= 0 for level in levels_all):
                blockers.append("RISEX_BOOK_PRICE_NONPOSITIVE")
            if any(level.canonical_price / price_step != (level.canonical_price / price_step).to_integral_value() for level in levels_all):
                blockers.append("RISEX_BOOK_PRICE_OFF_TICK")
            valid = not blockers
        except (KeyError, TypeError, ValueError):
            valid = False
            blockers = ["RISEX_BOOK_UNIT_EVIDENCE_INVALID"]
        self._unit_blockers[f"book:{symbol}"] = tuple(blockers)
        (self._book_units_consistent.add if valid else self._book_units_consistent.discard)(symbol)
        return OrderBook(Venue.RISEX, symbol, bids, asks, observed_at)

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
        self, payload: Any, *, received_at: datetime, session_id: str, ordinal: int
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
        # Current public messages carry server worker time, not block time. It is
        # retained and normalized explicitly; RISEx is never the maker cutoff leg.
        raw_time = outer.get("block_timestamp", outer.get("worker_timestamp", trade.get("time")))
        exchange_timestamp = (
            timestamp(raw_time, "nanoseconds") if raw_time is not None else None
        )
        trade_id = trade.get("id")
        key = f"RISEX|{market}|{trade_id}" if trade_id is not None else (
            f"RISEX|{session_id}|{raw_time}|{market}|{price}|{quantity}|"
            f"{aggressor.value if aggressor is not None else 'UNKNOWN'}|{ordinal}"
        )
        raw = self._raw_markets.get(market, {})
        try:
            config = require_mapping(raw.get("config"), "market.config")
            step = decimal_value(config["step_size"], "step_size")
            price_step = decimal_value(config["step_price"], "step_price")
            blockers = []
            if quantity <= 0:
                blockers.append("RISEX_TRADE_QUANTITY_NONPOSITIVE")
            if quantity / step != (quantity / step).to_integral_value():
                blockers.append("RISEX_TRADE_QUANTITY_OFF_STEP")
            if price <= 0:
                blockers.append("RISEX_TRADE_PRICE_NONPOSITIVE")
            if price / price_step != (price / price_step).to_integral_value():
                blockers.append("RISEX_TRADE_PRICE_OFF_TICK")
            valid = not blockers
        except (KeyError, TypeError, ValueError):
            valid = False
            blockers = ["RISEX_TRADE_UNIT_EVIDENCE_INVALID"]
        self._unit_blockers[f"trade:{market}"] = tuple(blockers)
        (self._trade_units_consistent.add if valid else self._trade_units_consistent.discard)(market)
        return TradeEvidence(
            key,
            Venue.RISEX,
            market,
            exchange_timestamp,
            received_at,
            raw_time,
            quantity,
            price,
            aggressor,
            True,
            "OFFICIAL_PUBLIC_WITH_PAPER_ASSUMPTIONS",
            (
                "RISEX_CONTRACT_AND_QUANTITY_UNITS_PAPER_ASSUMPTION",
                *(() if "block_timestamp" in outer else ("RISEX_WORKER_TIMESTAMP_USED_AS_SERVER_EVENT_TIME",)),
            ),
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
        observed_at = datetime.now(UTC)
        if (
            not RISEX_PAPER_FALLBACK_ASSUMPTIONS_ENABLED
            or market.base_multiplier != Decimal("1")
            or market.contract_type is not ContractType.LINEAR
        ):
            return self.unknown_funding_quote(
                market, observed_at=observed_at, assumed_open_at=assumed_open_at
            )
        raw = self._raw_markets.get(market.venue_symbol, {})
        rate_raw = raw.get("current_funding_rate")
        price_raw = raw.get("mark_price", raw.get("oracle_price", raw.get("index_price")))
        next_raw = raw.get("next_funding_timestamp", raw.get("next_funding_time"))
        try:
            rate = decimal_value(rate_raw, "funding_rate")
            price = decimal_value(price_raw, "funding_price")
            if next_raw is None:
                raise ValueError("official next funding time absent")
            integer = int(next_raw)
            unit = "nanoseconds" if abs(integer) >= 10**17 else (
                "milliseconds" if abs(integer) >= 10**11 else "seconds"
            )
            settlement_at = timestamp(integer, unit)
            if price <= 0 or settlement_at <= observed_at:
                raise ValueError("inconsistent RISEx funding inputs")
        except (KeyError, TypeError, ValueError):
            return await self._history_funding_quote(
                market, observed_at=observed_at, assumed_open_at=assumed_open_at,
                price_raw=price_raw,
            )
        cash = rate * price
        eligible = assumed_open_at < settlement_at
        return FundingCashQuote(
            Venue.RISEX,
            market.venue_symbol,
            observed_at,
            assumed_open_at,
            settlement_at,
            FundingQuality.ESTIMATED,
            FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT,
            True,
            -cash if eligible else Decimal("0"),
            cash if eligible else Decimal("0"),
            self.PAPER_ASSUMPTION_SOURCE,
        )

    async def _history_funding_quote(
        self, market: CanonicalMarket, *, observed_at: datetime,
        assumed_open_at: datetime, price_raw: Any,
    ) -> FundingCashQuote:
        try:
            market_id = self._market_ids[market.venue_symbol]
            payload = await self._get_json(f"/v1/markets/id/{market_id}/funding-rate-history")
            records = require_list(require_mapping(payload.get("data"), "data").get("records"), "data.records")
            recent = [require_mapping(row, "funding record") for row in records[:3]]
            if len(recent) < 3:
                raise ValueError("insufficient official history")
            starts = [int(row["start_time"]) for row in recent]
            ends = [int(row["end_time"]) for row in recent]
            intervals = [end - start for start, end in zip(starts, ends)]
            if len(set(intervals)) != 1 or intervals[0] <= 0 or not (
                ends[1] == starts[0] and ends[2] == starts[1]
            ):
                raise ValueError("unstable official history cadence")
            rate = decimal_value(recent[0]["funding_rate"], "funding_rate")
            price = decimal_value(price_raw, "funding_price")
            settlement_at = timestamp(ends[0] + intervals[0], "nanoseconds")
            if price <= 0 or settlement_at <= observed_at:
                raise ValueError("invalid official history estimate")
        except (KeyError, TypeError, ValueError):
            return self.unknown_funding_quote(market, observed_at=observed_at, assumed_open_at=assumed_open_at)
        cash = rate * price
        eligible = assumed_open_at < settlement_at
        return FundingCashQuote(
            Venue.RISEX, market.venue_symbol, observed_at, assumed_open_at,
            settlement_at, FundingQuality.ESTIMATED,
            FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT, True,
            -cash if eligible else Decimal("0"), cash if eligible else Decimal("0"),
            self.HISTORY_ASSUMPTION_SOURCE,
        )

    async def fetch_applied_funding_quotes(
        self, market: CanonicalMarket, *, since: datetime, until: datetime,
        assumed_open_at: datetime,
    ) -> tuple[FundingCashQuote, ...]:
        """Return official settled history in per-base cash units for reconciliation."""
        market_id = self._market_ids[market.venue_symbol]
        payload = await self._get_json(f"/v1/markets/id/{market_id}/funding-rate-history")
        records = require_list(require_mapping(payload.get("data"), "data").get("records"), "data.records")
        quotes: list[FundingCashQuote] = []
        for raw in records:
            row = require_mapping(raw, "funding record")
            settlement_at = timestamp(int(row["end_time"]), "nanoseconds")
            if settlement_at < since or settlement_at > until:
                continue
            cash = decimal_value(row["funding_rate"], "funding_rate") * decimal_value(row["index_price"], "index_price")
            eligible = assumed_open_at < settlement_at
            quotes.append(FundingCashQuote(
                Venue.RISEX, market.venue_symbol, settlement_at, assumed_open_at,
                settlement_at, FundingQuality.APPLIED_RATE,
                FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT, True,
                -cash if eligible else Decimal("0"), cash if eligible else Decimal("0"),
                self.FUNDING_SOURCE,
            ))
        return tuple(quotes)

    def market_id(self, venue_symbol: str) -> int:
        return int(self._market_ids[venue_symbol])

    @staticmethod
    def orderbook_subscription(market_ids: list[int]) -> dict[str, object]:
        return {"method": "subscribe", "params": {"channel": "orderbook", "market_ids": market_ids}}

    @staticmethod
    def orderbook_unsubscription() -> dict[str, object]:
        return {"method": "unsubscribe", "params": {"channel": "orderbook"}}

    @staticmethod
    def trades_subscription(market_ids: list[int]) -> dict[str, object]:
        return {"method": "subscribe", "params": {"channel": "trades", "market_ids": market_ids}}

    @staticmethod
    def handle_server_ping(payload: bytes) -> PublicHeartbeatAction:
        return PublicHeartbeatAction(WebSocketFrameAction.PONG, payload, True)
