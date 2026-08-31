from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from risex_farmer.exchanges.lighter import LighterAdapter
from risex_farmer.lifecycle import CloseReason, LifecycleEngine
from risex_farmer.market_data import BookStream
from risex_farmer.models import (
    BookDelta,
    BookExecutionCapture,
    BookLevel,
    CanonicalMarket,
    ContractType,
    DataQuality,
    FundingAccrualMethod,
    FundingCashQuote,
    FundingQuality,
    LifecycleState,
    MarketType,
    MarketVolume,
    OrderBook,
    RouteDirection,
    Side,
    StreamHealth,
    TradeEvidence,
    Venue,
)
from risex_farmer.paper_broker import (
    CancellationReason,
    PaperEntryBroker,
    TradeProcessOutcome,
)
from risex_farmer.scanner import MarketObservation, planned_fee_split, scan_once
from risex_farmer.storage import PaperRepository, _load


D = Decimal
NOW = datetime(2027, 8, 30, 12, tzinfo=UTC)
TARGET = NOW + timedelta(seconds=120)


def market_row(**overrides):
    row = {
        "symbol": "ETH",
        "market_id": 0,
        "market_type": "perp",
        "base_asset_id": 0,
        "quote_asset_id": 0,
        "multiplier": "1.000000000000000000",
        "quote_multiplier": 1,
        "supported_size_decimals": 3,
        "supported_price_decimals": 1,
        "supported_quote_decimals": 6,
        "size_decimals": 3,
        "price_decimals": 1,
        "min_base_amount": "0.001",
        "min_quote_amount": "10.000000",
        "status": "active",
        "daily_quote_token_volume": "12345",
        "market_config": {
            "force_reduce_only": False,
            "trading_hours": "",
            "hidden": False,
            "rfq_enabled": True,
        },
    }
    row.update(overrides)
    return row


def observation(
    venue: Venue,
    *,
    asset: str = "ETH",
    symbol: str | None = None,
    at: datetime = NOW,
    bids: tuple[tuple[str, str], ...] = (("99", "10"),),
    asks: tuple[tuple[str, str], ...] = (("101", "10"),),
    cash: str = "10",
) -> MarketObservation:
    venue_symbol = symbol or (asset if venue is Venue.LIGHTER else f"{asset}-{venue.value}")
    market = CanonicalMarket(
        asset,
        venue,
        venue_symbol,
        MarketType.PERPETUAL,
        ContractType.LINEAR,
        D("1"),
        "USDC",
        "USDC",
        D("0.1"),
        D("0.001"),
        D("0.001"),
        D("10"),
        None,
        True,
        False,
        False,
    )
    book = OrderBook(
        venue,
        venue_symbol,
        tuple(BookLevel(D(price), D(quantity)) for price, quantity in bids),
        tuple(BookLevel(D(price), D(quantity)) for price, quantity in asks),
        at,
        1,
    )
    funding = FundingCashQuote(
        venue,
        venue_symbol,
        at,
        at,
        TARGET,
        FundingQuality.PREDICTED,
        FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT,
        True,
        D(cash),
        D(cash),
        "official-test-fixture",
    )
    health = StreamHealth(at, at, True, True, True, DataQuality.COMPLETE)
    return MarketObservation(
        market,
        MarketVolume(venue, venue_symbol, D("1000000"), at, "official-test-fixture"),
        book,
        funding,
        health,
    )


def capture(row: MarketObservation, at: datetime) -> BookExecutionCapture:
    assert row.health is not None
    return BookExecutionCapture(
        row.book,
        row.health,
        row.health.last_market_event_at,
        at,
        1,
        0,
        1,
    )


@pytest.mark.asyncio
async def test_lighter_rest_contracts_normalize_metadata_volume_and_book_async(monkeypatch):
    adapter = LighterAdapter(object())
    details = {
        "code": 200,
        "order_book_details": [market_row()],
        "future_catalog_field": {"ignored": True},
    }
    calls = []

    async def get_json(path, *, params=None):
        calls.append((path, params))
        if path.endswith("orderBookOrders"):
            return {
                "code": 200,
                "asks": [
                    {"price": "101.0", "remaining_base_amount": "2"},
                    {"price": "102.0", "remaining_base_amount": "3"},
                ],
                "bids": [
                    {"price": "99.0", "remaining_base_amount": "1"},
                    {"price": "98.0", "remaining_base_amount": "4"},
                ],
            }
        return details

    monkeypatch.setattr(adapter, "_get_json", get_json)
    markets = await adapter.fetch_markets()
    volumes = await adapter.fetch_volumes()
    book = await adapter.fetch_book("ETH")

    market = markets[0]
    assert market == CanonicalMarket(
        "ETH", Venue.LIGHTER, "ETH", MarketType.PERPETUAL, ContractType.LINEAR,
        D("1"), "USDC", "USDC", D("0.1"), D("0.001"), D("0.001"), D("10"),
        None, True, False, False,
    )
    assert volumes[0].venue is Venue.LIGHTER
    assert book.bids == (BookLevel(D("99"), D("1")), BookLevel(D("98"), D("4")))
    assert book.asks == (BookLevel(D("101"), D("2")), BookLevel(D("102"), D("3")))
    assert calls == [
        ("/api/v1/orderBookDetails", {"filter": "perp"}),
        ("/api/v1/orderBookDetails", {"filter": "perp"}),
        ("/api/v1/orderBookOrders", {"market_id": "0", "limit": "250"}),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "error"),
    (
        ({"code": 200}, "order_book_details must be an array"),
        ({"code": 200, "order_book_details": {}}, "order_book_details must be an array"),
        ({"code": 500, "order_book_details": [market_row()]}, "not successful"),
        ({"code": 200, "order_book_details": [None]}, "order_book_details row must be an object"),
        (
            {"code": 200, "order_book_details": [market_row(market_id=None)]},
            "market_id must be an integer",
        ),
        (
            {"code": 200, "order_book_details": [market_row(market_config=None)]},
            "market_config must be an object",
        ),
    ),
)
async def test_lighter_perp_catalog_rejects_missing_or_malformed_required_data(
    monkeypatch, payload, error
):
    adapter = LighterAdapter(object())

    async def get_json(path, *, params=None):
        assert path == "/api/v1/orderBookDetails"
        assert params == {"filter": "perp"}
        return payload

    monkeypatch.setattr(adapter, "_get_json", get_json)
    with pytest.raises((TypeError, ValueError), match=error):
        await adapter.fetch_markets()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("change", "blocker"),
    (
        ({"market_type": "spot"}, "LIGHTER_MARKET_NOT_PERPETUAL"),
        ({"quote_asset_id": 3}, "LIGHTER_QUOTE_ASSET_NOT_USDC"),
    ),
)
async def test_lighter_perp_catalog_keeps_unsafe_metadata_fail_closed(
    monkeypatch, change, blocker
):
    adapter = LighterAdapter(object())
    payload = {"code": 200, "order_book_details": [market_row(**change)]}

    async def get_json(path, *, params=None):
        return payload

    monkeypatch.setattr(adapter, "_get_json", get_json)
    markets = await adapter.fetch_markets()

    assert len(markets) == 1
    market = markets[0]
    assert blocker in market.evidence_blockers
    assert market.contract_type is ContractType.OTHER
    assert market.base_multiplier is None
    assert not market.is_active


@pytest.mark.parametrize(
    ("change", "blocker"),
    (
        ({"market_type": "spot"}, "LIGHTER_MARKET_NOT_PERPETUAL"),
        ({"quote_asset_id": 3}, "LIGHTER_QUOTE_ASSET_NOT_USDC"),
        ({"quote_multiplier": "2"}, "LIGHTER_QUOTE_MULTIPLIER_NOT_ONE"),
        ({"price_decimals": 2}, "LIGHTER_GRID_DECIMAL_CONTRACT_MISMATCH"),
        ({"min_base_amount": "0.0005"}, "LIGHTER_MIN_BASE_AMOUNT_NOT_STEP_ALIGNED"),
        ({"min_quote_amount": "10.0000005"}, "LIGHTER_MIN_QUOTE_AMOUNT_NOT_GRID_ALIGNED"),
        ({"supported_quote_decimals": -1}, "LIGHTER_GRID_OR_MINIMUM_NONPOSITIVE"),
        ({"status": "inactive"}, "LIGHTER_MARKET_INACTIVE"),
        ({"market_config": {"force_reduce_only": True, "trading_hours": "", "hidden": False}}, "LIGHTER_MARKET_FORCE_REDUCE_ONLY"),
        ({"market_config": {"force_reduce_only": False, "trading_hours": "NYSE", "hidden": False}}, "LIGHTER_TRADING_HOURS_NOT_24X7_PROVEN"),
    ),
)
def test_lighter_metadata_blockers_fail_closed(change, blocker):
    adapter = LighterAdapter(object())
    market = adapter.normalize_market(market_row(**change))
    assert blocker in market.evidence_blockers
    assert market.contract_type is ContractType.OTHER
    assert market.base_multiplier is None
    assert not market.is_active


def test_lighter_book_nonce_chain_and_zero_delete_are_strict():
    adapter = LighterAdapter(object())
    adapter.normalize_market(market_row())
    snapshot = adapter.normalize_book_message(
        {
            "type": "subscribed/order_book",
            "channel": "order_book/0",
            "timestamp": 1_000_000,
            "order_book": {
                "code": 0,
                "bids": [
                    {"price": "99", "size": "2"},
                    {"price": "98", "size": "1"},
                ],
                "asks": [{"price": "101", "size": "2"}],
                "nonce": 10,
                "begin_nonce": 8,
            },
        },
        received_at=datetime(1970, 1, 1, 0, 16, 41, tzinfo=UTC),
        initial=True,
    )
    delta = adapter.normalize_book_message(
        {
            "type": "update/order_book",
            "channel": "order_book:0",
            "timestamp": 1_001_000,
            "order_book": {
                "code": 0,
                "bids": [{"price": "99", "size": "0"}],
                "asks": [{"price": "102", "size": "1"}],
                "nonce": 12,
                "begin_nonce": 10,
            },
        },
        received_at=datetime(1970, 1, 1, 0, 16, 41, tzinfo=UTC),
    )
    assert isinstance(delta, BookDelta)
    stream = BookStream(Venue.LIGHTER, "ETH")
    stream.connected(NOW)
    stream.snapshot(snapshot)
    assert stream.apply_delta(delta)
    assert stream.book() == OrderBook(
        Venue.LIGHTER,
        "ETH",
        (BookLevel(D("98"), D("1")),),
        (BookLevel(D("101"), D("2")), BookLevel(D("102"), D("1"))),
        delta.observed_at,
        12,
    )
    bad = replace(delta, previous_sequence=11, sequence=13)
    assert not stream.apply_delta(bad)
    assert not stream.book_sequence_valid
    with pytest.raises(ValueError, match="identity"):
        adapter.normalize_book_message(
            {
                "type": "update/order_book",
                "channel": "order_book:1",
                "market_id": 0,
                "timestamp": 1_001_000,
                "order_book": {
                    "code": 0,
                    "bids": [],
                    "asks": [],
                    "nonce": 13,
                    "begin_nonce": 12,
                },
            },
            received_at=datetime(1970, 1, 1, 0, 16, 41, tzinfo=UTC),
        )


@pytest.mark.parametrize(
    ("initial", "message_type"),
    ((True, "update/order_book"), (False, "subscribed/order_book")),
)
def test_lighter_book_message_type_matches_snapshot_phase(initial, message_type):
    adapter = LighterAdapter(object())
    adapter.normalize_market(market_row())
    with pytest.raises(ValueError, match="message type is invalid"):
        adapter.normalize_book_message(
            {
                "type": message_type,
                "channel": "order_book:0",
                "timestamp": 1_000_000,
                "order_book": {
                    "code": 0,
                    "bids": [{"price": "99", "size": "2"}],
                    "asks": [{"price": "101", "size": "2"}],
                    "nonce": 10,
                    "begin_nonce": 8,
                },
            },
            received_at=datetime(1970, 1, 1, 0, 16, 41, tzinfo=UTC),
            initial=initial,
        )


def lighter_trade_message(
    *,
    message_type: str = "update/trade",
    channel: str = "trade:0",
    market_id: int | str | None = 0,
    nonce: int | str = 7,
    trades: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "type": message_type,
        "channel": channel,
        "nonce": nonce,
        "trades": trades if trades is not None else [{
            "type": "trade",
            "market_id": 0,
            "trade_id": 123,
            "timestamp": int(NOW.timestamp() * 1000),
            "size": "1",
            "price": "101",
            "is_maker_ask": False,
        }],
    }
    if market_id is not None:
        payload["market_id"] = market_id
    return payload


@pytest.mark.parametrize(
    ("initial", "message_type"),
    ((True, "update/trade"), (False, "subscribed/trade")),
)
def test_lighter_trade_message_type_matches_subscription_phase(initial, message_type):
    adapter = LighterAdapter(object())
    adapter.normalize_market(market_row())
    with pytest.raises(ValueError, match="message type is invalid"):
        adapter.normalize_trade_message(
            lighter_trade_message(message_type=message_type),
            received_at=NOW,
            session_id="session",
            starting_ordinal=1,
            initial=initial,
        )


def test_lighter_trade_snapshot_only_establishes_identity_and_nonce():
    adapter = LighterAdapter(object())
    adapter.normalize_market(market_row())
    nonce, trades = adapter.normalize_trade_message(
        lighter_trade_message(
            message_type="subscribed/trade",
            nonce=50,
            trades=[
                {
                    "type": "trade",
                    "market_id": 0,
                    "trade_id": 999,
                    "timestamp": int(NOW.timestamp() * 1000),
                    "size": "500",
                    "price": "101",
                    "is_maker_ask": False,
                },
            ],
        ),
        received_at=NOW,
        session_id="session",
        starting_ordinal=1,
        initial=True,
    )

    assert nonce == 50
    assert trades == ()


@pytest.mark.parametrize(
    ("change", "error"),
    (
        ({"channel": "order_book:0"}, "trade channel is invalid"),
        ({"market_id": 1}, "trade channel identity mismatch"),
        (
            {
                "trades": [
                    {
                        "type": "trade",
                        "market_id": 1,
                    },
                ],
            },
            "trade channel identity mismatch",
        ),
        ({"nonce": True}, "trade.nonce must be an integer"),
    ),
)
def test_lighter_trade_message_identity_and_nonce_are_strict(change, error):
    adapter = LighterAdapter(object())
    adapter.normalize_market(market_row())
    with pytest.raises((TypeError, ValueError), match=error):
        adapter.normalize_trade_message(
            lighter_trade_message(**change),
            received_at=NOW,
            session_id="session",
            starting_ordinal=1,
        )


@pytest.mark.asyncio
async def test_lighter_refresh_cancels_when_exact_taker_depth_is_lost():
    risex = observation(Venue.RISEX)
    lighter = observation(
        Venue.LIGHTER,
        symbol="ETH",
        bids=(("98", "10"),),
        asks=(("100", "10"),),
    )
    snapshot = await scan_once((risex, lighter), NOW)
    plan = next(
        route
        for route in snapshot.evaluations
        if route.hedge_venue is Venue.LIGHTER
        and route.direction is RouteDirection.LONG_RISEX_SHORT_HEDGE
    )
    broker = PaperEntryBroker()
    await broker.activate(replace(snapshot, winner=plan), attempt_id="depth", activated_at=NOW)
    evaluated_at = NOW + timedelta(seconds=1)
    shallow_lighter = observation(
        Venue.LIGHTER,
        symbol="ETH",
        at=evaluated_at,
        bids=(("98", "0.001"),),
        asks=(("100", "10"),),
    )
    state = await broker.refresh(
        plan,
        observation(Venue.RISEX, at=evaluated_at),
        evaluated_at=evaluated_at,
        hedge_observation=shallow_lighter,
    )
    assert state.lifecycle_state is LifecycleState.FLAT
    assert state.order is not None
    assert state.order.cancellation_reason is CancellationReason.LIGHTER_ENTRY_DEPTH_UNAVAILABLE


@pytest.mark.asyncio
async def test_lighter_trade_and_market_stats_supply_identity_and_future_cash():
    adapter = LighterAdapter(object())
    market = adapter.normalize_market(market_row())
    received_at = NOW
    stats_at_ms = int(NOW.timestamp() * 1000)
    last_funding_at_ms = stats_at_ms
    nonce, trades = adapter.normalize_trade_message(
        {
            "type": "update/trade",
            "channel": "trade/0",
            "nonce": 7,
            "trades": [
                {
                    "type": "trade",
                    "market_id": 0,
                    "trade_id": 123,
                    "timestamp": stats_at_ms,
                    "size": "2.5",
                    "price": "101.2",
                    "is_maker_ask": False,
                },
                {
                    "type": "liquidation",
                    "market_id": 0,
                    "trade_id": 124,
                    "timestamp": stats_at_ms + 1,
                    "size": "1",
                    "price": "101",
                    "is_maker_ask": True,
                },
            ],
        },
        received_at=received_at,
        session_id="session",
        starting_ordinal=1,
    )
    assert nonce == 7
    assert len(trades) == 1
    assert trades[0].trade_event_key == "LIGHTER|ETH|123"
    assert trades[0].aggressor_side is Side.SELL
    assert trades[0].canonical_quantity == D("2.5")

    unknown = await adapter.fetch_funding_quote(market, assumed_open_at=NOW)
    assert unknown.quality is FundingQuality.UNKNOWN
    quote = adapter.normalize_market_stats_message(
        {
            "type": "update/market_stats",
            "channel": "market_stats:0",
            "timestamp": stats_at_ms,
            "market_stats": {
                "symbol": "ETH",
                "market_id": 0,
                "index_price": "100",
                "current_funding_rate": "0.001",
                "funding_rate": "0.0005",
                "funding_timestamp": last_funding_at_ms,
            },
        },
        market,
        received_at=received_at,
        assumed_open_at=NOW,
    )
    assert quote.quality is FundingQuality.PREDICTED
    assert quote.settlement_at == NOW + timedelta(hours=1)
    assert quote.long_cash_per_canonical_base_usd == D("-0.1")
    assert quote.short_cash_per_canonical_base_usd == D("0.1")
    refreshed = await adapter.fetch_funding_quote(market, assumed_open_at=NOW)
    assert refreshed.long_cash_per_canonical_base_usd == D("-0.1")
    invalid = adapter.normalize_market_stats_message(
        {
            "type": "update/market_stats",
            "channel": "market_stats:0",
            "market_stats": {
                "symbol": "ETH",
                "market_id": 0,
                "index_price": "100",
                "current_funding_rate": "0.001",
                "funding_timestamp": stats_at_ms + 3_600_000,
            },
            "timestamp": 1_800_000_000_000,
        },
        market,
        received_at=received_at,
        assumed_open_at=NOW,
    )
    assert invalid.quality is FundingQuality.UNKNOWN


def test_lighter_market_stats_subscription_snapshot_is_authoritative():
    adapter = LighterAdapter(object())
    market = adapter.normalize_market(market_row())
    stats_at_ms = int(NOW.timestamp() * 1000)
    quote = adapter.normalize_market_stats_message(
        {
            "type": "subscribed/market_stats",
            "channel": "market_stats:0",
            "timestamp": stats_at_ms,
            "market_stats": {
                "symbol": "ETH",
                "market_id": 0,
                "index_price": "100",
                "last_trade_price": "100",
                "current_funding_rate": "0.001",
                "funding_rate": "0.0005",
                "funding_timestamp": stats_at_ms,
            },
        },
        market,
        received_at=NOW,
        assumed_open_at=NOW,
    )
    assert quote.quality is FundingQuality.PREDICTED
    assert adapter._funding_quotes["ETH"] == quote


def test_lighter_funding_timestamp_one_millisecond_wire_skew_is_normalized():
    adapter = LighterAdapter(object())
    market = adapter.normalize_market(market_row())
    stats_at_ms = int(NOW.timestamp() * 1000)
    quote = adapter.normalize_market_stats_message(
        {
            "type": "update/market_stats",
            "channel": "market_stats:0",
            "timestamp": stats_at_ms,
            "market_stats": {
                "symbol": "ETH",
                "market_id": 0,
                "index_price": "100",
                "current_funding_rate": "0.001",
                "funding_timestamp": stats_at_ms + 1,
            },
        },
        market,
        received_at=NOW,
        assumed_open_at=NOW,
    )
    assert quote.quality is FundingQuality.PREDICTED
    assert quote.settlement_at == NOW + timedelta(hours=1)


def test_lighter_all_market_stats_snapshot_selects_target_market():
    adapter = LighterAdapter(object())
    market = adapter.normalize_market(market_row())
    stats_at_ms = int(NOW.timestamp() * 1000)
    rows = adapter.market_stats_rows(
        {
            "type": "subscribed/market_stats",
            "channel": "market_stats:all",
            "timestamp": stats_at_ms,
            "market_stats": {
                "0": {
                    "symbol": "ETH",
                    "market_id": 0,
                    "index_price": "100",
                    "current_funding_rate": "0.001",
                    "funding_timestamp": stats_at_ms,
                },
            },
        }
    )
    quote = adapter.normalize_market_stats_message(
        {
            "type": "subscribed/market_stats",
            "channel": "market_stats:all",
            "timestamp": stats_at_ms,
            "market_stats": {
                "0": {
                    "symbol": "ETH",
                    "market_id": 0,
                    "index_price": "100",
                    "current_funding_rate": "0.001",
                    "funding_timestamp": stats_at_ms,
                },
            },
        },
        market,
        received_at=NOW,
        assumed_open_at=NOW,
    )
    assert rows[0][0] == 0
    assert quote.quality is FundingQuality.PREDICTED


def test_lighter_non_hourly_funding_timestamp_fails_closed():
    adapter = LighterAdapter(object())
    market = adapter.normalize_market(market_row())
    stats_at_ms = int(NOW.timestamp() * 1000)
    quote = adapter.normalize_market_stats_message(
        {
            "type": "update/market_stats",
            "channel": "market_stats/0",
            "timestamp": stats_at_ms,
            "market_stats": {
                "symbol": "ETH",
                "market_id": 0,
                "index_price": "100",
                "current_funding_rate": "0.001",
                "funding_timestamp": stats_at_ms - 1_800_000,
            },
        },
        market,
        received_at=NOW,
        assumed_open_at=NOW,
    )
    assert quote.quality is FundingQuality.UNKNOWN


def test_lighter_heartbeats_and_no_applied_public_history_surface():
    adapter = LighterAdapter(object())
    assert adapter.subscription("order_book", 0) == {
        "type": "subscribe", "channel": "order_book/0"
    }
    assert adapter.subscription("trade", 0) == {
        "type": "subscribe", "channel": "trade/0"
    }
    assert adapter.subscription("market_stats", 0) == {
        "type": "subscribe", "channel": "market_stats/0"
    }
    assert adapter.subscription("market_stats", "all") == {
        "type": "subscribe", "channel": "market_stats/all"
    }
    assert adapter.client_ping_action().payload == b'{"type":"ping"}'
    assert adapter.handle_server_pong().connection_confirmed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "direction",
    (RouteDirection.LONG_RISEX_SHORT_HEDGE, RouteDirection.SHORT_RISEX_LONG_HEDGE),
)
async def test_lighter_full_dynamic_route_and_normal_lifecycle_persists_physical_legs(
    direction, tmp_path
):
    risex = observation(Venue.RISEX)
    lighter = observation(
        Venue.LIGHTER,
        symbol="ETH",
        bids=(("98", "10"),),
        asks=(("100", "10"),),
    )
    snapshot = await scan_once((risex, lighter), NOW)
    routes = [plan for plan in snapshot.evaluations if plan.hedge_venue is Venue.LIGHTER]
    assert {plan.direction for plan in routes} == {
        RouteDirection.LONG_RISEX_SHORT_HEDGE,
        RouteDirection.SHORT_RISEX_LONG_HEDGE,
    }
    plan = next(plan for plan in routes if plan.direction is direction)
    assert plan.entry_allowed
    fee_split = planned_fee_split(plan)
    assert fee_split is not None
    assert plan.planned_fees_usd == sum(fee_split, D("0")) > D("0")
    assert plan.risex_entry_price != plan.risex_exit_price
    assert plan.hedge_entry_price in {D("98"), D("100")}
    assert all(route.entry_allowed for route in routes)

    broker = PaperEntryBroker()
    await broker.activate(
        replace(snapshot, winner=plan), attempt_id="lighter", activated_at=NOW
    )
    order = broker.state.order
    assert order is not None
    assert order.venue is Venue.RISEX
    assert order.side is (Side.BUY if direction is RouteDirection.LONG_RISEX_SHORT_HEDGE else Side.SELL)
    opened_at = NOW + timedelta(seconds=1)
    entry_side = order.side
    entry_trade = TradeEvidence(
        "lighter-entry",
        Venue.RISEX,
        risex.market.venue_symbol,
        opened_at - timedelta(microseconds=1),
        opened_at,
        "synthetic-risex-maker-trade",
        plan.canonical_quantity,
        order.active_version.limit_price + (
            -risex.market.tick_size_raw
            if entry_side is Side.BUY
            else risex.market.tick_size_raw
        ),
        Side.SELL if entry_side is Side.BUY else Side.BUY,
        True,
    )

    async def recompute(route_plan, when):
        return tuple(
            replace(
                observation.funding,
                observed_at=when,
                assumed_or_actual_position_opened_at=when,
                long_cash_per_canonical_base_usd=D("0"),
                short_cash_per_canonical_base_usd=D("0"),
            )
            for observation in (risex, lighter)
        )

    result = await broker.process_trade(
        entry_trade,
        observed_version_id=order.active_version.version_id,
        processed_at=opened_at,
        risex_observation=risex,
        hedge_observation=lighter,
        recompute_funding=recompute,
        risex_capture=capture(risex, opened_at),
        hedge_capture=capture(lighter, opened_at),
    )
    assert result.outcome is TradeProcessOutcome.OPENED
    position = result.state.position
    assert position is not None
    assert position.maker_fill.venue is Venue.RISEX
    assert position.taker_fill.venue is Venue.LIGHTER
    assert position.taker_fill.fee.amount_usd == D("0")
    assert position.maker_fill.fee.liquidity_role.name == "MAKER"
    engine = LifecycleEngine(result.state)
    close_at = opened_at + timedelta(seconds=1)
    fresh_risex = observation(Venue.RISEX, at=close_at)
    fresh_lighter = observation(Venue.LIGHTER, symbol="ETH", at=close_at, bids=(("98", "10"),), asks=(("100", "10"),))
    await engine.restart(
        last_known_at=opened_at,
        recovered_at=close_at,
        risex_observation=fresh_risex,
        hedge_observation=fresh_lighter,
    )
    assert engine.snapshot.exit_order is not None
    assert engine.snapshot.exit_order.venue is Venue.RISEX
    exit_order = engine.snapshot.exit_order
    exit_trade = TradeEvidence(
        "lighter-exit",
        Venue.RISEX,
        risex.market.venue_symbol,
        close_at,
        close_at,
        "synthetic-risex-maker-trade",
        position.canonical_quantity,
        exit_order.active_version.limit_price + (
            risex.market.tick_size_raw
            if exit_order.side is Side.SELL
            else -risex.market.tick_size_raw
        ),
        Side.BUY if exit_order.side is Side.SELL else Side.SELL,
        True,
    )
    closed_result = await engine.process_exit_trade(
        exit_trade,
        observed_version_id=exit_order.active_version.version_id,
        processed_at=close_at,
        risex_observation=fresh_risex,
        hedge_observation=fresh_lighter,
        risex_capture=capture(fresh_risex, close_at),
        hedge_capture=capture(fresh_lighter, close_at),
    )
    assert closed_result.snapshot.closed_trade is not None
    closed = closed_result.snapshot.closed_trade
    assert closed.close_reason is CloseReason.NORMAL_MAKER
    assert closed.risex_exit_fill.venue is Venue.RISEX
    assert closed.hedge_exit_fill.venue is Venue.LIGHTER

    with PaperRepository(tmp_path / "lighter.db") as repository:
        lifecycle = LifecycleEngine(result.state)
        repository.save_decision(
            recorded_at=opened_at,
            entry_state=result.state,
            lifecycle_snapshot=lifecycle.snapshot,
            trade_events=(entry_trade,),
            fill_provenance=result.fill_provenance,
        )
        repository.save_decision(
            recorded_at=close_at,
            lifecycle_snapshot=closed_result.snapshot,
            trade_events=(exit_trade,),
            fill_provenance=closed_result.fill_provenance,
        )
        report = repository.report(as_of=close_at + timedelta(seconds=1))
        fills = {
            row["leg"]: _load(row["payload"])
            for row in repository.connection.execute("SELECT leg,payload FROM fills")
        }
        assert repository.load_runtime().closed_trade == closed
    assert report["fills"] == 4
    assert set(fills) == {"MAKER_ENTRY", "TAKER_ENTRY", "MAKER_EXIT", "TAKER_EXIT"}
    risex_volume = sum(
        fill.fee.fill_notional_usd
        for fill in fills.values()
        if fill.venue is Venue.RISEX
    )
    assert D(report["virtual_risex_volume_usd"]) == risex_volume


@pytest.mark.asyncio
async def test_lighter_hard_basis_requires_both_exact_taker_captures():
    risex = observation(Venue.RISEX)
    lighter = observation(Venue.LIGHTER, symbol="ETH", bids=(("98", "10"),), asks=(("100", "10"),))
    snapshot = await scan_once((risex, lighter), NOW)
    plan = next(
        plan for plan in snapshot.evaluations
        if plan.hedge_venue is Venue.LIGHTER
        and plan.direction is RouteDirection.LONG_RISEX_SHORT_HEDGE
    )
    broker = PaperEntryBroker()
    await broker.activate(replace(snapshot, winner=plan), attempt_id="hard", activated_at=NOW)
    opened_at = NOW + timedelta(seconds=1)

    async def recompute(route_plan, when):
        return tuple(
            replace(row.funding, observed_at=when, assumed_or_actual_position_opened_at=when)
            for row in (risex, lighter)
        )

    result = await broker.process_trade(
        TradeEvidence(
            "hard-entry", Venue.RISEX, "ETH-RISEX", opened_at, opened_at,
            "raw", plan.canonical_quantity, D("98.8"), Side.SELL, True,
        ),
        observed_version_id=broker.state.order.active_version.version_id,
        processed_at=opened_at,
        risex_observation=risex,
        hedge_observation=lighter,
        recompute_funding=recompute,
        risex_capture=capture(risex, opened_at),
        hedge_capture=capture(lighter, opened_at),
    )
    engine = LifecycleEngine(result.state)
    adverse_at = opened_at + timedelta(seconds=1)
    adverse_risex = observation(Venue.RISEX, at=adverse_at, bids=(("90", "10"),), asks=(("91", "10"),))
    adverse_lighter = observation(Venue.LIGHTER, symbol="ETH", at=adverse_at, bids=(("109", "10"),), asks=(("110", "10"),))
    closed = await engine.evaluate(
        evaluated_at=adverse_at,
        risex_observation=adverse_risex,
        hedge_observation=adverse_lighter,
        risex_capture=capture(adverse_risex, adverse_at),
        hedge_capture=capture(adverse_lighter, adverse_at),
    )
    assert closed.closed_trade is not None
    assert closed.closed_trade.close_reason is CloseReason.HARD_BASIS
    assert closed.closed_trade.risex_exit_fill.fee.liquidity_role.name == "TAKER"
    assert closed.closed_trade.hedge_exit_fill.fee.liquidity_role.name == "TAKER"
    assert all(type(proof).__name__ == "TakerFillProvenance" for _, proof in engine.fill_provenance)
