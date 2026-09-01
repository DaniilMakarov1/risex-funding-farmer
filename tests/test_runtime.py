import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import json
import pickle
from pathlib import Path
import sqlite3
import time
from types import SimpleNamespace

import aiohttp
import pytest

from risex_farmer.exchanges.base import PublicAdapter, PublicDataUnavailable
from risex_farmer.exchanges.extended import ExtendedAdapter
from risex_farmer.exchanges.lighter import LighterAdapter
from risex_farmer.exchanges.nado import NadoAdapter
from risex_farmer.exchanges.risex import RisexAdapter
from risex_farmer.config import PAPER_CONFIG, SYNTHETIC_TEST_OVERLAY_USD
from risex_farmer.models import (
    BookLevel,
    BookDelta,
    CanonicalMarket,
    ContractType,
    DataQuality,
    FundingAccrualMethod,
    FundingCashQuote,
    FundingQuality,
    FundingSettlement,
    LifecycleState,
    MarketType,
    MarketVolume,
    OrderBook,
    SettlementStatus,
    Side,
    StreamHealth,
    TradeEvidence,
    Venue,
)
from risex_farmer.market_data import BookStream
from risex_farmer.lifecycle import LifecycleEngine, LifecycleEventType
from risex_farmer.notifications import (
    NotificationOutbox,
    TELEGRAM_HTML_PARSE_MODE,
    TelegramDelivery,
    format_telegram_funding_countdown,
    format_telegram_money,
)
from risex_farmer.runtime import PublicPaperRuntime, public_paper_run, public_scan_once
from risex_farmer.orchestrator import run_fixture
from risex_farmer.paper_broker import PaperEntryBroker
from risex_farmer.scanner import MarketObservation, ScanSnapshot
from risex_farmer.storage import PaperRepository


D = Decimal
NOW = datetime(2027, 8, 1, 12, tzinfo=UTC)


class CaptureNotifications:
    def __init__(self) -> None:
        self.rows = []
        self.started = False
        self.closed = False

    async def start(self) -> None:
        self.started = True

    def enqueue(self, payload) -> bool:
        self.rows.append(payload)
        return True

    async def close(self) -> None:
        self.closed = True


def digest_cards(payloads):
    return [
        card
        for digest in payloads
        for card in digest.text.split("\n\n")[1:]
    ]


class FakeClock:
    def __init__(self, now: datetime = NOW) -> None:
        self.value = now

    def now(self) -> datetime:
        return self.value

    def advance(self, seconds: int) -> None:
        self.value += timedelta(seconds=seconds)


class FakeAdapter(PublicAdapter):
    def __init__(
        self,
        venue: Venue,
        clock: FakeClock,
        *,
        settlement_at: datetime,
        available: bool = True,
        funding_cash: str = "5",
    ) -> None:
        super().__init__(None, f"https://public.{venue.value.lower()}.invalid", "wss://public.invalid")
        self.venue = venue
        self.clock = clock
        self.settlement_at = settlement_at
        self.available = available
        self.funding_cash = D(funding_cash)
        self.funding_unknown = False
        self.calls: list[str] = []
        symbol = f"ABC-{venue.value}"
        self.market = CanonicalMarket(
            "ABC",
            venue,
            symbol,
            MarketType.PERPETUAL,
            ContractType.LINEAR,
            D("1"),
            "USDC" if venue is not Venue.NADO else "USDT0",
            "USDC" if venue is not Venue.NADO else "USDT0",
            D("1"),
            D("1"),
            D("1"),
            D("10"),
            D("20") if venue is Venue.NADO else None,
            True,
            False,
            False,
        )
    def _ready(self, name: str) -> None:
        self.calls.append(name)
        if not self.available:
            raise PublicDataUnavailable("synthetic public outage")

    async def fetch_markets(self):
        self._ready("markets")
        return (self.market,)

    async def fetch_volumes(self):
        self._ready("volumes")
        return (
            MarketVolume(
                self.venue,
                self.market.venue_symbol,
                D("1000000"),
                self.clock.now(),
                "official-public-synthetic-shape",
            ),
        )

    async def fetch_book(self, venue_symbol: str):
        self._ready("book")
        return OrderBook(
            self.venue,
            venue_symbol,
            (BookLevel(D("99"), D("20")),),
            (BookLevel(D("101"), D("20")),),
            self.clock.now(),
            1,
        )

    async def fetch_funding_quote(self, market, *, assumed_open_at):
        self._ready("funding")
        if self.funding_unknown:
            return self.unknown_funding_quote(
                market, observed_at=self.clock.now(), assumed_open_at=assumed_open_at
            )
        return FundingCashQuote(
            self.venue,
            market.venue_symbol,
            self.clock.now(),
            assumed_open_at,
            self.settlement_at,
            FundingQuality.ESTIMATED if self.venue is Venue.RISEX else FundingQuality.PREDICTED,
            FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT,
            True,
            self.funding_cash,
            self.funding_cash,
            "PAPER_ASSUMPTION:RISEX_PUBLIC_FALLBACK"
            if self.venue is Venue.RISEX
            else "official-public-synthetic-shape",
        )

    async def fetch_applied_settlements(self, market, *, since, until):
        self._ready("settlements")
        return ()

    def normalize_trade(self, payload, *, received_at, session_id, ordinal):
        return payload

    def unknown_funding_quote(self, market, *, observed_at, assumed_open_at):
        return FundingCashQuote(
            self.venue,
            market.venue_symbol,
            observed_at,
            assumed_open_at,
            observed_at,
            FundingQuality.UNKNOWN,
            FundingAccrualMethod.UNKNOWN,
            False,
            None,
            None,
            "official-public-synthetic-shape",
        )


class LighterStreamAdapter(LighterAdapter):
    def __init__(self, clock: FakeClock) -> None:
        super().__init__(None)
        self.clock = clock
        self._market_ids = {"ETH": 1}
        self._symbols_by_id = {1: "ETH"}
        self.fetch_book_calls = 0

    async def fetch_book(self, venue_symbol: str) -> OrderBook:
        self.fetch_book_calls += 1
        return OrderBook(
            Venue.LIGHTER,
            venue_symbol,
            (BookLevel(D("99"), D("20")),),
            (BookLevel(D("101"), D("20")),),
            self.clock.now(),
            1,
        )


class BoundaryLighterAdapter(LighterStreamAdapter):
    def __init__(self, clock: FakeClock, market, initial_stats) -> None:
        super().__init__(clock)
        self.market = market
        self.initial_stats = initial_stats

    async def fetch_markets(self):
        return (self.market,)

    async def fetch_volumes(self):
        return (
            MarketVolume(
                Venue.LIGHTER,
                self.market.venue_symbol,
                D("1000000"),
                self.clock.now(),
                "official-public-test-shape",
            ),
        )

    async def fetch_funding_quote(self, market, *, assumed_open_at):
        return self.normalize_market_stats_message(
            self.initial_stats,
            market,
            received_at=self.clock.now(),
            assumed_open_at=assumed_open_at,
        )


class ManyFakeAdapter(FakeAdapter):
    def __init__(
        self, venue: Venue, clock: FakeClock, *, settlement_at: datetime,
        asset_count: int = 5,
    ) -> None:
        super().__init__(venue, clock, settlement_at=settlement_at)
        self.many_markets = tuple(
            replace(self.market, canonical_asset=f"A{index}", venue_symbol=f"A{index}-{venue.value}")
            for index in range(asset_count)
        )

    async def fetch_markets(self):
        self._ready("markets")
        return self.many_markets

    async def fetch_volumes(self):
        self._ready("volumes")
        return tuple(
            MarketVolume(self.venue, market.venue_symbol, D("1000000"), self.clock.now(), "official-shaped")
            for market in self.many_markets
        )


class DynamicCatalogAdapter(FakeAdapter):
    def __init__(
        self,
        venue: Venue,
        clock: FakeClock,
        *,
        settlement_at: datetime,
        assets: tuple[str, ...],
    ) -> None:
        super().__init__(venue, clock, settlement_at=settlement_at)
        self.catalog = tuple(
            replace(
                self.market,
                canonical_asset=asset,
                venue_symbol=f"{asset}-{venue.value}",
            )
            for asset in assets
        )

    async def fetch_markets(self):
        self._ready("markets")
        return self.catalog

    async def fetch_volumes(self):
        self._ready("volumes")
        return tuple(
            MarketVolume(
                self.venue,
                market.venue_symbol,
                D("1000000"),
                self.clock.now(),
                "official-shaped",
            )
            for market in self.catalog
        )


class GatedAdapter(FakeAdapter):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.block_catalog = False
        self.block_catalog_after_calls: int | None = None
        self.catalog_calls = 0
        self.block_funding = False
        self.gate = asyncio.Event()
        self.request_started = asyncio.Event()
        self.cancelled = False

    async def _wait_if_blocked(self, blocked: bool) -> None:
        if not blocked:
            return
        self.request_started.set()
        try:
            await self.gate.wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise

    async def fetch_markets(self):
        self.catalog_calls += 1
        await self._wait_if_blocked(
            self.block_catalog or (
                self.block_catalog_after_calls is not None
                and self.catalog_calls > self.block_catalog_after_calls
            )
        )
        return await super().fetch_markets()

    async def fetch_volumes(self):
        self.catalog_calls += 1
        await self._wait_if_blocked(
            self.block_catalog or (
                self.block_catalog_after_calls is not None
                and self.catalog_calls > self.block_catalog_after_calls
            )
        )
        return await super().fetch_volumes()

    async def fetch_funding_quote(self, market, *, assumed_open_at):
        await self._wait_if_blocked(self.block_funding)
        return await super().fetch_funding_quote(
            market, assumed_open_at=assumed_open_at
        )


class SlowBootstrapFundingAdapter(FakeAdapter):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.funding_calls = 0
        self.initial_observed_at: datetime | None = None
        self.seeded_observed_at: datetime | None = None

    async def fetch_funding_quote(self, market, *, assumed_open_at):
        self.funding_calls += 1
        if self.funding_calls == 1:
            quote = await super().fetch_funding_quote(
                market, assumed_open_at=assumed_open_at
            )
            self.initial_observed_at = quote.observed_at
            self.clock.advance(10)
            return quote
        if self.funding_calls == 2:
            self.clock.advance(1)
        quote = await super().fetch_funding_quote(
            market, assumed_open_at=assumed_open_at
        )
        if self.funding_calls == 2:
            self.seeded_observed_at = quote.observed_at
        return quote


class GatedRecoveryAdapter(FakeAdapter):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.block_recovery = False
        self.recovery_started = asyncio.Event()
        self.recovery_gate = asyncio.Event()

    async def fetch_book(self, venue_symbol: str):
        if self.block_recovery:
            self.recovery_started.set()
            await self.recovery_gate.wait()
        return await super().fetch_book(venue_symbol)


class CombinedFakeAdapter(FakeAdapter):
    def product_id(self, symbol: str) -> str:
        return symbol

    def subscription(self, kind: str, product: str) -> dict[str, str]:
        return {"kind": kind, "product": product}


class GatedExtendedAdapter(ExtendedAdapter):
    def __init__(self, clock: FakeClock, *, settlement_at: datetime) -> None:
        super().__init__(None)
        fake = FakeAdapter(Venue.EXTENDED, clock, settlement_at=settlement_at)
        self.clock = clock
        self.settlement_at = settlement_at
        self.market = fake.market
        self.calls: list[str] = []
        self.catalog_calls = 0
        self.gate = asyncio.Event()
        self.request_started = asyncio.Event()
        self.cancelled = False

    async def fetch_catalog(self):
        self.catalog_calls += 1
        self.calls.append("catalog")
        if self.catalog_calls > 1:
            self.request_started.set()
            try:
                await self.gate.wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise
        volume = MarketVolume(
            Venue.EXTENDED, self.market.venue_symbol, D("1000000"),
            self.clock.now(), "official-shaped",
        )
        return (self.market,), (volume,)

    async def fetch_book(self, venue_symbol: str):
        self.calls.append("book")
        return OrderBook(
            Venue.EXTENDED, venue_symbol,
            (BookLevel(D("99"), D("20")),),
            (BookLevel(D("101"), D("20")),), self.clock.now(), None,
        )

    async def fetch_required_catalog(self, venue_symbols):
        assert tuple(venue_symbols) == (self.market.venue_symbol,)
        volume = MarketVolume(
            Venue.EXTENDED, self.market.venue_symbol, D("1000000"),
            self.clock.now(), "official-shaped",
        )
        return (self.market,), (volume,)

    async def fetch_funding_quote(self, market, *, assumed_open_at):
        self.calls.append("funding")
        return FundingCashQuote(
            Venue.EXTENDED, market.venue_symbol, self.clock.now(), assumed_open_at,
            self.settlement_at, FundingQuality.PREDICTED,
            FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT, True,
            D("5"), D("5"), "official-shaped",
        )


class GatedRisexAdapter(RisexAdapter):
    def __init__(self, clock: FakeClock, *, settlement_at: datetime) -> None:
        super().__init__(None)
        fake = FakeAdapter(Venue.RISEX, clock, settlement_at=settlement_at)
        self.clock = clock
        self.settlement_at = settlement_at
        self.market = fake.market
        self.calls: list[str] = []
        self._market_ids = {self.market.venue_symbol: "1"}
        self._symbols_by_id = {"1": self.market.venue_symbol}

    async def fetch_markets(self):
        self.calls.append("markets")
        return (self.market,)

    async def fetch_volumes(self):
        self.calls.append("volumes")
        return (
            MarketVolume(
                Venue.RISEX, self.market.venue_symbol, D("1000000"),
                self.clock.now(), "official-shaped",
            ),
        )

    async def fetch_book(self, venue_symbol: str):
        self.calls.append("book")
        return OrderBook(
            Venue.RISEX, venue_symbol,
            (BookLevel(D("99"), D("20")),),
            (BookLevel(D("101"), D("20")),), self.clock.now(),
        )

    async def prime_recent_trade_evidence(self, market, *, limit: int = 20):
        self.calls.append("trades")
        return market

    async def fetch_funding_quote(self, market, *, assumed_open_at):
        self.calls.append("funding")
        return FundingCashQuote(
            Venue.RISEX, market.venue_symbol, self.clock.now(), assumed_open_at,
            self.settlement_at, FundingQuality.ESTIMATED,
            FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT, True,
            D("5"), D("5"), "PAPER_ASSUMPTION:RISEX_PUBLIC_FALLBACK",
        )


class ClosingWebSocket:
    def __init__(
        self,
        stop_event: asyncio.Event,
        *,
        stop_on_iteration: bool,
        fail_subscription: bool = False,
        message_type=None,
    ) -> None:
        self.stop_event = stop_event
        self.stop_on_iteration = stop_on_iteration
        self.fail_subscription = fail_subscription
        self.message_type = message_type

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.message_type is not None:
            message_type, self.message_type = self.message_type, None
            return SimpleNamespace(type=message_type, data=None)
        if self.stop_on_iteration:
            self.stop_event.set()
        raise StopAsyncIteration

    async def send_json(self, _payload) -> None:
        if self.fail_subscription:
            raise ConnectionError("synthetic subscription failure")


class FailedConnection:
    async def __aenter__(self):
        raise ConnectionError("synthetic context failure")

    async def __aexit__(self, *_):
        return None


class ReconnectingSession:
    def __init__(
        self, stop_event: asyncio.Event, outcomes: tuple[str, ...] = ("eof", "stop")
    ) -> None:
        self.stop_event = stop_event
        self.outcomes = outcomes
        self.connections = 0

    def ws_connect(self, *_args, **_kwargs):
        outcome = self.outcomes[self.connections]
        self.connections += 1
        if outcome == "fail_context":
            return FailedConnection()
        return ClosingWebSocket(
            self.stop_event,
            stop_on_iteration=outcome == "stop",
            fail_subscription=outcome == "fail_subscription",
            message_type=(aiohttp.WSMsgType.ERROR if outcome == "error" else None),
        )


class TextWebSocket:
    def __init__(self, stop_event: asyncio.Event, payloads) -> None:
        self.stop_event = stop_event
        self.messages = list(payloads)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.messages:
            self.stop_event.set()
            raise StopAsyncIteration
        return SimpleNamespace(
            type=aiohttp.WSMsgType.TEXT,
            data=json.dumps(self.messages.pop(0)),
        )


class SingleWebSocketSession:
    def __init__(self, websocket: TextWebSocket) -> None:
        self.websocket = websocket
        self.connections = 0

    def ws_connect(self, *_args, **_kwargs):
        self.connections += 1
        return self.websocket


class LighterTextWebSocket(TextWebSocket):
    async def send_json(self, _payload) -> None:
        return None

    async def send_str(self, _payload) -> None:
        return None


def lighter_order_book_snapshot(*, timestamp: int, nonce: int = 1) -> dict[str, object]:
    return {
        "type": "subscribed/order_book",
        "channel": "order_book:1",
        "market_id": 1,
        "timestamp": timestamp,
        "order_book": {
            "code": 0,
            "bids": [{"price": "99", "size": "20"}],
            "asks": [{"price": "101", "size": "20"}],
            "nonce": nonce,
            "begin_nonce": nonce,
        },
    }


def lighter_market_stats_all_snapshot(
    *, timestamp: int, funding_timestamp: int | None = None
) -> dict[str, object]:
    funding_timestamp = timestamp + 1 if funding_timestamp is None else funding_timestamp
    return {
        "type": "subscribed/market_stats",
        "channel": "market_stats:all",
        "timestamp": timestamp,
        "market_stats": {
            "1": {
                "symbol": "ETH",
                "market_id": 1,
                "mark_price": "100",
                "last_trade_price": "100",
                "current_funding_rate": "0.001",
                "funding_rate": "0.0005",
                "funding_timestamp": funding_timestamp,
            },
        },
    }


def lighter_market_stats_all_update(
    *, timestamp: int, funding_timestamp: int, current_rate: str,
    applied_rate: str, mark_price: str = "0.004350",
) -> dict[str, object]:
    return {
        "type": "update/market_stats",
        "channel": "market_stats:all",
        "timestamp": timestamp,
        "market_stats": {
            "1": {
                "symbol": "ETH",
                "market_id": 1,
                "mark_price": mark_price,
                "current_funding_rate": current_rate,
                "funding_rate": applied_rate,
                "funding_timestamp": funding_timestamp,
            },
        },
    }


@pytest.mark.asyncio
async def test_lighter_combined_stream_bootstraps_from_authoritative_subscriptions(
    tmp_path,
):
    clock = FakeClock()
    stop = asyncio.Event()
    adapter = LighterStreamAdapter(clock)
    market = CanonicalMarket(
        "ETH", Venue.LIGHTER, "ETH", MarketType.PERPETUAL,
        ContractType.LINEAR, D("1"), "USDC", "USDC", D("0.1"), D("0.001"),
        D("0.001"), D("10"), None, True, False, False,
    )
    funding = adapter.unknown_funding_quote(
        market, observed_at=clock.now(), assumed_open_at=clock.now()
    )
    class TimedLighterTextWebSocket(LighterTextWebSocket):
        def __init__(self, stop_event, payloads) -> None:
            super().__init__(stop_event, payloads)
            self.index = 0

        async def __anext__(self):
            message = await super().__anext__()
            self.index += 1
            if self.index in {3, 4}:
                clock.advance(1)
            return message

    websocket = TimedLighterTextWebSocket(
        stop,
        (
            lighter_order_book_snapshot(
                timestamp=int(clock.now().timestamp() * 1000)
            ),
            lighter_market_stats_all_snapshot(
                timestamp=int(clock.now().timestamp() * 1000)
            ),
            {"type": "pong"},
            {"type": "pong", "extra": 1},
        ),
    )
    sent = []

    async def capture_send(payload) -> None:
        sent.append(payload)

    websocket.send_json = capture_send
    session = SingleWebSocketSession(websocket)
    with PaperRepository(tmp_path / "lighter-subscription-bootstrap.db") as repository:
        runtime = PublicPaperRuntime(
            repository,
            adapters={Venue.LIGHTER: adapter},
            clock=clock,
        )
        runtime._session = session
        runtime._stop_event = stop
        runtime.observations[(Venue.LIGHTER, "ETH")] = MarketObservation(
            market, None, None, funding, None, trade_stream_ready=False
        )
        session_id = runtime._new_stream_session(
            (Venue.LIGHTER, "*", "combined")
        )
        await runtime._combined_stream(
            Venue.LIGHTER, adapter, ("ETH",), session_id
        )
    stream = runtime.coordinator.stream(Venue.LIGHTER, "ETH")
    observation = runtime.observations[(Venue.LIGHTER, "ETH")]

    assert adapter.fetch_book_calls == 0
    assert [payload["channel"] for payload in sent] == [
        "order_book/1", "market_stats/all",
    ]
    assert stream.book() is not None
    assert stream.book().sequence == 1
    assert stream.health(clock.now()).last_connection_confirmation_at == NOW + timedelta(seconds=1)
    assert stream.health(clock.now()).data_quality is DataQuality.COMPLETE
    assert (Venue.LIGHTER, "ETH") not in runtime._trade_stream_ready
    assert not observation.trade_stream_ready
    assert observation.funding is not None
    assert observation.funding.quality is FundingQuality.PREDICTED


@pytest.mark.asyncio
async def test_lighter_application_heartbeat_is_exact_and_session_owned(
    tmp_path,
):
    clock = FakeClock()
    stop = asyncio.Event()
    adapter = LighterStreamAdapter(clock)
    websocket = LighterTextWebSocket(stop, ())
    sent: list[str] = []

    async def send_str(payload: str) -> None:
        sent.append(payload)

    websocket.send_str = send_str
    sleeps: list[float] = []

    async def fast_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        if len(sleeps) == 2:
            stop.set()
        await asyncio.sleep(0)

    with PaperRepository(tmp_path / "lighter-application-heartbeat.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.LIGHTER: adapter}, clock=clock,
            sleep=fast_sleep,
        )
        runtime._stop_event = stop
        key = (Venue.LIGHTER, "*", "combined")
        session_id = runtime._new_stream_session(key)
        task = asyncio.create_task(
            runtime._lighter_heartbeat(websocket, key, session_id)
        )
        await task

    assert sleeps == [10, 10]
    assert sent == ['{"type":"ping"}']


@pytest.mark.asyncio
async def test_lighter_application_heartbeat_stops_after_session_replacement(
    tmp_path,
):
    clock = FakeClock()
    stop = asyncio.Event()
    adapter = LighterStreamAdapter(clock)
    websocket = LighterTextWebSocket(stop, ())
    sent: list[str] = []

    async def send_str(payload: str) -> None:
        sent.append(payload)

    websocket.send_str = send_str
    sleep_calls: list[float] = []
    key = (Venue.LIGHTER, "*", "combined")

    with PaperRepository(tmp_path / "lighter-heartbeat-replacement.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.LIGHTER: adapter}, clock=clock,
        )
        runtime._stop_event = stop
        session_id = runtime._new_stream_session(key)

        async def replace_after_sleep(seconds: float) -> None:
            sleep_calls.append(seconds)
            runtime._new_stream_session(key)
            await asyncio.sleep(0)

        runtime._sleep = replace_after_sleep
        await runtime._lighter_heartbeat(websocket, key, session_id)

    assert sleep_calls == [10]
    assert sent == []


@pytest.mark.asyncio
async def test_lighter_market_stats_boundary_applies_once_and_keeps_next_prediction(
    tmp_path,
):
    clock = FakeClock(NOW + timedelta(minutes=59))
    boundary = NOW + timedelta(hours=1)
    initial_timestamp = int(clock.now().timestamp() * 1000)
    initial_stats = lighter_market_stats_all_snapshot(
        timestamp=initial_timestamp,
        funding_timestamp=int(NOW.timestamp() * 1000),
    )
    lighter_market = CanonicalMarket(
        "ETH", Venue.LIGHTER, "ETH", MarketType.PERPETUAL,
        ContractType.LINEAR, D("1"), "USDC", "USDC", D("1"), D("1"),
        D("1"), D("10"), None, True, False, False,
    )
    lighter = BoundaryLighterAdapter(clock, lighter_market, initial_stats)
    risex = FakeAdapter(
        Venue.RISEX, clock, settlement_at=boundary, funding_cash="5"
    )
    risex.market = replace(
        risex.market, canonical_asset="ETH", venue_symbol="ETH-RISEX"
    )
    update = lighter_market_stats_all_update(
        timestamp=int((boundary + timedelta(seconds=1)).timestamp() * 1000),
        funding_timestamp=int(boundary.timestamp() * 1000),
        current_rate="0.0074",
        applied_rate="0.0053",
    )
    stale = lighter_market_stats_all_update(
        timestamp=int((boundary + timedelta(seconds=2)).timestamp() * 1000),
        funding_timestamp=int(NOW.timestamp() * 1000),
        current_rate="0.0001",
        applied_rate="0.0001",
    )

    class AdvancingLighterSocket(LighterTextWebSocket):
        def __init__(self, stop_event, payloads) -> None:
            super().__init__(stop_event, payloads)
            self.index = 0

        async def __anext__(self):
            if self.index == 2:
                clock.value = boundary + timedelta(seconds=1)
            message = await super().__anext__()
            self.index += 1
            return message

    stop = asyncio.Event()
    websocket = AdvancingLighterSocket(
        stop,
        (
            lighter_order_book_snapshot(timestamp=initial_timestamp),
            initial_stats,
            update,
            stale,
            update,
        ),
    )
    async def send_json(_payload) -> None:
        return None

    websocket.send_json = send_json
    with PaperRepository(tmp_path / "lighter-market-stats-boundary.db") as repository:
        runtime = PublicPaperRuntime(
            repository,
            adapters={Venue.RISEX: risex, Venue.LIGHTER: lighter},
            clock=clock,
        )
        await runtime.scan()
        assert runtime.last_scan is not None and runtime.last_scan.winner is not None
        broker = PaperEntryBroker()
        await broker.activate(
            runtime.last_scan,
            attempt_id="lighter-boundary",
            activated_at=clock.now(),
        )
        runtime.broker = broker
        confirm_public_streams(runtime, clock.now())
        clock.advance(1)
        await runtime.deliver_trade(maker_trade(runtime, clock.now(), "lighter-boundary-entry"))
        assert runtime.lifecycle is not None
        position = runtime.lifecycle.snapshot.position
        assert position is not None
        pending = next(
            row for row in runtime.lifecycle.snapshot.settlements
            if row.venue is Venue.LIGHTER
        )
        assert pending.status is SettlementStatus.PENDING

        runtime._session = SingleWebSocketSession(websocket)
        runtime._stop_event = stop
        session_id = runtime._new_stream_session(
            (Venue.LIGHTER, "*", "combined")
        )
        await runtime._combined_stream(
            Venue.LIGHTER, lighter, ("ETH",), session_id
        )
        snapshot = runtime.lifecycle.snapshot
        applied = next(
            row for row in snapshot.settlements if row.venue is Venue.LIGHTER
        )
        predicted = runtime.observations[Venue.LIGHTER, "ETH"].funding
        lighter_rows = tuple(
            (row["status"], row["cash_usd"])
            for row in repository.connection.execute(
            "SELECT status,cash_usd FROM funding_settlements "
            "WHERE venue='LIGHTER'"
            ).fetchall()
        )

    assert applied.status is SettlementStatus.APPLIED_RATE
    expected_cash_per_base = D("0.004350") * D("0.0053") / D("100")
    expected_cash = position.canonical_quantity * (
        expected_cash_per_base
        if position.direction.value == "LONG_RISEX_SHORT_HEDGE"
        else -expected_cash_per_base
    )
    assert applied.cash_usd == expected_cash
    assert len(lighter_rows) == 1
    assert lighter_rows[0][0] == "APPLIED_RATE"
    assert D(lighter_rows[0][1]) == expected_cash
    assert predicted is not None and predicted.quality is FundingQuality.PREDICTED
    assert predicted.short_cash_per_canonical_base_usd == D("0.0000003219")
    assert predicted.long_cash_per_canonical_base_usd == D("-0.0000003219")


@pytest.mark.asyncio
async def test_lighter_healthy_ws_funding_is_retained_on_background_refresh(tmp_path):
    clock = FakeClock()

    class NoFundingRestLighterAdapter(LighterStreamAdapter):
        def __init__(self, clock: FakeClock) -> None:
            super().__init__(clock)
            self.fetch_funding_calls = 0

        async def fetch_funding_quote(self, market, *, assumed_open_at):
            self.fetch_funding_calls += 1
            raise AssertionError("healthy Lighter WS funding must be retained")

    adapter = NoFundingRestLighterAdapter(clock)
    market = CanonicalMarket(
        "ETH", Venue.LIGHTER, "ETH", MarketType.PERPETUAL,
        ContractType.LINEAR, D("1"), "USDC", "USDC", D("0.1"), D("0.001"),
        D("0.001"), D("10"), None, True, False, False,
    )
    quote = FundingCashQuote(
        Venue.LIGHTER, "ETH", clock.now(), clock.now(),
        clock.now() + timedelta(hours=1), FundingQuality.PREDICTED,
        FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT, True,
        D("-0.1"), D("0.1"), adapter.WS_SOURCE,
    )
    with PaperRepository(tmp_path / "lighter-ws-funding-refresh.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.LIGHTER: adapter}, clock=clock,
        )
        stream = runtime.coordinator.stream(Venue.LIGHTER, "ETH")
        stream.connected(clock.now())
        stream.snapshot(OrderBook(
            Venue.LIGHTER, "ETH",
            (BookLevel(D("99"), D("20")),),
            (BookLevel(D("101"), D("20")),),
            clock.now(), 10,
        ))
        stream.connection_confirmed(clock.now())
        runtime.observations[(Venue.LIGHTER, "ETH")] = MarketObservation(
            market, None, stream.book(), quote, stream.health(clock.now()),
            trade_stream_ready=False,
        )
        await runtime._market_observation(
            market, clock.now(), background=True
        )
        observation = runtime.observations[(Venue.LIGHTER, "ETH")]

    assert adapter.fetch_funding_calls == 0
    assert observation.funding is not None
    assert observation.funding.source == adapter.WS_SOURCE
    assert observation.funding == quote


@pytest.mark.asyncio
async def test_lighter_ws_snapshot_wins_startup_rest_race(tmp_path):
    clock = FakeClock()
    rest_started = asyncio.Event()
    release_rest = asyncio.Event()

    class DelayedRestLighterAdapter(LighterStreamAdapter):
        async def fetch_book(self, venue_symbol: str) -> OrderBook:
            rest_started.set()
            await release_rest.wait()
            self.fetch_book_calls += 1
            return OrderBook(
                Venue.LIGHTER,
                venue_symbol,
                (BookLevel(D("99"), D("20")),),
                (BookLevel(D("101"), D("20")),),
                self.clock.now(),
                None,
            )

    adapter = DelayedRestLighterAdapter(clock)
    market = CanonicalMarket(
        "ETH", Venue.LIGHTER, "ETH", MarketType.PERPETUAL,
        ContractType.LINEAR, D("1"), "USDC", "USDC", D("0.1"), D("0.001"),
        D("0.001"), D("10"), None, True, False, False,
    )
    with PaperRepository(tmp_path / "lighter-startup-race.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.LIGHTER: adapter}, clock=clock,
        )
        session_id = runtime._new_stream_session(
            (Venue.LIGHTER, "*", "combined")
        )
        observation_task = asyncio.create_task(
            runtime._market_observation(market, clock.now())
        )
        await rest_started.wait()
        assert await runtime.apply_book_event(
            OrderBook(
                Venue.LIGHTER, "ETH",
                (BookLevel(D("99"), D("20")),),
                (BookLevel(D("101"), D("20")),),
                clock.now(), 10,
            ),
            stream_session_id=session_id,
        )
        release_rest.set()
        await observation_task
        assert await runtime.apply_book_event(
            BookDelta(
                Venue.LIGHTER, "ETH",
                (BookLevel(D("100"), D("21")),), (),
                clock.now(), 12, 10,
            ),
            stream_session_id=session_id,
        )
        book = runtime.coordinator.stream(Venue.LIGHTER, "ETH").book()

    assert adapter.fetch_book_calls == 1
    assert book is not None and book.sequence == 12


@pytest.mark.asyncio
async def test_lighter_sequence_less_rest_snapshot_is_rejected_during_recovery(
    tmp_path,
):
    clock = FakeClock()
    adapter = LighterStreamAdapter(clock)
    with PaperRepository(tmp_path / "lighter-sequence-less-recovery.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.LIGHTER: adapter}, clock=clock,
        )
        session_id = runtime._new_stream_session(
            (Venue.LIGHTER, "*", "combined")
        )
        stream = runtime.coordinator.stream(Venue.LIGHTER, "ETH")
        stream.connected(clock.now())
        stream.snapshot(OrderBook(
            Venue.LIGHTER, "ETH",
            (BookLevel(D("99"), D("20")),),
            (BookLevel(D("101"), D("20")),),
            clock.now(), 7,
        ))
        assert not await runtime.apply_book_event(
            BookDelta(
                Venue.LIGHTER, "ETH", (), (), clock.now(), 8, 999,
            ),
            stream_session_id=session_id,
        )
        episode = runtime._recoveries[Venue.LIGHTER, "ETH"]
        assert not await runtime.apply_book_event(
            OrderBook(
                Venue.LIGHTER, "ETH",
                (BookLevel(D("98"), D("20")),),
                (BookLevel(D("102"), D("20")),),
                clock.now(), None,
            ),
            stream_session_id=session_id,
        )
        ignored = repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence "
            "WHERE event_type='PUBLIC_REST_SNAPSHOT_IGNORED_FOR_WS_RESYNC'"
        ).fetchone()[0]

    assert episode.terminal is None
    assert runtime.coordinator.stream(Venue.LIGHTER, "ETH").book() is None
    assert ignored == 1


@pytest.mark.asyncio
async def test_lighter_ws_resubscribe_snapshot_recovers_and_accepts_next_delta(
    tmp_path,
):
    clock = FakeClock()
    stop = asyncio.Event()
    adapter = LighterStreamAdapter(clock)
    sent = []
    timestamp = int(clock.now().timestamp() * 1000)
    bad_update = {
        "type": "update/order_book",
        "channel": "order_book:1",
        "market_id": 1,
        "timestamp": timestamp,
        "order_book": {
            "code": 0,
            "bids": [{"price": "100", "size": "21"}],
            "asks": [],
            "nonce": 2,
            "begin_nonce": 999,
        },
    }
    valid_update = {
        **bad_update,
        "order_book": {
            **bad_update["order_book"],
            "nonce": 4,
            "begin_nonce": 3,
        },
    }
    websocket = LighterTextWebSocket(stop, (
        lighter_order_book_snapshot(timestamp=timestamp, nonce=1),
        bad_update,
        lighter_order_book_snapshot(timestamp=timestamp, nonce=3),
        valid_update,
    ))

    async def capture_send(payload) -> None:
        sent.append(payload)

    websocket.send_json = capture_send
    session = SingleWebSocketSession(websocket)
    with PaperRepository(tmp_path / "lighter-ws-recovery.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.LIGHTER: adapter}, clock=clock,
        )
        runtime._session = session
        runtime._stop_event = stop
        session_id = runtime._new_stream_session(
            (Venue.LIGHTER, "*", "combined")
        )
        await runtime._combined_stream(
            Venue.LIGHTER, adapter, ("ETH",), session_id
        )
        episode = runtime._recoveries[Venue.LIGHTER, "ETH"]
        book = runtime.coordinator.stream(Venue.LIGHTER, "ETH").book()

    assert adapter.fetch_book_calls == 0
    assert [payload["channel"] for payload in sent] == [
        "order_book/1", "market_stats/all", "order_book/1",
    ]
    assert episode.terminal == "COMPLETE"
    assert book is not None and book.sequence == 4


@pytest.mark.asyncio
async def test_lighter_stale_recovery_session_cannot_install_snapshot(tmp_path):
    clock = FakeClock()
    adapter = LighterStreamAdapter(clock)
    with PaperRepository(tmp_path / "lighter-stale-recovery.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.LIGHTER: adapter}, clock=clock,
        )
        old_session = runtime._new_stream_session(
            (Venue.LIGHTER, "*", "combined")
        )
        stream = runtime.coordinator.stream(Venue.LIGHTER, "ETH")
        stream.connected(clock.now())
        stream.snapshot(OrderBook(
            Venue.LIGHTER, "ETH",
            (BookLevel(D("99"), D("20")),),
            (BookLevel(D("101"), D("20")),),
            clock.now(), 1,
        ))
        assert not await runtime.apply_book_event(
            BookDelta(
                Venue.LIGHTER, "ETH", (), (), clock.now(), 2, 999,
            ),
            stream_session_id=old_session,
        )
        new_session = runtime._new_stream_session(
            (Venue.LIGHTER, "*", "combined")
        )
        assert runtime._replace_displaced_combined_recoveries(
            Venue.LIGHTER, ("ETH",), new_session
        ) == ("ETH",)
        successor = runtime._recoveries[Venue.LIGHTER, "ETH"]
        assert not await runtime.apply_book_event(
            OrderBook(
                Venue.LIGHTER, "ETH",
                (BookLevel(D("98"), D("20")),),
                (BookLevel(D("102"), D("20")),),
                clock.now(), 3,
            ),
            stream_session_id=old_session,
        )
        assert runtime._recoveries[Venue.LIGHTER, "ETH"] is successor
        assert await runtime.apply_book_event(
            OrderBook(
                Venue.LIGHTER, "ETH",
                (BookLevel(D("98"), D("20")),),
                (BookLevel(D("102"), D("20")),),
                clock.now(), 3,
            ),
            stream_session_id=new_session,
        )
        book = stream.book()

    assert book is not None and book.sequence == 3


@pytest.mark.asyncio
async def test_lighter_failed_recovery_does_not_storm_on_same_session(tmp_path):
    clock = FakeClock()
    adapter = LighterStreamAdapter(clock)
    with PaperRepository(tmp_path / "lighter-recovery-storm.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.LIGHTER: adapter}, clock=clock,
        )
        session_id = runtime._new_stream_session(
            (Venue.LIGHTER, "*", "combined")
        )
        stream = runtime.coordinator.stream(Venue.LIGHTER, "ETH")
        stream.connected(clock.now())
        stream.snapshot(OrderBook(
            Venue.LIGHTER, "ETH",
            (BookLevel(D("99"), D("20")),),
            (BookLevel(D("101"), D("20")),),
            clock.now(), 1,
        ))
        assert not await runtime.apply_book_event(
            BookDelta(
                Venue.LIGHTER, "ETH", (), (), clock.now(), 2, 999,
            ),
            stream_session_id=session_id,
        )
        key = (Venue.LIGHTER, "ETH")
        episode = runtime._recoveries[key]
        runtime._fail_lighter_snapshot_recovery(
            key, episode, cause="WS_SNAPSHOT_TIMEOUT", at=clock.now()
        )
        for _ in range(3):
            assert runtime._start_snapshot_recovery(
                Venue.LIGHTER, "ETH",
                displaced_stream_session_id=session_id,
            ) is episode
            assert not await runtime.apply_book_event(
                BookDelta(
                    Venue.LIGHTER, "ETH", (), (), clock.now(), 3, 999,
                ),
                stream_session_id=session_id,
            )
        started = repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence "
            "WHERE event_type='PUBLIC_SNAPSHOT_RECOVERY_STARTED'"
        ).fetchone()[0]
        failed = repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence "
            "WHERE event_type='PUBLIC_SNAPSHOT_RECOVERY_FAILED'"
        ).fetchone()[0]

    assert episode.terminal == "FAILED"
    assert started == 1
    assert failed == 1


@pytest.mark.asyncio
async def test_lighter_combined_subscriptions_cap_inflight_without_route_cutoff(
    tmp_path,
):
    clock = FakeClock()
    stop = asyncio.Event()
    symbols = tuple(f"S{index}" for index in range(25))

    class ManyLighterStreamAdapter(LighterStreamAdapter):
        def __init__(self, clock: FakeClock) -> None:
            super().__init__(clock)
            self._market_ids = {
                symbol: index + 1 for index, symbol in enumerate(symbols)
            }
            self._symbols_by_id = {
                index + 1: symbol for index, symbol in enumerate(symbols)
            }

    adapter = ManyLighterStreamAdapter(clock)
    sent = []
    timestamp = int(clock.now().timestamp() * 1000)
    messages = []
    for index, symbol in enumerate(symbols, start=1):
        messages.append({
            "type": "subscribed/order_book",
            "channel": f"order_book:{index}",
            "market_id": index,
            "timestamp": timestamp,
            "order_book": {
                "code": 0,
                "bids": [{"price": "99", "size": "20"}],
                "asks": [{"price": "101", "size": "20"}],
                "nonce": 1,
                "begin_nonce": 1,
            },
        })
    first_read_sent_count = None

    class ObservingLighterWebSocket(LighterTextWebSocket):
        async def __anext__(self):
            nonlocal first_read_sent_count
            if first_read_sent_count is None:
                first_read_sent_count = len(sent)
            return await super().__anext__()

    websocket = ObservingLighterWebSocket(stop, tuple(messages))

    async def capture_send(payload) -> None:
        sent.append(payload)

    websocket.send_json = capture_send
    session = SingleWebSocketSession(websocket)
    with PaperRepository(tmp_path / "lighter-subscription-cap.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.LIGHTER: adapter}, clock=clock,
        )
        runtime._session = session
        runtime._stop_event = stop
        session_id = runtime._new_stream_session(
            (Venue.LIGHTER, "*", "combined")
        )
        await runtime._combined_stream(
            Venue.LIGHTER, adapter, symbols, session_id
        )

    assert first_read_sent_count == 26
    assert len(sent) == 26
    assert sent[25]["channel"] == "market_stats/all"
    assert len({
        payload["channel"] for payload in sent
        if payload["channel"].startswith("order_book/")
    }) == 25
    assert not any(
        payload["channel"].startswith("trade/") for payload in sent
    )


def lighter_trade_message(
    *, message_type: str, nonce: int, trade_id: int, channel: str = "trade:1"
) -> dict[str, object]:
    return {
        "type": message_type,
        "channel": channel,
        "nonce": nonce,
        "trades": [{
            "type": "trade",
            "market_id": 1,
            "trade_id": trade_id,
            "timestamp": str(int(NOW.timestamp() * 1000)),
            "size": "1",
            "price": "100",
            "is_maker_ask": False,
        }],
    }


def assert_socket_episode(rows):
    assert [row["event_type"] for row in rows] == [
        "PUBLIC_SOCKET_DISCONNECTED", "PUBLIC_SOCKET_RECONNECTED",
    ]
    disconnected = json.loads(rows[0]["detail"])
    reconnected = json.loads(rows[1]["detail"])
    assert disconnected == {
        key: value for key, value in reconnected.items()
        if key not in {"reconnected_at", "reconnected_stream_session_id"}
    }
    assert disconnected["disconnected_at"] == rows[0]["recorded_at"]
    assert reconnected["reconnected_at"] == rows[1]["recorded_at"]
    assert datetime.fromisoformat(reconnected["reconnected_at"]) >= datetime.fromisoformat(
        disconnected["disconnected_at"]
    )
    return disconnected, reconnected


class JsonResponse:
    def __init__(self, payload):
        self.payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    def raise_for_status(self):
        return None

    async def text(self):
        return json.dumps(self.payload)


class OfficialShapeRisexSession:
    def __init__(self, settlement_at: datetime):
        ns = str(int(settlement_at.timestamp() * 1_000_000_000))
        self.market = {
            "market_id": "7", "config": {"name": "ABC/USDC", "step_size": "1", "step_price": "1", "min_order_size": "1", "unlocked": True},
            "base_asset_symbol": "ABC/USDC", "quote_asset_symbol": "USDC",
            "underlying": "ABC/USDC", "display_name": "ABC/USDC",
            "display_base_asset_symbol": "ABC/USDC", "active": True,
            "quote_volume_24h": "1000000", "current_funding_rate": "0.05",
            "mark_price": "100", "next_funding_time": ns,
        }
        self.book = {"market_id": "7", "bids": [{"price": "99", "quantity": "20"}], "asks": [{"price": "101", "quantity": "20"}]}
        self.trades = [{"id": "recent", "price": "100", "size": "2", "time": ns, "maker_side": 0}]

    def get(self, url: str, **_):
        if url.endswith("/v1/markets"):
            return JsonResponse({"data": {"markets": [self.market]}})
        if url.endswith("/v1/orderbook"):
            return JsonResponse({"data": self.book})
        if "trade-history" in url:
            return JsonResponse({"data": {"trades": self.trades}})
        raise AssertionError(url)


class DelayedAppliedRisexAdapter(RisexAdapter):
    def __init__(self, settlement_at: datetime):
        super().__init__(None)
        self.settlement_at = settlement_at
        self.calls: list[datetime] = []

    async def fetch_applied_funding_quotes(self, market, *, since, until, assumed_open_at):
        self.calls.append(since)
        if len(self.calls) == 1:
            return ()
        return (FundingCashQuote(
            Venue.RISEX, market.venue_symbol, until, assumed_open_at,
            self.settlement_at, FundingQuality.APPLIED_RATE,
            FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT, True,
            D("-1"), D("1"), self.FUNDING_SOURCE,
        ),)

def adapters(clock: FakeClock, *, settlement_at: datetime, risex=True, funding="5"):
    return {
        venue: FakeAdapter(
            venue,
            clock,
            settlement_at=settlement_at,
            available=risex if venue is Venue.RISEX else True,
            funding_cash=funding,
        )
        for venue in (Venue.RISEX, Venue.EXTENDED, Venue.NADO)
    }


def confirm_extended_stream(
    runtime: PublicPaperRuntime,
    symbol: str,
    kind: str,
    at: datetime,
    *,
    data_ready: bool,
) -> None:
    key = (Venue.EXTENDED, symbol, kind)
    session_id = runtime._stream_sessions.get(key)
    if session_id is None:
        session_id = runtime._new_stream_session(key)
    PublicPaperRuntime._confirm_extended_stream(
        runtime, symbol, kind, at,
        data_ready=data_ready, stream_session_id=session_id,
    )


def stream_session(
    runtime: PublicPaperRuntime, venue: Venue, symbol: str, kind: str
):
    key = (
        (venue, "*", "combined")
        if venue is not Venue.EXTENDED or kind == "combined"
        else (venue, symbol, kind)
    )
    session_id = runtime._stream_sessions.get(key)
    if session_id is None:
        session_id = runtime._new_stream_session(key)
    return session_id


def confirm_public_streams(runtime: PublicPaperRuntime, at: datetime) -> None:
    for venue, symbol in runtime.observations:
        runtime.mark_trade_stream_connected(venue, symbol, at=at)
        if venue is Venue.EXTENDED:
            confirm_extended_stream(runtime, symbol, "book", at, data_ready=True)
            confirm_extended_stream(runtime, symbol, "funding", at, data_ready=False)
        else:
            runtime._live_book_ready.add((venue, symbol))


async def activate_with_live_streams(
    runtime: PublicPaperRuntime, clock: FakeClock
) -> None:
    await runtime.scan()
    confirm_public_streams(runtime, clock.now())
    clock.advance(1)
    await runtime.tick()
    assert runtime.broker is not None


def paper_entry_cancellations(
    repository: PaperRepository,
) -> list[dict[str, object]]:
    return [
        json.loads(row["detail"])
        for row in repository.connection.execute(
            "SELECT detail FROM runtime_evidence "
            "WHERE event_type='PAPER_ENTRY_CANCELLED_NO_FILL' "
            "ORDER BY evidence_id"
        )
    ]


@pytest.mark.asyncio
async def test_injected_ordinary_public_scan_builds_real_observations_and_diagnostics(tmp_path):
    clock = FakeClock()
    fakes = adapters(clock, settlement_at=NOW + timedelta(minutes=5))
    with PaperRepository(tmp_path / "scan.db") as repository:
        result = await public_scan_once(repository, adapters=fakes, clock=clock)
        report = repository.report(as_of=NOW)
    assert result["status"] == "OPPORTUNITY"
    assert result["routes"]
    assert len(result["routes"]) <= 15
    assert result["routes"][0]["planned_maker_net_pnl_usd"] is not None
    assert result["routes"][0]["rank"] == 1
    assert result["routes"][0]["seconds_to_earliest_funding"] == "300.0"
    for key in (
        "risex_exact_q_entry_vwap_usd", "risex_exact_q_exit_vwap_usd",
        "hedge_maker_entry_price_usd", "planned_hedge_exit_price_usd",
        "risex_funding_usd", "hedge_funding_usd", "net_funding_usd",
        "bbo_spread_usd", "taker_slippage_usd",
        "quoted_spread_plus_exact_slippage_proxy_usd",
    ):
        assert result["routes"][0][key] is not None
    route = result["routes"][0]
    assert D(route["planned_entry_fees_usd"]) + D(route["planned_exit_fees_usd"]) == D(
        route["planned_fees_usd"]
    )
    assert result["assumption_flags"]["risex_next_rate_estimate_is_a_paper_assumption"]
    assert report["runtime_evidence_count"] >= 4
    assert report["latest_routes"] == result["routes"]
    assert report["latest_routes"][0]["source_quality"]["risex_funding"]["marker"] == "PAPER_ASSUMPTION"
    assert all({"markets", "volumes", "book", "funding"} <= set(fake.calls) for fake in fakes.values())


@pytest.mark.asyncio
async def test_nado_future_book_timestamp_does_not_abort_initial_tick(tmp_path):
    clock = FakeClock()
    fakes = adapters(clock, settlement_at=NOW + timedelta(minutes=5))
    nado = fakes[Venue.NADO]
    payload = json.loads(
        (Path(__file__).parent / "fixtures" / "paper_002" / "nado.json").read_text()
    )
    normalizer = NadoAdapter(None)
    nado.market = normalizer.normalize_market(payload["market"])
    payload["book"]["timestamp"] = str(
        int((clock.now() + timedelta(seconds=1)).timestamp() * 1_000_000_000)
    )

    async def future_timestamp_book(_symbol):
        nado._ready("book")
        return normalizer.normalize_book(payload["book"], observed_at=clock.now())

    nado.fetch_book = future_timestamp_book
    with PaperRepository(tmp_path / "nado-future-book.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            await runtime.tick()
            observation = runtime.observations[
                Venue.NADO, nado.market.venue_symbol
            ]
            assert runtime.last_scan is not None
            assert observation.book is not None
            assert observation.book.observed_at == runtime.last_scan.logical_at


@pytest.mark.asyncio
async def test_extended_future_ws_book_timestamp_does_not_abort_followup_scan(tmp_path):
    clock = FakeClock()
    fakes = adapters(clock, settlement_at=NOW + timedelta(minutes=5))
    extended = GatedExtendedAdapter(
        clock, settlement_at=NOW + timedelta(minutes=5)
    )
    fakes[Venue.EXTENDED] = extended
    symbol = extended.market.venue_symbol
    stop = asyncio.Event()
    future_ts = int((clock.now() + timedelta(seconds=1)).timestamp() * 1000)
    payload = {
        "type": "SNAPSHOT", "seq": 2, "ts": future_ts,
        "data": {
            "m": symbol,
            "b": [{"p": "99", "q": "20"}],
            "a": [{"p": "101", "q": "20"}],
        },
    }

    with PaperRepository(tmp_path / "extended-future-ws-book.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            await runtime.tick()
            runtime._session = SingleWebSocketSession(
                TextWebSocket(stop, [payload])
            )
            runtime._stop_event = stop
            session_id = runtime._new_stream_session(
                (Venue.EXTENDED, symbol, "book")
            )
            await runtime._extended_stream(
                extended, symbol, "book", session_id
            )
            book = runtime.coordinator.stream(
                Venue.EXTENDED, symbol
            ).book()
            assert book is not None
            observation = runtime.observations[Venue.EXTENDED, symbol]
            runtime.observations[Venue.EXTENDED, symbol] = replace(
                observation, book=book
            )
            runtime._session = None
            await runtime.scan(
                refresh=False, scan_kind="FOCUSED", scheduled_at=clock.now()
            )
            assert book.observed_at == clock.now()


@pytest.mark.asyncio
async def test_risex_future_ws_book_timestamp_does_not_abort_followup_scan(tmp_path):
    clock = FakeClock()
    fakes = adapters(clock, settlement_at=NOW + timedelta(minutes=5))
    risex = GatedRisexAdapter(clock, settlement_at=NOW + timedelta(minutes=5))
    fakes[Venue.RISEX] = risex
    symbol = risex.market.venue_symbol
    stop = asyncio.Event()
    future_ts = int((clock.now() + timedelta(seconds=1)).timestamp() * 1_000_000_000)
    payload = {
        "channel": "orderbook", "type": "snapshot", "method": "snapshot",
        "market_id": "1", "worker_timestamp": future_ts,
        "data": {
            "market_id": 1,
            "bids": [{"price": "99", "quantity": "20"}],
            "asks": [{"price": "101", "quantity": "20"}],
        },
    }

    async def no_delay(_seconds: float) -> None:
        await asyncio.sleep(0)

    class RisexWebSocket(TextWebSocket):
        async def send_json(self, _payload) -> None:
            return None

        async def pong(self, _payload) -> None:
            return None

    with PaperRepository(tmp_path / "risex-future-ws-book.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock, sleep=no_delay
        ) as runtime:
            await runtime.tick()
            runtime._session = SingleWebSocketSession(
                RisexWebSocket(stop, [payload])
            )
            runtime._stop_event = stop
            session_id = runtime._new_stream_session(
                (Venue.RISEX, "*", "combined")
            )
            await runtime._combined_stream(
                Venue.RISEX, risex, (symbol,), session_id
            )
            book = runtime.coordinator.stream(Venue.RISEX, symbol).book()
            assert book is not None
            runtime._session = None
            await runtime.scan(
                refresh=False, scan_kind="FOCUSED", scheduled_at=clock.now()
            )
            assert book.observed_at == clock.now()


@pytest.mark.asyncio
async def test_risex_untrusted_future_observation_still_fails_closed(tmp_path):
    clock = FakeClock()
    fakes = adapters(clock, settlement_at=NOW + timedelta(minutes=5))
    risex = GatedRisexAdapter(clock, settlement_at=NOW + timedelta(minutes=5))
    fakes[Venue.RISEX] = risex
    with PaperRepository(
        tmp_path / "risex-future-observation-rejected.db"
    ) as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            await runtime.tick()
            key = Venue.RISEX, risex.market.venue_symbol
            observation = runtime.observations[key]
            assert observation.book is not None
            runtime.observations[key] = replace(
                observation,
                book=replace(
                    observation.book,
                    observed_at=clock.now() + timedelta(seconds=1),
                ),
            )
            with pytest.raises(
                RuntimeError, match="scan observation timestamp exceeds logical_at"
            ):
                await runtime.scan(
                    refresh=False, scan_kind="FOCUSED", scheduled_at=clock.now()
                )


@pytest.mark.asyncio
async def test_extended_untrusted_future_observation_still_fails_closed(tmp_path):
    clock = FakeClock()
    fakes = adapters(clock, settlement_at=NOW + timedelta(minutes=5))
    extended = GatedExtendedAdapter(
        clock, settlement_at=NOW + timedelta(minutes=5)
    )
    fakes[Venue.EXTENDED] = extended
    with PaperRepository(
        tmp_path / "extended-future-observation-rejected.db"
    ) as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            await runtime.tick()
            key = Venue.EXTENDED, extended.market.venue_symbol
            observation = runtime.observations[key]
            assert observation.book is not None
            runtime.observations[key] = replace(
                observation,
                book=replace(
                    observation.book,
                    observed_at=clock.now() + timedelta(seconds=1),
                ),
            )
            with pytest.raises(
                RuntimeError, match="scan observation timestamp exceeds logical_at"
            ):
                await runtime.scan(
                    refresh=False, scan_kind="FOCUSED", scheduled_at=clock.now()
                )


@pytest.mark.asyncio
async def test_scan_still_rejects_an_untrusted_future_observation(tmp_path):
    clock = FakeClock()
    fakes = adapters(clock, settlement_at=NOW + timedelta(minutes=5))
    with PaperRepository(tmp_path / "future-observation-rejected.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            await runtime.tick()
            key = Venue.NADO, fakes[Venue.NADO].market.venue_symbol
            observation = runtime.observations[key]
            assert observation.book is not None
            runtime.observations[key] = replace(
                observation,
                book=replace(
                    observation.book,
                    observed_at=clock.now() + timedelta(seconds=1),
                ),
            )
            with pytest.raises(
                RuntimeError, match="scan observation timestamp exceeds logical_at"
            ):
                await runtime.scan(
                    refresh=False, scan_kind="FOCUSED", scheduled_at=clock.now()
                )


@pytest.mark.asyncio
async def test_route_output_keeps_all_evaluated_routes_after_system_sort(tmp_path):
    clock = FakeClock()
    many = {
        venue: ManyFakeAdapter(venue, clock, settlement_at=NOW + timedelta(minutes=5))
        for venue in (Venue.RISEX, Venue.EXTENDED, Venue.NADO)
    }
    with PaperRepository(tmp_path / "route-limit.db") as repository:
        result = await public_scan_once(repository, adapters=many, clock=clock)
        persisted = repository.report(as_of=NOW)["latest_routes"]
    assert len(result["routes"]) == 20
    assert len(persisted) == 20


@pytest.mark.asyncio
async def test_asymmetric_catalog_add_remove_reconciles_all_route_subscriptions(tmp_path):
    clock = FakeClock()
    settlement_at = NOW + timedelta(minutes=5)
    fakes = {
        Venue.RISEX: DynamicCatalogAdapter(
            Venue.RISEX, clock, settlement_at=settlement_at,
            assets=("A", "B", "C", "D", "E"),
        ),
        Venue.EXTENDED: DynamicCatalogAdapter(
            Venue.EXTENDED, clock, settlement_at=settlement_at,
            assets=("A", "C"),
        ),
        Venue.NADO: DynamicCatalogAdapter(
            Venue.NADO, clock, settlement_at=settlement_at,
            assets=("B", "C"),
        ),
    }

    class IdleSession:
        closed = False

        async def close(self):
            self.closed = True

    with PaperRepository(tmp_path / "asymmetric-catalog.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            initial = await runtime.scan()
            initial_keys = {
                (row["canonical_asset"], row["hedge_venue"], row["direction"])
                for row in initial["routes"]
            }
            assert initial_keys == {
                (asset, venue.value, direction)
                for asset, venue in (
                    ("A", Venue.EXTENDED), ("B", Venue.NADO),
                    ("C", Venue.EXTENDED), ("C", Venue.NADO),
                )
                for direction in (
                    "LONG_RISEX_SHORT_HEDGE", "SHORT_RISEX_LONG_HEDGE",
                )
            }

            stop = asyncio.Event()
            runtime._session = IdleSession()
            runtime._stop_event = stop

            async def hold_combined(_venue, _adapter, _symbols, _session_id):
                await stop.wait()

            runtime._combined_stream = hold_combined
            await runtime._refresh_public_data()
            assert runtime._combined_symbols[Venue.RISEX] == (
                "A-RISEX", "B-RISEX", "C-RISEX"
            )
            assert runtime._combined_symbols[Venue.NADO] == (
                "B-NADO", "C-NADO"
            )

            fakes[Venue.RISEX].catalog = tuple(
                replace(
                    fakes[Venue.RISEX].catalog[0],
                    canonical_asset=asset,
                    venue_symbol=f"{asset}-RISEX",
                )
                for asset in ("A", "B", "C", "F")
            )
            fakes[Venue.EXTENDED].catalog = (
                replace(
                    fakes[Venue.EXTENDED].catalog[0],
                    canonical_asset="A", venue_symbol="A-EXTENDED",
                ),
            )
            fakes[Venue.NADO].catalog = tuple(
                replace(
                    fakes[Venue.NADO].catalog[0],
                    canonical_asset=asset,
                    venue_symbol=f"{asset}-NADO",
                )
                for asset in ("B", "C", "F")
            )
            previous_generation = runtime._catalog_generation
            await asyncio.gather(*(
                runtime._catalog(venue, adapter)
                for venue, adapter in fakes.items()
            ))
            assert runtime._catalog_generation > previous_generation
            assert runtime._catalog_refresh_pending
            await runtime._refresh_public_data()

            assert runtime._required_symbols(Venue.EXTENDED) == {"A-EXTENDED"}
            assert runtime._combined_symbols[Venue.RISEX] == (
                "A-RISEX", "B-RISEX", "C-RISEX", "F-RISEX"
            )
            assert runtime._combined_symbols[Venue.NADO] == (
                "B-NADO", "C-NADO", "F-NADO"
            )
            assert (Venue.EXTENDED, "C-EXTENDED") not in runtime.observations
            assert (Venue.NADO, "F-NADO") in runtime.observations

            clock.advance(1)
            updated = await runtime.scan(refresh=False, scan_kind="FULL")
            updated_keys = {
                (row["canonical_asset"], row["hedge_venue"], row["direction"])
                for row in updated["routes"]
            }
            assert updated_keys == {
                (asset, venue.value, direction)
                for asset, venue in (
                    ("A", Venue.EXTENDED), ("B", Venue.NADO),
                    ("C", Venue.NADO), ("F", Venue.NADO),
                )
                for direction in (
                    "LONG_RISEX_SHORT_HEDGE", "SHORT_RISEX_LONG_HEDGE",
                )
            }


@pytest.mark.asyncio
async def test_same_scan_primes_official_shaped_risex_rest_evidence(tmp_path):
    now = datetime.now(UTC)
    clock = FakeClock(now)
    target = now + timedelta(minutes=5)
    fakes = adapters(clock, settlement_at=target)
    fakes[Venue.RISEX] = RisexAdapter(OfficialShapeRisexSession(target))
    with PaperRepository(tmp_path / "rest-prime.db") as repository:
        result = await public_scan_once(repository, adapters=fakes)
    assert result["routes"]
    computed = [row for row in result["routes"] if row.get("canonical_quantity") is not None]
    assert computed, result
    assert computed[0]["risex_exact_q_entry_vwap_usd"] is not None
    assert computed[0]["planned_maker_net_pnl_usd"] is not None
    assert computed[0]["risex_contract_assumption_used"] is True


@pytest.mark.asyncio
async def test_scan_once_waits_for_complete_rest_snapshot(tmp_path):
    clock = FakeClock()
    fakes = adapters(clock, settlement_at=NOW + timedelta(minutes=5))
    gated = GatedAdapter(
        Venue.EXTENDED, clock, settlement_at=NOW + timedelta(minutes=5)
    )
    gated.block_funding = True
    fakes[Venue.EXTENDED] = gated
    with PaperRepository(tmp_path / "synchronous-scan.db") as repository:
        task = asyncio.create_task(
            public_scan_once(repository, adapters=fakes, clock=clock)
        )
        await gated.request_started.wait()
        assert not task.done()
        gated.gate.set()
        result = await task
    assert result["routes"]
    assert {"markets", "volumes", "book", "funding"} <= set(gated.calls)


@pytest.mark.asyncio
async def test_transient_catalog_failure_retains_last_good_and_fails_component(tmp_path):
    clock = FakeClock()
    fakes = adapters(clock, settlement_at=NOW + timedelta(minutes=5))
    with PaperRepository(tmp_path / "last-good.db") as repository:
        async with PublicPaperRuntime(repository, adapters=fakes, clock=clock) as runtime:
            await runtime.scan()
            before = runtime.markets[Venue.RISEX]
            fakes[Venue.RISEX].available = False
            await runtime._catalog(Venue.RISEX, fakes[Venue.RISEX])
            assert runtime.markets[Venue.RISEX] == before
            assert not runtime.readiness[Venue.RISEX].available
            clock.advance(1)
            result = await runtime.scan(refresh=False)
            assert result["status"] == "NO_TRADE"
    assert result["venue_readiness"]["RISEX"]["components"]["catalog"]["available"] is False


@pytest.mark.asyncio
async def test_venue_outage_is_specific_and_never_uses_empty_fail_closed_scan(tmp_path):
    clock = FakeClock()
    fakes = adapters(clock, settlement_at=NOW + timedelta(minutes=5), risex=False)
    with PaperRepository(tmp_path / "blocked.db") as repository:
        result = await public_scan_once(repository, adapters=fakes, clock=clock)
        failure = json.loads(repository.connection.execute(
            "SELECT detail FROM runtime_evidence "
            "WHERE event_type='PUBLIC_REQUEST_FAILED' AND venue='RISEX' LIMIT 1"
        ).fetchone()[0])
    assert result["status"] == "NO_TRADE"
    assert result["reason"] == "VENUE_SPECIFIC_BLOCKERS"
    assert result["routes"] == []
    assert result["venue_readiness"]["RISEX"]["detail"].startswith("PUBLIC_REST_UNAVAILABLE")
    assert {
        "component", "endpoint_class", "exception_class", "elapsed_ms", "http_status",
        "retry_state", "retry_backoff_seconds",
    } <= failure.keys()
    assert failure["retry_state"] == "NEXT_ABSOLUTE_FULL_SLOT"
    assert failure["retry_backoff_seconds"] == 120
    assert all("fail_closed_scan" not in call for fake in fakes.values() for call in fake.calls)


@pytest.mark.asyncio
async def test_unknown_official_risex_source_is_marked_unknown(tmp_path):
    clock = FakeClock()
    fakes = adapters(clock, settlement_at=NOW + timedelta(minutes=5))
    fakes[Venue.RISEX].funding_unknown = True
    with PaperRepository(tmp_path / "unknown-source.db") as repository:
        result = await public_scan_once(repository, adapters=fakes, clock=clock)
    assert result["routes"]
    assert result["routes"][0]["source_quality"]["risex_funding"]["marker"] == "UNKNOWN"


def test_runtime_has_no_private_auth_real_order_or_llm_surface() -> None:
    source = (Path(__file__).parents[1] / "src/risex_farmer/runtime.py").read_text()
    forbidden = ("Authorization", "api_key", "private_endpoint", "place_order", "submit_order", "openai", "anthropic")
    assert all(token not in source for token in forbidden)


@pytest.mark.asyncio
async def test_readiness_evidence_is_transition_bounded(tmp_path):
    clock = FakeClock()
    with PaperRepository(tmp_path / "bounded.db") as repository:
        async with PublicPaperRuntime(
            repository,
            adapters=adapters(clock, settlement_at=NOW + timedelta(minutes=5)),
            clock=clock,
        ) as runtime:
            runtime._set_readiness(Venue.EXTENDED, True, "READY", clock.now())
            for index in range(100):
                runtime._set_readiness(
                    Venue.EXTENDED,
                    bool(index % 2),
                    "READY" if index % 2 else "DISCONNECTED",
                    clock.now(),
                )
            count = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence WHERE venue='EXTENDED'"
            ).fetchone()[0]
    assert count == 1


def test_component_readiness_is_symbol_scoped_and_obsolete_failures_are_removed(tmp_path):
    clock = FakeClock()
    with PaperRepository(tmp_path / "component-readiness.db") as repository:
        runtime = PublicPaperRuntime(repository, adapters={}, clock=clock)
        runtime._set_component_readiness(
            Venue.EXTENDED, "catalog", True, "READY", clock.now()
        )
        for component in ("book", "trade", "funding", "connection"):
            runtime._set_component_readiness(
                Venue.EXTENDED, f"{component}:GOOD", True, "READY", clock.now()
            )
        runtime._set_component_readiness(
            Venue.EXTENDED, "book:BAD", False, "GAP", clock.now()
        )
        assert runtime._symbol_components_available(Venue.EXTENDED, "GOOD")
        assert not runtime._symbol_components_available(Venue.EXTENDED, "BAD")
        assert runtime.readiness[Venue.EXTENDED].available
        runtime._remove_obsolete_components(Venue.EXTENDED, {"GOOD"}, clock.now())
    assert "book:BAD" not in runtime.component_readiness[Venue.EXTENDED]


def test_component_reconcile_preserves_concrete_extended_failure_detail(tmp_path):
    clock = FakeClock()
    detail = "PUBLIC_REST_UNAVAILABLE:TimeoutError"
    with PaperRepository(tmp_path / "component-failure-detail.db") as repository:
        runtime = PublicPaperRuntime(repository, adapters={}, clock=clock)
        runtime._set_component_readiness(
            Venue.EXTENDED, "catalog", False, detail, clock.now()
        )
        runtime._set_component_readiness(
            Venue.EXTENDED, "book:ABC-EXTENDED", True, "READY", clock.now()
        )
        runtime._remove_obsolete_components(
            Venue.EXTENDED, {"ABC-EXTENDED"}, clock.now()
        )
    assert not runtime.readiness[Venue.EXTENDED].available
    assert runtime.readiness[Venue.EXTENDED].detail == detail


@pytest.mark.asyncio
async def test_shared_public_runtime_session_has_thirty_second_timeout_and_closes(tmp_path):
    with PaperRepository(tmp_path / "public-session-timeout.db") as repository:
        async with PublicPaperRuntime(repository) as runtime:
            session = runtime._session
            assert session is not None
            assert session.timeout.total == 30
            assert not session.closed
    assert session.closed


@pytest.mark.asyncio
async def test_paper_run_persists_through_no_trade_until_explicit_stop(tmp_path):
    clock = FakeClock()
    fakes = adapters(
        clock, settlement_at=NOW + timedelta(minutes=5), funding="0"
    )
    stop = asyncio.Event()
    with PaperRepository(tmp_path / "run.db") as repository:
        task = asyncio.create_task(
            public_paper_run(repository, adapters=fakes, clock=clock, stop_event=stop)
        )
        for _ in range(100):
            await asyncio.sleep(0)
            if repository.connection.execute("SELECT COUNT(*) FROM scanner_snapshots").fetchone()[0]:
                break
        assert not task.done()
        stop.set()
        result = await task
        report = repository.report(as_of=NOW)
        stopped = json.loads(repository.connection.execute(
            "SELECT detail FROM runtime_evidence WHERE event_type='STOPPED_SAFE' "
            "ORDER BY evidence_id DESC LIMIT 1"
        ).fetchone()[0])
    assert result == {"status": "STOPPED_SAFE", "forced_close": False}
    assert report["last_runtime_event"]["event_type"] == "STOPPED_SAFE"
    assert stopped["stop_cause"] == "STOP_EVENT"
    assert stopped["requested_at"] == NOW.isoformat()
    assert "signal" not in stopped


@pytest.mark.asyncio
async def test_runtime_fatal_is_distinct_from_intentional_safe_stop(tmp_path):
    clock = FakeClock()
    with PaperRepository(tmp_path / "runtime-fatal.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters=adapters(clock, settlement_at=NOW), clock=clock,
        )

        async def fatal_scan(*_args, **_kwargs):
            raise RuntimeError("synthetic internal failure")

        runtime.scan = fatal_scan
        with pytest.raises(RuntimeError, match="synthetic internal failure"):
            await runtime.run()
        rows = repository.connection.execute(
            "SELECT event_type,detail FROM runtime_evidence ORDER BY evidence_id"
        ).fetchall()
    assert [row["event_type"] for row in rows][-2:] == [
        "RUNTIME_FATAL", "RUNTIME_STOPPED_FATAL",
    ]
    assert not any(row["event_type"] == "STOPPED_SAFE" for row in rows)
    assert json.loads(rows[-1]["detail"])["stop_cause"] == "RUNTIME_FATAL"


@pytest.mark.asyncio
async def test_scheduled_scan_task_exception_is_supervised_as_runtime_fatal(tmp_path):
    clock = FakeClock()
    with PaperRepository(tmp_path / "scheduled-scan-exception.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters=adapters(clock, settlement_at=NOW), clock=clock,
        )

        async def startup_scan():
            runtime.last_scan = SimpleNamespace(logical_at=clock.now())

        async def no_restore(_at):
            return None

        async def no_streams():
            return None

        async def scheduled_scan_failure():
            raise RuntimeError("synthetic scheduled scan defect")

        runtime.scan = startup_scan
        runtime._restore = no_restore
        runtime.start_streams = no_streams
        runtime._start_public_refresh = lambda: None
        runtime._start_background_catalog_refresh = lambda **_kwargs: None
        runtime._try_mark_startup_ready = lambda: True
        runtime.tick = scheduled_scan_failure

        with pytest.raises(RuntimeError, match="synthetic scheduled scan defect"):
            await runtime.run(stop_event=asyncio.Event())
        rows = repository.connection.execute(
            "SELECT event_type,detail FROM runtime_evidence ORDER BY evidence_id"
        ).fetchall()

    assert [row["event_type"] for row in rows][-2:] == [
        "RUNTIME_FATAL", "RUNTIME_STOPPED_FATAL",
    ]
    assert json.loads(rows[-2]["detail"]) == {
        "exception_class": "RuntimeError",
        "task": "scheduled_scan",
    }
    assert json.loads(rows[-1]["detail"])["stop_cause"] == "RUNTIME_FATAL"


@pytest.mark.asyncio
async def test_unexpected_scheduled_scan_task_cancellation_is_runtime_fatal(tmp_path):
    clock = FakeClock()
    with PaperRepository(tmp_path / "scheduled-scan-cancellation.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters=adapters(clock, settlement_at=NOW), clock=clock,
        )

        async def startup_scan():
            runtime.last_scan = SimpleNamespace(logical_at=clock.now())

        async def no_restore(_at):
            return None

        async def no_streams():
            return None

        async def scheduled_scan_cancellation():
            raise asyncio.CancelledError()

        runtime.scan = startup_scan
        runtime._restore = no_restore
        runtime.start_streams = no_streams
        runtime._start_public_refresh = lambda: None
        runtime._start_background_catalog_refresh = lambda **_kwargs: None
        runtime._try_mark_startup_ready = lambda: True
        runtime.tick = scheduled_scan_cancellation

        with pytest.raises(asyncio.CancelledError):
            await runtime.run(stop_event=asyncio.Event())
        rows = repository.connection.execute(
            "SELECT event_type,detail FROM runtime_evidence ORDER BY evidence_id"
        ).fetchall()

    assert [row["event_type"] for row in rows][-2:] == [
        "RUNTIME_FATAL", "RUNTIME_STOPPED_FATAL",
    ]
    assert json.loads(rows[-2]["detail"]) == {
        "exception_class": "CancelledError",
        "task": "scheduled_scan",
    }
    assert json.loads(rows[-1]["detail"])["stop_cause"] == "RUNTIME_FATAL"


@pytest.mark.asyncio
async def test_internal_background_failure_requests_fatal_stop(tmp_path):
    clock = FakeClock()
    with PaperRepository(tmp_path / "background-fatal.db") as repository:
        runtime = PublicPaperRuntime(repository, adapters={}, clock=clock)
        runtime._stop_event = asyncio.Event()

        async def fail_reconcile():
            raise RuntimeError("synthetic background defect")

        runtime._reconcile_streams = fail_reconcile
        runtime._start_public_refresh()
        assert runtime._refresh_task is not None
        await asyncio.gather(runtime._refresh_task, return_exceptions=True)
        await asyncio.sleep(0)
        fatal = repository.connection.execute(
            "SELECT detail FROM runtime_evidence WHERE event_type='RUNTIME_FATAL'"
        ).fetchall()
    assert runtime._stop_event.is_set()
    assert runtime._stop_cause == "RUNTIME_FATAL"
    assert [json.loads(row["detail"])["exception_class"] for row in fatal] == [
        "RuntimeError"
    ]


@pytest.mark.parametrize("cause", ("SIGINT", "SIGTERM", "STOP_EVENT", None))
@pytest.mark.asyncio
async def test_shutdown_persists_bounded_signal_or_unknown_cause(tmp_path, cause):
    clock = FakeClock()
    with PaperRepository(tmp_path / f"stop-{cause}.db") as repository:
        runtime = PublicPaperRuntime(repository, adapters={}, clock=clock)
        runtime._stop_event = asyncio.Event()
        if cause is not None:
            runtime._request_stop(cause)
        await runtime.shutdown()
        detail = json.loads(repository.connection.execute(
            "SELECT detail FROM runtime_evidence WHERE event_type='STOPPED_SAFE'"
        ).fetchone()[0])
    assert detail["stop_cause"] == (cause or "UNKNOWN_EXTERNAL_STOP")
    assert detail["requested_at"] == NOW.isoformat()
    if cause not in {"SIGINT", "SIGTERM"}:
        assert "signal" not in detail
    else:
        assert detail["signal"] == cause


@pytest.mark.asyncio
async def test_paper_run_notifications_start_ready_and_safe_stop_after_evidence(tmp_path):
    clock = FakeClock()
    stop = asyncio.Event()
    delivery = CaptureNotifications()
    outbox = NotificationOutbox(delivery)

    async def stop_after_first_wait(_seconds: float) -> None:
        stop.set()
        await asyncio.sleep(0)

    with PaperRepository(tmp_path / "run-notifications.db") as repository:
        result = await public_paper_run(
            repository,
            adapters=adapters(
                clock, settlement_at=NOW + timedelta(minutes=5), funding="0"
            ),
            clock=clock,
            stop_event=stop,
            sleep=stop_after_first_wait,
            notifications=outbox,
        )
        persisted = {
            row[0] for row in repository.connection.execute(
                "SELECT event_type FROM runtime_evidence"
            )
        }
    assert result == {"status": "STOPPED_SAFE", "forced_close": False}
    assert delivery.started and delivery.closed
    assert [row.kind for row in delivery.rows if row.kind.startswith("RUNTIME_")] == [
        "RUNTIME_STARTED", "RUNTIME_READY",
    ]
    assert [row.kind for row in delivery.rows][-1] == "SAFE_STOP"
    assert {"PAPER_RUN_STARTED", "PAPER_RUN_READY", "STOPPED_SAFE"} <= persisted


@pytest.mark.asyncio
async def test_safe_stop_cancels_hung_telegram_delivery_without_waiting_for_timeout(tmp_path):
    clock = FakeClock()
    stop = asyncio.Event()

    class HangingResponse:
        async def __aenter__(self):
            await asyncio.Event().wait()
        async def __aexit__(self, *_args):
            return False

    class HangingSession:
        closed = False
        def post(self, *_args, **_kwargs):
            return HangingResponse()
        async def close(self):
            self.closed = True

    session = HangingSession()
    delivery = TelegramDelivery(
        "synthetic-token", "synthetic-chat", timeout_seconds=60,
        session_factory=lambda: session,
    )

    async def stop_after_first_wait(_seconds: float) -> None:
        stop.set()
        await asyncio.sleep(0)

    with PaperRepository(tmp_path / "safe-stop-notifications.db") as repository:
        result = await asyncio.wait_for(public_paper_run(
            repository,
            adapters=adapters(
                clock, settlement_at=NOW + timedelta(minutes=5), funding="0"
            ),
            clock=clock,
            stop_event=stop,
            sleep=stop_after_first_wait,
            notifications=NotificationOutbox(delivery),
        ), timeout=0.5)
        evidence = "".join(
            row[0] for row in repository.connection.execute(
                "SELECT COALESCE(detail, '') FROM runtime_evidence"
            )
        )
    assert result["status"] == "STOPPED_SAFE"
    assert delivery._worker is None and session.closed
    assert "synthetic-token" not in evidence and "synthetic-chat" not in evidence


@pytest.mark.asyncio
async def test_opportunity_notifications_copy_authoritative_plan_and_dedupe(tmp_path):
    clock = FakeClock()
    fakes = adapters(clock, settlement_at=NOW + timedelta(minutes=5))
    delivery = CaptureNotifications()
    with PaperRepository(tmp_path / "opportunity-notifications.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock,
            notifications=NotificationOutbox(delivery),
        ) as runtime:
            await runtime.scan()
            winner = runtime.last_scan.winner
            clock.advance(1)
            await runtime.scan()
            fakes[Venue.RISEX].funding_cash += D("0.000001")
            clock.advance(1)
            await runtime.scan()
            subcent = runtime.last_scan.winner
            assert subcent.planned_maker_net_pnl_usd != winner.planned_maker_net_pnl_usd
            assert subcent.planned_maker_net_pnl_usd.quantize(D("0.01")) == (
                winner.planned_maker_net_pnl_usd.quantize(D("0.01"))
            )
            fakes[winner.hedge_venue].funding_cash = D("0")
            clock.advance(1)
            await runtime.scan()
            fakes[Venue.RISEX].funding_cash += D("0.02")
            clock.advance(1)
            await runtime.scan()
            for adapter in fakes.values():
                adapter.funding_cash = D("0")
            clock.advance(1)
            await runtime.scan()
            fakes[Venue.RISEX].funding_cash = D("5")
            fakes[winner.hedge_venue].funding_cash = D("5")
            clock.advance(1)
            await runtime.scan()

    rows = [row for row in delivery.rows if row.kind.startswith("OPPORTUNITY") or row.kind == "ELIGIBLE_OPPORTUNITY"]
    assert [row.kind for row in rows] == [
        "ELIGIBLE_OPPORTUNITY", "ELIGIBLE_OPPORTUNITY",
        "ELIGIBLE_OPPORTUNITY", "OPPORTUNITY_DISAPPEARED",
        "ELIGIBLE_OPPORTUNITY",
    ]
    assert rows[0].ticker == winner.canonical_asset
    assert "RISEx" in rows[0].route and winner.hedge_venue.value in rows[0].route
    assert rows[0].planned_maker_net_pnl_usd == winner.planned_maker_net_pnl_usd
    assert rows[0].text == (
        f"{winner.canonical_asset} | {rows[0].route} | Expected PnL: "
        f"${format_telegram_money(winner.planned_maker_net_pnl_usd)} | "
        f"Scan UTC: {NOW.isoformat()}"
    )
    assert "RISEx" in rows[0].text
    assert winner.hedge_venue.value in rows[0].text
    assert "LONG" in rows[0].text and "SHORT" in rows[0].text
    assert rows[1].route != rows[0].route
    assert rows[-1].route == rows[0].route


@pytest.mark.asyncio
async def test_full_scan_digest_uses_persisted_authoritative_route_rows_in_order(tmp_path):
    clock = FakeClock()
    fakes = adapters(clock, settlement_at=NOW + timedelta(minutes=5))
    delivery = CaptureNotifications()
    with PaperRepository(tmp_path / "full-scan-digest.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock,
            notifications=NotificationOutbox(delivery),
        ) as runtime:
            result = await runtime.scan(scan_kind="FULL")
            digests = [row for row in delivery.rows if row.kind == "FULL_SCAN_DIGEST"]
            persisted = repository.connection.execute(
                "SELECT COUNT(*) FROM scanner_snapshots WHERE logical_at=?",
                (NOW.isoformat(),),
            ).fetchone()[0]
            persisted_rows = repository.report(as_of=NOW)["latest_routes"]

    assert persisted == 1
    assert result["routes"] == persisted_rows
    assert len(digests) == 1
    digest = digests[0]
    assert digest.occurred_at == NOW
    assert digest.parse_mode == TELEGRAM_HTML_PARSE_MODE
    assert digest.text.split("\n\n", 1)[0] == (
        "<b>Full Scan 1/1 | Status: OPPORTUNITY</b>\n"
        f"Scan UTC: <code>{NOW.isoformat()}</code>"
    )
    route_cards = digest_cards((digest,))
    assert len(route_cards) == min(10, len(result["routes"]))
    assert all(card.endswith("Funding in: <code>5 min</code>") for card in route_cards)
    for rank, (card, route_row) in enumerate(
        zip(route_cards, persisted_rows[:10]), 1
    ):
        risex_side = (
            "LONG"
            if route_row["direction"] == "LONG_RISEX_SHORT_HEDGE"
            else "SHORT"
        )
        hedge_side = "SHORT" if risex_side == "LONG" else "LONG"
        expected = (
            f"<b>{rank}. {route_row['canonical_asset']}</b> — "
            f"RISEx {risex_side} / {route_row['hedge_venue']} {hedge_side}\n"
            f"PnL: <code>${format_telegram_money(route_row['planned_maker_net_pnl_usd'])}"
            "</code> | Funding in: <code>5 min</code>"
        )
        assert card == expected


@pytest.mark.asyncio
async def test_full_scan_digest_keeps_negative_blocked_and_unknown_routes_visible(tmp_path):
    clock = FakeClock()
    fakes = adapters(
        clock, settlement_at=NOW + timedelta(minutes=5), funding="0"
    )
    delivery = CaptureNotifications()
    with PaperRepository(tmp_path / "full-scan-digest-blocked.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock,
            notifications=NotificationOutbox(delivery),
        ) as runtime:
            negative = await runtime.scan(scan_kind="FULL")
            clock.advance(1)
            for adapter in fakes.values():
                adapter.funding_unknown = True
            unknown = await runtime.scan(scan_kind="FULL")

    digests = [row for row in delivery.rows if row.kind == "FULL_SCAN_DIGEST"]
    assert len(digests) == 2
    assert "<b>Full Scan 1/1 | Status: NO TRADE</b>" in digests[0].text
    assert any(row["blockers"] for row in negative["routes"])
    assert any(
        D(row["planned_maker_net_pnl_usd"]) < 0
        for row in negative["routes"]
        if row["planned_maker_net_pnl_usd"] is not None
    )
    negative_cards = digest_cards((digests[0],))
    assert len(negative_cards) == min(10, len(negative["routes"]))
    for rank, (card, row) in enumerate(
        zip(negative_cards, negative["routes"]), 1
    ):
        assert card.startswith(f"<b>{rank}. {row['canonical_asset']}</b> — RISEx ")
        if row["planned_maker_net_pnl_usd"] is None:
            assert "PnL: <code>UNKNOWN — " in card
        else:
            assert (
                f"PnL: <code>${format_telegram_money(row['planned_maker_net_pnl_usd'])}"
                in card
            )
    assert any(row["planned_maker_net_pnl_usd"] is None for row in unknown["routes"])
    assert len(digest_cards((digests[1],))) == min(10, len(unknown["routes"]))
    assert "PnL: <code>UNKNOWN" in digests[1].text


@pytest.mark.asyncio
async def test_full_scan_digest_never_emits_for_other_scan_kinds(tmp_path):
    clock = FakeClock()
    delivery = CaptureNotifications()
    with PaperRepository(tmp_path / "non-full-scan-digest.db") as repository:
        async with PublicPaperRuntime(
            repository,
            adapters=adapters(clock, settlement_at=NOW + timedelta(minutes=5)),
            clock=clock, notifications=NotificationOutbox(delivery),
        ) as runtime:
            for kind in ("INITIAL", "FOCUSED", "RECOVERY"):
                await runtime.scan(scan_kind=kind)
                clock.advance(1)
    assert not [row for row in delivery.rows if row.kind == "FULL_SCAN_DIGEST"]


@pytest.mark.asyncio
async def test_synthetic_test_persists_separate_route_evidence_and_labels_digest(tmp_path):
    clock = FakeClock()
    config = replace(
        PAPER_CONFIG,
        synthetic_test_pnl_overlay_usd=SYNTHETIC_TEST_OVERLAY_USD,
    )
    delivery = CaptureNotifications()
    with PaperRepository(tmp_path / "synthetic-route-evidence.db") as repository:
        async with PublicPaperRuntime(
            repository,
            adapters=adapters(clock, settlement_at=NOW + timedelta(minutes=5)),
            clock=clock,
            config=config,
            notifications=NotificationOutbox(delivery),
        ) as runtime:
            result = await runtime.scan(scan_kind="FULL")
            winner = runtime.last_scan.winner
            assert winner is not None
            winner_row = next(
                row for row in result["routes"]
                if row["route_key"] == (
                    f"{winner.canonical_asset}|{winner.hedge_venue.value}|"
                    f"{winner.direction.value}"
                )
            )
        report = repository.report(as_of=NOW)

    assert result["synthetic_test"] == {
        "label": "SYNTHETIC TEST",
        "enabled": True,
        "overlay_usd": "0.50",
        "raw_expected_pnl_field": "raw_expected_pnl_usd",
        "adjusted_expected_pnl_field": "test_adjusted_expected_pnl_usd",
        "realized_accounting_includes_overlay": False,
    }
    assert winner_row["raw_expected_pnl_usd"] == winner_row["planned_maker_net_pnl_usd"]
    assert winner_row["synthetic_test_pnl_overlay_usd"] == "0.50"
    assert D(winner_row["test_adjusted_expected_pnl_usd"]) == (
        D(winner_row["raw_expected_pnl_usd"]) + D("0.50")
    )
    assert winner_row["realized_pnl_includes_synthetic_test_overlay"] is False
    assert report["synthetic_test"]["label"] == "SYNTHETIC TEST"
    assert report["synthetic_test"]["realized_accounting_includes_overlay"] is False
    assert report["latest_routes"] == result["routes"]
    assert any(
        "SYNTHETIC TEST (not realized)" in payload.text
        and "Test economics:" in payload.text
        and "Raw expected:" in payload.text
        and "Overlay: +$0.50" in payload.text
        and "Adjusted test expected:" in payload.text
        for payload in delivery.rows
        if payload.kind == "FULL_SCAN_DIGEST"
    )


@pytest.mark.asyncio
async def test_synthetic_test_config_identity_and_actual_accounting_remain_separate(tmp_path):
    config = replace(
        PAPER_CONFIG,
        synthetic_test_pnl_overlay_usd=SYNTHETIC_TEST_OVERLAY_USD,
    )
    with PaperRepository(tmp_path / "synthetic-config.db") as repository:
        result = await run_fixture(
            {"scenario": "negative_closed", "attempt_id": "synthetic-negative"},
            repository,
            config=config,
        )
        assert result["status"] == "CLOSED"
        with pytest.raises(ValueError, match="configuration identity conflict"):
            repository.ensure_synthetic_test_configuration(Decimal("0"))
        report = repository.report(as_of=NOW + timedelta(minutes=10))

    assert report["actual_pair_pnl_usd"] == "-10"
    assert report["actual_fees_usd"] == "0.21000"
    assert report["simulated_closed_net_pnl_usd"] == "-10.21000"
    assert report["synthetic_test"]["overlay_usd"] == "0.50"


@pytest.mark.asyncio
async def test_full_scan_digest_retains_all_58_authoritative_rows(tmp_path):
    clock = FakeClock()
    fakes = {
        Venue.RISEX: ManyFakeAdapter(
            Venue.RISEX, clock, settlement_at=NOW + timedelta(minutes=5),
            asset_count=15,
        ),
        Venue.EXTENDED: ManyFakeAdapter(
            Venue.EXTENDED, clock, settlement_at=NOW + timedelta(minutes=5),
            asset_count=15,
        ),
        Venue.NADO: ManyFakeAdapter(
            Venue.NADO, clock, settlement_at=NOW + timedelta(minutes=5),
            asset_count=14,
        ),
    }
    delivery = CaptureNotifications()
    with PaperRepository(tmp_path / "full-scan-digest-all-routes.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock,
            notifications=NotificationOutbox(delivery),
        ) as runtime:
            result = await runtime.scan(scan_kind="FULL")
        report = repository.report(as_of=NOW)
    digests = [row for row in delivery.rows if row.kind == "FULL_SCAN_DIGEST"]
    assert len(result["routes"]) == 58
    assert len(report["latest_routes"]) == 58
    assert len(digests) == 1
    delivered_cards = digest_cards(digests)
    assert len(delivered_cards) == 10
    assert len(set(delivered_cards)) == 10
    expected_cards = [
        f"<b>{rank}. {row['canonical_asset']}</b> — RISEx "
        f"{'LONG' if row['direction'] == 'LONG_RISEX_SHORT_HEDGE' else 'SHORT'} / "
        f"{row['hedge_venue']} "
        f"{'SHORT' if row['direction'] == 'LONG_RISEX_SHORT_HEDGE' else 'LONG'}\n"
        f"PnL: <code>${format_telegram_money(row['planned_maker_net_pnl_usd'])}"
        "</code> | Funding in: <code>5 min</code>"
        for rank, row in enumerate(result["routes"][:10], 1)
    ]
    assert delivered_cards == expected_cards
    assert result["routes"][10]["canonical_asset"] not in "\n".join(delivered_cards)
    assert all(len(row.text) <= 4096 for row in digests)


@pytest.mark.asyncio
async def test_paper_run_startup_catalog_gate_evaluates_all_58_routes_before_ready(
    tmp_path,
):
    clock = FakeClock()
    stop = asyncio.Event()
    fakes = {
        Venue.RISEX: ManyFakeAdapter(
            Venue.RISEX, clock, settlement_at=NOW + timedelta(minutes=5),
            asset_count=15,
        ),
        Venue.EXTENDED: ManyFakeAdapter(
            Venue.EXTENDED, clock, settlement_at=NOW + timedelta(minutes=5),
            asset_count=15,
        ),
        Venue.NADO: ManyFakeAdapter(
            Venue.NADO, clock, settlement_at=NOW + timedelta(minutes=5),
            asset_count=14,
        ),
    }

    async def stop_after_ready(_seconds: float) -> None:
        assert repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence "
            "WHERE event_type='PAPER_RUN_READY'"
        ).fetchone()[0] == 1
        stop.set()
        await asyncio.sleep(0)

    with PaperRepository(tmp_path / "startup-all-routes.db") as repository:
        result = await public_paper_run(
            repository, adapters=fakes, clock=clock,
            sleep=stop_after_ready, stop_event=stop,
        )
        scans = repository.connection.execute(
            "SELECT detail FROM runtime_evidence WHERE event_type='PUBLIC_SCAN' "
            "ORDER BY evidence_id"
        ).fetchall()
        lifecycle = repository.connection.execute(
            "SELECT event_type FROM runtime_evidence "
            "WHERE event_type IN ('PUBLIC_SCAN','PAPER_RUN_READY') "
            "ORDER BY evidence_id"
        ).fetchall()

    assert result == {"status": "STOPPED_SAFE", "forced_close": False}
    assert json.loads(scans[0][0])["evaluation_count"] == 58
    assert [row[0] for row in lifecycle[:2]] == [
        "PUBLIC_SCAN", "PAPER_RUN_READY"
    ]


@pytest.mark.asyncio
async def test_paper_run_startup_waits_for_delayed_extended_universe_before_ready(
    tmp_path,
):
    clock = FakeClock()
    stop = asyncio.Event()
    target = NOW + timedelta(minutes=5)
    extended = GatedExtendedAdapter(clock, settlement_at=target)

    async def delayed_catalog():
        extended.catalog_calls += 1
        extended.calls.append("catalog")
        extended.request_started.set()
        try:
            await extended.gate.wait()
        except asyncio.CancelledError:
            extended.cancelled = True
            raise
        volume = MarketVolume(
            Venue.EXTENDED, extended.market.venue_symbol, D("1000000"),
            clock.now(), "official-shaped",
        )
        return (extended.market,), (volume,)

    extended.fetch_catalog = delayed_catalog
    fakes = adapters(clock, settlement_at=target)
    fakes[Venue.EXTENDED] = extended

    async def stop_after_ready(_seconds: float) -> None:
        stop.set()
        await asyncio.sleep(0)

    with PaperRepository(tmp_path / "startup-delayed-extended.db") as repository:
        task = asyncio.create_task(public_paper_run(
            repository, adapters=fakes, clock=clock,
            sleep=stop_after_ready, stop_event=stop,
        ))
        await extended.request_started.wait()
        await asyncio.sleep(0)
        assert not task.done()
        assert repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence "
            "WHERE event_type='PAPER_RUN_READY'"
        ).fetchone()[0] == 0
        assert repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence "
            "WHERE event_type='PUBLIC_SCAN'"
        ).fetchone()[0] == 0
        extended.gate.set()
        result = await asyncio.wait_for(task, timeout=1)
        events = repository.connection.execute(
            "SELECT event_type, detail FROM runtime_evidence "
            "WHERE event_type IN ('PUBLIC_REQUEST_COMPLETED','PUBLIC_SCAN',"
            "'PAPER_RUN_READY') ORDER BY evidence_id"
        ).fetchall()

    assert result == {"status": "STOPPED_SAFE", "forced_close": False}
    event_types = [row[0] for row in events]
    initial_scan_index = next(
        index for index, row in enumerate(events)
        if row[0] == "PUBLIC_SCAN"
        and json.loads(row[1])["scan_kind"] == "INITIAL"
    )
    ready_index = event_types.index("PAPER_RUN_READY")
    assert initial_scan_index < ready_index
    assert "PUBLIC_REQUEST_COMPLETED" in event_types[:initial_scan_index]
    assert extended.catalog_calls == 1


@pytest.mark.asyncio
async def test_sigterm_cancels_inflight_startup_catalog_without_ready(tmp_path):
    clock = FakeClock()
    stop = asyncio.Event()
    target = NOW + timedelta(minutes=5)
    extended = GatedExtendedAdapter(clock, settlement_at=target)
    extended.catalog_calls = 1
    fakes = adapters(clock, settlement_at=target)
    fakes[Venue.EXTENDED] = extended

    with PaperRepository(tmp_path / "sigterm-startup-cancel.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            task = asyncio.create_task(runtime.run(stop_event=stop))
            await extended.request_started.wait()
            started = time.monotonic()
            runtime._request_stop("SIGTERM")
            result = await asyncio.wait_for(task, timeout=1)
            elapsed = time.monotonic() - started
            events = repository.connection.execute(
                "SELECT event_type FROM runtime_evidence ORDER BY evidence_id"
            ).fetchall()

    assert result == {"status": "STOPPED_SAFE", "forced_close": False}
    assert elapsed < 1
    assert extended.cancelled
    assert "PUBLIC_SCAN" not in {row[0] for row in events}
    assert "PAPER_RUN_READY" not in {row[0] for row in events}
    assert events[-1][0] == "STOPPED_SAFE"


@pytest.mark.asyncio
async def test_public_observation_rest_fallback_is_bounded_per_venue(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(minutes=5)

    class TrackedManyAdapter(ManyFakeAdapter):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.active = 0
            self.max_active = 0

        async def _tracked(self, operation):
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            try:
                await asyncio.sleep(0)
                return await operation()
            finally:
                self.active -= 1

        async def fetch_book(self, venue_symbol):
            return await self._tracked(
                lambda: FakeAdapter.fetch_book(self, venue_symbol)
            )

        async def fetch_funding_quote(self, market, *, assumed_open_at):
            return await self._tracked(
                lambda: FakeAdapter.fetch_funding_quote(
                    self, market, assumed_open_at=assumed_open_at
                )
            )

    fakes = {
        venue: TrackedManyAdapter(venue, clock, settlement_at=target)
        for venue in (Venue.RISEX, Venue.EXTENDED, Venue.NADO)
    }
    with PaperRepository(tmp_path / "bounded-rest-concurrency.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            result = await runtime.scan()

    assert len(result["routes"]) == 20
    assert {adapter.max_active for adapter in fakes.values()} == {2}


@pytest.mark.asyncio
async def test_full_refresh_deadline_records_bounded_fail_closed_result(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(minutes=5)
    blocked = GatedAdapter(Venue.RISEX, clock, settlement_at=target)
    fakes = adapters(clock, settlement_at=target)
    fakes[Venue.RISEX] = blocked
    with PaperRepository(tmp_path / "full-refresh-deadline.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            await runtime.tick()
            runtime._startup_gate_satisfied = True
            runtime._stop_event = asyncio.Event()
            blocked.block_funding = True
            clock.value = NOW + timedelta(seconds=120)
            await runtime.tick(clock.now())
            await blocked.request_started.wait()
            owner = runtime._refresh_task
            deadline = runtime._pending_full_deadline_at
            assert owner is not None and not owner.done()
            assert deadline is not None

            clock.value = deadline + timedelta(seconds=1)
            await runtime.tick(clock.now())
            blocked_rows = repository.connection.execute(
                "SELECT detail FROM runtime_evidence "
                "WHERE event_type='PUBLIC_SCAN_BLOCKED'"
            ).fetchall()
            full_count = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence "
                "WHERE event_type='PUBLIC_SCAN' "
                "AND json_extract(detail,'$.scan_kind')='FULL'"
            ).fetchone()[0]
            await runtime.shutdown()

    assert owner.done() and owner.cancelled()
    assert len(blocked_rows) == 1
    blocked_detail = json.loads(blocked_rows[0][0])
    assert blocked_detail["reason"] == "PUBLIC_REFRESH_DEADLINE_EXCEEDED"
    assert blocked_detail["completed"] is False
    assert full_count == 0
    assert blocked.cancelled


@pytest.mark.asyncio
async def test_sigterm_cancels_inflight_public_refresh_without_fabricated_full(
    tmp_path,
):
    clock = FakeClock()
    target = NOW + timedelta(minutes=5)
    blocked = GatedAdapter(Venue.RISEX, clock, settlement_at=target)
    fakes = adapters(clock, settlement_at=target)
    fakes[Venue.RISEX] = blocked
    with PaperRepository(tmp_path / "sigterm-refresh-cancel.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            await runtime.tick()
            runtime._startup_gate_satisfied = True
            runtime._stop_event = asyncio.Event()
            blocked.block_funding = True
            runtime._start_public_refresh()
            await blocked.request_started.wait()
            owner = runtime._refresh_task
            assert owner is not None and not owner.done()
            writes_before_stop = repository.connection.total_changes

            started = time.monotonic()
            runtime.accepting_entries = False
            runtime._request_stop("SIGTERM")
            await asyncio.wait_for(runtime.shutdown(), timeout=1)
            elapsed = time.monotonic() - started
            writes_after_stop = repository.connection.total_changes
            await asyncio.sleep(0)
            writes_quiescent = repository.connection.total_changes
            evidence = repository.connection.execute(
                "SELECT event_type, detail FROM runtime_evidence ORDER BY evidence_id"
            ).fetchall()
            integrity = repository.connection.execute(
                "PRAGMA integrity_check"
            ).fetchone()[0]

    assert elapsed < 1
    assert blocked.cancelled
    assert owner.done() and owner.cancelled()
    assert runtime._pending_full_scan_at is None
    assert not any(
        row[0] == "PUBLIC_SCAN"
        and json.loads(row[1]).get("scan_kind") == "FULL"
        for row in evidence
    )
    assert evidence[-1][0] == "STOPPED_SAFE"
    assert writes_after_stop > writes_before_stop
    assert writes_quiescent == writes_after_stop
    assert integrity == "ok"


@pytest.mark.asyncio
async def test_startup_seed_refresh_keeps_first_full_digest_funding_fresh(
    tmp_path, monkeypatch,
):
    clock = FakeClock()
    stop = asyncio.Event()
    fakes = adapters(clock, settlement_at=NOW + timedelta(minutes=10))
    risex = SlowBootstrapFundingAdapter(
        Venue.RISEX, clock, settlement_at=NOW + timedelta(minutes=10)
    )
    fakes[Venue.RISEX] = risex
    delivery = CaptureNotifications()
    captured: dict[str, PublicPaperRuntime] = {}

    async def start_synthetic_streams(runtime: PublicPaperRuntime) -> None:
        captured["runtime"] = runtime
        confirm_public_streams(runtime, clock.now())

    monkeypatch.setattr(PublicPaperRuntime, "start_streams", start_synthetic_streams)

    with PaperRepository(tmp_path / "startup-seed-freshness.db") as repository:
        async def drive(seconds: float) -> None:
            full_count = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence WHERE event_type='PUBLIC_SCAN' "
                "AND json_extract(detail,'$.scan_kind')='FULL'"
            ).fetchone()[0]
            if full_count:
                stop.set()
                await asyncio.sleep(0)
                return
            runtime = captured["runtime"]
            task = runtime._refresh_task
            if task is not None and not task.done():
                await asyncio.wait_for(asyncio.shield(task), timeout=1)
            clock.value += timedelta(seconds=seconds)
            confirm_public_streams(runtime, clock.now())
            await asyncio.sleep(0)

        result = await public_paper_run(
            repository, adapters=fakes, clock=clock, sleep=drive,
            stop_event=stop, notifications=NotificationOutbox(delivery),
        )
        digest = next(
            row for row in delivery.rows if row.kind == "FULL_SCAN_DIGEST"
        )
        full_quotes = tuple(
            pickle.loads(payload)
            for (payload,) in repository.connection.execute(
                "SELECT payload FROM funding_quotes WHERE opened_at=?",
                (digest.occurred_at.isoformat(),),
            )
        )
        full_routes = repository.report(
            as_of=digest.occurred_at
        )["latest_routes"]

    assert result["status"] == "STOPPED_SAFE"
    assert risex.initial_observed_at is not None
    assert risex.seeded_observed_at is not None
    assert digest.occurred_at - risex.initial_observed_at > timedelta(seconds=120)
    assert len(full_quotes) == 3
    assert all(
        quote.observed_at <= digest.occurred_at
        and digest.occurred_at - quote.observed_at <= timedelta(seconds=120)
        for quote in full_quotes
    )
    quote_sources = {
        (quote.venue.value, quote.canonical_market): quote.source
        for quote in full_quotes
    }
    assert all(
        row["source_quality"]["risex_funding"]["source"]
        == quote_sources["RISEX", f"{row['canonical_asset']}-RISEX"]
        and row["source_quality"]["hedge_funding"]["source"]
        == quote_sources[
            row["hedge_venue"],
            f"{row['canonical_asset']}-{row['hedge_venue']}",
        ]
        for row in full_routes
    )
    assert "PnL: <code>UNKNOWN" not in digest.text
    assert "PnL: <code>$" in digest.text


@pytest.mark.asyncio
async def test_late_extended_universe_schedules_post_seed_refresh(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(minutes=10)
    extended = GatedExtendedAdapter(clock, settlement_at=target)
    fakes = adapters(clock, settlement_at=target)
    fakes[Venue.EXTENDED] = extended
    pending_gate = asyncio.Event()
    started = []

    async def pending_seed_refresh() -> None:
        await pending_gate.wait()

    async def no_combined_streams() -> None:
        return None

    with PaperRepository(tmp_path / "late-universe-refresh.db") as repository:
        runtime = PublicPaperRuntime(repository, adapters=fakes, clock=clock)
        runtime._session = object()
        runtime._stop_event = asyncio.Event()
        runtime._reconcile_combined_streams = no_combined_streams
        runtime._start_extended_stream = lambda kind: started.append(kind)
        runtime._refresh_task = asyncio.create_task(pending_seed_refresh())
        seed_task = runtime._refresh_task
        universe_task = asyncio.create_task(runtime._refresh_extended_universe())
        await asyncio.sleep(0)
        pending_gate.set()
        await universe_task
        assert runtime._refresh_task is seed_task
        await runtime._refresh_task
        assert (
            Venue.EXTENDED, extended.market.venue_symbol
        ) not in runtime.observations
        runtime._start_public_refresh()
        assert runtime._refresh_task is not seed_task
        assert runtime._refresh_task is not None
        await runtime._refresh_task

    key = (Venue.EXTENDED, extended.market.venue_symbol)
    assert key in runtime.observations
    assert set(started) == {"book", "trade", "funding"}


@pytest.mark.asyncio
async def test_startup_seed_refresh_never_blocks_ready_or_safe_stop(
    tmp_path, monkeypatch,
):
    clock = FakeClock()
    stop = asyncio.Event()
    fakes = adapters(clock, settlement_at=NOW + timedelta(minutes=10))
    gated = GatedAdapter(
        Venue.RISEX, clock, settlement_at=NOW + timedelta(minutes=10)
    )
    gated.block_catalog_after_calls = 2
    fakes[Venue.RISEX] = gated

    async def start_synthetic_streams(runtime: PublicPaperRuntime) -> None:
        confirm_public_streams(runtime, clock.now())

    monkeypatch.setattr(PublicPaperRuntime, "start_streams", start_synthetic_streams)

    async def stop_after_seed_starts(_seconds: float) -> None:
        try:
            await asyncio.wait_for(gated.request_started.wait(), timeout=0.2)
        finally:
            stop.set()
        await asyncio.sleep(0)

    with PaperRepository(tmp_path / "startup-seed-nonblocking.db") as repository:
        result = await public_paper_run(
            repository, adapters=fakes, clock=clock,
            sleep=stop_after_seed_starts, stop_event=stop,
        )
        lifecycle = repository.connection.execute(
            "SELECT event_type FROM runtime_evidence WHERE event_type IN "
            "('PAPER_RUN_READY','PUBLIC_REFRESH_STARTED','STOPPED_SAFE') "
            "ORDER BY evidence_id"
        ).fetchall()

    assert result == {"status": "STOPPED_SAFE", "forced_close": False}
    assert gated.request_started.is_set()
    assert gated.catalog_calls == 4
    assert gated.cancelled
    assert [row["event_type"] for row in lifecycle] == [
        "PAPER_RUN_READY", "PUBLIC_REFRESH_STARTED", "STOPPED_SAFE",
    ]


@pytest.mark.asyncio
async def test_scheduling_full_focused_activation_and_strict_cutoff(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(seconds=400)
    fakes = adapters(clock, settlement_at=target)
    with PaperRepository(tmp_path / "schedule.db") as repository:
        async with PublicPaperRuntime(repository, adapters=fakes, clock=clock) as runtime:
            await runtime.tick()
            first_scans = repository.connection.execute("SELECT COUNT(*) FROM scanner_snapshots").fetchone()[0]
            clock.advance(100)  # T-300 focused window.
            await runtime.tick()
            assert runtime._refresh_task is None
            clock.advance(10)
            await runtime.tick()
            assert runtime._refresh_task is None
            clock.advance(10)  # Full scan at +120.
            await runtime.tick()
            await runtime._refresh_task
            for _ in range(16):  # Focused cadence through exact T-120 activation.
                clock.advance(10)
                confirm_public_streams(runtime, clock.now())
                await runtime.tick()
                if runtime._refresh_task is not None:
                    await runtime._refresh_task
            assert runtime.broker is not None
            assert runtime.broker.state.lifecycle_state is LifecycleState.ENTRY_MAKER_OPEN
            clock.advance(115)  # Exact T-5 cutoff.
            confirm_public_streams(runtime, clock.now())
            await runtime.tick()
            assert runtime.broker is None
            assert repository.load_runtime().lifecycle_state is LifecycleState.FLAT
            scans = repository.connection.execute("SELECT COUNT(*) FROM scanner_snapshots").fetchone()[0]
            recorded = {
                datetime.fromisoformat(row[0])
                for row in repository.connection.execute(
                    "SELECT logical_at FROM scanner_snapshots"
                ).fetchall()
            }
            telemetry = [
                json.loads(row[0])
                for row in repository.connection.execute(
                    "SELECT detail FROM runtime_evidence WHERE event_type='PUBLIC_SCAN'"
                )
            ]
            cancellations = paper_entry_cancellations(repository)
    assert scans >= first_scans + 4
    assert {NOW, NOW + timedelta(seconds=100), NOW + timedelta(seconds=110)} <= recorded
    assert NOW + timedelta(seconds=120) in recorded
    assert NOW + timedelta(seconds=280) in recorded
    assert NOW + timedelta(seconds=395) in recorded
    assert {"INITIAL", "FULL", "FOCUSED"} <= {
        row["scan_kind"] for row in telemetry
    }
    assert all({
        "scheduled_at", "started_at", "completed_at", "duration_ms",
        "missed_deadline_ms", "scan_kind", "observations_source",
    } <= row.keys() for row in telemetry)
    assert {row["observations_source"] for row in telemetry} <= {
        "REST_BOOTSTRAP", "LIVE_STREAM", "MIXED",
    }
    assert len(cancellations) == 1
    assert cancellations[0]["cancellation_reason"] == "PAPER_ORDER_CANCELLED_CUTOFF"
    assert cancellations[0]["attempt_id"]
    assert cancellations[0]["route_identity"]
    assert D(cancellations[0]["active_duration_seconds"]) == D("115")
    assert cancellations[0]["cumulative_eligible_maker_quantity"] == "0"
    assert cancellations[0]["full_maker_fill"] is False
    assert cancellations[0]["taker_hedge_taken"] is False
    assert cancellations[0]["opened_position_quantity"] == "0"
    assert cancellations[0]["position_opened"] is False
    assert cancellations[0]["returned_state"] == "FLAT"


@pytest.mark.asyncio
async def test_full_refresh_focus_cancellation_recreation_keeps_activation_timestamp_coherent(
    tmp_path,
):
    clock = FakeClock()
    target = NOW + timedelta(seconds=150)
    blocked = GatedAdapter(Venue.RISEX, clock, settlement_at=target)
    fakes = adapters(clock, settlement_at=target)
    fakes[Venue.RISEX] = blocked

    with PaperRepository(tmp_path / "activation-refresh-race.db") as repository:
        async with PublicPaperRuntime(repository, adapters=fakes, clock=clock) as runtime:
            await runtime.scan()
            confirm_public_streams(runtime, clock.now())
            await runtime.scan(
                refresh=False, scan_kind="FOCUSED", scheduled_at=clock.now()
            )
            assert runtime.last_scan is not None
            assert runtime.last_scan.winner is not None
            runtime.focused_cycle = runtime.last_scan.winner.target_cycle
            activation_at = target - timedelta(seconds=120)
            runtime.next_focused_scan_at = activation_at
            runtime.next_full_scan_at = target + timedelta(hours=1)
            runtime.next_health_check_at = target + timedelta(hours=1)
            runtime._startup_gate_satisfied = True

            clock.value = activation_at
            confirm_public_streams(runtime, clock.now())
            await runtime.tick()
            assert runtime.broker is not None

            full_at = NOW + timedelta(seconds=60)
            runtime.next_full_scan_at = full_at
            runtime.next_focused_scan_at = full_at
            blocked.block_catalog = True
            risex_key = Venue.RISEX, blocked.market.venue_symbol
            runtime.coordinator.stream(*risex_key).disconnected()
            runtime._live_book_ready.discard(risex_key)
            clock.value = full_at
            confirm_public_streams(runtime, clock.now())
            full_tick = asyncio.create_task(runtime.tick())
            await blocked.request_started.wait()
            await full_tick
            assert runtime.broker is None

            confirm_public_streams(runtime, clock.now())
            blocked.block_catalog = False
            blocked.gate.set()
            refresh = runtime._refresh_task
            assert refresh is not None
            await refresh
            runtime._refresh_task = None
            runtime._pending_full_scan_at = None
            runtime._pending_full_deadline_at = None
            runtime.next_full_scan_at = target + timedelta(hours=1)
            await runtime.scan(
                refresh=False, scan_kind="FULL", scheduled_at=full_at
            )
            assert runtime.last_scan is not None
            assert runtime.last_scan.logical_at == full_at
            assert runtime.last_scan.winner is not None
            assert runtime.next_focused_scan_at == full_at + timedelta(seconds=10)

            clock.value = full_at + timedelta(seconds=1)
            await runtime.tick()
            assert runtime.broker is not None
            assert runtime.broker.state.order is not None
            assert runtime.broker.state.order.created_at == clock.now()
            activations = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence "
                "WHERE event_type='PAPER_ENTRY_ACTIVATED'"
            ).fetchone()[0]
            orders = repository.connection.execute(
                "SELECT COUNT(*) FROM orders"
            ).fetchone()[0]
            fills = repository.connection.execute(
                "SELECT COUNT(*) FROM fills"
            ).fetchone()[0]

    assert activations == 2
    assert orders == 2
    assert fills == 0


@pytest.mark.asyncio
async def test_activation_fails_closed_when_current_scan_cannot_be_published(
    tmp_path, monkeypatch,
):
    clock = FakeClock()
    target = NOW + timedelta(seconds=150)
    with PaperRepository(tmp_path / "activation-no-current-scan.db") as repository:
        async with PublicPaperRuntime(
            repository,
            adapters=adapters(clock, settlement_at=target),
            clock=clock,
        ) as runtime:
            await runtime.scan()
            confirm_public_streams(runtime, clock.now())
            await runtime.scan(
                refresh=False, scan_kind="FOCUSED", scheduled_at=clock.now()
            )
            assert runtime.last_scan is not None
            assert runtime.last_scan.winner is not None
            runtime.focused_cycle = runtime.last_scan.winner.target_cycle
            activation_at = target - timedelta(seconds=120)
            runtime.next_focused_scan_at = activation_at + timedelta(seconds=10)
            runtime.next_full_scan_at = target + timedelta(hours=1)
            runtime.next_health_check_at = target + timedelta(hours=1)
            runtime._startup_gate_satisfied = True

            async def missing_scan(**_kwargs):
                return {}

            monkeypatch.setattr(runtime, "scan", missing_scan)
            clock.value = activation_at
            confirm_public_streams(runtime, clock.now())
            await runtime.tick()
            blocked = repository.connection.execute(
                "SELECT detail FROM runtime_evidence "
                "WHERE event_type='PAPER_ENTRY_ACTIVATION_BLOCKED'"
            ).fetchone()
            activated = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence "
                "WHERE event_type='PAPER_ENTRY_ACTIVATED'"
            ).fetchone()[0]

    assert runtime.broker is None
    assert activated == 0
    assert blocked is not None
    assert json.loads(blocked[0])["reason"] == "CURRENT_SHARED_SCAN_NOT_PUBLISHED"


@pytest.mark.asyncio
async def test_disconnect_invalidation_precedes_trade_during_gated_full_refresh(
    tmp_path,
):
    clock = FakeClock()
    target = NOW + timedelta(seconds=120)
    blocked_adapter = GatedAdapter(Venue.RISEX, clock, settlement_at=target)
    fakes = adapters(clock, settlement_at=target)
    fakes[Venue.RISEX] = blocked_adapter

    with PaperRepository(tmp_path / "disconnect-trade-refresh-race.db") as repository:
        async with PublicPaperRuntime(repository, adapters=fakes, clock=clock) as runtime:
            full_scan = None
            delivery = None
            disconnect = None
            release_recompute = asyncio.Event()
            original_recompute = runtime._recompute_funding
            try:
                await activate_with_live_streams(runtime, clock)
                assert runtime.broker is not None
                order = runtime.broker.state.order
                assert order is not None
                runtime.mark_trade_stream_connected(
                    order.venue, order.canonical_market, at=clock.now()
                )

                blocked_adapter.block_catalog = True
                full_scan = asyncio.create_task(
                    runtime.scan(
                        refresh=True, scan_kind="FULL", scheduled_at=clock.now()
                    )
                )
                await blocked_adapter.request_started.wait()
                assert not runtime._scan_coordination_lock.locked()

                recompute_started = asyncio.Event()

                async def gated_recompute(*args, **kwargs):
                    recompute_started.set()
                    await release_recompute.wait()
                    return await original_recompute(*args, **kwargs)

                runtime._recompute_funding = gated_recompute
                delivery = asyncio.create_task(
                    runtime.deliver_trade(
                        maker_trade(runtime, clock.now(), "disconnect-race-trade")
                    )
                )
                await recompute_started.wait()

                session_id = stream_session(
                    runtime, order.venue, order.canonical_market, "public"
                )
                disconnect = asyncio.create_task(
                    runtime.mark_disconnected(
                        order.venue,
                        order.canonical_market,
                        stream_kind="public",
                        stream_session_id=session_id,
                    )
                )
                await asyncio.wait_for(asyncio.shield(disconnect), timeout=1)
                assert runtime.broker is None
                assert (order.venue, order.canonical_market) not in runtime._trade_stream_ready

                release_recompute.set()
                await delivery
                assert runtime.lifecycle is None
                assert repository.connection.execute(
                    "SELECT COUNT(*) FROM fills"
                ).fetchone()[0] == 0
                assert repository.connection.execute(
                    "SELECT COUNT(*) FROM processed_trade_events"
                ).fetchone()[0] == 0

                blocked_adapter.block_catalog = False
                blocked_adapter.gate.set()
                await full_scan
                assert repository.load_runtime().lifecycle_state is LifecycleState.FLAT
                cancellations = paper_entry_cancellations(repository)
                assert len(cancellations) == 1
                assert cancellations[0]["cancellation_reason"] == (
                    "PAPER_ORDER_CANCELLED_DATA_STALE"
                )
                assert cancellations[0]["cumulative_eligible_maker_quantity"] == "0"
            finally:
                release_recompute.set()
                blocked_adapter.block_catalog = False
                blocked_adapter.gate.set()
                for task in (delivery, disconnect, full_scan):
                    if task is not None and not task.done():
                        task.cancel()
                await asyncio.gather(
                    *(task for task in (delivery, disconnect, full_scan) if task is not None),
                    return_exceptions=True,
                )
                runtime._recompute_funding = original_recompute


@pytest.mark.asyncio
async def test_focused_and_active_ticks_make_zero_rest_calls_through_cutoff(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(seconds=300)
    fakes = adapters(clock, settlement_at=target)
    with PaperRepository(tmp_path / "focused-no-rest.db") as repository:
        async with PublicPaperRuntime(repository, adapters=fakes, clock=clock) as runtime:
            await runtime.tick()
            runtime.next_full_scan_at = target + timedelta(hours=1)
            baseline = {venue: tuple(adapter.calls) for venue, adapter in fakes.items()}
            while clock.now() < target - timedelta(seconds=10):
                clock.advance(10)
                confirm_public_streams(runtime, clock.now())
                await runtime.tick()
            clock.advance(5)
            confirm_public_streams(runtime, clock.now())
            await runtime.tick()
            scan_details = [
                json.loads(row[0]) for row in repository.connection.execute(
                    "SELECT detail FROM runtime_evidence WHERE event_type='PUBLIC_SCAN'"
                )
            ]
            nado_disconnects = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence "
                "WHERE event_type='PUBLIC_SOCKET_DISCONNECTED' AND venue='NADO'"
            ).fetchone()[0]
    assert all(tuple(adapter.calls) == baseline[venue] for venue, adapter in fakes.items())
    assert {row["scan_kind"] for row in scan_details} <= {
        "INITIAL", "FOCUSED", "RECOVERY",
    }
    assert scan_details[0]["observations_source"] == "REST_BOOTSTRAP"
    assert {row["observations_source"] for row in scan_details[1:]} <= {
        "MIXED", "LIVE_STREAM",
    }
    assert "LIVE_STREAM" in {row["observations_source"] for row in scan_details}
    assert all({
        "scheduled_at", "started_at", "completed_at", "duration_ms",
        "missed_deadline_ms", "scan_kind", "observations_source",
    } <= row.keys() for row in scan_details)
    assert nado_disconnects == 0


@pytest.mark.asyncio
async def test_run_loop_wakes_on_activation_and_cutoff_deadlines(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(seconds=400)
    stop = asyncio.Event()
    wakeups: list[datetime] = []

    async def deadline_sleep(seconds: float) -> None:
        clock.value += timedelta(seconds=seconds)
        wakeups.append(clock.now())
        if clock.now() >= target:
            stop.set()
        await asyncio.sleep(0)

    with PaperRepository(tmp_path / "deadline-run.db") as repository:
        result = await public_paper_run(
            repository, adapters=adapters(clock, settlement_at=target),
            clock=clock, stop_event=stop, sleep=deadline_sleep,
        )
        recorded = {
            datetime.fromisoformat(row[0]) for row in repository.connection.execute(
                "SELECT logical_at FROM scanner_snapshots"
            )
        }
    assert result["status"] == "STOPPED_SAFE"
    assert target - timedelta(seconds=120) in wakeups
    assert target - timedelta(seconds=5) in wakeups
    assert target - timedelta(seconds=120) in recorded
    assert target - timedelta(seconds=5) in recorded


@pytest.mark.asyncio
async def test_hung_refresh_never_moves_absolute_entry_deadlines(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(seconds=400)
    fakes = adapters(clock, settlement_at=target)
    gated = GatedExtendedAdapter(clock, settlement_at=target)
    fakes[Venue.EXTENDED] = gated
    stop = asyncio.Event()

    async def deadline_sleep(seconds: float) -> None:
        clock.value += timedelta(seconds=seconds)
        if clock.now() >= target:
            stop.set()
        await asyncio.sleep(0)

    with PaperRepository(tmp_path / "hung-refresh-deadline.db") as repository:
        result = await public_paper_run(
            repository, adapters=fakes, clock=clock, sleep=deadline_sleep,
            stop_event=stop,
        )
        recorded = {
            datetime.fromisoformat(row[0]) for row in repository.connection.execute(
                "SELECT logical_at FROM scanner_snapshots"
            )
        }
        deadline_evaluations = [
            repository.connection.execute(
                "SELECT opportunity_count FROM scanner_snapshots WHERE logical_at=?",
                (at.isoformat(),),
            ).fetchone()[0]
            for at in (target - timedelta(seconds=120), target - timedelta(seconds=5))
        ]
    assert result["status"] == "STOPPED_SAFE"
    assert target - timedelta(seconds=120) in recorded
    assert target - timedelta(seconds=5) in recorded
    assert deadline_evaluations == [4, 4]
    assert gated.catalog_calls == 1
    assert not gated.cancelled


@pytest.mark.asyncio
async def test_full_tick_coalesces_background_refresh_and_skips_missed_slots(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(minutes=5)
    fakes = adapters(clock, settlement_at=target)
    gated = GatedAdapter(Venue.RISEX, clock, settlement_at=target)
    fakes[Venue.RISEX] = gated
    with PaperRepository(tmp_path / "single-flight.db") as repository:
        async with PublicPaperRuntime(repository, adapters=fakes, clock=clock) as runtime:
            await runtime.tick()
            gated.block_catalog = True
            runtime.next_full_scan_at = NOW + timedelta(seconds=120)
            clock.value = NOW + timedelta(seconds=360)
            await runtime.tick()
            first = runtime._refresh_task
            await gated.request_started.wait()
            await runtime.tick()
            assert runtime._refresh_task is first
            assert runtime.next_full_scan_at == NOW + timedelta(seconds=480)
            gated.gate.set()
            await first
            coalesced = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence "
                "WHERE event_type='PUBLIC_REFRESH_COALESCED'"
            ).fetchone()[0]
    assert coalesced == 0


@pytest.mark.asyncio
async def test_full_refresh_adopts_concurrent_catalog_volume_snapshot_once(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(minutes=5)
    catalog_started = {
        venue: asyncio.Event() for venue in (Venue.RISEX, Venue.NADO)
    }
    observation_started = {
        venue: asyncio.Event()
        for venue in (Venue.RISEX, Venue.EXTENDED, Venue.NADO)
    }
    release_catalog = asyncio.Event()
    release_observations = asyncio.Event()

    class RotatingCatalogAdapter(FakeAdapter):
        def __init__(self, venue: Venue) -> None:
            super().__init__(venue, clock, settlement_at=target)
            self.catalog_calls = 0
            self.book_calls = 0

        async def fetch_markets(self):
            self.catalog_calls += 1
            self._ready("markets")
            return (self.market,)

        async def fetch_volumes(self):
            self.catalog_calls += 1
            self._ready("volumes")
            if self.venue in catalog_started and self.catalog_calls > 2:
                catalog_started[self.venue].set()
                await release_catalog.wait()
            volume = D("2000000") if self.catalog_calls > 2 else D("1000000")
            return (
                MarketVolume(
                    self.venue, self.market.venue_symbol, volume,
                    self.clock.now(), "rotating-catalog",
                ),
            )

        async def fetch_book(self, venue_symbol: str):
            self.book_calls += 1
            if self.book_calls == 2:
                observation_started[self.venue].set()
                await release_observations.wait()
            return await super().fetch_book(venue_symbol)

    fakes = {
        venue: RotatingCatalogAdapter(venue)
        for venue in (Venue.RISEX, Venue.EXTENDED, Venue.NADO)
    }
    with PaperRepository(tmp_path / "full-refresh-catalog-snapshot.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            await runtime.scan()
            runtime.next_health_check_at = NOW + timedelta(hours=1)
            runtime.next_full_scan_at = NOW

            await runtime.tick(NOW)
            first = runtime._refresh_task
            assert first is not None and not first.done()
            await asyncio.gather(*(
                event.wait() for event in observation_started.values()
            ))
            await asyncio.gather(*(
                event.wait() for event in catalog_started.values()
            ))
            catalog_task = runtime._extended_universe_task
            assert catalog_task is not None
            release_catalog.set()
            await catalog_task
            release_observations.set()
            await first

            for venue in (Venue.RISEX, Venue.NADO):
                observation = runtime.observations[
                    venue, f"ABC-{venue.value}"
                ]
                assert observation.volume is not None
                assert observation.volume.quote_volume_usd == D("2000000")

            superseded = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence "
                "WHERE event_type='PUBLIC_REFRESH_SUPERSEDED'"
            ).fetchone()[0]
            assert superseded == 0

            for _ in range(3):
                task = runtime._refresh_task
                if task is None:
                    break
                await task
                await runtime.tick(NOW)

            refreshes = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence "
                "WHERE event_type='PUBLIC_REFRESH_STARTED'"
            ).fetchone()[0]
            full_scans = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence "
                "WHERE event_type='PUBLIC_SCAN' "
                "AND json_extract(detail,'$.scan_kind')='FULL'"
            ).fetchone()[0]
            assert refreshes == 1
            assert full_scans == 1
            assert all(adapter.book_calls == 2 for adapter in fakes.values())
            nado_liquidity = {
                plan.route.route_liquidity_usd
                for plan in runtime.last_scan.evaluations
                if plan.hedge_venue is Venue.NADO and plan.route is not None
            }
            assert nado_liquidity == {D("2000000")}


@pytest.mark.asyncio
async def test_full_refresh_adopts_permuted_catalog_volume_snapshot_once(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(minutes=5)
    venues = (Venue.RISEX, Venue.NADO, Venue.LIGHTER)
    catalog_started = {venue: asyncio.Event() for venue in venues}
    observation_started = {venue: asyncio.Event() for venue in venues}
    release_catalog = asyncio.Event()
    release_observations = asyncio.Event()
    permutations = {
        Venue.RISEX: (2, 0, 1),
        Venue.NADO: (1, 2, 0),
        Venue.LIGHTER: (0, 2, 1),
    }

    class PermutingCatalogAdapter(ManyFakeAdapter):
        def __init__(self, venue: Venue) -> None:
            super().__init__(venue, clock, settlement_at=target, asset_count=3)
            self.catalog_calls = 0
            self.book_calls = 0
            self.permuted_markets = tuple(
                self.many_markets[index] for index in permutations[venue]
            )

        async def fetch_markets(self):
            self.catalog_calls += 1
            self._ready("markets")
            if self.catalog_calls > 2:
                catalog_started[self.venue].set()
                await release_catalog.wait()
                return self.permuted_markets
            return self.many_markets

        async def fetch_volumes(self):
            self.catalog_calls += 1
            self._ready("volumes")
            if self.catalog_calls > 2:
                catalog_started[self.venue].set()
                await release_catalog.wait()
                volume = D("2000000")
                markets = self.permuted_markets
            else:
                volume = D("1000000")
                markets = self.many_markets
            return tuple(
                MarketVolume(
                    self.venue, market.venue_symbol, volume,
                    self.clock.now(), "permuted-catalog",
                )
                for market in markets
            )

        async def fetch_book(self, venue_symbol: str):
            self.book_calls += 1
            if self.book_calls > len(self.many_markets):
                observation_started[self.venue].set()
                await release_observations.wait()
            return await super().fetch_book(venue_symbol)

    fakes = {venue: PermutingCatalogAdapter(venue) for venue in venues}
    with PaperRepository(tmp_path / "full-refresh-permuted-catalog-snapshot.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            await runtime.scan()
            runtime.next_health_check_at = NOW + timedelta(hours=1)
            runtime.next_full_scan_at = NOW

            await runtime.tick(NOW)
            first = runtime._refresh_task
            assert first is not None and not first.done()
            await asyncio.gather(*(
                event.wait() for event in observation_started.values()
            ))
            await asyncio.gather(*(
                event.wait() for event in catalog_started.values()
            ))
            catalog_task = runtime._extended_universe_task
            assert catalog_task is not None
            release_catalog.set()
            await catalog_task
            release_observations.set()
            await first

            current_markets = {
                (market.venue, market.venue_symbol): market
                for rows in runtime.markets.values() for market in rows
            }
            assert all(
                observation.market == current_markets[key]
                for key, observation in runtime.observations.items()
            )

            superseded = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence "
                "WHERE event_type='PUBLIC_REFRESH_SUPERSEDED'"
            ).fetchone()[0]
            assert superseded == 0
            assert all(
                observation.volume is not None
                and observation.volume.quote_volume_usd == D("2000000")
                for observation in runtime.observations.values()
            )
            assert all(adapter.book_calls == 6 for adapter in fakes.values())

            for _ in range(3):
                task = runtime._refresh_task
                if task is None:
                    break
                await task
                await runtime.tick(NOW)

            refreshes = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence "
                "WHERE event_type='PUBLIC_REFRESH_STARTED'"
            ).fetchone()[0]
            full_scans = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence "
                "WHERE event_type='PUBLIC_SCAN' "
                "AND json_extract(detail,'$.scan_kind')='FULL'"
            ).fetchone()[0]
            assert refreshes == 1
            assert full_scans == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    ("duplicate", "same_key_field", "add", "remove", "candidate", "missing_volume"),
)
async def test_catalog_adoption_rejects_non_compatible_snapshot(
    tmp_path, mutation
):
    clock = FakeClock()
    venues = (Venue.RISEX, Venue.NADO, Venue.LIGHTER)
    fakes = {
        venue: ManyFakeAdapter(
            venue, clock, settlement_at=NOW + timedelta(minutes=5), asset_count=2
        )
        for venue in venues
    }
    with PaperRepository(
        tmp_path / f"catalog-adoption-{mutation}.db"
    ) as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            await runtime.scan()
            initial_markets = runtime._catalog_market_snapshot()
            initial_candidate_keys = {
                (market.venue, market.venue_symbol)
                for market in runtime._candidate_markets()
            }
            original_risex = runtime.markets[Venue.RISEX]
            if mutation == "duplicate":
                runtime.markets[Venue.RISEX] = original_risex + (original_risex[0],)
            elif mutation == "same_key_field":
                runtime.markets[Venue.RISEX] = tuple(
                    replace(market, is_off_hours=True)
                    if market.venue_symbol == "A0-RISEX" else market
                    for market in original_risex
                )
            elif mutation == "add":
                runtime.markets[Venue.RISEX] = original_risex + (
                    replace(
                        original_risex[0],
                        canonical_asset="NEW",
                        venue_symbol="NEW-RISEX",
                    ),
                )
            elif mutation == "remove":
                runtime.markets[Venue.RISEX] = original_risex[:-1]
            elif mutation == "candidate":
                runtime.markets[Venue.RISEX] = tuple(
                    replace(market, is_active=False)
                    if market.venue_symbol == "A0-RISEX" else market
                    for market in original_risex
                )
            else:
                runtime.volumes.pop((Venue.RISEX, "A0-RISEX"))

            assert not runtime._adopt_compatible_catalog_update(
                initial_markets=initial_markets,
                initial_candidate_keys=initial_candidate_keys,
            )


@pytest.mark.asyncio
async def test_full_refresh_supersedes_concurrent_catalog_market_change(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(minutes=5)
    catalog_started = asyncio.Event()
    observation_started = asyncio.Event()
    release_catalog = asyncio.Event()
    release_observation = asyncio.Event()

    class StructuralCatalogAdapter(FakeAdapter):
        def __init__(self) -> None:
            super().__init__(Venue.RISEX, clock, settlement_at=target)
            self.catalog_calls = 0
            self.book_calls = 0

        async def fetch_markets(self):
            self.catalog_calls += 1
            self._ready("markets")
            if self.catalog_calls > 2:
                market = replace(
                    self.market,
                    canonical_asset="DEF",
                    venue_symbol="DEF-RISEX",
                )
                return (market,)
            return (self.market,)

        async def fetch_volumes(self):
            self.catalog_calls += 1
            self._ready("volumes")
            if self.catalog_calls > 2:
                catalog_started.set()
                await release_catalog.wait()
                symbol = "DEF-RISEX"
            else:
                symbol = self.market.venue_symbol
            return (
                MarketVolume(
                    Venue.RISEX, symbol, D("1000000"),
                    self.clock.now(), "structural-catalog",
                ),
            )

        async def fetch_book(self, venue_symbol: str):
            self.book_calls += 1
            if self.book_calls == 2:
                observation_started.set()
                await release_observation.wait()
            return await super().fetch_book(venue_symbol)

    structural = StructuralCatalogAdapter()
    fakes = adapters(clock, settlement_at=target)
    fakes[Venue.RISEX] = structural
    with PaperRepository(tmp_path / "full-refresh-structural-change.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            await runtime.scan()
            runtime.next_health_check_at = NOW + timedelta(hours=1)
            runtime.next_full_scan_at = NOW

            await runtime.tick(NOW)
            first = runtime._refresh_task
            assert first is not None and not first.done()
            await observation_started.wait()
            await catalog_started.wait()
            catalog_task = runtime._extended_universe_task
            assert catalog_task is not None
            release_catalog.set()
            await catalog_task
            release_observation.set()
            await first

            superseded = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence "
                "WHERE event_type='PUBLIC_REFRESH_SUPERSEDED'"
            ).fetchone()[0]
            full_scans = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence "
                "WHERE event_type='PUBLIC_SCAN' "
                "AND json_extract(detail,'$.scan_kind')='FULL'"
            ).fetchone()[0]
            assert superseded == 1
            assert full_scans == 0
            assert runtime._catalog_refresh_pending


@pytest.mark.asyncio
async def test_negative_candidate_focus_trace_and_late_winner_activation(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(seconds=400)
    fakes = adapters(clock, settlement_at=target, funding="0")
    stop = asyncio.Event()

    async def trace_sleep(seconds: float) -> None:
        clock.value += timedelta(seconds=seconds)
        confirm_public_streams(runtime, clock.now())
        if clock.now() == target - timedelta(seconds=100):
            for key, observation in tuple(runtime.observations.items()):
                quote = replace(
                    observation.funding,
                    observed_at=clock.now(),
                    long_cash_per_canonical_base_usd=D("5"),
                    short_cash_per_canonical_base_usd=D("5"),
                )
                runtime.observations[key] = replace(observation, funding=quote)
        if clock.now() >= target - timedelta(seconds=90):
            stop.set()
        await asyncio.sleep(0)

    with PaperRepository(tmp_path / "negative-late-winner.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock, sleep=trace_sleep
        ) as runtime:
            await runtime.run(stop_event=stop)
        recorded = {
            datetime.fromisoformat(row[0])
            for row in repository.connection.execute(
                "SELECT logical_at FROM scanner_snapshots"
            )
        }
        activations = [
            datetime.fromisoformat(row[0])
            for row in repository.connection.execute(
                "SELECT recorded_at FROM runtime_evidence "
                "WHERE event_type='PAPER_ENTRY_ACTIVATED'"
            )
        ]
    expected = {
        target - timedelta(seconds=offset)
        for offset in range(300, 99, -10)
    }
    assert expected <= recorded
    assert activations == [target - timedelta(seconds=100)]


@pytest.mark.asyncio
async def test_negative_candidate_expires_at_cutoff_without_post_cutoff_focus(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(seconds=400)
    stop = asyncio.Event()

    async def trace_sleep(seconds: float) -> None:
        clock.value += timedelta(seconds=seconds)
        if clock.now() >= target + timedelta(seconds=20):
            stop.set()
        await asyncio.sleep(0)

    with PaperRepository(tmp_path / "negative-cutoff.db") as repository:
        await public_paper_run(
            repository,
            adapters=adapters(clock, settlement_at=target, funding="0"),
            clock=clock, stop_event=stop, sleep=trace_sleep,
        )
        recorded = [
            datetime.fromisoformat(row[0])
            for row in repository.connection.execute(
                "SELECT logical_at FROM scanner_snapshots"
            )
        ]
        activation_count = repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence "
            "WHERE event_type='PAPER_ENTRY_ACTIVATED'"
        ).fetchone()[0]
    focused_trace = {
        target - timedelta(seconds=offset)
        for offset in range(300, 9, -10)
    }
    assert focused_trace <= set(recorded)
    assert max(recorded) == target - timedelta(seconds=5)
    assert activation_count == 0


@pytest.mark.asyncio
async def test_focus_cycle_survives_empty_scan_then_advances_after_cutoff(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(seconds=400)
    with PaperRepository(tmp_path / "focus-cycle-state.db") as repository:
        async with PublicPaperRuntime(
            repository,
            adapters=adapters(clock, settlement_at=target, funding="0"),
            clock=clock,
        ) as runtime:
            await runtime.tick()
            first = runtime._refresh_focused_cycle(clock.now())
            assert first is not None
            original = runtime.last_scan
            runtime.last_scan = replace(original, evaluations=(), ranked_routes=(), winner=None)
            assert runtime._refresh_focused_cycle(target - timedelta(seconds=6)) == first
            plan = original.evaluations[0]
            cycle = plan.target_cycle
            assert cycle is not None
            shift = timedelta(hours=1)
            next_cycle = replace(
                cycle,
                cycle_id=f"{cycle.cycle_id}-next",
                start_at=cycle.start_at + shift,
                end_at=cycle.end_at + shift,
                risex_event=replace(
                    cycle.risex_event,
                    settlement_at=cycle.risex_event.settlement_at + shift,
                ),
                hedge_event=replace(
                    cycle.hedge_event,
                    settlement_at=cycle.hedge_event.settlement_at + shift,
                ),
            )
            next_plan = replace(plan, target_cycle=next_cycle)
            runtime.last_scan = replace(
                original, evaluations=(next_plan,), ranked_routes=(next_plan,), winner=None
            )
            selected = runtime._refresh_focused_cycle(target - timedelta(seconds=5))
    assert selected == next_cycle
    assert runtime.next_focused_scan_at is None


@pytest.mark.asyncio
async def test_fresh_nearer_cycle_replaces_unexpired_later_focus(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(seconds=400)
    with PaperRepository(tmp_path / "focus-nearer-cycle.db") as repository:
        async with PublicPaperRuntime(
            repository,
            adapters=adapters(clock, settlement_at=target, funding="0"),
            clock=clock,
        ) as runtime:
            await runtime.tick()
            original = runtime.last_scan
            plan = original.evaluations[0]
            cycle = plan.target_cycle
            assert cycle is not None

            def shifted(hours: int):
                delta = timedelta(hours=hours)
                return replace(
                    cycle,
                    cycle_id=f"{cycle.cycle_id}-{hours}",
                    start_at=cycle.start_at + delta,
                    end_at=cycle.end_at + delta,
                    risex_event=replace(
                        cycle.risex_event,
                        settlement_at=cycle.risex_event.settlement_at + delta,
                    ),
                    hedge_event=replace(
                        cycle.hedge_event,
                        settlement_at=cycle.hedge_event.settlement_at + delta,
                    ),
                )

            later = shifted(2)
            earlier = shifted(1)
            later_plan = replace(plan, target_cycle=later)
            runtime.focused_cycle = None
            runtime.last_scan = replace(
                original, evaluations=(later_plan,), ranked_routes=(later_plan,), winner=None
            )
            assert runtime._refresh_focused_cycle(clock.now()) == later
            runtime.next_focused_scan_at = clock.now() + timedelta(seconds=10)
            earlier_plan = replace(plan, target_cycle=earlier)
            runtime.last_scan = replace(
                original,
                evaluations=(later_plan, earlier_plan),
                ranked_routes=(later_plan, earlier_plan),
                winner=None,
            )
            selected = runtime._refresh_focused_cycle(clock.now())
    assert selected == earlier
    assert runtime.next_focused_scan_at is None


def maker_trade(runtime: PublicPaperRuntime, at: datetime, key: str = "public-trade") -> TradeEvidence:
    order = runtime.broker.state.order
    version = order.active_version
    tick = order.route_plan.hedge_market.tick_size_raw
    return TradeEvidence(
        key,
        order.venue,
        order.canonical_market,
        at,
        at,
        int(at.timestamp() * 1_000_000_000),
        order.canonical_quantity,
        version.limit_price - tick if order.side is Side.BUY else version.limit_price + tick,
        Side.SELL if order.side is Side.BUY else Side.BUY,
        True,
    )


@pytest.mark.asyncio
async def test_trade_time_cancellation_releases_broker_for_next_same_cycle_attempt(
    tmp_path,
):
    clock = FakeClock()
    target = NOW + timedelta(seconds=120)
    delivery = CaptureNotifications()
    with PaperRepository(tmp_path / "trade-cancel-outcome.db") as repository:
        async with PublicPaperRuntime(
            repository,
            adapters=adapters(clock, settlement_at=target),
            clock=clock,
            notifications=NotificationOutbox(delivery),
        ) as runtime:
            await activate_with_live_streams(runtime, clock)
            first_order = runtime.broker.state.order
            assert first_order is not None
            first_attempt = first_order.attempt_id
            clock.advance(31)
            runtime.mark_trade_stream_connected(
                first_order.venue, first_order.canonical_market, at=clock.now()
            )
            await runtime.deliver_trade(
                maker_trade(runtime, clock.now(), "trade-time-cancel")
            )
            assert runtime.broker is None
            assert runtime.lifecycle is None
            cancellations = paper_entry_cancellations(repository)
            assert len(cancellations) == 1
            assert cancellations[0]["cancellation_reason"] == (
                "PAPER_ORDER_CANCELLED_DATA_STALE"
            )
            assert cancellations[0]["cumulative_eligible_maker_quantity"] == "0"
            cancellation_rows = [
                row
                for row in delivery.rows
                if row.kind == "ENTRY_CANCELLED_NO_FILL"
            ]
            assert len(cancellation_rows) == 1
            assert "no taker hedge" in cancellation_rows[0].text
            assert "opened position quantity 0" in cancellation_rows[0].text
            assert "returned FLAT" in cancellation_rows[0].text
            runtime._save_entry_decision(
                recorded_at=clock.now(), entry_state=repository.load_runtime()
            )
            assert len(paper_entry_cancellations(repository)) == 1
            assert len([
                row for row in delivery.rows
                if row.kind == "ENTRY_CANCELLED_NO_FILL"
            ]) == 1

            confirm_public_streams(runtime, clock.now())
            runtime.next_focused_scan_at = clock.now()
            await runtime.tick()
            assert runtime.broker is not None
            second_order = runtime.broker.state.order
            assert second_order is not None
            assert second_order.attempt_id != first_attempt
            activation_rows = [
                row for row in delivery.rows if row.kind == "ENTRY_ACTIVATED"
            ]
            assert len(activation_rows) == 2
            assert activation_rows[0].event_id != activation_rows[1].event_id


@pytest.mark.asyncio
async def test_partial_maker_evidence_then_cancellation_is_not_reported_as_full_fill(
    tmp_path,
):
    clock = FakeClock()
    target = NOW + timedelta(seconds=120)
    delivery = CaptureNotifications()
    with PaperRepository(tmp_path / "partial-entry-cancel.db") as repository:
        async with PublicPaperRuntime(
            repository,
            adapters=adapters(clock, settlement_at=target),
            clock=clock,
            notifications=NotificationOutbox(delivery),
        ) as runtime:
            await activate_with_live_streams(runtime, clock)
            order = runtime.broker.state.order
            assert order is not None
            runtime.mark_trade_stream_connected(
                order.venue, order.canonical_market, at=clock.now()
            )
            partial_quantity = order.canonical_quantity / D("2")
            await runtime.deliver_trade(
                replace(
                    maker_trade(runtime, clock.now(), "partial-entry-evidence"),
                    canonical_quantity=partial_quantity,
                )
            )
            assert runtime.broker is not None
            assert runtime.broker.state.order.active_version.cumulative_eligible_quantity == (
                partial_quantity
            )

            clock.value = order.cutoff_at
            confirm_public_streams(runtime, clock.now())
            await runtime.tick()
            assert runtime.broker is None
            cancellations = paper_entry_cancellations(repository)
            assert len(cancellations) == 1
            detail = cancellations[0]
            assert detail["cumulative_eligible_maker_quantity"] == str(
                partial_quantity
            )
            assert detail["full_maker_fill"] is False
            assert detail["opened_position_quantity"] == "0"
            assert detail["taker_hedge_taken"] is False
            row = next(
                row for row in delivery.rows
                if row.kind == "ENTRY_CANCELLED_NO_FILL"
            )
            assert f"cumulative eligible maker quantity {partial_quantity}" in row.text
            assert "full maker fill no" in row.text
            assert "opened position quantity 0" in row.text


@pytest.mark.asyncio
async def test_cutoff_deadline_cancels_while_focused_scan_is_blocked(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(seconds=120)
    delivery = CaptureNotifications()
    scan_started = asyncio.Event()
    release_scan = asyncio.Event()
    with PaperRepository(tmp_path / "independent-cutoff-deadline.db") as repository:
        async with PublicPaperRuntime(
            repository,
            adapters=adapters(clock, settlement_at=target),
            clock=clock,
            notifications=NotificationOutbox(delivery),
        ) as runtime:
            await activate_with_live_streams(runtime, clock)
            broker = runtime.broker
            assert broker is not None
            order = broker.state.order
            assert order is not None
            original_scan = runtime.scan

            async def blocked_scan(*args, **kwargs):
                scan_started.set()
                await release_scan.wait()
                return await original_scan(*args, **kwargs)

            runtime.scan = blocked_scan
            clock.value = order.cutoff_at - timedelta(seconds=1)
            runtime.next_focused_scan_at = clock.now()
            tick_task = asyncio.create_task(runtime.tick())
            await scan_started.wait()

            # Replace the real timer with an already-due owned deadline so the
            # test observes the cutoff while the unrelated scan is still held.
            await runtime._cancel_entry_cutoff_deadline()
            clock.value = order.cutoff_at
            await runtime._start_entry_cutoff_deadline(broker, order.cutoff_at)
            deadline_task = runtime._entry_cutoff_task
            assert deadline_task is not None
            await asyncio.wait_for(asyncio.shield(deadline_task), timeout=1)

            assert not tick_task.done()
            assert runtime.broker is None
            assert repository.load_runtime().order.cancellation_reason.value == (
                "PAPER_ORDER_CANCELLED_CUTOFF"
            )
            assert len(paper_entry_cancellations(repository)) == 1
            assert len([
                row for row in delivery.rows
                if row.kind == "ENTRY_CANCELLED_NO_FILL"
            ]) == 1

            release_scan.set()
            await tick_task
            assert len(paper_entry_cancellations(repository)) == 1
            assert len([
                row for row in delivery.rows
                if row.kind == "ENTRY_CANCELLED_NO_FILL"
            ]) == 1


@pytest.mark.asyncio
async def test_cutoff_deadline_does_not_cancel_atomically_opened_pre_cutoff_position(
    tmp_path,
):
    clock = FakeClock()
    target = NOW + timedelta(seconds=120)
    with PaperRepository(tmp_path / "cutoff-open-race.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=adapters(clock, settlement_at=target), clock=clock,
        ) as runtime:
            await activate_with_live_streams(runtime, clock)
            broker = runtime.broker
            assert broker is not None
            order = broker.state.order
            assert order is not None
            clock.advance(1)
            runtime.mark_trade_stream_connected(
                order.venue, order.canonical_market, at=clock.now()
            )
            await runtime.deliver_trade(
                maker_trade(runtime, clock.now(), "pre-cutoff-atomic-open")
            )
            assert runtime.lifecycle is not None
            assert runtime.lifecycle.snapshot.position is not None

            clock.value = order.cutoff_at
            assert not await runtime._cancel_entry_at_cutoff(
                broker, at=clock.now()
            )
            assert runtime.broker is None
            assert repository.load_runtime().lifecycle_state is not LifecycleState.FLAT
            assert paper_entry_cancellations(repository) == []


@pytest.mark.asyncio
async def test_rest_bootstrap_cannot_activate_until_required_live_streams_are_ready(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(seconds=120)
    with PaperRepository(tmp_path / "live-entry-gate.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=adapters(clock, settlement_at=target), clock=clock,
        ) as runtime:
            await runtime.tick()
            assert runtime.broker is None
            assert runtime.last_scan is not None and runtime.last_scan.winner is None
            confirm_public_streams(runtime, clock.now())
            clock.advance(10)
            await runtime.tick()
            assert runtime.broker is not None
            event_count = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence "
                "WHERE event_type='PAPER_ENTRY_ACTIVATED'"
            ).fetchone()[0]
    assert event_count == 1


@pytest.mark.asyncio
async def test_extended_socket_failures_are_component_aware(tmp_path):
    clock = FakeClock()
    fakes = adapters(clock, settlement_at=NOW + timedelta(minutes=5))
    with PaperRepository(tmp_path / "extended-component-disconnect.db") as repository:
        async with PublicPaperRuntime(repository, adapters=fakes, clock=clock) as runtime:
            await runtime.scan()
            symbol = fakes[Venue.EXTENDED].market.venue_symbol
            runtime.mark_trade_stream_connected(Venue.EXTENDED, symbol)
            confirm_extended_stream(runtime,
                symbol, "book", clock.now(), data_ready=True
            )
            confirm_extended_stream(runtime,
                symbol, "funding", clock.now(), data_ready=False
            )
            runtime._new_stream_session((Venue.EXTENDED, symbol, "trade"))
            stream = runtime.coordinator.stream(Venue.EXTENDED, symbol)
            before = stream.book()
            assert before is not None and before.sequence == 1

            await runtime.mark_disconnected(
                Venue.EXTENDED, symbol, stream_kind="funding",
                exception=TimeoutError("funding socket"),
                stream_session_id=runtime._stream_sessions[
                    (Venue.EXTENDED, symbol, "funding")
                ],
            )
            assert stream.book() == before
            assert (Venue.EXTENDED, symbol) in runtime._trade_stream_ready
            assert runtime.component_readiness[Venue.EXTENDED][f"trade:{symbol}"].available
            assert runtime.component_readiness[Venue.EXTENDED][f"funding:{symbol}"].available
            assert not runtime.component_readiness[Venue.EXTENDED][
                f"applied_funding:{symbol}"
            ].available
            await runtime.scan(refresh=False, scan_kind="FULL")
            funding_plans = [
                plan for plan in runtime.last_scan.evaluations
                if plan.hedge_venue is Venue.EXTENDED
            ]
            assert funding_plans
            assert all(
                "FUNDING_STREAM_UNHEALTHY" in plan.no_trade_reasons
                for plan in funding_plans
            )
            assert all(
                "BOOK_UNHEALTHY" not in plan.no_trade_reasons
                for plan in funding_plans
            )

            await runtime.mark_disconnected(
                Venue.EXTENDED, symbol, stream_kind="trade",
                exception=ConnectionError("trade socket"),
                stream_session_id=runtime._stream_sessions[
                    (Venue.EXTENDED, symbol, "trade")
                ],
            )
            assert stream.book() == before
            assert stream.book().sequence == 1
            assert (Venue.EXTENDED, symbol) not in runtime._trade_stream_ready
            assert not runtime.component_readiness[Venue.EXTENDED][f"trade:{symbol}"].available
            await runtime.scan(refresh=False, scan_kind="FULL")
            plans = [
                plan for plan in runtime.last_scan.evaluations
                if plan.hedge_venue is Venue.EXTENDED
            ]
            assert plans
            assert all(
                "TRADE_STREAM_UNHEALTHY" in plan.no_trade_reasons
                for plan in plans
            )
            assert all(
                "BOOK_UNHEALTHY" not in plan.no_trade_reasons
                for plan in plans
            )


@pytest.mark.asyncio
async def test_healthy_public_trade_reaches_broker_and_deduplicates(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(seconds=120)
    fakes = adapters(clock, settlement_at=target)
    with PaperRepository(tmp_path / "trade.db") as repository:
        async with PublicPaperRuntime(repository, adapters=fakes, clock=clock) as runtime:
            await activate_with_live_streams(runtime, clock)
            assert runtime.broker is not None
            clock.advance(1)
            order = runtime.broker.state.order
            runtime.mark_trade_stream_connected(order.venue, order.canonical_market)
            trade = maker_trade(runtime, clock.now())
            await runtime.deliver_trade(trade)
            assert runtime.lifecycle is not None
            assert runtime.lifecycle.snapshot.position is not None
            await runtime.deliver_trade(trade)
            count = repository.connection.execute("SELECT COUNT(*) FROM processed_trade_events").fetchone()[0]
            evidence = repository.report(as_of=clock.now())["latest_trade_evidence"]
    assert count == 1
    assert evidence["risex_contract_assumption_used"] is True
    assert evidence["risex_funding_eligibility_assumption_used"] is True
    assert evidence["risex_funding_estimate_assumption_used"] is True
    assert evidence["paper_assumption_used"] is True


@pytest.mark.asyncio
async def test_runtime_lifecycle_notifications_follow_persisted_transitions(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(seconds=120)
    fakes = adapters(clock, settlement_at=target)
    delivery = CaptureNotifications()
    with PaperRepository(tmp_path / "lifecycle-notifications.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock,
            notifications=NotificationOutbox(delivery),
        ) as runtime:
            await activate_with_live_streams(runtime, clock)
            assert "ENTRY_ACTIVATED" in [row.kind for row in delivery.rows]
            for adapter in fakes.values():
                adapter.funding_unknown = True
            clock.advance(1)
            entry = runtime.broker.state.order
            runtime.mark_trade_stream_connected(entry.venue, entry.canonical_market)
            await runtime.deliver_trade(
                maker_trade(runtime, clock.now(), "notification-entry")
            )
            assert runtime.lifecycle is not None
            runtime.next_position_monitor_at = clock.now()
            await runtime.tick()
            pending = runtime.lifecycle.snapshot.settlements[0]
            estimated_at = clock.now()
            estimated = replace(
                pending, status=SettlementStatus.ESTIMATED, cash_usd=D("3.125")
            )
            await runtime.deliver_settlement(estimated)
            applied = replace(
                pending, status=SettlementStatus.APPLIED_RATE, cash_usd=D("3.25")
            )
            applied_at = clock.now()
            await runtime.deliver_settlement(applied)
            await runtime.deliver_settlement(applied)
            exit_order = runtime.lifecycle.snapshot.exit_order
            assert exit_order is not None and exit_order.active_version is not None
            runtime.mark_trade_stream_connected(
                exit_order.venue, exit_order.canonical_market
            )
            tick = runtime.lifecycle.snapshot.hedge_market.tick_size_raw
            exit_trade = TradeEvidence(
                "notification-exit", exit_order.venue, exit_order.canonical_market,
                clock.now(), clock.now(), "notification-exit-raw",
                exit_order.canonical_quantity,
                (
                    exit_order.active_version.limit_price - tick
                    if exit_order.side is Side.BUY
                    else exit_order.active_version.limit_price + tick
                ),
                Side.SELL if exit_order.side is Side.BUY else Side.BUY,
                True,
            )
            await runtime.deliver_trade(exit_trade)
            authoritative = repository.load_runtime().closed_trade
            cancellations = paper_entry_cancellations(repository)

    kinds = [row.kind for row in delivery.rows]
    assert kinds.count("ENTRY_CANCELLED_NO_FILL") == 0
    assert cancellations == []
    for required in (
        "ENTRY_ACTIVATED", "POSITION_OPENED", "EXIT_STARTED",
        "POSITION_CLOSED", "FINAL_FLAT",
    ):
        assert kinds.count(required) == 1
    assert kinds.count("FUNDING_STATUS") == 2
    paper_lifecycle_kinds = {
        "ENTRY_ACTIVATED", "POSITION_OPENED", "EXIT_STARTED",
        "FUNDING_STATUS", "POSITION_CLOSED", "FINAL_FLAT",
    }
    assert all(
        row.text.startswith("PAPER |")
        for row in delivery.rows
        if row.kind in paper_lifecycle_kinds
    )
    closed = next(row for row in delivery.rows if row.kind == "POSITION_CLOSED")
    assert closed.final_pnl_usd == authoritative.simulated_closed_net_pnl_usd
    assert closed.text.endswith(
        f"final PnL USD {format_telegram_money(authoritative.simulated_closed_net_pnl_usd)}"
    )
    funding_rows = [row for row in delivery.rows if row.kind == "FUNDING_STATUS"]
    assert funding_rows[0].occurred_at == estimated_at
    assert "status ESTIMATED" in funding_rows[0].text
    assert "cash USD 3.13" in funding_rows[0].text
    assert funding_rows[1].occurred_at == applied_at
    assert "status APPLIED_RATE" in funding_rows[1].text
    assert "cash USD 3.25" in funding_rows[1].text
    assert all("received" not in row.text.lower() for row in funding_rows)


@pytest.mark.asyncio
async def test_disconnect_cancels_entry_and_position_gap_recovers_from_snapshot(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(seconds=120)
    fakes = adapters(clock, settlement_at=target)
    with PaperRepository(tmp_path / "gap.db") as repository:
        async with PublicPaperRuntime(repository, adapters=fakes, clock=clock) as runtime:
            await activate_with_live_streams(runtime, clock)
            order = runtime.broker.state.order
            await runtime.mark_disconnected(
                order.venue, order.canonical_market,
                stream_session_id=stream_session(
                    runtime, order.venue, order.canonical_market, "public"
                ),
            )
            assert runtime.broker is None
            assert repository.load_runtime().lifecycle_state is LifecycleState.FLAT

        clock = FakeClock()
        fakes = adapters(clock, settlement_at=target)
        async with PublicPaperRuntime(repository, adapters=fakes, clock=clock) as runtime:
            await activate_with_live_streams(runtime, clock)
            clock.advance(1)
            order = runtime.broker.state.order
            runtime.mark_trade_stream_connected(order.venue, order.canonical_market)
            await runtime.deliver_trade(maker_trade(runtime, clock.now(), "gap-entry"))
            position = runtime.lifecycle.snapshot.position
            hedge = runtime.lifecycle.snapshot.hedge_market
            await runtime.mark_disconnected(
                hedge.venue, hedge.venue_symbol,
                stream_session_id=stream_session(
                    runtime, hedge.venue, hedge.venue_symbol, "public"
                ),
            )
            assert runtime.lifecycle.snapshot.gap_open
            clock.advance(1)
            snapshot = await fakes[hedge.venue].fetch_book(hedge.venue_symbol)
            runtime.mark_trade_stream_connected(hedge.venue, hedge.venue_symbol)
            for component in ("funding", "connection_combined"):
                runtime._set_component_readiness(
                    hedge.venue, f"{component}:{hedge.venue_symbol}", True,
                    "PUBLIC_STREAM_CONNECTED", clock.now(),
                )
            await runtime.recover_snapshot(snapshot)
            assert not runtime.lifecycle.snapshot.gap_open
            assert runtime.lifecycle.snapshot.data_quality.value == "DEGRADED"


@pytest.mark.asyncio
async def test_concurrent_component_failures_form_one_aggregate_execution_gap(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(seconds=120)
    fakes = adapters(clock, settlement_at=target)
    with PaperRepository(tmp_path / "aggregate-gap.db") as repository:
        async with PublicPaperRuntime(repository, adapters=fakes, clock=clock) as runtime:
            await activate_with_live_streams(runtime, clock)
            clock.advance(1)
            order = runtime.broker.state.order
            runtime.mark_trade_stream_connected(order.venue, order.canonical_market)
            await runtime.deliver_trade(maker_trade(runtime, clock.now(), "aggregate-gap-entry"))
            snapshot = runtime.lifecycle.snapshot
            risex_key = (Venue.RISEX, snapshot.risex_market.venue_symbol)
            hedge_key = (snapshot.hedge_market.venue, snapshot.hedge_market.venue_symbol)
            await runtime.mark_disconnected(
                hedge_key[0], hedge_key[1], stream_kind="funding",
                stream_session_id=stream_session(
                    runtime, *hedge_key, "funding"
                ),
            )
            assert not runtime.lifecycle.snapshot.gap_open
            await asyncio.gather(
                runtime.mark_disconnected(
                    *risex_key, stream_kind="book",
                    stream_session_id=stream_session(
                        runtime, *risex_key, "book"
                    ),
                ),
                runtime.mark_disconnected(
                    *hedge_key, stream_kind="trade",
                    stream_session_id=stream_session(
                        runtime, *hedge_key, "trade"
                    ),
                ),
            )
            assert runtime.lifecycle.snapshot.gap_count == 1
            assert runtime.lifecycle.snapshot.gap_open
            clock.advance(1)
            runtime.mark_trade_stream_connected(*risex_key, at=clock.now())
            await runtime.recover_snapshot(
                await fakes[risex_key[0]].fetch_book(risex_key[1]), at=clock.now()
            )
            assert runtime.lifecycle.snapshot.gap_open
            runtime.mark_trade_stream_connected(*hedge_key, at=clock.now())
            await runtime.recover_snapshot(
                await fakes[hedge_key[0]].fetch_book(hedge_key[1]), at=clock.now()
            )
            final = runtime.lifecycle.snapshot
            assert not final.gap_open
            assert final.gap_count == 1
            stale_key = (hedge_key[0], hedge_key[1], "trade")
            stale_session = stream_session(runtime, *stale_key)
            runtime._new_stream_session(stale_key)
            before_stale = runtime.lifecycle.snapshot
            await runtime.mark_disconnected(
                *hedge_key, at=clock.now() - timedelta(seconds=5),
                stream_kind="trade", stream_session_id=stale_session,
            )
            assert runtime.lifecycle.snapshot == before_stale
            event_times = [event.occurred_at for event in final.events]
            assert event_times == sorted(event_times)
            if final.exit_order is not None:
                for version in final.exit_order.versions:
                    assert version.created_at <= version.last_checked_at
                    if version.closed_at is not None:
                        assert version.last_checked_at <= version.closed_at


@pytest.mark.asyncio
async def test_production_shaped_zec_first_fill_chain_is_exact_and_single(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(seconds=120)
    async def no_delay(_seconds: float) -> None:
        await asyncio.sleep(0)

    fakes = adapters(clock, settlement_at=target)
    for venue, adapter in fakes.items():
        adapter.market = replace(
            adapter.market,
            canonical_asset="ZEC",
            venue_symbol=f"ZEC-{venue.value}",
            tick_size_raw=D("0.001"),
            quantity_step_raw=D("0.1"),
            minimum_quantity_raw=D("0.1"),
        )

        async def funding(market, *, assumed_open_at, _adapter=adapter):
            preferred_long = _adapter.venue is Venue.EXTENDED
            return FundingCashQuote(
                _adapter.venue, market.venue_symbol, clock.now(),
                assumed_open_at, target,
                FundingQuality.ESTIMATED
                if _adapter.venue is Venue.RISEX else FundingQuality.PREDICTED,
                FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT, True,
                D("10") if preferred_long else D("-10"),
                D("-10") if preferred_long else D("10"),
                "official-public-synthetic-shape",
            )

        adapter.fetch_funding_quote = funding

    async def risex_book(symbol):
        return OrderBook(
            Venue.RISEX, symbol, (BookLevel(D("800.53"), D("5")),),
            (BookLevel(D("800.531"), D("5")),), clock.now(), 1,
        )

    async def extended_book(symbol):
        return OrderBook(
            Venue.EXTENDED, symbol, (BookLevel(D("801.056"), D("5")),),
            (BookLevel(D("801.057"), D("5")),), clock.now(), 1,
        )

    fakes[Venue.RISEX].fetch_book = risex_book
    fakes[Venue.EXTENDED].fetch_book = extended_book
    config = replace(PAPER_CONFIG, target_notional_per_leg_usd=D("480.6336"))
    with PaperRepository(tmp_path / "zec-production-shape.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock, config=config, sleep=no_delay,
        ) as runtime:
            await activate_with_live_streams(runtime, clock)
            order = runtime.broker.state.order
            assert order.side is Side.BUY
            assert order.canonical_quantity == D("0.6")
            assert order.active_version.limit_price == D("801.056")
            clock.advance(1)
            runtime.mark_trade_stream_connected(order.venue, order.canonical_market)
            trade = TradeEvidence(
                "zec-public-sell", Venue.EXTENDED, "ZEC-EXTENDED",
                clock.now(), clock.now(), int(clock.now().timestamp() * 1_000_000_000),
                D("1.5"), D("800.938"), Side.SELL, True,
            )
            await runtime.deliver_trade(trade)
            position = runtime.lifecycle.snapshot.position
            assert position.canonical_quantity == D("0.6")
            assert position.hedge_maker_fill.canonical_price == D("801.056")
            assert position.risex_taker_fill.canonical_price == D("800.53")
            assert repository.connection.execute(
                "SELECT COUNT(*) FROM positions"
            ).fetchone()[0] == 1
            assert repository.connection.execute(
                "SELECT COUNT(*) FROM fills"
            ).fetchone()[0] == 2
            entry_fill_values = repository.connection.execute(
                "SELECT notional_usd,fee_usd FROM fills ORDER BY fill_id"
            ).fetchall()

            extended_required = next(
                row for row in runtime.lifecycle.snapshot.settlements
                if row.venue is Venue.EXTENDED
            )
            await runtime.deliver_settlement(replace(
                extended_required, status=SettlementStatus.APPLIED_RATE,
                cash_usd=D("1.25"),
            ))
            await runtime.mark_disconnected(
                Venue.EXTENDED, "ZEC-EXTENDED", stream_kind="funding",
                exception=TimeoutError("synthetic funding outage"),
                stream_session_id=stream_session(
                    runtime, Venue.EXTENDED, "ZEC-EXTENDED", "funding"
                ),
            )
            assert not runtime.lifecycle.snapshot.gap_open
            await asyncio.gather(
                runtime.mark_disconnected(
                    Venue.EXTENDED, "ZEC-EXTENDED", stream_kind="book",
                    exception=ConnectionError("synthetic book outage"),
                    stream_session_id=stream_session(
                        runtime, Venue.EXTENDED, "ZEC-EXTENDED", "book"
                    ),
                ),
                runtime.mark_disconnected(
                    Venue.EXTENDED, "ZEC-EXTENDED", stream_kind="trade",
                    exception=ConnectionError("synthetic trade outage"),
                    stream_session_id=stream_session(
                        runtime, Venue.EXTENDED, "ZEC-EXTENDED", "trade"
                    ),
                ),
                runtime.mark_disconnected(
                    Venue.RISEX, "ZEC-RISEX", stream_kind="book",
                    exception=ValueError("synthetic checksum resubscribe"),
                    stream_session_id=stream_session(
                        runtime, Venue.RISEX, "ZEC-RISEX", "book"
                    ),
                ),
            )
            assert runtime.lifecycle.snapshot.gap_open
            assert runtime.lifecycle.snapshot.gap_count == 1

            async def nado_timeout(_symbol):
                raise TimeoutError("synthetic snapshot timeout")

            fakes[Venue.NADO].fetch_book = nado_timeout
            nado_session = stream_session(
                runtime, Venue.NADO, "ZEC-NADO", "book"
            )
            episode = runtime._start_snapshot_recovery(
                Venue.NADO, "ZEC-NADO",
                displaced_stream_session_id=nado_session,
            )
            assert episode.task is not None
            await episode.task
            evidence = repository.connection.execute(
                "SELECT event_type FROM runtime_evidence ORDER BY evidence_id"
            ).fetchall()
            assert sum(
                row["event_type"] == "PUBLIC_SNAPSHOT_RECOVERY_FAILED"
                for row in evidence
            ) == 1
            assert not any(
                row["event_type"] == "PUBLIC_RECOVERY_DELTA_BUFFERED"
                for row in evidence
            )
            assert repository.connection.execute(
                "SELECT COUNT(*) FROM completed_trades"
            ).fetchone()[0] == 0
            assert repository.connection.execute(
                "SELECT notional_usd,fee_usd FROM fills ORDER BY fill_id"
            ).fetchall() == entry_fill_values
            applied = [
                row for row in runtime.lifecycle.snapshot.settlements
                if row.key == extended_required.key
            ]
            assert len(applied) == 1
            assert applied[0].status is SettlementStatus.APPLIED_RATE
            assert applied[0].cash_usd == D("1.25")


@pytest.mark.asyncio
async def test_sequence_gap_fetches_snapshot_and_reconnect_restores_readiness(tmp_path):
    clock = FakeClock()
    fakes = adapters(clock, settlement_at=NOW + timedelta(minutes=5))
    delivery = CaptureNotifications()
    with PaperRepository(tmp_path / "sequence-recovery.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock,
            notifications=NotificationOutbox(delivery),
        ) as runtime:
            await runtime.scan()
            market = fakes[Venue.EXTENDED].market
            session_id = stream_session(
                runtime, Venue.EXTENDED, market.venue_symbol, "book"
            )
            await runtime.apply_book_event(BookDelta(
                Venue.EXTENDED, market.venue_symbol,
                (BookLevel(D("100"), D("1")),), (), clock.now(), 3, 999,
            ), stream_session_id=session_id)
            adapter = ExtendedAdapter(None)
            rest = adapter.normalize_book(
                {
                    "market": market.venue_symbol,
                    "bid": [{"price": "99", "qty": "20"}],
                    "ask": [{"price": "101", "qty": "20"}],
                },
                observed_at=clock.now(),
            )
            assert rest.sequence is None
            session_id = runtime._stream_sessions[
                (Venue.EXTENDED, market.venue_symbol, "book")
            ]
            await runtime.apply_book_event(
                rest, stream_session_id=session_id
            )
            episode = runtime._recoveries[Venue.EXTENDED, market.venue_symbol]
            assert episode.terminal is None
            await runtime.apply_book_event(
                adapter.normalize_book_message(
                    {
                        "type": "UPDATE", "seq": 11,
                        "ts": str(int(clock.now().timestamp() * 1000)),
                        "data": {
                            "m": market.venue_symbol,
                            "b": [{"p": "100", "q": "2"}], "a": [],
                        },
                    },
                    received_at=clock.now(),
                ),
                stream_session_id=session_id,
            )
            snapshot = adapter.normalize_book_message(
                {
                    "type": "SNAPSHOT", "seq": 10,
                    "ts": str(int(clock.now().timestamp() * 1000)),
                    "data": {
                        "m": market.venue_symbol,
                        "b": [{"p": "99", "q": "20"}],
                        "a": [{"p": "101", "q": "20"}],
                    },
                },
                received_at=clock.now(),
            )
            await runtime.apply_book_event(
                snapshot, stream_session_id=session_id
            )
            state = runtime.readiness[Venue.EXTENDED]
            book_resyncs = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence "
                "WHERE event_type='PUBLIC_BOOK_RESYNC_REQUIRED'"
            ).fetchone()[0]
            socket_disconnects = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence "
                "WHERE event_type='PUBLIC_SOCKET_DISCONNECTED'"
            ).fetchone()[0]
            socket_reconnects = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence "
                "WHERE event_type='PUBLIC_SOCKET_RECONNECTED'"
            ).fetchone()[0]
    assert fakes[Venue.EXTENDED].calls.count("book") == 1
    assert runtime.coordinator.stream(Venue.EXTENDED, market.venue_symbol).book().sequence == 11
    assert state.available and state.detail == "PUBLIC_STREAM_RECOVERED"
    assert book_resyncs == 1
    assert socket_disconnects == 0 and socket_reconnects == 0
    assert [row.kind for row in delivery.rows if row.kind in {
        "CRITICAL_DATA_LOSS", "DATA_RECOVERY"
    }] == []


def test_repeated_outage_evidence_deduplicates_notifications_by_semantic_episode(tmp_path):
    clock = FakeClock()
    delivery = CaptureNotifications()
    with PaperRepository(tmp_path / "outage-notification-dedupe.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={}, clock=clock,
            notifications=NotificationOutbox(delivery),
        )
        detail = {"symbol": "ABC-EXTENDED", "stream": "book"}
        runtime._record(
            "PUBLIC_BOOK_RESYNC_REQUIRED", at=clock.now(),
            venue=Venue.EXTENDED, detail=detail,
        )
        clock.advance(1)
        runtime._record(
            "PUBLIC_SNAPSHOT_RECOVERY_FAILED", at=clock.now(),
            venue=Venue.EXTENDED, detail=detail,
        )
        clock.advance(1)
        runtime._record(
            "PUBLIC_SNAPSHOT_RECOVERY_COMPLETED", at=clock.now(),
            venue=Venue.EXTENDED, detail=detail,
        )
        clock.advance(1)
        runtime._record(
            "PUBLIC_SNAPSHOT_RECOVERY_COMPLETED", at=clock.now(),
            venue=Venue.EXTENDED, detail=detail,
        )
        clock.advance(1)
        runtime._record(
            "PUBLIC_BOOK_RESYNC_REQUIRED", at=clock.now(),
            venue=Venue.EXTENDED, detail=detail,
        )
        persisted = repository.connection.execute(
            "SELECT event_type FROM runtime_evidence ORDER BY recorded_at"
        ).fetchall()
    assert [row[0] for row in persisted] == [
        "PUBLIC_BOOK_RESYNC_REQUIRED", "PUBLIC_SNAPSHOT_RECOVERY_FAILED",
        "PUBLIC_SNAPSHOT_RECOVERY_COMPLETED", "PUBLIC_SNAPSHOT_RECOVERY_COMPLETED",
        "PUBLIC_BOOK_RESYNC_REQUIRED",
    ]
    assert [row.kind for row in delivery.rows] == [
        "CRITICAL_DATA_LOSS", "DATA_RECOVERY",
    ]


def test_socket_outage_evidence_keeps_physical_episode_identity_without_alerts(
    tmp_path,
):
    clock = FakeClock()
    delivery = CaptureNotifications()
    outbox = NotificationOutbox(delivery)
    detail = {
        "episode_id": "EXTENDED:trade:episode-1",
        "market": "ABC-EXTENDED",
        "stream_kind": "trade",
    }
    with PaperRepository(tmp_path / "socket-notification-dedupe.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={}, clock=clock,
            notifications=outbox,
        )
        runtime._record(
            "PUBLIC_SOCKET_DISCONNECTED", at=clock.now(),
            venue=Venue.EXTENDED, detail=detail,
        )
        clock.advance(1)
        runtime._record(
            "PUBLIC_SOCKET_DISCONNECTED", at=clock.now(),
            venue=Venue.EXTENDED, detail=detail,
        )
        clock.advance(1)
        runtime._record(
            "PUBLIC_SOCKET_RECONNECTED", at=clock.now(),
            venue=Venue.EXTENDED, detail=detail,
        )
        count = repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence WHERE event_type LIKE 'PUBLIC_SOCKET_%'"
        ).fetchone()[0]
    assert count == 3
    assert [row.kind for row in delivery.rows] == []
    assert outbox._active_outages == set()


def test_transient_socket_wave_persists_raw_lifecycle_without_alerts(tmp_path):
    clock = FakeClock()
    delivery = CaptureNotifications()
    outbox = NotificationOutbox(delivery)
    socket_count = 45
    episodes = tuple(
        (f"MARKET-{index}", ("book", "trade", "funding")[index % 3])
        for index in range(socket_count)
    )
    with PaperRepository(tmp_path / "transient-socket-wave.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={}, clock=clock, notifications=outbox,
        )
        for index, (symbol, stream_kind) in enumerate(episodes):
            detail = {
                "episode_id": f"EXTENDED:{stream_kind}:episode-{index}",
                "market": symbol,
                "symbol": symbol,
                "stream_kind": stream_kind,
                "stream": stream_kind,
            }
            runtime._record(
                "PUBLIC_SOCKET_DISCONNECTED", at=clock.now(),
                venue=Venue.EXTENDED, detail=detail,
            )
            if stream_kind == "book":
                runtime._record(
                    "PUBLIC_BOOK_RESYNC_REQUIRED", at=clock.now(),
                    venue=Venue.EXTENDED, detail=detail,
                )
            clock.advance(1)
            runtime._record(
                "PUBLIC_SOCKET_RECONNECTED", at=clock.now(),
                venue=Venue.EXTENDED, detail=detail,
            )
        rows = repository.connection.execute(
            "SELECT event_type FROM runtime_evidence ORDER BY evidence_id"
        ).fetchall()
    event_types = [row["event_type"] for row in rows]
    assert event_types.count("PUBLIC_SOCKET_DISCONNECTED") == len(episodes)
    assert event_types.count("PUBLIC_SOCKET_RECONNECTED") == len(episodes)
    assert event_types.count("PUBLIC_BOOK_RESYNC_REQUIRED") == sum(
        stream_kind == "book" for _, stream_kind in episodes
    )
    assert [row.kind for row in delivery.rows if row.kind in {
        "CRITICAL_DATA_LOSS", "DATA_RECOVERY"
    }] == []
    assert outbox._active_outages == set()


@pytest.mark.asyncio
async def test_pending_socket_episode_alerts_at_existing_silence_threshold(tmp_path):
    clock = FakeClock()
    delivery = CaptureNotifications()
    outbox = NotificationOutbox(delivery)
    symbol = "PENDING-EXTENDED"
    identity = (Venue.EXTENDED, "trade", (symbol,))
    with PaperRepository(tmp_path / "pending-socket-outage.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={}, clock=clock, notifications=outbox,
        )
        session_id = runtime._new_stream_session(
            (Venue.EXTENDED, symbol, "trade")
        )
        runtime._socket_disconnected(
            identity, at=clock.now(), stream_session_id=session_id,
        )
        runtime.last_scan = SimpleNamespace(logical_at=clock.now())
        runtime.next_full_scan_at = clock.now() + timedelta(hours=1)
        runtime.next_health_check_at = clock.now() + timedelta(hours=1)
        runtime._startup_gate_satisfied = True
        runtime.accepting_entries = False
        clock.advance(25)
        await runtime.tick(clock.now())
        assert [row.kind for row in delivery.rows] == ["CRITICAL_DATA_LOSS"]

        clock.advance(1)
        runtime._socket_reconnected(
            identity, at=clock.now(), stream_session_id=session_id,
        )
        lifecycle = repository.connection.execute(
            "SELECT event_type FROM runtime_evidence ORDER BY evidence_id"
        ).fetchall()

    assert [row["event_type"] for row in lifecycle] == [
        "PUBLIC_SOCKET_DISCONNECTED", "PUBLIC_SOCKET_RECONNECTED",
    ]
    assert [row.kind for row in delivery.rows] == [
        "CRITICAL_DATA_LOSS", "DATA_RECOVERY",
    ]
    assert outbox._active_outages == set()


def test_extended_socket_wave_coalesces_late_episodes_to_one_pair(tmp_path):
    clock = FakeClock()
    delivery = CaptureNotifications()
    outbox = NotificationOutbox(delivery)
    socket_count = 45
    episodes = []
    with PaperRepository(tmp_path / "late-socket-wave.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={}, clock=clock, notifications=outbox,
        )
        for index in range(socket_count):
            symbol = f"WAVE-{index}-EXTENDED"
            stream_kind = ("book", "trade", "funding")[index % 3]
            identity = (Venue.EXTENDED, stream_kind, (symbol,))
            session_id = runtime._new_stream_session(
                (Venue.EXTENDED, symbol, stream_kind)
            )
            episodes.append((identity, session_id))
            runtime._socket_disconnected(
                identity, at=clock.now(), stream_session_id=session_id,
            )

        clock.advance(40)
        runtime._notify_pending_socket_outages(clock.now())
        assert [row.kind for row in delivery.rows] == ["CRITICAL_DATA_LOSS"]
        for index, (identity, session_id) in enumerate(episodes):
            runtime._socket_reconnected(
                identity, at=clock.now(), stream_session_id=session_id,
            )
            if index < socket_count - 1:
                assert [row.kind for row in delivery.rows] == [
                    "CRITICAL_DATA_LOSS"
                ]
        lifecycle = repository.connection.execute(
            "SELECT event_type FROM runtime_evidence ORDER BY evidence_id"
        ).fetchall()

    event_types = [row["event_type"] for row in lifecycle]
    assert event_types.count("PUBLIC_SOCKET_DISCONNECTED") == socket_count
    assert event_types.count("PUBLIC_SOCKET_RECONNECTED") == socket_count
    assert [row.kind for row in delivery.rows] == [
        "CRITICAL_DATA_LOSS", "DATA_RECOVERY",
    ]
    assert outbox._active_outages == set()


def test_extended_transient_socket_wave_keeps_raw_rows_without_alerts(tmp_path):
    clock = FakeClock()
    delivery = CaptureNotifications()
    outbox = NotificationOutbox(delivery)
    socket_count = 45
    episodes = []
    with PaperRepository(tmp_path / "transient-socket-wave-pending.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={}, clock=clock, notifications=outbox,
        )
        for index in range(socket_count):
            symbol = f"TRANSIENT-{index}-EXTENDED"
            stream_kind = ("book", "trade", "funding")[index % 3]
            identity = (Venue.EXTENDED, stream_kind, (symbol,))
            session_id = runtime._new_stream_session(
                (Venue.EXTENDED, symbol, stream_kind)
            )
            episodes.append((identity, session_id))
            runtime._socket_disconnected(
                identity, at=clock.now(), stream_session_id=session_id,
            )

        clock.advance(24)
        runtime._notify_pending_socket_outages(clock.now())
        for identity, session_id in episodes:
            runtime._socket_reconnected(
                identity, at=clock.now(), stream_session_id=session_id,
            )
        lifecycle = repository.connection.execute(
            "SELECT event_type FROM runtime_evidence ORDER BY evidence_id"
        ).fetchall()

    event_types = [row["event_type"] for row in lifecycle]
    assert event_types.count("PUBLIC_SOCKET_DISCONNECTED") == socket_count
    assert event_types.count("PUBLIC_SOCKET_RECONNECTED") == socket_count
    assert [row.kind for row in delivery.rows] == []
    assert outbox._active_outages == set()


@pytest.mark.parametrize(("duration", "expected"), (
    (24, []),
    (34, ["CRITICAL_DATA_LOSS", "DATA_RECOVERY"]),
    (44, ["CRITICAL_DATA_LOSS", "DATA_RECOVERY"]),
))
def test_socket_reconnect_classifies_24_to_44_second_episodes(
    tmp_path, duration, expected,
):
    clock = FakeClock()
    delivery = CaptureNotifications()
    outbox = NotificationOutbox(delivery)
    symbol = f"LATE-{duration}-EXTENDED"
    identity = (Venue.EXTENDED, "trade", (symbol,))
    with PaperRepository(tmp_path / f"socket-reconnect-{duration}.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={}, clock=clock, notifications=outbox,
        )
        session_id = runtime._new_stream_session(
            (Venue.EXTENDED, symbol, "trade")
        )
        runtime._socket_disconnected(
            identity, at=clock.now(), stream_session_id=session_id,
        )
        clock.advance(duration)
        runtime._socket_reconnected(
            identity, at=clock.now(), stream_session_id=session_id,
        )
        lifecycle = repository.connection.execute(
            "SELECT event_type FROM runtime_evidence ORDER BY evidence_id"
        ).fetchall()

    assert [row["event_type"] for row in lifecycle] == [
        "PUBLIC_SOCKET_DISCONNECTED", "PUBLIC_SOCKET_RECONNECTED",
    ]
    assert [row.kind for row in delivery.rows] == expected
    assert outbox._active_outages == set()


def test_blocked_full_semantic_episode_notifies_once_with_reason(tmp_path):
    clock = FakeClock()
    delivery = CaptureNotifications()
    outbox = NotificationOutbox(delivery)
    detail = {
        "kind": "full",
        "reason": "PUBLIC_REFRESH_DEADLINE_EXCEEDED",
        "scheduled_at": NOW.isoformat(),
        "deadline_at": (NOW + timedelta(seconds=30)).isoformat(),
        "completed": False,
        "catalog_generation": 4,
    }
    with PaperRepository(tmp_path / "blocked-notification-dedupe.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={}, clock=clock, notifications=outbox,
        )
        runtime._record("PUBLIC_SCAN_BLOCKED", at=clock.now(), detail=detail)
        clock.advance(1)
        runtime._record("PUBLIC_SCAN_BLOCKED", at=clock.now(), detail=detail)
        lifecycle = repository.connection.execute(
            "SELECT event_type FROM runtime_evidence ORDER BY evidence_id"
        ).fetchall()

    assert [row["event_type"] for row in lifecycle] == [
        "PUBLIC_SCAN_BLOCKED", "PUBLIC_SCAN_BLOCKED",
    ]
    assert [row.kind for row in delivery.rows] == ["CRITICAL_DATA_LOSS"]
    assert delivery.rows[0].text == (
        "Critical public scan blocked: PUBLIC_REFRESH_DEADLINE_EXCEEDED"
    )


@pytest.mark.parametrize(("failure", "recovery", "venue", "detail"), (
    (
        "PUBLIC_STREAM_CONFIRMATION_STALE", "PUBLIC_STREAM_RESTARTED",
        Venue.EXTENDED,
        {
            "episode_id": "watchdog:EXTENDED:book:ABC-EXTENDED:1",
            "market": "ABC-EXTENDED", "stream_kind": "book",
        },
    ),
    (
        "PUBLIC_SNAPSHOT_RECOVERY_FAILED", "PUBLIC_SNAPSHOT_RECOVERY_COMPLETED",
        Venue.NADO,
        {
            "episode_id": "nado-recovery-1",
            "symbol": "ABC-NADO", "stream_kind": "book",
        },
    ),
))
def test_persistent_semantic_outage_notifies_once_and_pairs_recovery(
    tmp_path, failure, recovery, venue, detail,
):
    clock = FakeClock()
    delivery = CaptureNotifications()
    outbox = NotificationOutbox(delivery)
    with PaperRepository(tmp_path / f"{failure.lower()}.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={}, clock=clock, notifications=outbox,
        )
        runtime._record(failure, at=clock.now(), venue=venue, detail=detail)
        clock.advance(1)
        runtime._record(failure, at=clock.now(), venue=venue, detail=detail)
        clock.advance(1)
        runtime._record(recovery, at=clock.now(), venue=venue, detail=detail)
        clock.advance(1)
        runtime._record(recovery, at=clock.now(), venue=venue, detail=detail)
        persisted = repository.connection.execute(
            "SELECT event_type FROM runtime_evidence ORDER BY evidence_id"
        ).fetchall()
    assert [row["event_type"] for row in persisted] == [failure] * 2 + [recovery] * 2
    assert [row.kind for row in delivery.rows] == [
        "CRITICAL_DATA_LOSS", "DATA_RECOVERY",
    ]
    assert outbox._active_outages == set()


@pytest.mark.asyncio
async def test_nado_recovery_buffers_and_replays_newer_continuous_delta(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(minutes=5)
    fakes = adapters(clock, settlement_at=target)
    gated = GatedRecoveryAdapter(Venue.NADO, clock, settlement_at=target)
    fakes[Venue.NADO] = gated
    with PaperRepository(tmp_path / "nado-buffered-recovery.db") as repository:
        async with PublicPaperRuntime(repository, adapters=fakes, clock=clock) as runtime:
            await runtime.scan()
            symbol = gated.market.venue_symbol
            await runtime.recover_snapshot(await gated.fetch_book(symbol))
            assert repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence "
                "WHERE event_type IN "
                "('PUBLIC_SOCKET_DISCONNECTED','PUBLIC_SOCKET_RECONNECTED')"
            ).fetchone()[0] == 0
            baseline_book_calls = gated.calls.count("book")
            gated.block_recovery = True
            session_id = runtime._new_stream_session(
                (Venue.NADO, "*", "combined")
            )
            await runtime.apply_book_event(BookDelta(
                Venue.NADO, symbol, (), (), clock.now(), 3, 999,
            ), stream_session_id=session_id)
            episode = runtime._recoveries[(Venue.NADO, symbol)]
            recovery = episode.task
            assert recovery is not None
            runtime._start_snapshot_recovery(
                Venue.NADO, symbol,
                displaced_stream_session_id=session_id,
            )
            assert runtime._recoveries[(Venue.NADO, symbol)] is episode
            await gated.recovery_started.wait()
            await runtime.apply_book_event(BookDelta(
                Venue.NADO, symbol,
                (BookLevel(D("100"), D("2")),), (), clock.now(), 2, 1,
            ), stream_session_id=session_id)
            await runtime.apply_book_event(BookDelta(
                Venue.NADO, symbol,
                (BookLevel(D("100"), D("3")),), (), clock.now(), 3, 2,
            ), stream_session_id=session_id)
            assert len(episode.buffer) == 2
            gated.block_recovery = False
            gated.recovery_gate.set()
            await recovery
            book = runtime.coordinator.stream(Venue.NADO, symbol).book()
            await runtime.apply_book_event(
                replace(book, observed_at=clock.now()),
                stream_session_id=session_id,
            )
            await runtime.apply_book_event(
                replace(book, observed_at=clock.now()),
                stream_session_id=session_id,
            )
            evidence = repository.connection.execute(
                "SELECT detail FROM runtime_evidence "
                "WHERE event_type='PUBLIC_SNAPSHOT_RECOVERY_COMPLETED'"
            ).fetchone()
            socket_lifecycle_count = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence "
                "WHERE event_type IN "
                "('PUBLIC_SOCKET_DISCONNECTED','PUBLIC_SOCKET_RECONNECTED')"
            ).fetchone()[0]
    assert book.sequence == 3
    assert gated.calls.count("book") == baseline_book_calls + 1
    assert json.loads(evidence["detail"])["replayed"] == 2
    assert socket_lifecycle_count == 0


@pytest.mark.asyncio
async def test_risex_checksum_gap_resubscribes_and_recovers_from_ws_snapshots(tmp_path):
    clock = FakeClock()
    stop = asyncio.Event()
    symbols = ("AAA/USDC", "BBB/USDC")
    market_ids = {symbols[0]: "1", symbols[1]: "2"}
    timestamp_ns = str(int(clock.now().timestamp() * 1_000_000_000))

    def levels(quantity: str = "20") -> dict[str, object]:
        return {
            "bids": [{"price": "99", "quantity": quantity}],
            "asks": [{"price": "101", "quantity": "20"}],
        }

    checksum_book = BookStream(Venue.RISEX, symbols[0])
    checksum_book.snapshot(OrderBook(
        Venue.RISEX, symbols[0],
        (BookLevel(D("99"), D("18")),),
        (BookLevel(D("101"), D("20")),), clock.now(),
    ))
    valid_checksum = checksum_book.risex_checksum()
    payloads = [
        {
            "channel": "orderbook", "type": "update", "market_id": "1",
            "worker_timestamp": timestamp_ns, "checksum": 0,
            "data": {"market_id": 1, **levels("18")},
        },
        {
            "channel": "orderbook", "type": "update", "market_id": "1",
            "worker_timestamp": timestamp_ns, "checksum": 0,
            "data": {"market_id": 1, **levels("19")},
        },
        {
            "method": "snapshot", "channel": "orderbook", "type": "snapshot",
            "market_id": "1", "worker_timestamp": timestamp_ns,
            "data": {"market_id": 1, **levels()},
        },
        {
            "method": "snapshot", "channel": "orderbook", "type": "snapshot",
            "market_id": "2", "worker_timestamp": timestamp_ns,
            "data": {"market_id": 2, **levels()},
        },
        {
            "channel": "orderbook", "type": "update", "market_id": "1",
            "worker_timestamp": timestamp_ns, "checksum": valid_checksum,
            "data": {
                "market_id": 1,
                "bids": [{"price": "99", "quantity": "18"}], "asks": [],
            },
        },
    ]

    class SyntheticRisexAdapter(RisexAdapter):
        def __init__(self) -> None:
            super().__init__(None)
            self.book_calls = 0
            self._market_ids = dict(market_ids)
            self._symbols_by_id = {value: key for key, value in market_ids.items()}
            self._raw_markets = {
                symbol: {
                    "market_id": market_id,
                    "config": {
                        "name": symbol, "step_size": "1",
                        "step_price": "1", "min_order_size": "1",
                    },
                }
                for symbol, market_id in market_ids.items()
            }

        async def fetch_book(self, venue_symbol: str) -> OrderBook:
            self.book_calls += 1
            return OrderBook(
                Venue.RISEX, venue_symbol,
                (BookLevel(D("99"), D("20")),),
                (BookLevel(D("101"), D("20")),), clock.now(),
            )

    class TextMessage:
        type = aiohttp.WSMsgType.TEXT

        def __init__(self, payload: dict[str, object]) -> None:
            self.data = json.dumps(payload)

    class ScriptedWebSocket:
        def __init__(self) -> None:
            self.messages = [TextMessage(payload) for payload in payloads]
            self.sent: list[dict[str, object]] = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return None

        def __aiter__(self):
            return self

        async def __anext__(self):
            if not self.messages:
                stop.set()
                raise StopAsyncIteration
            return self.messages.pop(0)

        async def send_json(self, payload: dict[str, object]) -> None:
            self.sent.append(payload)

        async def pong(self, _payload) -> None:
            return None

    class ScriptedSession:
        def __init__(self, websocket: ScriptedWebSocket) -> None:
            self.websocket = websocket
            self.connections = 0

        def ws_connect(self, *_args, **_kwargs):
            self.connections += 1
            return self.websocket

    adapter = SyntheticRisexAdapter()
    websocket = ScriptedWebSocket()
    session = ScriptedSession(websocket)
    with PaperRepository(tmp_path / "risex-ws-resubscribe.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.RISEX: adapter}, clock=clock,
        )
        runtime._session = session
        runtime._stop_event = stop
        session_id = runtime._new_stream_session(
            (Venue.RISEX, "*", "combined")
        )
        await runtime._combined_stream(
            Venue.RISEX, adapter, symbols, session_id
        )
        lifecycle = repository.connection.execute(
            "SELECT event_type,detail FROM runtime_evidence WHERE event_type IN "
            "('PUBLIC_BOOK_RESYNC_REQUIRED','PUBLIC_BOOK_RESYNC_STARTED',"
            "'PUBLIC_SNAPSHOT_RECOVERY_STARTED','PUBLIC_SNAPSHOT_RECOVERY_COMPLETED',"
            "'PUBLIC_SNAPSHOT_RECOVERY_FAILED','PUBLIC_SOCKET_DISCONNECTED',"
            "'PUBLIC_SOCKET_RECONNECTED') ORDER BY evidence_id"
        ).fetchall()

    assert session.connections == 1
    assert adapter.book_calls == len(symbols)
    assert websocket.sent == [
        adapter.orderbook_subscription([1, 2]),
        adapter.trades_subscription([1, 2]),
        adapter.orderbook_unsubscription(),
        adapter.orderbook_subscription([1, 2]),
    ]
    assert [row["event_type"] for row in lifecycle] == [
        "PUBLIC_BOOK_RESYNC_REQUIRED", "PUBLIC_BOOK_RESYNC_REQUIRED",
        "PUBLIC_SNAPSHOT_RECOVERY_STARTED",
        "PUBLIC_SNAPSHOT_RECOVERY_STARTED",
        "PUBLIC_BOOK_RESYNC_STARTED", "PUBLIC_SNAPSHOT_RECOVERY_COMPLETED",
        "PUBLIC_SNAPSHOT_RECOVERY_COMPLETED",
    ]
    completed = [
        json.loads(row["detail"]) for row in lifecycle
        if row["event_type"] == "PUBLIC_SNAPSHOT_RECOVERY_COMPLETED"
    ]
    assert {row["symbol"] for row in completed} == set(symbols)
    assert {row["source"] for row in completed} == {"WS_RESUBSCRIBE_SNAPSHOT"}
    assert sum(row["buffered"] for row in completed) == 1
    assert all(row["replayed"] == 0 for row in completed)
    assert all(not episode.buffer for episode in runtime._recoveries.values())
    assert all(episode.task is None for episode in runtime._recoveries.values())
    assert all(
        runtime.coordinator.stream(Venue.RISEX, symbol).health(clock.now()).data_quality
        is DataQuality.COMPLETE
        for symbol in symbols
    )


@pytest.mark.asyncio
async def test_risex_delta_cannot_extend_rest_book_before_ws_snapshot(tmp_path):
    """A checksum-valid delta still needs the session's WS snapshot boundary."""
    clock = FakeClock()
    symbol = "ABC/USDC"
    key = (Venue.RISEX, symbol)
    rest_book = OrderBook(
        Venue.RISEX, symbol,
        (BookLevel(D("99"), D("20")),),
        (BookLevel(D("101"), D("20")),),
        clock.now(),
    )
    rest_after_delta = OrderBook(
        Venue.RISEX, symbol,
        (BookLevel(D("99"), D("21")),),
        (BookLevel(D("101"), D("20")),),
        clock.now(),
    )

    projected = BookStream(Venue.RISEX, symbol)
    projected.snapshot(rest_after_delta)

    with PaperRepository(tmp_path / "risex-rest-delta-boundary.db") as repository:
        runtime = PublicPaperRuntime(repository)
        session_id = runtime._new_stream_session(
            (Venue.RISEX, "*", "combined")
        )
        assert await runtime.recover_snapshot(rest_book)
        assert key not in runtime._risex_ws_book_sessions
        assert not await runtime.apply_book_event(
            BookDelta(
                Venue.RISEX, symbol,
                rest_after_delta.bids, (), clock.now(),
                checksum=projected.risex_checksum(),
            ),
            stream_session_id=session_id,
        )
        assert runtime.coordinator.stream(*key).book() is None
        assert repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence "
            "WHERE event_type='PUBLIC_BOOK_RESYNC_REQUIRED'"
        ).fetchone()[0] == 1


@pytest.mark.asyncio
async def test_risex_rest_bootstrap_does_not_claim_ws_checksum_ownership(
    tmp_path,
):
    """The startup REST bootstrap stays unowned until its WS snapshot arrives."""
    clock = FakeClock()
    stop = asyncio.Event()
    bootstrap_observed = asyncio.Event()
    release_messages = asyncio.Event()
    symbol = "ABC/USDC"
    key = (Venue.RISEX, symbol)
    stream_key = (Venue.RISEX, "*", "combined")
    bootstrap_book = OrderBook(
        Venue.RISEX, symbol,
        (BookLevel(D("99"), D("20")),),
        (BookLevel(D("101"), D("20")),),
        clock.now(),
    )
    refresh_book = OrderBook(
        Venue.RISEX, symbol,
        (BookLevel(D("98"), D("21")),),
        (BookLevel(D("102"), D("21")),),
        clock.now(),
    )
    ws_book = OrderBook(
        Venue.RISEX, symbol,
        (BookLevel(D("100"), D("2")),),
        (BookLevel(D("102"), D("2")),),
        clock.now(),
    )
    ws_after_update = OrderBook(
        Venue.RISEX, symbol,
        (BookLevel(D("100"), D("3")),),
        (BookLevel(D("102"), D("2")),),
        clock.now(),
    )

    def checksum(book: OrderBook) -> int:
        projected = BookStream(Venue.RISEX, symbol)
        projected.snapshot(book)
        return projected.risex_checksum()

    timestamp_ns = str(int(clock.now().timestamp() * 1_000_000_000))
    payloads = [
        {
            "method": "snapshot", "channel": "orderbook", "type": "snapshot",
            "market_id": "1", "worker_timestamp": timestamp_ns,
            "data": {
                "market_id": 1,
                "bids": [{"price": "100", "quantity": "2"}],
                "asks": [{"price": "102", "quantity": "2"}],
            },
        },
        {
            "channel": "orderbook", "type": "update", "market_id": "1",
            "worker_timestamp": timestamp_ns,
            "checksum": checksum(ws_after_update),
            "data": {
                "market_id": 1,
                "bids": [{"price": "100", "quantity": "3"}],
                "asks": [],
            },
        },
    ]

    class StartupRisexAdapter(RisexAdapter):
        def __init__(self) -> None:
            super().__init__(None)
            self.market = replace(
                FakeAdapter(
                    Venue.RISEX, clock, settlement_at=NOW + timedelta(minutes=5)
                ).market,
                venue_symbol=symbol,
            )
            self.book_calls = 0
            self._market_ids = {symbol: "1"}
            self._symbols_by_id = {"1": symbol}

        async def fetch_book(self, venue_symbol: str) -> OrderBook:
            assert venue_symbol == symbol
            self.book_calls += 1
            return bootstrap_book if self.book_calls == 1 else refresh_book

        async def prime_recent_trade_evidence(
            self, market: CanonicalMarket, *, limit: int = 20
        ) -> CanonicalMarket:
            return market

        async def fetch_funding_quote(self, market, *, assumed_open_at):
            return FundingCashQuote(
                Venue.RISEX, symbol, clock.now(), assumed_open_at,
                NOW + timedelta(minutes=5), FundingQuality.ESTIMATED,
                FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT, True,
                D("1"), D("1"), "PAPER_ASSUMPTION:RISEX_PUBLIC_FALLBACK",
            )

    class TextMessage:
        type = aiohttp.WSMsgType.TEXT

        def __init__(self, payload: dict[str, object]) -> None:
            self.data = json.dumps(payload)

    class StartupWebSocket:
        def __init__(self) -> None:
            self.messages = [TextMessage(payload) for payload in payloads]
            self.sent: list[dict[str, object]] = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return None

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.messages:
                bootstrap_observed.set()
                await release_messages.wait()
                return self.messages.pop(0)
            stop.set()
            raise StopAsyncIteration

        async def send_json(self, payload: dict[str, object]) -> None:
            self.sent.append(payload)

        async def pong(self, _payload) -> None:
            return None

    class StartupSession:
        def __init__(self, websocket: StartupWebSocket) -> None:
            self.websocket = websocket

        def ws_connect(self, *_args, **_kwargs):
            return self.websocket

    adapter = StartupRisexAdapter()
    websocket = StartupWebSocket()
    session = StartupSession(websocket)
    volume = MarketVolume(
        Venue.RISEX, symbol, D("1000000"), clock.now(), "official-shaped"
    )

    with PaperRepository(tmp_path / "risex-rest-bootstrap-ownership.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.RISEX: adapter}, clock=clock,
        )
        runtime._session = session
        runtime._stop_event = stop
        session_id = runtime._new_stream_session(stream_key)
        stream_task = asyncio.create_task(
            runtime._combined_stream(
                Venue.RISEX, adapter, (symbol,), session_id
            )
        )
        await bootstrap_observed.wait()

        assert adapter.book_calls == 1
        assert key not in runtime._live_book_ready
        assert runtime._owned_risex_ws_book(key) is None

        await runtime._market_observation(
            adapter.market, clock.now(), background=True,
            catalog_volumes={key: volume},
        )
        assert adapter.book_calls == 2
        assert key not in runtime._live_book_ready
        assert runtime._owned_risex_ws_book(key) is None

        release_messages.set()
        await stream_task
        resyncs = repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence "
            "WHERE event_type='PUBLIC_BOOK_RESYNC_REQUIRED'"
        ).fetchone()[0]

    assert adapter.book_calls == 2
    assert key in runtime._live_book_ready
    assert runtime._risex_ws_book_sessions[key] == session_id
    assert runtime.coordinator.stream(*key).book() == ws_after_update
    assert resyncs == 0


@pytest.mark.asyncio
async def test_risex_background_refresh_does_not_overwrite_live_combined_book(
    tmp_path,
):
    """A REST refresh racing WS ownership must retain the live checksum state."""
    clock = FakeClock()
    symbol = "ABC/USDC"
    key = (Venue.RISEX, symbol)
    stream_key = (Venue.RISEX, "*", "combined")
    book_started = asyncio.Event()
    release_book = asyncio.Event()
    stale_rest_book = OrderBook(
        Venue.RISEX, symbol,
        (BookLevel(D("99"), D("20")),),
        (BookLevel(D("101"), D("20")),),
        clock.now(),
    )

    class RacingRisexAdapter(RisexAdapter):
        def __init__(self) -> None:
            super().__init__(None)
            self.market = replace(
                FakeAdapter(
                    Venue.RISEX, clock, settlement_at=NOW + timedelta(minutes=5)
                ).market,
                venue_symbol=symbol,
            )
            self._market_ids = {symbol: "1"}
            self._symbols_by_id = {"1": symbol}

        async def fetch_book(self, venue_symbol: str) -> OrderBook:
            assert venue_symbol == symbol
            book_started.set()
            await release_book.wait()
            return stale_rest_book

        async def prime_recent_trade_evidence(
            self, market: CanonicalMarket, *, limit: int = 20
        ) -> CanonicalMarket:
            return market

        async def fetch_funding_quote(self, market, *, assumed_open_at):
            return FundingCashQuote(
                Venue.RISEX, symbol, clock.now(), assumed_open_at,
                NOW + timedelta(minutes=5), FundingQuality.ESTIMATED,
                FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT, True,
                D("1"), D("1"), "PAPER_ASSUMPTION:RISEX_PUBLIC_FALLBACK",
            )

    adapter = RacingRisexAdapter()
    volume = MarketVolume(
        Venue.RISEX, symbol, D("1000000"), clock.now(), "official-shaped"
    )
    old_quote = await adapter.fetch_funding_quote(
        adapter.market, assumed_open_at=clock.now()
    )
    live_snapshot = OrderBook(
        Venue.RISEX, symbol,
        (BookLevel(D("100"), D("2")),),
        (BookLevel(D("102"), D("2")),),
        clock.now(),
    )
    live_after_first_update = OrderBook(
        Venue.RISEX, symbol,
        (BookLevel(D("100"), D("3")),),
        (BookLevel(D("102"), D("2")),),
        clock.now(),
    )
    live_after_second_update = OrderBook(
        Venue.RISEX, symbol,
        (BookLevel(D("100"), D("4")),),
        (BookLevel(D("102"), D("2")),),
        clock.now(),
    )

    def checksum(book: OrderBook) -> int:
        projected = BookStream(Venue.RISEX, symbol)
        projected.snapshot(book)
        return projected.risex_checksum()

    with PaperRepository(tmp_path / "risex-refresh-book-ownership.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.RISEX: adapter}, clock=clock
        )
        stream = runtime.coordinator.stream(*key)
        stream.connected(clock.now())
        stream.snapshot(stale_rest_book)
        stream.connection_confirmed(clock.now())
        runtime.observations[key] = MarketObservation(
            adapter.market, volume, stale_rest_book, old_quote, stream.health(clock.now()),
            trade_stream_ready=False,
        )
        session_id = runtime._new_stream_session(stream_key)

        refresh = asyncio.create_task(runtime._market_observation(
            adapter.market, clock.now(), background=True,
            catalog_volumes={key: volume},
        ))
        await book_started.wait()

        replacement_session = runtime._new_stream_session(stream_key)
        assert replacement_session != session_id
        assert not await runtime.apply_book_event(
            live_snapshot, stream_session_id=session_id
        )
        assert await runtime.apply_book_event(
            live_snapshot, stream_session_id=replacement_session
        )
        assert await runtime.apply_book_event(
            BookDelta(
                Venue.RISEX, symbol,
                live_after_first_update.bids, (), clock.now(),
                checksum=checksum(live_after_first_update),
            ),
            stream_session_id=replacement_session,
        )

        release_book.set()
        await refresh

        # The refresh returned an older REST snapshot while the combined WS
        # stream had already established and advanced its own checksum state.
        assert runtime.coordinator.stream(*key).book() == live_after_first_update
        assert await runtime.apply_book_event(
            BookDelta(
                Venue.RISEX, symbol,
                live_after_second_update.bids, (), clock.now(),
                checksum=checksum(live_after_second_update),
            ),
            stream_session_id=replacement_session,
        )
        assert runtime.coordinator.stream(*key).book() == live_after_second_update
        assert repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence "
            "WHERE event_type='PUBLIC_BOOK_RESYNC_REQUIRED'"
        ).fetchone()[0] == 0


@pytest.mark.asyncio
async def test_risex_background_refresh_preserves_quiet_owned_checksum_book(
    tmp_path,
):
    """A quiet owned book remains the WS checksum baseline while unusable."""
    clock = FakeClock()
    symbol = "ABC/USDC"
    key = (Venue.RISEX, symbol)
    stream_key = (Venue.RISEX, "*", "combined")
    quiet_at = clock.now() - timedelta(seconds=26)
    ws_book = OrderBook(
        Venue.RISEX, symbol,
        (BookLevel(D("100"), D("2")),),
        (BookLevel(D("102"), D("2")),),
        quiet_at,
    )
    rest_book = OrderBook(
        Venue.RISEX, symbol,
        (BookLevel(D("99"), D("20")),),
        (BookLevel(D("101"), D("20")),),
        clock.now(),
    )
    ws_after_delta = OrderBook(
        Venue.RISEX, symbol,
        (BookLevel(D("100"), D("3")),),
        (BookLevel(D("102"), D("2")),),
        clock.now(),
    )

    class QuietRisexAdapter(RisexAdapter):
        def __init__(self) -> None:
            super().__init__(None)
            self.market = replace(
                FakeAdapter(
                    Venue.RISEX, clock, settlement_at=NOW + timedelta(minutes=5)
                ).market,
                venue_symbol=symbol,
            )
            self.book_calls = 0
            self.funding_calls = 0
            self._market_ids = {symbol: "1"}
            self._symbols_by_id = {"1": symbol}

        async def fetch_book(self, venue_symbol: str) -> OrderBook:
            assert venue_symbol == symbol
            self.book_calls += 1
            return rest_book

        async def prime_recent_trade_evidence(
            self, market: CanonicalMarket, *, limit: int = 20
        ) -> CanonicalMarket:
            return market

        async def fetch_funding_quote(self, market, *, assumed_open_at):
            self.funding_calls += 1
            return FundingCashQuote(
                Venue.RISEX, symbol, clock.now(), assumed_open_at,
                NOW + timedelta(minutes=5), FundingQuality.ESTIMATED,
                FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT, True,
                D("1"), D("1"), "PAPER_ASSUMPTION:RISEX_PUBLIC_FALLBACK",
            )

    adapter = QuietRisexAdapter()
    volume = MarketVolume(
        Venue.RISEX, symbol, D("1000000"), clock.now(), "official-shaped"
    )
    old_quote = FundingCashQuote(
        Venue.RISEX, symbol, quiet_at, quiet_at,
        NOW + timedelta(minutes=5), FundingQuality.ESTIMATED,
        FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT, True,
        D("1"), D("1"), "PAPER_ASSUMPTION:RISEX_PUBLIC_FALLBACK",
    )

    def checksum(book: OrderBook) -> int:
        projected = BookStream(Venue.RISEX, symbol)
        projected.snapshot(book)
        return projected.risex_checksum()

    with PaperRepository(tmp_path / "risex-quiet-book-ownership.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.RISEX: adapter}, clock=clock
        )
        stream = runtime.coordinator.stream(*key)
        stream.connected(quiet_at)
        stream.snapshot(ws_book)
        stream.connection_confirmed(quiet_at)
        session_id = runtime._new_stream_session(stream_key)
        runtime._bump_book_revision(key, checksum=checksum(ws_book))
        runtime._live_book_ready.add(key)
        runtime._risex_ws_book_sessions[key] = session_id
        runtime._trade_stream_ready.add(key)
        runtime.observations[key] = MarketObservation(
            adapter.market, volume, ws_book, old_quote,
            stream.health(clock.now()), trade_stream_ready=True,
        )
        revision = runtime._book_revisions[key]

        assert stream.health(clock.now()).data_quality is DataQuality.DEGRADED
        await runtime._market_observation(
            adapter.market, clock.now(), background=True,
            catalog_volumes={key: volume},
        )

        assert adapter.book_calls == 0
        assert adapter.funding_calls == 1
        assert runtime.coordinator.stream(*key).book() == ws_book
        assert runtime.observations[key].book == ws_book
        assert runtime.observations[key].health.data_quality is DataQuality.DEGRADED
        assert runtime.observations[key].volume == volume
        assert runtime.observations[key].funding is not old_quote
        assert runtime.observations[key].funding.observed_at == clock.now()
        assert runtime._stream_sessions[stream_key] == session_id
        assert runtime._stream_invalidation_revisions.get(key, 0) == 0
        assert runtime._book_revisions[key] == revision
        assert runtime._book_checksums[key] == checksum(ws_book)

        assert await runtime.apply_book_event(
            BookDelta(
                Venue.RISEX, symbol,
                ws_after_delta.bids, (), clock.now(),
                checksum=checksum(ws_after_delta),
            ),
            stream_session_id=session_id,
        )
        assert runtime.coordinator.stream(*key).book() == ws_after_delta
        assert repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence "
            "WHERE event_type='PUBLIC_BOOK_RESYNC_REQUIRED'"
        ).fetchone()[0] == 0


@pytest.mark.asyncio
async def test_extended_eof_persists_one_ordered_physical_socket_episode(tmp_path):
    clock = FakeClock()
    stop = asyncio.Event()
    session = ReconnectingSession(stop)

    async def no_delay(_seconds: float) -> None:
        await asyncio.sleep(0)

    with PaperRepository(tmp_path / "clean-ws-reconnect.db") as repository:
        runtime = PublicPaperRuntime(repository, adapters={}, clock=clock, sleep=no_delay)
        runtime._session = session
        runtime._stop_event = stop
        session_id = runtime._new_stream_session(
            (Venue.EXTENDED, "ABC-EXTENDED", "trade")
        )
        await runtime._extended_stream(
            ExtendedAdapter(None), "ABC-EXTENDED", "trade", session_id
        )
        await runtime.shutdown()
        lifecycle = repository.connection.execute(
            "SELECT recorded_at,event_type,venue,detail FROM runtime_evidence "
            "WHERE event_type IN "
            "('PUBLIC_SOCKET_DISCONNECTED','PUBLIC_SOCKET_RECONNECTED') "
            "ORDER BY evidence_id"
        ).fetchall()
    assert session.connections == 2
    assert {row["venue"] for row in lifecycle} == {"EXTENDED"}
    disconnected, reconnected = assert_socket_episode(lifecycle)
    assert disconnected["episode_id"] == reconnected["episode_id"]
    assert disconnected["episode_id"].startswith("EXTENDED:trade:")
    assert disconnected["stream_kind"] == reconnected["stream_kind"] == "trade"
    assert disconnected["market"] == reconnected["market"] == "ABC-EXTENDED"
    assert disconnected["cause"] == "SOCKET_CLOSED"
    assert disconnected["close_classification"] == "EOF"


@pytest.mark.asyncio
async def test_extended_error_frame_persists_one_transport_episode(tmp_path):
    clock = FakeClock()
    stop = asyncio.Event()
    session = ReconnectingSession(stop, outcomes=("error", "stop"))

    async def no_delay(_seconds: float) -> None:
        await asyncio.sleep(0)

    with PaperRepository(tmp_path / "error-ws-reconnect.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={}, clock=clock, sleep=no_delay,
        )
        runtime._session = session
        runtime._stop_event = stop
        session_id = runtime._new_stream_session(
            (Venue.EXTENDED, "ABC-EXTENDED", "trade")
        )
        await runtime._extended_stream(
            ExtendedAdapter(None), "ABC-EXTENDED", "trade", session_id
        )
        lifecycle = repository.connection.execute(
            "SELECT recorded_at,event_type,venue,detail FROM runtime_evidence "
            "WHERE event_type IN "
            "('PUBLIC_SOCKET_DISCONNECTED','PUBLIC_SOCKET_RECONNECTED') "
            "ORDER BY evidence_id"
        ).fetchall()
    assert session.connections == 2
    disconnected, reconnected = assert_socket_episode(lifecycle)
    assert disconnected["episode_id"] == reconnected["episode_id"]
    assert disconnected["cause"] == "SOCKET_CLOSED"
    assert disconnected["close_classification"] == "ERROR"


@pytest.mark.asyncio
async def test_extended_trade_sequence_is_monotonic_deduped_and_does_not_reconnect(tmp_path):
    clock = FakeClock()
    stop = asyncio.Event()
    timestamp_ms = str(int(NOW.timestamp() * 1000))
    trade = {
        "m": "ABC-EXTENDED", "p": "100", "q": "1", "T": timestamp_ms,
        "S": "SELL", "i": "trade-1", "tT": "TRADE",
    }

    websocket = TextWebSocket(stop, (
        {"seq": 10, "data": [trade]},
        {"seq": 12, "data": [{**trade, "i": "trade-2"}]},
        {"seq": 12, "data": [{**trade, "i": "duplicate-sequence"}]},
        {"seq": 11, "data": [{**trade, "i": "out-of-order"}]},
        {"seq": 13, "data": [{**trade, "i": "trade-3"}]},
    ))
    session = SingleWebSocketSession(websocket)
    delivered = []

    async def capture_trade(row) -> None:
        delivered.append(row)

    async def no_delay(_seconds: float) -> None:
        await asyncio.sleep(0)

    with PaperRepository(tmp_path / "logical-sequence-gap.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={}, clock=clock, sleep=no_delay,
        )
        runtime._session = session
        runtime._stop_event = stop
        runtime.deliver_trade = capture_trade
        session_id = runtime._new_stream_session(
            (Venue.EXTENDED, "ABC-EXTENDED", "trade")
        )
        await runtime._extended_stream(
            ExtendedAdapter(None), "ABC-EXTENDED", "trade", session_id
        )
        physical = repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence "
            "WHERE event_type LIKE 'PUBLIC_SOCKET_%'"
        ).fetchone()[0]
        discontinuities = repository.connection.execute(
            "SELECT detail FROM runtime_evidence "
            "WHERE event_type='PUBLIC_TRADE_SEQUENCE_DISCONTINUITY'"
        ).fetchall()
    assert session.connections == 1
    assert physical == 0
    assert [row.trade_event_key for row in delivered] == [
        "EXTENDED|ABC-EXTENDED|trade-1",
        "EXTENDED|ABC-EXTENDED|trade-2",
        "EXTENDED|ABC-EXTENDED|trade-3",
    ]
    assert len(discontinuities) == 1
    assert json.loads(discontinuities[0]["detail"]) == {
        "action": "ACCEPT_MONOTONIC",
        "classification": "FORWARD_GAP",
        "current_sequence": 12,
        "previous_sequence": 10,
        "stream": "trade",
        "symbol": "ABC-EXTENDED",
    }


@pytest.mark.asyncio
async def test_extended_trade_sequence_resets_for_each_physical_session(tmp_path):
    clock = FakeClock()
    stop = asyncio.Event()
    timestamp_ms = str(int(NOW.timestamp() * 1000))

    def payload(sequence, trade_id):
        return {
            "seq": sequence,
            "data": [{
                "m": "ABC-EXTENDED", "p": "100", "q": "1",
                "T": timestamp_ms, "S": "SELL", "i": trade_id,
                "tT": "TRADE",
            }],
        }

    class SessionWebSocket(TextWebSocket):
        def __init__(self, payloads, *, stop_on_eof):
            super().__init__(stop, payloads)
            self.stop_on_eof = stop_on_eof

        async def __anext__(self):
            if not self.messages:
                if self.stop_on_eof:
                    stop.set()
                raise StopAsyncIteration
            return SimpleNamespace(
                type=aiohttp.WSMsgType.TEXT,
                data=json.dumps(self.messages.pop(0)),
            )

    class TwoSession:
        connections = 0

        def ws_connect(self, *_args, **_kwargs):
            self.connections += 1
            if self.connections == 1:
                return SessionWebSocket((payload(100, "old-session"),), stop_on_eof=False)
            return SessionWebSocket((payload(1, "new-session"),), stop_on_eof=True)

    session = TwoSession()
    delivered = []

    async def capture_trade(row) -> None:
        delivered.append(row)

    async def no_delay(_seconds: float) -> None:
        await asyncio.sleep(0)

    with PaperRepository(tmp_path / "trade-session-reset.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={}, clock=clock, sleep=no_delay,
        )
        runtime._session = session
        runtime._stop_event = stop
        runtime.deliver_trade = capture_trade
        session_id = runtime._new_stream_session(
            (Venue.EXTENDED, "ABC-EXTENDED", "trade")
        )
        await runtime._extended_stream(
            ExtendedAdapter(None), "ABC-EXTENDED", "trade", session_id
        )
        discontinuities = repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence "
            "WHERE event_type='PUBLIC_TRADE_SEQUENCE_DISCONTINUITY'"
        ).fetchone()[0]
    assert session.connections == 2
    assert [row.trade_event_key for row in delivered] == [
        "EXTENDED|ABC-EXTENDED|old-session",
        "EXTENDED|ABC-EXTENDED|new-session",
    ]
    assert discontinuities == 0


@pytest.mark.asyncio
async def test_lighter_trade_snapshot_is_not_delivered_and_updates_require_new_nonce(
    tmp_path,
):
    clock = FakeClock()
    stop = asyncio.Event()
    websocket = LighterTextWebSocket(stop, (
        lighter_trade_message(
            message_type="subscribed/trade", nonce=50, trade_id=1000
        ),
        lighter_trade_message(
            message_type="update/trade", nonce=50, trade_id=1001
        ),
        lighter_trade_message(
            message_type="update/trade", nonce=49, trade_id=1002
        ),
        lighter_trade_message(
            message_type="update/trade", nonce=51, trade_id=1003
        ),
    ))
    session = SingleWebSocketSession(websocket)
    delivered = []

    async def capture_trade(row) -> None:
        delivered.append(row)

    async def no_delay(_seconds: float) -> None:
        await asyncio.sleep(0)

    adapter = LighterStreamAdapter(clock)
    with PaperRepository(tmp_path / "lighter-trade-sequence.db") as repository:
        runtime = PublicPaperRuntime(
            repository,
            adapters={Venue.LIGHTER: adapter},
            clock=clock,
            sleep=no_delay,
        )
        runtime._session = session
        runtime._stop_event = stop
        runtime.deliver_trade = capture_trade
        session_id = runtime._new_stream_session(
            (Venue.LIGHTER, "*", "combined")
        )
        await runtime._combined_stream(
            Venue.LIGHTER, adapter, ("ETH",), session_id
        )

    assert session.connections == 1
    assert [row.trade_event_key for row in delivered] == [
        "LIGHTER|ETH|1003",
    ]


@pytest.mark.asyncio
async def test_lighter_trade_sequence_resets_for_each_physical_session(tmp_path):
    clock = FakeClock()
    stop = asyncio.Event()

    class SessionWebSocket(LighterTextWebSocket):
        def __init__(self, payloads, *, stop_on_eof):
            super().__init__(stop, payloads)
            self.stop_on_eof = stop_on_eof

        async def __anext__(self):
            if not self.messages:
                if self.stop_on_eof:
                    stop.set()
                raise StopAsyncIteration
            return SimpleNamespace(
                type=aiohttp.WSMsgType.TEXT,
                data=json.dumps(self.messages.pop(0)),
            )

    first = SessionWebSocket(
        (
            lighter_trade_message(
                message_type="subscribed/trade", nonce=50, trade_id=2000
            ),
            lighter_trade_message(
                message_type="update/trade", nonce=51, trade_id=2001
            ),
        ),
        stop_on_eof=False,
    )
    second = SessionWebSocket(
        (
            lighter_trade_message(
                message_type="subscribed/trade", nonce=1, trade_id=3000
            ),
            lighter_trade_message(
                message_type="update/trade", nonce=2, trade_id=3001
            ),
        ),
        stop_on_eof=True,
    )

    class TwoSession:
        connections = 0

        def ws_connect(self, *_args, **_kwargs):
            self.connections += 1
            return (first, second)[self.connections - 1]

    session = TwoSession()
    delivered = []

    async def capture_trade(row) -> None:
        delivered.append(row)

    async def no_delay(_seconds: float) -> None:
        await asyncio.sleep(0)

    adapter = LighterStreamAdapter(clock)
    with PaperRepository(tmp_path / "lighter-trade-session-reset.db") as repository:
        runtime = PublicPaperRuntime(
            repository,
            adapters={Venue.LIGHTER: adapter},
            clock=clock,
            sleep=no_delay,
        )
        runtime._session = session
        runtime._stop_event = stop
        runtime.deliver_trade = capture_trade
        session_id = runtime._new_stream_session(
            (Venue.LIGHTER, "*", "combined")
        )
        await runtime._combined_stream(
            Venue.LIGHTER, adapter, ("ETH",), session_id
        )

    assert session.connections == 2
    assert [row.trade_event_key for row in delivered] == [
        "LIGHTER|ETH|2001",
        "LIGHTER|ETH|3001",
    ]


@pytest.mark.asyncio
async def test_extended_immediate_reconnect_failures_use_increasing_backoff(tmp_path):
    clock = FakeClock()
    stop = asyncio.Event()
    session = ReconnectingSession(
        stop, outcomes=("eof", "fail_context", "fail_context", "stop")
    )
    delays = []

    async def capture_delay(seconds: float) -> None:
        delays.append(seconds)
        await asyncio.sleep(0)

    with PaperRepository(tmp_path / "extended-backoff.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={}, clock=clock, sleep=capture_delay,
        )
        runtime._session = session
        runtime._stop_event = stop
        session_id = runtime._new_stream_session(
            (Venue.EXTENDED, "ABC-EXTENDED", "funding")
        )
        await runtime._extended_stream(
            ExtendedAdapter(None), "ABC-EXTENDED", "funding", session_id
        )
    assert session.connections == 4
    assert delays == [1, 2, 4]


@pytest.mark.asyncio
async def test_extended_book_eof_unifies_socket_and_resync_notification_episode(tmp_path):
    clock = FakeClock()
    stop = asyncio.Event()
    session = ReconnectingSession(stop, outcomes=("eof", "stop"))
    delivery = CaptureNotifications()
    outbox = NotificationOutbox(delivery)

    async def no_delay(_seconds: float) -> None:
        await asyncio.sleep(0)

    with PaperRepository(tmp_path / "book-ws-notification-reconnect.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={}, clock=clock, sleep=no_delay,
            notifications=outbox,
        )
        runtime._session = session
        runtime._stop_event = stop
        session_id = runtime._new_stream_session(
            (Venue.EXTENDED, "ABC-EXTENDED", "book")
        )
        await runtime._extended_stream(
            ExtendedAdapter(None), "ABC-EXTENDED", "book", session_id
        )
        evidence = repository.connection.execute(
            "SELECT recorded_at,event_type,venue,detail FROM runtime_evidence "
            "WHERE event_type IN "
            "('PUBLIC_SOCKET_DISCONNECTED','PUBLIC_BOOK_RESYNC_REQUIRED',"
            "'PUBLIC_SOCKET_RECONNECTED') ORDER BY evidence_id"
        ).fetchall()
        socket_rows = [
            row for row in evidence if row["event_type"].startswith("PUBLIC_SOCKET_")
        ]
        assert [row["event_type"] for row in evidence] == [
            "PUBLIC_SOCKET_DISCONNECTED", "PUBLIC_BOOK_RESYNC_REQUIRED",
            "PUBLIC_SOCKET_RECONNECTED",
        ]
        disconnected, reconnected = assert_socket_episode(socket_rows)
        assert disconnected["episode_id"] == reconnected["episode_id"]
        assert disconnected["stream_kind"] == "book"
        assert [row.kind for row in delivery.rows] == []
        assert outbox._active_outages == set()

        clock.advance(1)
        await runtime.mark_disconnected(
            Venue.EXTENDED, "ABC-EXTENDED", stream_kind="book",
            exception=ValueError("independent synthetic gap"),
            stream_session_id=runtime._stream_sessions[
                (Venue.EXTENDED, "ABC-EXTENDED", "book")
            ],
        )
        assert [row.kind for row in delivery.rows] == []
        assert outbox._active_outages == set()
        assert repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence "
            "WHERE event_type='PUBLIC_BOOK_RESYNC_REQUIRED'"
        ).fetchone()[0] == 2
        await runtime.shutdown()
    assert session.connections == 2


@pytest.mark.asyncio
async def test_combined_socket_uses_one_episode_for_sorted_market_set(tmp_path):
    clock = FakeClock()
    stop = asyncio.Event()
    session = ReconnectingSession(
        stop, outcomes=("eof", "fail_subscription", "stop")
    )
    symbols = ("XYZ-NADO", "ABC-NADO")
    adapter = CombinedFakeAdapter(
        Venue.NADO, clock, settlement_at=NOW + timedelta(minutes=5)
    )

    async def no_delay(_seconds: float) -> None:
        await asyncio.sleep(0)

    with PaperRepository(tmp_path / "combined-reconnect.db") as repository:
        runtime = PublicPaperRuntime(repository, adapters={}, clock=clock, sleep=no_delay)
        runtime._session = session
        runtime._stop_event = stop
        session_id = runtime._new_stream_session(
            (Venue.NADO, "*", "combined")
        )
        await runtime._combined_stream(
            Venue.NADO, adapter, symbols, session_id
        )
        lifecycle = repository.connection.execute(
            "SELECT recorded_at,event_type,venue,detail FROM runtime_evidence "
            "WHERE event_type IN "
            "('PUBLIC_SOCKET_DISCONNECTED','PUBLIC_SOCKET_RECONNECTED') "
            "ORDER BY evidence_id"
        ).fetchall()
    assert session.connections == 3
    assert {row["venue"] for row in lifecycle} == {"NADO"}
    disconnected, reconnected = assert_socket_episode(lifecycle)
    assert disconnected["episode_id"] == reconnected["episode_id"]
    assert disconnected["episode_id"].startswith("NADO:combined:")
    assert disconnected["stream_kind"] == reconnected["stream_kind"] == "combined"
    assert disconnected["markets"] == reconnected["markets"] == [
        "ABC-NADO", "XYZ-NADO",
    ]


@pytest.mark.asyncio
async def test_simple_combined_eof_then_reconnect_is_one_socket_episode(tmp_path):
    clock = FakeClock()
    stop = asyncio.Event()
    session = ReconnectingSession(stop, outcomes=("eof", "stop"))
    symbols = ("XYZ-NADO", "ABC-NADO")
    adapter = CombinedFakeAdapter(
        Venue.NADO, clock, settlement_at=NOW + timedelta(minutes=5)
    )
    delivery = CaptureNotifications()
    outbox = NotificationOutbox(delivery)

    async def no_delay(_seconds: float) -> None:
        await asyncio.sleep(0)

    with PaperRepository(tmp_path / "simple-combined-reconnect.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={}, clock=clock, sleep=no_delay,
            notifications=outbox,
        )
        runtime._session = session
        runtime._stop_event = stop
        session_id = runtime._new_stream_session(
            (Venue.NADO, "*", "combined")
        )
        await runtime._combined_stream(
            Venue.NADO, adapter, symbols, session_id
        )
        lifecycle = repository.connection.execute(
            "SELECT recorded_at,event_type,venue,detail FROM runtime_evidence "
            "WHERE event_type IN "
            "('PUBLIC_SOCKET_DISCONNECTED','PUBLIC_SOCKET_RECONNECTED') "
            "ORDER BY evidence_id"
        ).fetchall()

    assert session.connections == 2
    assert len(lifecycle) == 2
    assert lifecycle[0]["event_type"] == "PUBLIC_SOCKET_DISCONNECTED"
    assert lifecycle[1]["event_type"] == "PUBLIC_SOCKET_RECONNECTED"
    assert {row["venue"] for row in lifecycle} == {"NADO"}
    disconnected, reconnected = assert_socket_episode(lifecycle)
    assert disconnected["episode_id"] == reconnected["episode_id"]
    assert disconnected["stream_kind"] == reconnected["stream_kind"] == "combined"
    assert "stream_session_id" not in disconnected
    assert "stream_session_id" not in reconnected
    assert (
        disconnected["disconnected_stream_session_id"]
        == reconnected["disconnected_stream_session_id"]
    )
    assert reconnected["reconnected_stream_session_id"] == runtime._stream_sessions[
        Venue.NADO, "*", "combined"
    ].value
    assert (
        reconnected["reconnected_stream_session_id"]
        != disconnected["disconnected_stream_session_id"]
    )
    assert disconnected["markets"] == reconnected["markets"] == [
        "ABC-NADO", "XYZ-NADO",
    ]
    assert [row.kind for row in delivery.rows] == []
    assert outbox._active_outages == set()


@pytest.mark.asyncio
async def test_failed_extended_reconnect_attempts_do_not_duplicate_episode(tmp_path):
    clock = FakeClock()
    stop = asyncio.Event()
    session = ReconnectingSession(
        stop, outcomes=("eof", "fail_context", "fail_context", "stop")
    )

    async def no_delay(_seconds: float) -> None:
        await asyncio.sleep(0)

    with PaperRepository(tmp_path / "failed-reconnects.db") as repository:
        runtime = PublicPaperRuntime(repository, adapters={}, clock=clock, sleep=no_delay)
        runtime._session = session
        runtime._stop_event = stop
        session_id = runtime._new_stream_session(
            (Venue.EXTENDED, "ABC-EXTENDED", "funding")
        )
        await runtime._extended_stream(
            ExtendedAdapter(None), "ABC-EXTENDED", "funding", session_id
        )
        lifecycle = repository.connection.execute(
            "SELECT recorded_at,event_type,venue,detail FROM runtime_evidence "
            "WHERE event_type IN "
            "('PUBLIC_SOCKET_DISCONNECTED','PUBLIC_SOCKET_RECONNECTED') "
            "ORDER BY evidence_id"
        ).fetchall()
    assert session.connections == 4
    assert {row["venue"] for row in lifecycle} == {"EXTENDED"}
    disconnected, reconnected = assert_socket_episode(lifecycle)
    assert disconnected["episode_id"] == reconnected["episode_id"]
    assert disconnected["stream_kind"] == reconnected["stream_kind"] == "funding"
    assert disconnected["market"] == reconnected["market"] == "ABC-EXTENDED"


@pytest.mark.asyncio
async def test_healthy_live_stream_background_refresh_does_not_install_rest_book(tmp_path):
    clock = FakeClock()
    fakes = adapters(clock, settlement_at=NOW + timedelta(minutes=5))
    with PaperRepository(tmp_path / "healthy-stream-refresh.db") as repository:
        async with PublicPaperRuntime(repository, adapters=fakes, clock=clock) as runtime:
            await runtime.scan()
            symbol = fakes[Venue.NADO].market.venue_symbol
            runtime.mark_trade_stream_connected(Venue.NADO, symbol)
            before = fakes[Venue.NADO].calls.count("book")
            runtime._start_public_refresh()
            await runtime._refresh_task
    assert fakes[Venue.NADO].calls.count("book") == before


@pytest.mark.asyncio
async def test_extended_live_book_refresh_does_not_depend_on_trade_readiness(tmp_path):
    clock = FakeClock()
    extended = GatedExtendedAdapter(
        clock, settlement_at=NOW + timedelta(minutes=5)
    )
    with PaperRepository(tmp_path / "extended-live-book-refresh.db") as repository:
        async with PublicPaperRuntime(
            repository,
            adapters={
                Venue.RISEX: FakeAdapter(
                    Venue.RISEX, clock, settlement_at=NOW + timedelta(minutes=5)
                ),
                Venue.EXTENDED: extended,
                Venue.NADO: FakeAdapter(
                    Venue.NADO, clock, settlement_at=NOW + timedelta(minutes=5)
                ),
            },
            clock=clock,
        ) as runtime:
            await runtime.scan()
            symbol = extended.market.venue_symbol
            confirm_extended_stream(
                runtime, symbol, "book", clock.now(), data_ready=True
            )
            before = runtime.observations[Venue.EXTENDED, symbol]
            extended.calls.clear()
            runtime._start_public_refresh()
            assert runtime._refresh_task is not None
            await runtime._refresh_task
            after = runtime.observations[Venue.EXTENDED, symbol]

    assert extended.calls == ["funding"]
    assert (Venue.EXTENDED, symbol) not in runtime._trade_stream_ready
    assert after.book == before.book
    assert after.book is not None
    assert after.book.observed_at == before.book.observed_at


@pytest.mark.asyncio
async def test_extended_stream_registry_is_dynamic_deduplicated_and_lock_safe(tmp_path):
    clock = FakeClock()
    adapter = ExtendedAdapter(None)
    original = FakeAdapter(
        Venue.EXTENDED, clock, settlement_at=NOW + timedelta(minutes=5)
    ).market
    added = replace(original, canonical_asset="XYZ", venue_symbol="XYZ-EXTENDED")
    with PaperRepository(tmp_path / "dynamic-extended.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.EXTENDED: adapter}, clock=clock
        )
        runtime._session = object()
        runtime._stop_event = asyncio.Event()
        runtime.markets[Venue.EXTENDED] = (original,)
        risex_original = replace(
            original, venue=Venue.RISEX, venue_symbol="ABC-RISEX"
        )
        risex_added = replace(
            added, venue=Venue.RISEX, venue_symbol="XYZ-RISEX"
        )
        runtime.markets[Venue.RISEX] = (risex_original, risex_added)
        for market in (original, added, risex_original, risex_added):
            runtime.volumes[(market.venue, market.venue_symbol)] = MarketVolume(
                market.venue, market.venue_symbol, D("1000000"), NOW, "synthetic"
            )
        await runtime._reconcile_extended_streams()
        first = dict(runtime._stream_tasks)
        await runtime._reconcile_extended_streams()
        assert runtime._stream_tasks == first
        runtime.broker = SimpleNamespace(state=SimpleNamespace(
            lifecycle_state=LifecycleState.FLAT,
            order=SimpleNamespace(route_plan=SimpleNamespace(
                hedge_venue=Venue.EXTENDED, hedge_market=original,
            )),
        ))
        runtime.markets[Venue.EXTENDED] = (added,)
        await runtime._reconcile_extended_streams()
        symbols = {
            symbol for venue, symbol, _ in runtime._stream_tasks
            if venue is Venue.EXTENDED
        }
        extended_symbols = set(runtime._extended_stream_symbols)
        runtime.broker = None
        await runtime.shutdown()
        socket_lifecycle_count = repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence "
            "WHERE event_type IN "
            "('PUBLIC_SOCKET_DISCONNECTED','PUBLIC_SOCKET_RECONNECTED')"
        ).fetchone()[0]
    assert symbols == {"*"}
    assert extended_symbols == {
        original.venue_symbol, added.venue_symbol,
    }
    assert socket_lifecycle_count == 0


@pytest.mark.asyncio
async def test_public_refresh_starts_combined_stream_once_after_catalog_recovery(tmp_path):
    clock = FakeClock()
    fakes = adapters(clock, settlement_at=NOW + timedelta(minutes=5))
    fakes[Venue.NADO].available = False
    with PaperRepository(tmp_path / "dynamic-combined.db") as repository:
        runtime = PublicPaperRuntime(repository, adapters=fakes, clock=clock)
        await runtime.scan()
        runtime._session = object()
        runtime._stop_event = asyncio.Event()

        async def stable_combined(
            _venue, _adapter, _symbols, stream_session_id
        ):
            assert runtime._owns_stream_session(
                (_venue, "*", "combined"), stream_session_id
            )
            await runtime._stop_event.wait()

        runtime._combined_stream = stable_combined
        await runtime._reconcile_streams()
        assert not any(key[0] is Venue.NADO for key in runtime._stream_tasks)

        fakes[Venue.NADO].available = True
        runtime._start_background_catalog_refresh(
            include_extended_universe=False
        )
        assert runtime._extended_universe_task is not None
        await runtime._extended_universe_task
        await runtime._refresh_public_data()
        first = dict(runtime._stream_tasks)
        assert sum(
            key[0] is Venue.NADO and key[2] == "combined" for key in first
        ) == 1
        await runtime._refresh_public_data()
        assert runtime._stream_tasks == first
        await runtime.shutdown()
    assert {(key[0], key[2]) for key in first} == {
        (Venue.RISEX, "combined"), (Venue.NADO, "combined")
    }


@pytest.mark.asyncio
async def test_health_confirmation_silence_cancels_active_entry(tmp_path):
    clock = FakeClock()
    with PaperRepository(tmp_path / "health-silence.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=adapters(clock, settlement_at=NOW + timedelta(seconds=120)), clock=clock,
        ) as runtime:
            await activate_with_live_streams(runtime, clock)
            order = runtime.broker.state.order
            market = order.route_plan.risex_market
            runtime._new_stream_session((market.venue, "*", "combined"))
            runtime.mark_trade_stream_connected(
                market.venue, market.venue_symbol
            )
            runtime.next_health_check_at = clock.now() + timedelta(seconds=10)
            clock.advance(26)
            await runtime.tick()
            state = runtime.readiness[market.venue]
    assert runtime.broker is None
    assert state.detail.startswith("PUBLIC_STREAM_DISCONNECTED:health:StreamGap")


@pytest.mark.asyncio
async def test_official_applied_settlement_replaces_estimate_and_report_has_assumptions(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(seconds=120)
    fakes = adapters(clock, settlement_at=target)
    with PaperRepository(tmp_path / "settlement.db") as repository:
        async with PublicPaperRuntime(repository, adapters=fakes, clock=clock) as runtime:
            await activate_with_live_streams(runtime, clock)
            clock.advance(1)
            order = runtime.broker.state.order
            runtime.mark_trade_stream_connected(order.venue, order.canonical_market)
            await runtime.deliver_trade(maker_trade(runtime, clock.now(), "settlement-entry"))
            pending = runtime.lifecycle.snapshot.settlements[0]
            estimated = FundingSettlement(
                pending.venue,
                pending.canonical_market,
                pending.settlement_at,
                SettlementStatus.ESTIMATED,
                D("3"),
            )
            applied = FundingSettlement(
                pending.venue,
                pending.canonical_market,
                pending.settlement_at,
                SettlementStatus.APPLIED_RATE,
                D("4"),
            )
            await runtime.deliver_settlement(estimated)
            await runtime.deliver_settlement(applied)
            rows = repository.connection.execute(
                "SELECT status,cash_usd FROM funding_settlements WHERE venue=? AND canonical_market=? AND settlement_at=?",
                (pending.venue.value, pending.canonical_market, pending.settlement_at.isoformat()),
            ).fetchall()
            report = repository.report(as_of=clock.now())
    assert [(row["status"], row["cash_usd"]) for row in rows] == [("APPLIED_RATE", "4")]
    for flag in (
        "risex_contract_and_quantity_are_paper_assumptions",
        "risex_funding_eligibility_is_a_paper_assumption",
        "risex_next_rate_estimate_is_a_paper_assumption",
        "risex_assumed_funding_is_not_official_applied_funding",
    ):
        assert report["assumption_flags"][flag] is True


@pytest.mark.asyncio
async def test_extended_applied_record_preserves_future_quote_and_is_exact_idempotent(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(seconds=120)
    fakes = adapters(clock, settlement_at=target)
    with PaperRepository(tmp_path / "extended-applied-separation.db") as repository:
        async with PublicPaperRuntime(repository, adapters=fakes, clock=clock) as runtime:
            await runtime.scan()
            confirm_public_streams(runtime, clock.now())
            symbol = fakes[Venue.EXTENDED].market.venue_symbol
            expected = runtime.observations[Venue.EXTENDED, symbol].funding
            assert expected is not None and expected.quality is FundingQuality.PREDICTED
            await runtime._apply_extended_funding_record(FundingSettlement(
                Venue.EXTENDED, symbol, target,
                SettlementStatus.UNRESOLVED, None,
            ))
            assert runtime.observations[Venue.EXTENDED, symbol].funding == expected
            await runtime.scan(refresh=False, scan_kind="FULL")
            plans = [
                row for row in runtime.last_scan.evaluations
                if row.hedge_venue is Venue.EXTENDED
            ]
            assert plans and plans[0].planned_maker_net_pnl_usd is not None
            assert not {
                "FUNDING_ELIGIBILITY_UNKNOWN", "FUNDING_OPEN_TIME_MISMATCH",
                "TARGET_CYCLE_ELAPSED",
            }.intersection(plans[0].no_trade_reasons)
            assert repository.connection.execute(
                "SELECT COUNT(*) FROM funding_settlements WHERE venue='EXTENDED'"
            ).fetchone()[0] == 0

            await activate_with_live_streams(runtime, clock)
            clock.advance(1)
            order = runtime.broker.state.order
            runtime.mark_trade_stream_connected(order.venue, order.canonical_market)
            await runtime.deliver_trade(maker_trade(runtime, clock.now(), "extended-entry"))
            required = next(
                row for row in runtime.lifecycle.snapshot.settlements
                if row.venue is Venue.EXTENDED
            )
            mismatch = replace(required, settlement_at=required.settlement_at + timedelta(seconds=1))
            await runtime._apply_extended_funding_record(mismatch)
            await runtime._apply_extended_funding_record(replace(
                required, status=SettlementStatus.UNRESOLVED, cash_usd=None,
            ))
            await runtime._apply_extended_funding_record(replace(
                required, status=SettlementStatus.UNRESOLVED, cash_usd=None,
            ))
            rows = repository.connection.execute(
                "SELECT settlement_at,status,cash_usd FROM funding_settlements "
                "WHERE venue='EXTENDED' ORDER BY settlement_at"
            ).fetchall()
    assert [(row["settlement_at"], row["status"], row["cash_usd"]) for row in rows] == [
        (required.settlement_at.isoformat(), "UNRESOLVED", None)
    ]


@pytest.mark.parametrize("stale_kind", ("book", "trade", "funding"))
@pytest.mark.asyncio
async def test_extended_health_is_per_socket_and_stale_restart_has_one_episode(
    tmp_path, stale_kind,
):
    clock = FakeClock()
    symbol = "ABC-EXTENDED"
    with PaperRepository(tmp_path / "extended-health-isolation.db") as repository:
        runtime = PublicPaperRuntime(repository, adapters={}, clock=clock)
        runtime._stop_event = asyncio.Event()
        for kind in ("book", "trade", "funding"):
            confirm_extended_stream(runtime, symbol, kind, clock.now(), data_ready=True)
        runtime._extended_confirmed_at[symbol, stale_kind] = clock.now() - timedelta(seconds=26)

        async def waiting_stream():
            await runtime._stop_event.wait()

        tasks = {
            kind: asyncio.create_task(waiting_stream())
            for kind in ("book", "trade", "funding")
        }
        for kind, task in tasks.items():
            runtime._stream_tasks[Venue.EXTENDED, symbol, kind] = task

        restarted = []
        runtime._start_extended_stream = lambda _symbol, kind: restarted.append(kind)
        await runtime._check_extended_health(clock.now())
        recovery_owner = runtime._extended_health_recovery_task
        assert recovery_owner is not None
        await recovery_owner
        assert restarted == [stale_kind]
        assert tasks[stale_kind].cancelled()
        for kind in ("book", "trade", "funding"):
            data_kind = "applied_funding" if kind == "funding" else kind
            available = runtime.component_readiness[Venue.EXTENDED][
                f"{data_kind}:{symbol}"
            ].available
            connected = runtime.component_readiness[Venue.EXTENDED][f"connection_{kind}:{symbol}"].available
            assert available is (kind != stale_kind)
            assert connected is (kind != stale_kind)
        assert not any(
            name.startswith("connection_combined")
            for name in runtime.component_readiness[Venue.EXTENDED]
        )
        confirm_extended_stream(runtime, symbol, stale_kind, clock.now(), data_ready=True)
        runtime._watchdog_restarted(
            (Venue.EXTENDED, stale_kind, (symbol,)), at=clock.now()
        )
        replacement = asyncio.create_task(waiting_stream())
        runtime._stream_tasks[Venue.EXTENDED, symbol, stale_kind] = replacement
        tasks[stale_kind] = replacement
        runtime._extended_confirmed_at[symbol, stale_kind] = (
            clock.now() - timedelta(seconds=26)
        )
        await runtime._check_extended_health(clock.now())
        recovery_owner = runtime._extended_health_recovery_task
        assert recovery_owner is not None
        await recovery_owner
        confirm_extended_stream(runtime, symbol, stale_kind, clock.now(), data_ready=True)
        runtime._watchdog_restarted(
            (Venue.EXTENDED, stale_kind, (symbol,)), at=clock.now()
        )
        stale_data_kind = (
            "applied_funding" if stale_kind == "funding" else stale_kind
        )
        assert runtime.component_readiness[Venue.EXTENDED][
            f"{stale_data_kind}:{symbol}"
        ].available
        assert runtime.component_readiness[Venue.EXTENDED][f"connection_{stale_kind}:{symbol}"].available
        rows = repository.connection.execute(
            "SELECT event_type,detail FROM runtime_evidence WHERE event_type LIKE "
            "'PUBLIC_STREAM_%' ORDER BY evidence_id"
        ).fetchall()
        physical_count = repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence WHERE event_type LIKE "
            "'PUBLIC_SOCKET_%'"
        ).fetchone()[0]
        runtime._stop_event.set()
        for task in tasks.values():
            task.cancel()
        await asyncio.gather(*tasks.values(), return_exceptions=True)
    assert [row["event_type"] for row in rows] == [
        "PUBLIC_STREAM_CONFIRMATION_STALE", "PUBLIC_STREAM_RESTARTED",
        "PUBLIC_STREAM_CONFIRMATION_STALE", "PUBLIC_STREAM_RESTARTED",
    ]
    assert physical_count == 0
    assert json.loads(rows[0]["detail"])["episode_id"] == json.loads(rows[1]["detail"])["episode_id"]
    assert json.loads(rows[2]["detail"])["episode_id"] == json.loads(rows[3]["detail"])["episode_id"]
    assert json.loads(rows[0]["detail"])["episode_id"] != json.loads(rows[2]["detail"])["episode_id"]
    stale_detail = json.loads(rows[0]["detail"])
    assert {
        "stream_identity", "last_confirmation", "detected_at", "stale_age",
        "restart_reason",
    } <= set(stale_detail)
    assert stale_detail["restart_reason"] == "CONFIRMATION_STALE"


@pytest.mark.asyncio
async def test_extended_health_noncurrent_request_clears_watchdog_episode(
    tmp_path,
):
    clock = FakeClock()
    symbol = "ABC-EXTENDED"
    key = (Venue.EXTENDED, symbol, "book")
    with PaperRepository(tmp_path / "extended-health-noncurrent.db") as repository:
        runtime = PublicPaperRuntime(repository, adapters={}, clock=clock)
        runtime._stop_event = asyncio.Event()
        stream_task = asyncio.create_task(runtime._stop_event.wait())
        runtime._stream_tasks[key] = stream_task
        old_session = runtime._new_stream_session(key)
        runtime._extended_confirmed_at[symbol, "book"] = (
            clock.now() - timedelta(seconds=26)
        )
        original_disconnect = runtime.mark_disconnected

        async def superseding_disconnect(venue, market, **kwargs):
            runtime._new_stream_session(key)
            await original_disconnect(venue, market, **kwargs)

        runtime.mark_disconnected = superseding_disconnect
        restarted = []
        runtime._start_extended_stream = lambda _symbol, kind: restarted.append(kind)
        owner = None
        try:
            await runtime._check_extended_health(clock.now())
            owner = runtime._extended_health_recovery_task
            assert owner is not None
            await owner
            assert runtime._stream_sessions[key] != old_session
            assert restarted == []
            assert runtime._pending_watchdog_episodes == {}
            assert [row["event_type"] for row in repository.connection.execute(
                "SELECT event_type FROM runtime_evidence ORDER BY evidence_id"
            ).fetchall()] == ["PUBLIC_STREAM_CONFIRMATION_STALE"]
        finally:
            runtime._request_stop("STOP_EVENT")
            if owner is not None and not owner.done():
                owner.cancel()
                await asyncio.gather(owner, return_exceptions=True)
            stream_task.cancel()
            await asyncio.gather(stream_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_extended_health_wave_coalesces_and_preserves_full_deadline(
    tmp_path,
):
    clock = FakeClock(NOW - timedelta(seconds=60))
    target = NOW + timedelta(minutes=5)
    fakes = adapters(clock, settlement_at=target)
    blocked = GatedAdapter(Venue.RISEX, clock, settlement_at=target)
    fakes[Venue.RISEX] = blocked
    wave_symbols = tuple(
        f"WAVE-{index:02d}-EXTENDED" for index in range(45)
    )
    release_recovery = asyncio.Event()
    all_recoveries_started = asyncio.Event()
    recovery_arrivals = 0
    stop_streams = asyncio.Event()
    restarted: list[str] = []

    with PaperRepository(tmp_path / "extended-health-wave.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            await runtime.scan()
            blocked.block_funding = True
            runtime._stop_event = asyncio.Event()
            runtime.accepting_entries = False
            for symbol in wave_symbols:
                key = (Venue.EXTENDED, symbol, "book")
                task = asyncio.create_task(stop_streams.wait())
                runtime._stream_tasks[key] = task
                session_id = runtime._new_stream_session(key)
                runtime._extended_confirmed_at[symbol, "book"] = (
                    NOW - timedelta(seconds=26)
                )
                assert session_id.value > 0

            async def gated_disconnect(*args, **kwargs):
                nonlocal recovery_arrivals
                recovery_arrivals += 1
                if recovery_arrivals == len(wave_symbols):
                    all_recoveries_started.set()
                await all_recoveries_started.wait()
                await release_recovery.wait()
                return await original_disconnect(*args, **kwargs)

            original_disconnect = runtime.mark_disconnected
            runtime.mark_disconnected = gated_disconnect
            def start_replacement(symbol: str, kind: str) -> None:
                restarted.append(f"{symbol}:{kind}")
                replacement_key = (Venue.EXTENDED, symbol, kind)
                replacement_session = runtime._new_stream_session(
                    replacement_key
                )
                runtime._confirm_extended_stream(
                    symbol, kind, clock.now(), data_ready=True,
                    stream_session_id=replacement_session,
                )
                runtime._watchdog_restarted(
                    (Venue.EXTENDED, kind, (symbol,)), at=clock.now()
                )

            runtime._start_extended_stream = start_replacement
            runtime.next_health_check_at = NOW
            runtime.next_full_scan_at = NOW

            await asyncio.wait_for(runtime.tick(NOW), timeout=1)
            await asyncio.wait_for(
                all_recoveries_started.wait(), timeout=1
            )
            await asyncio.wait_for(blocked.request_started.wait(), timeout=1)
            refresh_owner = runtime._refresh_task
            assert refresh_owner is not None and not refresh_owner.done()
            assert runtime._extended_health_recovery_task is not None
            health_owner = runtime._extended_health_recovery_task
            assert len(runtime._extended_health_recovery_requests) == len(
                wave_symbols
            )

            await asyncio.gather(*(
                runtime._check_extended_health(NOW)
                for _ in range(3)
            ))
            assert runtime._extended_health_recovery_task is health_owner
            assert len(runtime._extended_health_recovery_requests) == len(
                wave_symbols
            )

            deadline = runtime._pending_full_deadline_at
            assert deadline is not None
            clock.value = deadline + timedelta(seconds=1)
            await asyncio.wait_for(runtime.tick(clock.now()), timeout=1)
            blocked_rows = repository.connection.execute(
                "SELECT detail FROM runtime_evidence "
                "WHERE event_type='PUBLIC_SCAN_BLOCKED'"
            ).fetchall()
            full_rows = repository.connection.execute(
                "SELECT detail FROM runtime_evidence "
                "WHERE event_type='PUBLIC_SCAN' "
                "AND json_extract(detail,'$.scan_kind')='FULL'"
            ).fetchall()
            assert len(blocked_rows) == 1
            blocked_detail = json.loads(blocked_rows[0]["detail"])
            assert blocked_detail == {
                "kind": "full",
                "reason": "PUBLIC_REFRESH_DEADLINE_EXCEEDED",
                "scheduled_at": NOW.isoformat(),
                "deadline_at": deadline.isoformat(),
                "completed": False,
                "catalog_generation": runtime._catalog_generation,
            }
            assert full_rows == []
            assert runtime.last_scan is not None
            assert runtime.last_scan.logical_at == NOW - timedelta(seconds=60)
            assert blocked.cancelled

            release_recovery.set()
            await asyncio.wait_for(health_owner, timeout=1)
            await asyncio.sleep(0)
            assert sorted(restarted) == [
                f"{symbol}:book" for symbol in sorted(wave_symbols)
            ]
            restart_rows = repository.connection.execute(
                "SELECT detail FROM runtime_evidence "
                "WHERE event_type='PUBLIC_STREAM_RESTARTED' "
                "ORDER BY evidence_id"
            ).fetchall()
            assert sorted(
                json.loads(row["detail"])["market"] for row in restart_rows
            ) == list(sorted(wave_symbols))
            assert runtime._extended_health_recovery_task is None
            assert runtime._extended_health_recovery_requests == {}
            assert all(task.done() for task in runtime._retired_stream_tasks)

            runtime._request_stop("STOP_EVENT")
            await asyncio.wait_for(runtime.shutdown(), timeout=1)
            assert blocked.cancelled
            assert repository.connection.execute(
                "PRAGMA integrity_check"
            ).fetchone()[0] == "ok"


@pytest.mark.asyncio
async def test_extended_socket_burst_yields_due_full_tick_during_45_market_wave(
    tmp_path,
):
    clock = FakeClock(NOW)
    symbols = tuple(f"WAVE-{index:02d}-EXTENDED" for index in range(45))
    burst_per_socket = 256
    processed = 0
    refresh_started_at: list[int] = []

    class BurstSocket:
        def __init__(self, key):
            self.key = key
            self.remaining = burst_per_socket

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return None

        def __aiter__(self):
            return self

        async def __anext__(self):
            nonlocal processed
            if self.remaining:
                self.remaining -= 1
                processed += 1
                return SimpleNamespace(type=aiohttp.WSMsgType.PONG, data=None)
            # Make this bounded wave finish without entering a reconnect loop.
            runtime._new_stream_session(self.key)
            raise StopAsyncIteration

        async def ping(self):
            return None

    class BurstSession:
        def __init__(self):
            self.sockets = [
                BurstSocket((Venue.EXTENDED, symbol, "book"))
                for symbol in symbols
            ]

        def ws_connect(self, *_args, **_kwargs):
            return self.sockets.pop(0)

    with PaperRepository(tmp_path / "extended-burst-cadence.db") as repository:
        runtime = PublicPaperRuntime(
            repository,
            adapters={Venue.EXTENDED: ExtendedAdapter(None)},
            clock=clock,
        )
        readiness_writes = 0
        original_set_venue_readiness = repository.set_venue_readiness

        def count_readiness_writes(**kwargs):
            nonlocal readiness_writes
            readiness_writes += 1
            return original_set_venue_readiness(**kwargs)

        repository.set_venue_readiness = count_readiness_writes
        runtime._session = BurstSession()
        runtime._stop_event = asyncio.Event()
        runtime.last_scan = SimpleNamespace(logical_at=NOW)
        runtime.next_full_scan_at = NOW
        runtime.next_health_check_at = NOW + timedelta(hours=1)
        runtime._startup_gate_satisfied = True
        runtime.accepting_entries = False

        refresh_gate = asyncio.Event()
        refresh_owner = None

        def start_refresh():
            nonlocal refresh_owner
            refresh_started_at.append(processed)
            refresh_owner = asyncio.create_task(refresh_gate.wait())
            runtime._refresh_task = refresh_owner

        runtime._start_public_refresh = start_refresh
        runtime._start_background_catalog_refresh = lambda **_kwargs: None

        stream_tasks = []
        for symbol in symbols:
            key = (Venue.EXTENDED, symbol, "book")
            session_id = runtime._new_stream_session(key)
            task = asyncio.create_task(
                runtime._extended_stream(
                    runtime.adapters[Venue.EXTENDED], symbol, "book", session_id
                )
            )
            runtime._stream_tasks[key] = task
            stream_tasks.append(task)
        tick = asyncio.create_task(runtime.tick(NOW))

        try:
            await asyncio.gather(*stream_tasks, tick)
            deadline_count = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence "
                "WHERE event_type='PUBLIC_SCAN_DEADLINE' "
                "AND json_extract(detail,'$.kind')='full'"
            ).fetchone()[0]
            assert deadline_count == 1
            assert len(refresh_started_at) == 1
            assert 0 < refresh_started_at[0] < len(symbols) * burst_per_socket
            assert processed == len(symbols) * burst_per_socket
            assert readiness_writes <= len(symbols) * 2
        finally:
            runtime._request_stop("STOP_EVENT")
            await runtime.shutdown()


@pytest.mark.asyncio
async def test_extended_health_recovery_stop_is_bounded_and_cleans_owner(
    tmp_path,
):
    clock = FakeClock()
    target = NOW + timedelta(minutes=5)
    fakes = adapters(clock, settlement_at=target)
    symbol = fakes[Venue.EXTENDED].market.venue_symbol
    key = (Venue.EXTENDED, symbol, "book")
    recovery_entered = asyncio.Event()
    stop_stream = asyncio.Event()

    with PaperRepository(tmp_path / "extended-health-stop.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            await runtime.scan()
            runtime._stop_event = asyncio.Event()
            stream_task = asyncio.create_task(stop_stream.wait())
            runtime._stream_tasks[key] = stream_task
            session_id = runtime._new_stream_session(key)
            runtime._extended_confirmed_at[symbol, "book"] = (
                clock.now() - timedelta(seconds=26)
            )

            async def blocked_disconnect(*args, **kwargs):
                recovery_entered.set()
                await asyncio.Event().wait()

            runtime.mark_disconnected = blocked_disconnect
            check = asyncio.create_task(runtime._check_extended_health(clock.now()))
            await check
            owner = runtime._extended_health_recovery_task
            assert owner is not None and not owner.done()
            await asyncio.wait_for(recovery_entered.wait(), timeout=1)

            runtime._request_stop("SIGTERM")
            started = time.monotonic()
            await asyncio.wait_for(runtime.shutdown(), timeout=1)
            elapsed = time.monotonic() - started

            evidence = repository.connection.execute(
                "SELECT event_type FROM runtime_evidence ORDER BY evidence_id"
            ).fetchall()
            assert elapsed < 1
            assert owner.done() and owner.cancelled()
            assert stream_task.done() and stream_task.cancelled()
            assert runtime._extended_health_recovery_task is None
            assert runtime._extended_health_recovery_requests == {}
            assert not runtime._retired_stream_tasks
            assert evidence[-1]["event_type"] == "STOPPED_SAFE"
            assert repository.connection.execute(
                "PRAGMA integrity_check"
            ).fetchone()[0] == "ok"


@pytest.mark.asyncio
async def test_extended_transport_wave_defers_rest_fanout_and_keeps_all_routes(
    tmp_path,
):
    clock = FakeClock()
    target = NOW + timedelta(minutes=5)
    asset_count = 45
    risex = ManyFakeAdapter(
        Venue.RISEX, clock, settlement_at=target, asset_count=asset_count
    )

    class ManyExtended(GatedExtendedAdapter):
        def __init__(self) -> None:
            super().__init__(clock, settlement_at=target)
            self.rows = tuple(
                replace(
                    self.market,
                    canonical_asset=f"A{index}",
                    venue_symbol=f"A{index}-EXTENDED",
                )
                for index in range(asset_count)
            )

        async def fetch_catalog(self):
            self.catalog_calls += 1
            return self.rows, tuple(
                MarketVolume(
                    Venue.EXTENDED,
                    market.venue_symbol,
                    D("1000000"),
                    clock.now(),
                    "official-shaped",
                )
                for market in self.rows
            )

        async def fetch_required_catalog(self, venue_symbols):
            selected = tuple(
                market for market in self.rows
                if market.venue_symbol in set(venue_symbols)
            )
            return selected, tuple(
                MarketVolume(
                    Venue.EXTENDED,
                    market.venue_symbol,
                    D("1000000"),
                    clock.now(),
                    "official-shaped",
                )
                for market in selected
            )

        async def fetch_book(self, venue_symbol):
            self.calls.append("book")
            return OrderBook(
                Venue.EXTENDED,
                venue_symbol,
                (BookLevel(D("99"), D("20")),),
                (BookLevel(D("101"), D("20")),),
                clock.now(),
                None,
            )

        async def fetch_funding_quote(self, market, *, assumed_open_at):
            self.calls.append("funding")
            return FundingCashQuote(
                Venue.EXTENDED,
                market.venue_symbol,
                clock.now(),
                assumed_open_at,
                target,
                FundingQuality.PREDICTED,
                FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT,
                True,
                D("5"),
                D("5"),
                "official-shaped",
            )

    extended = ManyExtended()
    stop = asyncio.Event()
    with PaperRepository(tmp_path / "extended-transport-wave-refresh.db") as repository:
        async with PublicPaperRuntime(
            repository,
            adapters={Venue.RISEX: risex, Venue.EXTENDED: extended},
            clock=clock,
        ) as runtime:
            await runtime.scan()
            funding_before = {
                market.venue_symbol: runtime.observations[
                    Venue.EXTENDED, market.venue_symbol
                ].funding
                for market in extended.rows
            }
            extended.calls.clear()
            runtime._stop_event = stop
            for market in extended.rows:
                for kind in ("book", "trade", "funding"):
                    key = (Venue.EXTENDED, market.venue_symbol, kind)
                    runtime._stream_tasks[key] = asyncio.create_task(stop.wait())
                    session_id = runtime._new_stream_session(key)
                    runtime._socket_disconnected(
                        (Venue.EXTENDED, kind, (market.venue_symbol,)),
                        at=clock.now(),
                        stream_session_id=session_id,
                    )
                    await runtime.mark_disconnected(
                        Venue.EXTENDED,
                        market.venue_symbol,
                        at=clock.now(),
                        stream_kind=kind,
                        stream_session_id=session_id,
                    )

            await runtime._refresh_public_data()
            result = await runtime.scan(
                refresh=False, scan_kind="FULL", scheduled_at=clock.now()
            )
            deferred = repository.connection.execute(
                "SELECT detail FROM runtime_evidence "
                "WHERE event_type='PUBLIC_MARKET_OBSERVATION_DEFERRED'"
            ).fetchall()
            full = repository.connection.execute(
                "SELECT detail FROM runtime_evidence "
                "WHERE event_type='PUBLIC_SCAN' "
                "AND json_extract(detail,'$.scan_kind')='FULL'"
            ).fetchall()
            blocked = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence "
                "WHERE event_type='PUBLIC_SCAN_BLOCKED'"
            ).fetchone()[0]
            runtime._request_stop("STOP_EVENT")
            await runtime.shutdown()

    assert extended.calls == []
    assert len(deferred) == asset_count
    assert all(
        json.loads(row[0])["components"] == ["book", "trade", "funding"]
        for row in deferred
    )
    assert len(result["routes"]) == asset_count * 2
    assert len(full) == 1
    assert blocked == 0
    assert all(
        runtime.observations[Venue.EXTENDED, market.venue_symbol].book is None
        and runtime.observations[
            Venue.EXTENDED, market.venue_symbol
        ].funding == funding_before[market.venue_symbol]
        for market in extended.rows
    )
    assert all(
        "BOOK_UNHEALTHY" in row["blockers"]
        and "TRADE_STREAM_UNHEALTHY" in row["blockers"]
        and "FUNDING_STREAM_UNHEALTHY" in row["blockers"]
        for row in result["routes"]
    )


@pytest.mark.asyncio
async def test_shutdown_drains_only_owned_extended_work_during_health_wave(
    tmp_path,
):
    clock = FakeClock()
    symbol = "ACTIVE-EXTENDED"
    resync_symbol = "RESYNC-EXTENDED"
    with PaperRepository(tmp_path / "extended-health-all-owners-stop.db") as repository:
        runtime = PublicPaperRuntime(
            repository,
            adapters={Venue.EXTENDED: ExtendedAdapter(None)},
            clock=clock,
        )
        class Session:
            closed = False

            async def close(self):
                self.closed = True

        session = Session()
        runtime._session = session
        runtime._stop_event = asyncio.Event()
        started: dict[str, asyncio.Event] = {}
        health_started = asyncio.Event()
        started["health"] = health_started

        async def owned_wait(name: str) -> None:
            entered = started.setdefault(name, asyncio.Event())
            entered.set()
            await asyncio.Event().wait()

        stream_key = (Venue.EXTENDED, symbol, "book")
        stream_task = asyncio.create_task(owned_wait("stream"))
        runtime._stream_tasks[stream_key] = stream_task
        stream_session_id = runtime._new_stream_session(stream_key)
        runtime._extended_confirmed_at[symbol, "book"] = (
            clock.now() - timedelta(seconds=26)
        )

        resync_key = (Venue.EXTENDED, resync_symbol)
        resync_session_id = runtime._new_stream_session(
            (Venue.EXTENDED, resync_symbol, "book")
        )
        resync = runtime._new_recovery_episode(resync_key, resync_session_id)
        resync_task = asyncio.create_task(owned_wait("resync"))
        resync.task = resync_task

        refresh_task = asyncio.create_task(owned_wait("refresh"))
        catalog_task = asyncio.create_task(owned_wait("catalog"))
        runtime._refresh_task = refresh_task
        runtime._extended_universe_task = catalog_task

        async def blocked_disconnect(*args, **kwargs):
            health_started.set()
            await asyncio.Event().wait()

        runtime.mark_disconnected = blocked_disconnect
        await runtime._check_extended_health(clock.now())
        health_owner = runtime._extended_health_recovery_task
        assert health_owner is not None
        await asyncio.gather(*(event.wait() for event in started.values()))
        await health_started.wait()
        unrelated = asyncio.create_task(asyncio.Event().wait())

        runtime._request_stop("SIGINT")
        started_at = time.monotonic()
        await asyncio.wait_for(runtime.shutdown(), timeout=1)
        elapsed = time.monotonic() - started_at
        events = repository.connection.execute(
            "SELECT event_type FROM runtime_evidence ORDER BY evidence_id"
        ).fetchall()
        stop_index = next(
            index for index, row in enumerate(events)
            if row["event_type"] == "STOPPED_SAFE"
        )

        assert elapsed < 1
        assert session.closed
        assert all(task.done() and task.cancelled() for task in (
            stream_task, resync_task, refresh_task, catalog_task, health_owner,
        ))
        assert runtime._stream_tasks == {}
        assert runtime._recoveries == {}
        assert runtime._retired_stream_tasks == set()
        assert runtime._retired_recovery_tasks == set()
        assert runtime._extended_health_recovery_task is None
        assert runtime._extended_health_recovery_requests == {}
        assert runtime._refresh_task is None
        assert runtime._extended_universe_task is None
        assert events[-1]["event_type"] == "STOPPED_SAFE"
        assert stop_index == len(events) - 1
        assert not any(
            row["event_type"].startswith("PUBLIC_SOCKET_")
            or row["event_type"].startswith("PUBLIC_SCAN")
            for row in events[stop_index:]
        )

        unrelated.cancel()
        await asyncio.gather(unrelated, return_exceptions=True)


@pytest.mark.asyncio
async def test_shutdown_reproduces_cancellation_resistant_session_and_socket_close(
    tmp_path,
):
    clock = FakeClock()
    session_close_started = asyncio.Event()
    socket_iteration_started = asyncio.Event()
    socket_close_started = asyncio.Event()
    release_session_close = asyncio.Event()
    release_socket_close = asyncio.Event()

    class CancellationResistantSocket:
        closed = False

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            socket_close_started.set()
            while not release_socket_close.is_set():
                try:
                    await release_socket_close.wait()
                except asyncio.CancelledError:
                    continue
            self.closed = True
            return False

        def __aiter__(self):
            return self

        async def __anext__(self):
            socket_iteration_started.set()
            await asyncio.Event().wait()

    class CancellationResistantSession:
        closed = False

        def __init__(self):
            self.socket = CancellationResistantSocket()

        def ws_connect(self, *_args, **_kwargs):
            return self.socket

        async def close(self):
            session_close_started.set()
            while not release_session_close.is_set():
                try:
                    await release_session_close.wait()
                except asyncio.CancelledError:
                    continue
            self.closed = True

    with PaperRepository(tmp_path / "cancellation-resistant-close.db") as repository:
        runtime = PublicPaperRuntime(repository, adapters={}, clock=clock)
        runtime._session = CancellationResistantSession()
        runtime._stop_event = asyncio.Event()
        stream_key = (Venue.EXTENDED, "CLOSE-RESISTANT", "trade")
        session_id = runtime._new_stream_session(stream_key)
        stream_task = asyncio.create_task(runtime._extended_stream(
            ExtendedAdapter(None), "CLOSE-RESISTANT", "trade", session_id,
        ))
        runtime._stream_tasks[stream_key] = stream_task
        await asyncio.wait_for(socket_iteration_started.wait(), timeout=1)

        runtime._request_stop("SIGINT")
        shutdown_task = asyncio.create_task(runtime.shutdown())
        try:
            await asyncio.wait_for(session_close_started.wait(), timeout=1)
            await asyncio.sleep(0)
            assert not shutdown_task.done()

            release_session_close.set()
            await asyncio.wait_for(socket_close_started.wait(), timeout=1)
            await asyncio.sleep(0)
            assert not shutdown_task.done()

            release_socket_close.set()
            await asyncio.wait_for(shutdown_task, timeout=1)
        finally:
            release_session_close.set()
            release_socket_close.set()
            if not shutdown_task.done():
                shutdown_task.cancel()
            await asyncio.gather(shutdown_task, return_exceptions=True)

        assert runtime._session.closed
        assert runtime._session.socket.closed
        assert stream_task.done() and stream_task.cancelled()
        assert repository.connection.execute(
            "SELECT event_type FROM runtime_evidence ORDER BY evidence_id"
        ).fetchall()[-1]["event_type"] == "STOPPED_SAFE"


@pytest.mark.asyncio
async def test_run_hands_off_cancellation_resistant_tick_to_shutdown_owner(tmp_path):
    clock = FakeClock()
    stop = asyncio.Event()
    tick_started = asyncio.Event()
    tick_cancelled = asyncio.Event()
    release_tick = asyncio.Event()
    session_close_started = asyncio.Event()

    class Session:
        closed = False

        async def close(self):
            session_close_started.set()
            release_tick.set()
            self.closed = True

    with PaperRepository(tmp_path / "owned-tick-handoff.db") as repository:
        runtime = PublicPaperRuntime(repository, adapters={}, clock=clock)
        runtime._session = Session()

        async def startup_scan():
            runtime.last_scan = SimpleNamespace(logical_at=clock.now())

        async def no_restore(_at):
            return None

        async def no_streams():
            return None

        async def cancellation_resistant_tick():
            tick_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                tick_cancelled.set()
                await release_tick.wait()
                raise

        runtime.scan = startup_scan
        runtime._restore = no_restore
        runtime.start_streams = no_streams
        runtime._start_public_refresh = lambda: None
        runtime._start_background_catalog_refresh = lambda **_kwargs: None
        runtime._try_mark_startup_ready = lambda: True
        runtime.tick = cancellation_resistant_tick

        run_task = asyncio.create_task(runtime.run(stop_event=stop))
        await asyncio.wait_for(tick_started.wait(), timeout=1)
        stop.set()
        await asyncio.wait_for(session_close_started.wait(), timeout=1)
        assert tick_cancelled.is_set()
        assert runtime._tick_task is not None
        result = await asyncio.wait_for(run_task, timeout=1)
        events = repository.connection.execute(
            "SELECT event_type FROM runtime_evidence ORDER BY evidence_id"
        ).fetchall()

        assert result == {"status": "STOPPED_SAFE", "forced_close": False}
        assert runtime._tick_task is None
        assert runtime._session.closed
        assert events[-1]["event_type"] == "STOPPED_SAFE"
        assert not any(
            row["event_type"] in {"RUNTIME_FATAL", "RUNTIME_STOPPED_FATAL"}
            for row in events
        )


@pytest.mark.asyncio
async def test_extended_watchdog_rotation_does_not_block_due_full_tick(
    tmp_path,
):
    clock = FakeClock()
    target = NOW + timedelta(minutes=5)
    fakes = adapters(clock, settlement_at=target)
    extended = GatedExtendedAdapter(clock, settlement_at=target)
    fakes[Venue.EXTENDED] = extended
    symbol = extended.market.venue_symbol
    key = (Venue.EXTENDED, symbol, "book")
    timeout = 2

    with PaperRepository(tmp_path / "extended-watchdog-prompt-rotation.db") as repository:
        runtime = PublicPaperRuntime(repository, adapters=fakes, clock=clock)
        try:
            await asyncio.wait_for(runtime.scan(), timeout=timeout)
        except BaseException:
            runtime._request_stop("STOP_EVENT")
            await asyncio.wait_for(runtime.shutdown(), timeout=timeout)
            raise
        assert isinstance(runtime.last_scan, ScanSnapshot)
        runtime._session = SimpleNamespace(closed=True)
        runtime._stop_event = asyncio.Event()
        old_session = runtime._new_stream_session(key)
        old_book = runtime.observations[Venue.EXTENDED, symbol].book
        assert old_book is not None
        cancel_acknowledged = asyncio.Event()
        release_retirement = asyncio.Event()
        old_mutation_attempted = asyncio.Event()

        async def delayed_old_stream():
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancel_acknowledged.set()
                await release_retirement.wait()
                runtime._confirm_extended_stream(
                    symbol, "book", clock.now(), data_ready=True,
                    stream_session_id=old_session,
                )
                stale_book = replace(
                    old_book,
                    bids=(BookLevel(D("70"), D("20")),),
                    asks=(BookLevel(D("71"), D("20")),),
                    observed_at=clock.now(), sequence=777,
                )
                await runtime.apply_book_event(
                    stale_book, stream_session_id=old_session
                )
                runtime._socket_disconnected(
                    (Venue.EXTENDED, "book", (symbol,)),
                    at=clock.now(), stream_session_id=old_session,
                )
                await runtime.mark_disconnected(
                    Venue.EXTENDED, symbol, at=clock.now(), stream_kind="book",
                    exception=RuntimeError("obsolete transport mutation"),
                    stream_session_id=old_session,
                )
                if runtime._owns_stream_session(key, old_session):
                    runtime._watchdog_restarted(
                        (Venue.EXTENDED, "book", (symbol,)), at=clock.now()
                    )
                old_mutation_attempted.set()

        old_task = asyncio.create_task(delayed_old_stream())
        runtime._stream_tasks[key] = old_task
        successor_sessions = []

        async def replacement_stream(_adapter, market, kind, session_id):
            if (market, kind) == (symbol, "book"):
                successor_sessions.append(session_id)
            runtime._confirm_extended_stream(
                market, kind, clock.now(), data_ready=True,
                stream_session_id=session_id,
            )
            runtime._watchdog_restarted(
                (Venue.EXTENDED, kind, (market,)), at=clock.now()
            )
            await runtime._stop_event.wait()

        runtime._extended_stream = replacement_stream
        runtime._extended_confirmed_at[symbol, "book"] = (
            NOW - timedelta(seconds=26)
        )
        runtime.next_health_check_at = NOW
        runtime.next_full_scan_at = NOW

        original_mark_disconnected = runtime.mark_disconnected
        health_arrivals = 0
        all_health_checks_arrived = asyncio.Event()

        async def synchronized_mark_disconnected(*args, **kwargs):
            nonlocal health_arrivals
            health_arrivals += 1
            if health_arrivals == 3:
                all_health_checks_arrived.set()
            await asyncio.wait_for(
                all_health_checks_arrived.wait(), timeout=timeout
            )
            await original_mark_disconnected(*args, **kwargs)

        runtime.mark_disconnected = synchronized_mark_disconnected
        tick = asyncio.create_task(runtime.tick(NOW))
        repeated_checks = [
            asyncio.create_task(runtime._check_extended_health(NOW))
            for _ in range(2)
        ]
        controlled_tasks = {tick, *repeated_checks, old_task}

        def old_task_ownership_locations():
            def contains_old_task(value, seen):
                if value is old_task:
                    return True
                if isinstance(value, asyncio.Task) or id(value) in seen:
                    return False
                seen.add(id(value))
                if isinstance(value, dict):
                    return any(
                        contains_old_task(item, seen)
                        for pair in value.items() for item in pair
                    )
                if isinstance(value, (list, set, tuple)):
                    return any(contains_old_task(item, seen) for item in value)
                owned = getattr(value, "task", None)
                return owned is old_task

            return tuple(
                name for name, value in vars(runtime).items()
                if any(token in name for token in ("task", "stream", "recover"))
                and contains_old_task(value, set())
            )

        try:
            await asyncio.wait_for(
                cancel_acknowledged.wait(), timeout=timeout
            )
            prompt_tasks, _ = await asyncio.wait(
                {tick, *repeated_checks}, timeout=0.05
            )
            await asyncio.sleep(0)
            successor_before_retirement = runtime._stream_tasks.get(key)
            session_before_retirement = runtime._stream_sessions.get(key)
            successors_before_retirement = len(successor_sessions)
            deadline_before_retirement = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence "
                "WHERE event_type='PUBLIC_SCAN_DEADLINE' "
                "AND detail LIKE '%\"kind\":\"full\"%'"
            ).fetchone()[0]
            refresh_before_retirement = runtime._refresh_task
            if refresh_before_retirement is not None:
                await asyncio.wait_for(
                    refresh_before_retirement, timeout=timeout
                )
                await asyncio.wait_for(runtime.tick(NOW), timeout=timeout)
                await asyncio.sleep(0)
            observable_before_retirement = (
                dict(runtime.component_readiness[Venue.EXTENDED]),
                runtime.readiness[Venue.EXTENDED],
                runtime.coordinator.stream(Venue.EXTENDED, symbol).book(),
                runtime.observations[Venue.EXTENDED, symbol],
                dict(runtime._extended_confirmed_at),
                tuple(repository.connection.execute(
                    "SELECT event_type,detail FROM runtime_evidence ORDER BY evidence_id"
                ).fetchall()),
            )

            release_retirement.set()
            await asyncio.wait_for(
                asyncio.gather(tick, *repeated_checks), timeout=timeout
            )
            await asyncio.wait_for(
                old_mutation_attempted.wait(), timeout=timeout
            )
            await asyncio.sleep(0)
            observable_after_retirement = (
                dict(runtime.component_readiness[Venue.EXTENDED]),
                runtime.readiness[Venue.EXTENDED],
                runtime.coordinator.stream(Venue.EXTENDED, symbol).book(),
                runtime.observations[Venue.EXTENDED, symbol],
                dict(runtime._extended_confirmed_at),
                tuple(repository.connection.execute(
                    "SELECT event_type,detail FROM runtime_evidence ORDER BY evidence_id"
                ).fetchall()),
            )
            successor_was_running = bool(
                successor_before_retirement is not None
                and not successor_before_retirement.done()
            )
            old_retirement = (
                old_task.done(), old_task.cancelled(), old_task.exception()
            )
            old_ownership_after_retirement = old_task_ownership_locations()
            refresh_was_started = (
                refresh_before_retirement is not None
                or runtime._refresh_task is not None
            )
            if runtime._pending_full_scan_at is not None:
                await asyncio.wait_for(runtime._refresh_task, timeout=timeout)
                await asyncio.wait_for(runtime.tick(NOW), timeout=timeout)
            post_gate_scan = runtime.last_scan
            pending_full_scan_at = runtime._pending_full_scan_at
            full_scans = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence "
                "WHERE event_type='PUBLIC_SCAN' "
                "AND detail LIKE '%\"scan_kind\":\"FULL\"%'"
            ).fetchone()[0]
            socket_rows = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence "
                "WHERE event_type LIKE 'PUBLIC_SOCKET_%'"
            ).fetchone()[0]
        finally:
            all_health_checks_arrived.set()
            release_retirement.set()
            for task in controlled_tasks:
                if not task.done():
                    task.cancel()
            try:
                await asyncio.wait_for(
                    asyncio.gather(*controlled_tasks, return_exceptions=True),
                    timeout=timeout,
                )
            finally:
                runtime._request_stop("STOP_EVENT")
                await asyncio.wait_for(runtime.shutdown(), timeout=timeout)

    assert prompt_tasks == {tick, *repeated_checks}, {
        "completed_before_release": len(prompt_tasks),
        "deadline_before_release": deadline_before_retirement,
        "successors_before_old_retirement": successors_before_retirement,
        "successors_total": len(successor_sessions),
        "post_gate_full_scan_valid": (
            isinstance(post_gate_scan, ScanSnapshot)
            and post_gate_scan.logical_at == NOW
            and pending_full_scan_at is None
            and full_scans == 1
        ),
    }
    assert successor_before_retirement is not None
    assert successor_before_retirement is not old_task
    assert successor_was_running
    assert session_before_retirement != old_session
    assert successors_before_retirement == 1
    assert len(successor_sessions) == 1
    assert deadline_before_retirement == 1
    assert observable_after_retirement == observable_before_retirement
    assert old_retirement == (True, False, None)
    assert old_ownership_after_retirement == ()
    assert refresh_was_started
    assert isinstance(post_gate_scan, ScanSnapshot)
    assert post_gate_scan.logical_at == NOW
    assert pending_full_scan_at is None
    assert full_scans == 1
    assert socket_rows == 0


@pytest.mark.asyncio
async def test_extended_watchdog_delayed_retirement_exception_is_fatal(tmp_path):
    clock = FakeClock()
    symbol = "ABC-EXTENDED"
    key = (Venue.EXTENDED, symbol, "book")
    adapter = ExtendedAdapter(None)
    timeout = 2
    with PaperRepository(tmp_path / "extended-watchdog-retirement-error.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.EXTENDED: adapter}, clock=clock,
        )
        runtime._session = SimpleNamespace(closed=True)
        runtime._stop_event = asyncio.Event()
        runtime._new_stream_session(key)
        cancel_acknowledged = asyncio.Event()
        release_retirement = asyncio.Event()

        async def failing_old_stream():
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancel_acknowledged.set()
                await release_retirement.wait()
                raise RuntimeError("synthetic delayed retirement failure")

        old_task = asyncio.create_task(failing_old_stream())
        runtime._stream_tasks[key] = old_task

        async def replacement_stream(_adapter, _symbol, _kind, _session_id):
            await runtime._stop_event.wait()

        runtime._extended_stream = replacement_stream
        restart = asyncio.create_task(
            runtime._restart_extended_stream(symbol, "book")
        )
        controlled_tasks = {restart, old_task}
        loop = asyncio.get_running_loop()
        previous_exception_handler = loop.get_exception_handler()
        loop_errors = []
        loop.set_exception_handler(
            lambda _loop, context: loop_errors.append(context)
        )
        try:
            await asyncio.wait_for(
                cancel_acknowledged.wait(), timeout=timeout
            )
            prompt, _ = await asyncio.wait({restart}, timeout=0.05)
            release_retirement.set()
            await asyncio.wait_for(restart, timeout=timeout)
            old_completed, _ = await asyncio.wait(
                {old_task}, timeout=timeout
            )
            await asyncio.sleep(0)
            stop_cause = runtime._stop_cause
            fatal = repository.connection.execute(
                "SELECT detail FROM runtime_evidence "
                "WHERE event_type='RUNTIME_FATAL'"
            ).fetchall()
            old_done = old_completed == {old_task}
            old_exception_consumed = not old_task._log_traceback
        finally:
            release_retirement.set()
            for task in controlled_tasks:
                if not task.done():
                    task.cancel()
            try:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*controlled_tasks, return_exceptions=True),
                        timeout=timeout,
                    )
                finally:
                    await asyncio.wait_for(runtime.shutdown(), timeout=timeout)
            finally:
                loop.set_exception_handler(previous_exception_handler)

    assert prompt == {restart}, {
        "completed_before_release": len(prompt),
        "post_gate_old_done": old_done,
        "post_gate_stop_cause": stop_cause,
        "post_gate_fatal_rows": len(fatal),
    }
    assert stop_cause == "RUNTIME_FATAL"
    assert [json.loads(row["detail"])["exception_class"] for row in fatal] == [
        "RuntimeError"
    ]
    assert old_done
    assert old_exception_consumed
    assert loop_errors == []


@pytest.mark.asyncio
async def test_simultaneous_stale_watchdog_mutation_is_generation_safe(tmp_path):
    clock = FakeClock()
    symbol = "ABC-EXTENDED"
    delivery = CaptureNotifications()
    with PaperRepository(tmp_path / "watchdog-generation-race.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={}, clock=clock,
            notifications=NotificationOutbox(delivery),
        )
        runtime._stop_event = asyncio.Event()

        async def waiting_stream():
            await runtime._stop_event.wait()

        tasks = {}
        for kind in ("book", "trade"):
            key = (Venue.EXTENDED, symbol, kind)
            task = asyncio.create_task(waiting_stream())
            tasks[kind] = task
            runtime._stream_tasks[key] = task
            runtime._new_stream_session(key)
            runtime._extended_confirmed_at[symbol, kind] = (
                clock.now() - timedelta(seconds=26)
            )
        original = runtime.mark_disconnected

        async def mutating_disconnect(venue, market, **kwargs):
            if kwargs.get("stream_kind") == "book":
                runtime._extended_confirmed_at.pop((symbol, "trade"), None)
                runtime._new_stream_session(
                    (Venue.EXTENDED, symbol, "trade")
                )
            await original(venue, market, **kwargs)

        runtime.mark_disconnected = mutating_disconnect
        restarted = []
        runtime._start_extended_stream = lambda _symbol, kind: restarted.append(kind)
        await runtime._check_extended_health(clock.now())
        recovery_owner = runtime._extended_health_recovery_task
        assert recovery_owner is not None
        await recovery_owner
        rows = repository.connection.execute(
            "SELECT event_type FROM runtime_evidence ORDER BY evidence_id"
        ).fetchall()
        runtime._stop_event.set()
        for task in tasks.values():
            task.cancel()
        await asyncio.gather(*tasks.values(), return_exceptions=True)
    assert restarted == ["book"]
    assert [row["event_type"] for row in rows].count(
        "PUBLIC_STREAM_CONFIRMATION_STALE"
    ) == 1
    assert not any(row["event_type"].startswith("PUBLIC_SOCKET_") for row in rows)
    assert [row.kind for row in delivery.rows].count("CRITICAL_DATA_LOSS") == 1


@pytest.mark.asyncio
async def test_extended_quiet_heartbeat_keeps_each_stream_healthy_for_sixty_seconds(tmp_path):
    clock = FakeClock()
    symbol = "ABC-EXTENDED"
    with PaperRepository(tmp_path / "extended-quiet-heartbeat.db") as repository:
        runtime = PublicPaperRuntime(repository, adapters={}, clock=clock)
        runtime._stop_event = asyncio.Event()

        async def waiting_stream():
            await runtime._stop_event.wait()

        tasks = {}
        for kind in ("book", "trade", "funding"):
            confirm_extended_stream(runtime, symbol, kind, clock.now(), data_ready=True)
            task = asyncio.create_task(waiting_stream())
            runtime._stream_tasks[Venue.EXTENDED, symbol, kind] = task
            tasks[kind] = task
        for _ in range(6):
            clock.advance(10)
            for kind in ("book", "trade", "funding"):
                confirm_extended_stream(runtime,
                    symbol, kind, clock.now(), data_ready=False
                )
            await runtime._check_extended_health(clock.now())
        assert repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence WHERE event_type LIKE "
            "'PUBLIC_STREAM_%' OR event_type LIKE 'PUBLIC_SOCKET_%'"
        ).fetchone()[0] == 0
        assert all(
            runtime.component_readiness[Venue.EXTENDED][
                f"connection_{kind}:{symbol}"
            ].available
            for kind in ("book", "trade", "funding")
        )
        runtime._stop_event.set()
        await asyncio.gather(*tasks.values())


def test_extended_book_ping_refreshes_only_book_connection_health(tmp_path):
    clock = FakeClock()
    symbol = "ABC-EXTENDED"
    with PaperRepository(tmp_path / "extended-book-ping-health.db") as repository:
        runtime = PublicPaperRuntime(repository, adapters={}, clock=clock)
        runtime._set_component_readiness(
            Venue.EXTENDED, f"book:{symbol}", False,
            "PUBLIC_BOOK_DATA_PENDING", clock.now(),
        )
        confirm_extended_stream(runtime,
            symbol, "book", clock.now(), data_ready=False
        )
        stream = runtime.coordinator.stream(Venue.EXTENDED, symbol)
        assert stream.health(clock.now()).data_quality is DataQuality.DEGRADED
        assert not runtime.component_readiness[Venue.EXTENDED][f"book:{symbol}"].available

        stream.snapshot(OrderBook(
            Venue.EXTENDED, symbol,
            (BookLevel(D("99"), D("20")),),
            (BookLevel(D("101"), D("20")),), clock.now(), 1,
        ))
        confirm_extended_stream(runtime,
            symbol, "book", clock.now(), data_ready=True
        )
        clock.advance(26)
        confirm_extended_stream(runtime,
            symbol, "book", clock.now(), data_ready=False
        )
        assert stream.health(clock.now()).data_quality is DataQuality.COMPLETE

        clock.advance(26)
        confirm_extended_stream(runtime,
            symbol, "trade", clock.now(), data_ready=False
        )
        assert stream.health(clock.now()).data_quality is DataQuality.DEGRADED
        confirm_extended_stream(runtime,
            symbol, "book", clock.now(), data_ready=False
        )
        assert stream.health(clock.now()).data_quality is DataQuality.COMPLETE

        clock.advance(26)
        confirm_extended_stream(runtime,
            symbol, "funding", clock.now(), data_ready=False
        )
        assert stream.health(clock.now()).data_quality is DataQuality.DEGRADED


@pytest.mark.asyncio
async def test_extended_valid_trade_restores_health_before_no_order_early_return(tmp_path):
    clock = FakeClock()
    stop = asyncio.Event()
    adapter = GatedExtendedAdapter(clock, settlement_at=NOW + timedelta(minutes=5))
    symbol = adapter.market.venue_symbol
    payload = {
        "seq": 1,
        "data": [{
            "i": 1, "m": symbol, "S": "SELL", "tT": "TRADE",
            "T": int(NOW.timestamp() * 1000), "p": "100", "q": "1",
        }],
    }
    session = SingleWebSocketSession(TextWebSocket(stop, [payload]))
    with PaperRepository(tmp_path / "extended-trade-ready.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.EXTENDED: adapter}, clock=clock,
        )
        runtime._session = session
        runtime._stop_event = stop
        session_id = runtime._new_stream_session(
            (Venue.EXTENDED, symbol, "trade")
        )
        await runtime._extended_stream(
            adapter, symbol, "trade", session_id
        )
    assert session.connections == 1
    assert (Venue.EXTENDED, symbol) in runtime._trade_stream_ready
    assert runtime.component_readiness[Venue.EXTENDED][f"trade:{symbol}"].available
    assert runtime.component_readiness[Venue.EXTENDED][f"connection_trade:{symbol}"].available


@pytest.mark.asyncio
async def test_extended_funding_ws_path_does_not_replace_rest_expected_quote(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(minutes=5)
    stop = asyncio.Event()
    adapter = GatedExtendedAdapter(clock, settlement_at=target)
    symbol = adapter.market.venue_symbol
    session = SingleWebSocketSession(TextWebSocket(stop, [{
        "ts": int(NOW.timestamp() * 1000), "seq": 1,
        "data": {"m": symbol, "T": int(target.timestamp() * 1000), "f": "0.001"},
    }]))
    fakes = adapters(clock, settlement_at=target)
    fakes[Venue.EXTENDED] = adapter
    with PaperRepository(tmp_path / "extended-funding-ws-inert.db") as repository:
        runtime = PublicPaperRuntime(repository, adapters=fakes, clock=clock)
        await runtime.scan()
        expected = runtime.observations[Venue.EXTENDED, symbol].funding
        runtime._session = session
        runtime._stop_event = stop
        session_id = runtime._new_stream_session(
            (Venue.EXTENDED, symbol, "funding")
        )
        await runtime._extended_stream(
            adapter, symbol, "funding", session_id
        )
        assert runtime.observations[Venue.EXTENDED, symbol].funding == expected
        assert repository.connection.execute(
            "SELECT COUNT(*) FROM funding_settlements WHERE venue='EXTENDED'"
        ).fetchone()[0] == 0
    assert session.connections == 1
    assert runtime.component_readiness[Venue.EXTENDED][f"funding:{symbol}"].available
    assert runtime.component_readiness[Venue.EXTENDED][f"applied_funding:{symbol}"].available
    assert runtime.component_readiness[Venue.EXTENDED][f"connection_funding:{symbol}"].available


@pytest.mark.asyncio
async def test_extended_funding_connection_allows_quiet_applied_stream(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(minutes=5)
    adapter = GatedExtendedAdapter(clock, settlement_at=target)
    fakes = adapters(clock, settlement_at=target)
    fakes[Venue.EXTENDED] = adapter
    symbol = adapter.market.venue_symbol
    with PaperRepository(tmp_path / "extended-connection-only.db") as repository:
        runtime = PublicPaperRuntime(repository, adapters=fakes, clock=clock)
        await runtime.scan()
        for venue, market_symbol in runtime.observations:
            if venue is not Venue.EXTENDED:
                runtime.mark_trade_stream_connected(venue, market_symbol, at=clock.now())
                runtime._live_book_ready.add((venue, market_symbol))
        runtime._session = object()
        runtime._stop_event = asyncio.Event()
        started = []
        runtime._start_extended_stream = lambda kind: started.append(kind)
        await runtime._reconcile_extended_streams()
        for kind in ("book", "trade", "funding"):
            confirm_extended_stream(runtime,
                symbol, kind, clock.now(), data_ready=False
            )
        components = runtime.component_readiness[Venue.EXTENDED]
        assert set(components) >= {
            f"{kind}:{symbol}" for kind in ("book", "trade", "funding", "applied_funding")
        } | {
            f"connection_{kind}:{symbol}" for kind in ("book", "trade", "funding")
        }
        assert all(
            components[f"connection_{kind}:{symbol}"].available
            for kind in ("book", "trade", "funding")
        )
        assert not components[f"book:{symbol}"].available
        assert not components[f"trade:{symbol}"].available
        assert components[f"funding:{symbol}"].available
        assert not components[f"applied_funding:{symbol}"].available
        await runtime.scan(refresh=False, scan_kind="FULL")
        before = next(
            row for row in runtime.last_scan.evaluations
            if row.hedge_venue is Venue.EXTENDED
        )
        assert before.planned_maker_net_pnl_usd is None

        await runtime.apply_book_event(OrderBook(
            Venue.EXTENDED, symbol,
            (BookLevel(D("99"), D("20")),),
            (BookLevel(D("101"), D("20")),), clock.now(), 1,
        ), stream_session_id=stream_session(
            runtime, Venue.EXTENDED, symbol, "book"
        ))
        confirm_extended_stream(runtime, symbol, "book", clock.now(), data_ready=True)
        clock.advance(1)
        await runtime.scan(refresh=False, scan_kind="FULL")
        assert next(
            row for row in runtime.last_scan.evaluations
            if row.hedge_venue is Venue.EXTENDED
        ).planned_maker_net_pnl_usd is None
        assert components[f"book:{symbol}"].available
        assert not components[f"trade:{symbol}"].available
        assert not components[f"applied_funding:{symbol}"].available

        confirm_extended_stream(runtime, symbol, "trade", clock.now(), data_ready=True)
        clock.advance(1)
        await runtime.scan(refresh=False, scan_kind="FULL")
        after_trade = next(
            row for row in runtime.last_scan.evaluations
            if row.hedge_venue is Venue.EXTENDED
        )
        assert after_trade.planned_maker_net_pnl_usd is not None
        assert "FUNDING_STREAM_UNHEALTHY" not in after_trade.no_trade_reasons
        assert "BOOK_UNHEALTHY" not in after_trade.no_trade_reasons
        assert components[f"trade:{symbol}"].available
        assert not components[f"applied_funding:{symbol}"].available
        assert runtime.readiness[Venue.EXTENDED].available

        record = adapter.normalize_applied_funding_message({
            "ts": int(clock.now().timestamp() * 1000), "seq": 1,
            "data": {
                "m": symbol, "T": int(target.timestamp() * 1000), "f": "0.001",
            },
        }, adapter.market)
        assert record is not None
        confirm_extended_stream(runtime, symbol, "funding", clock.now(), data_ready=True)
        await runtime._apply_extended_funding_record(record)
        clock.advance(1)
        await runtime.scan(refresh=False, scan_kind="FULL")
        after = next(
            row for row in runtime.last_scan.evaluations
            if row.hedge_venue is Venue.EXTENDED
        )
        assert after.planned_maker_net_pnl_usd is not None
        assert all(
            components[f"{name}:{symbol}"].available
            for name in (
                "book", "connection_book", "trade", "connection_trade",
                "funding", "connection_funding",
            )
        )
        await runtime.shutdown()
    assert sorted(started) == [
        "book", "funding", "trade",
    ]


def test_extended_catalog_and_metadata_ttl_edges_and_atomic_install(tmp_path):
    clock = FakeClock()
    adapter = GatedExtendedAdapter(clock, settlement_at=NOW + timedelta(minutes=5))
    market = adapter.market
    volume = MarketVolume(
        Venue.EXTENDED, market.venue_symbol, D("1000000"), NOW, "official-shaped"
    )
    with PaperRepository(tmp_path / "extended-cache-ttl.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.EXTENDED: adapter}, clock=clock,
        )
        runtime._update_extended_catalog_readiness(NOW)
        assert runtime.component_readiness[Venue.EXTENDED]["catalog"].detail == (
            "CATALOG_UNAVAILABLE"
        )
        runtime._install_extended_catalog((market,), (volume,), NOW, full=True)
        assert not runtime._extended_market_with_cache_blocker(
            market, NOW + timedelta(seconds=300)
        ).evidence_blockers
        assert "MARKET_METADATA_STALE" in runtime._extended_market_with_cache_blocker(
            market, NOW + timedelta(seconds=300, microseconds=1)
        ).evidence_blockers
        runtime._update_extended_catalog_readiness(NOW + timedelta(seconds=1200))
        assert runtime.component_readiness[Venue.EXTENDED]["catalog"].available
        runtime._update_extended_catalog_readiness(
            NOW + timedelta(seconds=1200, microseconds=1)
        )
        assert runtime.component_readiness[Venue.EXTENDED]["catalog"].detail == "CATALOG_STALE"
        before = runtime.markets[Venue.EXTENDED]
        with pytest.raises(ValueError, match="volumes are incomplete"):
            runtime._install_extended_catalog((market,), (), clock.now(), full=True)
        assert runtime.markets[Venue.EXTENDED] == before
        with pytest.raises(ValueError, match="volumes are incomplete"):
            runtime._install_extended_catalog(
                (market,), (volume, volume), clock.now(), full=False,
            )
        assert runtime.markets[Venue.EXTENDED] == before


@pytest.mark.asyncio
async def test_fresh_extended_cache_timeout_preserves_economics_without_book_failure(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(minutes=5)

    class TimeoutAfterBootstrap(GatedExtendedAdapter):
        fail_catalog = False

        async def fetch_catalog(self):
            if self.fail_catalog:
                raise TimeoutError("synthetic bounded catalog timeout")
            return await super().fetch_catalog()

    extended = TimeoutAfterBootstrap(clock, settlement_at=target)
    fakes = adapters(clock, settlement_at=target)
    fakes[Venue.EXTENDED] = extended
    with PaperRepository(tmp_path / "extended-cache-timeout.db") as repository:
        async with PublicPaperRuntime(repository, adapters=fakes, clock=clock) as runtime:
            await runtime.scan()
            confirm_public_streams(runtime, clock.now())
            for kind in ("book", "trade", "funding"):
                confirm_extended_stream(runtime,
                    extended.market.venue_symbol, kind, clock.now(), data_ready=True,
                )
            await runtime.scan(refresh=False, scan_kind="FULL")
            before = next(
                plan.planned_maker_net_pnl_usd
                for plan in runtime.last_scan.evaluations
                if plan.hedge_venue is Venue.EXTENDED
            )
            assert before is not None
            extended.fail_catalog = True
            await runtime._refresh_extended_universe()
            await runtime.scan(refresh=False, scan_kind="FULL")
            after = next(
                plan for plan in runtime.last_scan.evaluations
                if plan.hedge_venue is Venue.EXTENDED
            )
            evidence = repository.connection.execute(
                "SELECT event_type,detail FROM runtime_evidence ORDER BY evidence_id"
            ).fetchall()
    assert after.planned_maker_net_pnl_usd == before
    assert "BOOK_UNHEALTHY" not in after.no_trade_reasons
    assert not any(row["event_type"].startswith("PUBLIC_SOCKET_") for row in evidence)
    failed = [
        json.loads(row["detail"]) for row in evidence
        if row["event_type"] == "PUBLIC_REQUEST_FAILED"
    ]
    assert failed[-1]["cache_state"] == "CACHED_LAST_GOOD"


@pytest.mark.asyncio
async def test_extended_metadata_stale_is_precise_and_not_book_unhealthy(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(minutes=10)
    adapter = GatedExtendedAdapter(clock, settlement_at=target)
    fakes = adapters(clock, settlement_at=target)
    fakes[Venue.EXTENDED] = adapter
    with PaperRepository(tmp_path / "extended-metadata-stale.db") as repository:
        async with PublicPaperRuntime(repository, adapters=fakes, clock=clock) as runtime:
            await runtime.scan()
            confirm_public_streams(runtime, clock.now())
            clock.advance(301)
            confirm_public_streams(runtime, clock.now())
            for kind in ("book", "trade", "funding"):
                confirm_extended_stream(runtime,
                    adapter.market.venue_symbol, kind, clock.now(), data_ready=True
                )
            await runtime.scan(refresh=False, scan_kind="FULL")
            plans = [
                plan for plan in runtime.last_scan.evaluations
                if plan.hedge_venue is Venue.EXTENDED
            ]
    assert plans
    assert all("MARKET_METADATA_STALE" in plan.no_trade_reasons for plan in plans)
    assert all("BOOK_UNHEALTHY" not in plan.no_trade_reasons for plan in plans)


@pytest.mark.asyncio
async def test_extended_client_heartbeat_is_ten_seconds_and_owned(tmp_path):
    clock = FakeClock()
    stop = asyncio.Event()
    sleeps = []

    class Socket:
        def __init__(self):
            self.pings = 0

        async def ping(self):
            self.pings += 1

    socket = Socket()

    async def controlled_sleep(seconds):
        sleeps.append(seconds)
        if len(sleeps) == 2:
            stop.set()
        await asyncio.sleep(0)

    with PaperRepository(tmp_path / "extended-heartbeat.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={}, clock=clock, sleep=controlled_sleep,
        )
        runtime._stop_event = stop
        key = (Venue.EXTENDED, "ABC-EXTENDED", "book")
        session_id = runtime._new_stream_session(key)
        await runtime._extended_heartbeat(socket, key, session_id)
    assert sleeps == [10, 10]
    assert socket.pings == 1


@pytest.mark.asyncio
async def test_extended_aggregate_trade_heartbeat_makes_all_symbols_ready(tmp_path):
    clock = FakeClock()
    stop = asyncio.Event()

    class Socket:
        def __init__(self):
            self.messages = [
                SimpleNamespace(type=aiohttp.WSMsgType.PING, data=b"server"),
                SimpleNamespace(type=aiohttp.WSMsgType.PONG, data=b"client"),
            ]
            self.pongs = []

        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        def __aiter__(self): return self
        async def __anext__(self):
            if not self.messages:
                stop.set()
                raise StopAsyncIteration
            clock.advance(1)
            return self.messages.pop(0)
        async def pong(self, data): self.pongs.append(data)
        async def ping(self): return None

    socket = Socket()
    session = SimpleNamespace(ws_connect=lambda *_args, **_kwargs: socket)
    with PaperRepository(tmp_path / "extended-ping-pong.db") as repository:
        runtime = PublicPaperRuntime(repository, adapters={}, clock=clock)
        runtime._session = session
        runtime._stop_event = stop
        runtime._extended_stream_symbols = (
            "ABC-EXTENDED", "QUIET-EXTENDED",
        )
        session_id = runtime._new_stream_session(
            (Venue.EXTENDED, "*", "trade")
        )
        await runtime._extended_stream(
            ExtendedAdapter(None), runtime._extended_stream_symbols, "trade", session_id
        )
    assert socket.pongs == [b"server"]
    assert all(
        runtime._extended_confirmed_at[symbol, "trade"] == NOW + timedelta(seconds=2)
        for symbol in runtime._extended_stream_symbols
    )
    assert {
        (Venue.EXTENDED, symbol) for symbol in runtime._extended_stream_symbols
    } <= runtime._trade_stream_ready
    assert all(
        runtime.component_readiness[Venue.EXTENDED][f"trade:{symbol}"].available
        for symbol in runtime._extended_stream_symbols
    )
    assert all(
        runtime.component_readiness[Venue.EXTENDED][
            f"connection_trade:{symbol}"
        ].available
        for symbol in runtime._extended_stream_symbols
    )
    assert ("ABC-EXTENDED", "book") not in runtime._extended_confirmed_at
    assert ("ABC-EXTENDED", "funding") not in runtime._extended_confirmed_at


@pytest.mark.asyncio
async def test_extended_aggregate_trade_stale_session_cannot_ready_new_symbol(tmp_path):
    clock = FakeClock()
    stop = asyncio.Event()
    with PaperRepository(tmp_path / "extended-aggregate-trade-session-ownership.db") as repository:
        runtime = PublicPaperRuntime(repository, adapters={}, clock=clock)
        runtime._stop_event = stop
        runtime._extended_stream_symbols = ("ABC-EXTENDED",)
        key = (Venue.EXTENDED, "*", "trade")
        stale_session = runtime._new_stream_session(key)
        runtime._confirm_extended_aggregate(
            "trade", clock.now(), data_ready=False,
            stream_session_id=stale_session,
        )
        await runtime.mark_disconnected(
            Venue.EXTENDED, "ABC-EXTENDED", stream_kind="trade",
            stream_session_id=stale_session,
        )

        runtime._extended_stream_symbols = (
            "ABC-EXTENDED", "NEW-EXTENDED",
        )
        current_session = runtime._new_stream_session(key)
        runtime._confirm_extended_aggregate(
            "trade", clock.now(), data_ready=False,
            stream_session_id=stale_session,
        )
        assert (Venue.EXTENDED, "NEW-EXTENDED") not in runtime._trade_stream_ready
        assert "trade:NEW-EXTENDED" not in runtime.component_readiness.get(
            Venue.EXTENDED, {}
        )

        runtime._confirm_extended_aggregate(
            "trade", clock.now(), data_ready=False,
            stream_session_id=current_session,
        )
        assert {
            (Venue.EXTENDED, symbol) for symbol in runtime._extended_stream_symbols
        } <= runtime._trade_stream_ready


@pytest.mark.asyncio
async def test_extended_universe_refresh_is_owned_single_flight_and_cancelled(tmp_path):
    clock = FakeClock()

    class BlockingCatalog(GatedExtendedAdapter):
        async def fetch_catalog(self):
            self.request_started.set()
            try:
                await self.gate.wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise

    adapter = BlockingCatalog(clock, settlement_at=NOW + timedelta(minutes=5))
    with PaperRepository(tmp_path / "extended-universe-single-flight.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.EXTENDED: adapter}, clock=clock,
        )
        runtime._stop_event = asyncio.Event()
        runtime._start_extended_universe_refresh()
        first = runtime._extended_universe_task
        runtime._start_extended_universe_refresh()
        assert runtime._extended_universe_task is first
        await adapter.request_started.wait()
        await runtime.shutdown()
    assert first is not None and first.done()
    assert adapter.cancelled


@pytest.mark.asyncio
async def test_sixty_second_extended_universe_request_does_not_move_full_cadence(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(minutes=10)
    extended = GatedExtendedAdapter(clock, settlement_at=target)
    fakes = adapters(clock, settlement_at=target)
    fakes[Venue.EXTENDED] = extended
    with PaperRepository(tmp_path / "extended-universe-nonblocking.db") as repository:
        async with PublicPaperRuntime(repository, adapters=fakes, clock=clock) as runtime:
            await runtime.scan()
            runtime._stop_event = asyncio.Event()
            runtime.next_extended_catalog_at = NOW
            runtime.next_full_scan_at = NOW
            runtime.next_health_check_at = NOW + timedelta(hours=1)
            await runtime.tick(NOW)
            assert runtime._refresh_task is not None
            await runtime._refresh_task
            await runtime.tick(NOW)
            await extended.request_started.wait()
            assert runtime._extended_universe_task is not None
            assert not runtime._extended_universe_task.done()
            clock.advance(60)
            await runtime.tick(clock.now())
            assert not runtime._extended_universe_task.done()
            clock.advance(60)
            await runtime.tick(clock.now())
            assert runtime._refresh_task is not None
            await runtime._refresh_task
            await runtime.tick(clock.now())
            assert not runtime._extended_universe_task.done()
            scans = [
                json.loads(row["detail"])
                for row in repository.connection.execute(
                    "SELECT detail FROM runtime_evidence "
                    "WHERE event_type='PUBLIC_SCAN' ORDER BY evidence_id"
                ).fetchall()
            ]
            await runtime.shutdown()
    full_scheduled = [
        row["scheduled_at"] for row in scans if row["scan_kind"] == "FULL"
    ]
    assert full_scheduled[-2:] == [NOW.isoformat(), (NOW + timedelta(seconds=120)).isoformat()]
    assert extended.cancelled


@pytest.mark.asyncio
async def test_two_extended_markets_keep_expected_funding_across_full_scans(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(minutes=5)

    class TwoMarketExtendedAdapter(GatedExtendedAdapter):
        def __init__(self):
            super().__init__(clock, settlement_at=target)
            self.rows = tuple(
                replace(
                    self.market, canonical_asset=f"A{index}",
                    venue_symbol=f"A{index}-EXTENDED",
                )
                for index in range(2)
            )

        async def fetch_catalog(self):
            return self.rows, tuple(
                MarketVolume(
                    Venue.EXTENDED, market.venue_symbol, D("1000000"),
                    clock.now(), "official-shaped",
                )
                for market in self.rows
            )

        async def fetch_funding_quote(self, market, *, assumed_open_at):
            return FundingCashQuote(
                Venue.EXTENDED, market.venue_symbol, clock.now(), assumed_open_at,
                target, FundingQuality.PREDICTED,
                FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT, True,
                D("5"), D("5"), "official-shaped",
            )

    extended = TwoMarketExtendedAdapter()
    fakes = {
        Venue.RISEX: ManyFakeAdapter(Venue.RISEX, clock, settlement_at=target),
        Venue.EXTENDED: extended,
        Venue.NADO: ManyFakeAdapter(Venue.NADO, clock, settlement_at=target),
    }
    with PaperRepository(tmp_path / "extended-two-market-full.db") as repository:
        async with PublicPaperRuntime(repository, adapters=fakes, clock=clock) as runtime:
            await runtime.scan()
            confirm_public_streams(runtime, clock.now())
            for market in extended.rows:
                for kind in ("book", "trade", "funding"):
                    confirm_extended_stream(runtime,
                        market.venue_symbol, kind, clock.now(), data_ready=True
                    )
            clock.advance(1)
            await runtime.scan(refresh=False, scan_kind="FULL")
            for market in extended.rows:
                record = extended.normalize_applied_funding_message({
                    "ts": int(clock.now().timestamp() * 1000), "seq": 1,
                    "data": {
                        "m": market.venue_symbol,
                        "T": int(target.timestamp() * 1000), "f": "0.001",
                    },
                }, market)
                assert record is not None
                await runtime._apply_extended_funding_record(record)
            clock.advance(1)
            await runtime.scan(refresh=False, scan_kind="FULL")
            plans = [
                plan for plan in runtime.last_scan.evaluations
                if plan.hedge_venue is Venue.EXTENDED
            ]
            full_count = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence WHERE event_type='PUBLIC_SCAN' "
                "AND json_extract(detail,'$.scan_kind')='FULL'"
            ).fetchone()[0]
            for market in extended.rows:
                components = runtime.component_readiness[Venue.EXTENDED]
                assert all(
                    components[f"{name}:{market.venue_symbol}"].available
                    for name in (
                        "book", "connection_book", "trade", "connection_trade",
                        "funding", "connection_funding",
                    )
                )
    assert full_count == 2
    assert len(plans) == 4
    assert all(plan.planned_maker_net_pnl_usd is not None for plan in plans)
    assert all(
        "FUNDING_ELIGIBILITY_UNKNOWN" not in plan.no_trade_reasons
        for plan in plans
    )


@pytest.mark.asyncio
async def test_delayed_risex_applied_history_keeps_unresolved_since_boundary(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(seconds=120)
    with PaperRepository(tmp_path / "delayed-applied.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=adapters(clock, settlement_at=target), clock=clock,
        ) as runtime:
            await activate_with_live_streams(runtime, clock)
            clock.advance(1)
            order = runtime.broker.state.order
            runtime.mark_trade_stream_connected(order.venue, order.canonical_market)
            await runtime.deliver_trade(maker_trade(runtime, clock.now(), "delayed-entry"))
            delayed = DelayedAppliedRisexAdapter(target)
            runtime.adapters[Venue.RISEX] = delayed
            runtime.next_health_check_at = NOW + timedelta(hours=1)
            clock.value = target + timedelta(seconds=1)
            runtime.next_position_monitor_at = clock.now()
            await runtime.tick()
            clock.advance(10)
            runtime.next_position_monitor_at = clock.now()
            await runtime.tick()
            rows = [row for row in runtime.lifecycle.snapshot.settlements if row.venue is Venue.RISEX]
    assert delayed.calls == [target, target]
    assert len(rows) == 1 and rows[0].status is SettlementStatus.APPLIED_RATE


@pytest.mark.asyncio
async def test_safe_shutdown_cancels_only_virtual_entry_and_preserves_open_position(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(seconds=120)
    with PaperRepository(tmp_path / "shutdown.db") as repository:
        async with PublicPaperRuntime(
            repository,
            adapters=adapters(clock, settlement_at=target),
            clock=clock,
        ) as runtime:
            await activate_with_live_streams(runtime, clock)
            assert runtime.broker is not None
            await runtime.shutdown()
            state = repository.load_runtime()
            report = repository.report(as_of=clock.now())
            cancellations = paper_entry_cancellations(repository)
    assert state.lifecycle_state is LifecycleState.FLAT
    assert state.order.status.value == "CANCELLED"
    assert len(cancellations) == 1
    assert cancellations[0]["cancellation_reason"] == (
        "PAPER_ORDER_CANCELLED_PROCESS_RESTART"
    )
    assert report["last_runtime_event"]["event_type"] == "STOPPED_SAFE"
    assert report["last_runtime_event"]["detail"]["forced_close"] is False


@pytest.mark.asyncio
async def test_restore_persists_and_notifies_process_restart_entry_cancellation(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(seconds=120)
    delivery = CaptureNotifications()
    with PaperRepository(tmp_path / "restore-entry-cancel.db") as repository:
        original = PublicPaperRuntime(
            repository, adapters=adapters(clock, settlement_at=target), clock=clock
        )
        await original.__aenter__()
        await activate_with_live_streams(original, clock)
        restart_at = clock.now() + timedelta(seconds=4)
        replacement = PublicPaperRuntime(
            repository,
            adapters=adapters(clock, settlement_at=target),
            clock=clock,
            notifications=NotificationOutbox(delivery),
        )
        await replacement._restore(restart_at)
        await original._cancel_entry_cutoff_deadline()
        state = repository.load_runtime()
        cancellations = paper_entry_cancellations(repository)
    assert state.lifecycle_state is LifecycleState.FLAT
    assert state.order.cancellation_reason.value == (
        "PAPER_ORDER_CANCELLED_PROCESS_RESTART"
    )
    assert len(cancellations) == 1
    assert D(cancellations[0]["active_duration_seconds"]) == D("4")
    assert [row.kind for row in delivery.rows] == ["ENTRY_CANCELLED_NO_FILL"]


@pytest.mark.asyncio
async def test_safe_shutdown_preserves_actually_open_position(tmp_path):
    clock = FakeClock()
    with PaperRepository(tmp_path / "shutdown-open.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=adapters(clock, settlement_at=NOW + timedelta(seconds=120)), clock=clock,
        ) as runtime:
            await activate_with_live_streams(runtime, clock)
            clock.advance(1)
            order = runtime.broker.state.order
            runtime.mark_trade_stream_connected(order.venue, order.canonical_market)
            await runtime.deliver_trade(maker_trade(runtime, clock.now(), "open-at-shutdown"))
            await runtime.shutdown()
            state = repository.load_runtime()
            report = repository.report(as_of=clock.now())
    assert state.position is not None
    assert report["last_runtime_event"]["detail"]["open_position_preserved"] is True


@pytest.mark.asyncio
async def test_safe_stop_bounds_gated_io_and_high_rate_frames_with_open_exit(tmp_path):
    clock = FakeClock()
    with PaperRepository(tmp_path / "bounded-safe-stop.db") as repository:
        await run_fixture({"scenario": "exiting_aggressive_open"}, repository)
        state = repository.load_runtime()
        runtime = PublicPaperRuntime(
            repository, adapters=adapters(clock, settlement_at=NOW), clock=clock,
        )
        runtime.lifecycle = LifecycleEngine.from_snapshot(state)
        runtime._stop_event = asyncio.Event()
        position_hedge = state.hedge_market
        assert position_hedge.venue_symbol in runtime._required_symbols(
            position_hedge.venue
        )
        gate = asyncio.Event()

        async def gated_io():
            await gate.wait()

        async def high_rate_frames():
            symbol = "UNRELATED-PERP"
            session_id = stream_session(
                runtime, Venue.NADO, symbol, "combined"
            )
            await runtime.recover_snapshot(OrderBook(
                Venue.NADO, symbol, (BookLevel(D("10"), D("1")),),
                (BookLevel(D("11"), D("1")),), clock.now(), 0,
            ))
            sequence = 1
            while not runtime._stop_event.is_set():
                await runtime.apply_book_event(BookDelta(
                    Venue.NADO, symbol, (BookLevel(D("10"), D("1")),), (),
                    clock.now(), sequence, sequence - 1,
                ), stream_session_id=session_id)
                sequence += 1
                if sequence % 100 == 0:
                    await asyncio.sleep(0)

        runtime._refresh_task = asyncio.create_task(gated_io())
        gated_session = stream_session(
            runtime, Venue.NADO, "GATED", "book"
        )
        gated_episode = runtime._new_recovery_episode(
            (Venue.NADO, "GATED"), gated_session
        )
        gated_episode.task = asyncio.create_task(gated_io())
        runtime._stream_tasks[Venue.NADO, "*", "combined"] = asyncio.create_task(
            high_rate_frames()
        )
        started = time.monotonic()
        await asyncio.wait_for(runtime.shutdown(), timeout=2)
        elapsed = time.monotonic() - started
        stop_row = repository.connection.execute(
            "SELECT evidence_id,detail FROM runtime_evidence "
            "WHERE event_type='STOPPED_SAFE' ORDER BY evidence_id DESC LIMIT 1"
        ).fetchone()
        after_stop = repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence WHERE evidence_id>?",
            (stop_row["evidence_id"],),
        ).fetchone()[0]
        integrity = repository.connection.execute("PRAGMA integrity_check").fetchone()[0]
        restored = repository.load_runtime()
    assert elapsed < 2
    assert restored.position is not None
    assert json.loads(stop_row["detail"])["forced_close"] is False
    assert after_stop == 0
    assert integrity == "ok"


@pytest.mark.asyncio
@pytest.mark.parametrize("stop_path", ("external", "SIGTERM"))
async def test_run_cancels_blocked_tick_before_safe_shutdown(tmp_path, stop_path):
    clock = FakeClock()
    external_stop = asyncio.Event()
    with PaperRepository(tmp_path / f"run-stop-{stop_path}.db") as repository:
        await run_fixture({"scenario": "exiting_aggressive_open"}, repository)
        runtime = PublicPaperRuntime(
            repository, adapters=adapters(clock, settlement_at=NOW), clock=clock,
        )
        tick_started = asyncio.Event()
        tick_cancelled = asyncio.Event()
        gate = asyncio.Event()

        async def gated_tick(_at=None):
            tick_started.set()
            try:
                await gate.wait()
            except asyncio.CancelledError:
                tick_cancelled.set()
                raise

        runtime.tick = gated_tick
        run_task = asyncio.create_task(runtime.run(stop_event=external_stop))
        await tick_started.wait()

        async def high_rate_frames():
            symbol = "UNRELATED-RUN-PERP"
            session_id = stream_session(
                runtime, Venue.NADO, symbol, "combined"
            )
            await runtime.recover_snapshot(OrderBook(
                Venue.NADO, symbol, (BookLevel(D("10"), D("1")),),
                (BookLevel(D("11"), D("1")),), clock.now(), 0,
            ))
            sequence = 1
            while not runtime._stop_event.is_set():
                await runtime.apply_book_event(BookDelta(
                    Venue.NADO, symbol, (BookLevel(D("10"), D("1")),), (),
                    clock.now(), sequence, sequence - 1,
                ), stream_session_id=session_id)
                sequence += 1
                if sequence % 100 == 0:
                    await asyncio.sleep(0)

        producer = asyncio.create_task(high_rate_frames())
        runtime._stream_tasks[Venue.NADO, "run-load", "combined"] = producer
        started = time.monotonic()
        if stop_path == "external":
            external_stop.set()
        else:
            runtime._request_stop("SIGTERM")
        result = await asyncio.wait_for(run_task, timeout=2)
        elapsed = time.monotonic() - started
        rows = repository.connection.execute(
            "SELECT evidence_id,event_type,detail FROM runtime_evidence "
            "ORDER BY evidence_id"
        ).fetchall()
        restored = repository.load_runtime()
        integrity = repository.connection.execute("PRAGMA integrity_check").fetchone()[0]
    assert result == {"status": "STOPPED_SAFE", "forced_close": False}
    assert tick_cancelled.is_set()
    assert elapsed < 2
    assert restored.position is not None
    assert rows[-1]["event_type"] == "STOPPED_SAFE"
    assert not any(
        row["event_type"].startswith("PUBLIC_SOCKET_")
        or row["event_type"].startswith("PUBLIC_STREAM_CONFIRMATION_")
        for row in rows
    )
    assert integrity == "ok"


@pytest.mark.asyncio
async def test_unrelated_book_delta_does_not_evaluate_or_persist_open_position(tmp_path):
    clock = FakeClock()
    with PaperRepository(tmp_path / "unrelated-position-delta.db") as repository:
        async with PublicPaperRuntime(
            repository,
            adapters=adapters(clock, settlement_at=NOW + timedelta(seconds=120)),
            clock=clock,
        ) as runtime:
            await activate_with_live_streams(runtime, clock)
            clock.advance(1)
            order = runtime.broker.state.order
            runtime.mark_trade_stream_connected(order.venue, order.canonical_market)
            await runtime.deliver_trade(maker_trade(runtime, clock.now(), "unrelated-entry"))
            assert runtime.lifecycle is not None
            before = runtime.lifecycle.snapshot
            before_updated = repository.runtime_updated_at()
            await runtime.recover_snapshot(OrderBook(
                Venue.NADO, "UNRELATED-PERP",
                (BookLevel(D("10"), D("1")),),
                (BookLevel(D("11"), D("1")),), clock.now(), 1,
            ))
            session_id = stream_session(
                runtime, Venue.NADO, "UNRELATED-PERP", "combined"
            )
            await runtime.apply_book_event(BookDelta(
                Venue.NADO, "UNRELATED-PERP",
                (BookLevel(D("10"), D("2")),), (), clock.now(), 2, 1,
            ), stream_session_id=session_id)
            assert runtime.lifecycle.snapshot == before
            assert repository.runtime_updated_at() == before_updated


@pytest.mark.asyncio
async def test_ten_thousand_recovery_deltas_are_memory_bounded_and_episode_only(tmp_path):
    clock = FakeClock()
    with PaperRepository(tmp_path / "bounded-recovery.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=adapters(clock, settlement_at=NOW), clock=clock,
        ) as runtime:
            key = (Venue.NADO, "BTC-PERP")
            session_id = stream_session(runtime, *key, "combined")
            episode = runtime._new_recovery_episode(key, session_id)
            for sequence in range(1, 10_001):
                await runtime.apply_book_event(BookDelta(
                    *key, (BookLevel(D("10"), D("1")),), (),
                    clock.now(), sequence, sequence - 1,
                ), stream_session_id=session_id)
            assert len(episode.buffer) <= 2048
            assert repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence "
                "WHERE event_type='PUBLIC_RECOVERY_DELTA_BUFFERED'"
            ).fetchone()[0] == 0
            assert repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence "
                "WHERE event_type='PUBLIC_SNAPSHOT_RECOVERY_OVERFLOW'"
            ).fetchone()[0] == 1


@pytest.mark.asyncio
async def test_overflow_rejects_pre_overflow_snapshot_and_uses_new_boundary(tmp_path):
    clock = FakeClock()
    fakes = adapters(clock, settlement_at=NOW)
    first_started = asyncio.Event()
    second_started = asyncio.Event()
    first_gate = asyncio.Event()
    second_gate = asyncio.Event()
    calls = 0

    async def gated_book(symbol):
        nonlocal calls
        calls += 1
        call = calls
        if call == 1:
            first_started.set()
            try:
                await first_gate.wait()
            except asyncio.CancelledError:
                await first_gate.wait()
            price = D("1")
        else:
            second_started.set()
            await second_gate.wait()
            price = D("10")
        return OrderBook(
            Venue.NADO, symbol, (BookLevel(price, D("1")),),
            (BookLevel(price + 1, D("1")),), clock.now(), 0,
        )

    fakes[Venue.NADO].fetch_book = gated_book
    with PaperRepository(tmp_path / "overflow-boundary.db") as repository:
        async with PublicPaperRuntime(repository, adapters=fakes, clock=clock) as runtime:
            runtime._stop_event = asyncio.Event()
            key = (Venue.NADO, "BTC-PERP")
            session_id = stream_session(runtime, *key, "combined")
            episode = runtime._start_snapshot_recovery(
                *key, displaced_stream_session_id=session_id
            )
            old_task = episode.task
            assert old_task is not None
            await first_started.wait()
            for sequence in range(1, 2050):
                await runtime.apply_book_event(BookDelta(
                    *key, (BookLevel(D("2"), D("1")),), (),
                    clock.now(), sequence, sequence - 1,
                ), stream_session_id=session_id)
            await second_started.wait()
            new_task = episode.task
            assert new_task is not None
            first_gate.set()
            second_gate.set()
            await asyncio.gather(old_task, return_exceptions=True)
            await new_task
            book = runtime.coordinator.stream(*key).book()
            rows = repository.connection.execute(
                "SELECT event_type FROM runtime_evidence WHERE event_type LIKE "
                "'PUBLIC_SNAPSHOT_RECOVERY_%' ORDER BY evidence_id"
            ).fetchall()
    assert book.bids[0].canonical_price == D("10")
    assert [row["event_type"] for row in rows].count(
        "PUBLIC_SNAPSHOT_RECOVERY_OVERFLOW"
    ) == 1
    assert [row["event_type"] for row in rows].count(
        "PUBLIC_SNAPSHOT_RECOVERY_COMPLETED"
    ) == 1


@pytest.mark.asyncio
async def test_real_runtime_restart_restores_open_position_with_offline_gap(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(seconds=120)
    database = tmp_path / "restart-public.db"
    with PaperRepository(database) as repository:
        async with PublicPaperRuntime(
            repository,
            adapters=adapters(clock, settlement_at=target),
            clock=clock,
        ) as runtime:
            await activate_with_live_streams(runtime, clock)
            clock.advance(1)
            order = runtime.broker.state.order
            runtime.mark_trade_stream_connected(order.venue, order.canonical_market)
            await runtime.deliver_trade(maker_trade(runtime, clock.now(), "restart-entry"))
            assert runtime.lifecycle is not None
        clock.advance(30)
        async with PublicPaperRuntime(
            repository,
            adapters=(restart_adapters := adapters(clock, settlement_at=target)),
            clock=clock,
        ) as restarted:
            await restarted.scan()
            await restarted._restore(clock.now())
            assert restarted.lifecycle is not None
            assert restarted.lifecycle.snapshot.position is not None
            assert restarted.lifecycle.snapshot.gap_count == 1
            assert restarted.lifecycle.snapshot.data_quality.value == "DEGRADED"
            assert "settlements" in restart_adapters[Venue.RISEX].calls
            assert "settlements" in restart_adapters[Venue.EXTENDED].calls


@pytest.mark.asyncio
@pytest.mark.parametrize("history_outcome", ("empty", "error", "exact"))
async def test_restart_extended_elapsed_history_is_exact_or_unresolved(
    tmp_path, history_outcome,
):
    clock = FakeClock()
    target = NOW + timedelta(seconds=120)
    database = tmp_path / f"restart-history-{history_outcome}.db"
    with PaperRepository(database) as repository:
        async with PublicPaperRuntime(
            repository, adapters=adapters(clock, settlement_at=target), clock=clock,
        ) as runtime:
            await activate_with_live_streams(runtime, clock)
            clock.advance(1)
            order = runtime.broker.state.order
            runtime.mark_trade_stream_connected(order.venue, order.canonical_market)
            await runtime.deliver_trade(maker_trade(runtime, clock.now(), "history-entry"))
            risex = next(
                row for row in runtime.lifecycle.snapshot.settlements
                if row.venue is Venue.RISEX
            )
            extended = next(
                row for row in runtime.lifecycle.snapshot.settlements
                if row.venue is Venue.EXTENDED
            )
            assert extended.status is SettlementStatus.PENDING
            opened_at = runtime.lifecycle.snapshot.position.position_opened_at
            await runtime.deliver_settlement(replace(
                risex, status=SettlementStatus.APPLIED_RATE, cash_usd=D("4.25")
            ))
            if history_outcome == "empty":
                await runtime.deliver_settlement(replace(
                    extended, status=SettlementStatus.ESTIMATED, cash_usd=D("9.5")
                ))

        clock.value = target + timedelta(seconds=1)
        persisted_before = repository.load_runtime()
        persisted_rows = {row.key: row for row in persisted_before.settlements}
        assert persisted_rows[extended.key].status in {
            SettlementStatus.PENDING, SettlementStatus.ESTIMATED,
        }
        restart_adapters = adapters(clock, settlement_at=target)
        history_calls = []
        history_rows = []

        async def extended_history(_market, *, since, until):
            history_calls.append((since, until))
            assert since <= opened_at
            if history_outcome == "error":
                raise TimeoutError("synthetic history timeout")
            if history_outcome == "exact":
                history_rows.append(replace(
                    extended, status=SettlementStatus.APPLIED_RATE,
                    cash_usd=D("7.125"),
                ))
                return tuple(history_rows)
            return ()

        restart_adapters[Venue.EXTENDED].fetch_applied_settlements = extended_history
        async with PublicPaperRuntime(
            repository, adapters=restart_adapters, clock=clock,
        ) as restarted:
            await restarted.scan()
            await restarted._restore(clock.now())
            assert len(history_calls) == 1
            if history_outcome == "exact":
                assert len(history_rows) == 1
            assert restarted.lifecycle.snapshot.hedge_market.venue is Venue.EXTENDED
            rows = {row.key: row for row in restarted.lifecycle.snapshot.settlements}
            if history_rows:
                assert history_rows[0].key in rows
            assert rows[risex.key].status is SettlementStatus.APPLIED_RATE
            assert rows[risex.key].cash_usd == D("4.25")
            if history_outcome == "exact":
                assert rows[extended.key].status is SettlementStatus.APPLIED_RATE
                assert rows[extended.key].cash_usd == D("7.125")
            else:
                assert rows[extended.key].status is SettlementStatus.UNRESOLVED
                assert rows[extended.key].cash_usd is None
            assert repository.connection.execute(
                "SELECT COUNT(*) FROM funding_settlements WHERE venue='RISEX' "
                "AND status='APPLIED_RATE'"
            ).fetchone()[0] == 1


@pytest.mark.asyncio
async def test_all_route_initial_catalog_is_ready_before_next_full(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(minutes=10)
    risex = ManyFakeAdapter(Venue.RISEX, clock, settlement_at=target)
    nado = ManyFakeAdapter(Venue.NADO, clock, settlement_at=target)
    precise_tick = D("0.000000000000000001")
    precise_step = D("0.000000000001")
    risex.many_markets = tuple(replace(
        market, tick_size_raw=precise_tick, quantity_step_raw=precise_step,
        minimum_quantity_raw=precise_step, minimum_notional_usd=D("0"),
    ) for market in risex.many_markets)

    class TransitionExtended(ExtendedAdapter):
        def __init__(self):
            super().__init__(None)
            source = ManyFakeAdapter(Venue.EXTENDED, clock, settlement_at=target)
            self.rows = tuple(replace(
                market, tick_size_raw=precise_tick,
                quantity_step_raw=precise_step,
                minimum_quantity_raw=precise_step,
                minimum_notional_usd=D("0"),
            ) for market in source.many_markets)
            self.source = source
            self.catalog_started = asyncio.Event()
            self.catalog_gate = asyncio.Event()

        def volumes(self, rows):
            return tuple(MarketVolume(
                Venue.EXTENDED, market.venue_symbol, D("1000000"),
                clock.now(), "official-shaped",
            ) for market in rows)

        async def fetch_catalog(self):
            self.catalog_started.set()
            await self.catalog_gate.wait()
            return self.rows, self.volumes(self.rows)

        async def fetch_required_catalog(self, venue_symbols):
            wanted = set(venue_symbols)
            rows = tuple(row for row in self.rows if row.venue_symbol in wanted)
            assert len(rows) == len(wanted)
            return rows, self.volumes(rows)

        async def fetch_book(self, venue_symbol):
            return OrderBook(
                Venue.EXTENDED, venue_symbol,
                (BookLevel(D("8650.514169552906858181"), D("20")),),
                (BookLevel(D("8651.509081348565110708"), D("20")),),
                clock.now(), 1,
            )

        async def fetch_funding_quote(self, market, *, assumed_open_at):
            return await self.source.fetch_funding_quote(
                market, assumed_open_at=assumed_open_at,
            )

        def unknown_funding_quote(self, market, *, observed_at, assumed_open_at):
            return self.source.unknown_funding_quote(
                market, observed_at=observed_at, assumed_open_at=assumed_open_at,
            )

    extended = TransitionExtended()

    async def precise_risex_book(venue_symbol):
        return OrderBook(
            Venue.RISEX, venue_symbol,
            (BookLevel(D("8649.670545193748488977"), D("20")),),
            (BookLevel(D("8650.561362195468293120"), D("20")),),
            clock.now(), 1,
        )

    risex.fetch_book = precise_risex_book
    with PaperRepository(tmp_path / "nado-to-extended-transition.db") as repository:
        async with PublicPaperRuntime(
            repository,
            adapters={Venue.RISEX: risex, Venue.EXTENDED: extended, Venue.NADO: nado},
            clock=clock,
        ) as runtime:
            runtime._stop_event = asyncio.Event()
            extended.catalog_gate.set()
            initial = await runtime.scan(scan_kind="INITIAL")
            assert len(initial["routes"]) == 20
            assert {row["hedge_venue"] for row in initial["routes"]} == {
                "EXTENDED", "NADO",
            }
            await runtime._refresh_public_data()
            for venue, symbol in tuple(runtime.observations):
                adapter = runtime.adapters[venue]
                await runtime.recover_snapshot(await adapter.fetch_book(symbol))
                runtime.mark_trade_stream_connected(venue, symbol, at=clock.now())
                if venue is Venue.EXTENDED:
                    for kind in ("book", "trade", "funding"):
                        confirm_extended_stream(runtime,
                            symbol, kind, clock.now(), data_ready=True,
                        )

            runtime.next_full_scan_at = NOW + timedelta(seconds=120)
            runtime.next_health_check_at = NOW + timedelta(seconds=130)
            clock.advance(120)
            for venue, symbol in runtime.observations:
                runtime.coordinator.stream(venue, symbol).connection_confirmed(
                    clock.now()
                )
                if venue is Venue.EXTENDED:
                    for kind in ("book", "trade", "funding"):
                        confirm_extended_stream(runtime,
                            symbol, kind, clock.now(), data_ready=False,
                        )
            await runtime.tick(clock.now())
            assert runtime._refresh_task is not None
            await runtime._refresh_task
            await runtime.tick(clock.now())
            routes = repository.report(as_of=clock.now())["latest_routes"]
            plans = runtime.last_scan.evaluations
            fatal_count = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence "
                "WHERE event_type IN ('RUNTIME_FATAL','RUNTIME_STOPPED_FATAL')"
            ).fetchone()[0]
            await runtime.shutdown()

    assert len(plans) == len(routes) == 20
    assert {row["hedge_venue"] for row in routes} == {"EXTENDED", "NADO"}
    assert all(row["planned_maker_net_pnl_usd"] is not None for row in routes)
    assert all(
        plan.planned_entry_execution_pnl_usd
        + plan.planned_exit_execution_pnl_usd
        == plan.planned_execution_pnl_usd
        for plan in plans
    )
    assert all(
        set(row["blockers"]) <= {"PLANNED_NET_PNL_NEGATIVE"}
        for row in routes
    )
    assert fatal_count == 0


def _stabilization002_book(
    venue: Venue,
    symbol: str,
    at: datetime,
    sequence: int | None = 1,
    *,
    bid: str = "99",
    ask: str = "101",
    quantity: str = "20",
) -> OrderBook:
    return OrderBook(
        venue, symbol,
        (BookLevel(D(bid), D(quantity)),),
        (BookLevel(D(ask), D(quantity)),), at, sequence,
    )


async def _stabilization002_seed_position_books(
    runtime: PublicPaperRuntime, at: datetime
) -> None:
    lifecycle = runtime.lifecycle
    assert lifecycle is not None
    snapshot = lifecycle.snapshot
    runtime.lifecycle = None
    for market in (snapshot.risex_market, snapshot.hedge_market):
        book = _stabilization002_book(
            market.venue, market.venue_symbol, at, quantity="10"
        )
        runtime.observations[market.venue, market.venue_symbol] = MarketObservation(
            market,
            MarketVolume(
                market.venue, market.venue_symbol, D("1000"), at, "fixture"
            ),
            book,
            FundingCashQuote(
                market.venue, market.venue_symbol, at, at,
                at + timedelta(hours=1), FundingQuality.PREDICTED,
                FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT, True,
                D("0"), D("0"), "fixture",
            ),
            StreamHealth(at, at, True, True, True, DataQuality.COMPLETE),
        )
        await runtime.recover_snapshot(book, at=at)
        runtime.mark_trade_stream_connected(
            market.venue, market.venue_symbol, at=at
        )
    runtime.lifecycle = lifecycle


async def _stabilization002_position_runtime(
    repository: PaperRepository, clock: FakeClock
) -> PublicPaperRuntime:
    await run_fixture({"scenario": "exiting_aggressive_open"}, repository)
    runtime = PublicPaperRuntime(
        repository, adapters=adapters(clock, settlement_at=NOW), clock=clock
    )
    runtime.lifecycle = LifecycleEngine.from_snapshot(repository.load_runtime())
    await _stabilization002_seed_position_books(runtime, NOW)
    return runtime


async def _stabilization002_persist_gap(
    runtime: PublicPaperRuntime, repository: PaperRepository, at: datetime
) -> None:
    lifecycle = runtime.lifecycle
    assert lifecycle is not None
    candidate = lifecycle.detached()
    await candidate.start_gap(started_at=at)
    repository.save_decision(
        recorded_at=at, lifecycle_snapshot=candidate.snapshot
    )
    lifecycle.publish_candidate(candidate)


def _stabilization002_hedge_delta(
    runtime: PublicPaperRuntime, at: datetime, *, sequence: int = 2
) -> BookDelta:
    lifecycle = runtime.lifecycle
    assert lifecycle is not None
    market = lifecycle.snapshot.hedge_market
    assert market.venue is Venue.EXTENDED
    return BookDelta(
        Venue.EXTENDED,
        market.venue_symbol,
        (BookLevel(D("100"), D("10")),),
        (),
        at,
        sequence,
        None,
    )


@pytest.mark.asyncio
async def test_relevant_book_event_recovers_open_gap_when_both_legs_are_fresh(
    tmp_path,
):
    clock = FakeClock()
    with PaperRepository(tmp_path / "relevant-book-gap-recovery.db") as repository:
        runtime = await _stabilization002_position_runtime(repository, clock)
        await _stabilization002_persist_gap(runtime, repository, clock.now())
        clock.advance(1)
        at = clock.now()
        lifecycle = runtime.lifecycle
        assert lifecycle is not None
        symbol = lifecycle.snapshot.hedge_market.venue_symbol
        session_id = stream_session(runtime, Venue.EXTENDED, symbol, "book")

        assert await runtime.apply_book_event(
            _stabilization002_hedge_delta(runtime, at),
            stream_session_id=session_id,
        )

        assert runtime.lifecycle is not None
        after = runtime.lifecycle.snapshot
        assert not after.gap_open
        assert after.gaps[-1].ended_at == at
        assert repository.load_runtime() == after


@pytest.mark.asyncio
async def test_lighter_relevant_book_event_recovers_without_trade_stream_membership(
    tmp_path,
):
    clock = FakeClock(NOW + timedelta(minutes=59))
    target = NOW + timedelta(hours=1)
    initial_timestamp = int(clock.now().timestamp() * 1000)
    initial_stats = lighter_market_stats_all_snapshot(
        timestamp=initial_timestamp,
        funding_timestamp=int(NOW.timestamp() * 1000),
    )
    lighter_market = CanonicalMarket(
        "ETH", Venue.LIGHTER, "ETH", MarketType.PERPETUAL,
        ContractType.LINEAR, D("1"), "USDC", "USDC", D("1"), D("1"),
        D("1"), D("10"), None, True, False, False,
    )
    lighter = BoundaryLighterAdapter(clock, lighter_market, initial_stats)
    risex = FakeAdapter(
        Venue.RISEX, clock, settlement_at=target, funding_cash="5"
    )
    risex.market = replace(
        risex.market, canonical_asset="ETH", venue_symbol="ETH-RISEX"
    )
    with PaperRepository(tmp_path / "lighter-relevant-book-gap.db") as repository:
        runtime = PublicPaperRuntime(
            repository,
            adapters={Venue.RISEX: risex, Venue.LIGHTER: lighter},
            clock=clock,
        )
        await runtime.scan()
        assert runtime.last_scan is not None
        assert runtime.last_scan.winner is not None
        lighter_session_id = runtime._new_stream_session(
            (Venue.LIGHTER, "*", "combined")
        )
        assert await runtime.apply_book_event(
            OrderBook(
                Venue.LIGHTER,
                "ETH",
                (BookLevel(D("99"), D("20")),),
                (BookLevel(D("101"), D("20")),),
                clock.now(),
                1,
            ),
            stream_session_id=lighter_session_id,
        )
        broker = PaperEntryBroker()
        await broker.activate(
            runtime.last_scan,
            attempt_id="lighter-relevant-book-gap",
            activated_at=clock.now(),
        )
        runtime.broker = broker
        confirm_public_streams(runtime, clock.now())
        assert (Venue.LIGHTER, "ETH") not in runtime._trade_stream_ready
        clock.advance(1)
        await runtime.deliver_trade(
            maker_trade(runtime, clock.now(), "lighter-relevant-book-entry")
        )
        assert runtime.lifecycle is not None
        assert runtime.lifecycle.snapshot.hedge_market.venue is Venue.LIGHTER
        assert (Venue.LIGHTER, "ETH") not in runtime._trade_stream_ready
        await _stabilization002_persist_gap(runtime, repository, clock.now())

        clock.advance(1)
        at = clock.now()
        assert await runtime.apply_book_event(
            BookDelta(
                Venue.LIGHTER,
                "ETH",
                (BookLevel(D("100"), D("20")),),
                (),
                at,
                2,
                1,
            ),
            stream_session_id=lighter_session_id,
        )

        assert runtime.lifecycle is not None
        after = runtime.lifecycle.snapshot
        assert not after.gap_open
        assert after.gaps[-1].ended_at == at
        assert (Venue.LIGHTER, "ETH") not in runtime._trade_stream_ready
        assert repository.load_runtime() == after


@pytest.mark.asyncio
async def test_relevant_book_event_keeps_gap_open_when_other_leg_is_stale(
    tmp_path,
):
    clock = FakeClock()
    with PaperRepository(tmp_path / "relevant-book-gap-stale-leg.db") as repository:
        runtime = await _stabilization002_position_runtime(repository, clock)
        await _stabilization002_persist_gap(runtime, repository, clock.now())
        clock.advance(26)
        at = clock.now()
        lifecycle = runtime.lifecycle
        assert lifecycle is not None
        hedge_symbol = lifecycle.snapshot.hedge_market.venue_symbol
        runtime.coordinator.stream(
            Venue.EXTENDED, hedge_symbol
        ).connection_confirmed(at)
        session_id = stream_session(
            runtime, Venue.EXTENDED, hedge_symbol, "book"
        )

        assert await runtime.apply_book_event(
            _stabilization002_hedge_delta(runtime, at),
            stream_session_id=session_id,
        )

        assert runtime.lifecycle is not None
        after = runtime.lifecycle.snapshot
        assert after.gap_open
        assert after == repository.load_runtime()
        assert not any(
            event.event_type is LifecycleEventType.GAP_ENDED
            for event in after.events
        )


@pytest.mark.asyncio
async def test_relevant_book_event_keeps_gap_open_after_other_leg_invalidation(
    tmp_path,
):
    clock = FakeClock()
    with PaperRepository(tmp_path / "relevant-book-gap-invalidation.db") as repository:
        runtime = await _stabilization002_position_runtime(repository, clock)
        lifecycle = runtime.lifecycle
        assert lifecycle is not None
        risex_symbol = lifecycle.snapshot.risex_market.venue_symbol
        risex_session_id = stream_session(
            runtime, Venue.RISEX, risex_symbol, "combined"
        )
        await runtime.mark_disconnected(
            Venue.RISEX,
            risex_symbol,
            at=clock.now(),
            stream_kind="trade",
            stream_session_id=risex_session_id,
        )
        assert runtime.lifecycle is not None
        assert runtime.lifecycle.snapshot.gap_open
        assert runtime.coordinator.stream(
            Venue.RISEX, risex_symbol
        ).health(clock.now()).data_quality is DataQuality.COMPLETE
        assert (Venue.RISEX, risex_symbol) not in runtime._trade_stream_ready

        clock.advance(1)
        at = clock.now()
        hedge_symbol = runtime.lifecycle.snapshot.hedge_market.venue_symbol
        hedge_session_id = stream_session(
            runtime, Venue.EXTENDED, hedge_symbol, "book"
        )
        assert await runtime.apply_book_event(
            _stabilization002_hedge_delta(runtime, at),
            stream_session_id=hedge_session_id,
        )

        assert runtime.lifecycle is not None
        after = runtime.lifecycle.snapshot
        assert after.gap_open
        assert after == repository.load_runtime()
        assert not any(
            event.event_type is LifecycleEventType.GAP_ENDED
            for event in after.events
        )


def _stabilization002_fail_save(repository: PaperRepository) -> None:
    def fail_save(**_kwargs) -> None:
        raise sqlite3.OperationalError("synthetic lifecycle checkpoint failure")

    repository.save_decision = fail_save


def _stabilization002_lifecycle_rows(repository: PaperRepository):
    tables = (
        "runtime_state", "positions", "orders", "order_versions",
        "position_samples", "gaps", "lifecycle_events", "completed_trades",
        "fills", "processed_trade_events",
    )
    return {
        table: tuple(
            tuple(row) for row in repository.connection.execute(
                f"SELECT * FROM {table} ORDER BY rowid"
            )
        )
        for table in tables
    }


def _stabilization002_recovery_events(repository: PaperRepository):
    rows = repository.connection.execute(
        "SELECT event_type, detail FROM runtime_evidence "
        "WHERE event_type IN ('PUBLIC_SNAPSHOT_RECOVERY_STARTED', "
        "'PUBLIC_SNAPSHOT_RECOVERY_FAILED', "
        "'PUBLIC_SNAPSHOT_RECOVERY_COMPLETED') "
        "ORDER BY evidence_id"
    ).fetchall()
    return [(row["event_type"], json.loads(row["detail"])) for row in rows]


def _stabilization002_observable_state(runtime: PublicPaperRuntime):
    component_rows = tuple(sorted(
        (
            venue.value, component, row.available, row.detail, row.updated_at,
        )
        for venue, components in runtime.component_readiness.items()
        for component, row in components.items()
    ))
    venue_rows = tuple(sorted(
        (venue.value, row.available, row.detail, row.updated_at)
        for venue, row in runtime.readiness.items()
    ))
    projections = []
    for venue, symbol in sorted(
        runtime.observations, key=lambda key: (key[0].value, key[1])
    ):
        stream = runtime.coordinator.stream(venue, symbol)
        projections.append((
            venue.value, symbol, stream.book(), stream.health(runtime.clock.now()),
            runtime.observations[venue, symbol],
        ))
    return (
        component_rows,
        venue_rows,
        frozenset(runtime._trade_stream_ready),
        frozenset(runtime._live_book_ready),
        tuple(projections),
    )


def _stabilization002_assert_failed_recovery_successor(
    runtime: PublicPaperRuntime,
    repository: PaperRepository,
    venue: Venue,
    symbol: str,
    failed,
    successor_session,
) -> None:
    successor = runtime._recoveries[venue, symbol]
    assert successor is not failed
    assert successor.episode_id != failed.episode_id
    assert successor.attempt_generation != failed.attempt_generation
    assert successor.owned_stream_session_id == successor_session
    assert successor.terminal == "COMPLETE"
    assert runtime.coordinator.stream(venue, symbol).book() is not None
    readiness = runtime.component_readiness[venue]
    assert all(
        readiness[f"{component}:{symbol}"].available
        for component in ("book", "trade", "funding", "connection_combined")
    )
    events = _stabilization002_recovery_events(repository)
    assert [event for event, _ in events] == [
        "PUBLIC_SNAPSHOT_RECOVERY_STARTED",
        "PUBLIC_SNAPSHOT_RECOVERY_FAILED",
        "PUBLIC_SNAPSHOT_RECOVERY_STARTED",
        "PUBLIC_SNAPSHOT_RECOVERY_COMPLETED",
    ]
    assert events[0][1]["episode_id"] != events[2][1]["episode_id"]
    assert events[2][1]["stream_session_id"] == successor_session.value


def _stabilization002_install_closing_socket(
    runtime: PublicPaperRuntime,
) -> None:
    stop = asyncio.Event()
    runtime._session = SingleWebSocketSession(
        ClosingWebSocket(stop, stop_on_iteration=True)
    )
    runtime._stop_event = stop


@pytest.mark.asyncio
async def test_stabilization002_nado_failed_rest_recovery_restarts_on_physical_session(
    tmp_path,
):
    clock = FakeClock()
    symbol = "ABC-NADO"
    adapter = CombinedFakeAdapter(Venue.NADO, clock, settlement_at=NOW)
    fetch_calls = 0

    async def fetch_book(_symbol):
        nonlocal fetch_calls
        fetch_calls += 1
        if fetch_calls <= 3:
            raise RuntimeError(f"snapshot failure {fetch_calls}")
        return _stabilization002_book(Venue.NADO, symbol, clock.now(), 10)

    async def no_delay(_seconds):
        await asyncio.sleep(0)

    adapter.fetch_book = fetch_book
    with PaperRepository(tmp_path / "stabilization002-nado-liveness.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.NADO: adapter}, clock=clock,
            sleep=no_delay,
        )
        runtime._stop_event = asyncio.Event()
        stream_key = (Venue.NADO, "*", "combined")
        first_session = runtime._new_stream_session(stream_key)
        await runtime.recover_snapshot(
            _stabilization002_book(Venue.NADO, symbol, clock.now()),
            at=clock.now(),
        )
        assert not await runtime.apply_book_event(
            BookDelta(
                Venue.NADO, symbol, (), (), clock.now(), 3, 2,
            ),
            stream_session_id=first_session,
        )
        failed = runtime._recoveries[(Venue.NADO, symbol)]
        assert failed.task is not None
        await failed.task
        assert failed.terminal == "FAILED"
        assert fetch_calls == 3

        _stabilization002_install_closing_socket(runtime)
        successor_session = runtime._new_stream_session(stream_key)
        await runtime._combined_stream(
            Venue.NADO, adapter, (symbol,), successor_session
        )

        assert fetch_calls == 4
        _stabilization002_assert_failed_recovery_successor(
            runtime, repository, Venue.NADO, symbol, failed,
            successor_session,
        )


@pytest.mark.asyncio
async def test_stabilization002_obsolete_nado_recovery_cannot_publish_after_session_loss(
    tmp_path,
):
    clock = FakeClock()
    symbol = "ABC-NADO"
    adapter = CombinedFakeAdapter(Venue.NADO, clock, settlement_at=NOW)
    fetch_count = 0

    async def fetch_book(_symbol):
        nonlocal fetch_count
        fetch_count += 1
        sequence = 10 if fetch_count == 1 else 20
        return _stabilization002_book(
            Venue.NADO, symbol, clock.now(), sequence,
            bid=str(90 + sequence), ask=str(92 + sequence),
        )

    adapter.fetch_book = fetch_book
    built = asyncio.Event()
    release_old = asyncio.Event()
    with PaperRepository(tmp_path / "stabilization002-obsolete-publish.db") as repository:
        runtime = await _stabilization002_position_runtime(repository, clock)
        runtime.adapters[Venue.NADO] = adapter
        runtime._stop_event = asyncio.Event()
        stream_key = (Venue.NADO, "*", "combined")
        first_session = runtime._new_stream_session(stream_key)
        await runtime.recover_snapshot(
            _stabilization002_book(Venue.NADO, symbol, clock.now()),
            at=clock.now(),
        )
        original_publish_recovery_snapshot = runtime._publish_recovery_snapshot

        async def gated_publish_recovery_snapshot(
            key, episode, generation, recovered, **kwargs
        ):
            if (
                key == (Venue.NADO, symbol)
                and episode.owned_stream_session_id == first_session
            ):
                built.set()
                try:
                    await release_old.wait()
                except asyncio.CancelledError:
                    await release_old.wait()
            return await original_publish_recovery_snapshot(
                key, episode, generation, recovered, **kwargs
            )

        runtime._publish_recovery_snapshot = gated_publish_recovery_snapshot
        assert not await runtime.apply_book_event(
            BookDelta(Venue.NADO, symbol, (), (), clock.now(), 3, 2),
            stream_session_id=first_session,
        )
        obsolete = runtime._recoveries[Venue.NADO, symbol]
        assert obsolete.task is not None
        await built.wait()

        _stabilization002_install_closing_socket(runtime)
        successor_session = runtime._new_stream_session(stream_key)
        await runtime._combined_stream(
            Venue.NADO, adapter, (symbol,), successor_session
        )
        before_release = _stabilization002_observable_state(runtime)
        lifecycle_before = runtime.lifecycle.snapshot
        evidence_before = repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence"
        ).fetchone()[0]
        episode_before = runtime._recoveries[Venue.NADO, symbol]
        terminal_before = episode_before.terminal

        release_old.set()
        await obsolete.task

        assert _stabilization002_observable_state(runtime) == before_release
        assert runtime.lifecycle.snapshot == lifecycle_before
        assert repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence"
        ).fetchone()[0] == evidence_before
        assert runtime._recoveries[Venue.NADO, symbol] is episode_before
        assert episode_before.terminal == terminal_before
        assert not any(
            event == "PUBLIC_SNAPSHOT_RECOVERY_COMPLETED"
            and detail["episode_id"] == obsolete.episode_id.value
            for event, detail in _stabilization002_recovery_events(repository)
        )


@pytest.mark.asyncio
async def test_stabilization002_active_nado_recovery_is_replaced_by_physical_session(
    tmp_path,
):
    clock = FakeClock()
    symbol = "ABC-NADO"
    adapter = CombinedFakeAdapter(Venue.NADO, clock, settlement_at=NOW)
    first_fetch_started = asyncio.Event()
    release_first_fetch = asyncio.Event()
    second_fetch_started = asyncio.Event()
    release_second_fetch = asyncio.Event()
    fetch_count = 0

    async def fetch_book(_symbol):
        nonlocal fetch_count
        fetch_count += 1
        if fetch_count == 1:
            first_fetch_started.set()
            try:
                await release_first_fetch.wait()
            except asyncio.CancelledError:
                await release_first_fetch.wait()
            sequence = 10
        else:
            second_fetch_started.set()
            await release_second_fetch.wait()
            sequence = 20
        return _stabilization002_book(
            Venue.NADO, symbol, clock.now(), sequence
        )

    adapter.fetch_book = fetch_book
    with PaperRepository(tmp_path / "stabilization002-active-replaced.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.NADO: adapter}, clock=clock
        )
        runtime._stop_event = asyncio.Event()
        stream_key = (Venue.NADO, "*", "combined")
        first_session = runtime._new_stream_session(stream_key)
        await runtime.recover_snapshot(
            _stabilization002_book(Venue.NADO, symbol, clock.now()),
            at=clock.now(),
        )
        assert not await runtime.apply_book_event(
            BookDelta(Venue.NADO, symbol, (), (), clock.now(), 3, 2),
            stream_session_id=first_session,
        )
        first = runtime._recoveries[Venue.NADO, symbol]
        assert first.task is not None
        await first_fetch_started.wait()

        _stabilization002_install_closing_socket(runtime)
        successor_session = runtime._new_stream_session(stream_key)
        successor_reader = asyncio.create_task(runtime._combined_stream(
            Venue.NADO, adapter, (symbol,), successor_session
        ))
        try:
            await second_fetch_started.wait()
            successor = runtime._recoveries[Venue.NADO, symbol]
            assert successor is not first
            assert successor.episode_id != first.episode_id
            assert successor.attempt_generation != first.attempt_generation
            assert successor.owned_stream_session_id == successor_session
            readiness = runtime.component_readiness.get(Venue.NADO, {})
            assert all(
                (row := readiness.get(f"{component}:{symbol}")) is None
                or not row.available
                for component in (
                    "book", "trade", "funding", "connection_combined"
                )
            )
            release_second_fetch.set()
            await successor_reader
            successor = runtime._recoveries[Venue.NADO, symbol]
            assert successor.terminal == "COMPLETE"
            readiness = runtime.component_readiness[Venue.NADO]
            assert all(
                readiness[f"{component}:{symbol}"].available
                for component in (
                    "book", "trade", "funding", "connection_combined"
                )
            )
            evidence_before_old_release = _stabilization002_recovery_events(
                repository
            )
            release_first_fetch.set()
            await first.task
            assert (
                _stabilization002_recovery_events(repository)
                == evidence_before_old_release
            )
            assert runtime._recoveries[Venue.NADO, symbol] is successor
            assert successor.terminal == "COMPLETE"
            assert [event for event, _ in evidence_before_old_release] == [
                "PUBLIC_SNAPSHOT_RECOVERY_STARTED",
                "PUBLIC_SNAPSHOT_RECOVERY_STARTED",
                "PUBLIC_SNAPSHOT_RECOVERY_COMPLETED",
            ]
        finally:
            release_first_fetch.set()
            release_second_fetch.set()
            if not successor_reader.done():
                successor_reader.cancel()
            cleanup = [successor_reader]
            if first.task is not None and not first.task.done():
                first.task.cancel()
            if first.task is not None:
                cleanup.append(first.task)
            await asyncio.gather(*cleanup, return_exceptions=True)


@pytest.mark.asyncio
async def test_stabilization002_queued_extended_startup_snapshot_is_session_fenced(
    tmp_path,
):
    clock = FakeClock()
    delivered = asyncio.Event()

    class StartupSnapshotSocket(TextWebSocket):
        async def __anext__(self):
            message = await super().__anext__()
            delivered.set()
            return message

        async def ping(self):
            return None

    class ControlledSnapshotSocket:
        def __init__(self, payload) -> None:
            self.payload = payload
            self.release_frame = asyncio.Event()
            self.after_frame = asyncio.Event()
            self.finish = asyncio.Event()
            self.delivered = False

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return None

        def __aiter__(self):
            return self

        async def __anext__(self):
            if not self.delivered:
                await self.release_frame.wait()
                self.delivered = True
                return SimpleNamespace(
                    type=aiohttp.WSMsgType.TEXT,
                    data=json.dumps(self.payload),
                )
            self.after_frame.set()
            await self.finish.wait()
            raise StopAsyncIteration

        async def ping(self):
            return None

    with PaperRepository(tmp_path / "stabilization002-startup-fence.db") as repository:
        runtime = await _stabilization002_position_runtime(repository, clock)
        symbol = runtime.lifecycle.snapshot.hedge_market.venue_symbol
        key = (Venue.EXTENDED, symbol, "book")
        stream = runtime.coordinator.stream(Venue.EXTENDED, symbol)
        stream.disconnected()
        runtime._live_book_ready.discard((Venue.EXTENDED, symbol))
        runtime._set_component_readiness(
            Venue.EXTENDED, f"book:{symbol}", False,
            "PUBLIC_BOOK_DATA_PENDING", clock.now(),
        )
        assert (Venue.EXTENDED, symbol) not in runtime._recoveries
        timestamp_ms = str(int(clock.now().timestamp() * 1000))
        payload = {
            "type": "SNAPSHOT", "seq": 10, "ts": timestamp_ms,
            "data": {
                "m": symbol,
                "b": [{"p": "89", "q": "20"}],
                "a": [{"p": "91", "q": "20"}],
            },
        }
        successor_payload = {
            **payload, "seq": 20,
            "data": {
                "m": symbol,
                "b": [{"p": "109", "q": "20"}],
                "a": [{"p": "111", "q": "20"}],
            },
        }
        first_stop = asyncio.Event()
        runtime._session = SingleWebSocketSession(
            StartupSnapshotSocket(first_stop, (payload,))
        )
        runtime._stop_event = first_stop
        first_session = runtime._new_stream_session(key)
        await runtime._position_event_lock.acquire()
        first_reader = asyncio.create_task(runtime._extended_stream(
            ExtendedAdapter(None), symbol, "book", first_session
        ))
        second_reader = None
        try:
            await delivered.wait()
            await asyncio.sleep(0)
            assert not first_reader.done()

            successor_socket = ControlledSnapshotSocket(successor_payload)
            second_transport = SingleWebSocketSession(successor_socket)
            runtime._session = second_transport
            runtime._stop_event = asyncio.Event()
            second_session = runtime._new_stream_session(key)
            second_reader = asyncio.create_task(runtime._extended_stream(
                ExtendedAdapter(None), symbol, "book", second_session
            ))
            await asyncio.sleep(0)
            assert second_transport.connections == 1
            before_release = _stabilization002_observable_state(runtime)
            lifecycle_before = runtime.lifecycle.snapshot
            evidence_before = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence"
            ).fetchone()[0]
            book_before = stream.book()
            health_before = stream.health(clock.now())

            runtime._position_event_lock.release()
            await first_reader

            assert _stabilization002_observable_state(runtime) == before_release
            assert runtime.lifecycle.snapshot == lifecycle_before
            assert repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence"
            ).fetchone()[0] == evidence_before
            assert stream.book() == book_before
            assert stream.health(clock.now()) == health_before
            assert (Venue.EXTENDED, symbol) not in runtime._live_book_ready
            assert not runtime.component_readiness[Venue.EXTENDED][
                f"book:{symbol}"
            ].available
            assert (Venue.EXTENDED, symbol) not in runtime._recoveries

            successor_socket.release_frame.set()
            await successor_socket.after_frame.wait()
            successor_book = stream.book()
            assert successor_book is not None
            assert successor_book.sequence == 20
            assert stream.health(clock.now()).stream_connected
            assert stream.health(clock.now()).last_connection_confirmation_at == clock.now()
            assert (Venue.EXTENDED, symbol) in runtime._live_book_ready
            assert runtime.component_readiness[Venue.EXTENDED][
                f"book:{symbol}"
            ].available
            assert runtime.readiness[Venue.EXTENDED].available
            assert runtime.observations[Venue.EXTENDED, symbol].book == successor_book
            assert (Venue.EXTENDED, symbol) not in runtime._recoveries
            successor_socket.finish.set()
            runtime._stop_event.set()
            await second_reader
        finally:
            if runtime._position_event_lock.locked():
                runtime._position_event_lock.release()
            if not first_reader.done():
                first_reader.cancel()
            if second_reader is not None and not second_reader.done():
                second_reader.cancel()
            cleanup = [first_reader]
            if second_reader is not None:
                cleanup.append(second_reader)
            await asyncio.gather(*cleanup, return_exceptions=True)


class Stabilization002RisexAdapter(RisexAdapter):
    def __init__(self, clock: FakeClock, symbol: str) -> None:
        super().__init__(None)
        self.clock = clock
        self.symbol = symbol
        self._market_ids = {symbol: "1"}
        self._symbols_by_id = {"1": symbol}
        self._raw_markets = {
            symbol: {
                "market_id": "1",
                "config": {
                    "name": symbol, "step_size": "1", "step_price": "1",
                    "min_order_size": "1",
                },
            }
        }

    async def fetch_book(self, venue_symbol: str) -> OrderBook:
        assert venue_symbol == self.symbol
        return _stabilization002_book(
            Venue.RISEX, venue_symbol, self.clock.now(), None
        )


@pytest.mark.asyncio
async def test_stabilization002_risex_overflow_failed_recovery_restarts_on_ws_snapshot(
    tmp_path,
):
    clock = FakeClock()
    symbol = "ABC/USDC"
    adapter = Stabilization002RisexAdapter(clock, symbol)

    class SendSocket:
        def __init__(self) -> None:
            self.sent = []

        async def send_json(self, payload) -> None:
            self.sent.append(payload)

    with PaperRepository(tmp_path / "stabilization002-risex-liveness.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.RISEX: adapter}, clock=clock
        )
        runtime._stop_event = asyncio.Event()
        stream_key = (Venue.RISEX, "*", "combined")
        failed_session = runtime._new_stream_session(stream_key)
        await runtime._resubscribe_risex_orderbooks(
            SendSocket(), adapter, (symbol,), triggering_symbol=symbol,
            stream_session_id=failed_session,
        )
        failed = runtime._recoveries[(Venue.RISEX, symbol)]
        for overflow in range(3):
            for offset in range(2048):
                sequence = overflow * 2049 + offset + 1
                assert not await runtime.apply_book_event(
                    BookDelta(
                        Venue.RISEX, symbol, (), (), clock.now(), sequence,
                        sequence - 1,
                    ),
                    stream_session_id=failed_session,
                )
            sequence = (overflow + 1) * 2049
            assert not await runtime.apply_book_event(
                BookDelta(
                    Venue.RISEX, symbol, (), (), clock.now(), sequence,
                    sequence - 1,
                ),
                stream_session_id=failed_session,
            )
        assert failed.terminal == "FAILED"
        assert failed.overflows == 3

        timestamp_ns = str(int(clock.now().timestamp() * 1_000_000_000))
        snapshot_payload = {
            "method": "snapshot", "channel": "orderbook", "type": "snapshot",
            "market_id": "1", "worker_timestamp": timestamp_ns,
            "data": {
                "market_id": 1,
                "bids": [{"price": "99", "quantity": "20"}],
                "asks": [{"price": "101", "quantity": "20"}],
            },
        }

        class SingleSnapshotSocket(TextWebSocket):
            async def send_json(self, _payload):
                return None

            async def __anext__(self):
                message = await super().__anext__()
                self.stop_event.set()
                return message

        stop = asyncio.Event()
        runtime._session = SingleWebSocketSession(
            SingleSnapshotSocket(stop, (snapshot_payload,))
        )
        runtime._stop_event = stop
        successor_session = runtime._new_stream_session(stream_key)
        await runtime._combined_stream(
            Venue.RISEX, adapter, (symbol,), successor_session
        )

        _stabilization002_assert_failed_recovery_successor(
            runtime, repository, Venue.RISEX, symbol, failed,
            successor_session,
        )


@pytest.mark.asyncio
async def test_stabilization002_r1_recovery_repository_failure_is_atomic(
    tmp_path,
):
    clock = FakeClock()
    with PaperRepository(tmp_path / "stabilization002-r1-recovery-atomic.db") as repository:
        runtime = await _stabilization002_position_runtime(repository, clock)
        lifecycle = runtime.lifecycle
        assert lifecycle is not None
        await lifecycle.start_gap(started_at=clock.now())
        repository.save_decision(
            recorded_at=clock.now(), lifecycle_snapshot=lifecycle.snapshot
        )
        market = lifecycle.snapshot.risex_market
        key = (market.venue, market.venue_symbol)
        session_id = runtime._new_stream_session((Venue.RISEX, "*", "combined"))
        episode = runtime._new_recovery_episode(key, session_id)
        runtime._record_recovery_started(key, episode)
        recovered = _stabilization002_book(
            market.venue, market.venue_symbol, clock.now(), 60,
            bid="98", ask="102", quantity="20",
        )
        lifecycle_before = lifecycle.snapshot
        persisted_before = repository.load_runtime()
        rows_before = _stabilization002_lifecycle_rows(repository)
        observable_before = _stabilization002_observable_state(runtime)
        evidence_before = repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence"
        ).fetchone()[0]
        terminal_before = episode.terminal
        original_save_lifecycle = repository._save_lifecycle
        notifications = []
        runtime._notify_outage = lambda *args, **kwargs: notifications.append(
            (args, kwargs)
        )

        def fail_after_lifecycle_rows(snapshot, recorded_at):
            original_save_lifecycle(snapshot, recorded_at)
            raise sqlite3.OperationalError("synthetic post-lifecycle-write failure")

        repository._save_lifecycle = fail_after_lifecycle_rows

        with pytest.raises(sqlite3.OperationalError):
            await runtime._publish_recovery_snapshot(
                key,
                episode,
                episode.attempt_generation,
                recovered,
                at=clock.now(),
                buffered=0,
                replayed=0,
                source="WS_RESUBSCRIBE_SNAPSHOT",
            )

        assert _stabilization002_lifecycle_rows(repository) == rows_before
        assert repository.load_runtime() == persisted_before
        assert lifecycle.snapshot == lifecycle_before
        assert _stabilization002_observable_state(runtime) == observable_before
        assert episode.terminal == terminal_before
        assert repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence"
        ).fetchone()[0] == evidence_before
        assert episode.buffer == []
        assert notifications == []

        repository._save_lifecycle = original_save_lifecycle
        assert await runtime._publish_recovery_snapshot(
            key, episode, episode.attempt_generation, recovered,
            at=clock.now(), buffered=0, replayed=0,
            source="WS_RESUBSCRIBE_SNAPSHOT",
        )
        assert episode.terminal == "COMPLETE"
        assert repository.load_runtime() == lifecycle.snapshot
        assert repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence "
            "WHERE event_type='PUBLIC_SNAPSHOT_RECOVERY_COMPLETED'"
        ).fetchone()[0] == 1
        assert len(notifications) == 1


@pytest.mark.asyncio
async def test_stabilization002_r1_terminal_evidence_failure_rolls_back_and_retries(
    tmp_path,
):
    clock = FakeClock()
    with PaperRepository(tmp_path / "stabilization002-r1-evidence-atomic.db") as repository:
        runtime = await _stabilization002_position_runtime(repository, clock)
        lifecycle = runtime.lifecycle
        await lifecycle.start_gap(started_at=clock.now())
        repository.save_decision(
            recorded_at=clock.now(), lifecycle_snapshot=lifecycle.snapshot
        )
        market = lifecycle.snapshot.risex_market
        key = (market.venue, market.venue_symbol)
        session_id = runtime._new_stream_session((Venue.RISEX, "*", "combined"))
        episode = runtime._new_recovery_episode(key, session_id)
        runtime._record_recovery_started(key, episode)
        recovered = _stabilization002_book(
            market.venue, market.venue_symbol, clock.now(), 60,
            bid="98", ask="102", quantity="20",
        )
        lifecycle_before = lifecycle.snapshot
        persisted_before = repository.load_runtime()
        rows_before = _stabilization002_lifecycle_rows(repository)
        observable_before = _stabilization002_observable_state(runtime)
        evidence_before = repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence"
        ).fetchone()[0]
        original_insert = repository._insert_runtime_evidence
        notifications = []
        runtime._notify_outage = lambda *args, **kwargs: notifications.append(
            (args, kwargs)
        )

        def fail_terminal_evidence(*args):
            original_insert(*args)
            raise sqlite3.OperationalError("synthetic terminal-evidence failure")

        repository._insert_runtime_evidence = fail_terminal_evidence
        with pytest.raises(sqlite3.OperationalError):
            await runtime._publish_recovery_snapshot(
                key, episode, episode.attempt_generation, recovered,
                at=clock.now(), buffered=0, replayed=0,
                source="WS_RESUBSCRIBE_SNAPSHOT",
            )
        assert _stabilization002_lifecycle_rows(repository) == rows_before
        assert repository.load_runtime() == persisted_before
        assert lifecycle.snapshot == lifecycle_before
        assert _stabilization002_observable_state(runtime) == observable_before
        assert episode.terminal is None and episode.buffer == []
        assert repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence"
        ).fetchone()[0] == evidence_before
        assert notifications == []

        repository._insert_runtime_evidence = original_insert
        assert await runtime._publish_recovery_snapshot(
            key, episode, episode.attempt_generation, recovered,
            at=clock.now(), buffered=0, replayed=0,
            source="WS_RESUBSCRIBE_SNAPSHOT",
        )
        assert repository.load_runtime() == lifecycle.snapshot
        assert repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence "
            "WHERE event_type='PUBLIC_SNAPSHOT_RECOVERY_COMPLETED'"
        ).fetchone()[0] == 1
        assert len(notifications) == 1


@pytest.mark.asyncio
async def test_stabilization002_r1_readiness_failure_rolls_back_and_retries(
    tmp_path,
):
    clock = FakeClock()
    with PaperRepository(tmp_path / "stabilization002-r1-readiness-atomic.db") as repository:
        runtime = await _stabilization002_position_runtime(repository, clock)
        lifecycle = runtime.lifecycle
        await lifecycle.start_gap(started_at=clock.now())
        repository.save_decision(
            recorded_at=clock.now(), lifecycle_snapshot=lifecycle.snapshot
        )
        market = lifecycle.snapshot.risex_market
        key = (market.venue, market.venue_symbol)
        runtime._set_component_readiness(
            market.venue, f"book:{market.venue_symbol}", False,
            "PUBLIC_STREAM_GAP", clock.now(),
        )
        session_id = runtime._new_stream_session((Venue.RISEX, "*", "combined"))
        episode = runtime._new_recovery_episode(key, session_id)
        runtime._record_recovery_started(key, episode)
        recovered = _stabilization002_book(
            market.venue, market.venue_symbol, clock.now(), 60,
            bid="98", ask="102", quantity="20",
        )
        lifecycle_before = lifecycle.snapshot
        persisted_before = repository.load_runtime()
        rows_before = _stabilization002_lifecycle_rows(repository)
        observable_before = _stabilization002_observable_state(runtime)
        buffer_before = tuple(episode.buffer)
        evidence_before = repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence"
        ).fetchone()[0]
        readiness_before = tuple(repository.connection.execute(
            "SELECT venue,updated_at,available,detail FROM venue_readiness "
            "WHERE venue=?", (market.venue.value,),
        ).fetchone())
        notifications = []
        runtime._notify_outage = lambda *args, **kwargs: notifications.append(
            (args, kwargs)
        )
        repository.connection.execute(
            "CREATE TRIGGER fail_recovery_readiness BEFORE UPDATE ON venue_readiness "
            "BEGIN SELECT RAISE(FAIL, 'synthetic readiness write failure'); END"
        )

        with pytest.raises(sqlite3.IntegrityError):
            await runtime._publish_recovery_snapshot(
                key, episode, episode.attempt_generation, recovered,
                at=clock.now(), buffered=0, replayed=0,
                source="WS_RESUBSCRIBE_SNAPSHOT",
            )

        assert _stabilization002_lifecycle_rows(repository) == rows_before
        assert repository.load_runtime() == persisted_before
        assert lifecycle.snapshot == lifecycle_before
        assert _stabilization002_observable_state(runtime) == observable_before
        assert episode.terminal is None
        assert tuple(episode.buffer) == buffer_before
        assert repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence"
        ).fetchone()[0] == evidence_before
        assert tuple(repository.connection.execute(
            "SELECT venue,updated_at,available,detail FROM venue_readiness "
            "WHERE venue=?", (market.venue.value,),
        ).fetchone()) == readiness_before
        assert repository._settlement_cache is None
        assert repository._processed_key_cache is None
        assert notifications == []

        repository.connection.execute("DROP TRIGGER fail_recovery_readiness")
        component_before_success = dict(
            runtime.component_readiness[market.venue]
        )
        assert await runtime._publish_recovery_snapshot(
            key, episode, episode.attempt_generation, recovered,
            at=clock.now(), buffered=0, replayed=0,
            source="WS_RESUBSCRIBE_SNAPSHOT",
        )
        persisted_readiness = repository.connection.execute(
            "SELECT updated_at,available,detail FROM venue_readiness WHERE venue=?",
            (market.venue.value,),
        ).fetchone()
        live_readiness = runtime.readiness[market.venue]
        changed_components = {
            f"book:{market.venue_symbol}",
            f"connection_book:{market.venue_symbol}",
        }
        assert {
            name: row for name, row in runtime.component_readiness[market.venue].items()
            if name not in changed_components
        } == {
            name: row for name, row in component_before_success.items()
            if name not in changed_components
        }
        for name in changed_components:
            row = runtime.component_readiness[market.venue][name]
            assert (row.available, row.detail, row.updated_at) == (
                True, "PUBLIC_STREAM_RECOVERED", clock.now()
            )
        assert persisted_readiness[0] == live_readiness.updated_at.isoformat()
        assert bool(persisted_readiness[1]) is live_readiness.available is True
        assert persisted_readiness[2] == live_readiness.detail == "PUBLIC_STREAM_RECOVERED"
        assert repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence "
            "WHERE event_type='PUBLIC_SNAPSHOT_RECOVERY_COMPLETED'"
        ).fetchone()[0] == 1
        assert len(notifications) == 1


@pytest.mark.asyncio
async def test_stabilization002_r9_periodic_candidate_cannot_regress_later_gap(
    tmp_path,
):
    clock = FakeClock()
    with PaperRepository(tmp_path / "stabilization002-r9-frontier.db") as repository:
        runtime = await _stabilization002_position_runtime(repository, clock)
        runtime.next_health_check_at = NOW + timedelta(hours=1)
        runtime.next_full_scan_at = NOW + timedelta(hours=1)
        runtime.last_scan = SimpleNamespace(logical_at=NOW)
        runtime.next_position_monitor_at = NOW
        lifecycle = runtime.lifecycle
        entered = asyncio.Event()
        release = asyncio.Event()
        notifications = []
        runtime._notify_lifecycle_transition = lambda *args: notifications.append(args)
        original_evaluate = LifecycleEngine.evaluate

        async def gated_candidate(engine, **kwargs):
            if engine is not lifecycle:
                entered.set()
                await release.wait()
            return await original_evaluate(engine, **kwargs)

        LifecycleEngine.evaluate = gated_candidate
        tick = asyncio.create_task(runtime.tick(NOW))
        try:
            await entered.wait()
            later = NOW + timedelta(seconds=5)
            clock.value = later
            await lifecycle.start_gap(started_at=later)
            repository.save_decision(
                recorded_at=later, lifecycle_snapshot=lifecycle.snapshot
            )
            authoritative = lifecycle.snapshot
            checkpoint_at = repository.runtime_updated_at()
            release.set()
            await tick
        finally:
            release.set()
            LifecycleEngine.evaluate = original_evaluate
        assert runtime.lifecycle is lifecycle
        assert lifecycle.snapshot == authoritative
        assert lifecycle.snapshot.gap_open
        assert repository.load_runtime() == authoritative
        assert repository.runtime_updated_at() == checkpoint_at == later
        assert notifications == []


@pytest.mark.asyncio
async def test_stabilization002_r11_stale_exit_trade_is_wholly_inert(tmp_path):
    clock = FakeClock()
    with PaperRepository(tmp_path / "stabilization002-r11-stale-exit.db") as repository:
        runtime = await _stabilization002_position_runtime(repository, clock)
        lifecycle = runtime.lifecycle
        before = lifecycle.snapshot
        v1 = before.exit_order.active_version
        runtime.mark_trade_stream_connected(
            before.exit_order.venue, before.exit_order.canonical_market, at=NOW
        )
        stream = runtime.coordinator.stream(
            before.exit_order.venue, before.exit_order.canonical_market
        )
        trade = TradeEvidence(
            "stabilization002-r11-v1", before.exit_order.venue,
            before.exit_order.canonical_market, NOW, NOW, "r11-v1",
            before.exit_order.canonical_quantity,
            v1.limit_price + before.hedge_market.tick_size_raw,
            Side.BUY, True,
        )
        entered = asyncio.Event()
        release = asyncio.Event()
        original_process = LifecycleEngine.process_exit_trade

        async def gated_process(engine, *args, **kwargs):
            if engine is not lifecycle:
                entered.set()
                await release.wait()
            return await original_process(engine, *args, **kwargs)

        LifecycleEngine.process_exit_trade = gated_process
        delivery = asyncio.create_task(runtime.deliver_trade(
            trade, observed_version_id=v1.version_id, processed_at=NOW
        ))
        await entered.wait()
        later = NOW + timedelta(seconds=20)
        clock.value = later
        risex = runtime._observation(
            before.risex_market.venue, before.risex_market.venue_symbol, later
        )
        hedge = runtime._observation(
            before.hedge_market.venue, before.hedge_market.venue_symbol, later
        )
        await lifecycle.evaluate(
            evaluated_at=later,
            risex_observation=replace(
                risex, book=replace(risex.book, observed_at=later)
            ),
            hedge_observation=replace(hedge, book=replace(
                hedge.book,
                bids=(BookLevel(D("89"), D("10")),),
                asks=(BookLevel(D("91"), D("10")),),
                observed_at=later,
            )),
        )
        assert lifecycle.snapshot.exit_order.active_version.version_id != v1.version_id
        repository.save_decision(
            recorded_at=later, lifecycle_snapshot=lifecycle.snapshot
        )
        authoritative = lifecycle.snapshot
        rows_before_release = _stabilization002_lifecycle_rows(repository)
        confirmation_before = stream.health(clock.now()).last_connection_confirmation_at
        release.set()
        try:
            await delivery
        finally:
            LifecycleEngine.process_exit_trade = original_process
        assert lifecycle.snapshot == authoritative
        assert trade.trade_event_key not in lifecycle.snapshot.processed_trade_keys
        assert _stabilization002_lifecycle_rows(repository) == rows_before_release
        assert stream.health(clock.now()).last_connection_confirmation_at == confirmation_before
        current = lifecycle.snapshot.exit_order.active_version
        runtime.mark_trade_stream_connected(
            lifecycle.snapshot.exit_order.venue,
            lifecycle.snapshot.exit_order.canonical_market,
            at=later,
        )
        await runtime.deliver_trade(
            replace(
                trade,
                exchange_timestamp=later,
                received_at=later,
                canonical_price=current.limit_price
                + lifecycle.snapshot.hedge_market.tick_size_raw,
            ),
            observed_version_id=current.version_id,
            processed_at=later,
        )
        assert trade.trade_event_key in repository._processed_key_cache
        assert repository.connection.execute(
            "SELECT COUNT(*) FROM processed_trade_events WHERE trade_event_key=?",
            (trade.trade_event_key,),
        ).fetchone()[0] == 1


@pytest.mark.asyncio
async def test_stabilization002_r12_ws_trade_keeps_receipt_version(tmp_path):
    clock = FakeClock()
    with PaperRepository(tmp_path / "stabilization002-r12-receipt-version.db") as repository:
        runtime = await _stabilization002_position_runtime(repository, clock)
        lifecycle = runtime.lifecycle
        before = lifecycle.snapshot
        v1 = before.exit_order.active_version
        runtime.mark_trade_stream_connected(
            before.exit_order.venue, before.exit_order.canonical_market, at=NOW
        )
        trade = TradeEvidence(
            "stabilization002-r12-v1", before.exit_order.venue,
            before.exit_order.canonical_market, NOW, NOW, "r12-v1",
            before.exit_order.canonical_quantity,
            v1.limit_price + before.hedge_market.tick_size_raw,
            Side.BUY, True,
        )
        entered = asyncio.Event()
        release = asyncio.Event()
        captured_versions = []
        original_process = LifecycleEngine.process_exit_trade

        async def gated_process(engine, *args, **kwargs):
            if engine is not lifecycle:
                captured_versions.append(kwargs["observed_version_id"])
                entered.set()
                await release.wait()
            return await original_process(engine, *args, **kwargs)

        LifecycleEngine.process_exit_trade = gated_process
        delivery = asyncio.create_task(runtime.deliver_trade(trade))
        await entered.wait()
        later = NOW + timedelta(seconds=20)
        clock.value = later
        risex = runtime._observation(
            before.risex_market.venue, before.risex_market.venue_symbol, later
        )
        hedge = runtime._observation(
            before.hedge_market.venue, before.hedge_market.venue_symbol, later
        )
        await lifecycle.evaluate(
            evaluated_at=later,
            risex_observation=replace(
                risex, book=replace(risex.book, observed_at=later)
            ),
            hedge_observation=replace(hedge, book=replace(
                hedge.book,
                bids=(BookLevel(D("89"), D("10")),),
                asks=(BookLevel(D("91"), D("10")),),
                observed_at=later,
            )),
        )
        v2 = lifecycle.snapshot.exit_order.active_version
        assert v2.version_id != v1.version_id
        repository.save_decision(
            recorded_at=later, lifecycle_snapshot=lifecycle.snapshot
        )
        release.set()
        try:
            await delivery
        finally:
            LifecycleEngine.process_exit_trade = original_process
        assert captured_versions == [v1.version_id]
        assert lifecycle.snapshot.exit_order.active_version.version_id == v2.version_id
        assert trade.trade_event_key not in lifecycle.snapshot.processed_trade_keys
        assert repository.connection.execute(
            "SELECT COUNT(*) FROM processed_trade_events WHERE trade_event_key=?",
            (trade.trade_event_key,),
        ).fetchone()[0] == 0


@pytest.mark.asyncio
async def test_stabilization002_r13_obsolete_combined_trade_is_fully_inert(
    tmp_path,
):
    clock = FakeClock()
    with PaperRepository(tmp_path / "stabilization002-r13-combined-trade.db") as repository:
        runtime, adapter = await _stabilization002_nado_entry_runtime(
            repository, clock
        )
        order = runtime.broker.state.order
        adapter.trade = maker_trade(runtime, clock.now(), "r13-obsolete-trade")
        entered = asyncio.Event()
        blocked = asyncio.Event()

        async def gated_recompute(*_args, **_kwargs):
            entered.set()
            await blocked.wait()

        runtime._recompute_funding = gated_recompute
        stop = asyncio.Event()
        runtime._session = Stabilization002ReplacementSession(
            Stabilization002MessageSocket({"channel": "trade"}), stop
        )
        runtime._stop_event = stop
        stream_key = (Venue.NADO, "*", "combined")
        old_session = runtime._new_stream_session(stream_key)
        old_task = asyncio.create_task(runtime._combined_stream(
            Venue.NADO, adapter, (order.canonical_market,), old_session
        ))
        await entered.wait()
        broker_before = runtime.broker.state
        lifecycle_before = runtime.lifecycle
        fills_before = repository.connection.execute(
            "SELECT COUNT(*) FROM fills"
        ).fetchone()[0]
        receipts_before = repository.connection.execute(
            "SELECT COUNT(*) FROM processed_trade_events"
        ).fetchone()[0]
        await _stabilization002_cancel_combined_for_real_replacement(
            runtime, Venue.NADO, adapter, old_task
        )
        observable_after_replacement = _stabilization002_observable_state(runtime)
        evidence_after_replacement = tuple(repository.connection.execute(
            "SELECT event_type,detail FROM runtime_evidence ORDER BY evidence_id"
        ).fetchall())
        await asyncio.sleep(0)
        assert runtime._stream_sessions[stream_key] != old_session
        assert runtime.broker.state == broker_before
        assert runtime.lifecycle is lifecycle_before
        assert repository.connection.execute(
            "SELECT COUNT(*) FROM fills"
        ).fetchone()[0] == fills_before
        assert repository.connection.execute(
            "SELECT COUNT(*) FROM processed_trade_events"
        ).fetchone()[0] == receipts_before
        assert _stabilization002_observable_state(runtime) == observable_after_replacement
        assert tuple(repository.connection.execute(
            "SELECT event_type,detail FROM runtime_evidence ORDER BY evidence_id"
        ).fetchall()) == evidence_after_replacement


@pytest.mark.asyncio
async def test_stabilization002_r14_shutdown_awaits_displaced_recovery(
    tmp_path,
):
    clock = FakeClock()
    with PaperRepository(tmp_path / "stabilization002-r14-shutdown.db") as repository:
        runtime = await _stabilization002_position_runtime(repository, clock)
        runtime._stop_event = asyncio.Event()
        session_id = runtime._new_stream_session((Venue.NADO, "*", "combined"))
        episode = runtime._new_recovery_episode(
            (Venue.NADO, "R14-NADO"), session_id
        )
        started = asyncio.Event()
        first_cancel = asyncio.Event()
        never = asyncio.Event()

        async def displaced_owner():
            started.set()
            try:
                await never.wait()
            except asyncio.CancelledError:
                first_cancel.set()
                await never.wait()

        task = asyncio.create_task(displaced_owner())
        episode.task = task
        await started.wait()
        runtime._retire_recovery_task(task)
        episode.task = None
        await first_cancel.wait()
        assert task in runtime._retired_recovery_tasks
        runtime._request_stop("SIGINT")
        started_at = time.monotonic()
        await asyncio.wait_for(runtime.shutdown(), timeout=2)
        elapsed = time.monotonic() - started_at
        evidence_before = tuple(repository.connection.execute(
            "SELECT event_type,detail FROM runtime_evidence ORDER BY evidence_id"
        ).fetchall())
        changes_before = repository.connection.total_changes
        await asyncio.sleep(0)
        assert task.done()
        assert not runtime._retired_recovery_tasks
        assert elapsed < 2
        assert evidence_before[-1]["event_type"] == "STOPPED_SAFE"
        assert json.loads(evidence_before[-1]["detail"])["forced_close"] is False
        assert repository.connection.total_changes == changes_before
        assert tuple(repository.connection.execute(
            "SELECT event_type,detail FROM runtime_evidence ORDER BY evidence_id"
        ).fetchall()) == evidence_before


@pytest.mark.asyncio
async def test_stabilization002_r15_two_extended_recovery_cycles_are_distinct(
    tmp_path,
):
    clock = FakeClock()
    adapter = ExtendedAdapter(None)
    symbol = "ABC-EXTENDED"
    key = (Venue.EXTENDED, symbol)
    stream_key = (Venue.EXTENDED, symbol, "book")

    def ws_snapshot(sequence, price):
        return adapter.normalize_book_message({
            "type": "SNAPSHOT", "seq": sequence,
            "ts": str(int(clock.now().timestamp() * 1000)),
            "data": {
                "m": symbol,
                "b": [{"p": str(price), "q": "20"}],
                "a": [{"p": str(price + 1), "q": "20"}],
            },
        }, received_at=clock.now())

    with PaperRepository(tmp_path / "stabilization002-r15-extended.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.EXTENDED: CombinedFakeAdapter(
                Venue.EXTENDED, clock, settlement_at=NOW
            )}, clock=clock,
        )
        runtime._stop_event = asyncio.Event()
        startup_one = runtime._new_stream_session(stream_key)
        assert not await runtime.apply_book_event(
            BookDelta(*key, (), (), clock.now(), 3, None),
            stream_session_id=startup_one,
        )
        first = runtime._recoveries[key]
        if first.task is not None:
            await first.task
        assert first.terminal is None
        assert await runtime.apply_book_event(
            ws_snapshot(10, 100),
            stream_session_id=first.owned_stream_session_id,
        )
        assert first.terminal == "COMPLETE"

        startup_two = runtime._new_stream_session(stream_key)
        assert startup_two != startup_one
        assert not await runtime.apply_book_event(
            BookDelta(*key, (), (), clock.now(), 99, None),
            stream_session_id=startup_two,
        )
        second = runtime._recoveries[key]
        if second.task is not None:
            await second.task
        assert second.terminal is None
        assert second is not first
        assert second.episode_id != first.episode_id
        assert second.attempt_generation != first.attempt_generation
        assert second.owned_stream_session_id != first.owned_stream_session_id
        assert await runtime.apply_book_event(
            ws_snapshot(20, 200),
            stream_session_id=second.owned_stream_session_id,
        )
        assert second.terminal == "COMPLETE"
        events = _stabilization002_recovery_events(repository)
        assert [event for event, _ in events] == [
            "PUBLIC_SNAPSHOT_RECOVERY_STARTED",
            "PUBLIC_SNAPSHOT_RECOVERY_COMPLETED",
            "PUBLIC_SNAPSHOT_RECOVERY_STARTED",
            "PUBLIC_SNAPSHOT_RECOVERY_COMPLETED",
        ]
        assert [detail["episode_id"] for _, detail in events] == [
            first.episode_id.value,
            first.episode_id.value,
            second.episode_id.value,
            second.episode_id.value,
        ]


@pytest.mark.asyncio
async def test_stabilization002_periodic_evaluate_repository_failure_is_atomic(
    tmp_path,
):
    clock = FakeClock()
    with PaperRepository(tmp_path / "stabilization002-tick-atomic.db") as repository:
        runtime = await _stabilization002_position_runtime(repository, clock)
        runtime.next_health_check_at = NOW + timedelta(hours=1)
        runtime.next_full_scan_at = NOW + timedelta(hours=1)
        runtime.last_scan = SimpleNamespace(logical_at=NOW)
        runtime.next_position_monitor_at = NOW
        before = runtime.lifecycle.snapshot
        persisted_before = repository.load_runtime()
        scheduler_before = runtime.next_position_monitor_at
        observable_before = _stabilization002_observable_state(runtime)
        evidence_before = repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence"
        ).fetchone()[0]
        notifications = []
        runtime._notify_lifecycle_transition = lambda *args: notifications.append(args)
        _stabilization002_fail_save(repository)
        with pytest.raises(sqlite3.OperationalError):
            await runtime.tick(NOW)
        assert runtime.lifecycle.snapshot == before
        assert repository.load_runtime() == persisted_before
        assert notifications == []
        assert runtime.next_position_monitor_at == scheduler_before
        assert _stabilization002_observable_state(runtime) == observable_before
        assert repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence"
        ).fetchone()[0] == evidence_before


@pytest.mark.asyncio
async def test_stabilization002_periodic_evaluate_post_write_failure_and_retry(
    tmp_path,
):
    clock = FakeClock()
    with PaperRepository(tmp_path / "stabilization002-post-write.db") as repository:
        runtime = await _stabilization002_position_runtime(repository, clock)
        runtime.next_health_check_at = NOW + timedelta(hours=1)
        runtime.next_full_scan_at = NOW + timedelta(hours=1)
        runtime.last_scan = SimpleNamespace(logical_at=NOW)
        runtime.next_position_monitor_at = NOW
        before = runtime.lifecycle.snapshot
        scheduler_before = runtime.next_position_monitor_at
        rows_before = _stabilization002_lifecycle_rows(repository)
        notifications = []
        runtime._notify_lifecycle_transition = lambda *args: notifications.append(args)
        original_save_lifecycle = repository._save_lifecycle

        def fail_after_candidate_rows(candidate, at):
            original_save_lifecycle(candidate, at)
            raise sqlite3.OperationalError("synthetic post-write commit failure")

        repository._save_lifecycle = fail_after_candidate_rows
        try:
            with pytest.raises(sqlite3.OperationalError):
                await runtime.tick(NOW)
        finally:
            repository._save_lifecycle = original_save_lifecycle

        assert _stabilization002_lifecycle_rows(repository) == rows_before
        assert repository.load_runtime() == before
        assert repository._settlement_cache is None
        assert repository._processed_key_cache is None
        live_after_failure = runtime.lifecycle.snapshot
        assert live_after_failure == before, {
            "sample_counts": (
                len(before.samples), len(live_after_failure.samples)
            ),
            "exit_version_counts": (
                len(before.exit_order.versions),
                len(live_after_failure.exit_order.versions),
            ),
        }
        assert runtime.next_position_monitor_at == scheduler_before
        assert notifications == []

        await runtime.tick(NOW)

        assert runtime.lifecycle is not None
        after = runtime.lifecycle.snapshot
        assert after == repository.load_runtime()
        assert len(after.samples) == len(before.samples) + 1
        assert len(notifications) == 1
        rows_after = _stabilization002_lifecycle_rows(repository)
        assert len(rows_after["runtime_state"]) == 1
        assert len(rows_after["position_samples"]) == len(
            rows_before["position_samples"]
        ) + 1
        assert len(rows_after["fills"]) == len(rows_before["fills"])
        for table, primary_key_columns in {
            "orders": (0,),
            "order_versions": (0,),
            "position_samples": (0, 1),
            "gaps": (0, 1),
            "lifecycle_events": (0, 1),
            "completed_trades": (0,),
            "fills": (0,),
        }.items():
            keys = tuple(
                tuple(row[index] for index in primary_key_columns)
                for row in rows_after[table]
            )
            assert len(keys) == len(set(keys))


@pytest.mark.asyncio
async def test_stabilization002_relevant_book_repository_failure_is_atomic(
    tmp_path,
):
    clock = FakeClock()
    with PaperRepository(tmp_path / "stabilization002-book-atomic.db") as repository:
        runtime = await _stabilization002_position_runtime(repository, clock)
        snapshot = runtime.lifecycle.snapshot
        direction = snapshot.position.direction
        if direction.value == "LONG_RISEX_SHORT_HEDGE":
            prices = (("90", "92"), ("108", "110"))
        else:
            prices = (("108", "110"), ("90", "92"))
        for market, (bid, ask) in zip(
            (snapshot.risex_market, snapshot.hedge_market), prices
        ):
            runtime.coordinator.stream(market.venue, market.venue_symbol).snapshot(
                _stabilization002_book(
                    market.venue, market.venue_symbol, clock.now(), 50,
                    bid=bid, ask=ask, quantity="100",
                )
            )
        before = runtime.lifecycle.snapshot
        persisted_before = repository.load_runtime()
        scheduler_before = runtime.next_position_monitor_at
        observable_before = _stabilization002_observable_state(runtime)
        evidence_before = repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence"
        ).fetchone()[0]
        notifications = []
        runtime._notify_lifecycle_transition = lambda *args: notifications.append(args)
        _stabilization002_fail_save(repository)
        key = (snapshot.risex_market.venue, snapshot.risex_market.venue_symbol)
        with pytest.raises(sqlite3.OperationalError):
            await runtime._evaluate_relevant_book_event(key, NOW)
        assert runtime.lifecycle is not None
        assert runtime.lifecycle.snapshot == before
        assert repository.load_runtime() == persisted_before
        assert notifications == []
        assert runtime.next_position_monitor_at == scheduler_before
        assert _stabilization002_observable_state(runtime) == observable_before
        assert repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence"
        ).fetchone()[0] == evidence_before


@pytest.mark.asyncio
async def test_stabilization002_disconnect_gap_repository_failure_is_atomic(
    tmp_path,
):
    clock = FakeClock()
    with PaperRepository(tmp_path / "stabilization002-gap-atomic.db") as repository:
        runtime = await _stabilization002_position_runtime(repository, clock)
        snapshot = runtime.lifecycle.snapshot
        market = snapshot.risex_market
        assert not snapshot.gap_open
        disconnect_kwargs = {
            "stream_session_id": runtime._new_stream_session(
                (Venue.RISEX, "*", "combined")
            )
        }
        clock.advance(11)
        before = runtime.lifecycle.snapshot
        persisted_before = repository.load_runtime()
        scheduler_before = runtime.next_position_monitor_at
        target_components = {
            f"{name}:{market.venue_symbol}"
            for name in ("book", "trade", "funding", "connection_combined")
        }
        other_components_before = tuple(sorted(
            (venue.value, component, row.available, row.detail, row.updated_at)
            for venue, components in runtime.component_readiness.items()
            for component, row in components.items()
            if not (venue is market.venue and component in target_components)
        ))
        stream = runtime.coordinator.stream(market.venue, market.venue_symbol)
        book_before = stream.book()
        health_before = stream.health(clock.now())
        assert health_before.stream_connected
        evidence_id_before = repository.connection.execute(
            "SELECT COALESCE(MAX(evidence_id), 0) FROM runtime_evidence"
        ).fetchone()[0]
        notifications = []
        runtime._notify_lifecycle_transition = lambda *args: notifications.append(args)
        original_save_decision = repository.save_decision
        _stabilization002_fail_save(repository)
        try:
            with pytest.raises(sqlite3.OperationalError):
                await runtime.mark_disconnected(
                    market.venue, market.venue_symbol, stream_kind="combined",
                    **disconnect_kwargs,
                )
        finally:
            repository.save_decision = original_save_decision
        assert runtime.lifecycle.snapshot == before
        assert repository.load_runtime() == persisted_before
        assert notifications == []
        assert runtime.next_position_monitor_at == scheduler_before
        assert tuple(sorted(
            (venue.value, component, row.available, row.detail, row.updated_at)
            for venue, components in runtime.component_readiness.items()
            for component, row in components.items()
            if not (venue is market.venue and component in target_components)
        )) == other_components_before
        for component in target_components:
            row = runtime.component_readiness[market.venue][component]
            assert not row.available
            assert row.detail.startswith("PUBLIC_STREAM_DISCONNECTED:combined:")
        assert not runtime.readiness[market.venue].available
        assert (market.venue, market.venue_symbol) not in runtime._trade_stream_ready
        assert (market.venue, market.venue_symbol) not in runtime._live_book_ready
        assert book_before is not None
        assert stream.book() is None
        disconnected_health = stream.health(clock.now())
        assert not disconnected_health.stream_connected
        assert not disconnected_health.book_initialized
        assert not disconnected_health.book_sequence_valid
        assert disconnected_health.data_quality is DataQuality.DEGRADED
        evidence_after = repository.connection.execute(
            "SELECT event_type, detail FROM runtime_evidence "
            "WHERE evidence_id>? ORDER BY evidence_id",
            (evidence_id_before,),
        ).fetchall()
        assert [row["event_type"] for row in evidence_after] == [
            "VENUE_READINESS"
        ]
        assert json.loads(evidence_after[0]["detail"])["available"] is False

        await runtime.mark_disconnected(
            market.venue, market.venue_symbol, stream_kind="combined",
            **disconnect_kwargs,
        )
        assert runtime.lifecycle is not None
        committed = runtime.lifecycle.snapshot
        assert committed.gap_open
        assert len(committed.gaps) == len(before.gaps) + 1
        assert len(committed.events) == len(before.events) + 1
        assert repository.load_runtime() == committed
        assert notifications == []


class Stabilization002QueuedNadoAdapter(CombinedFakeAdapter):
    def __init__(self, clock: FakeClock, *, settlement_at: datetime) -> None:
        super().__init__(Venue.NADO, clock, settlement_at=settlement_at)
        self.trade = None
        self.quote = None

    def symbol_for_product(self, _product: int) -> str:
        return self.market.venue_symbol

    def normalize_trade(self, _payload, **_kwargs):
        assert self.trade is not None
        return self.trade

    def normalize_funding_rate_message(self, _payload, _market, **_kwargs):
        assert self.quote is not None
        return self.quote


class Stabilization002MessageSocket:
    def __init__(self, payload) -> None:
        self.payload = payload
        self.sent = []
        self.delivered = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.delivered:
            await asyncio.Event().wait()
        self.delivered = True
        return SimpleNamespace(
            type=aiohttp.WSMsgType.TEXT, data=json.dumps(self.payload)
        )

    async def send_json(self, payload) -> None:
        self.sent.append(payload)


class Stabilization002ReplacementSession:
    def __init__(self, first_socket, stop: asyncio.Event) -> None:
        self.first_socket = first_socket
        self.stop = stop
        self.connections = 0

    def ws_connect(self, *_args, **_kwargs):
        self.connections += 1
        if self.connections == 1:
            return self.first_socket
        return ClosingWebSocket(self.stop, stop_on_iteration=True)


async def _stabilization002_nado_entry_runtime(
    repository: PaperRepository, clock: FakeClock
):
    fakes = adapters(clock, settlement_at=NOW + timedelta(minutes=2))
    fakes[Venue.EXTENDED].available = False
    nado = Stabilization002QueuedNadoAdapter(
        clock, settlement_at=NOW + timedelta(minutes=2)
    )
    fakes[Venue.NADO] = nado
    runtime = PublicPaperRuntime(repository, adapters=fakes, clock=clock)
    await activate_with_live_streams(runtime, clock)
    assert runtime.broker is not None
    assert runtime.broker.state.order.venue is Venue.NADO
    return runtime, nado


async def _stabilization002_cancel_combined_for_real_replacement(
    runtime: PublicPaperRuntime,
    venue: Venue,
    adapter: PublicAdapter,
    old_task: asyncio.Task,
) -> None:
    key = (venue, "*", "combined")
    runtime._stream_tasks[key] = old_task
    runtime._combined_symbols[venue] = (f"OBSOLETE-{venue.value}",)
    runtime.adapters = {venue: adapter}
    await runtime._reconcile_combined_streams()
    replacement = runtime._stream_tasks.get(key)
    assert old_task.cancelled()
    if replacement is not None:
        replacement.cancel()
        await asyncio.gather(replacement, return_exceptions=True)
        runtime._stream_tasks.pop(key, None)


@pytest.mark.asyncio
async def test_stabilization002_queued_nado_trade_is_cancelled_on_real_replacement(
    tmp_path,
):
    clock = FakeClock()
    with PaperRepository(tmp_path / "stabilization002-queued-trade.db") as repository:
        runtime, adapter = await _stabilization002_nado_entry_runtime(
            repository, clock
        )
        order = runtime.broker.state.order
        adapter.trade = maker_trade(runtime, clock.now(), "obsolete-combined-trade")
        entered_commit = asyncio.Event()
        never_release = asyncio.Event()

        async def gated_recompute(*_args, **_kwargs):
            entered_commit.set()
            await never_release.wait()

        runtime._recompute_funding = gated_recompute
        stop = asyncio.Event()
        socket = Stabilization002MessageSocket({"channel": "trade"})
        runtime._session = Stabilization002ReplacementSession(socket, stop)
        runtime._stop_event = stop
        key = (Venue.NADO, "*", "combined")
        old_session = runtime._new_stream_session(key)
        old_task = asyncio.create_task(runtime._combined_stream(
            Venue.NADO, adapter, (order.canonical_market,), old_session
        ))
        await entered_commit.wait()
        state_before_replacement = runtime.broker.state
        fills_before = repository.connection.execute(
            "SELECT COUNT(*) FROM fills"
        ).fetchone()[0]
        receipts_before = repository.connection.execute(
            "SELECT COUNT(*) FROM processed_trade_events"
        ).fetchone()[0]

        await _stabilization002_cancel_combined_for_real_replacement(
            runtime, Venue.NADO, adapter, old_task
        )

        assert runtime.broker.state == state_before_replacement
        assert repository.connection.execute(
            "SELECT COUNT(*) FROM fills"
        ).fetchone()[0] == fills_before
        assert repository.connection.execute(
            "SELECT COUNT(*) FROM processed_trade_events"
        ).fetchone()[0] == receipts_before


@pytest.mark.asyncio
async def test_stabilization002_queued_nado_funding_is_cancelled_on_real_replacement(
    tmp_path,
):
    clock = FakeClock()
    with PaperRepository(tmp_path / "stabilization002-queued-funding.db") as repository:
        runtime, adapter = await _stabilization002_nado_entry_runtime(
            repository, clock
        )
        await runtime.deliver_trade(
            maker_trade(runtime, clock.now(), "open-nado-position")
        )
        assert runtime.lifecycle is not None
        assert runtime.broker is None
        snapshot = runtime.lifecycle.snapshot
        settlement = next(
            row for row in snapshot.settlements if row.venue is Venue.NADO
        )
        adapter.quote = FundingCashQuote(
            Venue.NADO, settlement.canonical_market, clock.now(),
            snapshot.position.position_opened_at, settlement.settlement_at,
            FundingQuality.APPLIED_RATE,
            FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT, True,
            D("1"), D("-1"), "official-shaped",
        )
        observation_before = runtime.observations[
            Venue.NADO, settlement.canonical_market
        ]
        settlement_before = settlement
        notifications = []
        runtime._notify_event = lambda *args: notifications.append(args)

        class GatedFundingSocket(Stabilization002MessageSocket):
            def __init__(self, payload) -> None:
                super().__init__(payload)
                self.waiting_for_release = asyncio.Event()
                self.release_delivery = asyncio.Event()
                self.dispatch_boundary = asyncio.Event()

            async def __anext__(self):
                if self.delivered:
                    await asyncio.Event().wait()
                self.waiting_for_release.set()
                await self.release_delivery.wait()
                self.delivered = True
                asyncio.get_running_loop().call_soon(
                    self.dispatch_boundary.set
                )
                return SimpleNamespace(
                    type=aiohttp.WSMsgType.TEXT,
                    data=json.dumps(self.payload),
                )

        stop = asyncio.Event()
        socket = GatedFundingSocket({
            "channel": "funding_rate", "product_id": 1,
        })
        runtime._session = Stabilization002ReplacementSession(socket, stop)
        runtime._stop_event = stop
        key = (Venue.NADO, "*", "combined")
        old_session = runtime._new_stream_session(key)
        old_task = asyncio.create_task(runtime._combined_stream(
            Venue.NADO, adapter, (settlement.canonical_market,), old_session
        ))
        await socket.waiting_for_release.wait()
        await runtime._position_event_lock.acquire()
        socket.release_delivery.set()
        await socket.dispatch_boundary.wait()
        observable_at_commit_gate = _stabilization002_observable_state(runtime)
        funding_at_commit_gate = runtime.observations[
            Venue.NADO, settlement.canonical_market
        ].funding
        evidence_id_at_commit_gate = repository.connection.execute(
            "SELECT COALESCE(MAX(evidence_id), 0) FROM runtime_evidence"
        ).fetchone()[0]
        notifications_at_commit_gate = tuple(notifications)
        try:
            await _stabilization002_cancel_combined_for_real_replacement(
                runtime, Venue.NADO, adapter, old_task
            )
        finally:
            runtime._position_event_lock.release()

        current = next(
            row for row in runtime.lifecycle.snapshot.settlements
            if row.key == settlement_before.key
        )
        assert current == settlement_before
        assert repository.load_runtime() == snapshot
        assert runtime.observations[
            Venue.NADO, settlement.canonical_market
        ].funding == observation_before.funding
        assert funding_at_commit_gate == observation_before.funding
        observable_after = _stabilization002_observable_state(runtime)
        assert observable_after[0] == observable_at_commit_gate[0]
        assert observable_after[2:] == observable_at_commit_gate[2:]
        evidence_after_gate = repository.connection.execute(
            "SELECT event_type FROM runtime_evidence WHERE evidence_id>? "
            "ORDER BY evidence_id",
            (evidence_id_at_commit_gate,),
        ).fetchall()
        assert [row["event_type"] for row in evidence_after_gate] == [
            "PUBLIC_STREAM_RECONCILED"
        ]
        assert tuple(notifications) == notifications_at_commit_gate
        assert notifications == []


@pytest.mark.asyncio
async def test_no_session_settlement_delivery_keeps_live_lifecycle_path(
    tmp_path, monkeypatch
):
    clock = FakeClock()
    with PaperRepository(tmp_path / "no-session-settlement.db") as repository:
        runtime = await _stabilization002_position_runtime(repository, clock)
        lifecycle = runtime.lifecycle
        assert lifecycle is not None
        before_snapshot = lifecycle.snapshot
        pending = next(
            row for row in before_snapshot.settlements
            if row.venue is Venue.EXTENDED
        )
        applied = replace(
            pending, status=SettlementStatus.APPLIED_RATE, cash_usd=D("1.25")
        )
        live_calls = []
        original_reconcile = lifecycle.reconcile_settlement

        async def live_reconcile(update):
            live_calls.append(lifecycle)
            return await original_reconcile(update)

        monkeypatch.setattr(lifecycle, "reconcile_settlement", live_reconcile)
        monkeypatch.setattr(
            lifecycle,
            "detached",
            lambda: pytest.fail("no-session delivery must not detach"),
        )
        notifications = []
        monkeypatch.setattr(
            runtime,
            "_notify_settlement_transition",
            lambda *args: notifications.append(args),
        )
        await runtime.deliver_settlement(applied)
        persisted = repository.load_runtime()

    assert live_calls == [lifecycle]
    assert lifecycle.snapshot is not before_snapshot
    assert next(
        row for row in lifecycle.snapshot.settlements
        if row.key == pending.key
    ) == applied
    assert persisted is not None
    assert next(
        row for row in persisted.settlements
        if row.key == pending.key
    ) == applied
    assert len(notifications) == 1


@pytest.mark.asyncio
async def test_extended_aggregate_funding_reconciliation_is_inert_after_replacement(
    tmp_path, monkeypatch
):
    clock = FakeClock()
    with PaperRepository(
        tmp_path / "extended-aggregate-funding-replacement.db"
    ) as repository:
        runtime = await _stabilization002_position_runtime(repository, clock)
        lifecycle = runtime.lifecycle
        assert lifecycle is not None
        before_snapshot = lifecycle.snapshot
        persisted_before = repository.load_runtime()
        pending = next(
            row for row in before_snapshot.settlements
            if row.venue is Venue.EXTENDED
        )
        applied = replace(
            pending, status=SettlementStatus.APPLIED_RATE, cash_usd=D("1.25")
        )
        session_key = (Venue.EXTENDED, "*", "funding")
        session_id = runtime._new_stream_session(session_key)
        staged = lifecycle.detached()
        entered = asyncio.Event()
        release = asyncio.Event()
        original_reconcile = staged.reconcile_settlement

        async def gated_reconcile(update):
            entered.set()
            await release.wait()
            return await original_reconcile(update)

        staged.reconcile_settlement = gated_reconcile
        monkeypatch.setattr(lifecycle, "detached", lambda: staged)
        notifications = []
        monkeypatch.setattr(
            runtime,
            "_notify_settlement_transition",
            lambda *args: notifications.append(args),
        )
        delivery = asyncio.create_task(
            runtime.deliver_settlement(
                applied, stream_session_id=session_id
            )
        )
        await entered.wait()
        assert lifecycle.snapshot is before_snapshot
        assert repository.load_runtime() == persisted_before
        persisted_rows_before = tuple(
            tuple(row) for row in repository.connection.execute(
                "SELECT venue,canonical_market,settlement_at,status,cash_usd "
                "FROM funding_settlements ORDER BY venue,canonical_market,settlement_at"
            )
        )

        replacement_session = runtime._new_stream_session(session_key)
        assert replacement_session != session_id
        release.set()
        await delivery
        persisted_after = repository.load_runtime()
        persisted_rows_after = tuple(
            tuple(row) for row in repository.connection.execute(
                "SELECT venue,canonical_market,settlement_at,status,cash_usd "
                "FROM funding_settlements ORDER BY venue,canonical_market,settlement_at"
            )
        )

    assert staged.snapshot != before_snapshot
    assert runtime.lifecycle is lifecycle
    assert lifecycle.snapshot is before_snapshot
    assert persisted_after == persisted_before
    assert persisted_rows_after == persisted_rows_before
    assert notifications == []


@pytest.mark.asyncio
async def test_stabilization002_queued_risex_trade_dispatch_is_cancelled_on_replacement(
    tmp_path,
):
    clock = FakeClock()
    with PaperRepository(tmp_path / "stabilization002-queued-risex.db") as repository:
        runtime = await _stabilization002_position_runtime(repository, clock)
        symbol = runtime.lifecycle.snapshot.risex_market.venue_symbol
        adapter = Stabilization002RisexAdapter(clock, symbol)
        delivered = []
        entered_dispatch = asyncio.Event()

        async def gated_delivery(trade):
            entered_dispatch.set()
            await asyncio.Event().wait()
            delivered.append(trade)

        adapter.normalize_trade = lambda *_args, **_kwargs: TradeEvidence(
            "obsolete-risex-trade", Venue.RISEX, symbol,
            clock.now(), clock.now(), 1, D("1"), D("100"), Side.SELL, True,
        )
        runtime.deliver_trade = gated_delivery
        stop = asyncio.Event()
        socket = Stabilization002MessageSocket({
            "channel": "trades", "type": "update",
        })
        runtime._session = Stabilization002ReplacementSession(socket, stop)
        runtime._stop_event = stop
        key = (Venue.RISEX, "*", "combined")
        old_session = runtime._new_stream_session(key)
        old_task = asyncio.create_task(runtime._combined_stream(
            Venue.RISEX, adapter, (symbol,), old_session
        ))
        await entered_dispatch.wait()
        await _stabilization002_cancel_combined_for_real_replacement(
            runtime, Venue.RISEX, adapter, old_task
        )
        assert old_task.cancelled()
        assert delivered == []
        assert repository.connection.execute(
            "SELECT COUNT(*) FROM processed_trade_events "
            "WHERE trade_event_key='obsolete-risex-trade'"
        ).fetchone()[0] == 0


@pytest.mark.asyncio
async def test_extended_aggregate_stream_routes_interleaved_market_evidence(
    tmp_path,
):
    clock = FakeClock()
    stop = asyncio.Event()
    base = FakeAdapter(
        Venue.EXTENDED, clock, settlement_at=NOW + timedelta(minutes=5)
    ).market
    markets = tuple(
        replace(base, canonical_asset=asset, venue_symbol=f"{asset}-EXTENDED")
        for asset in ("A", "B")
    )
    timestamp = int(clock.now().timestamp() * 1000)

    def book_message(market: str, sequence: int, *, snapshot: bool) -> dict:
        return {
            "type": "SNAPSHOT" if snapshot else "UPDATE",
            "ts": timestamp,
            "seq": sequence,
            "data": {
                "m": market,
                "b": [{"p": "99", "c": "2" if snapshot else "3"}],
                "a": [{"p": "101", "c": "2"}],
            },
        }

    def trade_message(sequence: int, market: str, trade_id: str) -> dict:
        return {
            "seq": sequence,
            "ts": timestamp,
            "data": [{
                "m": market, "p": "100", "q": "1", "T": timestamp,
                "S": "SELL", "i": trade_id, "tT": "TRADE",
            }],
        }

    def funding_message(sequence: int, market: str) -> dict:
        return {
            "seq": sequence,
            "ts": timestamp,
            "data": {"m": market, "T": timestamp, "f": "0.001"},
        }

    messages = (
        book_message(markets[0].venue_symbol, 1, snapshot=True),
        book_message(markets[1].venue_symbol, 2, snapshot=True),
        book_message(markets[0].venue_symbol, 3, snapshot=False),
        book_message(markets[1].venue_symbol, 4, snapshot=False),
        trade_message(1, markets[0].venue_symbol, "a-trade"),
        trade_message(2, markets[1].venue_symbol, "b-trade"),
        trade_message(3, "NOT-REQUIRED", "wrong-market"),
        funding_message(1, markets[0].venue_symbol),
        funding_message(2, markets[1].venue_symbol),
        funding_message(3, "NOT-REQUIRED"),
    )

    delivered: list[TradeEvidence] = []

    async def capture_trade(row, **_kwargs) -> None:
        delivered.append(row)

    with PaperRepository(tmp_path / "extended-aggregate-routing.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.EXTENDED: ExtendedAdapter(None)},
            clock=clock,
        )
        runtime._session = SingleWebSocketSession(TextWebSocket(stop, messages))
        runtime._stop_event = stop
        runtime._extended_stream_symbols = tuple(
            market.venue_symbol for market in markets
        )
        for market in markets:
            runtime.observations[Venue.EXTENDED, market.venue_symbol] = (
                MarketObservation(market, None, None, None, None)
            )
            await runtime.recover_snapshot(OrderBook(
                Venue.EXTENDED, market.venue_symbol,
                (BookLevel(D("98"), D("1")),),
                (BookLevel(D("102"), D("1")),),
                clock.now(),
            ))
        runtime.deliver_trade = capture_trade
        session_id = runtime._new_stream_session(
            (Venue.EXTENDED, "*", "book")
        )
        assert not await runtime.recover_snapshot(OrderBook(
            Venue.EXTENDED, markets[0].venue_symbol,
            (BookLevel(D("97"), D("1")),),
            (BookLevel(D("103"), D("1")),),
            clock.now(),
        ))
        # Use one aggregate session per stream kind, as the production
        # reconciler does. The book reader below is the only one that carries
        # data in this fixture.
        runtime._new_stream_session((Venue.EXTENDED, "*", "trade"))
        runtime._new_stream_session((Venue.EXTENDED, "*", "funding"))
        book_session = SingleWebSocketSession(
            TextWebSocket(stop, messages[:4])
        )
        runtime._session = book_session
        await runtime._extended_stream(
            runtime.adapters[Venue.EXTENDED],  # type: ignore[index]
            runtime._extended_stream_symbols,
            "book",
            session_id,
        )

        # Route trade and funding frames independently through their owned
        # aggregate sessions. Each uses a fresh socket fixture and therefore
        # has a clean per-session monotonic cursor.
        trade_stop = asyncio.Event()
        runtime._stop_event = trade_stop
        runtime._session = SingleWebSocketSession(
            TextWebSocket(trade_stop, messages[4:7])
        )
        trade_session = runtime._stream_sessions[Venue.EXTENDED, "*", "trade"]
        await runtime._extended_stream(
            runtime.adapters[Venue.EXTENDED],  # type: ignore[index]
            runtime._extended_stream_symbols,
            "trade",
            trade_session,
        )
        funding_stop = asyncio.Event()
        runtime._stop_event = funding_stop
        runtime._session = SingleWebSocketSession(
            TextWebSocket(funding_stop, messages[7:])
        )
        funding_session = runtime._stream_sessions[
            Venue.EXTENDED, "*", "funding"
        ]
        await runtime._extended_stream(
            runtime.adapters[Venue.EXTENDED],  # type: ignore[index]
            runtime._extended_stream_symbols,
            "funding",
            funding_session,
        )
        books = {
            market.venue_symbol: runtime.coordinator.stream(
                Venue.EXTENDED, market.venue_symbol
            ).book()
            for market in markets
        }
        readiness = runtime.component_readiness[Venue.EXTENDED]

    assert book_session.connections == 1
    assert [row.trade_event_key for row in delivered] == [
        f"EXTENDED|{markets[0].venue_symbol}|a-trade",
        f"EXTENDED|{markets[1].venue_symbol}|b-trade",
    ]
    assert books[markets[0].venue_symbol] is not None
    assert books[markets[1].venue_symbol] is not None
    assert books[markets[0].venue_symbol].sequence == 3
    assert books[markets[1].venue_symbol].sequence == 4
    assert all(
        readiness[f"{component}:{market.venue_symbol}"].available
        for market in markets
        for component in (
            "book", "connection_book", "trade", "connection_trade",
            "applied_funding", "connection_funding",
        )
    )
    assert "NOT-REQUIRED" not in runtime.observations


async def _assert_extended_aggregate_book_sequence_gap_fails_all(
    tmp_path, monkeypatch, gap_market: str, gap_sequence: int = 4
):
    clock = FakeClock()
    base = FakeAdapter(
        Venue.EXTENDED, clock, settlement_at=NOW + timedelta(minutes=5)
    ).market
    markets = tuple(
        replace(base, canonical_asset=asset, venue_symbol=f"{asset}-EXTENDED")
        for asset in ("A", "B")
    )

    def snapshot(market: str, sequence: int) -> OrderBook:
        return OrderBook(
            Venue.EXTENDED, market,
            (BookLevel(D("99"), D("2")),),
            (BookLevel(D("101"), D("2")),), clock.now(), sequence,
        )

    with PaperRepository(tmp_path / "extended-aggregate-gap.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.EXTENDED: ExtendedAdapter(None)},
            clock=clock,
        )
        runtime._extended_stream_symbols = tuple(
            market.venue_symbol for market in markets
        )
        book_key = (Venue.EXTENDED, "*", "book")
        stale_session_id = runtime._new_stream_session(
            book_key
        )
        assert await runtime.apply_book_event(
            snapshot(markets[0].venue_symbol, 1),
            stream_session_id=stale_session_id,
        )
        assert await runtime.apply_book_event(
            snapshot(markets[1].venue_symbol, 2),
            stream_session_id=stale_session_id,
        )
        restart_calls: list[str] = []
        replacement_sessions = []

        def start_replacement(kind: str) -> None:
            restart_calls.append(kind)
            replacement_sessions.append(runtime._new_stream_session(book_key))

        monkeypatch.setattr(runtime, "_start_extended_stream", start_replacement)
        gap_event = snapshot(gap_market, gap_sequence)
        assert not await runtime.apply_book_event(
            gap_event,
            stream_session_id=stale_session_id,
        )
        assert restart_calls == ["book"]
        assert len(replacement_sessions) == 1
        replacement_session_id = replacement_sessions[0]
        assert replacement_session_id != stale_session_id
        for market in markets:
            stream = runtime.coordinator.stream(
                Venue.EXTENDED, market.venue_symbol
            )
            assert stream.book() is None
            assert stream.health(clock.now()).data_quality is DataQuality.DEGRADED
            assert (Venue.EXTENDED, market.venue_symbol) not in (
                runtime._live_book_ready
            )
            assert not runtime.component_readiness[Venue.EXTENDED][
                f"book:{market.venue_symbol}"
            ].available

        # The displaced aggregate session cannot restore either book after the
        # physical sequence failure.
        assert not await runtime.apply_book_event(
            snapshot(markets[0].venue_symbol, 5),
            stream_session_id=stale_session_id,
        )
        assert not await runtime.apply_book_event(
            snapshot(markets[1].venue_symbol, 6),
            stream_session_id=stale_session_id,
        )
        assert all(
            runtime.coordinator.stream(
                Venue.EXTENDED, market.venue_symbol
            ).book() is None
            for market in markets
        )
        assert not await runtime.recover_snapshot(OrderBook(
            Venue.EXTENDED, markets[0].venue_symbol,
            (BookLevel(D("97"), D("1")),),
            (BookLevel(D("103"), D("1")),),
            clock.now(),
        ))
        assert all(
            runtime.coordinator.stream(
                Venue.EXTENDED, market.venue_symbol
            ).book() is None
            for market in markets
        )

        # A fresh aggregate session establishes each market independently. A
        # valid A snapshot must not make B healthy, and vice versa.
        assert await runtime.apply_book_event(
            snapshot(markets[0].venue_symbol, 100),
            stream_session_id=replacement_session_id,
        )
        assert runtime.coordinator.stream(
            Venue.EXTENDED, markets[0].venue_symbol
        ).book() is not None
        assert runtime.coordinator.stream(
            Venue.EXTENDED, markets[1].venue_symbol
        ).book() is None
        assert (Venue.EXTENDED, markets[0].venue_symbol) in (
            runtime._live_book_ready
        )
        assert (Venue.EXTENDED, markets[1].venue_symbol) not in (
            runtime._live_book_ready
        )
        assert await runtime.apply_book_event(
            snapshot(markets[1].venue_symbol, 101),
            stream_session_id=replacement_session_id,
        )
        assert all(
            runtime.coordinator.stream(
                Venue.EXTENDED, market.venue_symbol
            ).book() is not None
            for market in markets
        )
        sequence_rows = repository.connection.execute(
            "SELECT event_type FROM runtime_evidence "
            "WHERE event_type IN ('PUBLIC_BOOK_SEQUENCE_DISCONTINUITY',"
            "'PUBLIC_SOCKET_DISCONNECTED') ORDER BY evidence_id"
        ).fetchall()

    assert [row["event_type"] for row in sequence_rows] == [
        "PUBLIC_BOOK_SEQUENCE_DISCONTINUITY",
        "PUBLIC_SOCKET_DISCONNECTED",
    ]


@pytest.mark.asyncio
async def test_extended_aggregate_irrelevant_book_gap_fails_all_and_recovers(
    tmp_path, monkeypatch
):
    await _assert_extended_aggregate_book_sequence_gap_fails_all(
        tmp_path, monkeypatch, "NOT-REQUIRED"
    )


@pytest.mark.asyncio
async def test_extended_aggregate_required_snapshot_gap_fails_all_and_recovers(
    tmp_path, monkeypatch
):
    await _assert_extended_aggregate_book_sequence_gap_fails_all(
        tmp_path, monkeypatch, "A-EXTENDED"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("gap_sequence", [2, 1])
async def test_extended_aggregate_duplicate_or_out_of_order_gap_fails_all(
    tmp_path, monkeypatch, gap_sequence
):
    await _assert_extended_aggregate_book_sequence_gap_fails_all(
        tmp_path, monkeypatch, "NOT-REQUIRED", gap_sequence
    )


@pytest.mark.asyncio
async def test_extended_aggregate_book_gap_replaces_current_reader_and_reconnects(
    tmp_path,
):
    clock = FakeClock()
    base = FakeAdapter(
        Venue.EXTENDED, clock, settlement_at=NOW + timedelta(minutes=5)
    ).market
    markets = tuple(
        replace(base, canonical_asset=asset, venue_symbol=f"{asset}-EXTENDED")
        for asset in ("A", "B")
    )
    timestamp = str(int(clock.now().timestamp() * 1000))

    def message(market: str, sequence: int) -> dict:
        return {
            "type": "SNAPSHOT",
            "ts": timestamp,
            "seq": sequence,
            "data": {
                "m": market,
                "b": [{"p": "99", "c": "2"}],
                "a": [{"p": "101", "c": "2"}],
            },
        }

    class ControlledSocket:
        def __init__(self, payloads, connected: asyncio.Event) -> None:
            self.payloads = list(payloads)
            self.connected = connected
            self.release = asyncio.Event()

        async def __aenter__(self):
            self.connected.set()
            return self

        async def __aexit__(self, *_):
            return None

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.payloads:
                return SimpleNamespace(
                    type=aiohttp.WSMsgType.TEXT,
                    data=json.dumps(self.payloads.pop(0)),
                )
            await self.release.wait()
            raise StopAsyncIteration

        async def ping(self):
            return None

        async def pong(self, _data):
            return None

    with PaperRepository(
        tmp_path / "extended-aggregate-current-reader-gap.db"
    ) as repository:
        first_connected = asyncio.Event()
        replacement_connected = asyncio.Event()
        first_socket = ControlledSocket(
            (
                message(markets[0].venue_symbol, 1),
                message(markets[1].venue_symbol, 2),
                message("NOT-REQUIRED", 4),
            ),
            first_connected,
        )
        replacement_socket = ControlledSocket((), replacement_connected)

        class Session:
            def __init__(self) -> None:
                self.connections = 0

            def ws_connect(self, url, **_kwargs):
                assert url.endswith("/orderbooks")
                self.connections += 1
                return (
                    first_socket
                    if self.connections == 1
                    else replacement_socket
                )

        runtime = PublicPaperRuntime(
            repository, adapters={Venue.EXTENDED: ExtendedAdapter(None)},
            clock=clock,
        )
        runtime._session = Session()
        runtime._stop_event = asyncio.Event()
        runtime._extended_stream_symbols = tuple(
            market.venue_symbol for market in markets
        )
        book_key = (Venue.EXTENDED, "*", "book")
        first_session = runtime._new_stream_session(book_key)
        first_reader = asyncio.create_task(runtime._extended_stream(
            runtime.adapters[Venue.EXTENDED],  # type: ignore[index]
            runtime._extended_stream_symbols,
            "book",
            first_session,
        ))
        runtime._stream_tasks[book_key] = first_reader
        replacement_reader = None
        try:
            await first_connected.wait()
            await replacement_connected.wait()
            await first_reader
            replacement_session = runtime._stream_sessions[book_key]
            replacement_reader = runtime._stream_tasks[book_key]
            assert replacement_session != first_session
            assert replacement_reader is not first_reader
            assert all(
                runtime.coordinator.stream(
                    Venue.EXTENDED, market.venue_symbol
                ).book() is None
                for market in markets
            )
            assert await runtime.apply_book_event(
                OrderBook(
                    Venue.EXTENDED, markets[0].venue_symbol,
                    (BookLevel(D("99"), D("2")),),
                    (BookLevel(D("101"), D("2")),),
                    clock.now(), 100,
                ),
                stream_session_id=replacement_session,
            )
            assert runtime.coordinator.stream(
                Venue.EXTENDED, markets[0].venue_symbol
            ).book() is not None
            assert runtime.coordinator.stream(
                Venue.EXTENDED, markets[1].venue_symbol
            ).book() is None
            assert await runtime.apply_book_event(
                OrderBook(
                    Venue.EXTENDED, markets[1].venue_symbol,
                    (BookLevel(D("99"), D("2")),),
                    (BookLevel(D("101"), D("2")),),
                    clock.now(), 101,
                ),
                stream_session_id=replacement_session,
            )
        finally:
            runtime._stop_event.set()
            replacement_socket.release.set()
            if replacement_reader is not None:
                await replacement_reader

    assert runtime.coordinator.stream(
        Venue.EXTENDED, markets[0].venue_symbol
    ).book() is not None
    assert runtime.coordinator.stream(
        Venue.EXTENDED, markets[1].venue_symbol
    ).book() is not None


@pytest.mark.asyncio
async def test_extended_aggregate_valid_content_failure_is_market_local(
    tmp_path,
):
    clock = FakeClock()
    base = FakeAdapter(
        Venue.EXTENDED, clock, settlement_at=NOW + timedelta(minutes=5)
    ).market
    markets = tuple(
        replace(base, canonical_asset=asset, venue_symbol=f"{asset}-EXTENDED")
        for asset in ("A", "B")
    )

    def snapshot(market: str, sequence: int) -> OrderBook:
        return OrderBook(
            Venue.EXTENDED, market,
            (BookLevel(D("99"), D("2")),),
            (BookLevel(D("101"), D("2")),), clock.now(), sequence,
        )

    class NoRestExtendedAdapter(ExtendedAdapter):
        def __init__(self) -> None:
            super().__init__(None)
            self.fetch_book_calls = 0

        async def fetch_book(self, venue_symbol: str) -> OrderBook:
            self.fetch_book_calls += 1
            raise AssertionError(
                f"aggregate recovery attempted REST for {venue_symbol}"
            )

    adapter = NoRestExtendedAdapter()
    with PaperRepository(tmp_path / "extended-aggregate-content-failure.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.EXTENDED: adapter},
            clock=clock,
        )
        runtime._extended_stream_symbols = tuple(
            market.venue_symbol for market in markets
        )
        book_key = (Venue.EXTENDED, "*", "book")
        session_id = runtime._new_stream_session(book_key)
        assert await runtime.apply_book_event(
            snapshot(markets[0].venue_symbol, 1),
            stream_session_id=session_id,
        )
        assert await runtime.apply_book_event(
            snapshot(markets[1].venue_symbol, 2),
            stream_session_id=session_id,
        )
        assert not await runtime.apply_book_event(
            BookDelta(
                Venue.EXTENDED, markets[0].venue_symbol,
                (BookLevel(D("99"), D("-1")),), (),
                clock.now(), 3,
            ),
            stream_session_id=session_id,
        )
        assert runtime.coordinator.stream(
            Venue.EXTENDED, markets[0].venue_symbol
        ).book() is None
        b_book = runtime.coordinator.stream(
            Venue.EXTENDED, markets[1].venue_symbol
        ).book()
        assert b_book is not None and b_book.sequence == 2
        assert runtime._recoveries[
            Venue.EXTENDED, markets[0].venue_symbol
        ].terminal is None
        failed = runtime._recoveries[
            Venue.EXTENDED, markets[0].venue_symbol
        ]
        assert failed.task is None
        assert await runtime.apply_book_event(
            BookDelta(
                Venue.EXTENDED, markets[1].venue_symbol,
                (BookLevel(D("99"), D("3")),), (),
                clock.now(), 4,
            ),
            stream_session_id=session_id,
        )
        assert runtime.coordinator.stream(
            Venue.EXTENDED, markets[1].venue_symbol
        ).book().sequence == 4

        # Aggregate-local recovery is WS-only.  Neither the direct REST
        # entrypoint nor a background REST attempt may restore A while this
        # physical aggregate session remains owned.
        assert not await runtime.recover_snapshot(
            snapshot(markets[0].venue_symbol, 5)
        )
        await runtime._recover_snapshot_in_background(
            Venue.EXTENDED, markets[0].venue_symbol, failed
        )
        assert not await runtime._publish_recovery_snapshot(
            (Venue.EXTENDED, markets[0].venue_symbol),
            failed,
            failed.attempt_generation,
            snapshot(markets[0].venue_symbol, 5),
            at=clock.now(),
            buffered=0,
            replayed=0,
            source="REST_SNAPSHOT",
        )
        assert adapter.fetch_book_calls == 0
        assert runtime.coordinator.stream(
            Venue.EXTENDED, markets[0].venue_symbol
        ).book() is None
        assert runtime.coordinator.stream(
            Venue.EXTENDED, markets[1].venue_symbol
        ).book().sequence == 4

        # The next contiguous owned WS snapshot restores only the failed
        # market.  A displaced old session cannot mutate either market.
        assert await runtime.apply_book_event(
            snapshot(markets[0].venue_symbol, 5),
            stream_session_id=session_id,
        )
        assert runtime.coordinator.stream(
            Venue.EXTENDED, markets[0].venue_symbol
        ).book().sequence == 5
        assert runtime.coordinator.stream(
            Venue.EXTENDED, markets[1].venue_symbol
        ).book().sequence == 4
        replacement_session = runtime._new_stream_session(book_key)
        assert not await runtime.apply_book_event(
            BookDelta(
                Venue.EXTENDED, markets[0].venue_symbol,
                (BookLevel(D("99"), D("4")),), (),
                clock.now(), 6,
            ),
            stream_session_id=session_id,
        )
        assert not await runtime.apply_book_event(
            snapshot(markets[0].venue_symbol, 100),
            stream_session_id=session_id,
        )
        assert replacement_session != session_id
        assert runtime.coordinator.stream(
            Venue.EXTENDED, markets[0].venue_symbol
        ).book().sequence == 5


@pytest.mark.asyncio
async def test_extended_aggregate_reconciliation_keeps_three_owned_streams(
    tmp_path,
):
    clock = FakeClock()
    base = FakeAdapter(
        Venue.EXTENDED, clock, settlement_at=NOW + timedelta(minutes=5)
    ).market
    a_extended = replace(base, canonical_asset="A", venue_symbol="A-EXTENDED")
    b_extended = replace(base, canonical_asset="B", venue_symbol="B-EXTENDED")
    a_risex = replace(a_extended, venue=Venue.RISEX, venue_symbol="A-RISEX")
    b_risex = replace(b_extended, venue=Venue.RISEX, venue_symbol="B-RISEX")
    stop = asyncio.Event()
    urls: list[str] = []

    class WaitingSocket:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return None

        def __aiter__(self):
            return self

        async def __anext__(self):
            await stop.wait()
            raise StopAsyncIteration

        async def ping(self):
            return None

    class Session:
        closed = False

        def ws_connect(self, url, **_kwargs):
            urls.append(url)
            return WaitingSocket()

    def add_volume(market):
        return MarketVolume(
            market.venue, market.venue_symbol, D("1000000"), clock.now(),
            "official-shaped",
        )

    def snapshot(market: str, sequence: int) -> OrderBook:
        return OrderBook(
            Venue.EXTENDED, market,
            (BookLevel(D("99"), D("2")),),
            (BookLevel(D("101"), D("2")),), clock.now(), sequence,
        )

    with PaperRepository(tmp_path / "extended-aggregate-reconcile.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.EXTENDED: ExtendedAdapter(None)},
            clock=clock,
        )
        runtime._session = Session()
        runtime._stop_event = stop
        runtime.markets[Venue.EXTENDED] = (a_extended,)
        runtime.markets[Venue.RISEX] = (a_risex,)
        for market in (a_extended, a_risex):
            runtime.volumes[market.venue, market.venue_symbol] = add_volume(market)
        await runtime._reconcile_extended_streams()
        tasks = dict(runtime._stream_tasks)
        await asyncio.sleep(0)
        assert set(tasks) == {
            (Venue.EXTENDED, "*", "book"),
            (Venue.EXTENDED, "*", "trade"),
            (Venue.EXTENDED, "*", "funding"),
        }
        runtime.markets[Venue.EXTENDED] = (a_extended, b_extended)
        runtime.markets[Venue.RISEX] = (a_risex, b_risex)
        for market in (b_extended, b_risex):
            runtime.volumes[market.venue, market.venue_symbol] = add_volume(market)
        await runtime._reconcile_extended_streams()
        assert runtime._stream_tasks == tasks
        assert len(urls) == 3
        old_session = runtime._stream_sessions[Venue.EXTENDED, "*", "book"]
        runtime.markets[Venue.EXTENDED] = (b_extended,)
        runtime.markets[Venue.RISEX] = (b_risex,)
        await runtime._reconcile_extended_streams()
        assert runtime._stream_tasks == tasks
        assert runtime._extended_stream_symbols == ("B-EXTENDED",)
        assert not await runtime.apply_book_event(
            snapshot("A-EXTENDED", 1), stream_session_id=old_session
        )
        assert runtime.coordinator.stream(
            Venue.EXTENDED, "A-EXTENDED"
        ).book() is None
        await runtime.shutdown()

    assert urls == [
        "wss://api.starknet.extended.exchange/stream.extended.exchange/v1/orderbooks",
        "wss://api.starknet.extended.exchange/stream.extended.exchange/v1/publicTrades",
        "wss://api.starknet.extended.exchange/stream.extended.exchange/v1/funding",
    ]


@pytest.mark.asyncio
async def test_extended_aggregate_watchdog_replaces_one_owned_stream(
    tmp_path,
):
    clock = FakeClock()
    with PaperRepository(tmp_path / "extended-aggregate-watchdog.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.EXTENDED: ExtendedAdapter(None)},
            clock=clock,
        )
        runtime._stop_event = asyncio.Event()
        runtime._extended_stream_symbols = ("A-EXTENDED", "B-EXTENDED")
        key = (Venue.EXTENDED, "*", "book")
        session_id = runtime._new_stream_session(key)
        task = asyncio.create_task(runtime._stop_event.wait())
        runtime._stream_tasks[key] = task
        runtime._extended_connection_confirmed_at["book"] = (
            clock.now() - timedelta(seconds=26)
        )
        restarted: list[str] = []
        runtime._start_extended_stream = lambda kind: restarted.append(kind)
        await runtime._check_extended_health(clock.now())
        owner = runtime._extended_health_recovery_task
        assert owner is not None
        await owner
        assert restarted == ["book"]
        assert task.cancelled()
        assert runtime._pending_watchdog_episodes
        replacement = runtime._new_stream_session(key)
        runtime._confirm_extended_aggregate(
            "book", clock.now(), data_ready=False,
            stream_session_id=replacement,
        )
        runtime._watchdog_restarted(
            runtime._extended_aggregate_identity("book"), at=clock.now()
        )
        rows = repository.connection.execute(
            "SELECT event_type FROM runtime_evidence "
            "WHERE event_type IN ('PUBLIC_STREAM_CONFIRMATION_STALE',"
            "'PUBLIC_STREAM_RESTARTED') ORDER BY evidence_id"
        ).fetchall()

    assert [row["event_type"] for row in rows] == [
        "PUBLIC_STREAM_CONFIRMATION_STALE", "PUBLIC_STREAM_RESTARTED",
    ]


@pytest.mark.asyncio
async def test_extended_aggregate_stale_sessions_cannot_mutate_book_or_trade_health(
    tmp_path,
):
    clock = FakeClock()
    symbol = "A-EXTENDED"
    with PaperRepository(tmp_path / "extended-aggregate-stale-session.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={Venue.EXTENDED: ExtendedAdapter(None)},
            clock=clock,
        )
        runtime._extended_stream_symbols = (symbol,)
        book_key = (Venue.EXTENDED, "*", "book")
        stale_book = runtime._new_stream_session(book_key)
        runtime._new_stream_session(book_key)
        assert not await runtime.apply_book_event(
            OrderBook(
                Venue.EXTENDED, symbol,
                (BookLevel(D("99"), D("1")),),
                (BookLevel(D("101"), D("1")),),
                clock.now(), 1,
            ),
            stream_session_id=stale_book,
        )
        assert runtime.coordinator.stream(Venue.EXTENDED, symbol).book() is None

        trade_key = (Venue.EXTENDED, "*", "trade")
        stale_trade = runtime._new_stream_session(trade_key)
        runtime._new_stream_session(trade_key)
        stream = runtime.coordinator.stream(Venue.EXTENDED, symbol)
        stream.connected(clock.now())
        runtime._trade_stream_ready.add((Venue.EXTENDED, symbol))
        await PublicPaperRuntime.deliver_trade(
            runtime,
            TradeEvidence(
                "stale-trade", Venue.EXTENDED, symbol, clock.now(), clock.now(),
                "stale-trade", D("1"), D("100"), Side.SELL, True,
            ),
            processed_at=clock.now() + timedelta(seconds=1),
            stream_session_id=stale_trade,
        )
        confirmation = stream.health(clock.now()).last_connection_confirmation_at

        funding_key = (Venue.EXTENDED, "*", "funding")
        stale_funding = runtime._new_stream_session(funding_key)
        runtime._new_stream_session(funding_key)
        await runtime._apply_extended_funding_record(
            FundingSettlement(
                Venue.EXTENDED, symbol, clock.now(),
                SettlementStatus.UNRESOLVED, None,
            ),
            stream_session_id=stale_funding,
        )
        persisted_settlements = repository.connection.execute(
            "SELECT COUNT(*) FROM funding_settlements"
        ).fetchone()[0]

    assert confirmation == NOW
    assert persisted_settlements == 0
