import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from risex_farmer.exchanges.base import PublicAdapter, PublicDataUnavailable
from risex_farmer.exchanges.extended import ExtendedAdapter
from risex_farmer.exchanges.risex import RisexAdapter
from risex_farmer.models import (
    BookLevel,
    BookDelta,
    CanonicalMarket,
    ContractType,
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
    TradeEvidence,
    Venue,
)
from risex_farmer.notifications import NotificationOutbox, TelegramDelivery
from risex_farmer.runtime import PublicPaperRuntime, public_paper_run, public_scan_once
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


class ManyFakeAdapter(FakeAdapter):
    def __init__(self, venue: Venue, clock: FakeClock, *, settlement_at: datetime) -> None:
        super().__init__(venue, clock, settlement_at=settlement_at)
        self.many_markets = tuple(
            replace(self.market, canonical_asset=f"A{index}", venue_symbol=f"A{index}-{venue.value}")
            for index in range(5)
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

    async def fetch_funding_quote(self, market, *, assumed_open_at):
        self.calls.append("funding")
        return FundingCashQuote(
            Venue.EXTENDED, market.venue_symbol, self.clock.now(), assumed_open_at,
            self.settlement_at, FundingQuality.PREDICTED,
            FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT, True,
            D("5"), D("5"), "official-shaped",
        )


class ClosingWebSocket:
    def __init__(
        self,
        stop_event: asyncio.Event,
        *,
        stop_on_iteration: bool,
        fail_subscription: bool = False,
    ) -> None:
        self.stop_event = stop_event
        self.stop_on_iteration = stop_on_iteration
        self.fail_subscription = fail_subscription

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    def __aiter__(self):
        return self

    async def __anext__(self):
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
        )


def assert_socket_episode(rows):
    assert [row["event_type"] for row in rows] == [
        "PUBLIC_SOCKET_DISCONNECTED", "PUBLIC_SOCKET_RECONNECTED",
    ]
    disconnected = json.loads(rows[0]["detail"])
    reconnected = json.loads(rows[1]["detail"])
    assert disconnected == {
        key: value for key, value in reconnected.items()
        if key != "reconnected_at"
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
        for venue in Venue
    }


def confirm_public_streams(runtime: PublicPaperRuntime, at: datetime) -> None:
    for venue, symbol in runtime.observations:
        runtime.mark_trade_stream_connected(venue, symbol, at=at)
        runtime._live_book_ready.add((venue, symbol))


async def activate_with_live_streams(
    runtime: PublicPaperRuntime, clock: FakeClock
) -> None:
    await runtime.scan()
    confirm_public_streams(runtime, clock.now())
    clock.advance(1)
    await runtime.tick()
    assert runtime.broker is not None


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
async def test_route_output_is_hard_limited_after_system_sort(tmp_path):
    clock = FakeClock()
    many = {
        venue: ManyFakeAdapter(venue, clock, settlement_at=NOW + timedelta(minutes=5))
        for venue in Venue
    }
    with PaperRepository(tmp_path / "route-limit.db") as repository:
        result = await public_scan_once(repository, adapters=many, clock=clock)
        persisted = repository.report(as_of=NOW)["latest_routes"]
    assert len(result["routes"]) == 15
    assert len(persisted) == 15


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
    assert result["routes"] and result["routes"][0]["rank"] is None
    assert any("RISEX:PUBLIC_REST_UNAVAILABLE" in blocker for blocker in result["routes"][0]["blockers"])
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
    assert result == {"status": "STOPPED_SAFE", "forced_close": False}
    assert report["last_runtime_event"]["event_type"] == "STOPPED_SAFE"


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
        f"${winner.planned_maker_net_pnl_usd} | Scan UTC: {NOW.isoformat()}"
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

    assert persisted == 1
    assert len(digests) == 1
    digest = digests[0]
    assert digest.occurred_at == NOW
    assert digest.text.splitlines()[0] == (
        f"Full Scan | Scan UTC: {NOW.isoformat()} | Status: OPPORTUNITY"
    )
    route_lines = digest.text.splitlines()[1:]
    assert len(route_lines) == len(result["routes"])
    for line, route_row in zip(route_lines, result["routes"]):
        risex_side = (
            "LONG"
            if route_row["direction"] == "LONG_RISEX_SHORT_HEDGE"
            else "SHORT"
        )
        hedge_side = "SHORT" if risex_side == "LONG" else "LONG"
        expected = (
            f"{route_row['canonical_asset']} | RISEx {risex_side} / "
            f"{route_row['hedge_venue']} {hedge_side} | Expected PnL: "
            f"${route_row['planned_maker_net_pnl_usd']}"
        )
        assert line == expected


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
    assert digests[0].text.splitlines()[0].endswith("Status: NO TRADE")
    assert any(row["blockers"] for row in negative["routes"])
    assert any(
        D(row["planned_maker_net_pnl_usd"]) < 0
        for row in negative["routes"]
        if row["planned_maker_net_pnl_usd"] is not None
    )
    negative_lines = digests[0].text.splitlines()[1:]
    assert len(negative_lines) == len(negative["routes"])
    for line, row in zip(negative_lines, negative["routes"]):
        assert line.startswith(f"{row['canonical_asset']} | RISEx ")
        assert line.endswith(f"Expected PnL: ${row['planned_maker_net_pnl_usd']}")
    assert any(row["planned_maker_net_pnl_usd"] is None for row in unknown["routes"])
    assert len(digests[1].text.splitlines()[1:]) == len(unknown["routes"])
    assert "Expected PnL: UNKNOWN" in digests[1].text


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
async def test_full_scan_digest_retains_existing_fifteen_row_limit(tmp_path):
    clock = FakeClock()
    fakes = {
        venue: ManyFakeAdapter(
            venue, clock, settlement_at=NOW + timedelta(minutes=5)
        )
        for venue in Venue
    }
    delivery = CaptureNotifications()
    with PaperRepository(tmp_path / "full-scan-digest-fifteen.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock,
            notifications=NotificationOutbox(delivery),
        ) as runtime:
            result = await runtime.scan(scan_kind="FULL")
    digest = next(row for row in delivery.rows if row.kind == "FULL_SCAN_DIGEST")
    assert len(result["routes"]) == 15
    assert len(digest.text.splitlines()[1:]) == 15
    assert len(digest.text) <= 4096


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

    digest = next(row for row in delivery.rows if row.kind == "FULL_SCAN_DIGEST")
    assert result["status"] == "STOPPED_SAFE"
    assert risex.initial_observed_at is not None
    assert risex.seeded_observed_at is not None
    assert digest.occurred_at - risex.initial_observed_at > timedelta(seconds=120)
    assert digest.occurred_at - risex.seeded_observed_at < timedelta(seconds=120)
    assert "Expected PnL: UNKNOWN" not in digest.text
    assert "Expected PnL: $" in digest.text


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
    assert gated.cancelled


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
            runtime._live_book_ready.add((Venue.EXTENDED, symbol))
            stream = runtime.coordinator.stream(Venue.EXTENDED, symbol)
            before = stream.book()
            assert before is not None and before.sequence == 1

            await runtime.mark_disconnected(
                Venue.EXTENDED, symbol, stream_kind="funding",
                exception=TimeoutError("funding socket"),
            )
            assert stream.book() == before
            assert (Venue.EXTENDED, symbol) in runtime._trade_stream_ready
            assert runtime.component_readiness[Venue.EXTENDED][f"trade:{symbol}"].available
            assert not runtime.component_readiness[Venue.EXTENDED][f"funding:{symbol}"].available

            await runtime.mark_disconnected(
                Venue.EXTENDED, symbol, stream_kind="trade",
                exception=ConnectionError("trade socket"),
            )
            assert stream.book() == before
            assert stream.book().sequence == 1
            assert (Venue.EXTENDED, symbol) not in runtime._trade_stream_ready
            assert not runtime.component_readiness[Venue.EXTENDED][f"trade:{symbol}"].available


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
            await runtime.deliver_settlement(replace(
                pending, status=SettlementStatus.ESTIMATED, cash_usd=D("3.125")
            ))
            applied = replace(
                pending, status=SettlementStatus.APPLIED_RATE, cash_usd=D("3.25")
            )
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

    kinds = [row.kind for row in delivery.rows]
    for required in (
        "ENTRY_ACTIVATED", "POSITION_OPENED", "EXIT_STARTED",
        "FUNDING_RECEIVED", "FUNDING_RECONCILED", "POSITION_CLOSED",
    ):
        assert kinds.count(required) == 1
    closed = next(row for row in delivery.rows if row.kind == "POSITION_CLOSED")
    assert closed.final_pnl_usd == authoritative.simulated_closed_net_pnl_usd
    assert str(authoritative.simulated_closed_net_pnl_usd) in closed.text
    received = next(row for row in delivery.rows if row.kind == "FUNDING_RECEIVED")
    reconciled = next(row for row in delivery.rows if row.kind == "FUNDING_RECONCILED")
    assert received.text.startswith("Funding received:")
    assert reconciled.text.startswith("Funding reconciled:")


@pytest.mark.asyncio
async def test_disconnect_cancels_entry_and_position_gap_recovers_from_snapshot(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(seconds=120)
    fakes = adapters(clock, settlement_at=target)
    with PaperRepository(tmp_path / "gap.db") as repository:
        async with PublicPaperRuntime(repository, adapters=fakes, clock=clock) as runtime:
            await activate_with_live_streams(runtime, clock)
            order = runtime.broker.state.order
            await runtime.mark_disconnected(order.venue, order.canonical_market)
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
            await runtime.mark_disconnected(hedge.venue, hedge.venue_symbol)
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
            await runtime.apply_book_event(BookDelta(
                Venue.EXTENDED, market.venue_symbol,
                (BookLevel(D("100"), D("1")),), (), clock.now(), 3, 999,
            ))
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
            await runtime.apply_book_event(rest)
            assert (Venue.EXTENDED, market.venue_symbol) in runtime._recovery_buffers
            await runtime.apply_book_event(adapter.normalize_book_message({
                "type": "UPDATE", "seq": 11,
                "ts": str(int(clock.now().timestamp() * 1000)),
                "data": {
                    "m": market.venue_symbol,
                    "b": [{"p": "100", "q": "2"}], "a": [],
                },
            }))
            snapshot = adapter.normalize_book_message({
                "type": "SNAPSHOT", "seq": 10,
                "ts": str(int(clock.now().timestamp() * 1000)),
                "data": {
                    "m": market.venue_symbol,
                    "b": [{"p": "99", "q": "20"}],
                    "a": [{"p": "101", "q": "20"}],
                },
            })
            await runtime.apply_book_event(snapshot)
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
    }] == [
        "CRITICAL_DATA_LOSS", "DATA_RECOVERY",
    ]


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
        "CRITICAL_DATA_LOSS", "DATA_RECOVERY", "CRITICAL_DATA_LOSS",
    ]


def test_socket_outage_notification_identity_uses_physical_episode_id(tmp_path):
    clock = FakeClock()
    delivery = CaptureNotifications()
    detail = {
        "episode_id": "EXTENDED:trade:episode-1",
        "market": "ABC-EXTENDED",
        "stream_kind": "trade",
    }
    with PaperRepository(tmp_path / "socket-notification-dedupe.db") as repository:
        runtime = PublicPaperRuntime(
            repository, adapters={}, clock=clock,
            notifications=NotificationOutbox(delivery),
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
    assert [row.kind for row in delivery.rows] == [
        "CRITICAL_DATA_LOSS", "DATA_RECOVERY",
    ]


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
            await runtime.apply_book_event(BookDelta(
                Venue.NADO, symbol, (), (), clock.now(), 3, 999,
            ))
            recovery = runtime._recovery_tasks[(Venue.NADO, symbol)]
            runtime._start_snapshot_recovery(Venue.NADO, symbol)
            assert runtime._recovery_tasks[(Venue.NADO, symbol)] is recovery
            await gated.recovery_started.wait()
            await runtime.apply_book_event(BookDelta(
                Venue.NADO, symbol,
                (BookLevel(D("100"), D("2")),), (), clock.now(), 2, 1,
            ))
            await runtime.apply_book_event(BookDelta(
                Venue.NADO, symbol,
                (BookLevel(D("100"), D("3")),), (), clock.now(), 3, 2,
            ))
            assert len(runtime._recovery_buffers[(Venue.NADO, symbol)]) == 2
            gated.block_recovery = False
            gated.recovery_gate.set()
            await recovery
            book = runtime.coordinator.stream(Venue.NADO, symbol).book()
            await runtime.apply_book_event(replace(book, observed_at=clock.now()))
            await runtime.apply_book_event(replace(book, observed_at=clock.now()))
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
        await runtime._extended_stream(
            ExtendedAdapter(None), "ABC-EXTENDED", "trade"
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
        await runtime._extended_stream(
            ExtendedAdapter(None), "ABC-EXTENDED", "book"
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
        assert [row.kind for row in delivery.rows] == [
            "CRITICAL_DATA_LOSS", "DATA_RECOVERY",
        ]
        assert outbox._active_outages == set()

        clock.advance(1)
        await runtime.mark_disconnected(
            Venue.EXTENDED, "ABC-EXTENDED", stream_kind="book",
            exception=ValueError("independent synthetic gap"),
        )
        assert [row.kind for row in delivery.rows] == [
            "CRITICAL_DATA_LOSS", "DATA_RECOVERY", "CRITICAL_DATA_LOSS",
        ]
        assert outbox._active_outages == {"EXTENDED:ABC-EXTENDED:book"}
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
        await runtime._combined_stream(Venue.NADO, adapter, symbols)
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
        await runtime._combined_stream(Venue.NADO, adapter, symbols)
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
    assert disconnected["markets"] == reconnected["markets"] == [
        "ABC-NADO", "XYZ-NADO",
    ]
    assert [row.kind for row in delivery.rows] == [
        "CRITICAL_DATA_LOSS", "DATA_RECOVERY",
    ]
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
        await runtime._extended_stream(
            ExtendedAdapter(None), "ABC-EXTENDED", "funding"
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
        runtime.broker = None
        await runtime.shutdown()
        socket_lifecycle_count = repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence "
            "WHERE event_type IN "
            "('PUBLIC_SOCKET_DISCONNECTED','PUBLIC_SOCKET_RECONNECTED')"
        ).fetchone()[0]
    assert symbols == {original.venue_symbol, added.venue_symbol}
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

        async def stable_combined(_venue, _adapter, _symbols):
            await runtime._stop_event.wait()

        runtime._combined_stream = stable_combined
        await runtime._reconcile_streams()
        assert not any(key[0] is Venue.NADO for key in runtime._stream_tasks)

        fakes[Venue.NADO].available = True
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
            runtime.mark_trade_stream_connected(order.venue, order.canonical_market)
            runtime.next_health_check_at = clock.now() + timedelta(seconds=10)
            clock.advance(26)
            await runtime.tick()
            state = runtime.readiness[order.venue]
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
    assert state.lifecycle_state is LifecycleState.FLAT
    assert state.order.status.value == "CANCELLED"
    assert report["last_runtime_event"]["event_type"] == "STOPPED_SAFE"
    assert report["last_runtime_event"]["detail"]["forced_close"] is False


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
