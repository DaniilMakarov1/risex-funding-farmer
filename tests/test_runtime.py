import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path

import pytest

from risex_farmer.exchanges.base import PublicAdapter, PublicDataUnavailable
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
from risex_farmer.runtime import PublicPaperRuntime, public_paper_run, public_scan_once
from risex_farmer.storage import PaperRepository


D = Decimal
NOW = datetime(2027, 8, 1, 12, tzinfo=UTC)


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
async def test_venue_outage_is_specific_and_never_uses_empty_fail_closed_scan(tmp_path):
    clock = FakeClock()
    fakes = adapters(clock, settlement_at=NOW + timedelta(minutes=5), risex=False)
    with PaperRepository(tmp_path / "blocked.db") as repository:
        result = await public_scan_once(repository, adapters=fakes, clock=clock)
    assert result["status"] == "NO_TRADE"
    assert result["reason"] == "VENUE_SPECIFIC_BLOCKERS"
    assert result["routes"] and result["routes"][0]["rank"] is None
    assert any("RISEX:PUBLIC_REST_UNAVAILABLE" in blocker for blocker in result["routes"][0]["blockers"])
    assert result["venue_readiness"]["RISEX"]["detail"].startswith("PUBLIC_REST_UNAVAILABLE")
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
            clock.advance(10)
            await runtime.tick()
            clock.advance(10)  # Full scan at +120.
            await runtime.tick()
            clock.advance(160)  # Exact T-120 activation.
            await runtime.tick()
            assert runtime.broker is not None
            assert runtime.broker.state.lifecycle_state is LifecycleState.ENTRY_MAKER_OPEN
            clock.advance(115)  # Exact T-5 cutoff.
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
    assert scans >= first_scans + 4
    assert {NOW, NOW + timedelta(seconds=100), NOW + timedelta(seconds=110)} <= recorded
    assert NOW + timedelta(seconds=120) in recorded
    assert NOW + timedelta(seconds=280) in recorded
    assert NOW + timedelta(seconds=395) in recorded


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
async def test_negative_candidate_focus_trace_and_late_winner_activation(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(seconds=400)
    fakes = adapters(clock, settlement_at=target, funding="0")
    stop = asyncio.Event()

    async def trace_sleep(seconds: float) -> None:
        clock.value += timedelta(seconds=seconds)
        if clock.now() == target - timedelta(seconds=100):
            for adapter in fakes.values():
                adapter.funding_cash = D("5")
        if clock.now() >= target - timedelta(seconds=90):
            stop.set()
        await asyncio.sleep(0)

    with PaperRepository(tmp_path / "negative-late-winner.db") as repository:
        await public_paper_run(
            repository, adapters=fakes, clock=clock,
            stop_event=stop, sleep=trace_sleep,
        )
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
async def test_healthy_public_trade_reaches_broker_and_deduplicates(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(seconds=120)
    fakes = adapters(clock, settlement_at=target)
    with PaperRepository(tmp_path / "trade.db") as repository:
        async with PublicPaperRuntime(repository, adapters=fakes, clock=clock) as runtime:
            await runtime.tick()
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
async def test_disconnect_cancels_entry_and_position_gap_recovers_from_snapshot(tmp_path):
    clock = FakeClock()
    target = NOW + timedelta(seconds=120)
    fakes = adapters(clock, settlement_at=target)
    with PaperRepository(tmp_path / "gap.db") as repository:
        async with PublicPaperRuntime(repository, adapters=fakes, clock=clock) as runtime:
            await runtime.tick()
            order = runtime.broker.state.order
            await runtime.mark_disconnected(order.venue, order.canonical_market)
            assert runtime.broker is None
            assert repository.load_runtime().lifecycle_state is LifecycleState.FLAT

        clock = FakeClock()
        fakes = adapters(clock, settlement_at=target)
        async with PublicPaperRuntime(repository, adapters=fakes, clock=clock) as runtime:
            await runtime.tick()
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
            await runtime.recover_snapshot(snapshot)
            assert not runtime.lifecycle.snapshot.gap_open
            assert runtime.lifecycle.snapshot.data_quality.value == "DEGRADED"


@pytest.mark.asyncio
async def test_sequence_gap_fetches_snapshot_and_reconnect_restores_readiness(tmp_path):
    clock = FakeClock()
    fakes = adapters(clock, settlement_at=NOW + timedelta(minutes=5))
    with PaperRepository(tmp_path / "sequence-recovery.db") as repository:
        async with PublicPaperRuntime(repository, adapters=fakes, clock=clock) as runtime:
            await runtime.scan()
            market = fakes[Venue.EXTENDED].market
            before = fakes[Venue.EXTENDED].calls.count("book")
            await runtime.apply_book_event(BookDelta(
                Venue.EXTENDED, market.venue_symbol,
                (BookLevel(D("100"), D("1")),), (), clock.now(), 3, 999,
            ))
            state = runtime.readiness[Venue.EXTENDED]
    assert fakes[Venue.EXTENDED].calls.count("book") == before + 1
    assert state.available and state.detail == "PUBLIC_STREAM_RECOVERED"


@pytest.mark.asyncio
async def test_health_confirmation_silence_cancels_active_entry(tmp_path):
    clock = FakeClock()
    with PaperRepository(tmp_path / "health-silence.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=adapters(clock, settlement_at=NOW + timedelta(seconds=120)), clock=clock,
        ) as runtime:
            await runtime.tick()
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
            await runtime.tick()
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
            await runtime.tick()
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
            await runtime.tick()
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
            await runtime.tick()
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
            await runtime.tick()
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
