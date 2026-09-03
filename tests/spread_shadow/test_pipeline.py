from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal as D
import json
from pathlib import Path
import time
from types import SimpleNamespace

import pytest

from risex_farmer.models import (
    BookDelta,
    BookLevel,
    CanonicalMarket,
    ContractType,
    MarketType,
    OrderBook,
    Venue,
)
from risex_spread_shadow import (
    AppendOnlyEvidenceStore,
    BookEvidence,
    BookHistory,
    DataGapEvidence,
    FeedBookEvent,
    FeedGapEvent,
    FeedTradeEvent,
    HistoryCapacityExceeded,
    IngressQueue,
    EvidenceStorageLimitExceeded,
    MarketPair,
    MAX_PUBLIC_DURATION_SECONDS,
    ReplayHarness,
    ShadowConfig,
    SpreadObserver,
    TradeEvidence,
    Side,
    select_public_market_pairs,
    build_report,
    store_permissions,
)
from risex_spread_shadow.feed import PublicFeedRunner
import risex_spread_shadow.runner as runner_module
import risex_spread_shadow.store as store_module
import risex_spread_shadow.cli as cli_module


UTC = timezone.utc
NOW = datetime(2026, 9, 3, tzinfo=UTC)


def market(venue: Venue, symbol: str, asset: str = "BTC") -> CanonicalMarket:
    return CanonicalMarket(
        canonical_asset=asset,
        venue=venue,
        venue_symbol=symbol,
        market_type=MarketType.PERPETUAL,
        contract_type=ContractType.LINEAR,
        base_multiplier=D("1"),
        quote_asset="USDC",
        settlement_asset="USDC",
        tick_size_raw=D("1"),
        quantity_step_raw=D("1"),
        minimum_quantity_raw=D("1"),
        minimum_notional_usd=D("0"),
        minimum_fee_notional_usd=None,
        is_active=True,
        is_rfq=False,
        is_off_hours=False,
    )


PAIR = MarketPair("BTC", market(Venue.RISEX, "BTC/USDC"), market(Venue.LIGHTER, "BTC"))


def evidence_book(
    venue: Venue,
    *,
    received: int,
    session: str,
    bids=( ("99", "10"), ),
    asks=( ("101", "10"), ),
    recovery: int = 0,
    revision: int = 1,
    sequence: int | None = 1,
    checksum: int | str | None = 1,
) -> BookEvidence:
    return BookEvidence(
        venue=venue,
        canonical_market="BTC",
        bids=tuple(BookLevel(D(price), D(quantity)) for price, quantity in bids),
        asks=tuple(BookLevel(D(price), D(quantity)) for price, quantity in asks),
        received_monotonic_ns=received,
        stream_session_id=session,
        recovery_generation=recovery,
        book_revision=revision,
        sequence=sequence,
        checksum=checksum,
        sequence_valid=True,
        checksum_valid=True,
        received_utc=NOW,
        fresh=True,
    )


def trade(received: int = 101) -> TradeEvidence:
    return TradeEvidence(
        trade_event_key="trade-1",
        venue=Venue.RISEX,
        canonical_market="BTC",
        canonical_price=D("98"),
        canonical_quantity=D("1"),
        aggressor_side=Side.SELL,
        received_utc=NOW,
        received_monotonic_ns=received,
        stream_session_id="risex",
        recovery_generation=0,
        exchange_event_utc=NOW,
        exchange_event_time_provenance="fixture-event-time",
    )


def config(**changes) -> ShadowConfig:
    values = {
        "target_notionals_usd": (D("100"),),
        "target_margins_bps": (D("1"),),
        "ingress_queue_capacity": 16,
    }
    values.update(changes)
    return ShadowConfig(**values)


def test_public_duration_cap_is_shared_by_config_and_cli(tmp_path: Path, monkeypatch) -> None:
    assert ShadowConfig(duration_seconds=MAX_PUBLIC_DURATION_SECONDS).duration_seconds == 1_200
    with pytest.raises(ValueError, match="must not exceed"):
        ShadowConfig(duration_seconds=MAX_PUBLIC_DURATION_SECONDS + 1)

    captured: dict[str, object] = {}

    async def fake_smoke(*args, **kwargs):
        captured.update(kwargs)
        return {"record_count": 1, "byte_count": 1}

    monkeypatch.setattr(cli_module, "run_public_smoke", fake_smoke)
    monkeypatch.setattr(cli_module, "_source_commit", lambda: "fixture")
    assert (
        cli_module.main(
            [
                "smoke",
                "--store-root",
                str(tmp_path),
                "--duration-seconds",
                str(MAX_PUBLIC_DURATION_SECONDS),
                "--max-markets",
                "1",
            ]
        )
        == 0
    )
    assert captured["duration_seconds"] == MAX_PUBLIC_DURATION_SECONDS
    with pytest.raises(SystemExit, match="between 1 and 1200"):
        cli_module.main(
            [
                "smoke",
                "--store-root",
                str(tmp_path),
                "--duration-seconds",
                str(MAX_PUBLIC_DURATION_SECONDS + 1),
            ]
        )


@pytest.mark.asyncio
async def test_public_feed_honors_external_stop_and_duration_cap(monkeypatch) -> None:
    class FakeRisex:
        ws_base = "wss://risex.test"

        @staticmethod
        def market_id(_symbol):
            return 1

    class FakeLighter:
        ws_base = "wss://lighter.test"

        @staticmethod
        def market_id(_symbol):
            return 2

    runner = PublicFeedRunner(
        None,
        (PAIR,),
        IngressQueue(8),
        config=config(),
        risex_adapter=FakeRisex(),
        lighter_adapter=FakeLighter(),
    )
    started = asyncio.Event()
    started_count = 0

    async def fake_transport(venue, stop):
        nonlocal started_count
        started_count += 1
        if started_count == 2:
            started.set()
        await stop.wait()

    monkeypatch.setattr(runner, "_transport_loop", fake_transport)
    with pytest.raises(ValueError, match="1..1200"):
        await runner.run(duration_seconds=MAX_PUBLIC_DURATION_SECONDS + 1)

    requested_stop = asyncio.Event()
    drain = asyncio.Event()
    drain.set()
    task = asyncio.create_task(
        runner.run(
            duration_seconds=MAX_PUBLIC_DURATION_SECONDS,
            stop_event=requested_stop,
            drain_event=drain,
        )
    )
    await asyncio.wait_for(started.wait(), timeout=1)
    requested_stop.set()
    await asyncio.wait_for(task, timeout=1)
    assert runner.fatal_reason is None


@pytest.mark.asyncio
async def test_sample_stop_persists_on_silent_feed_wall_clock(tmp_path: Path) -> None:
    store = AppendOnlyEvidenceStore.create(
        tmp_path,
        metadata={"source_commit": "fixture", "evidence_mode": "OBSERVATIONAL"},
    )
    observer = SpreadObserver(
        config(sample_wall_clock_seconds=1),
        (PAIR,),
        store,
        sample_started_monotonic_ns=time.monotonic_ns(),
    )
    observer._sample_stop.wall_clock_limit_ns = 10_000_000
    seen: dict[str, object] = {}

    class Feed:
        ingress = observer.ingress

        async def run(self, **kwargs):
            seen.update(kwargs)
            await asyncio.wait_for(kwargs["stop_event"].wait(), timeout=1)
            await kwargs["drain_event"].wait()

    await runner_module.SpreadShadowRunner(Feed(), observer).run(duration_seconds=1)
    store.close()
    records = [json.loads(line) for line in store.path.read_text().splitlines()]
    stops = [record for record in records if record.get("kind") == "SAMPLE_STOP"]
    assert len(stops) == 1
    assert stops[0]["reason"] == "WALL_CLOCK_LIMIT"
    assert observer.sample_stop_signal is not None
    assert observer.sample_stop_signal.reason.value == "WALL_CLOCK_LIMIT"
    assert isinstance(seen["stop_event"], asyncio.Event)


@pytest.mark.asyncio
async def test_sample_stop_freezes_economics_and_drains_lighter_horizons(tmp_path: Path) -> None:
    store = AppendOnlyEvidenceStore.create(
        tmp_path,
        metadata={"source_commit": "fixture", "evidence_mode": "OBSERVATIONAL"},
    )
    base = time.monotonic_ns()
    observer = SpreadObserver(
        config(strict_episode_limit=1),
        (PAIR,),
        store,
        sample_started_monotonic_ns=base,
    )
    initial = (
        FeedBookEvent(evidence_book(Venue.RISEX, received=base, session="risex"), PAIR, "SNAPSHOT", "fixture"),
        FeedBookEvent(
            evidence_book(
                Venue.LIGHTER,
                received=base,
                session="lighter",
                bids=(("100", "10"),),
                asks=(("102", "10"),),
            ),
            PAIR,
            "SNAPSHOT",
            "fixture",
        ),
        FeedTradeEvent(trade(received=base + 1), PAIR, "fixture"),
    )
    delayed_book = FeedBookEvent(
        evidence_book(
            Venue.LIGHTER,
            received=base + 100_000_000,
            session="lighter",
            revision=2,
            bids=(("90", "10"),),
            asks=(("110", "10"),),
        ),
        PAIR,
        "DELTA",
        "fixture",
    )
    later_trade = FeedTradeEvent(
        TradeEvidence(
            trade_event_key="after-stop",
            venue=Venue.RISEX,
            canonical_market="BTC",
            canonical_price=D("98"),
            canonical_quantity=D("10"),
            aggressor_side=Side.SELL,
            received_utc=NOW,
            received_monotonic_ns=base + 200_000_000,
            stream_session_id="risex",
            recovery_generation=0,
            exchange_event_utc=NOW,
            exchange_event_time_provenance="fixture-event-time",
        ),
        PAIR,
        "fixture",
    )

    counts_after_stop: tuple[int, int, int] | None = None

    class Feed:
        ingress = observer.ingress

        async def run(self, **kwargs):
            nonlocal counts_after_stop
            for item in initial:
                assert self.ingress.offer(item)
            await asyncio.wait_for(kwargs["stop_event"].wait(), timeout=1)
            counts_after_stop = (
                observer.strict_episode_count,
                observer.optimistic_episode_count,
                observer.eligible_trade_count,
            )
            assert self.ingress.offer(delayed_book)
            assert self.ingress.offer(later_trade)
            await asyncio.wait_for(kwargs["drain_event"].wait(), timeout=3)

    await runner_module.SpreadShadowRunner(Feed(), observer).run(duration_seconds=60)
    store.close()
    records = [json.loads(line) for line in store.path.read_text().splitlines()]
    horizons = [record for record in records if record.get("kind") == "HEDGE_HORIZON"]
    assert counts_after_stop == (1, 1, 1)
    assert (observer.strict_episode_count, observer.optimistic_episode_count, observer.eligible_trade_count) == (1, 1, 1)
    assert len(horizons) == 8
    assert all(record["outcome"] == "HEDGE_FULL" for record in horizons)
    delayed = [record for record in horizons if record["horizon_ms"] == 1000]
    assert delayed and delayed[0]["book_received_monotonic_ns"] == base + 100_000_000
    assert len([record for record in records if record.get("kind") == "SAMPLE_STOP"]) == 1


def test_evidence_store_record_cap_reserves_terminal_failure_slot(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(store_module, "MAX_EVIDENCE_RECORDS", 4)
    monkeypatch.setattr(store_module, "TERMINAL_FAILURE_BYTES_RESERVE", 0)
    store = AppendOnlyEvidenceStore.create(tmp_path, metadata={"evidence_mode": "FIXTURE"})
    assert store.record_count == 1
    store.append_batch(({"kind": "EVIDENCE", "value": 1}, {"kind": "EVIDENCE", "value": 2}))
    with pytest.raises(EvidenceStorageLimitExceeded, match="RECORD_COUNT"):
        store.append_batch(({"kind": "EVIDENCE", "value": 3},))
    assert store.record_count == 3
    assert store.append_batch(({"kind": "RUN_FAILED", "fatal_reason": "EVIDENCE_STORAGE_LIMIT"},)) == (3,)
    assert store.record_count == 4
    store.close()


def test_evidence_store_file_cap_reserves_terminal_failure_bytes(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(store_module, "MAX_EVIDENCE_RECORDS", 100)
    store = AppendOnlyEvidenceStore.create(tmp_path, metadata={"evidence_mode": "FIXTURE"})
    reserve = 1_024
    monkeypatch.setattr(store_module, "TERMINAL_FAILURE_BYTES_RESERVE", reserve)
    monkeypatch.setattr(store_module, "MAX_EVIDENCE_FILE_BYTES", store.byte_count + reserve)
    with pytest.raises(EvidenceStorageLimitExceeded, match="FILE_BYTES"):
        store.append_batch(({"kind": "EVIDENCE", "value": "regular"},))
    store.append_batch(({"kind": "RUN_FAILED", "fatal_reason": "EVIDENCE_STORAGE_LIMIT"},))
    store.close()
    assert store.byte_count == store.path.stat().st_size
    assert store.byte_count <= store_module.MAX_EVIDENCE_FILE_BYTES


@pytest.mark.asyncio
async def test_evidence_store_limit_becomes_named_integrity_stop(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(store_module, "MAX_EVIDENCE_RECORDS", 2)
    store = AppendOnlyEvidenceStore.create(tmp_path, metadata={"evidence_mode": "FIXTURE"})
    observer = SpreadObserver(config(), (PAIR,), store)
    gap = FeedGapEvent(
        DataGapEvidence(
            source_venue=Venue.LIGHTER,
            canonical_market="BTC",
            stream_session_id="lighter",
            recovery_generation=0,
            gap_start_monotonic_ns=100,
            reason="CAP_TEST",
        )
    )
    with pytest.raises(EvidenceStorageLimitExceeded, match="RECORD_COUNT"):
        await observer.handle_gap(gap)
    assert observer.fatal_reason == "EVIDENCE_STORAGE_LIMIT"
    assert observer.sample_stop_signal is not None
    assert observer.sample_stop_signal.reason.value == "INTEGRITY_FAILURE"
    assert observer.sample_stop_event.is_set()
    store.append_batch(({"kind": "RUN_FAILED", "fatal_reason": observer.fatal_reason},))
    store.close()


@pytest.mark.asyncio
async def test_fixture_replay_is_labelled_and_captures_all_deadlines_without_lookahead(tmp_path: Path) -> None:
    store = AppendOnlyEvidenceStore.create(
        tmp_path,
        metadata={"source_commit": "fixture", "evidence_mode": "FIXTURE"},
    )
    observer = SpreadObserver(config(), (PAIR,), store)
    items = (
        FeedBookEvent(
            evidence_book(Venue.RISEX, received=100, session="risex"),
            PAIR,
            "SNAPSHOT",
            "fixture",
        ),
        FeedBookEvent(
            evidence_book(
                Venue.LIGHTER,
                received=100,
                session="lighter",
                bids=(("100", "10"),),
                asks=(("102", "10"),),
            ),
            PAIR,
            "SNAPSHOT",
            "fixture",
        ),
        FeedTradeEvent(trade(), PAIR, "fixture-time"),
        # This book is deliberately later than the 0 ms deadline.  Replay must
        # retain it for later horizons without applying it retroactively.
        FeedBookEvent(
            evidence_book(
                Venue.LIGHTER,
                received=102,
                session="lighter",
                bids=(("90", "10"),),
                asks=(("110", "10"),),
                revision=2,
                sequence=2,
            ),
            PAIR,
            "DELTA",
            "fixture",
        ),
    )
    await ReplayHarness(observer).run(items)
    await observer.close()
    store.close()

    records = [json.loads(line) for line in store.path.read_text().splitlines()]
    horizons = [record for record in records if record.get("kind") == "HEDGE_HORIZON"]
    assert len(horizons) == 8
    assert {record["fillability_model"] for record in horizons} == {
        "STRICT_LOWER_BOUND",
        "OPTIMISTIC_UPPER_BOUND",
    }
    assert {record["outcome"] for record in horizons} == {"HEDGE_FULL"}
    by_horizon = {record["horizon_ms"]: record["book_received_monotonic_ns"] for record in horizons}
    assert by_horizon[0] == 100
    assert by_horizon[300] == 102
    assert any(record.get("kind") == "REPLAY_MODE" for record in records)
    report = build_report(store.path)
    assert report["evidence_mode"] == "FIXTURE"
    assert report["horizon_record_count"] == 8
    assert {group["horizon_ms"] for group in report["groups"]} == {0, 300, 500, 1000}


@pytest.mark.asyncio
async def test_lighter_gap_after_would_fill_remains_named_hedge_gap(tmp_path: Path) -> None:
    store = AppendOnlyEvidenceStore.create(
        tmp_path,
        metadata={"source_commit": "fixture", "evidence_mode": "FIXTURE"},
    )
    observer = SpreadObserver(config(), (PAIR,), store)
    gap = FeedGapEvent(
    gap=DataGapEvidence(
            source_venue=Venue.LIGHTER,
            canonical_market="BTC",
            stream_session_id="lighter",
            recovery_generation=0,
            gap_start_monotonic_ns=101,
            reason="QUEUE_OVERFLOW",
        )
    )
    await ReplayHarness(
        observer
    ).run(
        (
            FeedBookEvent(evidence_book(Venue.RISEX, received=100, session="risex"), PAIR, "SNAPSHOT", "fixture"),
            FeedBookEvent(
                evidence_book(Venue.LIGHTER, received=100, session="lighter", bids=(("100", "10"),), asks=(("102", "10"),)),
                PAIR,
                "SNAPSHOT",
                "fixture",
            ),
            FeedTradeEvent(trade(), PAIR, "fixture-time"),
            gap,
        )
    )
    await observer.close()
    store.close()
    outcomes = {
        json.loads(line)["outcome"]
        for line in store.path.read_text().splitlines()
        if '"kind":"HEDGE_HORIZON"' in line
    }
    assert outcomes == {"HEDGE_DATA_GAP"}


def test_public_market_selection_proves_risex_public_units_before_eligibility() -> None:
    calls: list[str] = []
    risex_market = market(Venue.RISEX, "BTC/USDC")
    lighter_market = market(Venue.LIGHTER, "BTC")

    class FakeRisex:
        async def fetch_markets(self):
            return (risex_market,)

        async def fetch_book(self, symbol):
            calls.append(f"book:{symbol}")
            return None

        async def prime_recent_trade_evidence(self, candidate):
            calls.append(f"trade:{candidate.venue_symbol}")
            return candidate

    class FakeLighter:
        async def fetch_markets(self):
            return (lighter_market,)

    async def exercise():
        return await select_public_market_pairs(
            FakeRisex(), FakeLighter(), requested_markets=("BTC",), max_markets=1
        )

    selected = asyncio.run(exercise())
    assert selected == (MarketPair("BTC", risex_market, lighter_market),)
    assert calls == ["book:BTC/USDC", "trade:BTC/USDC"]


def test_risex_aggregate_recovery_latches_gap_for_every_selected_market() -> None:
    pair_two = MarketPair(
        "ETH", market(Venue.RISEX, "ETH/USDC", "ETH"), market(Venue.LIGHTER, "ETH", "ETH")
    )

    class FakeRisex:
        def market_id(self, symbol):
            return {"BTC/USDC": 1, "ETH/USDC": 2}[symbol]

        @staticmethod
        def orderbook_unsubscription():
            return {"unsubscribe": "books"}

        @staticmethod
        def orderbook_subscription(ids):
            return {"subscribe": tuple(ids)}

    class FakeLighter:
        def market_id(self, symbol):
            return {"BTC": 3, "ETH": 4}[symbol]

    runner = PublicFeedRunner(
        None,
        (PAIR, pair_two),
        IngressQueue(16),
        risex_adapter=FakeRisex(),
        lighter_adapter=FakeLighter(),
    )

    async def exercise():
        runner.begin_connection(Venue.RISEX, "r")
        await runner._recover_market(
            runner.state(Venue.RISEX, "BTC"), None, reason="RISEX_CHECKSUM_OR_SEQUENCE_INVALID"
        )
        first = await runner.ingress.next_item()
        second = await runner.ingress.next_item()
        return first, second

    first, second = asyncio.run(exercise())
    assert {first.gap.canonical_market, second.gap.canonical_market} == {"BTC", "ETH"}
    assert runner.state(Venue.RISEX, "BTC").recovery_generation == 1
    assert runner.state(Venue.RISEX, "ETH").recovery_generation == 1


def test_lighter_fresh_subscription_snapshot_restarts_nonce_chain() -> None:
    from risex_farmer.exchanges.lighter import LighterAdapter

    class FakeRisex:
        def market_id(self, _symbol):
            return 1

    lighter = LighterAdapter(None)
    lighter._market_ids = {"BTC": 2}
    lighter._symbols_by_id = {2: "BTC"}

    class FakeWebsocket:
        def __init__(self):
            self.sent = []

        async def send_json(self, payload):
            self.sent.append(payload)

    ingress = IngressQueue(16)
    runner = PublicFeedRunner(
        None,
        (PAIR,),
        ingress,
        risex_adapter=FakeRisex(),
        lighter_adapter=lighter,
    )
    ws = FakeWebsocket()
    timestamp_ms = str(int(NOW.timestamp() * 1000))

    def book_payload(message_type, *, nonce, begin_nonce):
        return {
            "type": message_type,
            "channel": "order_book/2",
            "market_id": 2,
            "timestamp": timestamp_ms,
            "order_book": {
                "code": 0,
                "nonce": nonce,
                "begin_nonce": begin_nonce,
                "bids": [{"price": "100", "size": "10"}],
                "asks": [{"price": "102", "size": "10"}],
            },
        }

    async def exercise():
        runner.begin_connection(Venue.LIGHTER, "l")
        await runner.ingest_lighter_payload(
            book_payload("subscribed/order_book", nonce=5, begin_nonce=5), ws=ws
        )
        await runner.ingest_lighter_payload(
            book_payload("update/order_book", nonce=6, begin_nonce=5), ws=ws
        )
        await runner.ingest_lighter_payload(
            book_payload("update/order_book", nonce=9, begin_nonce=8), ws=ws
        )
        first = await ingress.next_item()
        second = await ingress.next_item()
        third = await ingress.next_item()
        assert isinstance(first, FeedBookEvent)
        assert isinstance(second, FeedBookEvent)
        assert isinstance(third, FeedGapEvent)
        assert third.gap.reason == "LIGHTER_SEQUENCE_INVALID_FRESH_RESUBSCRIBE"
        await runner.ingest_lighter_payload(
            book_payload("subscribed/order_book", nonce=12, begin_nonce=12), ws=ws
        )
        fresh = await ingress.next_item()
        assert isinstance(fresh, FeedBookEvent)
        assert fresh.book.recovery_generation == 1
        assert fresh.book.sequence == 12

    asyncio.run(exercise())
    assert any(payload.get("channel") == "order_book/2" for payload in ws.sent)


def test_store_is_fresh_owner_only_append_only_and_rejects_secret_fields(tmp_path: Path) -> None:
    store = AppendOnlyEvidenceStore.create(
        tmp_path,
        metadata={"source_commit": "fixture", "evidence_mode": "FIXTURE"},
    )
    with pytest.raises(ValueError):
        store.append_batch(({"kind": "BAD", "credential": "never"},))
    store.append_batch(({"kind": "A", "observed_monotonic_ns": 2}, {"kind": "B", "observed_monotonic_ns": 1}))
    store.close()
    records = [json.loads(line) for line in store.path.read_text().splitlines()]
    assert [record["record_index"] for record in records] == list(range(len(records)))
    assert store_permissions(store.path) == 0o600


@pytest.mark.asyncio
async def test_observer_store_batch_flushes_by_count_interval_and_final_tail(tmp_path: Path) -> None:
    store = AppendOnlyEvidenceStore.create(
        tmp_path,
        metadata={"source_commit": "fixture", "evidence_mode": "FIXTURE"},
    )
    original_append_batch = store.append_batch
    batches: list[tuple[dict[str, object], ...]] = []

    def observed_append(records):
        batches.append(tuple(dict(record) for record in records))
        return original_append_batch(records)

    store.append_batch = observed_append
    observer = SpreadObserver(
        config(store_batch_size=3, store_batch_interval_seconds=0.03),
        (PAIR,),
        store,
    )

    await observer._append(
        ({"kind": "EVIDENCE", "record_id": "one", "observed_monotonic_ns": 1},)
    )
    assert observer.pending_record_count == 1
    assert len(batches) == 0

    await observer._append(
        (
            {"kind": "EVIDENCE", "record_id": "two", "observed_monotonic_ns": 2},
            {"kind": "EVIDENCE", "record_id": "three", "observed_monotonic_ns": 3},
        )
    )
    assert observer.pending_record_count == 0
    assert [len(batch) for batch in batches] == [3]

    await observer._append(
        ({"kind": "EVIDENCE", "record_id": "interval", "observed_monotonic_ns": 4},)
    )
    await asyncio.sleep(0.08)
    assert observer.pending_record_count == 0
    assert [len(batch) for batch in batches] == [3, 1]

    await observer._append(
        ({"kind": "EVIDENCE", "record_id": "tail", "observed_monotonic_ns": 5},)
    )
    assert observer.pending_record_count == 1
    await observer.close()
    assert observer.pending_record_count == 0
    assert [len(batch) for batch in batches] == [3, 1, 1]
    assert store.record_count == 6
    store.close()


@pytest.mark.asyncio
async def test_observer_writer_serializes_concurrent_append_order_and_unique_indices(
    tmp_path: Path,
) -> None:
    store = AppendOnlyEvidenceStore.create(
        tmp_path,
        metadata={"source_commit": "fixture", "evidence_mode": "FIXTURE"},
    )
    observer = SpreadObserver(
        config(store_batch_size=2, store_batch_interval_seconds=1),
        (PAIR,),
        store,
    )
    gate = asyncio.Event()

    async def append_after_gate(record_id: str, observed: int) -> None:
        await gate.wait()
        await observer._append(
            ({"kind": "EVIDENCE", "record_id": record_id, "observed_monotonic_ns": observed},)
        )

    writers = tuple(
        asyncio.create_task(append_after_gate(str(index), index))
        for index in range(8)
    )
    gate.set()
    await asyncio.gather(*writers)
    await observer.close()
    records = [json.loads(line) for line in store.path.read_text().splitlines()]
    evidence = [record for record in records if record.get("kind") == "EVIDENCE"]
    assert [record["record_index"] for record in records] == list(range(len(records)))
    assert [record["record_id"] for record in evidence] == [str(index) for index in range(8)]
    store.close()


@pytest.mark.asyncio
async def test_observer_writer_latches_failure_without_retrying_ambiguous_batch(
    tmp_path: Path,
) -> None:
    store = AppendOnlyEvidenceStore.create(
        tmp_path,
        metadata={"source_commit": "fixture", "evidence_mode": "FIXTURE"},
    )
    calls = 0

    def failing_append(_records):
        nonlocal calls
        calls += 1
        raise OSError("fixture sync failure")

    store.append_batch = failing_append
    observer = SpreadObserver(
        config(store_batch_size=8, store_batch_interval_seconds=0.01),
        (PAIR,),
        store,
    )
    await observer._append(
        ({"kind": "EVIDENCE", "record_id": "one", "observed_monotonic_ns": 1},)
    )
    await asyncio.sleep(0.04)
    assert observer.fatal_reason == "EVIDENCE_STORE_WRITE_FAILED"
    assert observer.sample_stop_event.is_set()
    assert calls == 1
    with pytest.raises(OSError, match="fixture sync failure"):
        await observer.close()
    assert calls == 1
    store.close()


@pytest.mark.asyncio
async def test_observed_three_market_load_has_bounded_lossless_batched_drain() -> None:
    observed_book_events = 39_877
    observed_quote_rows = 643_392
    observed_gate_seconds = 1_200
    quote_rows_per_event = 17
    rows_per_event = 1 + quote_rows_per_event
    expected_rows = observed_book_events * rows_per_event
    assert expected_rows >= observed_book_events + observed_quote_rows

    class CountingStore:
        run_id = "fixture-throughput"

        def __init__(self) -> None:
            self.append_calls = 0
            self.record_count = 0
            self.batch_sizes: list[int] = []
            self.expected_event = 0
            self.expected_slot = 0
            self.market_mask = 0

        def append_batch(self, records) -> None:
            # A bounded deterministic delay represents the synchronous
            # serialization/fsync cost without creating a multi-gigabyte file.
            time.sleep(0.0002)
            batch = tuple(records)
            self.append_calls += 1
            self.batch_sizes.append(len(batch))
            for record in batch:
                market_name = record["canonical_market"]
                expected_market = ("BTC", "ETH", "SOL")[self.expected_event % 3]
                if market_name != expected_market:
                    raise AssertionError("three-market append order changed")
                if record["event_index"] != self.expected_event:
                    raise AssertionError("event append order changed")
                if self.expected_slot == 0:
                    if record["kind"] != "BOOK":
                        raise AssertionError("book row missing at event boundary")
                elif record["kind"] != "QUOTE":
                    raise AssertionError("quote row missing inside event batch")
                self.expected_slot += 1
                if self.expected_slot == rows_per_event:
                    self.expected_slot = 0
                    self.expected_event += 1
                self.record_count += 1
                self.market_mask |= 1 << (self.expected_event % 3)

    store = CountingStore()
    pair_two = MarketPair(
        "ETH",
        market(Venue.RISEX, "ETH/USDC", "ETH"),
        market(Venue.LIGHTER, "ETH", "ETH"),
    )
    pair_three = MarketPair(
        "SOL",
        market(Venue.RISEX, "SOL/USDC", "SOL"),
        market(Venue.LIGHTER, "SOL", "SOL"),
    )
    observer = SpreadObserver(
        config(
            ingress_queue_capacity=4096,
            store_batch_size=128,
            store_batch_interval_seconds=0.25,
        ),
        (PAIR, pair_two, pair_three),
        store,
    )
    fixture_items = tuple(
        FeedBookEvent(
            BookEvidence(
                venue=Venue.LIGHTER,
                canonical_market=pair.canonical_market,
                bids=(BookLevel(D("99"), D("10")),),
                asks=(BookLevel(D("101"), D("10")),),
                received_monotonic_ns=1,
                stream_session_id=f"lighter-{pair.canonical_market}",
                recovery_generation=0,
                book_revision=1,
                sequence=1,
                checksum=1,
                sequence_valid=True,
                checksum_valid=True,
                received_utc=NOW,
                fresh=True,
            ),
            pair,
            "SNAPSHOT",
            "fixture",
        )
        for pair in (PAIR, pair_two, pair_three)
    )
    processed = 0
    offer_failures = 0
    maximum_queue = 0
    maximum_pending = 0
    burst_size = 64
    burst_interval_seconds = 0.01
    planned_offered_rate = burst_size / burst_interval_seconds
    observed_offered_rate = observed_book_events / observed_gate_seconds
    assert planned_offered_rate > observed_offered_rate

    async def synthetic_handle(_item) -> None:
        nonlocal processed, maximum_pending
        event_index = processed
        processed += 1
        market_name = ("BTC", "ETH", "SOL")[event_index % 3]
        rows = [
            {
                "kind": "BOOK",
                "canonical_market": market_name,
                "event_index": event_index,
                "row_index": 0,
                "observed_monotonic_ns": event_index,
            }
        ]
        rows.extend(
            {
                "kind": "QUOTE",
                "canonical_market": market_name,
                "event_index": event_index,
                "row_index": row_index,
                "observed_monotonic_ns": event_index,
            }
            for row_index in range(1, rows_per_event)
        )
        await observer._append(rows)
        maximum_pending = max(
            maximum_pending, getattr(observer, "pending_record_count", 0)
        )

    observer.handle_item = synthetic_handle
    consumer = asyncio.create_task(observer.consume())
    offer_started = time.monotonic()
    for burst_start in range(0, observed_book_events, burst_size):
        burst_end = min(observed_book_events, burst_start + burst_size)
        for event_index in range(burst_start, burst_end):
            if not observer.ingress.offer(fixture_items[event_index % 3]):
                offer_failures += 1
            maximum_queue = max(maximum_queue, observer.ingress.qsize)
        if burst_end < observed_book_events:
            await asyncio.sleep(burst_interval_seconds)
    offer_elapsed = time.monotonic() - offer_started
    achieved_offered_rate = observed_book_events / offer_elapsed
    assert achieved_offered_rate >= planned_offered_rate * 0.5
    assert achieved_offered_rate >= observed_offered_rate
    assert offer_elapsed <= observed_book_events / observed_offered_rate
    observer.ingress.close()
    await asyncio.wait_for(consumer, timeout=30)
    close_started = time.monotonic()
    await asyncio.wait_for(observer.close(), timeout=5)
    close_elapsed = time.monotonic() - close_started

    assert processed == observed_book_events
    assert offer_failures == 0
    assert maximum_queue < observer.ingress.capacity
    assert maximum_queue > 0
    assert maximum_pending <= observer.config.store_batch_size
    assert maximum_pending > 0
    assert store.record_count == expected_rows
    assert store.expected_event == observed_book_events
    assert store.expected_slot == 0
    assert store.market_mask == 0b111
    assert len(store.batch_sizes) == store.append_calls
    assert max(store.batch_sizes) <= observer.config.store_batch_size
    # The pre-correction writer performs one store call per event; the
    # configured 128-record batching contract must remain materially below it.
    assert store.append_calls < observed_book_events // 4
    assert close_elapsed < 5


@pytest.mark.asyncio
async def test_batched_cap_failure_leaves_reserved_terminal_marker_slot(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(store_module, "MAX_EVIDENCE_RECORDS", 2)
    monkeypatch.setattr(store_module, "TERMINAL_FAILURE_BYTES_RESERVE", 0)
    store = AppendOnlyEvidenceStore.create(
        tmp_path,
        metadata={"source_commit": "fixture", "evidence_mode": "FIXTURE"},
    )
    observer = SpreadObserver(
        config(store_batch_size=8, store_batch_interval_seconds=1),
        (PAIR,),
        store,
    )
    await observer._append(
        ({"kind": "EVIDENCE", "record_id": "over-cap", "observed_monotonic_ns": 1},)
    )
    with pytest.raises(EvidenceStorageLimitExceeded, match="RECORD_COUNT"):
        await observer.close()
    assert observer.fatal_reason == "EVIDENCE_STORAGE_LIMIT"
    store.append_batch(({"kind": "RUN_FAILED", "fatal_reason": observer.fatal_reason},))
    store.close()
    records = [json.loads(line) for line in (store.path).read_text().splitlines()]
    assert [record["kind"] for record in records] == ["RUN_METADATA", "RUN_FAILED"]


def test_queue_overflow_latches_explicit_identity_gap_without_blocking() -> None:
    queue = IngressQueue(1)
    first = FeedBookEvent(evidence_book(Venue.LIGHTER, received=10, session="l"), PAIR, "SNAPSHOT", "fixture")
    second = FeedBookEvent(evidence_book(Venue.LIGHTER, received=11, session="l"), PAIR, "DELTA", "fixture")
    assert queue.offer(first)
    assert not queue.offer(second)
    assert queue.has_latched_gap

    async def drain():
        assert isinstance(await queue.next_item(), FeedGapEvent)
        assert isinstance(await queue.next_item(), FeedBookEvent)

    asyncio.run(drain())


@pytest.mark.asyncio
async def test_empty_ingress_close_wakes_already_blocked_consumer() -> None:
    queue = IngressQueue(2)
    waiting = asyncio.create_task(queue.next_item())
    await asyncio.sleep(0)
    assert not waiting.done()

    queue.close()

    assert await asyncio.wait_for(waiting, timeout=1) is None
    queue.close()
    assert not queue.has_pending


@pytest.mark.asyncio
async def test_ingress_close_drains_latch_and_queue_and_rejects_late_offers() -> None:
    queue = IngressQueue(1)
    queued = FeedBookEvent(
        evidence_book(Venue.LIGHTER, received=10, session="lighter"),
        PAIR,
        "SNAPSHOT",
        "fixture",
    )
    overflow = FeedBookEvent(
        evidence_book(Venue.LIGHTER, received=11, session="lighter", revision=2),
        PAIR,
        "DELTA",
        "fixture",
    )
    late = FeedBookEvent(
        evidence_book(Venue.LIGHTER, received=12, session="lighter", revision=3),
        PAIR,
        "DELTA",
        "fixture",
    )
    assert queue.offer(queued)
    assert not queue.offer(overflow)
    queue.close()
    assert not queue.offer(late)

    first = await queue.next_item()
    second = await queue.next_item()
    third = await queue.next_item()

    assert isinstance(first, FeedGapEvent)
    assert first.gap.reason == "QUEUE_OVERFLOW"
    assert isinstance(second, FeedBookEvent)
    assert second == queued
    assert third is None
    assert not queue.has_pending


@pytest.mark.asyncio
async def test_observer_rejects_queued_old_identity_after_overflow_until_fresh_snapshot(tmp_path: Path) -> None:
    store = AppendOnlyEvidenceStore.create(
        tmp_path,
        metadata={"source_commit": "fixture", "evidence_mode": "FIXTURE"},
    )
    observer = SpreadObserver(config(ingress_queue_capacity=1), (PAIR,), store)
    await observer.handle_item(
        FeedBookEvent(evidence_book(Venue.RISEX, received=100, session="risex"), PAIR, "SNAPSHOT", "fixture")
    )
    await observer.handle_item(
        FeedBookEvent(
            evidence_book(
                Venue.LIGHTER,
                received=100,
                session="lighter",
                bids=(("100", "10"),),
                asks=(("102", "10"),),
            ),
            PAIR,
            "SNAPSHOT",
            "fixture",
        )
    )
    assert observer.active_version_count > 0
    old_book = FeedBookEvent(
        evidence_book(Venue.RISEX, received=200, session="risex", revision=2, sequence=2),
        PAIR,
        "DELTA",
        "fixture",
    )
    overflow_book = FeedBookEvent(
        evidence_book(Venue.RISEX, received=201, session="risex", revision=3, sequence=3),
        PAIR,
        "DELTA",
        "fixture",
    )
    assert observer.ingress.offer(old_book)
    assert not observer.ingress.offer(overflow_book)
    observer.ingress.close()
    await observer.consume()
    assert observer.active_version_count == 0
    fresh_book = FeedBookEvent(
        evidence_book(
            Venue.RISEX,
            received=202,
            session="risex-new",
            recovery=1,
            revision=1,
            sequence=1,
        ),
        PAIR,
        "SNAPSHOT",
        "fixture",
    )
    await observer.handle_book(fresh_book)
    assert observer.active_version_count > 0
    await observer.close()
    store.close()


@pytest.mark.asyncio
async def test_stale_paired_book_cannot_create_active_quote(tmp_path: Path) -> None:
    store = AppendOnlyEvidenceStore.create(
        tmp_path,
        metadata={"source_commit": "fixture", "evidence_mode": "FIXTURE"},
    )
    observer = SpreadObserver(
        config(freshness_max_age_ns=10),
        (PAIR,),
        store,
    )
    await observer.handle_book(
        FeedBookEvent(evidence_book(Venue.RISEX, received=100, session="risex"), PAIR, "SNAPSHOT", "fixture")
    )
    await observer.handle_book(
        FeedBookEvent(
            evidence_book(
                Venue.LIGHTER,
                received=0,
                session="lighter",
                bids=(("100", "10"),),
                asks=(("102", "10"),),
            ),
            PAIR,
            "SNAPSHOT",
            "fixture",
        )
    )
    assert observer.active_version_count == 0
    await observer.close()
    store.close()


@pytest.mark.asyncio
async def test_delayed_would_fill_retains_books_through_detection_based_deadline(tmp_path: Path) -> None:
    store = AppendOnlyEvidenceStore.create(
        tmp_path,
        metadata={"source_commit": "fixture", "evidence_mode": "FIXTURE"},
    )
    clock = [100]
    observer = SpreadObserver(
        config(book_history_retention_ns=1, freshness_max_age_ns=2_000_000_000),
        (PAIR,),
        store,
        monotonic_ns=lambda: clock[0],
    )
    await observer.handle_book(
        FeedBookEvent(evidence_book(Venue.RISEX, received=100, session="risex"), PAIR, "SNAPSHOT", "fixture")
    )
    await observer.handle_book(
        FeedBookEvent(
            evidence_book(
                Venue.LIGHTER,
                received=100,
                session="lighter",
                bids=(("100", "10"),),
                asks=(("102", "10"),),
            ),
            PAIR,
            "SNAPSHOT",
            "fixture",
        )
    )
    clock[0] = 200
    await observer.handle_trade(FeedTradeEvent(trade(received=200), PAIR, "fixture-time"))
    assert observer.pending_episode_count == 1
    version_id = next(iter(observer._pending))
    assert observer.history._pending[version_id][1] == 1_000_000_200
    clock[0] = 1_000_000_200
    await observer.handle_book(
        FeedBookEvent(
            evidence_book(
                Venue.LIGHTER,
                received=clock[0],
                session="lighter",
                bids=(("100", "10"),),
                asks=(("102", "10"),),
                revision=2,
                sequence=2,
            ),
            PAIR,
            "DELTA",
            "fixture",
        )
    )
    assert any(book.received_monotonic_ns == 100 for book in observer.history.books(Venue.LIGHTER, "BTC"))
    await observer.flush_pending(force=True)
    horizons = [
        json.loads(line)
        for line in store.path.read_text().splitlines()
        if '"kind":"HEDGE_HORIZON"' in line
    ]
    assert {row["horizon_ms"] for row in horizons} == {0, 300, 500, 1000}
    assert next(row for row in horizons if row["horizon_ms"] == 1000)["book_received_monotonic_ns"] == 1_000_000_200
    await observer.close()
    store.close()


@pytest.mark.asyncio
async def test_observer_shutdown_keeps_pending_horizon_outcomes_explicit(tmp_path: Path) -> None:
    store = AppendOnlyEvidenceStore.create(
        tmp_path,
        metadata={"source_commit": "fixture", "evidence_mode": "FIXTURE"},
    )
    clock = [200]
    observer = SpreadObserver(
        config(freshness_max_age_ns=2_000_000_000),
        (PAIR,),
        store,
        monotonic_ns=lambda: clock[0],
    )
    await observer.handle_book(
        FeedBookEvent(
            evidence_book(Venue.RISEX, received=100, session="risex"),
            PAIR,
            "SNAPSHOT",
            "fixture",
        )
    )
    await observer.handle_book(
        FeedBookEvent(
            evidence_book(
                Venue.LIGHTER,
                received=100,
                session="lighter",
                bids=(("100", "10"),),
                asks=(("102", "10"),),
            ),
            PAIR,
            "SNAPSHOT",
            "fixture",
        )
    )
    observer._replay_mode = True
    await observer.handle_trade(FeedTradeEvent(trade(received=200), PAIR, "fixture-time"))
    observer._replay_mode = False
    assert observer.pending_episode_count == 1

    await observer.close()
    records = [json.loads(line) for line in store.path.read_text().splitlines()]
    horizons = [record for record in records if record.get("kind") == "HEDGE_HORIZON"]
    by_horizon = {record["horizon_ms"]: record for record in horizons}
    assert by_horizon[0]["outcome"] == "HEDGE_FULL"
    for horizon_ms in (300, 500, 1000):
        assert by_horizon[horizon_ms]["outcome"] == "HEDGE_DATA_GAP"
        assert by_horizon[horizon_ms]["gap_reason"] == "RUN_STOPPED_BEFORE_HORIZON"
        assert by_horizon[horizon_ms]["entry_edge_usd"] is None
    store.close()


def test_book_history_capacity_is_explicit_not_silent_eviction() -> None:
    history = BookHistory(retention_ns=1000, capacity=1)
    history.add_book(evidence_book(Venue.LIGHTER, received=1, session="l"))
    history.register_pending("episode", 1, 100)
    with pytest.raises(HistoryCapacityExceeded) as caught:
        history.add_book(evidence_book(Venue.LIGHTER, received=2, session="l", revision=2, sequence=2))
    assert caught.value.gap.reason == "BOOK_HISTORY_CAPACITY"


def test_stale_gap_history_is_time_bounded_while_pending_books_are_retained() -> None:
    history = BookHistory(retention_ns=10)
    history.add_book(evidence_book(Venue.LIGHTER, received=100, session="expected"))
    history.register_pending(
        "episode",
        100,
        200,
        identity=(Venue.LIGHTER, "BTC", "expected", 0),
    )
    for received in range(0, 1_000):
        history.add_gap(
            DataGapEvidence(
                source_venue=Venue.LIGHTER,
                canonical_market="BTC",
                stream_session_id="unrelated",
                recovery_generation=0,
                gap_start_monotonic_ns=received,
                reason="STALE_DIAGNOSTIC_GAP",
            )
        )
    assert len(history.gaps()) <= 11
    assert history.book_count == 1


def test_public_feed_recovery_gates_deltas_until_fresh_snapshot() -> None:
    class FakeWebsocket:
        async def send_json(self, _payload):
            return None

    class FakeRisex:
        ws_base = "wss://risex.test"

        def market_id(self, _symbol):
            return 1

        @staticmethod
        def orderbook_subscription(_ids):
            return {"subscribe": "books"}

        @staticmethod
        def orderbook_unsubscription():
            return {"unsubscribe": "books"}

        @staticmethod
        def trades_subscription(_ids):
            return {"subscribe": "trades"}

        @staticmethod
        def client_ping_action():
            return {"method": "ping"}

        def normalize_book_message(self, payload, *, received_at):
            if payload["type"] == "snapshot":
                return OrderBook(Venue.RISEX, "BTC/USDC", (BookLevel(D("99"), D("10")),), (BookLevel(D("101"), D("10")),), received_at, 1)
            return BookDelta(Venue.RISEX, "BTC/USDC", (), (), received_at, 2, checksum=0)

        def normalize_trade(self, *_args, **_kwargs):
            raise AssertionError("trade normalization is not part of this recovery test")

    class FakeLighter:
        ws_base = "wss://lighter.test"

        def market_id(self, _symbol):
            return 2

        @staticmethod
        def subscription(_kind, _market_id):
            return {"subscribe": "book"}

    runner = PublicFeedRunner(
        None,
        (PAIR,),
        IngressQueue(16),
        config=config(),
        risex_adapter=FakeRisex(),
        lighter_adapter=FakeLighter(),
    )
    ws = FakeWebsocket()

    async def exercise():
        runner.begin_connection(Venue.RISEX, "r")
        await runner.ingest_risex_payload(
            {
                "method": "subscribe",
                "channel": "orderbook",
                "type": "subscribed",
                "status": "success",
                "data": {"market_ids": [1]},
            },
            ws=ws,
        )
        assert runner.fatal_reason is None
        await runner.ingest_risex_payload({"channel": "orderbook", "type": "snapshot", "market_id": "1", "block_number": 1, "log_index": 1, "data": {"market_id": 1, "bids": [], "asks": []}}, ws=ws)
        await runner.ingest_risex_payload({"channel": "orderbook", "type": "update", "market_id": "1"}, ws=ws)
        first = await runner.ingress.next_item()
        second = await runner.ingress.next_item()
        assert isinstance(first, FeedBookEvent)
        assert isinstance(second, FeedGapEvent)
        assert runner.state(Venue.RISEX, "BTC").awaiting_snapshot

    asyncio.run(exercise())


@pytest.mark.asyncio
async def test_risex_trade_channel_with_block_number_is_not_book_recovery() -> None:
    from risex_farmer.exchanges.risex import RisexAdapter

    risex = RisexAdapter(None)
    risex._market_ids = {"BTC/USDC": "1"}
    risex._symbols_by_id = {"1": "BTC/USDC"}
    risex._raw_markets = {
        "BTC/USDC": {"config": {"step_size": "1", "step_price": "1"}}
    }

    class FakeLighter:
        def market_id(self, _symbol):
            return 2

    runner = PublicFeedRunner(
        None,
        (PAIR,),
        IngressQueue(16),
        risex_adapter=risex,
        lighter_adapter=FakeLighter(),
    )

    async def exercise():
        runner.begin_connection(Venue.RISEX, "r")
        await runner.ingest_risex_payload(
            {
                "channel": "trades",
                "type": "update",
                "market_id": "1",
                "block_number": 123,
                "worker_timestamp": "1800000000000000000",
                "data": {
                    "id": "trade-realistic",
                    "maker_side": 0,
                    "price": "98",
                    "size": "1",
                },
            }
        )
        item = await runner.ingress.next_item()
        assert isinstance(item, FeedTradeEvent)
        assert item.trade.trade_event_key == "RISEX|BTC/USDC|trade-realistic"
        assert runner.state(Venue.RISEX, "BTC").recovery_generation == 0
        assert not runner.ingress.has_pending

    await exercise()


def test_public_text_payload_parser_preserves_decimal_wire_values() -> None:
    from types import SimpleNamespace

    parsed = PublicFeedRunner._payload(SimpleNamespace(data='{"price":1.25}'))
    assert parsed is not None
    assert parsed["price"] == D("1.25")
    assert PublicFeedRunner._payload(SimpleNamespace(data="not-json")) is None


@pytest.mark.asyncio
async def test_risex_server_ping_is_consumed_and_read_loop_continues() -> None:
    class FakeRisex:
        @staticmethod
        def market_id(_symbol):
            return 1

        @staticmethod
        def handle_server_ping(payload):
            assert payload == b"server"
            return SimpleNamespace(payload=b"pong", connection_confirmed=True)

    class FakeLighter:
        @staticmethod
        def market_id(_symbol):
            return 2

    class FakeWebsocket:
        def __init__(self):
            self.messages = iter(
                (
                    SimpleNamespace(type="PING", data=b"server"),
                    SimpleNamespace(type="CLOSE", data=b""),
                )
            )
            self.pongs = []

        async def receive(self):
            return next(self.messages)

        async def pong(self, payload):
            self.pongs.append(payload)

    runner = PublicFeedRunner(
        None,
        (PAIR,),
        IngressQueue(16),
        risex_adapter=FakeRisex(),
        lighter_adapter=FakeLighter(),
    )
    runner.begin_connection(Venue.RISEX, "risex")
    websocket = FakeWebsocket()

    await runner._read_risex(websocket, asyncio.Event())

    assert websocket.pongs == [b"pong"]
    assert runner.fatal_reason is None
    assert not runner.ingress.has_pending


@pytest.mark.asyncio
async def test_risex_invalid_control_frame_still_fails_closed() -> None:
    class FakeRisex:
        @staticmethod
        def market_id(_symbol):
            return 1

        @staticmethod
        def handle_server_ping(_payload):
            raise AssertionError("invalid frame must not be treated as PING")

    class FakeLighter:
        @staticmethod
        def market_id(_symbol):
            return 2

    class FakeWebsocket:
        async def receive(self):
            return SimpleNamespace(type="BINARY", data=b"invalid")

    runner = PublicFeedRunner(
        None,
        (PAIR,),
        IngressQueue(16),
        risex_adapter=FakeRisex(),
        lighter_adapter=FakeLighter(),
    )
    runner.begin_connection(Venue.RISEX, "risex")

    await runner._read_risex(FakeWebsocket(), asyncio.Event())

    assert runner.fatal_reason == "RISEX_PUBLIC_FRAME_INVALID"
    item = await runner.ingress.next_item()
    assert isinstance(item, FeedGapEvent)
    assert item.gap.reason == "RISEX_PUBLIC_FRAME_INVALID"


@pytest.mark.asyncio
async def test_transport_planned_stop_uses_only_explicit_planned_stop_gap() -> None:
    from types import SimpleNamespace

    class FakeWebsocket:
        def __init__(self) -> None:
            self.entered = asyncio.Event()
            self.release = asyncio.Event()

        async def __aenter__(self):
            self.entered.set()
            return self

        async def __aexit__(self, *_args):
            return False

        async def send_json(self, _payload):
            return None

        async def receive(self):
            await self.release.wait()
            return SimpleNamespace(type="CLOSE")

    class FakeSession:
        def __init__(self, websocket) -> None:
            self.websocket = websocket

        def ws_connect(self, *_args, **_kwargs):
            return self.websocket

    class FakeRisex:
        ws_base = "wss://risex.test"

        def market_id(self, _symbol):
            return 1

        @staticmethod
        def orderbook_subscription(_ids):
            return {"subscribe": "books"}

        @staticmethod
        def trades_subscription(_ids):
            return {"subscribe": "trades"}

    class FakeLighter:
        def market_id(self, _symbol):
            return 2

    websocket = FakeWebsocket()
    runner = PublicFeedRunner(
        FakeSession(websocket),
        (PAIR,),
        IngressQueue(16),
        risex_adapter=FakeRisex(),
        lighter_adapter=FakeLighter(),
    )
    stop = asyncio.Event()
    task = asyncio.create_task(runner._transport_loop(Venue.RISEX, stop))
    await websocket.entered.wait()
    stop.set()
    websocket.release.set()
    await asyncio.wait_for(task, timeout=1)

    assert runner.state(Venue.RISEX, "BTC").connected
    assert not runner.ingress.has_pending
    runner.disconnect(Venue.RISEX, reason="PUBLIC_SMOKE_STOPPED")
    item = await runner.ingress.next_item()
    assert isinstance(item, FeedGapEvent)
    assert item.gap.reason == "PUBLIC_SMOKE_STOPPED"


@pytest.mark.asyncio
async def test_transport_early_close_remains_socket_disconnect_gap() -> None:
    from types import SimpleNamespace

    class FakeWebsocket:
        def __init__(self) -> None:
            self.entered = asyncio.Event()

        async def __aenter__(self):
            self.entered.set()
            return self

        async def __aexit__(self, *_args):
            return False

        async def send_json(self, _payload):
            return None

        async def receive(self):
            return SimpleNamespace(type="CLOSE")

    class FakeSession:
        def __init__(self, websocket) -> None:
            self.websocket = websocket
            self.calls = 0

        def ws_connect(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls > 1:
                raise RuntimeError("reconnect is outside this adverse fixture")
            return self.websocket

    class FakeRisex:
        ws_base = "wss://risex.test"

        def market_id(self, _symbol):
            return 1

        @staticmethod
        def orderbook_subscription(_ids):
            return {"subscribe": "books"}

        @staticmethod
        def trades_subscription(_ids):
            return {"subscribe": "trades"}

    class FakeLighter:
        def market_id(self, _symbol):
            return 2

    websocket = FakeWebsocket()
    runner = PublicFeedRunner(
        FakeSession(websocket),
        (PAIR,),
        IngressQueue(16),
        risex_adapter=FakeRisex(),
        lighter_adapter=FakeLighter(),
    )
    stop = asyncio.Event()
    task = asyncio.create_task(runner._transport_loop(Venue.RISEX, stop))
    await websocket.entered.wait()
    item = await asyncio.wait_for(runner.ingress.next_item(), timeout=1)
    assert isinstance(item, FeedGapEvent)
    assert item.gap.reason == "PUBLIC_SOCKET_DISCONNECTED"
    stop.set()
    await asyncio.wait_for(task, timeout=1)


@pytest.mark.asyncio
async def test_spread_runner_bounded_shutdown_drains_all_queued_evidence(
    tmp_path: Path,
) -> None:
    store = AppendOnlyEvidenceStore.create(
        tmp_path,
        metadata={"source_commit": "fixture", "evidence_mode": "FIXTURE"},
    )
    observer = runner_module.SpreadObserver(config(), (PAIR,), store)
    gaps = tuple(
        FeedGapEvent(
            DataGapEvidence(
                source_venue=Venue.LIGHTER,
                canonical_market="BTC",
                stream_session_id="lighter",
                recovery_generation=0,
                gap_start_monotonic_ns=start,
                reason="QUEUED_TEST_GAP",
            )
        )
        for start in (100, 200, 300)
    )

    class Feed:
        ingress = observer.ingress

        async def run(self, **_kwargs):
            for item in gaps:
                assert self.ingress.offer(item)

    await runner_module.SpreadShadowRunner(Feed(), observer).run()

    store.close()
    records = [json.loads(line) for line in store.path.read_text().splitlines()]
    assert [record["reason"] for record in records if record.get("kind") == "DATA_GAP"] == [
        "QUEUED_TEST_GAP",
        "QUEUED_TEST_GAP",
        "QUEUED_TEST_GAP",
    ]
    assert observer.fatal_reason is None
    assert not observer.ingress.has_pending


@pytest.mark.asyncio
async def test_spread_runner_preserves_consumer_exception_and_closes_observer() -> None:
    ingress = IngressQueue(2)

    class FailingObserver:
        def __init__(self):
            self.ingress = ingress
            self.fatal_reason = None
            self.closed = False

        async def consume(self):
            raise LookupError("consumer failure")

        async def close(self):
            self.closed = True

    class Feed:
        def __init__(self):
            self.ingress = ingress

        async def run(self, **_kwargs):
            return None

    observer = FailingObserver()
    with pytest.raises(LookupError, match="consumer failure"):
        await runner_module.SpreadShadowRunner(Feed(), observer).run()
    assert observer.closed


@pytest.mark.asyncio
async def test_spread_runner_bounds_a_consumer_that_will_not_terminate(monkeypatch) -> None:
    monkeypatch.setattr(runner_module, "_SHUTDOWN_TIMEOUT_SECONDS", 0.01)
    ingress = IngressQueue(2)

    class StuckObserver:
        def __init__(self):
            self.ingress = ingress
            self.fatal_reason = None
            self.closed = False
            self.never = asyncio.Event()

        async def consume(self):
            await self.never.wait()

        async def close(self):
            self.closed = True

    class Feed:
        def __init__(self):
            self.ingress = ingress

        async def run(self, **_kwargs):
            return None

    observer = StuckObserver()
    with pytest.raises(asyncio.TimeoutError):
        await runner_module.SpreadShadowRunner(Feed(), observer).run()
    assert observer.fatal_reason == "INGRESS_DRAIN_TIMEOUT"
    assert observer.closed


@pytest.mark.asyncio
async def test_spread_runner_preserves_evidence_store_failure() -> None:
    class FailingStore:
        run_id = "fixture-run"

        def append_batch(self, _records):
            raise OSError("store failure")

    observer = runner_module.SpreadObserver(config(), (PAIR,), FailingStore())
    gap = FeedGapEvent(
        DataGapEvidence(
            source_venue=Venue.LIGHTER,
            canonical_market="BTC",
            stream_session_id="lighter",
            recovery_generation=0,
            gap_start_monotonic_ns=100,
            reason="STORE_FAILURE_TEST",
        )
    )

    class Feed:
        ingress = observer.ingress

        async def run(self, **_kwargs):
            assert self.ingress.offer(gap)

    with pytest.raises(OSError, match="store failure"):
        await runner_module.SpreadShadowRunner(Feed(), observer).run()
    assert observer.fatal_reason == "EVIDENCE_STORE_WRITE_FAILED"


@pytest.mark.asyncio
async def test_run_public_smoke_emits_one_clean_stop_or_one_failure_marker(
    tmp_path: Path,
    monkeypatch,
) -> None:
    async def select(_risex, _lighter, **_kwargs):
        return (PAIR,)

    monkeypatch.setattr(runner_module, "select_public_market_pairs", select)

    class NoOpFeed:
        def __init__(self, _session, _pairs, ingress, **_kwargs):
            self.ingress = ingress
            self.fatal_reason = None

    monkeypatch.setattr(runner_module, "PublicFeedRunner", NoOpFeed)

    async def clean(self, **_kwargs):
        return None

    monkeypatch.setattr(runner_module.SpreadShadowRunner, "run", clean)
    clean_result = await runner_module.run_public_smoke(
        str(tmp_path / "clean"), config=config(), source_commit="fixture", duration_seconds=1
    )
    clean_records = [
        json.loads(line) for line in Path(clean_result["store_path"]).read_text().splitlines()
    ]
    clean_terminal = [
        record["kind"]
        for record in clean_records
        if record.get("kind") in {"RUN_STOP", "RUN_FAILED"}
    ]
    assert clean_terminal == ["RUN_STOP"]
    assert clean_result["record_count"] == len(clean_records)
    assert clean_result["byte_count"] == Path(clean_result["store_path"]).stat().st_size

    async def failed(self, **_kwargs):
        raise LookupError("planned failure")

    monkeypatch.setattr(runner_module.SpreadShadowRunner, "run", failed)
    with pytest.raises(LookupError, match="planned failure"):
        await runner_module.run_public_smoke(
            str(tmp_path / "failed"), config=config(), source_commit="fixture", duration_seconds=1
        )
    failed_root = next((tmp_path / "failed").iterdir())
    failed_records = [json.loads(line) for line in (failed_root / "evidence.jsonl").read_text().splitlines()]
    failed_terminal = [
        record["kind"]
        for record in failed_records
        if record.get("kind") in {"RUN_STOP", "RUN_FAILED"}
    ]
    assert failed_terminal == ["RUN_FAILED"]
    assert failed_records[-1]["failure_class"] == "LookupError"

    async def fatal(self, **_kwargs):
        self.feed.fatal_reason = "TEST_FATAL"

    monkeypatch.setattr(runner_module.SpreadShadowRunner, "run", fatal)
    fatal_result = await runner_module.run_public_smoke(
        str(tmp_path / "fatal"), config=config(), source_commit="fixture", duration_seconds=1
    )
    fatal_records = [
        json.loads(line) for line in Path(fatal_result["store_path"]).read_text().splitlines()
    ]
    assert [
        record["kind"]
        for record in fatal_records
        if record.get("kind") in {"RUN_STOP", "RUN_FAILED"}
    ] == ["RUN_FAILED"]
    assert fatal_result["fatal_reason"] == "TEST_FATAL"


def test_report_uses_horizon_entry_edges_and_excludes_only_clean_terminal_stop_gap(
    tmp_path: Path,
) -> None:
    def write_records(root: Path, *, clean_stop: bool) -> Path:
        store = AppendOnlyEvidenceStore.create(
            root,
            metadata={"source_commit": "fixture", "evidence_mode": "OBSERVATIONAL"},
        )
        records = [
            {
                "kind": "QUOTE",
                "canonical_market": "BTC",
                "direction": "RISEX_BUY_LIGHTER_SELL",
                "target_notional_usd": "100",
                "target_margin_bps": "1",
                "policy_id": "policy-1",
                "quote_version_id": "version-1",
                "outcome": "QUOTE_ACTIVE",
                "actual_edge_usd": "999",
                "quote_created_monotonic_ns": 1_000_000_000,
                "quote_expires_monotonic_ns": 2_000_000_000,
                "quote_lifetime_ns": 1_000_000_000,
                "risex_tick_size": "1",
                "post_only_bound_price": "100",
                "maker_price": "99",
                "canonical_quantity": "1",
            },
            {"kind": "WOULD_FILL", "quote_version_id": "version-1"},
            {
                "kind": "HEDGE_HORIZON",
                "canonical_market": "BTC",
                "direction": "RISEX_BUY_LIGHTER_SELL",
                "target_notional_usd": "100",
                "target_margin_bps": "1",
                "policy_id": "policy-1",
                "quote_version_id": "version-1",
                "horizon_ms": 300,
                "outcome": "HEDGE_FULL",
                "entry_edge_usd": "2.00",
                "conditional_markout_usd": "-1.00",
            },
            {
                "kind": "DATA_GAP",
                "canonical_market": "BTC",
                "reason": "PUBLIC_SMOKE_STOPPED",
            },
            {
                "kind": "HEDGE_HORIZON",
                "canonical_market": "BTC",
                "direction": "RISEX_BUY_LIGHTER_SELL",
                "target_notional_usd": "100",
                "target_margin_bps": "1",
                "policy_id": "policy-1",
                "quote_version_id": "version-1",
                "horizon_ms": 500,
                "outcome": "HEDGE_PARTIAL",
                "entry_edge_usd": "123.00",
                "conditional_markout_usd": "0",
            },
        ]
        if clean_stop:
            records.append({"kind": "RUN_STOP", "fatal_reason": None})
        store.append_batch(records)
        path = store.path
        store.close()
        return path

    clean = build_report(write_records(tmp_path / "clean", clean_stop=True))
    clean_group = next(group for group in clean["groups"] if group["horizon_ms"] == 300)
    assert clean_group["data_completeness"] == "COMPLETE"
    assert clean_group["data_gap_count"] == 1
    assert clean_group["mean_entry_edge_usd"] == "2.00"
    assert clean_group["median_entry_edge_usd"] == "2.00"
    assert clean_group["p05_entry_edge_usd"] == "2.00"
    assert clean_group["positive_edge_share"] == "1"
    assert clean_group["mean_conditional_markout_usd"] == "-1.00"
    partial_group = next(group for group in clean["groups"] if group["horizon_ms"] == 500)
    assert partial_group["mean_entry_edge_usd"] is None
    assert partial_group["positive_edge_share"] is None
    assert partial_group["mean_conditional_markout_usd"] is None

    forced = build_report(write_records(tmp_path / "forced", clean_stop=False))
    forced_group = next(group for group in forced["groups"] if group["horizon_ms"] == 300)
    assert forced_group["data_completeness"] == "DEGRADED"


def test_report_completeness_is_episode_scoped_and_zero_fill_is_clean(
    tmp_path: Path,
) -> None:
    def quote(policy_id: str, version_id: str, *, created: int) -> dict:
        return {
            "kind": "QUOTE",
            "canonical_market": "BTC",
            "direction": "RISEX_BUY_LIGHTER_SELL",
            "target_notional_usd": "100",
            "target_margin_bps": "1",
            "policy_id": policy_id,
            "quote_version_id": version_id,
            "outcome": "QUOTE_ACTIVE",
            "quote_created_monotonic_ns": created,
            "quote_expires_monotonic_ns": created + 100,
            "quote_lifetime_ns": 100,
            "risex_tick_size": "1",
            "post_only_bound_price": "100",
            "maker_price": "99",
            "canonical_quantity": "1",
            "observed_monotonic_ns": created,
        }

    def fill(version_id: str, *, detected: int, session: str) -> dict:
        return {
            "kind": "WOULD_FILL",
            "canonical_market": "BTC",
            "venue": "RISEX",
            "quote_version_id": version_id,
            "would_fill_detected_monotonic_ns": detected,
            "hedge_stream_session_id": session,
            "hedge_recovery_generation": 0,
            "observed_monotonic_ns": detected,
        }

    def horizon(
        policy_id: str,
        version_id: str,
        *,
        detected: int,
        session: str,
        outcome: str = "HEDGE_FULL",
        edge: str | None = "1",
    ) -> dict:
        return {
            "kind": "HEDGE_HORIZON",
            "canonical_market": "BTC",
            "venue": "LIGHTER",
            "direction": "RISEX_BUY_LIGHTER_SELL",
            "target_notional_usd": "100",
            "target_margin_bps": "1",
            "policy_id": policy_id,
            "quote_version_id": version_id,
            "horizon_ms": 0,
            "would_fill_detected_monotonic_ns": detected,
            "horizon_deadline_monotonic_ns": detected,
            "expected_stream_session_id": session,
            "expected_recovery_generation": 0,
            "outcome": outcome,
            "entry_edge_usd": edge,
            "conditional_markout_usd": edge,
            "observed_monotonic_ns": detected,
        }

    store = AppendOnlyEvidenceStore.create(
        tmp_path,
        metadata={"source_commit": "fixture", "evidence_mode": "OBSERVATIONAL"},
    )
    store.append_batch(
        (
            quote("bad", "bad-v", created=100),
            quote("clean", "clean-v", created=100),
            quote("zero", "zero-v", created=1_000),
            quote("missing", "missing-v", created=2_000),
            quote("partial", "partial-v", created=3_000),
            fill("bad-v", detected=150, session="lighter-1"),
            fill("clean-v", detected=150, session="lighter-2"),
            fill("missing-v", detected=2_050, session="lighter-3"),
            fill("partial-v", detected=3_050, session="lighter-4"),
            horizon("bad", "bad-v", detected=150, session="lighter-1"),
            horizon("clean", "clean-v", detected=150, session="lighter-2"),
            horizon(
                "partial",
                "partial-v",
                detected=3_050,
                session="lighter-4",
                outcome="HEDGE_PARTIAL",
                edge="99",
            ),
            {
                "kind": "DATA_GAP",
                "canonical_market": "BTC",
                "venue": "LIGHTER",
                "stream_session_id": "lighter-1",
                "recovery_generation": 0,
                "gap_start_monotonic_ns": 149,
                "gap_end_monotonic_ns": 151,
                "reason": "QUEUE_OVERFLOW",
                "observed_monotonic_ns": 149,
            },
            {"kind": "RUN_STOP", "fatal_reason": None},
        )
    )
    path = store.path
    store.close()

    report = build_report(path)
    horizon_zero = [group for group in report["groups"] if group["horizon_ms"] == 0]
    bad = next(
        group
        for group in horizon_zero
        if group["data_completeness"] == "DEGRADED"
        and group["mean_entry_edge_usd"] == "1"
    )
    clean = next(
        group
        for group in horizon_zero
        if group["data_completeness"] == "COMPLETE"
        and group["mean_entry_edge_usd"] == "1"
    )
    zero = next(
        group
        for group in horizon_zero
        if group["strict_would_fill_count"] == 0
    )
    missing = next(
        group
        for group in horizon_zero
        if group["data_completeness"] == "DEGRADED"
        and group["strict_would_fill_count"] == 1
        and group["mean_entry_edge_usd"] is None
    )
    partial = next(
        group
        for group in horizon_zero
        if group["data_completeness"] == "COMPLETE"
        and group["partial_or_missing_rate"] == "1"
    )
    assert bad["data_gap_count"] == 1
    assert clean["data_completeness"] == "COMPLETE"
    assert zero["data_completeness"] == "COMPLETE"
    assert zero["strict_would_fill_count"] == 0
    assert zero["mean_entry_edge_usd"] is None
    assert missing["data_completeness"] == "DEGRADED"
    assert partial["mean_entry_edge_usd"] is None
    assert partial["positive_edge_share"] is None
