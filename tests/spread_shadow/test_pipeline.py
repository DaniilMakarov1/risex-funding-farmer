from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal as D
import json
from pathlib import Path

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
    MarketPair,
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
    assert len(horizons) == 4
    assert {record["outcome"] for record in horizons} == {"HEDGE_FULL"}
    by_horizon = {record["horizon_ms"]: record["book_received_monotonic_ns"] for record in horizons}
    assert by_horizon[0] == 100
    assert by_horizon[300] == 102
    assert any(record.get("kind") == "REPLAY_MODE" for record in records)
    report = build_report(store.path)
    assert report["evidence_mode"] == "FIXTURE"
    assert report["horizon_record_count"] == 4
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


def test_public_text_payload_parser_preserves_decimal_wire_values() -> None:
    from types import SimpleNamespace

    parsed = PublicFeedRunner._payload(SimpleNamespace(data='{"price":1.25}'))
    assert parsed is not None
    assert parsed["price"] == D("1.25")
    assert PublicFeedRunner._payload(SimpleNamespace(data="not-json")) is None
