from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from risex_farmer.models import Venue
from risex_spread_shadow.causal import CausalEvent
from risex_spread_shadow.feed import (
    FeedBookEvent,
    FeedTradeEvent,
    MarketPair,
)
from risex_spread_shadow.models import BookEvidence, TradeEvidence
from risex_spread_shadow.s3_cycle import (
    CycleCampaignManifest,
    CycleEnvelope,
    CycleEnvelopeLimitError,
    CycleEvidenceIntegrityError,
    CycleEvidenceWriter,
    CycleKernelState,
    CycleScenario,
    CycleManifestError,
    CyclePublicPreconditionError,
    CycleRunDriver,
    CycleWindow,
    CycleWindowClaimError,
    PublicCycleProducer,
    build_cycle_report,
    cycle_policy_fingerprint,
    fixture_campaign_windows,
    freeze_cycle_manifest,
    run_public_cycle_collection,
    _fixture_market,
)
from risex_spread_shadow.store import (
    AppendOnlyEvidenceStore,
    TERMINAL_FAILURE_BYTES_RESERVE,
    TERMINAL_FAILURE_RECORD_RESERVE,
    iter_records,
    new_run_id,
)


ACCEPTED_RELEASE = "62cee8d1185b09904ed747fa1e775392e46e9520"
PAIR = MarketPair(
    "BTC",
    _fixture_market(Venue.RISEX, "BTC/USDC"),
    _fixture_market(Venue.LIGHTER, "BTC"),
)


def _metadata(window: CycleWindow, envelope: CycleEnvelope) -> dict[str, object]:
    return {
        "schema_version": 1,
        "experiment_kind": "S3_COMPLETE_CYCLE",
        "evidence_mode": "FIXTURE",
        "campaign_id": window.campaign_id,
        "window_id": window.window_id,
        "envelope": {
            name: getattr(envelope, name)
            for name in (
                "window_seconds",
                "entry_cutoff_seconds",
                "market_deadline_seconds",
                "closing_tail_seconds",
                "max_records",
                "record_reserve",
                "max_bytes",
                "bytes_reserve",
                "kernel_retention_capacity",
            )
        },
    }


def _feed_item(raw: object, pair: MarketPair):
    payload = raw.payload if isinstance(raw, CausalEvent) else raw
    if isinstance(payload, BookEvidence):
        return FeedBookEvent(payload, pair, "DELTA", "VALID")
    if isinstance(payload, TradeEvidence):
        return FeedTradeEvent(payload, pair)
    raise TypeError(type(payload))


def test_public_producer_preserves_order_and_captures_post_calculation_ready_time() -> None:
    from risex_spread_shadow.s3_cycle import fixture_cycle_attempts

    window = fixture_campaign_windows()[0]
    attempt = fixture_cycle_attempts(window, count=1, profile="normal")[0]
    normalized_books = tuple(
        replace(
            book,
            normalized_ready_monotonic_ns=550_000_000,
            decision_ready_monotonic_ns=550_000_000,
        )
        for book in attempt.source_books
    )
    driver = CycleRunDriver(window, persist=False, streaming=True)
    calculation_times = iter((600_000_000, 600_000_000))
    producer = PublicCycleProducer(
        driver,
        PAIR,
        monotonic_ns=lambda: next(calculation_times),
    )

    items = [
        FeedBookEvent(normalized_books[0], PAIR, "SNAPSHOT", "VALID"),
        FeedBookEvent(normalized_books[1], PAIR, "SNAPSHOT", "VALID"),
        _feed_item(attempt.events[0], PAIR),
    ]
    asyncio.run(producer.run_items(items))

    first_trade = attempt.events[0].payload if isinstance(attempt.events[0], CausalEvent) else attempt.events[0]
    assert producer.processed_items[:3] == [
        "BOOK:RISEX:1",
        "BOOK:LIGHTER:1",
        f"TRADE:{first_trade.trade_event_key}",
    ]
    assert driver.decision_count == 1
    version = driver._decisions[0].quote_version
    assert version.ingress_received_monotonic_ns == 400_000_000
    assert version.normalized_ready_monotonic_ns == 550_000_000
    assert version.decision_ready_monotonic_ns == 600_000_000
    assert all(
        driver.kernel.state(scenario) is not CycleKernelState.UNRESOLVED_HALTED
        for scenario in CycleScenario
    )


def test_campaign_budget_consumes_closing_reserve_and_blocks_next_run(tmp_path: Path) -> None:
    envelope = CycleEnvelope(
        max_records=8,
        record_reserve=2,
        max_bytes=200_000,
        bytes_reserve=1_000,
    )
    window = CycleWindow(
        campaign_id="aggregate-campaign",
        window_id="window-1",
        start_utc=datetime(2026, 1, 1, tzinfo=UTC),
        end_utc=datetime(2026, 1, 1, 0, 45, tzinfo=UTC),
    )
    metadata = _metadata(window, envelope)
    run_id = new_run_id()
    from risex_spread_shadow.s3_cycle import _preflight_campaign_store

    _preflight_campaign_store(
        tmp_path,
        campaign_id=window.campaign_id,
        envelope=envelope,
        metadata=metadata,
        run_id=run_id,
    )
    store = AppendOnlyEvidenceStore.create(
        tmp_path,
        metadata=metadata,
        run_id=run_id,
        max_records=envelope.max_records + TERMINAL_FAILURE_RECORD_RESERVE,
        max_bytes=envelope.max_bytes + TERMINAL_FAILURE_BYTES_RESERVE,
    )
    writer = CycleEvidenceWriter(store, envelope, window=window, campaign_root=tmp_path)
    for index in range(5):
        writer.append({"kind": "CYCLE_NOTE", "observed_monotonic_ns": index})
    writer.append({"kind": "CYCLE_CLOSING_NOTE", "observed_monotonic_ns": window.cutoff_monotonic_ns})
    writer.append_terminal()
    store.close()

    with pytest.raises(CycleEnvelopeLimitError, match="records"):
        from risex_spread_shadow.s3_cycle import _preflight_campaign_store

        _preflight_campaign_store(
            tmp_path,
            campaign_id=window.campaign_id,
            envelope=envelope,
            metadata=metadata,
            run_id=new_run_id(),
        )


@pytest.mark.asyncio
async def test_public_collection_uses_manifest_claim_stream_replay_and_cli_report(tmp_path: Path, monkeypatch, capsys) -> None:
    windows = fixture_campaign_windows(campaign_id="public-campaign")
    manifest = CycleCampaignManifest(
        campaign_id="public-campaign",
        accepted_release=ACCEPTED_RELEASE,
        policy_fingerprint=cycle_policy_fingerprint(ACCEPTED_RELEASE),
        windows=windows,
        envelope=CycleEnvelope(),
        created_utc=datetime(2026, 1, 1, tzinfo=UTC),
    )

    async def selector(*_args, **_kwargs):
        return (PAIR,)

    class EmptyFeed:
        fatal_reason = None

        async def run(self, **_kwargs):
            return None

    def feed_factory(*_args, **_kwargs):
        return EmptyFeed()

    monkeypatch.setattr(
        "risex_spread_shadow.s3_cycle.validate_loaded_release",
        lambda *_args, **_kwargs: tmp_path,
    )
    output = await run_public_cycle_collection(
        tmp_path,
        manifest=manifest,
        window_id=windows[0].window_id,
        now_utc=lambda: windows[0].start_utc,
        monotonic_ns=lambda: 10_000_000_000,
        market_selector=selector,
        feed_factory=feed_factory,
    )

    report = build_cycle_report(output.store_path)
    assert report["measurement_validity"] == "VALID"
    assert report["aggregate_caps"]["within_caps"] is True
    assert report["windows"][0]["record_count"] >= 4

    from risex_spread_shadow import cli as cli_module

    assert cli_module.main(["cycle-report", str(output.store_path), "--format", "table"]) == 0
    assert "campaign=public-campaign" in capsys.readouterr().out

    with pytest.raises(CycleWindowClaimError, match="already claimed"):
        await run_public_cycle_collection(
            tmp_path,
            manifest=manifest,
            window_id=windows[0].window_id,
            now_utc=lambda: windows[0].start_utc,
            monotonic_ns=lambda: 10_000_000_000,
            market_selector=selector,
            feed_factory=feed_factory,
        )


@pytest.mark.asyncio
async def test_public_collection_does_not_start_feed_after_slow_catalog_setup(tmp_path: Path, monkeypatch) -> None:
    windows = fixture_campaign_windows(campaign_id="deadline-campaign")
    manifest = CycleCampaignManifest(
        campaign_id="deadline-campaign",
        accepted_release=ACCEPTED_RELEASE,
        policy_fingerprint=cycle_policy_fingerprint(ACCEPTED_RELEASE),
        windows=windows,
        envelope=CycleEnvelope(),
        created_utc=datetime(2026, 1, 1, tzinfo=UTC),
    )
    current = [windows[0].start_utc]
    feed_factory_calls: list[bool] = []

    async def selector(*_args, **_kwargs):
        current[0] = windows[0].end_utc + timedelta(seconds=1)
        return (PAIR,)

    def feed_factory(*_args, **_kwargs):
        feed_factory_calls.append(True)
        raise AssertionError("feed startup must not happen after the hard deadline")

    monkeypatch.setattr(
        "risex_spread_shadow.s3_cycle.validate_loaded_release",
        lambda *_args, **_kwargs: tmp_path,
    )
    with pytest.raises(CyclePublicPreconditionError, match="before public feed startup"):
        await run_public_cycle_collection(
            tmp_path,
            manifest=manifest,
            window_id=windows[0].window_id,
            now_utc=lambda: current[0],
            monotonic_ns=lambda: 10_000_000_000,
            market_selector=selector,
            feed_factory=feed_factory,
        )
    assert feed_factory_calls == []


@pytest.mark.asyncio
async def test_public_collection_stops_feed_promptly_on_consumer_failure(tmp_path: Path, monkeypatch) -> None:
    from risex_spread_shadow.s3_cycle import fixture_cycle_attempts

    windows = fixture_campaign_windows(campaign_id="consumer-failure-campaign")
    manifest = CycleCampaignManifest(
        campaign_id="consumer-failure-campaign",
        accepted_release=ACCEPTED_RELEASE,
        policy_fingerprint=cycle_policy_fingerprint(ACCEPTED_RELEASE),
        windows=windows,
        envelope=CycleEnvelope(),
        created_utc=datetime(2026, 1, 1, tzinfo=UTC),
    )
    source_book = fixture_cycle_attempts(windows[0], count=1, profile="normal")[0].source_books[0]
    wrong_pair = MarketPair(
        "BTC",
        _fixture_market(Venue.RISEX, "BTC/WRONG"),
        _fixture_market(Venue.LIGHTER, "BTC"),
    )

    async def selector(*_args, **_kwargs):
        return (PAIR,)

    class FailingFeed:
        fatal_reason = None

        def __init__(self, ingress):
            self.ingress = ingress

        async def run(self, **kwargs):
            self.ingress.offer(FeedBookEvent(source_book, wrong_pair, "SNAPSHOT", "VALID"))
            await kwargs["stop_event"].wait()

    feeds: list[FailingFeed] = []

    def feed_factory(*args, **_kwargs):
        feed = FailingFeed(args[2])
        feeds.append(feed)
        return feed

    monkeypatch.setattr(
        "risex_spread_shadow.s3_cycle.validate_loaded_release",
        lambda *_args, **_kwargs: tmp_path,
    )
    started = asyncio.get_running_loop().time()
    with pytest.raises(CycleEvidenceIntegrityError):
        await run_public_cycle_collection(
            tmp_path,
            manifest=manifest,
            window_id=windows[0].window_id,
            now_utc=lambda: windows[0].start_utc,
            monotonic_ns=lambda: 10_000_000_000,
            market_selector=selector,
            feed_factory=feed_factory,
        )
    assert asyncio.get_running_loop().time() - started < 1
    assert len(feeds) == 1
    evidence = tuple(tmp_path.glob("run-*/evidence.jsonl"))
    assert len(evidence) == 1
    assert list(iter_records(evidence[0]))[-1]["kind"] == "RUN_FAILED"


def test_manifest_is_create_once(tmp_path: Path) -> None:
    windows = fixture_campaign_windows(campaign_id="manifest-campaign")
    path = freeze_cycle_manifest(
        tmp_path,
        accepted_release=ACCEPTED_RELEASE,
        windows=windows,
    )
    assert path.exists()
    with pytest.raises(CycleManifestError, match="already exists"):
        freeze_cycle_manifest(
            tmp_path,
            accepted_release=ACCEPTED_RELEASE,
            windows=windows,
        )
