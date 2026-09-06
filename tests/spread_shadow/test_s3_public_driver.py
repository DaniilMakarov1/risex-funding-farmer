from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

import pytest

from risex_farmer.models import Venue
from risex_spread_shadow.causal import CausalEvent, CausalSourceIdentity
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
    run_fixture_window,
    freeze_cycle_manifest,
    run_public_cycle_collection,
    _dependence_groups,
    _fixture_book,
    _scenario_report,
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


@pytest.mark.asyncio
async def test_public_collection_stops_fake_feed_on_resource_failure_and_marks_prefix_metrics(
    tmp_path: Path,
    monkeypatch,
) -> None:
    windows = fixture_campaign_windows(campaign_id="resource-feed-campaign")
    envelope = CycleEnvelope(
        max_records=20,
        record_reserve=3,
        max_bytes=1_000_000,
        bytes_reserve=100_000,
    )
    manifest = CycleCampaignManifest(
        campaign_id="resource-feed-campaign",
        accepted_release=ACCEPTED_RELEASE,
        policy_fingerprint=cycle_policy_fingerprint(ACCEPTED_RELEASE),
        windows=windows,
        envelope=envelope,
        created_utc=datetime(2026, 1, 1, tzinfo=UTC),
    )
    runtime_start_ns = 10_000_000_000

    class StageClock:
        """Follow source delivery and the active processing stage."""

        def __init__(self, start_ns: int) -> None:
            self._ingress = None
            self._last_processed_ns = start_ns

        def bind(self, ingress) -> None:
            self._ingress = ingress
            complete_item = ingress.complete_item

            def complete(*, success: bool = True) -> None:
                if ingress._in_flight is not None:
                    self._last_processed_ns = max(
                        self._last_processed_ns,
                        ingress._in_flight[1],
                    )
                complete_item(success=success)

            ingress.complete_item = complete

        def __call__(self) -> int:
            if self._ingress is not None and self._ingress._in_flight is not None:
                return self._ingress._in_flight[1]
            return self._last_processed_ns

    clock = StageClock(runtime_start_ns)
    source_ns = runtime_start_ns + 500_000_000
    items = [
        FeedBookEvent(
            _fixture_book(Venue.RISEX, source_ns, 1),
            PAIR,
            "SNAPSHOT",
            "VALID",
        ),
        FeedBookEvent(
            _fixture_book(
                Venue.LIGHTER,
                source_ns,
                1,
                bids=(("99", "10"),),
                asks=(("100", "10"),),
            ),
            PAIR,
            "SNAPSHOT",
            "VALID",
        ),
    ]
    items.extend(
        FeedBookEvent(
            _fixture_book(Venue.RISEX, source_ns + index * 1_000_000, index + 2),
            PAIR,
            "DELTA",
            "VALID",
        )
        for index in range(40)
    )

    async def selector(*_args, **_kwargs):
        return (PAIR,)

    class BurstFeed:
        fatal_reason = None

        def __init__(self, ingress):
            self.ingress = ingress
            self.offered = 0
            self.stop_seen = False

        async def run(self, **kwargs):
            stop_event = kwargs["stop_event"]
            try:
                for item in items:
                    if stop_event.is_set():
                        break
                    if self.ingress.offer(item):
                        self.offered += 1
                    await asyncio.sleep(0)
            finally:
                self.stop_seen = stop_event.is_set()

    feeds: list[BurstFeed] = []

    def feed_factory(*args, **kwargs):
        assert kwargs["monotonic_ns"] is clock
        clock.bind(args[2])
        feed = BurstFeed(args[2])
        feeds.append(feed)
        return feed

    monkeypatch.setattr(
        "risex_spread_shadow.s3_cycle.validate_loaded_release",
        lambda *_args, **_kwargs: tmp_path,
    )
    with pytest.raises(CycleEnvelopeLimitError, match="records"):
        await run_public_cycle_collection(
            tmp_path,
            manifest=manifest,
            window_id=windows[0].window_id,
            now_utc=lambda: windows[0].start_utc,
            monotonic_ns=clock,
            market_selector=selector,
            feed_factory=feed_factory,
        )

    assert len(feeds) == 1
    assert feeds[0].stop_seen is True
    assert 0 < feeds[0].offered < len(items)
    evidence = tuple(tmp_path.glob("run-*/evidence.jsonl"))
    assert len(evidence) == 1
    records = list(iter_records(evidence[0]))
    assert [record["record_index"] for record in records] == list(range(len(records)))
    assert records[-1]["kind"] == "RUN_FAILED"
    assert records[-1]["fatal_reason"] == "S3_ENVELOPE_LIMIT_RECORDS"
    # The consumer stops the feed during the stream, but the already-persisted
    # prefix still leaves the bounded stream-end record available; the
    # subsequent final-result suffix is the resource-limited phase.
    assert records[-1]["incomplete_evidence"] == "FINAL_RESULT_PREFIX"
    assert all(record["kind"] not in {"RUN_STOP", "RUN_FAILED"} for record in records[:-1])

    report = build_cycle_report(evidence[0])
    summary = report["windows"][0]
    assert report["measurement_validity"] == "DATA_INSUFFICIENT"
    assert report["data_quality"]["cycle_result_metrics_status"] == "UNAVAILABLE_RESOURCE_LIMIT"
    assert summary["cycle_result_metrics_complete"] is False
    assert summary["primary"]["turnover_usd"] is None
    assert "do not mean observed zero exposure" in summary["stress"]["cycle_result_metrics_note"]


@pytest.mark.asyncio
async def test_public_collection_preserves_first_resource_failure_across_advancing_clock(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """The public coordinator must retain the append-time failure observation."""

    windows = fixture_campaign_windows(campaign_id="advancing-resource-feed-campaign")
    envelope = CycleEnvelope(
        max_records=20,
        record_reserve=3,
        max_bytes=1_000_000,
        bytes_reserve=100_000,
    )
    manifest = CycleCampaignManifest(
        campaign_id="advancing-resource-feed-campaign",
        accepted_release=ACCEPTED_RELEASE,
        policy_fingerprint=cycle_policy_fingerprint(ACCEPTED_RELEASE),
        windows=windows,
        envelope=envelope,
        created_utc=datetime(2026, 1, 1, tzinfo=UTC),
    )
    runtime_start_ns = 10_000_000_000
    source_ns = runtime_start_ns + 500_000_000
    items = [
        FeedBookEvent(
            _fixture_book(Venue.RISEX, source_ns, 1),
            PAIR,
            "SNAPSHOT",
            "VALID",
        ),
        FeedBookEvent(
            _fixture_book(
                Venue.LIGHTER,
                source_ns,
                1,
                bids=(("99", "10"),),
                asks=(("100", "10"),),
            ),
            PAIR,
            "SNAPSHOT",
            "VALID",
        ),
    ]
    items.extend(
        FeedBookEvent(
            _fixture_book(Venue.RISEX, source_ns + index * 1_000_000, index + 2),
            PAIR,
            "DELTA",
            "VALID",
        )
        for index in range(40)
    )

    class AdvancingClock:
        """Advance only at meaningful processing and cleanup boundaries."""

        handling_advance_ns = 1_000_000
        cleanup_advance_ns = 1_000_000

        def __init__(self, start_ns: int) -> None:
            self._ingress = None
            self._last_processed_ns = start_ns
            self.failed_item_ns: int | None = None
            self.consumer_handling_ns: int | None = None
            self.cleanup_ns: int | None = None

        def bind(self, ingress) -> None:
            self._ingress = ingress
            complete_item = ingress.complete_item

            def complete(*, success: bool = True) -> None:
                current = ingress._in_flight
                if current is not None:
                    current_ns = current[1]
                    self._last_processed_ns = max(
                        self._last_processed_ns,
                        current_ns,
                    )
                    if not success and self.failed_item_ns is None:
                        self.failed_item_ns = current_ns
                        self.consumer_handling_ns = (
                            current_ns + self.handling_advance_ns
                        )
                        self._last_processed_ns = self.consumer_handling_ns
                complete_item(success=success)

            ingress.complete_item = complete

        def mark_cleanup(self) -> None:
            if self.consumer_handling_ns is None:
                raise AssertionError("cleanup must follow failed consumer handling")
            self.cleanup_ns = self.consumer_handling_ns + self.cleanup_advance_ns
            self._last_processed_ns = self.cleanup_ns

        def __call__(self) -> int:
            if self._ingress is not None and self._ingress._in_flight is not None:
                return self._ingress._in_flight[1]
            return self._last_processed_ns

    clock = AdvancingClock(runtime_start_ns)

    async def selector(*_args, **_kwargs):
        return (PAIR,)

    class BurstFeed:
        fatal_reason = None

        def __init__(self, ingress):
            self.ingress = ingress
            self.offered = 0
            self.stop_seen = False

        async def run(self, **kwargs):
            stop_event = kwargs["stop_event"]
            try:
                for item in items:
                    if stop_event.is_set():
                        break
                    if self.ingress.offer(item):
                        self.offered += 1
                    await asyncio.sleep(0)
            finally:
                self.stop_seen = stop_event.is_set()
                clock.mark_cleanup()

    feeds: list[BurstFeed] = []

    def feed_factory(*args, **kwargs):
        assert kwargs["monotonic_ns"] is clock
        clock.bind(args[2])
        feed = BurstFeed(args[2])
        feeds.append(feed)
        return feed

    monkeypatch.setattr(
        "risex_spread_shadow.s3_cycle.validate_loaded_release",
        lambda *_args, **_kwargs: tmp_path,
    )
    with pytest.raises(CycleEnvelopeLimitError, match="records"):
        await run_public_cycle_collection(
            tmp_path,
            manifest=manifest,
            window_id=windows[0].window_id,
            now_utc=lambda: windows[0].start_utc,
            monotonic_ns=clock,
            market_selector=selector,
            feed_factory=feed_factory,
        )

    assert len(feeds) == 1
    assert feeds[0].stop_seen is True
    assert 0 < feeds[0].offered < len(items)
    assert clock.failed_item_ns is not None
    assert clock.consumer_handling_ns == (
        clock.failed_item_ns + clock.handling_advance_ns
    )
    assert clock.cleanup_ns == clock.consumer_handling_ns + clock.cleanup_advance_ns

    evidence = tuple(tmp_path.glob("run-*/evidence.jsonl"))
    assert len(evidence) == 1
    records = list(iter_records(evidence[0]))
    assert [record["record_index"] for record in records] == list(range(len(records)))
    assert records[-1]["kind"] == "RUN_FAILED"
    assert records[-1]["fatal_reason"] == "S3_ENVELOPE_LIMIT_RECORDS"
    assert records[-1]["failure_observed_monotonic_ns"] == clock.failed_item_ns
    assert records[-1]["observation_boundary_monotonic_ns"] == clock.failed_item_ns
    assert records[-1]["finalization_boundary_kind"] == "EARLY_FAILURE"
    assert records[-1]["incomplete_evidence"] == "FINAL_RESULT_PREFIX"
    assert all(record["kind"] not in {"RUN_STOP", "RUN_FAILED"} for record in records[:-1])

    report = build_cycle_report(evidence[0])
    assert report["measurement_validity"] == "DATA_INSUFFICIENT"
    assert report["data_quality"]["cycle_result_metrics_status"] == "UNAVAILABLE_RESOURCE_LIMIT"


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


def test_incomplete_fixture_campaign_is_not_a_sufficient_observed_campaign(tmp_path: Path) -> None:
    windows = fixture_campaign_windows(campaign_id="incomplete-campaign")
    for window in windows[:3]:
        run_fixture_window(
            tmp_path,
            accepted_release=ACCEPTED_RELEASE,
            window=window,
            fixture_profile="normal",
            count=8,
            claim=False,
        )

    report = build_cycle_report(tmp_path)
    assert report["window_count"] == 3
    assert report["measurement_validity"] == "VALID"
    assert report["evidence_sufficiency"] == "INSUFFICIENT"
    assert report["campaign_complete"] is False
    assert report["provenance"] == {
        "evidence_mode": "FIXTURE",
        "fixture_only": True,
        "observed_public": False,
        "prospective_public": False,
        "manifest_sha256": None,
        "campaign_eligible": False,
    }
    assert report["usefulness"]["label"] == "FIXTURE_ONLY"
    assert report["usefulness"]["campaign_qualification"] == "FIXTURE_ONLY"


def test_campaign_report_rejects_extra_and_duplicate_windows(tmp_path: Path) -> None:
    windows = fixture_campaign_windows(campaign_id="window-shape-campaign")
    for window in (*windows, windows[0]):
        run_fixture_window(
            tmp_path,
            accepted_release=ACCEPTED_RELEASE,
            window=window,
            count=1,
            claim=False,
        )

    with pytest.raises(CycleEvidenceIntegrityError, match="more than four windows"):
        build_cycle_report(tmp_path)

    duplicate_root = tmp_path / "duplicate"
    for window in (windows[0], windows[0], windows[1], windows[2]):
        run_fixture_window(
            duplicate_root,
            accepted_release=ACCEPTED_RELEASE,
            window=window,
            count=1,
            claim=False,
        )

    with pytest.raises(CycleEvidenceIntegrityError, match="duplicate window"):
        build_cycle_report(duplicate_root)


def test_report_rejects_mixed_fixture_and_observed_provenance(tmp_path: Path) -> None:
    windows = fixture_campaign_windows(campaign_id="provenance-campaign")
    first = run_fixture_window(
        tmp_path,
        accepted_release=ACCEPTED_RELEASE,
        window=windows[0],
        count=1,
        claim=False,
    )
    run_fixture_window(
        tmp_path,
        accepted_release=ACCEPTED_RELEASE,
        window=windows[1],
        count=1,
        claim=False,
    )
    records = list(iter_records(first.store_path))
    records[0]["metadata"]["evidence_mode"] = "OBSERVATIONAL"
    records[0]["metadata"]["manifest_sha256"] = "0" * 64
    records[0]["metadata"]["manifest_window_fingerprint"] = records[0]["metadata"]["window_fingerprint"]
    records[0]["metadata"]["prospective"] = True
    first.store_path.write_text(
        "".join(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n" for record in records),
        encoding="utf-8",
    )

    with pytest.raises(CycleEvidenceIntegrityError, match="mixes fixture"):
        build_cycle_report(tmp_path)


@pytest.mark.asyncio
async def test_stream_persistence_replays_decisions_in_physical_order(tmp_path: Path, monkeypatch) -> None:
    from risex_spread_shadow.s3_cycle import fixture_cycle_attempts

    window = fixture_campaign_windows(campaign_id="stream-replay-campaign")[0]
    manifest = CycleCampaignManifest(
        campaign_id=window.campaign_id,
        accepted_release=ACCEPTED_RELEASE,
        policy_fingerprint=cycle_policy_fingerprint(ACCEPTED_RELEASE),
        windows=fixture_campaign_windows(campaign_id=window.campaign_id),
        envelope=CycleEnvelope(),
        created_utc=datetime(2026, 1, 1, tzinfo=UTC),
    )
    attempt = fixture_cycle_attempts(window, count=1, profile="normal")[0]
    source_books = tuple(
        replace(
            book,
            received_monotonic_ns=10_400_000_000,
            ingress_received_monotonic_ns=10_400_000_000,
            normalized_ready_monotonic_ns=10_400_000_000,
            decision_ready_monotonic_ns=10_400_000_000,
        )
        for book in attempt.source_books
    )

    async def selector(*_args, **_kwargs):
        return (PAIR,)

    class BookOnlyFeed:
        fatal_reason = None

        def __init__(self, ingress):
            self.ingress = ingress

        async def run(self, **_kwargs):
            for book in source_books:
                self.ingress.offer(FeedBookEvent(book, PAIR, "SNAPSHOT", "VALID"))

    def feed_factory(*args, **_kwargs):
        return BookOnlyFeed(args[2])

    monkeypatch.setattr(
        "risex_spread_shadow.s3_cycle.validate_loaded_release",
        lambda *_args, **_kwargs: tmp_path,
    )
    monotonic_values = iter((10_000_000_000, 10_500_000_000, 10_600_000_000))
    output = await run_public_cycle_collection(
        tmp_path,
        manifest=manifest,
        window_id=window.window_id,
        now_utc=lambda: window.start_utc,
        monotonic_ns=lambda: next(monotonic_values),
        market_selector=selector,
        feed_factory=feed_factory,
    )

    records = list(iter_records(output.store_path))
    kinds = [record["kind"] for record in records]
    assert kinds.index("CYCLE_DECISION") > kinds.index("CYCLE_STREAM_INPUT")
    metadata = records[0]["metadata"]
    assert records[-1]["observed_monotonic_ns"] == metadata["monotonic_start_ns"] + 45 * 60 * 1_000_000_000
    report = build_cycle_report(output.store_path)
    assert report["measurement_validity"] == "VALID"
    assert report["provenance"]["observed_public"] is True
    assert report["economics"]["primary"]["aborted_count"] == 1


def test_report_rejects_corrupt_policy_release_and_window_metadata(tmp_path: Path) -> None:
    mutations = (
        ("policy", lambda metadata: metadata["policy"].update(target_margin_bps="2"), "policy metadata"),
        ("release", lambda metadata: metadata.update(accepted_release="0" * 40), "fingerprint"),
        ("window", lambda metadata: metadata.update(window_id="different-window"), "fingerprint"),
        ("provenance", lambda metadata: metadata.pop("evidence_mode"), "missing evidence_mode"),
    )
    for name, mutate, message in mutations:
        root = tmp_path / name
        window = fixture_campaign_windows(campaign_id=f"corrupt-{name}")[0]
        output = run_fixture_window(
            root,
            accepted_release=ACCEPTED_RELEASE,
            window=window,
            count=1,
            claim=False,
        )
        records = list(iter_records(output.store_path))
        mutate(records[0]["metadata"])
        output.store_path.write_text(
            "".join(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n" for record in records),
            encoding="utf-8",
        )
        with pytest.raises(CycleEvidenceIntegrityError, match=message):
            build_cycle_report(output.store_path)


def test_transitive_overlapping_entry_identities_form_one_dependence_group() -> None:
    from risex_spread_shadow.s3_cycle import fixture_cycle_attempts

    window = fixture_campaign_windows(campaign_id="transitive-groups")[0]
    driver = CycleRunDriver(window, persist=False)
    results = driver.run(fixture_cycle_attempts(window, count=3, profile="normal"))
    primary = [result for result in results if result.scenario is CycleScenario.PRIMARY]
    mutated = []
    for result, (left, right) in zip(primary, (("A", "B"), ("B", "C"), ("C", "D"))):
        assert result.fills
        assert all(isinstance(fill.source_identity, CausalSourceIdentity) for fill in result.fills)
        fills = tuple(
            replace(
                fill,
                source_identity=replace(
                    fill.source_identity,
                    maker_order_id=left,
                    taker_order_id=right,
                ),
            )
            for fill in result.fills
        )
        measurement = result.entry_measurement
        assert measurement is not None
        causal_fills = tuple(
            replace(
                fill,
                source_identity=replace(
                    fill.source_identity,
                    maker_order_id=left,
                    taker_order_id=right,
                ),
            )
            for fill in measurement.fills
        )
        mutated.append(
            replace(
                result,
                entry_measurement=replace(measurement, fills=causal_fills),
                ledger=replace(result.ledger, fills=fills),
            )
        )

    groups, unresolved = _dependence_groups(mutated)
    report = _scenario_report(mutated, CycleScenario.PRIMARY)
    assert unresolved == 0
    assert len(set(groups.values())) == 1
    assert report["filled_entry_dependence_group_count"] == 1
    assert report["total_without_best_dependence_group_usd"] == "0.000000"


def test_terminal_marker_stays_inside_s3_record_cap(tmp_path: Path) -> None:
    envelope = CycleEnvelope(
        max_records=5,
        record_reserve=1,
        max_bytes=200_000,
        bytes_reserve=1_000,
    )
    window = CycleWindow(
        campaign_id="terminal-cap-campaign",
        window_id="window-1",
        start_utc=datetime(2026, 1, 1, tzinfo=UTC),
        end_utc=datetime(2026, 1, 1, 0, 45, tzinfo=UTC),
    )
    from risex_spread_shadow.s3_cycle import _preflight_campaign_store

    metadata = _metadata(window, envelope)
    run_id = new_run_id()
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
    writer.append({"kind": "CYCLE_NOTE", "observed_monotonic_ns": 1})
    writer.append({"kind": "CYCLE_NOTE", "observed_monotonic_ns": 2})
    writer.append({"kind": "CYCLE_CLOSING_NOTE", "observed_monotonic_ns": window.cutoff_monotonic_ns})
    writer.append_terminal()
    assert store.record_count == envelope.max_records
    with pytest.raises(CycleEvidenceIntegrityError, match="more than one terminal"):
        writer.append_terminal()
    store.close()


def test_cutoff_deadline_and_configured_closing_tail_are_explicit() -> None:
    from risex_spread_shadow.s3_cycle import fixture_cycle_attempts

    window = fixture_campaign_windows(campaign_id="timing-envelope")[0]
    envelope = CycleEnvelope()
    assert envelope.worst_configured_tail_seconds == 131
    assert envelope.closing_tail_seconds == 135
    envelope.assert_tail_sufficient()

    late = fixture_cycle_attempts(window, count=1, profile="normal")[0]
    late_version = replace(
        late.quote_version,
        decision_ready_monotonic_ns=window.cutoff_monotonic_ns + 1,
    )
    driver = CycleRunDriver(window, persist=False)
    admissions = driver.admit_decision(
        0,
        late_version,
        source_books=late.source_books,
    )
    assert all(not admission.accepted for admission in admissions)
    assert {admission.reason for admission in admissions} == {"ENTRY_CUTOFF"}
    assert driver.decision_finished(0)

    streaming = CycleRunDriver(window, persist=False, streaming=True)
    with pytest.raises(CycleEvidenceIntegrityError, match="hard market deadline"):
        streaming.accept_global_input(
            __import__("risex_spread_shadow.cycle", fromlist=["CycleClock"]).CycleClock(
                window.deadline_monotonic_ns + 1
            )
        )


def test_retention_exhaustion_is_an_explicit_lane_halt() -> None:
    from risex_spread_shadow.s3_cycle import fixture_cycle_attempts

    window = fixture_campaign_windows(campaign_id="retention-envelope")[0]
    driver = CycleRunDriver(
        window,
        envelope=CycleEnvelope(kernel_retention_capacity=1),
        persist=False,
    )
    output = driver.run(fixture_cycle_attempts(window, count=2, profile="normal"))
    assert len(output) == 2
    assert all(driver.lanes_halted() for _ in (0,))
    assert [admission.reason for admission in driver.admissions[-2:]] == [
        "TERMINAL_RETENTION_EXHAUSTED",
        "TERMINAL_RETENTION_EXHAUSTED",
    ]
