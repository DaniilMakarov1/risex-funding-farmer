from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
import tracemalloc

import pytest

from risex_farmer.models import Venue
from risex_spread_shadow.cycle import CycleScenario, CycleTerminalState
from risex_spread_shadow.feed import FeedBookEvent, IngressQueue, MarketPair
from risex_spread_shadow.models import BookEvidence
from risex_spread_shadow.s3_cycle import (
    CycleCampaignManifest,
    CycleEnvelope,
    CycleEnvelopeLimitError,
    CycleRunDriver,
    CycleWindow,
    build_cycle_report,
    cycle_policy_fingerprint,
    fixture_campaign_windows,
    fixture_cycle_attempts,
    run_public_cycle_collection,
    _fixture_book,
    _fixture_market,
)
from risex_spread_shadow.store import iter_records


ACCEPTED_RELEASE = "a" * 40
PAIR = MarketPair(
    "BTC",
    _fixture_market(Venue.RISEX, "BTC/USDC"),
    _fixture_market(Venue.LIGHTER, "BTC"),
)
RUNTIME_START_NS = 10_000_000_000


def _manifest(
    campaign_id: str,
    *,
    envelope: CycleEnvelope | None = None,
) -> tuple[CycleCampaignManifest, tuple[CycleWindow, ...]]:
    windows = fixture_campaign_windows(campaign_id=campaign_id)
    selected = CycleEnvelope() if envelope is None else envelope
    return (
        CycleCampaignManifest(
            campaign_id=campaign_id,
            accepted_release=ACCEPTED_RELEASE,
            policy_fingerprint=cycle_policy_fingerprint(ACCEPTED_RELEASE),
            windows=windows,
            envelope=selected,
            created_utc=datetime(2026, 1, 1, tzinfo=UTC),
        ),
        windows,
    )


def _load_items(*, count: int, start_ns: int = RUNTIME_START_NS) -> tuple[FeedBookEvent, ...]:
    """Return a large, ordered two-venue book burst with no network input."""

    items: list[FeedBookEvent] = [
        FeedBookEvent(
            _fixture_book(Venue.RISEX, start_ns + 1_000_000_000, 1),
            PAIR,
            "SNAPSHOT",
            "VALID",
        ),
        FeedBookEvent(
            _fixture_book(
                Venue.LIGHTER,
                start_ns + 1_000_000_000,
                1,
                bids=(("99", "10"),),
                asks=(("100", "10"),),
            ),
            PAIR,
            "SNAPSHOT",
            "VALID",
        ),
    ]
    for index in range(2, count):
        received = start_ns + 1_000_000_000 + index * 1_000_000
        venue = Venue.RISEX if index % 2 == 0 else Venue.LIGHTER
        revision = index // 2 + 1
        book = _fixture_book(
            venue,
            received,
            revision,
            bids=(("99", "10"),),
            asks=(("100", "10"),) if venue is Venue.LIGHTER else (("101", "10"),),
        )
        items.append(FeedBookEvent(book, PAIR, "DELTA", "VALID"))
    return tuple(items)


class _ConcurrentBurstFeed:
    """A fake source with concurrent producers and measurable queue pressure."""

    fatal_reason = None

    def __init__(
        self,
        ingress: IngressQueue,
        items: tuple[FeedBookEvent, ...],
        *,
        workers: int = 4,
        yield_every: int = 256,
    ) -> None:
        self.ingress = ingress
        self.items = items
        self.workers = workers
        self.yield_every = yield_every
        self.offered = 0
        self.accepted = 0
        self.dropped = 0
        self.max_qsize = 0
        self.stop_seen = False
        self.cancel_seen = False
        self.source_exhausted = False
        self._cursor = 0
        self._cursor_lock = asyncio.Lock()

    async def _worker(self, stop_event: asyncio.Event) -> None:
        while True:
            async with self._cursor_lock:
                if self._cursor >= len(self.items) or stop_event.is_set():
                    return
                item = self.items[self._cursor]
                self._cursor += 1
                accepted = self.ingress.offer(item)
                self.offered += 1
                self.accepted += int(accepted)
                self.dropped += int(not accepted)
                self.max_qsize = max(self.max_qsize, self.ingress.qsize)
                cursor = self._cursor
            if cursor % self.yield_every == 0:
                await asyncio.sleep(0)

    async def run(self, **kwargs) -> None:
        stop_event = kwargs["stop_event"]
        tasks = tuple(
            asyncio.create_task(self._worker(stop_event))
            for _ in range(self.workers)
        )
        try:
            await asyncio.gather(*tasks)
            self.source_exhausted = self._cursor == len(self.items)
        except asyncio.CancelledError:
            self.cancel_seen = True
            raise
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.stop_seen = stop_event.is_set()


class _FiniteFeed:
    fatal_reason = None

    def __init__(self, ingress: IngressQueue, items: tuple[FeedBookEvent, ...]) -> None:
        self.ingress = ingress
        self.items = items
        self.offered = 0

    async def run(self, **_kwargs) -> None:
        for item in self.items:
            self.offered += 1
            self.ingress.offer(item)


def _collection_kwargs(manifest: CycleCampaignManifest, window: CycleWindow) -> dict[str, object]:
    return {
        "manifest": manifest,
        "window_id": window.window_id,
        "now_utc": lambda: window.start_utc,
        "monotonic_ns": lambda: RUNTIME_START_NS,
    }


@pytest.mark.asyncio
async def test_concurrent_burst_load_drains_losslessly_and_replays(
    tmp_path: Path,
    monkeypatch,
) -> None:
    manifest, windows = _manifest("concurrent-load-campaign")
    items = _load_items(count=2048)
    feeds: list[_ConcurrentBurstFeed] = []

    async def selector(*_args, **_kwargs):
        return (PAIR,)

    def feed_factory(*args, **_kwargs):
        feed = _ConcurrentBurstFeed(args[2], items)
        feeds.append(feed)
        return feed

    monkeypatch.setattr(
        "risex_spread_shadow.s3_cycle.validate_loaded_release",
        lambda *_args, **_kwargs: tmp_path,
    )
    tracemalloc.start()
    try:
        output = await run_public_cycle_collection(
            tmp_path,
            market_selector=selector,
            feed_factory=feed_factory,
            **_collection_kwargs(manifest, windows[0]),
        )
    finally:
        _current_memory, peak_memory = tracemalloc.get_traced_memory()
        tracemalloc.stop()
    assert peak_memory < 128 * 1024 * 1024

    assert len(feeds) == 1
    feed = feeds[0]
    assert feed.source_exhausted is True
    assert feed.offered == len(items)
    assert feed.accepted == len(items)
    assert feed.dropped == 0
    assert feed.max_qsize >= 512
    assert feed.ingress.qsize == 0
    assert feed.ingress.has_pending is False
    assert feed.ingress.offer_serial == len(items)

    records = list(iter_records(output.store_path))
    assert records[-1]["kind"] == "RUN_STOP"
    assert sum(record["kind"] in {"RUN_STOP", "RUN_FAILED"} for record in records) == 1
    stream_inputs = [record for record in records if record["kind"] == "CYCLE_STREAM_INPUT"]
    event_inputs = [
        record
        for record in stream_inputs
        if record["input"].get("kind") == "EVENT"
    ]
    gap_inputs = [
        record
        for record in event_inputs
        if record["input"].get("event", {}).get("kind") == "DATA_GAP"
    ]
    assert len(event_inputs) == feed.accepted
    assert gap_inputs == []
    assert [record["record_index"] for record in records] == list(range(len(records)))
    assert len(records) < manifest.envelope.max_records
    assert output.store_path.stat().st_size < manifest.envelope.max_bytes

    report = build_cycle_report(output.store_path)
    summary = report["windows"][0]
    assert report["measurement_validity"] == "VALID"
    assert summary["terminal"] == "RUN_STOP"
    assert summary["persisted_final_result_count"] == summary["replayed_final_result_count"]
    assert report["aggregate_caps"]["within_caps"] is True


@pytest.mark.asyncio
async def test_actual_burst_overflow_latches_gap_and_reports_insufficiency(
    tmp_path: Path,
    monkeypatch,
) -> None:
    manifest, windows = _manifest("actual-overflow-campaign")
    items = _load_items(count=4608)
    feeds: list[_ConcurrentBurstFeed] = []

    async def selector(*_args, **_kwargs):
        return (PAIR,)

    def feed_factory(*args, **_kwargs):
        # Do not yield until every item has been offered.  This deliberately
        # fills the actual 4096-entry collection queue before the consumer
        # gets a turn; the queue must preserve one explicit aggregate gap.
        feed = _ConcurrentBurstFeed(
            args[2],
            items,
            workers=4,
            yield_every=len(items) + 1,
        )
        feeds.append(feed)
        return feed

    monkeypatch.setattr(
        "risex_spread_shadow.s3_cycle.validate_loaded_release",
        lambda *_args, **_kwargs: tmp_path,
    )
    output = await run_public_cycle_collection(
        tmp_path,
        market_selector=selector,
        feed_factory=feed_factory,
        **_collection_kwargs(manifest, windows[0]),
    )

    feed = feeds[0]
    assert feed.source_exhausted is True
    assert feed.offered == len(items)
    assert feed.accepted == 4096
    assert feed.dropped == len(items) - feed.accepted
    assert feed.max_qsize == feed.ingress.capacity == 4096
    assert feed.ingress.qsize == 0
    assert feed.ingress.has_pending is False

    records = list(iter_records(output.store_path))
    stream_inputs = [record for record in records if record["kind"] == "CYCLE_STREAM_INPUT"]
    event_inputs = [
        record
        for record in stream_inputs
        if record["input"].get("kind") == "EVENT"
    ]
    gaps = [
        record
        for record in event_inputs
        if record["input"].get("event", {}).get("kind") == "DATA_GAP"
    ]
    # Overflow is latched once per stream identity, so the alternating
    # RISEx/Lighter burst produces two aggregate gaps covering all drops.
    assert len(gaps) == 2
    assert len(event_inputs) == feed.accepted + len(gaps)
    assert {
        gap["input"]["event"]["payload"]["source_venue"]
        for gap in gaps
    } == {Venue.RISEX.value, Venue.LIGHTER.value}
    assert all(
        gap["input"]["event"]["payload"]["reason"] == "QUEUE_OVERFLOW"
        for gap in gaps
    )
    assert records[-1]["kind"] == "RUN_STOP"

    report = build_cycle_report(output.store_path)
    assert report["measurement_validity"] == "VALID"
    assert report["evidence_sufficiency"] == "INSUFFICIENT"


@pytest.mark.asyncio
async def test_concurrent_burst_stops_source_on_record_resource_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    envelope = CycleEnvelope(
        max_records=20,
        record_reserve=3,
        max_bytes=1_000_000,
        bytes_reserve=100_000,
    )
    manifest, windows = _manifest("concurrent-record-failure", envelope=envelope)
    items = _load_items(count=512)
    feeds: list[_ConcurrentBurstFeed] = []

    async def selector(*_args, **_kwargs):
        return (PAIR,)

    def feed_factory(*args, **_kwargs):
        feed = _ConcurrentBurstFeed(args[2], items, yield_every=8)
        feeds.append(feed)
        return feed

    monkeypatch.setattr(
        "risex_spread_shadow.s3_cycle.validate_loaded_release",
        lambda *_args, **_kwargs: tmp_path,
    )
    with pytest.raises(CycleEnvelopeLimitError, match="records"):
        await run_public_cycle_collection(
            tmp_path,
            market_selector=selector,
            feed_factory=feed_factory,
            **_collection_kwargs(manifest, windows[0]),
        )

    feed = feeds[0]
    assert feed.offered < len(items)
    assert feed.dropped == 0
    assert feed.stop_seen is True
    assert feed.cancel_seen is True
    # The consumer fails closed at the resource boundary.  Items already in
    # the bounded ingress are intentionally not drained into an over-cap
    # evidence file; the terminal marker makes that prefix insufficiency
    # explicit.
    assert feed.ingress.qsize <= feed.ingress.capacity
    assert feed.ingress.has_pending is True
    evidence = tuple(tmp_path.glob("run-*/evidence.jsonl"))
    assert len(evidence) == 1
    records = list(iter_records(evidence[0]))
    assert records[-1]["kind"] == "RUN_FAILED"
    assert records[-1]["fatal_reason"] == "S3_ENVELOPE_LIMIT_RECORDS"
    assert records[-1]["incomplete_evidence"] == "FINAL_RESULT_PREFIX"
    assert sum(record["kind"] in {"RUN_STOP", "RUN_FAILED"} for record in records) == 1
    assert evidence[0].stat().st_size <= envelope.max_bytes
    report = build_cycle_report(evidence[0])
    assert report["measurement_validity"] == "DATA_INSUFFICIENT"
    assert report["data_quality"]["cycle_result_metrics_status"] == "UNAVAILABLE_RESOURCE_LIMIT"


@pytest.mark.asyncio
async def test_collection_byte_resource_failure_preserves_terminal_and_closing_prefix(
    tmp_path: Path,
    monkeypatch,
) -> None:
    envelope = CycleEnvelope(
        max_records=10_000,
        record_reserve=100,
        max_bytes=32_000,
        bytes_reserve=8_000,
    )
    manifest, windows = _manifest("concurrent-byte-failure", envelope=envelope)
    items = _load_items(count=512)
    feeds: list[_ConcurrentBurstFeed] = []

    async def selector(*_args, **_kwargs):
        return (PAIR,)

    def feed_factory(*args, **_kwargs):
        feed = _ConcurrentBurstFeed(args[2], items, yield_every=8)
        feeds.append(feed)
        return feed

    monkeypatch.setattr(
        "risex_spread_shadow.s3_cycle.validate_loaded_release",
        lambda *_args, **_kwargs: tmp_path,
    )
    with pytest.raises(CycleEnvelopeLimitError, match="bytes"):
        await run_public_cycle_collection(
            tmp_path,
            market_selector=selector,
            feed_factory=feed_factory,
            **_collection_kwargs(manifest, windows[0]),
        )

    feed = feeds[0]
    assert feed.offered < len(items)
    assert feed.stop_seen is True
    evidence = tuple(tmp_path.glob("run-*/evidence.jsonl"))
    records = list(iter_records(evidence[0]))
    assert records[-1]["kind"] == "RUN_FAILED"
    assert records[-1]["fatal_reason"] == "S3_ENVELOPE_LIMIT_BYTES"
    assert records[-1]["incomplete_evidence"] in {
        "STREAM_FINALIZATION_PREFIX",
        "FINAL_RESULT_PREFIX",
    }
    assert sum(record["kind"] in {"RUN_STOP", "RUN_FAILED"} for record in records) == 1
    assert evidence[0].stat().st_size <= envelope.max_bytes
    if records[-1]["incomplete_evidence"] == "FINAL_RESULT_PREFIX":
        assert any(record["kind"] == "CYCLE_STREAM_END" for record in records)
    report = build_cycle_report(evidence[0])
    assert report["measurement_validity"] == "DATA_INSUFFICIENT"
    assert report["data_quality"]["cycle_result_metrics_status"] == "UNAVAILABLE_RESOURCE_LIMIT"


@pytest.mark.asyncio
async def test_aggregate_record_budget_counts_closing_results_and_terminal(
    tmp_path: Path,
    monkeypatch,
) -> None:
    envelope = CycleEnvelope(
        max_records=30,
        record_reserve=2,
        max_bytes=2_000_000,
        bytes_reserve=100_000,
    )
    manifest, windows = _manifest("aggregate-record-budget", envelope=envelope)
    items = _load_items(count=2)
    feeds: list[_FiniteFeed] = []

    async def selector(*_args, **_kwargs):
        return (PAIR,)

    def feed_factory(*args, **_kwargs):
        feed = _FiniteFeed(args[2], items)
        feeds.append(feed)
        return feed

    monkeypatch.setattr(
        "risex_spread_shadow.s3_cycle.validate_loaded_release",
        lambda *_args, **_kwargs: tmp_path,
    )
    for window in windows[:2]:
        await run_public_cycle_collection(
            tmp_path,
            market_selector=selector,
            feed_factory=feed_factory,
            **_collection_kwargs(manifest, window),
        )

    evidence = tuple(sorted(tmp_path.glob("run-*/evidence.jsonl")))
    assert len(evidence) == 2
    all_records = [record for path in evidence for record in iter_records(path)]
    assert len(all_records) == 28
    assert all_records[-1]["kind"] == "RUN_STOP"
    assert all(
        sum(record["kind"] in {"RUN_STOP", "RUN_FAILED"} for record in iter_records(path)) == 1
        for path in evidence
    )
    assert all(
        any(record["kind"] == "CYCLE_FINAL_RESULT" for record in iter_records(path))
        for path in evidence
    )

    with pytest.raises(CycleEnvelopeLimitError, match="records"):
        await run_public_cycle_collection(
            tmp_path,
            market_selector=selector,
            feed_factory=feed_factory,
            **_collection_kwargs(manifest, windows[2]),
        )
    report = build_cycle_report(tmp_path)
    assert report["aggregate_caps"]["record_count"] == 28
    assert report["aggregate_caps"]["within_caps"] is True


@pytest.mark.asyncio
async def test_aggregate_byte_budget_counts_closing_results_and_terminal(
    tmp_path: Path,
    monkeypatch,
) -> None:
    envelope = CycleEnvelope(
        max_records=1_000,
        record_reserve=100,
        max_bytes=60_000,
        bytes_reserve=4_000,
    )
    manifest, windows = _manifest("aggregate-byte-budget", envelope=envelope)
    items = _load_items(count=2)
    feeds: list[_FiniteFeed] = []

    async def selector(*_args, **_kwargs):
        return (PAIR,)

    def feed_factory(*args, **_kwargs):
        feed = _FiniteFeed(args[2], items)
        feeds.append(feed)
        return feed

    monkeypatch.setattr(
        "risex_spread_shadow.s3_cycle.validate_loaded_release",
        lambda *_args, **_kwargs: tmp_path,
    )
    for window in windows[:2]:
        await run_public_cycle_collection(
            tmp_path,
            market_selector=selector,
            feed_factory=feed_factory,
            **_collection_kwargs(manifest, window),
        )

    with pytest.raises(CycleEnvelopeLimitError, match="bytes"):
        await run_public_cycle_collection(
            tmp_path,
            market_selector=selector,
            feed_factory=feed_factory,
            **_collection_kwargs(manifest, windows[2]),
        )

    evidence = tuple(sorted(tmp_path.glob("run-*/evidence.jsonl")))
    assert len(evidence) == 3
    run_records = [list(iter_records(path)) for path in evidence]
    by_window = {
        records[0]["metadata"]["window_id"]: records
        for records in run_records
    }
    assert by_window[windows[0].window_id][-1]["kind"] == "RUN_STOP"
    assert by_window[windows[1].window_id][-1]["kind"] == "RUN_STOP"
    failed = by_window[windows[2].window_id]
    assert failed[-1]["kind"] == "RUN_FAILED"
    assert failed[-1]["fatal_reason"] == "S3_ENVELOPE_LIMIT_BYTES"
    assert failed[-1]["incomplete_evidence"] in {
        "STREAM_FINALIZATION_PREFIX",
        "FINAL_RESULT_PREFIX",
    }
    assert sum(
        record["kind"] in {"RUN_STOP", "RUN_FAILED"}
        for records in run_records
        for record in records
    ) == 3
    assert all(
        any(record["kind"] == "CYCLE_FINAL_RESULT" for record in by_window[window.window_id])
        for window in windows[:2]
    )
    assert sum(path.stat().st_size for path in evidence) <= envelope.max_bytes
    report = build_cycle_report(tmp_path)
    assert report["aggregate_caps"]["within_caps"] is True
    assert report["data_quality"]["cycle_result_metrics_status"] == "PARTIAL_RESOURCE_LIMIT"


def _retention_book(
    template: BookEvidence,
    *,
    session: str,
    received: int,
    revision: int,
    serial: int,
) -> BookEvidence:
    return replace(
        template,
        stream_session_id=session,
        received_monotonic_ns=received,
        book_revision=revision,
        sequence=revision,
        checksum=revision,
        ingress_received_monotonic_ns=received,
        normalized_ready_monotonic_ns=received,
        decision_ready_monotonic_ns=received,
        block_number=100_000 + serial if template.venue is Venue.RISEX else None,
        log_index=serial if template.venue is Venue.RISEX else None,
        worker_timestamp=received if template.venue is Venue.RISEX else None,
    )


def _retention_version(
    template,
    *,
    index: int,
    decision: int,
) -> tuple[object, tuple[BookEvidence, ...]]:
    risex_session = f"retention-risex-{index}"
    lighter_session = f"retention-lighter-{index}"
    source_risex = _retention_book(
        template.source_books[0],
        session=risex_session,
        received=decision - 100,
        revision=1,
        serial=index * 10 + 1,
    )
    source_lighter = _retention_book(
        template.source_books[1],
        session=lighter_session,
        received=decision - 100,
        revision=1,
        serial=index * 10 + 1,
    )
    version = replace(
        template.quote_version,
        version_id=f"retention-cycle-{index}",
        quote_created_monotonic_ns=decision - 200,
        stream_session_id=risex_session,
        hedge_stream_session_id=lighter_session,
        risex_book_revision=1,
        lighter_book_revision=1,
        risex_book_revision_id=source_risex.book_revision_id,
        lighter_book_revision_id=source_lighter.book_revision_id,
        ingress_received_monotonic_ns=decision - 200,
        normalized_ready_monotonic_ns=decision - 150,
        decision_ready_monotonic_ns=decision,
    )
    return version, (source_risex, source_lighter)


def test_s3_retention_bound_supports_dense_complete_lanes_without_halt() -> None:
    window = CycleWindow(
        campaign_id="retention-load",
        window_id="window-1",
        start_utc=datetime(2026, 1, 1, tzinfo=UTC),
        end_utc=datetime(2026, 1, 1, 0, 45, tzinfo=UTC),
        monotonic_start_ns=0,
    )
    envelope = CycleEnvelope()
    assert envelope.required_kernel_retention_capacity == 2_566
    assert envelope.kernel_retention_capacity == envelope.required_kernel_retention_capacity
    template = fixture_cycle_attempts(window, count=1, profile="normal")[0]
    cycle_spacing_ns = 8_000_000_000
    terminal_tail_ns = 7_000_000_000
    cycles = (
        (window.cutoff_monotonic_ns - terminal_tail_ns - 100) // cycle_spacing_ns
    ) + 1
    assert cycles == 320
    assert cycles > 256

    driver = CycleRunDriver(window, envelope=envelope, persist=False)
    for index in range(cycles):
        decision = 200 + index * cycle_spacing_ns
        version, source_books = _retention_version(
            template,
            index=index,
            decision=decision,
        )
        admissions = driver.admit_decision(
            index,
            version,
            source_books=source_books,
        )
        assert all(admission.accepted for admission in admissions)
        # No trade is supplied.  Both lanes must still reach their explicit
        # zero-fill cancel boundaries and retain the aborted terminal identity.
        driver.finish_decision(index, end_monotonic_ns=decision + terminal_tail_ns)

    assert driver.lanes_halted() is False
    assert len(driver.kernel.retained_results(CycleScenario.PRIMARY)) == cycles
    assert len(driver.kernel.retained_results(CycleScenario.STRESS)) == cycles
    results = driver.finalize()
    assert len(results) == cycles * 2
    assert all(result.status is CycleTerminalState.ABORTED for result in results)
    assert all(result.is_flat for result in results)
