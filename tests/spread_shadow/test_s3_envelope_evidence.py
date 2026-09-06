from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
import json
from pathlib import Path

import pytest

from risex_farmer.models import Side, Venue
from risex_spread_shadow.causal import CausalEvent
from risex_spread_shadow.cycle import CycleClock, CycleKernelState, CycleScenario, CycleTerminalState, s2_cycle_policy
from risex_spread_shadow.feed import FeedBookEvent, FeedTradeEvent, IngressQueue, MarketPair
from risex_spread_shadow.models import BookEvidence, TradeEvidence
from risex_spread_shadow.s3_cycle import (
    CycleEnvelope,
    CycleEnvelopeLimitError,
    CycleEvidenceIntegrityError,
    CycleEvidenceWriter,
    CycleRunDriver,
    CycleWindow,
    PublicCycleProducer,
    _fixture_book,
    _fixture_market,
    _fixture_trade,
    _preflight_campaign_store,
    _primitive,
    build_cycle_report,
    cycle_policy_fingerprint,
    cycle_window_fingerprint,
    fixture_cycle_attempts,
    fixture_campaign_windows,
)
from risex_spread_shadow.store import (
    AppendOnlyEvidenceStore,
    TERMINAL_FAILURE_BYTES_RESERVE,
    TERMINAL_FAILURE_RECORD_RESERVE,
    iter_records,
    new_run_id,
)


ACCEPTED_RELEASE = "a" * 40
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
        "accepted_release": ACCEPTED_RELEASE,
        "campaign_id": window.campaign_id,
        "window_id": window.window_id,
        "window_fingerprint": cycle_window_fingerprint(
            accepted_release=ACCEPTED_RELEASE,
            window=window,
        ),
        "policy_fingerprint": cycle_policy_fingerprint(ACCEPTED_RELEASE),
        "source_scope": (Venue.RISEX.value, Venue.LIGHTER.value),
        "policy": _primitive(s2_cycle_policy()),
        "envelope": _primitive(envelope),
        "tail_required_ns": envelope.worst_configured_tail_ns(),
        "tail_sufficient": True,
        "funding_status": "UNKNOWN",
        "created_utc": datetime(2026, 1, 1, tzinfo=UTC),
        **window.to_metadata(),
    }


def _open_stream(
    root: Path,
    window: CycleWindow,
    *,
    envelope: CycleEnvelope | None = None,
    queue_capacity: int = 64,
    monotonic_ns=lambda: 500_000_000,
) -> tuple[AppendOnlyEvidenceStore, CycleEvidenceWriter, CycleRunDriver, IngressQueue, PublicCycleProducer]:
    selected = CycleEnvelope() if envelope is None else envelope
    metadata = _metadata(window, selected)
    run_id = new_run_id()
    _preflight_campaign_store(
        root,
        campaign_id=window.campaign_id,
        envelope=selected,
        metadata=metadata,
        run_id=run_id,
    )
    store = AppendOnlyEvidenceStore.create(
        root,
        metadata=metadata,
        run_id=run_id,
        max_records=selected.max_records + TERMINAL_FAILURE_RECORD_RESERVE,
        max_bytes=selected.max_bytes + TERMINAL_FAILURE_BYTES_RESERVE,
    )
    writer = CycleEvidenceWriter(
        store,
        selected,
        window=window,
        campaign_root=root,
    )
    driver = CycleRunDriver(
        window,
        envelope=selected,
        writer=writer,
        streaming=True,
    )
    ingress = IngressQueue(queue_capacity, preserve_offer_order=True)
    producer = PublicCycleProducer(
        driver,
        PAIR,
        ingress=ingress,
        monotonic_ns=monotonic_ns,
    )
    writer.append({"kind": "RUN_START", "observed_monotonic_ns": window.monotonic_start_ns})
    return store, writer, driver, ingress, producer


def _feed_value(
    value: CausalEvent | BookEvidence | TradeEvidence,
    *,
    source_kind: str = "DELTA",
) -> FeedBookEvent | FeedTradeEvent:
    payload = value.payload if isinstance(value, CausalEvent) else value
    if isinstance(payload, BookEvidence):
        return FeedBookEvent(payload, PAIR, source_kind, "VALID")
    if isinstance(payload, TradeEvidence):
        return FeedTradeEvent(payload, PAIR)
    raise TypeError(type(payload))


async def _drain_offered(
    ingress: IngressQueue,
    producer: PublicCycleProducer,
    items: tuple[FeedBookEvent | FeedTradeEvent, ...],
) -> int:
    accepted = sum(ingress.offer(item) for item in items)
    ingress.close()
    await producer.consume()
    return accepted


def _near_cutoff_items(
    window: CycleWindow,
    *,
    include_forced_close: bool,
) -> tuple[FeedBookEvent | FeedTradeEvent, ...]:
    """Build two-venue evidence just before each due boundary.

    The trigger book at a due time is deliberately preceded by a paired
    fresh book.  This makes the observed transition depend on actual
    per-venue evidence order rather than assuming two feeds arrive together.
    """

    policy = s2_cycle_policy()
    cutoff = window.cutoff_monotonic_ns
    initial_ns = cutoff - 100_000_000
    trade_ns = cutoff + 1_100_000_000
    items: list[FeedBookEvent | FeedTradeEvent] = [
        _feed_value(_fixture_book(Venue.RISEX, initial_ns, 1), source_kind="SNAPSHOT"),
        _feed_value(
            _fixture_book(
                Venue.LIGHTER,
                initial_ns,
                1,
                bids=(("99", "10"),),
                asks=(("100", "10"),),
            ),
            source_kind="SNAPSHOT",
        ),
        FeedTradeEvent(
            _fixture_trade(
                "near-cutoff-entry",
                trade_ns,
                "1.00",
                "102",
                aggressor=Side.BUY,
            ),
            PAIR,
        ),
    ]

    revisions = iter((2, 3, 4, 5, 10, 11, 20, 21))

    def add_due_pair(due_ns: int, *, asks: tuple[tuple[str, str], ...] = (("105", "10"),)) -> None:
        revision = next(revisions)
        ready_ns = due_ns - 100_000_000
        items.extend(
            (
                _feed_value(
                    _fixture_book(
                        Venue.RISEX,
                        ready_ns,
                        revision,
                        asks=asks,
                    )
                ),
                _feed_value(
                    _fixture_book(
                        Venue.LIGHTER,
                        ready_ns,
                        revision,
                        bids=(("99", "10"),),
                        asks=(("100", "10"),),
                    )
                ),
                # A same-stream book at the due boundary advances the real
                # kernel clock after both paired witnesses are already fresh.
                _feed_value(
                    _fixture_book(
                        Venue.RISEX,
                        due_ns,
                        next(revisions),
                        asks=asks,
                    )
                ),
            )
        )

    add_due_pair(trade_ns + policy.primary_taker_delay_ns)
    add_due_pair(trade_ns + policy.stress_taker_delay_ns)
    if include_forced_close:
        primary_force_due = (
            trade_ns
            + policy.max_hold_ns
            + policy.primary_cancel_delay_ns
            + policy.primary_taker_delay_ns
        )
        stress_force_due = (
            trade_ns
            + policy.max_hold_ns
            + policy.stress_cancel_delay_ns
            + policy.stress_taker_delay_ns
        )
        add_due_pair(primary_force_due)
        add_due_pair(stress_force_due)
    return tuple(items)


def _latest_admissible_stress_tail_items(
    window: CycleWindow,
    *,
    include_forced_close: bool,
) -> tuple[FeedBookEvent | FeedTradeEvent, ...]:
    """Place the stress entry immediately before cancel, then close at the tail."""

    policy = s2_cycle_policy()
    cutoff = window.cutoff_monotonic_ns
    initial_ns = cutoff - 100_000_000
    entry_cancel_effective_ns = (
        cutoff
        + policy.stress_activation_delay_ns
        + policy.entry_cancel_after_activation_ns
        + policy.stress_cancel_delay_ns
    )
    fill_ns = entry_cancel_effective_ns - 1_000_000
    force_due_ns = (
        fill_ns
        + policy.max_hold_ns
        + policy.stress_cancel_delay_ns
        + policy.stress_taker_delay_ns
    )

    def pair(at_ns: int, revision: int) -> tuple[FeedBookEvent, FeedBookEvent]:
        return (
            _feed_value(
                _fixture_book(Venue.RISEX, at_ns, revision, asks=(("105", "10"),))
            ),
            _feed_value(
                _fixture_book(
                    Venue.LIGHTER,
                    at_ns,
                    revision,
                    bids=(("99", "10"),),
                    asks=(("100", "10"),),
                )
            ),
        )

    items: list[FeedBookEvent | FeedTradeEvent] = [
        _feed_value(_fixture_book(Venue.RISEX, initial_ns, 1), source_kind="SNAPSHOT"),
        _feed_value(
            _fixture_book(
                Venue.LIGHTER,
                initial_ns,
                1,
                bids=(("99", "10"),),
                asks=(("100", "10"),),
            ),
            source_kind="SNAPSHOT",
        ),
        *pair(fill_ns - policy.input_freshness_max_age_ns + 100_000_000, 2),
        FeedTradeEvent(
            _fixture_trade(
                "latest-admissible-stress-entry",
                fill_ns,
                "0.50",
                "102",
                aggressor=Side.BUY,
            ),
            PAIR,
        ),
        *pair(fill_ns + 900_000_000, 3),
        _feed_value(_fixture_book(Venue.RISEX, fill_ns + 1_000_000_000, 4)),
    ]
    if include_forced_close:
        items.extend(
            (
                *pair(force_due_ns - 200_000_000, 5),
                _feed_value(_fixture_book(Venue.RISEX, force_due_ns, 6)),
            )
        )
    return tuple(items)


def test_burst_queue_drains_losslessly_into_gap_evidence_and_replays(tmp_path: Path) -> None:
    window = fixture_campaign_windows(campaign_id="burst-envelope")[0]
    store, writer, driver, ingress, producer = _open_stream(
        tmp_path,
        window,
        queue_capacity=3,
    )
    attempt = __import__(
        "risex_spread_shadow.s3_cycle",
        fromlist=["fixture_cycle_attempts"],
    ).fixture_cycle_attempts(window, count=1, profile="normal")[0]
    items: list[FeedBookEvent | FeedTradeEvent] = [
        _feed_value(attempt.source_books[0], source_kind="SNAPSHOT"),
        _feed_value(attempt.source_books[1], source_kind="SNAPSHOT"),
        _feed_value(attempt.events[0]),
    ]
    for index in range(12):
        items.append(
            _feed_value(
                _fixture_book(
                    Venue.RISEX,
                    2_000_000_000 + index * 10_000_000,
                    10 + index,
                )
            )
        )

    accepted = asyncio.run(_drain_offered(ingress, producer, tuple(items)))
    assert accepted == 3
    assert ingress.offer_serial == len(items)
    assert ingress.qsize == 0
    assert not ingress.has_pending
    assert producer.processed_items[:4] == [
        "BOOK:RISEX:1",
        "BOOK:LIGHTER:1",
        f"TRADE:{attempt.events[0].payload.trade_event_key}",
        "GAP:QUEUE_OVERFLOW:2000000000",
    ]

    output = producer.finalize()
    store.close()
    records = list(iter_records(output.store_path))
    stream_inputs = [record for record in records if record["kind"] == "CYCLE_STREAM_INPUT"]
    assert [record["record_index"] for record in records] == list(range(len(records)))
    assert [record["input_index"] for record in stream_inputs] == list(range(len(stream_inputs)))
    gap_inputs = [
        record["input"]
        for record in stream_inputs
        if record["input"].get("kind") == "EVENT"
        and record["input"].get("event", {}).get("kind") == "DATA_GAP"
    ]
    assert len(gap_inputs) == 1
    assert gap_inputs[0]["event"]["payload"]["reason"] == "QUEUE_OVERFLOW"
    assert records[-1]["kind"] == "RUN_STOP"
    assert "CYCLE_END" not in {record["kind"] for record in records}
    assert all(result.status is CycleTerminalState.UNRESOLVED for result in output.results)

    report = build_cycle_report(output.store_path)
    assert report["measurement_validity"] == "VALID"
    assert report["evidence_sufficiency"] == "INSUFFICIENT"
    assert report["economics"]["primary"]["unresolved_count"] == 1

    before_late_offer = output.store_path.read_bytes()
    assert not ingress.offer(_feed_value(_fixture_book(Venue.RISEX, 2_500_000_000, 99)))
    assert ingress.has_pending
    assert output.store_path.read_bytes() == before_late_offer
    assert writer.terminal_written


@pytest.mark.parametrize("include_forced_close", [True, False])
def test_near_cutoff_max_hold_uses_fresh_closing_liquidity_or_stays_unresolved(
    tmp_path: Path,
    include_forced_close: bool,
) -> None:
    campaign = "near-cutoff-complete" if include_forced_close else "near-cutoff-missing-close"
    window = fixture_campaign_windows(campaign_id=campaign)[0]
    store, writer, driver, ingress, producer = _open_stream(tmp_path, window)
    items = _near_cutoff_items(window, include_forced_close=include_forced_close)
    assert asyncio.run(_drain_offered(ingress, producer, items)) == len(items)
    output = producer.finalize()
    store.close()

    policy = s2_cycle_policy()
    trade_ns = window.cutoff_monotonic_ns + 1_100_000_000
    expected = {
        CycleScenario.PRIMARY: trade_ns + policy.max_hold_ns + policy.primary_cancel_delay_ns + policy.primary_taker_delay_ns,
        CycleScenario.STRESS: trade_ns + policy.max_hold_ns + policy.stress_cancel_delay_ns + policy.stress_taker_delay_ns,
    }
    by_scenario = {result.scenario: result for result in output.results}
    assert set(by_scenario) == set(CycleScenario)
    for scenario, result in by_scenario.items():
        assert result.first_maker_fill_monotonic_ns == trade_ns
        assert result.max_hold_deadline_monotonic_ns == trade_ns + policy.max_hold_ns
        assert result.terminal_monotonic_ns is not None
        assert result.terminal_monotonic_ns <= window.deadline_monotonic_ns
        if include_forced_close:
            assert result.status is CycleTerminalState.FORCED
            assert result.is_flat
            assert result.terminal_monotonic_ns == expected[scenario]
            assert "MAX_HOLD" in result.reason_codes
            assert any(
                action.action_id == "exit-cancel"
                and action.effective_monotonic_ns == trade_ns + policy.max_hold_ns + policy.delays(scenario).cancel_delay_ns
                for action in result.actions
            )
            assert all(not action.status.value in {"PENDING", "UNRESOLVED"} for action in result.actions)
        else:
            assert result.status is CycleTerminalState.UNRESOLVED
            assert not result.is_flat
            assert "REQUIRED_ACTION_DATA_STALE" in result.reason_codes

    records = list(iter_records(output.store_path))
    assert records[-1]["kind"] == "RUN_STOP"
    assert records[-1]["observed_monotonic_ns"] == window.deadline_monotonic_ns
    assert all(
        record.get("observed_monotonic_ns", 0) <= window.deadline_monotonic_ns
        for record in records
        if record["kind"] == "CYCLE_STREAM_INPUT"
    )
    assert "CYCLE_END" not in {record["kind"] for record in records}
    report = build_cycle_report(output.store_path)
    assert report["measurement_validity"] == "VALID"
    assert report["economics"]["primary"]["unresolved_count"] == (0 if include_forced_close else 1)


@pytest.mark.parametrize("include_forced_close", [True, False])
def test_latest_admissible_stress_fill_respects_tail_deadline_or_stays_unresolved(
    tmp_path: Path,
    include_forced_close: bool,
) -> None:
    campaign = "latest-tail-complete" if include_forced_close else "latest-tail-missing-close"
    window = fixture_campaign_windows(campaign_id=campaign)[0]
    store, writer, driver, ingress, producer = _open_stream(
        tmp_path,
        window,
        monotonic_ns=lambda: window.cutoff_monotonic_ns,
    )
    items = _latest_admissible_stress_tail_items(
        window,
        include_forced_close=include_forced_close,
    )
    assert asyncio.run(_drain_offered(ingress, producer, items)) == len(items)
    output = producer.finalize()
    store.close()

    policy = s2_cycle_policy()
    entry_cancel_effective_ns = (
        window.cutoff_monotonic_ns
        + policy.stress_activation_delay_ns
        + policy.entry_cancel_after_activation_ns
        + policy.stress_cancel_delay_ns
    )
    fill_ns = entry_cancel_effective_ns - 1_000_000
    max_hold_ns = fill_ns + policy.max_hold_ns
    force_due_ns = max_hold_ns + policy.stress_cancel_delay_ns + policy.stress_taker_delay_ns
    stress = next(result for result in output.results if result.scenario is CycleScenario.STRESS)
    assert stress.first_maker_fill_monotonic_ns == fill_ns
    assert stress.max_hold_deadline_monotonic_ns == max_hold_ns
    if include_forced_close:
        assert stress.status is CycleTerminalState.FORCED
        assert stress.is_flat
        assert stress.terminal_monotonic_ns == force_due_ns
        assert stress.terminal_monotonic_ns <= window.deadline_monotonic_ns
        assert window.deadline_monotonic_ns - stress.terminal_monotonic_ns == 6_001_000_000
        assert {"MAX_HOLD", "FORCED_UNWIND"}.issubset(stress.reason_codes)
        assert all(action.status.value not in {"PENDING", "UNRESOLVED"} for action in stress.actions)
    else:
        assert stress.status is CycleTerminalState.UNRESOLVED
        assert not stress.is_flat
        assert "REQUIRED_ACTION_DATA_STALE" in stress.reason_codes

    records = list(iter_records(output.store_path))
    assert records[-1]["kind"] == "RUN_STOP"
    result_records = [
        record
        for record in records
        if record["kind"] == "CYCLE_FINAL_RESULT" and record["scenario"] == CycleScenario.STRESS.value
    ]
    assert len(result_records) == 1
    assert result_records[0]["observed_monotonic_ns"] == window.deadline_monotonic_ns
    assert result_records[0]["resource_phase"] == "CLOSING"


def test_stream_primary_reentry_survives_pending_stress_and_replays(tmp_path: Path) -> None:
    window = fixture_campaign_windows(campaign_id="lane-envelope")[0]
    store, writer, driver, ingress, producer = _open_stream(tmp_path, window)
    first = __import__(
        "risex_spread_shadow.s3_cycle",
        fromlist=["fixture_cycle_attempts"],
    ).fixture_cycle_attempts(window, count=1, profile="normal")[0]
    items: list[FeedBookEvent | FeedTradeEvent] = [
        _feed_value(first.source_books[0], source_kind="SNAPSHOT"),
        _feed_value(first.source_books[1], source_kind="SNAPSHOT"),
        *(_feed_value(value) for value in first.events),
    ]
    # At 6.3s the first primary is flat, while stress is still in its exit
    # wait.  The fresh RISEx book itself is enough to build a valid second
    # quote because the Lighter witness at 5.8s is exactly within the 500ms
    # input-age bound.
    second_book_time = 6_300_000_000
    items.extend(
        (
            _feed_value(_fixture_book(Venue.RISEX, second_book_time, 30)),
            _feed_value(
                _fixture_book(
                    Venue.LIGHTER,
                    second_book_time,
                    30,
                    bids=(("99", "10"),),
                    asks=(("100", "10"),),
                )
            ),
            FeedTradeEvent(
                _fixture_trade(
                    "second-entry",
                    7_000_000_000,
                    "1.00",
                    "102",
                    aggressor=Side.BUY,
                ),
                PAIR,
            ),
        )
    )

    assert asyncio.run(_drain_offered(ingress, producer, tuple(items))) == len(items)

    admissions = [
        admission
        for admission in driver.admissions
        if admission.quote_version_id.endswith("public-2")
    ]
    assert [(admission.scenario, admission.accepted, admission.reason) for admission in admissions] == [
        (CycleScenario.PRIMARY, True, "ACCEPTED"),
        (CycleScenario.STRESS, False, "ACTIVE_CYCLE"),
    ]
    assert driver._decisions[1].finished is False
    assert driver.kernel.state(CycleScenario.PRIMARY) is CycleKernelState.PENDING
    assert driver.kernel.state(CycleScenario.STRESS) is CycleKernelState.FLAT
    assert any(
        result.quote_version_id.endswith("public-1")
        and result.entry_measurement is not None
        and result.entry_measurement.filled_quantity == Decimal("1.00")
        and result.status.value == "NORMAL"
        for result in driver.kernel.retained_results(CycleScenario.PRIMARY, include_active=False)
    )

    output = producer.finalize()
    store.close()
    assert any(
        result.quote_version_id.endswith("public-2")
        and result.status is CycleTerminalState.UNRESOLVED
        and not result.is_flat
        for result in output.results
        if result.scenario is CycleScenario.PRIMARY
    )

    records = list(iter_records(output.store_path))
    assert "CYCLE_END" not in {record["kind"] for record in records}
    assert records[-1]["kind"] == "RUN_STOP"
    report = build_cycle_report(output.store_path)
    assert report["measurement_validity"] == "VALID"
    assert report["economics"]["primary"]["cycle_count"] >= 2


def test_aggregate_caps_consume_closing_reserve_and_keep_one_terminal(tmp_path: Path) -> None:
    record_envelope = CycleEnvelope(
        max_records=8,
        record_reserve=2,
        max_bytes=100_000,
        bytes_reserve=20_000,
    )
    first_window = CycleWindow(
        campaign_id="aggregate-envelope",
        window_id="window-1",
        start_utc=datetime(2026, 1, 1, tzinfo=UTC),
        end_utc=datetime(2026, 1, 1, 0, 45, tzinfo=UTC),
    )
    metadata = _metadata(first_window, record_envelope)
    run_id = new_run_id()
    _preflight_campaign_store(
        tmp_path,
        campaign_id=first_window.campaign_id,
        envelope=record_envelope,
        metadata=metadata,
        run_id=run_id,
    )
    store = AppendOnlyEvidenceStore.create(
        tmp_path,
        metadata=metadata,
        run_id=run_id,
        max_records=record_envelope.max_records + TERMINAL_FAILURE_RECORD_RESERVE,
        max_bytes=record_envelope.max_bytes + TERMINAL_FAILURE_BYTES_RESERVE,
    )
    writer = CycleEvidenceWriter(store, record_envelope, window=first_window, campaign_root=tmp_path)
    for index in range(5):
        writer.append({"kind": "CYCLE_NOTE", "observed_monotonic_ns": index})
    writer.append({"kind": "CYCLE_CLOSING_NOTE", "observed_monotonic_ns": first_window.cutoff_monotonic_ns})
    writer.append_terminal()
    store.close()
    records = list(iter_records(Path(store.path)))
    assert len(records) == record_envelope.max_records
    assert records[-1]["kind"] == "RUN_STOP"
    with pytest.raises(CycleEnvelopeLimitError, match="records"):
        _preflight_campaign_store(
            tmp_path,
            campaign_id=first_window.campaign_id,
            envelope=record_envelope,
            metadata=_metadata(
                CycleWindow(
                    campaign_id=first_window.campaign_id,
                    window_id="window-2",
                    start_utc=datetime(2026, 1, 2, tzinfo=UTC),
                    end_utc=datetime(2026, 1, 2, 0, 45, tzinfo=UTC),
                ),
                record_envelope,
            ),
            run_id=new_run_id(),
        )

    byte_root = tmp_path / "bytes"
    byte_envelope = CycleEnvelope(
        max_records=100,
        record_reserve=10,
        max_bytes=100_000,
        bytes_reserve=20_000,
    )
    byte_window = CycleWindow(
        campaign_id="aggregate-bytes",
        window_id="window-1",
        start_utc=datetime(2026, 1, 1, tzinfo=UTC),
        end_utc=datetime(2026, 1, 1, 0, 45, tzinfo=UTC),
    )
    byte_metadata = _metadata(byte_window, byte_envelope)
    byte_run_id = new_run_id()
    _preflight_campaign_store(
        byte_root,
        campaign_id=byte_window.campaign_id,
        envelope=byte_envelope,
        metadata=byte_metadata,
        run_id=byte_run_id,
    )
    byte_store = AppendOnlyEvidenceStore.create(
        byte_root,
        metadata=byte_metadata,
        run_id=byte_run_id,
        max_records=byte_envelope.max_records + TERMINAL_FAILURE_RECORD_RESERVE,
        max_bytes=byte_envelope.max_bytes + TERMINAL_FAILURE_BYTES_RESERVE,
    )
    byte_writer = CycleEvidenceWriter(byte_store, byte_envelope, window=byte_window, campaign_root=byte_root)
    normal_limit = byte_envelope.max_bytes - byte_envelope.bytes_reserve
    while byte_store.byte_count < normal_limit - 12_000:
        byte_writer.append({"kind": "CYCLE_NOTE", "body": "x" * 3_000, "observed_monotonic_ns": 1})
    before_closing = byte_store.byte_count
    assert before_closing <= normal_limit
    for length in range(10_000, 0, -100):
        try:
            byte_writer.append(
                {
                    "kind": "CYCLE_CLOSING_NOTE",
                    "body": "y" * length,
                    "observed_monotonic_ns": byte_window.cutoff_monotonic_ns,
                }
            )
        except CycleEnvelopeLimitError as exc:
            if exc.resource != "bytes":
                raise
            continue
        break
    else:
        raise AssertionError("closing reserve could not be consumed")
    assert byte_store.byte_count > normal_limit
    assert byte_store.byte_count <= byte_envelope.max_bytes - TERMINAL_FAILURE_BYTES_RESERVE
    byte_writer.append_terminal()
    assert byte_store.byte_count <= byte_envelope.max_bytes
    byte_store.close()
    with pytest.raises(CycleEnvelopeLimitError, match="bytes"):
        _preflight_campaign_store(
            byte_root,
            campaign_id=byte_window.campaign_id,
            envelope=byte_envelope,
            metadata=byte_metadata,
            run_id=new_run_id(),
        )


@pytest.mark.parametrize(
    ("max_records", "record_reserve", "incomplete_evidence"),
    (
        (10, 1, "STREAM_FINALIZATION_PREFIX"),
        (11, 1, "STREAM_FINALIZATION_PREFIX"),
        (36, 3, "FINAL_RESULT_PREFIX"),
    ),
)
def test_resource_limit_finalization_persists_explicit_failed_terminal_and_replays_prefix(
    tmp_path: Path,
    max_records: int,
    record_reserve: int,
    incomplete_evidence: str,
) -> None:
    window = fixture_campaign_windows(campaign_id=f"resource-prefix-{max_records}")[0]
    envelope = CycleEnvelope(
        max_records=max_records,
        record_reserve=record_reserve,
        max_bytes=1_000_000,
        bytes_reserve=100_000,
    )
    store, writer, _driver, _ingress, producer = _open_stream(
        tmp_path,
        window,
        envelope=envelope,
        monotonic_ns=lambda: 500_000_000,
    )
    try:
        if incomplete_evidence == "FINAL_RESULT_PREFIX":
            attempt = fixture_cycle_attempts(window, count=1, profile="normal")[0]
            items = tuple(
                _feed_value(value, source_kind="SNAPSHOT")
                for value in attempt.source_books
            ) + tuple(_feed_value(value) for value in attempt.events)
        else:
            items = (
                _feed_value(
                    _fixture_book(Venue.RISEX, 500_000_000, 1),
                    source_kind="SNAPSHOT",
                ),
                _feed_value(
                    _fixture_book(
                        Venue.LIGHTER,
                        500_000_000,
                        1,
                        bids=(("99", "10"),),
                        asks=(("100", "10"),),
                    ),
                    source_kind="SNAPSHOT",
                ),
            )
        for item in items:
            producer.handle_item(item)
        with pytest.raises(CycleEnvelopeLimitError, match="records"):
            producer.finalize(failed=True, reason="probe")
        records_after_first_finalize = list(iter_records(store.path))
        assert records_after_first_finalize[-1]["kind"] == "RUN_FAILED"
        # A caller retry must not append a second terminal or change the
        # already-recorded failed resource identity.
        with pytest.raises(CycleEnvelopeLimitError, match="records"):
            producer.finalize(failed=True, reason="probe")
        records = list(iter_records(store.path))
        assert records == records_after_first_finalize
        assert len(records) == max_records
        assert records[-1]["kind"] == "RUN_FAILED"
        assert records[-1]["fatal_reason"] == "S3_ENVELOPE_LIMIT_RECORDS"
        assert records[-1]["incomplete_evidence"] == incomplete_evidence
        assert records[-1]["record_index"] == max_records - 1
        assert writer.terminal_written

        report = build_cycle_report(store.path)
        assert report["measurement_validity"] == "DATA_INSUFFICIENT"
        assert report["evidence_sufficiency"] == "INSUFFICIENT"
        summary = report["windows"][0]
        assert summary["incomplete_evidence"] == incomplete_evidence
        assert summary["primary"]["cycle_count"] == 0
        assert summary["cycle_result_metrics_complete"] is False
        assert summary["cycle_result_metrics_status"] == "UNAVAILABLE_RESOURCE_LIMIT"
        assert summary["primary"]["cycle_result_metrics_complete"] is False
        assert summary["primary"]["turnover_usd"] is None
        assert "do not mean observed zero exposure" in summary["primary"]["cycle_result_metrics_note"]
        assert report["data_quality"]["cycle_result_metrics_complete"] is False
        assert report["data_quality"]["cycle_result_metrics_status"] == "UNAVAILABLE_RESOURCE_LIMIT"
        assert report["economics"]["primary"]["cycle_result_metrics_complete"] is False
        assert report["economics"]["primary"]["turnover_usd"] is None
        if incomplete_evidence == "FINAL_RESULT_PREFIX":
            assert summary["replayed_final_result_count"] == 3
            assert any(
                record["kind"] == "CYCLE_FINAL_RESULT"
                and record["result"]["entry_measurement"]["filled_quantity"] == "1.00"
                for record in records
            )
    finally:
        store.close()


@pytest.mark.parametrize("corrupt_marker", [None, "CORRUPT_PREFIX"])
def test_resource_prefix_without_valid_marker_is_not_replay_authorized(
    tmp_path: Path,
    corrupt_marker: str | None,
) -> None:
    window = fixture_campaign_windows(campaign_id="resource-prefix-corrupt")[0]
    envelope = CycleEnvelope(
        max_records=12,
        record_reserve=3,
        max_bytes=1_000_000,
        bytes_reserve=100_000,
    )
    store, _writer, _driver, _ingress, producer = _open_stream(
        tmp_path,
        window,
        envelope=envelope,
        monotonic_ns=lambda: 500_000_000,
    )
    producer.handle_item(
        _feed_value(_fixture_book(Venue.RISEX, 500_000_000, 1), source_kind="SNAPSHOT")
    )
    producer.handle_item(
        _feed_value(
            _fixture_book(
                Venue.LIGHTER,
                500_000_000,
                1,
                bids=(("99", "10"),),
                asks=(("100", "10"),),
            ),
            source_kind="SNAPSHOT",
        )
    )
    with pytest.raises(CycleEnvelopeLimitError):
        producer.finalize(failed=True, reason="probe")
    records = list(iter_records(store.path))
    if corrupt_marker is None:
        records[-1].pop("incomplete_evidence")
    else:
        records[-1]["incomplete_evidence"] = corrupt_marker
    store.path.write_text(
        "".join(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n" for record in records),
        encoding="utf-8",
    )
    store.close()
    expected_error = (
        "cycle results do not replay identically"
        if corrupt_marker is None
        else "incomplete evidence marker is invalid"
    )
    with pytest.raises(CycleEvidenceIntegrityError, match=expected_error):
        build_cycle_report(store.path)
