from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path

import pytest

from risex_farmer.models import BookLevel, ContractType, MarketType, Venue

from risex_spread_shadow import (
    AppendOnlyEvidenceStore,
    DataGapEvidence,
    EvidenceStorageLimitExceeded,
    FeedBookEvent,
    FeedGapEvent,
    FeedTradeEvent,
    MarketPair,
    Side,
    SpreadDirection,
    SpreadObserver,
    build_fixed_offline_evaluation,
    fixed_scanner_config,
    fixed_scanner_policy_fingerprint,
    fixed_scanner_policy_fields,
    fixed_scanner_stage_fingerprint,
    reserve_scanner_stage,
)
from risex_spread_shadow import scanner as scanner_module
from risex_spread_shadow import offline_evaluation as offline_module
from risex_spread_shadow import cli as cli_module
from risex_spread_shadow import store as store_module
from risex_spread_shadow.book_chain import book_state_sha256
from risex_spread_shadow.models import BookEvidence, TradeEvidence

from tests.spread_shadow.test_offline_evaluation import _qualified_records
from tests.spread_shadow.test_pipeline import evidence_book, market, PAIR, trade


UTC = timezone.utc
RELEASE = "f" * 40
SAMPLE_START = datetime(2026, 9, 1, tzinfo=UTC)


def _fixed_records(
    *,
    stage_kind: str = "FIXTURE",
    stage_name: str = "CAL-001",
    negative_unit: int | None = None,
    omit_horizon: tuple[str, int] | None = None,
) -> list[dict[str, object]]:
    records = _qualified_records(negative_unit=negative_unit)
    for record in records:
        if record.get("kind") == "QUOTE":
            record["risex_fee_source"] = "SS-001Q"
            record["lighter_fee_source"] = "OFFICIAL_LIGHTER_ACCOUNT_TYPES_2026-09-05"
            record["observed_monotonic_ns"] = record["quote_created_monotonic_ns"]
        elif record.get("kind") == "HEDGE_HORIZON":
            record["observed_monotonic_ns"] = (
                int(record["would_fill_detected_monotonic_ns"])
                + int(record["horizon_ms"]) * 1_000_000
            )
        elif record.get("kind") == "WOULD_FILL":
            record["observed_monotonic_ns"] = record[
                "would_fill_detected_monotonic_ns"
            ]

    run_id = "synthetic-scan-003"
    sample_end = SAMPLE_START + timedelta(seconds=1201)
    interval = {
        "start_monotonic_ns": 0,
        "end_monotonic_ns": 1_201_000_000_000,
        "start_utc": SAMPLE_START.isoformat(),
        "end_utc": sample_end.isoformat(),
    }
    policy = fixed_scanner_policy_fields(RELEASE)
    policy_fingerprint = fixed_scanner_policy_fingerprint(RELEASE)
    stage: dict[str, object] = {
        "stage_name": stage_name,
        "stage_kind": stage_kind,
        "run_id": run_id,
        "accepted_release": RELEASE,
        "policy": policy,
        "policy_fingerprint": policy_fingerprint,
        "requested_window_utc": {
            "start_utc": SAMPLE_START.isoformat(),
            "end_utc": (SAMPLE_START + timedelta(hours=1)).isoformat(),
        },
        "sample_start": {
            "monotonic_ns": 0,
            "utc": SAMPLE_START.isoformat(),
        },
        "limits": {
            "eligible_trade_limit": 250,
            "wall_clock_seconds": 1200,
            "record_cap": 1_000_000,
            "byte_cap": 4 * 1024 * 1024 * 1024,
            "terminal_drain_allowance_ns": 2_200_000_000,
            "fill_count_stop": None,
        },
    }
    terminal_payload = {
        "stage_name": stage_name,
        "stage_kind": stage_kind,
        "run_id": run_id,
        "accepted_release": RELEASE,
        "policy_fingerprint": policy_fingerprint,
        "sample_interval": interval,
        "stop": {
            "reason": "WALL_CLOCK_LIMIT",
            "strict_episode_count": sum(
                record.get("kind") == "WOULD_FILL"
                and record.get("fillability_model") == "STRICT_LOWER_BOUND"
                for record in records
            ),
            "eligible_trade_count": sum(
                record.get("kind") == "RISEX_TRADE"
                and record.get("eligible_trade") is True
                for record in records
            ),
            "optimistic_episode_count": 0,
            "integrity_reason": None,
            "observed_monotonic_ns": 1_200_000_000_000,
        },
    }
    terminal_payload["stage_fingerprint"] = fixed_scanner_stage_fingerprint(
        stage_name=stage_name,
        stage_kind=stage_kind,
        run_id=run_id,
        accepted_release=RELEASE,
        sample_interval=interval,
        policy_fingerprint=policy_fingerprint,
    )
    records[0] = {
        "kind": "RUN_METADATA",
        "metadata": {
            "run_id": run_id,
            "evidence_mode": "FIXTURE" if stage_kind == "FIXTURE" else "OBSERVATIONAL",
            "scan_003": stage,
        },
    }
    if stage_kind == "FIXTURE":
        records.insert(1, {"kind": "REPLAY_MODE", "evidence_mode": "FIXTURE"})
    records.insert(
        2 if stage_kind == "FIXTURE" else 1,
        {
            "kind": "RUN_START",
            "scan_003": {
                "stage_name": stage_name,
                "stage_kind": stage_kind,
                "run_id": run_id,
                "accepted_release": RELEASE,
                "sample_start": dict(stage["sample_start"]),
            },
            "observed_monotonic_ns": 0,
        },
    )
    if omit_horizon is not None:
        version, horizon = omit_horizon
        records = [
            record
            for record in records
            if not (
                record.get("kind") == "HEDGE_HORIZON"
                and record.get("quote_version_id") == version
                and record.get("horizon_ms") == horizon
            )
        ]
    stop = terminal_payload["stop"]
    records.insert(
        len(records) - 1,
        {
            "kind": "SAMPLE_STOP",
            **dict(stop),
        },
    )
    records[-1] = {
        "kind": "RUN_STOP",
        "fatal_reason": None,
        "stopped_utc": sample_end,
        "observed_monotonic_ns": 1_201_000_000_000,
        "scan_003": terminal_payload,
    }
    return records


def _write_records(root: Path, records: list[dict[str, object]]) -> Path:
    metadata = records[0]["metadata"]
    assert isinstance(metadata, dict)
    store = AppendOnlyEvidenceStore.create(
        root,
        metadata=metadata,
        run_id=str(metadata["run_id"]),
        max_records=1_000_000,
        max_bytes=4 * 1024 * 1024 * 1024,
    )
    store.append_batch(records[1:])
    path = store.path
    store.close()
    return path


def _holdout_records(
    binding: dict[str, object],
    *,
    start: datetime,
    shared_identities: bool = False,
    selected_margin_bps: str | None = None,
) -> list[dict[str, object]]:
    records = _fixed_records(stage_kind="PUBLIC", stage_name="HOLDOUT-001")
    metadata = records[0]["metadata"]
    assert isinstance(metadata, dict)
    stage = metadata["scan_003"]
    assert isinstance(stage, dict)
    run_id = "synthetic-holdout-scan-003"
    metadata["run_id"] = run_id
    stage["run_id"] = run_id
    stage["requested_window_utc"] = {
        "start_utc": start.isoformat(),
        "end_utc": (start + timedelta(hours=1)).isoformat(),
    }
    stage["sample_start"] = {
        "monotonic_ns": 0,
        "utc": start.isoformat(),
    }
    stage["cal_reference"] = dict(binding)
    if selected_margin_bps is not None:
        stage["cal_reference"]["selected_margin_bps"] = selected_margin_bps

    terminal = records[-1]["scan_003"]
    assert isinstance(terminal, dict)
    terminal["run_id"] = run_id
    interval = terminal["sample_interval"]
    assert isinstance(interval, dict)
    interval["start_utc"] = start.isoformat()
    interval["end_utc"] = (start + timedelta(seconds=1201)).isoformat()
    records[-1]["stopped_utc"] = start + timedelta(seconds=1201)
    records[-1]["scan_003"]["stage_fingerprint"] = fixed_scanner_stage_fingerprint(
        stage_name="HOLDOUT-001",
        stage_kind="PUBLIC",
        run_id=run_id,
        accepted_release=RELEASE,
        sample_interval=interval,
        policy_fingerprint=stage["policy_fingerprint"],
    )
    run_start = next(record for record in records if record.get("kind") == "RUN_START")
    run_start_payload = run_start["scan_003"]
    assert isinstance(run_start_payload, dict)
    run_start_payload["run_id"] = run_id
    run_start_payload["sample_start"] = dict(stage["sample_start"])

    if not shared_identities:
        key_mapping: dict[str, str] = {}
        for record in records:
            if record.get("kind") != "RISEX_TRADE":
                continue
            old_key = record["trade_event_key"]
            assert isinstance(old_key, str)
            order_ids = old_key.rsplit("|", 1)[-1].split("-", 1)
            # Keep each synthetic side as one parser token.  The production
            # contract splits this compact key at the first hyphen.
            new_key = f"RISEX|BTC/USDC|h{order_ids[0]}-h{order_ids[1]}"
            key_mapping[old_key] = new_key
            record["trade_event_key"] = new_key
            record["maker_order_id"] = f"h{order_ids[0]}"
            record["taker_order_id"] = f"h{order_ids[1]}"
        for record in records:
            if record.get("kind") == "WOULD_FILL":
                keys = record["qualifying_trade_event_keys"]
                assert isinstance(keys, list)
                record["qualifying_trade_event_keys"] = [key_mapping[key] for key in keys]
    return records


def _wide_arm_single_detection_timestamp_records() -> list[dict[str, object]]:
    """Keep arm 1 qualified while arm 2 fails only its concentration data."""

    records = _fixed_records(stage_kind="PUBLIC")
    first_book = next(
        record
        for record in records
        if record.get("kind") == "BOOK" and record.get("venue") == Venue.LIGHTER.value
    )
    first_reference = {
        "book_received_monotonic_ns": first_book["received_monotonic_ns"],
        "book_stream_session_id": first_book["stream_session_id"],
        "book_recovery_generation": first_book["recovery_generation"],
        "book_revision": first_book["book_revision"],
        "book_revision_id": first_book["book_revision_id"],
        "book_state_sha256": first_book["book_state_sha256"],
    }
    for record in records:
        version = record.get("quote_version_id")
        if not isinstance(version, str) or not version.startswith("vw-"):
            continue
        if record.get("kind") == "QUOTE":
            record["quote_created_monotonic_ns"] = 9_000_000_000
            record["quote_expires_monotonic_ns"] = 1_000_000_000_000
        elif record.get("kind") == "WOULD_FILL":
            record["would_fill_detected_monotonic_ns"] = 10_000_000_000
        elif record.get("kind") == "HEDGE_HORIZON":
            record["would_fill_detected_monotonic_ns"] = 10_000_000_000
            record["horizon_deadline_monotonic_ns"] = (
                10_000_000_000 + int(record["horizon_ms"]) * 1_000_000
            )
            record.update(first_reference)
    return records


def _mild_primary_loss_records() -> list[dict[str, object]]:
    """Create three small losses while keeping both primary sums positive."""

    records = _fixed_records(stage_kind="PUBLIC")
    changed_digests: dict[int, str] = {}
    for record in records:
        if record.get("kind") != "BOOK" or record.get("venue") != Venue.LIGHTER.value:
            continue
        revision = int(record["book_revision"])
        if revision not in {1, 2, 3}:
            continue
        asks = (BookLevel(Decimal("101.2"), Decimal("2")),)
        record["asks"] = ({"price": "101.2", "quantity": "2"},)
        evidence = BookEvidence(
            Venue.LIGHTER,
            "BTC",
            (BookLevel(Decimal("98"), Decimal("2")),),
            asks,
            int(record["received_monotonic_ns"]),
            record["stream_session_id"],
            int(record["recovery_generation"]),
            revision,
            sequence=int(record["sequence"]),
            checksum=record["checksum"],
            sequence_valid=bool(record["sequence_valid"]),
            checksum_valid=bool(record["checksum_valid"]),
            fresh=bool(record["fresh"]),
        )
        digest = book_state_sha256(evidence)
        record["book_state_sha256"] = digest
        changed_digests[revision] = digest
    for record in records:
        version = record.get("quote_version_id")
        if not isinstance(version, str) or not version.rsplit("-", 1)[-1].isdigit():
            continue
        unit_index = int(version.rsplit("-", 1)[-1])
        revision = unit_index + 1
        if revision not in changed_digests or record.get("kind") != "HEDGE_HORIZON":
            continue
        record["book_state_sha256"] = changed_digests[revision]
        record["vwap_price"] = "101.2"
        record["notional_usd"] = "101.2"
        record["entry_edge_usd"] = "-1.21" if version.startswith("vn-") else "-0.2101"
    return records


def test_fixed_profile_is_immutable_and_material_stop_is_disabled(tmp_path: Path) -> None:
    config = fixed_scanner_config()
    assert config.target_notionals_usd == (Decimal("100"),)
    assert config.target_margins_bps == (Decimal("1"), Decimal("2"))
    assert config.eligible_trade_limit == 250
    assert config.duration_seconds == 1200
    assert config.risex_maker_fee_rate == Decimal("0.0001")
    assert config.lighter_taker_fee_rate == Decimal("0")
    policy = fixed_scanner_policy_fields(RELEASE)
    assert policy["configuration"]["direction"] == "RISEX_SELL_LIGHTER_BUY"
    assert policy["configuration"]["material_fill_stop"] is False
    assert policy["formulas"]["p05"] == "ordered_values[floor(0.05*(n-1))]"
    assert policy["formulas"]["median"] == "middle_value_or_midpoint_of_two_middle_values"
    assert policy["formulas"]["positive_sum"] == "sum_of_all_clean_unit_values_strictly_greater_than_zero"
    assert policy["thresholds"]["all_horizon_positive_sum"] is True
    assert policy["thresholds"]["strict_positive_p05_1000ms"] is True
    assert fixed_scanner_policy_fingerprint(RELEASE) != fixed_scanner_policy_fingerprint("e" * 40)

    store = AppendOnlyEvidenceStore.create(tmp_path, metadata={"evidence_mode": "FIXTURE"})
    observer = SpreadObserver(
        config,
        (),
        store,
        directions=(SpreadDirection.RISEX_SELL_LIGHTER_BUY,),
        material_stop_enabled=False,
    )
    assert observer.directions == (SpreadDirection.RISEX_SELL_LIGHTER_BUY,)
    assert observer.material_stop_enabled is False
    store.close()


def test_stage_reservation_is_create_once_and_directory_sync_failure_is_retained(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    claim = reserve_scanner_stage(
        tmp_path / "claims",
        stage_name="CAL-001",
        run_id="first",
        accepted_release=RELEASE,
        window_start_utc="2026-09-01T00:00:00+00:00",
        window_end_utc="2026-09-02T00:00:00+00:00",
        claimed_utc="2026-09-01T00:00:00+00:00",
    )
    assert claim.stat().st_mode & 0o777 == 0o600
    with pytest.raises(store_module.ScannerStageClaimError):
        reserve_scanner_stage(
            tmp_path / "claims",
            stage_name="CAL-001",
            run_id="second",
            accepted_release=RELEASE,
            window_start_utc="2026-09-01T00:00:00+00:00",
            window_end_utc="2026-09-02T00:00:00+00:00",
            claimed_utc="2026-09-01T00:00:01+00:00",
        )

    calls = 0
    real_fsync = store_module.os.fsync

    def fail_directory_sync(descriptor: int) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("directory sync unavailable")
        real_fsync(descriptor)

    monkeypatch.setattr(store_module.os, "fsync", fail_directory_sync)
    failed_root = tmp_path / "sync-failure"
    with pytest.raises(store_module.ScannerStageClaimError, match="durably synced|sync failed"):
        reserve_scanner_stage(
            failed_root,
            stage_name="HOLDOUT-001",
            run_id="ambiguous",
            accepted_release=RELEASE,
            window_start_utc="2026-09-03T00:00:00+00:00",
            window_end_utc="2026-09-04T00:00:00+00:00",
            claimed_utc="2026-09-01T00:00:01+00:00",
        )
    assert (failed_root / ".scan-003" / "HOLDOUT-001.claim").is_file()


def test_fixed_store_instance_cap_preserves_terminal_failure_reserve(tmp_path: Path) -> None:
    store = AppendOnlyEvidenceStore.create(
        tmp_path,
        metadata={"evidence_mode": "FIXTURE"},
        run_id="small-cap",
        max_records=3,
        max_bytes=64 * 1024,
    )
    assert store.append_batch(({"kind": "EVIDENCE", "observed_monotonic_ns": 1},)) == (1,)
    with pytest.raises(EvidenceStorageLimitExceeded, match="RECORD_COUNT"):
        store.append_batch(({"kind": "EVIDENCE", "observed_monotonic_ns": 2},))
    assert store.append_batch(
        ({"kind": "RUN_FAILED", "fatal_reason": "RECORD_CAP"},)
    ) == (2,)
    store.close()
    rows = list(store_module.iter_records(store.path))
    assert [row["record_index"] for row in rows] == [0, 1, 2]
    assert rows[-1]["kind"] == "RUN_FAILED"


def test_cli_and_run_preflight_release_window_and_claim_before_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    network_called = False

    async def public_selection_sentinel(*_args, **_kwargs):
        nonlocal network_called
        network_called = True
        raise AssertionError("public selection must not run")

    monkeypatch.setattr(scanner_module, "select_public_market_pairs", public_selection_sentinel)
    with pytest.raises(SystemExit, match="full lowercase"):
        cli_module.main(
            [
                "scan",
                "--store-root",
                str(tmp_path / "cli-release"),
                "--stage",
                "CAL-001",
                "--accepted-release",
                "not-a-release",
                "--window-start-utc",
                SAMPLE_START.isoformat(),
                "--window-end-utc",
                (SAMPLE_START + timedelta(hours=1)).isoformat(),
            ]
        )
    assert network_called is False
    assert not (tmp_path / "cli-release" / ".scan-003").exists()

    monkeypatch.setattr(
        scanner_module,
        "validate_loaded_release",
        lambda *_args, **_kwargs: tmp_path,
    )
    missed_root = tmp_path / "missed-window"
    with pytest.raises(scanner_module.ScannerPreconditionError, match="prospective UTC window"):
        asyncio.run(
            scanner_module.run_fixed_scanner(
                missed_root,
                stage_name="CAL-001",
                accepted_release=RELEASE,
                window_start_utc=SAMPLE_START,
                window_end_utc=SAMPLE_START + timedelta(hours=1),
                now_utc=lambda: SAMPLE_START - timedelta(minutes=1),
                monotonic_ns=lambda: 1,
            )
        )
    assert (missed_root / ".scan-003" / "CAL-001.claim").is_file()
    assert network_called is False

    reused_root = tmp_path / "reused-claim"
    reserve_scanner_stage(
        reused_root,
        stage_name="CAL-001",
        run_id="prior-attempt",
        accepted_release=RELEASE,
        window_start_utc=SAMPLE_START.isoformat(),
        window_end_utc=(SAMPLE_START + timedelta(hours=1)).isoformat(),
        claimed_utc=SAMPLE_START.isoformat(),
    )
    with pytest.raises(store_module.ScannerStageClaimError, match="already claimed"):
        asyncio.run(
            scanner_module.run_fixed_scanner(
                reused_root,
                stage_name="CAL-001",
                accepted_release=RELEASE,
                window_start_utc=SAMPLE_START,
                window_end_utc=SAMPLE_START + timedelta(hours=1),
                now_utc=lambda: SAMPLE_START + timedelta(minutes=1),
                monotonic_ns=lambda: 1,
            )
        )
    assert network_called is False


def test_fixed_synthetic_positive_negative_and_incomplete_results_are_not_public() -> None:
    positive = build_fixed_offline_evaluation(_fixed_records())
    assert positive["section"] == "SCAN_003_FIXED_OFFLINE_EVALUATION"
    assert positive["mathematical_verdict"] == "NUMERICAL_QUALIFIED"
    assert positive["stage_verdict"] == "FIXTURE_ONLY"
    assert positive["stage_qualified"] is False
    assert positive["evidence_outcome"] == "INSUFFICIENT"
    assert positive["fixed_profile_quote_issues"] == []

    negative = build_fixed_offline_evaluation(_fixed_records(negative_unit=0))
    assert negative["mathematical_verdict"] == "NUMERICAL_FAILED"
    assert negative["stage_verdict"] == "FIXTURE_ONLY"
    assert negative["evidence_outcome"] == "INSUFFICIENT"

    incomplete = build_fixed_offline_evaluation(
        _fixed_records(omit_horizon=("vn-0", 500))
    )
    assert incomplete["mathematical_verdict"] != "NUMERICAL_QUALIFIED"
    assert incomplete["coverage"]["unresolved_unit_count"] == 1
    assert incomplete["stage_verdict"] == "FIXTURE_ONLY"
    assert incomplete["evidence_outcome"] == "INSUFFICIENT"


def test_public_store_to_fixed_report_admits_only_complete_physical_shape(tmp_path: Path) -> None:
    path = _write_records(tmp_path, _fixed_records(stage_kind="PUBLIC"))
    result = build_fixed_offline_evaluation(path)
    assert result["mathematical_verdict"] == "NUMERICAL_QUALIFIED"
    assert result["stage_verdict"] == "CAL_PASS_PROVISIONAL"
    assert result["stage_qualified"] is True
    assert result["candidate_eligible"] is False
    assert result["evidence_outcome"] == "POSITIVE"
    assert result["failed_gates"] == []
    assert result["provenance"]["stage_fingerprint"] == result["provenance"]["computed_stage_fingerprint"]
    assert result["gate_results"]["SCANNER_PERSISTED_RECORD_INDICES"]["passed"] is True


def test_fixed_public_outcome_distinguishes_negative_economics_from_fixture_data(
    tmp_path: Path,
) -> None:
    path = _write_records(
        tmp_path,
        _fixed_records(stage_kind="PUBLIC", negative_unit=0),
    )
    result = build_fixed_offline_evaluation(path)
    assert result["stage_verdict"] == "CALIBRATION_FAILED"
    assert result["evidence_outcome"] == "NEGATIVE"

    table = scanner_module.render_fixed_report(path, format="table")
    assert "raw_strict_complete=True" in table
    assert "clusters=50 detection_timestamps=50" in table
    assert "conditional entry-unit scores; not executable PnL" in table
    assert "SS-001Q" in table


def test_fixed_public_positive_sums_below_threshold_are_not_confirmed(
    tmp_path: Path,
) -> None:
    path = _write_records(tmp_path, _mild_primary_loss_records())
    result = build_fixed_offline_evaluation(path)
    assert result["selector"]["arm_scores"]["1"]["sum_300ms"] == "42.9000"
    assert result["selector"]["arm_scores"]["2"]["sum_300ms"] == "92.8950"
    assert result["stage_verdict"] == "CALIBRATION_FAILED"
    assert result["evidence_outcome"] == "NOT_CONFIRMED"


def test_one_arm_can_qualify_when_nonselected_arm_lacks_concentration_data(
    tmp_path: Path,
) -> None:
    path = _write_records(
        tmp_path,
        _wide_arm_single_detection_timestamp_records(),
    )
    result = build_fixed_offline_evaluation(path)
    assert result["selector"]["selected_margin_bps"] == "1"
    assert result["selector"]["arm_qualifies"] == {"1": True, "2": False}
    assert result["gate_results"]["ARM_2_DETECTION_TIMESTAMP_FLOOR"]["passed"] is False
    assert result["stage_qualified"] is True
    assert result["evidence_outcome"] == "POSITIVE"


def _small_quantity_pair() -> MarketPair:
    return replace(
        PAIR,
        risex_market=replace(
            PAIR.risex_market,
            quantity_step_raw=Decimal("0.01"),
            minimum_quantity_raw=Decimal("0.01"),
        ),
        lighter_market=replace(
            PAIR.lighter_market,
            quantity_step_raw=Decimal("0.01"),
            minimum_quantity_raw=Decimal("0.01"),
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", ("positive", "adverse", "gap"))
async def test_actual_observer_store_to_fixed_report_preserves_causal_horizon_order(
    tmp_path: Path,
    scenario: str,
) -> None:
    pair = _small_quantity_pair()
    run_id = "actual-observer-scan-003"
    sample_start_ns = 1_000_000_000
    sample_end = SAMPLE_START + timedelta(seconds=1201)
    stage = scanner_module._stage_metadata(
        stage_name="CAL-001",
        run_id=run_id,
        accepted_release=RELEASE,
        window_start=SAMPLE_START,
        window_end=SAMPLE_START + timedelta(hours=2),
        sample_start_utc=SAMPLE_START,
        sample_start_ns=sample_start_ns,
    )
    stage["stage_kind"] = "FIXTURE"
    store = AppendOnlyEvidenceStore.create(
        tmp_path,
        metadata={"evidence_mode": "FIXTURE", "scan_003": stage},
        run_id=run_id,
        max_records=1_000_000,
        max_bytes=4 * 1024 * 1024 * 1024,
    )
    observer = SpreadObserver(
        fixed_scanner_config(),
        (pair,),
        store,
        now_utc=lambda: SAMPLE_START,
        monotonic_ns=lambda: sample_start_ns + 1_200_000_000_000,
        sample_started_monotonic_ns=sample_start_ns,
        directions=(SpreadDirection.RISEX_SELL_LIGHTER_BUY,),
        material_stop_enabled=False,
        enforce_sample_deadline=True,
    )
    await observer._append(
        (
            {
                "kind": "RUN_START",
                "scan_003": {
                    "stage_name": "CAL-001",
                    "stage_kind": "FIXTURE",
                    "run_id": run_id,
                    "accepted_release": RELEASE,
                    "sample_start": dict(stage["sample_start"]),
                },
                "observed_monotonic_ns": sample_start_ns,
            },
        )
    )
    risex_book = replace(
        evidence_book(
            Venue.RISEX,
            received=sample_start_ns + 1_000_000_000,
            session="risex",
        ),
        received_utc=SAMPLE_START + timedelta(seconds=1),
    )
    lighter_book = replace(
        evidence_book(
            Venue.LIGHTER,
            received=sample_start_ns + 1_000_000_000,
            session="lighter",
            bids=(("100", "10"),),
            asks=(("102", "10"),),
        ),
        received_utc=SAMPLE_START + timedelta(seconds=1),
    )
    await observer.handle_book(
        FeedBookEvent(risex_book, pair, "SNAPSHOT", "fixture")
    )
    await observer.handle_book(
        FeedBookEvent(lighter_book, pair, "SNAPSHOT", "fixture")
    )
    actual_trade = replace(
        trade(received=sample_start_ns + 2_000_000_000),
        trade_event_key="RISEX|BTC/USDC|maker-actual-taker",
        canonical_price=Decimal("105"),
        aggressor_side=Side.BUY,
        received_utc=SAMPLE_START + timedelta(seconds=2),
        exchange_event_utc=SAMPLE_START + timedelta(seconds=2),
    )
    await observer.handle_trade(FeedTradeEvent(actual_trade, pair, "fixture"))
    if scenario == "adverse":
        delayed_lighter_book = replace(
            evidence_book(
                Venue.LIGHTER,
                received=sample_start_ns + 2_500_000_000,
                session="lighter",
                bids=(("104", "10"),),
                asks=(("106", "10"),),
                revision=2,
                sequence=2,
            ),
            received_utc=SAMPLE_START + timedelta(seconds=2, milliseconds=500),
        )
        await observer.handle_book(
            FeedBookEvent(delayed_lighter_book, pair, "SNAPSHOT", "fixture")
        )
    elif scenario == "gap":
        await observer.handle_gap(
            FeedGapEvent(
                DataGapEvidence(
                    source_venue=Venue.LIGHTER,
                    canonical_market="BTC",
                    stream_session_id="lighter",
                    recovery_generation=0,
                    gap_start_monotonic_ns=sample_start_ns + 2_000_000_000,
                    reason="FIXTURE_HEDGE_GAP",
                )
            )
        )
    await asyncio.sleep(0)
    capture_tasks = tuple(observer._tasks)
    if capture_tasks:
        outcomes = await asyncio.gather(*capture_tasks, return_exceptions=True)
        assert all(not isinstance(outcome, BaseException) for outcome in outcomes)
    await observer.flush_pending(force=True)
    await observer.trigger_wall_clock_stop()
    await observer.append_terminal(
        scanner_module._terminal_record(
            kind="RUN_STOP",
            stage=stage,
            observer=observer,
            terminal_utc=sample_end,
            terminal_ns=sample_start_ns + 1_201_000_000_000,
            fatal_reason=observer.fatal_reason,
        )
    )
    await observer.close()
    store.close()

    rows = list(store_module.iter_records(store.path))
    trade_index = next(index for index, row in enumerate(rows) if row.get("kind") == "RISEX_TRADE")
    fill_indices = [
        index for index, row in enumerate(rows) if row.get("kind") == "WOULD_FILL"
    ]
    horizon_zero_indices = [
        index
        for index, row in enumerate(rows)
        if row.get("kind") == "HEDGE_HORIZON" and row.get("horizon_ms") == 0
    ]
    assert fill_indices
    assert horizon_zero_indices
    assert trade_index < min(fill_indices) < min(horizon_zero_indices)
    assert len(horizon_zero_indices) == 4

    result = build_fixed_offline_evaluation(store.path)
    assert result["integrity_issues"] == []
    assert result["stage_verdict"] == "FIXTURE_ONLY"
    assert result["coverage"]["unresolved_unit_count"] == 0
    if scenario == "positive":
        assert result["coverage"]["clean_unit_count"] == 1
    elif scenario == "adverse":
        assert result["coverage"]["clean_unit_count"] == 1
        assert any(
            Decimal(row["entry_edge_usd"]) < 0
            for row in rows
            if row.get("kind") == "HEDGE_HORIZON"
            and row.get("fillability_model") == "STRICT_LOWER_BOUND"
            and row.get("horizon_ms") in {500, 1000}
            and row.get("entry_edge_usd") is not None
        )
        assert result["mathematical_verdict"] == "NUMERICAL_FAILED"
    else:
        assert result["coverage"]["contaminated_unit_count"] == 1
        assert {
            row.get("outcome")
            for row in rows
            if row.get("kind") == "HEDGE_HORIZON"
        } == {"HEDGE_DATA_GAP"}


@pytest.mark.asyncio
async def test_fixed_cutoff_accepts_before_deadline_rejects_at_cutoff_and_fails_on_late_pre_cutoff_ingress(
    tmp_path: Path,
) -> None:
    pair = _small_quantity_pair()
    base = 1_000_000_000
    store = AppendOnlyEvidenceStore.create(tmp_path, metadata={"evidence_mode": "FIXTURE"})
    observer = SpreadObserver(
        fixed_scanner_config(),
        (pair,),
        store,
        sample_started_monotonic_ns=base,
        directions=(SpreadDirection.RISEX_SELL_LIGHTER_BUY,),
        material_stop_enabled=False,
        enforce_sample_deadline=True,
    )
    book_received = base + 1_195_000_000_000
    await observer.handle_book(
        FeedBookEvent(
            evidence_book(Venue.RISEX, received=book_received, session="risex"),
            pair,
            "SNAPSHOT",
            "fixture",
        )
    )
    await observer.handle_book(
        FeedBookEvent(
            evidence_book(
                Venue.LIGHTER,
                received=book_received,
                session="lighter",
                bids=(("100", "10"),),
                asks=(("102", "10"),),
            ),
            pair,
            "SNAPSHOT",
            "fixture",
        )
    )
    before_cutoff = replace(
        trade(received=base + 1_200_000_000_000 - 1),
        trade_event_key="before-cutoff",
        aggressor_side=Side.BUY,
    )
    await observer.handle_trade(FeedTradeEvent(before_cutoff, pair, "fixture"))
    assert observer.eligible_trade_count == 1
    assert observer.sample_stop_signal is None

    at_cutoff = replace(
        before_cutoff,
        trade_event_key="at-cutoff",
        received_monotonic_ns=base + 1_200_000_000_000,
    )
    await observer.handle_trade(FeedTradeEvent(at_cutoff, pair, "fixture"))
    assert observer.eligible_trade_count == 1
    assert observer.sample_stop_signal is not None
    assert observer.sample_stop_signal.reason.value == "WALL_CLOCK_LIMIT"

    queued_before_cutoff = replace(
        before_cutoff,
        trade_event_key="queued-before-cutoff",
        received_monotonic_ns=base + 1_200_000_000_000 - 2,
    )
    await observer.handle_trade(FeedTradeEvent(queued_before_cutoff, pair, "fixture"))
    assert observer.fatal_reason == "PRE_CUTOFF_INGRESS_AFTER_STOP"
    assert observer.eligible_trade_count == 1
    await observer.close()
    store.close()


@pytest.mark.asyncio
async def test_fixed_eligible_cap_stops_without_treating_later_pre_wall_trades_as_failure(
    tmp_path: Path,
) -> None:
    pair = _small_quantity_pair()
    base = 1_000_000_000
    store = AppendOnlyEvidenceStore.create(tmp_path, metadata={"evidence_mode": "FIXTURE"})
    observer = SpreadObserver(
        fixed_scanner_config(),
        (pair,),
        store,
        sample_started_monotonic_ns=base,
        directions=(SpreadDirection.RISEX_SELL_LIGHTER_BUY,),
        material_stop_enabled=False,
        enforce_sample_deadline=True,
    )
    await observer.handle_book(
        FeedBookEvent(
            evidence_book(Venue.RISEX, received=base, session="risex"),
            pair,
            "SNAPSHOT",
            "fixture",
        )
    )
    await observer.handle_book(
        FeedBookEvent(
            evidence_book(
                Venue.LIGHTER,
                received=base,
                session="lighter",
                bids=(("100", "10"),),
                asks=(("102", "10"),),
            ),
            pair,
            "SNAPSHOT",
            "fixture",
        )
    )
    for index in range(250):
        await observer.handle_trade(
            FeedTradeEvent(
                replace(
                    trade(received=base + index + 1),
                    trade_event_key=f"cap-{index}",
                    aggressor_side=Side.BUY,
                ),
                pair,
                "fixture",
            )
        )
    assert observer.eligible_trade_count == 250
    assert observer.sample_stop_signal is not None
    assert observer.sample_stop_signal.reason.value == "ELIGIBLE_TRADE_LIMIT"
    assert observer.fatal_reason is None

    await observer.handle_trade(
        FeedTradeEvent(
            replace(
                trade(received=base + 1_000_000_000),
                trade_event_key="after-cap-before-wall",
                aggressor_side=Side.BUY,
            ),
            pair,
            "fixture",
        )
    )
    assert observer.eligible_trade_count == 250
    assert observer.fatal_reason is None
    await observer.close()
    store.close()


@pytest.mark.asyncio
async def test_failed_cal_preflight_stops_before_claim_or_public_selection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    report = tmp_path / "failed-cal.json"
    report.write_text(
        json.dumps(
            {
                "section": "SCAN_003_FIXED_OFFLINE_EVALUATION",
                "stage_verdict": "FIXTURE_ONLY",
                "stage_qualified": False,
            }
        ),
        encoding="utf-8",
    )
    called = False

    async def public_selection_sentinel(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("public selection must not run")

    monkeypatch.setattr(scanner_module, "select_public_market_pairs", public_selection_sentinel)
    with pytest.raises(scanner_module.ScannerPreconditionError, match="accepted public CAL-001"):
        await scanner_module.run_fixed_scanner(
            tmp_path / "runs",
            stage_name="HOLDOUT-001",
            accepted_release=RELEASE,
            window_start_utc="2026-09-02T00:00:00+00:00",
            window_end_utc="2026-09-03T00:00:00+00:00",
            cal_report=report,
        )
    assert called is False
    assert not (tmp_path / "runs" / ".scan-003").exists()


def test_cal_reference_binding_and_large_reference_paths_are_bounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cal_path = _write_records(tmp_path / "cal", _fixed_records(stage_kind="PUBLIC"))
    cal_report = build_fixed_offline_evaluation(cal_path)
    report_path = tmp_path / "cal-report.json"
    report_path.write_text(json.dumps(cal_report, default=str), encoding="utf-8")
    binding = scanner_module._validate_holdout_reference(
        report_path,
        accepted_release=RELEASE,
        holdout_window_start=SAMPLE_START + timedelta(days=1),
    )
    assert binding["run_id"] == "synthetic-scan-003"
    assert binding["selected_margin_bps"] == "2"
    assert len(binding["reference_sha256"]) == 64
    stage = scanner_module._stage_metadata(
        stage_name="HOLDOUT-001",
        run_id="holdout-run",
        accepted_release=RELEASE,
        window_start=SAMPLE_START + timedelta(days=1),
        window_end=SAMPLE_START + timedelta(days=2),
        sample_start_utc=SAMPLE_START + timedelta(days=1),
        sample_start_ns=10,
        cal_reference=binding,
    )
    assert stage["cal_reference"] == binding

    evidence_path = tmp_path / "large-evidence.jsonl"
    evidence_path.write_text("{}\n{}\n", encoding="utf-8")
    monkeypatch.setattr(scanner_module, "_REFERENCE_REPORT_MAX_BYTES", 1)
    streamed_calls: list[Path] = []

    def streamed_evaluator(path: Path):
        streamed_calls.append(Path(path))
        return {"section": "SCAN_003_FIXED_OFFLINE_EVALUATION"}

    monkeypatch.setattr(scanner_module, "build_fixed_offline_evaluation", streamed_evaluator)
    monkeypatch.setattr(Path, "read_bytes", lambda _path: (_ for _ in ()).throw(AssertionError("whole-file read")))
    report, digest = scanner_module._reference_report(evidence_path)
    assert report["section"] == "SCAN_003_FIXED_OFFLINE_EVALUATION"
    assert streamed_calls == [evidence_path]
    assert len(digest) == 64

    monkeypatch.setattr(offline_module, "_REFERENCE_REPORT_MAX_BYTES", 1)
    offline_calls: list[Path] = []

    def offline_streamed_evaluator(path: Path):
        offline_calls.append(Path(path))
        return {"section": "SCAN_003_FIXED_OFFLINE_EVALUATION"}

    monkeypatch.setattr(offline_module, "build_offline_evaluation", offline_streamed_evaluator)
    loaded, issues = offline_module._load_stage_reference(evidence_path)
    assert loaded == {"section": "SCAN_003_FIXED_OFFLINE_EVALUATION"}
    assert issues == set()
    assert offline_calls == [evidence_path]
    assert offline_module._reference_sha256(evidence_path) is not None


def _cal_report_reference(tmp_path: Path) -> tuple[Path, dict[str, object]]:
    cal_path = _write_records(tmp_path / "cal", _fixed_records(stage_kind="PUBLIC"))
    cal_report = build_fixed_offline_evaluation(cal_path)
    report_path = tmp_path / "cal-report.json"
    report_path.write_text(json.dumps(cal_report, default=str), encoding="utf-8")
    binding = scanner_module._validate_holdout_reference(
        report_path,
        accepted_release=RELEASE,
        holdout_window_start=SAMPLE_START + timedelta(days=1),
    )
    return report_path, binding


def test_holdout_reference_accepts_separated_same_selector_public_result(tmp_path: Path) -> None:
    report_path, binding = _cal_report_reference(tmp_path)
    holdout_path = _write_records(
        tmp_path / "holdout",
        _holdout_records(
            binding,
            start=SAMPLE_START + timedelta(days=1),
        ),
    )
    result = build_fixed_offline_evaluation(
        holdout_path,
        cal_reference=report_path,
    )
    assert result["gate_results"]["SCANNER_HOLDOUT_REFERENCE_VALID"]["passed"] is True
    assert result["stage_verdict"] == "PUBLIC_PAPER_PROFITABILITY_CANDIDATE"
    assert result["stage_qualified"] is True
    assert result["selector"]["selected_margin_bps"] == binding["selected_margin_bps"]


@pytest.mark.parametrize(
    "failure",
    ("reversed", "overlap", "shared_identity", "reference_hash", "selector"),
)
def test_holdout_reference_failures_are_fail_closed(
    tmp_path: Path,
    failure: str,
) -> None:
    report_path, binding = _cal_report_reference(tmp_path)
    start = SAMPLE_START + timedelta(days=1)
    shared_identities = False
    selected_margin_bps = None
    reference = report_path
    if failure == "reversed":
        start = SAMPLE_START - timedelta(days=1)
    elif failure == "overlap":
        start = SAMPLE_START + timedelta(seconds=600)
    elif failure == "shared_identity":
        shared_identities = True
    elif failure == "reference_hash":
        reference = tmp_path / "changed-cal-report.json"
        reference.write_text(report_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
    else:
        selected_margin_bps = "1"
    holdout_path = _write_records(
        tmp_path / "holdout",
        _holdout_records(
            binding,
            start=start,
            shared_identities=shared_identities,
            selected_margin_bps=selected_margin_bps,
        ),
    )
    result = build_fixed_offline_evaluation(
        holdout_path,
        cal_reference=reference,
    )
    gate_result = result["gate_results"]["SCANNER_HOLDOUT_REFERENCE_VALID"]
    assert gate_result["passed"] is False
    assert result["stage_verdict"] != "PUBLIC_PAPER_PROFITABILITY_CANDIDATE"
