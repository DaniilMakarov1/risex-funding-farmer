from __future__ import annotations

from datetime import datetime, timezone

from risex_spread_shadow.cycle import CycleTerminalState
from risex_spread_shadow.s3_cycle import (
    CycleWindow,
    build_cycle_report,
    run_fixture_window,
)
from risex_spread_shadow.store import iter_records


UTC = timezone.utc
ACCEPTED_RELEASE = "62cee8d1185b09904ed747fa1e775392e46e9520"


def _window(window_id: str) -> CycleWindow:
    return CycleWindow(
        campaign_id="s3-prefix-campaign",
        window_id=window_id,
        start_utc=datetime(2026, 1, 1, tzinfo=UTC),
        end_utc=datetime(2026, 1, 1, 0, 45, tzinfo=UTC),
        ordinal=0,
        monotonic_start_ns=0,
    )


def test_s3_fixture_prefix_persists_and_replays_normal_and_negative_cycles(tmp_path) -> None:
    output = run_fixture_window(
        tmp_path,
        accepted_release=ACCEPTED_RELEASE,
        window=_window("window-normal-negative"),
        fixture_profile="mixed",
        count=2,
        claim=False,
    )

    records = list(iter_records(output.store_path))
    assert records[0]["kind"] == "RUN_METADATA"
    assert records[-1]["kind"] == "RUN_STOP"
    assert [record["record_index"] for record in records] == list(range(len(records)))

    report = build_cycle_report(output.store_path)
    assert report["measurement_validity"] == "VALID"
    assert report["evidence_sufficiency"] == "INSUFFICIENT"
    primary = report["economics"]["primary"]
    stress = report["economics"]["stress"]
    assert primary["complete_cycle_count"] == 2
    assert primary["normal_count"] == 2
    assert primary["total_pnl_usd"] == "1.960200"
    assert primary["gross_profit_usd"] == "1.980100"
    assert primary["gross_loss_usd"] == "-0.019900"
    assert stress["complete_cycle_count"] == 2
    assert stress["total_pnl_usd"] == "1.920400"
    assert all(result.is_flat for result in output.results)


def test_s3_fixture_prefix_replays_late_terminal_conflict_in_physical_order(tmp_path) -> None:
    output = run_fixture_window(
        tmp_path,
        accepted_release=ACCEPTED_RELEASE,
        window=_window("window-terminal-conflict"),
        fixture_profile="terminal_conflict",
        count=2,
        claim=False,
    )

    records = list(iter_records(output.store_path))
    late_inputs = [
        record
        for record in records
        if record["kind"] == "CYCLE_INPUT" and record["attempt_index"] == 1
    ]
    assert late_inputs[0]["input_index"] == 0
    assert late_inputs[0]["input"]["event"]["payload"]["canonical_quantity"] == "0.50"

    report = build_cycle_report(output.store_path)
    assert report["measurement_validity"] == "VALID"
    assert report["economics"]["primary"]["complete_cycle_count"] == 0
    assert report["economics"]["primary"]["unresolved_count"] == 2
    assert all(result.status is CycleTerminalState.UNRESOLVED for result in output.results)
    assert all(not result.is_flat for result in output.results)
