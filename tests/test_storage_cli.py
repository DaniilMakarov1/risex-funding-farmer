import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

import risex_farmer.cli as cli_module
from risex_farmer.cli import _money, _reason, _scan_table, main
from risex_farmer.lifecycle import LifecycleSnapshot
from risex_farmer.models import LifecycleState, SettlementStatus, Side, TradeEvidence, Venue
from risex_farmer.orchestrator import DEFAULT_LOGICAL_AT, load_fixture, run_fixture
from risex_farmer.paper_broker import PaperEntryState
from risex_farmer.storage import PaperRepository


D = Decimal
FIXTURES = Path(__file__).parent / "fixtures" / "paper_006"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fixture", "status", "state"),
    (
        ("no_opportunity.json", "NO_TRADE", None),
        ("maker_never_fills.json", "STOPPED_WITH_OPEN_ENTRY_ORDER", "ENTRY_MAKER_OPEN"),
        ("open_position.json", "STOPPED_WITH_OPEN_POSITION", "HOLDING"),
        ("positive_closed.json", "CLOSED", None),
        ("negative_closed.json", "CLOSED", None),
        ("long_exit.json", "CLOSED", None),
        ("unresolved_closed.json", "CLOSED", None),
    ),
)
async def test_fixture_only_e2e_runs(fixture, status, state, tmp_path) -> None:
    with PaperRepository(tmp_path / f"{fixture}.db") as repository:
        result = await run_fixture(load_fixture(FIXTURES / fixture), repository)
        assert result["status"] == status
        report = repository.report(as_of=DEFAULT_LOGICAL_AT + timedelta(minutes=10))
        if state is not None:
            assert report["open_position"]["state"] == state
        if status == "CLOSED":
            assert report["fills"] == 4
            assert report["open_position"] is None
        if fixture == "positive_closed.json":
            assert report["paper_orders"] == 1
            assert report["maker_fills"] == 1
            assert report["fill_rate"] == "1"
            assert report["normal_exit_fills"] == 1
            assert report["aggressive_exit_fills"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("scenario", "expected_type", "expected_state"),
    (
        ("maker_never_fills", PaperEntryState, LifecycleState.ENTRY_MAKER_OPEN),
        ("open_position", LifecycleSnapshot, LifecycleState.HOLDING),
        ("exiting_normal_open", LifecycleSnapshot, LifecycleState.EXITING_NORMAL),
        (
            "exiting_aggressive_open",
            LifecycleSnapshot,
            LifecycleState.EXITING_AGGRESSIVE,
        ),
        ("positive_closed", LifecycleSnapshot, LifecycleState.FLAT),
    ),
)
async def test_runtime_round_trips_all_five_states(
    scenario, expected_type, expected_state, tmp_path
) -> None:
    with PaperRepository(tmp_path / f"{scenario}.db") as repository:
        await run_fixture({"scenario": scenario, "attempt_id": scenario}, repository)
        restored = repository.load_runtime()
        assert isinstance(restored, expected_type)
        assert restored.lifecycle_state is expected_state
        if isinstance(restored, LifecycleSnapshot) and expected_state is not LifecycleState.FLAT:
            assert restored.position is not None


@pytest.mark.asyncio
async def test_trade_and_settlement_idempotency_and_conflicts(tmp_path) -> None:
    with PaperRepository(tmp_path / "authority.db") as repository:
        await run_fixture({"scenario": "open_position"}, repository)
        runtime = repository.load_runtime()
        settlement = runtime.settlements[0]
        repository.upsert_settlement(settlement)
        estimated = replace(
            settlement, status=SettlementStatus.ESTIMATED, cash_usd=D("4")
        )
        repository.upsert_settlement(estimated)
        repository.upsert_settlement(estimated)
        with pytest.raises(ValueError, match="conflicting settlement authority"):
            repository.upsert_settlement(replace(estimated, cash_usd=D("5")))

        trade = TradeEvidence(
            "dedup",
            Venue.EXTENDED,
            "ABC-EXTENDED",
            DEFAULT_LOGICAL_AT,
            DEFAULT_LOGICAL_AT,
            "synthetic",
            D("1"),
            D("100"),
            Side.BUY,
            True,
        )
        repository.insert_trade_event(trade)
        repository.insert_trade_event(trade)
        original_runtime_at = repository.runtime_updated_at()
        with pytest.raises(ValueError, match="conflicting duplicate trade"):
            repository.save_decision(
                recorded_at=DEFAULT_LOGICAL_AT + timedelta(seconds=30),
                trade_events=(replace(trade, canonical_price=D("101")),),
                lifecycle_snapshot=runtime,
            )
        assert repository.runtime_updated_at() == original_runtime_at


@pytest.mark.asyncio
async def test_report_filters_primary_and_applied_metrics_and_computes_totals(
    tmp_path,
) -> None:
    specs = (
        ("positive_closed", "positive", 0),
        ("negative_closed", "negative", 300),
        ("estimated_closed", "estimated", 600),
        ("unresolved_closed", "unresolved", 900),
        ("degraded_closed", "degraded", 1200),
    )
    with PaperRepository(tmp_path / "report.db") as repository:
        for scenario, attempt, seconds in specs:
            await run_fixture(
                {
                    "scenario": scenario,
                    "attempt_id": attempt,
                    "logical_at": (DEFAULT_LOGICAL_AT + timedelta(seconds=seconds)).isoformat(),
                },
                repository,
            )
        report = repository.report(as_of=DEFAULT_LOGICAL_AT + timedelta(hours=1))

    assert report["normal_exit_fills"] == 3
    assert report["aggressive_exit_fills"] == 2
    assert report["complete_trades"] == 4
    assert report["degraded_trades"] == 1
    assert report["primary_trade_count"] == 3
    assert report["applied_trade_count"] == 2
    assert report["simulated_win_rate"] == str(D("1") / D("3"))
    assert report["applied_rate_win_rate"] == "0.5"
    assert report["simulated_closed_net_pnl_usd"] == "UNKNOWN"
    assert report["applied_rate_closed_net_pnl_usd"] == "UNKNOWN"
    assert D(report["virtual_risex_volume_usd"]) > 0
    assert D(report["pnl_per_1000_risex_volume_usd"]).is_finite()
    assert D(report["max_drawdown_usd"]) > 0
    assert D(report["planned_vs_actual_error_usd"]).is_finite()
    assert report["estimated_funding"] == 1
    assert report["unresolved_settlements"] == 1
    assert report["applied_rate_funding_partial"] == 1
    assert report["eligible_opportunities"] == report["eligible_count"]
    assert report["exit_wait_seconds"] == report["exit_duration_seconds"]
    assert report["cycles"] == 5
    assert all(
        flag in report["assumption_flags"]
        for flag in (
            "paper_only",
            "taker_failure_and_latency_not_simulated",
            "partial_fills_not_simulated",
            "queue_position_not_simulated",
            "cancel_replace_latency_not_simulated",
            "stablecoin_depeg_not_simulated",
            "live_margin_and_liquidation_not_simulated",
            "expected_basis_convergence_pnl_usd",
            "points_value_usd",
            "risex_fee_tier",
            "nado_fees",
        )
    )
    assert all(
        report["assumption_flags"][flag] is True
        for flag in (
            "paper_only",
            "taker_failure_and_latency_not_simulated",
            "partial_fills_not_simulated",
            "queue_position_not_simulated",
            "cancel_replace_latency_not_simulated",
            "stablecoin_depeg_not_simulated",
            "live_margin_and_liquidation_not_simulated",
        )
    )


@pytest.mark.asyncio
async def test_restart_keeps_open_position_and_records_offline_gap(tmp_path) -> None:
    with PaperRepository(tmp_path / "restart.db") as repository:
        await run_fixture(
            {"scenario": "open_position", "attempt_id": "restart"}, repository
        )
        recovered = DEFAULT_LOGICAL_AT + timedelta(seconds=30)
        result = await run_fixture(
            {"scenario": "restart_open", "logical_at": recovered.isoformat()},
            repository,
        )
        runtime = repository.load_runtime()
        report = repository.report(as_of=recovered)
    assert result["forced_close"] is False
    assert runtime.position is not None
    assert runtime.gap_count == 1
    assert runtime.data_quality.value == "DEGRADED"
    assert report["open_position"]["position_id"] == "restart:position"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("initial", "recovery_seconds", "expected"),
    (
        ("maker_never_fills", 5, LifecycleState.FLAT),
        ("exiting_normal_open", 5, LifecycleState.EXITING_NORMAL),
        ("exiting_aggressive_open", 30, LifecycleState.EXITING_AGGRESSIVE),
        ("positive_closed", 30, LifecycleState.FLAT),
    ),
)
async def test_persisted_restart_rules_cover_entry_exit_and_flat(
    initial, recovery_seconds, expected, tmp_path
) -> None:
    with PaperRepository(tmp_path / f"restart-{initial}.db") as repository:
        await run_fixture(
            {"scenario": initial, "attempt_id": f"restart-{initial}"}, repository
        )
        recovered = DEFAULT_LOGICAL_AT + timedelta(seconds=recovery_seconds)
        result = await run_fixture(
            {"scenario": "restart_open", "logical_at": recovered.isoformat()},
            repository,
        )
        runtime = repository.load_runtime()
    assert result["state"] == expected.value
    assert runtime.lifecycle_state is expected
    if expected in {LifecycleState.EXITING_NORMAL, LifecycleState.EXITING_AGGRESSIVE}:
        assert runtime.position is not None
        assert runtime.exit_order.active_version is not None


def test_cli_commands_are_structured_and_network_free(tmp_path, capsys) -> None:
    database = tmp_path / "cli.db"
    paper_database = tmp_path / "cli-paper.db"
    assert main(
        [
            "--db",
            str(database),
            "scan-once",
            "--fixture",
            str(FIXTURES / "no_opportunity.json"),
        ]
    ) == 0
    no_live = json.loads(capsys.readouterr().out)
    assert no_live["status"] == "NO_TRADE"

    assert main(
        [
            "--db", str(database), "scan-once", "--format", "table",
            "--fixture", str(FIXTURES / "no_opportunity.json"),
        ]
    ) == 0
    table = capsys.readouterr().out
    assert table.startswith("RISEx Funding Scanner\n")
    assert "Scan UTC:" in table and "NO TRADE" in table
    assert "Readiness: RISEx: UNKNOWN | Extended: UNKNOWN | Nado: UNKNOWN" in table

    assert main(
        [
            "--db", str(database), "scan-once", "--format", "json",
            "--fixture", str(FIXTURES / "no_opportunity.json"),
        ]
    ) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "NO_TRADE"

    assert main(
        [
            "--db",
            str(paper_database),
            "paper-run",
            "--fixture",
            str(FIXTURES / "positive_closed.json"),
        ]
    ) == 0
    run = json.loads(capsys.readouterr().out)
    assert run["status"] == "CLOSED"

    assert main(["--db", str(paper_database), "report"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["fills"] == 4
    assert report["assumption_flags"]["paper_only"] is True


def test_default_and_explicit_json_preserve_route_semantics(
    tmp_path, capsys, monkeypatch
) -> None:
    result = {
        "scan_at": "2027-01-01T10:58:59+00:00",
        "status": "OPPORTUNITY",
        "eligible_count": 1,
        "winner": "BTC",
        "venue_readiness": {"RISEX": {"available": True}},
        "routes": [{
            "rank": 1,
            "canonical_asset": "BTC",
            "direction": "LONG_RISEX_SHORT_HEDGE",
            "hedge_venue": "EXTENDED",
            "canonical_quantity": "0.00695",
            "planned_entry_fees_usd": "0.10",
            "planned_exit_fees_usd": "0.11",
            "planned_maker_net_pnl_usd": "1.23456789",
            "entry_allowed": True,
            "blockers": [],
            "risex_contract_assumption_used": True,
        }],
    }

    async def fake_scan_once(repository, fixture):
        return result

    monkeypatch.setattr(cli_module, "_scan_once", fake_scan_once)
    database = tmp_path / "json-equivalence.db"
    assert main(["--db", str(database), "scan-once"]) == 0
    default = json.loads(capsys.readouterr().out)
    assert main(["--db", str(database), "scan-once", "--format", "json"]) == 0
    explicit = json.loads(capsys.readouterr().out)
    assert default == explicit == result
    assert default["routes"][0]["risex_contract_assumption_used"] is True


def test_human_scan_table_exact_columns_labels_precision_and_narrow_width() -> None:
    base = {
        "rank": 1, "canonical_asset": "BTC",
        "direction": "LONG_RISEX_SHORT_HEDGE", "hedge_venue": "EXTENDED",
        "canonical_quantity": "0.006950000", "target_cycle_start": "2027-01-01T12:00:00+00:00",
        "seconds_to_earliest_funding": "3661", "risex_funding_usd": "0.00001",
        "hedge_funding_usd": "-0.123456", "net_funding_usd": "0.123466",
        "planned_entry_fees_usd": "0.10", "planned_exit_fees_usd": "0.11",
        "entry_execution_pnl_usd": "-0.2", "exit_execution_pnl_usd": "0.3",
        "planned_maker_net_pnl_usd": "0.013", "entry_allowed": True, "blockers": [],
    }
    blocked = dict(
        base, rank=2, canonical_asset="ETH", direction="SHORT_RISEX_LONG_HEDGE",
        hedge_venue="NADO", entry_allowed=False,
        blockers=["FUNDING_STALE", "INSUFFICIENT_EXACT_DEPTH", "SOMETHING_ODD"],
        canonical_quantity=None, planned_maker_net_pnl_usd=None,
    )
    output = {
        "scan_at": "2027-01-01T10:58:59+00:00", "status": "OPPORTUNITY",
        "eligible_count": 1, "routes": [base, blocked],
        "venue_readiness": {
            "RISEX": {"available": True}, "EXTENDED": {"available": True},
            "NADO": {"available": False},
        },
    }
    wide = _scan_table(output, width=1000)
    expected_headers = (
        "Rank", "Asset", "Route", "Hedge", "Qty", "Funding At / T-",
        "RISEx Funding $", "Hedge Funding $", "Net Funding $", "Entry Fee $",
        "Exit Fee $", "Entry Execution PnL $", "Exit Execution PnL $",
        "Expected Net PnL $", "Status",
    )
    assert all(header in wide for header in expected_headers)
    assert wide.startswith("RISEx Funding Scanner\n")
    assert "Scan UTC: 2027-01-01T10:58:59+00:00 | OPPORTUNITY | Eligible: 1" in wide
    assert "Nearest funding UTC: 2027-01-01T12:00:00+00:00 / T-01:01:01" in wide
    assert "RISEx: READY | Extended: READY | Nado: UNAVAILABLE" in wide
    assert "R↑ / H↓" in wide and "R↓ / H↑" in wide
    assert "TRADE" in wide
    assert "NO TRADE — STALE DATA" in wide
    assert _reason({"blockers": ["INSUFFICIENT_EXACT_DEPTH"]}) == "INSUFFICIENT DEPTH"
    assert _reason({"blockers": ["FUNDING_UNKNOWN"]}) == "FUNDING UNKNOWN"
    assert _reason({"blockers": ["SOMETHING_ODD"]}) == "SOMETHING ODD"
    assert "Winner" not in wide and "Price" not in wide and "Diagnostics" not in wide
    assert _money("0.00001") == "0.00001"
    assert _money("-0.002344") == "-0.002344"
    assert _money(None) == "—"
    narrow = _scan_table(output, width=72)
    narrow_lines = narrow.splitlines()
    header_index = next(
        index for index, line in enumerate(narrow_lines) if line.startswith("Asset ")
    )
    assert [part.strip() for part in narrow_lines[header_index].split("|")] == [
        "Asset", "Route", "Hedge", "T-", "Net Funding", "Entry Fee",
        "Exit Fee", "Entry Exec", "Exit Exec", "NET PnL", "Status",
    ]
    assert narrow_lines[header_index + 1].count("-+-") == 10
    assert "BTC" in narrow_lines[header_index + 2]
    assert "ETH" in narrow_lines[header_index + 3]
    assert "Rank" not in narrow_lines[header_index]
    assert "-- Route" not in narrow
    missing_readiness = _scan_table(
        dict(output, venue_readiness={"RISEX": {"available": True}}), width=1000
    )
    assert "RISEx: READY | Extended: UNKNOWN | Nado: UNKNOWN" in missing_readiness
    fifteen = [dict(base, rank=index, canonical_asset=f"A{index:02d}") for index in range(1, 16)]
    all_rows = _scan_table(dict(output, routes=fifteen), width=72)
    all_lines = all_rows.splitlines()
    all_header = next(
        index for index, line in enumerate(all_lines) if line.startswith("Asset ")
    )
    assert len(all_lines[all_header + 2:]) == 15
    assert "A01" in all_lines[all_header + 2]
    assert "A15" in all_lines[all_header + 16]
