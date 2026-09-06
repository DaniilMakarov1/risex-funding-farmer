from __future__ import annotations

from decimal import Decimal
import json
from pathlib import Path

from risex_farmer.models import LiquidityRole, Side, Venue
from risex_spread_shadow.causal import CausalEvent
from risex_spread_shadow.cycle import CycleScenario, CycleTerminalState
from risex_spread_shadow.s3_cycle import (
    build_cycle_report,
    fixture_cycle_attempts,
    fixture_campaign_windows,
    render_cycle_report,
    run_fixture_window,
)
from risex_spread_shadow.store import iter_records


ACCEPTED_RELEASE = "a" * 40
ZERO = Decimal("0")


def _assert_decimal_value(actual: object, expected: Decimal | None) -> None:
    if expected is None:
        assert actual is None
    else:
        assert isinstance(actual, str)
        assert Decimal(actual) == expected


def _independent_totals(result, attempt, profile: str, scenario: CycleScenario) -> dict[str, Decimal | int | None]:
    """Recompute report arithmetic from serialized fill facts only.

    This intentionally does not call CycleLedger properties, CycleResult
    aggregate properties, or any S3 report helper.  It is a fixture checker
    for the exact signed quantities, cash-flow signs, fees, turnover, and
    forced-exit subtotal.
    """

    fills = tuple(result.fills)
    entry_quantity = sum(
        (fill.quantity for fill in fills if fill.action_id == "entry-maker"),
        ZERO,
    )
    hedged_quantity = sum(
        (fill.quantity for fill in fills if fill.action_id == "entry-hedge"),
        ZERO,
    )
    signed_cashflow = ZERO
    fees = ZERO
    scenario_cost = ZERO
    turnover = ZERO
    forced_exit_cashflow = ZERO
    risex_maker_fee = Decimal("0.0001")
    risex_taker_fee = Decimal("0.0003")
    lighter_taker_fee = ZERO
    stress_risex_fill_cost = Decimal("0.0001")
    for fill in fills:
        notional = fill.quantity * fill.price
        gross = notional if fill.side is Side.SELL else -notional
        if fill.venue is Venue.RISEX and fill.liquidity_role is LiquidityRole.MAKER:
            fee_rate = risex_maker_fee
        elif fill.venue is Venue.RISEX and fill.liquidity_role is LiquidityRole.TAKER:
            fee_rate = risex_taker_fee
        elif fill.venue is Venue.LIGHTER and fill.liquidity_role is LiquidityRole.TAKER:
            fee_rate = lighter_taker_fee
        else:
            raise AssertionError(f"unexpected fixture fill role: {fill.venue}/{fill.liquidity_role}")
        fee = notional * fee_rate
        modeled_cost = (
            notional * stress_risex_fill_cost
            if scenario is CycleScenario.STRESS and fill.venue is Venue.RISEX
            else ZERO
        )
        signed_cashflow += gross
        fees += fee
        scenario_cost += modeled_cost
        turnover += notional
        if fill.action_id in {"unmatched-risex", "forced-risex", "forced-lighter"}:
            forced_exit_cashflow += gross - fee - modeled_cost
    trade_times = [
        event.causal_monotonic_ns
        for event in attempt.events
        if isinstance(event, CausalEvent) and event.trade is not None
    ]
    if not trade_times:
        raise AssertionError("fixture attempt is missing its entry trade")
    entry_fill_ns = trade_times[0]
    cancel_delay_ns = 500_000_000 if scenario is CycleScenario.PRIMARY else 1_000_000_000
    taker_delay_ns = cancel_delay_ns
    if profile in {"normal", "negative"}:
        if len(trade_times) != 2:
            raise AssertionError("normal/negative fixture must contain entry and exit trades")
        terminal_ns = trade_times[1] + taker_delay_ns
        unmatched_duration_ns = 0
    elif profile == "forced":
        if len(trade_times) != 2:
            raise AssertionError("forced fixture must contain entry and partial-exit trades")
        terminal_ns = trade_times[1] + cancel_delay_ns + taker_delay_ns
        unmatched_duration_ns = 0
    elif profile == "unresolved":
        terminal_ns = entry_fill_ns + cancel_delay_ns + taker_delay_ns * (2 if scenario is CycleScenario.STRESS else 1)
        unmatched_duration_ns = taker_delay_ns if scenario is CycleScenario.STRESS else 0
    else:
        raise AssertionError(profile)
    unmatched_quantity: Decimal | None
    if profile == "unresolved":
        # The primary lane reaches an unresolved hedge before the unmatched
        # quantity is authoritative.  The stress lane observes zero hedge
        # depth, but the aggregate report intentionally keeps unresolved
        # quantities unknown rather than reporting a misleading zero.
        unmatched_quantity = None
    else:
        unmatched_quantity = entry_quantity - hedged_quantity
    return {
        "entry_quantity": entry_quantity,
        "hedged_quantity": hedged_quantity,
        "unmatched_quantity": unmatched_quantity,
        "signed_cashflow": signed_cashflow,
        "fees": fees,
        "scenario_cost": scenario_cost,
        "net_cashflow": signed_cashflow - fees - scenario_cost,
        "turnover": turnover,
        "forced_exit_cashflow": forced_exit_cashflow,
        "fill_count": len(fills),
        "holding_ns": terminal_ns - entry_fill_ns,
        "unmatched_duration_ns": unmatched_duration_ns,
    }


def _mark_synthetic_observed(path: Path, *, failed: bool = False) -> None:
    """Shape temporary fixture evidence like a public run for report gates.

    This is test-only fault injection.  No fixture output is used as public
    campaign evidence; the normal fixture report remains FIXTURE_ONLY.
    """

    records = list(iter_records(path))
    metadata = records[0]["metadata"]
    metadata["evidence_mode"] = "OBSERVATIONAL"
    metadata["prospective"] = True
    metadata["manifest_sha256"] = "0" * 64
    metadata["manifest_window_fingerprint"] = metadata["window_fingerprint"]
    if failed:
        records[-1]["kind"] = "RUN_FAILED"
        records[-1]["fatal_reason"] = "OFFLINE_FAULT_INJECTION"
    path.write_text(
        "".join(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n" for record in records),
        encoding="utf-8",
    )


def _synthetic_observed_campaign(
    root: Path,
    *,
    profiles: tuple[str, str, str, str],
    count: int,
    window_counts: tuple[int, int, int, int] | None = None,
    failed_index: int | None = None,
) -> dict[str, object]:
    windows = fixture_campaign_windows(campaign_id=f"synthetic-{root.name}")
    counts = (count, count, count, count) if window_counts is None else window_counts
    assert len(profiles) == len(windows) == len(counts)
    for index, (window, profile, window_count) in enumerate(zip(windows, profiles, counts)):
        output = run_fixture_window(
            root,
            accepted_release=ACCEPTED_RELEASE,
            window=window,
            fixture_profile=profile,
            count=window_count,
            claim=False,
        )
        _mark_synthetic_observed(output.store_path, failed=index == failed_index)
    return build_cycle_report(root)


def test_report_replays_normal_negative_forced_and_unresolved_with_independent_decimal_arithmetic(
    tmp_path: Path,
) -> None:
    profiles = ("normal", "negative", "forced", "unresolved")
    for profile in profiles:
        root = tmp_path / profile
        output = run_fixture_window(
            root,
            accepted_release=ACCEPTED_RELEASE,
            window=fixture_campaign_windows(campaign_id=f"numeric-{profile}")[0],
            fixture_profile=profile,
            count=1,
            claim=False,
        )
        attempt = fixture_cycle_attempts(
            fixture_campaign_windows(campaign_id=f"numeric-{profile}")[0],
            count=1,
            profile=profile,
        )[0]
        records = list(iter_records(output.store_path))
        assert records[-1]["kind"] == "RUN_STOP"
        assert [record["record_index"] for record in records] == list(range(len(records)))
        report = build_cycle_report(output.store_path)
        summary = report["windows"][0]
        assert summary["persisted_final_result_count"] == summary["replayed_final_result_count"]
        assert report["provenance"]["fixture_only"] is True
        assert report["usefulness"]["label"] == "FIXTURE_ONLY"

        for scenario in CycleScenario:
            result = next(item for item in output.results if item.scenario is scenario)
            expected = _independent_totals(result, attempt, profile, scenario)
            actual = summary[scenario.value.lower()]
            _assert_decimal_value(actual["entry_quantity_total"], expected["entry_quantity"])
            _assert_decimal_value(actual["hedged_quantity_total"], expected["hedged_quantity"])
            _assert_decimal_value(actual["unmatched_entry_quantity_total"], expected["unmatched_quantity"])
            _assert_decimal_value(actual["signed_cashflow_usd"], expected["signed_cashflow"])
            _assert_decimal_value(actual["total_fees_usd"], expected["fees"])
            _assert_decimal_value(actual["scenario_cost_usd"], expected["scenario_cost"])
            _assert_decimal_value(actual["net_cashflow_usd"], expected["net_cashflow"])
            _assert_decimal_value(actual["turnover_usd"], expected["turnover"])
            assert actual["fill_count"] == expected["fill_count"]
            assert actual["fee_count"] == expected["fill_count"]
            assert actual["cashflow_count"] == expected["fill_count"]
            _assert_decimal_value(
                actual["holding_duration_seconds"],
                Decimal(expected["holding_ns"]) / Decimal(1_000_000_000),
            )
            _assert_decimal_value(
                actual["occupancy_holding_duration_seconds"],
                Decimal(expected["holding_ns"]) / Decimal(1_000_000_000),
            )
            _assert_decimal_value(
                actual["unmatched_exposure_duration_seconds"],
                Decimal(expected["unmatched_duration_ns"]) / Decimal(1_000_000_000),
            )
            assert actual["duration_semantics"] == {
                "holding_duration_seconds": "first_maker_fill_to_terminal_boundary",
                "occupancy_holding_duration_seconds": "same_interval_as_holding_duration",
                "unmatched_exposure_duration_seconds": (
                    "unmatched_start_to_resolution_or_unresolved_observation_boundary;"
                    " not_full_exposure_duration"
                ),
            }

            if profile == "forced":
                assert result.status is CycleTerminalState.FORCED
                assert result.is_flat
                assert actual["total_pnl_usd"] == str(expected["net_cashflow"])
                assert actual["forced_or_unmatched_full_cycle_pnl_usd"] == str(expected["net_cashflow"])
                assert actual["forced_unmatched_exit_contribution_usd"] == str(expected["forced_exit_cashflow"])
                assert actual["forced_unmatched_exit_cashflow_usd"] == str(expected["forced_exit_cashflow"])
                assert actual["forced_unmatched_exit_contribution_kind"] == "NET_CASHFLOW_SUBTOTAL"
                assert actual["forced_unmatched_exit_contribution_usd"] == (
                    "-3.618900" if scenario is CycleScenario.PRIMARY else "-3.625200"
                )
                assert actual["total_pnl_usd"] != actual["forced_unmatched_exit_cashflow_usd"]
            if profile in {"normal", "negative", "forced"}:
                _assert_decimal_value(actual["total_pnl_usd"], expected["net_cashflow"])
                _assert_decimal_value(actual["mean_pnl_usd"], expected["net_cashflow"])
                _assert_decimal_value(
                    actual["gross_profit_usd"],
                    expected["net_cashflow"] if expected["net_cashflow"] > ZERO else ZERO,
                )
                _assert_decimal_value(
                    actual["gross_loss_usd"],
                    expected["net_cashflow"] if expected["net_cashflow"] < ZERO else ZERO,
                )
                assert actual["negative_cycle_count"] == int(expected["net_cashflow"] < ZERO)
            if profile == "unresolved":
                assert result.status is CycleTerminalState.UNRESOLVED
                assert not result.is_flat
                assert actual["total_pnl_usd"] is None
                assert actual["mean_pnl_usd"] is None

    forced_primary = build_cycle_report(tmp_path / "forced")["economics"]["primary"]
    forced_stress = build_cycle_report(tmp_path / "forced")["economics"]["stress"]
    assert forced_primary["total_pnl_usd"] == "-2.232920"
    assert forced_primary["forced_unmatched_exit_cashflow_usd"] == "-3.618900"
    assert forced_stress["total_pnl_usd"] == "-2.253240"
    assert forced_stress["forced_unmatched_exit_cashflow_usd"] == "-3.625200"
    assert forced_primary["total_pnl_usd"] != forced_stress["total_pnl_usd"]

    table = render_cycle_report(tmp_path / "forced", format="table")
    assert "aborted=0" in table
    assert "mean_pnl=-2.232920" in table
    assert "gross_profit=0" in table
    assert "gross_loss=-2.232920" in table
    assert "worst_cycle={'quote_version_id': 'window-1-cycle-0', 'pnl_usd': '-2.232920'}" in table
    assert "forced_full_cycle_pnl=-2.232920 forced_exit_cashflow=-3.618900" in table
    assert "holding_seconds=4.5 occupancy_seconds=4.5 unmatched_duration_seconds=0" in table

    unresolved = build_cycle_report(tmp_path / "unresolved")["economics"]
    assert unresolved["primary"]["total_pnl_usd"] is None
    assert unresolved["stress"]["total_pnl_usd"] is None
    assert unresolved["primary"]["unmatched_entry_quantity_total"] is None
    assert unresolved["stress"]["unmatched_entry_quantity_total"] is None
    assert unresolved["stress"]["unmatched_exposure_duration_seconds"] == "1"
    assert unresolved["stress"]["forced_or_unmatched_full_cycle_pnl_usd"] is None


def test_four_window_two_day_robustness_pass_has_explicit_machine_boundaries(tmp_path: Path) -> None:
    report = _synthetic_observed_campaign(
        tmp_path / "robust-pass",
        profiles=("normal", "normal", "normal", "normal"),
        count=7,
    )

    assert report["campaign_complete"] is True
    assert report["measurement_validity"] == "VALID"
    assert report["evidence_sufficiency"] == "SUFFICIENT"
    assert report["robustness"] == {
        "campaign_shape_pass": True,
        "floors_pass": True,
        "primary_positive_each_day": True,
        "aggregate_stress_positive": True,
        "primary_without_best_dependence_group_positive": True,
        "pass": True,
        "screen_eligible": True,
    }
    assert report["usefulness"]["label"] == "DESCRIPTIVE_CAMPAIGN_SCREEN_PASS"
    assert report["usefulness"]["campaign_qualification"] == "QUALIFIED_DESCRIPTIVE_ONLY"
    assert report["economics"]["primary"]["complete_cycle_count"] == 28
    assert report["economics"]["primary"]["filled_entry_dependence_group_count"] == 28
    assert report["economics"]["primary"]["total_pnl_usd"] == "55.442800"
    assert report["economics"]["stress"]["total_pnl_usd"] == "54.885600"
    assert report["economics"]["primary"]["total_without_best_dependence_group_usd"] == "53.462700"
    assert report["economics"]["primary"]["total_fees_usd"] == "0.557200"
    assert report["economics"]["primary"]["signed_cashflow_usd"] == "56.00"
    assert report["economics"]["primary"]["turnover_usd"] == "11144.00"
    assert report["economics"]["primary"]["holding_duration_seconds"] == "112"
    assert report["economics"]["stress"]["holding_duration_seconds"] == "126"


def test_four_window_floors_and_day_economics_fail_distinctly(tmp_path: Path) -> None:
    floor_report = _synthetic_observed_campaign(
        tmp_path / "floor-fail",
        profiles=("normal", "normal", "normal", "normal"),
        count=4,
    )
    assert floor_report["campaign_complete"] is True
    assert floor_report["measurement_validity"] == "VALID"
    assert floor_report["evidence_sufficiency"] == "INSUFFICIENT"
    assert floor_report["robustness"]["campaign_shape_pass"] is True
    assert floor_report["robustness"]["floors_pass"] is False
    assert floor_report["robustness"]["pass"] is False
    assert floor_report["usefulness"]["label"] == "INCOMPLETE_CAMPAIGN"
    assert floor_report["usefulness"]["campaign_qualification"] == "INSUFFICIENT"

    day_report = _synthetic_observed_campaign(
        tmp_path / "day-fail",
        profiles=("negative", "negative", "normal", "normal"),
        count=5,
    )
    assert day_report["campaign_complete"] is True
    assert day_report["evidence_sufficiency"] == "SUFFICIENT"
    assert day_report["robustness"]["floors_pass"] is True
    assert day_report["robustness"]["primary_positive_each_day"] is False
    assert day_report["robustness"]["aggregate_stress_positive"] is True
    assert day_report["robustness"]["primary_without_best_dependence_group_positive"] is True
    assert day_report["robustness"]["pass"] is False
    assert day_report["usefulness"]["label"] == "DESCRIPTIVE_CAMPAIGN_SCREEN_FAIL"
    assert day_report["usefulness"]["campaign_qualification"] == "INSUFFICIENT"


def test_persisted_failed_or_unresolved_four_window_campaign_never_screen_passes(tmp_path: Path) -> None:
    report = _synthetic_observed_campaign(
        tmp_path / "fault-injection",
        profiles=("normal", "normal", "normal", "normal"),
        count=7,
        failed_index=3,
    )
    assert report["window_count"] == 4
    assert report["campaign_complete"] is True
    assert report["measurement_validity"] == "DATA_INSUFFICIENT"
    assert report["evidence_sufficiency"] == "INSUFFICIENT"
    assert report["robustness"]["floors_pass"] is True
    assert report["robustness"]["primary_positive_each_day"] is True
    assert report["robustness"]["aggregate_stress_positive"] is True
    assert report["robustness"]["primary_without_best_dependence_group_positive"] is True
    assert report["robustness"]["pass"] is False
    assert report["usefulness"]["label"] == "INCOMPLETE_CAMPAIGN"
    assert report["usefulness"]["campaign_qualification"] == "INSUFFICIENT"


def test_normal_fixture_provenance_cannot_become_campaign_qualification(tmp_path: Path) -> None:
    report = build_cycle_report(
        run_fixture_window(
            tmp_path,
            accepted_release=ACCEPTED_RELEASE,
            window=fixture_campaign_windows(campaign_id="fixture-provenance")[0],
            fixture_profile="normal",
            count=20,
            claim=False,
        ).store_path
    )
    assert report["provenance"]["fixture_only"] is True
    assert report["provenance"]["observed_public"] is False
    assert report["campaign_complete"] is False
    assert report["usefulness"]["label"] == "FIXTURE_ONLY"
    assert report["usefulness"]["campaign_qualification"] == "FIXTURE_ONLY"


def test_twenty_complete_cycles_with_nineteen_filled_groups_fail_only_group_floor(
    tmp_path: Path,
) -> None:
    report = _synthetic_observed_campaign(
        tmp_path / "group-floor-boundary",
        profiles=("duplicate_group", "normal", "normal", "normal"),
        count=7,
        window_counts=(7, 7, 5, 1),
    )

    primary = report["economics"]["primary"]
    assert report["campaign_complete"] is True
    assert report["measurement_validity"] == "VALID"
    assert report["evidence_sufficiency"] == "INSUFFICIENT"
    assert primary["complete_cycle_count"] == 20
    assert primary["filled_entry_dependence_group_count"] == 19
    assert sum(
        window["primary"]["complete_cycle_count"] >= 5
        for window in report["windows"]
    ) == 3
    assert report["robustness"] == {
        "campaign_shape_pass": True,
        "floors_pass": False,
        "primary_positive_each_day": True,
        "aggregate_stress_positive": True,
        "primary_without_best_dependence_group_positive": True,
        "pass": False,
        "screen_eligible": False,
    }
    assert report["usefulness"]["label"] == "INCOMPLETE_CAMPAIGN"


def test_twenty_two_cycles_in_only_two_qualifying_windows_fail_window_floor(
    tmp_path: Path,
) -> None:
    report = _synthetic_observed_campaign(
        tmp_path / "window-floor-boundary",
        profiles=("normal", "normal", "normal", "normal"),
        count=7,
        window_counts=(10, 1, 10, 1),
    )

    primary = report["economics"]["primary"]
    assert report["campaign_complete"] is True
    assert report["measurement_validity"] == "VALID"
    assert report["evidence_sufficiency"] == "INSUFFICIENT"
    assert primary["complete_cycle_count"] == 22
    assert primary["filled_entry_dependence_group_count"] == 22
    assert sum(
        window["primary"]["complete_cycle_count"] >= 5
        for window in report["windows"]
    ) == 2
    assert len(
        {
            window["day"]
            for window in report["windows"]
            if window["primary"]["complete_cycle_count"] >= 5
        }
    ) == 2
    assert report["robustness"]["campaign_shape_pass"] is True
    assert report["robustness"]["floors_pass"] is False
    assert report["robustness"]["primary_positive_each_day"] is True
    assert report["robustness"]["aggregate_stress_positive"] is True
    assert report["robustness"]["primary_without_best_dependence_group_positive"] is True
    assert report["robustness"]["pass"] is False


def test_nonpositive_aggregate_stress_blocks_screen_when_other_economics_pass(
    tmp_path: Path,
) -> None:
    report = _synthetic_observed_campaign(
        tmp_path / "stress-boundary",
        profiles=("stress_boundary",) * 4,
        count=7,
    )

    close_bid = Decimal("97.03")
    expected_primary_per_cycle = (
        Decimal("100.989900") - Decimal("100") - Decimal("98.009800") + close_bid
    )
    expected_stress_per_cycle = (
        Decimal("100.979800") - Decimal("100") - Decimal("98.019600") + close_bid
    )
    primary = report["economics"]["primary"]
    stress = report["economics"]["stress"]
    assert Decimal(primary["total_pnl_usd"]) == expected_primary_per_cycle * 28
    assert Decimal(stress["total_pnl_usd"]) == expected_stress_per_cycle * 28
    assert expected_primary_per_cycle > ZERO
    assert expected_stress_per_cycle < ZERO
    assert report["evidence_sufficiency"] == "SUFFICIENT"
    assert report["robustness"] == {
        "campaign_shape_pass": True,
        "floors_pass": True,
        "primary_positive_each_day": True,
        "aggregate_stress_positive": False,
        "primary_without_best_dependence_group_positive": True,
        "pass": False,
        "screen_eligible": True,
    }
    assert report["usefulness"]["label"] == "DESCRIPTIVE_CAMPAIGN_SCREEN_FAIL"


def test_nonpositive_without_best_group_blocks_screen_when_other_economics_pass(
    tmp_path: Path,
) -> None:
    report = _synthetic_observed_campaign(
        tmp_path / "without-best-boundary",
        profiles=("without_best_boundary",) * 4,
        count=7,
        window_counts=(6, 6, 6, 5),
    )

    normal_primary = (
        Decimal("100.989900") - Decimal("100") - Decimal("98.009800") + Decimal("99")
    )
    negative_primary = (
        Decimal("100.989900") - Decimal("100") - Decimal("98.009800") + Decimal("97")
    )
    normal_stress = (
        Decimal("100.979800") - Decimal("100") - Decimal("98.019600") + Decimal("99")
    )
    negative_stress = (
        Decimal("100.979800") - Decimal("100") - Decimal("98.019600") + Decimal("97")
    )
    expected_primary = normal_primary * 4 + negative_primary * 19
    expected_stress = normal_stress * 4 + negative_stress * 19
    primary = report["economics"]["primary"]
    stress = report["economics"]["stress"]
    assert primary["complete_cycle_count"] == 23
    assert primary["filled_entry_dependence_group_count"] == 20
    assert Decimal(primary["total_pnl_usd"]) == expected_primary
    assert Decimal(stress["total_pnl_usd"]) == expected_stress
    assert Decimal(primary["total_without_best_dependence_group_usd"]) == negative_primary * 19
    assert expected_primary > ZERO
    assert expected_stress > ZERO
    assert negative_primary * 19 <= ZERO
    assert report["evidence_sufficiency"] == "SUFFICIENT"
    assert report["robustness"] == {
        "campaign_shape_pass": True,
        "floors_pass": True,
        "primary_positive_each_day": True,
        "aggregate_stress_positive": True,
        "primary_without_best_dependence_group_positive": False,
        "pass": False,
        "screen_eligible": True,
    }
    assert report["usefulness"]["label"] == "DESCRIPTIVE_CAMPAIGN_SCREEN_FAIL"


def test_unresolved_fourth_window_blocks_positive_three_window_prefix_screen(
    tmp_path: Path,
) -> None:
    report = _synthetic_observed_campaign(
        tmp_path / "unresolved-fourth-window",
        profiles=("normal", "normal", "normal", "unresolved"),
        count=7,
        window_counts=(7, 7, 7, 1),
    )

    primary = report["economics"]["primary"]
    stress = report["economics"]["stress"]
    assert report["campaign_complete"] is True
    assert report["measurement_validity"] == "VALID"
    assert report["evidence_sufficiency"] == "INSUFFICIENT"
    assert primary["complete_cycle_count"] == 21
    assert primary["filled_entry_dependence_group_count"] == 21
    assert primary["unresolved_count"] == 1
    assert stress["unresolved_count"] == 1
    assert sum(
        window["primary"]["complete_cycle_count"] >= 5
        for window in report["windows"]
    ) == 3
    assert report["robustness"] == {
        "campaign_shape_pass": True,
        "floors_pass": True,
        "primary_positive_each_day": True,
        "aggregate_stress_positive": True,
        "primary_without_best_dependence_group_positive": True,
        "pass": False,
        "screen_eligible": False,
    }
    assert report["usefulness"]["label"] == "INCOMPLETE_CAMPAIGN"
    assert report["usefulness"]["campaign_qualification"] == "INSUFFICIENT"
