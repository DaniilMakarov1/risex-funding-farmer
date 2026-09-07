from __future__ import annotations

from dataclasses import replace
from decimal import Decimal as D
import json

import pytest

from risex_farmer.models import Venue

from risex_spread_shadow import (
    CycleFillModel,
    CycleClock,
    CycleTerminalState,
    Scv1S1bKernel,
    build_scv1_s1b_d1_report,
    Side,
    run_scv1_s1b,
    run_scv1_s1b_alternatives,
)
from risex_spread_shadow.cycle import CycleScenario

from tests.spread_shadow.test_cycle import _book, _market, _trade, _version
from tests.spread_shadow.test_scv1_s1a import _custom_quantity_version
from risex_spread_shadow.s3_cycle import _input_to_dict, _quote_version_to_dict


def _activation_book() -> object:
    return _book(
        Venue.RISEX,
        received=400_000_000,
        revision=2,
        bids=(("99", "10"),),
        asks=(("102", "10"),),
    )


def _entry_trade(key: str, received: int, quantity: str, *, price: str = "101"):
    return _trade(key, received=received, quantity=quantity, price=price)


def _fresh_version(version_id: str, decision: int, revision: int):
    version, _ = _version(version_id, decision_ready=decision)
    risex = _book(
        Venue.RISEX,
        received=decision,
        revision=revision,
        bids=(("99", "10"),),
        asks=(("102", "10"),),
    )
    lighter = _book(
        Venue.LIGHTER,
        received=decision,
        revision=revision,
        bids=(("99", "10"),),
        asks=(("100", "10"),),
    )
    version = replace(
        version,
        risex_book_revision=risex.book_revision,
        lighter_book_revision=lighter.book_revision,
        risex_book_revision_id=risex.book_revision_id,
        lighter_book_revision_id=lighter.book_revision_id,
    )
    return version, (risex, lighter)


def test_s1b_accumulates_partial_entry_and_hedges_one_reserved_chunk() -> None:
    version, source_books = _version("s1b-accumulate")
    kernel = Scv1S1bKernel(fill_model=CycleFillModel.TOUCH_ALLOWED)
    assert kernel.admit(version, source_books=source_books).accepted

    kernel.advance(_activation_book())
    kernel.advance(_entry_trade("s1b-accumulate-1", 600_000_000, "0.35"))
    kernel.advance(_entry_trade("s1b-accumulate-2", 700_000_000, "0.25"))

    before_hedge = kernel.snapshot()
    assert before_hedge is not None
    assert before_hedge.entry_quantity == D("0.60")
    assert before_hedge.q_cap == D("1.00")
    assert before_hedge.pending_entry_hedge_quantity == D("0.60")
    assert before_hedge.positions.risex_signed_quantity == D("-0.60")
    assert before_hedge.positions.lighter_signed_quantity == D("0")

    kernel.advance(
        _book(
            Venue.RISEX,
            received=900_000_000,
            revision=3,
            bids=(("99", "10"),),
            asks=(("102", "10"),),
        )
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=900_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        )
    )
    kernel.advance_clock(1_300_000_000)
    after_hedge = kernel.snapshot()
    assert after_hedge is not None
    assert after_hedge.entry_quantity == D("0.60")
    assert after_hedge.hedged_quantity == D("0.60")
    assert after_hedge.pending_entry_hedge_quantity == D("0")
    assert after_hedge.positions.paired_risex_quantity == D("0.60")
    assert after_hedge.positions.paired_lighter_quantity == D("0.60")
    assert after_hedge.active_entry_version_id is None
    assert any(action.action_id == "entry-cancel" for action in after_hedge.pending_actions)


def test_s1b_pending_hedges_keep_individual_request_delays() -> None:
    version, source_books = _version("s1b-reserve")
    kernel = Scv1S1bKernel(fill_model=CycleFillModel.TOUCH_ALLOWED)
    assert kernel.admit(version, source_books=source_books).accepted
    kernel.advance(_activation_book())
    kernel.advance(_entry_trade("s1b-reserve-1", 600_000_000, "0.40"))
    kernel.advance(_entry_trade("s1b-reserve-2", 800_000_000, "0.30"))
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=900_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        )
    )

    snapshot = kernel.snapshot()
    assert snapshot is not None
    pending = [
        action
        for action in snapshot.actions
        if action.kind.value == "ENTRY_HEDGE"
    ]
    assert len(pending) == 2
    assert [(item.requested_quantity, item.due_monotonic_ns) for item in pending] == [
        (D("0.40"), 1_100_000_000),
        (D("0.30"), 1_300_000_000),
    ]
    assert snapshot.pending_entry_hedge_quantity == D("0.70")
    assert snapshot.hedged_quantity + snapshot.pending_entry_hedge_quantity <= snapshot.q_cap

    kernel.advance_clock(1_100_000_000)
    first = kernel.snapshot()
    assert first is not None
    assert first.hedged_quantity == D("0.40")
    assert first.pending_entry_hedge_quantity == D("0.30")
    assert [fill.quantity for fill in first.fills if fill.action_id.startswith("entry-hedge")] == [D("0.40")]

    kernel.advance_clock(1_300_000_000)
    second = kernel.snapshot()
    assert second is not None
    assert second.hedged_quantity == D("0.70")
    assert second.pending_entry_hedge_quantity == D("0")


def test_s1b_aggregates_subminimum_entry_reservations_at_latest_due_boundary() -> None:
    lighter_market = replace(
        _market(Venue.LIGHTER, "BTC", minimum_quantity="0.30"),
        quantity_step_raw=D("0.01"),
    )
    for scenario, activation_ns in (
        (CycleScenario.PRIMARY, 500_000_000),
        (CycleScenario.STRESS, 1_000_000_000),
    ):
        version, source_books = _version(
            f"s1b-subminimum-{scenario.value.lower()}",
            lighter_market=lighter_market,
        )
        kernel = Scv1S1bKernel(fill_model=CycleFillModel.TOUCH_ALLOWED)
        assert kernel.admit(version, scenario=scenario, source_books=source_books).accepted
        kernel.advance(
            _book(
                Venue.RISEX,
                received=activation_ns - 100_000_000,
                revision=2,
                bids=(("99", "10"),),
                asks=(("102", "10"),),
            ),
            scenario=scenario,
        )
        kernel.advance_clock(activation_ns, scenario=scenario)
        first_fill_ns = activation_ns + 100_000_000
        second_fill_ns = activation_ns + 300_000_000
        kernel.advance(
            _entry_trade(
                f"subminimum-first-{scenario.value}",
                first_fill_ns,
                "0.20",
            ),
            scenario=scenario,
        )
        kernel.advance(
            _entry_trade(
                f"subminimum-second-{scenario.value}",
                second_fill_ns,
                "0.20",
            ),
            scenario=scenario,
        )
        taker_delay = kernel.policy.delays(scenario).taker_delay_ns
        first_due_ns = first_fill_ns + taker_delay
        second_due_ns = second_fill_ns + taker_delay
        kernel.advance(
            _book(
                Venue.LIGHTER,
                received=first_due_ns - 100_000_000,
                revision=2,
                bids=(("99", "10"),),
                asks=(("100", "0.20"),),
            ),
            scenario=scenario,
        )
        kernel.advance_clock(first_due_ns, scenario=scenario)
        first = kernel.snapshot(scenario=scenario)
        assert first is not None
        assert first.hedged_quantity == D("0")
        assert first.pending_entry_hedge_quantity == D("0.40")
        first_action = next(action for action in first.actions if action.action_id == "entry-hedge")
        assert first_action.status.value == "PENDING"
        assert first_action.due_monotonic_ns is None

        kernel.advance(
            _book(
                Venue.LIGHTER,
                received=second_due_ns - 100_000_000,
                revision=3,
                bids=(("99", "10"),),
                asks=(("100", "10"),),
            ),
            scenario=scenario,
        )
        kernel.advance_clock(second_due_ns, scenario=scenario)
        second = kernel.snapshot(scenario=scenario)
        assert second is not None
        assert second.entry_quantity == D("0.40")
        assert second.hedged_quantity == D("0.40")
        assert second.pending_entry_hedge_quantity == D("0")
        assert second.positions.paired_risex_quantity == D("0.40")
        assert second.positions.paired_lighter_quantity == D("0.40")
        assert [
            fill.quantity
            for fill in second.fills
            if fill.action_id.startswith("entry-hedge")
        ] == [D("0.20"), D("0.20")]


def test_s1b_pre_exit_barrier_fill_counts_before_effective_cancel() -> None:
    version, source_books = _version("s1b-barrier-race")
    kernel = Scv1S1bKernel(fill_model=CycleFillModel.TOUCH_ALLOWED)
    assert kernel.admit(version, source_books=source_books).accepted
    kernel.advance(_activation_book())
    kernel.advance(_entry_trade("s1b-barrier-race-1", 600_000_000, "0.50"))
    kernel.advance(
        _book(
            Venue.RISEX,
            received=900_000_000,
            revision=3,
            bids=(("99", "10"),),
            asks=(("102", "10"),),
        )
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=900_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        )
    )
    kernel.advance_clock(1_300_000_000)
    barrier = kernel.snapshot()
    assert barrier is not None
    assert barrier.pending_entry_hedge_quantity == D("0")
    assert any(action.action_id == "entry-cancel" for action in barrier.pending_actions)

    kernel.advance(_entry_trade("s1b-barrier-race-2", 1_400_000_000, "0.20"))
    raced = kernel.snapshot()
    assert raced is not None
    assert raced.entry_quantity == D("0.70")
    assert raced.pending_entry_hedge_quantity == D("0.20")
    assert raced.positions.paired_risex_quantity == D("0.50")

    kernel.advance(
        _book(
            Venue.RISEX,
            received=1_500_000_000,
            revision=4,
            bids=(("99", "10"),),
            asks=(("102", "10"),),
        )
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=1_500_000_000,
            revision=3,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        )
    )
    kernel.advance_clock(2_100_000_000)
    result = kernel.snapshot()
    assert result is not None
    assert result.entry_quantity == D("0.70")
    assert result.hedged_quantity == D("0.70")
    assert result.pending_entry_hedge_quantity == D("0")
    assert result.positions.paired_risex_quantity == D("0.70")
    assert result.positions.paired_lighter_quantity == D("0.70")
    assert result.exit_measurement is not None
    assert result.exit_measurement.observed_filled_quantity == D("0")
    assert result.active_entry_version_id is None


def test_s1b_counts_fill_during_cancel_and_requotes_without_cap_or_deadline_reset() -> None:
    version, source_books = _custom_quantity_version(risex_minimum="0.1")
    kernel = Scv1S1bKernel(fill_model=CycleFillModel.TOUCH_ALLOWED)
    assert kernel.admit(version, source_books=source_books).accepted
    kernel.advance(_activation_book())
    kernel.advance(_entry_trade("s1b-cancel-race-1", 600_000_000, "0.20"))
    first_deadline = kernel.snapshot().max_hold_deadline_monotonic_ns
    assert first_deadline == 120_600_000_000
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=900_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        )
    )
    kernel.advance_clock(1_200_000_000)
    kernel.advance_clock(5_500_000_000)
    kernel.advance(_entry_trade("s1b-cancel-race-2", 5_700_000_000, "0.10"))
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=5_800_000_000,
            revision=3,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        )
    )
    kernel.advance_clock(6_400_000_000)
    raced = kernel.snapshot()
    assert raced is not None
    assert raced.entry_quantity == D("0.30")
    assert raced.q_cap == D("1.00")
    assert raced.max_hold_deadline_monotonic_ns == first_deadline
    assert raced.active_entry_version_id is None
    assert "MINIMUM_RESIDUE" in raced.reason_codes

    next_version, next_books = _fresh_version("s1b-requote", 6_400_000_100, 4)
    admission = kernel.admit(next_version, source_books=next_books)
    assert admission.accepted
    requoted = kernel.snapshot()
    assert requoted is not None
    assert requoted.q_cap == D("1.00")
    assert requoted.entry_quantity == D("0.30")
    assert requoted.active_entry_version_id == "s1b-requote"
    assert requoted.max_hold_deadline_monotonic_ns == first_deadline


def test_s1b_partial_exit_keeps_valid_maker_remainder_and_closes_only_actual_fill() -> None:
    version, source_books = _version("s1b-partial-exit")
    kernel = Scv1S1bKernel(fill_model=CycleFillModel.TOUCH_ALLOWED)
    assert kernel.admit(version, source_books=source_books).accepted
    kernel.advance(_activation_book())
    kernel.advance(_entry_trade("s1b-partial-exit-entry", 600_000_000, "0.80"))
    kernel.advance(
        _book(
            Venue.RISEX,
            received=900_000_000,
            revision=3,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        )
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=900_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        )
    )
    kernel.advance(
        _book(
            Venue.RISEX,
            received=1_400_000_000,
            revision=4,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        )
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=1_400_000_000,
            revision=3,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        )
    )
    kernel.advance_clock(1_700_000_000)
    kernel.advance(
        _book(
            Venue.RISEX,
            received=2_000_000_000,
            revision=5,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        )
    )
    kernel.advance(
        _trade(
            "s1b-partial-exit-fill",
            received=2_300_000_000,
            quantity="0.30",
            price="97",
            aggressor=Side.SELL,
        )
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=2_400_000_000,
            revision=4,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        )
    )
    kernel.advance_clock(2_900_000_000)
    result = kernel.snapshot()
    assert result is not None
    assert result.status is CycleTerminalState.PENDING
    assert result.positions.paired_risex_quantity == D("0.50")
    assert result.positions.paired_lighter_quantity == D("0.50")
    assert result.exit_measurement is not None
    assert result.exit_measurement.observed_filled_quantity == D("0.30")
    maker = next(action for action in result.actions if action.action_id == "exit-maker")
    assert maker.status.value == "PENDING"
    assert maker.remaining_quantity == D("0.50")
    close = next(action for action in result.actions if action.action_id == "exit-close:0")
    assert close.status.value == "COMPLETED"
    assert close.executed_quantity == D("0.30")


def test_s1b_exit_close_reservations_accumulate_without_blocking_maker_remainder() -> None:
    lighter_market = replace(
        _market(Venue.LIGHTER, "BTC", minimum_quantity="0.30"),
        quantity_step_raw=D("0.01"),
    )
    version, source_books = _version(
        "s1b-aggregate-close",
        lighter_market=lighter_market,
    )
    kernel = Scv1S1bKernel(fill_model=CycleFillModel.TOUCH_ALLOWED)
    assert kernel.admit(version, source_books=source_books).accepted
    kernel.advance(_activation_book())
    kernel.advance(_entry_trade("s1b-aggregate-entry", 600_000_000, "0.80"))
    for venue, received, revision in (
        (Venue.RISEX, 900_000_000, 3),
        (Venue.LIGHTER, 900_000_000, 2),
        (Venue.RISEX, 1_400_000_000, 4),
        (Venue.LIGHTER, 1_400_000_000, 3),
    ):
        kernel.advance(
            _book(
                venue,
                received=received,
                revision=revision,
                bids=(("99", "10"),),
                asks=(("105", "10"),) if venue is Venue.RISEX else (("100", "10"),),
            )
        )
    kernel.advance_clock(1_700_000_000)
    kernel.advance(
        _book(
            Venue.RISEX,
            received=2_000_000_000,
            revision=5,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        )
    )
    kernel.advance(
        _trade(
            "s1b-aggregate-exit-1",
            received=2_300_000_000,
            quantity="0.20",
            price="97",
            aggressor=Side.SELL,
        )
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=2_400_000_000,
            revision=4,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        )
    )
    kernel.advance_clock(2_900_000_000)
    waiting = kernel.snapshot()
    assert waiting is not None
    assert not waiting.policy_blocked
    deferred = next(action for action in waiting.actions if action.action_id == "exit-close:0")
    assert deferred.status.value == "PENDING"
    assert deferred.due_monotonic_ns is None

    kernel.advance(
        _trade(
            "s1b-aggregate-exit-2",
            received=3_000_000_000,
            quantity="0.30",
            price="97",
            aggressor=Side.SELL,
        )
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=3_000_000_000,
            revision=5,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        )
    )
    kernel.advance_clock(3_500_000_000)
    aggregate = kernel.snapshot()
    assert aggregate is not None
    assert not aggregate.policy_blocked
    assert aggregate.positions.paired_risex_quantity == D("0.30")
    assert aggregate.positions.paired_lighter_quantity == D("0.30")
    closes = [fill for fill in aggregate.fills if fill.action_id.startswith("exit-close:")]
    assert [fill.quantity for fill in closes] == [D("0.20"), D("0.30")]
    assert aggregate.pending_exit_close_quantity == D("0")


def test_s1b_exit_uses_both_venue_operation_grids_without_rounding_up() -> None:
    risex_market = replace(_market(Venue.RISEX, "BTC/USDC"), quantity_step_raw=D("0.1"))
    lighter_market = replace(_market(Venue.LIGHTER, "BTC"), quantity_step_raw=D("0.01"))
    version, source_books = _version(
        "s1b-own-grid",
        risex_market=risex_market,
        lighter_market=lighter_market,
    )
    kernel = Scv1S1bKernel(fill_model=CycleFillModel.TOUCH_ALLOWED)
    assert kernel.admit(version, source_books=source_books).accepted
    kernel.advance(_activation_book())
    kernel.advance(_entry_trade("s1b-own-grid-entry", 600_000_000, "0.55"))
    for venue, received, revision in (
        (Venue.RISEX, 900_000_000, 3),
        (Venue.LIGHTER, 900_000_000, 2),
        (Venue.RISEX, 1_400_000_000, 4),
        (Venue.LIGHTER, 1_400_000_000, 3),
    ):
        kernel.advance(
            _book(
                venue,
                received=received,
                revision=revision,
                bids=(("99", "10"),),
                asks=(("102", "10"),) if venue is Venue.RISEX else (("100", "10"),),
            )
        )
    kernel.advance_clock(1_700_000_000)
    result = kernel.snapshot()
    assert result is not None
    assert result.exit_measurement is not None
    assert result.exit_measurement.quote.quantity == D("0.5")
    maker = next(action for action in result.actions if action.action_id == "exit-maker")
    assert maker.requested_quantity == D("0.5")
    assert result.positions.paired_risex_quantity == D("0.55")
    assert result.positions.paired_lighter_quantity == D("0.55")


def test_s1b_exit_uses_joint_operation_grid_for_nondividing_steps() -> None:
    for index, (risex_step, lighter_step) in enumerate(
        ((D("0.03"), D("0.02")), (D("0.02"), D("0.03")))
    ):
        risex_market = replace(
            _market(Venue.RISEX, "BTC/USDC"),
            quantity_step_raw=risex_step,
        )
        lighter_market = replace(
            _market(Venue.LIGHTER, "BTC"),
            quantity_step_raw=lighter_step,
        )
        version, source_books = _version(
            f"s1b-nondividing-grid-{index}",
            risex_market=risex_market,
            lighter_market=lighter_market,
        )
        kernel = Scv1S1bKernel(fill_model=CycleFillModel.TOUCH_ALLOWED)
        assert kernel.admit(version, source_books=source_books).accepted
        kernel.advance(_activation_book())
        entry_quantity = "0.10" if index == 0 else "0.09"
        expected_inventory = D(entry_quantity)
        kernel.advance(_entry_trade(f"nondividing-entry-{index}", 600_000_000, entry_quantity))
        for venue, received, revision in (
            (Venue.RISEX, 900_000_000, 3),
            (Venue.LIGHTER, 900_000_000, 2),
            (Venue.RISEX, 1_400_000_000, 4),
            (Venue.LIGHTER, 1_400_000_000, 3),
        ):
            kernel.advance(
                _book(
                    venue,
                    received=received,
                    revision=revision,
                    bids=(("99", "10"),),
                    asks=(("105", "10"),) if venue is Venue.RISEX else (("100", "10"),),
                )
            )
        kernel.advance_clock(1_700_000_000)
        result = kernel.snapshot()
        assert result is not None
        assert result.exit_measurement is not None
        assert result.exit_measurement.quote.quantity == D("0.06")
        maker = next(action for action in result.actions if action.action_id == "exit-maker")
        assert maker.requested_quantity == D("0.06")
        assert result.positions.paired_risex_quantity == expected_inventory
        assert result.positions.paired_lighter_quantity == expected_inventory


def test_s1b_deadline_minimum_residue_is_policy_blocked_and_retained() -> None:
    version, source_books = _custom_quantity_version(risex_minimum="0.6")
    kernel = Scv1S1bKernel(fill_model=CycleFillModel.TOUCH_ALLOWED)
    assert kernel.admit(version, source_books=source_books).accepted
    kernel.advance(_activation_book())
    kernel.advance(_entry_trade("s1b-deadline-residue", 600_000_000, "0.20"))
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=900_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        )
    )
    kernel.advance_clock(120_600_000_000)
    kernel.advance(
        _book(
            Venue.RISEX,
            received=120_900_000_000,
            revision=3,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        )
    )
    kernel.advance_clock(121_800_000_000)
    result = kernel.snapshot()
    assert result is not None
    assert result.status is CycleTerminalState.UNRESOLVED
    assert result.policy_blocked
    assert not result.is_flat
    assert result.positions.risex_signed_quantity == D("-0.20")
    assert "POLICY_BLOCKED_MINIMUM" in result.reason_codes
    assert "POLICY_BLOCKED_RESIDUAL" in result.reason_codes
    assert result.executable_liquidating_close_estimate_usd is None
    kernel.advance(
        _book(
            Venue.RISEX,
            received=122_000_000_000,
            revision=4,
            bids=(("99", "10"),),
            asks=(("110", "10"),),
        )
    )
    repriced = kernel.snapshot()
    assert repriced is not None
    assert repriced.marked_risex_price == D("110")
    assert repriced.marked_execution_only_pnl_usd == D("-1.802020")
    assert repriced.marked_at_monotonic_ns == 122_000_000_000
    assert repriced.blocked_duration_ns is not None and repriced.blocked_duration_ns > 0
    next_version, next_books = _fresh_version("s1b-after-block", 122_000_000_000, 4)
    blocked_admission = kernel.admit(next_version, source_books=next_books)
    assert not blocked_admission.accepted
    assert blocked_admission.reason == "UNRESOLVED_HALTED"


def test_s1b_entry_wait_deadline_starts_completion_before_late_activation() -> None:
    version, source_books = _custom_quantity_version(risex_minimum="0.1")
    kernel = Scv1S1bKernel(fill_model=CycleFillModel.TOUCH_ALLOWED)
    assert kernel.admit(version, source_books=source_books).accepted
    kernel.advance(_activation_book())
    kernel.advance(_entry_trade("s1b-entry-wait-deadline-first", 600_000_000, "0.20"))
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=900_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        )
    )
    kernel.advance_clock(6_100_000_000)
    next_version, next_books = _fresh_version("s1b-entry-wait-deadline", 120_400_000_000, 4)
    assert kernel.admit(next_version, source_books=next_books).accepted
    for venue in (Venue.RISEX, Venue.LIGHTER):
        kernel.advance(
            _book(
                venue,
                received=120_500_000_000,
                revision=5,
                bids=(("99", "10"),),
                asks=(("102", "10"),) if venue is Venue.RISEX else (("100", "10"),),
            )
        )
    kernel.advance_clock(120_950_000_000)
    result = kernel.snapshot()
    assert result is not None
    assert result.status is CycleTerminalState.PENDING
    assert result.max_hold_deadline_monotonic_ns == 120_600_000_000
    assert "MAX_HOLD" in result.reason_codes
    assert "LATE_OLDER_EVENT" not in result.reason_codes
    maker = next(action for action in result.actions if action.action_id == "entry-maker:1")
    cancel = next(action for action in result.actions if action.action_id == "entry-cancel:1")
    assert maker.status.value == "PENDING"
    assert cancel.status.value == "PENDING"
    assert cancel.requested_monotonic_ns == 120_600_000_000
    assert cancel.effective_monotonic_ns == 121_100_000_000


def test_s1b_exit_wait_deadline_starts_cancel_before_late_activation() -> None:
    version, source_books = _version(
        "s1b-exit-wait-deadline",
        lighter_market=_market(Venue.LIGHTER, "BTC", minimum_quantity="0.30"),
    )
    kernel = Scv1S1bKernel(fill_model=CycleFillModel.TOUCH_ALLOWED)
    scenario = CycleScenario.STRESS
    assert kernel.admit(version, scenario=scenario, source_books=source_books).accepted
    kernel.advance(
        _book(
            Venue.RISEX,
            received=900_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("102", "10"),),
        ),
        scenario=scenario,
    )
    kernel.advance_clock(1_100_000_000, scenario=scenario)
    kernel.advance(
        _entry_trade("s1b-exit-wait-first", 1_100_000_000, "0.20"),
        scenario=scenario,
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=1_800_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        ),
        scenario=scenario,
    )
    kernel.advance_clock(2_100_000_000, scenario=scenario)
    kernel.advance_clock(7_100_000_000, scenario=scenario)
    next_version, next_books = _fresh_version("s1b-exit-wait-requote", 113_000_000_000, 10)
    assert kernel.admit(next_version, scenario=scenario, source_books=next_books).accepted
    for venue in (Venue.RISEX, Venue.LIGHTER):
        kernel.advance(
            _book(
                venue,
                received=113_600_000_000,
                revision=11,
                bids=(("99", "10"),),
                asks=(("105", "10"),) if venue is Venue.RISEX else (("100", "10"),),
            ),
            scenario=scenario,
        )
    kernel.advance_clock(114_000_000_000, scenario=scenario)
    kernel.advance(
        _entry_trade("s1b-exit-wait-second", 118_900_000_000, "0.20"),
        scenario=scenario,
    )
    for venue in (Venue.RISEX, Venue.LIGHTER):
        kernel.advance(
            _book(
                venue,
                received=119_800_000_000,
                revision=12,
                bids=(("99", "10"),),
                asks=(("105", "10"),) if venue is Venue.RISEX else (("100", "10"),),
            ),
            scenario=scenario,
        )
    kernel.advance_clock(119_900_000_000, scenario=scenario)
    for venue in (Venue.RISEX, Venue.LIGHTER):
        kernel.advance(
            _book(
                venue,
                received=120_700_000_000,
                revision=13,
                bids=(("99", "10"),),
                asks=(("105", "10"),) if venue is Venue.RISEX else (("100", "10"),),
            ),
            scenario=scenario,
        )
    kernel.advance_clock(120_900_000_000, scenario=scenario)
    before = kernel.snapshot(scenario=scenario)
    assert before is not None
    assert before.status is CycleTerminalState.PENDING
    assert before.positions.paired_risex_quantity == D("0.40")
    exit_maker = next(action for action in before.actions if action.action_id == "exit-maker")
    assert exit_maker.status.value == "PENDING"
    kernel.advance_clock(121_200_000_000, scenario=scenario)
    after = kernel.snapshot(scenario=scenario)
    assert after is not None
    assert after.status is CycleTerminalState.PENDING
    assert "MAX_HOLD" in after.reason_codes
    assert "LATE_OLDER_EVENT" not in after.reason_codes
    exit_cancel = next(action for action in after.actions if action.action_id == "exit-cancel")
    assert exit_cancel.status.value == "PENDING"
    assert exit_cancel.requested_monotonic_ns == 121_100_000_000
    assert exit_cancel.effective_monotonic_ns == 122_100_000_000


def test_s1b_marked_pnl_uses_open_inventory_without_adding_closed_pnl() -> None:
    version, source_books = _version("s1b-mark")
    kernel = Scv1S1bKernel(fill_model=CycleFillModel.TOUCH_ALLOWED)
    assert kernel.admit(version, source_books=source_books).accepted
    kernel.advance(_activation_book())
    kernel.advance(_entry_trade("s1b-mark-entry", 600_000_000, "0.50"))
    kernel.advance(
        _book(
            Venue.RISEX,
            received=900_000_000,
            revision=3,
            bids=(("99", "10"),),
            asks=(("102", "10"),),
        )
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=900_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        )
    )
    kernel.advance_clock(1_300_000_000)
    result = kernel.snapshot()
    assert result is not None
    assert result.status is CycleTerminalState.PENDING
    assert result.marked_risex_price == D("102")
    assert result.marked_lighter_price == D("99")
    assert result.marked_inventory_usd == D("-1.50")
    assert result.complete_execution_pnl_usd is None
    expected = result.ledger.net_cashflow_usd + (
        result.positions.risex_signed_quantity * result.marked_risex_price
        + result.positions.lighter_signed_quantity * result.marked_lighter_price
    )
    assert result.marked_execution_only_pnl_usd == expected


def test_s1b_four_alternatives_are_isolated_and_touch_is_explicit() -> None:
    version, source_books = _version("s1b-four")
    events = (
        _activation_book(),
        _entry_trade("s1b-four-touch", 600_000_000, "0.50", price="101"),
        _book(
            Venue.RISEX,
            received=900_000_000,
            revision=3,
            bids=(("99", "10"),),
            asks=(("102", "10"),),
        ),
        _book(
            Venue.LIGHTER,
            received=900_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        ),
        _entry_trade("s1b-four-touch-late", 1_200_000_000, "0.50", price="101"),
    )
    alternatives = run_scv1_s1b_alternatives(
        version,
        events,
        source_books=source_books,
        end_monotonic_ns=1_300_000_000,
    )
    assert len(alternatives) == 4
    strict_primary, strict_stress, touch_primary, touch_stress = alternatives
    assert strict_primary.entry_quantity == D("0")
    assert strict_stress.entry_quantity == D("0")
    assert touch_primary.entry_quantity == D("1.00")
    assert touch_stress.entry_quantity == D("0.50")
    assert any(
        decision.reason == "TOUCH_IGNORED_BY_MODEL"
        for decision in strict_primary.entry_measurement.decisions
    )
    assert any(
        decision.reason == "ELIGIBLE_TOUCH_ZERO_QUEUE"
        for decision in touch_primary.entry_measurement.decisions
    )
    assert len({id(result.ledger) for result in alternatives}) == 4


def test_s1b_d1_rebuilds_quotes_from_stream_inputs_not_old_decisions(tmp_path) -> None:
    version, source_books = _version("old-decision-only")
    old_quote = _quote_version_to_dict(version)
    old_quote["version_id"] = "OLD-DECISION-MUST-NOT-BE-ADMITTED"
    old_quote["quote"]["maker_price"] = "999999"
    old_quote["quote"]["canonical_quantity"] = "99"
    metadata = {
        "accepted_release": "historical-release",
        "created_utc": "2026-09-05T00:00:00+00:00",
        "policy": {
            "canonical_market": "BTC",
            "direction": "RISEX_SELL_LIGHTER_BUY",
            "target_notional_usd": "100",
            "target_margin_bps": "1",
            "risex_maker_fee_rate": "0.0001",
            "risex_fee_source": "SS-001Q",
            "lighter_taker_fee_rate": "0",
            "lighter_fee_source": "OFFICIAL_LIGHTER_STANDARD_RESEARCH_INPUT",
        },
    }
    records = [
        {"kind": "RUN_METADATA", "metadata": metadata},
        {
            "kind": "CYCLE_STREAM_INPUT",
            "input_index": 0,
            "input": _input_to_dict(source_books[0]),
        },
        {
            "kind": "CYCLE_STREAM_INPUT",
            "input_index": 1,
            "input": _input_to_dict(source_books[1]),
        },
        {
            "kind": "CYCLE_STREAM_INPUT",
            "input_index": 2,
            "input": _input_to_dict(CycleClock(100)),
        },
        {
            "kind": "CYCLE_DECISION",
            "attempt_index": 0,
            "quote_version": old_quote,
            "source_books": [],
        },
        {
            "kind": "CYCLE_STREAM_END",
            "end_monotonic_ns": 1_000_000_000,
        },
    ]
    path = tmp_path / "d1.jsonl"
    path.write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
        encoding="utf-8",
    )

    report = build_scv1_s1b_d1_report(path)

    assert report["source"]["historical_decisions_reused_as_inputs"] is False
    assert report["source"]["decision_contract"]["quote_recomputed_at_clock"] is True
    assert report["run"]["historical_decision_count"] == 1
    assert report["run"]["recomputed_active_quote_count"] == 1
    assert report["run"]["admitted_alternative_count"] == 4
    assert report["run"]["data_sufficiency"] == "SUFFICIENT_FOR_OBSERVED_REPLAY"
    attempts = report["decision_attempts"]
    assert len(attempts) == 1
    assert attempts[0]["recomputed_quote"]["version_id"] == "D1-REPLAY-00000000"
    assert attempts[0]["recomputed_quote"]["maker_price"] != "999999"
    assert all(row["quote_version_id"] == "D1-REPLAY-00000000" for row in report["admissions"])


@pytest.mark.parametrize(
    ("scenario", "activation_ns"),
    (
        (CycleScenario.PRIMARY, 500_000_000),
        (CycleScenario.STRESS, 1_000_000_000),
    ),
)
def test_s1b_matured_entry_hedge_wakes_on_partial_depth_recovery_without_new_fill(
    scenario: CycleScenario,
    activation_ns: int,
) -> None:
    lighter_market = replace(
        _market(Venue.LIGHTER, "BTC", minimum_quantity="0.30"),
        quantity_step_raw=D("0.01"),
    )
    version, source_books = _version(
        f"s1b-recovery-residue-{scenario.value.lower()}",
        lighter_market=lighter_market,
    )
    kernel = Scv1S1bKernel(fill_model=CycleFillModel.TOUCH_ALLOWED)
    assert kernel.admit(version, scenario=scenario, source_books=source_books).accepted
    kernel.advance(
        _book(
            Venue.RISEX,
            received=activation_ns - 100_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("102", "10"),),
        ),
        scenario=scenario,
    )
    kernel.advance_clock(activation_ns, scenario=scenario)
    first_fill_ns = activation_ns + 100_000_000
    second_fill_ns = activation_ns + 300_000_000
    kernel.advance(
        _entry_trade(
            f"s1b-recovery-residue-first-{scenario.value}",
            first_fill_ns,
            "0.20",
        ),
        scenario=scenario,
    )
    kernel.advance(
        _entry_trade(
            f"s1b-recovery-residue-second-{scenario.value}",
            second_fill_ns,
            "0.20",
        ),
        scenario=scenario,
    )
    taker_delay = kernel.policy.delays(scenario).taker_delay_ns
    first_due_ns = first_fill_ns + taker_delay
    second_due_ns = second_fill_ns + taker_delay
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=first_due_ns - 100_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("100", "0.20"),),
        ),
        scenario=scenario,
    )
    kernel.advance_clock(second_due_ns, scenario=scenario)
    before_recovery = kernel.snapshot(scenario=scenario)
    assert before_recovery is not None
    assert before_recovery.hedged_quantity == D("0")
    assert before_recovery.pending_entry_hedge_quantity == D("0.40")

    recovery_ns = second_due_ns + 100_000_000
    kernel.advance(
        _book(
            Venue.RISEX,
            received=recovery_ns,
            revision=3,
            bids=(("99", "10"),),
            asks=(("102", "10"),),
        ),
        scenario=scenario,
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=recovery_ns,
            revision=3,
            bids=(("99", "10"),),
            asks=(("100", "0.35"),),
        ),
        scenario=scenario,
    )
    partial = kernel.snapshot(scenario=scenario)
    assert partial is not None
    assert partial.entry_quantity == D("0.40")
    assert partial.hedged_quantity == D("0.35")
    assert partial.pending_entry_hedge_quantity == D("0.05")
    assert [fill.quantity for fill in partial.fills if fill.action_id.startswith("entry-maker")] == [
        D("0.20"),
        D("0.20"),
    ]
    assert sum(
        (fill.quantity for fill in partial.fills if fill.action_id.startswith("entry-hedge")),
        D("0"),
    ) == D("0.35")

    final_recovery_ns = recovery_ns + 100_000_000
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=final_recovery_ns,
            revision=4,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        ),
        scenario=scenario,
    )
    final = kernel.snapshot(scenario=scenario)
    assert final is not None
    assert final.entry_quantity == D("0.40")
    assert final.hedged_quantity == D("0.35")
    assert final.pending_entry_hedge_quantity == D("0.05")
    assert sum(
        (fill.quantity for fill in final.fills if fill.action_id.startswith("entry-hedge")),
        D("0"),
    ) == D("0.35")


@pytest.mark.parametrize(
    ("scenario", "activation_ns"),
    (
        (CycleScenario.PRIMARY, 500_000_000),
        (CycleScenario.STRESS, 1_000_000_000),
    ),
)
@pytest.mark.parametrize(
    "fill_model",
    (CycleFillModel.TOUCH_ALLOWED, CycleFillModel.TRADE_THROUGH_ONLY),
)
@pytest.mark.parametrize(
    ("recovery_price", "expected_hedged"),
    (("100.1", D("0")), ("101", D("0.40"))),
)
def test_s1b_entry_hedge_wake_tracks_changed_ask_notional(
    scenario: CycleScenario,
    activation_ns: int,
    fill_model: CycleFillModel,
    recovery_price: str,
    expected_hedged: D,
) -> None:
    """A price-only minimum recovery wakes, while a still-bad book stays deferred."""

    lighter_market = replace(
        _market(Venue.LIGHTER, "BTC", minimum_notional="40.2"),
        quantity_step_raw=D("0.01"),
    )
    version, source_books = _version(
        f"s1b-notional-wake-{scenario.value}-{fill_model.value}-{recovery_price}",
        lighter_market=lighter_market,
    )
    kernel = Scv1S1bKernel(fill_model=fill_model)
    assert kernel.admit(version, scenario=scenario, source_books=source_books).accepted

    kernel.advance(
        _book(
            Venue.RISEX,
            received=activation_ns - 100_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("102", "10"),),
        ),
        scenario=scenario,
    )
    for offset, key in ((100_000_000, "first"), (300_000_000, "second")):
        kernel.advance(
            _entry_trade(
                f"s1b-notional-wake-{scenario.value}-{fill_model.value}-{key}",
                activation_ns + offset,
                "0.20",
                price="102",
            ),
            scenario=scenario,
        )
    due_ns = activation_ns + 300_000_000 + kernel.policy.delays(scenario).taker_delay_ns
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=due_ns - 300_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("100", "0.40"),),
        ),
        scenario=scenario,
    )
    kernel.advance_clock(due_ns, scenario=scenario)
    before = kernel.snapshot(scenario=scenario)
    assert before is not None
    assert before.hedged_quantity == D("0")
    assert before.pending_entry_hedge_quantity == D("0.40")

    unchanged = _book(
        Venue.LIGHTER,
        received=due_ns + 100_000_000,
        revision=3,
        bids=(("99", "10"),),
        asks=(("100", "0.40"),),
    )
    kernel.advance(unchanged, scenario=scenario)
    kernel.advance(unchanged, scenario=scenario)
    same_depth = kernel.snapshot(scenario=scenario)
    assert same_depth is not None
    assert same_depth.hedged_quantity == D("0")
    assert same_depth.pending_entry_hedge_quantity == D("0.40")
    assert not any(fill.action_id.startswith("entry-hedge") for fill in same_depth.fills)

    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=due_ns + 400_000_000,
            revision=4,
            bids=(("99", "10"),),
            asks=((recovery_price, "0.40"),),
        ),
        scenario=scenario,
    )
    recovered = kernel.snapshot(scenario=scenario)
    assert recovered is not None
    assert recovered.hedged_quantity == expected_hedged
    assert recovered.pending_entry_hedge_quantity == D("0.40") - expected_hedged
    assert sum(
        (fill.quantity for fill in recovered.fills if fill.action_id.startswith("entry-hedge")),
        D("0"),
    ) == expected_hedged

    if expected_hedged == D("0"):
        still_bad = _book(
            Venue.LIGHTER,
            received=due_ns + 500_000_000,
            revision=5,
            bids=(("99", "10"),),
            asks=((recovery_price, "0.40"),),
        )
        kernel.advance(still_bad, scenario=scenario)
        after_still_bad = kernel.snapshot(scenario=scenario)
        assert after_still_bad is not None
        assert after_still_bad.hedged_quantity == D("0")
        assert after_still_bad.pending_entry_hedge_quantity == D("0.40")


@pytest.mark.parametrize(
    ("scenario", "activation_ns"),
    (
        (CycleScenario.PRIMARY, 500_000_000),
        (CycleScenario.STRESS, 1_000_000_000),
    ),
)
def test_s1b_same_depth_revision_cannot_reconsume_entry_hedge(
    scenario: CycleScenario,
    activation_ns: int,
) -> None:
    lighter_market = replace(
        _market(Venue.LIGHTER, "BTC", minimum_quantity="0.30"),
        quantity_step_raw=D("0.01"),
    )
    version, source_books = _version(
        f"s1b-no-reconsume-{scenario.value}",
        lighter_market=lighter_market,
    )
    kernel = Scv1S1bKernel(fill_model=CycleFillModel.TOUCH_ALLOWED)
    assert kernel.admit(version, scenario=scenario, source_books=source_books).accepted
    kernel.advance(
        _book(
            Venue.RISEX,
            received=activation_ns - 100_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("102", "10"),),
        ),
        scenario=scenario,
    )
    fill_ns = activation_ns + 100_000_000
    kernel.advance(
        _entry_trade(
            f"s1b-no-reconsume-entry-{scenario.value}",
            fill_ns,
            "0.80",
        ),
        scenario=scenario,
    )
    due_ns = fill_ns + kernel.policy.delays(scenario).taker_delay_ns
    first_book = _book(
        Venue.LIGHTER,
        received=due_ns - 100_000_000,
        revision=2,
        bids=(("99", "10"),),
        asks=(("100", "0.40"),),
    )
    kernel.advance(first_book, scenario=scenario)
    kernel.advance_clock(due_ns, scenario=scenario)
    first = kernel.snapshot(scenario=scenario)
    assert first is not None
    assert first.hedged_quantity == D("0.40")
    assert first.pending_entry_hedge_quantity == D("0.40")

    unchanged_revision = _book(
        Venue.LIGHTER,
        received=due_ns + 100_000_000,
        revision=3,
        bids=(("99", "10"),),
        asks=(("100", "0.40"),),
    )
    kernel.advance(unchanged_revision, scenario=scenario)
    kernel.advance(unchanged_revision, scenario=scenario)
    after = kernel.snapshot(scenario=scenario)
    assert after is not None
    assert after.hedged_quantity == D("0.40")
    assert after.pending_entry_hedge_quantity == D("0.40")
    assert sum(
        (fill.quantity for fill in after.fills if fill.action_id.startswith("entry-hedge")),
        D("0"),
    ) == D("0.40")


def _entry_deadline_gate_setup(
    scenario: CycleScenario,
    *,
    activation_bid: str,
) -> tuple[Scv1S1bKernel, int]:
    initial, source_books = _custom_quantity_version(risex_minimum="0.1")
    kernel = Scv1S1bKernel(fill_model=CycleFillModel.TOUCH_ALLOWED)
    assert kernel.admit(initial, scenario=scenario, source_books=source_books).accepted
    activation_ns = kernel.policy.delays(scenario).activation_delay_ns
    first_fill_ns = activation_ns + 100_000_000
    kernel.advance(
        _book(
            Venue.RISEX,
            received=activation_ns - 100_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("102", "10"),),
        ),
        scenario=scenario,
    )
    kernel.advance_clock(activation_ns, scenario=scenario)
    kernel.advance(
        _entry_trade(
            f"s1b-activation-gate-first-{scenario.value}",
            first_fill_ns,
            "0.20",
        ),
        scenario=scenario,
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=first_fill_ns + kernel.policy.delays(scenario).taker_delay_ns - 100_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        ),
        scenario=scenario,
    )
    initial_cancel_effective_ns = (
        initial.decision_ready_monotonic_ns
        + kernel.policy.delays(scenario).activation_delay_ns
        + kernel.policy.entry_cancel_after_activation_ns
        + kernel.policy.delays(scenario).cancel_delay_ns
    )
    kernel.advance_clock(initial_cancel_effective_ns, scenario=scenario)

    deadline_ns = first_fill_ns + kernel.policy.max_hold_ns
    decision_ns = deadline_ns - 200_000_000
    next_version, next_books = _fresh_version(
        f"s1b-activation-gate-{scenario.value.lower()}",
        decision_ns,
        4,
    )
    next_version = replace(next_version, quote=initial.quote)
    assert kernel.admit(next_version, scenario=scenario, source_books=next_books).accepted

    late_activation_ns = decision_ns + kernel.policy.delays(scenario).activation_delay_ns
    before_activation_ns = late_activation_ns - 100_000_000
    kernel.advance(
        _book(
            Venue.RISEX,
            received=before_activation_ns,
            revision=5,
            bids=((activation_bid, "10"),),
            asks=(("103", "10"),),
        ),
        scenario=scenario,
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=before_activation_ns,
            revision=5,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        ),
        scenario=scenario,
    )
    return kernel, late_activation_ns


@pytest.mark.parametrize("scenario", (CycleScenario.PRIMARY, CycleScenario.STRESS))
def test_s1b_entry_activation_gate_survives_deadline_cancel_for_crossing_requote(
    scenario: CycleScenario,
) -> None:
    kernel, late_activation_ns = _entry_deadline_gate_setup(
        scenario,
        activation_bid="102",
    )
    kernel.advance_clock(late_activation_ns, scenario=scenario)
    crossing = kernel.snapshot(scenario=scenario)
    assert crossing is not None
    assert crossing.entry_quantity == D("0.20")
    assert not any(
        fill.action_id == "entry-maker:1" for fill in crossing.fills
    )
    assert "INVALID_ENTRY_QUOTE" in crossing.reason_codes
    kernel.advance(
        _trade(
            f"s1b-activation-gate-crossing-{scenario.value}",
            received=late_activation_ns + 50_000_000,
            quantity="0.10",
            price="103",
        ),
        scenario=scenario,
    )
    after = kernel.snapshot(scenario=scenario)
    assert after is not None
    assert after.entry_quantity == D("0.20")
    assert not any(fill.action_id == "entry-maker:1" for fill in after.fills)


@pytest.mark.parametrize("scenario", (CycleScenario.PRIMARY, CycleScenario.STRESS))
def test_s1b_entry_activation_gate_keeps_eligible_pre_cancel_race(
    scenario: CycleScenario,
) -> None:
    kernel, late_activation_ns = _entry_deadline_gate_setup(
        scenario,
        activation_bid="99",
    )
    kernel.advance_clock(late_activation_ns, scenario=scenario)
    before_race = kernel.snapshot(scenario=scenario)
    assert before_race is not None
    assert before_race.entry_quantity == D("0.20")
    assert "INVALID_ENTRY_QUOTE" not in before_race.reason_codes
    kernel.advance(
        _entry_trade(
            f"s1b-activation-gate-eligible-race-{scenario.value}",
            late_activation_ns + 50_000_000,
            "0.10",
            price="103",
        ),
        scenario=scenario,
    )
    after = kernel.snapshot(scenario=scenario)
    assert after is not None
    assert after.entry_quantity == D("0.30")
    assert any(fill.action_id == "entry-maker:1" for fill in after.fills)


def _exit_deadline_gate_setup(
    scenario: CycleScenario,
) -> tuple[Scv1S1bKernel, int, int]:
    lighter_market = replace(
        _market(Venue.LIGHTER, "BTC", minimum_quantity="0.30"),
        quantity_step_raw=D("0.01"),
    )
    version, source_books = _version(
        f"s1b-exit-activation-gate-{scenario.value.lower()}",
        lighter_market=lighter_market,
    )
    kernel = Scv1S1bKernel(fill_model=CycleFillModel.TOUCH_ALLOWED)
    assert kernel.admit(version, scenario=scenario, source_books=source_books).accepted

    entry_activation_ns = version.decision_ready_monotonic_ns + kernel.policy.delays(scenario).activation_delay_ns
    kernel.advance(
        _book(
            Venue.RISEX,
            received=entry_activation_ns - 100_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("102", "10"),),
        ),
        scenario=scenario,
    )
    kernel.advance_clock(entry_activation_ns, scenario=scenario)
    first_fill_ns = entry_activation_ns + 100_000_000
    kernel.advance(
        _entry_trade(
            f"s1b-exit-activation-entry-{scenario.value}",
            first_fill_ns,
            "0.40",
        ),
        scenario=scenario,
    )
    entry_taker_delay = kernel.policy.delays(scenario).taker_delay_ns
    entry_due_ns = first_fill_ns + entry_taker_delay
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=entry_due_ns - 100_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("100", "0.20"),),
        ),
        scenario=scenario,
    )
    kernel.advance_clock(entry_due_ns, scenario=scenario)
    before_recovery = kernel.snapshot(scenario=scenario)
    assert before_recovery is not None
    assert before_recovery.hedged_quantity == D("0")
    assert before_recovery.pending_entry_hedge_quantity == D("0.40")

    initial_cancel_effective_ns = (
        version.decision_ready_monotonic_ns
        + kernel.policy.delays(scenario).activation_delay_ns
        + kernel.policy.entry_cancel_after_activation_ns
        + kernel.policy.delays(scenario).cancel_delay_ns
    )
    kernel.advance_clock(initial_cancel_effective_ns, scenario=scenario)
    first_fill_deadline_ns = first_fill_ns + kernel.policy.max_hold_ns
    recovery_ns = 120_200_000_000
    assert recovery_ns < first_fill_deadline_ns
    kernel.advance(
        _book(
            Venue.RISEX,
            received=recovery_ns,
            revision=3,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        ),
        scenario=scenario,
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=recovery_ns,
            revision=3,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        ),
        scenario=scenario,
    )
    after_recovery = kernel.snapshot(scenario=scenario)
    assert after_recovery is not None
    assert after_recovery.hedged_quantity == D("0.40")
    exit_maker = next(action for action in after_recovery.actions if action.action_id == "exit-maker")
    exit_activation_ns = exit_maker.effective_monotonic_ns
    assert exit_activation_ns is not None and exit_activation_ns > first_fill_deadline_ns
    return kernel, exit_activation_ns, first_fill_deadline_ns


@pytest.mark.parametrize("scenario", (CycleScenario.PRIMARY, CycleScenario.STRESS))
def test_s1b_exit_activation_gate_survives_deadline_cancel_for_crossing_quote(
    scenario: CycleScenario,
) -> None:
    kernel, exit_activation_ns, first_fill_deadline_ns = _exit_deadline_gate_setup(scenario)

    kernel.advance_clock(first_fill_deadline_ns, scenario=scenario)
    deadline = kernel.snapshot(scenario=scenario)
    assert deadline is not None
    exit_cancel = next(action for action in deadline.actions if action.action_id == "exit-cancel")
    assert exit_cancel.status.value == "PENDING"
    assert exit_cancel.requested_monotonic_ns == first_fill_deadline_ns

    crossing_book_ns = exit_activation_ns - 50_000_000
    kernel.advance(
        _book(
            Venue.RISEX,
            received=crossing_book_ns,
            revision=4,
            bids=(("96", "10"),),
            asks=(("97", "10"),),
        ),
        scenario=scenario,
    )
    kernel.advance_clock(exit_activation_ns, scenario=scenario)
    crossing = kernel.snapshot(scenario=scenario)
    assert crossing is not None
    assert not any(fill.action_id == "exit-maker" for fill in crossing.fills)
    assert "EXIT_QUOTE_INVALID" in crossing.reason_codes
    invalidated = next(action for action in crossing.actions if action.action_id == "exit-maker")
    assert invalidated.status.value == "COMPLETED"
    assert invalidated.reason == "EXIT_QUOTE_INVALID"

    kernel.advance(
        _trade(
            f"s1b-exit-activation-crossing-{scenario.value}",
            received=exit_activation_ns + 50_000_000,
            quantity="0.10",
            price="97",
            aggressor=Side.SELL,
        ),
        scenario=scenario,
    )
    after = kernel.snapshot(scenario=scenario)
    assert after is not None
    assert not any(fill.action_id == "exit-maker" for fill in after.fills)


@pytest.mark.parametrize("scenario", (CycleScenario.PRIMARY, CycleScenario.STRESS))
def test_s1b_exit_activation_gate_keeps_eligible_pre_cancel_race(
    scenario: CycleScenario,
) -> None:
    kernel, exit_activation_ns, first_fill_deadline_ns = _exit_deadline_gate_setup(scenario)
    kernel.advance_clock(first_fill_deadline_ns, scenario=scenario)
    eligible_book_ns = exit_activation_ns - 50_000_000
    kernel.advance(
        _book(
            Venue.RISEX,
            received=eligible_book_ns,
            revision=4,
            bids=(("96", "10"),),
            asks=(("105", "10"),),
        ),
        scenario=scenario,
    )
    kernel.advance_clock(exit_activation_ns, scenario=scenario)
    before_race = kernel.snapshot(scenario=scenario)
    assert before_race is not None
    assert "EXIT_QUOTE_INVALID" not in before_race.reason_codes
    kernel.advance(
        _trade(
            f"s1b-exit-activation-eligible-race-{scenario.value}",
            received=exit_activation_ns + 50_000_000,
            quantity="0.10",
            price="97",
            aggressor=Side.SELL,
        ),
        scenario=scenario,
    )
    after = kernel.snapshot(scenario=scenario)
    assert after is not None
    assert [fill.quantity for fill in after.fills if fill.action_id == "exit-maker"] == [D("0.10")]
    exit_maker = next(action for action in after.actions if action.action_id == "exit-maker")
    assert exit_maker.executed_quantity == D("0.10")
