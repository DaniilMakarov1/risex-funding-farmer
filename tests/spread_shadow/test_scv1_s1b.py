from __future__ import annotations

from dataclasses import replace
from decimal import Decimal as D
from fractions import Fraction
import json
from types import SimpleNamespace

import pytest

from risex_farmer.models import Venue

from risex_spread_shadow import (
    CausalEvent,
    CycleFillModel,
    CycleClock,
    CycleTerminalState,
    DataGapEvidence,
    Scv1S1bKernel,
    S1bExitVariant,
    build_scv1_s1b_d1_report,
    Side,
    run_scv1_s1b,
    run_scv1_s1b_alternatives,
)
from risex_spread_shadow.cycle import CycleReason, CycleScenario

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


def _position_quantities(positions: object) -> tuple[D, D, D, D, D]:
    return (
        positions.risex_signed_quantity,
        positions.lighter_signed_quantity,
        positions.paired_risex_quantity,
        positions.paired_lighter_quantity,
        positions.unmatched_risex_quantity,
    )


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


def test_s1b_temporary_stale_taker_book_retries_only_on_fresh_book() -> None:
    version, source_books = _version("s1b-stale-recovery")
    kernel = Scv1S1bKernel(fill_model=CycleFillModel.TOUCH_ALLOWED)
    assert kernel.admit(version, source_books=source_books).accepted
    activation_ns = version.decision_ready_monotonic_ns + kernel.policy.delays(
        CycleScenario.PRIMARY
    ).activation_delay_ns
    kernel.advance(
        _book(
            Venue.RISEX,
            received=activation_ns - 100_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("102", "10"),),
        )
    )
    kernel.advance_clock(activation_ns)
    fill_ns = activation_ns + 100_000_000
    kernel.advance(_entry_trade("s1b-stale-recovery-fill", fill_ns, "0.20", price="102"))
    due_ns = fill_ns + kernel.policy.delays(CycleScenario.PRIMARY).taker_delay_ns

    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=due_ns - 100_000_000,
            revision=2,
            fresh=False,
            bids=(("99", "10"),),
            asks=(("100", "0.20"),),
        )
    )
    kernel.advance_clock(due_ns)
    stale = kernel.snapshot()
    assert stale is not None
    assert stale.status is CycleTerminalState.PENDING
    assert stale.hedged_quantity == D("0")
    stale_action = next(action for action in stale.actions if action.action_id == "entry-hedge")
    assert stale_action.status.value == "PENDING"
    assert stale_action.reason == "REQUIRED_ACTION_DATA_STALE"
    assert stale_action.due_monotonic_ns == due_ns

    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=due_ns + 100_000_000,
            revision=3,
            bids=(("99", "10"),),
            asks=(("100", "0.20"),),
        )
    )
    recovered = kernel.snapshot()
    assert recovered is not None
    assert recovered.hedged_quantity == D("0.20")
    assert recovered.pending_entry_hedge_quantity == D("0")
    recovered_action = next(action for action in recovered.actions if action.action_id == "entry-hedge")
    assert recovered_action.status.value == "COMPLETED"
    assert recovered_action.due_monotonic_ns == due_ns
    assert [fill.quantity for fill in recovered.fills if fill.action_id == "entry-hedge"] == [D("0.20")]


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


def _s1b_cancel_effective_boundary(
    scenario: CycleScenario,
    fill_model: CycleFillModel,
) -> tuple[Scv1S1bKernel, int]:
    version, source_books = _version(
        f"s1b-cancel-accounting-{scenario.value}-{fill_model.value}"
    )
    kernel = Scv1S1bKernel(fill_model=fill_model)
    assert kernel.admit(version, scenario=scenario, source_books=source_books).accepted
    assert version.decision_ready_monotonic_ns is not None
    activation_ns = (
        version.decision_ready_monotonic_ns
        + kernel.policy.delays(scenario).activation_delay_ns
    )
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
    cancel_requested_ns = activation_ns + kernel.policy.entry_cancel_after_activation_ns
    cancel_effective_ns = cancel_requested_ns + kernel.policy.delays(scenario).cancel_delay_ns
    kernel.advance_clock(cancel_effective_ns, scenario=scenario)
    return kernel, cancel_effective_ns


@pytest.mark.parametrize("scenario", tuple(CycleScenario))
@pytest.mark.parametrize("fill_model", tuple(CycleFillModel))
def test_s1b_cancel_completion_reconciles_pre_effective_fill_processed_late(
    scenario: CycleScenario,
    fill_model: CycleFillModel,
) -> None:
    kernel, cancel_effective_ns = _s1b_cancel_effective_boundary(scenario, fill_model)
    processing_ready_ns = cancel_effective_ns + 100
    kernel.advance(
        replace(
            _entry_trade(
                f"s1b-cancel-accounting-pre-effective-{scenario.value}-{fill_model.value}",
                cancel_effective_ns - 1,
                "0.22",
                price="102",
            ),
            normalized_ready_monotonic_ns=processing_ready_ns,
        ),
        scenario=scenario,
    )
    after_fill = kernel.snapshot(scenario=scenario)
    assert after_fill is not None
    assert after_fill.entry_quantity == D("0.22")
    assert after_fill.pending_entry_hedge_quantity == D("0.22")

    hedge_due_ns = processing_ready_ns + kernel.policy.delays(scenario).taker_delay_ns
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=hedge_due_ns - 100_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        ),
        scenario=scenario,
    )
    # The hedge boundary re-enters entry-cancellation completion for every
    # prior version.  The delayed fill must reconcile the same cancel action,
    # rather than leave requested and executed quantities inconsistent.
    kernel.advance_clock(hedge_due_ns, scenario=scenario)
    result = kernel.snapshot(scenario=scenario)
    assert result is not None
    cancel = next(action for action in result.actions if action.action_id == "entry-cancel")
    assert cancel.status.value == "COMPLETED"
    assert cancel.requested_quantity == D("0.78")
    assert cancel.executed_quantity == D("0.78")
    assert cancel.remaining_quantity == D("0")
    maker = next(action for action in result.actions if action.action_id == "entry-maker")
    hedge = next(action for action in result.actions if action.action_id == "entry-hedge")
    assert maker.executed_quantity == D("0.22")
    assert hedge.executed_quantity == D("0.22")
    assert result.entry_quantity == D("0.22")
    assert result.hedged_quantity == D("0.22")
    assert result.ledger.signed_cashflow_usd == D("0.22")
    assert result.ledger.total_fees_usd == D("0.002222")
    assert all(action.remaining_quantity >= D("0") for action in result.actions)


@pytest.mark.parametrize("scenario", tuple(CycleScenario))
@pytest.mark.parametrize("fill_model", tuple(CycleFillModel))
def test_s1b_cancel_window_excludes_post_effective_fill_and_preserves_version_separation(
    scenario: CycleScenario,
    fill_model: CycleFillModel,
) -> None:
    kernel, cancel_effective_ns = _s1b_cancel_effective_boundary(scenario, fill_model)
    kernel.advance(
        replace(
            _entry_trade(
                f"s1b-cancel-accounting-post-effective-{scenario.value}-{fill_model.value}",
                cancel_effective_ns + 1,
                "0.22",
                price="102",
            ),
            normalized_ready_monotonic_ns=cancel_effective_ns + 100,
        ),
        scenario=scenario,
    )
    old_version_only = kernel.snapshot(scenario=scenario)
    assert old_version_only is not None
    assert old_version_only.entry_quantity == D("0")
    assert not any(fill.action_id.startswith("entry-maker") for fill in old_version_only.fills)
    old_cancel = next(action for action in old_version_only.actions if action.action_id == "entry-cancel")
    assert old_cancel.requested_quantity == D("1.00")
    assert old_cancel.executed_quantity == D("1.00")
    assert old_cancel.remaining_quantity == D("0")

    next_decision_ns = cancel_effective_ns + 1_000_000_000
    next_version, next_books = _fresh_version(
        f"s1b-cancel-accounting-next-{scenario.value}-{fill_model.value}",
        next_decision_ns,
        3,
    )
    assert kernel.admit(next_version, scenario=scenario, source_books=next_books).accepted
    next_activation_ns = next_decision_ns + kernel.policy.delays(scenario).activation_delay_ns
    kernel.advance(
        _book(
            Venue.RISEX,
            received=next_activation_ns - 100_000_000,
            revision=4,
            bids=(("99", "10"),),
            asks=(("102", "10"),),
        ),
        scenario=scenario,
    )
    kernel.advance_clock(next_activation_ns, scenario=scenario)
    kernel.advance(
        _entry_trade(
            f"s1b-cancel-accounting-next-fill-{scenario.value}-{fill_model.value}",
            next_activation_ns + 100_000_000,
            "0.22",
            price="102",
        ),
        scenario=scenario,
    )
    separated = kernel.snapshot(scenario=scenario)
    assert separated is not None
    assert separated.entry_version_ids == (
        f"s1b-cancel-accounting-{scenario.value}-{fill_model.value}",
        f"s1b-cancel-accounting-next-{scenario.value}-{fill_model.value}",
    )
    assert separated.entry_quantity == D("0.22")
    old_maker = next(action for action in separated.actions if action.action_id == "entry-maker")
    new_maker = next(action for action in separated.actions if action.action_id == "entry-maker:1")
    old_cancel = next(action for action in separated.actions if action.action_id == "entry-cancel")
    assert old_maker.executed_quantity == D("0")
    assert new_maker.executed_quantity == D("0.22")
    assert old_cancel.requested_quantity == D("1.00")
    assert old_cancel.executed_quantity == D("1.00")
    assert old_cancel.remaining_quantity == D("0")


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


@pytest.mark.parametrize("scenario", tuple(CycleScenario))
@pytest.mark.parametrize("fill_model", tuple(CycleFillModel))
def test_s1b_gap_overlap_uses_inclusive_activation_boundary(
    scenario: CycleScenario,
    fill_model: CycleFillModel,
) -> None:
    version, source_books = _version(
        f"s1b-gap-activation-{scenario.value}-{fill_model.value}"
    )
    kernel = Scv1S1bKernel(fill_model=fill_model)
    assert kernel.admit(
        version,
        scenario=scenario,
        source_books=source_books,
    ).accepted
    activation_ns = (
        version.decision_ready_monotonic_ns
        + kernel.policy.delays(scenario).activation_delay_ns
    )

    # A matching gap that is fully closed before activation remains in the
    # evidence history but does not falsely block the future quote.  The
    # activation guard still uses a fresh post-only book afterward.
    closed_before = DataGapEvidence(
        source_venue=Venue.LIGHTER,
        canonical_market="BTC",
        stream_session_id="lighter-s2",
        recovery_generation=0,
        gap_start_monotonic_ns=activation_ns - 300_000_000,
        gap_end_monotonic_ns=activation_ns - 200_000_000,
        reason="PRE_ACTIVATION_GAP",
    )
    kernel.advance(closed_before, scenario=scenario)
    before_activation = kernel.snapshot(scenario=scenario)
    assert before_activation is not None
    assert before_activation.status is CycleTerminalState.PENDING
    assert before_activation.reason_codes == ()
    active = kernel._lane(scenario).active
    assert active is not None
    assert active.gaps == [closed_before]

    kernel.advance(
        _book(
            Venue.RISEX,
            received=activation_ns - 50_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("102", "10"),),
        ),
        scenario=scenario,
    )
    kernel.advance_clock(activation_ns, scenario=scenario)
    activated = kernel.snapshot(scenario=scenario)
    assert activated is not None
    assert activated.status is CycleTerminalState.PENDING
    assert "REQUIRED_ACTION_DATA_GAP" not in activated.reason_codes

    # Touching activation is inclusive and therefore remains uncertain.
    touching_kernel = Scv1S1bKernel(fill_model=fill_model)
    assert touching_kernel.admit(
        version,
        scenario=scenario,
        source_books=source_books,
    ).accepted
    touching_kernel.advance(
        _book(
            Venue.RISEX,
            received=activation_ns - 50_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("102", "10"),),
        ),
        scenario=scenario,
    )
    touching = DataGapEvidence(
        source_venue=Venue.LIGHTER,
        canonical_market="BTC",
        stream_session_id="lighter-s2",
        recovery_generation=0,
        gap_start_monotonic_ns=activation_ns,
        gap_end_monotonic_ns=activation_ns,
        reason="ACTIVATION_BOUNDARY_GAP",
    )
    touching_kernel.advance(touching, scenario=scenario)
    touching_result = touching_kernel.snapshot(scenario=scenario)
    assert touching_result is not None
    assert touching_result.status is CycleTerminalState.PENDING
    assert "REQUIRED_ACTION_DATA_GAP" in touching_result.reason_codes
    touching_finished = touching_kernel.finish(scenario=scenario)
    assert touching_finished.status is CycleTerminalState.UNRESOLVED

    # An open matching gap that starts before activation can continue through
    # that boundary and must remain uncertain as well.
    open_kernel = Scv1S1bKernel(fill_model=fill_model)
    assert open_kernel.admit(
        version,
        scenario=scenario,
        source_books=source_books,
    ).accepted
    open_kernel.advance(
        _book(
            Venue.RISEX,
            received=activation_ns - 50_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("102", "10"),),
        ),
        scenario=scenario,
    )
    open_gap = DataGapEvidence(
        source_venue=Venue.LIGHTER,
        canonical_market="BTC",
        stream_session_id="lighter-s2",
        recovery_generation=0,
        gap_start_monotonic_ns=activation_ns - 1,
        gap_end_monotonic_ns=None,
        reason="OPEN_PRE_ACTIVATION_GAP",
    )
    open_kernel.advance(open_gap, scenario=scenario)
    open_result = open_kernel.snapshot(scenario=scenario)
    assert open_result is not None
    assert open_result.status is CycleTerminalState.PENDING
    assert "REQUIRED_ACTION_DATA_GAP" in open_result.reason_codes
    open_finished = open_kernel.finish(scenario=scenario)
    assert open_finished.status is CycleTerminalState.UNRESOLVED


@pytest.mark.parametrize("scenario", tuple(CycleScenario))
@pytest.mark.parametrize("fill_model", tuple(CycleFillModel))
@pytest.mark.parametrize(
    ("source_venue", "stream_session_id", "recovery_generation"),
    (
        (Venue.RISEX, "wrong-risex-session", 0),
        (Venue.LIGHTER, "wrong-lighter-session", 0),
        (Venue.LIGHTER, "lighter-s2", 1),
    ),
)
def test_s1b_wrong_gap_identity_is_ignored_before_interval_and_state_change(
    scenario: CycleScenario,
    fill_model: CycleFillModel,
    source_venue: Venue,
    stream_session_id: str,
    recovery_generation: int,
) -> None:
    version, source_books = _version(
        f"s1b-gap-identity-{scenario.value}-{fill_model.value}-{source_venue.value}-{recovery_generation}"
    )
    kernel = Scv1S1bKernel(fill_model=fill_model)
    assert kernel.admit(
        version,
        scenario=scenario,
        source_books=source_books,
    ).accepted
    activation_ns = (
        version.decision_ready_monotonic_ns
        + kernel.policy.delays(scenario).activation_delay_ns
    )
    before = kernel.snapshot(scenario=scenario)
    assert before is not None
    active = kernel._lane(scenario).active
    assert active is not None
    assert active.gaps == []
    event_count_before = active.event_count

    wrong_identity_gap = DataGapEvidence(
        source_venue=source_venue,
        canonical_market="BTC",
        stream_session_id=stream_session_id,
        recovery_generation=recovery_generation,
        gap_start_monotonic_ns=activation_ns - 300_000_000,
        gap_end_monotonic_ns=activation_ns - 200_000_000,
        reason="WRONG_IDENTITY_GAP",
    )
    kernel.advance(wrong_identity_gap, scenario=scenario)
    after = kernel.snapshot(scenario=scenario)
    assert after is not None
    assert after.status is before.status is CycleTerminalState.PENDING
    assert after.reason_codes == before.reason_codes == ()
    assert after.positions == before.positions
    assert after.ledger == before.ledger
    assert after.actions == before.actions
    assert after.entry_quantity == before.entry_quantity == D("0")
    assert after.hedged_quantity == before.hedged_quantity == D("0")
    assert active.event_count == event_count_before
    assert active.gaps == []


@pytest.mark.parametrize("scenario", tuple(CycleScenario))
@pytest.mark.parametrize("fill_model", tuple(CycleFillModel))
def test_s1b_late_processed_gap_preserves_fills_positions_and_time(
    scenario: CycleScenario,
    fill_model: CycleFillModel,
) -> None:
    version, source_books = _version(
        f"s1b-gap-late-{scenario.value}-{fill_model.value}"
    )
    kernel = Scv1S1bKernel(fill_model=fill_model)
    assert kernel.admit(
        version,
        scenario=scenario,
        source_books=source_books,
    ).accepted
    activation_ns = (
        version.decision_ready_monotonic_ns
        + kernel.policy.delays(scenario).activation_delay_ns
    )
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
    fill_ns = activation_ns + 100_000_000
    kernel.advance(
        _entry_trade(
            f"s1b-gap-late-fill-{scenario.value}-{fill_model.value}",
            fill_ns,
            "0.50",
            price="102",
        ),
        scenario=scenario,
    )
    hedge_due_ns = fill_ns + kernel.policy.delays(scenario).taker_delay_ns
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=hedge_due_ns - 100_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        ),
        scenario=scenario,
    )
    kernel.advance_clock(hedge_due_ns, scenario=scenario)
    before = kernel.snapshot(scenario=scenario)
    assert before is not None
    assert before.entry_quantity == D("0.50")
    assert before.hedged_quantity == D("0.50")
    active = kernel._lane(scenario).active
    assert active is not None
    current_before = active.current_ns
    fills_before = before.fills
    actions_before = before.actions
    ledger_before = before.ledger
    positions_before = before.positions

    late_gap = DataGapEvidence(
        source_venue=Venue.LIGHTER,
        canonical_market="BTC",
        stream_session_id="lighter-s2",
        recovery_generation=0,
        gap_start_monotonic_ns=fill_ns + 1,
        gap_end_monotonic_ns=fill_ns + 2,
        reason="LATE_PROCESSED_GAP",
    )
    late_event = CausalEvent.from_gap(
        late_gap,
        ingress_received_monotonic_ns=current_before + 100,
    )
    kernel.advance(late_event, scenario=scenario)
    after = kernel.snapshot(scenario=scenario)
    assert after is not None
    assert after.status is CycleTerminalState.PENDING
    assert "REQUIRED_ACTION_DATA_GAP" in after.reason_codes
    assert after.fills == fills_before
    assert after.actions == actions_before
    assert after.ledger == ledger_before
    assert _position_quantities(after.positions) == _position_quantities(positions_before)
    finished = kernel.finish(scenario=scenario)
    assert finished.status is CycleTerminalState.UNRESOLVED
    terminal = kernel._lane(scenario).terminal_cycles[-1]
    assert terminal.current_ns >= current_before


@pytest.mark.parametrize("scenario", tuple(CycleScenario))
@pytest.mark.parametrize("fill_model", tuple(CycleFillModel))
def test_s1b_gap_preserves_pending_entry_hedge_obligation(
    scenario: CycleScenario,
    fill_model: CycleFillModel,
) -> None:
    lighter_market = replace(
        _market(Venue.LIGHTER, "BTC", minimum_quantity="0.30"),
        quantity_step_raw=D("0.01"),
    )
    version, source_books = _version(
        f"s1b-gap-pending-hedge-{scenario.value}-{fill_model.value}",
        lighter_market=lighter_market,
    )
    kernel = Scv1S1bKernel(fill_model=fill_model)
    assert kernel.admit(
        version,
        scenario=scenario,
        source_books=source_books,
    ).accepted
    activation_ns = (
        version.decision_ready_monotonic_ns
        + kernel.policy.delays(scenario).activation_delay_ns
    )
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
    fill_ns = activation_ns + 100_000_000
    kernel.advance(
        _entry_trade(
            f"s1b-gap-pending-hedge-fill-{scenario.value}-{fill_model.value}",
            fill_ns,
            "0.20",
            price="102",
        ),
        scenario=scenario,
    )
    hedge_due_ns = fill_ns + kernel.policy.delays(scenario).taker_delay_ns
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=hedge_due_ns - 100_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("100", "0.20"),),
        ),
        scenario=scenario,
    )
    kernel.advance_clock(hedge_due_ns, scenario=scenario)
    before = kernel.snapshot(scenario=scenario)
    assert before is not None
    assert before.pending_entry_hedge_quantity == D("0.20")
    assert before.positions.risex_signed_quantity == D("-0.20")
    active = kernel._lane(scenario).active
    assert active is not None
    current_before = active.current_ns
    gap = DataGapEvidence(
        source_venue=Venue.LIGHTER,
        canonical_market="BTC",
        stream_session_id="lighter-s2",
        recovery_generation=0,
        gap_start_monotonic_ns=current_before,
        gap_end_monotonic_ns=current_before,
        reason="PENDING_HEDGE_GAP",
    )
    kernel.advance(
        CausalEvent.from_gap(
            gap,
            ingress_received_monotonic_ns=current_before + 100,
        ),
        scenario=scenario,
    )
    after = kernel.snapshot(scenario=scenario)
    assert after is not None
    assert after.status is CycleTerminalState.PENDING
    assert after.pending_entry_hedge_quantity == before.pending_entry_hedge_quantity
    assert _position_quantities(after.positions) == _position_quantities(before.positions)
    assert after.ledger == before.ledger
    assert after.actions == before.actions
    finished = kernel.finish(scenario=scenario)
    assert finished.status is CycleTerminalState.UNRESOLVED


def test_s1b_lighter_gap_keeps_risex_maker_fill_and_delayed_hedge_visible() -> None:
    version, source_books = _version("s1b-lighter-gap-risex-fill")
    kernel = Scv1S1bKernel(fill_model=CycleFillModel.TOUCH_ALLOWED)
    assert kernel.admit(version, source_books=source_books).accepted
    activation_ns = version.decision_ready_monotonic_ns + kernel.policy.delays(
        CycleScenario.PRIMARY
    ).activation_delay_ns
    kernel.advance(
        _book(
            Venue.RISEX,
            received=activation_ns - 100_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("102", "10"),),
        )
    )
    kernel.advance_clock(activation_ns)
    gap = DataGapEvidence(
        source_venue=Venue.LIGHTER,
        canonical_market="BTC",
        stream_session_id="lighter-s2",
        recovery_generation=0,
        gap_start_monotonic_ns=activation_ns + 100_000_000,
        gap_end_monotonic_ns=None,
        reason="LIGHTER_ONLY_OUTAGE",
    )
    kernel.advance(gap)
    fill_ns = activation_ns + 200_000_000
    kernel.advance(_entry_trade("s1b-lighter-gap-risex-fill", fill_ns, "0.20", price="102"))
    observed = kernel.snapshot()
    assert observed is not None
    assert observed.status is CycleTerminalState.PENDING
    assert observed.entry_quantity == D("0.20")
    assert observed.positions.risex_signed_quantity == D("-0.20")
    assert observed.pending_entry_hedge_quantity == D("0.20")
    assert not any(fill.venue is Venue.LIGHTER for fill in observed.fills)
    assert "REQUIRED_ACTION_DATA_GAP" in observed.reason_codes

    finished = kernel.finish(end_monotonic_ns=fill_ns + 1_000_000_000)
    assert finished.status is CycleTerminalState.UNRESOLVED
    assert finished.positions.risex_signed_quantity == D("-0.20")
    assert finished.pending_entry_hedge_quantity == D("0.20")
    assert not finished.cashflow_complete


def test_s1b_lighter_recovery_requires_new_valid_snapshot_binding() -> None:
    version, source_books = _version("s1b-lighter-recovery-boundary")
    kernel = Scv1S1bKernel(fill_model=CycleFillModel.TOUCH_ALLOWED)
    assert kernel.admit(version, source_books=source_books).accepted
    activation_ns = version.decision_ready_monotonic_ns + kernel.policy.delays(
        CycleScenario.PRIMARY
    ).activation_delay_ns
    kernel.advance(
        _book(
            Venue.RISEX,
            received=activation_ns - 100_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("102", "10"),),
        )
    )
    kernel.advance_clock(activation_ns)
    fill_ns = activation_ns + 100_000_000
    kernel.advance(_entry_trade("s1b-lighter-recovery-fill", fill_ns, "0.20", price="102"))
    due_ns = fill_ns + kernel.policy.delays(CycleScenario.PRIMARY).taker_delay_ns

    # A replacement stream before the gap boundary cannot heal the old
    # binding, even though its levels are otherwise executable.
    pre_recovery = _book(
        Venue.LIGHTER,
        received=due_ns - 400_000_000,
        revision=2,
        session="lighter-reconnect-pre",
        recovery=1,
        bids=(("99", "10"),),
        asks=(("100", "0.20"),),
    )
    kernel.advance(pre_recovery)
    gap = DataGapEvidence(
        source_venue=Venue.LIGHTER,
        canonical_market="BTC",
        stream_session_id="lighter-s2",
        recovery_generation=0,
        gap_start_monotonic_ns=due_ns - 300_000_000,
        gap_end_monotonic_ns=due_ns - 200_000_000,
        reason="LIGHTER_RECONNECT_BOUNDARY",
    )
    kernel.advance(gap)
    kernel.advance_clock(due_ns)
    before_recovery = kernel.snapshot()
    assert before_recovery is not None
    assert before_recovery.status is CycleTerminalState.PENDING
    assert before_recovery.pending_entry_hedge_quantity == D("0.20")
    assert "REQUIRED_ACTION_DATA_GAP" in before_recovery.reason_codes

    # A fresh book on the old session, then stale and sequence-unhealthy
    # replacement books, remain non-authoritative recovery attempts.
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=due_ns + 100_000_000,
            revision=3,
            session="lighter-s2",
            recovery=0,
            bids=(("99", "10"),),
            asks=(("100", "0.20"),),
        )
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=due_ns + 200_000_000,
            revision=4,
            session="lighter-reconnect-pre",
            recovery=1,
            fresh=False,
            bids=(("99", "10"),),
            asks=(("100", "0.20"),),
        )
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=due_ns + 300_000_000,
            revision=5,
            session="lighter-reconnect-sequence-bad",
            recovery=1,
            sequence_valid=False,
            bids=(("99", "10"),),
            asks=(("100", "0.20"),),
        )
    )
    bad_market = replace(
        _book(
            Venue.LIGHTER,
            received=due_ns + 400_000_000,
            revision=6,
            session="lighter-reconnect-market-bad",
            recovery=1,
            bids=(("99", "10"),),
            asks=(("100", "0.20"),),
        ),
        canonical_market="ETH",
    )
    kernel.advance(bad_market)
    still_waiting = kernel.snapshot()
    assert still_waiting is not None
    assert still_waiting.hedged_quantity == D("0")
    assert still_waiting.pending_entry_hedge_quantity == D("0.20")
    assert not any(fill.action_id == "entry-hedge" for fill in still_waiting.fills)

    valid = _book(
        Venue.LIGHTER,
        received=due_ns + 500_000_000,
        revision=7,
        session="lighter-reconnect-valid",
        recovery=1,
        bids=(("99", "10"),),
        asks=(("100", "0.20"),),
    )
    kernel.advance(valid)
    recovered = kernel.snapshot()
    assert recovered is not None
    assert recovered.hedged_quantity == D("0.20")
    assert recovered.pending_entry_hedge_quantity == D("0")
    assert recovered.actions[1].due_monotonic_ns == due_ns
    assert [fill.quantity for fill in recovered.fills if fill.action_id == "entry-hedge"] == [D("0.20")]
    hedge_fill = next(fill for fill in recovered.fills if fill.action_id == "entry-hedge")
    assert hedge_fill.book_revision_id == "LIGHTER|BTC|lighter-reconnect-valid|1|7"
    active = kernel._lane(CycleScenario.PRIMARY).active
    assert active is not None
    assert active.gaps == [gap]
    assert active.effective_hedge_stream_session_id == "lighter-reconnect-valid"
    assert active.effective_hedge_recovery_generation == 1

    # Replaying the recovery witness cannot reserve or execute a second hedge.
    kernel.advance(valid)
    replayed = kernel.snapshot()
    assert replayed is not None
    assert [fill.quantity for fill in replayed.fills if fill.action_id == "entry-hedge"] == [D("0.20")]


def test_s1b_unmatched_subminimum_residue_does_not_block_paired_reduction() -> None:
    risex_market = replace(
        _market(Venue.RISEX, "BTC/USDC"),
        minimum_quantity_raw=D("0.6"),
        quantity_step_raw=D("0.01"),
    )
    lighter_market = replace(
        _market(Venue.LIGHTER, "BTC"),
        minimum_quantity_raw=D("0.3"),
        quantity_step_raw=D("0.01"),
    )
    version, source_books = _version(
        "s1b-unmatched-residue-paired-reduction",
        risex_market=risex_market,
        lighter_market=lighter_market,
    )
    kernel = Scv1S1bKernel(fill_model=CycleFillModel.TOUCH_ALLOWED)
    assert kernel.admit(version, source_books=source_books).accepted
    activation_ns = version.decision_ready_monotonic_ns + kernel.policy.delays(
        CycleScenario.PRIMARY
    ).activation_delay_ns
    kernel.advance(
        _book(
            Venue.RISEX,
            received=activation_ns - 100_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("102", "10"),),
        )
    )
    kernel.advance_clock(activation_ns)
    kernel.advance(_entry_trade("s1b-residue-entry", 600_000_000, "0.95", price="102"))
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
            asks=(("100", "0.75"),),
        )
    )
    kernel.advance_clock(1_100_000_000)
    paired = kernel.snapshot()
    assert paired is not None
    assert paired.positions.paired_risex_quantity == D("0.75")
    assert paired.positions.paired_lighter_quantity == D("0.75")
    assert paired.positions.unmatched_risex_quantity == D("0.20")
    assert paired.pending_entry_hedge_quantity == D("0.20")

    # The cancellation barrier finalizes the subminimum hedge residue without
    # rounding it up, then schedules the exact unmatched RISEx residue.
    kernel.advance(
        _book(
            Venue.RISEX,
            received=1_500_000_000,
            revision=4,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
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
    kernel.advance_clock(1_600_000_000)
    barrier = kernel.snapshot()
    assert barrier is not None
    assert barrier.positions.unmatched_risex_quantity == D("0.20")
    assert barrier.positions.paired_risex_quantity == D("0.75")
    assert next(action for action in barrier.actions if action.action_id == "unmatched-risex").status.value == "PENDING"

    # The unmatched operation is below the RISEx minimum, but its failed
    # attempt must not prevent the already executable pair from getting an
    # exit quote and closing independently.
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
        _book(
            Venue.LIGHTER,
            received=2_000_000_000,
            revision=4,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        )
    )
    kernel.advance_clock(2_100_000_000)
    exit_ready = kernel.snapshot()
    assert exit_ready is not None
    assert exit_ready.positions.unmatched_risex_quantity == D("0.20")
    assert exit_ready.positions.paired_risex_quantity == D("0.75")
    assert exit_ready.exit_measurement is not None
    assert exit_ready.exit_measurement.quote.quantity == D("0.75")
    unmatched = next(action for action in exit_ready.actions if action.action_id == "unmatched-risex")
    assert unmatched.status.value == "COMPLETED"
    assert unmatched.executed_quantity == D("0")
    assert unmatched.reason == "MINIMUM_RESIDUE"
    assert "POLICY_BLOCKED_RESIDUAL" in exit_ready.reason_codes

    kernel.advance(
        _book(
            Venue.RISEX,
            received=2_500_000_000,
            revision=6,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        )
    )
    kernel.advance_clock(2_600_000_000)
    kernel.advance(
        _trade(
            "s1b-residue-exit-fill",
            received=2_700_000_000,
            quantity="0.75",
            price="97",
            aggressor=Side.SELL,
        )
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=3_100_000_000,
            revision=5,
            bids=(("99", "0.75"),),
            asks=(("100", "10"),),
        )
    )
    closed_pair = kernel.advance_clock(3_200_000_000) or kernel.snapshot()
    assert closed_pair is not None
    assert closed_pair.status is CycleTerminalState.UNRESOLVED
    assert closed_pair.policy_blocked
    assert closed_pair.positions.paired_risex_quantity == D("0")
    assert closed_pair.positions.paired_lighter_quantity == D("0")
    assert closed_pair.positions.unmatched_risex_quantity == D("0.20")
    assert closed_pair.positions.risex_signed_quantity == D("-0.20")
    assert closed_pair.positions.lighter_signed_quantity == D("0")
    assert not closed_pair.is_flat
    assert not closed_pair.cashflow_complete
    assert "POLICY_BLOCKED_RESIDUAL" in closed_pair.reason_codes
    assert [fill.quantity for fill in closed_pair.fills if fill.action_id == "exit-close:0"] == [D("0.75")]


def test_s1b_gap_preserves_pending_exit_and_position_obligations() -> None:
    scenario = CycleScenario.PRIMARY
    kernel, exit_activation_ns, _ = _exit_deadline_gate_setup(scenario)
    before = kernel.snapshot(scenario=scenario)
    assert before is not None
    assert before.positions.paired_risex_quantity > D("0")
    assert any(action.action_id == "exit-maker" for action in before.pending_actions)
    active = kernel._lane(scenario).active
    assert active is not None
    current_before = active.current_ns
    assert current_before < exit_activation_ns

    gap = DataGapEvidence(
        source_venue=Venue.RISEX,
        canonical_market="BTC",
        stream_session_id="risex-s2",
        recovery_generation=0,
        gap_start_monotonic_ns=current_before,
        gap_end_monotonic_ns=None,
        reason="PENDING_EXIT_GAP",
    )
    kernel.advance(
        CausalEvent.from_gap(
            gap,
            ingress_received_monotonic_ns=current_before + 100,
        ),
        scenario=scenario,
    )
    after = kernel.snapshot(scenario=scenario)
    assert after is not None
    assert after.status is CycleTerminalState.UNRESOLVED
    assert _position_quantities(after.positions) == _position_quantities(before.positions)
    assert after.ledger == before.ledger
    assert after.actions == before.actions
    assert after.exit_price == before.exit_price


@pytest.mark.parametrize("scenario", tuple(CycleScenario))
@pytest.mark.parametrize("fill_model", tuple(CycleFillModel))
def test_s1b_gap_checks_future_requote_activation_points(
    scenario: CycleScenario,
    fill_model: CycleFillModel,
) -> None:
    kernel, cancel_effective_ns = _s1b_cancel_effective_boundary(scenario, fill_model)
    next_decision_ns = cancel_effective_ns + 1_000_000_000
    next_version, next_books = _fresh_version(
        f"s1b-gap-future-requote-{scenario.value}-{fill_model.value}",
        next_decision_ns,
        4,
    )
    assert kernel.admit(
        next_version,
        scenario=scenario,
        source_books=next_books,
    ).accepted
    active = kernel._lane(scenario).active
    assert active is not None
    activation_ns = next_decision_ns + kernel.policy.delays(scenario).activation_delay_ns
    assert active.current_ns < activation_ns

    gap = DataGapEvidence(
        source_venue=Venue.LIGHTER,
        canonical_market="BTC",
        stream_session_id="lighter-s2",
        recovery_generation=0,
        gap_start_monotonic_ns=activation_ns - 1,
        gap_end_monotonic_ns=None,
        reason="FUTURE_REQUOTE_OPEN_GAP",
    )
    kernel.advance(
        CausalEvent.from_gap(
            gap,
            ingress_received_monotonic_ns=active.current_ns + 100,
        ),
        scenario=scenario,
    )
    result = kernel.snapshot(scenario=scenario)
    assert result is not None
    assert result.status is CycleTerminalState.PENDING
    assert "REQUIRED_ACTION_DATA_GAP" in result.reason_codes
    finished = kernel.finish(scenario=scenario)
    assert finished.status is CycleTerminalState.UNRESOLVED


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

    c_report = build_scv1_s1b_d1_report(
        path,
        exit_variant=S1bExitVariant.C_BE_REPRICE_V1,
    )
    assert c_report["exit_variant"] == S1bExitVariant.C_BE_REPRICE_V1.value
    c_episodes = [
        episode
        for alternative in c_report["alternatives"]
        for episode in alternative["episodes"]
    ]
    assert c_episodes
    assert all(
        episode["exit_variant"] == S1bExitVariant.C_BE_REPRICE_V1.value
        for episode in c_episodes
    )


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


@pytest.mark.parametrize("scenario", (CycleScenario.PRIMARY, CycleScenario.STRESS))
@pytest.mark.parametrize(
    "fill_model",
    (CycleFillModel.TOUCH_ALLOWED, CycleFillModel.TRADE_THROUGH_ONLY),
)
def test_s1b_later_entry_due_does_not_reconsume_consumed_book(
    scenario: CycleScenario,
    fill_model: CycleFillModel,
) -> None:
    lighter_market = replace(
        _market(Venue.LIGHTER, "BTC", minimum_quantity="0.30"),
        quantity_step_raw=D("0.01"),
    )
    version, source_books = _version(
        f"s1b-no-reconsume-later-due-{scenario.value}-{fill_model.value}",
        lighter_market=lighter_market,
    )
    kernel = Scv1S1bKernel(fill_model=fill_model)
    assert kernel.admit(version, scenario=scenario, source_books=source_books).accepted
    activation_ns = kernel.policy.delays(scenario).activation_delay_ns
    kernel.advance(
        _book(
            Venue.RISEX,
            received=activation_ns - 100_000_000,
            revision=2,
            bids=(
                ("99", "10"),
            ),
            asks=(
                ("102", "10"),
            ),
        ),
        scenario=scenario,
    )
    kernel.advance_clock(activation_ns, scenario=scenario)
    first_fill_ns = activation_ns + 100_000_000
    second_fill_ns = activation_ns + 300_000_000
    kernel.advance(
        _entry_trade(
            f"s1b-no-reconsume-later-due-first-{scenario.value}-{fill_model.value}",
            first_fill_ns,
            "0.60",
            price="102",
        ),
        scenario=scenario,
    )
    kernel.advance(
        _entry_trade(
            f"s1b-no-reconsume-later-due-second-{scenario.value}-{fill_model.value}",
            second_fill_ns,
            "0.20",
            price="102",
        ),
        scenario=scenario,
    )
    taker_delay_ns = kernel.policy.delays(scenario).taker_delay_ns
    first_due_ns = first_fill_ns + taker_delay_ns
    second_due_ns = second_fill_ns + taker_delay_ns
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=first_due_ns - 100_000_000,
            revision=2,
            bids=(
                ("99", "10"),
            ),
            asks=(
                ("100", "0.40"),
            ),
        ),
        scenario=scenario,
    )
    kernel.advance_clock(first_due_ns, scenario=scenario)
    first = kernel.snapshot(scenario=scenario)
    assert first is not None
    assert first.hedged_quantity == D("0.40")
    assert first.pending_entry_hedge_quantity == D("0.40")

    # The second reservation matures without a new Lighter observation.  The
    # first 0.40 must not be counted again as fresh depth.
    kernel.advance_clock(second_due_ns, scenario=scenario)
    second = kernel.snapshot(scenario=scenario)
    assert second is not None
    assert second.hedged_quantity == D("0.40")
    assert second.pending_entry_hedge_quantity == D("0.40")
    assert sum(
        (fill.quantity for fill in second.fills if fill.action_id.startswith("entry-hedge")),
        D("0"),
    ) == D("0.40")


def test_s1b_stale_exit_barrier_defers_until_fresh_book_without_scheduler_halt() -> None:
    def build_kernel() -> Scv1S1bKernel:
        version, source_books = _version("chief-stale-exit-barrier")
        kernel = Scv1S1bKernel(fill_model=CycleFillModel.TOUCH_ALLOWED)
        assert kernel.admit(version, source_books=source_books).accepted
        kernel.advance(_activation_book())
        kernel.advance(_entry_trade("f", 600_000_000, "0.60"))
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
                revision=3,
                bids=(("99", "10"),),
                asks=(("100", "10"),),
            )
        )
        return kernel

    no_recovery = build_kernel()
    no_recovery.advance_clock(1_100_000_000)
    before_stale = no_recovery.snapshot()
    assert before_stale is not None
    assert before_stale.status is CycleTerminalState.PENDING
    assert before_stale.positions.paired_risex_quantity == D("0.60")
    assert before_stale.positions.paired_lighter_quantity == D("0.60")
    deadline = before_stale.max_hold_deadline_monotonic_ns

    no_recovery.advance_clock(1_600_000_000)
    deferred = no_recovery.snapshot()
    assert deferred is not None
    assert deferred.status is CycleTerminalState.PENDING
    assert deferred.positions.authoritative
    assert deferred.positions.paired_risex_quantity == D("0.60")
    assert deferred.positions.paired_lighter_quantity == D("0.60")
    assert "REQUIRED_ACTION_DATA_STALE" in deferred.reason_codes
    assert "REQUIRED_ACTION_AMBIGUOUS" not in deferred.reason_codes
    assert deferred.max_hold_deadline_monotonic_ns == deadline

    finished = no_recovery.finish(end_monotonic_ns=2_000_000_000)
    assert finished.status is CycleTerminalState.UNRESOLVED
    assert not finished.positions.authoritative
    assert finished.positions.paired_risex_quantity == D("0.60")
    assert finished.positions.paired_lighter_quantity == D("0.60")
    assert finished.max_hold_deadline_monotonic_ns == deadline
    assert not finished.cashflow_complete

    recovered = build_kernel()
    recovered.advance_clock(1_600_000_000)
    for venue, asks in (
        (Venue.RISEX, (("102", "10"),)),
        (Venue.LIGHTER, (("100", "10"),)),
    ):
        recovered.advance(
            _book(
                venue,
                received=1_700_000_000,
                revision=4,
                bids=(("99", "10"),),
                asks=asks,
            )
        )
    resumed = recovered.snapshot()
    assert resumed is not None
    assert resumed.status is CycleTerminalState.PENDING
    assert resumed.positions.paired_risex_quantity == D("0.60")
    assert resumed.positions.paired_lighter_quantity == D("0.60")
    assert any(
        action.action_id == "exit-maker"
        and action.status.value == "PENDING"
        for action in resumed.actions
    )
    assert resumed.max_hold_deadline_monotonic_ns == deadline


@pytest.mark.parametrize("scenario", (CycleScenario.PRIMARY, CycleScenario.STRESS))
@pytest.mark.parametrize(
    "fill_model",
    (CycleFillModel.TOUCH_ALLOWED, CycleFillModel.TRADE_THROUGH_ONLY),
)
def test_s1b_pair_rechecks_exit_readiness_on_recovered_book(
    scenario: CycleScenario,
    fill_model: CycleFillModel,
) -> None:
    lighter_market = replace(
        _market(Venue.LIGHTER, "BTC", minimum_quantity="0.30"),
        quantity_step_raw=D("0.01"),
    )
    version, source_books = _version(
        f"s1b-pair-book-recovery-{scenario.value}-{fill_model.value}",
        lighter_market=lighter_market,
    )
    kernel = Scv1S1bKernel(fill_model=fill_model)
    assert kernel.admit(version, scenario=scenario, source_books=source_books).accepted
    activation_ns = kernel.policy.delays(scenario).activation_delay_ns
    kernel.advance(
        _book(
            Venue.RISEX,
            received=activation_ns - 100_000_000,
            revision=2,
            bids=(
                ("99", "10"),
            ),
            asks=(
                ("105", "10"),
            ),
        ),
        scenario=scenario,
    )
    kernel.advance_clock(activation_ns, scenario=scenario)
    first_fill_ns = activation_ns + 100_000_000
    kernel.advance(
        _entry_trade(
            f"s1b-pair-book-recovery-entry-{scenario.value}-{fill_model.value}",
            first_fill_ns,
            "0.40",
            price="102",
        ),
        scenario=scenario,
    )
    hedge_due_ns = first_fill_ns + kernel.policy.delays(scenario).taker_delay_ns
    kernel.advance(
        _book(
            Venue.RISEX,
            received=hedge_due_ns - 100_000_000,
            revision=3,
            bids=(
                ("99", "10"),
            ),
            asks=(
                ("105", "10"),
            ),
        ),
        scenario=scenario,
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=hedge_due_ns - 100_000_000,
            revision=3,
            bids=(
                ("99", "0.20"),
            ),
            asks=(
                ("100", "10"),
            ),
        ),
        scenario=scenario,
    )
    kernel.advance_clock(hedge_due_ns, scenario=scenario)
    before = kernel.snapshot(scenario=scenario)
    assert before is not None
    assert before.positions.paired_risex_quantity == D("0.40")
    assert not any(action.action_id == "entry-cancel" for action in before.actions)

    recovery_ns = hedge_due_ns + 100_000_000
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=recovery_ns,
            revision=4,
            bids=(
                ("99", "10"),
            ),
            asks=(
                ("100", "10"),
            ),
        ),
        scenario=scenario,
    )
    recovered = kernel.snapshot(scenario=scenario)
    assert recovered is not None
    cancel = next(action for action in recovered.actions if action.action_id == "entry-cancel")
    assert cancel.status.value == "PENDING"


@pytest.mark.parametrize("scenario", (CycleScenario.PRIMARY, CycleScenario.STRESS))
@pytest.mark.parametrize(
    "fill_model",
    (CycleFillModel.TOUCH_ALLOWED, CycleFillModel.TRADE_THROUGH_ONLY),
)
def test_s1b_matured_exit_close_wakes_on_depth_recovery_without_new_fill(
    scenario: CycleScenario,
    fill_model: CycleFillModel,
) -> None:
    lighter_market = replace(
        _market(Venue.LIGHTER, "BTC", minimum_quantity="0.30"),
        quantity_step_raw=D("0.01"),
    )
    version, source_books = _version(
        f"s1b-exit-close-recovery-{scenario.value}-{fill_model.value}",
        lighter_market=lighter_market,
    )
    kernel = Scv1S1bKernel(fill_model=fill_model)
    assert kernel.admit(version, scenario=scenario, source_books=source_books).accepted
    activation_ns = version.decision_ready_monotonic_ns + kernel.policy.delays(scenario).activation_delay_ns
    kernel.advance(
        _book(
            Venue.RISEX,
            received=activation_ns - 100_000_000,
            revision=2,
            bids=(
                ("99", "10"),
            ),
            asks=(
                ("105", "10"),
            ),
        ),
        scenario=scenario,
    )
    kernel.advance_clock(activation_ns, scenario=scenario)
    entry_fill_ns = activation_ns + 100_000_000
    kernel.advance(
        _entry_trade(
            f"s1b-exit-close-recovery-entry-{scenario.value}-{fill_model.value}",
            entry_fill_ns,
            "0.80",
            price="102",
        ),
        scenario=scenario,
    )
    entry_due_ns = entry_fill_ns + kernel.policy.delays(scenario).taker_delay_ns
    kernel.advance(
        _book(
            Venue.RISEX,
            received=entry_due_ns - 100_000_000,
            revision=3,
            bids=(
                ("99", "10"),
            ),
            asks=(
                ("105", "10"),
            ),
        ),
        scenario=scenario,
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=entry_due_ns - 100_000_000,
            revision=3,
            bids=(
                ("99", "10"),
            ),
            asks=(
                ("100", "10"),
            ),
        ),
        scenario=scenario,
    )
    kernel.advance_clock(entry_due_ns, scenario=scenario)
    entry_cancel = next(
        action for action in kernel.snapshot(scenario=scenario).actions  # type: ignore[union-attr]
        if action.action_id == "entry-cancel"
    )
    cancel_effective_ns = entry_cancel.effective_monotonic_ns
    assert cancel_effective_ns is not None
    kernel.advance(
        _book(
            Venue.RISEX,
            received=cancel_effective_ns - 100_000_000,
            revision=4,
            bids=(
                ("99", "10"),
            ),
            asks=(
                ("105", "10"),
            ),
        ),
        scenario=scenario,
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=cancel_effective_ns - 100_000_000,
            revision=4,
            bids=(
                ("99", "10"),
            ),
            asks=(
                ("100", "10"),
            ),
        ),
        scenario=scenario,
    )
    kernel.advance_clock(cancel_effective_ns, scenario=scenario)
    exit_action = next(
        action for action in kernel.snapshot(scenario=scenario).actions  # type: ignore[union-attr]
        if action.action_id == "exit-maker"
    )
    exit_activation_ns = exit_action.effective_monotonic_ns
    assert exit_activation_ns is not None
    kernel.advance(
        _book(
            Venue.RISEX,
            received=exit_activation_ns - 100_000_000,
            revision=5,
            bids=(
                ("99", "10"),
            ),
            asks=(
                ("105", "10"),
            ),
        ),
        scenario=scenario,
    )
    exit_fill_ns = exit_activation_ns + 100_000_000
    kernel.advance(
        _trade(
            f"s1b-exit-close-recovery-fill-{scenario.value}-{fill_model.value}",
            received=exit_fill_ns,
            quantity="0.40",
            price="97",
            aggressor=Side.SELL,
        ),
        scenario=scenario,
    )
    close_due_ns = exit_fill_ns + kernel.policy.delays(scenario).taker_delay_ns
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=close_due_ns - 100_000_000,
            revision=5,
            bids=(
                ("99", "0.20"),
            ),
            asks=(
                ("100", "10"),
            ),
        ),
        scenario=scenario,
    )
    kernel.advance_clock(close_due_ns, scenario=scenario)
    before = kernel.snapshot(scenario=scenario)
    assert before is not None
    assert before.positions.paired_lighter_quantity == D("0.80")
    assert before.pending_exit_close_quantity == D("0.40")

    recovery_ns = close_due_ns + 100_000_000
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=recovery_ns,
            revision=6,
            bids=(
                ("99", "0.40"),
            ),
            asks=(
                ("100", "10"),
            ),
        ),
        scenario=scenario,
    )
    after = kernel.snapshot(scenario=scenario)
    assert after is not None
    assert after.positions.risex_signed_quantity == D("-0.40")
    assert after.positions.lighter_signed_quantity == D("0.40")
    assert after.pending_exit_close_quantity == D("0")
    assert sum(
        (fill.quantity for fill in after.fills if fill.action_id == "exit-close:0"),
        D("0"),
    ) == D("0.40")

    # A later maker partial gets its own reservation, but the unchanged
    # 0.40-depth witness cannot be consumed a second time.
    second_exit_fill_ns = recovery_ns + 100_000_000
    kernel.advance(
        _trade(
            f"s1b-exit-close-recovery-second-{scenario.value}-{fill_model.value}",
            received=second_exit_fill_ns,
            quantity="0.20",
            price="97",
            aggressor=Side.SELL,
        ),
        scenario=scenario,
    )
    second_close_due_ns = second_exit_fill_ns + kernel.policy.delays(scenario).taker_delay_ns
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=second_close_due_ns - 100_000_000,
            revision=7,
            bids=(
                ("99", "0.40"),
            ),
            asks=(
                ("100", "10"),
            ),
        ),
        scenario=scenario,
    )
    kernel.advance_clock(
        second_close_due_ns,
        scenario=scenario,
    )
    unchanged = kernel.snapshot(scenario=scenario)
    assert unchanged is not None
    assert unchanged.positions.paired_lighter_quantity == D("0.40")
    assert unchanged.pending_exit_close_quantity == D("0.20")
    assert sum(
        (fill.quantity for fill in unchanged.fills if fill.action_id.startswith("exit-close:")),
        D("0"),
    ) == D("0.40")


@pytest.mark.parametrize("scenario", (CycleScenario.PRIMARY, CycleScenario.STRESS))
@pytest.mark.parametrize(
    "fill_model",
    (CycleFillModel.TOUCH_ALLOWED, CycleFillModel.TRADE_THROUGH_ONLY),
)
def test_s1b_latest_unhealthy_book_invalidates_current_mark(
    scenario: CycleScenario,
    fill_model: CycleFillModel,
) -> None:
    version, source_books = _version(
        f"s1b-unhealthy-latest-mark-{scenario.value}-{fill_model.value}"
    )
    kernel = Scv1S1bKernel(fill_model=fill_model)
    assert kernel.admit(version, scenario=scenario, source_books=source_books).accepted
    activation_ns = version.decision_ready_monotonic_ns + kernel.policy.delays(scenario).activation_delay_ns
    kernel.advance(
        _book(
            Venue.RISEX,
            received=activation_ns - 100_000_000,
            revision=2,
            bids=(
                ("99", "10"),
            ),
            asks=(
                ("102", "10"),
            ),
        ),
        scenario=scenario,
    )
    kernel.advance_clock(activation_ns, scenario=scenario)
    fill_ns = activation_ns + 100_000_000
    kernel.advance(
        _entry_trade(
            f"s1b-unhealthy-latest-mark-fill-{scenario.value}-{fill_model.value}",
            fill_ns,
            "0.20",
            price="102",
        ),
        scenario=scenario,
    )
    healthy = kernel.snapshot(scenario=scenario)
    assert healthy is not None
    assert healthy.marked_risex_price == D("102")
    assert healthy.marked_risex_book_revision_id is not None
    assert healthy.marked_risex_book_revision_id.endswith("|2")

    kernel.advance(
        _book(
            Venue.RISEX,
            received=fill_ns + 100_000_000,
            revision=3,
            bids=(
                ("99", "10"),
            ),
            asks=(
                ("110", "10"),
            ),
            sequence_valid=False,
        ),
        scenario=scenario,
    )
    unhealthy = kernel.snapshot(scenario=scenario)
    assert unhealthy is not None
    assert unhealthy.marked_risex_price is None
    assert unhealthy.marked_inventory_usd is None
    assert unhealthy.marked_execution_only_pnl_usd is None
    assert unhealthy.marked_risex_book_revision_id is None


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


@pytest.mark.parametrize("fill_model", tuple(CycleFillModel))
def test_s1b_invalid_post_only_completed_cancel_preserves_reason(
    fill_model: CycleFillModel,
) -> None:
    scenario = CycleScenario.PRIMARY
    invalid_version, source_books = _version(
        f"s1b-invalid-post-only-{fill_model.value}"
    )
    kernel = Scv1S1bKernel(fill_model=fill_model)
    assert kernel.admit(
        invalid_version,
        scenario=scenario,
        source_books=source_books,
    ).accepted
    crossing_activation_ns = (
        invalid_version.decision_ready_monotonic_ns
        + kernel.policy.delays(scenario).activation_delay_ns
    )
    kernel.advance(
        _book(
            Venue.RISEX,
            received=crossing_activation_ns - 100_000_000,
            revision=2,
            bids=(("102", "10"),),
            asks=(("103", "10"),),
        ),
        scenario=scenario,
    )
    kernel.advance_clock(crossing_activation_ns, scenario=scenario)
    crossing = kernel.snapshot(scenario=scenario)
    assert crossing is not None
    invalid_cancel = next(
        action
        for action in crossing.actions
        if action.reason == "ENTRY_INVALIDATED_NO_ORDER"
    )
    assert invalid_cancel.status.value == "COMPLETED"
    assert invalid_cancel.requested_quantity == invalid_cancel.executed_quantity
    assert invalid_cancel.remaining_quantity == D("0")

    next_decision_ns = crossing_activation_ns + 1_000_000_000
    next_version, next_books = _fresh_version(
        f"s1b-invalid-post-only-next-{fill_model.value}",
        next_decision_ns,
        3,
    )
    assert kernel.admit(
        next_version,
        scenario=scenario,
        source_books=next_books,
    ).accepted
    next_activation_ns = (
        next_decision_ns + kernel.policy.delays(scenario).activation_delay_ns
    )
    kernel.advance(
        _book(
            Venue.RISEX,
            received=next_activation_ns - 100_000_000,
            revision=4,
            bids=(("99", "10"),),
            asks=(("102", "10"),),
        ),
        scenario=scenario,
    )
    kernel.advance_clock(next_activation_ns, scenario=scenario)

    # Re-enter the completed-action boundary after the crossing version was
    # invalidated.  This must preserve the truthful no-order reason while the
    # late-fill path still reconciles genuine cancellation actions.
    kernel.advance_clock(
        next_activation_ns
        + kernel.policy.entry_cancel_after_activation_ns
        + kernel.policy.delays(scenario).cancel_delay_ns,
        scenario=scenario,
    )
    after = kernel.snapshot(scenario=scenario)
    assert after is not None
    preserved = next(
        action for action in after.actions if action.action_id == invalid_cancel.action_id
    )
    assert preserved.status.value == "COMPLETED"
    assert preserved.reason == "ENTRY_INVALIDATED_NO_ORDER"
    assert preserved.requested_quantity == invalid_cancel.requested_quantity
    assert preserved.executed_quantity == invalid_cancel.executed_quantity
    assert preserved.remaining_quantity == invalid_cancel.remaining_quantity


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


def _independent_c_expected_price(
    cashflow: str,
    quantity: str,
    lighter_levels: tuple[tuple[str, str], ...],
    risex_best_ask: str,
    tick: str,
    stress: str,
) -> D:
    """Compute C's fixture expectation independently with stdlib Fraction."""

    q = Fraction(quantity)
    lighter_notional = sum(
        (Fraction(level_quantity) * Fraction(level_price) for level_price, level_quantity in lighter_levels),
        Fraction(0),
    )
    raw = (
        Fraction(cashflow) + lighter_notional
    ) / (q * (Fraction(10001, 10000) + Fraction(stress)))
    floor_units = (raw / Fraction(tick)).numerator // (raw / Fraction(tick)).denominator
    economic_floor = Fraction(floor_units) * Fraction(tick)
    post_only_cap = Fraction(risex_best_ask) - Fraction(tick)
    result = min(economic_floor, post_only_cap)
    return D(result.numerator) / D(result.denominator)


def _c_candidate_fixture(
    name: str,
    *,
    cashflow: str,
    quantity: str,
    lighter_levels: tuple[tuple[str, str], ...],
    risex_best_ask: str = "80100",
    tick: str = ".1",
    scenario: CycleScenario = CycleScenario.PRIMARY,
) -> tuple[Scv1S1bKernel, object]:
    risex_market = replace(
        _market(Venue.RISEX, "BTC/USDC", minimum_quantity=".0001"),
        tick_size_raw=D(tick),
        quantity_step_raw=D(".0001"),
    )
    lighter_market = replace(
        _market(Venue.LIGHTER, "BTC", minimum_quantity=".0001"),
        tick_size_raw=D(tick),
        quantity_step_raw=D(".0001"),
    )
    version, source_books = _version(
        name,
        decision_ready=0,
        risex_bids=(("80000", "10"),),
        risex_asks=(("80100", "10"),),
        lighter_bids=lighter_levels,
        lighter_asks=(("80000", "10"),),
        risex_market=risex_market,
        lighter_market=lighter_market,
    )
    version = replace(
        version,
        quote=replace(
            version.quote,
            risex_tick_size=D(tick),
            post_only_bound_price=D("80000.1"),
        ),
    )
    kernel = Scv1S1bKernel(
        fill_model=CycleFillModel.TOUCH_ALLOWED,
        exit_variant=S1bExitVariant.C_BE_REPRICE_V1,
    )
    assert kernel.admit(version, scenario=scenario, source_books=source_books).accepted
    cycle = kernel._cycle(scenario)
    assert cycle is not None
    if risex_best_ask != "80100":
        kernel.advance(
            _book(
                Venue.RISEX,
                received=0,
                revision=2,
                bids=(("80000", "10"),),
                asks=((risex_best_ask, "10"),),
            ),
            scenario=scenario,
        )
    cycle.paired_risex_quantity = D(quantity)
    cycle.paired_lighter_quantity = D(quantity)
    cycle.risex_signed_quantity = -D(quantity)
    cycle.lighter_signed_quantity = D(quantity)
    cycle.entry_observed_quantity = D(quantity)
    cycle.hedged_quantity = D(quantity)
    cycle.cashflows = [SimpleNamespace(net_cashflow_usd=D(cashflow))]
    return kernel, cycle


@pytest.mark.parametrize(
    ("cashflow", "quantity", "lighter_levels", "risex_best_ask", "scenario", "expected"),
    (
        ("0.011998", ".001", (("79995", ".0004"), ("79985", ".0006")), "80100", CycleScenario.PRIMARY, "79992.9"),
        ("0.003996", ".001", (("79995", ".0004"), ("79985", ".0006")), "80100", CycleScenario.STRESS, "79977"),
        ("0.00479832", ".0006", (("79985", ".0006"),), "80100", CycleScenario.PRIMARY, "79984.9"),
        ("-0.00640336", ".0006", (("79985", ".0006"),), "80100", CycleScenario.STRESS, "79958.3"),
        ("0.011998", ".001", (("79995", ".0004"), ("79985", ".0006")), "79990", CycleScenario.PRIMARY, "79989.9"),
    ),
)
def test_s1b_c_candidate_matches_independent_fraction_cases(
    cashflow: str,
    quantity: str,
    lighter_levels: tuple[tuple[str, str], ...],
    risex_best_ask: str,
    scenario: CycleScenario,
    expected: str,
) -> None:
    kernel, cycle = _c_candidate_fixture(
        f"c-arithmetic-{expected}-{scenario.value}",
        cashflow=cashflow,
        quantity=quantity,
        lighter_levels=lighter_levels,
        risex_best_ask=risex_best_ask,
        scenario=scenario,
    )
    candidate, reason, detail = kernel._s1b_c_exit_quote_candidate(
        cycle,
        0,
        sequence=0,
    )
    assert reason is None and detail is None
    assert candidate is not None
    assert candidate.price == _independent_c_expected_price(
        cashflow,
        quantity,
        lighter_levels,
        risex_best_ask,
        ".1",
        "0" if scenario is CycleScenario.PRIMARY else ".0001",
    )
    assert candidate.price == D(expected)
    assert candidate.quantity == D(quantity)
    assert candidate.source_book_revision_id is not None
    assert candidate.hedge_source_book_revision_id is not None


def _c_active_kernel(name: str) -> tuple[Scv1S1bKernel, int, int]:
    version, source_books = _version(name, decision_ready=0)
    kernel = Scv1S1bKernel(
        fill_model=CycleFillModel.TOUCH_ALLOWED,
        exit_variant=S1bExitVariant.C_BE_REPRICE_V1,
    )
    assert kernel.admit(version, source_books=source_books).accepted
    kernel.advance(
        _book(
            Venue.RISEX,
            received=400_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("102", "10"),),
        )
    )
    kernel.advance_clock(500_000_000)
    cycle = kernel._cycle(CycleScenario.PRIMARY)
    assert cycle is not None and cycle.active_entry_version is not None
    kernel.advance(
        _entry_trade(
            f"{name}-entry",
            1_000_000_000,
            ".20",
            price=str(cycle.active_entry_version.quote.price),
        )
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=1_400_000_000,
            revision=2,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        )
    )
    kernel.advance(
        _book(
            Venue.RISEX,
            received=1_400_000_000,
            revision=3,
            bids=(("99", "10"),),
            asks=(("102", "10"),),
        )
    )
    kernel.advance_clock(1_500_000_000)
    kernel.advance(
        _book(
            Venue.RISEX,
            received=1_900_000_000,
            revision=4,
            bids=(("99", "10"),),
            asks=(("102", "10"),),
        )
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=1_900_000_000,
            revision=4,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        )
    )
    kernel.advance_clock(2_000_000_000)
    kernel.advance(
        _book(
            Venue.RISEX,
            received=2_400_000_000,
            revision=5,
            bids=(("99", "10"),),
            asks=(("102", "10"),),
        )
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=2_400_000_000,
            revision=5,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        )
    )
    kernel.advance_clock(2_500_000_000)
    cycle = kernel._cycle(CycleScenario.PRIMARY)
    assert cycle is not None
    assert cycle.phase.value == "EXIT_ACTIVE"
    return kernel, 2_500_000_000, 121_000_000_000


def test_s1b_c_initial_exit_is_causal_and_closes_flat_with_exact_ledger() -> None:
    kernel, exit_activation_ns, _ = _c_active_kernel("c-initial")
    cycle = kernel._cycle(CycleScenario.PRIMARY)
    assert cycle is not None and cycle.active_exit_version is not None
    version = cycle.active_exit_version
    assert version.sequence == 0
    assert version.quote.source_book_revision_id is not None
    assert version.quote.hedge_source_book_revision_id is not None
    kernel.advance(
        _trade(
            "c-initial-exit",
            received=exit_activation_ns + 100_000_000,
            quantity=".20",
            price=str(version.quote.price),
            aggressor=Side.SELL,
        )
    )
    pending_close = kernel.snapshot()
    assert pending_close is not None
    assert pending_close.positions.risex_signed_quantity == D("0")
    assert pending_close.positions.lighter_signed_quantity == D("0.20")
    assert [fill.action_id for fill in pending_close.fills if fill.action_id.startswith("exit-")] == [
        "exit-maker",
    ]
    assert any(action.action_id == "exit-close:0" for action in pending_close.pending_actions)
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=3_000_000_000,
            revision=6,
            bids=(("99", "10"),),
            asks=(("100", "10"),),
        )
    )
    kernel.advance_clock(3_100_000_000)
    result = kernel.snapshot()
    assert result is not None
    assert result.status is CycleTerminalState.NORMAL
    assert result.is_flat
    assert result.cashflow_complete
    assert result.exit_variant == S1bExitVariant.C_BE_REPRICE_V1.value
    assert result.exit_versions[0]["activation_post_only"] is True
    assert result.exit_versions[0]["fills"][0]["source_event_id"]
    assert result.ledger.turnover_usd == sum((fill.notional_usd for fill in result.fills), D("0"))


def test_s1b_c_reprice_waits_for_cancel_and_new_activation_without_overlap() -> None:
    kernel, _, _ = _c_active_kernel("c-reprice-boundaries")
    kernel.advance(
        _book(
            Venue.RISEX,
            received=2_900_000_000,
            revision=6,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        )
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=2_900_000_000,
            revision=6,
            bids=(("101", "10"),),
            asks=(("102", "10"),),
        )
    )
    kernel.advance_clock(3_000_000_000)
    cycle = kernel._cycle(CycleScenario.PRIMARY)
    assert cycle is not None
    assert cycle.phase.value == "EXIT_CANCEL_WAIT"
    assert cycle.exit_reprice_pending
    assert len(cycle.exit_versions) == 1
    assert cycle.exit_versions[0].cancel_effective_ns == 3_500_000_000
    assert any(item["outcome"] == "REPRICE_REQUESTED" for item in cycle.exit_reprice_decisions)
    kernel.advance_clock(3_500_000_000)
    assert cycle.phase.value == "EXIT_REPRICE_WAIT"
    assert cycle.active_exit_version is None
    kernel.advance(
        _book(
            Venue.RISEX,
            received=3_900_000_000,
            revision=7,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        )
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=3_900_000_000,
            revision=7,
            bids=(("101", "10"),),
            asks=(("102", "10"),),
        )
    )
    kernel.advance_clock(4_000_000_000)
    assert cycle.active_exit_version is not None
    replacement = cycle.active_exit_version
    assert replacement.sequence == 1
    assert replacement.activation_ns == 4_500_000_000
    assert cycle.exit_versions[0].cancel_effective_ns == 3_500_000_000
    kernel.advance(
        _trade(
            "c-reprice-before-activation",
            received=4_100_000_000,
            quantity=".20",
            price=str(replacement.quote.price),
            aggressor=Side.SELL,
        )
    )
    assert replacement.observed_quantity == D("0")
    kernel.advance(
        _book(
            Venue.RISEX,
            received=4_400_000_000,
            revision=8,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        )
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=4_400_000_000,
            revision=8,
            bids=(("101", "10"),),
            asks=(("102", "10"),),
        )
    )
    kernel.advance_clock(4_500_000_000)
    assert replacement.activation_post_only is True
    kernel.advance(
        _trade(
            "c-reprice-after-activation",
            received=4_600_000_000,
            quantity=".20",
            price=str(replacement.quote.price),
            aggressor=Side.SELL,
        )
    )
    result = kernel.snapshot()
    assert result is not None
    assert [fill.action_id for fill in result.fills if fill.action_id.startswith("exit-maker")] == [
        "exit-maker:1",
    ]
    assert [version["version_id"] for version in result.exit_versions] == [
        "c-reprice-boundaries:exit",
        "c-reprice-boundaries:exit:1",
    ]
    assert all(
        version["cancel_effective_ns"] is not None
        for version in result.exit_versions[:1]
    )


def test_s1b_c_old_quote_fill_is_valid_before_cancel_but_not_after_effective_boundary() -> None:
    kernel, _, _ = _c_active_kernel("c-old-race")
    kernel.advance(
        _book(
            Venue.RISEX,
            received=2_900_000_000,
            revision=6,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        )
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=2_900_000_000,
            revision=6,
            bids=(("101", "10"),),
            asks=(("102", "10"),),
        )
    )
    kernel.advance_clock(3_000_000_000)
    old_trade = _trade(
        "c-old-race-before-cancel",
        received=3_200_000_000,
        quantity=".10",
        price="99",
        aggressor=Side.SELL,
    )
    kernel.advance(old_trade)
    cycle = kernel._cycle(CycleScenario.PRIMARY)
    assert cycle is not None and cycle.exit_versions[0].observed_quantity == D(".10")
    fills_before_boundary = len(cycle.exit_fills)
    kernel.advance_clock(3_500_000_000)
    kernel.advance(
        _trade(
            "c-old-race-after-cancel",
            received=3_600_000_000,
            quantity=".10",
            price="99",
            aggressor=Side.SELL,
        )
    )
    assert len(cycle.exit_fills) == fills_before_boundary
    assert cycle.active_exit_version is None
    assert cycle.phase.value == "EXIT_REPRICE_WAIT"


def test_s1b_c_late_old_quote_fill_reconciles_completed_cancel_quantity() -> None:
    kernel, _, _ = _c_active_kernel("c-old-late")
    kernel.advance(
        _book(
            Venue.RISEX,
            received=2_900_000_000,
            revision=6,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        )
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=2_900_000_000,
            revision=6,
            bids=(("101", "10"),),
            asks=(("102", "10"),),
        )
    )
    kernel.advance_clock(3_000_000_000)
    kernel.advance_clock(3_500_000_000)

    kernel.advance(
        _trade(
            "c-old-late-after-cancel",
            received=3_200_000_000,
            normalized=3_600_000_000,
            quantity=".10",
            price="99",
            aggressor=Side.SELL,
        )
    )
    cycle = kernel._cycle(CycleScenario.PRIMARY)
    assert cycle is not None
    assert cycle.exit_versions[0].observed_quantity == D(".10")
    cancel = next(
        action
        for action in cycle.actions
        if action.action_id == cycle.exit_versions[0].cancel_action_id
    )
    assert cancel.status.value == "COMPLETED"
    assert cancel.requested_quantity == D(".10")
    assert cancel.executed_quantity == D(".10")
    assert cancel.remaining_quantity == D("0")
    assert len(cycle.exit_fills) == 1
    assert cycle.phase.value == "EXIT_REPRICE_WAIT"


def test_s1b_c_stale_books_retain_existing_quote_without_reprice_cancel() -> None:
    kernel, _, _ = _c_active_kernel("c-stale-reprice")
    cycle = kernel._cycle(CycleScenario.PRIMARY)
    assert cycle is not None and cycle.active_exit_version is not None
    old = cycle.active_exit_version
    kernel.advance_clock(3_000_000_000)
    assert cycle.active_exit_version is old
    assert old.cancel_requested_ns is None
    decision = cycle.exit_reprice_decisions[-1]
    assert decision["outcome"] == "RETAINED"
    assert decision["reason"] == CycleReason.REQUIRED_ACTION_DATA_STALE.value


def test_s1b_c_deadline_wins_over_pending_reprice_cancel_without_duplicate_cancel() -> None:
    kernel, _, deadline_ns = _c_active_kernel("c-deadline-reprice")
    kernel.advance(
        _book(
            Venue.RISEX,
            received=119_900_000_000,
            revision=6,
            bids=(("99", "10"),),
            asks=(("105", "10"),),
        )
    )
    kernel.advance(
        _book(
            Venue.LIGHTER,
            received=119_900_000_000,
            revision=6,
            bids=(("101", "10"),),
            asks=(("102", "10"),),
        )
    )
    kernel.advance_clock(120_000_000_000)
    cycle = kernel._cycle(CycleScenario.PRIMARY)
    assert cycle is not None
    assert cycle.phase.value == "EXIT_CANCEL_WAIT"
    assert cycle.exit_reprice_pending
    kernel.advance_clock(deadline_ns)
    result = kernel.snapshot()
    assert result is not None
    assert cycle.deadline_forced
    assert cycle.max_hold_deadline_ns == deadline_ns
    assert cycle.phase.value == "FORCE_WAIT"
    exit_cancels = [action for action in result.actions if action.action_id == "exit-cancel"]
    assert len(exit_cancels) == 1
    assert exit_cancels[0].requested_monotonic_ns == 120_000_000_000
    assert exit_cancels[0].effective_monotonic_ns == 120_500_000_000
    assert exit_cancels[0].status.value == "COMPLETED"
    assert "MAX_HOLD" in result.reason_codes


def test_s1b_c_outside_domain_is_explicit_and_initial_invalid_proposal_uses_baseline() -> None:
    kernel, cycle = _c_candidate_fixture(
        "c-outside-domain",
        cashflow="0.011998",
        quantity=".001",
        lighter_levels=(("79995", ".0004"), ("79985", ".0006")),
    )
    cycle.unmatched_entry_quantity = D(".0001")
    before = (cycle.paired_risex_quantity, cycle.paired_lighter_quantity)
    candidate, reason, detail = kernel._s1b_c_exit_quote_candidate(cycle, 0, sequence=0)
    assert candidate is None
    assert reason is CycleReason.C_OUTSIDE_BALANCED_DOMAIN
    assert detail == "UNMATCHED_RESIDUE"
    assert (cycle.paired_risex_quantity, cycle.paired_lighter_quantity) == before

    version, books = _version("c-baseline-fallback", decision_ready=0)
    fallback_kernel = Scv1S1bKernel(
        fill_model=CycleFillModel.TOUCH_ALLOWED,
        exit_variant=S1bExitVariant.C_BE_REPRICE_V1,
    )
    assert fallback_kernel.admit(version, source_books=books).accepted
    fallback_cycle = fallback_kernel._cycle(CycleScenario.PRIMARY)
    assert fallback_cycle is not None
    fallback_cycle.paired_risex_quantity = D(".20")
    fallback_cycle.paired_lighter_quantity = D(".20")
    fallback_cycle.risex_signed_quantity = D("-.20")
    fallback_cycle.lighter_signed_quantity = D(".20")
    fallback_cycle.entry_observed_quantity = D(".20")
    fallback_cycle.hedged_quantity = D(".20")
    fallback_cycle.cashflows = [SimpleNamespace(net_cashflow_usd=D("-1000"))]
    fallback_kernel._prepare_exit(fallback_cycle, 0)
    assert fallback_cycle.active_exit_version is not None
    assert fallback_cycle.active_exit_version.sequence == 0
    assert fallback_cycle.active_exit_version.quote.price == D("98")
    assert CycleReason.C_REPRICE_INVALID.value in fallback_cycle.reasons
    assert fallback_cycle.exit_reprice_decisions[-1]["outcome"] == "BASELINE_FALLBACK"
