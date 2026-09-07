"""SCV1-S1b connected episode kernel and offline D1 replay.

The accepted S1a adapter intentionally exercises the historical S2 cycle
spine.  S1b needs a slightly different episode contract: an immutable cap,
sequential entry versions, independently scheduled hedge reservations, an
entry cancellation barrier, and marked residual inventory.  This module
keeps that state machine versioned and uses the existing causal event,
quantity-grid, exact VWAP, fee, and book-selection helpers.  The default
``CycleKernel`` remains the legacy/S1a-compatible implementation.

All functions in this module are offline-only.  The D1 reader consumes the
already persisted ``CYCLE_STREAM_INPUT`` clock/event records and immutable
run configuration.  Historical ``CYCLE_DECISION``/``CYCLE_ADMISSION``/
``CYCLE_FINAL_RESULT`` records are audit context, never new-policy
instructions.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from decimal import Decimal, ROUND_FLOOR
from enum import StrEnum
import hashlib
import json
from math import gcd
import os
from pathlib import Path
from typing import Any

from risex_farmer.economics import exact_quantity_vwap
from risex_farmer.models import ExactVwap, LiquidityRole

from .causal import (
    CausalEvent,
    CausalEventDecision,
    CausalEventKind,
    CausalFill,
    CausalOutcome,
    CausalQuoteMeasurement,
    CausalRestingQuote,
    CausalSourceIdentity,
    CausalTimingDiagnostics,
    CausalUncertainty,
    build_causal_resting_quote,
)
from .cycle import (
    CycleAction,
    CycleActionKind,
    CycleActionStatus,
    CycleAdmission,
    CycleAdmissionError,
    CycleAttempt,
    CycleClock,
    CycleDelays,
    CycleFill,
    CycleFillModel,
    CycleKernel,
    CycleKernelState,
    CycleLedger,
    CyclePolicy,
    CyclePositions,
    CycleReason,
    CycleResult,
    CycleScenario,
    CycleTerminalState,
    _BookObservation,
    _MutableAction,
    _ZERO,
    _add_action,
    _append_fill,
    _book_gap_blocks,
    _canonical_decimal_text,
    _coerce_event,
    _entry_input_failure,
    _event_identity_key,
    _event_signature,
    _floor_quantity,
    _make_causal_measurement,
    _minimum_ok_with_notional,
    _paired_books,
    _processing_ready_ns,
    _schedule_taker,
    _select_book,
    _stream_position,
    _text,
    _trade_crosses,
    _trade_price_is_tick_aligned,
    _venue_quantity_step,
)
from .economics import build_hypothetical_maker_quote
from .models import (
    BookEvidence,
    DataGapEvidence,
    QuotePolicy,
    QuoteVersion,
    Side,
    SpreadDirection,
    TradeEvidence,
    Venue,
)


SCV1_S1B_CONTRACT_VERSION = "SCV1-1.1-S1b"
_ONE = Decimal("1")
_MAX_TIME = 10**30
_POLICY_BLOCKED_MINIMUM = "POLICY_BLOCKED_MINIMUM"
_POLICY_BLOCKED_RESIDUAL = "POLICY_BLOCKED_RESIDUAL"
_ENTRY_DEFERRED_REASONS = frozenset(
    {
        CycleReason.INSUFFICIENT_DEPTH.value,
        CycleReason.GRID_RESIDUE.value,
        CycleReason.MINIMUM_RESIDUE.value,
    }
)


def _s1b_hedge_book_signature(book: BookEvidence) -> tuple[tuple[str, str], ...]:
    """Identify the Lighter ask state relevant to a deferred entry hedge.

    Revision/timestamp metadata is intentionally excluded.  A new revision
    with the same executable asks is not a changed liquidity witness and must
    not make an already-consumed depth executable a second time.  Price and
    quantity changes remain visible, including price-only notional recovery.
    """

    return tuple(
        (
            _canonical_decimal_text(level.canonical_price),
            _canonical_decimal_text(level.canonical_quantity),
        )
        for level in book.asks
    )


def _s1b_close_book_signature(book: BookEvidence) -> tuple[tuple[str, str], ...]:
    """Identify the Lighter bid state relevant to an exit close."""

    return tuple(
        (
            _canonical_decimal_text(level.canonical_price),
            _canonical_decimal_text(level.canonical_quantity),
        )
        for level in book.bids
    )


def _s1b_levels_after_consumed(
    levels: Iterable[Any],
    consumed_quantity: Decimal,
) -> tuple[Any, ...]:
    """Return the visible levels after this cycle's own prior consumption.

    A book observation is an aggregate liquidity witness.  Once this cycle
    has consumed part of that exact state, a later reservation may use only
    its remaining depth; a new price/quantity signature resets the witness.
    """

    materialized = tuple(levels)
    if consumed_quantity <= _ZERO:
        return materialized
    remaining_to_skip = consumed_quantity
    result: list[Any] = []
    for level in materialized:
        quantity = level.canonical_quantity
        if quantity <= _ZERO:
            result.append(level)
            continue
        if remaining_to_skip >= quantity:
            remaining_to_skip -= quantity
            continue
        if remaining_to_skip > _ZERO:
            result.append(
                replace(
                    level,
                    canonical_quantity=quantity - remaining_to_skip,
                )
            )
            remaining_to_skip = _ZERO
        else:
            result.append(level)
    return tuple(result)


def _joint_operation_step(left: Decimal, right: Decimal) -> Decimal | None:
    """Return the smallest positive quantity valid on both decimal grids."""

    if left <= _ZERO or right <= _ZERO or not left.is_finite() or not right.is_finite():
        return None
    scale = max(0, -left.as_tuple().exponent, -right.as_tuple().exponent)
    scale_factor = 10**scale
    left_units = int(left * scale_factor)
    right_units = int(right * scale_factor)
    if left_units <= 0 or right_units <= 0:
        return None
    common_units = (left_units // gcd(left_units, right_units)) * right_units
    return Decimal(common_units).scaleb(-scale)


class _S1bPhase(StrEnum):
    ENTRY_WAIT = "ENTRY_WAIT"
    ENTRY_ACTIVE = "ENTRY_ACTIVE"
    ENTRY_CANCEL_WAIT = "ENTRY_CANCEL_WAIT"
    ENTRY_REQUOTE_WAIT = "ENTRY_REQUOTE_WAIT"
    ENTRY_BARRIER = "ENTRY_BARRIER"
    UNMATCHED_WAIT = "UNMATCHED_WAIT"
    EXIT_WAIT = "EXIT_WAIT"
    EXIT_ACTIVE = "EXIT_ACTIVE"
    EXIT_CANCEL_WAIT = "EXIT_CANCEL_WAIT"
    CLOSE_WAIT = "CLOSE_WAIT"
    FORCE_WAIT = "FORCE_WAIT"
    COMPLETE = "COMPLETE"
    ABORTED = "ABORTED"
    UNRESOLVED = "UNRESOLVED"


@dataclass(frozen=True, slots=True)
class S1bCycleResult(CycleResult):
    """CycleResult plus the S1b cap, pending, mark, and block witnesses."""

    q_cap: Decimal = _ZERO
    entry_version_ids: tuple[str, ...] = ()
    active_entry_version_id: str | None = None
    pending_entry_hedge_quantity: Decimal = _ZERO
    pending_exit_close_quantity: Decimal = _ZERO
    marked_risex_price: Decimal | None = None
    marked_lighter_price: Decimal | None = None
    marked_inventory_usd: Decimal | None = None
    marked_execution_only_pnl_usd: Decimal | None = None
    executable_liquidating_close_estimate_usd: Decimal | None = None
    policy_blocked: bool = False
    observation_end_monotonic_ns: int | None = None
    fill_model: CycleFillModel = CycleFillModel.TRADE_THROUGH_ONLY
    marked_at_monotonic_ns: int | None = None
    marked_risex_book_revision_id: str | None = None
    marked_lighter_book_revision_id: str | None = None
    marked_risex_book_received_monotonic_ns: int | None = None
    marked_lighter_book_received_monotonic_ns: int | None = None
    simulation_active_duration_ns: int | None = None
    blocked_duration_ns: int | None = None

    def __post_init__(self) -> None:
        # ``super()`` without arguments is not reliable in a slotted
        # dataclass subclass on the supported Python 3.11 runtime.
        CycleResult.__post_init__(self)
        if self.q_cap < _ZERO or not self.q_cap.is_finite():
            raise ValueError("q_cap must be a finite non-negative Decimal")
        if not isinstance(self.entry_version_ids, tuple):
            raise TypeError("entry_version_ids must be a tuple")
        if any(not isinstance(value, str) or not value for value in self.entry_version_ids):
            raise ValueError("entry_version_ids must contain non-empty strings")
        for value, name in (
            (self.pending_entry_hedge_quantity, "pending_entry_hedge_quantity"),
            (self.pending_exit_close_quantity, "pending_exit_close_quantity"),
        ):
            if not isinstance(value, Decimal) or not value.is_finite() or value < _ZERO:
                raise ValueError(f"{name} must be a finite non-negative Decimal")
        for value, name in (
            (self.marked_risex_price, "marked_risex_price"),
            (self.marked_lighter_price, "marked_lighter_price"),
            (self.marked_inventory_usd, "marked_inventory_usd"),
            (self.marked_execution_only_pnl_usd, "marked_execution_only_pnl_usd"),
            (
                self.executable_liquidating_close_estimate_usd,
                "executable_liquidating_close_estimate_usd",
            ),
        ):
            if value is not None and (not isinstance(value, Decimal) or not value.is_finite()):
                raise ValueError(f"{name} must be a finite Decimal or None")
        if not isinstance(self.policy_blocked, bool):
            raise TypeError("policy_blocked must be bool")
        model = self.fill_model
        if not isinstance(model, CycleFillModel):
            model = CycleFillModel(model)
            object.__setattr__(self, "fill_model", model)
        if self.observation_end_monotonic_ns is not None and (
            isinstance(self.observation_end_monotonic_ns, bool)
            or not isinstance(self.observation_end_monotonic_ns, int)
            or self.observation_end_monotonic_ns < 0
        ):
            raise ValueError("observation_end_monotonic_ns must be a non-negative integer or None")
        for value, name in (
            (self.marked_at_monotonic_ns, "marked_at_monotonic_ns"),
            (
                self.marked_risex_book_received_monotonic_ns,
                "marked_risex_book_received_monotonic_ns",
            ),
            (
                self.marked_lighter_book_received_monotonic_ns,
                "marked_lighter_book_received_monotonic_ns",
            ),
            (self.simulation_active_duration_ns, "simulation_active_duration_ns"),
            (self.blocked_duration_ns, "blocked_duration_ns"),
        ):
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 0
            ):
                raise ValueError(f"{name} must be a non-negative integer or None")
        for value, name in (
            (self.marked_risex_book_revision_id, "marked_risex_book_revision_id"),
            (self.marked_lighter_book_revision_id, "marked_lighter_book_revision_id"),
        ):
            if value is not None and (not isinstance(value, str) or not value):
                raise ValueError(f"{name} must be a non-empty string or None")

    @property
    def q_cap_quantity(self) -> Decimal:
        return self.q_cap

    @property
    def cap_quantity(self) -> Decimal:
        return self.q_cap

    @property
    def pending_hedge_quantity(self) -> Decimal:
        return self.pending_entry_hedge_quantity

    @property
    def pending_close_quantity(self) -> Decimal:
        return self.pending_exit_close_quantity

    @property
    def marked_pnl_usd(self) -> Decimal | None:
        return self.marked_execution_only_pnl_usd

    @property
    def contract_version(self) -> str:
        return SCV1_S1B_CONTRACT_VERSION

    @property
    def key(self) -> str:
        return f"{self.fill_model.value}:{self.scenario.value}"

    @property
    def result(self) -> S1bCycleResult:
        """Compatibility view for callers that consume S1a alternatives."""

        return self


@dataclass(slots=True)
class _S1bEntryVersion:
    quote_version: QuoteVersion
    quote: CausalRestingQuote
    sequence: int
    target_quantity: Decimal
    activation_ns: int
    cancel_schedule_ns: int
    maker_action_id: str
    cancel_action_id: str
    cancel_requested_ns: int | None = None
    cancel_effective_ns: int | None = None
    observed_quantity: Decimal = _ZERO
    remaining_quantity: Decimal = _ZERO
    fills: list[CausalFill] = field(default_factory=list)
    decisions: list[CausalEventDecision] = field(default_factory=list)
    uncertainty: list[str] = field(default_factory=list)
    activation_checked: bool = False
    activation_post_only: bool | None = None


@dataclass(slots=True)
class _S1bCycle:
    initial_quote_version: QuoteVersion
    quote_version: QuoteVersion
    scenario: CycleScenario
    policy: CyclePolicy
    delays: CycleDelays
    entry_quote: CausalRestingQuote
    entry_measurement_quote: CausalRestingQuote
    phase: _S1bPhase
    current_ns: int
    entry_versions: list[_S1bEntryVersion] = field(default_factory=list)
    active_entry_version: _S1bEntryVersion | None = None
    q_cap: Decimal = _ZERO
    entry_target_quantity: Decimal = _ZERO
    entry_observed_quantity: Decimal = _ZERO
    entry_remaining_quantity: Decimal = _ZERO
    entry_fills: list[CausalFill] = field(default_factory=list)
    entry_decisions: list[CausalEventDecision] = field(default_factory=list)
    entry_uncertainty: list[str] = field(default_factory=list)
    entry_block_fence: int | None = None
    entry_cancel_requested_ns: int | None = None
    entry_cancel_effective_ns: int | None = None
    exit_quote: CausalRestingQuote | None = None
    exit_price: Decimal | None = None
    exit_activation_ns: int | None = None
    exit_cancel_requested_ns: int | None = None
    exit_cancel_effective_ns: int | None = None
    exit_activation_checked: bool = False
    exit_activation_post_only: bool | None = None
    exit_remaining_quantity: Decimal = _ZERO
    exit_target_quantity: Decimal = _ZERO
    exit_fills: list[CausalFill] = field(default_factory=list)
    exit_decisions: list[CausalEventDecision] = field(default_factory=list)
    exit_uncertainty: list[str] = field(default_factory=list)
    fills: list[CycleFill] = field(default_factory=list)
    fees: list[Any] = field(default_factory=list)
    cashflows: list[Any] = field(default_factory=list)
    actions: list[_MutableAction] = field(default_factory=list)
    scheduled_takers: dict[str, tuple[Venue, Side, Decimal, str]] = field(default_factory=dict)
    reasons: list[str] = field(default_factory=list)
    books: list[_BookObservation] = field(default_factory=list)
    book_identity_keys: set[tuple[Any, ...]] = field(default_factory=set)
    initial_book_signatures: dict[tuple[Any, ...], tuple[Any, ...]] = field(default_factory=dict)
    book_revision_signatures: dict[str, tuple[Any, ...]] = field(default_factory=dict)
    displaced_book_venues: set[Venue] = field(default_factory=set)
    gaps: list[DataGapEvidence] = field(default_factory=list)
    seen_events: dict[tuple[Any, ...], tuple[Any, ...]] = field(default_factory=dict)
    last_stream_time: dict[tuple[Any, ...], int] = field(default_factory=dict)
    last_stream_position: dict[tuple[Any, ...], tuple[int, ...]] = field(default_factory=dict)
    event_count: int = 0
    duplicate_event_count: int = 0
    ignored_event_count: int = 0
    first_maker_fill_ns: int | None = None
    max_hold_deadline_ns: int | None = None
    unmatched_entry_quantity: Decimal = _ZERO
    initial_unmatched_quantity: Decimal = _ZERO
    unmatched_started_ns: int | None = None
    unmatched_resolved_ns: int | None = None
    hedged_quantity: Decimal = _ZERO
    paired_risex_quantity: Decimal = _ZERO
    paired_lighter_quantity: Decimal = _ZERO
    risex_signed_quantity: Decimal = _ZERO
    lighter_signed_quantity: Decimal = _ZERO
    forced_used: bool = False
    deadline_forced: bool = False
    unresolved: bool = False
    policy_blocked: bool = False
    exit_chosen: bool = False
    exit_commitment_ns: int | None = None
    terminal_ns: int | None = None
    observation_end_ns: int | None = None
    blocked_started_ns: int | None = None
    entry_version_serial: int = 0
    entry_hedge_serial: int = 0
    exit_close_serial: int = 0
    entry_hedge_deferred_depth: dict[str, Decimal] = field(default_factory=dict)
    entry_hedge_deferred_book_signature: dict[str, tuple[tuple[str, str], ...]] = field(
        default_factory=dict
    )
    entry_hedge_consumed_book_signature: tuple[tuple[str, str], ...] | None = None
    entry_hedge_consumed_quantity: Decimal = _ZERO
    exit_close_consumed_book_signature: tuple[tuple[str, str], ...] | None = None
    exit_close_consumed_quantity: Decimal = _ZERO

    def add_reason(self, reason: CycleReason | str) -> None:
        value = reason.value if isinstance(reason, CycleReason) else str(reason)
        if value not in self.reasons:
            self.reasons.append(value)

    @property
    def positions(self) -> CyclePositions:
        return CyclePositions(
            self.risex_signed_quantity,
            self.lighter_signed_quantity,
            self.paired_risex_quantity,
            self.paired_lighter_quantity,
            self.unmatched_entry_quantity,
            authoritative=not self.unresolved,
        )


def _s1b_pending_actions(
    cycle: _S1bCycle,
    *kinds: CycleActionKind,
) -> tuple[_MutableAction, ...]:
    allowed = set(kinds)
    return tuple(
        action
        for action in cycle.actions
        if action.status is CycleActionStatus.PENDING
        and action.action_id in cycle.scheduled_takers
        and (not allowed or action.kind in allowed)
    )


def _s1b_pending_quantity(cycle: _S1bCycle, *kinds: CycleActionKind) -> Decimal:
    return sum((action.remaining_quantity for action in _s1b_pending_actions(cycle, *kinds)), _ZERO)


def _s1b_action(cycle: _S1bCycle, action_id: str) -> _MutableAction:
    for action in cycle.actions:
        if action.action_id == action_id:
            return action
    raise KeyError(action_id)


def _s1b_set_action(
    cycle: _S1bCycle,
    action: _MutableAction,
    *,
    status: CycleActionStatus,
    executed: Decimal,
    reason: str,
    evidence_id: str | None = None,
) -> None:
    action.status = status
    action.executed_quantity = executed
    action.reason = reason
    if evidence_id is not None and evidence_id not in action.evidence_ids:
        action.evidence_ids.append(evidence_id)


def _s1b_mark_snapshot(
    cycle: _S1bCycle,
) -> tuple[
    Decimal | None,
    Decimal | None,
    Decimal | None,
    Decimal | None,
    _BookObservation | None,
    _BookObservation | None,
]:
    """Return current BBO marks, PnL, and the exact book witnesses used.

    A zero position does not require a valuation book on that venue.  This is
    important for a retained one-sided residue: its mark remains a valid
    execution-only valuation even when the other leg has already reached
    exact zero.  Every non-zero leg still requires a fresh, sequence-healthy,
    causally ready book from its bound session/recovery.
    """

    prices: dict[Venue, Decimal] = {}
    observations: dict[Venue, _BookObservation] = {}
    for venue in (Venue.RISEX, Venue.LIGHTER):
        signed_quantity = (
            cycle.risex_signed_quantity
            if venue is Venue.RISEX
            else cycle.lighter_signed_quantity
        )
        if signed_quantity == _ZERO:
            continue
        expected_session = (
            cycle.quote_version.stream_session_id
            if venue is Venue.RISEX
            else cycle.quote_version.hedge_stream_session_id
        )
        expected_recovery = (
            cycle.quote_version.recovery_generation
            if venue is Venue.RISEX
            else cycle.quote_version.hedge_recovery_generation
        )
        if expected_session is None or expected_recovery is None:
            return None, None, None, None, None, None
        candidates = [
            item
            for item in cycle.books
            if item.book.venue is venue
            and item.book.canonical_market == cycle.quote_version.canonical_market
            and item.book.stream_session_id == expected_session
            and item.book.recovery_generation == expected_recovery
            and item.processing_ready_ns is not None
            and item.processing_ready_ns <= cycle.current_ns
            and item.book.received_monotonic_ns <= cycle.current_ns
        ]
        if not candidates:
            return None, None, None, None, None, None
        candidates.sort(
            key=lambda item: (
                item.book.received_monotonic_ns,
                item.processing_ready_ns or -1,
                item.book.book_revision,
                item.arrival_index,
            )
        )
        observation = candidates[-1]
        observations[venue] = observation
        book = observation.book
        if (
            not observation.identity_complete
            or not book.fresh
            or not book.is_sequence_healthy
            or not 0 <= cycle.current_ns - book.received_monotonic_ns <= cycle.policy.input_freshness_max_age_ns
            or _book_gap_blocks(cycle, book, cycle.current_ns, venue=venue)
        ):
            return None, None, None, None, None, None
        if venue is Venue.RISEX:
            if signed_quantity < _ZERO:
                if not book.asks:
                    return None, None, None, None, None, None
                prices[venue] = book.asks[0].canonical_price
            elif signed_quantity > _ZERO:
                if not book.bids:
                    return None, None, None, None, None, None
                prices[venue] = book.bids[0].canonical_price
        elif signed_quantity > _ZERO:
            if not book.bids:
                return None, None, None, None, None, None
            prices[venue] = book.bids[0].canonical_price
        elif signed_quantity < _ZERO:
            if not book.asks:
                return None, None, None, None, None, None
            prices[venue] = book.asks[0].canonical_price
    risex_price = prices.get(Venue.RISEX)
    lighter_price = prices.get(Venue.LIGHTER)
    risex_observation = observations.get(Venue.RISEX)
    lighter_observation = observations.get(Venue.LIGHTER)
    inventory: Decimal | None = _ZERO
    if cycle.risex_signed_quantity != _ZERO:
        if risex_price is None:
            inventory = None
        else:
            inventory = cycle.risex_signed_quantity * risex_price
    if inventory is not None and cycle.lighter_signed_quantity != _ZERO:
        if lighter_price is None:
            inventory = None
        else:
            inventory += cycle.lighter_signed_quantity * lighter_price
    if inventory is None:
        return risex_price, lighter_price, None, None, risex_observation, lighter_observation
    return (
        risex_price,
        lighter_price,
        inventory,
        cycle_net_cashflow(cycle) + inventory,
        risex_observation,
        lighter_observation,
    )


def _s1b_mark_positions(
    cycle: _S1bCycle,
) -> tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None]:
    """Compatibility projection of :func:`_s1b_mark_snapshot`."""

    return _s1b_mark_snapshot(cycle)[:4]


def cycle_net_cashflow(cycle: _S1bCycle) -> Decimal:
    return sum((flow.net_cashflow_usd for flow in cycle.cashflows), _ZERO)


def _s1b_liquidating_close_estimate(cycle: _S1bCycle) -> Decimal | None:
    """Estimate executable taker cashflow for every current non-zero leg.

    This is a read-only valuation witness.  It returns ``None`` unless the
    current public books can satisfy each exact venue operation on its own
    quantity grid and minimum contract; it never rounds up or creates an
    exemption for a residue.
    """

    if cycle.positions.is_zero:
        return _ZERO
    if _s1b_pending_actions(cycle):
        return None
    operations = (
        (Venue.RISEX, Side.BUY, abs(cycle.risex_signed_quantity)),
        (Venue.LIGHTER, Side.SELL, abs(cycle.lighter_signed_quantity)),
    )
    net = _ZERO
    for venue, side, quantity in operations:
        if quantity <= _ZERO:
            continue
        book, reason = _select_book(cycle, venue, cycle.current_ns)
        if reason is not None or book is None:
            return None
        step = _venue_quantity_step(cycle, venue)
        if step is None or quantity % step != _ZERO:
            return None
        levels = tuple(book.bids), tuple(book.asks)
        vwap = exact_quantity_vwap(side, quantity, *levels)
        if not vwap.is_executable or vwap.price is None:
            return None
        if not _minimum_ok_with_notional(
            cycle,
            venue,
            quantity,
            vwap.price,
            notional_usd=vwap.notional_usd,
        ):
            return None
        if venue is Venue.RISEX:
            rate = cycle.policy.risex_taker_fee_rate
            scenario_cost = vwap.notional_usd * cycle.delays.risex_fill_cost_rate
        else:
            rate = cycle.policy.lighter_taker_fee_rate
            scenario_cost = _ZERO
        fee = vwap.notional_usd * rate
        gross = vwap.notional_usd if side is Side.SELL else -vwap.notional_usd
        net += gross - fee - scenario_cost
    return net


class Scv1S1bKernel(CycleKernel):
    """Versioned SCV1-S1b episode kernel.

    The class intentionally subclasses ``CycleKernel`` only to reuse its
    validated book/identity helpers and immutable public action/fill models;
    all state transitions are local to this S1b class.  A caller must select
    one explicit ``CycleFillModel``.
    """

    def __init__(
        self,
        policy: CyclePolicy | None = None,
        *,
        terminal_retention_capacity: int = 64,
        fill_model: CycleFillModel | str,
    ) -> None:
        model = fill_model if isinstance(fill_model, CycleFillModel) else CycleFillModel(fill_model)
        super().__init__(
            policy,
            terminal_retention_capacity=terminal_retention_capacity,
            fill_model=model,
        )

    @staticmethod
    def _scenario(value: CycleScenario) -> CycleScenario:
        return value if isinstance(value, CycleScenario) else CycleScenario(value)

    def _cycle(self, scenario: CycleScenario) -> _S1bCycle | None:
        active = self._lane(scenario).active
        return active if isinstance(active, _S1bCycle) else None

    def snapshot(self, scenario: CycleScenario = CycleScenario.PRIMARY) -> S1bCycleResult | None:
        lane = self._lane(scenario)
        if lane.active is None:
            return lane.last_result  # type: ignore[return-value]
        result = self._result(lane.active, terminal=False)  # type: ignore[arg-type]
        lane.last_result = result
        return result

    def retained_results(
        self,
        scenario: CycleScenario = CycleScenario.PRIMARY,
        *,
        include_active: bool = True,
    ) -> tuple[S1bCycleResult, ...]:
        lane = self._lane(scenario)
        results = tuple(
            self._result(item, terminal=True)  # type: ignore[arg-type]
            for item in lane.terminal_cycles
            if isinstance(item, _S1bCycle)
        )
        if include_active and isinstance(lane.active, _S1bCycle):
            return (*results, self._result(lane.active, terminal=False))
        return results

    def _valid_s1b_quote(self, version: QuoteVersion) -> bool:
        return self._valid_entry_quote(version) and version.quote.risex_tick_size is not None

    @staticmethod
    def _residual_entry_quantity(version: QuoteVersion, residual: Decimal) -> Decimal:
        candidate = version.quote.canonical_quantity or _ZERO
        amount = min(candidate, max(_ZERO, residual))
        sizing = version.quote.sizing_evidence
        if sizing is None or sizing.common_quantity_step <= _ZERO:
            return _ZERO
        return _floor_quantity(amount, sizing.common_quantity_step)

    def _build_entry_quote(
        self,
        version: QuoteVersion,
        delays: CycleDelays,
        *,
        target_quantity: Decimal,
        source_books: tuple[BookEvidence, ...] = (),
    ) -> CausalRestingQuote:
        source_book = next(
            (
                book
                for book in source_books
                if book.venue is Venue.RISEX
                and book.canonical_market == version.canonical_market
                and book.stream_session_id == version.stream_session_id
                and book.recovery_generation == version.recovery_generation
                and book.book_revision == version.risex_book_revision
                and book.book_revision_id == version.risex_book_revision_id
            ),
            None,
        )
        hedge_source_book = next(
            (
                book
                for book in source_books
                if book.venue is Venue.LIGHTER
                and book.canonical_market == version.canonical_market
                and book.stream_session_id == version.hedge_stream_session_id
                and book.recovery_generation == version.hedge_recovery_generation
                and book.book_revision == version.lighter_book_revision
                and book.book_revision_id == version.lighter_book_revision_id
            ),
            None,
        )
        quote = build_causal_resting_quote(
            version,
            activation_delay_ns=delays.activation_delay_ns,
            cancel_delay_ns=delays.cancel_delay_ns,
            cancel_on_first_partial=False,
            source_book=source_book,
            hedge_source_book=hedge_source_book,
            source_book_freshness_max_age_ns=self.policy.input_freshness_max_age_ns,
        )
        return quote if quote.quantity == target_quantity else replace(quote, quantity=target_quantity)

    def _new_version(
        self,
        cycle: _S1bCycle,
        version: QuoteVersion,
        *,
        decision: int,
        target_quantity: Decimal,
        initial: bool,
        source_books: tuple[BookEvidence, ...],
    ) -> _S1bEntryVersion:
        delays = self.policy.delays(cycle.scenario)
        quote = self._build_entry_quote(
            version,
            delays,
            target_quantity=target_quantity,
            source_books=source_books,
        )
        sequence = cycle.entry_version_serial
        cycle.entry_version_serial += 1
        suffix = "" if initial else f":{sequence}"
        maker_action_id = "entry-maker" if initial else f"entry-maker:{sequence}"
        cancel_action_id = "entry-cancel" if initial else f"entry-cancel:{sequence}"
        state = _S1bEntryVersion(
            quote_version=version,
            quote=quote,
            sequence=sequence,
            target_quantity=target_quantity,
            activation_ns=decision + delays.activation_delay_ns,
            cancel_schedule_ns=decision + delays.activation_delay_ns + self.policy.entry_cancel_after_activation_ns,
            maker_action_id=maker_action_id,
            cancel_action_id=cancel_action_id,
            remaining_quantity=target_quantity,
        )
        cycle.entry_versions.append(state)
        cycle.active_entry_version = state
        cycle.quote_version = version
        cycle.entry_quote = quote
        cycle.entry_target_quantity = target_quantity
        cycle.entry_remaining_quantity = target_quantity
        cycle.entry_cancel_requested_ns = None
        cycle.entry_cancel_effective_ns = None
        if initial:
            cycle.entry_measurement_quote = quote
        _add_action(
            cycle,
            action_id=maker_action_id,
            kind=CycleActionKind.ENTRY_MAKER,
            status=CycleActionStatus.PENDING,
            requested_ns=decision,
            effective_ns=state.activation_ns,
            due_ns=state.cancel_schedule_ns,
            quantity=target_quantity,
            reason="ENTRY_RISEX_MAKER_QUOTE",
        )
        for book in source_books:
            self._record_book(cycle, CausalEvent.from_book(book), initial=initial)
        return state

    def _create_cycle(
        self,
        version: QuoteVersion,
        *,
        scenario: CycleScenario,
        source_books: tuple[BookEvidence, ...],
    ) -> _S1bCycle:
        decision = version.decision_ready_monotonic_ns
        assert decision is not None
        quantity = version.quote.canonical_quantity
        assert quantity is not None
        delays = self.policy.delays(scenario)
        placeholder = self._build_entry_quote(version, delays, target_quantity=quantity)
        cycle = _S1bCycle(
            initial_quote_version=version,
            quote_version=version,
            scenario=scenario,
            policy=self.policy,
            delays=delays,
            entry_quote=placeholder,
            entry_measurement_quote=placeholder,
            phase=_S1bPhase.ENTRY_WAIT,
            current_ns=decision,
            q_cap=quantity,
        )
        self._new_version(
            cycle,
            version,
            decision=decision,
            target_quantity=quantity,
            initial=True,
            source_books=source_books,
        )
        return cycle

    def _admission_failure(self, version: QuoteVersion, scenario: CycleScenario, reason: CycleReason) -> S1bCycleResult:
        quantity = version.quote.canonical_quantity or _ZERO
        result = S1bCycleResult(
            scenario=scenario,
            quote_version_id=version.version_id,
            canonical_market=version.canonical_market,
            status=CycleTerminalState.ABORTED,
            reason_codes=(reason.value,),
            entry_measurement=None,
            exit_measurement=None,
            entry_edge_usd=version.quote.actual_edge_usd,
            entry_quantity=_ZERO,
            hedged_quantity=_ZERO,
            unmatched_entry_quantity=_ZERO,
            exit_price=None,
            first_maker_fill_monotonic_ns=None,
            max_hold_deadline_monotonic_ns=None,
            terminal_monotonic_ns=version.decision_ready_monotonic_ns,
            positions=CyclePositions(_ZERO, _ZERO, _ZERO, _ZERO, _ZERO),
            ledger=CycleLedger(),
            actions=(),
            cashflow_complete=False,
            complete_execution_pnl_usd=None,
            holding_duration_ns=None,
            unmatched_exposure_duration_ns=0,
            q_cap=quantity,
            entry_version_ids=(),
            marked_execution_only_pnl_usd=_ZERO,
            observation_end_monotonic_ns=version.decision_ready_monotonic_ns,
            fill_model=self.fill_model,
        )
        return result

    def admit(
        self,
        quote_version: QuoteVersion,
        *,
        scenario: CycleScenario = CycleScenario.PRIMARY,
        source_books: Iterable[BookEvidence] = (),
    ) -> CycleAdmission:
        if not isinstance(quote_version, QuoteVersion):
            raise TypeError("quote_version must be QuoteVersion")
        scenario = self._scenario(scenario)
        lane = self._lane(scenario)
        books = tuple(source_books)
        if any(not isinstance(book, BookEvidence) for book in books):
            raise TypeError("source_books must contain BookEvidence")
        decision = quote_version.decision_ready_monotonic_ns
        accepted = True
        reason = "ACCEPTED"
        cycle = lane.active if isinstance(lane.active, _S1bCycle) else None
        if lane.halted_unresolved:
            accepted = False
            reason = CycleReason.UNRESOLVED_HALTED.value
        elif decision is None:
            accepted = False
            reason = CycleReason.MISSING_ENTRY_TIMING.value
        elif lane.last_decision_ns is not None and decision < lane.last_decision_ns + self._MIN_DECISION_INTERVAL_NS:
            accepted = False
            reason = CycleReason.DECISION_RATE_LIMIT.value
        elif not self._valid_s1b_quote(quote_version):
            accepted = False
            reason = CycleReason.INVALID_ENTRY_QUOTE.value
        elif len(lane.terminal_cycles) >= self.terminal_retention_capacity and cycle is None:
            accepted = False
            reason = CycleReason.TERMINAL_RETENTION_EXHAUSTED.value
            lane.halted_unresolved = True
        elif cycle is not None:
            if decision < cycle.current_ns:
                accepted = False
                reason = CycleReason.LATE_OLDER_EVENT.value
            elif cycle.exit_chosen or cycle.phase not in {
                _S1bPhase.ENTRY_REQUOTE_WAIT,
                _S1bPhase.ENTRY_CANCEL_WAIT,
            } or cycle.active_entry_version is not None:
                accepted = False
                reason = CycleReason.ACTIVE_CYCLE.value
            elif cycle.first_maker_fill_ns is not None and cycle.max_hold_deadline_ns is not None and decision >= cycle.max_hold_deadline_ns:
                accepted = False
                reason = CycleReason.MAX_HOLD.value
            else:
                residual = cycle.q_cap - cycle.entry_observed_quantity
                target_quantity = self._residual_entry_quantity(quote_version, residual)
                if target_quantity <= 0 or residual <= 0:
                    accepted = False
                    reason = CycleReason.MINIMUM_RESIDUE.value
                else:
                    if not _minimum_ok_with_notional(
                        cycle,
                        Venue.RISEX,
                        target_quantity,
                        quote_version.quote.maker_price or _ZERO,
                        notional_usd=target_quantity * (quote_version.quote.maker_price or _ZERO),
                    ):
                        accepted = False
                        reason = CycleReason.MINIMUM_RESIDUE.value
        if accepted:
            assert decision is not None
            input_reason, _ = _entry_input_failure(quote_version, self.policy, books)
            if input_reason is not None:
                accepted = False
                reason = input_reason.value
                # A bad re-quote witness must not invalidate the already
                # active episode.  Only an initial admission with no active
                # state can halt the lane at this boundary; the existing
                # episode remains available for its own bounded deadline and
                # finish/mark handling.
                if cycle is None:
                    lane.halted_unresolved = True
        admission = CycleAdmission(
            accepted=accepted,
            scenario=scenario,
            quote_version_id=quote_version.version_id,
            decision_monotonic_ns=decision,
            reason=reason,
        )
        lane.admission_history.append(admission)
        if not accepted:
            lane.last_result = (
                self._admission_failure(quote_version, scenario, CycleReason(reason))
                if reason in {item.value for item in CycleReason}
                else lane.last_result
            )
            return admission
        assert decision is not None
        if cycle is None:
            cycle = self._create_cycle(quote_version, scenario=scenario, source_books=books)
            lane.active = cycle
        else:
            self._run_due_until(cycle, decision)
            if cycle.exit_chosen or cycle.unresolved or cycle.policy_blocked or cycle.active_entry_version is not None:
                # The request may have crossed an action boundary while it
                # was being validated.  Preserve a truthful rejection.
                admission = CycleAdmission(
                    accepted=False,
                    scenario=scenario,
                    quote_version_id=quote_version.version_id,
                    decision_monotonic_ns=decision,
                    reason=CycleReason.ACTIVE_CYCLE.value,
                )
                lane.admission_history[-1] = admission
                return admission
            residual = cycle.q_cap - cycle.entry_observed_quantity
            target_quantity = self._residual_entry_quantity(quote_version, residual)
            self._new_version(
                cycle,
                quote_version,
                decision=decision,
                target_quantity=target_quantity,
                initial=False,
                source_books=books,
            )
            cycle.phase = _S1bPhase.ENTRY_WAIT
            cycle.current_ns = decision
        lane.last_decision_ns = decision
        lane.last_result = self._result(cycle)
        return admission

    @staticmethod
    def _s1b_book_is_bound(cycle: _S1bCycle, book: BookEvidence) -> bool:
        expected_session = (
            cycle.quote_version.stream_session_id
            if book.venue is Venue.RISEX
            else cycle.quote_version.hedge_stream_session_id
        )
        expected_recovery = (
            cycle.quote_version.recovery_generation
            if book.venue is Venue.RISEX
            else cycle.quote_version.hedge_recovery_generation
        )
        return (
            book.venue in {Venue.RISEX, Venue.LIGHTER}
            and book.canonical_market == cycle.quote_version.canonical_market
            and expected_session is not None
            and expected_recovery is not None
            and book.stream_session_id == expected_session
            and book.recovery_generation == expected_recovery
        )

    def _record_book(self, cycle: _S1bCycle, event: CausalEvent, *, initial: bool = False) -> None:
        book = event.book
        assert book is not None
        if book.canonical_market != cycle.quote_version.canonical_market:
            return
        expected_session = (
            cycle.quote_version.stream_session_id
            if book.venue is Venue.RISEX
            else cycle.quote_version.hedge_stream_session_id
        )
        expected_recovery = (
            cycle.quote_version.recovery_generation
            if book.venue is Venue.RISEX
            else cycle.quote_version.hedge_recovery_generation
        )
        if (
            book.venue not in {Venue.RISEX, Venue.LIGHTER}
            or expected_session is None
            or expected_recovery is None
            or book.stream_session_id != expected_session
            or book.recovery_generation != expected_recovery
        ):
            if book.venue in {Venue.RISEX, Venue.LIGHTER}:
                cycle.displaced_book_venues.add(book.venue)
            return
        identity_key = _event_identity_key(event)
        signature = self._book_signature(book)
        if identity_key is not None:
            cycle.book_identity_keys.add(identity_key)
            if initial:
                previous = cycle.initial_book_signatures.get(identity_key)
                if previous is not None:
                    if previous != signature:
                        self._halt(cycle, CycleReason.REQUIRED_ACTION_AMBIGUOUS)
                    else:
                        cycle.duplicate_event_count += 1
                        cycle.ignored_event_count += 1
                    return
                cycle.initial_book_signatures[identity_key] = signature
        revision_id = book.book_revision_id
        previous = cycle.book_revision_signatures.get(revision_id)
        if previous is not None:
            if previous != signature:
                self._halt(cycle, CycleReason.REQUIRED_ACTION_AMBIGUOUS)
            else:
                cycle.duplicate_event_count += 1
                cycle.ignored_event_count += 1
            return
        cycle.book_revision_signatures[revision_id] = signature
        cycle.books.append(
            _BookObservation(
                event=event,
                book=book,
                processing_ready_ns=_processing_ready_ns(event),
                arrival_index=len(cycle.books),
                identity_complete=event.source_identity_complete and event.identity_metadata_consistent,
            )
        )
        if book.venue is Venue.RISEX and book.book_revision_id == cycle.quote_version.risex_book_revision_id:
            cycle.entry_block_fence = book.block_number

    @staticmethod
    def _book_signature(book: BookEvidence) -> tuple[Any, ...]:
        levels = (
            tuple((_canonical_decimal_text(level.canonical_price), _canonical_decimal_text(level.canonical_quantity)) for level in book.bids),
            tuple((_canonical_decimal_text(level.canonical_price), _canonical_decimal_text(level.canonical_quantity)) for level in book.asks),
        )
        return (
            book.venue,
            book.canonical_market,
            book.stream_session_id,
            book.recovery_generation,
            book.book_revision,
            book.sequence,
            book.checksum,
            book.sequence_valid,
            book.checksum_valid,
            book.fresh,
            book.received_monotonic_ns,
            hashlib.sha256(repr(levels).encode()).hexdigest(),
        )

    def _s1b_entry_version_window_for(
        self,
        cycle: _S1bCycle,
        event: CausalEvent,
    ) -> _S1bEntryVersion | None:
        if event.trade is None or event.venue is not Venue.RISEX:
            return None
        matches = []
        for version in cycle.entry_versions:
            cutoff = version.cancel_effective_ns or (
                version.cancel_requested_ns + cycle.delays.cancel_delay_ns
                if version.cancel_requested_ns is not None
                else _MAX_TIME
            )
            if version.activation_ns <= event.causal_monotonic_ns < cutoff:
                matches.append(version)
        return matches[-1] if matches else None

    def _s1b_entry_version_for(self, cycle: _S1bCycle, event: CausalEvent) -> _S1bEntryVersion | None:
        version = self._s1b_entry_version_window_for(cycle, event)
        if version is None or not version.activation_checked:
            return None
        return version if version.activation_post_only is True else None

    def _s1b_ensure_entry_activation(
        self,
        cycle: _S1bCycle,
        version: _S1bEntryVersion,
        at_ns: int,
    ) -> bool:
        """Latch activation-time post-only eligibility exactly once.

        A deadline can request cancellation before a delayed quote activates.
        That does not remove the quote's activation boundary: a crossing quote
        never becomes a maker order, while a valid quote can still accept a
        causally eligible race until cancellation is effective.
        """

        if at_ns < version.activation_ns:
            return True
        if version.activation_checked:
            return version.activation_post_only is True
        version.activation_checked = True
        post_only = self._activation_post_only_s1b(cycle, version.quote, version.activation_ns)
        version.activation_post_only = post_only
        if post_only is None:
            cycle.add_reason(CycleReason.POST_ONLY_ELIGIBILITY_UNKNOWN)
            version.uncertainty.append(CausalUncertainty.MISSING_CAUSAL_TIMING.value)
            cycle.entry_uncertainty.append(CausalUncertainty.MISSING_CAUSAL_TIMING.value)
            self._halt(cycle, CycleReason.POST_ONLY_ELIGIBILITY_UNKNOWN)
            return False
        if not post_only:
            cycle.add_reason(CycleReason.INVALID_ENTRY_QUOTE)
            self._finish_no_entry_version(cycle, version, invalid=True)
            return False
        return True

    def _s1b_exit_candidate(self, cycle: _S1bCycle, event: CausalEvent) -> bool:
        trade = event.trade
        quote = cycle.exit_quote
        if trade is None or quote is None or event.venue is not Venue.RISEX:
            return False
        if trade.aggressor_side is not Side.SELL or quote.activation_monotonic_ns is None:
            return False
        if not cycle.exit_activation_checked or cycle.exit_activation_post_only is not True:
            return False
        cutoff = cycle.exit_cancel_effective_ns or (
            cycle.exit_cancel_requested_ns + cycle.delays.cancel_delay_ns
            if cycle.exit_cancel_requested_ns is not None
            else _MAX_TIME
        )
        if not quote.activation_monotonic_ns <= event.causal_monotonic_ns < cutoff:
            return False
        if cycle.exit_remaining_quantity <= 0:
            return False
        crosses, ambiguous = _trade_crosses(quote, trade)
        return crosses or ambiguous

    def _s1b_ensure_exit_activation(self, cycle: _S1bCycle, at_ns: int) -> bool:
        quote = cycle.exit_quote
        if quote is None or cycle.exit_activation_ns is None:
            return True
        if at_ns < cycle.exit_activation_ns:
            return True
        if cycle.exit_activation_checked:
            return cycle.exit_activation_post_only is True
        cycle.exit_activation_checked = True
        post_only = self._activation_post_only_s1b(cycle, quote, cycle.exit_activation_ns)
        cycle.exit_activation_post_only = post_only
        if post_only is None:
            cycle.add_reason(CycleReason.POST_ONLY_ELIGIBILITY_UNKNOWN)
            cycle.exit_uncertainty.append(CausalUncertainty.MISSING_CAUSAL_TIMING.value)
            self._halt(cycle, CycleReason.POST_ONLY_ELIGIBILITY_UNKNOWN)
            return False
        if not post_only:
            cycle.add_reason(CycleReason.EXIT_QUOTE_INVALID)
            return False
        return True

    def _s1b_entry_late_uncertainty(self, cycle: _S1bCycle, event: CausalEvent) -> None:
        cycle.add_reason(CycleReason.ENTRY_CAUSAL_UNCERTAINTY)
        cycle.add_reason(CycleReason.LATE_OLDER_EVENT)
        if CausalUncertainty.LATE_OLDER_EVENT.value not in cycle.entry_uncertainty:
            cycle.entry_uncertainty.append(CausalUncertainty.LATE_OLDER_EVENT.value)
        cycle.entry_decisions.append(
            CausalEventDecision(
                event.kind,
                event.event_id,
                event.ingress_received_monotonic_ns,
                "UNCERTAIN",
                "LATE_ENTRY_AFTER_COMMIT",
            )
        )
        self._halt(cycle, CycleReason.ENTRY_CAUSAL_UNCERTAINTY)

    def _s1b_exit_late_uncertainty(self, cycle: _S1bCycle, event: CausalEvent) -> None:
        cycle.add_reason(CycleReason.EXIT_CAUSAL_UNCERTAINTY)
        cycle.add_reason(CycleReason.LATE_OLDER_EVENT)
        if CausalUncertainty.LATE_OLDER_EVENT.value not in cycle.exit_uncertainty:
            cycle.exit_uncertainty.append(CausalUncertainty.LATE_OLDER_EVENT.value)
        cycle.exit_decisions.append(
            CausalEventDecision(
                event.kind,
                event.event_id,
                event.ingress_received_monotonic_ns,
                "UNCERTAIN",
                "LATE_EXIT_AFTER_COMMIT",
            )
        )
        self._halt(cycle, CycleReason.EXIT_CAUSAL_UNCERTAINTY)

    def _s1b_schedule_entry_hedge(self, cycle: _S1bCycle, requested_ns: int) -> None:
        pending = _s1b_pending_actions(cycle, CycleActionKind.ENTRY_HEDGE)
        requested_total = cycle.entry_observed_quantity - cycle.hedged_quantity
        pending_total = sum((item.remaining_quantity for item in pending), _ZERO)
        unreserved = requested_total - pending_total
        if unreserved <= 0:
            return
        # A reservation is immutable once requested.  In particular, a fill
        # arriving after an earlier hedge was scheduled receives its own
        # taker delay; enlarging the earlier descriptor would let the later
        # quantity execute on the earlier due boundary.
        cycle.entry_hedge_serial += 1
        action_id = "entry-hedge" if cycle.entry_hedge_serial == 1 else f"entry-hedge:{cycle.entry_hedge_serial - 1}"
        _schedule_taker(
            cycle,
            action_id=action_id,
            kind=CycleActionKind.ENTRY_HEDGE,
            venue=Venue.LIGHTER,
            side=Side.BUY,
            quantity=unreserved,
            requested_ns=requested_ns,
            reason="ENTRY_LIGHTER_TAKER_HEDGE",
        )

    @staticmethod
    def _s1b_has_deferred_entry_hedge(cycle: _S1bCycle) -> bool:
        return any(
            action.kind is CycleActionKind.ENTRY_HEDGE
            and action.status is CycleActionStatus.PENDING
            and action.action_id in cycle.scheduled_takers
            and action.due_ns is None
            and action.reason in _ENTRY_DEFERRED_REASONS
            for action in cycle.actions
        )

    def _s1b_wake_deferred_entry_hedges(self, cycle: _S1bCycle, at_ns: int) -> None:
        """Reconsider already-matured hedge reservations on fresh BOOK input."""

        if cycle.phase not in {
            _S1bPhase.ENTRY_WAIT,
            _S1bPhase.ENTRY_ACTIVE,
            _S1bPhase.ENTRY_CANCEL_WAIT,
            _S1bPhase.ENTRY_BARRIER,
            _S1bPhase.ENTRY_REQUOTE_WAIT,
        } or not self._s1b_has_deferred_entry_hedge(cycle):
            return
        book, reason = _select_book(cycle, Venue.LIGHTER, at_ns)
        if reason is not None or book is None:
            return
        current_signature = _s1b_hedge_book_signature(book)
        if cycle.entry_hedge_consumed_book_signature == current_signature:
            return
        self._s1b_execute_due_entry_hedges(cycle, at_ns, wake_deferred=True)
        if cycle.unresolved or cycle.policy_blocked:
            return
        if cycle.phase is _S1bPhase.ENTRY_BARRIER:
            # A successful wake can form the pair that was waiting behind the
            # barrier.  Re-run only the finite same-time transition logic;
            # no action is executed twice because completed reservations are
            # no longer pending.
            self._handle_boundary(cycle, at_ns)

    @staticmethod
    def _s1b_has_deferred_exit_close(cycle: _S1bCycle) -> bool:
        return any(
            action.kind is CycleActionKind.EXIT_HEDGE_CLOSE
            and action.status is CycleActionStatus.PENDING
            and action.action_id in cycle.scheduled_takers
            and action.due_ns is None
            for action in cycle.actions
        )

    def _s1b_wake_deferred_exit_closes(self, cycle: _S1bCycle, at_ns: int) -> None:
        """Reconsider matured exit-close reservations on fresh Lighter input."""

        if cycle.phase not in {
            _S1bPhase.EXIT_ACTIVE,
            _S1bPhase.EXIT_CANCEL_WAIT,
            _S1bPhase.CLOSE_WAIT,
            _S1bPhase.FORCE_WAIT,
        } or not self._s1b_has_deferred_exit_close(cycle):
            return
        book, reason = _select_book(cycle, Venue.LIGHTER, at_ns)
        if reason is not None or book is None:
            return
        if cycle.exit_close_consumed_book_signature == _s1b_close_book_signature(book):
            return
        self._s1b_execute_due_exit_closes(cycle, at_ns)
        if cycle.unresolved or cycle.policy_blocked:
            return
        self._handle_boundary(cycle, at_ns)

    def _s1b_reconsider_pair_on_book(self, cycle: _S1bCycle, at_ns: int) -> None:
        """Commit the first executable pair at a newly ready BOOK boundary."""

        if (
            cycle.paired_risex_quantity <= _ZERO
            or cycle.exit_chosen
            or cycle.phase
            not in {
                _S1bPhase.ENTRY_WAIT,
                _S1bPhase.ENTRY_ACTIVE,
                _S1bPhase.ENTRY_CANCEL_WAIT,
                _S1bPhase.ENTRY_REQUOTE_WAIT,
                _S1bPhase.ENTRY_BARRIER,
                _S1bPhase.UNMATCHED_WAIT,
            }
        ):
            return
        exit_ready, _ = self._s1b_exit_ready(cycle, at_ns)
        if exit_ready is True:
            self._s1b_pair_formed(cycle, at_ns)

    def _s1b_execute_due_entry_hedges(
        self,
        cycle: _S1bCycle,
        at_ns: int,
        *,
        finalize_if_deferred: bool = False,
        wake_deferred: bool = False,
    ) -> None:
        """Execute matured entry reservations as one venue operation.

        Each entry fill retains its own taker due boundary.  A reservation
        that is below the Lighter minimum is kept pending after its boundary
        and may join later reservations only once those later boundaries have
        matured.  This keeps the delay causal while allowing several
        subminimum fills to form one executable operation.
        """

        has_due_boundary = any(
            action.kind is CycleActionKind.ENTRY_HEDGE
            and action.status is CycleActionStatus.PENDING
            and action.action_id in cycle.scheduled_takers
            and action.due_ns is not None
            and action.due_ns <= at_ns
            for action in cycle.actions
        )
        has_future_reservation = any(
            action.kind is CycleActionKind.ENTRY_HEDGE
            and action.status is CycleActionStatus.PENDING
            and action.action_id in cycle.scheduled_takers
            and action.due_ns is not None
            and action.due_ns > at_ns
            for action in cycle.actions
        )
        include_deferred = (
            has_due_boundary
            or wake_deferred
            or (finalize_if_deferred and not has_future_reservation)
        )
        actions = tuple(
            action
            for action in cycle.actions
            if action.kind is CycleActionKind.ENTRY_HEDGE
            and action.status is CycleActionStatus.PENDING
            and action.action_id in cycle.scheduled_takers
            and (
                action.due_ns is not None
                and action.due_ns <= at_ns
                or action.due_ns is None
                and include_deferred
            )
        )
        if not actions:
            return

        if finalize_if_deferred and not has_due_boundary and not has_future_reservation:
            for item in actions:
                item.status = CycleActionStatus.COMPLETED
                item.due_ns = None
                cycle.entry_hedge_deferred_depth.pop(item.action_id, None)
                cycle.entry_hedge_deferred_book_signature.pop(item.action_id, None)
                if item.reason not in {reason.value for reason in CycleReason}:
                    item.reason = CycleReason.MINIMUM_RESIDUE.value
                cycle.add_reason(item.reason)
                cycle.add_reason(CycleReason.HEDGE_PARTIAL)
            return

        def defer_or_finalize(reason: CycleReason) -> None:
            cycle.add_reason(reason)
            finalize = finalize_if_deferred and not has_future_reservation
            for item in actions:
                if item.remaining_quantity <= _ZERO:
                    item.status = CycleActionStatus.COMPLETED
                    item.reason = "ENTRY_LIGHTER_TAKER_HEDGE"
                    cycle.entry_hedge_deferred_depth.pop(item.action_id, None)
                    cycle.entry_hedge_deferred_book_signature.pop(item.action_id, None)
                elif finalize:
                    item.status = CycleActionStatus.COMPLETED
                    item.due_ns = None
                    item.reason = reason.value
                    cycle.entry_hedge_deferred_depth.pop(item.action_id, None)
                    cycle.entry_hedge_deferred_book_signature.pop(item.action_id, None)
                    cycle.add_reason(CycleReason.HEDGE_PARTIAL)
                else:
                    item.status = CycleActionStatus.PENDING
                    item.due_ns = None
                    item.reason = reason.value
                    cycle.entry_hedge_deferred_depth[item.action_id] = available
                    cycle.entry_hedge_deferred_book_signature[item.action_id] = _s1b_hedge_book_signature(book)

        requested = min(
            sum((item.remaining_quantity for item in actions), _ZERO),
            max(_ZERO, cycle.entry_observed_quantity - cycle.hedged_quantity),
        )
        if requested <= _ZERO:
            for item in actions:
                item.status = CycleActionStatus.NOT_REQUIRED
                item.reason = CycleReason.OVER_CLOSE_BLOCKED.value
                item.due_ns = None
                cycle.entry_hedge_deferred_depth.pop(item.action_id, None)
                cycle.entry_hedge_deferred_book_signature.pop(item.action_id, None)
            return

        book, book_reason = _select_book(cycle, Venue.LIGHTER, at_ns)
        if book_reason is not None or book is None:
            self._action_data_failure(
                cycle,
                actions[0],
                book_reason or CycleReason.REQUIRED_ACTION_DATA_MISSING,
            )
            for item in actions[1:]:
                item.status = CycleActionStatus.UNRESOLVED
                item.reason = (book_reason or CycleReason.REQUIRED_ACTION_DATA_MISSING).value
            return
        book_signature = _s1b_hedge_book_signature(book)
        if cycle.entry_hedge_consumed_book_signature != book_signature:
            cycle.entry_hedge_consumed_book_signature = book_signature
            cycle.entry_hedge_consumed_quantity = _ZERO
        levels = _s1b_levels_after_consumed(
            book.asks,
            cycle.entry_hedge_consumed_quantity,
        )
        available = sum(
            (level.canonical_quantity for level in levels if level.canonical_quantity > _ZERO),
            _ZERO,
        )
        if available <= _ZERO:
            defer_or_finalize(CycleReason.INSUFFICIENT_DEPTH)
            return
        step = _venue_quantity_step(cycle, Venue.LIGHTER)
        if step is None:
            self._action_data_failure(cycle, actions[0], CycleReason.REQUIRED_ACTION_DATA_MISSING)
            for item in actions[1:]:
                item.status = CycleActionStatus.UNRESOLVED
                item.reason = CycleReason.REQUIRED_ACTION_DATA_MISSING.value
            return
        executable = _floor_quantity(min(requested, available), step)
        if executable <= _ZERO:
            defer_or_finalize(CycleReason.GRID_RESIDUE)
            return
        vwap = exact_quantity_vwap(
            Side.BUY,
            executable,
            tuple(book.bids),
            levels,
        )
        if not vwap.is_executable or vwap.price is None:
            defer_or_finalize(CycleReason.INSUFFICIENT_DEPTH)
            return
        if not _minimum_ok_with_notional(
            cycle,
            Venue.LIGHTER,
            executable,
            vwap.price,
            notional_usd=vwap.notional_usd,
        ):
            defer_or_finalize(CycleReason.MINIMUM_RESIDUE)
            return

        level_index = 0
        level_remaining = levels[0].canonical_quantity if levels else _ZERO

        def consume_notional(quantity: Decimal) -> Decimal:
            nonlocal level_index, level_remaining
            remaining = quantity
            notional = _ZERO
            while remaining > _ZERO and level_index < len(levels):
                level = levels[level_index]
                if level_remaining <= _ZERO:
                    level_index += 1
                    if level_index < len(levels):
                        level_remaining = levels[level_index].canonical_quantity
                    continue
                taken = min(remaining, level_remaining)
                notional += taken * level.canonical_price
                remaining -= taken
                level_remaining -= taken
            if remaining > _ZERO:
                raise ArithmeticError("entry hedge allocation exceeded selected book depth")
            return notional

        identity = CausalSourceIdentity.from_book(book)
        remaining_to_execute = executable
        actual_executed = _ZERO
        for item in actions:
            quantity = min(item.remaining_quantity, remaining_to_execute)
            if quantity <= _ZERO:
                continue
            notional = consume_notional(quantity)
            _append_fill(
                cycle,
                action_id=item.action_id,
                venue=Venue.LIGHTER,
                side=Side.BUY,
                role=LiquidityRole.TAKER,
                quantity=quantity,
                price=notional / quantity,
                reason="ENTRY_LIGHTER_TAKER_HEDGE",
                observed_ns=at_ns,
                processing_ns=at_ns,
                evidence_id=book.book_revision_id,
                source_identity=identity,
                session=book.stream_session_id,
                recovery=book.recovery_generation,
                book_revision_id=book.book_revision_id,
                notional_usd=notional,
            )
            item.executed_quantity += quantity
            item.reason = "ENTRY_LIGHTER_TAKER_HEDGE"
            if book.book_revision_id not in item.evidence_ids:
                item.evidence_ids.append(book.book_revision_id)
            remaining_to_execute -= quantity
            actual_executed += quantity
            if item.remaining_quantity <= _ZERO:
                item.status = CycleActionStatus.COMPLETED
                cycle.entry_hedge_deferred_depth.pop(item.action_id, None)
                cycle.entry_hedge_deferred_book_signature.pop(item.action_id, None)
            else:
                item.status = CycleActionStatus.PENDING
                item.due_ns = None
                item.reason = CycleReason.INSUFFICIENT_DEPTH.value
                cycle.entry_hedge_deferred_depth[item.action_id] = available
                cycle.entry_hedge_deferred_book_signature[item.action_id] = _s1b_hedge_book_signature(book)
            if remaining_to_execute <= _ZERO:
                break
        for item in actions:
            if item.status is CycleActionStatus.PENDING:
                item.due_ns = None
                item.reason = CycleReason.INSUFFICIENT_DEPTH.value
                cycle.entry_hedge_deferred_depth[item.action_id] = available
                cycle.entry_hedge_deferred_book_signature[item.action_id] = _s1b_hedge_book_signature(book)
                if finalize_if_deferred and not has_future_reservation:
                    item.status = CycleActionStatus.COMPLETED
                    item.reason = CycleReason.INSUFFICIENT_DEPTH.value
                    cycle.entry_hedge_deferred_depth.pop(item.action_id, None)
                    cycle.entry_hedge_deferred_book_signature.pop(item.action_id, None)
                    cycle.add_reason(CycleReason.HEDGE_PARTIAL)
        cycle.entry_hedge_consumed_quantity += actual_executed
        cycle.hedged_quantity += actual_executed
        cycle.paired_risex_quantity += actual_executed
        cycle.paired_lighter_quantity += actual_executed
        self._s1b_update_unmatched(cycle)
        if actual_executed < requested:
            cycle.add_reason(CycleReason.HEDGE_PARTIAL)
            cycle.add_reason(CycleReason.INSUFFICIENT_DEPTH)
        if cycle.paired_risex_quantity > _ZERO and not cycle.exit_chosen:
            self._s1b_pair_formed(cycle, at_ns)

    def _s1b_schedule_exit_close(self, cycle: _S1bCycle, requested_ns: int) -> None:
        maker_closed = cycle.exit_target_quantity - cycle.exit_remaining_quantity
        executed_close = sum(
            fill.quantity
            for fill in cycle.fills
            if fill.action_id.startswith("exit-close:")
        )
        pending = _s1b_pending_actions(cycle, CycleActionKind.EXIT_HEDGE_CLOSE)
        pending_total = sum((item.remaining_quantity for item in pending), _ZERO)
        unreserved = maker_closed - executed_close - pending_total
        if unreserved <= 0:
            return
        # Exit-close reservations obey the same causal rule as entry hedges:
        # later maker partials cannot be appended to an already due action.
        action_id = f"exit-close:{cycle.exit_close_serial}"
        cycle.exit_close_serial += 1
        _schedule_taker(
            cycle,
            action_id=action_id,
            kind=CycleActionKind.EXIT_HEDGE_CLOSE,
            venue=Venue.LIGHTER,
            side=Side.SELL,
            quantity=unreserved,
            requested_ns=requested_ns,
            reason="EXIT_LIGHTER_TAKER_CLOSE",
        )

    def _s1b_request_entry_cancellation(self, cycle: _S1bCycle, at_ns: int) -> None:
        for version in cycle.entry_versions:
            if version.cancel_effective_ns is not None:
                continue
            if version.cancel_requested_ns is None:
                version.cancel_requested_ns = at_ns
                if version.remaining_quantity <= _ZERO:
                    version.cancel_effective_ns = at_ns
                else:
                    version.cancel_effective_ns = at_ns + cycle.delays.cancel_delay_ns
            if version.remaining_quantity <= _ZERO:
                action = _add_action(
                    cycle,
                    action_id=version.cancel_action_id,
                    kind=CycleActionKind.ENTRY_CANCEL,
                    status=CycleActionStatus.NOT_REQUIRED,
                    requested_ns=version.cancel_requested_ns,
                    effective_ns=version.cancel_effective_ns,
                    due_ns=version.cancel_effective_ns,
                    quantity=_ZERO,
                    reason="ENTRY_CANCEL_NOT_REQUIRED",
                )
                _s1b_set_action(
                    cycle,
                    action,
                    status=CycleActionStatus.NOT_REQUIRED,
                    executed=_ZERO,
                    reason="ENTRY_CANCEL_NOT_REQUIRED",
                )
            else:
                action = _add_action(
                    cycle,
                    action_id=version.cancel_action_id,
                    kind=CycleActionKind.ENTRY_CANCEL,
                    status=CycleActionStatus.PENDING,
                    requested_ns=version.cancel_requested_ns,
                    effective_ns=version.cancel_effective_ns,
                    due_ns=version.cancel_effective_ns,
                    quantity=version.remaining_quantity,
                    reason="ENTRY_CANCEL_REQUESTED",
                )
                action.requested_quantity = version.remaining_quantity
            cycle.entry_cancel_requested_ns = version.cancel_requested_ns
            cycle.entry_cancel_effective_ns = version.cancel_effective_ns
        cycle.active_entry_version = None
        cycle.phase = _S1bPhase.ENTRY_BARRIER

    def _s1b_pair_formed(self, cycle: _S1bCycle, at_ns: int) -> None:
        if cycle.paired_risex_quantity > 0 and not cycle.exit_chosen:
            exit_ready, _ = self._s1b_exit_ready(cycle, at_ns)
            if exit_ready:
                # This is an irreversible policy commitment, not merely a
                # hint to prepare a quote after the cancellation barrier.
                # Inputs may worsen while the barrier drains, but that must
                # never reopen accumulation or admit a new entry version.
                cycle.exit_chosen = True
                cycle.exit_commitment_ns = at_ns
                self._s1b_request_entry_cancellation(cycle, at_ns)

    def _s1b_entry_action_complete(self, cycle: _S1bCycle, version: _S1bEntryVersion) -> None:
        maker = _s1b_action(cycle, version.maker_action_id)
        maker.status = CycleActionStatus.COMPLETED
        maker.executed_quantity = version.observed_quantity
        maker.reason = "ENTRY_MAKER_CANCELLED_PARTIAL" if version.remaining_quantity > 0 else "ENTRY_MAKER_FILLED"
        if version.cancel_effective_ns is not None and version.cancel_action_id in {item.action_id for item in cycle.actions}:
            cancel = _s1b_action(cycle, version.cancel_action_id)
            cancel.requested_quantity = version.remaining_quantity
            if cancel.status is CycleActionStatus.PENDING:
                _s1b_set_action(
                    cycle,
                    cancel,
                    status=CycleActionStatus.COMPLETED,
                    executed=version.remaining_quantity,
                    reason="ENTRY_CANCEL_EFFECTIVE",
                )

    def _s1b_update_unmatched(self, cycle: _S1bCycle) -> None:
        cycle.unmatched_entry_quantity = max(_ZERO, cycle.entry_observed_quantity - cycle.hedged_quantity)
        if cycle.initial_unmatched_quantity < cycle.unmatched_entry_quantity:
            cycle.initial_unmatched_quantity = cycle.unmatched_entry_quantity

    def _s1b_handle_entry_trade(self, cycle: _S1bCycle, event: CausalEvent) -> None:
        trade = event.trade
        assert trade is not None
        version = self._s1b_entry_version_for(cycle, event)
        if version is None:
            window = self._s1b_entry_version_window_for(cycle, event)
            if window is not None:
                if not window.activation_checked:
                    self._s1b_ensure_entry_activation(
                        cycle,
                        window,
                        event.causal_monotonic_ns,
                    )
                    if cycle.unresolved:
                        return
                if window.activation_post_only is not True:
                    cycle.ignored_event_count += 1
                    return
            if cycle.exit_chosen and any(
                item.activation_ns <= event.causal_monotonic_ns < (item.cancel_effective_ns or _MAX_TIME)
                for item in cycle.entry_versions
            ):
                self._s1b_entry_late_uncertainty(cycle, event)
            else:
                cycle.ignored_event_count += 1
            return
        ready = _processing_ready_ns(event)
        if ready is None:
            cycle.add_reason(CycleReason.EVENT_NOT_READY)
            cycle.entry_uncertainty.append(CausalUncertainty.MISSING_CAUSAL_TIMING.value)
            self._halt(cycle, CycleReason.EVENT_NOT_READY)
            return
        if (
            event.stream_session_id != version.quote_version.stream_session_id
            or event.recovery_generation != version.quote_version.recovery_generation
        ):
            cycle.entry_uncertainty.append(CausalUncertainty.RECOVERY_TRANSITION.value)
            self._halt(cycle, CycleReason.ENTRY_CAUSAL_UNCERTAINTY)
            return
        if trade.aggressor_side is not Side.BUY:
            cycle.ignored_event_count += 1
            decision = CausalEventDecision(
                event.kind,
                event.event_id,
                event.ingress_received_monotonic_ns,
                "IGNORED",
                "WRONG_AGGRESSOR_SIDE",
            )
            version.decisions.append(decision)
            cycle.entry_decisions.append(decision)
            return
        if not _trade_price_is_tick_aligned(version.quote, trade):
            version.uncertainty.append(CausalUncertainty.INVALID_TRADE_PRICE_GRID.value)
            cycle.entry_uncertainty.append(CausalUncertainty.INVALID_TRADE_PRICE_GRID.value)
            cycle.entry_decisions.append(
                CausalEventDecision(event.kind, event.event_id, event.ingress_received_monotonic_ns, "UNCERTAIN", CausalUncertainty.INVALID_TRADE_PRICE_GRID.value)
            )
            self._halt(cycle, CycleReason.ENTRY_CAUSAL_UNCERTAINTY)
            return
        crosses, ambiguous = _trade_crosses(version.quote, trade)
        if ambiguous:
            if self.fill_model is CycleFillModel.TOUCH_ALLOWED:
                crosses = True
                fill_reason = "ELIGIBLE_TOUCH_ZERO_QUEUE"
            else:
                cycle.ignored_event_count += 1
                decision = CausalEventDecision(
                    event.kind,
                    event.event_id,
                    event.ingress_received_monotonic_ns,
                    "IGNORED",
                    "TOUCH_IGNORED_BY_MODEL",
                )
                version.decisions.append(decision)
                cycle.entry_decisions.append(decision)
                return
        else:
            fill_reason = "ELIGIBLE_TRADE"
        if not crosses or version.remaining_quantity <= 0:
            cycle.ignored_event_count += 1
            decision = CausalEventDecision(
                event.kind,
                event.event_id,
                event.ingress_received_monotonic_ns,
                "IGNORED",
                "NOT_TRADE_THROUGH_QUOTE_PRICE" if not crosses else "QUOTE_QUANTITY_EXHAUSTED",
            )
            version.decisions.append(decision)
            cycle.entry_decisions.append(decision)
            return
        if cycle.entry_block_fence is not None and (
            event.block_number is None or event.block_number <= cycle.entry_block_fence
        ):
            cycle.entry_uncertainty.append(CausalUncertainty.WATERMARK_BOUNDARY_AMBIGUOUS.value)
            self._halt(cycle, CycleReason.ENTRY_CAUSAL_UNCERTAINTY)
            return
        consumed = min(trade.canonical_quantity, version.remaining_quantity, cycle.q_cap - cycle.entry_observed_quantity)
        if consumed <= 0:
            cycle.ignored_event_count += 1
            return
        identity = event.source_identity
        assert isinstance(identity, CausalSourceIdentity)
        version.remaining_quantity -= consumed
        version.observed_quantity += consumed
        fill = CausalFill(
            source_event_id=identity.source_event_id,  # type: ignore[arg-type]
            source_identity=identity,
            received_monotonic_ns=event.causal_monotonic_ns,
            price=version.quote.price,
            observed_quantity=trade.canonical_quantity,
            consumed_quantity=consumed,
            remaining_quantity=version.remaining_quantity,
            observed_trade_price=trade.canonical_price,
            processed_ready_monotonic_ns=ready,
        )
        version.fills.append(fill)
        cycle.entry_fills.append(fill)
        cycle.entry_observed_quantity += consumed
        cycle.entry_remaining_quantity = version.remaining_quantity
        version.decisions.append(CausalEventDecision(event.kind, event.event_id, event.ingress_received_monotonic_ns, "FILL", fill_reason, consumed))
        cycle.entry_decisions.append(version.decisions[-1])
        entry_action = _s1b_action(cycle, version.maker_action_id)
        entry_action.executed_quantity = version.observed_quantity
        entry_action.reason = "ENTRY_MAKER_PARTIAL" if version.remaining_quantity > 0 else "ENTRY_MAKER_FILLED"
        _append_fill(
            cycle,
            action_id=version.maker_action_id,
            venue=Venue.RISEX,
            side=Side.SELL,
            role=LiquidityRole.MAKER,
            quantity=consumed,
            price=version.quote.price,
            reason="ENTRY_MAKER_FILL",
            observed_ns=event.causal_monotonic_ns,
            processing_ns=ready,
            evidence_id=identity.source_event_id,  # type: ignore[arg-type]
            source_identity=identity,
            session=event.stream_session_id,
            recovery=event.recovery_generation,
            book_revision_id=None,
        )
        if cycle.first_maker_fill_ns is None:
            cycle.first_maker_fill_ns = event.causal_monotonic_ns
            cycle.max_hold_deadline_ns = cycle.first_maker_fill_ns + cycle.policy.max_hold_ns
        if version.remaining_quantity <= _ZERO:
            _s1b_set_action(
                cycle,
                entry_action,
                status=CycleActionStatus.COMPLETED,
                executed=version.observed_quantity,
                reason="ENTRY_MAKER_FILLED",
                evidence_id=identity.source_event_id,
            )
        self._s1b_schedule_entry_hedge(cycle, ready)
        if cycle.paired_risex_quantity > 0:
            self._s1b_pair_formed(cycle, ready)

    def _s1b_handle_exit_trade(self, cycle: _S1bCycle, event: CausalEvent) -> None:
        trade = event.trade
        assert trade is not None
        quote = cycle.exit_quote
        if quote is None or cycle.phase not in {_S1bPhase.EXIT_ACTIVE, _S1bPhase.EXIT_CANCEL_WAIT}:
            cycle.ignored_event_count += 1
            return
        ready = _processing_ready_ns(event)
        if ready is None:
            cycle.exit_uncertainty.append(CausalUncertainty.MISSING_CAUSAL_TIMING.value)
            self._halt(cycle, CycleReason.EXIT_CAUSAL_UNCERTAINTY)
            return
        if event.stream_session_id != quote.stream_session_id or event.recovery_generation != quote.recovery_generation:
            cycle.exit_uncertainty.append(CausalUncertainty.RECOVERY_TRANSITION.value)
            self._halt(cycle, CycleReason.EXIT_CAUSAL_UNCERTAINTY)
            return
        if quote.activation_monotonic_ns is not None and event.causal_monotonic_ns >= quote.activation_monotonic_ns:
            if not cycle.exit_activation_checked:
                if not self._s1b_ensure_exit_activation(cycle, event.causal_monotonic_ns):
                    if cycle.unresolved:
                        return
                    self._s1b_invalidate_exit_at_activation(cycle, quote.activation_monotonic_ns)
                    return
            if cycle.exit_activation_post_only is not True:
                cycle.ignored_event_count += 1
                return
        cutoff = cycle.exit_cancel_effective_ns or (
            cycle.exit_cancel_requested_ns + cycle.delays.cancel_delay_ns
            if cycle.exit_cancel_requested_ns is not None
            else _MAX_TIME
        )
        if quote.activation_monotonic_ns is None or not quote.activation_monotonic_ns <= event.causal_monotonic_ns < cutoff:
            cycle.ignored_event_count += 1
            return
        if trade.aggressor_side is not Side.SELL:
            cycle.ignored_event_count += 1
            cycle.exit_decisions.append(CausalEventDecision(event.kind, event.event_id, event.ingress_received_monotonic_ns, "IGNORED", "WRONG_AGGRESSOR_SIDE"))
            return
        if not _trade_price_is_tick_aligned(quote, trade):
            cycle.exit_uncertainty.append(CausalUncertainty.INVALID_TRADE_PRICE_GRID.value)
            cycle.exit_decisions.append(CausalEventDecision(event.kind, event.event_id, event.ingress_received_monotonic_ns, "UNCERTAIN", CausalUncertainty.INVALID_TRADE_PRICE_GRID.value))
            self._halt(cycle, CycleReason.EXIT_CAUSAL_UNCERTAINTY)
            return
        crosses, ambiguous = _trade_crosses(quote, trade)
        if ambiguous:
            if self.fill_model is CycleFillModel.TOUCH_ALLOWED:
                crosses = True
                fill_reason = "ELIGIBLE_TOUCH_ZERO_QUEUE"
            else:
                cycle.ignored_event_count += 1
                cycle.exit_decisions.append(CausalEventDecision(event.kind, event.event_id, event.ingress_received_monotonic_ns, "IGNORED", "TOUCH_IGNORED_BY_MODEL"))
                return
        else:
            fill_reason = "ELIGIBLE_TRADE"
        if not crosses or cycle.exit_remaining_quantity <= 0:
            cycle.ignored_event_count += 1
            return
        identity = event.source_identity
        assert isinstance(identity, CausalSourceIdentity)
        consumed = min(trade.canonical_quantity, cycle.exit_remaining_quantity, cycle.paired_risex_quantity)
        if consumed <= 0:
            cycle.add_reason(CycleReason.OVER_CLOSE_BLOCKED)
            return
        cycle.exit_remaining_quantity -= consumed
        causal_fill = CausalFill(
            source_event_id=identity.source_event_id,  # type: ignore[arg-type]
            source_identity=identity,
            received_monotonic_ns=event.causal_monotonic_ns,
            price=quote.price,
            observed_quantity=trade.canonical_quantity,
            consumed_quantity=consumed,
            remaining_quantity=cycle.exit_remaining_quantity,
            observed_trade_price=trade.canonical_price,
            processed_ready_monotonic_ns=ready,
        )
        cycle.exit_fills.append(causal_fill)
        decision = CausalEventDecision(event.kind, event.event_id, event.ingress_received_monotonic_ns, "FILL", fill_reason, consumed)
        cycle.exit_decisions.append(decision)
        exit_action = _s1b_action(cycle, "exit-maker")
        exit_action.executed_quantity = cycle.exit_target_quantity - cycle.exit_remaining_quantity
        exit_action.reason = "EXIT_MAKER_PARTIAL" if cycle.exit_remaining_quantity > 0 else "EXIT_MAKER_FILLED"
        _append_fill(
            cycle,
            action_id="exit-maker",
            venue=Venue.RISEX,
            side=Side.BUY,
            role=LiquidityRole.MAKER,
            quantity=consumed,
            price=quote.price,
            reason="EXIT_RISEX_MAKER_FILL",
            observed_ns=event.causal_monotonic_ns,
            processing_ns=ready,
            evidence_id=identity.source_event_id,  # type: ignore[arg-type]
            source_identity=identity,
            session=event.stream_session_id,
            recovery=event.recovery_generation,
            book_revision_id=None,
        )
        cycle.paired_risex_quantity -= consumed
        self._s1b_schedule_exit_close(cycle, ready)
        if cycle.exit_remaining_quantity == _ZERO:
            _s1b_set_action(cycle, exit_action, status=CycleActionStatus.COMPLETED, executed=cycle.exit_target_quantity, reason="EXIT_MAKER_FILLED", evidence_id=identity.source_event_id)
            cycle.phase = _S1bPhase.CLOSE_WAIT

    def _handle_trade(self, cycle: _S1bCycle, event: CausalEvent) -> None:
        trade = event.trade
        assert trade is not None
        if not event.source_identity_complete or not event.identity_metadata_consistent:
            if self._s1b_entry_version_for(cycle, event) is not None:
                cycle.entry_uncertainty.append(CausalUncertainty.MISSING_SOURCE_IDENTITY.value)
                self._halt(cycle, CycleReason.ENTRY_CAUSAL_UNCERTAINTY)
            elif self._s1b_exit_candidate(cycle, event):
                cycle.exit_uncertainty.append(CausalUncertainty.MISSING_SOURCE_IDENTITY.value)
                self._halt(cycle, CycleReason.EXIT_CAUSAL_UNCERTAINTY)
            else:
                cycle.ignored_event_count += 1
            return
        if event.venue is not Venue.RISEX or event.canonical_market != cycle.quote_version.canonical_market:
            cycle.ignored_event_count += 1
            return
        if self._s1b_entry_version_for(cycle, event) is not None or cycle.phase in {
            _S1bPhase.ENTRY_WAIT,
            _S1bPhase.ENTRY_ACTIVE,
            _S1bPhase.ENTRY_CANCEL_WAIT,
            _S1bPhase.ENTRY_REQUOTE_WAIT,
            _S1bPhase.ENTRY_BARRIER,
        }:
            self._s1b_handle_entry_trade(cycle, event)
            return
        if self._s1b_exit_candidate(cycle, event) or cycle.phase in {
            _S1bPhase.EXIT_ACTIVE,
            _S1bPhase.EXIT_CANCEL_WAIT,
        }:
            self._s1b_handle_exit_trade(cycle, event)
            return
        cycle.ignored_event_count += 1

    def _accept_event(self, cycle: _S1bCycle, event: CausalEvent) -> None:
        if event.venue not in {Venue.RISEX, Venue.LIGHTER} or event.canonical_market != cycle.quote_version.canonical_market:
            cycle.ignored_event_count += 1
            return
        identity_key = _event_identity_key(event)
        signature = _event_signature(event)
        if identity_key is not None:
            previous = cycle.seen_events.get(identity_key)
            if previous is not None:
                if previous == signature:
                    cycle.duplicate_event_count += 1
                    cycle.ignored_event_count += 1
                    return
                cycle.add_reason(CycleReason.DUPLICATE_CONFLICT)
                self._halt(cycle, CycleReason.DUPLICATE_CONFLICT)
                return
            cycle.seen_events[identity_key] = signature
        stream_key = (event.stream_key, event.kind)
        previous_time = cycle.last_stream_time.get(stream_key)
        position = _stream_position(event)
        previous_position = cycle.last_stream_position.get(stream_key)
        if previous_time is not None and event.causal_monotonic_ns < previous_time:
            self._halt(cycle, CycleReason.LATE_OLDER_EVENT)
            return
        if previous_position is not None and position is not None and position < previous_position:
            self._halt(cycle, CycleReason.LATE_OLDER_EVENT)
            return
        cycle.last_stream_time[stream_key] = max(previous_time or event.causal_monotonic_ns, event.causal_monotonic_ns)
        if position is not None:
            cycle.last_stream_position[stream_key] = position
        if event.kind is CausalEventKind.BOOK:
            self._record_book(cycle, event)
            ready = _processing_ready_ns(event)
            if ready is not None and ready > cycle.current_ns:
                self._run_due_until(cycle, ready)
            if (
                ready is not None
                and event.book is not None
                and event.source_identity_complete
                and event.identity_metadata_consistent
                and self._s1b_book_is_bound(cycle, event.book)
            ):
                if event.book.venue is Venue.LIGHTER:
                    self._s1b_wake_deferred_entry_hedges(cycle, cycle.current_ns)
                    self._s1b_wake_deferred_exit_closes(cycle, cycle.current_ns)
                self._s1b_reconsider_pair_on_book(cycle, cycle.current_ns)
            return
        if event.kind is CausalEventKind.DATA_GAP:
            gap = event.gap
            assert gap is not None
            cycle.gaps.append(gap)
            if self._gap_overlaps_s1b(cycle, gap):
                self._halt(cycle, CycleReason.REQUIRED_ACTION_DATA_GAP)
            if event.causal_monotonic_ns > cycle.current_ns:
                self._run_due_until(cycle, event.causal_monotonic_ns)
            return
        if event.causal_monotonic_ns > cycle.current_ns:
            self._run_due_until(cycle, event.causal_monotonic_ns)
        ready = _processing_ready_ns(event)
        if ready is not None and ready < cycle.current_ns:
            if self._s1b_entry_version_for(cycle, event) is not None:
                self._s1b_entry_late_uncertainty(cycle, event)
                return
            if self._s1b_exit_candidate(cycle, event):
                self._s1b_exit_late_uncertainty(cycle, event)
                return
        self._handle_trade(cycle, event)
        if ready is None and self._s1b_entry_version_for(cycle, event) is not None:
            self._halt(cycle, CycleReason.EVENT_NOT_READY)
        elif ready is not None and ready > cycle.current_ns:
            self._run_due_until(cycle, ready)

    def _gap_overlaps_s1b(self, cycle: _S1bCycle, gap: DataGapEvidence) -> bool:
        if gap.canonical_market != cycle.quote_version.canonical_market:
            return False
        if cycle.phase in {
            _S1bPhase.ENTRY_WAIT,
            _S1bPhase.ENTRY_ACTIVE,
            _S1bPhase.ENTRY_CANCEL_WAIT,
            _S1bPhase.ENTRY_REQUOTE_WAIT,
            _S1bPhase.ENTRY_BARRIER,
            _S1bPhase.UNMATCHED_WAIT,
        }:
            start = min((item.activation_ns for item in cycle.entry_versions), default=cycle.current_ns)
            end = max(cycle.current_ns, cycle.max_hold_deadline_ns or cycle.current_ns)
        elif cycle.phase in {
            _S1bPhase.EXIT_WAIT,
            _S1bPhase.EXIT_ACTIVE,
            _S1bPhase.EXIT_CANCEL_WAIT,
            _S1bPhase.CLOSE_WAIT,
            _S1bPhase.FORCE_WAIT,
        }:
            start = cycle.exit_activation_ns or cycle.current_ns
            end = max(cycle.current_ns, cycle.max_hold_deadline_ns or cycle.current_ns)
        else:
            return False
        if gap.source_venue not in {Venue.RISEX, Venue.LIGHTER}:
            return False
        expected_session = (
            cycle.quote_version.stream_session_id
            if gap.source_venue is Venue.RISEX
            else cycle.quote_version.hedge_stream_session_id
        )
        expected_recovery = (
            cycle.quote_version.recovery_generation
            if gap.source_venue is Venue.RISEX
            else cycle.quote_version.hedge_recovery_generation
        )
        return (
            expected_session is not None
            and expected_recovery is not None
            and gap.overlaps(start, end)
            and gap.matches(
                gap.source_venue,
                cycle.quote_version.canonical_market,
                expected_session,
                expected_recovery,
            )
        )

    def _next_due(self, cycle: _S1bCycle) -> int | None:
        # A close reservation whose first attempt was below the venue
        # minimum is deliberately deferred (``due_ns=None``) until another
        # independent reservation can make the aggregate operation valid.
        # Past due boundaries must never be selected again: doing so would
        # replay the same reservation forever without advancing the clock.
        pending = [
            action.due_ns
            for action in cycle.actions
            if action.status is CycleActionStatus.PENDING
            and action.action_id in cycle.scheduled_takers
            and action.due_ns is not None
            and action.due_ns > cycle.current_ns
        ]
        if cycle.phase is _S1bPhase.ENTRY_WAIT:
            values: list[int] = []
            if cycle.active_entry_version is not None and not cycle.active_entry_version.activation_checked:
                if cycle.active_entry_version.activation_ns > cycle.current_ns:
                    values.append(cycle.active_entry_version.activation_ns)
                else:
                    values.append(cycle.current_ns)
            if cycle.max_hold_deadline_ns is not None and not cycle.deadline_forced and cycle.max_hold_deadline_ns > cycle.current_ns:
                values.append(cycle.max_hold_deadline_ns)
            return min((*values, *pending), default=None)
        if cycle.phase is _S1bPhase.ENTRY_ACTIVE:
            values = [cycle.active_entry_version.cancel_schedule_ns] if cycle.active_entry_version else []
            if cycle.max_hold_deadline_ns is not None:
                values.append(cycle.max_hold_deadline_ns)
            return min((*values, *pending), default=None)
        if cycle.phase in {_S1bPhase.ENTRY_CANCEL_WAIT, _S1bPhase.ENTRY_BARRIER, _S1bPhase.ENTRY_REQUOTE_WAIT}:
            values = [
                item.cancel_effective_ns
                for item in cycle.entry_versions
                if item.cancel_effective_ns is not None and item.cancel_effective_ns > cycle.current_ns
            ]
            values.extend(
                item.activation_ns
                for item in cycle.entry_versions
                if not item.activation_checked and item.activation_ns > cycle.current_ns
            )
            if any(
                not item.activation_checked and item.activation_ns <= cycle.current_ns
                for item in cycle.entry_versions
            ):
                values.append(cycle.current_ns)
            # A hedge can complete at the same boundary that requests the
            # entry cancellation barrier.  Re-enter the boundary handler at
            # the current clock so a fully effective barrier can immediately
            # choose the exit or the residual re-quote; otherwise the next
            # external event would leave the episode stuck in ENTRY_BARRIER.
            if cycle.phase is _S1bPhase.ENTRY_BARRIER:
                all_cancelled = all(
                    item.cancel_effective_ns is not None and item.cancel_effective_ns <= cycle.current_ns
                    for item in cycle.entry_versions
                )
                if all_cancelled and not _s1b_pending_actions(cycle, CycleActionKind.ENTRY_HEDGE):
                    values.append(cycle.current_ns)
            if (
                cycle.max_hold_deadline_ns is not None
                and not cycle.deadline_forced
            ):
                values.append(cycle.max_hold_deadline_ns)
            return min((*values, *pending), default=None)
        if cycle.phase is _S1bPhase.UNMATCHED_WAIT:
            return min(pending, default=None)
        if cycle.phase is _S1bPhase.EXIT_WAIT:
            values: list[int] = []
            if cycle.exit_activation_ns is not None and not cycle.exit_activation_checked:
                if cycle.exit_activation_ns > cycle.current_ns:
                    values.append(cycle.exit_activation_ns)
                else:
                    values.append(cycle.current_ns)
            if cycle.max_hold_deadline_ns is not None and not cycle.deadline_forced and cycle.max_hold_deadline_ns > cycle.current_ns:
                values.append(cycle.max_hold_deadline_ns)
            return min((*values, *pending), default=None)
        if cycle.phase in {_S1bPhase.EXIT_ACTIVE, _S1bPhase.EXIT_CANCEL_WAIT, _S1bPhase.CLOSE_WAIT}:
            values = list(pending)
            if (
                cycle.phase is _S1bPhase.EXIT_CANCEL_WAIT
                and cycle.exit_activation_ns is not None
                and not cycle.exit_activation_checked
            ):
                if cycle.exit_activation_ns > cycle.current_ns:
                    values.append(cycle.exit_activation_ns)
                else:
                    values.append(cycle.current_ns)
            if cycle.phase in {_S1bPhase.EXIT_ACTIVE, _S1bPhase.CLOSE_WAIT} and cycle.max_hold_deadline_ns is not None:
                values.append(cycle.max_hold_deadline_ns)
            if cycle.phase is _S1bPhase.EXIT_CANCEL_WAIT and cycle.exit_cancel_effective_ns is not None:
                values.append(cycle.exit_cancel_effective_ns)
            return min(values, default=None)
        if cycle.phase is _S1bPhase.FORCE_WAIT:
            return min(pending, default=None)
        return None

    def _run_due_until(self, cycle: _S1bCycle, target_ns: int) -> None:
        if target_ns < cycle.current_ns:
            self._halt(cycle, CycleReason.LATE_OLDER_EVENT)
            return
        iterations = 0
        while cycle.phase not in {_S1bPhase.COMPLETE, _S1bPhase.ABORTED, _S1bPhase.UNRESOLVED}:
            due = self._next_due(cycle)
            if due is None or due > target_ns:
                break
            if due < cycle.current_ns:
                self._halt(cycle, CycleReason.LATE_OLDER_EVENT)
                break
            before = (
                cycle.phase,
                cycle.current_ns,
                cycle.deadline_forced,
                len(cycle.actions),
                sum(
                    action.status is CycleActionStatus.PENDING
                    for action in cycle.actions
                ),
            )
            cycle.current_ns = max(cycle.current_ns, due)
            self._handle_boundary(cycle, cycle.current_ns)
            iterations += 1
            if iterations > 10_000:
                self._halt(cycle, CycleReason.REQUIRED_ACTION_AMBIGUOUS)
                break
            after = (
                cycle.phase,
                cycle.current_ns,
                cycle.deadline_forced,
                len(cycle.actions),
                sum(
                    action.status is CycleActionStatus.PENDING
                    for action in cycle.actions
                ),
            )
            next_due = self._next_due(cycle)
            if next_due is not None and next_due <= cycle.current_ns and after == before:
                # A boundary that neither advances time nor changes state is
                # malformed scheduler input; fail closed rather than spin.
                self._halt(cycle, CycleReason.REQUIRED_ACTION_AMBIGUOUS)
                break
        cycle.current_ns = max(cycle.current_ns, target_ns)

    def _activation_post_only_s1b(self, cycle: _S1bCycle, quote: CausalRestingQuote, at_ns: int) -> bool | None:
        book, reason = _select_book(cycle, Venue.RISEX, at_ns)
        if reason is not None or book is None or not book.bids or not book.asks:
            return None
        if quote.maker_side is Side.SELL:
            return quote.price > book.bids[0].canonical_price
        return quote.price < book.asks[0].canonical_price

    def _finish_no_entry_version(self, cycle: _S1bCycle, version: _S1bEntryVersion, *, invalid: bool = False) -> None:
        if invalid and version.cancel_effective_ns is None:
            # A crossing post-only quote never became an active order.  Close
            # its causal interval at the activation boundary so a later
            # valid re-quote cannot resurrect it or make the entry barrier
            # wait for a cancellation that was never needed.
            version.cancel_requested_ns = version.activation_ns
            version.cancel_effective_ns = version.activation_ns
        maker = _s1b_action(cycle, version.maker_action_id)
        _s1b_set_action(
            cycle,
            maker,
            status=CycleActionStatus.COMPLETED,
            executed=version.observed_quantity,
            reason=CycleReason.INVALID_ENTRY_QUOTE.value if invalid else "ENTRY_MAKER_CANCELLED",
        )
        if version.cancel_effective_ns is not None:
            cancel = _add_action(
                cycle,
                action_id=version.cancel_action_id,
                kind=CycleActionKind.ENTRY_CANCEL,
                status=CycleActionStatus.COMPLETED,
                requested_ns=version.cancel_requested_ns or version.activation_ns,
                effective_ns=version.cancel_effective_ns,
                due_ns=version.cancel_effective_ns,
                quantity=version.remaining_quantity,
                reason="ENTRY_INVALIDATED_NO_ORDER" if invalid else "ENTRY_CANCEL_EFFECTIVE",
            )
            cancel.requested_quantity = version.remaining_quantity
            _s1b_set_action(
                cycle,
                cancel,
                status=CycleActionStatus.COMPLETED,
                executed=version.remaining_quantity,
                reason="ENTRY_INVALIDATED_NO_ORDER" if invalid else "ENTRY_CANCEL_EFFECTIVE",
            )

    def _s1b_invalidate_exit_at_activation(self, cycle: _S1bCycle, at_ns: int) -> None:
        """Close an exit quote that failed post-only at its activation boundary."""

        maker = _s1b_action(cycle, "exit-maker")
        observed = cycle.exit_target_quantity - cycle.exit_remaining_quantity
        if maker.status is CycleActionStatus.PENDING:
            _s1b_set_action(
                cycle,
                maker,
                status=CycleActionStatus.COMPLETED,
                executed=observed,
                reason=CycleReason.EXIT_QUOTE_INVALID.value,
            )
        cancel = next(
            (item for item in cycle.actions if item.action_id == "exit-cancel"),
            None,
        )
        if cancel is not None and cancel.status is CycleActionStatus.PENDING:
            _s1b_set_action(
                cycle,
                cancel,
                status=CycleActionStatus.COMPLETED,
                executed=cancel.remaining_quantity,
                reason="EXIT_INVALIDATED_NO_ORDER",
            )
        self._schedule_forced_remaining(cycle, at_ns)

    def _s1b_force_entry_deadline(self, cycle: _S1bCycle, at_ns: int) -> None:
        if cycle.deadline_forced:
            return
        cycle.add_reason(CycleReason.MAX_HOLD)
        cycle.forced_used = True
        cycle.deadline_forced = True
        self._s1b_request_entry_cancellation(cycle, at_ns)

    def _s1b_force_exit_deadline(self, cycle: _S1bCycle, at_ns: int) -> None:
        """Start the one immutable completion boundary for an exit maker."""

        if cycle.exit_cancel_requested_ns is not None:
            return
        cycle.add_reason(CycleReason.MAX_HOLD)
        cycle.forced_used = True
        cycle.deadline_forced = True
        cycle.exit_cancel_requested_ns = at_ns
        cycle.exit_cancel_effective_ns = at_ns + cycle.delays.cancel_delay_ns
        _add_action(
            cycle,
            action_id="exit-cancel",
            kind=CycleActionKind.EXIT_CANCEL,
            status=CycleActionStatus.PENDING,
            requested_ns=at_ns,
            effective_ns=cycle.exit_cancel_effective_ns,
            due_ns=cycle.exit_cancel_effective_ns,
            quantity=cycle.exit_remaining_quantity,
            reason="MAX_HOLD_CANCEL_REQUESTED",
        )
        cycle.phase = _S1bPhase.EXIT_CANCEL_WAIT

    def _handle_boundary(self, cycle: _S1bCycle, at_ns: int) -> None:
        if cycle.phase is _S1bPhase.ENTRY_WAIT:
            self._s1b_execute_due_entry_hedges(cycle, at_ns)
            if cycle.unresolved or cycle.policy_blocked or cycle.phase is not _S1bPhase.ENTRY_WAIT:
                return
            if (
                cycle.max_hold_deadline_ns is not None
                and at_ns >= cycle.max_hold_deadline_ns
                and not cycle.deadline_forced
            ):
                self._s1b_force_entry_deadline(cycle, at_ns)
                return
            version = cycle.active_entry_version
            if version is None:
                cycle.phase = _S1bPhase.ENTRY_REQUOTE_WAIT
                return
            if not self._s1b_ensure_entry_activation(cycle, version, at_ns):
                if cycle.unresolved:
                    return
                cycle.active_entry_version = None
                cycle.phase = _S1bPhase.ENTRY_REQUOTE_WAIT
                return
            cycle.phase = _S1bPhase.ENTRY_ACTIVE
            return
        if cycle.phase is _S1bPhase.ENTRY_ACTIVE:
            self._s1b_execute_due_entry_hedges(cycle, at_ns)
            if cycle.unresolved or cycle.policy_blocked:
                return
            version = cycle.active_entry_version
            if (
                version is not None
                and cycle.max_hold_deadline_ns is not None
                and at_ns >= cycle.max_hold_deadline_ns
                and not cycle.deadline_forced
            ):
                self._s1b_force_entry_deadline(cycle, at_ns)
                return
            if version is not None and version.cancel_requested_ns is None and at_ns >= version.cancel_schedule_ns:
                self._s1b_request_entry_cancellation(cycle, at_ns)
            return
        if cycle.phase in {_S1bPhase.ENTRY_CANCEL_WAIT, _S1bPhase.ENTRY_BARRIER, _S1bPhase.ENTRY_REQUOTE_WAIT}:
            for version in cycle.entry_versions:
                if version.activation_ns <= at_ns and not version.activation_checked:
                    self._s1b_ensure_entry_activation(cycle, version, at_ns)
                    if cycle.unresolved:
                        return
            self._s1b_execute_due_entry_hedges(cycle, at_ns)
            if cycle.unresolved or cycle.policy_blocked:
                return
            if (
                cycle.max_hold_deadline_ns is not None
                and at_ns >= cycle.max_hold_deadline_ns
                and not cycle.deadline_forced
            ):
                self._s1b_force_entry_deadline(cycle, at_ns)
                if cycle.unresolved:
                    return
            for version in cycle.entry_versions:
                if version.cancel_requested_ns is not None and version.cancel_effective_ns is not None and at_ns >= version.cancel_effective_ns:
                    self._s1b_entry_action_complete(cycle, version)
            all_cancelled = all(version.cancel_effective_ns is not None and at_ns >= version.cancel_effective_ns for version in cycle.entry_versions)
            can_requote = (
                not cycle.exit_chosen
                and not cycle.deadline_forced
                and cycle.entry_observed_quantity < cycle.q_cap
            )
            if all_cancelled:
                self._s1b_execute_due_entry_hedges(
                    cycle,
                    at_ns,
                    finalize_if_deferred=not can_requote,
                )
                if cycle.unresolved or cycle.policy_blocked:
                    return
            pending_hedges = _s1b_pending_actions(cycle, CycleActionKind.ENTRY_HEDGE)
            if cycle.phase is _S1bPhase.ENTRY_BARRIER and all_cancelled and pending_hedges:
                if can_requote:
                    cycle.active_entry_version = None
                    cycle.phase = _S1bPhase.ENTRY_REQUOTE_WAIT
                return
            if cycle.phase is _S1bPhase.ENTRY_BARRIER and all_cancelled and not pending_hedges:
                if cycle.exit_chosen:
                    # The first executable pair already committed the lane to
                    # exit.  Barrier completion may prepare the current
                    # quote, or go directly to bounded forced handling after
                    # the immutable first-fill deadline, but never reopens
                    # entry because current inputs deteriorated.
                    self._s1b_begin_exit_or_unmatched(cycle, at_ns)
                elif cycle.paired_risex_quantity > _ZERO:
                    exit_ready, exit_reason = self._s1b_exit_ready(cycle, at_ns)
                    if exit_ready:
                        self._s1b_begin_exit_or_unmatched(cycle, at_ns)
                    elif cycle.entry_observed_quantity < cycle.q_cap and not cycle.deadline_forced:
                        # Matched inventory below the current exit
                        # minimum/depth is not an executable paired position.
                        # Keep it visible and permit only the residual cap to
                        # be accumulated by a fresh entry version.
                        if exit_reason is not None:
                            cycle.add_reason(exit_reason)
                        cycle.phase = _S1bPhase.ENTRY_REQUOTE_WAIT
                        cycle.active_entry_version = None
                    elif exit_reason is not None and exit_reason is not CycleReason.EXIT_QUOTE_INVALID:
                        self._halt(cycle, exit_reason)
                    else:
                        self._s1b_begin_exit_or_unmatched(cycle, at_ns)
                elif cycle.entry_observed_quantity < cycle.q_cap and not cycle.deadline_forced:
                    # A normally cancelled partial entry keeps its immutable
                    # episode cap.  A fresh decision may fill only the
                    # residual; no future quote or fill is inferred here.
                    cycle.phase = _S1bPhase.ENTRY_REQUOTE_WAIT
                    cycle.active_entry_version = None
                else:
                    self._s1b_begin_exit_or_unmatched(cycle, at_ns)
            elif cycle.paired_risex_quantity > _ZERO and not cycle.exit_chosen:
                cycle.phase = _S1bPhase.ENTRY_BARRIER
            elif all_cancelled and cycle.phase is _S1bPhase.ENTRY_CANCEL_WAIT:
                cycle.active_entry_version = None
                cycle.phase = _S1bPhase.ENTRY_REQUOTE_WAIT
            return
        if cycle.phase is _S1bPhase.UNMATCHED_WAIT:
            for action in tuple(_s1b_pending_actions(cycle, CycleActionKind.UNMATCHED_RISEX_UNWIND)):
                if action.due_ns == at_ns:
                    self._execute_taker(cycle, action, at_ns)
            if cycle.unresolved or cycle.policy_blocked:
                return
            if cycle.unmatched_entry_quantity <= _ZERO:
                cycle.unmatched_resolved_ns = at_ns
                if cycle.deadline_forced:
                    self._schedule_forced_remaining(cycle, at_ns)
                else:
                    self._prepare_exit(cycle, at_ns)
            return
        if cycle.phase is _S1bPhase.EXIT_WAIT:
            if (
                cycle.max_hold_deadline_ns is not None
                and at_ns >= cycle.max_hold_deadline_ns
                and not cycle.deadline_forced
            ):
                self._s1b_force_exit_deadline(cycle, at_ns)
                return
            if cycle.exit_quote is None:
                self._schedule_forced_remaining(cycle, at_ns)
                return
            if not self._s1b_ensure_exit_activation(cycle, at_ns):
                if cycle.unresolved:
                    return
                self._s1b_invalidate_exit_at_activation(cycle, at_ns)
                return
            cycle.phase = _S1bPhase.EXIT_ACTIVE
            return
        if cycle.phase is _S1bPhase.EXIT_ACTIVE:
            self._s1b_execute_due_exit_closes(cycle, at_ns)
            if cycle.unresolved or cycle.policy_blocked:
                return
            if cycle.max_hold_deadline_ns is not None and at_ns >= cycle.max_hold_deadline_ns:
                self._s1b_force_exit_deadline(cycle, at_ns)
            return
        if cycle.phase is _S1bPhase.EXIT_CANCEL_WAIT:
            if cycle.exit_activation_ns is not None and cycle.exit_activation_ns <= at_ns and not cycle.exit_activation_checked:
                if not self._s1b_ensure_exit_activation(cycle, at_ns):
                    if cycle.unresolved:
                        return
                    self._s1b_invalidate_exit_at_activation(cycle, at_ns)
                    return
            self._s1b_execute_due_exit_closes(cycle, at_ns)
            for action in tuple(
                action
                for action in _s1b_pending_actions(cycle)
                if action.kind is not CycleActionKind.EXIT_HEDGE_CLOSE
            ):
                if action.due_ns == at_ns:
                    self._execute_taker(cycle, action, at_ns)
            if cycle.unresolved or cycle.policy_blocked:
                return
            if cycle.exit_cancel_effective_ns is not None and at_ns >= cycle.exit_cancel_effective_ns:
                cancel = _s1b_action(cycle, "exit-cancel")
                _s1b_set_action(cycle, cancel, status=CycleActionStatus.COMPLETED, executed=cycle.exit_remaining_quantity, reason="EXIT_CANCEL_EFFECTIVE")
                maker = _s1b_action(cycle, "exit-maker")
                maker.status = CycleActionStatus.COMPLETED
                maker.executed_quantity = cycle.exit_target_quantity - cycle.exit_remaining_quantity
                maker.reason = "EXIT_MAKER_CANCELLED_PARTIAL"
                # No later maker partial can make an old deferred close
                # reservation executable after this barrier.  Future-due
                # reservations still retain their own delay and are handled
                # in FORCE_WAIT without overlapping forced exposure.
                self._s1b_execute_due_exit_closes(
                    cycle,
                    at_ns,
                    finalize_if_deferred=True,
                )
                self._schedule_forced_remaining(cycle, at_ns)
            return
        if cycle.phase is _S1bPhase.CLOSE_WAIT:
            self._s1b_execute_due_exit_closes(cycle, at_ns)
            if cycle.unresolved or cycle.policy_blocked:
                return
            if cycle.positions.is_zero and not _s1b_pending_actions(cycle):
                cycle.phase = _S1bPhase.COMPLETE
                cycle.terminal_ns = at_ns
                return
            if cycle.max_hold_deadline_ns is not None and at_ns >= cycle.max_hold_deadline_ns:
                if cycle.exit_remaining_quantity > _ZERO:
                    self._s1b_force_exit_deadline(cycle, at_ns)
                else:
                    cycle.add_reason(CycleReason.MAX_HOLD)
                    cycle.forced_used = True
                    cycle.deadline_forced = True
                    self._s1b_execute_due_exit_closes(
                        cycle,
                        at_ns,
                        finalize_if_deferred=True,
                    )
                    self._schedule_forced_remaining(cycle, at_ns)
                return
            if not _s1b_pending_actions(cycle):
                self._schedule_forced_remaining(cycle, at_ns)
            return
        if cycle.phase is _S1bPhase.FORCE_WAIT:
            self._s1b_execute_due_exit_closes(
                cycle,
                at_ns,
                finalize_if_deferred=True,
            )
            for action in tuple(
                action
                for action in _s1b_pending_actions(cycle)
                if action.kind is not CycleActionKind.EXIT_HEDGE_CLOSE
            ):
                if action.due_ns == at_ns:
                    self._execute_taker(cycle, action, at_ns)
            if cycle.unresolved or cycle.policy_blocked:
                return
            if cycle.positions.is_zero and not _s1b_pending_actions(cycle):
                cycle.phase = _S1bPhase.COMPLETE
                cycle.terminal_ns = at_ns
            elif not _s1b_pending_actions(cycle):
                self._policy_block(cycle, _POLICY_BLOCKED_RESIDUAL)

    def _s1b_begin_exit_or_unmatched(self, cycle: _S1bCycle, at_ns: int) -> None:
        self._s1b_update_unmatched(cycle)
        if cycle.paired_risex_quantity <= _ZERO:
            if cycle.unmatched_entry_quantity <= _ZERO:
                cycle.phase = _S1bPhase.COMPLETE
                cycle.terminal_ns = at_ns
            else:
                self._schedule_unmatched(cycle, at_ns)
            return
        if cycle.unmatched_entry_quantity > _ZERO:
            self._schedule_unmatched(cycle, at_ns)
        elif cycle.deadline_forced:
            self._schedule_forced_remaining(cycle, at_ns)
        else:
            self._prepare_exit(cycle, at_ns)

    def _schedule_unmatched(self, cycle: _S1bCycle, at_ns: int) -> None:
        cycle.forced_used = True
        cycle.add_reason(CycleReason.FORCED_UNWIND)
        cycle.unmatched_started_ns = cycle.unmatched_started_ns or at_ns
        if not any(action.action_id == "unmatched-risex" and action.status is CycleActionStatus.PENDING for action in cycle.actions):
            _schedule_taker(cycle, action_id="unmatched-risex", kind=CycleActionKind.UNMATCHED_RISEX_UNWIND, venue=Venue.RISEX, side=Side.BUY, quantity=cycle.unmatched_entry_quantity, requested_ns=at_ns, reason="UNMATCHED_RISEX_TAKER_UNWIND")
        cycle.phase = _S1bPhase.UNMATCHED_WAIT

    def _exit_quote_candidate(
        self,
        cycle: _S1bCycle,
        decision_ns: int,
    ) -> tuple[CausalRestingQuote | None, CycleReason | None]:
        if cycle.paired_risex_quantity <= _ZERO:
            return None, None
        risex_book, lighter_book, pair_reason = _paired_books(cycle, decision_ns)
        if pair_reason is not None or risex_book is None or lighter_book is None:
            return None, pair_reason or CycleReason.REQUIRED_ACTION_DATA_MISSING
        risex_step = _venue_quantity_step(cycle, Venue.RISEX)
        lighter_step = _venue_quantity_step(cycle, Venue.LIGHTER)
        if risex_step is None or lighter_step is None:
            return None, CycleReason.REQUIRED_ACTION_DATA_MISSING
        # A paired position may contain a legitimate partial that is not a
        # valid new order on one of the venues.  Select only the largest
        # already-held portion that is simultaneously on both operation
        # grids; retain the exact remainder instead of rounding it upward.
        quantity = min(cycle.paired_risex_quantity, cycle.paired_lighter_quantity)
        joint_step = _joint_operation_step(risex_step, lighter_step)
        if joint_step is None:
            return None, CycleReason.REQUIRED_ACTION_DATA_MISSING
        quantity = _floor_quantity(quantity, joint_step)
        if quantity <= _ZERO:
            return None, CycleReason.EXIT_QUOTE_INVALID
        lighter_vwap: ExactVwap = exact_quantity_vwap(Side.SELL, quantity, tuple(lighter_book.bids), tuple(lighter_book.asks))
        tick = cycle.quote_version.quote.risex_tick_size
        if tick is None or not lighter_vwap.is_executable or lighter_vwap.price is None or not _minimum_ok_with_notional(cycle, Venue.LIGHTER, quantity, lighter_vwap.price, notional_usd=lighter_vwap.notional_usd):
            return None, CycleReason.EXIT_QUOTE_INVALID
        risex_best_ask = risex_book.asks[0].canonical_price if risex_book.asks else None
        if risex_best_ask is None:
            return None, CycleReason.EXIT_QUOTE_INVALID
        raw = lighter_vwap.notional_usd * (_ONE - cycle.policy.lighter_taker_fee_rate) / (quantity * (_ONE + cycle.policy.risex_maker_fee_rate))
        maker_price = min((raw / tick).to_integral_value(rounding=ROUND_FLOOR) * tick, risex_best_ask - tick)
        if maker_price <= _ZERO:
            return None, CycleReason.EXIT_QUOTE_INVALID
        if not _minimum_ok_with_notional(
            cycle,
            Venue.RISEX,
            quantity,
            maker_price,
            notional_usd=quantity * maker_price,
        ):
            return None, CycleReason.EXIT_QUOTE_INVALID
        return CausalRestingQuote(
            quote_id=f"{cycle.initial_quote_version.version_id}:exit",
            quote_version_id=f"{cycle.initial_quote_version.version_id}:exit",
            canonical_market=cycle.quote_version.canonical_market,
            maker_side=Side.BUY,
            price=maker_price,
            quantity=quantity,
            stream_session_id=cycle.quote_version.stream_session_id,
            recovery_generation=cycle.quote_version.recovery_generation,
            decision_ready_monotonic_ns=decision_ns,
            activation_delay_ns=cycle.delays.activation_delay_ns,
            cancel_delay_ns=cycle.delays.cancel_delay_ns,
            cancel_on_first_partial=False,
            source_book=risex_book,
            source_book_revision=risex_book.book_revision,
            source_book_revision_id=risex_book.book_revision_id,
            source_identity=CausalSourceIdentity.from_book(risex_book),
            hedge_source_book=lighter_book,
            hedge_stream_session_id=lighter_book.stream_session_id,
            hedge_recovery_generation=lighter_book.recovery_generation,
            tick_size=tick,
        ), None

    def _s1b_exit_ready(self, cycle: _S1bCycle, at_ns: int) -> tuple[bool | None, CycleReason | None]:
        quote, reason = self._exit_quote_candidate(cycle, at_ns)
        if quote is not None:
            return True, None
        if reason is None and cycle.paired_risex_quantity <= _ZERO:
            return False, None
        if reason is CycleReason.EXIT_QUOTE_INVALID:
            return False, reason
        return None, reason or CycleReason.REQUIRED_ACTION_DATA_MISSING

    def _prepare_exit(self, cycle: _S1bCycle, decision_ns: int) -> None:
        if cycle.paired_risex_quantity <= _ZERO:
            if cycle.paired_lighter_quantity <= _ZERO and cycle.unmatched_entry_quantity <= _ZERO:
                cycle.phase = _S1bPhase.COMPLETE
                cycle.terminal_ns = decision_ns
            return
        exit_quote, reason = self._exit_quote_candidate(cycle, decision_ns)
        if exit_quote is None:
            if reason is not CycleReason.EXIT_QUOTE_INVALID:
                self._halt(cycle, reason or CycleReason.REQUIRED_ACTION_DATA_MISSING)
                return
            cycle.add_reason(CycleReason.EXIT_QUOTE_INVALID)
            self._schedule_forced_remaining(cycle, decision_ns)
            return
        quantity = exit_quote.quantity
        cycle.exit_chosen = True
        cycle.exit_price = exit_quote.price
        cycle.exit_activation_ns = decision_ns + cycle.delays.activation_delay_ns
        cycle.exit_remaining_quantity = quantity
        cycle.exit_target_quantity = quantity
        cycle.exit_quote = exit_quote
        _add_action(cycle, action_id="exit-maker", kind=CycleActionKind.EXIT_MAKER, status=CycleActionStatus.PENDING, requested_ns=decision_ns, effective_ns=cycle.exit_activation_ns, due_ns=cycle.max_hold_deadline_ns, quantity=quantity, reason="EXIT_RISEX_MAKER_QUOTE")
        cycle.phase = _S1bPhase.EXIT_WAIT

    def _schedule_forced_remaining(self, cycle: _S1bCycle, at_ns: int) -> None:
        cycle.forced_used = True
        cycle.add_reason(CycleReason.FORCED_UNWIND)
        cycle.phase = _S1bPhase.FORCE_WAIT
        if cycle.unmatched_entry_quantity > _ZERO and not any(action.action_id == "unmatched-risex" and action.status is CycleActionStatus.PENDING for action in cycle.actions):
            cycle.unmatched_started_ns = cycle.unmatched_started_ns or at_ns
            _schedule_taker(cycle, action_id="unmatched-risex", kind=CycleActionKind.UNMATCHED_RISEX_UNWIND, venue=Venue.RISEX, side=Side.BUY, quantity=cycle.unmatched_entry_quantity, requested_ns=at_ns, reason="UNMATCHED_RISEX_TAKER_UNWIND")
        if cycle.paired_risex_quantity > _ZERO and not any(action.action_id == "forced-risex" and action.status is CycleActionStatus.PENDING for action in cycle.actions):
            _schedule_taker(cycle, action_id="forced-risex", kind=CycleActionKind.FORCED_RISEX_UNWIND, venue=Venue.RISEX, side=Side.BUY, quantity=cycle.paired_risex_quantity, requested_ns=at_ns, reason="FORCED_RISEX_TAKER_UNWIND")
        reserved_lighter = _s1b_pending_quantity(cycle, CycleActionKind.EXIT_HEDGE_CLOSE)
        force_lighter = cycle.paired_lighter_quantity - reserved_lighter
        if force_lighter > _ZERO and not any(action.action_id == "forced-lighter" and action.status is CycleActionStatus.PENDING for action in cycle.actions):
            _schedule_taker(cycle, action_id="forced-lighter", kind=CycleActionKind.FORCED_LIGHTER_UNWIND, venue=Venue.LIGHTER, side=Side.SELL, quantity=force_lighter, requested_ns=at_ns, reason="FORCED_LIGHTER_TAKER_UNWIND")
        if not _s1b_pending_actions(cycle):
            if cycle.positions.is_zero:
                cycle.phase = _S1bPhase.COMPLETE
                cycle.terminal_ns = at_ns
            else:
                self._policy_block(cycle, _POLICY_BLOCKED_RESIDUAL)

    def _policy_block(self, cycle: _S1bCycle, reason: CycleReason | str) -> None:
        cycle.policy_blocked = True
        if cycle.blocked_started_ns is None:
            cycle.blocked_started_ns = cycle.current_ns
        cycle.add_reason(reason)
        if reason == _POLICY_BLOCKED_MINIMUM:
            cycle.add_reason(CycleReason.MINIMUM_RESIDUE)
        cycle.add_reason(_POLICY_BLOCKED_RESIDUAL)
        cycle.phase = _S1bPhase.UNRESOLVED
        cycle.terminal_ns = cycle.current_ns

    def _halt(self, cycle: _S1bCycle, reason: CycleReason | str) -> None:
        cycle.add_reason(reason)
        cycle.unresolved = True
        cycle.phase = _S1bPhase.UNRESOLVED
        cycle.terminal_ns = cycle.current_ns

    def _action_data_failure(self, cycle: _S1bCycle, action: _MutableAction, reason: CycleReason) -> None:
        action.status = CycleActionStatus.UNRESOLVED
        action.reason = reason.value
        self._halt(cycle, reason)

    def _s1b_execute_due_exit_closes(
        self,
        cycle: _S1bCycle,
        at_ns: int,
        *,
        finalize_if_deferred: bool = False,
    ) -> None:
        """Execute causally due close reservations as one Lighter operation.

        Each maker partial creates its own immutable reservation and due
        boundary.  A reservation that is below Lighter's minimum is retained
        without a fake fill and can join a later reservation when that later
        reservation becomes due.  Once the maker remainder is no longer
        working (deadline/cancel), deferred reservations are finalized as
        residue instead of keeping the scheduler alive forever.
        """

        actions = tuple(
            action
            for action in cycle.actions
            if action.kind is CycleActionKind.EXIT_HEDGE_CLOSE
            and action.status is CycleActionStatus.PENDING
            and action.action_id in cycle.scheduled_takers
            and (action.due_ns is None or action.due_ns <= at_ns)
        )
        if not actions:
            return

        def defer_or_finalize(reason: CycleReason) -> None:
            cycle.add_reason(reason)
            for action in actions:
                if action.remaining_quantity <= _ZERO:
                    action.status = CycleActionStatus.COMPLETED
                    action.reason = "EXIT_LIGHTER_TAKER_CLOSE"
                elif finalize_if_deferred:
                    action.status = CycleActionStatus.COMPLETED
                    action.due_ns = None
                    action.reason = reason.value
                else:
                    action.status = CycleActionStatus.PENDING
                    action.due_ns = None
                    action.reason = "EXIT_CLOSE_WAITING_FOR_AGGREGATION"

        requested = min(
            sum((action.remaining_quantity for action in actions), _ZERO),
            cycle.paired_lighter_quantity,
        )
        if requested <= _ZERO:
            for action in actions:
                action.status = CycleActionStatus.NOT_REQUIRED
                action.reason = CycleReason.OVER_CLOSE_BLOCKED.value
                action.due_ns = None
            return
        book, book_reason = _select_book(cycle, Venue.LIGHTER, at_ns)
        if book_reason is not None or book is None:
            self._action_data_failure(
                cycle,
                actions[0],
                book_reason or CycleReason.REQUIRED_ACTION_DATA_MISSING,
            )
            for action in actions[1:]:
                action.status = CycleActionStatus.UNRESOLVED
                action.reason = (book_reason or CycleReason.REQUIRED_ACTION_DATA_MISSING).value
            return
        book_signature = _s1b_close_book_signature(book)
        if cycle.exit_close_consumed_book_signature != book_signature:
            cycle.exit_close_consumed_book_signature = book_signature
            cycle.exit_close_consumed_quantity = _ZERO
        levels = _s1b_levels_after_consumed(
            book.bids,
            cycle.exit_close_consumed_quantity,
        )
        available = sum(
            (level.canonical_quantity for level in levels if level.canonical_quantity > _ZERO),
            _ZERO,
        )
        if available <= _ZERO:
            defer_or_finalize(CycleReason.INSUFFICIENT_DEPTH)
            return
        step = _venue_quantity_step(cycle, Venue.LIGHTER)
        if step is None:
            self._action_data_failure(cycle, actions[0], CycleReason.REQUIRED_ACTION_DATA_MISSING)
            for action in actions[1:]:
                action.status = CycleActionStatus.UNRESOLVED
                action.reason = CycleReason.REQUIRED_ACTION_DATA_MISSING.value
            return
        executable = _floor_quantity(min(requested, available), step)
        if executable <= _ZERO:
            defer_or_finalize(CycleReason.GRID_RESIDUE)
            return
        vwap = exact_quantity_vwap(
            Side.SELL,
            executable,
            levels,
            tuple(book.asks),
        )
        if not vwap.is_executable or vwap.price is None:
            defer_or_finalize(CycleReason.INSUFFICIENT_DEPTH)
            return
        # Minimums apply to the aggregate Lighter operation, not to each
        # subminimum reservation that contributed to it.
        if not _minimum_ok_with_notional(
            cycle,
            Venue.LIGHTER,
            executable,
            vwap.price,
            notional_usd=vwap.notional_usd,
        ):
            defer_or_finalize(CycleReason.MINIMUM_RESIDUE)
            return

        # Attribute exact level notional to each reservation in order.  The
        # sum is the exact aggregate VWAP notional, while no rounded VWAP is
        # ever used to reconstruct fees or cashflow.
        level_index = 0
        level_remaining = levels[0].canonical_quantity if levels else _ZERO

        def consume_notional(quantity: Decimal) -> Decimal:
            nonlocal level_index, level_remaining
            remaining = quantity
            notional = _ZERO
            while remaining > _ZERO and level_index < len(levels):
                level = levels[level_index]
                if level_remaining <= _ZERO:
                    level_index += 1
                    if level_index < len(levels):
                        level_remaining = levels[level_index].canonical_quantity
                    continue
                taken = min(remaining, level_remaining)
                notional += taken * level.canonical_price
                remaining -= taken
                level_remaining -= taken
            if remaining > _ZERO:
                raise ArithmeticError("close allocation exceeded selected book depth")
            return notional

        identity = CausalSourceIdentity.from_book(book)
        remaining_to_execute = executable
        actual_executed = _ZERO
        for action in actions:
            quantity = min(action.remaining_quantity, remaining_to_execute)
            if quantity <= _ZERO:
                continue
            notional = consume_notional(quantity)
            _append_fill(
                cycle,
                action_id=action.action_id,
                venue=Venue.LIGHTER,
                side=Side.SELL,
                role=LiquidityRole.TAKER,
                quantity=quantity,
                price=notional / quantity,
                reason="EXIT_LIGHTER_TAKER_CLOSE",
                observed_ns=at_ns,
                processing_ns=at_ns,
                evidence_id=book.book_revision_id,
                source_identity=identity,
                session=book.stream_session_id,
                recovery=book.recovery_generation,
                book_revision_id=book.book_revision_id,
                notional_usd=notional,
            )
            action.executed_quantity += quantity
            action.reason = "EXIT_LIGHTER_TAKER_CLOSE"
            if book.book_revision_id not in action.evidence_ids:
                action.evidence_ids.append(book.book_revision_id)
            remaining_to_execute -= quantity
            actual_executed += quantity
            if action.remaining_quantity <= _ZERO:
                action.status = CycleActionStatus.COMPLETED
            else:
                action.status = CycleActionStatus.PENDING
                action.due_ns = None
                action.reason = "EXIT_CLOSE_WAITING_FOR_AGGREGATION"
            if remaining_to_execute <= _ZERO:
                break
        # Actions not reached because depth was exhausted keep their exact
        # unexecuted remainder and become deferred reservations as well.
        for action in actions:
            if action.status is CycleActionStatus.PENDING:
                action.due_ns = None
                action.reason = "EXIT_CLOSE_WAITING_FOR_AGGREGATION"
        cycle.paired_lighter_quantity = max(
            _ZERO,
            cycle.paired_lighter_quantity - actual_executed,
        )
        cycle.exit_close_consumed_quantity += actual_executed

    def _execute_taker(self, cycle: _S1bCycle, action: _MutableAction, at_ns: int) -> None:
        if action.kind is CycleActionKind.ENTRY_HEDGE:
            self._s1b_execute_due_entry_hedges(cycle, at_ns)
            return
        descriptor = cycle.scheduled_takers.get(action.action_id)
        if descriptor is None:
            return
        venue, side, requested, reason = descriptor
        if action.kind is CycleActionKind.ENTRY_HEDGE:
            position_cap = max(_ZERO, cycle.entry_observed_quantity - cycle.hedged_quantity)
        elif action.kind is CycleActionKind.UNMATCHED_RISEX_UNWIND:
            position_cap = cycle.unmatched_entry_quantity
        elif action.kind is CycleActionKind.EXIT_HEDGE_CLOSE:
            position_cap = cycle.paired_lighter_quantity
        elif action.kind is CycleActionKind.FORCED_RISEX_UNWIND:
            position_cap = cycle.paired_risex_quantity
        elif action.kind is CycleActionKind.FORCED_LIGHTER_UNWIND:
            position_cap = max(_ZERO, cycle.paired_lighter_quantity - _s1b_pending_quantity(cycle, CycleActionKind.EXIT_HEDGE_CLOSE))
        else:
            position_cap = requested
        requested = min(requested, position_cap)
        if requested <= _ZERO:
            _s1b_set_action(cycle, action, status=CycleActionStatus.NOT_REQUIRED, executed=_ZERO, reason=CycleReason.OVER_CLOSE_BLOCKED.value)
            return
        # Each taker operation is executable against its own venue-local
        # book.  Paired books are required only when the exit maker quote is
        # formed, because that formula is hedge-anchored.  Requiring the
        # other venue again here would incorrectly block an accumulated
        # Lighter hedge or a forced one-sided close when its own book is
        # sufficient and fresh.
        book, book_reason = _select_book(cycle, venue, at_ns)
        if book_reason is not None or book is None:
            self._action_data_failure(cycle, action, book_reason or CycleReason.REQUIRED_ACTION_DATA_MISSING)
            return
        levels = book.asks if side is Side.BUY else book.bids
        available = sum((level.canonical_quantity for level in levels if level.canonical_quantity > _ZERO), _ZERO)
        if available <= _ZERO:
            _s1b_set_action(cycle, action, status=CycleActionStatus.COMPLETED, executed=_ZERO, reason=CycleReason.INSUFFICIENT_DEPTH.value)
            if action.kind is CycleActionKind.ENTRY_HEDGE:
                cycle.add_reason(CycleReason.INSUFFICIENT_DEPTH)
            else:
                self._policy_block(cycle, _POLICY_BLOCKED_RESIDUAL)
            return
        step = _venue_quantity_step(cycle, venue)
        if step is None:
            self._action_data_failure(cycle, action, CycleReason.REQUIRED_ACTION_DATA_MISSING)
            return
        executable = _floor_quantity(min(requested, available), step)
        if executable <= _ZERO:
            failure_reason = CycleReason.GRID_RESIDUE
            _s1b_set_action(cycle, action, status=CycleActionStatus.COMPLETED, executed=_ZERO, reason=failure_reason.value)
            cycle.add_reason(failure_reason)
            if action.kind is not CycleActionKind.ENTRY_HEDGE:
                self._policy_block(cycle, _POLICY_BLOCKED_MINIMUM)
            return
        vwap = exact_quantity_vwap(side, executable, tuple(book.bids), tuple(book.asks))
        if not vwap.is_executable or vwap.price is None:
            _s1b_set_action(cycle, action, status=CycleActionStatus.COMPLETED, executed=_ZERO, reason=CycleReason.INSUFFICIENT_DEPTH.value)
            if action.kind is CycleActionKind.ENTRY_HEDGE:
                cycle.add_reason(CycleReason.INSUFFICIENT_DEPTH)
            else:
                self._policy_block(cycle, _POLICY_BLOCKED_RESIDUAL)
            return
        if not _minimum_ok_with_notional(cycle, venue, executable, vwap.price, notional_usd=vwap.notional_usd):
            _s1b_set_action(cycle, action, status=CycleActionStatus.COMPLETED, executed=_ZERO, reason=CycleReason.MINIMUM_RESIDUE.value)
            cycle.add_reason(CycleReason.MINIMUM_RESIDUE)
            if action.kind is CycleActionKind.ENTRY_HEDGE:
                cycle.add_reason(CycleReason.HEDGE_PARTIAL)
            else:
                self._policy_block(cycle, _POLICY_BLOCKED_MINIMUM)
            return
        identity = CausalSourceIdentity.from_book(book)
        fill = _append_fill(
            cycle,
            action_id=action.action_id,
            venue=venue,
            side=side,
            role=LiquidityRole.TAKER,
            quantity=executable,
            price=vwap.price,
            reason=reason,
            observed_ns=at_ns,
            processing_ns=at_ns,
            evidence_id=book.book_revision_id,
            source_identity=identity,
            session=book.stream_session_id,
            recovery=book.recovery_generation,
            book_revision_id=book.book_revision_id,
            notional_usd=vwap.notional_usd,
        )
        _s1b_set_action(cycle, action, status=CycleActionStatus.COMPLETED, executed=executable, reason=CycleReason.HEDGE_PARTIAL.value if executable < requested else reason, evidence_id=book.book_revision_id)
        if executable < requested:
            cycle.add_reason(CycleReason.HEDGE_PARTIAL)
            cycle.add_reason(CycleReason.INSUFFICIENT_DEPTH)
        if action.kind is CycleActionKind.ENTRY_HEDGE:
            cycle.hedged_quantity += executable
            cycle.paired_risex_quantity += executable
            cycle.paired_lighter_quantity += executable
            self._s1b_update_unmatched(cycle)
            if cycle.paired_risex_quantity > _ZERO and not cycle.exit_chosen:
                self._s1b_pair_formed(cycle, at_ns)
        elif action.kind is CycleActionKind.UNMATCHED_RISEX_UNWIND:
            cycle.unmatched_entry_quantity = max(_ZERO, cycle.unmatched_entry_quantity - executable)
        elif action.kind is CycleActionKind.EXIT_HEDGE_CLOSE:
            cycle.paired_lighter_quantity = max(_ZERO, cycle.paired_lighter_quantity - executable)
        elif action.kind is CycleActionKind.FORCED_RISEX_UNWIND:
            cycle.paired_risex_quantity = max(_ZERO, cycle.paired_risex_quantity - executable)
        elif action.kind is CycleActionKind.FORCED_LIGHTER_UNWIND:
            cycle.paired_lighter_quantity = max(_ZERO, cycle.paired_lighter_quantity - executable)

    def _observe_blocked_terminal(self, cycle: _S1bCycle, event: CausalEvent) -> None:
        """Retain post-block public observations for bounded valuation.

        A policy block stops new decisions and execution actions in that
        lane, but it does not stop the observer.  Books, clocks, and gaps
        through the observation boundary remain auditable; trades are
        intentionally ignored so a blocked lane can never create a new fill.
        """

        if event.venue not in {Venue.RISEX, Venue.LIGHTER} or event.canonical_market != cycle.quote_version.canonical_market:
            cycle.ignored_event_count += 1
            return
        identity_key = _event_identity_key(event)
        signature = _event_signature(event)
        if identity_key is not None:
            previous = cycle.seen_events.get(identity_key)
            if previous is not None:
                if previous == signature:
                    cycle.duplicate_event_count += 1
                    cycle.ignored_event_count += 1
                else:
                    cycle.add_reason(CycleReason.DUPLICATE_CONFLICT)
                return
            cycle.seen_events[identity_key] = signature
        stream_key = (event.stream_key, event.kind)
        previous_time = cycle.last_stream_time.get(stream_key)
        position = _stream_position(event)
        previous_position = cycle.last_stream_position.get(stream_key)
        if (
            previous_time is not None and event.causal_monotonic_ns < previous_time
        ) or (
            previous_position is not None
            and position is not None
            and position < previous_position
        ):
            cycle.add_reason(CycleReason.LATE_OLDER_EVENT)
            cycle.ignored_event_count += 1
            return
        cycle.last_stream_time[stream_key] = max(
            previous_time or event.causal_monotonic_ns,
            event.causal_monotonic_ns,
        )
        if position is not None:
            cycle.last_stream_position[stream_key] = position
        if event.kind is CausalEventKind.BOOK:
            self._record_book(cycle, event)
        elif event.kind is CausalEventKind.DATA_GAP:
            assert event.gap is not None
            cycle.gaps.append(event.gap)
        ready = _processing_ready_ns(event)
        cycle.current_ns = max(
            cycle.current_ns,
            event.causal_monotonic_ns if ready is None else ready,
        )

    def advance(
        self,
        event: CausalEvent | TradeEvidence | BookEvidence | DataGapEvidence | CycleClock | int | None = None,
        *,
        scenario: CycleScenario = CycleScenario.PRIMARY,
        at_monotonic_ns: int | None = None,
    ) -> Any:
        lane = self._lane(scenario)
        scenario = self._scenario(scenario)
        if isinstance(event, CycleClock):
            if at_monotonic_ns is not None:
                raise ValueError("clock event and at_monotonic_ns are mutually exclusive")
            at_monotonic_ns = event.at_monotonic_ns
            event = None
        elif isinstance(event, int) and not isinstance(event, bool):
            if at_monotonic_ns is not None:
                raise ValueError("clock integer and at_monotonic_ns are mutually exclusive")
            at_monotonic_ns = event
            event = None
        if lane.active is None:
            terminal = lane.terminal_cycles[-1] if lane.terminal_cycles else None
            if isinstance(terminal, _S1bCycle) and terminal.policy_blocked:
                if event is None:
                    if at_monotonic_ns is None:
                        raise RuntimeError("no admitted cycle is pending")
                    self.advance_clock(at_monotonic_ns, scenario=scenario)
                    return None
                causal_event = _coerce_event(event)
                terminal.event_count += 1
                self._observe_blocked_terminal(terminal, causal_event)
                lane.last_result = self._result(terminal, terminal=True)
                return None
            if event is None or at_monotonic_ns is not None:
                raise RuntimeError("no admitted cycle is pending")
            causal_event = _coerce_event(event)
            if isinstance(terminal, _S1bCycle):
                if self._s1b_entry_version_for(terminal, causal_event):
                    self._s1b_entry_late_uncertainty(terminal, causal_event)
                    lane.halted_unresolved = True
                    lane.last_result = self._result(terminal, terminal=True)
                elif self._s1b_exit_candidate(terminal, causal_event):
                    self._s1b_exit_late_uncertainty(terminal, causal_event)
                    lane.halted_unresolved = True
                    lane.last_result = self._result(terminal, terminal=True)
                else:
                    terminal.ignored_event_count += 1
            return None
        cycle = lane.active
        if not isinstance(cycle, _S1bCycle):
            raise RuntimeError("S1b lane contains an incompatible cycle")
        if event is None:
            if at_monotonic_ns is None:
                raise ValueError("advance requires an event or clock boundary")
            self.advance_clock(at_monotonic_ns, scenario=scenario)
            return None
        causal_event = _coerce_event(event)
        cycle.event_count += 1
        self._accept_event(cycle, causal_event)
        lane.last_result = self._result(cycle)
        if cycle.phase in {_S1bPhase.COMPLETE, _S1bPhase.ABORTED, _S1bPhase.UNRESOLVED}:
            self._latch_terminal(lane, cycle)
        return None

    def advance_clock(self, at_monotonic_ns: int, *, scenario: CycleScenario = CycleScenario.PRIMARY) -> None:
        lane = self._lane(scenario)
        if lane.active is None:
            terminal = lane.terminal_cycles[-1] if lane.terminal_cycles else None
            if not isinstance(terminal, _S1bCycle) or not terminal.policy_blocked:
                raise RuntimeError("no admitted cycle is pending")
            if isinstance(at_monotonic_ns, bool) or not isinstance(at_monotonic_ns, int) or at_monotonic_ns < 0:
                raise ValueError("at_monotonic_ns must be a non-negative integer")
            if at_monotonic_ns >= terminal.current_ns:
                terminal.current_ns = at_monotonic_ns
            else:
                terminal.add_reason(CycleReason.LATE_OLDER_EVENT)
            lane.last_result = self._result(terminal, terminal=True)
            return
        if not isinstance(lane.active, _S1bCycle):
            raise RuntimeError("S1b lane contains an incompatible cycle")
        cycle = lane.active
        if isinstance(at_monotonic_ns, bool) or not isinstance(at_monotonic_ns, int) or at_monotonic_ns < 0:
            raise ValueError("at_monotonic_ns must be a non-negative integer")
        if at_monotonic_ns < cycle.current_ns:
            self._halt(cycle, CycleReason.LATE_OLDER_EVENT)
        else:
            self._run_due_until(cycle, at_monotonic_ns)
        lane.last_result = self._result(cycle)
        if cycle.phase in {_S1bPhase.COMPLETE, _S1bPhase.ABORTED, _S1bPhase.UNRESOLVED}:
            self._latch_terminal(lane, cycle)

    clock = advance_clock

    def finish(self, *, scenario: CycleScenario = CycleScenario.PRIMARY, end_monotonic_ns: int | None = None) -> S1bCycleResult:
        lane = self._lane(scenario)
        if lane.active is None:
            terminal = lane.terminal_cycles[-1] if lane.terminal_cycles else None
            if isinstance(terminal, _S1bCycle) and terminal.policy_blocked:
                if end_monotonic_ns is not None:
                    self.advance_clock(end_monotonic_ns, scenario=scenario)
                    terminal.observation_end_ns = end_monotonic_ns
                lane.last_result = self._result(terminal, terminal=True)
        else:
            cycle = lane.active
            if not isinstance(cycle, _S1bCycle):
                raise RuntimeError("S1b lane contains an incompatible cycle")
            if end_monotonic_ns is not None:
                self.advance_clock(end_monotonic_ns, scenario=scenario)
            if lane.active is not None and isinstance(lane.active, _S1bCycle):
                cycle = lane.active
                cycle.observation_end_ns = end_monotonic_ns if end_monotonic_ns is not None else cycle.current_ns
                if cycle.phase is _S1bPhase.ENTRY_REQUOTE_WAIT and cycle.entry_observed_quantity == _ZERO and not _s1b_pending_actions(cycle):
                    cycle.add_reason(CycleReason.NO_ENTRY)
                    cycle.phase = _S1bPhase.ABORTED
                    cycle.terminal_ns = cycle.current_ns
                    self._latch_terminal(lane, cycle)
                elif cycle.phase not in {_S1bPhase.COMPLETE, _S1bPhase.UNRESOLVED, _S1bPhase.ABORTED}:
                    cycle.observation_end_ns = cycle.current_ns
        result = lane.last_result
        if result is None:
            raise RuntimeError("no cycle has been admitted")
        return result  # type: ignore[return-value]

    def run(
        self,
        quote_version: QuoteVersion,
        events: Iterable[CausalEvent | TradeEvidence | BookEvidence | DataGapEvidence],
        *,
        scenario: CycleScenario = CycleScenario.PRIMARY,
        source_books: Iterable[BookEvidence] = (),
        end_monotonic_ns: int | None = None,
    ) -> S1bCycleResult:
        admission = self.admit(quote_version, scenario=scenario, source_books=source_books)
        if not admission.accepted:
            lane = self._lane(scenario)
            if isinstance(lane.last_result, S1bCycleResult):
                return lane.last_result
            if admission.reason in {item.value for item in CycleReason}:
                return self._admission_failure(quote_version, self._scenario(scenario), CycleReason(admission.reason))
            raise CycleAdmissionError(admission)
        for event in events:
            self.advance(event, scenario=scenario)
        return self.finish(scenario=scenario, end_monotonic_ns=end_monotonic_ns)

    replay = run

    def run_sequence(self, attempts: Iterable[CycleAttempt]) -> tuple[S1bCycleResult, ...]:
        results: list[S1bCycleResult] = []
        for attempt in attempts:
            if not isinstance(attempt, CycleAttempt):
                raise TypeError("run_sequence expects CycleAttempt values")
            admission = self.admit(attempt.quote_version, scenario=attempt.scenario, source_books=attempt.source_books)
            if not admission.accepted:
                continue
            for event in attempt.events:
                self.advance(event, scenario=attempt.scenario)
            self.finish(scenario=attempt.scenario, end_monotonic_ns=attempt.end_monotonic_ns)
        for scenario in CycleScenario:
            results.extend(self.retained_results(scenario, include_active=True))
        return tuple(results)

    def _result(self, cycle: _S1bCycle, *, terminal: bool = False) -> S1bCycleResult:
        if cycle.unresolved or cycle.policy_blocked:
            status = CycleTerminalState.UNRESOLVED
        elif cycle.phase is _S1bPhase.COMPLETE:
            status = CycleTerminalState.FORCED if cycle.forced_used else CycleTerminalState.NORMAL
        elif cycle.phase is _S1bPhase.ABORTED:
            status = CycleTerminalState.ABORTED
        else:
            status = CycleTerminalState.PENDING
        pending = tuple(action.public() for action in cycle.actions if action.status in {CycleActionStatus.PENDING, CycleActionStatus.UNRESOLVED})
        positions = cycle.positions
        flat = status in {CycleTerminalState.NORMAL, CycleTerminalState.FORCED, CycleTerminalState.ABORTED} and positions.is_zero and not pending
        cashflow_complete = status in {CycleTerminalState.NORMAL, CycleTerminalState.FORCED} and flat and len(cycle.fills) == len(cycle.fees) == len(cycle.cashflows)
        terminal_ns = cycle.terminal_ns if terminal else (cycle.current_ns if status is CycleTerminalState.UNRESOLVED else None)
        if terminal and cycle.terminal_ns is not None:
            terminal_ns = cycle.terminal_ns
        holding = None if cycle.first_maker_fill_ns is None or terminal_ns is None else max(_ZERO, Decimal(terminal_ns - cycle.first_maker_fill_ns)).to_integral_value()
        holding_ns = None if holding is None else int(holding)
        if cycle.unmatched_started_ns is None:
            unmatched_duration = 0
        elif cycle.unmatched_resolved_ns is not None:
            unmatched_duration = max(0, cycle.unmatched_resolved_ns - cycle.unmatched_started_ns)
        elif terminal_ns is not None:
            unmatched_duration = max(0, terminal_ns - cycle.unmatched_started_ns)
        else:
            unmatched_duration = None
        (
            mark_risex,
            mark_lighter,
            marked_inventory,
            marked_pnl,
            mark_risex_observation,
            mark_lighter_observation,
        ) = _s1b_mark_snapshot(cycle)
        observation_ns = cycle.observation_end_ns or cycle.current_ns
        decision_ns = cycle.initial_quote_version.decision_ready_monotonic_ns or cycle.current_ns
        if cycle.policy_blocked and cycle.blocked_started_ns is not None:
            active_duration_ns = max(0, cycle.blocked_started_ns - decision_ns)
            blocked_duration_ns = max(0, observation_ns - cycle.blocked_started_ns)
        else:
            active_duration_ns = max(0, observation_ns - decision_ns)
            blocked_duration_ns = None
        marked_at_ns = (
            cycle.current_ns
            if marked_pnl is not None or positions.is_zero
            else None
        )
        executable_close = (
            None
            if cycle.policy_blocked or cycle.unresolved
            else _s1b_liquidating_close_estimate(cycle)
        )
        return S1bCycleResult(
            scenario=cycle.scenario,
            quote_version_id=cycle.initial_quote_version.version_id,
            canonical_market=cycle.initial_quote_version.canonical_market,
            status=status,
            reason_codes=tuple(cycle.reasons),
            entry_measurement=self._entry_measurement(cycle),
            exit_measurement=self._exit_measurement(cycle),
            entry_edge_usd=cycle.initial_quote_version.quote.actual_edge_usd,
            entry_quantity=cycle.entry_observed_quantity,
            hedged_quantity=cycle.hedged_quantity,
            unmatched_entry_quantity=cycle.unmatched_entry_quantity,
            exit_price=cycle.exit_price,
            first_maker_fill_monotonic_ns=cycle.first_maker_fill_ns,
            max_hold_deadline_monotonic_ns=cycle.max_hold_deadline_ns,
            terminal_monotonic_ns=terminal_ns,
            positions=positions,
            ledger=CycleLedger(tuple(cycle.fills), tuple(cycle.fees), tuple(cycle.cashflows)),
            actions=tuple(action.public() for action in cycle.actions),
            cashflow_complete=cashflow_complete,
            complete_execution_pnl_usd=cycle_net_cashflow(cycle) if cashflow_complete else None,
            holding_duration_ns=holding_ns,
            unmatched_exposure_duration_ns=unmatched_duration,
            q_cap=cycle.q_cap,
            entry_version_ids=tuple(item.quote_version.version_id for item in cycle.entry_versions),
            active_entry_version_id=(None if cycle.active_entry_version is None else cycle.active_entry_version.quote_version.version_id),
            pending_entry_hedge_quantity=_s1b_pending_quantity(cycle, CycleActionKind.ENTRY_HEDGE),
            pending_exit_close_quantity=_s1b_pending_quantity(cycle, CycleActionKind.EXIT_HEDGE_CLOSE),
            marked_risex_price=mark_risex,
            marked_lighter_price=mark_lighter,
            marked_inventory_usd=marked_inventory,
            marked_execution_only_pnl_usd=marked_pnl,
            executable_liquidating_close_estimate_usd=executable_close,
            policy_blocked=cycle.policy_blocked,
            observation_end_monotonic_ns=cycle.observation_end_ns,
            fill_model=self.fill_model,
            marked_at_monotonic_ns=marked_at_ns,
            marked_risex_book_revision_id=(
                None
                if mark_risex_observation is None
                else mark_risex_observation.book.book_revision_id
            ),
            marked_lighter_book_revision_id=(
                None
                if mark_lighter_observation is None
                else mark_lighter_observation.book.book_revision_id
            ),
            marked_risex_book_received_monotonic_ns=(
                None
                if mark_risex_observation is None
                else mark_risex_observation.book.received_monotonic_ns
            ),
            marked_lighter_book_received_monotonic_ns=(
                None
                if mark_lighter_observation is None
                else mark_lighter_observation.book.received_monotonic_ns
            ),
            simulation_active_duration_ns=active_duration_ns,
            blocked_duration_ns=blocked_duration_ns,
        )

    def _entry_measurement(self, cycle: _S1bCycle) -> CausalQuoteMeasurement:
        if cycle.entry_uncertainty:
            outcome = CausalOutcome.CAUSAL_UNCERTAIN
        elif cycle.entry_observed_quantity == cycle.q_cap and cycle.q_cap > _ZERO:
            outcome = CausalOutcome.FULL_FILL
        elif cycle.entry_observed_quantity > _ZERO:
            outcome = CausalOutcome.PARTIAL_FILL
        elif all(item.cancel_effective_ns is not None for item in cycle.entry_versions) and cycle.entry_versions:
            outcome = CausalOutcome.CANCELLED_NO_FILL
        else:
            outcome = CausalOutcome.NO_FILL
        return _make_causal_measurement(
            cycle,
            quote=cycle.entry_measurement_quote,
            fills=cycle.entry_fills,
            decisions=cycle.entry_decisions,
            observed_quantity=cycle.entry_observed_quantity,
            remaining_quantity=max(_ZERO, cycle.q_cap - cycle.entry_observed_quantity),
            uncertainty=cycle.entry_uncertainty,
            effective_cancel_ns=cycle.entry_cancel_effective_ns,
            cancel_requested_ns=cycle.entry_cancel_requested_ns,
            outcome=outcome,
        )

    def _exit_measurement(self, cycle: _S1bCycle) -> CausalQuoteMeasurement | None:
        if cycle.exit_quote is None:
            return None
        observed = sum((fill.consumed_quantity for fill in cycle.exit_fills), _ZERO)
        if cycle.exit_uncertainty:
            outcome = CausalOutcome.CAUSAL_UNCERTAIN
        elif observed == cycle.exit_target_quantity and cycle.exit_target_quantity > _ZERO:
            outcome = CausalOutcome.FULL_FILL
        elif observed > _ZERO:
            outcome = CausalOutcome.PARTIAL_FILL
        elif cycle.exit_cancel_effective_ns is not None and cycle.current_ns >= cycle.exit_cancel_effective_ns:
            outcome = CausalOutcome.CANCELLED_NO_FILL
        else:
            outcome = CausalOutcome.NO_FILL
        return _make_causal_measurement(
            cycle,
            quote=cycle.exit_quote,
            fills=cycle.exit_fills,
            decisions=cycle.exit_decisions,
            observed_quantity=observed,
            remaining_quantity=max(_ZERO, cycle.exit_target_quantity - observed),
            uncertainty=cycle.exit_uncertainty,
            effective_cancel_ns=cycle.exit_cancel_effective_ns,
            cancel_requested_ns=cycle.exit_cancel_requested_ns,
            outcome=outcome,
        )

    def _latch_terminal(self, lane: Any, cycle: _S1bCycle) -> None:
        lane.last_result = self._result(cycle, terminal=True)
        lane.last_terminal_ns = cycle.terminal_ns
        if cycle.unresolved or cycle.policy_blocked or not lane.last_result.is_flat:
            lane.halted_unresolved = True
        if not any(existing is cycle for existing in lane.terminal_cycles):
            lane.terminal_cycles.append(cycle)
        lane.terminal_cycle = cycle
        lane.active = None


S1bCycleKernel = Scv1S1bKernel


def scv1_s1b_policy() -> CyclePolicy:
    """Return the frozen policy used by the SCV1-S1b kernel."""

    return CyclePolicy()


def _four_s1b_kernels(
    policy: CyclePolicy | None = None,
) -> tuple[Scv1S1bKernel, ...]:
    return tuple(
        Scv1S1bKernel(
            policy,
            fill_model=model,
        )
        for model in (CycleFillModel.TRADE_THROUGH_ONLY, CycleFillModel.TOUCH_ALLOWED)
        for _scenario in (CycleScenario.PRIMARY, CycleScenario.STRESS)
    )


def run_scv1_s1b(
    quote_version: QuoteVersion,
    events: Iterable[CausalEvent | TradeEvidence | BookEvidence | DataGapEvidence],
    *,
    fill_model: CycleFillModel | str,
    scenario: CycleScenario = CycleScenario.PRIMARY,
    policy: CyclePolicy | None = None,
    source_books: Iterable[BookEvidence] = (),
    end_monotonic_ns: int | None = None,
) -> S1bCycleResult:
    kernel = Scv1S1bKernel(policy, fill_model=fill_model)
    return kernel.run(
        quote_version,
        events,
        scenario=scenario,
        source_books=source_books,
        end_monotonic_ns=end_monotonic_ns,
    )


def run_scv1_s1b_alternatives(
    quote_version: QuoteVersion,
    events: Iterable[CausalEvent | TradeEvidence | BookEvidence | DataGapEvidence],
    *,
    policy: CyclePolicy | None = None,
    source_books: Iterable[BookEvidence] = (),
    end_monotonic_ns: int | None = None,
) -> tuple[S1bCycleResult, S1bCycleResult, S1bCycleResult, S1bCycleResult]:
    materialized = tuple(events)
    books = tuple(source_books)
    return tuple(
        run_scv1_s1b(
            quote_version,
            materialized,
            fill_model=model,
            scenario=scenario,
            policy=policy,
            source_books=books,
            end_monotonic_ns=end_monotonic_ns,
        )
        for model in (CycleFillModel.TRADE_THROUGH_ONLY, CycleFillModel.TOUCH_ALLOWED)
        for scenario in (CycleScenario.PRIMARY, CycleScenario.STRESS)
    )  # type: ignore[return-value]


def _d1_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        result = Decimal(str(value))
    except Exception:
        return None
    return result if result.is_finite() else None


_D1_DECISION_INTERVAL_NS = 1_000_000_000
_D1_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _d1_parse_utc(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _d1_market_payload(market: Any) -> dict[str, Any]:
    def primitive(value: Any) -> Any:
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, (Venue, StrEnum)):
            return value.value
        return value

    return {
        name: primitive(getattr(market, name))
        for name in (
            "canonical_asset",
            "venue",
            "venue_symbol",
            "market_type",
            "contract_type",
            "base_multiplier",
            "quote_asset",
            "settlement_asset",
            "tick_size_raw",
            "quantity_step_raw",
            "minimum_quantity_raw",
            "minimum_notional_usd",
            "minimum_fee_notional_usd",
            "is_active",
            "is_rfq",
            "is_off_hours",
        )
    }


def _d1_discover_policy_metadata(
    path: Path,
) -> tuple[dict[str, Any], dict[str, Any] | None, tuple[str, ...]]:
    """Find immutable config without decoding any historical quote result."""

    metadata: dict[str, Any] = {}
    historical_policy: dict[str, Any] | None = None
    errors: list[str] = []
    with path.open("rb") as handle:
        for raw_line in handle:
            if not raw_line.strip():
                continue
            try:
                record = json.loads(raw_line.decode("utf-8"))
            except Exception as exc:
                if len(errors) < 8:
                    errors.append(f"{type(exc).__name__}: {exc}")
                continue
            if not isinstance(record, dict):
                if len(errors) < 8:
                    errors.append("ValueError: D1 JSONL record must be an object")
                continue
            kind = record.get("kind")
            if kind == "RUN_METADATA" and isinstance(record.get("metadata"), dict):
                if not metadata:
                    metadata = dict(record["metadata"])
            elif kind == "CYCLE_DECISION" and historical_policy is None:
                # The historical decision contributes only the two immutable
                # venue market descriptors.  Its quote ID, timestamps, price,
                # quantity, outcome, and source books never enter replay.
                quote_version = record.get("quote_version")
                quote = quote_version.get("quote") if isinstance(quote_version, Mapping) else None
                policy = quote.get("policy") if isinstance(quote, Mapping) else None
                if isinstance(policy, Mapping) and all(
                    isinstance(policy.get(name), Mapping)
                    for name in ("risex_market", "lighter_market")
                ):
                    historical_policy = dict(policy)
            if historical_policy is not None and (
                isinstance(metadata.get("policy"), Mapping) or not metadata
            ):
                break
    return metadata, historical_policy, tuple(errors)


def _d1_build_policy(
    metadata: Mapping[str, Any],
    historical_policy: Mapping[str, Any] | None,
    market_from_dict: Any,
) -> tuple[QuotePolicy | None, dict[str, Any], str | None]:
    metadata_policy = metadata.get("policy")
    if isinstance(metadata_policy, Mapping):
        scalar_source = metadata_policy
        scalar_provenance = "RUN_METADATA.metadata.policy"
    elif isinstance(historical_policy, Mapping):
        scalar_source = historical_policy
        scalar_provenance = "CYCLE_DECISION.quote_version.quote.policy_fallback_only"
    else:
        return None, {"scalar_source": None, "market_source": None}, "MISSING_IMMUTABLE_POLICY"

    market_source = (
        historical_policy
        if isinstance(historical_policy, Mapping)
        else scalar_source
    )

    def text(name: str) -> str:
        value = scalar_source.get(name)
        if not isinstance(value, str) or not value:
            raise ValueError(f"policy field {name} is missing")
        return value

    def decimal(name: str) -> Decimal:
        value = _d1_decimal(scalar_source.get(name))
        if value is None:
            raise ValueError(f"policy field {name} is missing or invalid")
        return value

    try:
        risex_market = market_from_dict(
            market_source.get("risex_market"),
            context="S1b D1 RISEx market metadata",
        )
        lighter_market = market_from_dict(
            market_source.get("lighter_market"),
            context="S1b D1 Lighter market metadata",
        )
        if risex_market is None or lighter_market is None:
            raise ValueError("both immutable market descriptors are required")
        policy = QuotePolicy(
            canonical_market=text("canonical_market"),
            direction=SpreadDirection(text("direction")),
            target_notional_usd=decimal("target_notional_usd"),
            target_margin_bps=decimal("target_margin_bps"),
            risex_maker_fee_rate=decimal("risex_maker_fee_rate"),
            lighter_taker_fee_rate=decimal("lighter_taker_fee_rate"),
            risex_fee_source=text("risex_fee_source"),
            lighter_fee_source=text("lighter_fee_source"),
            risex_market=risex_market,
            lighter_market=lighter_market,
            fee_observed_or_configured_at=_d1_parse_utc(
                market_source.get("fee_observed_or_configured_at")
            ),
        )
        frozen = CyclePolicy()
        mismatches = [
            name
            for name in (
                "canonical_market",
                "direction",
                "target_notional_usd",
                "target_margin_bps",
                "risex_maker_fee_rate",
                "lighter_taker_fee_rate",
                "risex_fee_source",
                "lighter_fee_source",
            )
            if getattr(policy, name) != getattr(frozen, name)
        ]
        if mismatches:
            raise ValueError("frozen policy mismatch: " + ", ".join(mismatches))
    except Exception as exc:
        return (
            None,
            {
                "scalar_source": scalar_provenance,
                "market_source": "historical CYCLE_DECISION policy market metadata",
            },
            f"{type(exc).__name__}: {exc}",
        )
    return (
        policy,
        {
            "scalar_source": scalar_provenance,
            "market_source": "historical CYCLE_DECISION policy market metadata",
            "old_quote_fields_used": [],
        },
        None,
    )


def _d1_current_books(
    latest_books: Mapping[Venue, tuple[CausalEvent, BookEvidence]],
    at_ns: int,
    policy: QuotePolicy,
    frozen: CyclePolicy,
) -> tuple[tuple[BookEvidence, BookEvidence] | None, tuple[str, ...]]:
    selected: dict[Venue, BookEvidence] = {}
    reasons: list[str] = []
    for venue in (Venue.RISEX, Venue.LIGHTER):
        item = latest_books.get(venue)
        if item is None:
            reasons.append(f"MISSING_{venue.value}_BOOK")
            continue
        event, book = item
        if book.canonical_market != policy.canonical_market:
            reasons.append(f"{venue.value}_MARKET_MISMATCH")
        if not event.source_identity_complete or not event.identity_metadata_consistent:
            reasons.append(f"{venue.value}_IDENTITY_INCOMPLETE")
        ready = _processing_ready_ns(event)
        if ready is None:
            reasons.append(f"{venue.value}_TIMING_MISSING")
        elif ready > at_ns or book.received_monotonic_ns > at_ns:
            reasons.append(f"{venue.value}_NOT_READY")
        elif at_ns - book.received_monotonic_ns > frozen.input_freshness_max_age_ns:
            reasons.append(f"{venue.value}_STALE")
        if not book.fresh:
            reasons.append(f"{venue.value}_UNHEALTHY_FRESHNESS")
        if not book.is_sequence_healthy:
            reasons.append(f"{venue.value}_UNHEALTHY_SEQUENCE")
        if book.venue is not venue:
            reasons.append(f"{venue.value}_IDENTITY_MISMATCH")
        selected[venue] = book
    if len(selected) == 2:
        risex = selected[Venue.RISEX]
        lighter = selected[Venue.LIGHTER]
        if abs(risex.received_monotonic_ns - lighter.received_monotonic_ns) > frozen.input_receipt_skew_max_ns:
            reasons.append("INPUT_RECEIPT_SKEW")
    if reasons:
        return None, tuple(dict.fromkeys(reasons))
    return (selected[Venue.RISEX], selected[Venue.LIGHTER]), ()


def _d1_recomputed_version(
    policy: QuotePolicy,
    books: tuple[BookEvidence, BookEvidence],
    decision_ns: int,
    serial: int,
    fallback_created_utc: datetime,
) -> tuple[QuoteVersion | None, Any, str | None]:
    risex_book, lighter_book = books
    risex_market = policy.risex_market
    lighter_market = policy.lighter_market
    if risex_market is None or lighter_market is None:
        return None, None, "MISSING_IMMUTABLE_MARKET_METADATA"
    try:
        dynamic_policy = replace(
            policy,
            risex_best_bid=(risex_book.bids[0].canonical_price if risex_book.bids else None),
            risex_best_ask=(risex_book.asks[0].canonical_price if risex_book.asks else None),
            risex_tick_size=risex_market.tick_size_raw,
        )
        quote = build_hypothetical_maker_quote(
            dynamic_policy,
            lighter_book,
            risex_market=risex_market,
            lighter_market=lighter_market,
            risex_best_bid=dynamic_policy.risex_best_bid,
            risex_best_ask=dynamic_policy.risex_best_ask,
            risex_tick_size=dynamic_policy.risex_tick_size,
        )
    except (ArithmeticError, TypeError, ValueError) as exc:
        return None, None, f"QUOTE_RECOMPUTE_ERROR:{type(exc).__name__}:{exc}"
    if not quote.is_active:
        return None, quote, quote.outcome.value
    created_values = [
        book.received_utc
        for book in books
        if book.received_utc is not None
    ]
    created_utc = max(created_values) if created_values else fallback_created_utc
    ingress_values = [
        book.ingress_received_monotonic_ns
        for book in books
        if book.ingress_received_monotonic_ns is not None
    ]
    normalized_values = [
        book.normalized_ready_monotonic_ns
        for book in books
        if book.normalized_ready_monotonic_ns is not None
    ]
    ingress = max(ingress_values) if ingress_values else None
    normalized = max(normalized_values) if normalized_values and ingress is not None else None
    version = QuoteVersion(
        version_id=f"D1-REPLAY-{serial:08d}",
        quote=quote,
        quote_created_utc=created_utc,
        quote_created_monotonic_ns=max(book.received_monotonic_ns for book in books),
        stream_session_id=risex_book.stream_session_id,
        recovery_generation=risex_book.recovery_generation,
        hedge_stream_session_id=lighter_book.stream_session_id,
        hedge_recovery_generation=lighter_book.recovery_generation,
        risex_book_revision=risex_book.book_revision,
        lighter_book_revision=lighter_book.book_revision,
        risex_book_revision_id=risex_book.book_revision_id,
        lighter_book_revision_id=lighter_book.book_revision_id,
        ingress_received_monotonic_ns=ingress,
        normalized_ready_monotonic_ns=normalized,
        decision_ready_monotonic_ns=decision_ns,
    )
    return version, quote, None


def _d1_result_row(result: S1bCycleResult) -> dict[str, Any]:
    def decimal(value: Decimal | None) -> str | None:
        return None if value is None else str(value)

    return {
        "contract_version": result.contract_version,
        "fill_model": result.fill_model.value,
        "quote_version_id": result.quote_version_id,
        "status": result.status.value,
        "reasons": list(result.reason_codes),
        "q_cap": decimal(result.q_cap),
        "entry_version_ids": list(result.entry_version_ids),
        "entry_quantity": decimal(result.entry_quantity),
        "hedged_quantity": decimal(result.hedged_quantity),
        "unmatched_entry_quantity": decimal(result.unmatched_entry_quantity),
        "pending_entry_hedge_quantity": decimal(result.pending_entry_hedge_quantity),
        "pending_exit_close_quantity": decimal(result.pending_exit_close_quantity),
        "positions": {
            "risex_signed_quantity": decimal(result.positions.risex_signed_quantity),
            "lighter_signed_quantity": decimal(result.positions.lighter_signed_quantity),
            "paired_risex_quantity": decimal(result.positions.paired_risex_quantity),
            "paired_lighter_quantity": decimal(result.positions.paired_lighter_quantity),
            "unmatched_risex_quantity": decimal(result.positions.unmatched_risex_quantity),
            "position_state_proven": result.positions.authoritative,
        },
        "fill_count": len(result.fills),
        "closed": result.cashflow_complete,
        "closed_execution_pnl_usd": decimal(result.complete_execution_pnl_usd),
        "signed_cashflow_usd": decimal(result.ledger.signed_cashflow_usd),
        "net_cashflow_usd": decimal(result.ledger.net_cashflow_usd),
        "fees_usd": decimal(result.ledger.total_fees_usd),
        "stress_cost_usd": decimal(result.ledger.scenario_cost_usd),
        "turnover_usd": decimal(result.ledger.turnover_usd),
        "marked_risex_price": decimal(result.marked_risex_price),
        "marked_lighter_price": decimal(result.marked_lighter_price),
        "marked_inventory_usd": decimal(result.marked_inventory_usd),
        "marked_execution_only_pnl_usd": decimal(result.marked_execution_only_pnl_usd),
        "executable_liquidating_close_estimate_usd": decimal(result.executable_liquidating_close_estimate_usd),
        "marked_at_monotonic_ns": result.marked_at_monotonic_ns,
        "marked_risex_book_revision_id": result.marked_risex_book_revision_id,
        "marked_lighter_book_revision_id": result.marked_lighter_book_revision_id,
        "marked_risex_book_received_monotonic_ns": result.marked_risex_book_received_monotonic_ns,
        "marked_lighter_book_received_monotonic_ns": result.marked_lighter_book_received_monotonic_ns,
        "simulation_active_duration_ns": result.simulation_active_duration_ns,
        "blocked_duration_ns": result.blocked_duration_ns,
        "holding_duration_ns": result.holding_duration_ns,
        "unmatched_exposure_duration_ns": result.unmatched_exposure_duration_ns,
        "max_hold_deadline_monotonic_ns": result.max_hold_deadline_monotonic_ns,
        "observation_end_monotonic_ns": result.observation_end_monotonic_ns,
        "funding": result.funding_status,
        "policy_blocked": result.policy_blocked,
        "pending_actions": [action.action_id for action in result.pending_actions],
    }


def _d1_summary(results: Iterable[S1bCycleResult]) -> dict[str, Any]:
    result_list = tuple(results)
    statuses = Counter(result.status.value for result in result_list)
    reason_counts: Counter[str] = Counter()
    for result in result_list:
        reason_counts.update(result.reason_codes)

    def optional_total(values: Iterable[Decimal | None]) -> str | None:
        present = tuple(value for value in values if value is not None)
        return None if not present else str(sum(present, _ZERO))

    return {
        "episode_count": len(result_list),
        "status_counts": dict(sorted(statuses.items())),
        "fills": sum(len(result.fills) for result in result_list),
        "closed_count": sum(result.cashflow_complete for result in result_list),
        "forced_count": sum(result.status is CycleTerminalState.FORCED for result in result_list),
        "pending_count": sum(result.status is CycleTerminalState.PENDING for result in result_list),
        "blocked_count": sum(result.policy_blocked for result in result_list),
        "unresolved_count": sum(result.status is CycleTerminalState.UNRESOLVED for result in result_list),
        "entry_quantity": str(sum((result.entry_quantity for result in result_list), _ZERO)),
        "hedged_quantity": str(sum((result.hedged_quantity for result in result_list), _ZERO)),
        "signed_cashflow_usd": str(sum((result.ledger.signed_cashflow_usd for result in result_list), _ZERO)),
        "net_cashflow_usd": str(sum((result.ledger.net_cashflow_usd for result in result_list), _ZERO)),
        "fees_usd": str(sum((result.ledger.total_fees_usd for result in result_list), _ZERO)),
        "stress_cost_usd": str(sum((result.ledger.scenario_cost_usd for result in result_list), _ZERO)),
        "closed_execution_pnl_usd": str(sum((result.complete_execution_pnl_usd or _ZERO for result in result_list), _ZERO)),
        "marked_inventory_usd": optional_total(result.marked_inventory_usd for result in result_list),
        "marked_execution_only_pnl_usd": optional_total(
            result.marked_execution_only_pnl_usd for result in result_list
        ),
        "simulation_active_duration_ns": sum(
            result.simulation_active_duration_ns or 0 for result in result_list
        ),
        "blocked_duration_ns": sum(result.blocked_duration_ns or 0 for result in result_list),
        "marked_rows": sum(result.marked_execution_only_pnl_usd is not None for result in result_list),
        "reason_counts": dict(sorted(reason_counts.items())),
        "funding": "UNKNOWN",
    }


def _d1_records(path: str | os.PathLike[str]) -> Iterator[tuple[dict[str, Any], int, str]]:
    with Path(path).open("rb") as handle:
        for raw_line in handle:
            if not raw_line.strip():
                continue
            record = json.loads(raw_line.decode("utf-8"))
            if not isinstance(record, dict):
                raise ValueError("D1 JSONL record must be an object")
            yield record, len(raw_line), hashlib.sha256(raw_line).hexdigest()


def build_scv1_s1b_d1_report(
    source: str | os.PathLike[str],
    *,
    accepted_release: str | None = None,
    expected_records: int | None = None,
    expected_sha256: str | None = None,
) -> dict[str, Any]:
    """Replay persisted ``CYCLE_STREAM_INPUT`` records through four alternatives."""

    path = Path(source)
    if not path.is_file():
        raise FileNotFoundError(path)
    try:
        from .s3_cycle import _input_from_dict, _market_from_dict
    except ImportError as exc:
        raise RuntimeError("offline D1 decoder unavailable") from exc

    discovered_metadata, historical_policy, discovery_errors = _d1_discover_policy_metadata(path)
    metadata: dict[str, Any] = dict(discovered_metadata)
    policy, policy_provenance, policy_failure = _d1_build_policy(
        metadata,
        historical_policy,
        _market_from_dict,
    )
    fallback_created_utc = _d1_parse_utc(metadata.get("created_utc")) or _D1_EPOCH_UTC
    frozen = CyclePolicy()
    kernels = {
        (model, scenario): Scv1S1bKernel(fill_model=model)
        for model in CycleFillModel
        for scenario in CycleScenario
    }
    admissions: list[dict[str, Any]] = []
    decision_attempts: list[dict[str, Any]] = []
    per_key_results: dict[tuple[CycleFillModel, CycleScenario], list[S1bCycleResult]] = defaultdict(list)
    latest_books: dict[Venue, tuple[CausalEvent, BookEvidence]] = {}
    latest_book_keys: dict[Venue, tuple[int, int, int, int]] = {}
    record_count = 0
    byte_count = 0
    raw_hash = hashlib.sha256()
    stream_end: int | None = None
    run_failed = False
    failure_reason: str | None = None
    stream_input_count = 0
    clock_input_count = 0
    event_input_count = 0
    decode_error_count = len(discovery_errors)
    decode_error_samples: list[str] = list(discovery_errors)
    replay_error_count = 0
    replay_error_samples: list[str] = []
    historical_decision_count = 0
    historical_admission_count = 0
    historical_final_result_count = 0
    last_attempt_ns: int | None = None
    last_clock_ns: int | None = None

    def record_error(target: list[str], error: Exception | str) -> None:
        if len(target) < 8:
            target.append(
                str(error)
                if isinstance(error, str)
                else f"{type(error).__name__}: {error}"
            )

    def deliver(item: CausalEvent | CycleClock) -> None:
        nonlocal replay_error_count
        for (model, scenario), kernel in kernels.items():
            last = kernel.last_result(scenario)
            if isinstance(item, CycleClock):
                should_deliver = (
                    kernel.state(scenario) is CycleKernelState.PENDING
                    or isinstance(last, S1bCycleResult) and last.policy_blocked
                )
            else:
                should_deliver = (
                    kernel.state(scenario) is CycleKernelState.PENDING
                    or last is not None
                )
            if not should_deliver:
                continue
            try:
                kernel.advance(item, scenario=scenario)
            except RuntimeError as exc:
                replay_error_count += 1
                record_error(replay_error_samples, f"{model.value}/{scenario.value}: {exc}")

    def record_decision_attempt(at_ns: int) -> None:
        nonlocal last_attempt_ns
        if last_attempt_ns is not None and at_ns < last_attempt_ns + _D1_DECISION_INTERVAL_NS:
            return
        last_attempt_ns = at_ns
        attempt_index = len(decision_attempts)
        attempt: dict[str, Any] = {
            "attempt_index": attempt_index,
            "decision_monotonic_ns": at_ns,
            "clock_source": "CYCLE_STREAM_INPUT.CLOCK.at_monotonic_ns",
            "input_availability": "UNKNOWN",
            "input_reasons": [],
            "recomputed_quote": None,
            "admissions": [],
        }
        if policy is None:
            attempt["input_availability"] = "DATA_INSUFFICIENT"
            attempt["input_reasons"] = [policy_failure or "MISSING_IMMUTABLE_POLICY"]
            decision_attempts.append(attempt)
            return
        books, reasons = _d1_current_books(latest_books, at_ns, policy, frozen)
        if books is None:
            attempt["input_availability"] = "UNAVAILABLE"
            attempt["input_reasons"] = list(reasons)
            decision_attempts.append(attempt)
            return
        attempt["input_availability"] = "AVAILABLE"
        version, quote, reason = _d1_recomputed_version(
            policy,
            books,
            at_ns,
            attempt_index,
            fallback_created_utc,
        )
        if version is None:
            attempt["input_reasons"] = [
                reason
                or ("QUOTE_OUTCOME:" + quote.outcome.value if quote is not None else "QUOTE_RECOMPUTE_FAILED")
            ]
            attempt["recomputed_quote"] = {
                "outcome": quote.outcome.value if quote is not None else "RECOMPUTE_FAILED",
                "canonical_quantity": (
                    None if quote is None or quote.canonical_quantity is None else str(quote.canonical_quantity)
                ),
                "maker_price": None if quote is None or quote.maker_price is None else str(quote.maker_price),
                "reason": reason,
                "source_book_revision_ids": {
                    "risex": books[0].book_revision_id,
                    "lighter": books[1].book_revision_id,
                },
            }
            decision_attempts.append(attempt)
            return
        attempt["recomputed_quote"] = {
            "version_id": version.version_id,
            "outcome": version.quote.outcome.value,
            "canonical_quantity": (
                None if version.quote.canonical_quantity is None else str(version.quote.canonical_quantity)
            ),
            "maker_price": None if version.quote.maker_price is None else str(version.quote.maker_price),
            "lighter_vwap_price": (
                None if version.quote.lighter_vwap_price is None else str(version.quote.lighter_vwap_price)
            ),
            "source_book_revision_ids": {
                "risex": books[0].book_revision_id,
                "lighter": books[1].book_revision_id,
            },
        }
        for model in CycleFillModel:
            for scenario in CycleScenario:
                kernel = kernels[(model, scenario)]
                admission = kernel.admit(version, scenario=scenario, source_books=books)
                row = {
                    "attempt_index": attempt_index,
                    "fill_model": model.value,
                    "scenario": scenario.value,
                    "accepted": admission.accepted,
                    "quote_version_id": admission.quote_version_id,
                    "decision_monotonic_ns": admission.decision_monotonic_ns,
                    "reason": admission.reason,
                }
                admissions.append(row)
                attempt["admissions"].append(row)
        decision_attempts.append(attempt)

    with path.open("rb") as handle:
        for raw_line in handle:
            if not raw_line.strip():
                continue
            byte_count += len(raw_line)
            record_count += 1
            raw_hash.update(raw_line)
            try:
                record = json.loads(raw_line.decode("utf-8"))
            except Exception as exc:
                decode_error_count += 1
                record_error(decode_error_samples, exc)
                continue
            if not isinstance(record, dict):
                decode_error_count += 1
                record_error(decode_error_samples, "D1 JSONL record must be an object")
                continue
            kind = record.get("kind")
            if kind == "RUN_METADATA" and isinstance(record.get("metadata"), dict):
                if not metadata:
                    metadata = dict(record["metadata"])
            elif kind == "CYCLE_DECISION":
                historical_decision_count += 1
            elif kind == "CYCLE_ADMISSION":
                historical_admission_count += 1
            elif kind == "CYCLE_FINAL_RESULT":
                historical_final_result_count += 1
            elif kind == "CYCLE_STREAM_INPUT":
                stream_input_count += 1
                try:
                    item = _input_from_dict(record.get("input"), context="S1b D1 stream input")
                except Exception as exc:
                    decode_error_count += 1
                    record_error(decode_error_samples, exc)
                    continue
                if isinstance(item, CycleClock):
                    clock_input_count += 1
                    if last_clock_ns is not None and item.at_monotonic_ns < last_clock_ns:
                        replay_error_count += 1
                        record_error(replay_error_samples, "CYCLE_STREAM_INPUT clock moved backwards")
                    last_clock_ns = max(last_clock_ns or item.at_monotonic_ns, item.at_monotonic_ns)
                    deliver(item)
                    record_decision_attempt(item.at_monotonic_ns)
                else:
                    event_input_count += 1
                    if item.kind is CausalEventKind.BOOK and item.book is not None:
                        book = item.book
                        ready = _processing_ready_ns(item)
                        key = (
                            book.received_monotonic_ns,
                            ready if ready is not None else -1,
                            book.book_revision,
                            record.get("input_index") if isinstance(record.get("input_index"), int) else event_input_count,
                        )
                        previous = latest_book_keys.get(book.venue)
                        if previous is None or key >= previous:
                            latest_book_keys[book.venue] = key
                            latest_books[book.venue] = (item, book)
                    deliver(item)
            elif kind == "CYCLE_STREAM_END":
                value = record.get("end_monotonic_ns")
                if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                    stream_end = value
                else:
                    decode_error_count += 1
                    record_error(decode_error_samples, "CYCLE_STREAM_END has invalid end_monotonic_ns")
            elif kind == "RUN_FAILED":
                run_failed = True
                raw_failure = record.get("fatal_reason")
                failure_reason = raw_failure if isinstance(raw_failure, str) else str(raw_failure)

    if stream_end is not None:
        for (model, scenario), kernel in kernels.items():
            last = kernel.last_result(scenario)
            if kernel.state(scenario) is CycleKernelState.PENDING or (
                isinstance(last, S1bCycleResult) and last.policy_blocked
            ):
                try:
                    kernel.finish(scenario=scenario, end_monotonic_ns=stream_end)
                except RuntimeError as exc:
                    replay_error_count += 1
                    record_error(replay_error_samples, f"{model.value}/{scenario.value}: {exc}")
    for key, kernel in kernels.items():
        per_key_results[key].extend(kernel.retained_results(key[1], include_active=True))

    actual_hash = raw_hash.hexdigest()
    identity_match = (
        (expected_records is None or expected_records == record_count)
        and (expected_sha256 is None or expected_sha256 == actual_hash)
    )
    identity = {
        "path": str(path),
        "bytes": byte_count,
        "records": record_count,
        "sha256": actual_hash,
        "expected_records": expected_records,
        "expected_sha256": expected_sha256,
        "identity_match": identity_match,
    }
    insufficiency_reasons: list[str] = []
    if policy is None:
        insufficiency_reasons.append(policy_failure or "MISSING_IMMUTABLE_POLICY")
    if not stream_input_count:
        insufficiency_reasons.append("MISSING_CYCLE_STREAM_INPUT")
    if stream_end is None:
        insufficiency_reasons.append("MISSING_CYCLE_STREAM_END")
    if run_failed:
        insufficiency_reasons.append("RUN_FAILED")
    if decode_error_count:
        insufficiency_reasons.append("DECODE_ERRORS")
    if replay_error_count:
        insufficiency_reasons.append("REPLAY_ERRORS")
    if not identity_match:
        insufficiency_reasons.append("INPUT_IDENTITY_MISMATCH")
    alternatives = []
    for model in CycleFillModel:
        for scenario in CycleScenario:
            rows = tuple(per_key_results[(model, scenario)])
            alternatives.append({
                "fill_model": model.value,
                "scenario": scenario.value,
                "summary": _d1_summary(rows),
                "episodes": [_d1_result_row(result) for result in rows],
            })
    market_metadata: dict[str, Any] = {}
    if policy is not None and policy.risex_market is not None and policy.lighter_market is not None:
        market_metadata = {
            "risex": _d1_market_payload(policy.risex_market),
            "lighter": _d1_market_payload(policy.lighter_market),
        }
    return {
        "contract_version": SCV1_S1B_CONTRACT_VERSION,
        "report_kind": "SCV1_S1B_D1_DEVELOPMENT",
        "source": {
            "accepted_release": accepted_release,
            "historical_source_accepted_release": metadata.get("accepted_release"),
            "policy_version": SCV1_S1B_CONTRACT_VERSION,
            "metadata": metadata,
            "policy_provenance": policy_provenance,
            "market_metadata": market_metadata,
            "historical_decisions_reused_as_inputs": False,
            "historical_decision_fields_ignored": [
                "quote_version_id",
                "quote_created_monotonic_ns",
                "decision_ready_monotonic_ns",
                "maker_price",
                "canonical_quantity",
                "outcome",
                "source_books",
            ],
            "replay_input": "CYCLE_STREAM_INPUT BOOK/CLOCK records",
            "decision_contract": {
                "clock": "CYCLE_STREAM_INPUT.input.CLOCK.at_monotonic_ns",
                "books": "CYCLE_STREAM_INPUT.input.EVENT.BOOK payloads",
                "cadence_ns": _D1_DECISION_INTERVAL_NS,
                "freshness_max_age_ns": frozen.input_freshness_max_age_ns,
                "receipt_skew_max_ns": frozen.input_receipt_skew_max_ns,
                "quote_recomputed_at_clock": True,
            },
        },
        "input_identity": identity,
        "run": {
            "stream_end_monotonic_ns": stream_end,
            "run_failed": run_failed,
            "failure_reason": failure_reason,
            "funding": "UNKNOWN",
            "data_sufficiency": (
                "SUFFICIENT_FOR_OBSERVED_REPLAY"
                if not insufficiency_reasons
                else "DATA_INSUFFICIENT"
            ),
            "data_sufficiency_reasons": list(dict.fromkeys(insufficiency_reasons)),
            "stream_input_count": stream_input_count,
            "event_input_count": event_input_count,
            "clock_input_count": clock_input_count,
            "decode_error_count": decode_error_count,
            "decode_error_samples": decode_error_samples,
            "replay_error_count": replay_error_count,
            "replay_error_samples": replay_error_samples,
            "recomputed_decision_attempt_count": len(decision_attempts),
            "recomputed_active_quote_count": sum(
                attempt.get("recomputed_quote", {}).get("outcome") == "QUOTE_ACTIVE"
                for attempt in decision_attempts
                if isinstance(attempt.get("recomputed_quote"), dict)
            ),
            "admitted_alternative_count": sum(row["accepted"] for row in admissions),
            "historical_decision_count": historical_decision_count,
            "historical_admission_count": historical_admission_count,
            "historical_final_result_count": historical_final_result_count,
        },
        "decision_attempts": decision_attempts,
        "admissions": admissions,
        "alternatives": alternatives,
        "limitations": [
            "D1 is historical observational evidence replayed from persisted BOOK/CLOCK inputs under the explicit S1b policy clock; historical decisions, admissions, and final results are not instructions.",
            "Historical CYCLE_DECISION policy data is metadata-only: old quote IDs, timestamps, prices, quantities, outcomes, and source books are ignored.",
            "A failed or incomplete source run remains DATA_INSUFFICIENT; missing/corrupt timing is not a clean no-fill or policy block.",
            "Funding is UNKNOWN and open marks are conditional public valuation evidence, not all-in profitability.",
            "This report does not prove live execution, storage capacity, or release readiness.",
        ],
    }


def render_scv1_s1b_d1_report(report: Mapping[str, Any]) -> str:
    """Render the compact first-readable four-alternative D1 table."""

    lines = [
        "SCV1-S1b D1 development replay",
        f"contract={report.get('contract_version')} source={report.get('input_identity', {}).get('sha256')}",
        "model                  scenario  episodes fills closed blocked net_usd mark_pnl_usd active_ns blocked_ns statuses/reasons",
    ]
    for alternative in report.get("alternatives", ()):
        summary = alternative.get("summary", {})
        statuses = ",".join(f"{key}:{value}" for key, value in sorted(summary.get("status_counts", {}).items())) or "-"
        reasons = ",".join(f"{key}:{value}" for key, value in sorted(summary.get("reason_counts", {}).items())[:4]) or "-"
        lines.append(
            f"{str(alternative.get('fill_model', '')):22} "
            f"{str(alternative.get('scenario', '')):8} "
            f"{summary.get('episode_count', 0):8} "
            f"{summary.get('fills', 0):5} "
            f"{summary.get('closed_count', 0):6} "
            f"{summary.get('blocked_count', 0):7} "
            f"{str(summary.get('net_cashflow_usd', '-')):>8} "
            f"{str(summary.get('marked_execution_only_pnl_usd', '-')):>11} "
            f"{summary.get('simulation_active_duration_ns', 0):>9} "
            f"{summary.get('blocked_duration_ns', 0):>10} "
            f"{statuses}; {reasons}"
        )
    run = report.get("run", {})
    lines.append(
        f"run_failed={run.get('run_failed')} "
        f"failure_reason={run.get('failure_reason') or '-'} "
        f"data_sufficiency={run.get('data_sufficiency', 'UNKNOWN')} "
        f"inputs={run.get('stream_input_count', 0)} "
        f"recomputed_decisions={run.get('recomputed_decision_attempt_count', 0)} "
        f"historical_decisions_ignored={run.get('historical_decision_count', 0)} "
        f"decode_errors={run.get('decode_error_count', 0)} funding=UNKNOWN"
    )
    lines.append("limitations=" + " | ".join(str(value) for value in report.get("limitations", ())))
    return "\n".join(lines)


__all__ = [
    "SCV1_S1B_CONTRACT_VERSION",
    "S1bCycleKernel",
    "S1bCycleResult",
    "Scv1S1bKernel",
    "build_scv1_s1b_d1_report",
    "render_scv1_s1b_d1_report",
    "run_scv1_s1b",
    "run_scv1_s1b_alternatives",
    "scv1_s1b_policy",
]
