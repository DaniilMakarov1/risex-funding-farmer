"""Sequential, offline-only one-policy complete-cycle kernel.

The cycle lane is deliberately small and explicit.  It consumes one causal
event or one observed clock boundary at a time; fills, scheduled actions,
positions, and the signed cash-flow ledger are updated at that boundary.  No
network, credential, signing, dispatch, persistence, or funding path belongs
to this module.

The primary and stress scenarios are separate lanes.  They share the input
fixture, but never share mutable positions, actions, or economics.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_FLOOR
from enum import StrEnum
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
    measure_causal_quote,
)
from .models import (
    BookEvidence,
    DataGapEvidence,
    EntryViabilityOutcome,
    QuoteVersion,
    Side,
    SpreadDirection,
    TradeEvidence,
    Venue,
)


_ZERO = Decimal("0")
_ONE = Decimal("1")
_BPS = Decimal("10000")
_NS_PER_MS = 1_000_000


def _decimal(value: Decimal, name: str) -> Decimal:
    if not isinstance(value, Decimal):
        raise TypeError(f"{name} must be Decimal")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    return value


def _non_negative_int(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")


def _optional_non_negative_int(value: int | None, name: str) -> None:
    if value is not None:
        _non_negative_int(value, name)


def _text(value: str, name: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")


def _session(value: str | int, name: str = "stream_session_id") -> None:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise TypeError(f"{name} must be str or int")
    if isinstance(value, str) and not value:
        raise ValueError(f"{name} must be non-empty")


class CycleScenario(StrEnum):
    """Alternative delay/economic lanes required by S2."""

    PRIMARY = "PRIMARY"
    STRESS = "STRESS"


class CycleTerminalState(StrEnum):
    """Terminal, or current snapshot, classification of one cycle."""

    PENDING = "PENDING"
    NORMAL = "NORMAL"
    FORCED = "FORCED"
    ABORTED = "ABORTED"
    UNRESOLVED = "UNRESOLVED"


class CycleKernelState(StrEnum):
    """Admission state of one scenario lane."""

    FLAT = "FLAT"
    PENDING = "PENDING"
    UNRESOLVED_HALTED = "UNRESOLVED_HALTED"


class CycleActionKind(StrEnum):
    """Explicit cycle actions retained in the result ledger."""

    ENTRY_MAKER = "ENTRY_MAKER"
    ENTRY_CANCEL = "ENTRY_CANCEL"
    ENTRY_HEDGE = "ENTRY_HEDGE"
    UNMATCHED_RISEX_UNWIND = "UNMATCHED_RISEX_UNWIND"
    EXIT_MAKER = "EXIT_MAKER"
    EXIT_CANCEL = "EXIT_CANCEL"
    EXIT_HEDGE_CLOSE = "EXIT_HEDGE_CLOSE"
    FORCED_RISEX_UNWIND = "FORCED_RISEX_UNWIND"
    FORCED_LIGHTER_UNWIND = "FORCED_LIGHTER_UNWIND"


class CycleActionStatus(StrEnum):
    """Status of one requested action."""

    COMPLETED = "COMPLETED"
    NOT_REQUIRED = "NOT_REQUIRED"
    PENDING = "PENDING"
    UNRESOLVED = "UNRESOLVED"


class CycleReason(StrEnum):
    """Bounded reasons visible in cycle evidence."""

    NO_ENTRY = "NO_ENTRY"
    INVALID_ENTRY_QUOTE = "INVALID_ENTRY_QUOTE"
    MISSING_ENTRY_TIMING = "MISSING_ENTRY_TIMING"
    MISSING_TERMINAL_BOUNDARY = "MISSING_TERMINAL_BOUNDARY"
    ENTRY_CAUSAL_UNCERTAINTY = "ENTRY_CAUSAL_UNCERTAINTY"
    ENTRY_INPUT_STALE = "ENTRY_INPUT_STALE"
    ENTRY_INPUT_GAP = "ENTRY_INPUT_GAP"
    ENTRY_INPUT_AMBIGUOUS = "ENTRY_INPUT_AMBIGUOUS"
    REQUIRED_ACTION_DATA_MISSING = "REQUIRED_ACTION_DATA_MISSING"
    REQUIRED_ACTION_DATA_GAP = "REQUIRED_ACTION_DATA_GAP"
    REQUIRED_ACTION_DATA_STALE = "REQUIRED_ACTION_DATA_STALE"
    REQUIRED_ACTION_SESSION_DISPLACED = "REQUIRED_ACTION_SESSION_DISPLACED"
    REQUIRED_ACTION_UNHEALTHY = "REQUIRED_ACTION_UNHEALTHY"
    REQUIRED_ACTION_TIMING_MISSING = "REQUIRED_ACTION_TIMING_MISSING"
    REQUIRED_ACTION_RECEIPT_SKEW = "REQUIRED_ACTION_RECEIPT_SKEW"
    REQUIRED_ACTION_AMBIGUOUS = "REQUIRED_ACTION_AMBIGUOUS"
    FUTURE_BOOK_REJECTED = "FUTURE_BOOK_REJECTED"
    INSUFFICIENT_DEPTH = "INSUFFICIENT_DEPTH"
    GRID_RESIDUE = "GRID_RESIDUE"
    MINIMUM_RESIDUE = "MINIMUM_RESIDUE"
    HEDGE_PARTIAL = "HEDGE_PARTIAL"
    EXIT_QUOTE_INVALID = "EXIT_QUOTE_INVALID"
    EXIT_CAUSAL_UNCERTAINTY = "EXIT_CAUSAL_UNCERTAINTY"
    MAX_HOLD = "MAX_HOLD"
    FORCED_UNWIND = "FORCED_UNWIND"
    TERMINAL_PENDING_ACTION = "TERMINAL_PENDING_ACTION"
    TERMINAL_NON_FLAT = "TERMINAL_NON_FLAT"
    OVER_CLOSE_BLOCKED = "OVER_CLOSE_BLOCKED"
    NEGATIVE_ORIENTATION = "NEGATIVE_ORIENTATION"
    DUPLICATE_CONFLICT = "DUPLICATE_CONFLICT"
    LATE_OLDER_EVENT = "LATE_OLDER_EVENT"
    EVENT_NOT_READY = "EVENT_NOT_READY"
    DECISION_RATE_LIMIT = "DECISION_RATE_LIMIT"
    DECISION_WITHIN_PREVIOUS_CYCLE = "DECISION_WITHIN_PREVIOUS_CYCLE"
    ACTIVE_CYCLE = "ACTIVE_CYCLE"
    UNRESOLVED_HALTED = "UNRESOLVED_HALTED"
    TERMINAL_RETENTION_EXHAUSTED = "TERMINAL_RETENTION_EXHAUSTED"


@dataclass(frozen=True, slots=True)
class CycleDelays:
    """Scenario-specific hypothetical delays."""

    activation_delay_ns: int
    cancel_delay_ns: int
    taker_delay_ns: int
    risex_fill_cost_rate: Decimal

    def __post_init__(self) -> None:
        for value, name in (
            (self.activation_delay_ns, "activation_delay_ns"),
            (self.cancel_delay_ns, "cancel_delay_ns"),
            (self.taker_delay_ns, "taker_delay_ns"),
        ):
            _non_negative_int(value, name)
        rate = _decimal(self.risex_fill_cost_rate, "risex_fill_cost_rate")
        if rate < 0:
            raise ValueError("risex_fill_cost_rate must be non-negative")


@dataclass(frozen=True, slots=True)
class CyclePolicy:
    """The frozen BTC/$100 RISEx SELL/Lighter BUY S2 policy."""

    canonical_market: str = "BTC"
    direction: SpreadDirection = SpreadDirection.RISEX_SELL_LIGHTER_BUY
    target_notional_usd: Decimal = Decimal("100")
    target_margin_bps: Decimal = Decimal("1")
    risex_maker_fee_rate: Decimal = Decimal("0.0001")
    risex_taker_fee_rate: Decimal = Decimal("0.0003")
    lighter_taker_fee_rate: Decimal = Decimal("0")
    risex_fee_source: str = "SS-001Q"
    lighter_fee_source: str = "OFFICIAL_LIGHTER_STANDARD_RESEARCH_INPUT"
    input_freshness_max_age_ns: int = 500_000_000
    input_receipt_skew_max_ns: int = 500_000_000
    entry_cancel_after_activation_ns: int = 5_000_000_000
    max_hold_ns: int = 120_000_000_000
    primary_activation_delay_ns: int = 500_000_000
    primary_cancel_delay_ns: int = 500_000_000
    primary_taker_delay_ns: int = 500_000_000
    stress_activation_delay_ns: int = 1_000_000_000
    stress_cancel_delay_ns: int = 1_000_000_000
    stress_taker_delay_ns: int = 1_000_000_000
    stress_risex_fill_cost_rate: Decimal = Decimal("0.0001")

    def __post_init__(self) -> None:
        _text(self.canonical_market, "canonical_market")
        direction = self.direction
        if not isinstance(direction, SpreadDirection):
            direction = SpreadDirection(direction)
            object.__setattr__(self, "direction", direction)
        if self.canonical_market != "BTC":
            raise ValueError("S2 cycle market is fixed at BTC")
        if direction is not SpreadDirection.RISEX_SELL_LIGHTER_BUY:
            raise ValueError("S2 cycle direction is fixed at RISEx SELL/Lighter BUY")
        for value, name, expected in (
            (self.target_notional_usd, "target_notional_usd", Decimal("100")),
            (self.target_margin_bps, "target_margin_bps", Decimal("1")),
            (self.risex_maker_fee_rate, "risex_maker_fee_rate", Decimal("0.0001")),
            (self.risex_taker_fee_rate, "risex_taker_fee_rate", Decimal("0.0003")),
            (self.lighter_taker_fee_rate, "lighter_taker_fee_rate", Decimal("0")),
        ):
            if _decimal(value, name) != expected:
                raise ValueError(f"S2 {name} is frozen at {expected}")
        _text(self.risex_fee_source, "risex_fee_source")
        _text(self.lighter_fee_source, "lighter_fee_source")
        for value, name in (
            (self.input_freshness_max_age_ns, "input_freshness_max_age_ns"),
            (self.input_receipt_skew_max_ns, "input_receipt_skew_max_ns"),
            (self.entry_cancel_after_activation_ns, "entry_cancel_after_activation_ns"),
            (self.max_hold_ns, "max_hold_ns"),
            (self.primary_activation_delay_ns, "primary_activation_delay_ns"),
            (self.primary_cancel_delay_ns, "primary_cancel_delay_ns"),
            (self.primary_taker_delay_ns, "primary_taker_delay_ns"),
            (self.stress_activation_delay_ns, "stress_activation_delay_ns"),
            (self.stress_cancel_delay_ns, "stress_cancel_delay_ns"),
            (self.stress_taker_delay_ns, "stress_taker_delay_ns"),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if _decimal(self.stress_risex_fill_cost_rate, "stress_risex_fill_cost_rate") != Decimal("0.0001"):
            raise ValueError("S2 stress RISEx fill cost is frozen at 1 bp")

    def delays(self, scenario: CycleScenario) -> CycleDelays:
        scenario = scenario if isinstance(scenario, CycleScenario) else CycleScenario(scenario)
        if scenario is CycleScenario.PRIMARY:
            return CycleDelays(
                self.primary_activation_delay_ns,
                self.primary_cancel_delay_ns,
                self.primary_taker_delay_ns,
                _ZERO,
            )
        return CycleDelays(
            self.stress_activation_delay_ns,
            self.stress_cancel_delay_ns,
            self.stress_taker_delay_ns,
            self.stress_risex_fill_cost_rate,
        )


def s2_cycle_policy() -> CyclePolicy:
    """Return the sole admitted S2 policy."""

    return CyclePolicy()


@dataclass(frozen=True, slots=True)
class CycleClock:
    """An explicit observed clock boundary for sequential advancement."""

    at_monotonic_ns: int

    def __post_init__(self) -> None:
        _non_negative_int(self.at_monotonic_ns, "at_monotonic_ns")


@dataclass(frozen=True, slots=True)
class CyclePositions:
    """Signed venue positions and pairing/residue witnesses."""

    risex_signed_quantity: Decimal
    lighter_signed_quantity: Decimal
    paired_risex_quantity: Decimal
    paired_lighter_quantity: Decimal
    unmatched_risex_quantity: Decimal
    authoritative: bool = True

    def __post_init__(self) -> None:
        for value, name in (
            (self.risex_signed_quantity, "risex_signed_quantity"),
            (self.lighter_signed_quantity, "lighter_signed_quantity"),
            (self.paired_risex_quantity, "paired_risex_quantity"),
            (self.paired_lighter_quantity, "paired_lighter_quantity"),
            (self.unmatched_risex_quantity, "unmatched_risex_quantity"),
        ):
            value = _decimal(value, name)
            if name not in {"risex_signed_quantity", "lighter_signed_quantity"} and value < 0:
                raise ValueError(f"{name} must be non-negative")
        if not isinstance(self.authoritative, bool):
            raise TypeError("authoritative must be bool")

    @property
    def risex_quantity(self) -> Decimal:
        return self.risex_signed_quantity

    @property
    def lighter_quantity(self) -> Decimal:
        return self.lighter_signed_quantity

    @property
    def is_zero(self) -> bool:
        return (
            self.risex_signed_quantity == _ZERO
            and self.lighter_signed_quantity == _ZERO
            and self.paired_risex_quantity == _ZERO
            and self.paired_lighter_quantity == _ZERO
            and self.unmatched_risex_quantity == _ZERO
        )


@dataclass(frozen=True, slots=True)
class CycleFee:
    """One exact fee component attached to one fill."""

    fee_id: str
    fill_id: str
    venue: Venue
    liquidity_role: LiquidityRole
    notional_usd: Decimal
    rate: Decimal
    amount_usd: Decimal
    source: str

    def __post_init__(self) -> None:
        venue = self.venue if isinstance(self.venue, Venue) else Venue(self.venue)
        role = self.liquidity_role if isinstance(self.liquidity_role, LiquidityRole) else LiquidityRole(self.liquidity_role)
        object.__setattr__(self, "venue", venue)
        object.__setattr__(self, "liquidity_role", role)
        for value, name in (
            (self.notional_usd, "notional_usd"),
            (self.rate, "rate"),
            (self.amount_usd, "amount_usd"),
        ):
            _decimal(value, name)
        if self.notional_usd < 0 or self.rate < 0 or self.amount_usd < 0:
            raise ValueError("cycle fee values must be non-negative")
        if self.amount_usd != self.notional_usd * self.rate:
            raise ValueError("cycle fee amount does not match exact notional and rate")
        _text(self.fee_id, "fee_id")
        _text(self.fill_id, "fill_id")
        _text(self.source, "source")


@dataclass(frozen=True, slots=True)
class CycleCashFlow:
    """Signed cash movement and modeled scenario deduction for one fill."""

    flow_id: str
    fill_id: str
    gross_cashflow_usd: Decimal
    fee_usd: Decimal
    scenario_cost_usd: Decimal
    net_cashflow_usd: Decimal

    def __post_init__(self) -> None:
        for value, name in (
            (self.gross_cashflow_usd, "gross_cashflow_usd"),
            (self.fee_usd, "fee_usd"),
            (self.scenario_cost_usd, "scenario_cost_usd"),
            (self.net_cashflow_usd, "net_cashflow_usd"),
        ):
            _decimal(value, name)
        if self.fee_usd < 0 or self.scenario_cost_usd < 0:
            raise ValueError("cashflow deductions must be non-negative")
        if self.net_cashflow_usd != self.gross_cashflow_usd - self.fee_usd - self.scenario_cost_usd:
            raise ValueError("net cashflow does not match exact deductions")
        _text(self.flow_id, "flow_id")
        _text(self.fill_id, "fill_id")


@dataclass(frozen=True, slots=True)
class CycleFill:
    """One exact hypothetical fill with identity, fee, and positions."""

    fill_id: str
    action_id: str
    venue: Venue
    side: Side
    liquidity_role: LiquidityRole
    quantity: Decimal
    price: Decimal
    notional_usd: Decimal
    fee_rate: Decimal
    fee_usd: Decimal
    gross_cashflow_usd: Decimal
    scenario_cost_usd: Decimal
    net_cashflow_usd: Decimal
    reason: str
    observed_monotonic_ns: int
    processing_ready_monotonic_ns: int | None
    evidence_id: str
    source_identity: CausalSourceIdentity | None
    stream_session_id: str | int
    recovery_generation: int
    book_revision_id: str | None
    risex_position_after: Decimal
    lighter_position_after: Decimal

    def __post_init__(self) -> None:
        venue = self.venue if isinstance(self.venue, Venue) else Venue(self.venue)
        side = self.side if isinstance(self.side, Side) else Side(self.side)
        role = self.liquidity_role if isinstance(self.liquidity_role, LiquidityRole) else LiquidityRole(self.liquidity_role)
        object.__setattr__(self, "venue", venue)
        object.__setattr__(self, "side", side)
        object.__setattr__(self, "liquidity_role", role)
        for value, name in (
            (self.quantity, "quantity"),
            (self.price, "price"),
            (self.notional_usd, "notional_usd"),
            (self.fee_rate, "fee_rate"),
            (self.fee_usd, "fee_usd"),
            (self.gross_cashflow_usd, "gross_cashflow_usd"),
            (self.scenario_cost_usd, "scenario_cost_usd"),
            (self.net_cashflow_usd, "net_cashflow_usd"),
            (self.risex_position_after, "risex_position_after"),
            (self.lighter_position_after, "lighter_position_after"),
        ):
            _decimal(value, name)
        if self.quantity <= 0 or self.price <= 0:
            raise ValueError("cycle fill quantity and price must be positive")
        if self.notional_usd != self.quantity * self.price:
            raise ValueError("cycle fill notional does not match exact quantity and price")
        if self.fee_rate < 0 or self.fee_usd != self.notional_usd * self.fee_rate:
            raise ValueError("cycle fill fee does not match exact notional and rate")
        expected_gross = self.notional_usd if self.side is Side.SELL else -self.notional_usd
        if self.gross_cashflow_usd != expected_gross:
            raise ValueError("cycle fill gross cashflow has the wrong side sign")
        if self.net_cashflow_usd != self.gross_cashflow_usd - self.fee_usd - self.scenario_cost_usd:
            raise ValueError("cycle fill net cashflow does not match deductions")
        _non_negative_int(self.observed_monotonic_ns, "observed_monotonic_ns")
        _optional_non_negative_int(self.processing_ready_monotonic_ns, "processing_ready_monotonic_ns")
        if self.processing_ready_monotonic_ns is not None and self.processing_ready_monotonic_ns < self.observed_monotonic_ns:
            raise ValueError("processing readiness must not precede observed fill time")
        _text(self.fill_id, "fill_id")
        _text(self.action_id, "action_id")
        _text(self.reason, "reason")
        _text(self.evidence_id, "evidence_id")
        _session(self.stream_session_id)
        _non_negative_int(self.recovery_generation, "recovery_generation")


@dataclass(frozen=True, slots=True)
class CycleLedger:
    """Immutable view of exact fills, fees, and signed cash flows."""

    fills: tuple[CycleFill, ...] = ()
    fees: tuple[CycleFee, ...] = ()
    cashflows: tuple[CycleCashFlow, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.fills, tuple) or not isinstance(self.fees, tuple) or not isinstance(self.cashflows, tuple):
            raise TypeError("cycle ledger collections must be tuples")

    @property
    def signed_cashflow_usd(self) -> Decimal:
        return sum((flow.gross_cashflow_usd for flow in self.cashflows), _ZERO)

    @property
    def total_fees_usd(self) -> Decimal:
        return sum((fee.amount_usd for fee in self.fees), _ZERO)

    @property
    def scenario_cost_usd(self) -> Decimal:
        return sum((flow.scenario_cost_usd for flow in self.cashflows), _ZERO)

    @property
    def net_cashflow_usd(self) -> Decimal:
        return sum((flow.net_cashflow_usd for flow in self.cashflows), _ZERO)

    @property
    def turnover_usd(self) -> Decimal:
        return sum((fill.notional_usd for fill in self.fills), _ZERO)

    @property
    def fee_count(self) -> int:
        return len(self.fees)

    @property
    def cashflow_complete(self) -> bool:
        return len(self.fills) == len(self.fees) == len(self.cashflows)


@dataclass(frozen=True, slots=True)
class CycleAction:
    """One requested, delayed, completed, or unresolved action."""

    action_id: str
    kind: CycleActionKind
    status: CycleActionStatus
    requested_monotonic_ns: int
    effective_monotonic_ns: int | None
    due_monotonic_ns: int | None
    requested_quantity: Decimal
    executed_quantity: Decimal
    remaining_quantity: Decimal
    reason: str
    evidence_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        kind = self.kind if isinstance(self.kind, CycleActionKind) else CycleActionKind(self.kind)
        status = self.status if isinstance(self.status, CycleActionStatus) else CycleActionStatus(self.status)
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "status", status)
        for value, name in (
            (self.requested_quantity, "requested_quantity"),
            (self.executed_quantity, "executed_quantity"),
            (self.remaining_quantity, "remaining_quantity"),
        ):
            value = _decimal(value, name)
            if value < 0:
                raise ValueError(f"{name} must be non-negative")
        if self.executed_quantity > self.requested_quantity or self.remaining_quantity != self.requested_quantity - self.executed_quantity:
            raise ValueError("cycle action quantities are inconsistent")
        _non_negative_int(self.requested_monotonic_ns, "requested_monotonic_ns")
        _optional_non_negative_int(self.effective_monotonic_ns, "effective_monotonic_ns")
        _optional_non_negative_int(self.due_monotonic_ns, "due_monotonic_ns")
        if self.effective_monotonic_ns is not None and self.effective_monotonic_ns < self.requested_monotonic_ns:
            raise ValueError("action effective time must not precede request")
        if self.due_monotonic_ns is not None and self.due_monotonic_ns < self.requested_monotonic_ns:
            raise ValueError("action due time must not precede request")
        _text(self.action_id, "action_id")
        _text(self.reason, "reason")
        if not isinstance(self.evidence_ids, tuple):
            raise TypeError("evidence_ids must be a tuple")


@dataclass(frozen=True, slots=True)
class CycleResult:
    """Current or terminal evidence for one alternative scenario."""

    scenario: CycleScenario
    quote_version_id: str
    canonical_market: str
    status: CycleTerminalState
    reason_codes: tuple[str, ...]
    entry_measurement: CausalQuoteMeasurement | None
    exit_measurement: CausalQuoteMeasurement | None
    entry_edge_usd: Decimal | None
    entry_quantity: Decimal
    hedged_quantity: Decimal
    unmatched_entry_quantity: Decimal
    exit_price: Decimal | None
    first_maker_fill_monotonic_ns: int | None
    max_hold_deadline_monotonic_ns: int | None
    terminal_monotonic_ns: int | None
    positions: CyclePositions
    ledger: CycleLedger
    actions: tuple[CycleAction, ...]
    cashflow_complete: bool
    complete_execution_pnl_usd: Decimal | None
    holding_duration_ns: int | None
    unmatched_exposure_duration_ns: int | None
    funding_status: str = "UNKNOWN"

    def __post_init__(self) -> None:
        scenario = self.scenario if isinstance(self.scenario, CycleScenario) else CycleScenario(self.scenario)
        status = self.status if isinstance(self.status, CycleTerminalState) else CycleTerminalState(self.status)
        object.__setattr__(self, "scenario", scenario)
        object.__setattr__(self, "status", status)
        _text(self.quote_version_id, "quote_version_id")
        if self.canonical_market != "BTC":
            raise ValueError("cycle result market is fixed at BTC")
        if not isinstance(self.reason_codes, tuple) or not isinstance(self.actions, tuple):
            raise TypeError("cycle result reasons and actions must be tuples")
        for value, name in (
            (self.entry_quantity, "entry_quantity"),
            (self.hedged_quantity, "hedged_quantity"),
            (self.unmatched_entry_quantity, "unmatched_entry_quantity"),
        ):
            if _decimal(value, name) < 0:
                raise ValueError(f"{name} must be non-negative")
        if self.entry_edge_usd is not None:
            _decimal(self.entry_edge_usd, "entry_edge_usd")
        if self.exit_price is not None and _decimal(self.exit_price, "exit_price") <= 0:
            raise ValueError("exit_price must be positive when present")
        if self.complete_execution_pnl_usd is not None:
            _decimal(self.complete_execution_pnl_usd, "complete_execution_pnl_usd")
        if self.funding_status != "UNKNOWN":
            raise ValueError("S2 funding is always UNKNOWN")

    @property
    def fills(self) -> tuple[CycleFill, ...]:
        return self.ledger.fills

    @property
    def fees(self) -> tuple[CycleFee, ...]:
        return self.ledger.fees

    @property
    def cashflows(self) -> tuple[CycleCashFlow, ...]:
        return self.ledger.cashflows

    @property
    def pending_actions(self) -> tuple[CycleAction, ...]:
        return tuple(
            action
            for action in self.actions
            if action.status in {CycleActionStatus.PENDING, CycleActionStatus.UNRESOLVED}
        )

    @property
    def is_flat(self) -> bool:
        return (
            self.status in {
                CycleTerminalState.NORMAL,
                CycleTerminalState.FORCED,
                CycleTerminalState.ABORTED,
            }
            and self.positions.authoritative
            and self.positions.is_zero
            and not self.pending_actions
        )

    @property
    def is_unresolved(self) -> bool:
        return self.status is CycleTerminalState.UNRESOLVED

    @property
    def is_aborted(self) -> bool:
        return self.status is CycleTerminalState.ABORTED

    @property
    def pnl_usd(self) -> Decimal | None:
        return self.complete_execution_pnl_usd

    @property
    def turnover_usd(self) -> Decimal:
        return self.ledger.turnover_usd

    @property
    def normal(self) -> bool:
        return self.status is CycleTerminalState.NORMAL

    @property
    def forced(self) -> bool:
        return self.status is CycleTerminalState.FORCED


@dataclass(frozen=True, slots=True)
class CycleAlternatives:
    """Primary and stress results retained as alternatives, never summed."""

    primary: CycleResult
    stress: CycleResult

    def __post_init__(self) -> None:
        if self.primary.scenario is not CycleScenario.PRIMARY or self.stress.scenario is not CycleScenario.STRESS:
            raise ValueError("cycle alternatives have incorrect scenarios")

    @property
    def by_scenario(self) -> tuple[CycleResult, CycleResult]:
        return self.primary, self.stress


@dataclass(frozen=True, slots=True)
class CycleAdmission:
    """Auditable result of attempting one new decision."""

    accepted: bool
    scenario: CycleScenario
    quote_version_id: str
    decision_monotonic_ns: int | None
    reason: str

    def __post_init__(self) -> None:
        scenario = self.scenario if isinstance(self.scenario, CycleScenario) else CycleScenario(self.scenario)
        object.__setattr__(self, "scenario", scenario)
        _text(self.quote_version_id, "quote_version_id")
        _text(self.reason, "reason")
        _optional_non_negative_int(self.decision_monotonic_ns, "decision_monotonic_ns")


@dataclass(frozen=True, slots=True)
class CycleAttempt:
    """One decision plus its ordered offline evidence."""

    quote_version: QuoteVersion
    events: tuple[CausalEvent | TradeEvidence | BookEvidence | DataGapEvidence, ...]
    scenario: CycleScenario = CycleScenario.PRIMARY
    source_books: tuple[BookEvidence, ...] = ()
    end_monotonic_ns: int | None = None

    def __post_init__(self) -> None:
        scenario = self.scenario if isinstance(self.scenario, CycleScenario) else CycleScenario(self.scenario)
        object.__setattr__(self, "scenario", scenario)
        if not isinstance(self.quote_version, QuoteVersion):
            raise TypeError("cycle attempt quote_version must be QuoteVersion")
        if not isinstance(self.events, tuple) or not isinstance(self.source_books, tuple):
            raise TypeError("cycle attempt events and source_books must be tuples")
        _optional_non_negative_int(self.end_monotonic_ns, "end_monotonic_ns")


class CycleAdmissionError(RuntimeError):
    """Raised by convenience runners when a decision is rejected."""

    def __init__(self, admission: CycleAdmission) -> None:
        self.admission = admission
        super().__init__(admission.reason)


@dataclass(frozen=True, slots=True)
class CycleProgress:
    """Small witness returned after each sequential input."""

    scenario: CycleScenario
    quote_version_id: str
    event_index: int
    event_kind: CausalEventKind | None
    event_monotonic_ns: int
    kernel_state: CycleKernelState


@dataclass(slots=True)
class _BookObservation:
    event: CausalEvent
    book: BookEvidence
    processing_ready_ns: int | None
    arrival_index: int
    identity_complete: bool


@dataclass(slots=True)
class _MutableAction:
    action_id: str
    kind: CycleActionKind
    status: CycleActionStatus
    requested_ns: int
    effective_ns: int | None
    due_ns: int | None
    requested_quantity: Decimal
    executed_quantity: Decimal = _ZERO
    reason: str = ""
    evidence_ids: list[str] = field(default_factory=list)

    @property
    def remaining_quantity(self) -> Decimal:
        return self.requested_quantity - self.executed_quantity

    def public(self) -> CycleAction:
        return CycleAction(
            action_id=self.action_id,
            kind=self.kind,
            status=self.status,
            requested_monotonic_ns=self.requested_ns,
            effective_monotonic_ns=self.effective_ns,
            due_monotonic_ns=self.due_ns,
            requested_quantity=self.requested_quantity,
            executed_quantity=self.executed_quantity,
            remaining_quantity=self.remaining_quantity,
            reason=self.reason,
            evidence_ids=tuple(self.evidence_ids),
        )


class _Phase(StrEnum):
    ENTRY_WAIT = "ENTRY_WAIT"
    ENTRY_ACTIVE = "ENTRY_ACTIVE"
    ENTRY_HEDGE_WAIT = "ENTRY_HEDGE_WAIT"
    UNMATCHED_WAIT = "UNMATCHED_WAIT"
    EXIT_WAIT = "EXIT_WAIT"
    EXIT_ACTIVE = "EXIT_ACTIVE"
    EXIT_CANCEL_WAIT = "EXIT_CANCEL_WAIT"
    CLOSE_WAIT = "CLOSE_WAIT"
    FORCE_WAIT = "FORCE_WAIT"
    COMPLETE = "COMPLETE"
    ABORTED = "ABORTED"
    UNRESOLVED = "UNRESOLVED"


@dataclass(slots=True)
class _MutableCycle:
    quote_version: QuoteVersion
    scenario: CycleScenario
    policy: CyclePolicy
    delays: CycleDelays
    entry_quote: CausalRestingQuote
    phase: _Phase
    current_ns: int
    entry_activation_ns: int
    entry_cancel_schedule_ns: int
    entry_cancel_requested_ns: int | None = None
    entry_cancel_effective_ns: int | None = None
    entry_hedge_due_ns: int | None = None
    entry_hedge_action_id: str | None = None
    entry_hedge_done: bool = False
    entry_target_quantity: Decimal = _ZERO
    entry_observed_quantity: Decimal = _ZERO
    entry_remaining_quantity: Decimal = _ZERO
    entry_fills: list[CausalFill] = field(default_factory=list)
    entry_decisions: list[CausalEventDecision] = field(default_factory=list)
    entry_uncertainty: list[str] = field(default_factory=list)
    exit_quote: CausalRestingQuote | None = None
    exit_price: Decimal | None = None
    exit_activation_ns: int | None = None
    exit_cancel_requested_ns: int | None = None
    exit_cancel_effective_ns: int | None = None
    exit_remaining_quantity: Decimal = _ZERO
    exit_fills: list[CausalFill] = field(default_factory=list)
    exit_decisions: list[CausalEventDecision] = field(default_factory=list)
    exit_uncertainty: list[str] = field(default_factory=list)
    fills: list[CycleFill] = field(default_factory=list)
    fees: list[CycleFee] = field(default_factory=list)
    cashflows: list[CycleCashFlow] = field(default_factory=list)
    actions: list[_MutableAction] = field(default_factory=list)
    scheduled_takers: dict[str, tuple[Venue, Side, Decimal, str]] = field(default_factory=dict)
    reasons: list[str] = field(default_factory=list)
    books: list[_BookObservation] = field(default_factory=list)
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
    unresolved: bool = False
    input_witness_deferred: bool = False
    terminal_ns: int | None = None

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


@dataclass(slots=True)
class _KernelLane:
    scenario: CycleScenario
    active: _MutableCycle | None = None
    terminal_cycle: _MutableCycle | None = None
    terminal_cycles: list[_MutableCycle] = field(default_factory=list)
    last_result: CycleResult | None = None
    last_decision_ns: int | None = None
    last_terminal_ns: int | None = None
    halted_unresolved: bool = False
    admission_history: list[CycleAdmission] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class _TerminalAudit:
    """Outcome of checking one trailing event against retained attempts."""

    event_index: int | None
    invalidated: bool = False
    duplicate: bool = False


def _coerce_event(value: CausalEvent | TradeEvidence | BookEvidence | DataGapEvidence) -> CausalEvent:
    if isinstance(value, CausalEvent):
        return value
    if isinstance(value, TradeEvidence):
        return CausalEvent.from_trade(value)
    if isinstance(value, BookEvidence):
        return CausalEvent.from_book(value)
    if isinstance(value, DataGapEvidence):
        return CausalEvent.from_gap(value)
    raise TypeError("cycle events must be causal or accepted public evidence")


def _processing_ready_ns(event: CausalEvent) -> int | None:
    """Return the first local boundary at which an event is usable."""

    if event.kind is CausalEventKind.DATA_GAP:
        return event.causal_monotonic_ns
    if event.ingress_received_monotonic_ns is None or event.normalized_ready_monotonic_ns is None:
        return None
    values = [event.causal_monotonic_ns, event.normalized_ready_monotonic_ns]
    if event.decision_ready_monotonic_ns is not None:
        values.append(event.decision_ready_monotonic_ns)
    return max(values)


def _event_signature(event: CausalEvent) -> tuple[Any, ...]:
    payload = event.payload
    identity = event.source_identity
    identity_value = (
        None
        if not isinstance(identity, CausalSourceIdentity)
        else (
            identity.source_kind,
            identity.source_event_id,
            identity.source_trade_id,
            identity.maker_order_id,
            identity.taker_order_id,
            identity.maker,
            identity.taker,
            identity.tx_hash,
            identity.block_number,
            identity.sequence,
            identity.revision,
            identity.log_index,
            identity.worker_timestamp,
        )
    )
    if isinstance(payload, TradeEvidence):
        payload_value: Any = (
            payload.canonical_price,
            payload.canonical_quantity,
            payload.aggressor_side,
            event.block_number,
            event.sequence,
            event.revision,
            event.match_id,
            identity_value,
        )
    elif isinstance(payload, BookEvidence):
        payload_value = (
            tuple((level.canonical_price, level.canonical_quantity) for level in payload.bids),
            tuple((level.canonical_price, level.canonical_quantity) for level in payload.asks),
            payload.sequence,
            payload.checksum,
            payload.sequence_valid,
            payload.checksum_valid,
            payload.fresh,
            event.block_number,
            event.sequence,
            event.revision,
            identity_value,
        )
    else:
        payload_value = (
            payload.gap_start_monotonic_ns,
            payload.gap_end_monotonic_ns,
            payload.reason,
            payload.transport_event,
            payload.transport_failure_class,
            identity_value,
        )
    return event.kind, payload_value


def _stream_position(event: CausalEvent) -> tuple[int, ...] | None:
    if event.venue is Venue.RISEX:
        if event.block_number is None or event.log_index is None:
            return None
        return event.block_number, event.log_index
    if event.venue is Venue.LIGHTER and event.sequence is not None:
        return (event.sequence,)
    return None


def _trade_crosses(quote: CausalRestingQuote, trade: TradeEvidence) -> tuple[bool, bool]:
    if quote.maker_side is Side.BUY:
        if trade.canonical_price > quote.price:
            return False, False
        improvement = quote.price - trade.canonical_price
    else:
        if trade.canonical_price < quote.price:
            return False, False
        improvement = trade.canonical_price - quote.price
    if improvement == 0:
        return False, True
    if quote.tick_size is None:
        return True, False
    return improvement >= quote.tick_size, improvement < quote.tick_size


def _floor_quantity(value: Decimal, step: Decimal) -> Decimal:
    if value <= 0 or step <= 0:
        return _ZERO
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step


def _market(cycle: _MutableCycle, venue: Venue) -> Any:
    market = cycle.quote_version.quote.policy.risex_market if venue is Venue.RISEX else cycle.quote_version.quote.policy.lighter_market
    if market is None or market.base_multiplier is None or market.base_multiplier <= 0:
        return None
    return market


def _common_step(cycle: _MutableCycle) -> Decimal | None:
    sizing = cycle.quote_version.quote.sizing_evidence
    if sizing is None or sizing.common_quantity_step <= 0:
        return None
    return sizing.common_quantity_step


def _minimum_ok(cycle: _MutableCycle, venue: Venue, quantity: Decimal, price: Decimal) -> bool:
    market = _market(cycle, venue)
    return market is not None and quantity / market.base_multiplier >= market.minimum_quantity_raw and abs(quantity * price) >= market.minimum_notional_usd


def _book_signature(book: BookEvidence) -> tuple[Any, ...]:
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
        tuple((level.canonical_price, level.canonical_quantity) for level in book.bids),
        tuple((level.canonical_price, level.canonical_quantity) for level in book.asks),
    )


def _action(cycle: _MutableCycle, action_id: str) -> _MutableAction:
    for action in cycle.actions:
        if action.action_id == action_id:
            return action
    raise KeyError(action_id)


def _add_action(
    cycle: _MutableCycle,
    *,
    action_id: str,
    kind: CycleActionKind,
    status: CycleActionStatus,
    requested_ns: int,
    effective_ns: int | None,
    due_ns: int | None,
    quantity: Decimal,
    reason: str,
) -> _MutableAction:
    existing = next((item for item in cycle.actions if item.action_id == action_id), None)
    if existing is not None:
        return existing
    item = _MutableAction(
        action_id=action_id,
        kind=kind,
        status=status,
        requested_ns=requested_ns,
        effective_ns=effective_ns,
        due_ns=due_ns,
        requested_quantity=quantity,
        reason=reason,
    )
    cycle.actions.append(item)
    return item


def _schedule_taker(
    cycle: _MutableCycle,
    *,
    action_id: str,
    kind: CycleActionKind,
    venue: Venue,
    side: Side,
    quantity: Decimal,
    requested_ns: int,
    reason: str,
) -> None:
    due = requested_ns + cycle.delays.taker_delay_ns
    _add_action(
        cycle,
        action_id=action_id,
        kind=kind,
        status=CycleActionStatus.PENDING,
        requested_ns=requested_ns,
        effective_ns=requested_ns,
        due_ns=due,
        quantity=quantity,
        reason=reason,
    )
    cycle.scheduled_takers[action_id] = (venue, side, quantity, reason)


def _book_gap_blocks(cycle: _MutableCycle, book: BookEvidence | None, due_ns: int, *, venue: Venue) -> bool:
    if book is not None:
        start_ns = book.received_monotonic_ns
        session = book.stream_session_id
        recovery = book.recovery_generation
    else:
        start_ns = max(0, due_ns - cycle.policy.input_freshness_max_age_ns)
        if venue is Venue.RISEX:
            session = cycle.quote_version.stream_session_id
            recovery = cycle.quote_version.recovery_generation
        else:
            session = cycle.quote_version.hedge_stream_session_id
            recovery = cycle.quote_version.hedge_recovery_generation
        if session is None or recovery is None:
            return False
    return any(
        gap.matches(venue, cycle.quote_version.canonical_market, session, recovery)
        and gap.overlaps(start_ns, due_ns)
        for gap in cycle.gaps
    )


def _select_book(cycle: _MutableCycle, venue: Venue, due_ns: int) -> tuple[BookEvidence | None, CycleReason | None]:
    if venue is Venue.RISEX:
        expected_session = cycle.quote_version.stream_session_id
        expected_recovery = cycle.quote_version.recovery_generation
    else:
        expected_session = cycle.quote_version.hedge_stream_session_id
        expected_recovery = cycle.quote_version.hedge_recovery_generation
    if expected_session is None or expected_recovery is None:
        return None, CycleReason.REQUIRED_ACTION_DATA_MISSING
    candidates = [
        observation
        for observation in cycle.books
        if observation.book.venue is venue
        and observation.book.canonical_market == cycle.quote_version.canonical_market
        and observation.book.stream_session_id == expected_session
        and observation.book.recovery_generation == expected_recovery
    ]
    venue_candidates = [
        observation
        for observation in cycle.books
        if observation.book.venue is venue and observation.book.canonical_market == cycle.quote_version.canonical_market
    ]
    if not candidates:
        if venue_candidates:
            return None, CycleReason.REQUIRED_ACTION_SESSION_DISPLACED
        return None, CycleReason.REQUIRED_ACTION_DATA_MISSING
    candidates.sort(
        key=lambda observation: (
            observation.book.received_monotonic_ns,
            observation.processing_ready_ns if observation.processing_ready_ns is not None else -1,
            observation.book.book_revision,
            observation.arrival_index,
        )
    )
    temporal = [
        observation
        for observation in candidates
        if observation.processing_ready_ns is not None
        and observation.processing_ready_ns <= due_ns
        and observation.book.received_monotonic_ns <= due_ns
    ]
    future = [observation for observation in candidates if observation not in temporal]
    fresh_temporal = [
        observation
        for observation in temporal
        if observation.book.fresh
        and observation.book.is_sequence_healthy
        and due_ns - observation.book.received_monotonic_ns <= cycle.policy.input_freshness_max_age_ns
    ]
    if not temporal:
        if any(observation.processing_ready_ns is None for observation in candidates):
            return None, CycleReason.REQUIRED_ACTION_TIMING_MISSING
        return None, CycleReason.FUTURE_BOOK_REJECTED
    if not fresh_temporal and future:
        # A book that is already stale at the due boundary is not an eligible
        # witness merely because a newer (future) revision was recorded.
        return None, CycleReason.FUTURE_BOOK_REJECTED
    # A future revision is retained as evidence but cannot shadow the latest
    # revision that was actually ready at this scheduled boundary.
    latest = temporal[-1]
    if not latest.identity_complete:
        return None, CycleReason.REQUIRED_ACTION_AMBIGUOUS
    if latest.processing_ready_ns is None:
        return None, CycleReason.REQUIRED_ACTION_TIMING_MISSING
    if not latest.book.is_sequence_healthy:
        return None, CycleReason.REQUIRED_ACTION_UNHEALTHY
    if not latest.book.fresh or due_ns - latest.book.received_monotonic_ns > cycle.policy.input_freshness_max_age_ns:
        return None, CycleReason.REQUIRED_ACTION_DATA_STALE
    if _book_gap_blocks(cycle, latest.book, due_ns, venue=venue):
        return None, CycleReason.REQUIRED_ACTION_DATA_GAP
    return latest.book, None


def _paired_books(cycle: _MutableCycle, due_ns: int) -> tuple[BookEvidence | None, BookEvidence | None, CycleReason | None]:
    risex_book, risex_reason = _select_book(cycle, Venue.RISEX, due_ns)
    lighter_book, lighter_reason = _select_book(cycle, Venue.LIGHTER, due_ns)
    reason = risex_reason or lighter_reason
    if reason is None and risex_book is not None and lighter_book is not None:
        if abs(risex_book.received_monotonic_ns - lighter_book.received_monotonic_ns) > cycle.policy.input_receipt_skew_max_ns:
            reason = CycleReason.REQUIRED_ACTION_RECEIPT_SKEW
    return risex_book, lighter_book, reason


def _append_fill(
    cycle: _MutableCycle,
    *,
    action_id: str,
    venue: Venue,
    side: Side,
    role: LiquidityRole,
    quantity: Decimal,
    price: Decimal,
    reason: str,
    observed_ns: int,
    processing_ns: int | None,
    evidence_id: str,
    source_identity: CausalSourceIdentity | None,
    session: str | int,
    recovery: int,
    book_revision_id: str | None,
) -> CycleFill:
    if quantity <= 0:
        raise ValueError("cycle fills require positive quantity")
    if venue is Venue.RISEX and role is LiquidityRole.MAKER:
        rate = cycle.policy.risex_maker_fee_rate
        source = cycle.policy.risex_fee_source
    elif venue is Venue.RISEX and role is LiquidityRole.TAKER:
        rate = cycle.policy.risex_taker_fee_rate
        source = cycle.policy.risex_fee_source
    elif venue is Venue.LIGHTER and role is LiquidityRole.TAKER:
        rate = cycle.policy.lighter_taker_fee_rate
        source = cycle.policy.lighter_fee_source
    else:
        raise ValueError("unsupported S2 fee role")
    notional = quantity * price
    fee = notional * rate
    gross = notional if side is Side.SELL else -notional
    scenario_cost = notional * cycle.delays.risex_fill_cost_rate if venue is Venue.RISEX else _ZERO
    net = gross - fee - scenario_cost
    if venue is Venue.RISEX:
        cycle.risex_signed_quantity += quantity if side is Side.BUY else -quantity
    else:
        cycle.lighter_signed_quantity += quantity if side is Side.BUY else -quantity
    fill_id = f"{action_id}:fill:{len(cycle.fills)}"
    fill = CycleFill(
        fill_id=fill_id,
        action_id=action_id,
        venue=venue,
        side=side,
        liquidity_role=role,
        quantity=quantity,
        price=price,
        notional_usd=notional,
        fee_rate=rate,
        fee_usd=fee,
        gross_cashflow_usd=gross,
        scenario_cost_usd=scenario_cost,
        net_cashflow_usd=net,
        reason=reason,
        observed_monotonic_ns=observed_ns,
        processing_ready_monotonic_ns=processing_ns,
        evidence_id=evidence_id,
        source_identity=source_identity,
        stream_session_id=session,
        recovery_generation=recovery,
        book_revision_id=book_revision_id,
        risex_position_after=cycle.risex_signed_quantity,
        lighter_position_after=cycle.lighter_signed_quantity,
    )
    cycle.fills.append(fill)
    cycle.fees.append(
        CycleFee(
            fee_id=f"{fill_id}:fee",
            fill_id=fill_id,
            venue=venue,
            liquidity_role=role,
            notional_usd=notional,
            rate=rate,
            amount_usd=fee,
            source=source,
        )
    )
    cycle.cashflows.append(
        CycleCashFlow(
            flow_id=f"{fill_id}:cashflow",
            fill_id=fill_id,
            gross_cashflow_usd=gross,
            fee_usd=fee,
            scenario_cost_usd=scenario_cost,
            net_cashflow_usd=net,
        )
    )
    return fill


def _record_entry_hedge(cycle: _MutableCycle, executable: Decimal) -> None:
    """Apply the entry-hedge observation before deciding the next phase."""

    cycle.hedged_quantity = executable
    cycle.paired_risex_quantity = executable
    cycle.paired_lighter_quantity = executable
    cycle.unmatched_entry_quantity = max(_ZERO, cycle.entry_observed_quantity - executable)
    cycle.initial_unmatched_quantity = cycle.unmatched_entry_quantity


def _set_action_result(
    cycle: _MutableCycle,
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


def _make_causal_measurement(
    cycle: _MutableCycle,
    *,
    quote: CausalRestingQuote,
    fills: list[CausalFill],
    decisions: list[CausalEventDecision],
    observed_quantity: Decimal,
    remaining_quantity: Decimal,
    uncertainty: list[str],
    effective_cancel_ns: int | None,
    cancel_requested_ns: int | None,
    outcome: CausalOutcome,
) -> CausalQuoteMeasurement:
    timing = CausalTimingDiagnostics(
        quote_ingress_received_monotonic_ns=quote.ingress_received_monotonic_ns,
        quote_normalized_ready_monotonic_ns=quote.normalized_ready_monotonic_ns,
        quote_decision_ready_monotonic_ns=quote.decision_ready_monotonic_ns,
        quote_activation_monotonic_ns=quote.activation_monotonic_ns,
        first_event_received_monotonic_ns=(
            min(fill.received_monotonic_ns for fill in fills) if fills else None
        ),
        last_event_received_monotonic_ns=(
            max(fill.received_monotonic_ns for fill in fills) if fills else None
        ),
        input_receipt_fresh=None,
    )
    proven = _ZERO if uncertainty else observed_quantity
    causal_reasons = tuple(CausalUncertainty(reason) for reason in uncertainty)
    return CausalQuoteMeasurement(
        quote=quote,
        outcome=outcome,
        observed_filled_quantity=observed_quantity,
        filled_quantity=proven,
        remaining_quantity=remaining_quantity,
        fills=tuple(fills),
        decisions=tuple(decisions),
        uncertainty_reasons=causal_reasons,
        timing=timing,
        event_count=cycle.event_count,
        duplicate_event_count=cycle.duplicate_event_count,
        ignored_event_count=cycle.ignored_event_count,
        last_event_monotonic_ns=cycle.current_ns,
        effective_cancel_monotonic_ns=effective_cancel_ns,
        cancel_requested_monotonic_ns=cancel_requested_ns,
    )


def _entry_measurement(cycle: _MutableCycle) -> CausalQuoteMeasurement:
    if cycle.unresolved and cycle.entry_uncertainty:
        outcome = CausalOutcome.CAUSAL_UNCERTAIN
    elif cycle.entry_observed_quantity == cycle.entry_target_quantity and cycle.entry_target_quantity > 0:
        outcome = CausalOutcome.FULL_FILL
    elif cycle.entry_observed_quantity > 0:
        outcome = CausalOutcome.PARTIAL_FILL
    elif cycle.entry_cancel_effective_ns is not None and cycle.current_ns >= cycle.entry_cancel_effective_ns:
        outcome = CausalOutcome.CANCELLED_NO_FILL
    else:
        outcome = CausalOutcome.NO_FILL
    return _make_causal_measurement(
        cycle,
        quote=cycle.entry_quote,
        fills=cycle.entry_fills,
        decisions=cycle.entry_decisions,
        observed_quantity=cycle.entry_observed_quantity,
        remaining_quantity=cycle.entry_remaining_quantity,
        uncertainty=cycle.entry_uncertainty,
        effective_cancel_ns=cycle.entry_cancel_effective_ns,
        cancel_requested_ns=cycle.entry_cancel_requested_ns,
        outcome=outcome,
    )


def _exit_measurement(cycle: _MutableCycle) -> CausalQuoteMeasurement | None:
    if cycle.exit_quote is None:
        return None
    target = cycle.exit_quote.quantity
    observed = sum((fill.consumed_quantity for fill in cycle.exit_fills), _ZERO)
    if cycle.exit_uncertainty:
        outcome = CausalOutcome.CAUSAL_UNCERTAIN
        uncertainty = cycle.exit_uncertainty
    elif observed == target and target > 0:
        outcome = CausalOutcome.FULL_FILL
        uncertainty = []
    elif observed > 0:
        outcome = CausalOutcome.PARTIAL_FILL
        uncertainty = []
    elif cycle.exit_cancel_effective_ns is not None and cycle.current_ns >= cycle.exit_cancel_effective_ns:
        outcome = CausalOutcome.CANCELLED_NO_FILL
        uncertainty = []
    else:
        outcome = CausalOutcome.NO_FILL
        uncertainty = []
    return _make_causal_measurement(
        cycle,
        quote=cycle.exit_quote,
        fills=cycle.exit_fills,
        decisions=cycle.exit_decisions,
        observed_quantity=observed,
        remaining_quantity=max(_ZERO, target - observed),
        uncertainty=uncertainty,
        effective_cancel_ns=cycle.exit_cancel_effective_ns,
        cancel_requested_ns=cycle.exit_cancel_requested_ns,
        outcome=outcome,
    )


def _result(cycle: _MutableCycle, *, terminal: bool = False) -> CycleResult:
    if cycle.unresolved:
        status = CycleTerminalState.UNRESOLVED
    elif cycle.phase is _Phase.COMPLETE:
        status = CycleTerminalState.FORCED if cycle.forced_used else CycleTerminalState.NORMAL
    elif cycle.phase is _Phase.ABORTED:
        status = CycleTerminalState.ABORTED
    else:
        status = CycleTerminalState.PENDING
    pending = any(action.status in {CycleActionStatus.PENDING, CycleActionStatus.UNRESOLVED} for action in cycle.actions)
    positions = cycle.positions
    flat_terminal = status in {CycleTerminalState.NORMAL, CycleTerminalState.FORCED, CycleTerminalState.ABORTED} and positions.is_zero and not pending
    cashflow_complete = (
        status in {CycleTerminalState.NORMAL, CycleTerminalState.FORCED}
        and flat_terminal
        and len(cycle.fills) == len(cycle.fees) == len(cycle.cashflows)
    )
    terminal_ns = cycle.terminal_ns if terminal else (cycle.current_ns if status is CycleTerminalState.UNRESOLVED else None)
    if status is CycleTerminalState.UNRESOLVED:
        cycle.add_reason(CycleReason.TERMINAL_NON_FLAT if not positions.is_zero else CycleReason.TERMINAL_PENDING_ACTION if pending else CycleReason.TERMINAL_PENDING_ACTION)
    holding = None
    if cycle.first_maker_fill_ns is not None and terminal_ns is not None:
        holding = max(0, terminal_ns - cycle.first_maker_fill_ns)
    unmatched_duration: int | None
    if cycle.unmatched_started_ns is None:
        unmatched_duration = 0
    elif cycle.unmatched_resolved_ns is not None:
        unmatched_duration = max(0, cycle.unmatched_resolved_ns - cycle.unmatched_started_ns)
    elif terminal_ns is not None:
        unmatched_duration = max(0, terminal_ns - cycle.unmatched_started_ns)
    else:
        unmatched_duration = None
    return CycleResult(
        scenario=cycle.scenario,
        quote_version_id=cycle.quote_version.version_id,
        canonical_market=cycle.quote_version.canonical_market,
        status=status,
        reason_codes=tuple(cycle.reasons),
        entry_measurement=_entry_measurement(cycle),
        exit_measurement=_exit_measurement(cycle),
        entry_edge_usd=cycle.quote_version.quote.actual_edge_usd,
        entry_quantity=cycle.entry_observed_quantity,
        hedged_quantity=cycle.hedged_quantity,
        unmatched_entry_quantity=cycle.initial_unmatched_quantity,
        exit_price=cycle.exit_price,
        first_maker_fill_monotonic_ns=cycle.first_maker_fill_ns,
        max_hold_deadline_monotonic_ns=cycle.max_hold_deadline_ns,
        terminal_monotonic_ns=terminal_ns,
        positions=positions,
        ledger=CycleLedger(tuple(cycle.fills), tuple(cycle.fees), tuple(cycle.cashflows)),
        actions=tuple(action.public() for action in cycle.actions),
        cashflow_complete=cashflow_complete,
        complete_execution_pnl_usd=cycle_net_cashflow(cycle) if cashflow_complete else None,
        holding_duration_ns=holding,
        unmatched_exposure_duration_ns=unmatched_duration,
    )


def cycle_net_cashflow(cycle: _MutableCycle) -> Decimal:
    """Small internal aggregate used only after the flat barrier."""

    return sum((flow.net_cashflow_usd for flow in cycle.cashflows), _ZERO)


def _invalid_result(
    quote_version: QuoteVersion,
    *,
    scenario: CycleScenario,
    reason: CycleReason,
    unresolved: bool = False,
    uncertainty: CausalUncertainty | None = None,
) -> CycleResult:
    policy = s2_cycle_policy()
    delays = policy.delays(scenario)
    # A minimal invalid lane retains the reason but never manufactures a fill
    # or a terminal PnL value.
    placeholder = CausalRestingQuote(
        quote_id=f"{quote_version.version_id}:invalid",
        quote_version_id=f"{quote_version.version_id}:invalid",
        canonical_market=quote_version.canonical_market,
        maker_side=Side.SELL,
        price=Decimal("1"),
        quantity=Decimal("1"),
        stream_session_id=quote_version.stream_session_id,
        recovery_generation=quote_version.recovery_generation,
        decision_ready_monotonic_ns=quote_version.decision_ready_monotonic_ns,
        activation_delay_ns=delays.activation_delay_ns,
    )
    cycle = _MutableCycle(
        quote_version=quote_version,
        scenario=scenario,
        policy=policy,
        delays=delays,
        entry_quote=placeholder,
        phase=_Phase.UNRESOLVED if unresolved else _Phase.ABORTED,
        current_ns=quote_version.decision_ready_monotonic_ns or quote_version.quote_created_monotonic_ns,
        entry_activation_ns=placeholder.activation_monotonic_ns or 0,
        entry_cancel_schedule_ns=(placeholder.activation_monotonic_ns or 0) + policy.entry_cancel_after_activation_ns,
        entry_target_quantity=Decimal("1"),
        entry_remaining_quantity=Decimal("1"),
        unresolved=unresolved,
    )
    cycle.add_reason(reason)
    if not unresolved:
        cycle.add_reason(CycleReason.INVALID_ENTRY_QUOTE)
    if uncertainty is not None:
        cycle.entry_uncertainty.append(uncertainty.value)
    cycle.terminal_ns = cycle.current_ns
    return _result(cycle, terminal=True)


def _book_matches_version_binding(
    book: BookEvidence,
    *,
    venue: Venue,
    canonical_market: str,
    stream_session_id: str | int | None,
    recovery_generation: int | None,
    book_revision: int | None,
    book_revision_id: str | None,
) -> bool:
    """Match one admission witness to every immutable S1 binding field."""

    return (
        stream_session_id is not None
        and recovery_generation is not None
        and book_revision is not None
        and book_revision_id is not None
        and book.venue is venue
        and book.canonical_market == canonical_market
        and book.stream_session_id == stream_session_id
        and book.recovery_generation == recovery_generation
        and book.book_revision == book_revision
        and book.book_revision_id == book_revision_id
    )


def _admission_witness(
    quote_version: QuoteVersion,
    books: tuple[BookEvidence, ...],
    *,
    venue: Venue,
) -> BookEvidence | None:
    """Return a unique exact witness, or one candidate for S1 diagnostics."""

    if venue is Venue.RISEX:
        session = quote_version.stream_session_id
        recovery = quote_version.recovery_generation
        revision = quote_version.risex_book_revision
        revision_id = quote_version.risex_book_revision_id
    else:
        session = quote_version.hedge_stream_session_id
        recovery = quote_version.hedge_recovery_generation
        revision = quote_version.lighter_book_revision
        revision_id = quote_version.lighter_book_revision_id
    candidates = tuple(
        book
        for book in books
        if book.venue is venue and book.canonical_market == quote_version.canonical_market
    )
    exact = tuple(
        book
        for book in candidates
        if _book_matches_version_binding(
            book,
            venue=venue,
            canonical_market=quote_version.canonical_market,
            stream_session_id=session,
            recovery_generation=recovery,
            book_revision=revision,
            book_revision_id=revision_id,
        )
    )
    if len(exact) == 1:
        return exact[0]
    # Passing one sole wrong candidate through the S1 measurement validator
    # retains its concrete mismatch/staleness classification.  Multiple
    # candidates are intentionally left ambiguous rather than guessed.
    return candidates[0] if len(candidates) == 1 else None


def _entry_input_failure(
    quote_version: QuoteVersion,
    policy: CyclePolicy,
    books: tuple[BookEvidence, ...],
) -> tuple[CycleReason | None, tuple[CausalUncertainty, ...]]:
    """Validate the S1 causal input contract before S2 admits a cycle.

    The S2 transition path must not infer its initial quote inputs from later
    stream evidence.  Building and measuring the S1 causal quote here reuses
    the accepted identity/readiness/freshness/health checks, while the small
    additional skew check applies the S2 policy's configured receipt bound.
    """

    decision = quote_version.decision_ready_monotonic_ns
    if decision is None:
        return CycleReason.MISSING_ENTRY_TIMING, (CausalUncertainty.MISSING_CAUSAL_TIMING,)
    if (
        quote_version.ingress_received_monotonic_ns is None
        or quote_version.normalized_ready_monotonic_ns is None
    ):
        return CycleReason.ENTRY_INPUT_AMBIGUOUS, (CausalUncertainty.MISSING_CAUSAL_TIMING,)
    if quote_version.normalized_ready_monotonic_ns > decision:
        return CycleReason.ENTRY_INPUT_STALE, (CausalUncertainty.SOURCE_BOOK_AFTER_DECISION,)

    source_book = _admission_witness(quote_version, books, venue=Venue.RISEX)
    hedge_book = _admission_witness(quote_version, books, venue=Venue.LIGHTER)
    try:
        causal_quote = build_causal_resting_quote(
            quote_version,
            source_book=source_book,
            hedge_source_book=hedge_book,
            source_book_freshness_max_age_ns=policy.input_freshness_max_age_ns,
        )
        measurement = measure_causal_quote(
            causal_quote,
            (),
            end_monotonic_ns=decision,
            source_books=books,
        )
    except (ArithmeticError, TypeError, ValueError):
        return CycleReason.ENTRY_INPUT_AMBIGUOUS, (CausalUncertainty.MISSING_CAUSAL_TIMING,)

    uncertainties = tuple(measurement.uncertainty_reasons)
    if measurement.timing.input_receipt_skew_ns is not None and (
        measurement.timing.input_receipt_skew_ns > policy.input_receipt_skew_max_ns
    ):
        uncertainties = tuple(
            dict.fromkeys((*uncertainties, CausalUncertainty.SOURCE_BOOK_RECEIPT_SKEW))
        )
    if not uncertainties:
        return None, ()

    stale_reasons = {
        CausalUncertainty.SOURCE_BOOK_STALE,
        CausalUncertainty.SOURCE_BOOK_AFTER_DECISION,
        CausalUncertainty.SOURCE_BOOK_RECEIPT_SKEW,
    }
    reason = (
        CycleReason.ENTRY_INPUT_STALE
        if any(item in stale_reasons for item in uncertainties)
        else CycleReason.ENTRY_INPUT_AMBIGUOUS
    )
    return reason, uncertainties


def _input_uncertainty_for_reason(reason: CycleReason) -> CausalUncertainty:
    if reason is CycleReason.ENTRY_INPUT_STALE:
        return CausalUncertainty.SOURCE_BOOK_STALE
    if reason is CycleReason.ENTRY_INPUT_GAP:
        return CausalUncertainty.DATA_GAP
    return CausalUncertainty.MISSING_SOURCE_IDENTITY


class CycleKernel:
    """Stateful one-pair transition kernel for fixture streams and replay.

    ``advance`` and ``advance_clock`` are the execution path.  They mutate the
    active lane immediately; ``run_cycle`` and ``replay`` only feed those same
    transitions.  A clock boundary never advances to a later scheduled
    boundary on its own.
    """

    _MIN_DECISION_INTERVAL_NS = 1_000_000_000
    # No venue or fixture contract supplies a maximum normalization delay.
    # Retain enough completed quote intervals for the caller-bounded research
    # window; callers with a different resource envelope can set this
    # explicitly.  Exhaustion halts before an older identity is retired.
    _DEFAULT_TERMINAL_RETENTION_CAPACITY = 64

    def __init__(
        self,
        policy: CyclePolicy | None = None,
        *,
        terminal_retention_capacity: int = _DEFAULT_TERMINAL_RETENTION_CAPACITY,
    ) -> None:
        self.policy = s2_cycle_policy() if policy is None else policy
        if not isinstance(self.policy, CyclePolicy):
            raise TypeError("policy must be CyclePolicy")
        if (
            isinstance(terminal_retention_capacity, bool)
            or not isinstance(terminal_retention_capacity, int)
            or terminal_retention_capacity <= 0
        ):
            raise ValueError("terminal_retention_capacity must be a positive integer")
        self.terminal_retention_capacity = terminal_retention_capacity
        self._lanes = {scenario: _KernelLane(scenario) for scenario in CycleScenario}

    @staticmethod
    def _scenario(value: CycleScenario) -> CycleScenario:
        return value if isinstance(value, CycleScenario) else CycleScenario(value)

    def _lane(self, scenario: CycleScenario) -> _KernelLane:
        return self._lanes[self._scenario(scenario)]

    def state(self, scenario: CycleScenario = CycleScenario.PRIMARY) -> CycleKernelState:
        lane = self._lane(scenario)
        if lane.halted_unresolved:
            return CycleKernelState.UNRESOLVED_HALTED
        return CycleKernelState.PENDING if lane.active is not None else CycleKernelState.FLAT

    @property
    def admissions(self) -> tuple[CycleAdmission, ...]:
        return tuple(item for lane in self._lanes.values() for item in lane.admission_history)

    def admissions_for(self, scenario: CycleScenario = CycleScenario.PRIMARY) -> tuple[CycleAdmission, ...]:
        return tuple(self._lane(scenario).admission_history)

    def last_result(self, scenario: CycleScenario = CycleScenario.PRIMARY) -> CycleResult | None:
        return self._lane(scenario).last_result

    def snapshot(self, scenario: CycleScenario = CycleScenario.PRIMARY) -> CycleResult | None:
        """Return current state without advancing time or fabricating evidence."""

        lane = self._lane(scenario)
        if lane.active is None:
            return lane.last_result
        result = _result(lane.active)
        lane.last_result = result
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
        try:
            books = tuple(source_books)
        except TypeError:
            raise TypeError("source_books must be iterable") from None
        if any(not isinstance(book, BookEvidence) for book in books):
            raise TypeError("source_books must contain BookEvidence")
        decision = quote_version.decision_ready_monotonic_ns
        accepted = True
        reason = "ACCEPTED"
        input_witness_deferred = False
        if lane.halted_unresolved:
            accepted = False
            reason = CycleReason.UNRESOLVED_HALTED.value
        elif lane.active is not None:
            accepted = False
            reason = CycleReason.ACTIVE_CYCLE.value
        elif len(lane.terminal_cycles) >= self.terminal_retention_capacity:
            # Without an explicit processing watermark, do not retire an
            # older quote interval merely to admit another one.
            accepted = False
            reason = CycleReason.TERMINAL_RETENTION_EXHAUSTED.value
            lane.halted_unresolved = True
        elif decision is None:
            accepted = False
            reason = CycleReason.MISSING_ENTRY_TIMING.value
        elif lane.last_terminal_ns is not None and decision < lane.last_terminal_ns:
            accepted = False
            reason = CycleReason.DECISION_WITHIN_PREVIOUS_CYCLE.value
        elif lane.last_decision_ns is not None and decision < lane.last_decision_ns + self._MIN_DECISION_INTERVAL_NS:
            accepted = False
            reason = CycleReason.DECISION_RATE_LIMIT.value
        elif not self._valid_entry_quote(quote_version):
            accepted = False
            reason = CycleReason.INVALID_ENTRY_QUOTE.value
        if accepted:
            input_reason, _ = _entry_input_failure(quote_version, self.policy, books)
            if input_reason is not None:
                # Preserve the historical no-event re-entry probe: it may
                # open a no-fill attempt for an already-flat lane, but any
                # evidence presented to that attempt is halted rather than
                # executed without its own S1 witnesses.
                input_witness_deferred = (
                    not books
                    and lane.last_terminal_ns is not None
                    and lane.last_result is not None
                    and lane.last_result.is_flat
                )
                if not input_witness_deferred:
                    accepted = False
                    reason = input_reason.value
                    # A rejected causal input is a terminal safety failure
                    # for this lane.  Later books belong to the stream, not
                    # to the invalidated decision, and must not rehabilitate
                    # it.
                    lane.halted_unresolved = True
        admission = CycleAdmission(
            accepted=accepted,
            scenario=scenario,
            quote_version_id=quote_version.version_id,
            decision_monotonic_ns=decision,
            reason=reason,
        )
        lane.admission_history.append(admission)
        if accepted:
            assert decision is not None
            delays = self.policy.delays(scenario)
            activation = decision + delays.activation_delay_ns
            entry_quote = CausalRestingQuote(
                quote_id=quote_version.version_id,
                quote_version_id=quote_version.version_id,
                canonical_market=quote_version.canonical_market,
                maker_side=Side.SELL,
                price=quote_version.quote.maker_price,  # type: ignore[arg-type]
                quantity=quote_version.quote.canonical_quantity,  # type: ignore[arg-type]
                stream_session_id=quote_version.stream_session_id,
                recovery_generation=quote_version.recovery_generation,
                ingress_received_monotonic_ns=quote_version.ingress_received_monotonic_ns,
                normalized_ready_monotonic_ns=quote_version.normalized_ready_monotonic_ns,
                decision_ready_monotonic_ns=decision,
                activation_delay_ns=delays.activation_delay_ns,
                cancel_requested_monotonic_ns=None,
                cancel_delay_ns=delays.cancel_delay_ns,
                cancel_on_first_partial=True,
                quote_created_monotonic_ns=quote_version.quote_created_monotonic_ns,
                source_book_freshness_max_age_ns=self.policy.input_freshness_max_age_ns,
                tick_size=quote_version.quote.risex_tick_size,
                hedge_stream_session_id=quote_version.hedge_stream_session_id,
                hedge_recovery_generation=quote_version.hedge_recovery_generation,
            )
            cycle = _MutableCycle(
                quote_version=quote_version,
                scenario=scenario,
                policy=self.policy,
                delays=delays,
                entry_quote=entry_quote,
                phase=_Phase.ENTRY_WAIT,
                current_ns=decision,
                entry_activation_ns=activation,
                entry_cancel_schedule_ns=activation + self.policy.entry_cancel_after_activation_ns,
                entry_target_quantity=quote_version.quote.canonical_quantity,  # type: ignore[arg-type]
                entry_remaining_quantity=quote_version.quote.canonical_quantity,  # type: ignore[arg-type]
                input_witness_deferred=input_witness_deferred,
            )
            lane.active = cycle
            lane.last_decision_ns = decision
            _add_action(
                cycle,
                action_id="entry-maker",
                kind=CycleActionKind.ENTRY_MAKER,
                status=CycleActionStatus.PENDING,
                requested_ns=decision,
                effective_ns=activation,
                due_ns=cycle.entry_cancel_schedule_ns,
                quantity=cycle.entry_target_quantity,
                reason="ENTRY_RISEX_MAKER_QUOTE",
            )
            for book in books:
                self._record_book(cycle, CausalEvent.from_book(book), initial=True)
            lane.last_result = _result(cycle)
        return admission

    @staticmethod
    def _valid_entry_quote(quote_version: QuoteVersion) -> bool:
        quote = quote_version.quote
        return (
            quote_version.canonical_market == "BTC"
            and quote.policy.direction is SpreadDirection.RISEX_SELL_LIGHTER_BUY
            and quote.outcome is EntryViabilityOutcome.QUOTE_ACTIVE
            and quote.is_active
            and quote.canonical_quantity is not None
            and quote.canonical_quantity > 0
            and quote.maker_price is not None
            and quote.maker_price > 0
            and quote.policy.target_notional_usd == Decimal("100")
            and quote.policy.target_margin_bps == Decimal("1")
            and quote.policy.risex_maker_fee_rate == Decimal("0.0001")
            and quote.policy.lighter_taker_fee_rate == Decimal("0")
        )

    def advance(
        self,
        event: CausalEvent | TradeEvidence | BookEvidence | DataGapEvidence | CycleClock | int | None = None,
        *,
        scenario: CycleScenario = CycleScenario.PRIMARY,
        at_monotonic_ns: int | None = None,
    ) -> CycleProgress:
        """Consume exactly one event or one explicit clock boundary."""

        lane = self._lane(scenario)
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
            if event is None or at_monotonic_ns is not None:
                raise RuntimeError("no admitted cycle is pending")
            if not lane.terminal_cycles:
                raise RuntimeError("no admitted cycle is pending")
            causal_event = _coerce_event(event)
            audit = self._audit_terminal_cycles(lane, causal_event)
            terminal = lane.terminal_cycles[-1]
            event_index = audit.event_index
            if event_index is None:
                event_index = terminal.event_count
            return CycleProgress(
                scenario=terminal.scenario,
                quote_version_id=terminal.quote_version.version_id,
                event_index=event_index,
                event_kind=causal_event.kind,
                event_monotonic_ns=causal_event.causal_monotonic_ns,
                kernel_state=self.state(scenario),
            )
        cycle = lane.active
        if event is None:
            if at_monotonic_ns is None:
                raise ValueError("advance requires an event or clock boundary")
            _non_negative_int(at_monotonic_ns, "at_monotonic_ns")
            self.advance_clock(at_monotonic_ns, scenario=scenario)
            return CycleProgress(
                scenario=cycle.scenario,
                quote_version_id=cycle.quote_version.version_id,
                event_index=cycle.event_count,
                event_kind=None,
                event_monotonic_ns=at_monotonic_ns,
                kernel_state=self.state(scenario),
            )
        causal_event = _coerce_event(event)
        audit = self._audit_previous_terminal(lane, causal_event)
        if audit.invalidated:
            cycle.add_reason(CycleReason.ENTRY_CAUSAL_UNCERTAINTY)
            cycle.entry_uncertainty.append(CausalUncertainty.LATE_OLDER_EVENT.value)
            self._halt(cycle, CycleReason.ENTRY_CAUSAL_UNCERTAINTY)
            lane.last_result = _result(cycle, terminal=True)
            self._latch_terminal(lane, cycle)
            return CycleProgress(
                scenario=cycle.scenario,
                quote_version_id=cycle.quote_version.version_id,
                event_index=cycle.event_count,
                event_kind=causal_event.kind,
                event_monotonic_ns=causal_event.causal_monotonic_ns,
                kernel_state=self.state(scenario),
            )
        if audit.duplicate:
            # The event identity was already committed by a prior attempt.
            # Consume it as a benign cross-attempt duplicate, but never let
            # it become a fresh fill in the active attempt.
            cycle.event_count += 1
            event_index = cycle.event_count - 1
            cycle.duplicate_event_count += 1
            cycle.ignored_event_count += 1
            decisions = (
                cycle.entry_decisions
                if cycle.phase
                in {_Phase.ENTRY_WAIT, _Phase.ENTRY_ACTIVE, _Phase.ENTRY_HEDGE_WAIT}
                else cycle.exit_decisions
            )
            decisions.append(
                CausalEventDecision(
                    causal_event.kind,
                    causal_event.event_id,
                    causal_event.ingress_received_monotonic_ns,
                    "IGNORED",
                    "DUPLICATE_ALREADY_COMMITTED",
                )
            )
            lane.last_result = _result(cycle)
            return CycleProgress(
                scenario=cycle.scenario,
                quote_version_id=cycle.quote_version.version_id,
                event_index=event_index,
                event_kind=causal_event.kind,
                event_monotonic_ns=causal_event.causal_monotonic_ns,
                kernel_state=self.state(scenario),
            )
        if cycle.input_witness_deferred:
            cycle.event_count += 1
            event_index = cycle.event_count - 1
            identity_key = self._terminal_event_key(causal_event)
            if identity_key is not None:
                cycle.seen_events[identity_key] = _event_signature(causal_event)
            cycle.add_reason(CycleReason.ENTRY_INPUT_AMBIGUOUS)
            cycle.entry_uncertainty.append(CausalUncertainty.MISSING_SOURCE_IDENTITY.value)
            cycle.entry_decisions.append(
                CausalEventDecision(
                    causal_event.kind,
                    causal_event.event_id,
                    causal_event.ingress_received_monotonic_ns,
                    "UNCERTAIN",
                    CycleReason.ENTRY_INPUT_AMBIGUOUS.value,
                )
            )
            cycle.input_witness_deferred = False
            self._halt(cycle, CycleReason.ENTRY_INPUT_AMBIGUOUS)
            lane.last_result = _result(cycle, terminal=True)
            self._latch_terminal(lane, cycle)
            return CycleProgress(
                scenario=cycle.scenario,
                quote_version_id=cycle.quote_version.version_id,
                event_index=event_index,
                event_kind=causal_event.kind,
                event_monotonic_ns=causal_event.causal_monotonic_ns,
                kernel_state=self.state(scenario),
            )
        cycle.event_count += 1
        event_index = cycle.event_count - 1
        event_time = causal_event.causal_monotonic_ns
        self._accept_event(cycle, causal_event)
        lane.last_result = _result(cycle)
        if cycle.phase in {_Phase.COMPLETE, _Phase.ABORTED, _Phase.UNRESOLVED}:
            self._latch_terminal(lane, cycle)
        return CycleProgress(
            scenario=cycle.scenario,
            quote_version_id=cycle.quote_version.version_id,
            event_index=event_index,
            event_kind=causal_event.kind,
            event_monotonic_ns=event_time,
            kernel_state=self.state(scenario),
        )

    def advance_clock(self, at_monotonic_ns: int, *, scenario: CycleScenario = CycleScenario.PRIMARY) -> None:
        """Process due actions through exactly this observed clock boundary."""

        lane = self._lane(scenario)
        if lane.active is None:
            raise RuntimeError("no admitted cycle is pending")
        cycle = lane.active
        _non_negative_int(at_monotonic_ns, "at_monotonic_ns")
        if at_monotonic_ns < cycle.current_ns:
            cycle.add_reason(CycleReason.LATE_OLDER_EVENT)
            self._halt(cycle, CycleReason.LATE_OLDER_EVENT)
            lane.last_result = _result(cycle, terminal=True)
            lane.halted_unresolved = True
            lane.active = None
            return
        self._run_due_until(cycle, at_monotonic_ns)
        lane.last_result = _result(cycle)
        if cycle.phase in {_Phase.COMPLETE, _Phase.ABORTED, _Phase.UNRESOLVED}:
            self._latch_terminal(lane, cycle)

    clock = advance_clock

    def finish(self, *, scenario: CycleScenario = CycleScenario.PRIMARY, end_monotonic_ns: int | None = None) -> CycleResult:
        """Return a snapshot, optionally through one explicit final boundary.

        With no boundary this method is intentionally a snapshot.  It does
        not infer a future cancel, taker fill, or max-hold event from a prefix.
        """

        lane = self._lane(scenario)
        if end_monotonic_ns is not None and lane.active is not None:
            self.advance_clock(end_monotonic_ns, scenario=scenario)
        result = self.snapshot(scenario)
        if result is None:
            raise RuntimeError("no cycle has been admitted")
        return result

    def run(
        self,
        quote_version: QuoteVersion,
        events: Iterable[CausalEvent | TradeEvidence | BookEvidence | DataGapEvidence],
        *,
        scenario: CycleScenario = CycleScenario.PRIMARY,
        source_books: Iterable[BookEvidence] = (),
        end_monotonic_ns: int | None = None,
    ) -> CycleResult:
        admission = self.admit(quote_version, scenario=scenario, source_books=source_books)
        if not admission.accepted:
            if admission.reason == CycleReason.INVALID_ENTRY_QUOTE.value:
                return _invalid_result(quote_version, scenario=self._scenario(scenario), reason=CycleReason.INVALID_ENTRY_QUOTE)
            if admission.reason in {
                CycleReason.ENTRY_INPUT_AMBIGUOUS.value,
                CycleReason.ENTRY_INPUT_STALE.value,
                CycleReason.ENTRY_INPUT_GAP.value,
            }:
                result = _invalid_result(
                    quote_version,
                    scenario=self._scenario(scenario),
                    reason=CycleReason(admission.reason),
                    unresolved=True,
                    uncertainty=_input_uncertainty_for_reason(CycleReason(admission.reason)),
                )
                self._lane(scenario).last_result = result
                return result
            raise CycleAdmissionError(admission)
        for event in events:
            self.advance(event, scenario=scenario)
        return self.finish(scenario=scenario, end_monotonic_ns=end_monotonic_ns)

    run_stream = run

    def replay(
        self,
        quote_version: QuoteVersion,
        events: Iterable[CausalEvent | TradeEvidence | BookEvidence | DataGapEvidence],
        *,
        scenario: CycleScenario = CycleScenario.PRIMARY,
        source_books: Iterable[BookEvidence] = (),
        end_monotonic_ns: int | None = None,
    ) -> CycleResult:
        return self.run(
            quote_version,
            events,
            scenario=scenario,
            source_books=source_books,
            end_monotonic_ns=end_monotonic_ns,
        )

    def run_sequence(self, attempts: Iterable[CycleAttempt]) -> tuple[CycleResult, ...]:
        cycles: list[_MutableCycle] = []
        for attempt in attempts:
            if not isinstance(attempt, CycleAttempt):
                raise TypeError("run_sequence expects CycleAttempt values")
            admission = self.admit(
                attempt.quote_version,
                scenario=attempt.scenario,
                source_books=attempt.source_books,
            )
            if not admission.accepted:
                continue
            for event in attempt.events:
                self.advance(event, scenario=attempt.scenario)
            self.finish(scenario=attempt.scenario, end_monotonic_ns=attempt.end_monotonic_ns)
            lane = self._lane(attempt.scenario)
            if lane.active is not None:
                cycles.append(lane.active)
            elif lane.terminal_cycles:
                cycles.append(lane.terminal_cycles[-1])
            else:
                raise RuntimeError("accepted cycle was not retained")
        # A later attempt can audit and invalidate an older retained cycle.
        # Rebuild every returned snapshot from its mutable evidence so the
        # sequence cannot preserve a stale authoritative NORMAL/FLAT result.
        return tuple(
            _result(
                cycle,
                terminal=cycle.phase
                in {_Phase.COMPLETE, _Phase.ABORTED, _Phase.UNRESOLVED},
            )
            for cycle in cycles
        )

    def alternatives(
        self,
        quote_version: QuoteVersion,
        events: Iterable[CausalEvent | TradeEvidence | BookEvidence | DataGapEvidence],
        *,
        source_books: Iterable[BookEvidence] = (),
    ) -> CycleAlternatives:
        return run_cycle_alternatives(quote_version, events, policy=self.policy, source_books=source_books)

    @staticmethod
    def _terminal_event_key(event: CausalEvent) -> tuple[Any, ...] | None:
        if event.stream_key is None or event.event_id is None:
            return None
        return event.stream_key, event.event_id

    def _mark_late_terminal_trade(
        self,
        cycle: _MutableCycle,
        event: CausalEvent,
        *,
        entry: bool,
    ) -> None:
        reason = (
            CycleReason.ENTRY_CAUSAL_UNCERTAINTY
            if entry
            else CycleReason.EXIT_CAUSAL_UNCERTAINTY
        )
        uncertainty = (
            cycle.entry_uncertainty
            if entry
            else cycle.exit_uncertainty
        )
        cycle.add_reason(reason)
        cycle.add_reason(CycleReason.LATE_OLDER_EVENT)
        if CausalUncertainty.LATE_OLDER_EVENT.value not in uncertainty:
            uncertainty.append(CausalUncertainty.LATE_OLDER_EVENT.value)
        if not event.source_identity_complete:
            missing = CausalUncertainty.MISSING_SOURCE_IDENTITY.value
            if missing not in uncertainty:
                uncertainty.append(missing)
        elif not event.identity_metadata_consistent:
            mismatch = CausalUncertainty.SOURCE_IDENTITY_MISMATCH.value
            if mismatch not in uncertainty:
                uncertainty.append(mismatch)
        elif (
            event.stream_session_id != cycle.quote_version.stream_session_id
            or event.recovery_generation != cycle.quote_version.recovery_generation
        ):
            recovery = CausalUncertainty.RECOVERY_TRANSITION.value
            if recovery not in uncertainty:
                uncertainty.append(recovery)
        cycle_decisions = cycle.entry_decisions if entry else cycle.exit_decisions
        cycle_decisions.append(
            CausalEventDecision(
                event.kind,
                event.event_id,
                event.ingress_received_monotonic_ns,
                "UNCERTAIN",
                "LATE_ENTRY_AFTER_COMMIT" if entry else "LATE_EXIT_AFTER_COMMIT",
            )
        )
        self._halt(cycle, reason)

    def _accept_terminal_event(self, cycle: _MutableCycle, event: CausalEvent) -> bool:
        """Audit trailing evidence without reopening a completed cycle.

        A terminal cycle is immutable with respect to committed fills and
        actions, but relevant evidence can still arrive after its processing
        boundary.  Such evidence is retained and can invalidate the
        authoritative result; it can never be replayed as a new fill.
        """

        # Identity is checked before the candidate predicate.  A previously
        # committed identity must still reject a conflicting payload even
        # after the quote has filled completely or the changed payload no
        # longer crosses the committed price.
        # Cross-attempt identity retention is for possible fills.  Book
        # revisions routinely repeat across independent fixture attempts and
        # must still be admitted to the new attempt's own book history.
        identity_key = (
            self._terminal_event_key(event)
            if event.kind is CausalEventKind.TRADE
            else None
        )
        signature = _event_signature(event)
        if identity_key is not None:
            previous = cycle.seen_events.get(identity_key)
            if previous is not None:
                if previous == signature:
                    cycle.duplicate_event_count += 1
                    cycle.ignored_event_count += 1
                    return True
                cycle.add_reason(CycleReason.DUPLICATE_CONFLICT)
                cycle.entry_decisions.append(
                    CausalEventDecision(
                        event.kind,
                        event.event_id,
                        event.ingress_received_monotonic_ns,
                        "CONFLICTING_DUPLICATE",
                        CycleReason.DUPLICATE_CONFLICT.value,
                    )
                )
                self._halt(cycle, CycleReason.DUPLICATE_CONFLICT)
                return False
        if event.venue not in {Venue.RISEX, Venue.LIGHTER} or event.canonical_market != cycle.quote_version.canonical_market:
            cycle.ignored_event_count += 1
            return False
        entry_candidate = self._entry_candidate_after_commit(cycle, event)
        exit_candidate = self._exit_candidate_after_commit(cycle, event)
        if not (entry_candidate or exit_candidate):
            # Terminal books, gaps, and non-crossing trades cannot affect a
            # committed result.  Do not retain an unbounded post-terminal
            # event log merely because the input stream continues.
            cycle.ignored_event_count += 1
            return False
        if identity_key is not None:
            cycle.seen_events[identity_key] = signature

        stream_key = (event.stream_key, event.kind)
        previous_time = cycle.last_stream_time.get(stream_key)
        position = _stream_position(event)
        previous_position = cycle.last_stream_position.get(stream_key)
        if previous_time is not None and event.causal_monotonic_ns < previous_time:
            cycle.add_reason(CycleReason.LATE_OLDER_EVENT)
        if previous_position is not None and position is not None and position < previous_position:
            cycle.add_reason(CycleReason.LATE_OLDER_EVENT)
        if previous_time is None or event.causal_monotonic_ns >= previous_time:
            cycle.last_stream_time[stream_key] = event.causal_monotonic_ns
        if position is not None:
            cycle.last_stream_position[stream_key] = position

        if entry_candidate:
            self._mark_late_terminal_trade(cycle, event, entry=True)
            return False
        if exit_candidate:
            self._mark_late_terminal_trade(cycle, event, entry=False)
            return False
        cycle.ignored_event_count += 1
        cycle.entry_decisions.append(
            CausalEventDecision(
                event.kind,
                event.event_id,
                event.ingress_received_monotonic_ns,
                "IGNORED",
                "TERMINAL_AFTER_COMMIT",
            )
        )
        return False

    def _propagate_terminal_uncertainty(
        self,
        lane: _KernelLane,
        source: _MutableCycle,
    ) -> None:
        """Halt the lane if an older unsealed cycle becomes uncertain."""

        latest = lane.terminal_cycles[-1]
        if latest is not source and not latest.unresolved:
            latest.add_reason(CycleReason.ENTRY_CAUSAL_UNCERTAINTY)
            latest.add_reason(CycleReason.LATE_OLDER_EVENT)
            if CausalUncertainty.LATE_OLDER_EVENT.value not in latest.entry_uncertainty:
                latest.entry_uncertainty.append(CausalUncertainty.LATE_OLDER_EVENT.value)
            latest.unresolved = True
            latest.phase = _Phase.UNRESOLVED
        lane.halted_unresolved = True
        lane.last_result = _result(latest, terminal=True)

    def _audit_terminal_cycles(
        self,
        lane: _KernelLane,
        event: CausalEvent,
    ) -> _TerminalAudit:
        """Audit all retained terminal intervals without retaining noise."""

        identity_key = (
            self._terminal_event_key(event)
            if event.kind is CausalEventKind.TRADE
            else None
        )
        matching = [
            cycle
            for cycle in lane.terminal_cycles
            if not cycle.unresolved
            and (
                (identity_key is not None and identity_key in cycle.seen_events)
                or self._entry_candidate_after_commit(cycle, event)
                or self._exit_candidate_after_commit(cycle, event)
            )
        ]
        if not matching:
            return _TerminalAudit(None)
        event_index: int | None = None
        invalidated: _MutableCycle | None = None
        duplicate = False
        for cycle in matching:
            cycle.event_count += 1
            event_index = cycle.event_count - 1
            was_unresolved = cycle.unresolved
            was_duplicate = self._accept_event(cycle, event)
            duplicate = duplicate or was_duplicate is True
            if cycle.unresolved and not was_unresolved:
                invalidated = cycle
        if invalidated is not None:
            self._propagate_terminal_uncertainty(lane, invalidated)
        else:
            # Keep the public terminal snapshot in sync with a benign
            # duplicate audit, without exposing the retained older attempt.
            lane.last_result = _result(lane.terminal_cycles[-1], terminal=True)
        return _TerminalAudit(
            event_index,
            invalidated=invalidated is not None,
            duplicate=duplicate and invalidated is None,
        )

    def _audit_previous_terminal(self, lane: _KernelLane, event: CausalEvent) -> _TerminalAudit:
        """Audit retained prior cycles before a new one acts."""

        return self._audit_terminal_cycles(lane, event)

    def _accept_event(self, cycle: _MutableCycle, event: CausalEvent) -> bool | None:
        if cycle.phase in {_Phase.COMPLETE, _Phase.ABORTED, _Phase.UNRESOLVED}:
            return self._accept_terminal_event(cycle, event)
        if event.venue not in {Venue.RISEX, Venue.LIGHTER} or event.canonical_market != cycle.quote_version.canonical_market:
            cycle.ignored_event_count += 1
            return
        key = event.stream_key
        identity_key = None if key is None or event.event_id is None else (key, event.event_id)
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
                cycle.entry_decisions.append(CausalEventDecision(event.kind, event.event_id, event.ingress_received_monotonic_ns, "CONFLICTING_DUPLICATE", CycleReason.DUPLICATE_CONFLICT.value))
                return
            cycle.seen_events[identity_key] = signature
        stream_key = (event.stream_key, event.kind)
        previous_time = cycle.last_stream_time.get(stream_key)
        position = _stream_position(event)
        previous_position = cycle.last_stream_position.get(stream_key)
        if previous_time is not None and event.causal_monotonic_ns < previous_time:
            cycle.add_reason(CycleReason.LATE_OLDER_EVENT)
            self._halt(cycle, CycleReason.LATE_OLDER_EVENT)
            return
        if previous_position is not None and position is not None and position < previous_position:
            cycle.add_reason(CycleReason.LATE_OLDER_EVENT)
            self._halt(cycle, CycleReason.LATE_OLDER_EVENT)
            return
        if previous_time is None or event.causal_monotonic_ns >= previous_time:
            cycle.last_stream_time[stream_key] = event.causal_monotonic_ns
        if position is not None:
            cycle.last_stream_position[stream_key] = position
        if event.kind is CausalEventKind.BOOK:
            self._record_book(cycle, event)
            ready = _processing_ready_ns(event)
            if ready is not None and ready > cycle.current_ns:
                self._run_due_until(cycle, ready)
            return
        if event.kind is CausalEventKind.DATA_GAP:
            gap = event.gap
            assert gap is not None
            cycle.gaps.append(gap)
            if self._gap_overlaps_live_cycle(cycle, gap):
                cycle.add_reason(CycleReason.REQUIRED_ACTION_DATA_GAP)
                self._halt(cycle, CycleReason.REQUIRED_ACTION_DATA_GAP)
            if event.causal_monotonic_ns > cycle.current_ns:
                self._run_due_until(cycle, event.causal_monotonic_ns)
            return
        # A trade's receipt boundary controls whether it is eligible.  Its
        # processing-ready boundary controls cancellation reactions.
        if event.causal_monotonic_ns > cycle.current_ns:
            self._run_due_until(cycle, event.causal_monotonic_ns)
        self._handle_trade(cycle, event)
        ready = _processing_ready_ns(event)
        if ready is None:
            if self._trade_could_matter(cycle, event):
                cycle.add_reason(CycleReason.EVENT_NOT_READY)
                self._halt(cycle, CycleReason.EVENT_NOT_READY)
            return
        if ready > cycle.current_ns:
            self._run_due_until(cycle, ready)

    def _record_book(self, cycle: _MutableCycle, event: CausalEvent, *, initial: bool = False) -> None:
        book = event.book
        assert book is not None
        key = book.book_revision_id
        signature = _book_signature(book)
        for existing in cycle.books:
            if existing.book.book_revision_id == key:
                if _book_signature(existing.book) != signature:
                    cycle.add_reason(CycleReason.REQUIRED_ACTION_AMBIGUOUS)
                    self._halt(cycle, CycleReason.REQUIRED_ACTION_AMBIGUOUS)
                else:
                    cycle.duplicate_event_count += 1
                    cycle.ignored_event_count += 1
                return
        cycle.books.append(
            _BookObservation(
                event=event,
                book=book,
                processing_ready_ns=_processing_ready_ns(event),
                arrival_index=len(cycle.books),
                identity_complete=event.source_identity_complete and event.identity_metadata_consistent,
            )
        )
        if not initial and not event.source_identity_complete and self._action_is_live(cycle):
            cycle.add_reason(CycleReason.REQUIRED_ACTION_AMBIGUOUS)

    def _trade_could_matter(self, cycle: _MutableCycle, event: CausalEvent) -> bool:
        return event.venue is Venue.RISEX and event.canonical_market == cycle.quote_version.canonical_market and cycle.phase in {
            _Phase.ENTRY_WAIT,
            _Phase.ENTRY_ACTIVE,
            _Phase.ENTRY_HEDGE_WAIT,
            _Phase.UNMATCHED_WAIT,
            _Phase.EXIT_WAIT,
            _Phase.EXIT_ACTIVE,
            _Phase.EXIT_CANCEL_WAIT,
        }

    def _gap_overlaps_live_cycle(self, cycle: _MutableCycle, gap: DataGapEvidence) -> bool:
        if gap.canonical_market != cycle.quote_version.canonical_market:
            return False
        if cycle.phase in {_Phase.ENTRY_WAIT, _Phase.ENTRY_ACTIVE, _Phase.ENTRY_HEDGE_WAIT, _Phase.UNMATCHED_WAIT}:
            start = cycle.entry_activation_ns
            pending_due = max(
                (
                    action.due_ns
                    for action in cycle.actions
                    if action.action_id in cycle.scheduled_takers
                    and action.status is CycleActionStatus.PENDING
                    and action.due_ns is not None
                ),
                default=cycle.current_ns,
            )
            end = max(
                cycle.current_ns,
                cycle.entry_cancel_effective_ns or cycle.current_ns,
                cycle.entry_hedge_due_ns or cycle.current_ns,
                pending_due,
            )
        elif cycle.phase in {_Phase.EXIT_WAIT, _Phase.EXIT_ACTIVE, _Phase.EXIT_CANCEL_WAIT, _Phase.CLOSE_WAIT, _Phase.FORCE_WAIT}:
            start = cycle.exit_activation_ns or cycle.current_ns
            end = max(cycle.current_ns, cycle.max_hold_deadline_ns or cycle.current_ns)
        else:
            return False
        return gap.overlaps(start, end)

    def _action_is_live(self, cycle: _MutableCycle) -> bool:
        return cycle.phase not in {_Phase.COMPLETE, _Phase.ABORTED, _Phase.UNRESOLVED}

    @staticmethod
    def _entry_candidate_after_commit(cycle: _MutableCycle, event: CausalEvent) -> bool:
        """Identify a possible entry fill after the entry state was committed.

        A trade's local receipt interval is immutable evidence.  It can arrive
        after a delayed processing boundary has already moved the lane into
        hedging or exit, but silently treating a quote-crossing trade as an
        irrelevant exit/late event would erase possible exposure.  The kernel
        therefore only identifies the candidate here; the committed state is
        halted below instead of being rewritten or replayed.
        """

        trade = event.trade
        if trade is None or event.venue is not Venue.RISEX:
            return False
        if trade.aggressor_side is not Side.BUY:
            return False
        cutoff = (
            cycle.entry_cancel_effective_ns
            if cycle.entry_cancel_effective_ns is not None
            else cycle.entry_cancel_requested_ns + cycle.delays.cancel_delay_ns
            if cycle.entry_cancel_requested_ns is not None
            else cycle.entry_cancel_schedule_ns + cycle.delays.cancel_delay_ns
        )
        if not cycle.entry_activation_ns <= event.causal_monotonic_ns < cutoff:
            return False
        if cycle.entry_remaining_quantity <= 0:
            return False
        crosses, ambiguous = _trade_crosses(cycle.entry_quote, trade)
        return crosses or ambiguous

    @staticmethod
    def _exit_candidate_after_commit(cycle: _MutableCycle, event: CausalEvent) -> bool:
        """Identify a possible exit fill after a close/force transition."""

        trade = event.trade
        quote = cycle.exit_quote
        if trade is None or quote is None or event.venue is not Venue.RISEX:
            return False
        if trade.aggressor_side is not Side.SELL:
            return False
        activation = quote.activation_monotonic_ns
        if activation is None:
            return False
        if cycle.exit_remaining_quantity <= 0:
            return False
        cutoff = (
            cycle.exit_cancel_effective_ns
            if cycle.exit_cancel_effective_ns is not None
            else cycle.exit_cancel_requested_ns + cycle.delays.cancel_delay_ns
            if cycle.exit_cancel_requested_ns is not None
            else 10**30
        )
        if not activation <= event.causal_monotonic_ns < cutoff:
            return False
        crosses, ambiguous = _trade_crosses(quote, trade)
        return crosses or ambiguous

    def _handle_trade(self, cycle: _MutableCycle, event: CausalEvent) -> None:
        trade = event.trade
        assert trade is not None
        if not event.source_identity_complete or not event.identity_metadata_consistent:
            if self._trade_could_matter(cycle, event):
                if cycle.phase in {_Phase.ENTRY_WAIT, _Phase.ENTRY_ACTIVE, _Phase.ENTRY_HEDGE_WAIT}:
                    cycle.add_reason(CycleReason.ENTRY_CAUSAL_UNCERTAINTY)
                    cycle.entry_uncertainty.append(CausalUncertainty.MISSING_SOURCE_IDENTITY.value)
                    self._halt(cycle, CycleReason.ENTRY_CAUSAL_UNCERTAINTY)
                else:
                    cycle.add_reason(CycleReason.EXIT_CAUSAL_UNCERTAINTY)
                    cycle.exit_uncertainty.append(CausalUncertainty.MISSING_SOURCE_IDENTITY.value)
                    self._halt(cycle, CycleReason.EXIT_CAUSAL_UNCERTAINTY)
            return
        identity = event.source_identity
        assert isinstance(identity, CausalSourceIdentity)
        if event.venue is not Venue.RISEX or event.canonical_market != cycle.quote_version.canonical_market:
            cycle.ignored_event_count += 1
            return
        if event.stream_session_id != cycle.quote_version.stream_session_id or event.recovery_generation != cycle.quote_version.recovery_generation:
            reason = CycleReason.ENTRY_CAUSAL_UNCERTAINTY if cycle.phase in {
                _Phase.ENTRY_WAIT,
                _Phase.ENTRY_ACTIVE,
                _Phase.ENTRY_HEDGE_WAIT,
            } else CycleReason.EXIT_CAUSAL_UNCERTAINTY
            cycle.add_reason(reason)
            if reason is CycleReason.ENTRY_CAUSAL_UNCERTAINTY:
                cycle.entry_uncertainty.append(CausalUncertainty.RECOVERY_TRANSITION.value)
            else:
                cycle.exit_uncertainty.append(CausalUncertainty.RECOVERY_TRANSITION.value)
            self._halt(cycle, reason)
            return
        ready = _processing_ready_ns(event)
        entry_phases = {
            _Phase.ENTRY_WAIT,
            _Phase.ENTRY_ACTIVE,
            _Phase.ENTRY_HEDGE_WAIT,
        }
        exit_phases = {_Phase.EXIT_ACTIVE, _Phase.EXIT_CANCEL_WAIT}
        if cycle.phase not in entry_phases and self._entry_candidate_after_commit(cycle, event):
            cycle.add_reason(CycleReason.ENTRY_CAUSAL_UNCERTAINTY)
            cycle.add_reason(CycleReason.LATE_OLDER_EVENT)
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
            return
        if cycle.phase not in exit_phases and self._exit_candidate_after_commit(cycle, event):
            cycle.add_reason(CycleReason.EXIT_CAUSAL_UNCERTAINTY)
            cycle.add_reason(CycleReason.LATE_OLDER_EVENT)
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
            return
        expected_aggressor = Side.BUY if cycle.phase in {
            _Phase.ENTRY_WAIT,
            _Phase.ENTRY_ACTIVE,
            _Phase.ENTRY_HEDGE_WAIT,
        } else Side.SELL
        if cycle.phase in {_Phase.ENTRY_WAIT, _Phase.ENTRY_ACTIVE, _Phase.ENTRY_HEDGE_WAIT}:
            if ready is None:
                cycle.add_reason(CycleReason.EVENT_NOT_READY)
                cycle.entry_uncertainty.append(CausalUncertainty.MISSING_CAUSAL_TIMING.value)
                self._halt(cycle, CycleReason.EVENT_NOT_READY)
                return
            entry_cutoff = (
                cycle.entry_cancel_effective_ns
                if cycle.entry_cancel_effective_ns is not None
                else cycle.entry_cancel_requested_ns + cycle.delays.cancel_delay_ns
                if cycle.entry_cancel_requested_ns is not None
                else cycle.entry_cancel_schedule_ns + cycle.delays.cancel_delay_ns
            )
            if event.causal_monotonic_ns < cycle.entry_activation_ns or event.causal_monotonic_ns >= entry_cutoff:
                cycle.ignored_event_count += 1
                cycle.entry_decisions.append(CausalEventDecision(event.kind, event.event_id, event.ingress_received_monotonic_ns, "IGNORED", "ENTRY_BOUNDARY"))
                return
            if trade.aggressor_side is not expected_aggressor:
                cycle.ignored_event_count += 1
                cycle.entry_decisions.append(CausalEventDecision(event.kind, event.event_id, event.ingress_received_monotonic_ns, "IGNORED", "WRONG_AGGRESSOR_SIDE"))
                return
            crosses, ambiguous = _trade_crosses(cycle.entry_quote, trade)
            if ambiguous:
                cycle.add_reason(CycleReason.ENTRY_CAUSAL_UNCERTAINTY)
                cycle.entry_uncertainty.append(CausalUncertainty.QUOTE_TOUCH_ORDER_UNPROVEN.value)
                self._halt(cycle, CycleReason.ENTRY_CAUSAL_UNCERTAINTY)
                cycle.entry_decisions.append(CausalEventDecision(event.kind, event.event_id, event.ingress_received_monotonic_ns, "UNCERTAIN", "QUOTE_TOUCH_ORDER_UNPROVEN"))
                return
            if not crosses or cycle.entry_remaining_quantity <= 0:
                cycle.ignored_event_count += 1
                cycle.entry_decisions.append(CausalEventDecision(event.kind, event.event_id, event.ingress_received_monotonic_ns, "IGNORED", "NOT_TRADE_THROUGH_QUOTE_PRICE" if not crosses else "QUOTE_QUANTITY_EXHAUSTED"))
                return
            block_fence = None
            for observation in cycle.books:
                if observation.book.venue is Venue.RISEX and observation.book.book_revision_id == cycle.quote_version.risex_book_revision_id:
                    block_fence = observation.book.block_number
                    break
            if block_fence is not None and (event.block_number is None or event.block_number <= block_fence):
                cycle.add_reason(CycleReason.ENTRY_CAUSAL_UNCERTAINTY)
                cycle.entry_uncertainty.append(CausalUncertainty.WATERMARK_BOUNDARY_AMBIGUOUS.value)
                self._halt(cycle, CycleReason.ENTRY_CAUSAL_UNCERTAINTY)
                return
            consumed = min(trade.canonical_quantity, cycle.entry_remaining_quantity)
            cycle.entry_observed_quantity += consumed
            cycle.entry_remaining_quantity -= consumed
            causal_fill = CausalFill(
                source_event_id=identity.source_event_id,  # type: ignore[arg-type]
                source_identity=identity,
                received_monotonic_ns=event.causal_monotonic_ns,
                price=cycle.entry_quote.price,
                observed_quantity=trade.canonical_quantity,
                consumed_quantity=consumed,
                remaining_quantity=cycle.entry_remaining_quantity,
                observed_trade_price=trade.canonical_price,
                processed_ready_monotonic_ns=ready,
            )
            cycle.entry_fills.append(causal_fill)
            cycle.entry_decisions.append(CausalEventDecision(event.kind, event.event_id, event.ingress_received_monotonic_ns, "FILL", "ELIGIBLE_TRADE", consumed))
            entry_action = _add_action(
                cycle,
                action_id="entry-maker",
                kind=CycleActionKind.ENTRY_MAKER,
                status=CycleActionStatus.PENDING,
                requested_ns=cycle.quote_version.decision_ready_monotonic_ns or event.causal_monotonic_ns,
                effective_ns=cycle.entry_activation_ns,
                due_ns=cycle.entry_cancel_schedule_ns,
                quantity=cycle.entry_target_quantity,
                reason="ENTRY_RISEX_MAKER_QUOTE",
            )
            entry_action.executed_quantity = cycle.entry_observed_quantity
            entry_action.reason = "ENTRY_MAKER_PARTIAL"
            if cycle.entry_cancel_requested_ns is not None:
                cancel = _action(cycle, "entry-cancel")
                if cancel.status is CycleActionStatus.PENDING:
                    cancel.requested_quantity = cycle.entry_remaining_quantity
                elif cancel.status is CycleActionStatus.COMPLETED and cycle.entry_cancel_effective_ns is not None:
                    # A fill received before the effective boundary may be
                    # processed later.  Retain the fill and reconcile the
                    # already-recorded cancellation to the exact remaining
                    # maker quantity instead of silently losing the event.
                    cancel.requested_quantity = cycle.entry_remaining_quantity
                    cancel.executed_quantity = cycle.entry_remaining_quantity
            _append_fill(
                cycle,
                action_id="entry-maker",
                venue=Venue.RISEX,
                side=Side.SELL,
                role=LiquidityRole.MAKER,
                quantity=consumed,
                price=cycle.entry_quote.price,
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
            if cycle.entry_remaining_quantity > 0 and cycle.entry_cancel_requested_ns is None:
                cycle.entry_cancel_requested_ns = ready
                _add_action(
                    cycle,
                    action_id="entry-cancel",
                    kind=CycleActionKind.ENTRY_CANCEL,
                    status=CycleActionStatus.PENDING,
                    requested_ns=ready,
                    effective_ns=ready + cycle.delays.cancel_delay_ns,
                    due_ns=ready + cycle.delays.cancel_delay_ns,
                    quantity=cycle.entry_remaining_quantity,
                    reason="ENTRY_CANCEL_ON_FIRST_PARTIAL",
                )
            if cycle.entry_remaining_quantity == 0:
                entry_action.status = CycleActionStatus.COMPLETED
                entry_action.executed_quantity = cycle.entry_target_quantity
                entry_action.reason = "ENTRY_MAKER_FILLED"
                if cycle.entry_cancel_requested_ns is not None:
                    cancel = _add_action(
                        cycle,
                        action_id="entry-cancel",
                        kind=CycleActionKind.ENTRY_CANCEL,
                        status=CycleActionStatus.NOT_REQUIRED,
                        requested_ns=cycle.entry_cancel_requested_ns,
                        effective_ns=ready,
                        due_ns=ready,
                        quantity=_ZERO,
                        reason="ENTRY_FULLY_FILLED_DURING_CANCEL_WINDOW",
                    )
                    cancel.requested_quantity = _ZERO
                    _set_action_result(cycle, cancel, status=CycleActionStatus.NOT_REQUIRED, executed=_ZERO, reason="ENTRY_FULLY_FILLED_DURING_CANCEL_WINDOW")
                    cancel.effective_ns = ready
                    cancel.due_ns = ready
                else:
                    _add_action(
                        cycle,
                        action_id="entry-cancel",
                        kind=CycleActionKind.ENTRY_CANCEL,
                        status=CycleActionStatus.NOT_REQUIRED,
                        requested_ns=ready,
                        effective_ns=ready,
                        due_ns=ready,
                        quantity=_ZERO,
                        reason="ENTRY_FULLY_FILLED",
                    )
                cycle.entry_hedge_due_ns = max(ready, cycle.current_ns) + cycle.delays.taker_delay_ns
                cycle.phase = _Phase.ENTRY_HEDGE_WAIT
            else:
                cycle.phase = _Phase.ENTRY_HEDGE_WAIT if cycle.entry_hedge_due_ns is not None else _Phase.ENTRY_ACTIVE
            return
        if cycle.exit_quote is None or cycle.phase not in {_Phase.EXIT_ACTIVE, _Phase.EXIT_CANCEL_WAIT}:
            cycle.ignored_event_count += 1
            return
        quote = cycle.exit_quote
        if ready is None:
            cycle.add_reason(CycleReason.EXIT_CAUSAL_UNCERTAINTY)
            cycle.exit_uncertainty.append(CausalUncertainty.MISSING_CAUSAL_TIMING.value)
            cycle.exit_decisions.append(CausalEventDecision(event.kind, event.event_id, event.ingress_received_monotonic_ns, "UNCERTAIN", CycleReason.EVENT_NOT_READY.value))
            self._halt(cycle, CycleReason.EXIT_CAUSAL_UNCERTAINTY)
            return
        if event.causal_monotonic_ns < quote.activation_monotonic_ns or event.causal_monotonic_ns >= (cycle.exit_cancel_effective_ns or 10**30):
            cycle.ignored_event_count += 1
            cycle.exit_decisions.append(CausalEventDecision(event.kind, event.event_id, event.ingress_received_monotonic_ns, "IGNORED", "EXIT_BOUNDARY"))
            return
        if trade.aggressor_side is not expected_aggressor:
            cycle.ignored_event_count += 1
            cycle.exit_decisions.append(CausalEventDecision(event.kind, event.event_id, event.ingress_received_monotonic_ns, "IGNORED", "WRONG_AGGRESSOR_SIDE"))
            return
        crosses, ambiguous = _trade_crosses(quote, trade)
        if ambiguous:
            cycle.add_reason(CycleReason.EXIT_CAUSAL_UNCERTAINTY)
            cycle.exit_uncertainty.append(CausalUncertainty.QUOTE_TOUCH_ORDER_UNPROVEN.value)
            cycle.exit_decisions.append(CausalEventDecision(event.kind, event.event_id, event.ingress_received_monotonic_ns, "UNCERTAIN", "QUOTE_TOUCH_ORDER_UNPROVEN"))
            self._halt(cycle, CycleReason.EXIT_CAUSAL_UNCERTAINTY)
            return
        if not crosses or cycle.exit_remaining_quantity <= 0:
            cycle.ignored_event_count += 1
            cycle.exit_decisions.append(CausalEventDecision(event.kind, event.event_id, event.ingress_received_monotonic_ns, "IGNORED", "NOT_TRADE_THROUGH_QUOTE_PRICE" if not crosses else "QUOTE_QUANTITY_EXHAUSTED"))
            return
        consumed = min(trade.canonical_quantity, cycle.exit_remaining_quantity, cycle.paired_risex_quantity)
        if consumed <= 0:
            cycle.add_reason(CycleReason.OVER_CLOSE_BLOCKED)
            cycle.exit_decisions.append(CausalEventDecision(event.kind, event.event_id, event.ingress_received_monotonic_ns, "IGNORED", CycleReason.OVER_CLOSE_BLOCKED.value))
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
        cycle.exit_decisions.append(CausalEventDecision(event.kind, event.event_id, event.ingress_received_monotonic_ns, "FILL", "ELIGIBLE_TRADE", consumed))
        exit_action = _action(cycle, "exit-maker")
        exit_action.executed_quantity = cycle.exit_quote.quantity - cycle.exit_remaining_quantity
        exit_action.reason = "EXIT_MAKER_PARTIAL"
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
        close_id = f"exit-close:{len([a for a in cycle.actions if a.kind is CycleActionKind.EXIT_HEDGE_CLOSE])}"
        _schedule_taker(
            cycle,
            action_id=close_id,
            kind=CycleActionKind.EXIT_HEDGE_CLOSE,
            venue=Venue.LIGHTER,
            side=Side.SELL,
            quantity=consumed,
            requested_ns=ready,
            reason="EXIT_LIGHTER_TAKER_CLOSE",
        )
        if cycle.exit_remaining_quantity > 0 and cycle.exit_cancel_requested_ns is None:
            cycle.exit_cancel_requested_ns = ready
            _add_action(
                cycle,
                action_id="exit-cancel",
                kind=CycleActionKind.EXIT_CANCEL,
                status=CycleActionStatus.PENDING,
                requested_ns=ready,
                effective_ns=ready + cycle.delays.cancel_delay_ns,
                due_ns=ready + cycle.delays.cancel_delay_ns,
                quantity=cycle.exit_remaining_quantity,
                reason="EXIT_CANCEL_ON_FIRST_PARTIAL",
            )
            cycle.phase = _Phase.EXIT_CANCEL_WAIT
        elif cycle.exit_remaining_quantity == 0:
            exit_action = _action(cycle, "exit-maker")
            _set_action_result(cycle, exit_action, status=CycleActionStatus.COMPLETED, executed=cycle.exit_quote.quantity, reason="EXIT_MAKER_FILLED", evidence_id=identity.source_event_id)
            if cycle.exit_cancel_requested_ns is not None:
                cancel = _add_action(
                    cycle,
                    action_id="exit-cancel",
                    kind=CycleActionKind.EXIT_CANCEL,
                    status=CycleActionStatus.NOT_REQUIRED,
                    requested_ns=cycle.exit_cancel_requested_ns,
                    effective_ns=ready,
                    due_ns=ready,
                    quantity=_ZERO,
                    reason="EXIT_FULLY_FILLED_DURING_CANCEL_WINDOW",
                )
                cancel.requested_quantity = _ZERO
                _set_action_result(
                    cycle,
                    cancel,
                    status=CycleActionStatus.NOT_REQUIRED,
                    executed=_ZERO,
                    reason="EXIT_FULLY_FILLED_DURING_CANCEL_WINDOW",
                )
                cancel.effective_ns = ready
                cancel.due_ns = ready
            cycle.phase = _Phase.CLOSE_WAIT

    def _run_due_until(self, cycle: _MutableCycle, target_ns: int) -> None:
        if target_ns < cycle.current_ns:
            cycle.add_reason(CycleReason.LATE_OLDER_EVENT)
            self._halt(cycle, CycleReason.LATE_OLDER_EVENT)
            return
        while cycle.phase not in {_Phase.COMPLETE, _Phase.ABORTED, _Phase.UNRESOLVED}:
            due = self._next_due(cycle)
            if due is None or due > target_ns:
                break
            if due < cycle.current_ns:
                due = cycle.current_ns
            cycle.current_ns = due
            self._handle_boundary(cycle, due)
        cycle.current_ns = max(cycle.current_ns, target_ns)

    def _next_due(self, cycle: _MutableCycle) -> int | None:
        if cycle.phase is _Phase.ENTRY_WAIT:
            return cycle.entry_activation_ns
        if cycle.phase is _Phase.ENTRY_ACTIVE:
            if cycle.entry_cancel_requested_ns is None:
                return cycle.entry_cancel_schedule_ns
            if cycle.entry_cancel_effective_ns is None:
                return cycle.entry_cancel_requested_ns + cycle.delays.cancel_delay_ns
            return cycle.entry_cancel_effective_ns + cycle.delays.taker_delay_ns
        if cycle.phase is _Phase.ENTRY_HEDGE_WAIT:
            return cycle.entry_hedge_due_ns
        if cycle.phase is _Phase.UNMATCHED_WAIT:
            return min(
                (action.due_ns for action in cycle.actions if action.action_id in cycle.scheduled_takers and action.status is CycleActionStatus.PENDING and action.due_ns is not None),
                default=None,
            )
        if cycle.phase is _Phase.EXIT_WAIT:
            if cycle.exit_activation_ns is not None:
                return cycle.exit_activation_ns
            return None
        if cycle.phase is _Phase.EXIT_ACTIVE:
            candidates = [cycle.max_hold_deadline_ns]
            if cycle.exit_cancel_requested_ns is not None:
                candidates.append(cycle.exit_cancel_requested_ns + cycle.delays.cancel_delay_ns)
            return min(value for value in candidates if value is not None)
        if cycle.phase is _Phase.EXIT_CANCEL_WAIT:
            pending = [
                action.due_ns
                for action in cycle.actions
                if action.action_id in cycle.scheduled_takers
                and action.status is CycleActionStatus.PENDING
                and action.due_ns is not None
            ]
            cancel_due = (
                cycle.exit_cancel_effective_ns
                if cycle.exit_cancel_effective_ns is not None
                else cycle.exit_cancel_requested_ns + cycle.delays.cancel_delay_ns
                if cycle.exit_cancel_requested_ns is not None
                else None
            )
            if cancel_due is None:
                return min(pending, default=None)
            return min((*pending, cancel_due), default=cancel_due)
        if cycle.phase is _Phase.CLOSE_WAIT:
            pending = [action.due_ns for action in cycle.actions if action.action_id in cycle.scheduled_takers and action.status is CycleActionStatus.PENDING and action.due_ns is not None]
            if pending:
                return min(pending)
            if cycle.paired_risex_quantity == 0 and cycle.paired_lighter_quantity == 0 and cycle.unmatched_entry_quantity == 0:
                return cycle.current_ns
            if cycle.exit_remaining_quantity > 0 and cycle.exit_cancel_effective_ns is None:
                return cycle.max_hold_deadline_ns
            return cycle.current_ns + cycle.delays.taker_delay_ns
        if cycle.phase is _Phase.FORCE_WAIT:
            pending = [action.due_ns for action in cycle.actions if action.action_id in cycle.scheduled_takers and action.status is CycleActionStatus.PENDING and action.due_ns is not None]
            return min(pending, default=None)
        return None

    def _handle_boundary(self, cycle: _MutableCycle, at_ns: int) -> None:
        if cycle.phase is _Phase.ENTRY_WAIT:
            cycle.phase = _Phase.ENTRY_ACTIVE
            return
        if cycle.phase is _Phase.ENTRY_ACTIVE:
            if cycle.entry_cancel_requested_ns is None:
                cycle.entry_cancel_requested_ns = at_ns
                cycle.add_reason(CycleReason.NO_ENTRY if cycle.entry_observed_quantity == 0 else CycleReason.HEDGE_PARTIAL)
                _add_action(
                    cycle,
                    action_id="entry-cancel",
                    kind=CycleActionKind.ENTRY_CANCEL,
                    status=CycleActionStatus.PENDING,
                    requested_ns=at_ns,
                    effective_ns=at_ns + cycle.delays.cancel_delay_ns,
                    due_ns=at_ns + cycle.delays.cancel_delay_ns,
                    quantity=cycle.entry_remaining_quantity,
                    reason="ENTRY_CANCEL_REQUESTED",
                )
                return
            if cycle.entry_cancel_effective_ns is None:
                cycle.entry_cancel_effective_ns = at_ns
                maker = _action(cycle, "entry-maker")
                action = _action(cycle, "entry-cancel")
                if cycle.entry_observed_quantity == 0:
                    _set_action_result(cycle, maker, status=CycleActionStatus.COMPLETED, executed=_ZERO, reason="ENTRY_MAKER_CANCELLED")
                    action.requested_quantity = _ZERO
                    _set_action_result(cycle, action, status=CycleActionStatus.COMPLETED, executed=_ZERO, reason="ENTRY_CANCEL_EFFECTIVE")
                    cycle.add_reason(CycleReason.NO_ENTRY)
                    cycle.phase = _Phase.ABORTED
                    cycle.terminal_ns = at_ns
                    return
                maker.status = CycleActionStatus.COMPLETED
                maker.executed_quantity = cycle.entry_observed_quantity
                maker.reason = "ENTRY_MAKER_CANCELLED_PARTIAL"
                action.requested_quantity = cycle.entry_remaining_quantity
                _set_action_result(cycle, action, status=CycleActionStatus.COMPLETED, executed=cycle.entry_remaining_quantity, reason="ENTRY_CANCEL_EFFECTIVE")
                cycle.entry_hedge_due_ns = at_ns + cycle.delays.taker_delay_ns
                cycle.phase = _Phase.ENTRY_HEDGE_WAIT
                return
            if cycle.entry_hedge_due_ns is None:
                cycle.entry_hedge_due_ns = at_ns + cycle.delays.taker_delay_ns
            cycle.phase = _Phase.ENTRY_HEDGE_WAIT
            return
        if cycle.phase is _Phase.ENTRY_HEDGE_WAIT:
            if cycle.entry_hedge_action_id is None:
                cycle.entry_hedge_action_id = "entry-hedge"
                _schedule_taker(
                    cycle,
                    action_id="entry-hedge",
                    kind=CycleActionKind.ENTRY_HEDGE,
                    venue=Venue.LIGHTER,
                    side=Side.BUY,
                    quantity=cycle.entry_observed_quantity,
                    requested_ns=at_ns - cycle.delays.taker_delay_ns,
                    reason="ENTRY_LIGHTER_TAKER_HEDGE",
                )
            action = _action(cycle, "entry-hedge")
            if action.status is CycleActionStatus.PENDING:
                self._execute_taker(cycle, action, at_ns)
            if cycle.unresolved:
                return
            cycle.entry_hedge_done = True
            if cycle.unmatched_entry_quantity > 0:
                cycle.unmatched_started_ns = at_ns
                cycle.forced_used = True
                cycle.add_reason(CycleReason.FORCED_UNWIND)
                cycle.phase = _Phase.UNMATCHED_WAIT
                _schedule_taker(
                    cycle,
                    action_id="unmatched-risex",
                    kind=CycleActionKind.UNMATCHED_RISEX_UNWIND,
                    venue=Venue.RISEX,
                    side=Side.BUY,
                    quantity=cycle.unmatched_entry_quantity,
                    requested_ns=at_ns,
                    reason="UNMATCHED_RISEX_TAKER_UNWIND",
                )
                return
            self._prepare_exit(cycle, at_ns)
            return
        if cycle.phase is _Phase.UNMATCHED_WAIT:
            action = next((item for item in cycle.actions if item.action_id == "unmatched-risex"), None)
            if action is None or action.status is CycleActionStatus.PENDING:
                if action is not None:
                    self._execute_taker(cycle, action, at_ns)
            if cycle.unresolved:
                return
            if cycle.unmatched_entry_quantity > 0:
                cycle.add_reason(CycleReason.INSUFFICIENT_DEPTH)
                self._halt(cycle, CycleReason.INSUFFICIENT_DEPTH)
                return
            cycle.unmatched_resolved_ns = at_ns
            self._prepare_exit(cycle, at_ns)
            return
        if cycle.phase is _Phase.EXIT_WAIT:
            cycle.phase = _Phase.EXIT_ACTIVE
            return
        if cycle.phase is _Phase.EXIT_ACTIVE:
            if cycle.max_hold_deadline_ns is not None and at_ns >= cycle.max_hold_deadline_ns and cycle.exit_cancel_requested_ns is None:
                cycle.add_reason(CycleReason.MAX_HOLD)
                cycle.forced_used = True
                cycle.exit_cancel_requested_ns = at_ns
                cycle.exit_cancel_effective_ns = at_ns + cycle.delays.cancel_delay_ns
                action = _action(cycle, "exit-maker")
                _set_action_result(cycle, action, status=CycleActionStatus.COMPLETED, executed=cycle.exit_quote.quantity - cycle.exit_remaining_quantity if cycle.exit_quote else _ZERO, reason="MAX_HOLD_CANCEL_REQUESTED")
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
                cycle.phase = _Phase.EXIT_CANCEL_WAIT
                return
            if cycle.exit_cancel_requested_ns is not None and cycle.exit_cancel_effective_ns is None:
                cycle.exit_cancel_effective_ns = at_ns
                action = _action(cycle, "exit-cancel")
                _set_action_result(cycle, action, status=CycleActionStatus.COMPLETED, executed=_ZERO, reason="EXIT_CANCEL_EFFECTIVE")
                cycle.phase = _Phase.EXIT_CANCEL_WAIT
                self._schedule_forced_remaining(cycle, at_ns)
                return
            if cycle.max_hold_deadline_ns is not None and at_ns >= cycle.max_hold_deadline_ns:
                cycle.add_reason(CycleReason.MAX_HOLD)
                self._schedule_forced_remaining(cycle, at_ns)
            return
        if cycle.phase is _Phase.EXIT_CANCEL_WAIT:
            for action in tuple(cycle.actions):
                if action.action_id in cycle.scheduled_takers and action.status is CycleActionStatus.PENDING and action.due_ns == at_ns:
                    self._execute_taker(cycle, action, at_ns)
            if cycle.unresolved:
                return
            effective_target = (
                cycle.exit_cancel_effective_ns
                if cycle.exit_cancel_effective_ns is not None
                else cycle.exit_cancel_requested_ns + cycle.delays.cancel_delay_ns
                if cycle.exit_cancel_requested_ns is not None
                else at_ns
            )
            if at_ns < effective_target:
                return
            cycle.exit_cancel_effective_ns = effective_target
            action = _action(cycle, "exit-cancel")
            action.requested_quantity = cycle.exit_remaining_quantity
            _set_action_result(cycle, action, status=CycleActionStatus.COMPLETED, executed=cycle.exit_remaining_quantity, reason="EXIT_CANCEL_EFFECTIVE")
            maker = _action(cycle, "exit-maker")
            maker.status = CycleActionStatus.COMPLETED
            maker.executed_quantity = cycle.exit_quote.quantity - cycle.exit_remaining_quantity if cycle.exit_quote is not None else _ZERO
            maker.reason = "EXIT_MAKER_CANCELLED_PARTIAL"
            self._schedule_forced_remaining(cycle, effective_target)
            return
        if cycle.phase is _Phase.CLOSE_WAIT:
            for action in tuple(cycle.actions):
                if action.action_id in cycle.scheduled_takers and action.status is CycleActionStatus.PENDING and action.due_ns == at_ns:
                    self._execute_taker(cycle, action, at_ns)
            if cycle.unresolved:
                return
            if cycle.paired_risex_quantity == 0 and cycle.paired_lighter_quantity == 0 and cycle.unmatched_entry_quantity == 0 and not any(item.status is CycleActionStatus.PENDING for item in cycle.actions):
                cycle.phase = _Phase.COMPLETE
                cycle.terminal_ns = at_ns
                return
            if any(
                action.action_id in cycle.scheduled_takers
                and action.status is CycleActionStatus.PENDING
                for action in cycle.actions
            ):
                # A later queued close may still account for part of the
                # paired position.  Do not schedule a forced action against
                # the pre-close quantity or manufacture an over-close.
                return
            if cycle.exit_remaining_quantity > 0:
                if cycle.exit_cancel_requested_ns is None and cycle.max_hold_deadline_ns is not None and at_ns >= cycle.max_hold_deadline_ns:
                    cycle.forced_used = True
                    cycle.add_reason(CycleReason.MAX_HOLD)
                    cycle.exit_cancel_requested_ns = at_ns
                    cycle.exit_cancel_effective_ns = at_ns + cycle.delays.cancel_delay_ns
                    _add_action(cycle, action_id="exit-cancel", kind=CycleActionKind.EXIT_CANCEL, status=CycleActionStatus.PENDING, requested_ns=at_ns, effective_ns=cycle.exit_cancel_effective_ns, due_ns=cycle.exit_cancel_effective_ns, quantity=cycle.exit_remaining_quantity, reason="MAX_HOLD_CANCEL_REQUESTED")
                    cycle.phase = _Phase.EXIT_CANCEL_WAIT
                    return
            self._schedule_forced_remaining(cycle, at_ns)
            return
        if cycle.phase is _Phase.FORCE_WAIT:
            for action in tuple(cycle.actions):
                if action.action_id in cycle.scheduled_takers and action.status is CycleActionStatus.PENDING and action.due_ns == at_ns:
                    self._execute_taker(cycle, action, at_ns)
            if cycle.unresolved:
                return
            if cycle.paired_risex_quantity == 0 and cycle.paired_lighter_quantity == 0 and cycle.unmatched_entry_quantity == 0 and not any(item.status is CycleActionStatus.PENDING for item in cycle.actions):
                cycle.phase = _Phase.COMPLETE
                cycle.terminal_ns = at_ns
            else:
                self._halt(cycle, CycleReason.TERMINAL_NON_FLAT)

    def _execute_taker(self, cycle: _MutableCycle, action: _MutableAction, at_ns: int) -> None:
        descriptor = cycle.scheduled_takers.get(action.action_id)
        if descriptor is None:
            return
        venue, side, requested, reason = descriptor
        position_cap: Decimal | None = None
        if action.kind is CycleActionKind.UNMATCHED_RISEX_UNWIND:
            position_cap = cycle.unmatched_entry_quantity
        elif action.kind is CycleActionKind.EXIT_HEDGE_CLOSE:
            position_cap = cycle.paired_lighter_quantity
        elif action.kind is CycleActionKind.FORCED_RISEX_UNWIND:
            position_cap = cycle.paired_risex_quantity
        elif action.kind is CycleActionKind.FORCED_LIGHTER_UNWIND:
            position_cap = cycle.paired_lighter_quantity
        if position_cap is not None:
            if position_cap <= 0:
                cycle.add_reason(CycleReason.OVER_CLOSE_BLOCKED)
                _set_action_result(
                    cycle,
                    action,
                    status=CycleActionStatus.NOT_REQUIRED,
                    executed=_ZERO,
                    reason=CycleReason.OVER_CLOSE_BLOCKED.value,
                )
                return
            if position_cap < requested:
                # Another queued action has already accounted for part of
                # this close.  Retain the original action identity but shrink
                # its still-live quantity to the exact remaining exposure.
                cycle.add_reason(CycleReason.OVER_CLOSE_BLOCKED)
                requested = position_cap
                action.requested_quantity = position_cap
        paired_required = venue is Venue.LIGHTER or (
            action.kind in {CycleActionKind.FORCED_LIGHTER_UNWIND, CycleActionKind.FORCED_RISEX_UNWIND}
            and cycle.paired_risex_quantity > 0
            and cycle.paired_lighter_quantity > 0
        )
        if paired_required:
            risex_book, lighter_book, pair_reason = _paired_books(cycle, at_ns)
            book = lighter_book if venue is Venue.LIGHTER else risex_book
            if pair_reason is not None or book is None:
                action.status = CycleActionStatus.UNRESOLVED
                action.reason = pair_reason.value if isinstance(pair_reason, CycleReason) else CycleReason.REQUIRED_ACTION_DATA_MISSING.value
                cycle.add_reason(pair_reason or CycleReason.REQUIRED_ACTION_DATA_MISSING)
                self._halt(cycle, pair_reason or CycleReason.REQUIRED_ACTION_DATA_MISSING)
                return
        else:
            book, book_reason = _select_book(cycle, venue, at_ns)
            if book_reason is not None or book is None:
                action.status = CycleActionStatus.UNRESOLVED
                action.reason = book_reason.value if isinstance(book_reason, CycleReason) else CycleReason.REQUIRED_ACTION_DATA_MISSING.value
                cycle.add_reason(book_reason or CycleReason.REQUIRED_ACTION_DATA_MISSING)
                self._halt(cycle, book_reason or CycleReason.REQUIRED_ACTION_DATA_MISSING)
                return
        assert book is not None
        levels = book.asks if side is Side.BUY else book.bids
        available = sum((level.canonical_quantity for level in levels if level.canonical_quantity > 0), _ZERO)
        if available <= 0:
            _set_action_result(cycle, action, status=CycleActionStatus.COMPLETED, executed=_ZERO, reason=CycleReason.INSUFFICIENT_DEPTH.value)
            cycle.add_reason(CycleReason.INSUFFICIENT_DEPTH)
            if action.kind is CycleActionKind.ENTRY_HEDGE:
                _record_entry_hedge(cycle, _ZERO)
            self._apply_taker_residue(cycle, action, _ZERO, at_ns)
            return
        step = _common_step(cycle)
        if step is None:
            action.status = CycleActionStatus.UNRESOLVED
            action.reason = CycleReason.REQUIRED_ACTION_DATA_MISSING.value
            self._halt(cycle, CycleReason.REQUIRED_ACTION_DATA_MISSING)
            return
        executable = _floor_quantity(min(requested, available), step)
        if executable <= 0:
            _set_action_result(cycle, action, status=CycleActionStatus.COMPLETED, executed=_ZERO, reason=CycleReason.GRID_RESIDUE.value)
            cycle.add_reason(CycleReason.GRID_RESIDUE)
            if action.kind is CycleActionKind.ENTRY_HEDGE:
                _record_entry_hedge(cycle, _ZERO)
            self._apply_taker_residue(cycle, action, _ZERO, at_ns)
            return
        vwap: ExactVwap = exact_quantity_vwap(side, executable, tuple(book.bids), tuple(book.asks))
        if not vwap.is_executable or vwap.price is None:
            _set_action_result(cycle, action, status=CycleActionStatus.COMPLETED, executed=_ZERO, reason=CycleReason.INSUFFICIENT_DEPTH.value)
            cycle.add_reason(CycleReason.INSUFFICIENT_DEPTH)
            if action.kind is CycleActionKind.ENTRY_HEDGE:
                _record_entry_hedge(cycle, _ZERO)
            self._apply_taker_residue(cycle, action, _ZERO, at_ns)
            return
        if not _minimum_ok(cycle, venue, executable, vwap.price):
            _set_action_result(cycle, action, status=CycleActionStatus.COMPLETED, executed=_ZERO, reason=CycleReason.MINIMUM_RESIDUE.value)
            cycle.add_reason(CycleReason.MINIMUM_RESIDUE)
            if action.kind is CycleActionKind.ENTRY_HEDGE:
                _record_entry_hedge(cycle, _ZERO)
            self._apply_taker_residue(cycle, action, _ZERO, at_ns)
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
        )
        action_reason = CycleReason.HEDGE_PARTIAL.value if executable < requested else reason
        _set_action_result(cycle, action, status=CycleActionStatus.COMPLETED, executed=executable, reason=action_reason, evidence_id=book.book_revision_id)
        if executable < requested:
            cycle.add_reason(CycleReason.HEDGE_PARTIAL)
            cycle.add_reason(CycleReason.INSUFFICIENT_DEPTH)
        if action.kind is CycleActionKind.ENTRY_HEDGE:
            _record_entry_hedge(cycle, executable)
        self._apply_taker_residue(cycle, action, executable, at_ns)
        if action.kind is CycleActionKind.UNMATCHED_RISEX_UNWIND:
            cycle.unmatched_entry_quantity -= executable
            if cycle.unmatched_entry_quantity < 0:
                cycle.add_reason(CycleReason.OVER_CLOSE_BLOCKED)
                cycle.unmatched_entry_quantity = _ZERO
        elif action.kind is CycleActionKind.EXIT_HEDGE_CLOSE:
            cycle.paired_lighter_quantity -= executable
            if cycle.paired_lighter_quantity < 0:
                cycle.add_reason(CycleReason.OVER_CLOSE_BLOCKED)
                cycle.paired_lighter_quantity = _ZERO
        elif action.kind is CycleActionKind.FORCED_RISEX_UNWIND:
            if cycle.unmatched_entry_quantity > 0:
                cycle.unmatched_entry_quantity -= executable
            else:
                cycle.paired_risex_quantity -= executable
            if cycle.unmatched_entry_quantity < 0 or cycle.paired_risex_quantity < 0:
                cycle.add_reason(CycleReason.OVER_CLOSE_BLOCKED)
                cycle.unmatched_entry_quantity = max(_ZERO, cycle.unmatched_entry_quantity)
                cycle.paired_risex_quantity = max(_ZERO, cycle.paired_risex_quantity)
        elif action.kind is CycleActionKind.FORCED_LIGHTER_UNWIND:
            cycle.paired_lighter_quantity -= executable
            if cycle.paired_lighter_quantity < 0:
                cycle.add_reason(CycleReason.OVER_CLOSE_BLOCKED)
                cycle.paired_lighter_quantity = _ZERO

    def _apply_taker_residue(self, cycle: _MutableCycle, action: _MutableAction, executable: Decimal, at_ns: int) -> None:
        if executable < action.requested_quantity and action.kind in {
            CycleActionKind.UNMATCHED_RISEX_UNWIND,
            CycleActionKind.EXIT_HEDGE_CLOSE,
            CycleActionKind.FORCED_RISEX_UNWIND,
            CycleActionKind.FORCED_LIGHTER_UNWIND,
        }:
            # A known non-executable terminal residue cannot be silently
            # retried or declared flat.
            if action.kind is CycleActionKind.UNMATCHED_RISEX_UNWIND:
                cycle.add_reason(CycleReason.INSUFFICIENT_DEPTH)
            self._halt(cycle, CycleReason.TERMINAL_NON_FLAT)

    def _prepare_exit(self, cycle: _MutableCycle, decision_ns: int) -> None:
        if cycle.paired_risex_quantity <= 0:
            if cycle.paired_lighter_quantity == 0 and cycle.unmatched_entry_quantity == 0:
                cycle.phase = _Phase.COMPLETE
                cycle.terminal_ns = decision_ns
            else:
                self._schedule_forced_remaining(cycle, decision_ns)
            return
        risex_book, lighter_book, pair_reason = _paired_books(cycle, decision_ns)
        if pair_reason is not None or risex_book is None or lighter_book is None:
            _add_action(cycle, action_id="exit-maker", kind=CycleActionKind.EXIT_MAKER, status=CycleActionStatus.UNRESOLVED, requested_ns=decision_ns, effective_ns=decision_ns, due_ns=decision_ns, quantity=cycle.paired_risex_quantity, reason=(pair_reason or CycleReason.REQUIRED_ACTION_DATA_MISSING).value)
            cycle.add_reason(pair_reason or CycleReason.REQUIRED_ACTION_DATA_MISSING)
            self._halt(cycle, pair_reason or CycleReason.REQUIRED_ACTION_DATA_MISSING)
            return
        quantity = cycle.paired_risex_quantity
        lighter_vwap = exact_quantity_vwap(Side.SELL, quantity, tuple(lighter_book.bids), tuple(lighter_book.asks))
        tick = cycle.quote_version.quote.risex_tick_size
        if tick is None or lighter_vwap.price is None or not lighter_vwap.is_executable or not _minimum_ok(cycle, Venue.LIGHTER, quantity, lighter_vwap.price):
            _add_action(cycle, action_id="exit-maker", kind=CycleActionKind.EXIT_MAKER, status=CycleActionStatus.NOT_REQUIRED, requested_ns=decision_ns, effective_ns=decision_ns, due_ns=decision_ns, quantity=quantity, reason=CycleReason.EXIT_QUOTE_INVALID.value)
            cycle.add_reason(CycleReason.EXIT_QUOTE_INVALID)
            cycle.forced_used = True
            self._schedule_forced_remaining(cycle, decision_ns)
            return
        raw = lighter_vwap.notional_usd * (_ONE - cycle.policy.lighter_taker_fee_rate) / (quantity * (_ONE + cycle.policy.risex_maker_fee_rate))
        risex_best_ask = risex_book.asks[0].canonical_price if risex_book.asks else None
        if risex_best_ask is None:
            _add_action(cycle, action_id="exit-maker", kind=CycleActionKind.EXIT_MAKER, status=CycleActionStatus.NOT_REQUIRED, requested_ns=decision_ns, effective_ns=decision_ns, due_ns=decision_ns, quantity=quantity, reason=CycleReason.EXIT_QUOTE_INVALID.value)
            cycle.add_reason(CycleReason.EXIT_QUOTE_INVALID)
            cycle.forced_used = True
            self._schedule_forced_remaining(cycle, decision_ns)
            return
        maker_price = min((raw / tick).to_integral_value(rounding=ROUND_FLOOR) * tick, risex_best_ask - tick)
        # A BUY below the best bid is still valid post-only liquidity.  The
        # closed-world exit formula already caps the price below the best ask;
        # requiring bid+tick here would incorrectly reject a safe, non-crossing
        # quote whenever the executable hedge markout is below the RISEx BBO.
        if maker_price <= 0:
            _add_action(cycle, action_id="exit-maker", kind=CycleActionKind.EXIT_MAKER, status=CycleActionStatus.NOT_REQUIRED, requested_ns=decision_ns, effective_ns=decision_ns, due_ns=decision_ns, quantity=quantity, reason=CycleReason.EXIT_QUOTE_INVALID.value)
            cycle.add_reason(CycleReason.EXIT_QUOTE_INVALID)
            cycle.forced_used = True
            self._schedule_forced_remaining(cycle, decision_ns)
            return
        cycle.exit_price = maker_price
        cycle.exit_activation_ns = decision_ns + cycle.delays.activation_delay_ns
        cycle.exit_quote = CausalRestingQuote(
            quote_id=f"{cycle.quote_version.version_id}:exit",
            quote_version_id=f"{cycle.quote_version.version_id}:exit",
            canonical_market=cycle.quote_version.canonical_market,
            maker_side=Side.BUY,
            price=maker_price,
            quantity=quantity,
            stream_session_id=cycle.quote_version.stream_session_id,
            recovery_generation=cycle.quote_version.recovery_generation,
            decision_ready_monotonic_ns=decision_ns,
            activation_delay_ns=cycle.delays.activation_delay_ns,
            cancel_delay_ns=cycle.delays.cancel_delay_ns,
            cancel_on_first_partial=True,
            source_book=risex_book,
            source_book_revision=risex_book.book_revision,
            source_book_revision_id=risex_book.book_revision_id,
            source_book_binding_required=False,
            source_identity=CausalSourceIdentity.from_book(risex_book),
            hedge_source_book=lighter_book,
            hedge_stream_session_id=lighter_book.stream_session_id,
            hedge_recovery_generation=lighter_book.recovery_generation,
            tick_size=tick,
        )
        cycle.exit_remaining_quantity = quantity
        _add_action(cycle, action_id="exit-maker", kind=CycleActionKind.EXIT_MAKER, status=CycleActionStatus.PENDING, requested_ns=decision_ns, effective_ns=cycle.exit_activation_ns, due_ns=cycle.max_hold_deadline_ns, quantity=quantity, reason="EXIT_RISEX_MAKER_QUOTE")
        cycle.phase = _Phase.EXIT_WAIT

    def _schedule_forced_remaining(self, cycle: _MutableCycle, requested_ns: int) -> None:
        cycle.forced_used = True
        cycle.add_reason(CycleReason.FORCED_UNWIND)
        if any(
            action.action_id in cycle.scheduled_takers
            and action.status is CycleActionStatus.PENDING
            for action in cycle.actions
        ):
            # Let already queued delayed closes settle before sizing forced
            # actions from the remaining positions.
            cycle.phase = _Phase.CLOSE_WAIT
            return
        cycle.phase = _Phase.FORCE_WAIT
        if cycle.unmatched_entry_quantity > 0 and not any(action.action_id == "unmatched-risex" for action in cycle.actions):
            cycle.unmatched_started_ns = cycle.unmatched_started_ns or requested_ns
            _schedule_taker(cycle, action_id="unmatched-risex", kind=CycleActionKind.UNMATCHED_RISEX_UNWIND, venue=Venue.RISEX, side=Side.BUY, quantity=cycle.unmatched_entry_quantity, requested_ns=requested_ns, reason="UNMATCHED_RISEX_TAKER_UNWIND")
        if cycle.paired_risex_quantity > 0 and not any(action.action_id == "forced-risex" for action in cycle.actions):
            _schedule_taker(cycle, action_id="forced-risex", kind=CycleActionKind.FORCED_RISEX_UNWIND, venue=Venue.RISEX, side=Side.BUY, quantity=cycle.paired_risex_quantity, requested_ns=requested_ns, reason="FORCED_RISEX_TAKER_UNWIND")
        if cycle.paired_lighter_quantity > 0 and not any(action.action_id == "forced-lighter" for action in cycle.actions):
            _schedule_taker(cycle, action_id="forced-lighter", kind=CycleActionKind.FORCED_LIGHTER_UNWIND, venue=Venue.LIGHTER, side=Side.SELL, quantity=cycle.paired_lighter_quantity, requested_ns=requested_ns, reason="FORCED_LIGHTER_TAKER_UNWIND")
        pending = any(
            action.action_id in cycle.scheduled_takers
            and action.status is CycleActionStatus.PENDING
            for action in cycle.actions
        )
        if not pending:
            if cycle.positions.is_zero:
                cycle.phase = _Phase.COMPLETE
                cycle.terminal_ns = requested_ns
            else:
                self._halt(cycle, CycleReason.TERMINAL_NON_FLAT)

    def _halt(self, cycle: _MutableCycle, reason: CycleReason | str) -> None:
        cycle.add_reason(reason)
        cycle.unresolved = True
        cycle.phase = _Phase.UNRESOLVED
        cycle.terminal_ns = cycle.current_ns

    def _latch_terminal(self, lane: _KernelLane, cycle: _MutableCycle) -> None:
        result = _result(cycle, terminal=True)
        lane.last_result = result
        lane.last_terminal_ns = cycle.terminal_ns
        if cycle.unresolved or not result.is_flat:
            lane.halted_unresolved = True
        if not any(existing is cycle for existing in lane.terminal_cycles):
            lane.terminal_cycles.append(cycle)
        lane.terminal_cycle = cycle
        lane.active = None


def run_cycle(
    quote_version: QuoteVersion,
    events: Iterable[CausalEvent | TradeEvidence | BookEvidence | DataGapEvidence],
    *,
    scenario: CycleScenario = CycleScenario.PRIMARY,
    policy: CyclePolicy | None = None,
    source_books: Iterable[BookEvidence] = (),
    end_monotonic_ns: int | None = None,
) -> CycleResult:
    """Feed one cycle through the same sequential transition spine."""

    kernel = CycleKernel(policy)
    return kernel.run(
        quote_version,
        events,
        scenario=scenario,
        source_books=source_books,
        end_monotonic_ns=end_monotonic_ns,
    )


def run_cycle_alternatives(
    quote_version: QuoteVersion,
    events: Iterable[CausalEvent | TradeEvidence | BookEvidence | DataGapEvidence],
    *,
    policy: CyclePolicy | None = None,
    source_books: Iterable[BookEvidence] = (),
) -> CycleAlternatives:
    """Run primary and stress independently on the same ordered inputs."""

    materialized = tuple(events)
    books = tuple(source_books)
    primary = CycleKernel(policy).run(quote_version, materialized, scenario=CycleScenario.PRIMARY, source_books=books)
    stress = CycleKernel(policy).run(quote_version, materialized, scenario=CycleScenario.STRESS, source_books=books)
    return CycleAlternatives(primary=primary, stress=stress)


__all__ = [
    "CycleAction",
    "CycleActionKind",
    "CycleActionStatus",
    "CycleAlternatives",
    "CycleAdmission",
    "CycleAdmissionError",
    "CycleAttempt",
    "CycleCashFlow",
    "CycleClock",
    "CycleDelays",
    "CycleFee",
    "CycleFill",
    "CycleKernel",
    "CycleKernelState",
    "CycleLedger",
    "CyclePolicy",
    "CyclePositions",
    "CycleProgress",
    "CycleReason",
    "CycleResult",
    "CycleScenario",
    "CycleTerminalState",
    "run_cycle",
    "run_cycle_alternatives",
    "s2_cycle_policy",
]
