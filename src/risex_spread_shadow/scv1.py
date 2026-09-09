"""SCV1-S1a versioned execution alternatives.

This module is deliberately a small adapter around the existing sequential
cycle transition spine.  It does not add a collector, storage layer, or a
second accounting implementation.  Each returned alternative owns a fresh
``CycleKernel`` so positions, pending actions, duplicate-event ledgers, and
accounting cannot leak between the two fill models or the two delay regimes.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from .causal import CausalEvent
from .cycle import (
    CycleFillModel,
    CycleKernel,
    CyclePolicy,
    CycleResult,
    CycleScenario,
    s2_cycle_policy,
)
from .models import BookEvidence, DataGapEvidence, QuoteVersion, TradeEvidence


SCV1_S1A_CONTRACT_VERSION = "SCV1-1.1-S1a"


Scv1Input = CausalEvent | TradeEvidence | BookEvidence | DataGapEvidence


@dataclass(frozen=True, slots=True)
class Scv1Alternative:
    """One isolated SCV1 fill-model/delay alternative."""

    fill_model: CycleFillModel
    scenario: CycleScenario
    result: CycleResult
    contract_version: str = SCV1_S1A_CONTRACT_VERSION

    def __post_init__(self) -> None:
        model = self.fill_model
        if not isinstance(model, CycleFillModel):
            model = CycleFillModel(model)
            object.__setattr__(self, "fill_model", model)
        scenario = self.scenario
        if not isinstance(scenario, CycleScenario):
            scenario = CycleScenario(scenario)
            object.__setattr__(self, "scenario", scenario)
        if self.result.scenario is not scenario:
            raise ValueError("alternative result scenario does not match its key")
        if not self.contract_version:
            raise ValueError("contract_version must be non-empty")

    @property
    def key(self) -> str:
        return f"{self.fill_model.value}:{self.scenario.value}"

    @property
    def status(self):
        return self.result.status

    @property
    def ledger(self):
        return self.result.ledger


@dataclass(frozen=True, slots=True)
class Scv1Alternatives:
    """The four non-additive SCV1 alternatives."""

    trade_through_primary: Scv1Alternative
    trade_through_stress: Scv1Alternative
    touch_allowed_primary: Scv1Alternative
    touch_allowed_stress: Scv1Alternative
    contract_version: str = SCV1_S1A_CONTRACT_VERSION

    def __post_init__(self) -> None:
        expected = (
            (self.trade_through_primary, CycleFillModel.TRADE_THROUGH_ONLY, CycleScenario.PRIMARY),
            (self.trade_through_stress, CycleFillModel.TRADE_THROUGH_ONLY, CycleScenario.STRESS),
            (self.touch_allowed_primary, CycleFillModel.TOUCH_ALLOWED, CycleScenario.PRIMARY),
            (self.touch_allowed_stress, CycleFillModel.TOUCH_ALLOWED, CycleScenario.STRESS),
        )
        for alternative, model, scenario in expected:
            if not isinstance(alternative, Scv1Alternative):
                raise TypeError("SCV1 alternatives must contain Scv1Alternative values")
            if alternative.fill_model is not model or alternative.scenario is not scenario:
                raise ValueError("SCV1 alternatives must contain exactly one result per model/regime")
        if not self.contract_version:
            raise ValueError("contract_version must be non-empty")

    @property
    def by_alternative(self) -> tuple[Scv1Alternative, ...]:
        return (
            self.trade_through_primary,
            self.trade_through_stress,
            self.touch_allowed_primary,
            self.touch_allowed_stress,
        )

    @property
    def by_fill_model(self) -> tuple[tuple[Scv1Alternative, Scv1Alternative], ...]:
        return (
            (self.trade_through_primary, self.trade_through_stress),
            (self.touch_allowed_primary, self.touch_allowed_stress),
        )

    def select(
        self,
        fill_model: CycleFillModel | str,
        scenario: CycleScenario | str,
    ) -> Scv1Alternative:
        model = fill_model if isinstance(fill_model, CycleFillModel) else CycleFillModel(fill_model)
        regime = scenario if isinstance(scenario, CycleScenario) else CycleScenario(scenario)
        for alternative in self.by_alternative:
            if alternative.fill_model is model and alternative.scenario is regime:
                return alternative
        raise KeyError((model, regime))

    def report_rows(self) -> tuple[dict[str, object], ...]:
        """Return a compact, human-readable fixture-report projection."""

        rows: list[dict[str, object]] = []
        for alternative in self.by_alternative:
            result = alternative.result
            rows.append(
                {
                    "contract_version": alternative.contract_version,
                    "fill_model": alternative.fill_model.value,
                    "scenario": alternative.scenario.value,
                    "status": result.status.value,
                    "reasons": list(result.reason_codes),
                    "entry_quantity": str(result.entry_quantity),
                    "hedged_quantity": str(result.hedged_quantity),
                    "unmatched_entry_quantity": str(result.unmatched_entry_quantity),
                    "fill_count": len(result.fills),
                    "closed_execution_pnl_usd": (
                        None
                        if result.pnl_usd is None
                        else str(result.pnl_usd)
                    ),
                    "signed_cashflow_usd": str(result.ledger.signed_cashflow_usd),
                    "fees_usd": str(result.ledger.total_fees_usd),
                    "stress_cost_usd": str(result.ledger.scenario_cost_usd),
                    "turnover_usd": str(result.ledger.turnover_usd),
                    "pending_actions": [
                        action.action_id for action in result.pending_actions
                    ],
                }
            )
        return tuple(rows)


def scv1_s1a_policy() -> CyclePolicy:
    """Return the frozen SCV1 BTC policy used by the S1a adapter.

    The numerical assumptions are intentionally shared with the accepted
    cycle policy.  Versioning lives at the alternative/report boundary so the
    historical S2 evidence schema and factory remain unchanged.
    """

    return s2_cycle_policy()


def run_scv1_s1a(
    quote_version: QuoteVersion,
    events: Iterable[Scv1Input],
    *,
    fill_model: CycleFillModel | str,
    scenario: CycleScenario = CycleScenario.PRIMARY,
    policy: CyclePolicy | None = None,
    source_books: Iterable[BookEvidence] = (),
    end_monotonic_ns: int | None = None,
) -> Scv1Alternative:
    """Run one explicit SCV1 alternative through the production kernel."""

    model = fill_model if isinstance(fill_model, CycleFillModel) else CycleFillModel(fill_model)
    regime = scenario if isinstance(scenario, CycleScenario) else CycleScenario(scenario)
    kernel = CycleKernel(
        policy,
        fill_model=model,
    )
    result = kernel.run(
        quote_version,
        events,
        scenario=regime,
        source_books=source_books,
        end_monotonic_ns=end_monotonic_ns,
    )
    return Scv1Alternative(model, regime, result)


def run_scv1_s1a_alternatives(
    quote_version: QuoteVersion,
    events: Iterable[Scv1Input],
    *,
    policy: CyclePolicy | None = None,
    source_books: Iterable[BookEvidence] = (),
    end_monotonic_ns: int | None = None,
) -> Scv1Alternatives:
    """Run all four SCV1 alternatives on independent kernel instances."""

    materialized = tuple(events)
    books = tuple(source_books)
    return Scv1Alternatives(
        trade_through_primary=run_scv1_s1a(
            quote_version,
            materialized,
            fill_model=CycleFillModel.TRADE_THROUGH_ONLY,
            scenario=CycleScenario.PRIMARY,
            policy=policy,
            source_books=books,
            end_monotonic_ns=end_monotonic_ns,
        ),
        trade_through_stress=run_scv1_s1a(
            quote_version,
            materialized,
            fill_model=CycleFillModel.TRADE_THROUGH_ONLY,
            scenario=CycleScenario.STRESS,
            policy=policy,
            source_books=books,
            end_monotonic_ns=end_monotonic_ns,
        ),
        touch_allowed_primary=run_scv1_s1a(
            quote_version,
            materialized,
            fill_model=CycleFillModel.TOUCH_ALLOWED,
            scenario=CycleScenario.PRIMARY,
            policy=policy,
            source_books=books,
            end_monotonic_ns=end_monotonic_ns,
        ),
        touch_allowed_stress=run_scv1_s1a(
            quote_version,
            materialized,
            fill_model=CycleFillModel.TOUCH_ALLOWED,
            scenario=CycleScenario.STRESS,
            policy=policy,
            source_books=books,
            end_monotonic_ns=end_monotonic_ns,
        ),
    )


from .s1b import (
    SCV1_S1B_CONTRACT_VERSION,
    S1bCycleKernel,
    S1bCycleResult,
    S1bExitVariant,
    Scv1S1bKernel,
    build_scv1_s1b_d1_report,
    render_scv1_s1b_d1_report,
    run_scv1_s1b,
    run_scv1_s1b_alternatives,
    scv1_s1b_policy,
)


__all__ = [
    "SCV1_S1A_CONTRACT_VERSION",
    "Scv1Alternative",
    "Scv1Alternatives",
    "Scv1Input",
    "scv1_s1a_policy",
    "run_scv1_s1a",
    "run_scv1_s1a_alternatives",
    "SCV1_S1B_CONTRACT_VERSION",
    "S1bCycleKernel",
    "S1bCycleResult",
    "S1bExitVariant",
    "Scv1S1bKernel",
    "build_scv1_s1b_d1_report",
    "render_scv1_s1b_d1_report",
    "run_scv1_s1b",
    "run_scv1_s1b_alternatives",
    "scv1_s1b_policy",
]
