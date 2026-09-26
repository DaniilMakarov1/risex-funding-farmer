"""One source LIMIT filled by several receiver MARKETs in one attempt (fan-out).

The 1 -> 1 handoff is unchanged.  A fan-out attempt reuses the proven per-leg
steps of :class:`HandoffEngine` (preparation, prepared dispatch, order
observation, exact cancellation, leg reconciliation) and changes only the
orchestration: every receiver order is signed before the source LIMIT, the
unchanged ACK or WS admission decides once for the source, then every receiver
intent is journaled and all receiver MARKETs are sent at the same time.  A
receiver counts as matched only by reciprocal trade, order and account
identities with the source.  Legs are named ``source``, ``receiver`` and
``receiver_2`` .. ``receiver_k``; the dispatch events of receiver ``i`` are
``RECEIVER_i_DISPATCH_*`` (``RECEIVER_DISPATCH_*`` for the first).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal
import re
import time
from typing import Any, Mapping, Sequence

from .contracts import (
    AccountSnapshot,
    ContractError,
    Direction,
    HandoffConfig,
    HandoffResult,
    LegReconciliation,
    MarketMetadata,
    MutationReceipt,
    OperationMode,
    OrderPlan,
    OrderSnapshot,
    Outcome,
    Phase,
    PreflightBlocked,
    TransientStreamContractError,
    _decimal,
    _int,
    _positive,
    _text,
    _timestamp,
    _nonnegative,
    _wire_decimal,
    decimal_to_integer,
)
from .engine import (
    PRE_DISPATCH_QUOTE_EXPIRED_REASON,
    PRE_SOURCE_STREAM_REASON,
    Clock,
    HandoffClient,
    HandoffEngine,
    _account_from_client,
    _as_account,
    _as_market,
    _as_receipt,
    _client_order_index,
    _coerce_public_book,
    _config_binding,
    _is_pre_dispatch_reason,
    _joint_trade_match,
    _leg_proves_no_execution,
    _prepared_timing_values,
    _safe_preflight_reason,
)
from .journal import DurableJournal, sanitize_exception

# A fan-out attempt stays bounded: every receiver adds reads and one order.
MAX_FANOUT_RECEIVERS = 16
RECEIVER_EVENT = re.compile(r"RECEIVER(?:_([2-9]|1[0-6]))?_DISPATCH_(INTENT|RESULT|UNKNOWN)")


def receiver_leg(position: int) -> str:
    """Leg name of the receiver at 1-based ``position``."""
    return "receiver" if position == 1 else f"receiver_{position}"


def receiver_event_prefix(position: int) -> str:
    return "RECEIVER" if position == 1 else f"RECEIVER_{position}"


@dataclass(frozen=True, slots=True)
class FanoutReceiver:
    account_index: int
    quantity: Decimal
    expected_position: Decimal | None = None

    def __post_init__(self) -> None:
        _int(self.account_index, "fan-out receiver account_index", minimum=0)
        object.__setattr__(self, "quantity", _positive(self.quantity, "fan-out receiver quantity"))
        if self.expected_position is not None:
            object.__setattr__(self, "expected_position", _decimal(self.expected_position, "fan-out expected position"))

    def as_dict(self) -> dict[str, Any]:
        return {
            "account_index": self.account_index,
            "quantity": _wire_decimal(self.quantity),
            "expected_position": None if self.expected_position is None else _wire_decimal(self.expected_position),
        }


@dataclass(frozen=True, slots=True)
class FanoutHandoffConfig(HandoffConfig):
    """A paired operation whose receiver side is split over several accounts.

    ``quantity`` is the source LIMIT; ``receivers`` lists every receiver (the
    first is the bound client's receiver) with its exact part.  The parts sum
    to ``quantity``.  Only the stream admissions (ACK or WS) are supported.
    """

    receivers: tuple[FanoutReceiver, ...] = ()

    def __post_init__(self) -> None:
        HandoffConfig.__post_init__(self)
        receivers = tuple(self.receivers)
        if not all(isinstance(item, FanoutReceiver) for item in receivers):
            raise ContractError("fan-out receivers must be explicit FanoutReceiver values")
        if not 2 <= len(receivers) <= MAX_FANOUT_RECEIVERS:
            raise ContractError(f"fan-out requires 2 to {MAX_FANOUT_RECEIVERS} receivers")
        if len({item.account_index for item in receivers}) != len(receivers):
            raise ContractError("fan-out receiver accounts must differ")
        if sum((item.quantity for item in receivers), Decimal(0)) != self.quantity:
            raise ContractError("fan-out receiver parts must sum to the source quantity")
        if self.receiver_admission not in ("ack", "ws_confirmed"):
            raise ContractError("fan-out supports only ACK or WS receiver admission")
        if self.operation_mode not in (OperationMode.PAIRED_OPENING, OperationMode.PAIRED_CLOSING):
            raise ContractError("fan-out requires a paired opening or closing")
        first = receivers[0]
        if (self.expected_receiver_position is not None
                and first.expected_position != self.expected_receiver_position):
            raise ContractError("first fan-out receiver expectation conflicts with the paired expectation")
        object.__setattr__(self, "receivers", receivers)

    @property
    def receiver_accounts(self) -> tuple[int, ...]:
        return tuple(item.account_index for item in self.receivers)

    def validate_fanout(
        self,
        metadata: MarketMetadata,
        source: AccountSnapshot,
        receivers: Sequence[AccountSnapshot],
        now: float,
    ) -> None:
        """The paired admission checks for every account and every part."""
        market_label = self.market_symbol.upper()
        accounts = (source, *receivers)
        if len(receivers) != len(self.receivers):
            raise PreflightBlocked("fan-out receiver observations are incomplete")
        if any(snapshot.market_id != self.market_id for snapshot in accounts) or metadata.market_id != self.market_id:
            raise PreflightBlocked(f"market identity does not match configured {market_label} market")
        if metadata.symbol.upper() != market_label:
            raise PreflightBlocked(f"market identity is not {market_label}")
        if metadata.market_type.lower() != "perp":
            raise PreflightBlocked("market identity is not a perpetual")
        if self.environment == "robinhood" and metadata.venue.lower() not in {"", "robinhood", "robinhood-chain"}:
            raise PreflightBlocked("market identity is from the wrong venue")
        if metadata.status.lower() not in {"active", "open", "online", "listed"}:
            raise PreflightBlocked(f"{market_label} market is not active")
        if metadata.observed_at > now:
            raise PreflightBlocked("market metadata is from the future")
        if now - metadata.observed_at > self.freshness_seconds:
            raise PreflightBlocked("market metadata is stale")
        try:
            quantity_int = decimal_to_integer(self.quantity, metadata.size_decimals, "quantity")
            parts_int = [decimal_to_integer(item.quantity, metadata.size_decimals, "fan-out part")
                         for item in self.receivers]
            decimal_to_integer(self.source_limit_price, metadata.price_decimals, "source_limit_price")
            decimal_to_integer(self.receiver_worst_price, metadata.price_decimals, "receiver_worst_price")
        except ContractError as exc:
            raise PreflightBlocked(str(exc)) from exc
        if quantity_int <= 0 or any(value <= 0 for value in parts_int) or sum(parts_int) != quantity_int:
            raise PreflightBlocked("fan-out quantities are not on the venue size grid")
        if self.quantity * self.source_limit_price < metadata.minimum_quote_amount:
            raise PreflightBlocked("source maker notional is below the documented quote minimum")
        for item in self.receivers:
            if item.quantity < metadata.minimum_base_amount:
                raise PreflightBlocked("a fan-out part is below the documented base minimum")
            if item.quantity * self.receiver_worst_price < metadata.minimum_quote_amount:
                raise PreflightBlocked("a fan-out part is below the documented quote minimum")
        labels = ["source", *(receiver_leg(position) for position in range(1, len(receivers) + 1))]
        for snapshot, label in zip(accounts, labels):
            if not snapshot.authorized or not snapshot.ready:
                raise PreflightBlocked(f"{label} account authorization/readiness is unproven")
            if snapshot.observed_at > now:
                raise PreflightBlocked(f"{label} account state is from the future")
            if now - snapshot.observed_at > self.freshness_seconds:
                raise PreflightBlocked(f"{label} account state is stale")
            if not snapshot.source_identity:
                raise PreflightBlocked(f"{label} account identity is missing")
            if snapshot.active_orders:
                raise PreflightBlocked(f"{label} has active {market_label} orders")
            if snapshot.margin_available is None or snapshot.margin_required is None:
                raise PreflightBlocked(f"{label} margin evidence is missing")
            incremental_missing = (snapshot.incremental_margin_required is None
                                   or not snapshot.incremental_margin_evidence)
            if (incremental_missing and self.operation_mode is not OperationMode.PAIRED_CLOSING
                    and not self.defer_incremental_margin_calculation):
                raise PreflightBlocked(f"{label} incremental planned-operation margin evidence is missing")
            if (not incremental_missing and self.operation_mode is not OperationMode.PAIRED_CLOSING
                    and snapshot.incremental_margin_required > snapshot.margin_available):
                raise PreflightBlocked(f"{label} incremental planned-operation margin is insufficient")
        if self.source_quote_observed_at is not None:
            if self.source_quote_observed_at > now:
                raise PreflightBlocked("source quote is from the future")
            if now - self.source_quote_observed_at > self.freshness_seconds:
                raise PreflightBlocked("source quote is stale")
        indices = [snapshot.account_index for snapshot in accounts]
        if len(set(indices)) != len(indices):
            raise PreflightBlocked("fan-out accounts must all differ")
        if [snapshot.account_index for snapshot in receivers] != list(self.receiver_accounts):
            raise PreflightBlocked("fan-out receiver observations do not match the configured receivers")
        sign = self.direction.sign
        if self.operation_mode is OperationMode.PAIRED_OPENING:
            expectations = [self.expected_source_position, *(item.expected_position for item in self.receivers)]
            if all(value is None for value in expectations):
                if any(snapshot.signed_position != 0 for snapshot in accounts):
                    raise PreflightBlocked("fan-out opening requires every selected-market position to be flat")
            elif any(value is None for value in expectations):
                raise PreflightBlocked("fan-out opening expected positions must all be bound")
            else:
                if source.signed_position != self.expected_source_position:
                    raise PreflightBlocked("source position does not match the expected paired-opening position")
                if self.expected_source_position * sign > 0:
                    raise PreflightBlocked("paired-opening source position has the receiver direction")
                for snapshot, item in zip(receivers, self.receivers):
                    if snapshot.signed_position != item.expected_position:
                        raise PreflightBlocked("receiver position does not match the expected paired-opening position")
                    if item.expected_position * sign < 0:
                        raise PreflightBlocked("paired-opening receiver position has the source direction")
        else:
            if self.expected_source_position is not None and source.signed_position != self.expected_source_position:
                raise PreflightBlocked("source position does not match the expected paired-closing position")
            if (source.signed_position < self.quantity) if sign > 0 else (source.signed_position > -self.quantity):
                raise PreflightBlocked("paired closing source position is smaller than Q")
            for snapshot, item in zip(receivers, self.receivers):
                if item.expected_position is not None and snapshot.signed_position != item.expected_position:
                    raise PreflightBlocked("receiver position does not match the expected paired-closing position")
                if (snapshot.signed_position > -item.quantity) if sign > 0 else (snapshot.signed_position < item.quantity):
                    raise PreflightBlocked("paired closing receiver position is smaller than its part")
        if not metadata.margin_evidence:
            raise PreflightBlocked("minimum/margin evidence is missing")


@dataclass(frozen=True, slots=True)
class FanoutHandoffPlan:
    """The immutable orders of one fan-out attempt (source plus every receiver)."""

    run_id: str
    source: OrderPlan
    receivers: tuple[OrderPlan, ...]
    source_position_before: Decimal
    receiver_positions_before: tuple[Decimal, ...]
    direction: Direction
    quantity: Decimal
    created_at: float
    source_fee_rate: Decimal | None = None
    receiver_fee_rate: Decimal | None = None
    source_identity: str = ""
    receiver_identities: tuple[str, ...] = ()
    metadata_observed_at: float | None = None
    operation_mode: OperationMode | str = OperationMode.PAIRED_OPENING
    defer_incremental_margin_calculation: bool = False

    def __post_init__(self) -> None:
        direction = self.direction if isinstance(self.direction, Direction) else Direction(self.direction)
        object.__setattr__(self, "direction", direction)
        mode = OperationMode.parse(self.operation_mode)
        object.__setattr__(self, "operation_mode", mode)
        _text(self.run_id, "run_id")
        object.__setattr__(self, "quantity", _positive(self.quantity, "quantity"))
        object.__setattr__(self, "source_position_before", _decimal(self.source_position_before, "source_position_before"))
        positions = tuple(_decimal(value, "receiver_position_before") for value in self.receiver_positions_before)
        object.__setattr__(self, "receiver_positions_before", positions)
        receivers = tuple(self.receivers)
        object.__setattr__(self, "receivers", receivers)
        _timestamp(self.created_at, "created_at")
        if self.metadata_observed_at is not None:
            object.__setattr__(self, "metadata_observed_at", _timestamp(self.metadata_observed_at, "metadata_observed_at"))
        for value, name in ((self.source_fee_rate, "source_fee_rate"), (self.receiver_fee_rate, "receiver_fee_rate")):
            if value is not None:
                object.__setattr__(self, name, _nonnegative(value, name))
        identities = tuple(self.receiver_identities)
        object.__setattr__(self, "receiver_identities", identities)
        if not 2 <= len(receivers) <= MAX_FANOUT_RECEIVERS:
            raise ContractError("fan-out plan requires 2 or more receivers")
        if len(positions) != len(receivers) or len(identities) != len(receivers):
            raise ContractError("fan-out plan receiver fields are misaligned")
        accounts = [self.source.account_index, *(item.account_index for item in receivers)]
        if len(set(accounts)) != len(accounts):
            raise ContractError("fan-out plan accounts must differ")
        clients = [self.source.client_order_index, *(item.client_order_index for item in receivers)]
        if len(set(clients)) != len(clients):
            raise ContractError("fan-out plan client order identities must differ")
        if any(item.market_id != self.source.market_id for item in receivers):
            raise ContractError("fan-out plan markets must match")
        if self.source.quantity != self.quantity or sum((item.quantity for item in receivers), Decimal(0)) != self.quantity:
            raise ContractError("fan-out plan quantities must sum to the source quantity")
        if sum(item.quantity_int for item in receivers) != self.source.quantity_int:
            raise ContractError("fan-out plan integer quantities must sum to the source quantity")
        if self.source.side != direction.source_side or any(item.side != direction.receiver_side for item in receivers):
            raise ContractError("fan-out plan sides conflict with direction")
        closing = mode is OperationMode.PAIRED_CLOSING
        if mode not in (OperationMode.PAIRED_OPENING, OperationMode.PAIRED_CLOSING):
            raise ContractError("fan-out plan requires a paired operation")
        if (self.source.order_type != "LIMIT" or self.source.time_in_force != "POST_ONLY"
                or self.source.reduce_only != closing or self.source.order_expiry_ms <= 0
                or any(item.order_type != "MARKET" or item.time_in_force != "IOC"
                       or item.reduce_only != closing or item.order_expiry_ms != 0 for item in receivers)):
            raise ContractError("fan-out plan order semantics conflict with the selected operation mode")

    @property
    def receiver(self) -> OrderPlan:
        return self.receivers[0]

    @property
    def receiver_identity(self) -> str:
        return self.receiver_identities[0]

    @property
    def receiver_position_before(self) -> Decimal:
        return self.receiver_positions_before[0]

    def as_dict(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "run_id": self.run_id,
            "source": self.source.as_dict(),
        }
        for position, plan in enumerate(self.receivers, 1):
            value[receiver_leg(position)] = plan.as_dict()
        value.update({
            "source_position_before": _wire_decimal(self.source_position_before),
            "receiver_position_before": _wire_decimal(self.receiver_positions_before[0]),
            "direction": self.direction.value,
            "quantity": _wire_decimal(self.quantity),
            "created_at": self.created_at,
            "source_fee_rate": None if self.source_fee_rate is None else _wire_decimal(self.source_fee_rate),
            "receiver_fee_rate": None if self.receiver_fee_rate is None else _wire_decimal(self.receiver_fee_rate),
            "source_identity": self.source_identity,
            "receiver_identity": self.receiver_identities[0],
            "metadata_observed_at": self.metadata_observed_at,
            "operation_mode": self.operation_mode.value,
            "defer_incremental_margin_calculation": self.defer_incremental_margin_calculation,
            "fanout": {
                "receiver_count": len(self.receivers),
                "receiver_account_indices": [plan.account_index for plan in self.receivers],
                "receiver_quantities": [_wire_decimal(plan.quantity) for plan in self.receivers],
                "receiver_positions_before": [_wire_decimal(value) for value in self.receiver_positions_before],
                "receiver_identities": list(self.receiver_identities),
            },
        })
        return value


def _trade_payload(trade: Any) -> dict[str, Any]:
    return {
        "trade_id": trade.trade_id,
        "account_index": trade.account_index,
        "market_id": trade.market_id,
        "order_id": trade.order_id,
        "side": trade.side,
        "quantity": _wire_decimal(trade.quantity),
        "price": _wire_decimal(trade.price),
        "fee": None if trade.fee is None else _wire_decimal(trade.fee),
        "fee_role": trade.fee_role,
        "venue_fee_raw": trade.venue_fee_raw,
        "integrator_fee_raw": trade.integrator_fee_raw,
        "fee_evidence": trade.fee_evidence,
        "counterparty_account_index": trade.counterparty_account_index,
        "counterparty_order_id": trade.counterparty_order_id,
        "counterparty_client_order_index": trade.counterparty_client_order_index,
        "client_order_index": trade.client_order_index,
        "observed_at": trade.observed_at,
    }


def _leg_payload(value: LegReconciliation | None, peers: set[int], matched: Decimal | None,
                 match_status: str) -> Any:
    """The 1 -> 1 receipt leg shape, with the counterparty set of this leg."""
    if value is None:
        return None
    external = sum((trade.quantity for trade in value.trades
                    if trade.counterparty_account_index is not None
                    and trade.counterparty_account_index not in peers), Decimal(0))
    named = sum((trade.quantity for trade in value.trades if trade.counterparty_account_index in peers), Decimal(0))
    unknown_counterparty = not value.trades or any(trade.counterparty_account_index is None for trade in value.trades)
    return {
        "account_index": value.account_index,
        "order_id": value.order_id,
        "filled_quantity": _wire_decimal(value.filled_quantity),
        "external_counterparty_quantity": _wire_decimal(external),
        "unproved_counterparty_quantity": _wire_decimal(
            max(Decimal(0), value.filled_quantity - external - (matched or Decimal(0)))),
        "counterparty_matched_quantity": None if matched is None else _wire_decimal(matched),
        "counterparty_match_status": match_status,
        "named_counterparty_quantity": _wire_decimal(named),
        "named_counterparty_status": ("UNKNOWN" if unknown_counterparty
                                      else "MATCHED" if named > 0 else "KNOWN_ZERO"),
        "fee_total": None if value.fee_total is None else _wire_decimal(value.fee_total),
        "gross_notional": _wire_decimal(value.gross_notional),
        "position_before": _wire_decimal(value.position_before),
        "position_after": None if value.position_after is None else _wire_decimal(value.position_after),
        "history_complete": value.history_complete,
        "dispatched": value.dispatched,
        "unknown_reasons": list(value.unknown_reasons),
        "trades": [_trade_payload(trade) for trade in value.trades],
        "order": None if value.order is None else {
            "order_id": value.order.order_id,
            "client_order_index": value.order.client_order_index,
            "account_index": value.order.account_index,
            "market_id": value.order.market_id,
            "side": value.order.side,
            "order_type": value.order.order_type,
            "time_in_force": value.order.time_in_force,
            "reduce_only": value.order.reduce_only,
            "price": None if value.order.price is None else _wire_decimal(value.order.price),
            "status": value.order.status,
            "initial_quantity": _wire_decimal(value.order.initial_quantity),
            "remaining_quantity": _wire_decimal(value.order.remaining_quantity),
            "filled_quantity": _wire_decimal(value.order.filled_quantity),
            "observed_at": value.order.observed_at,
        },
    }


@dataclass(frozen=True, slots=True)
class FanoutHandoffResult(HandoffResult):
    """``receiver`` is the first receiver's leg; ``receivers`` holds all of them."""

    receivers: tuple[LegReconciliation | None, ...] = ()
    receiver_matches: tuple[tuple[str, Decimal], ...] = ()

    @property
    def mutual_execution_proven(self) -> bool:
        plan = self.plan
        if (self.outcome is not Outcome.SUCCESS or not isinstance(plan, FanoutHandoffPlan)
                or self.joint_match_status != "MATCHED" or self.joint_match_quantity != plan.quantity
                or self.source is None or self.source.filled_quantity != plan.quantity
                or not self.source.history_complete or self.source.unknown_reasons
                or len(self.receivers) != len(plan.receivers)
                or len(self.receiver_matches) != len(plan.receivers)):
            return False
        for leg, order, (status, matched) in zip(self.receivers, plan.receivers, self.receiver_matches):
            if (leg is None or leg.filled_quantity != order.quantity or status != "MATCHED"
                    or matched != order.quantity or not leg.history_complete or leg.unknown_reasons):
                return False
        return True

    @property
    def receiver_account_indices(self) -> tuple[int, ...]:
        plan = self.plan
        return tuple(order.account_index for order in plan.receivers) if isinstance(plan, FanoutHandoffPlan) else ()

    def as_dict(self) -> dict[str, Any]:
        plan = self.plan
        operation_mode = plan.operation_mode if plan is not None else self.operation_mode
        receiver_ids = set(self.receiver_account_indices)
        if not receiver_ids:
            receiver_ids = {leg.account_index for leg in self.receivers if leg is not None}
        source_id = None if self.source is None else self.source.account_index
        value: dict[str, Any] = {
            "outcome": self.outcome.value,
            "phase": self.phase.value,
            "run_id": self.run_id,
            "operation_mode": operation_mode.value,
            "mutual_execution_proven": self.mutual_execution_proven,
            "plan": None if plan is None else plan.as_dict(),
            "source": _leg_payload(self.source, receiver_ids, self.joint_match_quantity, self.joint_match_status),
        }
        for position, leg in enumerate(self.receivers, 1):
            status, matched = (self.receiver_matches[position - 1] if position - 1 < len(self.receiver_matches)
                               else ("UNKNOWN", Decimal(0)))
            value[receiver_leg(position)] = _leg_payload(
                leg, set() if source_id is None else {source_id}, matched, status)
        value.update({
            "reason": self.reason,
            "unknown_reasons": list(self.unknown_reasons),
            "joint_trade_match": {"status": self.joint_match_status,
                                  "quantity": _wire_decimal(self.joint_match_quantity)},
            "economic_status": self.economic_status,
            "economic_findings": list(self.economic_findings),
            "findings": list(self.findings),
            "retryable_pair": self.retryable_pair,
            "attempt_index": self.attempt_index,
            "latency": None if self.latency is None else dict(self.latency),
            "priority_guard": None if self.priority_guard is None else dict(self.priority_guard),
            "fanout": {
                "receiver_count": len(self.receivers),
                "receivers": [
                    {"leg": receiver_leg(position),
                     "account_index": None if leg is None else leg.account_index,
                     "match_status": status, "matched_quantity": _wire_decimal(matched)}
                    for position, (leg, (status, matched)) in enumerate(
                        zip(self.receivers, self.receiver_matches), 1)
                ],
            },
        })
        return value


@dataclass(slots=True, repr=False)
class FanoutPreflightContext:
    """One fan-out plan's original observations, carried across the final quote."""

    config: FanoutHandoffConfig
    metadata: MarketMetadata
    source: AccountSnapshot
    receivers: tuple[AccountSnapshot, ...]
    reserved_nonces: Mapping[int, Any]
    nonce_deadline: float | None = None
    used: bool = False

    def claim(self, config: FanoutHandoffConfig) -> tuple[MarketMetadata, AccountSnapshot, tuple[AccountSnapshot, ...]]:
        if self.used or config != self.config:
            raise PreflightBlocked("prepared preflight context is consumed or bound to another plan")
        self.used = True
        return self.metadata, self.source, self.receivers


def fanout_binding(config: FanoutHandoffConfig, client: HandoffClient) -> dict[str, Any]:
    binding = _config_binding(config, client)
    binding["fanout_receivers"] = [item.as_dict() for item in config.receivers]
    return binding


def _zero_mutation_all(source: LegReconciliation, receivers: Sequence[LegReconciliation]) -> bool:
    return all(HandoffEngine._zero_mutation(source, leg) for leg in receivers) and bool(receivers)


def _fanout_economic_findings(source: LegReconciliation, receivers: Sequence[LegReconciliation]) -> tuple[str, tuple[str, ...]]:
    findings: list[str] = []
    has_unknown = False
    has_evidence = False
    for label, leg in (("source", source), *((receiver_leg(i), leg) for i, leg in enumerate(receivers, 1))):
        if not leg.trades:
            if _leg_proves_no_execution(leg):
                findings.append(f"{label} no execution proven; no execution fees were due")
                has_evidence = True
            else:
                has_unknown = True
            continue
        has_evidence = True
        fee_total = leg.fee_total
        if fee_total is None:
            has_unknown = True
            findings.append(f"{label} fee economics UNKNOWN: at least one official receipt has no fee")
        else:
            findings.append(f"{label} observed gross {leg.gross_notional} and fee {fee_total}")
    if has_unknown:
        return "UNKNOWN", tuple(findings)
    return ("PROVEN" if has_evidence else "UNKNOWN"), tuple(findings)


class FanoutHandoffEngine(HandoffEngine):
    """Execute or refuse exactly one fan-out attempt; 1 -> 1 configs are delegated."""

    async def _execute_locked(self, config: HandoffConfig, journal: DurableJournal) -> HandoffResult:
        if not isinstance(config, FanoutHandoffConfig):
            return await super()._execute_locked(config, journal)
        return await self._execute_fanout(config, journal)

    def _refused(self, config: FanoutHandoffConfig, run_id: str, reason: str, *,
                 outcome: Outcome = Outcome.UNKNOWN, phase: Phase = Phase.RECONCILIATION,
                 unknown: bool = True) -> FanoutHandoffResult:
        return FanoutHandoffResult(
            outcome=outcome, phase=phase, run_id=run_id, plan=None, source=None, receiver=None,
            reason=reason, unknown_reasons=(reason,) if unknown else (),
            operation_mode=config.operation_mode, attempt_index=config.attempt_index,
        )

    async def _preflight_fanout(
        self, config: FanoutHandoffConfig, journal: DurableJournal, run_id: str,
    ) -> tuple[FanoutHandoffPlan, AccountSnapshot, tuple[AccountSnapshot, ...]]:
        source_account_index = _account_from_client(self.client, "source")
        first_receiver_index = _account_from_client(self.client, "receiver")
        extras = tuple(getattr(self.client, "extra_receiver_account_indices", ()) or ())
        if (config.receiver_accounts[0] != first_receiver_index
                or tuple(config.receiver_accounts[1:]) != extras):
            raise PreflightBlocked("fan-out receivers do not match the execution client")
        if not self._supports_prepared_dispatch():
            raise PreflightBlocked("fan-out requires every order prepared before the LIMIT")
        context = getattr(self.client, "handoff_preflight_context", None)
        if context is not None:
            if not isinstance(context, FanoutPreflightContext):
                raise PreflightBlocked("prepared preflight context has an unsupported type")
            metadata, source, receivers = context.claim(config)
            self._preflight_context = context
            if (source.account_index != source_account_index
                    or tuple(item.account_index for item in receivers) != config.receiver_accounts):
                raise PreflightBlocked("prepared preflight accounts do not match the execution client")
        else:
            async def read_metadata() -> MarketMetadata:
                return _as_market(await self._bounded(self.client.market_metadata(config.market_id),
                                                      "market metadata read"))

            async def read_account(index: int, label: str) -> AccountSnapshot:
                return _as_account(await self._bounded(self.client.account_snapshot(index, config.market_id), label))

            tasks = [asyncio.create_task(read_metadata()),
                     asyncio.create_task(read_account(source_account_index, "source account read")),
                     *(asyncio.create_task(read_account(index, f"{receiver_leg(position)} account read"))
                       for position, index in enumerate(config.receiver_accounts, 1))]
            try:
                values = await asyncio.gather(*tasks)
            except BaseException:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                raise
            metadata, source, receivers = values[0], values[1], tuple(values[2:])
        now = self.clock.now()
        config.validate_fanout(metadata, source, receivers, now)
        if ((config.direction.source_side == "BUY" and config.receiver_worst_price > config.source_limit_price)
                or (config.direction.source_side == "SELL" and config.receiver_worst_price < config.source_limit_price)):
            raise PreflightBlocked("receiver price bound cannot execute the source limit")
        quantity_int = decimal_to_integer(config.quantity, metadata.size_decimals, "quantity")
        source_price_int = decimal_to_integer(config.source_limit_price, metadata.price_decimals, "source_limit_price")
        receiver_price_int = decimal_to_integer(config.receiver_worst_price, metadata.price_decimals, "receiver_worst_price")
        expiry_ms = int(now * 1000) + config.source_order_lifetime_seconds * 1000
        closing = config.operation_mode is OperationMode.PAIRED_CLOSING
        receiver_plans = tuple(
            OrderPlan(
                account_index=item.account_index,
                market_id=config.market_id,
                side=config.direction.receiver_side,
                quantity=item.quantity,
                quantity_int=decimal_to_integer(item.quantity, metadata.size_decimals, "fan-out part"),
                price=config.receiver_worst_price,
                price_int=receiver_price_int,
                order_type="MARKET",
                time_in_force="IOC",
                reduce_only=closing,
                order_expiry_ms=0,
                client_order_index=_client_order_index(run_id, receiver_leg(position)),
            )
            for position, item in enumerate(config.receivers, 1)
        )
        plan = FanoutHandoffPlan(
            run_id=run_id,
            source=OrderPlan(
                account_index=source.account_index,
                market_id=config.market_id,
                side=config.direction.source_side,
                quantity=config.quantity,
                quantity_int=quantity_int,
                price=config.source_limit_price,
                price_int=source_price_int,
                order_type="LIMIT",
                time_in_force="POST_ONLY",
                reduce_only=closing,
                order_expiry_ms=expiry_ms,
                client_order_index=_client_order_index(run_id, "source"),
            ),
            receivers=receiver_plans,
            source_position_before=source.signed_position,
            receiver_positions_before=tuple(item.signed_position for item in receivers),
            direction=config.direction,
            quantity=config.quantity,
            created_at=now,
            source_fee_rate=metadata.source_fee_rate,
            receiver_fee_rate=metadata.receiver_fee_rate,
            source_identity=source.source_identity,
            receiver_identities=tuple(item.source_identity for item in receivers),
            metadata_observed_at=metadata.observed_at,
            operation_mode=config.operation_mode,
            defer_incremental_margin_calculation=config.defer_incremental_margin_calculation,
        )
        journal.append("PREFLIGHT_PROVED", {
            "market_id": metadata.market_id,
            "symbol": metadata.symbol,
            "source_account_index": source.account_index,
            "receiver_account_index": receivers[0].account_index,
            "receiver_account_indices": [item.account_index for item in receivers],
            "operation_mode": config.operation_mode.value,
            "source_identity": source.source_identity,
            "receiver_identity": receivers[0].source_identity,
            "receiver_identities": [item.source_identity for item in receivers],
            "source_position": str(source.signed_position),
            "receiver_position": str(receivers[0].signed_position),
            "receiver_positions": [str(item.signed_position) for item in receivers],
            "context_reused_before_quote": context is not None,
            "source_observed_at": source.observed_at,
            "receiver_observed_at": receivers[0].observed_at,
            "receivers_observed_at": [item.observed_at for item in receivers],
            "binding": fanout_binding(config, self.client),
        }, run_id=run_id)
        return plan, source, receivers

    async def _prepare_all(self, plans: Sequence[OrderPlan]) -> list[Any]:
        """Prepare every leg concurrently; one failure invalidates all unsent preparations."""
        tasks = [asyncio.create_task(self._prepare_order(plan)) for plan in plans]
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        except BaseException:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for task in tasks:
                if not task.cancelled() and task.exception() is None:
                    await self._invalidate_prepared(task.result())
            raise
        failure = next((item for item in results if isinstance(item, BaseException)), None)
        if failure is not None:
            for item in results:
                if not isinstance(item, BaseException):
                    await self._invalidate_prepared(item)
            raise failure
        return list(results)

    def _guard_payload(self, plan: FanoutHandoffPlan, source_order: OrderSnapshot | None,
                       source: AccountSnapshot, receivers: Sequence[AccountSnapshot]) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "source_account": self._account_observation_payload(source),
            "receiver_account": self._account_observation_payload(receivers[0]),
            "receiver_accounts": [self._account_observation_payload(item) for item in receivers],
            "fanout_receiver_count": len(receivers),
        }
        if source_order is not None:
            payload["source_order"] = self._order_observation_payload(source_order)
        return payload

    async def _execute_fanout(self, config: FanoutHandoffConfig, journal: DurableJournal) -> HandoffResult:
        self._source_dispatch_monotonic = None
        self._cancel_preparation_task = None
        self._prepared_cancel = None
        self._cancel_preparation_order_id = None
        self._preflight_context = None
        self._configured_poll_limit = config.max_poll_count
        self._configured_poll_interval = config.poll_interval_seconds
        self._configured_order_timeout = config.order_timeout_seconds
        self._configured_reconcile_timeout = config.reconcile_timeout_seconds
        self._configured_freshness = config.freshness_seconds
        self._configured_request_timeout = config.request_timeout_seconds
        self._visibility_checks_incremental_margin = config.operation_mode is not OperationMode.PAIRED_CLOSING
        if journal.has_unresolved_mutation():
            # A fan-out attempt is never resumed in place: operator recovery
            # proves every earlier intent terminal before any new order.
            reason = "fan-out attempt journal has an unresolved mutation; use operator recovery"
            journal.append("RESTART_RECONCILIATION_BLOCKED", {"reason": reason}, run_id=journal.run_id)
            return self._refused(config, journal.run_id, reason)
        if journal.has_completed_mutation():
            return self._refused(config, journal.run_id,
                                 "journal already contains a completed attempt; use a new owner-only journal path")
        run_id = journal.run_id
        try:
            binding = fanout_binding(config, self.client)
        except Exception as exc:
            reason = sanitize_exception(exc)
            journal.append("PREFLIGHT_BLOCKED", {"reason": reason}, run_id=run_id)
            return self._refused(config, run_id, reason, outcome=Outcome.FAILED_PREFLIGHT_BLOCKED, phase=Phase.PREFLIGHT)
        from .provenance import capture_provenance
        journal.append("ATTEMPT_STARTED", {"run_id": run_id, "binding": binding,
                                            "runtime_provenance": capture_provenance(binding)})
        try:
            plan, source, receivers = await self._preflight_fanout(config, journal, run_id)
        except Exception as exc:
            reason = _safe_preflight_reason(exc)
            journal.append("PREFLIGHT_BLOCKED", {"reason": reason}, run_id=run_id)
            return self._refused(config, run_id, reason, outcome=Outcome.FAILED_PREFLIGHT_BLOCKED,
                                 phase=Phase.PREFLIGHT, unknown=False)
        plan_payload = {"plan": plan.as_dict(), "config": binding, "review_required": True}
        journal.append("PLAN_READY", plan_payload, run_id=run_id)
        bind_stream_order = getattr(self.client, "bind_read_stream_order", None)
        if callable(bind_stream_order):
            for role, leg in (("source", plan.source),
                              *((receiver_leg(i), order) for i, order in enumerate(plan.receivers, 1))):
                bind_stream_order(leg.account_index, leg.market_id, leg.client_order_index,
                                  run_id=run_id, phase=config.operation_mode.value, role=role,
                                  attempt_index=config.attempt_index)
        if not config.operator_execution_opt_in or not config.operator_plan_reviewed:
            journal.append("PREVIEW", plan_payload, run_id=run_id)
            return FanoutHandoffResult(
                outcome=Outcome.PREVIEW, phase=Phase.PREFLIGHT, run_id=run_id, plan=plan,
                source=None, receiver=None,
                reason="explicit plan review and execution opt-in are required; no signing or mutation was attempted",
                operation_mode=config.operation_mode, attempt_index=config.attempt_index,
            )

        k = len(plan.receivers)
        ack_admission = config.receiver_admission == "ack"
        unknown_reasons: list[str] = []
        latency: dict[str, Any] = {}
        pre_dispatch_transient = True
        pre_source_stream: str | None = None
        source_receipt: MutationReceipt | None = None
        source_order: OrderSnapshot | None = None
        source_order_id: str | None = None
        source_dispatch_attempted = False
        source_dispatch_intent_at: float | None = None
        priority_guard: dict[str, Any] | None = None
        guard_event_recorded = False
        prepared_source: Any | None = None
        prepared_receivers: list[Any] = [None] * k
        prepared_source_plan: OrderPlan | None = None
        prepared_receiver_plans: list[OrderPlan | None] = [None] * k
        receiver_dispatched = [False] * k
        receiver_receipts: list[MutationReceipt | None] = [None] * k
        receiver_orders: list[OrderSnapshot | None] = [None] * k
        receiver_mutation_observations: tuple[Any, ...] = (source, *receivers, plan.metadata_observed_at)
        ws_anchor = None
        observations = (source, *receivers, plan.metadata_observed_at, config.source_quote_observed_at)

        preparation_started = time.perf_counter()
        try:
            prepared_source_plan = self._mutation_plan(plan.source, config, observations=observations)
            for index, order in enumerate(plan.receivers):
                prepared_receiver_plans[index] = self._mutation_plan(order, config, observations=observations)
            prepared = await self._prepare_all([prepared_source_plan, *prepared_receiver_plans])
            prepared_source, prepared_receivers = prepared[0], list(prepared[1:])
        except Exception as exc:
            unknown_reasons.append(f"order preparation failed before source exposure: {sanitize_exception(exc)}")
            if not isinstance(exc, (TimeoutError, asyncio.TimeoutError, ConnectionError, OSError)):
                pre_dispatch_transient = False
        finally:
            latency["paired_preparation_seconds"] = max(0.0, time.perf_counter() - preparation_started)
        latency.update(_prepared_timing_values(prepared_source, "source"))
        if prepared_receivers and prepared_receivers[0] is not None:
            latency.update(_prepared_timing_values(prepared_receivers[0], "receiver"))
        if (not unknown_reasons and config.max_quote_age_seconds is not None
                and self.clock.now() - config.source_quote_observed_at > config.max_quote_age_seconds):
            unknown_reasons.append(PRE_DISPATCH_QUOTE_EXPIRED_REASON)
        if not unknown_reasons:
            try:
                ws_anchor = self.client.begin_ws_admission()
            except Exception as exc:
                pre_source_stream = "TRANSIENT" if isinstance(exc, TransientStreamContractError) else "UNAVAILABLE"
                unknown_reasons.append(f"{PRE_SOURCE_STREAM_REASON}{sanitize_exception(exc)}")

        async def release_receivers() -> None:
            for item in prepared_receivers:
                await self._invalidate_prepared(item)

        if not unknown_reasons:
            try:
                source_dispatch_attempted = True
                source_submit_started = time.perf_counter()
                source_dispatch_intent_at = self.clock.now()
                self._source_dispatch_monotonic = time.monotonic()
                latency["source_dispatch_intent_at"] = source_dispatch_intent_at
                if config.source_quote_observed_at is not None:
                    quote_age = max(0.0, source_dispatch_intent_at - config.source_quote_observed_at)
                    latency["quote_age_to_source_dispatch_seconds"] = quote_age
                    latency["source_quote_age_seconds"] = quote_age
                journal.append("SOURCE_DISPATCH_INTENT", {"plan": prepared_source_plan.as_dict()}, run_id=run_id)
                self._stream_milestone("send_entered", plan.source)
                source_receipt = _as_receipt(await self._submit_order(prepared_source_plan, prepared=prepared_source))
                latency["source_submit_ack_seconds"] = max(0.0, time.perf_counter() - source_submit_started)
                latency["source_dispatch_ack_at"] = self.clock.now()
                self._stream_milestone("ack_parsed", plan.source, source_receipt.order_id)
                if source_receipt.accepted:
                    self._start_cancel_nonce(plan.source)
                journal.append("SOURCE_DISPATCH_RESULT", {
                    "operation_mode": plan.operation_mode.value,
                    "accepted": source_receipt.accepted,
                    "order_id": source_receipt.order_id,
                    "tx_hash": source_receipt.tx_hash,
                    "response_code": source_receipt.response_code,
                    "error": source_receipt.error,
                }, run_id=run_id)
            except Exception as exc:
                unknown_reasons.append(f"source dispatch outcome unknown: {sanitize_exception(exc)}")
                journal.append("SOURCE_DISPATCH_UNKNOWN", {
                    "operation_mode": plan.operation_mode.value, "reason": unknown_reasons[-1],
                }, run_id=run_id)
                await release_receivers()

        if source_receipt is not None and not ack_admission:
            if not source_receipt.accepted:
                unknown_reasons.append("source dispatch was rejected")
            visibility_started = time.perf_counter()
            if source_receipt.accepted:
                try:
                    source_order = await asyncio.wait_for(
                        self.client.wait_order_observation(
                            plan.source.account_index, plan.source.market_id,
                            plan.source.client_order_index, source_receipt.order_id,
                            min(config.order_timeout_seconds, 1.0), terminal_only=False),
                        timeout=min(config.order_timeout_seconds, 1.0),
                    )
                except Exception as exc:
                    unknown_reasons.append("WS_ADMISSION_STOP: source observation unavailable: " + sanitize_exception(exc))
                latency["source_visibility_lookup_seconds"] = time.perf_counter() - visibility_started
                if source_order is not None:
                    source_order_id = source_order.order_id
                    bind_cancel = getattr(self.client, "bind_read_stream_cancel", None)
                    if callable(bind_cancel) and self._order_matches(source_order, plan.source):
                        bind_cancel(plan.source.account_index, plan.source.market_id,
                                    plan.source.client_order_index, source_order_id)
                    self._start_cancel_preparation(plan.source, source_order)
                if source_order is None and not unknown_reasons:
                    unknown_reasons.append("WS_ADMISSION_STOP: source event absent")
                elif source_order is not None and not self._order_matches(source_order, plan.source):
                    unknown_reasons.append("source order identity/parameters conflict with plan")
                elif source_order is not None and source_order.filled_quantity > 0:
                    unknown_reasons.append("source fill observed before receiver dispatch")
                    journal.append("SOURCE_FILLED_BEFORE_RECEIVER", {
                        "order_id": source_order.order_id, "filled_quantity": str(source_order.filled_quantity),
                    }, run_id=run_id)
                    await self._cancel_if_safe(plan.source, source_order, journal, run_id, unknown_reasons,
                                               expected_order_id=source_order_id)
                elif source_order is not None and (not source_order.active
                                                   or source_order.remaining_quantity != plan.quantity):
                    if self._is_exact_canceled_post_only_zero_fill(source_order, plan.source):
                        reason = "source canceled-post-only zero-fill; receiver was not dispatched"
                        if reason not in unknown_reasons:
                            unknown_reasons.append(reason)
                        journal.append("SOURCE_CANCELED_POST_ONLY_ZERO_FILL", {
                            "order_id": source_order.order_id, "status": source_order.status,
                            "filled_quantity": str(source_order.filled_quantity),
                            "remaining_quantity": str(source_order.remaining_quantity),
                            "terminal_reason": "canceled-post-only",
                        }, run_id=run_id)
                    else:
                        unknown_reasons.append("source order did not prove exact resting quantity")
                    await self._cancel_if_safe(plan.source, source_order, journal, run_id, unknown_reasons,
                                               expected_order_id=source_order_id)
            latency["source_visibility_seconds"] = max(0.0, time.perf_counter() - visibility_started)

        if ack_admission and source_receipt is not None:
            source_order_id = source_receipt.order_id
        if ack_admission and source_dispatch_attempted and (
                source_receipt is None or not source_receipt.accepted
                or type(source_receipt.response_code) is not int or source_receipt.response_code != 200
                or source_receipt.error is not None):
            unknown_reasons.append("ACK_ADMISSION_STOP: positive application ACK is not proved")

        if not unknown_reasons and (source_order is not None or ack_admission):
            local_started = time.perf_counter()
            guard_started_at = self.clock.now()
            ws_evidence: dict[str, Any] = {}
            try:
                source_order, local_book = self.client.ws_admission_view(ws_anchor, plan.source, source_order_id)
                if source_order is None and not ack_admission:
                    raise PreflightBlocked("exact source event is missing/stale")
                if source_order is not None:
                    ws_evidence["source_order"] = self._order_observation_payload(source_order)
                    if not self._source_exact_resting(source_order, plan.source, source_order_id):
                        raise PreflightBlocked("exact source is no longer fully resting")
                    if source_order.observed_at < source_dispatch_intent_at:
                        raise PreflightBlocked("source observation predates dispatch")
                    source_order_id = source_order.order_id
                ws_evidence["source_resting_confirmed"] = source_order is not None
                now = self.clock.now()
                if not all(self._snapshot_fresh(value, now, config.freshness_seconds) for value in (source, *receivers)):
                    raise PreflightBlocked("pre-LIMIT account evidence expired")
                if config.max_source_to_receiver_seconds is not None and (
                        time.monotonic() - self._source_dispatch_monotonic > config.max_source_to_receiver_seconds
                        or now - source_dispatch_intent_at > config.max_source_to_receiver_seconds):
                    raise PreflightBlocked("source-to-receiver latency budget expired")
                public_book = _coerce_public_book(local_book, config, now)
                ws_evidence.update(self._ws_l2_evidence(public_book, plan.source, now))
                ws_reason = {
                    "BETTER_PRICE": "L2 shows better-priced volume",
                    "SAME_PRICE_EXTRA_VOLUME": "L2 shows extra volume at source price",
                    "SOURCE_LEVEL_SMALLER": "L2 source level is smaller than exact private remainder",
                }.get(ws_evidence["ws_l2_status"])
                if ws_reason is not None:
                    raise PreflightBlocked(ws_reason)
                priority_guard = {
                    "status": "ACK_ADMITTED" if ack_admission else "WS_CONFIRMED", "priority_status": "UNPROVED",
                    "receiver_admission": config.receiver_admission, "priority_proof_admitted": False,
                    "priority_reason": ("positive ACK + local L2 veto; source may be absent; owner/FIFO not proved"
                                        if ack_admission else "exact private source + fresh L2 veto; owner/FIFO not proved"),
                    **self._guard_payload(plan, None, source, receivers),
                    **ws_evidence,
                    "account_evidence_basis": "pre-LIMIT snapshots",
                }
                receiver_mutation_observations = tuple(
                    value for value in (source, *receivers, source_order, public_book, plan.metadata_observed_at)
                    if value is not None)
            except Exception as exc:
                reason = str(exc) if isinstance(exc, PreflightBlocked) else sanitize_exception(exc)
                unknown_reasons.append(f"WS_ADMISSION_STOP: {reason}")
                priority_guard = {"status": "ACK_REFUSED" if ack_admission else "WS_REFUSED",
                                  "priority_status": "UNPROVED", "receiver_admission": config.receiver_admission,
                                  "priority_proof_admitted": False, "priority_reason": unknown_reasons[-1],
                                  "fanout_receiver_count": k, **ws_evidence}
            latency["source_to_receiver_decision_seconds"] = max(0.0, self.clock.now() - source_dispatch_intent_at)
            latency["pre_receiver_checks_seconds"] = time.perf_counter() - local_started
            latency["receiver_admission_seconds"] = latency["pre_receiver_checks_seconds"]
            latency["receiver_admission_at"] = self.clock.now()
            journal.append("PRE_RECEIVER_GUARD", {**priority_guard, "request_started_at": guard_started_at,
                                                  "request_finished_at": self.clock.now()}, run_id=run_id)
            self._stream_milestone("admission_decision", plan.source, source_order_id)
            guard_event_recorded = True

        if not guard_event_recorded:
            priority_guard = self._priority_guard_unavailable(
                plan, source_order_id, "paired pre-receiver admission did not obtain a complete guard window", None)
            priority_guard["fanout_receiver_count"] = k
            if self._is_exact_canceled_post_only_zero_fill(source_order, plan.source):
                priority_guard["priority_reason"] = ("source canceled-post-only before receiver admission; "
                                                     "priority proof was not admitted")
                priority_guard["terminal_reason"] = "canceled-post-only"
                priority_guard["priority_proof_admitted"] = False
            if source_order is not None:
                priority_guard["source_order"] = self._order_observation_payload(source_order)
            priority_guard["admission_reasons"] = list(unknown_reasons[:8])
            if pre_source_stream is not None:
                priority_guard["pre_source_stream"] = pre_source_stream
            if (not source_dispatch_attempted and unknown_reasons
                    and all(_is_pre_dispatch_reason(reason) for reason in unknown_reasons)
                    and not any(str(reason).startswith("WS_ADMISSION_STOP: ") for reason in unknown_reasons)):
                priority_guard["pre_dispatch"] = "TRANSIENT" if pre_dispatch_transient else "FAILED"
            journal.append("PRE_RECEIVER_GUARD", {**priority_guard, "request_started_at": self.clock.now(),
                                                  "request_finished_at": self.clock.now(),
                                                  "local_observed_at": self.clock.now(),
                                                  "latency_seconds": 0.0}, run_id=run_id)
            guard_event_recorded = True

        if unknown_reasons:
            await release_receivers()
            cleanup_just_read = False
            if source_order is None and source_dispatch_attempted:
                try:
                    source_order = await self._lookup_cleanup_order(
                        plan.source,
                        source_order_id if source_order_id is not None else (
                            source_receipt.order_id if source_receipt is not None else None),
                        journal, run_id)
                    cleanup_just_read = source_order is not None
                except Exception as exc:
                    unknown_reasons.append(f"source cancellation lookup unresolved: {sanitize_exception(exc)}")
            if source_order is not None:
                await self._cancel_if_safe(
                    plan.source, source_order, journal, run_id, unknown_reasons,
                    expected_order_id=(source_order_id if source_order_id is not None else (
                        source_receipt.order_id if source_receipt is not None else None)),
                    exact_order_just_read=cleanup_just_read)
            reconciliation_started = time.perf_counter()
            source_leg, receiver_legs = await self._reconcile_fanout(
                config, plan, source, receivers,
                source_order_id=source_order_id if source_order_id is not None else (
                    source_order.order_id if source_order else (source_receipt.order_id if source_receipt else None)),
                receiver_order_ids=[None] * k, source_dispatched=source_dispatch_attempted,
                receiver_dispatched=receiver_dispatched, run_id=run_id, journal=journal,
                unknown_reasons=unknown_reasons)
            latency["reconciliation_seconds"] = max(0.0, time.perf_counter() - reconciliation_started)
            retryable = self._retryable_fanout_after_guard(plan, source_leg, receiver_legs, unknown_reasons, priority_guard)
            return await self._finish_fanout(
                journal, run_id, plan, source_leg, receiver_legs, unknown_reasons, config=config, binding=binding,
                forced_outcome=Outcome.PARTIAL if retryable else None, retryable=retryable,
                latency=latency, priority_guard=priority_guard)

        # Every receiver intent is durable before any receiver order is sent.
        receiver_intent_at = self.clock.now()
        latency["receiver_dispatch_intent_at"] = receiver_intent_at
        if source_dispatch_intent_at is not None:
            latency["source_to_receiver_intent_seconds"] = max(0.0, receiver_intent_at - source_dispatch_intent_at)
        if latency.get("source_dispatch_ack_at") is not None:
            latency["source_ack_to_receiver_intent_seconds"] = max(0.0, receiver_intent_at - latency["source_dispatch_ack_at"])
        dispatch_plans: list[OrderPlan] = []
        deadlines: list[float | None] = []
        for index, order in enumerate(plan.receivers):
            admission_plan = self._mutation_plan(order, config, observations=receiver_mutation_observations)
            dispatch_plan = prepared_receiver_plans[index] or admission_plan
            deadline = admission_plan.mutation_deadline_monotonic
            prepared_deadline = None if prepared_receiver_plans[index] is None else prepared_receiver_plans[index].mutation_deadline_monotonic
            if deadline is not None and prepared_deadline is not None:
                deadline = min(deadline, prepared_deadline)
            dispatch_plans.append(dispatch_plan)
            deadlines.append(deadline)
        for position, dispatch_plan in enumerate(dispatch_plans, 1):
            journal.append(f"{receiver_event_prefix(position)}_DISPATCH_INTENT",
                           {"plan": dispatch_plan.as_dict(), "receiver_position": position}, run_id=run_id)
            self._stream_milestone("send_entered", plan.receivers[position - 1])
            receiver_dispatched[position - 1] = True
        submit_started = time.perf_counter()

        async def send(index: int) -> MutationReceipt:
            return _as_receipt(await self._submit_order(dispatch_plans[index], prepared=prepared_receivers[index],
                                                        final_deadline=deadlines[index]))

        outcomes = await asyncio.gather(*(send(index) for index in range(k)), return_exceptions=True)
        ack_seconds = max(0.0, time.perf_counter() - submit_started)
        latency["receiver_submit_ack_seconds"] = ack_seconds
        latency["receiver_dispatch_ack_at"] = self.clock.now()
        for position, value in enumerate(outcomes, 1):
            prefix = receiver_event_prefix(position)
            if isinstance(value, BaseException):
                if isinstance(value, asyncio.CancelledError):
                    raise value
                reason = f"{receiver_leg(position)} dispatch outcome unknown: {sanitize_exception(value)}"
                unknown_reasons.append(reason)
                journal.append(f"{prefix}_DISPATCH_UNKNOWN", {"operation_mode": plan.operation_mode.value,
                                                              "reason": reason, "receiver_position": position},
                               run_id=run_id)
                continue
            receiver_receipts[position - 1] = value
            self._stream_milestone("ack_parsed", plan.receivers[position - 1], value.order_id)
            journal.append(f"{prefix}_DISPATCH_RESULT", {
                "operation_mode": plan.operation_mode.value, "accepted": value.accepted,
                "order_id": value.order_id, "tx_hash": value.tx_hash,
                "response_code": value.response_code, "error": value.error, "receiver_position": position,
            }, run_id=run_id)

        async def observe(index: int) -> OrderSnapshot | None:
            receipt = receiver_receipts[index]
            if receipt is None:
                return None
            return await self._poll_order(plan.receivers[index], receipt.order_id, journal, run_id, require_terminal=True)

        observed = await asyncio.gather(*(observe(index) for index in range(k)), return_exceptions=True)
        for index, value in enumerate(observed):
            label = receiver_leg(index + 1)
            if receiver_receipts[index] is None:
                continue
            if isinstance(value, BaseException):
                if isinstance(value, asyncio.CancelledError):
                    raise value
                unknown_reasons.append(f"{label} order identity or terminal status is unresolved")
                continue
            receiver_orders[index] = value
            if value is None:
                unknown_reasons.append(f"{label} order identity or terminal status is unresolved")
            elif not self._order_matches(value, plan.receivers[index]):
                unknown_reasons.append(f"{label} order identity/parameters conflict with plan")
            elif not value.terminal:
                unknown_reasons.append(f"{label} IOC did not prove terminal status")
            elif index == 0:
                latency["receiver_terminal_observed_at"] = value.observed_at
                if value.filled_quantity > 0:
                    latency["receiver_fill_observed_at"] = value.observed_at

        if ack_admission and source_order is None and source_dispatch_attempted:
            source_order = await self._poll_order(plan.source, source_receipt.order_id if source_receipt else None,
                                                  journal, run_id, require_terminal=False)
            if source_order is not None:
                source_order_id = source_order.order_id
        if source_order is not None:
            try:
                current_source = await self._lookup_order(plan.source, source_order.order_id)
            except Exception as exc:
                current_source = None
                unknown_reasons.append(f"source cancellation lookup unresolved: {sanitize_exception(exc)}")
            if current_source is not None:
                await self._cancel_if_safe(plan.source, current_source, journal, run_id, unknown_reasons,
                                           expected_order_id=source_order.order_id, exact_order_just_read=True)
            elif not any("source cancellation lookup unresolved" in item for item in unknown_reasons):
                unknown_reasons.append("source order disappeared before cancellation reconciliation")

        reconciliation_started = time.perf_counter()
        source_leg, receiver_legs = await self._reconcile_fanout(
            config, plan, source, receivers,
            source_order_id=source_order_id if source_order_id is not None else (
                source_order.order_id if source_order else (source_receipt.order_id if source_receipt else None)),
            receiver_order_ids=[
                (receiver_orders[index].order_id if receiver_orders[index] is not None else
                 (receiver_receipts[index].order_id if receiver_receipts[index] is not None else None))
                for index in range(k)],
            source_dispatched=source_dispatch_attempted, receiver_dispatched=receiver_dispatched,
            run_id=run_id, journal=journal, unknown_reasons=unknown_reasons)
        latency["reconciliation_seconds"] = max(0.0, time.perf_counter() - reconciliation_started)
        if source_leg.order is not None and source_leg.order.terminal:
            self._stream_milestone("cancel_reconciled", plan.source, source_leg.order.order_id)
        return await self._finish_fanout(journal, run_id, plan, source_leg, receiver_legs, unknown_reasons,
                                         config=config, binding=binding, latency=latency,
                                         priority_guard=priority_guard)

    async def _reconcile_fanout(
        self, config: FanoutHandoffConfig, plan: FanoutHandoffPlan, source: AccountSnapshot,
        receivers: Sequence[AccountSnapshot], *, source_order_id: str | None,
        receiver_order_ids: Sequence[str | None], source_dispatched: bool,
        receiver_dispatched: Sequence[bool], run_id: str, journal: DurableJournal,
        unknown_reasons: list[str],
    ) -> tuple[LegReconciliation, list[LegReconciliation]]:
        journal.append("RECONCILIATION_STARTED", {
            "source_order_id": source_order_id,
            "receiver_order_id": receiver_order_ids[0],
            "receiver_order_ids": list(receiver_order_ids),
        }, run_id=run_id)
        source_leg = await self._reconcile_leg(config, plan.source, source, source_order_id, source_dispatched,
                                               plan.source_identity, run_id, journal, unknown_reasons)
        legs = []
        for index, order in enumerate(plan.receivers):
            legs.append(await self._reconcile_leg(
                config, order, receivers[index], receiver_order_ids[index], receiver_dispatched[index],
                plan.receiver_identities[index], run_id, journal, unknown_reasons))
        return source_leg, legs

    @staticmethod
    def _retryable_fanout_after_guard(
        plan: FanoutHandoffPlan, source: LegReconciliation, receivers: Sequence[LegReconciliation],
        unknown_reasons: Sequence[str], priority_guard: Mapping[str, Any] | None,
    ) -> bool:
        """The 1 -> 1 retry rule, required on every receiver: nothing was sent to any of them."""
        if priority_guard is None:
            return False
        if priority_guard.get("pre_source_stream") == "TRANSIENT":
            return (len(unknown_reasons) == 1 and str(unknown_reasons[0]).startswith(PRE_SOURCE_STREAM_REASON)
                    and _zero_mutation_all(source, receivers))
        if priority_guard.get("pre_dispatch") == "TRANSIENT":
            return (bool(unknown_reasons)
                    and all(_is_pre_dispatch_reason(reason) and not str(reason).startswith("WS_ADMISSION_STOP: ")
                            for reason in unknown_reasons)
                    and _zero_mutation_all(source, receivers))
        ws_liquidity_veto = (
            (priority_guard.get("status"), priority_guard.get("receiver_admission"))
            in {("WS_REFUSED", "ws_confirmed"), ("ACK_REFUSED", "ack")}
            and priority_guard.get("ws_l2_status") in {"BETTER_PRICE", "SAME_PRICE_EXTRA_VOLUME"}
        )
        terminal_source = HandoffEngine._is_exact_canceled_post_only_zero_fill(source.order, plan.source)
        if ws_liquidity_veto:
            if source.order is None or not source.order.status.lower().startswith("canceled"):
                return False
            expected = priority_guard.get("priority_reason")
            if (not isinstance(expected, str) or not expected.startswith("WS_ADMISSION_STOP: ")
                    or not unknown_reasons or any(reason != expected for reason in unknown_reasons)):
                return False
        elif terminal_source:
            if any(str(reason) != "source canceled-post-only zero-fill; receiver was not dispatched"
                   for reason in unknown_reasons):
                return False
        else:
            return False
        if (not source.dispatched or source.filled_quantity != 0 or source.trades
                or not source.history_complete or source.unknown_reasons
                or source.order is None or not source.order.terminal
                or source.order.filled_quantity != 0 or source.order.remaining_quantity != 0
                or source.position_after != source.position_before):
            return False
        return all(not leg.dispatched and leg.filled_quantity == 0 and not leg.trades and leg.history_complete
                   and not leg.unknown_reasons and leg.position_after == leg.position_before for leg in receivers)

    @staticmethod
    def _known_source_only_partial_fanout(
        plan: FanoutHandoffPlan, source: LegReconciliation, receivers: Sequence[LegReconciliation],
        unknown_reasons: Sequence[str],
    ) -> bool:
        provisional_reasons = {"source fill observed before receiver dispatch",
                               "source order did not prove exact resting quantity"}

        def provisional(reason: str) -> bool:
            return reason in provisional_reasons or reason.startswith("WS_ADMISSION_STOP: ")

        if not unknown_reasons or not all(provisional(reason) for reason in unknown_reasons):
            return False
        if source.unknown_reasons or any(leg.unknown_reasons for leg in receivers):
            return False
        if not source.dispatched or any(leg.dispatched for leg in receivers):
            return False
        if not source.history_complete or not all(leg.history_complete for leg in receivers):
            return False
        if source.order is None or not source.order.terminal:
            return False
        if any(leg.order is not None or leg.trades or leg.position_after != leg.position_before for leg in receivers):
            return False
        if source.filled_quantity < 0 or source.filled_quantity > plan.quantity:
            return False
        if source.order.filled_quantity != source.filled_quantity:
            return False
        if source.filled_quantity == 0 and (
                source.trades or source.order.remaining_quantity != 0
                or not source.order.status.lower().startswith("canceled")
                or any(not reason.startswith("WS_ADMISSION_STOP: ") for reason in unknown_reasons)):
            return False
        expected = plan.source_position_before - source.filled_quantity * plan.direction.sign
        return source.position_after == expected

    def _classify_fanout(self, plan: FanoutHandoffPlan, source: LegReconciliation,
                         receivers: Sequence[LegReconciliation], unknown_reasons: Sequence[str]) -> Outcome:
        if (unknown_reasons and all(_is_pre_dispatch_reason(r) for r in unknown_reasons)
                and _zero_mutation_all(source, receivers)):
            return Outcome.FAILED_PREFLIGHT_BLOCKED
        known = {"source fill observed before receiver dispatch", "source order did not prove exact resting quantity"}
        if self._known_source_only_partial_fanout(plan, source, receivers, unknown_reasons):
            known.update(reason for reason in unknown_reasons if reason.startswith("WS_ADMISSION_STOP: "))
        unresolved = [reason for reason in (*unknown_reasons, *source.unknown_reasons,
                                            *(r for leg in receivers for r in leg.unknown_reasons))
                      if reason not in known]
        if unresolved:
            return Outcome.UNKNOWN
        if (source.filled_quantity < plan.quantity
                and not any(leg.dispatched or leg.trades for leg in receivers)):
            return Outcome.PARTIAL
        if (source.filled_quantity == plan.quantity
                and all(leg.filled_quantity == order.quantity for leg, order in zip(receivers, plan.receivers))):
            sign = plan.direction.sign
            if (source.order is not None and source.order.terminal
                    and source.position_after == plan.source_position_before - plan.quantity * sign
                    and all(leg.order is not None and leg.order.terminal
                            and leg.position_after == before + order.quantity * sign
                            for leg, order, before in zip(receivers, plan.receivers, plan.receiver_positions_before))):
                return Outcome.SUCCESS
            return Outcome.UNKNOWN
        return Outcome.PARTIAL

    async def _finish_fanout(
        self, journal: DurableJournal, run_id: str, plan: FanoutHandoffPlan, source: LegReconciliation,
        receivers: list[LegReconciliation], unknown_reasons: list[str], *, config: FanoutHandoffConfig,
        binding: Mapping[str, Any] | None = None, forced_outcome: Outcome | None = None,
        retryable: bool = False, latency: Mapping[str, Any] | None = None,
        priority_guard: Mapping[str, Any] | None = None,
    ) -> FanoutHandoffResult:
        provisional = "source order disappeared before cancellation reconciliation"
        if (provisional in unknown_reasons and source.dispatched and source.history_complete
                and not source.unknown_reasons and source.order is not None and source.order.terminal
                and self._order_matches(source.order, plan.source) and source.order_id == source.order.order_id
                and source.order.filled_quantity == source.filled_quantity
                and source.position_after == source.position_before + (
                    source.filled_quantity if plan.source.side == "BUY" else -source.filled_quantity)):
            journal.append("SOURCE_OBSERVATION_RESOLVED", {
                "reason": provisional, "order_id": source.order_id, "status": source.order.status,
                "filled_quantity": str(source.filled_quantity),
            }, run_id=run_id)
            unknown_reasons = [r for r in unknown_reasons if r != provisional]
        matches: list[tuple[str, Decimal]] = []
        match_reasons: list[str] = []
        for position, (leg, order) in enumerate(zip(receivers, plan.receivers), 1):
            status, quantity, reasons = _joint_trade_match(source, leg, order.quantity)
            matches.append((status, quantity))
            match_reasons.extend(f"{receiver_leg(position)}: {reason}" for reason in reasons)
        total = sum((quantity for _, quantity in matches), Decimal(0))
        statuses = [status for status, _ in matches]
        if all(status == "MATCHED" for status in statuses) and total == plan.quantity:
            joint_status = "MATCHED"
        elif "CONFLICTING" in statuses:
            joint_status = "CONFLICTING"
        elif total > 0:
            joint_status = "PARTIAL"
        elif "UNKNOWN" in statuses:
            joint_status = "UNKNOWN"
        else:
            joint_status = "KNOWN_ZERO"
        economic_status, economic_findings = _fanout_economic_findings(source, receivers)
        findings = tuple(dict.fromkeys((*match_reasons, *economic_findings)))
        outcome = forced_outcome or self._classify_fanout(plan, source, receivers, unknown_reasons)
        mutual_failure = outcome is Outcome.SUCCESS and (joint_status != "MATCHED" or total != plan.quantity)
        if mutual_failure:
            outcome = Outcome.PARTIAL
        reason = None if outcome is Outcome.SUCCESS else (
            unknown_reasons[0] if unknown_reasons else (
                economic_findings[0] if economic_findings else "fan-out attempt did not prove full completion"))
        if mutual_failure:
            reason = "full own-account execution was not proven for every receiver"
        result = FanoutHandoffResult(
            outcome=outcome, phase=Phase.COMPLETE, run_id=run_id, plan=plan, source=source,
            receiver=receivers[0], reason=reason, unknown_reasons=tuple(dict.fromkeys(unknown_reasons)),
            joint_match_status=joint_status, joint_match_quantity=total, economic_status=economic_status,
            economic_findings=economic_findings, findings=findings, operation_mode=plan.operation_mode,
            retryable_pair=retryable, attempt_index=config.attempt_index,
            latency=None if latency is None else dict(latency),
            priority_guard=None if priority_guard is None else dict(priority_guard),
            receivers=tuple(receivers), receiver_matches=tuple(matches),
        )
        dispatch_evidence = [
            {"event": event.event, "at": event.at, "payload": event.payload}
            for event in journal.events
            if event.run_id == run_id and (
                event.event in {"SOURCE_DISPATCH_INTENT", "SOURCE_DISPATCH_RESULT", "SOURCE_DISPATCH_UNKNOWN",
                                "CANCEL_DISPATCH_INTENT", "CANCEL_DISPATCH_RESULT", "CANCEL_DISPATCH_UNKNOWN"}
                or RECEIVER_EVENT.fullmatch(event.event))
        ]
        journal.append("COMPLETE", {
            "outcome": outcome.value,
            "operation_mode": plan.operation_mode.value,
            "reason": reason,
            "binding": dict(binding or {}),
            "source_filled_quantity": str(source.filled_quantity),
            "receiver_filled_quantity": str(receivers[0].filled_quantity),
            "receiver_filled_quantities": [str(leg.filled_quantity) for leg in receivers],
            "unknown_reasons": list(dict.fromkeys(unknown_reasons)),
            "joint_trade_match": {"status": joint_status, "quantity": str(total),
                                  "reasons": list(match_reasons),
                                  "receivers": [{"status": status, "quantity": str(quantity)}
                                                for status, quantity in matches]},
            "economic_status": economic_status,
            "economic_findings": list(economic_findings),
            "findings": list(findings),
            "retryable_pair": retryable,
            "latency": None if latency is None else dict(latency),
            "priority_guard": None if priority_guard is None else dict(priority_guard),
            "receipt": result.as_dict(),
            "dispatch_evidence": dispatch_evidence,
        }, run_id=run_id)
        return result


async def run_fanout_handoff(config: FanoutHandoffConfig, client: HandoffClient, *,
                             clock: Clock | None = None) -> HandoffResult:
    return await FanoutHandoffEngine(client, clock=clock).execute(config)


__all__ = [
    "FanoutHandoffConfig",
    "FanoutHandoffEngine",
    "FanoutHandoffPlan",
    "FanoutHandoffResult",
    "FanoutPreflightContext",
    "FanoutReceiver",
    "MAX_FANOUT_RECEIVERS",
    "fanout_binding",
    "receiver_event_prefix",
    "receiver_leg",
    "run_fanout_handoff",
]
