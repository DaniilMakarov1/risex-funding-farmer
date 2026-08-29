"""Offline/read-only RISEx--Extended mainnet readiness evidence.

This module is deliberately isolated from the Farmer runtime.  It contains
only local evidence validation and a separately callable protected identity
provisioner.  It has no transport, database, venue-runner, order-preparation,
dispatch, or cryptographic dependency, and it never grants current mainnet
write authority.
"""

from __future__ import annotations

import getpass
import json
import math
import os
import stat
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


READY_FOR_PROTECTED_PROVISIONING = "READY_FOR_PROTECTED_PROVISIONING"
READY_FOR_PRIVATE_READ_GATES = "READY_FOR_PRIVATE_READ_GATES"
PRIVATE_READ_GATES_COMPLETE = "PRIVATE_READ_GATES_COMPLETE"
FUTURE_DISPATCH_APPROVAL_COMPLETE = "FUTURE_DISPATCH_APPROVAL_COMPLETE"
POST_LIFECYCLE_EVIDENCE_COMPLETE = "POST_LIFECYCLE_EVIDENCE_COMPLETE"
BLOCKED = "BLOCKED"
PROVISIONED = "PROTECTED_FILES_CREATED"

# This slice never grants current mainnet write authority.  A future approval
# fixture is evidence for a later, separately authorized decision only.
NO_MAINNET_WRITE_AUTHORITY = "NO_MAINNET_WRITE_AUTHORITY"

VENUES = ("RISEx", "Extended")
OPPOSITE_DIRECTIONS = frozenset(
    {"LONG_RISEX_SHORT_EXTENDED", "SHORT_RISEX_LONG_EXTENDED"}
)
DISPATCH_SEQUENCE = (
    ("RISEx", "ENTRY"),
    ("Extended", "ENTRY"),
    ("RISEx", "CLOSE"),
    ("Extended", "CLOSE"),
)
DISPATCH_APPROVAL_SCOPE = "ONE_MANUAL_LIFECYCLE_DISPATCH"
FUNDING_PHASES = ("BEFORE", "AT", "AFTER")
FUNDING_STATUSES = frozenset({"APPLIED_RATE", "PROVEN_NON_ACCRUAL"})

PROTECTED_SECRET_DIRECTORY = (
    Path.home() / ".config" / "risex-farmer" / "extended-mainnet-readiness"
)
_SECRET_FILENAMES = {
    "RISEx": "risex.identity",
    "Extended": "extended.identity",
}
_SECRET_MAX_BYTES = 4096
_PROTECTED_DIRECTORY_MODE = 0o700
_PROTECTED_FILE_MODE = 0o600


class ReadinessViolation(ValueError):
    """A bounded evidence, state, or protected-file contract violation."""


def _decimal(value: Any, field: str) -> Decimal:
    if isinstance(value, Decimal):
        result = value
    elif type(value) is int:
        result = Decimal(value)
    elif type(value) is str:
        try:
            result = Decimal(value)
        except InvalidOperation as exc:
            raise ReadinessViolation(f"DECIMAL_INVALID:{field}") from exc
    else:
        raise ReadinessViolation(f"DECIMAL_INVALID:{field}")
    if not result.is_finite():
        raise ReadinessViolation(f"DECIMAL_INVALID:{field}")
    return result


def _optional_decimal(value: Mapping[str, Any], key: str, field: str) -> Decimal | None:
    if value.get(key) is None:
        return None
    return _decimal(value[key], field)


def _required(value: Any, key: str, label: str) -> Any:
    if not isinstance(value, Mapping) or key not in value:
        raise ReadinessViolation(f"FIXTURE_FIELD_MISSING:{label}.{key}")
    return value[key]


def _sequence(value: Any, field: str) -> tuple[Any, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ReadinessViolation(f"FIXTURE_SEQUENCE_INVALID:{field}")
    return tuple(value)


def _token(value: Any, field: str) -> str:
    if type(value) is not str or not value or len(value) > 160:
        raise ReadinessViolation(f"TOKEN_INVALID:{field}")
    if value != value.strip() or any(
        ord(char) < 0x21 or ord(char) > 0x7E for char in value
    ):
        raise ReadinessViolation(f"TOKEN_INVALID:{field}")
    return value


def _positive_decimal(value: Any, field: str) -> Decimal:
    result = _decimal(value, field)
    if result <= 0:
        raise ReadinessViolation(f"POSITIVE_VALUE_REQUIRED:{field}")
    return result


@dataclass(frozen=True)
class AccountIdentity:
    venue: str
    account_id: str
    environment: str = "MAINNET"
    exact: bool = True
    authoritative: bool = True


@dataclass(frozen=True)
class VenueReadiness:
    venue: str
    canonical_asset: str
    active: bool
    linear_perpetual: bool
    non_rfq: bool
    metadata_current: bool
    minimum_quantity: Decimal
    quantity_step: Decimal
    reference_price_usd: Decimal
    available_buy_quantity: Decimal
    available_sell_quantity: Decimal
    fee_status: str
    fee_source: str
    maker_fee_rate: Decimal | None
    taker_fee_rate: Decimal | None
    schedule_status: str
    schedule_source: str
    funding_interval_seconds: int
    next_funding_at: int
    private_stream_status: str
    private_stream_source: str


@dataclass(frozen=True)
class RouteEvidence:
    route_id: str
    canonical_asset: str
    direction: str
    loss_bound_usd: Decimal
    self_trade_free: bool
    counterparty_account_id: str | None = None


@dataclass(frozen=True)
class ReadinessEvidence:
    """Phase-A public/offline evidence only.

    Exact accounts, account-scoped fees, private streams, runtime/write
    identities, approval caps, user approval, and lifecycle facts intentionally
    have no fields in this object.
    """

    route: RouteEvidence
    venues: tuple[VenueReadiness, ...]

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "ReadinessEvidence":
        if not isinstance(raw, Mapping):
            raise ReadinessViolation("FIXTURE_SCHEMA_INVALID")
        for forbidden in (
            "caps",
            "identities",
            "private_read",
            "operational",
            "dispatch_approval",
            "lifecycle",
        ):
            if forbidden in raw:
                raise ReadinessViolation(
                    f"PUBLIC_EVIDENCE_MUST_NOT_CLAIM:{forbidden}"
                )

        route = _required(raw, "route", "root")
        route_evidence = RouteEvidence(
            route_id=_required(route, "route_id", "route"),
            canonical_asset=_required(route, "canonical_asset", "route"),
            direction=_required(route, "direction", "route"),
            loss_bound_usd=_decimal(
                _required(route, "loss_bound_usd", "route"),
                "loss_bound_usd",
            ),
            self_trade_free=_required(route, "self_trade_free", "route"),
            counterparty_account_id=route.get("counterparty_account_id"),
        )

        venue_rows = _required(raw, "venues", "root")
        if not isinstance(venue_rows, Sequence) or isinstance(venue_rows, (str, bytes)):
            raise ReadinessViolation("FIXTURE_SCHEMA_INVALID:venues")
        venues = tuple(
            VenueReadiness(
                venue=_required(item, "venue", "venue"),
                canonical_asset=_required(item, "canonical_asset", "venue"),
                active=_required(item, "active", "venue"),
                linear_perpetual=_required(item, "linear_perpetual", "venue"),
                non_rfq=_required(item, "non_rfq", "venue"),
                metadata_current=_required(item, "metadata_current", "venue"),
                minimum_quantity=_decimal(
                    _required(item, "minimum_quantity", "venue"),
                    "minimum_quantity",
                ),
                quantity_step=_decimal(
                    _required(item, "quantity_step", "venue"),
                    "quantity_step",
                ),
                reference_price_usd=_decimal(
                    _required(item, "reference_price_usd", "venue"),
                    "reference_price_usd",
                ),
                available_buy_quantity=_decimal(
                    _required(item, "available_buy_quantity", "venue"),
                    "available_buy_quantity",
                ),
                available_sell_quantity=_decimal(
                    _required(item, "available_sell_quantity", "venue"),
                    "available_sell_quantity",
                ),
                fee_status=_required(item, "fee_status", "venue"),
                fee_source=_required(item, "fee_source", "venue"),
                maker_fee_rate=_optional_decimal(
                    item,
                    "maker_fee_rate",
                    "venue.maker_fee_rate",
                ),
                taker_fee_rate=_optional_decimal(
                    item,
                    "taker_fee_rate",
                    "venue.taker_fee_rate",
                ),
                schedule_status=_required(item, "schedule_status", "venue"),
                schedule_source=_required(item, "schedule_source", "venue"),
                funding_interval_seconds=_required(
                    item,
                    "funding_interval_seconds",
                    "venue",
                ),
                next_funding_at=_required(item, "next_funding_at", "venue"),
                private_stream_status=_required(
                    item,
                    "private_stream_status",
                    "venue",
                ),
                private_stream_source=_required(
                    item,
                    "private_stream_source",
                    "venue",
                ),
            )
            for item in venue_rows
        )
        return cls(route=route_evidence, venues=venues)


@dataclass(frozen=True)
class PrivateVenueRead:
    venue: str
    account_id: str
    fee_status: str
    fee_source: str
    maker_fee_rate: Decimal
    taker_fee_rate: Decimal
    private_stream_status: str
    private_stream_source: str
    unrelated_state_clear: bool


@dataclass(frozen=True)
class PlannedDeposit:
    venue: str
    account_id: str
    amount_usd: Decimal


@dataclass(frozen=True)
class PrivateReadEvidence:
    """Phase-C authenticated-private-read observations, supplied separately."""

    identities: tuple[AccountIdentity, ...]
    venues: tuple[PrivateVenueRead, ...]
    planned_deposits: tuple[PlannedDeposit, ...]

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "PrivateReadEvidence":
        if not isinstance(raw, Mapping):
            raise ReadinessViolation("FIXTURE_SCHEMA_INVALID:private_read")
        for forbidden in ("approval", "operational", "lifecycle"):
            if forbidden in raw:
                raise ReadinessViolation(
                    f"PRIVATE_READ_MUST_NOT_CLAIM:{forbidden}"
                )
        identity_rows = _required(raw, "identities", "private_read")
        if not isinstance(identity_rows, Sequence) or isinstance(
            identity_rows, (str, bytes)
        ):
            raise ReadinessViolation("FIXTURE_SCHEMA_INVALID:private_read.identities")
        identities = tuple(
            AccountIdentity(
                venue=_required(item, "venue", "identity"),
                account_id=_required(item, "account_id", "identity"),
                environment=_required(item, "environment", "identity"),
                exact=_required(item, "exact", "identity"),
                authoritative=_required(item, "authoritative", "identity"),
            )
            for item in identity_rows
        )

        venue_rows = _required(raw, "venues", "private_read")
        if not isinstance(venue_rows, Sequence) or isinstance(venue_rows, (str, bytes)):
            raise ReadinessViolation("FIXTURE_SCHEMA_INVALID:private_read.venues")
        venues = tuple(
            PrivateVenueRead(
                venue=_required(item, "venue", "private_venue"),
                account_id=_required(item, "account_id", "private_venue"),
                fee_status=_required(item, "fee_status", "private_venue"),
                fee_source=_required(item, "fee_source", "private_venue"),
                maker_fee_rate=_decimal(
                    _required(item, "maker_fee_rate", "private_venue"),
                    "private_venue.maker_fee_rate",
                ),
                taker_fee_rate=_decimal(
                    _required(item, "taker_fee_rate", "private_venue"),
                    "private_venue.taker_fee_rate",
                ),
                private_stream_status=_required(
                    item,
                    "private_stream_status",
                    "private_venue",
                ),
                private_stream_source=_required(
                    item,
                    "private_stream_source",
                    "private_venue",
                ),
                unrelated_state_clear=_required(
                    item,
                    "unrelated_state_clear",
                    "private_venue",
                ),
            )
            for item in venue_rows
        )

        deposit_rows = _required(raw, "planned_deposits", "private_read")
        if not isinstance(deposit_rows, Sequence) or isinstance(
            deposit_rows, (str, bytes)
        ):
            raise ReadinessViolation(
                "FIXTURE_SCHEMA_INVALID:private_read.planned_deposits"
            )
        planned_deposits = tuple(
            PlannedDeposit(
                venue=_required(item, "venue", "planned_deposit"),
                account_id=_required(item, "account_id", "planned_deposit"),
                amount_usd=_decimal(
                    _required(item, "amount_usd", "planned_deposit"),
                    "planned_deposit.amount_usd",
                ),
            )
            for item in deposit_rows
        )
        return cls(
            identities=identities,
            venues=venues,
            planned_deposits=planned_deposits,
        )


@dataclass(frozen=True)
class DispatchIdentity:
    sequence: int
    venue: str
    purpose: str
    account_id: str
    runtime_id: str
    write_identity: str
    durable_before_dispatch: bool


@dataclass(frozen=True)
class OperationalEvidence:
    """Phase-D runtime/write evidence immediately before future dispatch."""

    runtime_id: str
    runtime_fresh: bool
    runtime_durable_before_dispatch: bool
    captured_immediately_before_dispatch: bool
    dispatches: tuple[DispatchIdentity, ...]
    sequential_writes: bool
    no_blind_replay: bool
    restart_requires_reconciliation: bool


@dataclass(frozen=True)
class FutureDispatchApproval:
    """A future manual-dispatch binding, never current mainnet authority."""

    approval_id: str
    route_id: str
    direction: str
    risex_venue: str
    risex_account_id: str
    extended_venue: str
    extended_account_id: str
    risex_planned_deposit_usd: Decimal
    extended_planned_deposit_usd: Decimal
    deposit_cap_usd: Decimal
    maximum_loss_usd: Decimal
    approval_mode: str
    scope: str
    manual_lifecycle_dispatch_authorized: bool
    authorization_count: int


@dataclass(frozen=True)
class DispatchApprovalEvidence:
    """Phase-D approval and immediately-before-dispatch operational evidence."""

    approval: FutureDispatchApproval
    operational: OperationalEvidence

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "DispatchApprovalEvidence":
        if not isinstance(raw, Mapping):
            raise ReadinessViolation("FIXTURE_SCHEMA_INVALID:dispatch_approval")
        for forbidden in ("execution", "funding", "terminal_rounds", "lifecycle"):
            if forbidden in raw:
                raise ReadinessViolation(
                    f"DISPATCH_APPROVAL_MUST_NOT_CLAIM:{forbidden}"
                )
        approval = _required(raw, "approval", "dispatch_approval")
        dispatch_approval = FutureDispatchApproval(
            approval_id=_required(approval, "approval_id", "approval"),
            route_id=_required(approval, "route_id", "approval"),
            direction=_required(approval, "direction", "approval"),
            risex_venue=_required(approval, "risex_venue", "approval"),
            risex_account_id=_required(approval, "risex_account_id", "approval"),
            extended_venue=_required(
                approval,
                "extended_venue",
                "approval",
            ),
            extended_account_id=_required(
                approval,
                "extended_account_id",
                "approval",
            ),
            risex_planned_deposit_usd=_decimal(
                _required(
                    approval,
                    "risex_planned_deposit_usd",
                    "approval",
                ),
                "approval.risex_planned_deposit_usd",
            ),
            extended_planned_deposit_usd=_decimal(
                _required(
                    approval,
                    "extended_planned_deposit_usd",
                    "approval",
                ),
                "approval.extended_planned_deposit_usd",
            ),
            deposit_cap_usd=_decimal(
                _required(approval, "deposit_cap_usd", "approval"),
                "approval.deposit_cap_usd",
            ),
            maximum_loss_usd=_decimal(
                _required(approval, "maximum_loss_usd", "approval"),
                "approval.maximum_loss_usd",
            ),
            approval_mode=_required(approval, "approval_mode", "approval"),
            scope=_required(approval, "scope", "approval"),
            manual_lifecycle_dispatch_authorized=_required(
                approval,
                "manual_lifecycle_dispatch_authorized",
                "approval",
            ),
            authorization_count=_required(
                approval,
                "authorization_count",
                "approval",
            ),
        )

        operation = _required(raw, "operational", "dispatch_approval")
        dispatch_rows = _required(operation, "dispatches", "operational")
        if not isinstance(dispatch_rows, Sequence) or isinstance(
            dispatch_rows, (str, bytes)
        ):
            raise ReadinessViolation("FIXTURE_SCHEMA_INVALID:operational.dispatches")
        operational = OperationalEvidence(
            runtime_id=_required(operation, "runtime_id", "operational"),
            runtime_fresh=_required(operation, "runtime_fresh", "operational"),
            runtime_durable_before_dispatch=_required(
                operation,
                "runtime_durable_before_dispatch",
                "operational",
            ),
            captured_immediately_before_dispatch=_required(
                operation,
                "captured_immediately_before_dispatch",
                "operational",
            ),
            dispatches=tuple(
                DispatchIdentity(
                    sequence=_required(item, "sequence", "dispatch"),
                    venue=_required(item, "venue", "dispatch"),
                    purpose=_required(item, "purpose", "dispatch"),
                    account_id=_required(item, "account_id", "dispatch"),
                    runtime_id=_required(item, "runtime_id", "dispatch"),
                    write_identity=_required(item, "write_identity", "dispatch"),
                    durable_before_dispatch=_required(
                        item,
                        "durable_before_dispatch",
                        "dispatch",
                    ),
                )
                for item in dispatch_rows
            ),
            sequential_writes=_required(
                operation,
                "sequential_writes",
                "operational",
            ),
            no_blind_replay=_required(
                operation,
                "no_blind_replay",
                "operational",
            ),
            restart_requires_reconciliation=_required(
                operation,
                "restart_requires_reconciliation",
                "operational",
            ),
        )
        return cls(approval=dispatch_approval, operational=operational)


@dataclass(frozen=True)
class LegExecutionEvidence:
    venue: str
    account_id: str
    route_id: str
    canonical_asset: str
    entry_side: str
    canonical_quantity: Decimal
    order_id: str
    fill_id: str
    position_id: str
    order_reconciled: bool
    fill_reconciled: bool
    position_reconciled: bool
    authoritative: bool
    close_order_id: str
    close_fill_id: str
    reduce_only: bool
    close_reconciled: bool
    close_authoritative: bool
    # The aggregate entry flag is retained as a conjunction check.  These
    # fields prevent one shared flag from standing in for three authoritative
    # order/fill/position observations.
    order_authoritative: bool = True
    fill_authoritative: bool = True
    position_authoritative: bool = True


@dataclass(frozen=True)
class ExecutionEvidence:
    risex: LegExecutionEvidence
    extended: LegExecutionEvidence


@dataclass(frozen=True)
class FundingObservation:
    venue: str
    canonical_asset: str
    phase: str
    settlement_id: str
    settlement_at: int
    observed_at: int
    status: str
    cash_usd: Decimal | None
    eligible_known: bool
    exposure_confirmed: bool
    authoritative: bool
    missing: bool = False
    contradictory: bool = False


@dataclass(frozen=True)
class TerminalRound:
    round_number: int
    observed_at: int
    phase: str
    signature: str
    relevant_open_orders: int
    trigger_orders: int
    unrelated_open_orders: int
    unrelated_positions: int
    risex_net_position_quantity: Decimal
    extended_net_position_quantity: Decimal
    authoritative: bool


@dataclass(frozen=True)
class LifecycleEvidence:
    execution: ExecutionEvidence
    funding: tuple[FundingObservation, ...]
    terminal_rounds: tuple[TerminalRound, ...]

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "LifecycleEvidence":
        if not isinstance(raw, Mapping):
            raise ReadinessViolation("FIXTURE_SCHEMA_INVALID:lifecycle")
        for forbidden in (
            "caps",
            "route",
            "venues",
            "identities",
            "planned_deposits",
            "approval",
            "operational",
        ):
            if forbidden in raw:
                raise ReadinessViolation(
                    f"LIFECYCLE_MUST_NOT_CLAIM:{forbidden}"
                )
        execution = _required(raw, "execution", "lifecycle")
        execution_evidence = ExecutionEvidence(
            risex=_parse_execution_leg(
                _required(execution, "risex", "execution"),
                "execution.risex",
            ),
            extended=_parse_execution_leg(
                _required(execution, "extended", "execution"),
                "execution.extended",
            ),
        )

        funding_rows = _required(raw, "funding", "lifecycle")
        if not isinstance(funding_rows, Sequence) or isinstance(
            funding_rows, (str, bytes)
        ):
            raise ReadinessViolation("FIXTURE_SCHEMA_INVALID:funding")
        funding = tuple(
            FundingObservation(
                venue=_required(item, "venue", "funding"),
                canonical_asset=_required(item, "canonical_asset", "funding"),
                phase=_required(item, "phase", "funding"),
                settlement_id=_required(item, "settlement_id", "funding"),
                settlement_at=_required(item, "settlement_at", "funding"),
                observed_at=_required(item, "observed_at", "funding"),
                status=_required(item, "status", "funding"),
                cash_usd=(
                    None
                    if item.get("cash_usd") is None
                    else _decimal(item["cash_usd"], "funding.cash_usd")
                ),
                eligible_known=_required(item, "eligible_known", "funding"),
                exposure_confirmed=_required(
                    item,
                    "exposure_confirmed",
                    "funding",
                ),
                authoritative=_required(item, "authoritative", "funding"),
                missing=item.get("missing", False),
                contradictory=item.get("contradictory", False),
            )
            for item in funding_rows
        )

        terminal_rows = _required(raw, "terminal_rounds", "lifecycle")
        if not isinstance(terminal_rows, Sequence) or isinstance(
            terminal_rows, (str, bytes)
        ):
            raise ReadinessViolation("FIXTURE_SCHEMA_INVALID:terminal_rounds")
        terminal_rounds = tuple(
            TerminalRound(
                round_number=_required(item, "round_number", "terminal"),
                observed_at=_required(item, "observed_at", "terminal"),
                phase=_required(item, "phase", "terminal"),
                signature=_required(item, "signature", "terminal"),
                relevant_open_orders=_required(
                    item,
                    "relevant_open_orders",
                    "terminal",
                ),
                trigger_orders=_required(item, "trigger_orders", "terminal"),
                unrelated_open_orders=_required(
                    item,
                    "unrelated_open_orders",
                    "terminal",
                ),
                unrelated_positions=_required(
                    item,
                    "unrelated_positions",
                    "terminal",
                ),
                risex_net_position_quantity=_decimal(
                    _required(
                        item,
                        "risex_net_position_quantity",
                        "terminal",
                    ),
                    "risex_net_position_quantity",
                ),
                extended_net_position_quantity=_decimal(
                    _required(
                        item,
                        "extended_net_position_quantity",
                        "terminal",
                    ),
                    "extended_net_position_quantity",
                ),
                authoritative=_required(item, "authoritative", "terminal"),
            )
            for item in terminal_rows
        )
        return cls(
            execution=execution_evidence,
            funding=funding,
            terminal_rounds=terminal_rounds,
        )


def _parse_execution_leg(raw: Any, label: str) -> LegExecutionEvidence:
    return LegExecutionEvidence(
        venue=_required(raw, "venue", label),
        account_id=_required(raw, "account_id", label),
        route_id=_required(raw, "route_id", label),
        canonical_asset=_required(raw, "canonical_asset", label),
        entry_side=_required(raw, "entry_side", label),
        canonical_quantity=_decimal(
            _required(raw, "canonical_quantity", label),
            f"{label}.canonical_quantity",
        ),
        order_id=_required(raw, "order_id", label),
        fill_id=_required(raw, "fill_id", label),
        position_id=_required(raw, "position_id", label),
        order_reconciled=_required(raw, "order_reconciled", label),
        fill_reconciled=_required(raw, "fill_reconciled", label),
        position_reconciled=_required(raw, "position_reconciled", label),
        authoritative=_required(raw, "authoritative", label),
        close_order_id=_required(raw, "close_order_id", label),
        close_fill_id=_required(raw, "close_fill_id", label),
        reduce_only=_required(raw, "reduce_only", label),
        close_reconciled=_required(raw, "close_reconciled", label),
        close_authoritative=_required(raw, "close_authoritative", label),
        order_authoritative=_required(raw, "order_authoritative", label),
        fill_authoritative=_required(raw, "fill_authoritative", label),
        position_authoritative=_required(raw, "position_authoritative", label),
    )


@dataclass(frozen=True)
class ProtectedFileState:
    venue: str
    path: str
    present: bool
    protected: bool
    reason: str
    mode: int | None = None
    link_count: int | None = None


@dataclass(frozen=True)
class ProtectedSecretFiles:
    risex: ProtectedFileState
    extended: ProtectedFileState

    @property
    def states(self) -> tuple[ProtectedFileState, ProtectedFileState]:
        return self.risex, self.extended

    @property
    def all_protected(self) -> bool:
        return all(item.protected for item in self.states)


@dataclass(frozen=True)
class ProvisioningResult:
    status: str
    reason: str
    files: ProtectedSecretFiles
    mainnet_write_authority: str = NO_MAINNET_WRITE_AUTHORITY

    @property
    def write_ready(self) -> bool:
        return False

    def evidence(self) -> str:
        return json.dumps(
            {
                "mainnet_write_authority": self.mainnet_write_authority,
                "reason": self.reason,
                "status": self.status,
                "venues": list(VENUES),
                "write_ready": self.write_ready,
            },
            sort_keys=True,
            separators=(",", ":"),
        )


@dataclass(frozen=True)
class ReadinessResult:
    status: str
    reason: str
    blockers: tuple[str, ...]
    route_id: str
    direction: str
    common_quantity: Decimal | None
    gross_trade_notional_usd: Decimal | None
    loss_bound_usd: Decimal | None
    mainnet_write_authority: str = NO_MAINNET_WRITE_AUTHORITY

    @property
    def ready(self) -> bool:
        return self.status in {
            READY_FOR_PROTECTED_PROVISIONING,
            READY_FOR_PRIVATE_READ_GATES,
        }

    @property
    def write_ready(self) -> bool:
        return False

    def evidence(self) -> str:
        return json.dumps(
            {
                "blockers": list(self.blockers),
                "common_quantity": (
                    None if self.common_quantity is None else str(self.common_quantity)
                ),
                "direction": self.direction,
                "gross_trade_notional_usd": (
                    None
                    if self.gross_trade_notional_usd is None
                    else str(self.gross_trade_notional_usd)
                ),
                "loss_bound_usd": (
                    None if self.loss_bound_usd is None else str(self.loss_bound_usd)
                ),
                "mainnet_write_authority": self.mainnet_write_authority,
                "reason": self.reason,
                "route_id": self.route_id,
                "status": self.status,
                "write_ready": self.write_ready,
            },
            sort_keys=True,
            separators=(",", ":"),
        )


@dataclass(frozen=True)
class PrivateReadResult:
    status: str
    reason: str
    blockers: tuple[str, ...]
    route_id: str
    direction: str
    planned_deposit_total_usd: Decimal | None
    mainnet_write_authority: str = NO_MAINNET_WRITE_AUTHORITY

    @property
    def complete(self) -> bool:
        return self.status == PRIVATE_READ_GATES_COMPLETE

    @property
    def write_ready(self) -> bool:
        return False

    def evidence(self) -> str:
        return json.dumps(
            {
                "blockers": list(self.blockers),
                "direction": self.direction,
                "mainnet_write_authority": self.mainnet_write_authority,
                "planned_deposit_total_usd": (
                    None
                    if self.planned_deposit_total_usd is None
                    else str(self.planned_deposit_total_usd)
                ),
                "reason": self.reason,
                "route_id": self.route_id,
                "status": self.status,
                "write_ready": self.write_ready,
            },
            sort_keys=True,
            separators=(",", ":"),
        )


@dataclass(frozen=True)
class DispatchApprovalResult:
    status: str
    reason: str
    blockers: tuple[str, ...]
    route_id: str
    direction: str
    mainnet_write_authority: str = NO_MAINNET_WRITE_AUTHORITY

    @property
    def complete(self) -> bool:
        return self.status == FUTURE_DISPATCH_APPROVAL_COMPLETE

    @property
    def write_ready(self) -> bool:
        return False

    def evidence(self) -> str:
        return json.dumps(
            {
                "blockers": list(self.blockers),
                "direction": self.direction,
                "mainnet_write_authority": self.mainnet_write_authority,
                "reason": self.reason,
                "route_id": self.route_id,
                "status": self.status,
                "write_ready": self.write_ready,
            },
            sort_keys=True,
            separators=(",", ":"),
        )


@dataclass(frozen=True)
class LifecycleResult:
    status: str
    reason: str
    blockers: tuple[str, ...]
    route_id: str
    direction: str
    mainnet_write_authority: str = NO_MAINNET_WRITE_AUTHORITY

    @property
    def complete(self) -> bool:
        return self.status == POST_LIFECYCLE_EVIDENCE_COMPLETE

    @property
    def write_ready(self) -> bool:
        return False

    def evidence(self) -> str:
        return json.dumps(
            {
                "blockers": list(self.blockers),
                "direction": self.direction,
                "mainnet_write_authority": self.mainnet_write_authority,
                "reason": self.reason,
                "route_id": self.route_id,
                "status": self.status,
                "write_ready": self.write_ready,
            },
            sort_keys=True,
            separators=(",", ":"),
        )


def protected_secret_paths() -> Mapping[str, Path]:
    """Return the two fixed local paths without opening either file."""

    directory = PROTECTED_SECRET_DIRECTORY
    return {
        venue: directory / filename
        for venue, filename in _SECRET_FILENAMES.items()
    }


def _directory_state(directory: Path) -> tuple[bool, str]:
    try:
        info = os.lstat(directory)
    except FileNotFoundError:
        return False, "PROTECTED_DIRECTORY_MISSING"
    except OSError:
        return False, "PROTECTED_DIRECTORY_UNREADABLE"
    if stat.S_ISLNK(info.st_mode):
        return False, "PROTECTED_DIRECTORY_SYMLINK"
    if not stat.S_ISDIR(info.st_mode):
        return False, "PROTECTED_DIRECTORY_NOT_DIRECTORY"
    if info.st_uid != os.getuid():
        return False, "PROTECTED_DIRECTORY_OWNER_NOT_CURRENT_USER"
    if stat.S_IMODE(info.st_mode) != _PROTECTED_DIRECTORY_MODE:
        return False, "PROTECTED_DIRECTORY_MODE_NOT_0700"
    return True, "PROTECTED_DIRECTORY_OK"


def _file_state(
    venue: str,
    path: Path,
    directory_ok: bool,
    directory_reason: str,
) -> ProtectedFileState:
    if not directory_ok:
        return ProtectedFileState(
            venue=venue,
            path=str(path),
            present=False,
            protected=False,
            reason=directory_reason,
        )
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        return ProtectedFileState(
            venue=venue,
            path=str(path),
            present=False,
            protected=False,
            reason="PROTECTED_FILE_MISSING",
        )
    except OSError:
        return ProtectedFileState(
            venue=venue,
            path=str(path),
            present=False,
            protected=False,
            reason="PROTECTED_FILE_UNREADABLE",
        )

    mode = stat.S_IMODE(info.st_mode)
    links = info.st_nlink
    if stat.S_ISLNK(info.st_mode):
        reason = "PROTECTED_FILE_SYMLINK"
    elif not stat.S_ISREG(info.st_mode):
        reason = "PROTECTED_FILE_NOT_REGULAR"
    elif info.st_uid != os.getuid():
        reason = "PROTECTED_FILE_OWNER_NOT_CURRENT_USER"
    elif links != 1:
        reason = "PROTECTED_FILE_HARDLINK"
    elif mode != _PROTECTED_FILE_MODE:
        reason = "PROTECTED_FILE_MODE_NOT_0600"
    elif info.st_size <= 0:
        reason = "PROTECTED_FILE_EMPTY"
    elif info.st_size > _SECRET_MAX_BYTES:
        reason = "PROTECTED_FILE_TOO_LARGE"
    else:
        reason = "PROTECTED_FILE_OK"
    return ProtectedFileState(
        venue=venue,
        path=str(path),
        present=True,
        protected=reason == "PROTECTED_FILE_OK",
        reason=reason,
        mode=mode,
        link_count=links,
    )


def inspect_protected_secret_files() -> ProtectedSecretFiles:
    """Inspect fixed file metadata only; never read secret bytes."""

    paths = protected_secret_paths()
    directory_ok, directory_reason = _directory_state(PROTECTED_SECRET_DIRECTORY)
    return ProtectedSecretFiles(
        risex=_file_state("RISEx", paths["RISEx"], directory_ok, directory_reason),
        extended=_file_state(
            "Extended",
            paths["Extended"],
            directory_ok,
            directory_reason,
        ),
    )


def _ensure_fixed_directory() -> None:
    directory = PROTECTED_SECRET_DIRECTORY
    if not directory.is_absolute():
        raise ReadinessViolation("PROTECTED_DIRECTORY_NOT_ABSOLUTE")

    missing: list[Path] = []
    current = directory
    while True:
        try:
            info = os.lstat(current)
        except FileNotFoundError:
            missing.append(current)
            if current.parent == current:
                raise ReadinessViolation("PROTECTED_DIRECTORY_PARENT_MISSING")
            current = current.parent
            continue
        except OSError as exc:
            raise ReadinessViolation("PROTECTED_DIRECTORY_UNREADABLE") from exc
        if stat.S_ISLNK(info.st_mode):
            raise ReadinessViolation("PROTECTED_DIRECTORY_SYMLINK")
        if not stat.S_ISDIR(info.st_mode):
            raise ReadinessViolation("PROTECTED_DIRECTORY_NOT_DIRECTORY")
        if info.st_uid != os.getuid():
            raise ReadinessViolation("PROTECTED_DIRECTORY_OWNER_NOT_CURRENT_USER")
        break

    for item in reversed(missing):
        try:
            os.mkdir(item, _PROTECTED_DIRECTORY_MODE)
        except FileExistsError:
            pass
        try:
            info = os.lstat(item)
        except OSError as exc:
            raise ReadinessViolation("PROTECTED_DIRECTORY_UNREADABLE") from exc
        if stat.S_ISLNK(info.st_mode):
            raise ReadinessViolation("PROTECTED_DIRECTORY_SYMLINK")
        if not stat.S_ISDIR(info.st_mode):
            raise ReadinessViolation("PROTECTED_DIRECTORY_NOT_DIRECTORY")

    directory_ok, reason = _directory_state(directory)
    if not directory_ok:
        raise ReadinessViolation(reason)


def _secret_bytes(value: Any) -> bytearray:
    if type(value) is not str or not value or value != value.strip():
        raise ReadinessViolation("PROTECTED_INPUT_INVALID")
    if any(char in value for char in ("\x00", "\n", "\r")):
        raise ReadinessViolation("PROTECTED_INPUT_INVALID")
    try:
        payload = bytearray(value.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise ReadinessViolation("PROTECTED_INPUT_INVALID") from exc
    if not 0 < len(payload) <= _SECRET_MAX_BYTES:
        payload[:] = b"\x00" * len(payload)
        raise ReadinessViolation("PROTECTED_INPUT_INVALID")
    return payload


def _write_payload(directory_fd: int, filename: str, payload: bytearray) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = -1
    created = False
    completed = False
    try:
        descriptor = os.open(
            filename,
            flags,
            _PROTECTED_FILE_MODE,
            dir_fd=directory_fd,
        )
        created = True
        info = os.fstat(descriptor)
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise ReadinessViolation("PROTECTED_FILE_NOT_REGULAR")
        if info.st_uid != os.getuid():
            raise ReadinessViolation("PROTECTED_FILE_OWNER_NOT_CURRENT_USER")
        if info.st_nlink != 1:
            raise ReadinessViolation("PROTECTED_FILE_HARDLINK")
        os.fchmod(descriptor, _PROTECTED_FILE_MODE)
        view = memoryview(payload)
        try:
            while len(view):
                written = os.write(descriptor, view)
                if written <= 0:
                    raise ReadinessViolation("PROTECTED_FILE_WRITE_INCOMPLETE")
                view = view[written:]
        finally:
            view.release()
        os.fsync(descriptor)
        final_info = os.fstat(descriptor)
        if (
            stat.S_IMODE(final_info.st_mode) != _PROTECTED_FILE_MODE
            or final_info.st_uid != os.getuid()
            or final_info.st_nlink != 1
            or final_info.st_size != len(payload)
        ):
            if final_info.st_uid != os.getuid():
                raise ReadinessViolation("PROTECTED_FILE_OWNER_NOT_CURRENT_USER")
            raise ReadinessViolation("PROTECTED_FILE_METADATA_CHANGED")
        completed = True
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if created and not completed:
            try:
                os.unlink(filename, dir_fd=directory_fd)
            except OSError:
                pass


def provision_protected_identities(
    input_fn: Callable[[str], str] | None = None,
) -> ProvisioningResult:
    """Interactively create the two fixed protected files.

    The default input function is hidden terminal input.  Tests may inject a
    callback with synthetic values.  The callback receives only a generic
    venue prompt and the result never returns any input value.
    """

    input_fn = getpass.getpass if input_fn is None else input_fn
    before = inspect_protected_secret_files()
    directory_ok, directory_reason = _directory_state(PROTECTED_SECRET_DIRECTORY)
    if not directory_ok and directory_reason != "PROTECTED_DIRECTORY_MISSING":
        return ProvisioningResult(
            status=BLOCKED,
            reason=directory_reason,
            files=before,
        )
    if any(item.present for item in before.states):
        return ProvisioningResult(
            status=BLOCKED,
            reason="PROTECTED_PATH_ALREADY_EXISTS",
            files=before,
        )

    payloads: list[bytearray] = []
    try:
        for venue in VENUES:
            prompt = f"{venue} opaque credential (not a private key or seed phrase): "
            try:
                supplied = input_fn(prompt)
            except Exception as exc:
                raise ReadinessViolation("PROTECTED_INPUT_UNAVAILABLE") from exc
            payloads.append(_secret_bytes(supplied))
        if payloads[0] == payloads[1]:
            raise ReadinessViolation("PROTECTED_IDENTITIES_NOT_DISTINCT")

        _ensure_fixed_directory()
        directory_fd = os.open(
            os.fspath(PROTECTED_SECRET_DIRECTORY),
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        created: list[str] = []
        try:
            for filename, payload in zip(_SECRET_FILENAMES.values(), payloads):
                _write_payload(directory_fd, filename, payload)
                created.append(filename)
            os.fsync(directory_fd)
        except Exception:
            for filename in reversed(created):
                try:
                    os.unlink(filename, dir_fd=directory_fd)
                except OSError:
                    pass
            raise
        finally:
            os.close(directory_fd)
    except ReadinessViolation as exc:
        return ProvisioningResult(
            status=BLOCKED,
            reason=str(exc),
            files=inspect_protected_secret_files(),
        )
    except OSError:
        return ProvisioningResult(
            status=BLOCKED,
            reason="PROTECTED_FILESYSTEM_OPERATION_FAILED",
            files=inspect_protected_secret_files(),
        )
    finally:
        for payload in payloads:
            payload[:] = b"\x00" * len(payload)

    return ProvisioningResult(
        status=PROVISIONED,
        reason="PROTECTED_FILES_CREATED",
        files=inspect_protected_secret_files(),
    )


def _rational_lcm(values: Sequence[Decimal]) -> tuple[int, int]:
    """Return the exact least common multiple as numerator/denominator."""

    fractions = [value.as_integer_ratio() for value in values]
    denominator = 1
    for _, item_denominator in fractions:
        denominator = math.lcm(denominator, item_denominator)
    units = [
        item_numerator * (denominator // item_denominator)
        for item_numerator, item_denominator in fractions
    ]
    common_numerator = 1
    for unit in units:
        common_numerator = math.lcm(common_numerator, abs(unit))
    divisor = math.gcd(common_numerator, denominator)
    return common_numerator // divisor, denominator // divisor


def _fraction_to_decimal(numerator: int, denominator: int) -> Decimal:
    with localcontext() as context:
        context.prec = 50
        return Decimal(numerator) / Decimal(denominator)


def _ceil_fraction(numerator: int, denominator: int) -> int:
    quotient, remainder = divmod(numerator, denominator)
    return quotient if remainder == 0 else quotient + 1


def _add_blocker(blockers: list[str], reason: str) -> None:
    if reason not in blockers:
        blockers.append(reason)


def _valid_token(value: Any, field: str) -> bool:
    try:
        _token(value, field)
    except ReadinessViolation:
        return False
    return True


def _valid_decimal(value: Any, field: str, *, positive: bool = False) -> bool:
    try:
        result = _decimal(value, field)
    except ReadinessViolation:
        return False
    return result > 0 if positive else True


def _public_evaluation(evidence: ReadinessEvidence) -> ReadinessResult:
    blockers: list[str] = []
    if not isinstance(evidence, ReadinessEvidence):
        return ReadinessResult(
            status=BLOCKED,
            reason="EVIDENCE_SCHEMA_INVALID",
            blockers=("EVIDENCE_SCHEMA_INVALID",),
            route_id="UNKNOWN_ROUTE",
            direction="UNKNOWN_DIRECTION",
            common_quantity=None,
            gross_trade_notional_usd=None,
            loss_bound_usd=None,
        )
    if not isinstance(evidence.route, RouteEvidence):
        return ReadinessResult(
            status=BLOCKED,
            reason="EVIDENCE_SCHEMA_INVALID:route",
            blockers=("EVIDENCE_SCHEMA_INVALID:route",),
            route_id="UNKNOWN_ROUTE",
            direction="UNKNOWN_DIRECTION",
            common_quantity=None,
            gross_trade_notional_usd=None,
            loss_bound_usd=None,
        )
    if not isinstance(evidence.venues, tuple) or not all(
        isinstance(item, VenueReadiness) for item in evidence.venues
    ):
        return ReadinessResult(
            status=BLOCKED,
            reason="EVIDENCE_SCHEMA_INVALID:venues",
            blockers=("EVIDENCE_SCHEMA_INVALID:venues",),
            route_id="UNKNOWN_ROUTE",
            direction="UNKNOWN_DIRECTION",
            common_quantity=None,
            gross_trade_notional_usd=None,
            loss_bound_usd=None,
        )

    route_id = (
        evidence.route.route_id
        if type(evidence.route.route_id) is str
        else "UNKNOWN_ROUTE"
    )
    route_asset = evidence.route.canonical_asset
    direction = (
        evidence.route.direction
        if type(evidence.route.direction) is str
        else "UNKNOWN_DIRECTION"
    )
    if not _valid_token(evidence.route.route_id, "route_id"):
        _add_blocker(blockers, "ROUTE_IDENTITY_NOT_EXACT")
    if not _valid_token(route_asset, "canonical_asset"):
        _add_blocker(blockers, "CANONICAL_ASSET_NOT_EXACT")
    if direction not in OPPOSITE_DIRECTIONS:
        _add_blocker(blockers, "ONE_ROUTE_DIRECTION_REQUIRED")
    if evidence.route.self_trade_free is not True:
        _add_blocker(blockers, "SELF_TRADE_GUARD_NOT_PROVEN")
    if evidence.route.counterparty_account_id is not None:
        _add_blocker(blockers, "PUBLIC_ROUTE_MUST_NOT_CLAIM_ACCOUNT_IDENTITY")

    try:
        loss_bound = _decimal(evidence.route.loss_bound_usd, "loss_bound_usd")
        if loss_bound < 0:
            _add_blocker(blockers, "LOSS_BOUND_MUST_NOT_BE_NEGATIVE")
    except ReadinessViolation:
        loss_bound = None
        _add_blocker(blockers, "LOSS_BOUND_MUST_BE_FINITE_DECIMAL")

    venue_map: dict[str, VenueReadiness] = {}
    for venue in evidence.venues:
        if venue.venue not in VENUES:
            _add_blocker(blockers, "VENUE_EVIDENCE_OUT_OF_SCOPE")
            continue
        if venue.venue in venue_map:
            _add_blocker(blockers, f"VENUE_EVIDENCE_DUPLICATE:{venue.venue}")
        venue_map[venue.venue] = venue
        if venue.canonical_asset != route_asset:
            _add_blocker(blockers, f"CANONICAL_ASSET_MISMATCH:{venue.venue}")
        if (
            venue.active is not True
            or venue.linear_perpetual is not True
            or venue.non_rfq is not True
        ):
            _add_blocker(blockers, f"INSTRUMENT_NOT_ELIGIBLE:{venue.venue}")
        if venue.metadata_current is not True:
            _add_blocker(blockers, f"MARKET_METADATA_NOT_CURRENT:{venue.venue}")
        for field_name, field_value in (
            ("minimum_quantity", venue.minimum_quantity),
            ("quantity_step", venue.quantity_step),
            ("reference_price_usd", venue.reference_price_usd),
            ("available_buy_quantity", venue.available_buy_quantity),
            ("available_sell_quantity", venue.available_sell_quantity),
        ):
            if not _valid_decimal(
                field_value,
                f"{venue.venue}.{field_name}",
                positive=True,
            ):
                _add_blocker(
                    blockers,
                    f"CURRENT_MARKET_VALUE_INVALID:{venue.venue}:{field_name}",
                )
        for field_name, field_value in (
            ("maker_fee_rate", venue.maker_fee_rate),
            ("taker_fee_rate", venue.taker_fee_rate),
        ):
            if field_value is not None and not _valid_decimal(
                field_value,
                f"{venue.venue}.{field_name}",
            ):
                _add_blocker(
                    blockers,
                    f"PUBLIC_FEE_VALUE_INVALID:{venue.venue}:{field_name}",
                )

        if venue.venue == "RISEx":
            if (
                venue.fee_status != "PENDING_ACCOUNT_SCOPED"
                or venue.fee_source != "PRIVATE_READ_PENDING"
                or venue.maker_fee_rate is not None
                or venue.taker_fee_rate is not None
            ):
                _add_blocker(blockers, "RISEX_ACCOUNT_FEE_MUST_REMAIN_PENDING")
        elif (
            venue.fee_status != "PUBLIC_CURRENT"
            or venue.fee_source != "PUBLIC_READ"
            or venue.maker_fee_rate is None
            or venue.taker_fee_rate is None
        ):
            _add_blocker(blockers, "EXTENDED_PUBLIC_FEE_NOT_CURRENT")

        if (
            venue.schedule_status != "CURRENT_PUBLIC"
            or venue.schedule_source != "OFFICIAL_CURRENT_SCHEDULE"
        ):
            _add_blocker(
                blockers,
                f"PUBLIC_FUNDING_SCHEDULE_NOT_AUTHORITATIVE:{venue.venue}",
            )
        if (
            venue.private_stream_status != "PENDING_PRIVATE_READ"
            or venue.private_stream_source != "PRIVATE_READ_PENDING"
        ):
            _add_blocker(
                blockers,
                f"PRIVATE_STREAM_MUST_REMAIN_PENDING:{venue.venue}",
            )
        if (
            type(venue.funding_interval_seconds) is not int
            or venue.funding_interval_seconds <= 0
        ):
            _add_blocker(blockers, f"FUNDING_INTERVAL_INVALID:{venue.venue}")
        if type(venue.next_funding_at) is not int or venue.next_funding_at <= 0:
            _add_blocker(
                blockers,
                f"FUNDING_SCHEDULE_TIMESTAMP_INVALID:{venue.venue}",
            )

    if set(venue_map) != set(VENUES) or len(evidence.venues) != len(VENUES):
        _add_blocker(blockers, "PUBLIC_MARKET_READINESS_REQUIRED_FOR_BOTH_VENUES")
    elif (
        venue_map["RISEx"].funding_interval_seconds
        != venue_map["Extended"].funding_interval_seconds
        or venue_map["RISEx"].next_funding_at
        != venue_map["Extended"].next_funding_at
    ):
        _add_blocker(blockers, "PUBLIC_FUNDING_SCHEDULE_NOT_COMMON")

    common_quantity: Decimal | None = None
    gross_trade_notional: Decimal | None = None
    if set(venue_map) == set(VENUES):
        try:
            risex = venue_map["RISEx"]
            extended = venue_map["Extended"]
            for venue in (risex, extended):
                _positive_decimal(
                    venue.minimum_quantity,
                    f"{venue.venue}.minimum_quantity",
                )
                _positive_decimal(
                    venue.quantity_step,
                    f"{venue.venue}.quantity_step",
                )
                _positive_decimal(
                    venue.reference_price_usd,
                    f"{venue.venue}.reference_price_usd",
                )
                _positive_decimal(
                    venue.available_buy_quantity,
                    f"{venue.venue}.available_buy_quantity",
                )
                _positive_decimal(
                    venue.available_sell_quantity,
                    f"{venue.venue}.available_sell_quantity",
                )
            step_numerator, step_denominator = _rational_lcm(
                [risex.quantity_step, extended.quantity_step]
            )
            minimum = max(risex.minimum_quantity, extended.minimum_quantity)
            minimum_numerator, minimum_denominator = minimum.as_integer_ratio()
            scaled_minimum = minimum_numerator * step_denominator
            scaled_step = step_numerator * minimum_denominator
            multiplier = _ceil_fraction(scaled_minimum, scaled_step)
            common_quantity = _fraction_to_decimal(
                step_numerator * multiplier,
                step_denominator,
            )
            for venue in (risex, extended):
                if (
                    common_quantity > venue.available_buy_quantity
                    or common_quantity > venue.available_sell_quantity
                ):
                    _add_blocker(
                        blockers,
                        f"COMMON_QUANTITY_NOT_EXECUTABLE:{venue.venue}",
                    )
            gross_trade_notional = common_quantity * (
                risex.reference_price_usd + extended.reference_price_usd
            )
        except ReadinessViolation:
            _add_blocker(blockers, "COMMON_QUANTITY_NOT_COMPUTABLE")

    status = BLOCKED if blockers else READY_FOR_PROTECTED_PROVISIONING
    return ReadinessResult(
        status=status,
        reason=blockers[0] if blockers else "PUBLIC_OFFLINE_REQUIREMENTS_PROVEN",
        blockers=tuple(blockers),
        route_id=route_id,
        direction=direction,
        common_quantity=common_quantity,
        gross_trade_notional_usd=gross_trade_notional,
        loss_bound_usd=loss_bound,
    )


def _with_readiness_phase(
    base: ReadinessResult,
    *,
    status: str,
    reason: str,
    blockers: tuple[str, ...] = (),
) -> ReadinessResult:
    return ReadinessResult(
        status=status,
        reason=reason,
        blockers=blockers,
        route_id=base.route_id,
        direction=base.direction,
        common_quantity=base.common_quantity,
        gross_trade_notional_usd=base.gross_trade_notional_usd,
        loss_bound_usd=base.loss_bound_usd,
    )


def assess_readiness(evidence: ReadinessEvidence) -> ReadinessResult:
    """Evaluate Phase A public/offline route readiness and Phase B file state."""

    base = _public_evaluation(evidence)
    if base.blockers:
        return base

    protected = inspect_protected_secret_files()
    directory_ok, directory_reason = _directory_state(PROTECTED_SECRET_DIRECTORY)
    if all(not item.present for item in protected.states):
        if not directory_ok and directory_reason != "PROTECTED_DIRECTORY_MISSING":
            reason = f"PROTECTED_SECRET_DIRECTORY_NOT_READY:{directory_reason}"
            return _with_readiness_phase(
                base,
                status=BLOCKED,
                reason=reason,
                blockers=(reason,),
            )
        return _with_readiness_phase(
            base,
            status=READY_FOR_PROTECTED_PROVISIONING,
            reason="PUBLIC_OFFLINE_REQUIREMENTS_PROVEN_PROTECTED_PROVISIONING_PENDING",
        )

    if protected.all_protected:
        return _with_readiness_phase(
            base,
            status=READY_FOR_PRIVATE_READ_GATES,
            reason="PROTECTED_IDENTITIES_PRESENT_PRIVATE_READ_GATES_PENDING",
        )

    blockers = tuple(
        f"PROTECTED_SECRET_FILE_NOT_SAFE:{item.venue}:{item.reason}"
        for item in protected.states
        if not item.protected
    )
    return _with_readiness_phase(
        base,
        status=BLOCKED,
        reason=blockers[0],
        blockers=blockers,
    )


def _invalid_private_result(
    base: ReadinessResult,
    blockers: tuple[str, ...],
    *,
    planned_total: Decimal | None = None,
) -> PrivateReadResult:
    unique_blockers = tuple(dict.fromkeys(blockers))
    return PrivateReadResult(
        status=BLOCKED,
        reason=unique_blockers[0],
        blockers=unique_blockers,
        route_id=base.route_id,
        direction=base.direction,
        planned_deposit_total_usd=planned_total,
    )


def _private_identity_map(
    private_read: PrivateReadEvidence,
    blockers: list[str],
) -> dict[str, str]:
    identities = private_read.identities
    if len(identities) != len(VENUES):
        _add_blocker(
            blockers,
            "PRIVATE_IDENTITIES_MUST_INCLUDE_ONE_EXACT_ACCOUNT_PER_VENUE",
        )
    account_ids: dict[str, str] = {}
    for identity in identities:
        if identity.venue not in VENUES:
            _add_blocker(blockers, "PRIVATE_IDENTITIES_OUT_OF_SCOPE")
            continue
        if identity.venue in account_ids:
            _add_blocker(blockers, f"PRIVATE_IDENTITY_DUPLICATE_VENUE:{identity.venue}")
        if not _valid_token(identity.account_id, f"{identity.venue}.account_id"):
            _add_blocker(
                blockers,
                f"PRIVATE_ACCOUNT_IDENTITY_NOT_EXACT:{identity.venue}",
            )
        if identity.environment != "MAINNET":
            _add_blocker(
                blockers,
                f"PRIVATE_ACCOUNT_ENVIRONMENT_NOT_MAINNET:{identity.venue}",
            )
        if identity.exact is not True or identity.authoritative is not True:
            _add_blocker(
                blockers,
                f"PRIVATE_ACCOUNT_IDENTITY_NOT_AUTHORITATIVE:{identity.venue}",
            )
        account_ids[identity.venue] = identity.account_id
    if set(account_ids) != set(VENUES):
        _add_blocker(
            blockers,
            "PRIVATE_IDENTITIES_MUST_INCLUDE_ONE_EXACT_ACCOUNT_PER_VENUE",
        )
    if len(account_ids) == len(VENUES) and len(set(account_ids.values())) != len(
        VENUES
    ):
        _add_blocker(blockers, "PRIVATE_ACCOUNT_IDENTITIES_MUST_BE_DISTINCT")
    return account_ids


def _private_deposit_map(
    private_read: PrivateReadEvidence,
    account_ids: Mapping[str, str],
    blockers: list[str],
) -> Decimal | None:
    if len(private_read.planned_deposits) != len(VENUES):
        _add_blocker(
            blockers,
            "PLANNED_DEPOSITS_MUST_INCLUDE_ONE_AMOUNT_PER_VENUE",
        )
    deposits: dict[str, PlannedDeposit] = {}
    amounts: dict[str, Decimal] = {}
    for deposit in private_read.planned_deposits:
        if deposit.venue not in VENUES:
            _add_blocker(blockers, "PLANNED_DEPOSIT_VENUE_OUT_OF_SCOPE")
            continue
        if deposit.venue in deposits:
            _add_blocker(
                blockers,
                f"PLANNED_DEPOSIT_DUPLICATE_VENUE:{deposit.venue}",
            )
        deposits[deposit.venue] = deposit
        if deposit.account_id != account_ids.get(deposit.venue):
            _add_blocker(
                blockers,
                f"PLANNED_DEPOSIT_ACCOUNT_MISMATCH:{deposit.venue}",
            )
        try:
            amount = _decimal(
                deposit.amount_usd,
                f"planned_deposit.{deposit.venue}.amount_usd",
            )
            if amount < 0:
                _add_blocker(
                    blockers,
                    f"PLANNED_DEPOSIT_MUST_BE_POSITIVE:{deposit.venue}",
                )
            elif amount == 0:
                _add_blocker(
                    blockers,
                    f"PLANNED_DEPOSIT_MUST_BE_POSITIVE:{deposit.venue}",
                )
            else:
                amounts[deposit.venue] = amount
        except ReadinessViolation:
            _add_blocker(
                blockers,
                f"PLANNED_DEPOSIT_INVALID:{deposit.venue}",
            )
    if set(deposits) != set(VENUES):
        _add_blocker(
            blockers,
            "PLANNED_DEPOSITS_MUST_INCLUDE_ONE_AMOUNT_PER_VENUE",
        )
        return None
    if set(amounts) != set(VENUES):
        return None
    total = sum((amounts[venue] for venue in VENUES), Decimal("0"))
    if total <= 0:
        _add_blocker(blockers, "PLANNED_DEPOSIT_TOTAL_MUST_BE_POSITIVE")
    return total


def assess_private_read(
    evidence: ReadinessEvidence,
    private_read: PrivateReadEvidence,
) -> PrivateReadResult:
    """Evaluate Phase C private-read facts after safe files exist."""

    public = assess_readiness(evidence)
    if public.status != READY_FOR_PRIVATE_READ_GATES:
        blocker = "PRIVATE_READ_REQUIRES_READY_PRIVATE_READ_GATES"
        inherited = public.blockers or (blocker,)
        return _invalid_private_result(public, (blocker, *inherited))
    if not isinstance(private_read, PrivateReadEvidence):
        return _invalid_private_result(
            public,
            ("PRIVATE_READ_EVIDENCE_SCHEMA_INVALID",),
        )
    if not isinstance(private_read.identities, tuple) or not all(
        isinstance(item, AccountIdentity) for item in private_read.identities
    ):
        return _invalid_private_result(
            public,
            ("PRIVATE_READ_EVIDENCE_SCHEMA_INVALID:identities",),
        )
    if not isinstance(private_read.venues, tuple) or not all(
        isinstance(item, PrivateVenueRead) for item in private_read.venues
    ):
        return _invalid_private_result(
            public,
            ("PRIVATE_READ_EVIDENCE_SCHEMA_INVALID:venues",),
        )
    if not isinstance(private_read.planned_deposits, tuple) or not all(
        isinstance(item, PlannedDeposit) for item in private_read.planned_deposits
    ):
        return _invalid_private_result(
            public,
            ("PRIVATE_READ_EVIDENCE_SCHEMA_INVALID:planned_deposits",),
        )

    blockers: list[str] = []
    account_ids = _private_identity_map(private_read, blockers)
    private_venues: dict[str, PrivateVenueRead] = {}
    for venue in private_read.venues:
        if venue.venue not in VENUES:
            _add_blocker(blockers, "PRIVATE_VENUE_READ_OUT_OF_SCOPE")
            continue
        if venue.venue in private_venues:
            _add_blocker(
                blockers,
                f"PRIVATE_VENUE_READ_DUPLICATE:{venue.venue}",
            )
        private_venues[venue.venue] = venue
        if venue.account_id != account_ids.get(venue.venue):
            _add_blocker(
                blockers,
                f"PRIVATE_VENUE_ACCOUNT_BINDING_MISMATCH:{venue.venue}",
            )
        if (
            venue.fee_status != "ACCOUNT_SCOPED_AUTHORITATIVE"
            or venue.fee_source != "ACCOUNT_SCOPED_READ"
        ):
            _add_blocker(
                blockers,
                f"ACCOUNT_FEE_NOT_AUTHORITATIVE:{venue.venue}",
            )
        if not _valid_decimal(
            venue.maker_fee_rate,
            f"{venue.venue}.maker_fee_rate",
        ):
            _add_blocker(
                blockers,
                f"ACCOUNT_FEE_VALUE_INVALID:{venue.venue}:maker",
            )
        if not _valid_decimal(
            venue.taker_fee_rate,
            f"{venue.venue}.taker_fee_rate",
        ):
            _add_blocker(
                blockers,
                f"ACCOUNT_FEE_VALUE_INVALID:{venue.venue}:taker",
            )
        if (
            venue.private_stream_status != "READY_ACCOUNT_SCOPED"
            or venue.private_stream_source != "ACCOUNT_PRIVATE_STREAM"
        ):
            _add_blocker(
                blockers,
                f"PRIVATE_STREAM_NOT_READY:{venue.venue}",
            )
        if venue.unrelated_state_clear is not True:
            _add_blocker(
                blockers,
                f"UNRELATED_PRIVATE_STATE_PRESENT:{venue.venue}",
            )
    if set(private_venues) != set(VENUES) or len(private_read.venues) != len(VENUES):
        _add_blocker(blockers, "PRIVATE_READ_REQUIRED_FOR_BOTH_VENUES")

    planned_total = _private_deposit_map(
        private_read,
        account_ids,
        blockers,
    )
    if blockers:
        return _invalid_private_result(
            public,
            tuple(blockers),
            planned_total=planned_total,
        )
    return PrivateReadResult(
        status=PRIVATE_READ_GATES_COMPLETE,
        reason="AUTHENTICATED_PRIVATE_READ_REQUIREMENTS_PROVEN",
        blockers=(),
        route_id=public.route_id,
        direction=public.direction,
        planned_deposit_total_usd=planned_total,
    )


def _approval_result(
    public: ReadinessResult,
    *,
    status: str,
    reason: str,
    blockers: tuple[str, ...] = (),
) -> DispatchApprovalResult:
    return DispatchApprovalResult(
        status=status,
        reason=reason,
        blockers=blockers,
        route_id=public.route_id,
        direction=public.direction,
    )


def _validate_operational(
    operational: OperationalEvidence,
    account_ids: Mapping[str, str],
    blockers: list[str],
) -> None:
    if not _valid_token(operational.runtime_id, "runtime_id"):
        _add_blocker(blockers, "RUNTIME_IDENTITY_INVALID")
    if operational.runtime_fresh is not True:
        _add_blocker(blockers, "RUNTIME_IDENTITY_NOT_FRESH")
    if operational.runtime_durable_before_dispatch is not True:
        _add_blocker(blockers, "RUNTIME_IDENTITY_NOT_DURABLE_BEFORE_DISPATCH")
    if operational.captured_immediately_before_dispatch is not True:
        _add_blocker(blockers, "RUNTIME_WRITE_EVIDENCE_NOT_IMMEDIATELY_BEFORE_DISPATCH")

    dispatches = operational.dispatches
    if not isinstance(dispatches, tuple) or len(dispatches) != len(DISPATCH_SEQUENCE):
        _add_blocker(blockers, "EXACTLY_FOUR_DISPATCHES_REQUIRED")
    elif not all(isinstance(item, DispatchIdentity) for item in dispatches):
        _add_blocker(blockers, "DISPATCH_EVIDENCE_SCHEMA_INVALID")
    else:
        write_ids: list[str] = []
        for expected_sequence, (dispatch, expected) in enumerate(
            zip(dispatches, DISPATCH_SEQUENCE),
            start=1,
        ):
            expected_venue, expected_purpose = expected
            if (
                dispatch.sequence != expected_sequence
                or dispatch.venue != expected_venue
                or dispatch.purpose != expected_purpose
            ):
                _add_blocker(blockers, "DISPATCH_SEQUENCE_NOT_EXACT")
            if dispatch.account_id != account_ids.get(expected_venue):
                _add_blocker(
                    blockers,
                    f"DISPATCH_ACCOUNT_BINDING_MISMATCH:{expected_venue}:{expected_purpose}",
                )
            if dispatch.runtime_id != operational.runtime_id:
                _add_blocker(blockers, "DISPATCH_RUNTIME_IDENTITY_MISMATCH")
            if _valid_token(dispatch.write_identity, "dispatch.write_identity"):
                write_ids.append(dispatch.write_identity)
            else:
                _add_blocker(blockers, "DISPATCH_WRITE_IDENTITY_INVALID")
            if dispatch.durable_before_dispatch is not True:
                _add_blocker(
                    blockers,
                    f"DISPATCH_WRITE_IDENTITY_NOT_DURABLE:{expected_venue}:{expected_purpose}",
                )
        if len(write_ids) != len(DISPATCH_SEQUENCE) or len(set(write_ids)) != len(
            write_ids
        ):
            _add_blocker(blockers, "DISPATCH_WRITE_IDENTITIES_NOT_DISTINCT")
    if operational.sequential_writes is not True:
        _add_blocker(blockers, "SEQUENTIAL_WRITE_CONTRACT_NOT_PROVEN")
    if operational.no_blind_replay is not True:
        _add_blocker(blockers, "AMBIGUOUS_WRITE_REPLAY_NOT_BLOCKED")
    if operational.restart_requires_reconciliation is not True:
        _add_blocker(blockers, "RESTART_RECONCILIATION_NOT_REQUIRED")


def assess_dispatch_approval(
    evidence: ReadinessEvidence,
    private_read: PrivateReadEvidence,
    approval_evidence: DispatchApprovalEvidence,
) -> DispatchApprovalResult:
    """Evaluate Phase D future approval and immediately-before-dispatch facts."""

    public = assess_readiness(evidence)
    private = assess_private_read(evidence, private_read)
    if not private.complete:
        return _approval_result(
            public,
            status=BLOCKED,
            reason="PRIVATE_READ_REQUIREMENTS_REQUIRED_FOR_APPROVAL",
            blockers=(
                "PRIVATE_READ_REQUIREMENTS_REQUIRED_FOR_APPROVAL",
                *private.blockers,
            ),
        )
    if not isinstance(approval_evidence, DispatchApprovalEvidence):
        return _approval_result(
            public,
            status=BLOCKED,
            reason="DISPATCH_APPROVAL_EVIDENCE_SCHEMA_INVALID",
            blockers=("DISPATCH_APPROVAL_EVIDENCE_SCHEMA_INVALID",),
        )
    if not isinstance(approval_evidence.approval, FutureDispatchApproval):
        return _approval_result(
            public,
            status=BLOCKED,
            reason="DISPATCH_APPROVAL_SCHEMA_INVALID:approval",
            blockers=("DISPATCH_APPROVAL_SCHEMA_INVALID:approval",),
        )
    if not isinstance(approval_evidence.operational, OperationalEvidence):
        return _approval_result(
            public,
            status=BLOCKED,
            reason="DISPATCH_APPROVAL_SCHEMA_INVALID:operational",
            blockers=("DISPATCH_APPROVAL_SCHEMA_INVALID:operational",),
        )

    blockers: list[str] = []
    approval = approval_evidence.approval
    identities = {
        item.venue: item.account_id for item in private_read.identities
    }
    deposits = {
        item.venue: item.amount_usd for item in private_read.planned_deposits
    }
    if not _valid_token(approval.approval_id, "approval_id"):
        _add_blocker(blockers, "DISPATCH_APPROVAL_ID_INVALID")
    if approval.route_id != public.route_id or approval.direction != public.direction:
        _add_blocker(blockers, "DISPATCH_APPROVAL_ROUTE_MISMATCH")
    if approval.risex_venue != "RISEx" or approval.extended_venue != "Extended":
        _add_blocker(blockers, "DISPATCH_APPROVAL_VENUE_BINDING_MISMATCH")
    if approval.risex_account_id != identities.get("RISEx"):
        _add_blocker(blockers, "DISPATCH_APPROVAL_RISEX_ACCOUNT_MISMATCH")
    if approval.extended_account_id != identities.get("Extended"):
        _add_blocker(blockers, "DISPATCH_APPROVAL_EXTENDED_ACCOUNT_MISMATCH")
    if approval.risex_planned_deposit_usd != deposits.get("RISEx"):
        _add_blocker(blockers, "DISPATCH_APPROVAL_RISEX_DEPOSIT_MISMATCH")
    if approval.extended_planned_deposit_usd != deposits.get("Extended"):
        _add_blocker(blockers, "DISPATCH_APPROVAL_EXTENDED_DEPOSIT_MISMATCH")
    if approval.approval_mode != "EXPLICIT_ABSOLUTE_USD":
        _add_blocker(
            blockers,
            "DISPATCH_APPROVAL_CAPS_MUST_BE_EXPLICIT_ABSOLUTE_USD",
        )
    try:
        deposit_cap = _decimal(
            approval.deposit_cap_usd,
            "approval.deposit_cap_usd",
        )
    except ReadinessViolation:
        deposit_cap = None
        _add_blocker(blockers, "DISPATCH_APPROVAL_DEPOSIT_CAP_INVALID")
    try:
        maximum_loss = _decimal(
            approval.maximum_loss_usd,
            "approval.maximum_loss_usd",
        )
    except ReadinessViolation:
        maximum_loss = None
        _add_blocker(blockers, "DISPATCH_APPROVAL_MAXIMUM_LOSS_CAP_INVALID")
    if deposit_cap is not None and deposit_cap <= 0:
        _add_blocker(blockers, "DISPATCH_APPROVAL_DEPOSIT_CAP_MUST_BE_POSITIVE")
    if maximum_loss is not None and maximum_loss <= 0:
        _add_blocker(
            blockers,
            "DISPATCH_APPROVAL_MAXIMUM_LOSS_CAP_MUST_BE_POSITIVE",
        )
    if (
        deposit_cap is not None
        and maximum_loss is not None
        and maximum_loss > deposit_cap
    ):
        _add_blocker(
            blockers,
            "DISPATCH_APPROVAL_MAXIMUM_LOSS_EXCEEDS_DEPOSIT_CAP",
        )
    if (
        deposit_cap is not None
        and private.planned_deposit_total_usd is not None
        and private.planned_deposit_total_usd > deposit_cap
    ):
        _add_blocker(
            blockers,
            "DISPATCH_APPROVAL_PLANNED_DEPOSITS_EXCEED_DEPOSIT_CAP",
        )
    if (
        maximum_loss is not None
        and public.loss_bound_usd is not None
        and public.loss_bound_usd > maximum_loss
    ):
        _add_blocker(
            blockers,
            "DISPATCH_APPROVAL_ROUTE_LOSS_EXCEEDS_MAXIMUM_LOSS_CAP",
        )
    if approval.scope != DISPATCH_APPROVAL_SCOPE:
        _add_blocker(blockers, "DISPATCH_APPROVAL_SCOPE_INVALID")
    if approval.manual_lifecycle_dispatch_authorized is not True:
        _add_blocker(blockers, "MANUAL_LIFECYCLE_DISPATCH_NOT_AUTHORIZED")
    if (
        type(approval.authorization_count) is not int
        or approval.authorization_count != 1
    ):
        _add_blocker(
            blockers,
            "EXACTLY_ONE_MANUAL_LIFECYCLE_AUTHORIZATION_REQUIRED",
        )

    _validate_operational(
        approval_evidence.operational,
        identities,
        blockers,
    )
    if blockers:
        return _approval_result(
            public,
            status=BLOCKED,
            reason=blockers[0],
            blockers=tuple(blockers),
        )
    return _approval_result(
        public,
        status=FUTURE_DISPATCH_APPROVAL_COMPLETE,
        reason="FUTURE_MANUAL_LIFECYCLE_APPROVAL_BOUND",
    )


def _expected_entry_sides(direction: str) -> Mapping[str, str]:
    if direction == "LONG_RISEX_SHORT_EXTENDED":
        return {"RISEx": "LONG", "Extended": "SHORT"}
    if direction == "SHORT_RISEX_LONG_EXTENDED":
        return {"RISEx": "SHORT", "Extended": "LONG"}
    return {}


def _terminal_fingerprint(round_value: TerminalRound) -> tuple[Any, ...]:
    return (
        round_value.phase,
        round_value.signature,
        round_value.relevant_open_orders,
        round_value.trigger_orders,
        round_value.unrelated_open_orders,
        round_value.unrelated_positions,
        round_value.risex_net_position_quantity,
        round_value.extended_net_position_quantity,
        round_value.authoritative,
    )


def _validate_lifecycle(
    public: ReadinessResult,
    evidence: ReadinessEvidence,
    private_read: PrivateReadEvidence,
    lifecycle: LifecycleEvidence,
    canonical_asset: str,
) -> list[str]:
    blockers: list[str] = []
    identities = {
        item.venue: item.account_id for item in private_read.identities
    }
    expected_sides = _expected_entry_sides(public.direction)
    public_schedule = {
        item.venue: item.next_funding_at for item in evidence.venues
    }
    execution_ids: list[str] = []
    for venue, leg in (
        ("RISEx", lifecycle.execution.risex),
        ("Extended", lifecycle.execution.extended),
    ):
        if leg.venue != venue:
            _add_blocker(blockers, f"EXECUTION_VENUE_BINDING_MISMATCH:{venue}")
        if leg.account_id != identities.get(venue):
            _add_blocker(blockers, f"EXECUTION_ACCOUNT_BINDING_MISMATCH:{venue}")
        if leg.route_id != public.route_id:
            _add_blocker(blockers, f"EXECUTION_ROUTE_ID_MISMATCH:{venue}")
        if leg.canonical_asset != canonical_asset:
            _add_blocker(blockers, f"EXECUTION_ASSET_MISMATCH:{venue}")
        if leg.entry_side != expected_sides.get(venue):
            _add_blocker(blockers, f"EXECUTION_ENTRY_SIDE_MISMATCH:{venue}")
        if leg.canonical_quantity != public.common_quantity:
            _add_blocker(
                blockers,
                f"EXECUTION_QUANTITY_NOT_EXACT_COMMON_QUANTITY:{venue}",
            )
        for field_name, field_value in (
            ("order_id", leg.order_id),
            ("fill_id", leg.fill_id),
            ("position_id", leg.position_id),
            ("close_order_id", leg.close_order_id),
            ("close_fill_id", leg.close_fill_id),
        ):
            if _valid_token(field_value, f"execution.{venue}.{field_name}"):
                execution_ids.append(field_value)
            else:
                _add_blocker(
                    blockers,
                    f"EXECUTION_IDENTITY_INVALID:{venue}:{field_name}",
                )
        if leg.order_reconciled is not True:
            _add_blocker(blockers, f"ENTRY_ORDER_NOT_EXACTLY_RECONCILED:{venue}")
        if leg.fill_reconciled is not True:
            _add_blocker(blockers, f"ENTRY_FILL_NOT_EXACTLY_RECONCILED:{venue}")
        if leg.position_reconciled is not True:
            _add_blocker(
                blockers,
                f"ENTRY_POSITION_NOT_EXACTLY_RECONCILED:{venue}",
            )
        if leg.authoritative is not True:
            _add_blocker(
                blockers,
                f"ENTRY_RECONCILIATION_NOT_AUTHORITATIVE:{venue}",
            )
        for field_name, field_value in (
            ("order", leg.order_authoritative),
            ("fill", leg.fill_authoritative),
            ("position", leg.position_authoritative),
        ):
            if field_value is not True:
                _add_blocker(
                    blockers,
                    f"ENTRY_{field_name.upper()}_NOT_AUTHORITATIVE:{venue}",
                )
        if leg.reduce_only is not True:
            _add_blocker(blockers, f"REDUCE_ONLY_CLOSE_NOT_ACCEPTED:{venue}")
        if leg.close_reconciled is not True or leg.close_authoritative is not True:
            _add_blocker(
                blockers,
                f"CLOSE_NOT_EXACTLY_RECONCILED:{venue}",
            )
    if len(execution_ids) != len(VENUES) * 5 or len(set(execution_ids)) != len(
        execution_ids
    ):
        _add_blocker(blockers, "EXECUTION_IDENTITIES_NOT_DISTINCT")

    expected_funding = {
        (venue, phase) for venue in VENUES for phase in FUNDING_PHASES
    }
    seen_funding: set[tuple[str, str]] = set()
    funding_by_venue: dict[str, list[FundingObservation]] = {
        venue: [] for venue in VENUES
    }
    for observation in lifecycle.funding:
        key = (observation.venue, observation.phase)
        if key in seen_funding:
            _add_blocker(
                blockers,
                f"FUNDING_DUPLICATE_OBSERVATION:{observation.venue}:{observation.phase}",
            )
        seen_funding.add(key)
        if observation.venue not in VENUES or observation.phase not in FUNDING_PHASES:
            _add_blocker(blockers, "FUNDING_IDENTITY_INVALID")
            continue
        funding_by_venue[observation.venue].append(observation)
        if observation.canonical_asset != canonical_asset:
            _add_blocker(
                blockers,
                f"FUNDING_ASSET_MISMATCH:{observation.venue}",
            )
        if not _valid_token(observation.settlement_id, "funding.settlement_id"):
            _add_blocker(
                blockers,
                f"FUNDING_SETTLEMENT_ID_INVALID:{observation.venue}",
            )
        if (
            type(observation.settlement_at) is not int
            or observation.settlement_at <= 0
        ):
            _add_blocker(
                blockers,
                f"FUNDING_SETTLEMENT_TIME_INVALID:{observation.venue}",
            )
        elif observation.settlement_at != public_schedule.get(observation.venue):
            _add_blocker(
                blockers,
                f"FUNDING_SCHEDULE_IDENTITY_MISMATCH:{observation.venue}",
            )
        if type(observation.observed_at) is not int or observation.observed_at <= 0:
            _add_blocker(
                blockers,
                f"FUNDING_OBSERVATION_TIME_INVALID:{observation.venue}",
            )
        if (
            type(observation.missing) is not bool
            or type(observation.contradictory) is not bool
        ):
            _add_blocker(
                blockers,
                f"FUNDING_FLAGS_INVALID:{observation.venue}:{observation.phase}",
            )
        if observation.missing or observation.contradictory:
            _add_blocker(
                blockers,
                f"FUNDING_MISSING_OR_CONTRADICTORY:{observation.venue}:{observation.phase}",
            )
        if observation.status not in FUNDING_STATUSES:
            _add_blocker(
                blockers,
                f"FUNDING_STATUS_UNKNOWN:{observation.venue}:{observation.phase}",
            )
        if observation.cash_usd is None:
            _add_blocker(
                blockers,
                f"FUNDING_CASH_MISSING:{observation.venue}:{observation.phase}",
            )
        else:
            try:
                _decimal(observation.cash_usd, "funding.cash_usd")
            except ReadinessViolation:
                _add_blocker(
                    blockers,
                    f"FUNDING_CASH_INVALID:{observation.venue}:{observation.phase}",
                )
        if (
            observation.status == "PROVEN_NON_ACCRUAL"
            and observation.cash_usd not in (None, Decimal("0"))
        ):
            _add_blocker(
                blockers,
                f"FUNDING_NON_ACCRUAL_CASH_CONTRADICTION:{observation.venue}:{observation.phase}",
            )
        for field_name, field_value in (
            ("eligible_known", observation.eligible_known),
            ("exposure_confirmed", observation.exposure_confirmed),
            ("authoritative", observation.authoritative),
        ):
            if field_value is not True:
                _add_blocker(
                    blockers,
                    f"FUNDING_{field_name.upper()}_NOT_PROVEN:{observation.venue}:{observation.phase}",
                )

    if seen_funding != expected_funding:
        _add_blocker(
            blockers,
            "FUNDING_BEFORE_AT_AFTER_REQUIRED_FOR_BOTH_VENUES",
        )
    for venue in VENUES:
        observations = funding_by_venue[venue]
        if len(observations) != len(FUNDING_PHASES):
            continue
        observations.sort(key=lambda item: FUNDING_PHASES.index(item.phase))
        settlement_ids = {item.settlement_id for item in observations}
        settlement_times = {item.settlement_at for item in observations}
        if len(settlement_ids) != 1 or len(settlement_times) != 1:
            _add_blocker(blockers, f"FUNDING_IDENTITY_CONTRADICTION:{venue}")
        if any(
            earlier.observed_at >= later.observed_at
            for earlier, later in zip(observations, observations[1:])
        ):
            _add_blocker(blockers, f"FUNDING_PHASE_ORDER_INVALID:{venue}")

    terminal_rounds = lifecycle.terminal_rounds
    if (
        len(terminal_rounds) != 2
        or {item.round_number for item in terminal_rounds} != {1, 2}
    ):
        _add_blocker(blockers, "EXACTLY_TWO_TERMINAL_ROUNDS_REQUIRED")
    ordered_rounds = sorted(terminal_rounds, key=lambda item: item.round_number)
    for terminal in ordered_rounds:
        if terminal.phase != "TERMINAL" or terminal.authoritative is not True:
            _add_blocker(
                blockers,
                f"TERMINAL_ROUND_NOT_AUTHORITATIVE:{terminal.round_number}",
            )
        if not _valid_token(terminal.signature, "terminal.signature"):
            _add_blocker(
                blockers,
                f"TERMINAL_SIGNATURE_INVALID:{terminal.round_number}",
            )
        if type(terminal.observed_at) is not int or terminal.observed_at <= 0:
            _add_blocker(
                blockers,
                f"TERMINAL_TIME_INVALID:{terminal.round_number}",
            )
        for field_name, field_value in (
            ("relevant_open_orders", terminal.relevant_open_orders),
            ("trigger_orders", terminal.trigger_orders),
            ("unrelated_open_orders", terminal.unrelated_open_orders),
            ("unrelated_positions", terminal.unrelated_positions),
        ):
            if type(field_value) is not int or field_value < 0:
                _add_blocker(
                    blockers,
                    f"TERMINAL_COUNT_INVALID:{field_name}",
                )
        if terminal.relevant_open_orders != 0:
            _add_blocker(blockers, "TERMINAL_RELEVANT_ORDERS_NOT_ZERO")
        if terminal.trigger_orders != 0:
            _add_blocker(blockers, "TERMINAL_TRIGGER_ORDERS_NOT_ZERO")
        if terminal.unrelated_open_orders != 0 or terminal.unrelated_positions != 0:
            _add_blocker(blockers, "UNRELATED_ACCOUNT_STATE_PRESENT")
        if (
            terminal.risex_net_position_quantity != 0
            or terminal.extended_net_position_quantity != 0
        ):
            _add_blocker(blockers, "EXACT_FLATNESS_NOT_PROVEN")
    if len(ordered_rounds) == 2:
        if ordered_rounds[0].observed_at >= ordered_rounds[1].observed_at:
            _add_blocker(blockers, "TERMINAL_ROUNDS_NOT_SEQUENTIAL")
        if _terminal_fingerprint(ordered_rounds[0]) != _terminal_fingerprint(
            ordered_rounds[1]
        ):
            _add_blocker(blockers, "TERMINAL_ROUNDS_DISAGREE")
    return blockers


def assess_post_lifecycle(
    evidence: ReadinessEvidence,
    private_read: PrivateReadEvidence | None = None,
    approval_evidence: DispatchApprovalEvidence | None = None,
    lifecycle: LifecycleEvidence | None = None,
) -> LifecycleResult:
    """Evaluate Phase E post-dispatch lifecycle evidence separately."""

    public = assess_readiness(evidence)
    if private_read is None or approval_evidence is None or lifecycle is None:
        return LifecycleResult(
            status=BLOCKED,
            reason="POST_LIFECYCLE_EVIDENCE_REQUIRES_PRIOR_PHASES",
            blockers=("POST_LIFECYCLE_EVIDENCE_REQUIRES_PRIOR_PHASES",),
            route_id=public.route_id,
            direction=public.direction,
        )
    private = assess_private_read(evidence, private_read)
    approval = assess_dispatch_approval(
        evidence,
        private_read,
        approval_evidence,
    )
    if not private.complete:
        return LifecycleResult(
            status=BLOCKED,
            reason="PRIVATE_READ_REQUIREMENTS_REQUIRED_FOR_LIFECYCLE",
            blockers=(
                "PRIVATE_READ_REQUIREMENTS_REQUIRED_FOR_LIFECYCLE",
                *private.blockers,
            ),
            route_id=public.route_id,
            direction=public.direction,
        )
    if not approval.complete:
        return LifecycleResult(
            status=BLOCKED,
            reason="DISPATCH_APPROVAL_REQUIRED_FOR_LIFECYCLE",
            blockers=(
                "DISPATCH_APPROVAL_REQUIRED_FOR_LIFECYCLE",
                *approval.blockers,
            ),
            route_id=public.route_id,
            direction=public.direction,
        )
    if not isinstance(lifecycle, LifecycleEvidence):
        return LifecycleResult(
            status=BLOCKED,
            reason="POST_LIFECYCLE_EVIDENCE_SCHEMA_INVALID",
            blockers=("POST_LIFECYCLE_EVIDENCE_SCHEMA_INVALID",),
            route_id=public.route_id,
            direction=public.direction,
        )
    if not isinstance(lifecycle.execution, ExecutionEvidence) or not isinstance(
        lifecycle.execution.risex,
        LegExecutionEvidence,
    ) or not isinstance(
        lifecycle.execution.extended,
        LegExecutionEvidence,
    ):
        return LifecycleResult(
            status=BLOCKED,
            reason="POST_LIFECYCLE_EXECUTION_SCHEMA_INVALID",
            blockers=("POST_LIFECYCLE_EXECUTION_SCHEMA_INVALID",),
            route_id=public.route_id,
            direction=public.direction,
        )
    if not isinstance(lifecycle.funding, tuple) or not all(
        isinstance(item, FundingObservation) for item in lifecycle.funding
    ):
        return LifecycleResult(
            status=BLOCKED,
            reason="POST_LIFECYCLE_FUNDING_SCHEMA_INVALID",
            blockers=("POST_LIFECYCLE_FUNDING_SCHEMA_INVALID",),
            route_id=public.route_id,
            direction=public.direction,
        )
    if not isinstance(lifecycle.terminal_rounds, tuple) or not all(
        isinstance(item, TerminalRound) for item in lifecycle.terminal_rounds
    ):
        return LifecycleResult(
            status=BLOCKED,
            reason="POST_LIFECYCLE_TERMINAL_SCHEMA_INVALID",
            blockers=("POST_LIFECYCLE_TERMINAL_SCHEMA_INVALID",),
            route_id=public.route_id,
            direction=public.direction,
        )
    blockers = _validate_lifecycle(
        public,
        evidence,
        private_read,
        lifecycle,
        evidence.route.canonical_asset,
    )
    if blockers:
        return LifecycleResult(
            status=BLOCKED,
            reason=blockers[0],
            blockers=tuple(blockers),
            route_id=public.route_id,
            direction=public.direction,
        )
    return LifecycleResult(
        status=POST_LIFECYCLE_EVIDENCE_COMPLETE,
        reason="POST_DISPATCH_TERMINAL_EVIDENCE_PROVEN",
        blockers=(),
        route_id=public.route_id,
        direction=public.direction,
    )


__all__ = [
    "AccountIdentity",
    "BLOCKED",
    "DISPATCH_APPROVAL_SCOPE",
    "DISPATCH_SEQUENCE",
    "DispatchApprovalEvidence",
    "DispatchApprovalResult",
    "DispatchIdentity",
    "ExecutionEvidence",
    "FUNDING_PHASES",
    "FUNDING_STATUSES",
    "FUTURE_DISPATCH_APPROVAL_COMPLETE",
    "FutureDispatchApproval",
    "FundingObservation",
    "LegExecutionEvidence",
    "LifecycleEvidence",
    "LifecycleResult",
    "NO_MAINNET_WRITE_AUTHORITY",
    "OPPOSITE_DIRECTIONS",
    "OperationalEvidence",
    "POST_LIFECYCLE_EVIDENCE_COMPLETE",
    "PRIVATE_READ_GATES_COMPLETE",
    "PROTECTED_SECRET_DIRECTORY",
    "PROVISIONED",
    "PlannedDeposit",
    "PrivateReadEvidence",
    "PrivateReadResult",
    "PrivateVenueRead",
    "ProvisioningResult",
    "ProtectedFileState",
    "ProtectedSecretFiles",
    "READY_FOR_PRIVATE_READ_GATES",
    "READY_FOR_PROTECTED_PROVISIONING",
    "ReadinessEvidence",
    "ReadinessResult",
    "ReadinessViolation",
    "RouteEvidence",
    "TerminalRound",
    "VENUES",
    "VenueReadiness",
    "assess_dispatch_approval",
    "assess_post_lifecycle",
    "assess_private_read",
    "assess_readiness",
    "inspect_protected_secret_files",
    "provision_protected_identities",
    "protected_secret_paths",
]
