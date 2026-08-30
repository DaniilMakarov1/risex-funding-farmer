"""Offline/read-only RISEx--Nado mainnet readiness evidence.

This is a route-local evidence contract, not a Nado client.  It validates
public observations, future injected account-read evidence, and future
operator approval evidence without opening transport, persistence, signing,
order construction, or venue-action surfaces.

The RISEx identity is intentionally borrowed by path from the accepted
RISEx--Extended readiness boundary.  This module never writes that path.
Nado read evidence uses public wallet/subaccount identity and unsigned
queries; authenticated streams remain a future gate.
"""

from __future__ import annotations

import json
import math
import os
import stat
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
from typing import Any, Mapping, Sequence


READY_FOR_PROTECTED_PROVISIONING = "READY_FOR_PROTECTED_PROVISIONING"
READY_FOR_PRIVATE_READ_GATES = "READY_FOR_PRIVATE_READ_GATES"
PRIVATE_READ_GATES_COMPLETE = "PRIVATE_READ_GATES_COMPLETE"
FUTURE_DISPATCH_APPROVAL_COMPLETE = "FUTURE_DISPATCH_APPROVAL_COMPLETE"
POST_LIFECYCLE_EVIDENCE_COMPLETE = "POST_LIFECYCLE_EVIDENCE_COMPLETE"
BLOCKED = "BLOCKED"

NO_MAINNET_WRITE_AUTHORITY = "NO_MAINNET_WRITE_AUTHORITY"

VENUES = ("RISEx", "Nado")
OPPOSITE_DIRECTIONS = frozenset(
    {"LONG_RISEX_SHORT_NADO", "SHORT_RISEX_LONG_NADO"}
)
DISPATCH_SEQUENCE = (
    ("RISEx", "ENTRY"),
    ("Nado", "ENTRY"),
    ("RISEx", "CLOSE"),
    ("Nado", "CLOSE"),
)
DISPATCH_APPROVAL_SCOPE = "ONE_MANUAL_LIFECYCLE_DISPATCH"
FUNDING_PHASES = ("BEFORE", "AT", "AFTER")
FUNDING_STATUSES = frozenset({"APPLIED_RATE", "PROVEN_NON_ACCRUAL"})

# Offline contract metadata observed from the official Nado mainnet
# documentation.  These constants do not create a client or authorize an
# action.
NADO_MAINNET_CHAIN_ID = 57073
NADO_MAINNET_ENDPOINT_CONTRACT = "0x05ec92D78ED421f3D3Ada77FFdE167106565974E"
NADO_MAINNET_PERP_ENGINE_CONTRACT = "0xF8599D58d1137fC56EcDd9C16ee139C8BDf96da1"
NADO_BTC_PERP_PRODUCT_ID = 2
NADO_PUBLIC_MINIMUM_NOTIONAL_USD = Decimal("100")
NADO_PUBLIC_MINIMUM_FEE_NOTIONAL_USD = Decimal("100")
NADO_PUBLIC_IDENTITY_SOURCE = "PUBLIC_WALLET_SUBACCOUNT"
NADO_UNSIGNED_QUERY_AUTHENTICATION = "UNSIGNED_QUERY"
NADO_UNSIGNED_QUERY_STATUS = "AUTHORITATIVE_UNSIGNED_QUERY"
NADO_UNSIGNED_QUERY_SOURCE = "NADO_UNSIGNED_QUERY"
NADO_PRIVATE_STREAM_PENDING_STATUS = "PENDING_SIGNED_AUTHENTICATE"
NADO_PRIVATE_STREAM_PENDING_SOURCE = "FUTURE_SIGNED_AUTHENTICATE"
NADO_REQUIRED_QUERY_KINDS = frozenset(
    {"SUBACCOUNT_INFO", "SUBACCOUNT_ORDERS", "FEE_RATES"}
)
NADO_SDK_PACKAGE = "nado-protocol"
NADO_SDK_VERSION = "2.0.0"

# This is the already accepted canonical RISEx identity path.  It is
# deliberately a separate path from the Nado directory and is never written
# by this module.
CANONICAL_RISEX_IDENTITY_PATH = (
    Path.home()
    / ".config"
    / "risex-farmer"
    / "extended-mainnet-readiness"
    / "risex.identity"
)
_SECRET_MAX_BYTES = 4096
_PROTECTED_DIRECTORY_MODE = 0o700
_PROTECTED_FILE_MODE = 0o600


class ReadinessViolation(ValueError):
    """A bounded evidence or protected-file contract violation."""


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


def _optional_decimal(
    value: Mapping[str, Any], key: str, field: str
) -> Decimal | None:
    if value.get(key) is None:
        return None
    return _decimal(value[key], field)


def _required(value: Any, key: str, label: str) -> Any:
    if not isinstance(value, Mapping) or key not in value:
        raise ReadinessViolation(f"FIXTURE_FIELD_MISSING:{label}.{key}")
    return value[key]


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ReadinessViolation(f"FIXTURE_MAPPING_INVALID:{field}")
    return value


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


def _hex_identifier(value: Any, field: str, byte_length: int) -> str:
    if (
        type(value) is not str
        or len(value) != 2 + (byte_length * 2)
        or not value.startswith("0x")
        or any(char not in "0123456789abcdefABCDEF" for char in value[2:])
    ):
        raise ReadinessViolation(f"HEX_IDENTITY_INVALID:{field}")
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
    wallet_address: str | None = None
    subaccount: str | None = None
    identity_source: str | None = None


@dataclass(frozen=True)
class VenueReadiness:
    venue: str
    canonical_asset: str
    market: str
    product_id: int | None
    active: bool
    linear_perpetual: bool
    non_rfq: bool
    metadata_current: bool
    minimum_quantity: Decimal
    quantity_step: Decimal
    minimum_notional_usd: Decimal | None
    minimum_fee_notional_usd: Decimal | None
    tick_size_usd: Decimal
    best_bid_usd: Decimal
    best_ask_usd: Decimal
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
    """Phase-A public/offline evidence only."""

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
            "approval",
            "operational",
            "execution",
            "funding",
            "terminal_rounds",
            "lifecycle",
        ):
            if forbidden in raw:
                raise ReadinessViolation(
                    f"PUBLIC_EVIDENCE_MUST_NOT_CLAIM:{forbidden}"
                )

        route = _mapping(_required(raw, "route", "root"), "route")
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

        venue_rows = _sequence(_required(raw, "venues", "root"), "venues")
        venues = tuple(
            VenueReadiness(
                venue=_required(item, "venue", "venue"),
                canonical_asset=_required(item, "canonical_asset", "venue"),
                market=_required(item, "market", "venue"),
                product_id=item.get("product_id"),
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
                minimum_notional_usd=_optional_decimal(
                    item, "minimum_notional_usd", "minimum_notional_usd"
                ),
                minimum_fee_notional_usd=_optional_decimal(
                    item,
                    "minimum_fee_notional_usd",
                    "minimum_fee_notional_usd",
                ),
                tick_size_usd=_decimal(
                    _required(item, "tick_size_usd", "venue"),
                    "tick_size_usd",
                ),
                best_bid_usd=_decimal(
                    _required(item, "best_bid_usd", "venue"),
                    "best_bid_usd",
                ),
                best_ask_usd=_decimal(
                    _required(item, "best_ask_usd", "venue"),
                    "best_ask_usd",
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
                    item, "maker_fee_rate", "venue.maker_fee_rate"
                ),
                taker_fee_rate=_optional_decimal(
                    item, "taker_fee_rate", "venue.taker_fee_rate"
                ),
                schedule_status=_required(item, "schedule_status", "venue"),
                schedule_source=_required(item, "schedule_source", "venue"),
                funding_interval_seconds=_required(
                    item, "funding_interval_seconds", "venue"
                ),
                next_funding_at=_required(item, "next_funding_at", "venue"),
                private_stream_status=_required(
                    item, "private_stream_status", "venue"
                ),
                private_stream_source=_required(
                    item, "private_stream_source", "venue"
                ),
            )
            for item in (_mapping(item, "venue") for item in venue_rows)
        )
        return cls(route=route_evidence, venues=venues)


@dataclass(frozen=True)
class PrivateVenueRead:
    venue: str
    account_id: str
    query_authentication: str
    query_status: str
    query_source: str
    query_kinds: tuple[str, ...]
    fee_status: str
    fee_source: str
    maker_fee_rate: Decimal
    taker_fee_rate: Decimal
    private_stream_status: str
    private_stream_source: str
    collateral_status: str
    collateral_usd: Decimal
    zero_relevant_orders: bool
    zero_trigger_orders: bool
    exact_flat: bool
    unrelated_state_clear: bool


@dataclass(frozen=True)
class PlannedDeposit:
    venue: str
    account_id: str
    amount_usd: Decimal


@dataclass(frozen=True)
class PrivateReadEvidence:
    """Injected Phase-C account-scoped read evidence."""

    identities: tuple[AccountIdentity, ...]
    venues: tuple[PrivateVenueRead, ...]
    planned_deposits: tuple[PlannedDeposit, ...]

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "PrivateReadEvidence":
        if not isinstance(raw, Mapping):
            raise ReadinessViolation("FIXTURE_SCHEMA_INVALID:private_read")
        for forbidden in ("approval", "operational", "execution", "lifecycle"):
            if forbidden in raw:
                raise ReadinessViolation(
                    f"PRIVATE_READ_MUST_NOT_CLAIM:{forbidden}"
                )
        identities = tuple(
            AccountIdentity(
                venue=_required(item, "venue", "identity"),
                account_id=_required(item, "account_id", "identity"),
                environment=_required(item, "environment", "identity"),
                exact=_required(item, "exact", "identity"),
                authoritative=_required(item, "authoritative", "identity"),
                wallet_address=item.get("wallet_address"),
                subaccount=item.get("subaccount"),
                identity_source=item.get("identity_source"),
            )
            for item in _sequence(
                _required(raw, "identities", "private_read"),
                "private_read.identities",
            )
        )
        private_venues = tuple(
            PrivateVenueRead(
                venue=_required(item, "venue", "private_venue"),
                account_id=_required(item, "account_id", "private_venue"),
                query_authentication=_required(
                    item, "query_authentication", "private_venue"
                ),
                query_status=_required(item, "query_status", "private_venue"),
                query_source=_required(item, "query_source", "private_venue"),
                query_kinds=tuple(
                    _token(value, "private_venue.query_kind")
                    for value in _sequence(
                        _required(item, "query_kinds", "private_venue"),
                        "private_venue.query_kinds",
                    )
                ),
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
                    item, "private_stream_status", "private_venue"
                ),
                private_stream_source=_required(
                    item, "private_stream_source", "private_venue"
                ),
                collateral_status=_required(
                    item, "collateral_status", "private_venue"
                ),
                collateral_usd=_decimal(
                    _required(item, "collateral_usd", "private_venue"),
                    "private_venue.collateral_usd",
                ),
                zero_relevant_orders=_required(
                    item, "zero_relevant_orders", "private_venue"
                ),
                zero_trigger_orders=_required(
                    item, "zero_trigger_orders", "private_venue"
                ),
                exact_flat=_required(item, "exact_flat", "private_venue"),
                unrelated_state_clear=_required(
                    item, "unrelated_state_clear", "private_venue"
                ),
            )
            for item in _sequence(
                _required(raw, "venues", "private_read"),
                "private_read.venues",
            )
        )
        deposits = tuple(
            PlannedDeposit(
                venue=_required(item, "venue", "planned_deposit"),
                account_id=_required(item, "account_id", "planned_deposit"),
                amount_usd=_decimal(
                    _required(item, "amount_usd", "planned_deposit"),
                    "planned_deposit.amount_usd",
                ),
            )
            for item in _sequence(
                _required(raw, "planned_deposits", "private_read"),
                "private_read.planned_deposits",
            )
        )
        return cls(
            identities=identities,
            venues=private_venues,
            planned_deposits=deposits,
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
    approval_id: str
    route_id: str
    direction: str
    risex_venue: str
    risex_account_id: str
    nado_venue: str
    nado_account_id: str
    risex_planned_deposit_usd: Decimal
    nado_planned_deposit_usd: Decimal
    deposit_cap_usd: Decimal
    maximum_loss_usd: Decimal
    approval_mode: str
    scope: str
    manual_lifecycle_dispatch_authorized: bool
    authorization_count: int


@dataclass(frozen=True)
class DispatchApprovalEvidence:
    approval: FutureDispatchApproval
    operational: OperationalEvidence

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "DispatchApprovalEvidence":
        if not isinstance(raw, Mapping):
            raise ReadinessViolation(
                "FIXTURE_SCHEMA_INVALID:dispatch_approval"
            )
        for forbidden in ("execution", "funding", "terminal_rounds", "lifecycle"):
            if forbidden in raw:
                raise ReadinessViolation(
                    f"DISPATCH_APPROVAL_MUST_NOT_CLAIM:{forbidden}"
                )
        approval = _mapping(
            _required(raw, "approval", "dispatch_approval"), "approval"
        )
        approval_evidence = FutureDispatchApproval(
            approval_id=_required(approval, "approval_id", "approval"),
            route_id=_required(approval, "route_id", "approval"),
            direction=_required(approval, "direction", "approval"),
            risex_venue=_required(approval, "risex_venue", "approval"),
            risex_account_id=_required(
                approval, "risex_account_id", "approval"
            ),
            nado_venue=_required(approval, "nado_venue", "approval"),
            nado_account_id=_required(
                approval, "nado_account_id", "approval"
            ),
            risex_planned_deposit_usd=_decimal(
                _required(
                    approval,
                    "risex_planned_deposit_usd",
                    "approval",
                ),
                "approval.risex_planned_deposit_usd",
            ),
            nado_planned_deposit_usd=_decimal(
                _required(
                    approval,
                    "nado_planned_deposit_usd",
                    "approval",
                ),
                "approval.nado_planned_deposit_usd",
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
                approval, "authorization_count", "approval"
            ),
        )

        operation = _mapping(
            _required(raw, "operational", "dispatch_approval"), "operational"
        )
        dispatches = tuple(
            DispatchIdentity(
                sequence=_required(item, "sequence", "dispatch"),
                venue=_required(item, "venue", "dispatch"),
                purpose=_required(item, "purpose", "dispatch"),
                account_id=_required(item, "account_id", "dispatch"),
                runtime_id=_required(item, "runtime_id", "dispatch"),
                write_identity=_required(
                    item, "write_identity", "dispatch"
                ),
                durable_before_dispatch=_required(
                    item, "durable_before_dispatch", "dispatch"
                ),
            )
            for item in (
                _mapping(item, "dispatch")
                for item in _sequence(
                    _required(operation, "dispatches", "operational"),
                    "operational.dispatches",
                )
            )
        )
        operational = OperationalEvidence(
            runtime_id=_required(operation, "runtime_id", "operational"),
            runtime_fresh=_required(
                operation, "runtime_fresh", "operational"
            ),
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
            dispatches=dispatches,
            sequential_writes=_required(
                operation, "sequential_writes", "operational"
            ),
            no_blind_replay=_required(
                operation, "no_blind_replay", "operational"
            ),
            restart_requires_reconciliation=_required(
                operation,
                "restart_requires_reconciliation",
                "operational",
            ),
        )
        return cls(approval=approval_evidence, operational=operational)


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
    entry_observed_at: int
    order_reconciled: bool
    fill_reconciled: bool
    position_reconciled: bool
    authoritative: bool
    close_order_id: str
    close_fill_id: str
    close_observed_at: int
    reduce_only: bool
    close_reconciled: bool
    close_authoritative: bool
    order_authoritative: bool = True
    fill_authoritative: bool = True
    position_authoritative: bool = True


@dataclass(frozen=True)
class ExecutionEvidence:
    risex: LegExecutionEvidence
    nado: LegExecutionEvidence


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
    cash_source: str
    public_aggregate_payment_usd: Decimal | None
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
    isolated_orders: int
    unrelated_open_orders: int
    unrelated_positions: int
    risex_net_position_quantity: Decimal
    nado_net_position_quantity: Decimal
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
                raise ReadinessViolation(f"LIFECYCLE_MUST_NOT_CLAIM:{forbidden}")
        execution = _mapping(
            _required(raw, "execution", "lifecycle"), "execution"
        )
        execution_evidence = ExecutionEvidence(
            risex=_parse_execution_leg(
                _mapping(_required(execution, "risex", "execution"), "execution.risex"),
                "execution.risex",
            ),
            nado=_parse_execution_leg(
                _mapping(_required(execution, "nado", "execution"), "execution.nado"),
                "execution.nado",
            ),
        )
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
                cash_source=_required(item, "cash_source", "funding"),
                public_aggregate_payment_usd=_optional_decimal(
                    item,
                    "public_aggregate_payment_usd",
                    "funding.public_aggregate_payment_usd",
                ),
                eligible_known=_required(
                    item, "eligible_known", "funding"
                ),
                exposure_confirmed=_required(
                    item, "exposure_confirmed", "funding"
                ),
                authoritative=_required(item, "authoritative", "funding"),
                missing=item.get("missing", False),
                contradictory=item.get("contradictory", False),
            )
            for item in (
                _mapping(item, "funding")
                for item in _sequence(_required(raw, "funding", "lifecycle"), "funding")
            )
        )
        terminal_rounds = tuple(
            TerminalRound(
                round_number=_required(item, "round_number", "terminal"),
                observed_at=_required(item, "observed_at", "terminal"),
                phase=_required(item, "phase", "terminal"),
                signature=_required(item, "signature", "terminal"),
                relevant_open_orders=_required(
                    item, "relevant_open_orders", "terminal"
                ),
                trigger_orders=_required(item, "trigger_orders", "terminal"),
                isolated_orders=_required(item, "isolated_orders", "terminal"),
                unrelated_open_orders=_required(
                    item, "unrelated_open_orders", "terminal"
                ),
                unrelated_positions=_required(
                    item, "unrelated_positions", "terminal"
                ),
                risex_net_position_quantity=_decimal(
                    _required(
                        item,
                        "risex_net_position_quantity",
                        "terminal",
                    ),
                    "terminal.risex_net_position_quantity",
                ),
                nado_net_position_quantity=_decimal(
                    _required(
                        item,
                        "nado_net_position_quantity",
                        "terminal",
                    ),
                    "terminal.nado_net_position_quantity",
                ),
                authoritative=_required(item, "authoritative", "terminal"),
            )
            for item in (
                _mapping(item, "terminal")
                for item in _sequence(
                    _required(raw, "terminal_rounds", "lifecycle"),
                    "terminal_rounds",
                )
            )
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
        entry_observed_at=_required(raw, "entry_observed_at", label),
        order_reconciled=_required(raw, "order_reconciled", label),
        fill_reconciled=_required(raw, "fill_reconciled", label),
        position_reconciled=_required(raw, "position_reconciled", label),
        authoritative=_required(raw, "authoritative", label),
        close_order_id=_required(raw, "close_order_id", label),
        close_fill_id=_required(raw, "close_fill_id", label),
        close_observed_at=_required(raw, "close_observed_at", label),
        reduce_only=_required(raw, "reduce_only", label),
        close_reconciled=_required(raw, "close_reconciled", label),
        close_authoritative=_required(raw, "close_authoritative", label),
        order_authoritative=_required(raw, "order_authoritative", label),
        fill_authoritative=_required(raw, "fill_authoritative", label),
        position_authoritative=_required(
            raw, "position_authoritative", label
        ),
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


def inspect_canonical_risex_identity() -> ProtectedFileState:
    """Inspect the accepted RISEx protected identity by metadata only."""

    return _file_state("RISEx", CANONICAL_RISEX_IDENTITY_PATH)


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


def _file_state(venue: str, path: Path) -> ProtectedFileState:
    parent_ok, parent_reason = _directory_state(path.parent)
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        return ProtectedFileState(
            venue=venue,
            path=str(path),
            present=False,
            protected=False,
            reason=parent_reason if not parent_ok else "PROTECTED_FILE_MISSING",
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
    if not parent_ok:
        reason = parent_reason
    elif stat.S_ISLNK(info.st_mode):
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


def _rational_lcm(values: Sequence[Decimal]) -> tuple[int, int]:
    fractions = [value.as_integer_ratio() for value in values]
    denominator = 1
    for _, item_denominator in fractions:
        denominator = math.lcm(denominator, item_denominator)
    units = [
        numerator * (denominator // item_denominator)
        for numerator, item_denominator in fractions
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


def _exact_multiple(value: Decimal, step: Decimal) -> bool:
    try:
        value_numerator, value_denominator = value.as_integer_ratio()
        step_numerator, step_denominator = step.as_integer_ratio()
        numerator = value_numerator * step_denominator
        denominator = value_denominator * step_numerator
        return denominator != 0 and numerator % denominator == 0
    except (ArithmeticError, ValueError):
        return False


def _venue_common_quantity(
    risex: VenueReadiness, nado: VenueReadiness
) -> Decimal:
    step_numerator, step_denominator = _rational_lcm(
        [risex.quantity_step, nado.quantity_step]
    )
    minimum = max(risex.minimum_quantity, nado.minimum_quantity)
    minimum_numerator, minimum_denominator = minimum.as_integer_ratio()
    notional_floor = NADO_PUBLIC_MINIMUM_NOTIONAL_USD / min(
        nado.best_bid_usd, nado.best_ask_usd
    )
    notional_numerator, notional_denominator = notional_floor.as_integer_ratio()
    required_numerator = max(
        minimum_numerator * notional_denominator,
        notional_numerator * minimum_denominator,
    )
    required_denominator = minimum_denominator * notional_denominator
    multiplier = _ceil_fraction(
        required_numerator * step_denominator,
        required_denominator * step_numerator,
    )
    return _fraction_to_decimal(
        step_numerator * multiplier,
        step_denominator,
    )


def _invalid_public_result(reason: str) -> ReadinessResult:
    return ReadinessResult(
        status=BLOCKED,
        reason=reason,
        blockers=(reason,),
        route_id="UNKNOWN_ROUTE",
        direction="UNKNOWN_DIRECTION",
        common_quantity=None,
        gross_trade_notional_usd=None,
        loss_bound_usd=None,
    )


def _public_evaluation(evidence: ReadinessEvidence) -> ReadinessResult:
    if not isinstance(evidence, ReadinessEvidence):
        return _invalid_public_result("EVIDENCE_SCHEMA_INVALID")
    if not isinstance(evidence.route, RouteEvidence):
        return _invalid_public_result("EVIDENCE_SCHEMA_INVALID:route")
    if not isinstance(evidence.venues, tuple) or not all(
        isinstance(item, VenueReadiness) for item in evidence.venues
    ):
        return _invalid_public_result("EVIDENCE_SCHEMA_INVALID:venues")

    blockers: list[str] = []
    route = evidence.route
    route_id = route.route_id if type(route.route_id) is str else "UNKNOWN_ROUTE"
    direction = (
        route.direction if type(route.direction) is str else "UNKNOWN_DIRECTION"
    )
    if not _valid_token(route.route_id, "route_id"):
        _add_blocker(blockers, "ROUTE_IDENTITY_NOT_EXACT")
    if not _valid_token(route.canonical_asset, "canonical_asset"):
        _add_blocker(blockers, "CANONICAL_ASSET_NOT_EXACT")
    if direction not in OPPOSITE_DIRECTIONS:
        _add_blocker(blockers, "ONE_ROUTE_DIRECTION_REQUIRED")
    if route.self_trade_free is not True:
        _add_blocker(blockers, "SELF_TRADE_GUARD_NOT_PROVEN")
    if route.counterparty_account_id is not None:
        _add_blocker(blockers, "PUBLIC_ROUTE_MUST_NOT_CLAIM_ACCOUNT_IDENTITY")
    try:
        loss_bound = _decimal(route.loss_bound_usd, "loss_bound_usd")
        if loss_bound < 0:
            _add_blocker(blockers, "LOSS_BOUND_MUST_NOT_BE_NEGATIVE")
    except ReadinessViolation:
        loss_bound = None
        _add_blocker(blockers, "LOSS_BOUND_MUST_BE_FINITE_DECIMAL")

    venue_map: dict[str, VenueReadiness] = {}
    expected = {
        "RISEx": {
            "market": "BTC/USDC",
            "product_id": None,
            "minimum_quantity": Decimal("0.00015"),
            "quantity_step": Decimal("0.000001"),
            "minimum_notional_usd": None,
            "minimum_fee_notional_usd": None,
        },
        "Nado": {
            "market": "BTC-PERP",
            "product_id": NADO_BTC_PERP_PRODUCT_ID,
            "minimum_quantity": Decimal("0.00005"),
            "quantity_step": Decimal("0.00005"),
            "minimum_notional_usd": NADO_PUBLIC_MINIMUM_NOTIONAL_USD,
            "minimum_fee_notional_usd": NADO_PUBLIC_MINIMUM_FEE_NOTIONAL_USD,
        },
    }
    for venue in evidence.venues:
        if venue.venue not in VENUES:
            _add_blocker(blockers, "VENUE_EVIDENCE_OUT_OF_SCOPE")
            continue
        if venue.venue in venue_map:
            _add_blocker(blockers, f"VENUE_EVIDENCE_DUPLICATE:{venue.venue}")
        venue_map[venue.venue] = venue
        contract = expected[venue.venue]
        if venue.canonical_asset != route.canonical_asset:
            _add_blocker(blockers, f"CANONICAL_ASSET_MISMATCH:{venue.venue}")
        if venue.market != contract["market"] or venue.product_id != contract["product_id"]:
            _add_blocker(blockers, f"MARKET_CONTRACT_MISMATCH:{venue.venue}")
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
            ("tick_size_usd", venue.tick_size_usd),
            ("best_bid_usd", venue.best_bid_usd),
            ("best_ask_usd", venue.best_ask_usd),
            ("reference_price_usd", venue.reference_price_usd),
            ("available_buy_quantity", venue.available_buy_quantity),
            ("available_sell_quantity", venue.available_sell_quantity),
        ):
            if not _valid_decimal(field_value, f"{venue.venue}.{field_name}", positive=True):
                _add_blocker(
                    blockers,
                    f"CURRENT_MARKET_VALUE_INVALID:{venue.venue}:{field_name}",
                )
        if venue.minimum_quantity != contract["minimum_quantity"]:
            _add_blocker(blockers, f"MINIMUM_QUANTITY_NOT_EXACT:{venue.venue}")
        if venue.quantity_step != contract["quantity_step"]:
            _add_blocker(blockers, f"QUANTITY_STEP_NOT_EXACT:{venue.venue}")
        if venue.minimum_notional_usd != contract["minimum_notional_usd"]:
            _add_blocker(blockers, f"MINIMUM_NOTIONAL_NOT_EXACT:{venue.venue}")
        if venue.minimum_fee_notional_usd != contract["minimum_fee_notional_usd"]:
            _add_blocker(blockers, f"MINIMUM_FEE_NOTIONAL_NOT_EXACT:{venue.venue}")
        if not _exact_multiple(venue.minimum_quantity, venue.quantity_step):
            _add_blocker(blockers, f"MINIMUM_QUANTITY_NOT_ON_STEP:{venue.venue}")
        if venue.tick_size_usd != Decimal("0.1"):
            _add_blocker(blockers, f"TICK_SIZE_NOT_EXACT:{venue.venue}")
        prices_valid = all(
            _valid_decimal(value, f"{venue.venue}.price", positive=True)
            for value in (
                venue.tick_size_usd,
                venue.best_bid_usd,
                venue.best_ask_usd,
                venue.reference_price_usd,
            )
        )
        if prices_valid:
            try:
                bbo_safe = (
                    venue.best_bid_usd < venue.best_ask_usd
                    and venue.best_bid_usd % venue.tick_size_usd == 0
                    and venue.best_ask_usd % venue.tick_size_usd == 0
                    and venue.best_bid_usd <= venue.reference_price_usd <= venue.best_ask_usd
                )
            except ArithmeticError:
                bbo_safe = False
            if not bbo_safe:
                _add_blocker(blockers, f"BBO_OR_REFERENCE_NOT_SAFE:{venue.venue}")
        if venue.fee_status != "PENDING_ACCOUNT_SCOPED" or venue.fee_source != "PRIVATE_READ_PENDING":
            _add_blocker(blockers, f"ACCOUNT_FEE_MUST_REMAIN_PENDING:{venue.venue}")
        if venue.maker_fee_rate is not None or venue.taker_fee_rate is not None:
            _add_blocker(blockers, f"PUBLIC_ACCOUNT_FEE_MUST_NOT_BE_ASSUMED:{venue.venue}")
        if (
            venue.schedule_status != "CURRENT_PUBLIC"
            or venue.schedule_source != "OFFICIAL_CURRENT_SCHEDULE"
        ):
            _add_blocker(blockers, f"PUBLIC_FUNDING_SCHEDULE_NOT_AUTHORITATIVE:{venue.venue}")
        if (
            type(venue.funding_interval_seconds) is not int
            or venue.funding_interval_seconds <= 0
            or type(venue.next_funding_at) is not int
            or venue.next_funding_at <= 0
        ):
            _add_blocker(blockers, f"PUBLIC_FUNDING_SCHEDULE_INVALID:{venue.venue}")
        if (
            venue.private_stream_status != "PENDING_PRIVATE_READ"
            or venue.private_stream_source != "PRIVATE_READ_PENDING"
        ):
            _add_blocker(blockers, f"PRIVATE_STREAM_MUST_REMAIN_PENDING:{venue.venue}")

    common_quantity: Decimal | None = None
    gross_trade_notional: Decimal | None = None
    if set(venue_map) != set(VENUES) or len(evidence.venues) != len(VENUES):
        _add_blocker(blockers, "PUBLIC_MARKET_READINESS_REQUIRED_FOR_BOTH_VENUES")
    else:
        risex = venue_map["RISEx"]
        nado = venue_map["Nado"]
        if (
            risex.funding_interval_seconds != nado.funding_interval_seconds
            or risex.next_funding_at != nado.next_funding_at
        ):
            _add_blocker(blockers, "PUBLIC_FUNDING_SCHEDULE_NOT_COMMON")
        try:
            common_quantity = _venue_common_quantity(risex, nado)
            if common_quantity * min(nado.best_bid_usd, nado.best_ask_usd) < NADO_PUBLIC_MINIMUM_NOTIONAL_USD:
                _add_blocker(blockers, "NADO_COMMON_QUANTITY_BELOW_MINIMUM_NOTIONAL")
            for venue in (risex, nado):
                if (
                    common_quantity > venue.available_buy_quantity
                    or common_quantity > venue.available_sell_quantity
                ):
                    _add_blocker(blockers, f"COMMON_QUANTITY_NOT_EXECUTABLE:{venue.venue}")
            gross_trade_notional = common_quantity * (
                risex.reference_price_usd + nado.reference_price_usd
            )
        except (ArithmeticError, ReadinessViolation):
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
    """Evaluate public evidence and the canonical RISEx protected-path phase."""

    base = _public_evaluation(evidence)
    if base.blockers:
        return base
    protected = inspect_canonical_risex_identity()
    if not protected.present and protected.reason in {
        "PROTECTED_FILE_MISSING",
        "PROTECTED_DIRECTORY_MISSING",
    }:
        return _with_readiness_phase(
            base,
            status=READY_FOR_PROTECTED_PROVISIONING,
            reason="PUBLIC_OFFLINE_REQUIREMENTS_PROVEN_CANONICAL_RISEX_PROTECTED_PATH_PENDING",
        )
    if protected.protected:
        return _with_readiness_phase(
            base,
            status=READY_FOR_PRIVATE_READ_GATES,
            reason="CANONICAL_RISEX_PROTECTED_NADO_UNSIGNED_READ_GATES_PENDING",
        )
    blocker = f"PROTECTED_SECRET_FILE_NOT_SAFE:RISEx:{protected.reason}"
    return _with_readiness_phase(
        base,
        status=BLOCKED,
        reason=blocker,
        blockers=(blocker,),
    )


def _invalid_private_result(
    base: ReadinessResult,
    blockers: Sequence[str],
    *,
    planned_total: Decimal | None = None,
) -> PrivateReadResult:
    unique = tuple(dict.fromkeys(blockers))
    reason = unique[0] if unique else "PRIVATE_READ_BLOCKED"
    return PrivateReadResult(
        status=BLOCKED,
        reason=reason,
        blockers=unique or (reason,),
        route_id=base.route_id,
        direction=base.direction,
        planned_deposit_total_usd=planned_total,
    )


def _private_identity_map(
    private_read: PrivateReadEvidence, blockers: list[str]
) -> dict[str, str]:
    if len(private_read.identities) != len(VENUES):
        _add_blocker(blockers, "PRIVATE_IDENTITIES_MUST_INCLUDE_ONE_EXACT_ACCOUNT_PER_VENUE")
    result: dict[str, str] = {}
    for identity in private_read.identities:
        if identity.venue not in VENUES:
            _add_blocker(blockers, "PRIVATE_IDENTITIES_OUT_OF_SCOPE")
            continue
        if identity.venue in result:
            _add_blocker(blockers, f"PRIVATE_IDENTITY_DUPLICATE_VENUE:{identity.venue}")
        if not _valid_token(identity.account_id, f"{identity.venue}.account_id"):
            _add_blocker(blockers, f"PRIVATE_ACCOUNT_IDENTITY_NOT_EXACT:{identity.venue}")
        if identity.environment != "MAINNET":
            _add_blocker(blockers, f"PRIVATE_ACCOUNT_ENVIRONMENT_NOT_MAINNET:{identity.venue}")
        if identity.exact is not True or identity.authoritative is not True:
            _add_blocker(blockers, f"PRIVATE_ACCOUNT_IDENTITY_NOT_AUTHORITATIVE:{identity.venue}")
        if identity.venue == "Nado":
            try:
                wallet = _hex_identifier(
                    identity.wallet_address,
                    "Nado.wallet_address",
                    20,
                )
                subaccount = _hex_identifier(
                    identity.subaccount,
                    "Nado.subaccount",
                    32,
                )
            except ReadinessViolation:
                _add_blocker(blockers, "NADO_PUBLIC_WALLET_SUBACCOUNT_NOT_EXACT")
            else:
                if identity.identity_source != NADO_PUBLIC_IDENTITY_SOURCE:
                    _add_blocker(blockers, "NADO_PUBLIC_IDENTITY_SOURCE_NOT_EXACT")
                if identity.account_id.lower() != subaccount.lower():
                    _add_blocker(blockers, "NADO_ACCOUNT_ID_NOT_SUBACCOUNT")
                if subaccount[2:42].lower() != wallet[2:].lower():
                    _add_blocker(blockers, "NADO_SUBACCOUNT_WALLET_MISMATCH")
        elif any(
            value is not None
            for value in (
                identity.wallet_address,
                identity.subaccount,
                identity.identity_source,
            )
        ):
            _add_blocker(blockers, "RISEX_IDENTITY_MUST_REUSE_CANONICAL_PATH")
        result[identity.venue] = identity.account_id
    if set(result) != set(VENUES):
        _add_blocker(blockers, "PRIVATE_IDENTITIES_MUST_INCLUDE_ONE_EXACT_ACCOUNT_PER_VENUE")
    if len(result) == len(VENUES) and len(set(result.values())) != len(VENUES):
        _add_blocker(blockers, "PRIVATE_ACCOUNT_IDENTITIES_MUST_BE_DISTINCT")
    return result


def _private_deposit_map(
    private_read: PrivateReadEvidence,
    account_ids: Mapping[str, str],
    blockers: list[str],
) -> tuple[dict[str, Decimal], Decimal | None]:
    if len(private_read.planned_deposits) != len(VENUES):
        _add_blocker(blockers, "PLANNED_DEPOSITS_MUST_INCLUDE_ONE_AMOUNT_PER_VENUE")
    deposits: dict[str, Decimal] = {}
    seen: set[str] = set()
    for deposit in private_read.planned_deposits:
        if deposit.venue not in VENUES:
            _add_blocker(blockers, "PLANNED_DEPOSIT_VENUE_OUT_OF_SCOPE")
            continue
        if deposit.venue in seen:
            _add_blocker(blockers, f"PLANNED_DEPOSIT_DUPLICATE_VENUE:{deposit.venue}")
        seen.add(deposit.venue)
        if deposit.account_id != account_ids.get(deposit.venue):
            _add_blocker(blockers, f"PLANNED_DEPOSIT_ACCOUNT_MISMATCH:{deposit.venue}")
        try:
            amount = _decimal(deposit.amount_usd, f"planned_deposit.{deposit.venue}")
        except ReadinessViolation:
            _add_blocker(blockers, f"PLANNED_DEPOSIT_INVALID:{deposit.venue}")
            continue
        if amount <= 0:
            _add_blocker(blockers, f"PLANNED_DEPOSIT_MUST_BE_POSITIVE:{deposit.venue}")
        else:
            deposits[deposit.venue] = amount
    if set(seen) != set(VENUES):
        _add_blocker(blockers, "PLANNED_DEPOSITS_MUST_INCLUDE_ONE_AMOUNT_PER_VENUE")
    if set(deposits) != set(VENUES):
        return deposits, None
    total = sum(deposits.values(), Decimal("0"))
    if total <= 0:
        _add_blocker(blockers, "PLANNED_DEPOSIT_TOTAL_MUST_BE_POSITIVE")
        return deposits, None
    return deposits, total


def assess_private_read(
    evidence: ReadinessEvidence,
    private_read: PrivateReadEvidence,
) -> PrivateReadResult:
    """Evaluate future account-scoped Nado/RISEx read evidence."""

    public = assess_readiness(evidence)
    if public.status != READY_FOR_PRIVATE_READ_GATES:
        return _invalid_private_result(
            public,
            ("PRIVATE_READ_REQUIRES_READY_PRIVATE_READ_GATES", *public.blockers),
        )
    if not isinstance(private_read, PrivateReadEvidence):
        return _invalid_private_result(public, ("PRIVATE_READ_EVIDENCE_SCHEMA_INVALID",))
    if not all(isinstance(item, AccountIdentity) for item in private_read.identities):
        return _invalid_private_result(public, ("PRIVATE_READ_EVIDENCE_SCHEMA_INVALID:identities",))
    if not all(isinstance(item, PrivateVenueRead) for item in private_read.venues):
        return _invalid_private_result(public, ("PRIVATE_READ_EVIDENCE_SCHEMA_INVALID:venues",))
    if not all(isinstance(item, PlannedDeposit) for item in private_read.planned_deposits):
        return _invalid_private_result(public, ("PRIVATE_READ_EVIDENCE_SCHEMA_INVALID:planned_deposits",))

    blockers: list[str] = []
    account_ids = _private_identity_map(private_read, blockers)
    private_venues: dict[str, PrivateVenueRead] = {}
    for venue in private_read.venues:
        if venue.venue not in VENUES:
            _add_blocker(blockers, "PRIVATE_VENUE_READ_OUT_OF_SCOPE")
            continue
        if venue.venue in private_venues:
            _add_blocker(blockers, f"PRIVATE_VENUE_READ_DUPLICATE:{venue.venue}")
        private_venues[venue.venue] = venue
        if venue.account_id != account_ids.get(venue.venue):
            _add_blocker(blockers, f"PRIVATE_VENUE_ACCOUNT_BINDING_MISMATCH:{venue.venue}")
        if venue.venue == "Nado":
            if venue.query_authentication != NADO_UNSIGNED_QUERY_AUTHENTICATION:
                _add_blocker(blockers, "NADO_QUERY_MUST_BE_UNSIGNED")
            if venue.query_status != NADO_UNSIGNED_QUERY_STATUS or venue.query_source != NADO_UNSIGNED_QUERY_SOURCE:
                _add_blocker(blockers, "NADO_UNSIGNED_QUERY_NOT_AUTHORITATIVE")
            if not NADO_REQUIRED_QUERY_KINDS.issubset(set(venue.query_kinds)):
                _add_blocker(blockers, "NADO_UNSIGNED_QUERY_SET_INCOMPLETE")
            if (
                venue.private_stream_status != NADO_PRIVATE_STREAM_PENDING_STATUS
                or venue.private_stream_source != NADO_PRIVATE_STREAM_PENDING_SOURCE
            ):
                _add_blocker(blockers, "NADO_PRIVATE_STREAM_MUST_REMAIN_PENDING")
        else:
            if venue.query_authentication != "CANONICAL_PROTECTED_READ":
                _add_blocker(blockers, "RISEX_QUERY_MUST_USE_CANONICAL_PROTECTED_PATH")
            if venue.query_status != "AUTHORITATIVE_CURRENT" or venue.query_source != "ACCOUNT_SCOPED_PRIVATE_READ":
                _add_blocker(blockers, "ACCOUNT_STATE_NOT_AUTHORITATIVE:RISEx")
            if (
                venue.private_stream_status != "READY_ACCOUNT_SCOPED"
                or venue.private_stream_source != "ACCOUNT_PRIVATE_STREAM"
            ):
                _add_blocker(blockers, "PRIVATE_STREAM_NOT_READY:RISEx")
        if venue.fee_status != "ACCOUNT_SCOPED_AUTHORITATIVE" or venue.fee_source != "ACCOUNT_SCOPED_READ":
            _add_blocker(blockers, f"ACCOUNT_FEE_NOT_AUTHORITATIVE:{venue.venue}")
        if not _valid_decimal(venue.maker_fee_rate, f"{venue.venue}.maker_fee_rate"):
            _add_blocker(blockers, f"ACCOUNT_FEE_VALUE_INVALID:{venue.venue}:maker")
        if not _valid_decimal(venue.taker_fee_rate, f"{venue.venue}.taker_fee_rate"):
            _add_blocker(blockers, f"ACCOUNT_FEE_VALUE_INVALID:{venue.venue}:taker")
        if venue.collateral_status != "AUTHORITATIVE_POSITIVE" or not _valid_decimal(
            venue.collateral_usd, f"{venue.venue}.collateral_usd", positive=True
        ):
            _add_blocker(blockers, f"POSITIVE_COLLATERAL_NOT_PROVEN:{venue.venue}")
        for field_name, field_value in (
            ("zero_relevant_orders", venue.zero_relevant_orders),
            ("zero_trigger_orders", venue.zero_trigger_orders),
            ("exact_flat", venue.exact_flat),
            ("unrelated_state_clear", venue.unrelated_state_clear),
        ):
            if field_value is not True:
                _add_blocker(blockers, f"PRIVATE_STATE_NOT_CLEAR:{venue.venue}:{field_name}")
    if set(private_venues) != set(VENUES) or len(private_read.venues) != len(VENUES):
        _add_blocker(blockers, "PRIVATE_READ_REQUIRED_FOR_BOTH_VENUES")
    deposits, planned_total = _private_deposit_map(private_read, account_ids, blockers)
    if blockers:
        return _invalid_private_result(public, blockers, planned_total=planned_total)
    return PrivateReadResult(
        status=PRIVATE_READ_GATES_COMPLETE,
        reason="ACCOUNT_SCOPED_READ_REQUIREMENTS_PROVEN_UNSIGNED_NADO_QUERY_PENDING_STREAM_AUTH",
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
    blockers: Sequence[str] = (),
) -> DispatchApprovalResult:
    unique = tuple(dict.fromkeys(blockers))
    return DispatchApprovalResult(
        status=status,
        reason=reason,
        blockers=unique,
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
    if len(operational.dispatches) != len(DISPATCH_SEQUENCE):
        _add_blocker(blockers, "EXACTLY_FOUR_DISPATCHES_REQUIRED")
        return
    write_ids: list[str] = []
    for expected_sequence, (dispatch, expected) in enumerate(
        zip(operational.dispatches, DISPATCH_SEQUENCE), start=1
    ):
        expected_venue, expected_purpose = expected
        if (
            dispatch.sequence != expected_sequence
            or dispatch.venue != expected_venue
            or dispatch.purpose != expected_purpose
        ):
            _add_blocker(blockers, "DISPATCH_SEQUENCE_NOT_EXACT")
        if dispatch.account_id != account_ids.get(expected_venue):
            _add_blocker(blockers, f"DISPATCH_ACCOUNT_BINDING_MISMATCH:{expected_venue}:{expected_purpose}")
        if dispatch.runtime_id != operational.runtime_id:
            _add_blocker(blockers, "DISPATCH_RUNTIME_IDENTITY_MISMATCH")
        if _valid_token(dispatch.write_identity, "dispatch.write_identity"):
            write_ids.append(dispatch.write_identity)
            if dispatch.write_identity == operational.runtime_id:
                _add_blocker(blockers, "DISPATCH_IDENTITY_MUST_BE_SEPARATE_FROM_RUNTIME")
        else:
            _add_blocker(blockers, "DISPATCH_WRITE_IDENTITY_INVALID")
        if dispatch.durable_before_dispatch is not True:
            _add_blocker(blockers, f"DISPATCH_WRITE_IDENTITY_NOT_DURABLE:{expected_venue}:{expected_purpose}")
    if len(write_ids) != len(DISPATCH_SEQUENCE) or len(set(write_ids)) != len(write_ids):
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
    """Validate future approval evidence; this function never dispatches."""

    public = assess_readiness(evidence)
    private = assess_private_read(evidence, private_read)
    if not private.complete:
        return _approval_result(
            public,
            status=BLOCKED,
            reason="PRIVATE_READ_REQUIREMENTS_REQUIRED_FOR_APPROVAL",
            blockers=("PRIVATE_READ_REQUIREMENTS_REQUIRED_FOR_APPROVAL", *private.blockers),
        )
    if not isinstance(approval_evidence, DispatchApprovalEvidence):
        return _approval_result(
            public,
            status=BLOCKED,
            reason="DISPATCH_APPROVAL_EVIDENCE_SCHEMA_INVALID",
            blockers=("DISPATCH_APPROVAL_EVIDENCE_SCHEMA_INVALID",),
        )
    approval = approval_evidence.approval
    operational = approval_evidence.operational
    if not isinstance(approval, FutureDispatchApproval) or not isinstance(operational, OperationalEvidence):
        return _approval_result(
            public,
            status=BLOCKED,
            reason="DISPATCH_APPROVAL_SCHEMA_INVALID",
            blockers=("DISPATCH_APPROVAL_SCHEMA_INVALID",),
        )

    blockers: list[str] = []
    identities = {item.venue: item.account_id for item in private_read.identities}
    deposits = {item.venue: item.amount_usd for item in private_read.planned_deposits}
    if not _valid_token(approval.approval_id, "approval_id"):
        _add_blocker(blockers, "DISPATCH_APPROVAL_ID_INVALID")
    if approval.route_id != public.route_id or approval.direction != public.direction:
        _add_blocker(blockers, "DISPATCH_APPROVAL_ROUTE_MISMATCH")
    if (
        approval.risex_venue != "RISEx"
        or approval.nado_venue != "Nado"
        or approval.risex_account_id != identities.get("RISEx")
        or approval.nado_account_id != identities.get("Nado")
    ):
        _add_blocker(blockers, "DISPATCH_APPROVAL_ACCOUNT_OR_VENUE_BINDING_MISMATCH")
    if approval.risex_planned_deposit_usd != deposits.get("RISEx") or approval.nado_planned_deposit_usd != deposits.get("Nado"):
        _add_blocker(blockers, "DISPATCH_APPROVAL_DEPOSIT_BINDING_MISMATCH")
    if not _valid_decimal(approval.deposit_cap_usd, "approval.deposit_cap_usd", positive=True):
        _add_blocker(blockers, "DEPOSIT_CAP_MUST_BE_POSITIVE")
    if not _valid_decimal(approval.maximum_loss_usd, "approval.maximum_loss_usd", positive=True):
        _add_blocker(blockers, "MAXIMUM_LOSS_CAP_MUST_BE_POSITIVE")
    planned_total = private.planned_deposit_total_usd
    if planned_total is None or planned_total > approval.deposit_cap_usd:
        _add_blocker(blockers, "PLANNED_DEPOSITS_EXCEED_DEPOSIT_CAP")
    if public.gross_trade_notional_usd is None or public.gross_trade_notional_usd > approval.deposit_cap_usd:
        _add_blocker(blockers, "COMMON_GROSS_NOTIONAL_EXCEEDS_DEPOSIT_CAP")
    if public.loss_bound_usd is None or public.loss_bound_usd > approval.maximum_loss_usd:
        _add_blocker(blockers, "ROUTE_LOSS_EXCEEDS_MAXIMUM_LOSS_CAP")
    if approval.maximum_loss_usd > approval.deposit_cap_usd:
        _add_blocker(blockers, "MAXIMUM_LOSS_EXCEEDS_DEPOSIT_CAP")
    if approval.approval_mode != "ONE_EXPLICIT_MANUAL_APPROVAL":
        _add_blocker(blockers, "EXPLICIT_MANUAL_APPROVAL_REQUIRED")
    if approval.scope != DISPATCH_APPROVAL_SCOPE:
        _add_blocker(blockers, "DISPATCH_APPROVAL_SCOPE_NOT_EXACT")
    if approval.manual_lifecycle_dispatch_authorized is not True:
        _add_blocker(blockers, "MANUAL_LIFECYCLE_DISPATCH_NOT_AUTHORIZED")
    if approval.authorization_count != 1:
        _add_blocker(blockers, "EXACTLY_ONE_AUTHORIZATION_REQUIRED")
    _validate_operational(operational, identities, blockers)
    if blockers:
        return _approval_result(public, status=BLOCKED, reason=blockers[0], blockers=blockers)
    return _approval_result(
        public,
        status=FUTURE_DISPATCH_APPROVAL_COMPLETE,
        reason="FUTURE_DISPATCH_APPROVAL_REQUIREMENTS_PROVEN",
    )


def _invalid_lifecycle_result(
    public: ReadinessResult, blockers: Sequence[str]
) -> LifecycleResult:
    unique = tuple(dict.fromkeys(blockers))
    reason = unique[0] if unique else "LIFECYCLE_BLOCKED"
    return LifecycleResult(
        status=BLOCKED,
        reason=reason,
        blockers=unique or (reason,),
        route_id=public.route_id,
        direction=public.direction,
    )


def _validate_execution(
    public: ReadinessResult,
    private_read: PrivateReadEvidence,
    execution: ExecutionEvidence,
    blockers: list[str],
) -> None:
    legs = (execution.risex, execution.nado)
    identities = {item.venue: item.account_id for item in private_read.identities}
    if execution.risex.venue != "RISEx" or execution.nado.venue != "Nado":
        _add_blocker(blockers, "EXECUTION_VENUE_ORDER_NOT_EXACT")
    expected_sides = {
        "LONG_RISEX_SHORT_NADO": ("BUY", "SELL"),
        "SHORT_RISEX_LONG_NADO": ("SELL", "BUY"),
    }
    if public.direction in expected_sides:
        expected_risex_side, expected_nado_side = expected_sides[public.direction]
        if execution.risex.entry_side != expected_risex_side or execution.nado.entry_side != expected_nado_side:
            _add_blocker(blockers, "EXECUTION_DIRECTION_NOT_OPPOSITE")
    ids: list[str] = []
    for leg in legs:
        if leg.account_id != identities.get(leg.venue):
            _add_blocker(blockers, f"EXECUTION_ACCOUNT_BINDING_MISMATCH:{leg.venue}")
        if leg.route_id != public.route_id or leg.canonical_asset != "BTC":
            _add_blocker(blockers, f"EXECUTION_ROUTE_BINDING_MISMATCH:{leg.venue}")
        if public.common_quantity is None or leg.canonical_quantity != public.common_quantity:
            _add_blocker(blockers, f"EXECUTION_QUANTITY_NOT_EXACT_COMMON_QUANTITY:{leg.venue}")
        for field_name, value in (
            ("order_id", leg.order_id),
            ("fill_id", leg.fill_id),
            ("position_id", leg.position_id),
            ("close_order_id", leg.close_order_id),
            ("close_fill_id", leg.close_fill_id),
        ):
            if _valid_token(value, f"{leg.venue}.{field_name}"):
                ids.append(value)
            else:
                _add_blocker(blockers, f"EXECUTION_IDENTITY_INVALID:{leg.venue}:{field_name}")
        if leg.entry_observed_at <= 0 or leg.close_observed_at <= leg.entry_observed_at:
            _add_blocker(blockers, f"EXECUTION_ENTRY_CLOSE_ORDER_INVALID:{leg.venue}")
        for field_name, value in (
            ("order_reconciled", leg.order_reconciled),
            ("fill_reconciled", leg.fill_reconciled),
            ("position_reconciled", leg.position_reconciled),
            ("authoritative", leg.authoritative),
            ("order_authoritative", leg.order_authoritative),
            ("fill_authoritative", leg.fill_authoritative),
            ("position_authoritative", leg.position_authoritative),
            ("reduce_only", leg.reduce_only),
            ("close_reconciled", leg.close_reconciled),
            ("close_authoritative", leg.close_authoritative),
        ):
            if value is not True:
                _add_blocker(blockers, f"EXECUTION_EVIDENCE_NOT_AUTHORITATIVE:{leg.venue}:{field_name}")
    if len(ids) != len(set(ids)):
        _add_blocker(blockers, "EXECUTION_IDENTITIES_NOT_DISTINCT")


def _validate_funding(
    evidence: ReadinessEvidence,
    lifecycle: LifecycleEvidence,
    blockers: list[str],
) -> None:
    if len(lifecycle.funding) != len(VENUES) * len(FUNDING_PHASES):
        _add_blocker(blockers, "EXACTLY_SIX_FUNDING_OBSERVATIONS_REQUIRED")
    schedule = {item.venue: item.next_funding_at for item in evidence.venues}
    expected_keys = {(venue, phase) for venue in VENUES for phase in FUNDING_PHASES}
    seen: set[tuple[str, str]] = set()
    by_venue: dict[str, list[FundingObservation]] = {venue: [] for venue in VENUES}
    for observation in lifecycle.funding:
        key = (observation.venue, observation.phase)
        if key in seen:
            _add_blocker(blockers, f"FUNDING_OBSERVATION_DUPLICATE:{observation.venue}:{observation.phase}")
        seen.add(key)
        if observation.venue not in VENUES or observation.phase not in FUNDING_PHASES:
            _add_blocker(blockers, "FUNDING_OBSERVATION_OUT_OF_SCOPE")
            continue
        by_venue[observation.venue].append(observation)
        if observation.canonical_asset != "BTC":
            _add_blocker(blockers, f"FUNDING_ASSET_MISMATCH:{observation.venue}")
        if not _valid_token(observation.settlement_id, "funding.settlement_id"):
            _add_blocker(blockers, f"FUNDING_SETTLEMENT_ID_INVALID:{observation.venue}")
        if (
            type(observation.settlement_at) is not int
            or observation.settlement_at <= 0
            or type(observation.observed_at) is not int
            or observation.observed_at <= 0
        ):
            _add_blocker(blockers, f"FUNDING_TIMESTAMP_INVALID:{observation.venue}:{observation.phase}")
        if observation.settlement_at != schedule.get(observation.venue):
            _add_blocker(blockers, f"FUNDING_SETTLEMENT_NOT_PUBLIC_SCHEDULE:{observation.venue}")
        if observation.status not in FUNDING_STATUSES:
            _add_blocker(blockers, f"FUNDING_STATUS_INVALID:{observation.venue}:{observation.phase}")
        if observation.missing is True or observation.contradictory is True:
            _add_blocker(blockers, f"FUNDING_MISSING_OR_CONTRADICTORY:{observation.venue}:{observation.phase}")
        if observation.authoritative is not True or observation.eligible_known is not True or observation.exposure_confirmed is not True:
            _add_blocker(blockers, f"FUNDING_NOT_AUTHORITATIVE:{observation.venue}:{observation.phase}")
        if observation.cash_usd is None or not _valid_decimal(observation.cash_usd, "funding.cash_usd"):
            _add_blocker(blockers, f"FUNDING_CASH_NOT_AUTHORITATIVE:{observation.venue}:{observation.phase}")
        expected_cash_source = (
            "ACCOUNT_SCOPED_APPLIED_CASH"
            if observation.status == "APPLIED_RATE"
            else "ACCOUNT_SCOPED_NON_ACCRUAL"
        )
        if observation.cash_source != expected_cash_source:
            _add_blocker(blockers, f"FUNDING_CASH_SOURCE_NOT_ACCOUNT_SCOPED:{observation.venue}:{observation.phase}")
        if observation.venue == "Nado" and observation.cash_source == "PUBLIC_AGGREGATE":
            _add_blocker(blockers, "NADO_PUBLIC_AGGREGATE_NOT_ACCOUNT_CASH")
        if observation.public_aggregate_payment_usd is not None and not _valid_decimal(
            observation.public_aggregate_payment_usd, "funding.public_aggregate_payment_usd"
        ):
            _add_blocker(blockers, f"FUNDING_PUBLIC_AGGREGATE_INVALID:{observation.venue}:{observation.phase}")
    if seen != expected_keys:
        _add_blocker(blockers, "FUNDING_BEFORE_AT_AFTER_REQUIRED_PER_VENUE")
    for venue in VENUES:
        rows = {item.phase: item for item in by_venue[venue]}
        if set(rows) != set(FUNDING_PHASES):
            continue
        settlement_ids = {item.settlement_id for item in rows.values()}
        settlement_times = {item.settlement_at for item in rows.values()}
        if len(settlement_ids) != 1 or len(settlement_times) != 1:
            _add_blocker(blockers, f"FUNDING_SETTLEMENT_NOT_COMMON:{venue}")
            continue
        before, at, after = (rows[phase] for phase in FUNDING_PHASES)
        if not before.observed_at < at.observed_at < after.observed_at:
            _add_blocker(blockers, f"FUNDING_PHASE_ORDER_INVALID:{venue}")
        if not (before.observed_at < before.settlement_at <= at.observed_at < after.observed_at):
            _add_blocker(blockers, f"FUNDING_PHASE_BOUNDARY_INVALID:{venue}")


def _validate_terminal_rounds(
    lifecycle: LifecycleEvidence, blockers: list[str]
) -> None:
    rounds = lifecycle.terminal_rounds
    if len(rounds) != 2:
        _add_blocker(blockers, "EXACTLY_TWO_TERMINAL_ROUNDS_REQUIRED")
        return
    if tuple(item.round_number for item in rounds) != (1, 2):
        _add_blocker(blockers, "TERMINAL_ROUND_SEQUENCE_NOT_EXACT")
    if rounds[0].signature != rounds[1].signature or not _valid_token(rounds[0].signature, "terminal.signature"):
        _add_blocker(blockers, "TERMINAL_ROUNDS_DO_NOT_AGREE")
    if rounds[1].observed_at <= rounds[0].observed_at:
        _add_blocker(blockers, "TERMINAL_ROUNDS_NOT_INCREASING")
    for item in rounds:
        if item.phase != "TERMINAL_ZERO_ORDER_EXACT_FLAT":
            _add_blocker(blockers, "TERMINAL_ROUND_PHASE_NOT_EXACT")
        if item.authoritative is not True:
            _add_blocker(blockers, "TERMINAL_ROUND_NOT_AUTHORITATIVE")
        if any(
            value != 0
            for value in (
                item.relevant_open_orders,
                item.trigger_orders,
                item.isolated_orders,
                item.unrelated_open_orders,
                item.unrelated_positions,
            )
        ):
            _add_blocker(blockers, "TERMINAL_ROUND_HAS_ORDERS_OR_UNRELATED_STATE")
        if item.risex_net_position_quantity != 0 or item.nado_net_position_quantity != 0:
            _add_blocker(blockers, "TERMINAL_ROUND_NOT_EXACTLY_FLAT")


def assess_post_lifecycle(
    evidence: ReadinessEvidence,
    private_read: PrivateReadEvidence,
    approval_evidence: DispatchApprovalEvidence,
    lifecycle: LifecycleEvidence,
) -> LifecycleResult:
    """Validate future lifecycle evidence after all earlier gates."""

    public = assess_readiness(evidence)
    approval = assess_dispatch_approval(evidence, private_read, approval_evidence)
    if not approval.complete:
        return _invalid_lifecycle_result(
            public,
            ("FUTURE_DISPATCH_APPROVAL_REQUIRED", *approval.blockers),
        )
    if not isinstance(lifecycle, LifecycleEvidence):
        return _invalid_lifecycle_result(public, ("LIFECYCLE_EVIDENCE_SCHEMA_INVALID",))
    if not isinstance(lifecycle.execution, ExecutionEvidence):
        return _invalid_lifecycle_result(public, ("LIFECYCLE_EXECUTION_SCHEMA_INVALID",))
    if not all(isinstance(item, FundingObservation) for item in lifecycle.funding):
        return _invalid_lifecycle_result(public, ("LIFECYCLE_FUNDING_SCHEMA_INVALID",))
    if not all(isinstance(item, TerminalRound) for item in lifecycle.terminal_rounds):
        return _invalid_lifecycle_result(public, ("LIFECYCLE_TERMINAL_SCHEMA_INVALID",))

    blockers: list[str] = []
    _validate_execution(public, private_read, lifecycle.execution, blockers)
    _validate_funding(evidence, lifecycle, blockers)
    _validate_terminal_rounds(lifecycle, blockers)
    if blockers:
        return _invalid_lifecycle_result(public, blockers)
    return LifecycleResult(
        status=POST_LIFECYCLE_EVIDENCE_COMPLETE,
        reason="POST_LIFECYCLE_EVIDENCE_REQUIREMENTS_PROVEN",
        blockers=(),
        route_id=public.route_id,
        direction=public.direction,
    )


__all__ = [
    "AccountIdentity",
    "BLOCKED",
    "CANONICAL_RISEX_IDENTITY_PATH",
    "DISPATCH_APPROVAL_SCOPE",
    "DISPATCH_SEQUENCE",
    "DispatchApprovalEvidence",
    "DispatchApprovalResult",
    "DispatchIdentity",
    "ExecutionEvidence",
    "FUNDING_PHASES",
    "FutureDispatchApproval",
    "FundingObservation",
    "LifecycleEvidence",
    "LifecycleResult",
    "NADO_BTC_PERP_PRODUCT_ID",
    "NADO_MAINNET_CHAIN_ID",
    "NADO_MAINNET_ENDPOINT_CONTRACT",
    "NADO_MAINNET_PERP_ENGINE_CONTRACT",
    "NADO_PUBLIC_MINIMUM_FEE_NOTIONAL_USD",
    "NADO_PUBLIC_MINIMUM_NOTIONAL_USD",
    "NADO_PUBLIC_IDENTITY_SOURCE",
    "NADO_SDK_PACKAGE",
    "NADO_SDK_VERSION",
    "NADO_PRIVATE_STREAM_PENDING_SOURCE",
    "NADO_PRIVATE_STREAM_PENDING_STATUS",
    "NADO_REQUIRED_QUERY_KINDS",
    "NADO_UNSIGNED_QUERY_AUTHENTICATION",
    "NADO_UNSIGNED_QUERY_SOURCE",
    "NADO_UNSIGNED_QUERY_STATUS",
    "NO_MAINNET_WRITE_AUTHORITY",
    "OPPOSITE_DIRECTIONS",
    "POST_LIFECYCLE_EVIDENCE_COMPLETE",
    "PRIVATE_READ_GATES_COMPLETE",
    "PlannedDeposit",
    "PrivateReadEvidence",
    "PrivateReadResult",
    "PrivateVenueRead",
    "ProtectedFileState",
    "READY_FOR_PRIVATE_READ_GATES",
    "READY_FOR_PROTECTED_PROVISIONING",
    "ReadinessEvidence",
    "ReadinessResult",
    "ReadinessViolation",
    "RouteEvidence",
    "TerminalRound",
    "VenueReadiness",
    "assess_dispatch_approval",
    "assess_post_lifecycle",
    "assess_private_read",
    "assess_readiness",
    "inspect_canonical_risex_identity",
]
