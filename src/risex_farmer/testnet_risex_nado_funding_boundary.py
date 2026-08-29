"""RISEx-side funding-boundary coordination for the fixed Nado ETH route.

This module is deliberately separate from the historical RISEx Level-C
entrypoint.  It owns a fresh pair of protected funding-boundary journals,
coordinates only the fixed ETH/USDC route, and exposes a synchronous,
write-free terminal-evidence callback for the Nado contract seam.  The
callback uses a dedicated RISEx coordination loop; it never creates an event
loop around an existing venue/session and never returns Nado evidence.
"""

from __future__ import annotations

import asyncio
from concurrent.futures import Future, TimeoutError as FutureTimeoutError
from dataclasses import dataclass, fields, is_dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum
import hashlib
import inspect
import json
import math
import os
from pathlib import Path
import pwd
import threading
import time
from typing import Any, Callable, Mapping, Protocol

from . import testnet_risex_two_account_coordinator as _coordinator
from .nado_testnet_lifecycle import (
    COMPLETE as NADO_COMPLETE,
    FundingBoundaryBinding,
    FundingLegBinding,
    FundingRouteBinding,
    JournalIdentity,
    LONG,
    NADO_VENUE,
    RISEX_VENUE,
    RisexTerminalEvidence,
    SHORT,
    TerminalEvidence,
    canonical_payload,
    terminal_evidence_digest,
)


CoordinatorSafetyError = _coordinator.CoordinatorSafetyError
AccountRole = _coordinator.AccountRole
CoordinatorReport = _coordinator.CoordinatorReport
CoordinatorResult = _coordinator.CoordinatorResult
MARKET_SYMBOL = _coordinator.MARKET_SYMBOL
MARKET_ID = _coordinator.MARKET_ID
MARKET_TICK = _coordinator.MARKET_TICK
MARKET_STEP = _coordinator.MARKET_STEP
MARKET_MINIMUM = _coordinator.MARKET_MINIMUM
MAX_AGE_SECONDS = _coordinator.MAX_AGE_SECONDS
PairJournal = _coordinator.PairJournal
Phase = _coordinator.Phase
RoleIdentity = _coordinator.RoleIdentity
TwoAccountCoordinator = _coordinator.TwoAccountCoordinator
VenueObservation = _coordinator.VenueObservation
_maybe_await = _coordinator._maybe_await
_bound = _coordinator._bound
_canonical_digest = _coordinator._canonical_digest
_validate_market = _coordinator._validate_market


# These names are intentionally different from the historical Level-C
# journals.  The new production builder rejects an existing path before any
# PairJournal can open it, so a completed historical database is never reused.
FUNDING_BOUNDARY_PRIMARY_JOURNAL = (
    ".risex-funding-farmer-risex-nado-boundary-primary-v5.sqlite3"
)
FUNDING_BOUNDARY_COUNTERPARTY_JOURNAL = (
    ".risex-funding-farmer-risex-nado-boundary-counterparty-v5.sqlite3"
)
FUNDING_BOUNDARY_PRIMARY_STORE_IDENTITY = "risex-nado-boundary-primary-v5"
FUNDING_BOUNDARY_COUNTERPARTY_STORE_IDENTITY = "risex-nado-boundary-counterparty-v5"

TARGET_CANONICAL_ASSET = "ETH"
TARGET_RISEX_MARKET = MARKET_SYMBOL
TARGET_NADO_MARKET = "ETH-PERP_USDT0"
TARGET_NADO_PRODUCT_ID = 4
TARGET_QUANTITY = Decimal("0.1")
TARGET_MULTIPLIER = Decimal("1")

FUNDING_UNRESOLVED = "UNRESOLVED"
FUNDING_BLOCKER_MISSING_CONTRACT = "RISEX_APPLIED_FUNDING_CONTRACT_MISSING"
FUNDING_BLOCKER_BOUNDARY_GATE_MISSING = "RISEX_BOUNDARY_GATE_MISSING"
FUNDING_BLOCKER_BOUNDARY_INTERRUPTED = "RISEX_BOUNDARY_GATE_INTERRUPTED"
FUNDING_BLOCKER_EVIDENCE_MISSING = "RISEX_BOUNDARY_EVIDENCE_MISSING"
FUNDING_BLOCKER_EVIDENCE_CONTRADICTORY = "RISEX_BOUNDARY_EVIDENCE_CONTRADICTORY"
FUNDING_BLOCKER_EVIDENCE_STALE = "RISEX_BOUNDARY_EVIDENCE_STALE"
FUNDING_BLOCKER_CANCELLED = "RISEX_BOUNDARY_EVIDENCE_CANCELLED"
FUNDING_BLOCKER_ENTRY_AFTER_BOUNDARY = "RISEX_ENTRY_AFTER_FUNDING_BOUNDARY"
FUNDING_BLOCKER_GATE_CALLBACK_CANCELLED = "RISEX_BOUNDARY_GATE_CALLBACK_CANCELLED"
FUNDING_BLOCKER_AUTHORITATIVE_INJECTION = (
    "RISEX_AUTHORITATIVE_FUNDING_INJECTION_REJECTED"
)

_BOUNDARY_PAYLOAD_KEY = "funding_boundary:payload"
_BOUNDARY_DIGEST_KEY = "funding_boundary:digest"
_ENTRY_RECONCILIATION_AT_KEY = "funding_entry:reconciled_at_ms"
_GATE_INVOCATION_KEY = "funding_gate:invocation"
_GATE_RESULT_KEY = "funding_gate:result"
_FUNDING_STATUS_KEY = "funding:status"
_FUNDING_BLOCKER_KEY = "funding:blocker"
_PROVIDER_PREFIX = "risex_provider:"
_PROVIDER_ROUND_PREFIX = f"{_PROVIDER_PREFIX}round:"
_FRESHNESS_MS = MAX_AGE_SECONDS * 1_000
_KNOWN_FUNDING_BLOCKERS = frozenset({
    FUNDING_BLOCKER_MISSING_CONTRACT,
    FUNDING_BLOCKER_BOUNDARY_GATE_MISSING,
    FUNDING_BLOCKER_BOUNDARY_INTERRUPTED,
    FUNDING_BLOCKER_EVIDENCE_MISSING,
    FUNDING_BLOCKER_EVIDENCE_CONTRADICTORY,
    FUNDING_BLOCKER_EVIDENCE_STALE,
    FUNDING_BLOCKER_CANCELLED,
    FUNDING_BLOCKER_ENTRY_AFTER_BOUNDARY,
    FUNDING_BLOCKER_GATE_CALLBACK_CANCELLED,
    FUNDING_BLOCKER_AUTHORITATIVE_INJECTION,
})


class RisexFundingBoundaryError(CoordinatorSafetyError):
    """Sanitized fail-closed funding-boundary rejection."""


_PREPARATION_FAILURE_CODES = frozenset({
    "STARTUP_FAILED",
    "STARTUP_INTERRUPTED",
    "BINDING_MISMATCH",
    "BARRIER_ABORTED",
    "NADO_FAILED",
    "RISEX_FAILED",
    "ASYMMETRIC_PARTIAL_EXPOSURE",
    "MANUAL_RECOVERY_REQUIRED",
    "CLOSE_FAILED",
})
_PREPARATION_FAILURE_CLASS = "SAFETY"
_PREPARATION_FAILURE_STAGE = "ENTRY_PREPARATION"


class _PreparationGateFailure(RisexFundingBoundaryError):
    """Sanitized allowlisted failure from the owner preparation barrier."""

    def __init__(self, code: str) -> None:
        if type(code) is not str or code not in _PREPARATION_FAILURE_CODES:
            raise ValueError("unsupported preparation failure")
        self.code = code
        self.failure_class = _PREPARATION_FAILURE_CLASS
        self.stage = _PREPARATION_FAILURE_STAGE
        super().__init__(code)


def _preparation_gate_failure(error: object) -> tuple[str, str, str] | None:
    """Accept only the fixed code/class/stage tuple from the owner barrier."""
    try:
        code = getattr(error, "code", None)
        failure_class = getattr(error, "failure_class", None)
        stage = getattr(error, "stage", None)
    except BaseException:
        return None
    if (
        type(code) is not str
        or code not in _PREPARATION_FAILURE_CODES
        or failure_class != _PREPARATION_FAILURE_CLASS
        or stage != _PREPARATION_FAILURE_STAGE
    ):
        return None
    return code, _PREPARATION_FAILURE_CLASS, _PREPARATION_FAILURE_STAGE


class FundingBoundaryResult(str, Enum):
    COMPLETE = "COMPLETE"
    BLOCKED = "BLOCKED"
    FAILED_HALTED_MANUAL_RECOVERY = "FAILED_HALTED_MANUAL_RECOVERY"


@dataclass(frozen=True)
class FundingBoundaryReport:
    result: FundingBoundaryResult
    coordinator_report: CoordinatorReport
    funding_status: str
    funding_blocker: str | None
    gate_invocations: int

    @property
    def phase(self) -> Phase:
        return self.coordinator_report.phase

    @property
    def run_id(self) -> str:
        return self.coordinator_report.run_id

    @property
    def primary_run_id(self) -> str:
        return self.coordinator_report.primary_run_id

    @property
    def counterparty_run_id(self) -> str:
        return self.coordinator_report.counterparty_run_id

    def sanitized(self) -> dict[str, Any]:
        return {
            "result": self.result.value,
            "coordinator": self.coordinator_report.sanitized(),
            "funding_status": self.funding_status,
            "funding_blocker": self.funding_blocker,
            "gate_invocations": self.gate_invocations,
        }


class BoundarySignalKind(str, Enum):
    RELEASED = "RELEASED"
    CANCELLED = "CANCELLED"


@dataclass(frozen=True)
class HoldReleaseSignal:
    """Coordination-only signal; it carries no funding status or cash claim."""

    kind: str
    binding_digest: str
    settlement_at_ms: int
    observed_at_ms: int
    signal_id: str

    @classmethod
    def released(
        cls,
        binding: "RisexFundingBoundaryBinding",
        *,
        observed_at_ms: int,
        signal_id: str,
    ) -> "HoldReleaseSignal":
        return cls(
            BoundarySignalKind.RELEASED.value,
            binding.identity_digest,
            binding.boundary.route.settlement_at_ms,
            observed_at_ms,
            signal_id,
        )

    @classmethod
    def cancelled(
        cls,
        binding: "RisexFundingBoundaryBinding",
        *,
        observed_at_ms: int,
        signal_id: str,
    ) -> "HoldReleaseSignal":
        return cls(
            BoundarySignalKind.CANCELLED.value,
            binding.identity_digest,
            binding.boundary.route.settlement_at_ms,
            observed_at_ms,
            signal_id,
        )


@dataclass(frozen=True)
class RisexFundingBoundaryBinding:
    """Exact RISEx pair plus the accepted Nado contract binding."""

    boundary: FundingBoundaryBinding
    primary_journal: JournalIdentity
    counterparty_journal: JournalIdentity

    @property
    def route(self) -> FundingRouteBinding:
        return self.boundary.route

    @property
    def nado_journal(self) -> JournalIdentity:
        return self.boundary.nado_journal

    @property
    def identity_digest(self) -> str:
        return _binding_digest(self)

    def assert_contract(self) -> None:
        if type(self.boundary) is not FundingBoundaryBinding:
            raise RisexFundingBoundaryError("RISEx funding boundary type rejected")
        if type(self.primary_journal) is not JournalIdentity:
            raise RisexFundingBoundaryError("RISEx primary journal type rejected")
        if type(self.counterparty_journal) is not JournalIdentity:
            raise RisexFundingBoundaryError("RISEx counterparty journal type rejected")
        try:
            self.boundary.assert_contract()
            self.primary_journal.assert_contract()
            self.counterparty_journal.assert_contract()
        except Exception:
            raise RisexFundingBoundaryError("RISEx funding boundary identity rejected") from None
        if (
            self.boundary.risex_journal != self.primary_journal
            or self.primary_journal.venue != RISEX_VENUE
            or self.counterparty_journal.venue != RISEX_VENUE
            or self.boundary.nado_journal.venue != NADO_VENUE
            or self.primary_journal.account_id == self.counterparty_journal.account_id
        ):
            raise RisexFundingBoundaryError("RISEx funding journal identity rejected")


def _exact_decimal(value: object, label: str) -> Decimal:
    if isinstance(value, bool) or isinstance(value, float) or not isinstance(
        value, (str, int, Decimal)
    ):
        raise RisexFundingBoundaryError(f"{label} rejected")
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise RisexFundingBoundaryError(f"{label} rejected") from None
    if not parsed.is_finite():
        raise RisexFundingBoundaryError(f"{label} rejected")
    return parsed


def _safe_signal_id(value: object) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise RisexFundingBoundaryError("RISEx boundary signal identity rejected")
    if len(value) > 128 or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
        raise RisexFundingBoundaryError("RISEx boundary signal identity rejected")
    return value


def _route_payload(route: FundingRouteBinding) -> dict[str, object]:
    return {
        "canonical_asset": route.canonical_asset,
        "risex_leg": {
            "venue": route.risex_leg.venue,
            "market": route.risex_leg.market,
            "direction": route.risex_leg.direction,
            "raw_quantity": str(route.risex_leg.raw_quantity),
            "base_multiplier": str(route.risex_leg.base_multiplier),
            "canonical_quantity": str(route.risex_leg.canonical_quantity),
        },
        "nado_leg": {
            "venue": route.nado_leg.venue,
            "market": route.nado_leg.market,
            "direction": route.nado_leg.direction,
            "raw_quantity": str(route.nado_leg.raw_quantity),
            "base_multiplier": str(route.nado_leg.base_multiplier),
            "canonical_quantity": str(route.nado_leg.canonical_quantity),
        },
        "nado_product_id": route.nado_product_id,
        "settlement_at_ms": route.settlement_at_ms,
    }


def _journal_payload(journal: JournalIdentity) -> dict[str, object]:
    return {
        "venue": journal.venue,
        "run_id": journal.run_id,
        "store_identity": journal.store_identity,
        "account_id": journal.account_id,
    }


def _boundary_payload(binding: RisexFundingBoundaryBinding) -> dict[str, object]:
    return {
        "route": _route_payload(binding.route),
        "primary_journal": _journal_payload(binding.primary_journal),
        "counterparty_journal": _journal_payload(binding.counterparty_journal),
        "nado_journal": _journal_payload(binding.nado_journal),
    }


def _binding_digest(binding: RisexFundingBoundaryBinding) -> str:
    return hashlib.sha256(canonical_payload(_boundary_payload(binding))).hexdigest()


def fixed_funding_route(settlement_at_ms: int) -> FundingRouteBinding:
    if type(settlement_at_ms) is not int or settlement_at_ms <= 0:
        raise RisexFundingBoundaryError("RISEx funding boundary timestamp rejected")
    route = FundingRouteBinding(
        canonical_asset=TARGET_CANONICAL_ASSET,
        risex_leg=FundingLegBinding(
            RISEX_VENUE,
            TARGET_RISEX_MARKET,
            LONG,
            TARGET_QUANTITY,
            TARGET_MULTIPLIER,
            TARGET_QUANTITY,
        ),
        nado_leg=FundingLegBinding(
            NADO_VENUE,
            TARGET_NADO_MARKET,
            SHORT,
            TARGET_QUANTITY,
            TARGET_MULTIPLIER,
            TARGET_QUANTITY,
        ),
        nado_product_id=TARGET_NADO_PRODUCT_ID,
        settlement_at_ms=settlement_at_ms,
    )
    try:
        route.assert_contract()
    except Exception:
        raise RisexFundingBoundaryError("RISEx fixed funding route rejected") from None
    return route


def _signal_payload(signal: HoldReleaseSignal) -> dict[str, object]:
    return {
        "kind": signal.kind,
        "binding_digest": signal.binding_digest,
        "settlement_at_ms": signal.settlement_at_ms,
        "observed_at_ms": signal.observed_at_ms,
        "signal_id": signal.signal_id,
    }


def _signal_json(signal: HoldReleaseSignal) -> str:
    return canonical_payload(_signal_payload(signal)).decode("ascii")


def _forbidden_funding_claim(value: object) -> bool:
    forbidden = {
        "authoritative", "cash", "cash_x18", "funding_cash", "funding_rate",
        "rate", "source", "status", "funding_status", "quality",
    }
    if isinstance(value, Mapping):
        try:
            fields = tuple(value)
        except Exception:
            return True
    else:
        try:
            fields = tuple(vars(value))
        except TypeError:
            return False
        except Exception:
            return True
    for field in fields:
        if not isinstance(field, str):
            continue
        normalized = field.lower()
        if normalized in forbidden or "authoritative" in normalized:
            return True
        if normalized in {"funding", "funding_event", "funding_claim"}:
            return True
    return False


def _required_observation_timestamps(observation: VenueObservation) -> tuple[int, ...]:
    values: list[int] = [
        observation.market.observed_at,
        observation.market.book.observed_at,
    ]
    for role in (AccountRole.PRIMARY, AccountRole.COUNTERPARTY):
        account = observation.accounts[role]
        values.extend((account.observed_at, account.portfolio.observed_at))
        values.extend(item.observed_at for item in account.open_orders)
        values.extend(item.observed_at for item in account.history_orders)
        values.extend(item.observed_at for item in account.trades)
        if account.private is not None:
            values.append(account.private.observed_at)
            values.extend(item.observed_at for item in account.private.orders_snapshot)
            values.extend(item.observed_at for item in account.private.orders_updates)
    if any(type(value) is not int or value <= 0 for value in values):
        raise RisexFundingBoundaryError("RISEx observation timestamp rejected")
    return tuple(values)


def _oldest_required_observation_ms(observation: VenueObservation) -> int:
    return min(_required_observation_timestamps(observation)) * 1_000


def _newest_required_observation_ms(observation: VenueObservation) -> int:
    return max(_required_observation_timestamps(observation)) * 1_000


def _stable_immutable_value(value: object) -> object:
    if value is None or type(value) in {bool, int, str}:
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise RisexFundingBoundaryError("RISEx callback evidence is nonfinite")
        return str(value)
    if isinstance(value, (tuple, list)):
        return tuple(_stable_immutable_value(item) for item in value)
    if isinstance(value, Mapping):
        normalized: dict[str, object] = {}
        for key, item in value.items():
            if not isinstance(key, (str, int)):
                raise RisexFundingBoundaryError("RISEx callback evidence key rejected")
            normalized[str(key)] = _stable_immutable_value(item)
        return normalized
    if is_dataclass(value) and not isinstance(value, type):
        params = getattr(type(value), "__dataclass_params__", None)
        if params is None or params.frozen is not True:
            raise RisexFundingBoundaryError("RISEx callback evidence is mutable")
        return {
            item.name: _stable_immutable_value(getattr(value, item.name))
            for item in fields(value)
        }
    if isinstance(value, Enum):
        return _stable_immutable_value(value.value)
    raise RisexFundingBoundaryError("RISEx callback evidence field rejected")


def _callback_reference(value: object) -> tuple[int, str]:
    """Extract the exact immutable evidence path used by Nado's callback seam."""
    if value is None or isinstance(value, (str, bytes, bytearray, Mapping)):
        raise RisexFundingBoundaryError("RISEx Nado callback reference rejected")
    try:
        evidence = getattr(value, "evidence")
    except Exception:
        raise RisexFundingBoundaryError("RISEx Nado callback reference rejected") from None
    if not is_dataclass(evidence) or isinstance(evidence, type):
        raise RisexFundingBoundaryError("RISEx Nado callback reference rejected")
    params = getattr(type(evidence), "__dataclass_params__", None)
    if params is None or params.frozen is not True:
        raise RisexFundingBoundaryError("RISEx Nado callback evidence is mutable")
    try:
        observed_at_ms = getattr(evidence, "observed_at_ms")
        evidence_payload = {
            item.name: _stable_immutable_value(getattr(evidence, item.name))
            for item in fields(evidence)
        }
    except Exception:
        raise RisexFundingBoundaryError("RISEx Nado callback reference rejected") from None
    if type(observed_at_ms) is not int or observed_at_ms <= 0:
        raise RisexFundingBoundaryError("RISEx Nado callback reference rejected")
    type_name = f"{type(evidence).__module__}.{type(evidence).__qualname__}"
    token = hashlib.sha256(canonical_payload({
        "type": type_name,
        "evidence": evidence_payload,
    })).hexdigest()
    return observed_at_ms, token


class HoldReleaseGate(Protocol):
    def __call__(self, binding: RisexFundingBoundaryBinding) -> object: ...


PreparationGate = Callable[[FundingBoundaryBinding], object]


class RisexFundingBoundaryCoordinator(TwoAccountCoordinator):
    """Fixed-route wrapper that gates the first observation and all prep."""

    def __init__(
        self,
        *,
        venue: _coordinator.VenueAdapter,
        primary_identity: RoleIdentity,
        counterparty_identity: RoleIdentity,
        primary_journal: PairJournal,
        counterparty_journal: PairJournal,
        now: Callable[[], int] = lambda: int(time.time()),
        identity_factory: _coordinator.IdentityFactory = _coordinator._default_identity_factory,
        boundary: FundingBoundaryBinding | None = None,
        hold_release_gate: HoldReleaseGate | None = None,
        preparation_gate: PreparationGate | None = None,
    ) -> None:
        super().__init__(
            venue=venue,
            primary_identity=primary_identity,
            counterparty_identity=counterparty_identity,
            primary_journal=primary_journal,
            counterparty_journal=counterparty_journal,
            now=now,
            identity_factory=identity_factory,
        )
        self._funding_boundary: FundingBoundaryBinding | None = None
        self._local_binding: RisexFundingBoundaryBinding | None = None
        self._hold_release_gate = hold_release_gate
        if preparation_gate is not None and not callable(preparation_gate):
            raise RisexFundingBoundaryError("RISEx preparation gate unavailable")
        self._preparation_gate = preparation_gate
        self._preparation_gate_used = False
        self._gate_invocations = 0
        self._venue_closed = False
        if boundary is not None:
            self.bind_funding_boundary(boundary)

    @classmethod
    def _fixture(
        cls,
        *,
        venue: _coordinator.VenueAdapter,
        primary_journal: str | Path | PairJournal,
        counterparty_journal: str | Path | PairJournal,
        now: Callable[[], int] = lambda: int(time.time()),
        identity_factory: _coordinator.IdentityFactory | None = None,
        primary_account: str = _coordinator.PRIMARY_ACCOUNT,
        primary_signer: str = _coordinator.PRIMARY_SIGNER,
        counterparty_account: str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        counterparty_signer: str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        boundary: FundingBoundaryBinding | None = None,
        hold_release_gate: HoldReleaseGate | None = None,
        preparation_gate: PreparationGate | None = None,
    ) -> "RisexFundingBoundaryCoordinator":
        base = TwoAccountCoordinator._fixture(
            venue=venue,
            primary_journal=primary_journal,
            counterparty_journal=counterparty_journal,
            now=now,
            identity_factory=identity_factory,
            primary_account=primary_account,
            primary_signer=primary_signer,
            counterparty_account=counterparty_account,
            counterparty_signer=counterparty_signer,
        )
        return cls(
            venue=base._venue,
            primary_identity=base._identities[AccountRole.PRIMARY],
            counterparty_identity=base._identities[AccountRole.COUNTERPARTY],
            primary_journal=base._journals[AccountRole.PRIMARY],
            counterparty_journal=base._journals[AccountRole.COUNTERPARTY],
            now=base._now,
            identity_factory=base._identity_factory,
            boundary=boundary,
            hold_release_gate=hold_release_gate,
            preparation_gate=preparation_gate,
        )

    def _journal_identity(self, role: AccountRole) -> JournalIdentity:
        identity = self._identities[role]
        store_identity = (
            FUNDING_BOUNDARY_PRIMARY_STORE_IDENTITY
            if role is AccountRole.PRIMARY
            else FUNDING_BOUNDARY_COUNTERPARTY_STORE_IDENTITY
        )
        return JournalIdentity(
            RISEX_VENUE,
            self._journals[role].run_id,
            store_identity,
            identity.account,
        )

    @property
    def potential_write(self) -> bool:
        """Conservatively expose whether a RISEx write may have started."""
        return any(
            item.dispatch_count > 0
            for journal in self._journals.values()
            for item in (*journal.intents(), *journal.cancels())
        )

    def _fixed_route_check(self, boundary: FundingBoundaryBinding) -> None:
        if type(boundary) is not FundingBoundaryBinding:
            raise RisexFundingBoundaryError("RISEx funding boundary type rejected")
        try:
            boundary.assert_contract()
        except Exception:
            raise RisexFundingBoundaryError("RISEx funding boundary contract rejected") from None
        route = boundary.route
        if (
            route.canonical_asset != TARGET_CANONICAL_ASSET
            or route.risex_leg.venue != RISEX_VENUE
            or route.risex_leg.market != TARGET_RISEX_MARKET
            or route.risex_leg.direction != LONG
            or route.nado_leg.venue != NADO_VENUE
            or route.nado_leg.market != TARGET_NADO_MARKET
            or route.nado_leg.direction != SHORT
            or route.nado_product_id != TARGET_NADO_PRODUCT_ID
        ):
            raise RisexFundingBoundaryError("RISEx fixed funding route mismatch")
        for label, leg in (("RISEx", route.risex_leg), ("Nado", route.nado_leg)):
            if (
                _exact_decimal(leg.raw_quantity, f"{label} route quantity") != TARGET_QUANTITY
                or _exact_decimal(leg.base_multiplier, f"{label} route multiplier") != TARGET_MULTIPLIER
                or _exact_decimal(leg.canonical_quantity, f"{label} canonical quantity") != TARGET_QUANTITY
            ):
                raise RisexFundingBoundaryError("RISEx fixed funding quantity mismatch")
        expected_primary = self._journal_identity(AccountRole.PRIMARY)
        if boundary.risex_journal != expected_primary:
            raise RisexFundingBoundaryError("RISEx primary journal binding mismatch")

    def bind_funding_boundary(
        self, boundary: FundingBoundaryBinding,
    ) -> RisexFundingBoundaryBinding:
        """Persist exact route and all RISEx/Nado journal identities before prep."""
        self._fixed_route_check(boundary)
        local = RisexFundingBoundaryBinding(
            boundary=boundary,
            primary_journal=self._journal_identity(AccountRole.PRIMARY),
            counterparty_journal=self._journal_identity(AccountRole.COUNTERPARTY),
        )
        local.assert_contract()
        encoded = canonical_payload(_boundary_payload(local)).decode("ascii")
        digest = _binding_digest(local)
        stored_payload = self._paired_terminal(_BOUNDARY_PAYLOAD_KEY)
        stored_digest = self._paired_terminal(_BOUNDARY_DIGEST_KEY)
        if stored_payload is not None or stored_digest is not None:
            if stored_payload != encoded or stored_digest != digest:
                raise RisexFundingBoundaryError("RISEx funding boundary is immutable")
        else:
            if any(journal.intents() or journal.cancels() for journal in self._journals.values()):
                raise RisexFundingBoundaryError("RISEx funding boundary was bound too late")
            if any(
                journal.phase is not Phase.START or journal.outcome != "ACTIVE"
                for journal in self._journals.values()
            ):
                raise RisexFundingBoundaryError("RISEx funding boundary lifecycle state rejected")
            self._set_paired_terminal(_BOUNDARY_PAYLOAD_KEY, encoded)
            self._set_paired_terminal(_BOUNDARY_DIGEST_KEY, digest)
        self._funding_boundary = boundary
        self._local_binding = local
        return local

    def _paired_terminal(self, key: str) -> str | None:
        values = tuple(journal.terminal(key) for journal in self._journals.values())
        if values[0] != values[1]:
            raise RisexFundingBoundaryError("RISEx paired journal terminal mismatch")
        return values[0]

    def _set_paired_terminal(self, key: str, value: str) -> None:
        if not isinstance(value, str) or not value:
            raise RisexFundingBoundaryError("RISEx paired journal value rejected")
        current = self._paired_terminal(key)
        if current is not None and current != value:
            raise RisexFundingBoundaryError("RISEx paired journal value is immutable")
        if current == value:
            return
        self._journals[AccountRole.PRIMARY].set_terminal(key, value)
        self._journals[AccountRole.COUNTERPARTY].set_terminal(key, value)

    def _require_bound(self) -> RisexFundingBoundaryBinding:
        binding = self._local_binding
        if binding is None or self._funding_boundary is None:
            raise RisexFundingBoundaryError("RISEx funding boundary is not bound")
        binding.assert_contract()
        self._fixed_route_check(binding.boundary)
        expected_payload = canonical_payload(_boundary_payload(binding)).decode("ascii")
        if (
            self._paired_terminal(_BOUNDARY_PAYLOAD_KEY) != expected_payload
            or self._paired_terminal(_BOUNDARY_DIGEST_KEY) != binding.identity_digest
        ):
            raise RisexFundingBoundaryError("RISEx persisted funding boundary mismatch")
        return binding

    def _persist_funding_blocker(self, reason: str) -> None:
        if reason not in _KNOWN_FUNDING_BLOCKERS:
            raise RisexFundingBoundaryError("RISEx funding blocker rejected")
        self._set_paired_terminal(_FUNDING_STATUS_KEY, FUNDING_UNRESOLVED)
        self._set_paired_terminal(_FUNDING_BLOCKER_KEY, reason)

    def _assert_fixed_preparation(
        self,
        role: AccountRole,
        observation: VenueObservation,
        *,
        step: str,
        side: str,
        order_type: str,
        time_in_force: str,
        reduce_only: bool,
        post_only: bool,
        size: Decimal,
        price: Decimal,
        source_position: Decimal,
    ) -> None:
        self._require_bound()
        try:
            _validate_market(observation.market, _coordinator._now_int(self._now()))
            if (role, step) == (AccountRole.COUNTERPARTY, "ENTRY_MAKER"):
                expected = (
                    "SELL", "LIMIT", "GTC", False, True, Decimal("0"),
                    _coordinator.maker_price(observation.market, "SELL"),
                )
            elif (role, step) == (AccountRole.PRIMARY, "ENTRY_TAKER"):
                expected = (
                    "BUY", "MARKET", "IOC", False, False, Decimal("0"),
                    _bound(observation.market.book.ask, observation.market.tick, "BUY"),
                )
            elif (role, step) == (AccountRole.COUNTERPARTY, "EXIT_MAKER"):
                expected = (
                    "BUY", "LIMIT", "GTC", True, True, -TARGET_QUANTITY,
                    _coordinator.maker_price(observation.market, "BUY"),
                )
            elif (role, step) == (AccountRole.PRIMARY, "EXIT_TAKER"):
                expected = (
                    "SELL", "MARKET", "IOC", True, False, TARGET_QUANTITY,
                    _bound(observation.market.book.bid, observation.market.tick, "SELL"),
                )
            else:
                raise ValueError
            expected_side, expected_order_type, expected_tif, expected_reduce, expected_post, expected_source, expected_price = expected
            parsed_price = _exact_decimal(price, "RISEx preparation price")
            parsed_size = _exact_decimal(size, "RISEx preparation quantity")
            parsed_source = _exact_decimal(source_position, "RISEx preparation source position")
            if (
                side != expected_side
                or order_type != expected_order_type
                or time_in_force != expected_tif
                or reduce_only is not expected_reduce
                or post_only is not expected_post
                or parsed_size != TARGET_QUANTITY
                or parsed_source != expected_source
                or parsed_price != expected_price
                or parsed_price <= 0
                or parsed_price % MARKET_TICK != 0
            ):
                raise ValueError
        except (KeyError, ValueError, CoordinatorSafetyError):
            raise RisexFundingBoundaryError("RISEx fixed preparation contract rejected") from None

    def _run_preparation_gate(self, binding: RisexFundingBoundaryBinding) -> None:
        if self._preparation_gate is None or self._preparation_gate_used:
            return
        try:
            result = self._preparation_gate(binding.boundary)
            if inspect.isawaitable(result):
                raise RisexFundingBoundaryError(
                    "RISEx preparation gate must be synchronous"
                )
        except RisexFundingBoundaryError:
            raise
        except BaseException as error:
            preserved = _preparation_gate_failure(error)
            if preserved is not None:
                raise _PreparationGateFailure(preserved[0]) from None
            raise RisexFundingBoundaryError(
                "RISEx preparation gate rejected"
            ) from None
        self._preparation_gate_used = True

    def _prepare(
        self,
        role: AccountRole,
        observation: VenueObservation,
        *,
        step: str,
        side: str,
        order_type: str,
        time_in_force: str,
        reduce_only: bool,
        post_only: bool,
        size: Decimal,
        price: Decimal,
        source_position: Decimal,
    ) -> _coordinator.DurableIntent:
        self._assert_fixed_preparation(
            role,
            observation,
            step=step,
            side=side,
            order_type=order_type,
            time_in_force=time_in_force,
            reduce_only=reduce_only,
            post_only=post_only,
            size=size,
            price=price,
            source_position=source_position,
        )
        return super()._prepare(
            role,
            observation,
            step=step,
            side=side,
            order_type=order_type,
            time_in_force=time_in_force,
            reduce_only=reduce_only,
            post_only=post_only,
            size=size,
            price=price,
            source_position=source_position,
        )

    def _gate_invocation_id(self) -> str:
        binding = self._require_bound()
        return _canonical_digest({
            "binding_digest": binding.identity_digest,
            "purpose": "RISEx_FUNDING_BOUNDARY_HOLD_RELEASE",
        })

    def _persist_gate_result(self, result: str) -> None:
        self._set_paired_terminal(_GATE_RESULT_KEY, result)

    def _record_gate_blocker(self, reason: str) -> bool:
        self._persist_gate_result(f"BLOCKED|{reason}")
        self._persist_funding_blocker(reason)
        return False

    def _decode_persisted_release(self, encoded: str) -> HoldReleaseSignal:
        try:
            payload = json.loads(encoded)
            if type(payload) is not dict or set(payload) != {
                "kind", "binding_digest", "settlement_at_ms",
                "observed_at_ms", "signal_id",
            }:
                raise ValueError
            signal = HoldReleaseSignal(
                payload["kind"], payload["binding_digest"],
                payload["settlement_at_ms"], payload["observed_at_ms"],
                payload["signal_id"],
            )
            return self._validate_signal(signal, require_current_fresh=False)
        except (CoordinatorSafetyError, ValueError, TypeError, json.JSONDecodeError):
            raise RisexFundingBoundaryError("RISEx persisted gate release rejected") from None

    def _assert_entry_contract(self) -> None:
        expected = {
            (AccountRole.COUNTERPARTY, "ENTRY_MAKER"): (
                "SELL", "LIMIT", "GTC", False, True, Decimal("0"),
            ),
            (AccountRole.PRIMARY, "ENTRY_TAKER"): (
                "BUY", "MARKET", "IOC", False, False, Decimal("0"),
            ),
        }
        for key, contract in expected.items():
            role, step = key
            intent = self._journal_intent(role, step)
            side, order_type, tif, reduce_only, post_only, source = contract
            if (
                intent.ordinal != 1
                or intent.step != step
                or intent.side != side
                or intent.order_type != order_type
                or intent.time_in_force != tif
                or intent.reduce_only is not reduce_only
                or intent.post_only is not post_only
                or intent.market_id != MARKET_ID
                or intent.size != TARGET_QUANTITY
                or intent.source_position != source
                or intent.state != "TERMINAL"
                or intent.dispatch_count != 1
                or intent.reconciled is not True
                or intent.filled_size != TARGET_QUANTITY
                or intent.order_id is None
            ):
                raise RisexFundingBoundaryError("RISEx entry reconciliation contract rejected")

    def _assert_entry_observation(self, observation: VenueObservation) -> None:
        self._validate_observation(observation)
        self._assert_entry_contract()
        expected_positions = {
            AccountRole.PRIMARY: TARGET_QUANTITY,
            AccountRole.COUNTERPARTY: -TARGET_QUANTITY,
        }
        for role, expected in expected_positions.items():
            account = observation.accounts[role]
            if (
                account.position != expected
                or account.open_orders
                or account.unexplained
            ):
                raise RisexFundingBoundaryError("RISEx held entry state rejected")
            if account.private is not None and any(
                size != expected
                for market_id, size in account.private.positions_snapshot
                if market_id == MARKET_ID
            ):
                raise RisexFundingBoundaryError("RISEx held private position rejected")

    def _record_entry_reconciliation(self, observation: VenueObservation) -> int:
        self._assert_entry_observation(observation)
        observed_at_ms = _newest_required_observation_ms(observation)
        stored = self._paired_terminal(_ENTRY_RECONCILIATION_AT_KEY)
        if stored is None:
            self._set_paired_terminal(_ENTRY_RECONCILIATION_AT_KEY, str(observed_at_ms))
            return observed_at_ms
        try:
            stored_at_ms = int(stored)
        except (TypeError, ValueError):
            raise RisexFundingBoundaryError(
                "RISEx persisted entry reconciliation timestamp rejected"
            ) from None
        if stored_at_ms <= 0 or observed_at_ms < stored_at_ms:
            raise RisexFundingBoundaryError(
                "RISEx entry reconciliation timestamp regressed"
            )
        return stored_at_ms

    def _validate_signal(
        self, value: object, *, require_current_fresh: bool = True,
    ) -> HoldReleaseSignal:
        if type(value) is not HoldReleaseSignal:
            if _forbidden_funding_claim(value):
                raise RisexFundingBoundaryError(FUNDING_BLOCKER_AUTHORITATIVE_INJECTION)
            raise RisexFundingBoundaryError(FUNDING_BLOCKER_EVIDENCE_MISSING)
        binding = self._require_bound()
        if value.kind not in {item.value for item in BoundarySignalKind}:
            raise RisexFundingBoundaryError(FUNDING_BLOCKER_EVIDENCE_CONTRADICTORY)
        if (
            value.binding_digest != binding.identity_digest
            or value.settlement_at_ms != binding.route.settlement_at_ms
        ):
            raise RisexFundingBoundaryError(FUNDING_BLOCKER_EVIDENCE_CONTRADICTORY)
        _safe_signal_id(value.signal_id)
        if type(value.observed_at_ms) is not int or value.observed_at_ms <= 0:
            raise RisexFundingBoundaryError(FUNDING_BLOCKER_EVIDENCE_STALE)
        if require_current_fresh:
            now_ms = _coordinator._now_int(self._now()) * 1_000
            if value.observed_at_ms > now_ms or now_ms - value.observed_at_ms > _FRESHNESS_MS:
                raise RisexFundingBoundaryError(FUNDING_BLOCKER_EVIDENCE_STALE)
        if (
            value.kind == BoundarySignalKind.RELEASED.value
            and value.observed_at_ms < binding.route.settlement_at_ms
        ):
            raise RisexFundingBoundaryError(FUNDING_BLOCKER_EVIDENCE_STALE)
        return value

    def _resolve_hold_release(self) -> bool:
        """Resolve the one gate once, durably, after exact entry reconciliation."""
        binding = self._require_bound()
        invocation_id = self._gate_invocation_id()
        prior_result = self._paired_terminal(_GATE_RESULT_KEY)
        if prior_result is not None:
            if prior_result.startswith("RELEASED|"):
                self._decode_persisted_release(prior_result[len("RELEASED|"):])
                self._persist_funding_blocker(FUNDING_BLOCKER_MISSING_CONTRACT)
                return True
            if prior_result.startswith("BLOCKED|"):
                reason = prior_result[len("BLOCKED|"):]
                if reason not in _KNOWN_FUNDING_BLOCKERS:
                    raise RisexFundingBoundaryError("RISEx persisted gate blocker rejected")
                self._persist_funding_blocker(reason)
                return False
            raise RisexFundingBoundaryError("RISEx persisted gate result rejected")

        prior_invocation = self._paired_terminal(_GATE_INVOCATION_KEY)
        if prior_invocation is not None:
            if prior_invocation != f"STARTED|{invocation_id}":
                raise RisexFundingBoundaryError("RISEx gate invocation identity rejected")
            return self._record_gate_blocker(FUNDING_BLOCKER_BOUNDARY_INTERRUPTED)
        if self._hold_release_gate is None:
            return self._record_gate_blocker(FUNDING_BLOCKER_BOUNDARY_GATE_MISSING)

        # This marker is durable before the external callback.  A restart that
        # sees STARTED with no result blocks and flattens; it never calls back.
        self._set_paired_terminal(_GATE_INVOCATION_KEY, f"STARTED|{invocation_id}")
        self._gate_invocations += 1
        try:
            result = self._hold_release_gate(binding)
        except asyncio.CancelledError:
            return self._record_gate_blocker(FUNDING_BLOCKER_GATE_CALLBACK_CANCELLED)
        except Exception:
            return self._record_gate_blocker(FUNDING_BLOCKER_EVIDENCE_MISSING)
        if inspect.isawaitable(result):
            return self._record_gate_blocker(FUNDING_BLOCKER_EVIDENCE_MISSING)
        try:
            signal = self._validate_signal(result)
        except RisexFundingBoundaryError as error:
            reason = str(error)
            if reason not in {
                FUNDING_BLOCKER_AUTHORITATIVE_INJECTION,
                FUNDING_BLOCKER_EVIDENCE_MISSING,
                FUNDING_BLOCKER_EVIDENCE_CONTRADICTORY,
                FUNDING_BLOCKER_EVIDENCE_STALE,
            }:
                reason = FUNDING_BLOCKER_EVIDENCE_CONTRADICTORY
            return self._record_gate_blocker(reason)
        if signal.kind == BoundarySignalKind.CANCELLED.value:
            return self._record_gate_blocker(FUNDING_BLOCKER_CANCELLED)
        self._persist_gate_result(f"RELEASED|{_signal_json(signal)}")
        # RISEx has no accepted applied-funding cash/status wire contract.
        # Even a valid coordination release therefore remains UNRESOLVED.
        self._persist_funding_blocker(FUNDING_BLOCKER_MISSING_CONTRACT)
        return True

    def _assert_gate_before_exit(self) -> None:
        result = self._paired_terminal(_GATE_RESULT_KEY)
        if result is None:
            raise RisexFundingBoundaryError("RISEx exit gate decision is missing")
        if result.startswith("RELEASED|"):
            self._decode_persisted_release(result[len("RELEASED|"):])
        elif result.startswith("BLOCKED|"):
            if result[len("BLOCKED|"):] not in _KNOWN_FUNDING_BLOCKERS:
                raise RisexFundingBoundaryError("RISEx exit gate blocker rejected")
        else:
            raise RisexFundingBoundaryError("RISEx exit gate decision is missing")
        if self._paired_terminal(_FUNDING_STATUS_KEY) != FUNDING_UNRESOLVED:
            raise RisexFundingBoundaryError("RISEx funding status contract is not unresolved")
        if self._paired_terminal(_FUNDING_BLOCKER_KEY) not in _KNOWN_FUNDING_BLOCKERS:
            raise RisexFundingBoundaryError("RISEx funding blocker is missing")

    async def _preflight(self) -> CoordinatorReport | None:
        if self.phase is Phase.START:
            binding = self._require_bound()
            if self._hold_release_gate is None:
                self._block_before_write(FUNDING_BLOCKER_BOUNDARY_GATE_MISSING)
                return self._report(CoordinatorResult.BLOCKED_BEFORE_WRITE)
            # The owner barrier must complete before the first observation.
            # This prevents a slow Nado baseline from aging a RISEx snapshot
            # whose price would otherwise be reused after the barrier.
            self._run_preparation_gate(binding)
        return None

    def _format_run_report(self, report: CoordinatorReport) -> FundingBoundaryReport:
        return self._report_funding(report)

    def _close_after_run(self) -> bool:
        return False

    async def _before_exit_preparation(
        self, observation: VenueObservation,
    ) -> VenueObservation:
        entry_at_ms = self._record_entry_reconciliation(observation)
        binding = self._require_bound()
        if entry_at_ms >= binding.route.settlement_at_ms:
            self._record_gate_blocker(FUNDING_BLOCKER_ENTRY_AFTER_BOUNDARY)
        else:
            self._resolve_hold_release()
        # The gate may have waited for the boundary.  Never use the pre-gate
        # snapshot for flattening; obtain a new authoritative full observation.
        fresh = await self._observe()
        self._assert_entry_observation(fresh)
        return fresh

    async def _exit_maker(self, observation: VenueObservation) -> VenueObservation:
        self._assert_gate_before_exit()
        return await super()._exit_maker(observation)

    def _report_funding(self, report: CoordinatorReport) -> FundingBoundaryReport:
        status = self._paired_terminal(_FUNDING_STATUS_KEY) or FUNDING_UNRESOLVED
        blocker = self._paired_terminal(_FUNDING_BLOCKER_KEY)
        if report.result is CoordinatorResult.FAILED_HALTED_MANUAL_RECOVERY:
            result = FundingBoundaryResult.FAILED_HALTED_MANUAL_RECOVERY
        elif blocker is not None or report.result is not CoordinatorResult.COMPLETE:
            result = FundingBoundaryResult.BLOCKED
        else:
            result = FundingBoundaryResult.COMPLETE
        return FundingBoundaryReport(
            result=result,
            coordinator_report=report,
            funding_status=status,
            funding_blocker=blocker,
            gate_invocations=self._gate_invocations,
        )

    def _block_before_write(self, reason: str) -> FundingBoundaryReport:
        self._last_failure = reason
        self._persist_funding_blocker(reason)
        self._persist_gate_result(f"BLOCKED|{reason}")
        self._set_outcome("HALTED")
        self._set_phase(Phase.HALTED)
        return self._report_funding(self._report(CoordinatorResult.BLOCKED_BEFORE_WRITE))

    async def close(self) -> None:
        if self._venue_closed:
            return
        self._venue_closed = True
        closer = getattr(self._venue, "close", None)
        if callable(closer):
            await _maybe_await(closer())

    def _terminal_observation_check(self, observation: VenueObservation) -> None:
        self._require_bound()
        if self.phase is not Phase.COMPLETE or any(
            journal.outcome != "COMPLETE" for journal in self._journals.values()
        ):
            raise RisexFundingBoundaryError("RISEx terminal lifecycle is not complete")
        for role, journal in self._journals.items():
            if journal.pending_writes() or any(
                item.state != "TERMINAL" or item.dispatch_count != 1
                for item in journal.cancels()
            ):
                raise RisexFundingBoundaryError("RISEx terminal write identity is unresolved")
            for intent in journal.intents():
                if (
                    intent.state != "TERMINAL"
                    or intent.dispatch_count != 1
                    or intent.reconciled is not True
                    or intent.filled_size != intent.size
                ):
                    raise RisexFundingBoundaryError("RISEx terminal intent is unresolved")
            account = observation.accounts[role]
            if (
                account.position != 0
                or account.open_orders
                or account.unexplained
                or (account.private is not None and any(
                    size != 0 for _market_id, size in account.private.positions_snapshot
                ))
            ):
                raise RisexFundingBoundaryError("RISEx terminal zero-flat proof rejected")
            self._prove_final_account(role, account)

    def _terminal_fingerprint(self, observation: VenueObservation) -> str:
        return _canonical_digest({
            "market": {
                "host": observation.market.host,
                "chain_id": observation.market.chain_id,
                "domain_name": observation.market.domain_name,
                "domain_version": observation.market.domain_version,
                "router": observation.market.router,
                "authorization": observation.market.authorization,
                "market_id": observation.market.market_id,
                "symbol": observation.market.symbol,
                "active": observation.market.active,
                "unlocked": observation.market.unlocked,
                "tick": str(observation.market.tick),
                "step": str(observation.market.step),
                "minimum": str(observation.market.minimum),
                "book": {
                    "bid": str(observation.market.book.bid),
                    "ask": str(observation.market.book.ask),
                    "bids": tuple(
                        (str(level.price), str(level.quantity), level.order_count)
                        for level in observation.market.book.bids
                    ),
                    "asks": tuple(
                        (str(level.price), str(level.quantity), level.order_count)
                        for level in observation.market.book.asks
                    ),
                },
            },
            "accounts": {
                role.value: {
                    "account": observation.accounts[role].account,
                    "signer": observation.accounts[role].signer,
                    "signer_status": observation.accounts[role].signer_status,
                    "position": str(observation.accounts[role].position),
                    "open_orders": tuple(sorted(
                        _coordinator._order_history_evidence(item)
                        for item in observation.accounts[role].open_orders
                    )),
                    "history_orders": tuple(sorted(
                        _coordinator._order_history_evidence(item)
                        for item in observation.accounts[role].history_orders
                    )),
                    "trades": tuple(sorted(
                        _coordinator._trade_history_evidence(item)
                        for item in observation.accounts[role].trades
                    )),
                    "portfolio": {
                        "in_liquidation": observation.accounts[role].portfolio.in_liquidation,
                        "risk_level": observation.accounts[role].portfolio.risk_level,
                    },
                }
                for role in (AccountRole.PRIMARY, AccountRole.COUNTERPARTY)
            },
        })

    def _combined_journal_digest(self) -> str:
        values = {
            role.value: self._journals[role].stable_content_digest()
            for role in (AccountRole.PRIMARY, AccountRole.COUNTERPARTY)
        }
        return hashlib.sha256(canonical_payload(values)).hexdigest()

    def _provider_round_key(self, round_index: int) -> str:
        return f"{_PROVIDER_ROUND_PREFIX}{round_index}"

    def _stored_terminal_round(self, round_index: int) -> dict[str, object]:
        encoded = self._paired_terminal(self._provider_round_key(round_index))
        if encoded is None:
            raise RisexFundingBoundaryError("RISEx persisted terminal round is missing")
        try:
            payload = json.loads(encoded)
            if type(payload) is not dict or set(payload) != {
                "binding_digest", "round_index", "rest_round", "fingerprint",
                "observed_at_ms", "journal_content_sha256", "evidence_digest",
                "callback_observed_at_ms", "callback_reference_token",
            }:
                raise ValueError
            if (
                payload["binding_digest"] != self._require_bound().identity_digest
                or type(payload["round_index"]) is not int
                or payload["round_index"] != round_index
                or type(payload["rest_round"]) is not int
                or payload["rest_round"] <= 0
                or type(payload["fingerprint"]) is not str
                or len(payload["fingerprint"]) != 64
                or type(payload["observed_at_ms"]) is not int
                or payload["observed_at_ms"] <= 0
                or payload["journal_content_sha256"] != "0x" + self._combined_journal_digest()
                or type(payload["callback_observed_at_ms"]) is not int
                or payload["callback_observed_at_ms"] <= 0
                or type(payload["callback_reference_token"]) is not str
                or len(payload["callback_reference_token"]) != 64
                or type(payload["evidence_digest"]) is not str
                or len(payload["evidence_digest"]) != 66
                or not payload["evidence_digest"].startswith("0x")
            ):
                raise ValueError
            int(payload["fingerprint"], 16)
            int(payload["callback_reference_token"], 16)
            int(payload["evidence_digest"][2:], 16)
            return payload
        except Exception:
            raise RisexFundingBoundaryError("RISEx persisted terminal round rejected") from None

    def _validate_callback_reference(self, value: object) -> tuple[int, str]:
        observed_at_ms, token = _callback_reference(value)
        now_ms = _coordinator._now_int(self._now()) * 1_000
        if observed_at_ms > now_ms or now_ms - observed_at_ms > _FRESHNESS_MS:
            raise RisexFundingBoundaryError("RISEx Nado callback reference is stale")
        return observed_at_ms, token

    def _validate_terminal_freshness(
        self,
        observation: VenueObservation,
        *,
        callback_observed_at_ms: int,
    ) -> int:
        oldest = _oldest_required_observation_ms(observation)
        now_ms = _coordinator._now_int(self._now()) * 1_000
        if (
            oldest > now_ms
            or now_ms - oldest > _FRESHNESS_MS
            or oldest < callback_observed_at_ms - _FRESHNESS_MS
        ):
            raise RisexFundingBoundaryError("RISEx terminal observation is stale")
        return oldest

    async def terminal_evidence(
        self,
        binding: FundingBoundaryBinding,
        round_index: int,
        callback_reference: object,
    ) -> RisexTerminalEvidence:
        """Build one exact RISEx terminal proof from two-account REST evidence."""
        if type(binding) is not FundingBoundaryBinding:
            raise RisexFundingBoundaryError("RISEx terminal binding type rejected")
        local = self._require_bound()
        if binding != local.boundary:
            raise RisexFundingBoundaryError("RISEx terminal route or journal mismatch")
        if type(round_index) is not int or round_index not in {1, 2}:
            raise RisexFundingBoundaryError("RISEx terminal round index rejected")
        self._assert_gate_before_exit()
        key = self._provider_round_key(round_index)
        if self._paired_terminal(key) is not None:
            raise RisexFundingBoundaryError("RISEx terminal round replay rejected")
        callback_observed_at_ms, callback_reference_token = (
            self._validate_callback_reference(callback_reference)
        )
        first_payload: dict[str, object] | None = None
        if round_index == 2:
            first_payload = self._stored_terminal_round(1)
            if first_payload["callback_reference_token"] == callback_reference_token:
                raise RisexFundingBoundaryError("RISEx Nado callback reference reused")
        observation = await self.rest_round_for_terminal()
        raw_final_round = self._journals[AccountRole.PRIMARY].terminal(
            "final_round_one_id"
        )
        try:
            final_round = int(raw_final_round) if raw_final_round is not None else 0
        except (TypeError, ValueError):
            raise RisexFundingBoundaryError("RISEx final REST round identity rejected") from None
        if final_round <= 0 or observation.rest_round <= final_round:
            raise RisexFundingBoundaryError("RISEx terminal REST round replay rejected")
        self._terminal_observation_check(observation)
        fingerprint = self._terminal_fingerprint(observation)
        observed_at_ms = self._validate_terminal_freshness(
            observation,
            callback_observed_at_ms=callback_observed_at_ms,
        )
        if first_payload is not None:
            try:
                if (
                    first_payload["fingerprint"] != fingerprint
                    or observation.rest_round <= first_payload["rest_round"]
                    or observed_at_ms <= first_payload["observed_at_ms"]
                    or callback_observed_at_ms < first_payload["callback_observed_at_ms"]
                ):
                    raise ValueError
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                raise RisexFundingBoundaryError("RISEx terminal REST rounds disagree") from None
        journal_content_sha256 = "0x" + self._combined_journal_digest()
        unsigned_terminal = TerminalEvidence(
            binding.risex_journal,
            NADO_COMPLETE,
            observed_at_ms,
            journal_content_sha256,
            True,
            True,
            True,
            (),
            "0x" + "00" * 32,
            True,
        )
        evidence_digest = "0x" + terminal_evidence_digest(unsigned_terminal)
        terminal = TerminalEvidence(
            binding.risex_journal,
            NADO_COMPLETE,
            observed_at_ms,
            journal_content_sha256,
            True,
            True,
            True,
            (),
            evidence_digest,
            True,
        )
        evidence = RisexTerminalEvidence(
            binding.route,
            binding.risex_journal,
            terminal,
            round_index,
        )
        try:
            evidence.assert_contract()
        except Exception:
            raise RisexFundingBoundaryError("RISEx terminal evidence contract rejected") from None
        encoded = canonical_payload({
            "binding_digest": local.identity_digest,
            "round_index": round_index,
            "rest_round": observation.rest_round,
            "fingerprint": fingerprint,
            "observed_at_ms": observed_at_ms,
            "journal_content_sha256": journal_content_sha256,
            "evidence_digest": evidence_digest,
            "callback_observed_at_ms": callback_observed_at_ms,
            "callback_reference_token": callback_reference_token,
        }).decode("ascii")
        self._set_paired_terminal(key, encoded)
        return evidence


class _CoordinatorFactory(Protocol):
    def __call__(self) -> Any: ...


class RisexCoordinationLoopBridge:
    """Own the RISEx venue/session loop for synchronous external callbacks."""

    _LIFECYCLE_MARGIN_SECONDS = 30.0

    def __init__(
        self,
        coordinator_factory: _CoordinatorFactory,
        *,
        startup_timeout_seconds: float = 15.0,
        callback_timeout_seconds: float = 10.0,
        lifecycle_timeout_seconds: float = 900.0,
        shutdown_timeout_seconds: float = 15.0,
    ) -> None:
        timeouts = (
            startup_timeout_seconds,
            callback_timeout_seconds,
            lifecycle_timeout_seconds,
            shutdown_timeout_seconds,
        )
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or value <= 0
            for value in timeouts
        ):
            raise RisexFundingBoundaryError("RISEx coordination timeout rejected")
        self._factory = coordinator_factory
        self._startup_timeout_seconds = float(startup_timeout_seconds)
        self._callback_timeout_seconds = float(callback_timeout_seconds)
        self._lifecycle_timeout_seconds = float(lifecycle_timeout_seconds)
        self._shutdown_timeout_seconds = float(shutdown_timeout_seconds)
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread_id: int | None = None
        self._coordinator: RisexFundingBoundaryCoordinator | None = None
        self._error: BaseException | None = None
        self._closed = False
        self._settlement_at_ms: int | None = None
        self._lifecycle_future: Future[Any] | None = None
        self._lifecycle_lock = threading.Lock()

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._thread_main,
            name="risex-funding-boundary-loop",
            daemon=True,
        )
        self._thread.start()
        if not self._ready.wait(self._startup_timeout_seconds):
            raise RisexFundingBoundaryError("RISEx coordination loop did not start")
        if self._error is not None:
            raise RisexFundingBoundaryError("RISEx coordination loop binding rejected") from None

    def _thread_main(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._thread_id = threading.get_ident()
        try:
            value = self._factory()
            if inspect.isawaitable(value):
                value = loop.run_until_complete(value)
            if not isinstance(value, RisexFundingBoundaryCoordinator):
                raise RisexFundingBoundaryError("RISEx coordination object rejected")
            self._coordinator = value
            self._ready.set()
            loop.run_forever()
        except BaseException as error:
            self._error = error
            self._ready.set()
        finally:
            if self._coordinator is not None and not self._coordinator._venue_closed:
                try:
                    loop.run_until_complete(self._coordinator.close())
                except Exception:
                    pass
            if self._coordinator is not None:
                for journal in self._coordinator._journals.values():
                    try:
                        journal.close()
                    except Exception:
                        pass
            loop.close()

    async def _invoke(self, operation: Callable[[RisexFundingBoundaryCoordinator], Any]) -> Any:
        coordinator = self._coordinator
        if coordinator is None:
            raise RisexFundingBoundaryError("RISEx coordination object is unavailable")
        result = operation(coordinator)
        if inspect.isawaitable(result):
            return await result
        return result

    def _submit(
        self,
        operation: Callable[[RisexFundingBoundaryCoordinator], Any],
    ) -> Future[Any]:
        if self._thread is None:
            self.start()
        if self._closed or self._loop is None:
            raise RisexFundingBoundaryError("RISEx coordination loop is closed")
        if threading.get_ident() == self._thread_id:
            raise RisexFundingBoundaryError("RISEx synchronous callback cannot run on its owner loop")
        return asyncio.run_coroutine_threadsafe(self._invoke(operation), self._loop)

    def _wait(
        self,
        future: Future[Any],
        *,
        timeout_seconds: float,
        cancel_on_timeout: bool,
    ) -> Any:
        try:
            return future.result(timeout_seconds)
        except FutureTimeoutError:
            if cancel_on_timeout:
                future.cancel()
            raise RisexFundingBoundaryError("RISEx coordination operation timed out") from None
        except RisexFundingBoundaryError:
            raise
        except CoordinatorSafetyError:
            raise
        except asyncio.CancelledError:
            raise RisexFundingBoundaryError("RISEx coordination operation cancelled") from None
        except Exception:
            raise RisexFundingBoundaryError("RISEx coordination operation failed") from None

    def _call(
        self,
        operation: Callable[[RisexFundingBoundaryCoordinator], Any],
        *,
        timeout_seconds: float,
        cancel_on_timeout: bool = True,
    ) -> Any:
        return self._wait(
            self._submit(operation),
            timeout_seconds=timeout_seconds,
            cancel_on_timeout=cancel_on_timeout,
        )

    def call(self, operation: Callable[[RisexFundingBoundaryCoordinator], Any]) -> Any:
        """Run one short callback operation on the loop owning the venue."""
        return self._call(operation, timeout_seconds=self._callback_timeout_seconds)

    def bind_funding_boundary(self, boundary: FundingBoundaryBinding) -> RisexFundingBoundaryBinding:
        result = self._call(
            lambda coordinator: coordinator.bind_funding_boundary(boundary),
            timeout_seconds=self._callback_timeout_seconds,
        )
        if type(result) is not RisexFundingBoundaryBinding:
            raise RisexFundingBoundaryError("RISEx funding boundary result rejected")
        self._settlement_at_ms = result.route.settlement_at_ms
        return result

    def journal_identities(self) -> tuple[JournalIdentity, JournalIdentity]:
        result = self._call(
            lambda coordinator: (
                coordinator._journal_identity(AccountRole.PRIMARY),
                coordinator._journal_identity(AccountRole.COUNTERPARTY),
            ),
            timeout_seconds=self._callback_timeout_seconds,
        )
        if (
            type(result) is not tuple
            or len(result) != 2
            or any(type(item) is not JournalIdentity for item in result)
        ):
            raise RisexFundingBoundaryError("RISEx journal identity result rejected")
        return result

    @property
    def potential_write(self) -> bool:
        try:
            result = self._call(
                lambda coordinator: coordinator.potential_write,
                timeout_seconds=self._callback_timeout_seconds,
            )
        except BaseException:
            # Inability to inspect a running writer is itself unsafe to treat
            # as prewrite; callers must retain manual-recovery state.
            return True
        if type(result) is not bool:
            raise RisexFundingBoundaryError("RISEx write status result rejected")
        return result

    def run_lifecycle(self, *, timeout_seconds: float | None = None) -> FundingBoundaryReport:
        if timeout_seconds is None:
            if self._settlement_at_ms is None:
                raise RisexFundingBoundaryError("RISEx funding boundary is not bound")
            timeout_seconds = max(
                self._lifecycle_timeout_seconds,
                max(0.0, self._settlement_at_ms / 1_000 - time.time())
                + self._LIFECYCLE_MARGIN_SECONDS,
            )
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(float(timeout_seconds))
            or timeout_seconds <= 0
        ):
            raise RisexFundingBoundaryError("RISEx lifecycle timeout rejected")
        with self._lifecycle_lock:
            if self._lifecycle_future is None:
                self._lifecycle_future = self._submit(
                    lambda coordinator: coordinator.run(),
                )
            future = self._lifecycle_future
        result = self._wait(
            future,
            timeout_seconds=float(timeout_seconds),
            cancel_on_timeout=False,
        )
        if type(result) is not FundingBoundaryReport:
            raise RisexFundingBoundaryError("RISEx lifecycle report rejected")
        return result

    def lifecycle_status(self) -> str:
        with self._lifecycle_lock:
            future = self._lifecycle_future
        if future is None:
            return "NOT_STARTED"
        if not future.done():
            return "RUNNING"
        if future.cancelled():
            return "CANCELLED"
        try:
            if future.exception() is not None:
                return "FAILED"
        except Exception:
            return "FAILED"
        return "FINISHED"

    def close(self) -> None:
        if self._thread is None or self._closed:
            return
        if threading.get_ident() == self._thread_id:
            raise RisexFundingBoundaryError("RISEx coordination loop cannot close itself")
        if self._lifecycle_future is not None and not self._lifecycle_future.done():
            raise RisexFundingBoundaryError("RISEx lifecycle is still running")
        self._closed = True
        loop = self._loop
        if loop is not None:
            loop.call_soon_threadsafe(loop.stop)
        if self._thread is not None:
            self._thread.join(self._shutdown_timeout_seconds)
            if self._thread.is_alive():
                raise RisexFundingBoundaryError("RISEx coordination loop did not close")


class RisexTerminalEvidenceProvider:
    """Synchronous exact-type provider for the accepted Nado callback seam."""

    def __init__(self, bridge: RisexCoordinationLoopBridge) -> None:
        self._bridge = bridge

    def __call__(
        self,
        binding: FundingBoundaryBinding,
        live_observation: object,
        round_index: int,
    ) -> RisexTerminalEvidence:
        _callback_reference(live_observation)
        result = self._bridge.call(
            lambda coordinator: coordinator.terminal_evidence(
                binding, round_index, live_observation,
            )
        )
        if type(result) is not RisexTerminalEvidence:
            raise RisexFundingBoundaryError("RISEx terminal provider result type rejected")
        return result


async def _build_fresh_production_coordinator(
    hold_release_gate: HoldReleaseGate | None,
    preparation_gate: PreparationGate | None = None,
) -> RisexFundingBoundaryCoordinator:
    home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    base = await _coordinator._build_risex_two_account_coordinator_at_paths(
        primary_path=home / FUNDING_BOUNDARY_PRIMARY_JOURNAL,
        counterparty_path=home / FUNDING_BOUNDARY_COUNTERPARTY_JOURNAL,
        require_fresh=True,
    )
    try:
        return RisexFundingBoundaryCoordinator(
            venue=base._venue,
            primary_identity=base._identities[AccountRole.PRIMARY],
            counterparty_identity=base._identities[AccountRole.COUNTERPARTY],
            primary_journal=base._journals[AccountRole.PRIMARY],
            counterparty_journal=base._journals[AccountRole.COUNTERPARTY],
            now=base._now,
            identity_factory=base._identity_factory,
            hold_release_gate=hold_release_gate,
            preparation_gate=preparation_gate,
        )
    except Exception:
        await base._venue.close()
        for journal in base._journals.values():
            journal.close()
        raise


def build_risex_nado_funding_boundary_bridge(
    *,
    hold_release_gate: HoldReleaseGate | None,
    preparation_gate: PreparationGate | None = None,
) -> tuple[RisexCoordinationLoopBridge, RisexTerminalEvidenceProvider]:
    """Create the fixed production RISEx loop and its exact sync provider."""
    bridge = RisexCoordinationLoopBridge(
        lambda: _build_fresh_production_coordinator(
            hold_release_gate, preparation_gate,
        )
    )
    bridge.start()
    return bridge, RisexTerminalEvidenceProvider(bridge)


__all__ = [
    "BoundarySignalKind",
    "FUNDING_BLOCKER_AUTHORITATIVE_INJECTION",
    "FUNDING_BLOCKER_BOUNDARY_GATE_MISSING",
    "FUNDING_BLOCKER_BOUNDARY_INTERRUPTED",
    "FUNDING_BLOCKER_CANCELLED",
    "FUNDING_BLOCKER_ENTRY_AFTER_BOUNDARY",
    "FUNDING_BLOCKER_EVIDENCE_CONTRADICTORY",
    "FUNDING_BLOCKER_EVIDENCE_MISSING",
    "FUNDING_BLOCKER_EVIDENCE_STALE",
    "FUNDING_BLOCKER_GATE_CALLBACK_CANCELLED",
    "FUNDING_BOUNDARY_COUNTERPARTY_JOURNAL",
    "FUNDING_BOUNDARY_PRIMARY_JOURNAL",
    "FundingBoundaryReport",
    "FundingBoundaryResult",
    "FUNDING_BLOCKER_MISSING_CONTRACT",
    "FUNDING_UNRESOLVED",
    "HoldReleaseGate",
    "HoldReleaseSignal",
    "PreparationGate",
    "RisexCoordinationLoopBridge",
    "RisexFundingBoundaryBinding",
    "RisexFundingBoundaryCoordinator",
    "RisexFundingBoundaryError",
    "RisexTerminalEvidenceProvider",
    "TARGET_CANONICAL_ASSET",
    "TARGET_NADO_MARKET",
    "TARGET_NADO_PRODUCT_ID",
    "TARGET_QUANTITY",
    "TARGET_RISEX_MARKET",
    "build_risex_nado_funding_boundary_bridge",
    "fixed_funding_route",
]
