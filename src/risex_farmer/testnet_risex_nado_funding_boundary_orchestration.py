"""Owned startup seam for the fixed RISEx/Nado ETH funding boundary.

This is an operational-only owner.  It creates the fresh venue resources,
obtains Nado's final immutable binding, persists that exact value on RISEx,
and only then starts the two accepted lifecycles.  The module has no normal
startup hook, command-line entrypoint, or venue writer of its own.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import inspect
import threading
import time
from typing import Any, Callable, Protocol

from . import nado_testnet_lifecycle_operational as _nado
from . import testnet_risex_nado_funding_boundary as _risex
from .nado_testnet_lifecycle import (
    FUNDING_APPLIED,
    FundingBoundaryBinding,
    FundingRouteBinding,
    JournalIdentity,
    NADO_VENUE,
    RISEX_VENUE,
)


_STARTUP_FAILURES = frozenset({
    "STARTUP_FAILED",
    "STARTUP_INTERRUPTED",
    "BINDING_MISMATCH",
    "BARRIER_ABORTED",
})
_RECOVERY_FAILURES = frozenset({
    "NADO_FAILED",
    "RISEX_FAILED",
    "ASYMMETRIC_PARTIAL_EXPOSURE",
    "MANUAL_RECOVERY_REQUIRED",
    "CLOSE_FAILED",
})


class FundingBoundaryOrchestrationError(RuntimeError):
    """Sanitized startup or lifecycle-owner failure."""

    def __init__(self, code: str) -> None:
        if code not in _STARTUP_FAILURES | _RECOVERY_FAILURES | {
            "NOT_STARTED", "ALREADY_CLOSED", "TIMEOUT", "INTERRUPTED",
        }:
            raise ValueError("unsupported orchestration failure")
        self.code = code
        super().__init__(code)


class FundingBoundaryOrchestrationTimeout(FundingBoundaryOrchestrationError):
    """The owner wait ended without cancelling either accepted lifecycle."""

    def __init__(self) -> None:
        super().__init__("TIMEOUT")


class _PreparationBarrierAborted(Exception):
    """Private wake-up for a peer whose prewrite barrier was aborted."""


class _PreparationBarrier:
    """One immutable, two-party barrier immediately before durable prepare."""

    def __init__(self, now_ms: Callable[[], int] | None = None) -> None:
        self._condition = threading.Condition()
        if now_ms is not None and not callable(now_ms):
            raise FundingBoundaryOrchestrationError("BARRIER_ABORTED")
        self._now_ms = now_ms
        self._binding: FundingBoundaryBinding | None = None
        self._arrived: set[str] = set()
        self._passed = False
        self._aborted = False

    def bind(
        self,
        binding: FundingBoundaryBinding,
        *,
        now_ms: Callable[[], int] | None = None,
    ) -> None:
        binding.assert_contract()
        if now_ms is not None and not callable(now_ms):
            raise FundingBoundaryOrchestrationError("BARRIER_ABORTED")
        with self._condition:
            if self._binding is not None and self._binding != binding:
                raise FundingBoundaryOrchestrationError("BINDING_MISMATCH")
            if self._arrived or self._passed or self._aborted:
                raise FundingBoundaryOrchestrationError("BINDING_MISMATCH")
            self._binding = binding
            if now_ms is not None:
                self._now_ms = now_ms

    def _assert_before_target(self, binding: FundingBoundaryBinding) -> None:
        if self._now_ms is None:
            raise FundingBoundaryOrchestrationError("BARRIER_ABORTED")
        try:
            now_ms = self._now_ms()
        except BaseException:
            raise FundingBoundaryOrchestrationError("BARRIER_ABORTED") from None
        if (
            type(now_ms) is not int
            or now_ms <= 0
            or now_ms >= binding.route.settlement_at_ms
        ):
            raise FundingBoundaryOrchestrationError("BARRIER_ABORTED")

    def _assert_before_target_or_abort(self, binding: FundingBoundaryBinding) -> None:
        try:
            self._assert_before_target(binding)
        except FundingBoundaryOrchestrationError:
            # A peer may already be blocked in the condition.  Publish the
            # abort while holding the same lock, then wake it before the
            # deterministic rejection reaches the caller.
            self._aborted = True
            self._passed = False
            self._condition.notify_all()
            raise

    def wait(self, side: str, binding: FundingBoundaryBinding) -> None:
        if side not in {"NADO", "RISEX"}:
            raise FundingBoundaryOrchestrationError("BARRIER_ABORTED")
        with self._condition:
            if self._binding is None or self._binding != binding:
                raise FundingBoundaryOrchestrationError("BINDING_MISMATCH")
            if self._passed or side in self._arrived:
                raise FundingBoundaryOrchestrationError("BARRIER_ABORTED")
            if self._aborted:
                raise _PreparationBarrierAborted
            self._assert_before_target_or_abort(binding)
            self._arrived.add(side)
            if len(self._arrived) == 2:
                self._assert_before_target_or_abort(binding)
                self._passed = True
                self._condition.notify_all()
                return
            while not self._passed and not self._aborted:
                self._condition.wait()
            if self._aborted:
                raise _PreparationBarrierAborted

    def abort(self, code: str = "BARRIER_ABORTED") -> None:
        if code not in _STARTUP_FAILURES | _RECOVERY_FAILURES:
            code = "BARRIER_ABORTED"
        with self._condition:
            if self._passed:
                return
            self._aborted = True
            self._condition.notify_all()

    @property
    def passed(self) -> bool:
        with self._condition:
            return self._passed


class _BoundaryProgressionRelay:
    """Nado-sourced, one-shot coordination signal consumed by RISEx."""

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._nado_binding: FundingBoundaryBinding | None = None
        self._risex_binding: _risex.RisexFundingBoundaryBinding | None = None
        self._signal: _risex.HoldReleaseSignal | None = None

    def bind(
        self,
        nado_binding: FundingBoundaryBinding,
        risex_binding: _risex.RisexFundingBoundaryBinding,
    ) -> None:
        nado_binding.assert_contract()
        risex_binding.assert_contract()
        if risex_binding.boundary != nado_binding:
            raise FundingBoundaryOrchestrationError("BINDING_MISMATCH")
        with self._condition:
            if self._nado_binding is not None and (
                self._nado_binding != nado_binding
                or self._risex_binding != risex_binding
            ):
                raise FundingBoundaryOrchestrationError("BINDING_MISMATCH")
            if self._signal is not None:
                raise FundingBoundaryOrchestrationError("BINDING_MISMATCH")
            self._nado_binding = nado_binding
            self._risex_binding = risex_binding

    def _make_signal(self, kind: str, observed_at_ms: int) -> _risex.HoldReleaseSignal:
        nado_binding = self._nado_binding
        risex_binding = self._risex_binding
        if nado_binding is None or risex_binding is None:
            raise FundingBoundaryOrchestrationError("BINDING_MISMATCH")
        if kind not in {
            _risex.BoundarySignalKind.RELEASED.value,
            _risex.BoundarySignalKind.CANCELLED.value,
        }:
            raise FundingBoundaryOrchestrationError("BARRIER_ABORTED")
        if type(observed_at_ms) is not int or observed_at_ms <= 0:
            raise FundingBoundaryOrchestrationError("BARRIER_ABORTED")
        if (
            kind == _risex.BoundarySignalKind.RELEASED.value
            and observed_at_ms < nado_binding.route.settlement_at_ms
        ):
            raise FundingBoundaryOrchestrationError("BARRIER_ABORTED")
        signal_id = (
            "nado-boundary-"
            + risex_binding.identity_digest[:32]
            + "-"
            + kind.lower()
        )
        return _risex.HoldReleaseSignal(
            kind,
            risex_binding.identity_digest,
            nado_binding.route.settlement_at_ms,
            observed_at_ms,
            signal_id,
        )

    def publish(
        self, binding: FundingBoundaryBinding, kind: str, observed_at_ms: int,
    ) -> None:
        with self._condition:
            if self._nado_binding is None or binding != self._nado_binding:
                raise FundingBoundaryOrchestrationError("BINDING_MISMATCH")
            if self._signal is not None:
                raise FundingBoundaryOrchestrationError("BARRIER_ABORTED")
            self._signal = self._make_signal(kind, observed_at_ms)
            self._condition.notify_all()

    def cancel_if_unset(
        self, binding: FundingBoundaryBinding, observed_at_ms: int,
    ) -> None:
        with self._condition:
            if self._nado_binding is None or binding != self._nado_binding:
                raise FundingBoundaryOrchestrationError("BINDING_MISMATCH")
            if self._signal is not None:
                return
            self._signal = self._make_signal(
                _risex.BoundarySignalKind.CANCELLED.value, observed_at_ms,
            )
            self._condition.notify_all()

    def wait(self, binding: _risex.RisexFundingBoundaryBinding) -> object:
        with self._condition:
            if self._risex_binding is None or binding != self._risex_binding:
                raise FundingBoundaryOrchestrationError("BINDING_MISMATCH")
            while self._signal is None:
                self._condition.wait()
            return self._signal

    @property
    def signal(self) -> _risex.HoldReleaseSignal | None:
        with self._condition:
            return self._signal


@dataclass
class _OwnedNado:
    runner: _nado.SealedFundingBoundaryRunner | None
    store: _nado.IntentStore | None
    private_read: Callable[[], object]
    # The production factory defers all SQLite-backed construction to the
    # Nado worker.  Fixture factories may continue to return an already-built
    # runner through the original three-field shape.
    worker_factory: Callable[
        [], tuple[_nado.SealedFundingBoundaryRunner, _nado.IntentStore]
    ] | None = None
    now_ms: Callable[[], int] | None = None


class _BridgeFactory(Protocol):
    def __call__(
        self, *, hold_release_gate: Callable[[object], object],
        preparation_gate: Callable[[FundingBoundaryBinding], object],
    ) -> tuple[object, object]: ...


class _NadoFactory(Protocol):
    def __call__(
        self,
        *,
        route: FundingRouteBinding,
        risex_journal: JournalIdentity,
        risex_attestation_provider: object,
        preparation_gate: Callable[[FundingBoundaryBinding], object],
        boundary_progression_sink: Callable[[FundingBoundaryBinding, str, int], None],
        private_read: Callable[[], object],
    ) -> _OwnedNado: ...


@dataclass(frozen=True)
class FundingBoundaryOrchestrationReport:
    """Bounded owner status, including both venue-local terminal reports."""

    status: str
    terminal: bool
    binding_digest: str | None
    nado_task: str
    risex_task: str
    nado_potential_write: bool
    risex_potential_write: bool
    nado_report: _nado.FundingBoundaryReport | None
    risex_report: _risex.FundingBoundaryReport | None
    reason: str | None = None
    closed: bool = False
    risex_counterparty: dict[str, object] | None = None

    def sanitized(self) -> dict[str, object]:
        counterparty: dict[str, object] | None = None
        if self.risex_report is not None:
            report = self.risex_report.coordinator_report
            counterparty = {
                "run_id": report.counterparty_run_id,
                "intents": report.counterparty_intents,
                "dispatches": report.counterparty_dispatches,
                "cancels": report.counterparty_cancels,
            }
        elif self.risex_counterparty is not None:
            counterparty = dict(self.risex_counterparty)
        else:
            counterparty = {"availability": "UNAVAILABLE"}
        return {
            "schema_version": 1,
            "status": self.status,
            "terminal": self.terminal,
            "binding_digest": self.binding_digest,
            "nado_task": self.nado_task,
            "risex_task": self.risex_task,
            "nado_potential_write": self.nado_potential_write,
            "risex_potential_write": self.risex_potential_write,
            "nado": None if self.nado_report is None else self.nado_report.sanitized(),
            "risex": None if self.risex_report is None else self.risex_report.sanitized(),
            "risex_counterparty": counterparty,
            "reason": self.reason,
            "closed": self.closed,
        }


def _binding_digest(binding: _risex.RisexFundingBoundaryBinding) -> str:
    return binding.identity_digest


def _default_bridge_factory(
    *,
    hold_release_gate: Callable[[object], object],
    preparation_gate: Callable[[FundingBoundaryBinding], object],
) -> tuple[object, object]:
    return _risex.build_risex_nado_funding_boundary_bridge(
        hold_release_gate=hold_release_gate,
        preparation_gate=preparation_gate,
    )


def _default_nado_factory(
    *,
    route: FundingRouteBinding,
    risex_journal: JournalIdentity,
    risex_attestation_provider: object,
    preparation_gate: Callable[[FundingBoundaryBinding], object],
    boundary_progression_sink: Callable[[FundingBoundaryBinding, str, int], None],
    private_read: Callable[[], object],
) -> _OwnedNado:
    if not callable(risex_attestation_provider):
        raise FundingBoundaryOrchestrationError("STARTUP_FAILED")
    owner, sender = _nado._strict_identity()
    path = _nado._funding_boundary_store_path()

    def build() -> tuple[
        _nado.SealedFundingBoundaryRunner, _nado.IntentStore
    ]:
        # This factory is called only by the Nado worker.  In particular, the
        # IntentStore connection, RuntimeRunJournal writes, and the runner's
        # persisted funding binding all acquire their SQLite ownership on that
        # same thread before any observation or preparation is possible.
        _nado._prepare_file(path)
        store: _nado.IntentStore | None = None
        try:
            store = _nado.IntentStore(path)
            if (
                store.funding_boundary_binding() is not None
                or store.intents()
                or store.lifecycle_status() != "RUNNING"
            ):
                raise FundingBoundaryOrchestrationError("STARTUP_FAILED")
            io = _nado.OperationalVenueIO(owner, sender)
            io._enable_funding_boundary_target()
            runner = _nado.SealedFundingBoundaryRunner(
                store=store,
                journal=_nado.RuntimeRunJournal(path),
                io=io,
                capability_loader=_nado._load_capability,
                owner=owner,
                sender=sender,
                route=route,
                risex_journal=risex_journal,
                risex_attestation_provider=risex_attestation_provider,
                preparation_gate=preparation_gate,
                boundary_progression_sink=boundary_progression_sink,
            )
            return runner, store
        except BaseException:
            if store is not None:
                store.close()
            raise

    return _OwnedNado(
        None,
        None,
        private_read,
        worker_factory=build,
        # OperationalVenueIO.now_ms is a wall-clock observation.  Keeping the
        # owner-side barrier clock independent of the worker object avoids a
        # caller-thread method call on the worker-owned runner.
        now_ms=lambda: time.time_ns() // 1_000_000,
    )


class FundingBoundaryOrchestrator:
    """Single owner for one fresh fixed-route RISEx/Nado startup."""

    def __init__(
        self,
        *,
        route: FundingRouteBinding,
        bridge_factory: _BridgeFactory | None = None,
        nado_factory: _NadoFactory | None = None,
        private_read: Callable[[], object] | None = None,
    ) -> None:
        try:
            route.assert_contract()
            expected = _risex.fixed_funding_route(route.settlement_at_ms)
        except Exception:
            raise FundingBoundaryOrchestrationError("BINDING_MISMATCH") from None
        if route != expected:
            raise FundingBoundaryOrchestrationError("BINDING_MISMATCH")
        if bridge_factory is not None and not callable(bridge_factory):
            raise FundingBoundaryOrchestrationError("STARTUP_FAILED")
        if nado_factory is not None and not callable(nado_factory):
            raise FundingBoundaryOrchestrationError("STARTUP_FAILED")
        if private_read is not None and not callable(private_read):
            raise FundingBoundaryOrchestrationError("STARTUP_FAILED")
        self._route = route
        self._bridge_factory = bridge_factory or _default_bridge_factory
        self._nado_factory = nado_factory or _default_nado_factory
        self._private_read = private_read or _nado._accepted_private_read
        self._barrier = _PreparationBarrier()
        self._relay = _BoundaryProgressionRelay()
        self._condition = threading.Condition()
        self._started = False
        self._closed = False
        self._startup_failure: str | None = None
        self._binding: _risex.RisexFundingBoundaryBinding | None = None
        self._bridge: object | None = None
        self._provider: object | None = None
        self._nado: _OwnedNado | None = None
        self._nado_primary_journal: JournalIdentity | None = None
        self._nado_binding: FundingBoundaryBinding | None = None
        self._nado_report: _nado.FundingBoundaryReport | None = None
        self._risex_report: _risex.FundingBoundaryReport | None = None
        self._risex_counterparty: dict[str, object] | None = None
        self._nado_blocker: str | None = None
        self._nado_error: str | None = None
        self._risex_error: str | None = None
        self._nado_potential_write = False
        self._risex_potential_write = False
        self._nado_thread: threading.Thread | None = None
        self._risex_thread: threading.Thread | None = None
        self._nado_worker_owned = False
        self._nado_ready = False
        self._nado_startup_ready = False
        self._nado_startup_abort: str | None = None
        self._nado_run_abort: str | None = None
        self._nado_startup_gate: threading.Event | None = None
        self._nado_run_gate: threading.Event | None = None
        self._nado_close_gate: threading.Event | None = None
        self._nado_resources_closed = False
        self._nado_close_ready = False
        self._nado_persistence_unknown = False
        self._nado_manual_recovery = False
        self._nado_done = False
        self._risex_done = False

    @property
    def nado_runner(self) -> _nado.SealedFundingBoundaryRunner | None:
        return None if self._nado is None else self._nado.runner

    @property
    def risex_bridge(self) -> object | None:
        return self._bridge

    @staticmethod
    def _validate_journals(
        journals: object, route: FundingRouteBinding,
    ) -> tuple[JournalIdentity, JournalIdentity]:
        if (
            type(journals) is not tuple
            or len(journals) != 2
            or any(type(item) is not JournalIdentity for item in journals)
        ):
            raise FundingBoundaryOrchestrationError("BINDING_MISMATCH")
        primary, counterparty = journals
        try:
            primary.assert_contract()
            counterparty.assert_contract()
        except Exception:
            raise FundingBoundaryOrchestrationError("BINDING_MISMATCH") from None
        if (
            primary != JournalIdentity(
                RISEX_VENUE,
                primary.run_id,
                _risex.FUNDING_BOUNDARY_PRIMARY_STORE_IDENTITY,
                primary.account_id,
            )
            or counterparty != JournalIdentity(
                RISEX_VENUE,
                counterparty.run_id,
                _risex.FUNDING_BOUNDARY_COUNTERPARTY_STORE_IDENTITY,
                counterparty.account_id,
            )
            or primary.account_id == counterparty.account_id
            or route.risex_leg.venue != RISEX_VENUE
        ):
            raise FundingBoundaryOrchestrationError("BINDING_MISMATCH")
        return primary, counterparty

    def _validate_nado_binding(
        self,
        owned: _OwnedNado,
        primary_journal: JournalIdentity,
    ) -> FundingBoundaryBinding:
        runner = owned.runner
        binding = getattr(runner, "funding_binding", None)
        store = owned.store
        if (
            runner is None
            or store is None
            or type(binding) is not FundingBoundaryBinding
            or store is not getattr(runner, "store", None)
            or not callable(getattr(runner, "run", None))
            or not callable(getattr(runner, "terminalize", None))
            or not callable(getattr(runner, "cancel_boundary_progression", None))
        ):
            raise FundingBoundaryOrchestrationError("BINDING_MISMATCH")
        try:
            binding.assert_contract()
            if (
                binding.route != self._route
                or binding.risex_journal != primary_journal
                or binding.nado_journal.venue != NADO_VENUE
                or binding.nado_journal.run_id != runner.run_id
                or binding.nado_journal.account_id != runner.sender
                or binding.nado_journal.store_identity
                != _nado._journal_store_identity(runner.journal.path)
                or store.funding_boundary_binding() != binding
                or store.lifecycle_status() != "RUNNING"
                or store.intents()
                or not runner.run_id
                or runner.journal.path != store.path
            ):
                raise ValueError
        except Exception:
            raise FundingBoundaryOrchestrationError("BINDING_MISMATCH") from None
        return binding

    def _wait_nado_ready(self) -> FundingBoundaryBinding:
        with self._condition:
            while not self._nado_ready and not self._nado_done:
                self._condition.wait()
            binding = self._nado_binding
            failure = self._startup_failure
        if binding is not None and self._nado_ready:
            return binding
        raise FundingBoundaryOrchestrationError(failure or "STARTUP_FAILED")

    def _wait_nado_startup(self) -> None:
        with self._condition:
            while not self._nado_startup_ready and not self._nado_done:
                self._condition.wait()
            if self._nado_startup_ready:
                return
            failure = self._startup_failure
        raise FundingBoundaryOrchestrationError(failure or "STARTUP_FAILED")

    @staticmethod
    def _run_private_read(private_read: Callable[[], object]) -> object:
        result = private_read()
        if inspect.isawaitable(result):
            result = asyncio.run(result)
        if type(result) is not dict or result.get("status") != "FINALIZED":
            raise FundingBoundaryOrchestrationError("STARTUP_FAILED")
        return result

    @staticmethod
    def _nado_timestamp(runner: _nado.SealedFundingBoundaryRunner) -> int:
        now_ms = runner.io.now_ms()
        if type(now_ms) is not int or now_ms <= 0:
            raise FundingBoundaryOrchestrationError("BARRIER_ABORTED")
        return now_ms

    @staticmethod
    def _require_nado_prewrite_time(
        runner: _nado.SealedFundingBoundaryRunner,
        settlement_at_ms: int,
    ) -> None:
        now_ms = FundingBoundaryOrchestrator._nado_timestamp(runner)
        if now_ms >= settlement_at_ms:
            raise FundingBoundaryOrchestrationError("STARTUP_FAILED")

    @staticmethod
    def _potential_nado(
        runner: _nado.SealedFundingBoundaryRunner | None,
    ) -> bool:
        if runner is None:
            return False
        try:
            return bool(runner.potential_write)
        except BaseException:
            return True

    def _potential_risex(self) -> bool:
        bridge = self._bridge
        if bridge is None:
            return False
        try:
            value = getattr(bridge, "potential_write")
            return bool(value)
        except BaseException:
            return True

    def _cancel_after_nado_termination(self, runner: _nado.SealedFundingBoundaryRunner) -> None:
        if not self._barrier.passed or self._relay.signal is not None:
            return
        try:
            cancel = getattr(runner, "cancel_boundary_progression", None)
            if not callable(cancel):
                raise FundingBoundaryOrchestrationError("BINDING_MISMATCH")
            cancel()
        except BaseException:
            # A missing cancellation is unsafe; the peer remains owned and
            # the aggregate status will stay manual-recovery rather than close.
            pass

    @staticmethod
    def _worker_startup_code(error: BaseException) -> str:
        code = getattr(error, "code", None)
        return code if code in _STARTUP_FAILURES else "STARTUP_FAILED"

    def _persist_nado_worker_failure(
        self,
        runner: _nado.SealedFundingBoundaryRunner,
        error: BaseException,
    ) -> bool:
        """Persist a sanitized failure while still on the Nado owner thread."""
        try:
            # Keep the accepted venue-local classification and the exact
            # runner stage even for a zero-write startup/observation failure.
            # The caller-facing orchestration code never receives exception
            # text or performs this persistence itself.
            potential = self._potential_nado(runner)
            _nado._persist_runner_failure(runner, error)
            return not potential
        except BaseException:
            # If terminalization itself cannot be persisted, the owner must
            # retain the live resource for manual recovery.  It may not
            # claim a safe pre-write terminal result from an unknown state.
            with self._condition:
                self._nado_error = "NADO_FAILED"
                self._nado_persistence_unknown = True
                self._nado_manual_recovery = True
                self._nado_potential_write = True
                self._condition.notify_all()
            return False

    def _signal_nado_startup_abort(self, code: str) -> None:
        with self._condition:
            self._nado_startup_abort = code
            self._nado_run_abort = code
            startup_gate = self._nado_startup_gate
            run_gate = self._nado_run_gate
            close_gate = self._nado_close_gate
            self._condition.notify_all()
        if startup_gate is not None:
            startup_gate.set()
        if run_gate is not None:
            run_gate.set()
        # Startup failures happen before either lifecycle is released, so a
        # successfully terminalized worker may close only after this explicit
        # owner request.  No caller-thread SQLite close is permitted.
        if close_gate is not None:
            close_gate.set()

    def _nado_worker(self) -> None:
        owned = self._nado
        if owned is None:
            return
        runner = owned.runner
        worker_owned = callable(owned.worker_factory)
        startup_phase = worker_owned
        close_after_owner_request = False
        try:
            if worker_owned:
                assert owned.worker_factory is not None
                built = owned.worker_factory()
                if (
                    type(built) is not tuple
                    or len(built) != 2
                    or not isinstance(built[0], _nado.SealedFundingBoundaryRunner)
                    or not isinstance(built[1], _nado.IntentStore)
                ):
                    raise FundingBoundaryOrchestrationError("STARTUP_FAILED")
                runner, store = built
                owned.runner = runner
                owned.store = store
                with self._condition:
                    primary_journal = self._nado_primary_journal
                if primary_journal is None:
                    raise FundingBoundaryOrchestrationError("BINDING_MISMATCH")
                binding = self._validate_nado_binding(owned, primary_journal)
                with self._condition:
                    self._nado_binding = binding
                    self._nado_ready = True
                    self._condition.notify_all()
                startup_gate = self._nado_startup_gate
                run_gate = self._nado_run_gate
                if startup_gate is None or run_gate is None:
                    raise FundingBoundaryOrchestrationError("STARTUP_FAILED")
                startup_gate.wait()
                with self._condition:
                    startup_abort = self._nado_startup_abort
                if startup_abort is not None:
                    close_after_owner_request = not self._potential_nado(runner)
                    if close_after_owner_request:
                        runner.terminalize("SAFETY", "RUNNER_STARTUP")
                    return
                # This whole preflight gate stays on the Nado owner thread;
                # the caller receives only its immutable success signal.
                runner.stage = "PRIVATE_READ_BARRIER"
                self._run_private_read(owned.private_read)
                note_barrier = getattr(runner.io, "note_private_read_barrier", None)
                if not callable(note_barrier):
                    raise FundingBoundaryOrchestrationError("STARTUP_FAILED")
                note_barrier()
                self._require_nado_prewrite_time(
                    runner, self._route.settlement_at_ms,
                )
                with self._condition:
                    self._nado_startup_ready = True
                    self._condition.notify_all()
                run_gate.wait()
                with self._condition:
                    run_abort = self._nado_run_abort
                if run_abort is not None:
                    close_after_owner_request = not self._potential_nado(runner)
                    if close_after_owner_request:
                        runner.terminalize("SAFETY", "RUNNER_STARTUP")
                    return
                startup_phase = False
            if runner is None:
                raise FundingBoundaryOrchestrationError("NADO_FAILED")
            result = runner.run()
            if type(result) is not _nado.FundingBoundaryReport:
                raise FundingBoundaryOrchestrationError("NADO_FAILED")
            if result.status == "BLOCKED":
                # Match the accepted one-run wrapper: a bounded funding
                # blocker is terminal evidence only after the runtime journal
                # is explicitly closed at the final safety barrier.
                runner.terminalize("SAFETY", "FINAL_BARRIER")
                blocker = getattr(runner.store, "funding_boundary_blocker", None)
                if callable(blocker):
                    value = blocker()
                    if type(value) is str and value:
                        self._nado_blocker = value
                if self._nado_blocker is None and type(result.reason) is str:
                    self._nado_blocker = result.reason
            with self._condition:
                self._nado_report = result
            close_after_owner_request = worker_owned
        except BaseException as error:
            if worker_owned:
                failure_code = self._worker_startup_code(error)
                if runner is None:
                    with self._condition:
                        self._startup_failure = failure_code
                        self._nado_error = "NADO_FAILED"
                        self._condition.notify_all()
                else:
                    terminalized = self._persist_nado_worker_failure(
                        runner, error,
                    )
                    close_after_owner_request = terminalized
                    with self._condition:
                        if startup_phase:
                            self._startup_failure = failure_code
                        self._nado_error = "NADO_FAILED"
                        self._condition.notify_all()
            else:
                self._nado_error = "NADO_FAILED"
                try:
                    _nado._persist_runner_failure(
                        runner, RuntimeError("owned nado failure")
                    )
                except BaseException:
                    pass
        finally:
            if runner is not None and self._barrier.passed:
                self._cancel_after_nado_termination(runner)
            elif runner is not None and not self._potential_nado(runner):
                self._barrier.abort("BARRIER_ABORTED")
            with self._condition:
                # A failed durable terminalization is deliberately sticky:
                # the runner's in-memory potential flag cannot turn an
                # unknown persisted state into a safe pre-write verdict.
                if self._nado_persistence_unknown or self._nado_manual_recovery:
                    self._nado_potential_write = True
                else:
                    self._nado_potential_write = self._potential_nado(runner)
                if (
                    worker_owned
                    and close_after_owner_request
                    and not self._nado_persistence_unknown
                    and not self._nado_manual_recovery
                ):
                    # Report readiness and resource closure are separate
                    # states.  The owner may now request close even when the
                    # terminalized pre-write failure produced no report.
                    self._nado_close_ready = True
                if worker_owned and runner is None:
                    # No SQLite resource was acquired when construction
                    # failed before the worker could build its owner object.
                    self._nado_resources_closed = True
                self._nado_done = True
                self._condition.notify_all()
            if worker_owned and close_after_owner_request:
                close_gate = self._nado_close_gate
                if close_gate is not None:
                    close_gate.wait()
                try:
                    store = owned.store
                    if store is None:
                        raise FundingBoundaryOrchestrationError("CLOSE_FAILED")
                    # This is deliberately the only close of the worker-owned
                    # SQLite connection.
                    store.close()
                except BaseException:
                    with self._condition:
                        self._nado_error = "NADO_FAILED"
                        self._nado_manual_recovery = True
                        self._nado_potential_write = True
                        self._condition.notify_all()
                else:
                    with self._condition:
                        self._nado_resources_closed = True
                        self._condition.notify_all()

    def _risex_worker(self) -> None:
        bridge = self._bridge
        if bridge is None:
            return
        try:
            result = bridge.run_lifecycle()
            if type(result) is not _risex.FundingBoundaryReport:
                raise FundingBoundaryOrchestrationError("RISEX_FAILED")
            with self._condition:
                self._risex_report = result
        except BaseException:
            self._risex_error = "RISEX_FAILED"
        finally:
            potential = self._potential_risex()
            if not self._barrier.passed and not potential:
                self._barrier.abort("BARRIER_ABORTED")
            with self._condition:
                self._risex_potential_write = potential
                self._risex_done = True
                self._condition.notify_all()

    def _startup_cleanup(self, code: str) -> None:
        self._barrier.abort(code)
        with self._condition:
            nado_thread = self._nado_thread
            risex_thread = self._risex_thread
            worker_owned = self._nado_worker_owned
        if worker_owned:
            # The default Nado worker may be waiting at either startup or
            # lifecycle release, or at the post-report close handshake.  Wake
            # it and let that same thread terminalize and close its SQLite
            # connection; the caller never touches worker-owned resources.
            self._signal_nado_startup_abort(code)
            if nado_thread is not None and nado_thread.is_alive():
                nado_thread.join(5.0)
        threads_alive = any(
            thread is not None and thread.is_alive()
            for thread in (nado_thread, risex_thread)
        )
        owned = self._nado
        runner = None if owned is None else owned.runner
        with self._condition:
            nado_resources_closed = self._nado_resources_closed
            nado_potential = (
                self._nado_potential_write
                if worker_owned
                else False if runner is None else self._potential_nado(runner)
            )
        risex_potential = (
            True if threads_alive and risex_thread is not None
            else self._potential_risex()
        )
        if threads_alive:
            with self._condition:
                self._startup_failure = code
                if worker_owned and nado_thread is not None and nado_thread.is_alive():
                    self._nado_manual_recovery = True
                self._nado_potential_write = (
                    True if worker_owned and nado_thread is not None
                    and nado_thread.is_alive() else nado_potential
                )
                self._risex_potential_write = risex_potential
                self._condition.notify_all()
            return
        if worker_owned and not nado_resources_closed:
            # A worker-owned store is never closed by startup cleanup.  If
            # the worker did not positively confirm its own close, retain the
            # resource and expose manual recovery instead of claiming that
            # startup safely unwound.
            with self._condition:
                self._startup_failure = code
                self._nado_manual_recovery = True
                self._nado_potential_write = True
                self._condition.notify_all()
            return
        if runner is not None and not nado_potential and not worker_owned:
            try:
                runner.terminalize("SAFETY", "RUNNER_STARTUP")
            except BaseException:
                pass
        if nado_potential or risex_potential:
            with self._condition:
                self._startup_failure = code
                self._nado_potential_write = nado_potential
                self._risex_potential_write = risex_potential
                self._condition.notify_all()
            return
        bridge = self._bridge
        if bridge is not None:
            try:
                bridge.close()
            except BaseException:
                pass
        if owned is not None and not worker_owned:
            try:
                if owned.store is not None:
                    owned.store.close()
            except BaseException:
                pass
        with self._condition:
            self._startup_failure = code
            self._closed = True
            self._nado_done = True
            self._risex_done = True
            self._condition.notify_all()

    def start(self) -> None:
        with self._condition:
            if self._closed:
                raise FundingBoundaryOrchestrationError("ALREADY_CLOSED")
            if self._started:
                return
            self._started = True
        try:
            bridge, provider = self._bridge_factory(
                hold_release_gate=self._relay.wait,
                preparation_gate=lambda binding: self._barrier.wait("RISEX", binding),
            )
            if not callable(getattr(bridge, "start", None)):
                raise FundingBoundaryOrchestrationError("STARTUP_FAILED")
            with self._condition:
                self._bridge = bridge
                self._provider = provider
            if not callable(provider):
                raise FundingBoundaryOrchestrationError("STARTUP_FAILED")
            bridge.start()
            journals = self._validate_journals(
                bridge.journal_identities(), self._route,
            )
            self._risex_counterparty = {
                "availability": "BOUND",
                "run_id": journals[1].run_id,
                "store_identity": journals[1].store_identity,
                "account_id": journals[1].account_id,
                "intents": None,
                "dispatches": None,
                "cancels": None,
            }
            owned = self._nado_factory(
                route=self._route,
                risex_journal=journals[0],
                risex_attestation_provider=provider,
                preparation_gate=lambda binding: self._barrier.wait("NADO", binding),
                boundary_progression_sink=self._relay.publish,
                private_read=self._private_read,
            )
            if not isinstance(owned, _OwnedNado):
                raise FundingBoundaryOrchestrationError("STARTUP_FAILED")
            with self._condition:
                self._nado = owned
                self._nado_primary_journal = journals[0]
                self._nado_worker_owned = callable(owned.worker_factory)
            if self._nado_worker_owned:
                self._nado_startup_gate = threading.Event()
                self._nado_run_gate = threading.Event()
                self._nado_close_gate = threading.Event()
                with self._condition:
                    self._nado_thread = threading.Thread(
                        target=self._nado_worker,
                        name="nado-funding-boundary-owned",
                        daemon=True,
                    )
                assert self._nado_thread is not None
                self._nado_thread.start()
                nado_binding = self._wait_nado_ready()
            else:
                nado_binding = self._validate_nado_binding(owned, journals[0])
            risex_binding = bridge.bind_funding_boundary(nado_binding)
            if type(risex_binding) is not _risex.RisexFundingBoundaryBinding:
                raise FundingBoundaryOrchestrationError("BINDING_MISMATCH")
            if (
                risex_binding.boundary != nado_binding
                or risex_binding.primary_journal != journals[0]
                or risex_binding.counterparty_journal != journals[1]
            ):
                raise FundingBoundaryOrchestrationError("BINDING_MISMATCH")
            self._relay.bind(nado_binding, risex_binding)
            if self._nado_worker_owned:
                if not callable(owned.now_ms):
                    raise FundingBoundaryOrchestrationError("STARTUP_FAILED")
                barrier_now_ms = owned.now_ms
            else:
                runner = owned.runner
                if runner is None:
                    raise FundingBoundaryOrchestrationError("BINDING_MISMATCH")
                barrier_now_ms = runner.io.now_ms
            self._barrier.bind(nado_binding, now_ms=barrier_now_ms)
            if self._nado_worker_owned:
                startup_gate = self._nado_startup_gate
                if startup_gate is None:
                    raise FundingBoundaryOrchestrationError("STARTUP_FAILED")
                # Release only the worker's accepted private-read/prewrite
                # gate.  It will publish readiness before either lifecycle is
                # released to run.
                startup_gate.set()
                self._wait_nado_startup()
            else:
                # Complete the accepted authenticated Nado read barrier before
                # either lifecycle can take its first observation.  This keeps
                # a RISEx price observation from aging while Nado is still
                # pacing its private-read gateway window.
                runner = owned.runner
                if runner is None:
                    raise FundingBoundaryOrchestrationError("BINDING_MISMATCH")
                runner.stage = "PRIVATE_READ_BARRIER"
                self._run_private_read(owned.private_read)
                note_barrier = getattr(runner.io, "note_private_read_barrier", None)
                if not callable(note_barrier):
                    raise FundingBoundaryOrchestrationError("STARTUP_FAILED")
                note_barrier()
                # Do not start either lifecycle once the exact target has
                # arrived.  This is a strict boundary check only; it adds no
                # lead-time rule.
                self._require_nado_prewrite_time(
                    runner, self._route.settlement_at_ms,
                )
            with self._condition:
                self._binding = risex_binding
                if not self._nado_worker_owned:
                    self._nado_thread = threading.Thread(
                        target=self._nado_worker,
                        name="nado-funding-boundary-owned",
                        daemon=True,
                    )
                self._risex_thread = threading.Thread(
                    target=self._risex_worker,
                    name="risex-funding-boundary-owned",
                    daemon=True,
                )
            # Launch RISEx first, then Nado.  Neither can cross its one-shot
            # prewrite gate until both lifecycles are present at the barrier.
            assert self._risex_thread is not None
            assert self._nado_thread is not None
            self._risex_thread.start()
            if self._nado_worker_owned:
                run_gate = self._nado_run_gate
                if run_gate is None:
                    raise FundingBoundaryOrchestrationError("STARTUP_FAILED")
                run_gate.set()
            else:
                self._nado_thread.start()
        except KeyboardInterrupt:
            self._startup_cleanup("STARTUP_INTERRUPTED")
            raise FundingBoundaryOrchestrationError("STARTUP_INTERRUPTED") from None
        except FundingBoundaryOrchestrationError as error:
            self._startup_cleanup(error.code)
            raise
        except BaseException:
            self._startup_cleanup("STARTUP_FAILED")
            raise FundingBoundaryOrchestrationError("STARTUP_FAILED") from None

    def _snapshot_locked(self) -> FundingBoundaryOrchestrationReport:
        nado_task = (
            "DONE" if self._nado_done else "RUNNING" if self._started else "NOT_STARTED"
        )
        risex_task = (
            "DONE" if self._risex_done else "RUNNING" if self._started else "NOT_STARTED"
        )
        if not (self._nado_done and self._risex_done):
            status = "RUNNING" if self._started else "NOT_STARTED"
            terminal = False
        elif self._nado_persistence_unknown or self._nado_manual_recovery:
            # Manual-recovery state dominates all in-memory runner flags and
            # any partial report.  In particular, finally cannot turn a
            # failed durable terminalization into BLOCKED_BEFORE_WRITE.
            status = "FAILED_HALTED_MANUAL_RECOVERY"
            terminal = False
        elif self._safe_terminal_locked():
            status = "COMPLETE"
            terminal = True
        elif self._terminal_evidence_locked():
            # Both venue-local lifecycles are terminal and flat, but at least
            # one funding contract is unresolved.  This is a safe terminal
            # BLOCKED verdict, not an aggregate completion.
            status = "BLOCKED"
            terminal = True
        elif self._nado_potential_write or self._risex_potential_write:
            status = "FAILED_HALTED_MANUAL_RECOVERY"
            terminal = False
        else:
            status = "BLOCKED_BEFORE_WRITE"
            terminal = True
        reason = self._reason_locked(status)
        return FundingBoundaryOrchestrationReport(
            status=status,
            terminal=terminal,
            binding_digest=None if self._binding is None else _binding_digest(self._binding),
            nado_task=nado_task,
            risex_task=risex_task,
            nado_potential_write=self._nado_potential_write,
            risex_potential_write=self._risex_potential_write,
            nado_report=self._nado_report,
            risex_report=self._risex_report,
            reason=reason,
            closed=self._closed,
            risex_counterparty=self._risex_counterparty,
        )

    def _safe_terminal_locked(self) -> bool:
        nado = self._nado_report
        risex = self._risex_report
        if nado is None or risex is None:
            return False
        return (
            self._terminal_evidence_locked()
            and nado.status == "COMPLETE"
            and nado.funding_status == FUNDING_APPLIED
            and risex.coordinator_report.result is _risex.CoordinatorResult.COMPLETE
            and risex.result is _risex.FundingBoundaryResult.COMPLETE
            and risex.funding_status != _risex.FUNDING_UNRESOLVED
            and risex.funding_blocker is None
        )

    def _terminal_evidence_locked(self) -> bool:
        nado = self._nado_report
        risex = self._risex_report
        if nado is None or risex is None:
            return False
        return (
            nado.status in {"COMPLETE", "BLOCKED"}
            and nado.final_rounds_agree
            and nado.final_zero_regular
            and nado.final_zero_trigger
            and nado.final_exact_flat
            and risex.coordinator_report.result is _risex.CoordinatorResult.COMPLETE
            and risex.coordinator_report.phase is _risex.Phase.COMPLETE
            and (
                risex.result is _risex.FundingBoundaryResult.COMPLETE
                or (
                    risex.result is _risex.FundingBoundaryResult.BLOCKED
                    and risex.funding_status == _risex.FUNDING_UNRESOLVED
                    and risex.funding_blocker is not None
                )
            )
        )

    def _reason_locked(self, status: str) -> str | None:
        if status == "COMPLETE":
            return None
        if self._startup_failure is not None:
            return self._startup_failure
        if status == "BLOCKED" and self._terminal_evidence_locked():
            if self._risex_report is not None and self._risex_report.funding_blocker:
                return self._risex_report.funding_blocker
            if self._nado_blocker:
                return self._nado_blocker
            if self._nado_report is not None and self._nado_report.reason:
                return self._nado_report.reason
            return "FUNDING_UNRESOLVED"
        if status == "FAILED_HALTED_MANUAL_RECOVERY":
            if (
                (self._nado_error is not None and self._risex_potential_write)
                or (self._risex_error is not None and self._nado_potential_write)
            ):
                return "ASYMMETRIC_PARTIAL_EXPOSURE"
            return "MANUAL_RECOVERY_REQUIRED"
        return self._nado_error or self._risex_error or "BARRIER_ABORTED"

    def status(self) -> FundingBoundaryOrchestrationReport:
        with self._condition:
            return self._snapshot_locked()

    @staticmethod
    def _validate_timeout(timeout_seconds: float) -> float:
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or timeout_seconds <= 0
            or not float(timeout_seconds) < float("inf")
        ):
            raise FundingBoundaryOrchestrationError("TIMEOUT")
        return float(timeout_seconds)

    def retrieve(self, *, timeout_seconds: float = 900.0) -> FundingBoundaryOrchestrationReport:
        timeout = self._validate_timeout(timeout_seconds)
        with self._condition:
            if not self._started:
                raise FundingBoundaryOrchestrationError("NOT_STARTED")
            deadline = time.monotonic() + timeout
            while not (self._nado_done and self._risex_done):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise FundingBoundaryOrchestrationTimeout
                self._condition.wait(remaining)
            report = self._snapshot_locked()
        if report.terminal and not report.closed:
            self.close()
            report = self.status()
        return report

    def run(self, *, timeout_seconds: float = 900.0) -> FundingBoundaryOrchestrationReport:
        self.start()
        try:
            return self.retrieve(timeout_seconds=timeout_seconds)
        except KeyboardInterrupt:
            if not self._barrier.passed:
                self._barrier.abort("BARRIER_ABORTED")
            raise FundingBoundaryOrchestrationError("INTERRUPTED") from None

    def close(self) -> None:
        with self._condition:
            if self._closed:
                return
            if not self._started:
                self._closed = True
                return
            report = self._snapshot_locked()
            if not report.terminal:
                raise FundingBoundaryOrchestrationError("MANUAL_RECOVERY_REQUIRED")
            worker_owned = self._nado_worker_owned
            if worker_owned and not self._nado_close_ready:
                raise FundingBoundaryOrchestrationError("MANUAL_RECOVERY_REQUIRED")
            nado_alive = (
                self._nado_thread is not None
                and self._nado_thread.is_alive()
            )
            risex_alive = (
                self._risex_thread is not None
                and self._risex_thread.is_alive()
            )
            if risex_alive or (
                nado_alive
                and not (
                    worker_owned
                    and self._nado_done
                )
            ):
                raise FundingBoundaryOrchestrationError("MANUAL_RECOVERY_REQUIRED")
            bridge = self._bridge
            owned = self._nado
        if bridge is not None:
            try:
                bridge.close()
            except BaseException:
                with self._condition:
                    self._nado_manual_recovery = True
                    self._risex_potential_write = True
                    self._condition.notify_all()
                raise FundingBoundaryOrchestrationError("CLOSE_FAILED") from None
        if worker_owned:
            close_gate = self._nado_close_gate
            if close_gate is None:
                with self._condition:
                    self._nado_manual_recovery = True
                    self._nado_potential_write = True
                    self._condition.notify_all()
                raise FundingBoundaryOrchestrationError("CLOSE_FAILED")
            close_gate.set()
            nado_thread = self._nado_thread
            if nado_thread is not None and nado_thread.is_alive():
                nado_thread.join(5.0)
            with self._condition:
                if not self._nado_resources_closed:
                    self._nado_manual_recovery = True
                    self._nado_potential_write = True
                    self._condition.notify_all()
                    raise FundingBoundaryOrchestrationError("CLOSE_FAILED")
        if owned is not None and not worker_owned:
            try:
                if owned.store is not None:
                    owned.store.close()
            except BaseException:
                with self._condition:
                    self._nado_manual_recovery = True
                    self._nado_potential_write = True
                    self._condition.notify_all()
                raise FundingBoundaryOrchestrationError("CLOSE_FAILED") from None
        with self._condition:
            self._closed = True
            self._condition.notify_all()


__all__ = [
    "FundingBoundaryOrchestrationError",
    "FundingBoundaryOrchestrationReport",
    "FundingBoundaryOrchestrationTimeout",
    "FundingBoundaryOrchestrator",
]
