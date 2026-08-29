from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
import sqlite3
import threading
import time

import pytest

from risex_farmer import nado_testnet_lifecycle_operational as _nado
from risex_farmer import testnet_risex_nado_funding_boundary as _risex
from risex_farmer import testnet_risex_nado_funding_boundary_orchestration as _owner
from risex_farmer.nado_testnet_lifecycle import (
    FundingBoundaryBinding,
    JournalIdentity,
    NADO_VENUE,
    RISEX_VENUE,
)


SETTLEMENT = 1_700_000_000_000
PRIMARY_ACCOUNT = "risex-primary-account"
COUNTERPARTY_ACCOUNT = "risex-counterparty-account"
NADO_ACCOUNT = "nado-sender-account"


@dataclass
class _Config:
    nado_failure: str | None = None
    risex_failure: str | None = None
    wait_for_release: bool = False
    delay_nado_baseline: bool = False
    nado_now_ms: int | None = None
    nado_status: str = "BLOCKED"
    risex_result: _risex.FundingBoundaryResult = _risex.FundingBoundaryResult.BLOCKED
    risex_funding_status: str = _risex.FUNDING_UNRESOLVED
    risex_blocker: str | None = "RISEX_APPLIED_FUNDING_CONTRACT_MISSING"
    bind_failure: bool = False


class _FakeStore:
    def __init__(self, binding: FundingBoundaryBinding, path: Path) -> None:
        self._binding = binding
        self.path = path
        self.lifecycle = "RUNNING"
        self.blocker = _nado.FUNDING_BLOCKED_MISSING
        self.closed = False

    def funding_boundary_binding(self) -> FundingBoundaryBinding:
        return self._binding

    def lifecycle_status(self) -> str:
        return self.lifecycle

    def intents(self) -> tuple[object, ...]:
        return ()

    def halt(self) -> None:
        self.lifecycle = "HALTED"

    def funding_boundary_blocker(self) -> str:
        return self.blocker

    def close(self) -> None:
        self.closed = True


class _FakeNadoIO:
    def __init__(self, timestamp: int) -> None:
        self.timestamp = timestamp
        self.private_barrier_noted = False

    def now_ms(self) -> int:
        return self.timestamp

    def note_private_read_barrier(self) -> None:
        self.private_barrier_noted = True


class _FakeNadoRunner:
    def __init__(
        self,
        *,
        binding: FundingBoundaryBinding,
        path: Path,
        preparation_gate,
        progression_sink,
        config: _Config,
        release_event: threading.Event,
        baseline_event: threading.Event,
        log: list[str],
    ) -> None:
        self.funding_binding = binding
        self.store = _FakeStore(binding, path)
        self.journal = SimpleNamespace(path=path)
        self.sender = binding.nado_journal.account_id
        self.run_id = binding.nado_journal.run_id
        self.io = _FakeNadoIO(
            binding.route.settlement_at_ms - 1
            if config.nado_now_ms is None else config.nado_now_ms
        )
        self._preparation_gate = preparation_gate
        self._progression_sink = progression_sink
        self._config = config
        self._release_event = release_event
        self._baseline_event = baseline_event
        self._log = log
        self.stage = "RUNNER_STARTUP"
        self.potential_write = False
        self.prepare_count = 0
        self.dispatch_count = 0
        self.terminalizations: list[tuple[str, str]] = []

    def terminalize(self, failure_class: str, stage: str | None = None) -> None:
        self.terminalizations.append((failure_class, stage or self.stage))
        self.store.halt()

    def cancel_boundary_progression(self) -> None:
        self._progression_sink(
            self.funding_binding,
            _nado.BOUNDARY_CANCELLED,
            self.io.now_ms(),
        )

    def run(self) -> _nado.FundingBoundaryReport:
        if self._config.delay_nado_baseline:
            self._log.append("nado_baseline_started")
            self._baseline_event.wait()
            self._log.append("nado_baseline_finalized")
        self._log.append("nado_gate")
        self._preparation_gate(self.funding_binding)
        self._log.append("nado_gate_passed")
        self.prepare_count += 1
        self._log.append("nado_prepare")
        if self._config.nado_failure == "after_barrier":
            self.potential_write = True
            raise RuntimeError("fixture nado failure")
        if self._config.nado_failure == "after_barrier_before_write":
            raise RuntimeError("fixture nado preparation failure")
        self.potential_write = True
        self.dispatch_count += 1
        self._log.append("nado_dispatch")
        if self._config.wait_for_release:
            self._release_event.wait()
        self._progression_sink(
            self.funding_binding,
            _nado.BOUNDARY_RELEASED,
            self.funding_binding.route.settlement_at_ms + 10,
        )
        return _nado.FundingBoundaryReport(
            1,
            self._config.nado_status,
            "nado-owned-run",
            2,
            1,
            "APPLIED" if self._config.nado_status == "COMPLETE" else _nado.FUNDING_UNRESOLVED,
            1 if self._config.nado_status == "COMPLETE" else None,
            1 if self._config.nado_status == "COMPLETE" else None,
            True,
            True,
            True,
            True,
            None if self._config.nado_status == "COMPLETE" else _nado.FUNDING_BLOCKED_MISSING,
        )


class _FakeBridge:
    def __init__(self, *, preparation_gate, hold_release_gate, config: _Config, log: list[str]) -> None:
        self._preparation_gate = preparation_gate
        self._hold_release_gate = hold_release_gate
        self._config = config
        self._log = log
        self.primary = JournalIdentity(
            RISEX_VENUE, "risex-primary-run", _risex.FUNDING_BOUNDARY_PRIMARY_STORE_IDENTITY,
            PRIMARY_ACCOUNT,
        )
        self.counterparty = JournalIdentity(
            RISEX_VENUE, "risex-counterparty-run", _risex.FUNDING_BOUNDARY_COUNTERPARTY_STORE_IDENTITY,
            COUNTERPARTY_ACCOUNT,
        )
        self.local_binding: _risex.RisexFundingBoundaryBinding | None = None
        self.preparation_bindings: list[FundingBoundaryBinding] = []
        self.signal: object | None = None
        self.started = False
        self.closed = False
        self.potential_write = False
        self.observation_count = 0
        self.prepare_count = 0
        self.dispatch_count = 0

    def start(self) -> None:
        self.started = True

    def journal_identities(self) -> tuple[JournalIdentity, JournalIdentity]:
        return self.primary, self.counterparty

    def bind_funding_boundary(
        self, boundary: FundingBoundaryBinding,
    ) -> _risex.RisexFundingBoundaryBinding:
        if self._config.bind_failure:
            raise _owner.FundingBoundaryOrchestrationError("BINDING_MISMATCH")
        self.local_binding = _risex.RisexFundingBoundaryBinding(
            boundary, self.primary, self.counterparty,
        )
        return self.local_binding

    def run_lifecycle(self) -> _risex.FundingBoundaryReport:
        if self.local_binding is None:
            raise RuntimeError("unbound fixture bridge")
        self._log.append("risex_gate")
        self._preparation_gate(self.local_binding.boundary)
        self._log.append("risex_gate_passed")
        self.observation_count += 1
        self._log.append("risex_observation")
        self._log.append("risex_price_validation")
        self.preparation_bindings.append(self.local_binding.boundary)
        self.prepare_count += 1
        self._log.append("risex_prepare")
        if self._config.risex_failure == "before_dispatch":
            raise RuntimeError("fixture RISEx preparation failure")
        self.potential_write = True
        self.dispatch_count += 1
        self._log.append("risex_dispatch")
        if self._config.risex_failure == "after_dispatch":
            raise RuntimeError("fixture RISEx dispatch failure")
        self.signal = self._hold_release_gate(self.local_binding)
        if getattr(self.signal, "kind", None) == _risex.BoundarySignalKind.CANCELLED.value:
            return _risex_report(
                result=_risex.FundingBoundaryResult.BLOCKED,
                funding_status=_risex.FUNDING_UNRESOLVED,
                blocker=_risex.FUNDING_BLOCKER_CANCELLED,
                complete=True,
            )
        return _risex_report(
            result=self._config.risex_result,
            funding_status=self._config.risex_funding_status,
            blocker=self._config.risex_blocker,
            complete=True,
        )

    def close(self) -> None:
        self.closed = True


def _risex_report(
    *,
    result: _risex.FundingBoundaryResult,
    funding_status: str,
    blocker: str | None,
    complete: bool,
) -> _risex.FundingBoundaryReport:
    coordinator = _risex.CoordinatorReport(
        "risex-owned-run",
        "risex-primary-run",
        "risex-counterparty-run",
        _risex.CoordinatorResult.COMPLETE if complete else _risex.CoordinatorResult.BLOCKED_BEFORE_WRITE,
        _risex.Phase.COMPLETE if complete else _risex.Phase.HALTED,
        4 if complete else 0,
        4 if complete else 0,
        4 if complete else 0,
        4 if complete else 0,
        0,
        2 if complete else 0,
        None,
    )
    return _risex.FundingBoundaryReport(
        result, coordinator, funding_status, blocker, 1,
    )


def _fixture(
    tmp_path: Path,
    *,
    config: _Config | None = None,
    private_read=None,
):
    config = config or _Config()
    log: list[str] = []
    release_event = threading.Event()
    baseline_event = threading.Event()
    holder: dict[str, object] = {}
    holder["baseline_event"] = baseline_event

    def bridge_factory(*, hold_release_gate, preparation_gate):
        bridge = _FakeBridge(
            preparation_gate=preparation_gate,
            hold_release_gate=hold_release_gate,
            config=config,
            log=log,
        )
        holder["bridge"] = bridge
        return bridge, lambda *args, **kwargs: None

    def nado_factory(
        *,
        route,
        risex_journal,
        risex_attestation_provider,
        preparation_gate,
        boundary_progression_sink,
        private_read,
    ):
        del risex_attestation_provider
        path = tmp_path / "fresh-nado.sqlite3"
        nado_journal = JournalIdentity(
            NADO_VENUE,
            "nado-owned-run",
            _nado._journal_store_identity(path),
            NADO_ACCOUNT,
        )
        binding = FundingBoundaryBinding(route, risex_journal, nado_journal)
        runner = _FakeNadoRunner(
            binding=binding,
            path=path,
            preparation_gate=preparation_gate,
            progression_sink=boundary_progression_sink,
            config=config,
            release_event=release_event,
            baseline_event=baseline_event,
            log=log,
        )
        holder["nado"] = runner
        return _owner._OwnedNado(runner, runner.store, private_read)

    def read_value():
        result = private_read() if private_read is not None else {"status": "FINALIZED"}
        if type(result) is dict and result.get("status") == "FINALIZED":
            log.append("private_read_finalized")
        return result

    orchestrator = _owner.FundingBoundaryOrchestrator(
        route=_risex.fixed_funding_route(SETTLEMENT),
        bridge_factory=bridge_factory,
        nado_factory=nado_factory,
        private_read=read_value,
    )
    return orchestrator, holder, log, release_event


def _read_ok() -> dict[str, str]:
    return {"status": "FINALIZED"}


REAL_NADO_OWNER = "0x" + ("11" * 20)
REAL_NADO_SENDER = _nado.encode_subaccount(REAL_NADO_OWNER, _nado.SUBACCOUNT_NAME)


class _RealNadoIO:
    """Small no-network adapter used by the worker-owned SQLite regressions."""

    def __init__(self, *, timestamp: int, observe_error: str | None = None) -> None:
        self.timestamp = timestamp
        self.observe_error = observe_error
        self.observe_thread_id: int | None = None
        self.private_barrier_noted = False

    def _enable_funding_boundary_target(self) -> None:
        return None

    def now_ms(self) -> int:
        return self.timestamp

    def note_private_read_barrier(self) -> None:
        self.private_barrier_noted = True

    def observe(self, digests: tuple[str, ...]):
        del digests
        self.observe_thread_id = threading.get_ident()
        if self.observe_error is None:
            raise AssertionError("the normal ownership regression must not observe")
        raise _nado.OperationalSafetyError(self.observe_error)

    def capture_funding_baseline(self, *args, **kwargs):
        raise AssertionError("unexpected funding baseline access")

    def capture_funding_exposure(self, *args, **kwargs):
        raise AssertionError("unexpected funding exposure access")

    def await_funding_boundary(self, *args, **kwargs):
        raise AssertionError("unexpected funding wait")

    def read_funding_boundary(self, *args, **kwargs):
        raise AssertionError("unexpected funding read")


class _NoNetworkRisexVenue:
    """Actual-coordinator fixture that fails the test if lifecycle IO runs."""

    def __init__(self) -> None:
        self.observe_calls = 0
        self.rest_round_calls = 0
        self.place_calls: list[object] = []
        self.cancel_calls: list[object] = []
        self.closed = False

    async def observe(self):
        self.observe_calls += 1
        raise AssertionError("unexpected RISEx observation")

    async def rest_round(self):
        self.rest_round_calls += 1
        raise AssertionError("unexpected RISEx REST round")

    async def place(self, role, request):
        self.place_calls.append((role, request))
        raise AssertionError("unexpected RISEx place")

    async def cancel(self, role, request):
        self.cancel_calls.append((role, request))
        raise AssertionError("unexpected RISEx cancel")

    async def close(self) -> None:
        self.closed = True


def _real_worker_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    observe_error: str | None = None,
    private_read=None,
    fake_run: bool = False,
    hold_risex_after_report: bool = False,
    fail_terminalize: bool = False,
):
    settlement = time.time_ns() // 1_000_000 + 60_000
    path = tmp_path / "worker-owned-nado.sqlite3"
    io = _RealNadoIO(timestamp=settlement - 1, observe_error=observe_error)
    monkeypatch.setattr(
        _nado, "_strict_identity", lambda: (REAL_NADO_OWNER, REAL_NADO_SENDER),
    )
    monkeypatch.setattr(_nado, "_funding_boundary_store_path", lambda: path)
    monkeypatch.setattr(_nado, "OperationalVenueIO", lambda owner, sender: io)

    config = _Config()
    log: list[str] = []
    holder: dict[str, object] = {}
    release_risex = threading.Event()
    risex_report_ready = threading.Event()
    holder["release_risex"] = release_risex
    holder["risex_report_ready"] = risex_report_ready

    def bridge_factory(*, hold_release_gate, preparation_gate):
        bridge = _FakeBridge(
            preparation_gate=preparation_gate,
            hold_release_gate=hold_release_gate,
            config=config,
            log=log,
        )
        holder["bridge"] = bridge
        if hold_risex_after_report:
            original_run = bridge.run_lifecycle

            def delayed_run():
                result = original_run()
                risex_report_ready.set()
                release_risex.wait(2)
                return result

            bridge.run_lifecycle = delayed_run
        return bridge, lambda *args, **kwargs: None

    def nado_factory(**kwargs):
        owned = _owner._default_nado_factory(**kwargs)
        holder["factory_runner_before_worker"] = owned.runner
        build = owned.worker_factory
        assert build is not None
        if fake_run or fail_terminalize:
            def worker_build():
                runner, store = build()
                if fake_run:
                    def bounded_fake_run():
                        runner._preparation_gate(runner.funding_binding)
                        io.timestamp = settlement + 10
                        runner._emit_boundary_progression(_nado.BOUNDARY_RELEASED)
                        return _nado.FundingBoundaryReport(
                            1, "BLOCKED", "worker-owned-test", 0, 0,
                            _nado.FUNDING_UNRESOLVED, None, None,
                            True, True, True, True, "TEST_BLOCKER",
                        )

                    runner.run = bounded_fake_run
                if fail_terminalize:
                    def failed_terminalize(*args, **kwargs):
                        del args, kwargs
                        raise RuntimeError("terminal persistence outage")

                    runner.terminalize = failed_terminalize
                return runner, store

            owned.worker_factory = worker_build
        return owned

    orchestrator = _owner.FundingBoundaryOrchestrator(
        route=_risex.fixed_funding_route(settlement),
        bridge_factory=bridge_factory,
        nado_factory=nado_factory,
        private_read=_read_ok if private_read is None else private_read,
    )
    return orchestrator, holder, path, io, settlement


def _track_worker_calls(monkeypatch: pytest.MonkeyPatch):
    calls: dict[str, list[int]] = {}

    def track(cls, name: str) -> None:
        original = getattr(cls, name)
        calls[name] = []

        def wrapped(instance, *args, **kwargs):
            calls[name].append(threading.get_ident())
            return original(instance, *args, **kwargs)

        monkeypatch.setattr(cls, name, wrapped)

    for name in (
        "funding_boundary_binding", "intents", "lifecycle_status", "halt", "close",
    ):
        track(_nado.IntentStore, name)
    for name in ("begin", "terminalize"):
        track(_nado.RuntimeRunJournal, name)
    return calls


def _read_worker_database(path: Path) -> dict[str, object]:
    connection = sqlite3.connect(path)
    try:
        runtime = connection.execute(
            "SELECT state, failure_class, stage FROM nado_runtime_runs"
        ).fetchone()
        lifecycle = connection.execute(
            "SELECT status FROM nado_lifecycle_state WHERE singleton = 1"
        ).fetchone()
        intents = connection.execute("SELECT COUNT(*) FROM nado_intents").fetchone()
        integrity = connection.execute("PRAGMA integrity_check").fetchone()
    finally:
        connection.close()
    return {
        "runtime": runtime,
        "lifecycle": None if lifecycle is None else lifecycle[0],
        "intents": None if intents is None else intents[0],
        "integrity": None if integrity is None else integrity[0],
        "bytes": path.read_bytes(),
    }


def _wait_for(predicate, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError("worker regression did not reach its expected state")
        time.sleep(0.001)


def test_real_sqlite_worker_owned_normal_access_and_close_handshake(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _track_worker_calls(monkeypatch)
    orchestrator, holder, path, _io, _settlement = _real_worker_fixture(
        tmp_path, monkeypatch, fake_run=True, hold_risex_after_report=True,
    )

    orchestrator.start()
    assert holder["factory_runner_before_worker"] is None
    assert holder["risex_report_ready"].wait(1)
    _wait_for(lambda: orchestrator._nado_done)
    running = orchestrator.status()
    assert running.status == "RUNNING"
    assert running.nado_task == "DONE"
    assert running.risex_task == "RUNNING"
    assert orchestrator._nado_close_ready is True
    assert orchestrator._nado_resources_closed is False
    assert holder["bridge"].closed is False
    with pytest.raises(_owner.FundingBoundaryOrchestrationError) as error:
        orchestrator.close()
    assert error.value.code == "MANUAL_RECOVERY_REQUIRED"
    assert orchestrator._nado_resources_closed is False

    holder["release_risex"].set()
    report = orchestrator.retrieve(timeout_seconds=1)

    assert report.status == "BLOCKED"
    assert report.terminal is True
    assert report.closed is True
    assert orchestrator._nado_resources_closed is True
    assert holder["bridge"].closed is True
    worker_ids = set(calls["begin"])
    assert len(worker_ids) == 1
    worker_id = next(iter(worker_ids))
    assert worker_id != threading.get_ident()
    for name in (
        "funding_boundary_binding", "intents", "lifecycle_status", "halt", "close",
        "terminalize",
    ):
        assert calls[name]
        assert set(calls[name]) == {worker_id}
    database = _read_worker_database(path)
    assert database["runtime"] == ("BLOCKED", "SAFETY", "FINAL_BARRIER")
    assert database["lifecycle"] == "HALTED"
    assert database["intents"] == 0
    assert database["integrity"] == "ok"


def test_actual_barrier_binding_reaches_both_sides_once_before_observation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The accepted components pass the canonical owner barrier exactly once."""
    settlement = time.time_ns() // 1_000_000 + 60_000
    nado_path = tmp_path / "actual-nado.sqlite3"
    nado_io = _RealNadoIO(timestamp=settlement - 1)
    risex_venues: list[_NoNetworkRisexVenue] = []
    nado_barrier_bindings: list[FundingBoundaryBinding] = []
    risex_barrier_bindings: list[FundingBoundaryBinding] = []
    nado_barrier_errors: list[BaseException] = []
    risex_barrier_errors: list[BaseException] = []
    nado_errors: list[BaseException] = []
    risex_errors: list[BaseException] = []
    lifecycle_log: list[str] = []
    holder: dict[str, object] = {}
    monkeypatch.setattr(
        _nado, "_strict_identity", lambda: (REAL_NADO_OWNER, REAL_NADO_SENDER),
    )
    monkeypatch.setattr(_nado, "_funding_boundary_store_path", lambda: nado_path)
    monkeypatch.setattr(_nado, "OperationalVenueIO", lambda owner, sender: nado_io)

    def bridge_factory(*, hold_release_gate, preparation_gate):
        def coordinator_factory():
            venue = _NoNetworkRisexVenue()
            risex_venues.append(venue)
            coordinator = _risex.RisexFundingBoundaryCoordinator._fixture(
                venue=venue,
                primary_journal=tmp_path / "actual-risex-primary.sqlite3",
                counterparty_journal=tmp_path / "actual-risex-counterparty.sqlite3",
                hold_release_gate=hold_release_gate,
                preparation_gate=preparation_gate,
            )
            holder["coordinator"] = coordinator
            original_barrier_gate = coordinator._preparation_gate
            assert original_barrier_gate is not None

            def tracked_barrier_gate(binding):
                risex_barrier_bindings.append(binding)
                try:
                    return original_barrier_gate(binding)
                except BaseException as error:
                    risex_barrier_errors.append(error)
                    raise

            coordinator._preparation_gate = tracked_barrier_gate
            original_gate = coordinator._run_preparation_gate

            def tracked_gate(binding):
                try:
                    return original_gate(binding)
                except BaseException as error:
                    risex_errors.append(error)
                    raise

            coordinator._run_preparation_gate = tracked_gate

            async def stop_after_barrier():
                lifecycle_log.append("risex_observation")
                raise _risex.CoordinatorSafetyError(
                    "RISEx diagnostic observation stop"
                )

            # Use the actual coordinator.run path through _preflight, then
            # stop at its first post-barrier observation without venue I/O.
            coordinator._observe = stop_after_barrier
            return coordinator

        bridge = _risex.RisexCoordinationLoopBridge(
            coordinator_factory,
            startup_timeout_seconds=2,
            callback_timeout_seconds=2,
            lifecycle_timeout_seconds=2,
            shutdown_timeout_seconds=2,
        )
        return bridge, _risex.RisexTerminalEvidenceProvider(bridge)

    def nado_factory(**kwargs):
        owned = _owner._default_nado_factory(**kwargs)
        build = owned.worker_factory
        assert build is not None

        def worker_build():
            runner, store = build()
            original_barrier_gate = runner._preparation_gate
            assert original_barrier_gate is not None

            def tracked_barrier_gate(binding):
                nado_barrier_bindings.append(binding)
                try:
                    return original_barrier_gate(binding)
                except BaseException as error:
                    nado_barrier_errors.append(error)
                    raise

            runner._preparation_gate = tracked_barrier_gate
            original_gate = runner._await_preparation_gate

            def tracked_gate():
                try:
                    return original_gate()
                except BaseException as error:
                    nado_errors.append(error)
                    raise

            runner._await_preparation_gate = tracked_gate

            def barrier_only_run():
                runner.stage = "ENTRY_PREPARATION"
                runner._await_preparation_gate()
                lifecycle_log.append("nado_after_barrier")
                return _nado.FundingBoundaryReport(
                    1, "BLOCKED", "worker-owned-test", 0, 0,
                    _nado.FUNDING_UNRESOLVED, None, None,
                    True, True, True, True, "TEST_BLOCKER",
                )

            runner.run = barrier_only_run
            return runner, store

        owned.worker_factory = worker_build
        return owned

    orchestrator = _owner.FundingBoundaryOrchestrator(
        route=_risex.fixed_funding_route(settlement),
        bridge_factory=bridge_factory,
        nado_factory=nado_factory,
        private_read=_read_ok,
    )
    report = orchestrator.run(timeout_seconds=2)

    assert report.status == "BLOCKED_BEFORE_WRITE"
    assert report.terminal is True
    assert report.reason == "BARRIER_ABORTED"
    assert report.failure_code is None
    assert report.failure_class is None
    assert report.failure_stage is None
    assert report.nado_report is not None
    assert report.risex_report is not None
    assert report.risex_report.coordinator_report.result is (
        _risex.CoordinatorResult.BLOCKED_BEFORE_WRITE
    )
    assert report.risex_report.coordinator_report.primary_intents == 0
    assert report.risex_report.coordinator_report.counterparty_intents == 0
    assert report.risex_report.coordinator_report.primary_dispatches == 0
    assert report.risex_report.coordinator_report.counterparty_dispatches == 0
    assert report.risex_report.coordinator_report.counterparty_cancels == 0
    assert report.nado_report.status == "BLOCKED"
    assert report.nado_potential_write is False
    assert report.risex_potential_write is False
    assert nado_barrier_errors == []
    assert risex_barrier_errors == []
    assert nado_errors == []
    assert risex_errors == []
    assert len(nado_barrier_bindings) == 1
    assert len(risex_barrier_bindings) == 1
    nado_binding = orchestrator._nado_binding
    coordinator = holder["coordinator"]
    assert isinstance(coordinator, _risex.RisexFundingBoundaryCoordinator)
    risex_binding = coordinator._local_binding
    assert nado_binding is not None
    assert risex_binding is not None
    assert nado_barrier_bindings[0] is nado_binding
    assert risex_barrier_bindings[0] is nado_binding
    assert risex_binding.boundary is nado_binding
    assert orchestrator._barrier._arrived == {"NADO", "RISEX"}
    assert orchestrator._barrier.passed is True
    assert coordinator._preparation_gate_used is True
    nado_runner = orchestrator.nado_runner
    assert nado_runner is not None
    assert nado_runner._preparation_gate_used is True
    assert set(lifecycle_log) == {"nado_after_barrier", "risex_observation"}
    assert nado_io.observe_thread_id is None
    assert risex_venues and risex_venues[0].observe_calls == 0
    assert risex_venues[0].rest_round_calls == 0
    assert risex_venues[0].place_calls == []
    assert risex_venues[0].cancel_calls == []

    database = _read_worker_database(nado_path)
    assert database["runtime"] == ("BLOCKED", "SAFETY", "FINAL_BARRIER")
    assert database["lifecycle"] == "HALTED"
    assert database["intents"] == 0
    assert database["integrity"] == "ok"

    assert report.closed is True
    assert report.sanitized()["binding_digest"] == risex_binding.identity_digest


def test_actual_barrier_abort_provenance_survives_both_wrappers_and_worker_terminalization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A target-time owner abort remains bounded through both real wrappers."""
    settlement = time.time_ns() // 1_000_000 + 60_000
    nado_path = tmp_path / "actual-abort-nado.sqlite3"
    nado_io = _RealNadoIO(timestamp=settlement - 1)
    risex_venues: list[_NoNetworkRisexVenue] = []
    nado_barrier_errors: list[BaseException] = []
    risex_barrier_errors: list[BaseException] = []
    nado_errors: list[BaseException] = []
    risex_errors: list[BaseException] = []
    monkeypatch.setattr(
        _nado, "_strict_identity", lambda: (REAL_NADO_OWNER, REAL_NADO_SENDER),
    )
    monkeypatch.setattr(_nado, "_funding_boundary_store_path", lambda: nado_path)
    monkeypatch.setattr(_nado, "OperationalVenueIO", lambda owner, sender: nado_io)

    def bridge_factory(*, hold_release_gate, preparation_gate):
        def coordinator_factory():
            venue = _NoNetworkRisexVenue()
            risex_venues.append(venue)
            coordinator = _risex.RisexFundingBoundaryCoordinator._fixture(
                venue=venue,
                primary_journal=tmp_path / "actual-abort-risex-primary.sqlite3",
                counterparty_journal=tmp_path / "actual-abort-risex-counterparty.sqlite3",
                hold_release_gate=hold_release_gate,
                preparation_gate=preparation_gate,
            )
            original_barrier_gate = coordinator._preparation_gate
            assert original_barrier_gate is not None

            def tracked_barrier_gate(binding):
                try:
                    return original_barrier_gate(binding)
                except BaseException as error:
                    risex_barrier_errors.append(error)
                    raise

            coordinator._preparation_gate = tracked_barrier_gate
            original_gate = coordinator._run_preparation_gate

            def tracked_gate(binding):
                try:
                    return original_gate(binding)
                except BaseException as error:
                    risex_errors.append(error)
                    raise

            coordinator._run_preparation_gate = tracked_gate
            return coordinator

        bridge = _risex.RisexCoordinationLoopBridge(
            coordinator_factory,
            startup_timeout_seconds=2,
            callback_timeout_seconds=2,
            lifecycle_timeout_seconds=2,
            shutdown_timeout_seconds=2,
        )
        return bridge, _risex.RisexTerminalEvidenceProvider(bridge)

    def nado_factory(**kwargs):
        owned = _owner._default_nado_factory(**kwargs)
        # Keep Nado's startup read strictly pre-target while making the
        # owner-side barrier clock the accepted non-hypothetical target time.
        owned.now_ms = lambda: settlement
        build = owned.worker_factory
        assert build is not None

        def worker_build():
            runner, store = build()
            original_barrier_gate = runner._preparation_gate
            assert original_barrier_gate is not None

            def tracked_barrier_gate(binding):
                try:
                    return original_barrier_gate(binding)
                except BaseException as error:
                    nado_barrier_errors.append(error)
                    raise

            runner._preparation_gate = tracked_barrier_gate
            original_gate = runner._await_preparation_gate

            def tracked_gate():
                try:
                    return original_gate()
                except BaseException as error:
                    nado_errors.append(error)
                    raise

            runner._await_preparation_gate = tracked_gate

            def barrier_only_run():
                runner.stage = "ENTRY_PREPARATION"
                runner._await_preparation_gate()
                raise AssertionError("the actual barrier unexpectedly passed")

            runner.run = barrier_only_run
            return runner, store

        owned.worker_factory = worker_build
        return owned

    orchestrator = _owner.FundingBoundaryOrchestrator(
        route=_risex.fixed_funding_route(settlement),
        bridge_factory=bridge_factory,
        nado_factory=nado_factory,
        private_read=_read_ok,
    )
    report = orchestrator.run(timeout_seconds=2)

    assert orchestrator._barrier._now_ms() == settlement
    assert report.status == "BLOCKED_BEFORE_WRITE"
    assert report.terminal is True
    assert report.reason == "BARRIER_ABORTED"
    assert report.failure_code == "BARRIER_ABORTED"
    assert report.failure_class == "SAFETY"
    assert report.failure_stage == "ENTRY_PREPARATION"
    assert report.nado_report is None
    assert report.risex_report is not None
    assert report.risex_report.coordinator_report.failure_code == (
        "BARRIER_ABORTED"
    )

    assert len(risex_barrier_errors) == 1
    assert len(nado_barrier_errors) == 1
    for error in (risex_barrier_errors[0], nado_barrier_errors[0]):
        assert error.code == "BARRIER_ABORTED"
        assert error.failure_class == "SAFETY"
        assert error.stage == "ENTRY_PREPARATION"
    assert {
        type(risex_barrier_errors[0]), type(nado_barrier_errors[0]),
    } == {_owner._PreparationBarrierFailure, _owner._PreparationBarrierAborted}

    assert len(nado_errors) == 1
    assert nado_errors[0].code == "BARRIER_ABORTED"
    assert nado_errors[0].failure_class == "SAFETY"
    assert nado_errors[0].stage == "ENTRY_PREPARATION"
    assert len(risex_errors) == 1
    assert risex_errors[0].code == "BARRIER_ABORTED"
    assert risex_errors[0].failure_class == "SAFETY"
    assert risex_errors[0].stage == "ENTRY_PREPARATION"

    assert report.risex_report.coordinator_report.primary_intents == 0
    assert report.risex_report.coordinator_report.counterparty_intents == 0
    assert report.risex_report.coordinator_report.primary_dispatches == 0
    assert report.risex_report.coordinator_report.counterparty_dispatches == 0
    assert report.risex_report.coordinator_report.counterparty_cancels == 0
    assert report.nado_potential_write is False
    assert report.risex_potential_write is False
    nado_runner = orchestrator.nado_runner
    assert nado_runner is not None
    assert nado_runner.writes == 0
    assert nado_io.observe_thread_id is None
    assert risex_venues and risex_venues[0].observe_calls == 0
    assert risex_venues[0].rest_round_calls == 0
    assert risex_venues[0].place_calls == []
    assert risex_venues[0].cancel_calls == []

    database = _read_worker_database(nado_path)
    assert database["runtime"] == ("BLOCKED", "SAFETY", "ENTRY_PREPARATION")
    assert database["lifecycle"] == "HALTED"
    assert database["intents"] == 0
    assert database["integrity"] == "ok"
    assert b"preparation gate rejected" not in database["bytes"]

    sanitized = report.sanitized()
    assert sanitized["reason"] == "BARRIER_ABORTED"
    assert sanitized["failure_code"] == "BARRIER_ABORTED"
    assert sanitized["failure_class"] == "SAFETY"
    assert sanitized["failure_stage"] == "ENTRY_PREPARATION"
    assert report.closed is True


def test_preparation_barrier_rejects_risex_local_binding_mismatch() -> None:
    primary = JournalIdentity(
        RISEX_VENUE, "risex-run", _risex.FUNDING_BOUNDARY_PRIMARY_STORE_IDENTITY,
        PRIMARY_ACCOUNT,
    )
    counterparty = JournalIdentity(
        RISEX_VENUE, "risex-counterparty-run",
        _risex.FUNDING_BOUNDARY_COUNTERPARTY_STORE_IDENTITY,
        COUNTERPARTY_ACCOUNT,
    )
    canonical = FundingBoundaryBinding(
        _risex.fixed_funding_route(SETTLEMENT),
        primary,
        JournalIdentity(NADO_VENUE, "nado-run", "nado-store", NADO_ACCOUNT),
    )
    local = _risex.RisexFundingBoundaryBinding(
        canonical, primary, counterparty,
    )
    barrier = _owner._PreparationBarrier(lambda: SETTLEMENT)
    barrier.bind(canonical)

    with pytest.raises(_owner.FundingBoundaryOrchestrationError) as error:
        barrier.wait("RISEX", local)

    assert error.value.code == "BINDING_MISMATCH"
    assert barrier.passed is False
    assert barrier._arrived == set()


def test_unknown_preparation_gate_exceptions_remain_generic_and_sanitized(
    tmp_path: Path,
) -> None:
    def unknown_gate(_binding):
        raise RuntimeError("opaque secret exception text")

    nado_runner = object.__new__(_nado.SealedFundingBoundaryRunner)
    nado_runner.funding_binding = object()
    nado_runner._preparation_gate = unknown_gate
    nado_runner._preparation_gate_used = False
    with pytest.raises(_nado.OperationalSafetyError) as nado_error:
        nado_runner._await_preparation_gate()
    assert str(nado_error.value) == "Nado preparation gate rejected"
    assert not hasattr(nado_error.value, "code")
    assert "opaque secret" not in str(nado_error.value)

    venue = _NoNetworkRisexVenue()
    coordinator = _risex.RisexFundingBoundaryCoordinator._fixture(
        venue=venue,
        primary_journal=tmp_path / "unknown-risex-primary.sqlite3",
        counterparty_journal=tmp_path / "unknown-risex-counterparty.sqlite3",
        preparation_gate=unknown_gate,
    )
    route = _risex.fixed_funding_route(SETTLEMENT)
    boundary = FundingBoundaryBinding(
        route,
        coordinator._journal_identity(_risex.AccountRole.PRIMARY),
        JournalIdentity(NADO_VENUE, "nado-run", "nado-store", NADO_ACCOUNT),
    )
    try:
        coordinator.bind_funding_boundary(boundary)
        with pytest.raises(_risex.RisexFundingBoundaryError) as risex_error:
            coordinator._run_preparation_gate(coordinator._require_bound())
        assert str(risex_error.value) == "RISEx preparation gate rejected"
        assert not hasattr(risex_error.value, "code")
        assert "opaque secret" not in str(risex_error.value)
    finally:
        for journal in coordinator._journals.values():
            journal.close()


def test_real_sqlite_worker_owned_prewrite_failure_persists_exact_class_and_stage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _track_worker_calls(monkeypatch)
    orchestrator, holder, path, io, _settlement = _real_worker_fixture(
        tmp_path, monkeypatch, observe_error="schema failure",
    )

    report = orchestrator.run(timeout_seconds=1)

    assert report.status == "BLOCKED_BEFORE_WRITE"
    assert report.terminal is True
    assert report.closed is True
    assert report.nado_report is None
    assert report.nado_potential_write is False
    assert report.risex_potential_write is False
    assert io.observe_thread_id is not None
    assert holder["bridge"].closed is True
    worker_id = io.observe_thread_id
    assert worker_id != threading.get_ident()
    assert set(calls["begin"]) == {worker_id}
    assert set(calls["terminalize"]) == {worker_id}
    for name in (
        "funding_boundary_binding", "intents", "lifecycle_status", "halt", "close",
    ):
        assert calls[name]
        assert set(calls[name]) == {worker_id}
    database = _read_worker_database(path)
    assert database["runtime"] == ("BLOCKED", "SCHEMA", "LIVE_OBSERVATION")
    assert database["lifecycle"] == "HALTED"
    assert database["intents"] == 0
    assert database["integrity"] == "ok"
    assert b"schema failure" not in database["bytes"]


def test_real_sqlite_worker_owned_startup_failure_persists_exact_private_read_stage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _track_worker_calls(monkeypatch)

    def failed_private_read():
        raise _nado.OperationalSafetyError("transport private-read failure")

    orchestrator, holder, path, _io, _settlement = _real_worker_fixture(
        tmp_path, monkeypatch, private_read=failed_private_read,
    )

    with pytest.raises(_owner.FundingBoundaryOrchestrationError) as error:
        orchestrator.start()
    report = orchestrator.status()

    assert error.value.code == "STARTUP_FAILED"
    assert report.status == "BLOCKED_BEFORE_WRITE"
    assert report.terminal is True
    assert report.closed is True
    assert report.nado_report is None
    assert holder["bridge"].closed is True
    worker_id = next(iter(calls["begin"]))
    assert worker_id != threading.get_ident()
    assert set(calls["terminalize"]) == {worker_id}
    for name in (
        "funding_boundary_binding", "intents", "lifecycle_status", "halt", "close",
    ):
        assert calls[name]
        assert set(calls[name]) == {worker_id}
    database = _read_worker_database(path)
    assert database["runtime"] == (
        "BLOCKED", "TRANSPORT", "PRIVATE_READ_BARRIER",
    )
    assert database["lifecycle"] == "HALTED"
    assert database["intents"] == 0
    assert database["integrity"] == "ok"
    assert b"transport private-read failure" not in database["bytes"]


def test_real_sqlite_worker_owned_persistence_failure_stays_manual_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _track_worker_calls(monkeypatch)
    orchestrator, holder, path, io, _settlement = _real_worker_fixture(
        tmp_path, monkeypatch, observe_error="schema failure", fail_terminalize=True,
    )

    report = orchestrator.run(timeout_seconds=1)

    assert report.status == "FAILED_HALTED_MANUAL_RECOVERY"
    assert report.terminal is False
    assert report.closed is False
    assert report.nado_potential_write is True
    assert report.nado_report is None
    assert io.observe_thread_id is not None
    assert holder["bridge"].closed is False
    assert orchestrator._nado_persistence_unknown is True
    assert orchestrator._nado_manual_recovery is True
    assert orchestrator._nado_resources_closed is False
    assert calls["close"] == []
    with pytest.raises(_owner.FundingBoundaryOrchestrationError) as error:
        orchestrator.close()
    assert error.value.code == "MANUAL_RECOVERY_REQUIRED"
    database = _read_worker_database(path)
    assert database["runtime"] == ("STARTED", None, None)
    assert database["lifecycle"] == "RUNNING"
    assert database["intents"] == 0
    assert database["integrity"] == "ok"
    assert b"terminal persistence outage" not in database["bytes"]


@pytest.mark.parametrize("nado_now_ms", [SETTLEMENT, SETTLEMENT + 1])
def test_past_or_equal_nado_target_fails_before_lifecycle_threads(
    tmp_path: Path, nado_now_ms: int,
) -> None:
    orchestrator, holder, log, _release_event = _fixture(
        tmp_path, config=_Config(nado_now_ms=nado_now_ms),
    )

    with pytest.raises(_owner.FundingBoundaryOrchestrationError) as error:
        orchestrator.start()
    report = orchestrator.status()

    nado = holder["nado"]
    bridge = holder["bridge"]
    assert error.value.code == "STARTUP_FAILED"
    assert isinstance(nado, _FakeNadoRunner)
    assert isinstance(bridge, _FakeBridge)
    assert log == ["private_read_finalized"]
    assert nado.io.private_barrier_noted is True
    assert nado.prepare_count == nado.dispatch_count == 0
    assert bridge.observation_count == 0
    assert bridge.prepare_count == bridge.dispatch_count == 0
    assert nado.terminalizations == [("SAFETY", "RUNNER_STARTUP")]
    assert report.status == "BLOCKED_BEFORE_WRITE"
    assert report.terminal is True
    assert report.nado_potential_write is False
    assert report.risex_potential_write is False
    assert bridge.closed is True and nado.store.closed is True


@pytest.mark.parametrize("side", ["NADO", "RISEX"])
@pytest.mark.parametrize("now_ms", [SETTLEMENT, SETTLEMENT + 1])
def test_preparation_barrier_rejects_past_or_equal_target_on_each_arrival(
    side: str, now_ms: int,
) -> None:
    barrier = _owner._PreparationBarrier(lambda: now_ms)
    primary = JournalIdentity(
        RISEX_VENUE, "risex-run", _risex.FUNDING_BOUNDARY_PRIMARY_STORE_IDENTITY,
        PRIMARY_ACCOUNT,
    )
    boundary = FundingBoundaryBinding(
        _risex.fixed_funding_route(SETTLEMENT),
        primary,
        JournalIdentity(NADO_VENUE, "nado-run", "nado-store", NADO_ACCOUNT),
    )
    barrier.bind(boundary)

    with pytest.raises(_owner.FundingBoundaryOrchestrationError) as error:
        barrier.wait(side, boundary)

    assert error.value.code == "BARRIER_ABORTED"
    assert barrier.passed is False


def test_barrier_deadline_rejection_wakes_already_waiting_peer() -> None:
    clock = {"now_ms": SETTLEMENT - 1}
    first_seen = threading.Event()
    first_release = threading.Event()
    second_seen = threading.Event()
    second_release = threading.Event()
    calls = 0

    def now_ms() -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            first_seen.set()
            assert first_release.wait(1)
        elif calls == 2:
            second_seen.set()
            assert second_release.wait(1)
        return clock["now_ms"]

    barrier = _owner._PreparationBarrier(now_ms)
    primary = JournalIdentity(
        RISEX_VENUE, "risex-run", _risex.FUNDING_BOUNDARY_PRIMARY_STORE_IDENTITY,
        PRIMARY_ACCOUNT,
    )
    boundary = FundingBoundaryBinding(
        _risex.fixed_funding_route(SETTLEMENT),
        primary,
        JournalIdentity(NADO_VENUE, "nado-run", "nado-store", NADO_ACCOUNT),
    )
    barrier.bind(boundary)
    errors: dict[str, BaseException] = {}
    done = {side: threading.Event() for side in ("NADO", "RISEX")}

    def waiter(side: str) -> None:
        try:
            barrier.wait(side, boundary)
        except BaseException as error:
            errors[side] = error
        finally:
            done[side].set()

    first = threading.Thread(target=waiter, args=("NADO",), daemon=True)
    second = threading.Thread(target=waiter, args=("RISEX",), daemon=True)
    try:
        first.start()
        assert first_seen.wait(1)
        second.start()
        first_release.set()
        assert second_seen.wait(1)
        clock["now_ms"] = SETTLEMENT
    finally:
        first_release.set()
        second_release.set()
        first.join(1)
        second.join(1)

    assert not first.is_alive() and not second.is_alive()
    assert done["NADO"].is_set() and done["RISEX"].is_set()
    assert isinstance(errors["RISEX"], _owner.FundingBoundaryOrchestrationError)
    assert errors["RISEX"].code == "BARRIER_ABORTED"
    assert isinstance(errors["NADO"], _owner._PreparationBarrierAborted)
    assert barrier.passed is False


def test_barrier_commit_is_shared_when_clock_crosses_before_waiter_resumes() -> None:
    clock = {"now_ms": SETTLEMENT - 1}
    primary = JournalIdentity(
        RISEX_VENUE, "risex-run", _risex.FUNDING_BOUNDARY_PRIMARY_STORE_IDENTITY,
        PRIMARY_ACCOUNT,
    )
    boundary = FundingBoundaryBinding(
        _risex.fixed_funding_route(SETTLEMENT),
        primary,
        JournalIdentity(NADO_VENUE, "nado-run", "nado-store", NADO_ACCOUNT),
    )
    barrier = _owner._PreparationBarrier(lambda: clock["now_ms"])
    barrier.bind(boundary)

    class _AdvanceAfterNotify(threading.Condition):
        def __init__(self):
            super().__init__()
            self.first_waiting = threading.Event()

        def wait(self, *args, **kwargs):
            self.first_waiting.set()
            return super().wait(*args, **kwargs)

        def notify_all(self):
            result = super().notify_all()
            assert barrier._passed is True
            clock["now_ms"] = SETTLEMENT
            return result

    condition = _AdvanceAfterNotify()
    barrier._condition = condition
    errors: list[BaseException] = []
    passed: list[str] = []
    done = {side: threading.Event() for side in ("NADO", "RISEX")}

    def waiter(side: str) -> None:
        try:
            barrier.wait(side, boundary)
            passed.append(side)
        except BaseException as error:
            errors.append(error)
        finally:
            done[side].set()

    first = threading.Thread(target=waiter, args=("NADO",), daemon=True)
    second = threading.Thread(target=waiter, args=("RISEX",), daemon=True)
    first.start()
    assert condition.first_waiting.wait(1)
    second.start()
    first.join(1)
    second.join(1)

    assert not first.is_alive() and not second.is_alive()
    assert errors == []
    assert set(passed) == {"NADO", "RISEX"}
    assert done["NADO"].is_set() and done["RISEX"].is_set()
    assert barrier.passed is True


def test_one_progression_publish_wakes_waiter_with_exact_signal() -> None:
    primary = JournalIdentity(
        RISEX_VENUE, "risex-run", _risex.FUNDING_BOUNDARY_PRIMARY_STORE_IDENTITY,
        PRIMARY_ACCOUNT,
    )
    counterparty = JournalIdentity(
        RISEX_VENUE, "risex-counterparty-run",
        _risex.FUNDING_BOUNDARY_COUNTERPARTY_STORE_IDENTITY,
        COUNTERPARTY_ACCOUNT,
    )
    boundary = FundingBoundaryBinding(
        _risex.fixed_funding_route(SETTLEMENT),
        primary,
        JournalIdentity(NADO_VENUE, "nado-run", "nado-store", NADO_ACCOUNT),
    )
    risex_binding = _risex.RisexFundingBoundaryBinding(
        boundary, primary, counterparty,
    )
    relay = _owner._BoundaryProgressionRelay()
    relay.bind(boundary, risex_binding)
    waiting = threading.Event()
    result: dict[str, object] = {}
    errors: list[BaseException] = []

    def waiter() -> None:
        waiting.set()
        try:
            result["signal"] = relay.wait(risex_binding)
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=waiter)
    thread.start()
    assert waiting.wait(1)
    relay.publish(boundary, _nado.BOUNDARY_RELEASED, SETTLEMENT)
    thread.join(1)

    assert not thread.is_alive()
    assert errors == []
    assert result["signal"] is relay.signal
    signal = result["signal"]
    assert isinstance(signal, _risex.HoldReleaseSignal)
    assert signal.kind == _risex.BoundarySignalKind.RELEASED.value
    assert signal.binding_digest == risex_binding.identity_digest


def test_exact_binding_prewrite_order_and_nado_sourced_release(tmp_path: Path) -> None:
    orchestrator, holder, log, _release_event = _fixture(tmp_path)

    report = orchestrator.run(timeout_seconds=1)

    nado = holder["nado"]
    bridge = holder["bridge"]
    assert isinstance(nado, _FakeNadoRunner)
    assert isinstance(bridge, _FakeBridge)
    assert report.status == "BLOCKED"
    assert report.terminal is True
    assert report.closed is True
    assert report.reason == "RISEX_APPLIED_FUNDING_CONTRACT_MISSING"
    assert nado.io.private_barrier_noted is True
    assert log.index("private_read_finalized") < log.index("nado_gate")
    assert log.index("private_read_finalized") < log.index("risex_gate")
    first_prepare = min(log.index("nado_prepare"), log.index("risex_prepare"))
    assert log.index("nado_gate") < first_prepare
    assert log.index("risex_gate") < first_prepare
    assert nado.prepare_count == 1
    assert bridge.prepare_count == 1
    assert nado.dispatch_count == 1
    assert bridge.dispatch_count == 1
    assert bridge.preparation_bindings == [nado.funding_binding]
    assert bridge.signal is not None
    assert isinstance(bridge.signal, _risex.HoldReleaseSignal)
    assert bridge.signal.kind == _risex.BoundarySignalKind.RELEASED.value
    assert bridge.signal.observed_at_ms >= SETTLEMENT
    assert set(vars(bridge.signal)) == {
        "kind", "binding_digest", "settlement_at_ms", "observed_at_ms", "signal_id",
    }
    assert report.binding_digest == bridge.local_binding.identity_digest
    assert report.sanitized()["risex_counterparty"] == {
        "run_id": "risex-counterparty-run",
        "intents": 4,
        "dispatches": 4,
        "cancels": 0,
    }
    assert nado.terminalizations == [("SAFETY", "FINAL_BARRIER")]
    assert bridge.closed is True and nado.store.closed is True


def test_cross_binding_accepts_only_fresh_v7_risex_store_identities() -> None:
    route = _risex.fixed_funding_route(SETTLEMENT)
    fresh = (
        JournalIdentity(
            RISEX_VENUE,
            "risex-primary-run",
            _risex.FUNDING_BOUNDARY_PRIMARY_STORE_IDENTITY,
            PRIMARY_ACCOUNT,
        ),
        JournalIdentity(
            RISEX_VENUE,
            "risex-counterparty-run",
            _risex.FUNDING_BOUNDARY_COUNTERPARTY_STORE_IDENTITY,
            COUNTERPARTY_ACCOUNT,
        ),
    )
    assert _owner.FundingBoundaryOrchestrator._validate_journals(
        fresh, route,
    ) == fresh

    for version in ("v1", "v2", "v3", "v4", "v5", "v6"):
        historical = (
            JournalIdentity(
                RISEX_VENUE,
                "risex-primary-run",
                f"risex-nado-boundary-primary-{version}",
                PRIMARY_ACCOUNT,
            ),
            JournalIdentity(
                RISEX_VENUE,
                "risex-counterparty-run",
                f"risex-nado-boundary-counterparty-{version}",
                COUNTERPARTY_ACCOUNT,
            ),
        )
        with pytest.raises(_owner.FundingBoundaryOrchestrationError) as error:
            _owner.FundingBoundaryOrchestrator._validate_journals(
                historical, route,
            )
        assert error.value.code == "BINDING_MISMATCH"


def test_complete_requires_both_funding_contracts_to_be_completion_eligible(
    tmp_path: Path,
) -> None:
    orchestrator, holder, _log, _release_event = _fixture(
        tmp_path,
        config=_Config(
            nado_status="COMPLETE",
            risex_result=_risex.FundingBoundaryResult.COMPLETE,
            risex_funding_status="APPLIED",
            risex_blocker=None,
        ),
    )

    report = orchestrator.run(timeout_seconds=1)

    assert report.status == "COMPLETE"
    assert report.terminal is True
    assert report.reason is None
    assert report.closed is True
    assert holder["bridge"].closed is True
    assert holder["nado"].store.closed is True


def test_prewrite_failure_aborts_peer_without_prepare_or_dispatch(tmp_path: Path) -> None:
    orchestrator, holder, log, _release_event = _fixture(
        tmp_path, private_read=lambda: {"status": "BLOCKED"},
    )

    with pytest.raises(_owner.FundingBoundaryOrchestrationError) as error:
        orchestrator.run(timeout_seconds=1)
    report = orchestrator.status()

    nado = holder["nado"]
    bridge = holder["bridge"]
    assert isinstance(nado, _FakeNadoRunner)
    assert isinstance(bridge, _FakeBridge)
    assert error.value.code == "STARTUP_FAILED"
    assert report.status == "BLOCKED_BEFORE_WRITE"
    assert report.terminal is True
    assert report.nado_potential_write is False
    assert report.risex_potential_write is False
    assert nado.prepare_count == nado.dispatch_count == 0
    assert bridge.prepare_count == bridge.dispatch_count == 0
    assert bridge.observation_count == 0
    assert "nado_gate" not in log
    assert "risex_gate" not in log
    assert bridge.closed is True and nado.store.closed is True


def test_binding_failure_does_not_start_lifecycles(tmp_path: Path) -> None:
    orchestrator, holder, _log, _release_event = _fixture(
        tmp_path, config=_Config(bind_failure=True),
    )

    with pytest.raises(_owner.FundingBoundaryOrchestrationError) as error:
        orchestrator.start()

    nado = holder["nado"]
    bridge = holder["bridge"]
    assert error.value.code == "BINDING_MISMATCH"
    assert isinstance(nado, _FakeNadoRunner)
    assert isinstance(bridge, _FakeBridge)
    assert nado.prepare_count == nado.dispatch_count == 0
    assert bridge.prepare_count == bridge.dispatch_count == 0
    assert bridge.started is True
    assert bridge.closed is True and nado.store.closed is True
    assert nado.terminalizations == [("SAFETY", "RUNNER_STARTUP")]


def test_nado_failure_after_barrier_publishes_immutable_cancelled(tmp_path: Path) -> None:
    orchestrator, holder, _log, _release_event = _fixture(
        tmp_path, config=_Config(nado_failure="after_barrier"),
    )

    report = orchestrator.run(timeout_seconds=1)

    nado = holder["nado"]
    bridge = holder["bridge"]
    assert isinstance(nado, _FakeNadoRunner)
    assert isinstance(bridge, _FakeBridge)
    assert isinstance(bridge.signal, _risex.HoldReleaseSignal)
    assert bridge.signal.kind == _risex.BoundarySignalKind.CANCELLED.value
    assert report.status == "FAILED_HALTED_MANUAL_RECOVERY"
    assert report.terminal is False
    assert report.nado_potential_write is True
    with pytest.raises(_owner.FundingBoundaryOrchestrationError):
        orchestrator.close()
    assert bridge.closed is False and nado.store.closed is False


def test_asymmetric_partial_exposure_retains_both_owned_tasks(tmp_path: Path) -> None:
    orchestrator, holder, _log, _release_event = _fixture(
        tmp_path, config=_Config(risex_failure="after_dispatch"),
    )

    report = orchestrator.run(timeout_seconds=1)

    nado = holder["nado"]
    bridge = holder["bridge"]
    assert isinstance(nado, _FakeNadoRunner)
    assert isinstance(bridge, _FakeBridge)
    assert report.status == "FAILED_HALTED_MANUAL_RECOVERY"
    assert report.reason == "ASYMMETRIC_PARTIAL_EXPOSURE"
    assert report.terminal is False
    assert report.nado_potential_write is True
    assert report.risex_potential_write is True
    assert bridge.closed is False and nado.store.closed is False
    assert report.risex_report is None
    assert report.nado_report is not None
    assert report.nado_report.final_exact_flat is True
    counterparty = report.sanitized()["risex_counterparty"]
    assert counterparty == {
        "availability": "BOUND",
        "run_id": "risex-counterparty-run",
        "store_identity": _risex.FUNDING_BOUNDARY_COUNTERPARTY_STORE_IDENTITY,
        "account_id": COUNTERPARTY_ACCOUNT,
        "intents": None,
        "dispatches": None,
        "cancels": None,
    }


def test_timeout_does_not_cancel_and_retrieval_preserves_blocked_verdict(tmp_path: Path) -> None:
    orchestrator, holder, _log, release_event = _fixture(
        tmp_path, config=_Config(wait_for_release=True),
    )

    with pytest.raises(_owner.FundingBoundaryOrchestrationTimeout):
        orchestrator.run(timeout_seconds=0.02)
    running = orchestrator.status()
    assert running.status == "RUNNING"
    assert running.closed is False
    bridge = holder["bridge"]
    assert isinstance(bridge, _FakeBridge)
    assert bridge.closed is False

    release_event.set()
    report = orchestrator.retrieve(timeout_seconds=1)

    assert report.status == "BLOCKED"
    assert report.terminal is True
    assert report.reason == "RISEX_APPLIED_FUNDING_CONTRACT_MISSING"
    assert report.closed is True
    assert bridge.closed is True


def test_risex_observes_only_after_delayed_nado_baseline_releases_barrier(
    tmp_path: Path,
) -> None:
    orchestrator, holder, log, _release_event = _fixture(
        tmp_path, config=_Config(delay_nado_baseline=True),
    )

    with pytest.raises(_owner.FundingBoundaryOrchestrationTimeout):
        orchestrator.run(timeout_seconds=0.02)

    bridge = holder["bridge"]
    baseline_event = holder["baseline_event"]
    assert isinstance(bridge, _FakeBridge)
    assert isinstance(baseline_event, threading.Event)
    assert bridge.observation_count == 0
    assert "nado_baseline_started" in log
    assert "nado_baseline_finalized" not in log
    assert "risex_observation" not in log
    assert "risex_price_validation" not in log

    baseline_event.set()
    report = orchestrator.retrieve(timeout_seconds=1)

    assert report.status == "BLOCKED"
    assert report.terminal is True
    assert bridge.observation_count == 1
    baseline_done = log.index("nado_baseline_finalized")
    nado_gate = log.index("nado_gate_passed")
    risex_gate = log.index("risex_gate_passed")
    risex_observation = log.index("risex_observation")
    risex_price_validation = log.index("risex_price_validation")
    assert baseline_done < nado_gate
    assert baseline_done < risex_gate
    assert nado_gate < risex_observation
    assert risex_gate < risex_observation
    assert risex_observation < risex_price_validation
    assert report.closed is True


def test_historical_import_and_zero_argument_surface_remain_inert() -> None:
    import risex_farmer

    assert not hasattr(risex_farmer, "FundingBoundaryOrchestrator")
    assert callable(_nado.run)
    assert callable(_risex.build_risex_nado_funding_boundary_bridge)
