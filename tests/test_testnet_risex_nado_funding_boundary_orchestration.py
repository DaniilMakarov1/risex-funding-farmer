from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
import threading

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
