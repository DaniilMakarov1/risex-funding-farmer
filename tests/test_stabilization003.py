import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import json
import pickle

import pytest

import risex_farmer.runtime as runtime_module
from risex_farmer.exchanges.base import PublicDataUnavailable
from risex_farmer.exchanges.extended import ExtendedAdapter
from risex_farmer.models import (
    BookLevel,
    DataQuality,
    FundingAccrualMethod,
    FundingCashQuote,
    FundingQuality,
    LifecycleState,
    MarketVolume,
    OrderBook,
    Venue,
)
from risex_farmer.notifications import NotificationOutbox
from risex_farmer.notifications import format_telegram_money
from risex_farmer.runtime import PublicPaperRuntime, public_paper_run, public_scan_once
from risex_farmer.storage import PaperRepository
from risex_farmer.paper_broker import (
    CancellationReason,
    PaperEntryState,
    PaperOrderStatus,
)
from test_runtime import (
    CaptureNotifications,
    FakeAdapter,
    FakeClock,
    GatedAdapter,
    GatedExtendedAdapter,
    ManyFakeAdapter,
    adapters,
    confirm_public_streams,
)


D = Decimal
BASE = datetime(2027, 8, 1, 18, 58, tzinfo=UTC)


def _full_scan_count(repository: PaperRepository) -> int:
    return repository.connection.execute(
        "SELECT COUNT(*) FROM runtime_evidence WHERE event_type='PUBLIC_SCAN' "
        "AND json_extract(detail,'$.scan_kind')='FULL'"
    ).fetchone()[0]


def _latest_routes(repository: PaperRepository) -> list[dict[str, object]]:
    return repository.report(as_of=BASE + timedelta(hours=2))["latest_routes"]


class RunLoopSleep:
    def __init__(self) -> None:
        self.waits: list[tuple[float, asyncio.Event]] = []
        self.tasks: list[asyncio.Task[None]] = []
        self.cancelled = 0

    async def __call__(self, seconds: float) -> None:
        task = asyncio.current_task()
        assert task is not None
        self.tasks.append(task)
        gate = asyncio.Event()
        self.waits.append((seconds, gate))
        try:
            await gate.wait()
        except asyncio.CancelledError:
            self.cancelled += 1
            raise

    def release_latest(self) -> None:
        assert self.waits
        self.waits[-1][1].set()


async def _spin_until(predicate, *, turns: int = 500) -> bool:
    for _ in range(turns):
        if predicate():
            return True
        await asyncio.sleep(0)
    return predicate()


async def _run_loop_ready(
    repository: PaperRepository,
    clock: FakeClock,
    controlled: "ControlledFundingAdapter",
    *,
    target: datetime,
):
    fakes = adapters(clock, settlement_at=target)
    fakes[Venue.RISEX] = controlled
    sleeper = RunLoopSleep()
    stop = asyncio.Event()
    runtime = PublicPaperRuntime(
        repository, adapters=fakes, clock=clock, sleep=sleeper
    )

    async def no_streams() -> None:
        return None

    runtime.start_streams = no_streams
    run_task = asyncio.create_task(runtime.run(stop_event=stop))
    assert await _spin_until(lambda: bool(sleeper.waits))
    assert repository.connection.execute(
        "SELECT COUNT(*) FROM runtime_evidence "
        "WHERE event_type='PAPER_RUN_READY'"
    ).fetchone()[0] == 1
    assert await _spin_until(
        lambda: runtime._refresh_task is not None
        and runtime._refresh_task.done()
    )
    assert await _spin_until(
        lambda: runtime._extended_universe_task is None
        or runtime._extended_universe_task.done()
    )
    return runtime, fakes, sleeper, stop, run_task


def _scan_details(repository: PaperRepository, kind: str) -> list[dict[str, object]]:
    return [
        json.loads(row[0])
        for row in repository.connection.execute(
            "SELECT detail FROM runtime_evidence WHERE event_type='PUBLIC_SCAN' "
            "AND json_extract(detail,'$.scan_kind')=? ORDER BY evidence_id",
            (kind,),
        )
    ]


class ControlledFundingAdapter(FakeAdapter):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.block_funding = False
        self.funding_started = asyncio.Event()
        self.funding_gate = asyncio.Event()
        self.funding_error: BaseException | None = None
        self.cancelled = False

    async def fetch_funding_quote(self, market, *, assumed_open_at):
        if self.block_funding:
            self.funding_started.set()
            try:
                await self.funding_gate.wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise
        if self.funding_error is not None:
            raise self.funding_error
        return await super().fetch_funding_quote(
            market, assumed_open_at=assumed_open_at
        )


class FatalAfterSiblingFundingAdapter(FakeAdapter):
    def __init__(self, *args, sibling_started: asyncio.Event, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.enabled = False
        self.sibling_started = sibling_started

    async def fetch_funding_quote(self, market, *, assumed_open_at):
        if self.enabled:
            await self.sibling_started.wait()
            raise RuntimeError("synthetic observation programmer defect")
        return await super().fetch_funding_quote(
            market, assumed_open_at=assumed_open_at
        )


class FatalAfterSiblingCatalogAdapter(FakeAdapter):
    def __init__(self, *args, sibling_started: asyncio.Event, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.enabled = False
        self.sibling_started = sibling_started

    async def fetch_markets(self):
        if self.enabled:
            await self.sibling_started.wait()
            raise RuntimeError("synthetic catalog programmer defect")
        return await super().fetch_markets()


class FatalNestedObservationAdapter(FakeAdapter):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.enabled = False
        self.book_started = asyncio.Event()
        self.book_gate = asyncio.Event()
        self.book_cancelled = False

    async def fetch_book(self, venue_symbol):
        if self.enabled:
            self.book_started.set()
            try:
                await self.book_gate.wait()
            except asyncio.CancelledError:
                self.book_cancelled = True
                raise
        return await super().fetch_book(venue_symbol)

    async def fetch_funding_quote(self, market, *, assumed_open_at):
        if self.enabled:
            await self.book_started.wait()
            raise RuntimeError("synthetic nested funding defect")
        return await super().fetch_funding_quote(
            market, assumed_open_at=assumed_open_at
        )


class FatalNestedCatalogAdapter(FakeAdapter):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.enabled = False
        self.volumes_started = asyncio.Event()
        self.volumes_gate = asyncio.Event()
        self.volumes_cancelled = False

    async def fetch_volumes(self):
        if self.enabled:
            self.volumes_started.set()
            try:
                await self.volumes_gate.wait()
            except asyncio.CancelledError:
                self.volumes_cancelled = True
                raise
        return await super().fetch_volumes()

    async def fetch_markets(self):
        if self.enabled:
            await self.volumes_started.wait()
            raise RuntimeError("synthetic nested markets defect")
        return await super().fetch_markets()


class CatalogIsolationExtended(GatedExtendedAdapter):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.block_universe = False
        self.block_required = False
        self.universe_started = asyncio.Event()
        self.required_started = asyncio.Event()
        self.funding_started = asyncio.Event()
        self.universe_gate = asyncio.Event()
        self.required_gate = asyncio.Event()
        self.required_calls = 0

    def _catalog_result(self):
        return (
            (self.market,),
            (MarketVolume(
                Venue.EXTENDED,
                self.market.venue_symbol,
                D("1000000"),
                self.clock.now(),
                "official-shaped",
            ),),
        )

    async def fetch_catalog(self):
        self.catalog_calls += 1
        self.calls.append("catalog")
        if self.block_universe:
            self.universe_started.set()
            await self.universe_gate.wait()
        return self._catalog_result()

    async def fetch_required_catalog(self, venue_symbols):
        assert tuple(venue_symbols) == (self.market.venue_symbol,)
        self.required_calls += 1
        self.calls.append("required_catalog")
        if self.block_required:
            self.required_started.set()
            await self.required_gate.wait()
        return self._catalog_result()

    async def fetch_funding_quote(self, market, *, assumed_open_at):
        self.funding_started.set()
        return await super().fetch_funding_quote(
            market, assumed_open_at=assumed_open_at
        )


@pytest.mark.asyncio
async def test_stabilization003_startup_catalogs_are_not_duplicated(
    tmp_path, monkeypatch,
):
    clock = FakeClock(BASE)
    target = BASE + timedelta(hours=1)
    stop = asyncio.Event()
    risex = GatedAdapter(Venue.RISEX, clock, settlement_at=target)
    nado = GatedAdapter(Venue.NADO, clock, settlement_at=target)
    extended = GatedExtendedAdapter(clock, settlement_at=target)
    captured: dict[str, PublicPaperRuntime] = {}

    async def start_synthetic_streams(runtime: PublicPaperRuntime) -> None:
        captured["runtime"] = runtime
        confirm_public_streams(runtime, clock.now())

    async def stop_after_ready(_seconds: float) -> None:
        runtime = captured["runtime"]
        if runtime._refresh_task is not None:
            await asyncio.gather(runtime._refresh_task, return_exceptions=True)
        stop.set()
        await asyncio.sleep(0)

    monkeypatch.setattr(
        PublicPaperRuntime, "start_streams", start_synthetic_streams
    )
    with PaperRepository(tmp_path / "stabilization003-startup-catalog.db") as repository:
        result = await public_paper_run(
            repository,
            adapters={
                Venue.RISEX: risex,
                Venue.EXTENDED: extended,
                Venue.NADO: nado,
            },
            clock=clock,
            sleep=stop_after_ready,
            stop_event=stop,
        )
        refresh_started = repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence "
            "WHERE event_type='PUBLIC_REFRESH_STARTED'"
        ).fetchone()[0]
        last_event = repository.connection.execute(
            "SELECT event_type FROM runtime_evidence ORDER BY evidence_id DESC LIMIT 1"
        ).fetchone()[0]

    assert result == {"status": "STOPPED_SAFE", "forced_close": False}
    assert risex.catalog_calls == 4
    assert nado.catalog_calls == 4
    assert extended.catalog_calls == 1
    assert refresh_started == 1
    assert last_event == "STOPPED_SAFE"
    assert captured["runtime"]._refresh_task is None
    assert captured["runtime"]._extended_universe_task is None


@pytest.mark.asyncio
async def test_stabilization003_full_and_universe_catalog_deadlines_overlap_once(
    tmp_path,
):
    clock = FakeClock(BASE)
    target = BASE + timedelta(hours=1)
    risex = GatedAdapter(Venue.RISEX, clock, settlement_at=target)
    nado = GatedAdapter(Venue.NADO, clock, settlement_at=target)
    extended = CatalogIsolationExtended(clock, settlement_at=target)
    with PaperRepository(tmp_path / "stabilization003-catalog-overlap.db") as repository:
        async with PublicPaperRuntime(
            repository,
            adapters={
                Venue.RISEX: risex,
                Venue.EXTENDED: extended,
                Venue.NADO: nado,
            },
            clock=clock,
        ) as runtime:
            runtime.accepting_entries = False
            await runtime.tick()
            runtime._stop_event = asyncio.Event()
            clock.advance(120)
            confirm_public_streams(runtime, clock.now())
            runtime.next_full_scan_at = clock.now()
            runtime.next_extended_catalog_at = clock.now()
            await runtime.tick(clock.now())
            catalog_task = runtime._extended_universe_task
            refresh_task = runtime._refresh_task
            assert catalog_task is not None
            assert refresh_task is not None
            runtime._start_background_catalog_refresh(
                include_extended_universe=False
            )
            assert runtime._extended_universe_task is catalog_task
            await asyncio.gather(catalog_task, refresh_task)
            await runtime.tick(clock.now())

    assert risex.catalog_calls == 4
    assert nado.catalog_calls == 4
    assert extended.catalog_calls == 2
    assert extended.required_calls == 1
    assert extended.calls.count("funding") == 2


@pytest.mark.asyncio
async def test_stabilization003_a_due_full_waits_for_same_slot_cycle_rollover(
    tmp_path,
):
    clock = FakeClock(BASE)
    old_cycle = BASE + timedelta(seconds=120)
    new_cycle = BASE + timedelta(hours=1, seconds=120)
    fakes = adapters(clock, settlement_at=old_cycle)
    fakes[Venue.NADO].settlement_at = new_cycle
    with PaperRepository(tmp_path / "stabilization003-a.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            runtime.accepting_entries = False
            await runtime.tick()
            confirm_public_streams(runtime, clock.now())
            fakes[Venue.RISEX].settlement_at = new_cycle
            fakes[Venue.EXTENDED].settlement_at = new_cycle
            clock.value = BASE + timedelta(seconds=123)
            confirm_public_streams(runtime, clock.now())
            await runtime.tick()
            refresh = runtime._refresh_task
            assert refresh is not None
            await refresh
            await runtime.tick()
            rows = _latest_routes(repository)
            full_count = _full_scan_count(repository)
            if runtime._extended_universe_task is not None:
                await runtime._extended_universe_task

    assert full_count == 1
    assert rows
    assert all("TARGET_CYCLE_ELAPSED" not in row["blockers"] for row in rows)
    assert {row["target_cycle_start"] for row in rows} == {
        new_cycle.isoformat()
    }


@pytest.mark.asyncio
async def test_stabilization003_b_due_full_uses_refresh_at_funding_ttl_edge(
    tmp_path,
):
    clock = FakeClock(BASE)
    target = BASE + timedelta(hours=1)
    fakes = adapters(clock, settlement_at=target)
    with PaperRepository(tmp_path / "stabilization003-b.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            runtime.accepting_entries = False
            await runtime.tick()
            initial_observed = {
                key: row.funding.observed_at for key, row in runtime.observations.items()
            }
            clock.value = BASE + timedelta(seconds=123)
            confirm_public_streams(runtime, clock.now())
            await runtime.tick()
            refresh = runtime._refresh_task
            assert refresh is not None
            await refresh
            await runtime.tick()
            rows = _latest_routes(repository)
            if runtime._extended_universe_task is not None:
                await runtime._extended_universe_task

    assert rows
    assert all("FUNDING_STALE" not in row["blockers"] for row in rows)
    assert all(row["planned_maker_net_pnl_usd"] is not None for row in rows)
    assert all(
        runtime.observations[key].funding.observed_at > observed_at
        for key, observed_at in initial_observed.items()
    )


@pytest.mark.asyncio
async def test_stabilization003_c_full_never_captures_half_finished_refresh(
    tmp_path,
):
    clock = FakeClock(BASE)
    target = BASE + timedelta(hours=1)
    fakes = adapters(clock, settlement_at=target)
    extended = ControlledFundingAdapter(
        Venue.EXTENDED, clock, settlement_at=target
    )
    fakes[Venue.EXTENDED] = extended
    with PaperRepository(tmp_path / "stabilization003-c.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            runtime.accepting_entries = False
            await runtime.tick()
            old_extended = runtime.observations[
                Venue.EXTENDED, extended.market.venue_symbol
            ]
            clock.value = BASE + timedelta(seconds=120)
            confirm_public_streams(runtime, clock.now())
            extended.block_funding = True
            extended.funding_error = TimeoutError("official component timeout")
            runtime._start_public_refresh()
            await extended.funding_started.wait()
            await asyncio.sleep(0)
            nado_key = (Venue.NADO, fakes[Venue.NADO].market.venue_symbol)
            assert runtime.observations[nado_key].funding.observed_at == clock.now()
            assert runtime.observations[
                Venue.EXTENDED, extended.market.venue_symbol
            ] == old_extended
            await runtime.tick()
            full_while_refresh_pending = _full_scan_count(repository)
            extended.funding_gate.set()
            assert runtime._refresh_task is not None
            await runtime._refresh_task
            await runtime.tick()
            logical_at = runtime.last_scan.logical_at
            included = tuple(runtime.observations.values())
            final_full_count = _full_scan_count(repository)
            if runtime._extended_universe_task is not None:
                await runtime._extended_universe_task

    assert full_while_refresh_pending == 0
    assert final_full_count == 1
    assert all(
        row.book is None or row.book.observed_at <= logical_at for row in included
    )
    assert all(
        row.funding is None or row.funding.observed_at <= logical_at
        for row in included
    )
    assert runtime.observations[
        Venue.EXTENDED, extended.market.venue_symbol
    ].funding == old_extended.funding
    assert runtime.observations[nado_key].funding.observed_at == logical_at


@pytest.mark.asyncio
async def test_stabilization003_d_expected_failure_uses_precise_blocker_then_recovers(
    tmp_path,
):
    clock = FakeClock(BASE)
    target = BASE + timedelta(hours=1)
    fakes = adapters(clock, settlement_at=target)
    risex = ControlledFundingAdapter(Venue.RISEX, clock, settlement_at=target)
    fakes[Venue.RISEX] = risex
    with PaperRepository(tmp_path / "stabilization003-d.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            runtime.accepting_entries = False
            await runtime.tick()
            risex_key = (Venue.RISEX, risex.market.venue_symbol)
            last_good_risex_funding = runtime.observations[risex_key].funding
            clock.value = BASE + timedelta(seconds=123)
            confirm_public_streams(runtime, clock.now())
            risex.funding_error = TimeoutError("official timeout")
            await runtime._refresh_public_data()
            failed = await runtime.scan(refresh=False, scan_kind="FULL")
            failed_blockers = {
                blocker for row in failed["routes"] for blocker in row["blockers"]
            }
            nado_observed_at = runtime.observations[
                Venue.NADO, fakes[Venue.NADO].market.venue_symbol
            ].funding.observed_at
            failed_risex_funding = runtime.observations[risex_key].funding
            risex.funding_error = None
            await runtime._refresh_public_data()
            recovered = await runtime.scan(refresh=False, scan_kind="FULL")

    assert nado_observed_at == BASE + timedelta(seconds=123)
    assert failed_risex_funding == last_good_risex_funding
    assert "FUNDING_STALE" in failed_blockers
    assert "BOOK_UNHEALTHY" not in failed_blockers
    assert all(
        row["planned_maker_net_pnl_usd"] is not None
        for row in recovered["routes"]
    )


@pytest.mark.asyncio
async def test_stabilization003_d_programmer_error_remains_runtime_fatal(tmp_path):
    clock = FakeClock(BASE)
    target = BASE + timedelta(hours=1)
    fakes = adapters(clock, settlement_at=target)
    risex = ControlledFundingAdapter(Venue.RISEX, clock, settlement_at=target)
    fakes[Venue.RISEX] = risex
    with PaperRepository(tmp_path / "stabilization003-d-fatal.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            runtime.accepting_entries = False
            await runtime.tick()
            risex.funding_error = RuntimeError("synthetic programmer defect")
            runtime._start_public_refresh()
            assert runtime._refresh_task is not None
            task_result = await asyncio.gather(
                runtime._refresh_task, return_exceptions=True
            )
            await asyncio.sleep(0)
            background_fatal = runtime._background_fatal
            stop_cause = runtime._stop_cause

    assert isinstance(task_result[0], RuntimeError)
    assert isinstance(background_fatal, RuntimeError)
    assert stop_cause == "RUNTIME_FATAL"


@pytest.mark.asyncio
async def test_stabilization003_d_observation_fatal_cancels_owned_sibling(
    tmp_path,
):
    clock = FakeClock(BASE)
    target = BASE + timedelta(hours=1)
    sibling = ControlledFundingAdapter(
        Venue.NADO, clock, settlement_at=target
    )
    fatal = FatalAfterSiblingFundingAdapter(
        Venue.RISEX, clock, settlement_at=target,
        sibling_started=sibling.funding_started,
    )
    fakes = adapters(clock, settlement_at=target)
    fakes[Venue.RISEX] = fatal
    fakes[Venue.NADO] = sibling
    with PaperRepository(tmp_path / "stabilization003-d-fanout.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            await runtime.tick()
            fatal.enabled = True
            sibling.block_funding = True
            runtime._stop_event = asyncio.Event()
            runtime._start_public_refresh()
            assert runtime._refresh_task is not None
            result = await asyncio.gather(
                runtime._refresh_task, return_exceptions=True
            )
            await asyncio.sleep(0)
            started = asyncio.get_running_loop().time()
            await runtime.shutdown()
            stopped_in = asyncio.get_running_loop().time() - started
            evidence_after_stop = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence"
            ).fetchone()[0]
            await asyncio.sleep(0)
            evidence_final = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence"
            ).fetchone()[0]

    assert isinstance(result[0], RuntimeError)
    assert sibling.cancelled
    assert runtime._background_fatal is result[0]
    assert stopped_in <= 2
    assert evidence_final == evidence_after_stop


@pytest.mark.asyncio
async def test_stabilization003_d_catalog_fatal_cancels_owned_sibling(tmp_path):
    clock = FakeClock(BASE)
    target = BASE + timedelta(hours=1)
    sibling = GatedAdapter(Venue.NADO, clock, settlement_at=target)
    fatal = FatalAfterSiblingCatalogAdapter(
        Venue.RISEX, clock, settlement_at=target,
        sibling_started=sibling.request_started,
    )
    fakes = adapters(clock, settlement_at=target)
    fakes[Venue.RISEX] = fatal
    fakes[Venue.NADO] = sibling
    with PaperRepository(
        tmp_path / "stabilization003-d-catalog-fanout.db"
    ) as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            await runtime.tick()
            fatal.enabled = True
            sibling.block_catalog = True
            runtime._stop_event = asyncio.Event()
            runtime._start_background_catalog_refresh(
                include_extended_universe=False
            )
            task = runtime._extended_universe_task
            assert task is not None
            result = await asyncio.gather(task, return_exceptions=True)
            await asyncio.sleep(0)
            started = asyncio.get_running_loop().time()
            await runtime.shutdown()
            stopped_in = asyncio.get_running_loop().time() - started
            evidence_after_stop = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence"
            ).fetchone()[0]
            await asyncio.sleep(0)
            evidence_final = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence"
            ).fetchone()[0]

    assert isinstance(result[0], RuntimeError)
    assert sibling.cancelled
    assert runtime._background_fatal is result[0]
    assert stopped_in <= 2
    assert evidence_final == evidence_after_stop


@pytest.mark.asyncio
async def test_stabilization003_d_nested_book_is_owned_on_funding_fatal(
    tmp_path,
):
    clock = FakeClock(BASE)
    target = BASE + timedelta(hours=1)
    fatal = FatalNestedObservationAdapter(
        Venue.RISEX, clock, settlement_at=target
    )
    fakes = adapters(clock, settlement_at=target)
    fakes[Venue.RISEX] = fatal
    with PaperRepository(tmp_path / "stabilization003-nested-book.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            await runtime.tick()
            fatal.enabled = True
            runtime._stop_event = asyncio.Event()
            runtime._start_public_refresh()
            task = runtime._refresh_task
            assert task is not None
            result = await asyncio.gather(task, return_exceptions=True)
            await asyncio.sleep(0)
            await runtime.shutdown()
            evidence_after_stop = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence"
            ).fetchone()[0]
            cancelled_after_shutdown = fatal.book_cancelled
            fatal.book_gate.set()
            await asyncio.sleep(0)
            evidence_final = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence"
            ).fetchone()[0]

    assert isinstance(result[0], RuntimeError)
    assert cancelled_after_shutdown
    assert runtime._background_fatal is result[0]
    assert evidence_final == evidence_after_stop


@pytest.mark.asyncio
async def test_stabilization003_d_nested_volumes_is_owned_on_markets_fatal(
    tmp_path,
):
    clock = FakeClock(BASE)
    target = BASE + timedelta(hours=1)
    fatal = FatalNestedCatalogAdapter(
        Venue.RISEX, clock, settlement_at=target
    )
    fakes = adapters(clock, settlement_at=target)
    fakes[Venue.RISEX] = fatal
    with PaperRepository(tmp_path / "stabilization003-nested-catalog.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            await runtime.tick()
            fatal.enabled = True
            runtime._stop_event = asyncio.Event()
            runtime._start_background_catalog_refresh(
                include_extended_universe=False
            )
            task = runtime._extended_universe_task
            assert task is not None
            result = await asyncio.gather(task, return_exceptions=True)
            await asyncio.sleep(0)
            await runtime.shutdown()
            evidence_after_stop = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence"
            ).fetchone()[0]
            cancelled_after_shutdown = fatal.volumes_cancelled
            fatal.volumes_gate.set()
            await asyncio.sleep(0)
            evidence_final = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence"
            ).fetchone()[0]

    assert isinstance(result[0], RuntimeError)
    assert cancelled_after_shutdown
    assert runtime._background_fatal is result[0]
    assert evidence_final == evidence_after_stop


@pytest.mark.asyncio
async def test_stabilization003_ordinary_close_owns_background_tasks(
    tmp_path,
):
    clock = FakeClock(BASE)
    target = BASE + timedelta(hours=1)
    risex = GatedAdapter(Venue.RISEX, clock, settlement_at=target)
    nado = GatedAdapter(Venue.NADO, clock, settlement_at=target)
    fakes = adapters(clock, settlement_at=target)
    fakes[Venue.RISEX] = risex
    fakes[Venue.NADO] = nado
    with PaperRepository(tmp_path / "stabilization003-close-owners.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            await runtime.tick()
            runtime._pending_full_scan_at = clock.now()
            risex.block_funding = True
            nado.block_catalog = True
            runtime._start_public_refresh()
            runtime._start_background_catalog_refresh(
                include_extended_universe=False
            )
            await asyncio.gather(
                risex.request_started.wait(), nado.request_started.wait()
            )
            refresh_task = runtime._refresh_task
            catalog_task = runtime._extended_universe_task
            assert refresh_task is not None and not refresh_task.done()
            assert catalog_task is not None and not catalog_task.done()
            await runtime.close()
            evidence_after_close = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence"
            ).fetchone()[0]
            await runtime.shutdown()
            await asyncio.sleep(0)
            evidence_final = repository.connection.execute(
                "SELECT COUNT(*) FROM runtime_evidence"
            ).fetchone()[0]
            last_event = repository.connection.execute(
                "SELECT event_type FROM runtime_evidence "
                "ORDER BY evidence_id DESC LIMIT 1"
            ).fetchone()[0]

    assert risex.cancelled
    assert nado.cancelled
    assert refresh_task.done()
    assert catalog_task.done()
    assert runtime._refresh_task is None
    assert runtime._extended_universe_task is None
    assert runtime._pending_full_scan_at is None
    assert runtime._shutdown_started
    assert last_event == "STOPPED_SAFE"
    assert evidence_final == evidence_after_close


@pytest.mark.asyncio
async def test_stabilization003_public_scan_once_keeps_baseline_evidence(tmp_path):
    clock = FakeClock(BASE)
    with PaperRepository(tmp_path / "stabilization003-scan-once.db") as repository:
        result = await public_scan_once(
            repository,
            adapters=adapters(
                clock, settlement_at=BASE + timedelta(hours=1)
            ),
            clock=clock,
        )
        evidence = [
            tuple(row)
            for row in repository.connection.execute(
                "SELECT event_type, COUNT(*) FROM runtime_evidence "
                "GROUP BY event_type ORDER BY event_type"
            )
        ]

    assert len(result["routes"]) == 4
    assert evidence == [
        ("PUBLIC_REQUEST_COMPLETED", 3),
        ("PUBLIC_SCAN", 1),
        ("VENUE_READINESS", 3),
    ]
    assert all(event_type != "STOPPED_SAFE" for event_type, _ in evidence)


@pytest.mark.asyncio
async def test_stabilization003_e_slow_full_refresh_never_blocks_deadline_scheduler(
    tmp_path,
):
    clock = FakeClock(BASE)
    target = BASE + timedelta(seconds=400)
    fakes = adapters(clock, settlement_at=target)
    risex = GatedAdapter(Venue.RISEX, clock, settlement_at=target)
    fakes[Venue.RISEX] = risex
    health_ticks: list[datetime] = []
    with PaperRepository(tmp_path / "stabilization003-e.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            await runtime.tick()
            original_health = runtime._check_extended_health
            original_scan = runtime.scan
            focused_captures: list[
                tuple[datetime, datetime, tuple[datetime, ...]]
            ] = []

            async def observed_health(at):
                health_ticks.append(at)
                await original_health(at)

            runtime._check_extended_health = observed_health

            async def observed_scan(**kwargs):
                result = await original_scan(**kwargs)
                if kwargs.get("scan_kind") == "FOCUSED":
                    assert runtime.last_scan is not None
                    focused_captures.append((
                        kwargs["scheduled_at"],
                        runtime.last_scan.logical_at,
                        tuple(
                            observed_at
                            for observation in runtime.observations.values()
                            for observed_at in (
                                observation.book.observed_at
                                if observation.book is not None else None,
                                observation.funding.observed_at
                                if observation.funding is not None else None,
                            )
                            if observed_at is not None
                        ),
                    ))
                return result

            runtime.scan = observed_scan
            risex.block_funding = True
            clock.value = BASE + timedelta(seconds=90)
            confirm_public_streams(runtime, clock.now())
            runtime._start_public_refresh()
            await risex.request_started.wait()
            refresh_owner = runtime._refresh_task
            assert refresh_owner is not None
            activated_state = None
            for offset in range(100, 391, 10):
                clock.value = BASE + timedelta(seconds=offset)
                for key, observation in tuple(runtime.observations.items()):
                    runtime.observations[key] = replace(
                        observation,
                        funding=replace(
                            observation.funding, observed_at=clock.now()
                        ),
                    )
                confirm_public_streams(runtime, clock.now())
                await runtime.tick()
                if offset == 280:
                    activated_state = repository.load_runtime()
                    assert runtime._refresh_task is refresh_owner
                    assert not refresh_owner.done()
                    assert _full_scan_count(repository) == 0
            clock.value = target - timedelta(seconds=5)
            for key, observation in tuple(runtime.observations.items()):
                runtime.observations[key] = replace(
                    observation,
                    funding=replace(
                        observation.funding, observed_at=clock.now()
                    ),
                )
            confirm_public_streams(runtime, clock.now())
            await runtime.tick()
            cutoff_state = repository.load_runtime()
            activation_rows = [
                datetime.fromisoformat(row[0])
                for row in repository.connection.execute(
                    "SELECT recorded_at FROM runtime_evidence "
                    "WHERE event_type='PAPER_ENTRY_ACTIVATED'"
                )
            ]
            assert runtime._refresh_task is refresh_owner
            assert not refresh_owner.done()
            full_while_pending = _full_scan_count(repository)
            scans_while_pending = [
                json.loads(row[0]) for row in repository.connection.execute(
                    "SELECT detail FROM runtime_evidence "
                    "WHERE event_type='PUBLIC_SCAN' ORDER BY evidence_id"
                )
            ]
            for adapter in fakes.values():
                adapter.settlement_at = BASE + timedelta(hours=2)
            risex.gate.set()
            assert runtime._refresh_task is not None
            await runtime._refresh_task
            published_after_focused = runtime.observations[
                Venue.RISEX, risex.market.venue_symbol
            ].funding.observed_at
            await runtime.tick()
            final_full_count = _full_scan_count(repository)

    focused = [row for row in scans_while_pending if row["scan_kind"] == "FOCUSED"]
    assert isinstance(activated_state, PaperEntryState)
    assert activated_state.lifecycle_state is LifecycleState.ENTRY_MAKER_OPEN
    assert activated_state.order is not None
    assert activated_state.order.status is PaperOrderStatus.OPEN
    assert activated_state.order.created_at == target - timedelta(seconds=120)
    assert activated_state.order.cutoff_at == target - timedelta(seconds=5)
    assert activation_rows == [target - timedelta(seconds=120)]
    assert isinstance(cutoff_state, PaperEntryState)
    assert cutoff_state.lifecycle_state is LifecycleState.FLAT
    assert cutoff_state.order is not None
    assert cutoff_state.order.status is PaperOrderStatus.CANCELLED
    assert cutoff_state.order.cancelled_at == target - timedelta(seconds=5)
    assert cutoff_state.order.cancellation_reason is CancellationReason.CUTOFF
    assert full_while_pending == 0
    assert final_full_count == 1
    assert len(health_ticks) >= 25
    assert focused
    assert all(
        all(observed_at <= logical_at for observed_at in observed_ats)
        for _, logical_at, observed_ats in focused_captures
    )
    assert published_after_focused > focused_captures[0][1]
    assert published_after_focused not in focused_captures[0][2]
    assert any(
        row["scheduled_at"] == (BASE + timedelta(seconds=100)).isoformat()
        for row in focused
    )
    assert any(
        row["scheduled_at"] == (target - timedelta(seconds=120)).isoformat()
        for row in focused
    )
    assert any(
        row["scan_kind"] == "FOCUSED"
        and row["started_at"] == (target - timedelta(seconds=5)).isoformat()
        for row in scans_while_pending
    )


@pytest.mark.asyncio
async def test_stabilization003_f_catalog_paths_do_not_gate_full_observations(
    tmp_path,
):
    clock = FakeClock(BASE)
    target = BASE + timedelta(hours=1)
    fakes = adapters(clock, settlement_at=target)
    extended = CatalogIsolationExtended(clock, settlement_at=target)
    fakes[Venue.EXTENDED] = extended
    with PaperRepository(tmp_path / "stabilization003-f.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock
        ) as runtime:
            runtime.accepting_entries = False
            await runtime.tick()
            extended.funding_started.clear()
            extended.block_universe = True
            extended.block_required = True
            runtime._stop_event = asyncio.Event()
            runtime._start_extended_universe_refresh()
            await extended.universe_started.wait()
            clock.value = BASE + timedelta(seconds=120)
            confirm_public_streams(runtime, clock.now())
            await runtime.tick()
            assert runtime._refresh_task is not None
            await extended.required_started.wait()
            for _ in range(3):
                await asyncio.sleep(0)
            observation_started_before_catalog_release = (
                extended.funding_started.is_set()
            )
            focused = await runtime.scan(
                refresh=False, scan_kind="FOCUSED", scheduled_at=clock.now()
            )
            full_while_catalog_pending = _full_scan_count(repository)
            extended.universe_gate.set()
            extended.required_gate.set()
            await runtime._refresh_task
            assert runtime._extended_universe_task is not None
            await runtime._extended_universe_task
            await runtime.tick()
            final_full_count = _full_scan_count(repository)
            if runtime._refresh_task is not None:
                await runtime._refresh_task

    assert focused["routes"]
    assert observation_started_before_catalog_release
    assert full_while_catalog_pending == 0
    assert final_full_count == 1
    assert extended.catalog_calls == 2
    assert extended.required_calls == 1
    assert extended.calls.count("funding") == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("scan_kind", ("INITIAL", "FULL", "FOCUSED", "RECOVERY"))
async def test_stabilization003_g_scan_rows_use_one_captured_tuple(
    tmp_path, monkeypatch, scan_kind,
):
    clock = FakeClock(BASE)
    fakes = adapters(clock, settlement_at=BASE + timedelta(hours=1))
    delivery = CaptureNotifications()
    with PaperRepository(tmp_path / f"stabilization003-g-{scan_kind}.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock,
            notifications=NotificationOutbox(delivery),
        ) as runtime:
            await runtime.scan(scan_kind="INITIAL")
            confirm_public_streams(runtime, clock.now())
            clock.advance(1)
            original_scan_once = runtime_module.scan_once
            captured_quotes: dict[
                tuple[Venue, str], tuple[datetime, str]
            ] = {}

            async def mutate_after_scanner_capture(observations, logical_at, **kwargs):
                frozen = tuple(observations)
                captured_quotes.update({
                    (row.market.venue, row.market.venue_symbol): (
                        row.funding.observed_at, row.funding.source
                    )
                    for row in frozen
                })
                snapshot = await original_scan_once(frozen, logical_at, **kwargs)
                for key, live in tuple(runtime.observations.items()):
                    runtime.observations[key] = replace(
                        live,
                        funding=replace(
                            live.funding,
                            observed_at=logical_at + timedelta(seconds=1),
                            source="MUTATED_AFTER_SCANNER_CAPTURE",
                        ),
                    )
                    runtime.coordinator.stream(*key).gap()
                return snapshot

            monkeypatch.setattr(
                runtime_module, "scan_once", mutate_after_scanner_capture
            )
            result = await runtime.scan(
                refresh=(scan_kind == "INITIAL"),
                scan_kind=scan_kind,
                scheduled_at=clock.now(),
            )
            persisted = _latest_routes(repository)
            persisted_quotes = tuple(
                pickle.loads(row[0])
                for row in repository.connection.execute(
                    "SELECT payload FROM funding_quotes ORDER BY rowid DESC LIMIT ?",
                    (len(captured_quotes),),
                )
            )
            snapshot = runtime.last_scan

    assert result["routes"] == persisted
    assert snapshot.logical_at == clock.now()
    expected_blockers = {
        (
            plan.canonical_asset,
            plan.hedge_venue.value,
            plan.direction.value,
        ): list(plan.no_trade_reasons)
        for plan in snapshot.evaluations
    }
    assert all(
        row["blockers"] == expected_blockers[
            row["canonical_asset"], row["hedge_venue"], row["direction"]
        ]
        for row in persisted
    )
    assert all(
        row["source_quality"]["risex_funding"]["source"]
        == captured_quotes[
            (Venue.RISEX, f"{row['canonical_asset']}-RISEX")
        ][1]
        for row in persisted
    )
    assert all(
        row["source_quality"]["hedge_funding"]["source"]
        == captured_quotes[
            (Venue(row["hedge_venue"]),
             f"{row['canonical_asset']}-{row['hedge_venue']}")
        ][1]
        for row in persisted
    )
    assert {
        (quote.venue, quote.canonical_market, quote.observed_at, quote.source)
        for quote in persisted_quotes
    } == {
        (venue, symbol, observed_at, source)
        for (venue, symbol), (observed_at, source) in captured_quotes.items()
    }
    assert all(
        row["source_quality"]["risex_funding"]["source"]
        != "MUTATED_AFTER_SCANNER_CAPTURE"
        for row in persisted
    )
    if scan_kind == "FULL":
        digest = next(row for row in delivery.rows if row.kind == "FULL_SCAN_DIGEST")
        assert digest.occurred_at == snapshot.logical_at
        digest_lines = digest.text.splitlines()[1:]
        assert len(digest_lines) == len(persisted)
        for line, row in zip(digest_lines, persisted):
            if row["planned_maker_net_pnl_usd"] is not None:
                assert line.endswith(
                    "Expected PnL: $" + format_telegram_money(
                        row["planned_maker_net_pnl_usd"]
                    )
                )


@pytest.mark.asyncio
async def test_stabilization003_h_top5_component_blocker_and_numeric_matrix(
    tmp_path,
):
    clock = FakeClock(BASE)
    fakes = {
        venue: ManyFakeAdapter(
            venue, clock, settlement_at=BASE + timedelta(hours=1)
        )
        for venue in Venue
    }
    for adapter in fakes.values():
        adapter.funding_cash = D("0")
    delivery = CaptureNotifications()
    with PaperRepository(tmp_path / "stabilization003-h.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock,
            notifications=NotificationOutbox(delivery),
        ) as runtime:
            baseline = await runtime.scan(scan_kind="FULL")
            baseline_snapshot = runtime.last_scan
            assert baseline_snapshot is not None
            confirm_public_streams(runtime, clock.now())
            assert len(baseline["routes"]) == 20
            assert {row["canonical_asset"] for row in baseline["routes"]} == {
                f"A{index}" for index in range(5)
            }
            assert {row["hedge_venue"] for row in baseline["routes"]} == {
                "EXTENDED", "NADO"
            }
            assert {row["direction"] for row in baseline["routes"]} == {
                "LONG_RISEX_SHORT_HEDGE", "SHORT_RISEX_LONG_HEDGE"
            }
            assert all(
                row["planned_maker_net_pnl_usd"] is not None
                and D(row["planned_maker_net_pnl_usd"]) < 0
                and row["entry_allowed"] is False
                and row["blockers"] == ["PLANNED_NET_PNL_NEGATIVE"]
                for row in baseline["routes"]
            )
            assert baseline["routes"] == _latest_routes(repository)
            baseline_observations = dict(runtime.observations)
            baseline_quotes = {
                (quote.venue, quote.canonical_market): quote
                for (payload,) in repository.connection.execute(
                    "SELECT payload FROM funding_quotes"
                )
                for quote in (pickle.loads(payload),)
            }
            assert set(baseline_quotes) == set(baseline_observations)
            assert all(
                baseline_quotes[key] == observation.funding
                for key, observation in baseline_observations.items()
            )
            assert {
                quote.quality for quote in baseline_quotes.values()
                if quote.venue is Venue.RISEX
            } == {FundingQuality.ESTIMATED}
            assert {
                quote.quality for quote in baseline_quotes.values()
                if quote.venue in {Venue.EXTENDED, Venue.NADO}
            } == {FundingQuality.PREDICTED}
            assert {
                quote.accrual_method for quote in baseline_quotes.values()
            } == {FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT}
            assert all(
                quote.observed_at <= baseline_snapshot.logical_at
                and baseline_snapshot.logical_at - quote.observed_at
                <= timedelta(
                    seconds=runtime.config.default_max_funding_data_age_seconds
                )
                for quote in baseline_quotes.values()
            )
            baseline_plans = {
                (
                    plan.canonical_asset,
                    plan.hedge_venue.value,
                    plan.direction.value,
                ): plan
                for plan in baseline_snapshot.evaluations
            }
            for row in baseline["routes"]:
                plan = baseline_plans[
                    row["canonical_asset"], row["hedge_venue"], row["direction"]
                ]
                fee_split = runtime_module.planned_fee_split(
                    plan, config=runtime.config
                )
                risex_quote = baseline_quotes[
                    Venue.RISEX, plan.risex_market.venue_symbol
                ]
                hedge_quote = baseline_quotes[
                    plan.hedge_venue, plan.hedge_market.venue_symbol
                ]
                assert row["blockers"] == list(plan.no_trade_reasons)
                assert row["canonical_quantity"] == str(plan.canonical_quantity)
                assert row["risex_exact_q_entry_vwap_usd"] == str(
                    plan.risex_entry_price
                )
                assert row["risex_exact_q_exit_vwap_usd"] == str(
                    plan.risex_exit_price
                )
                assert row["hedge_maker_entry_price_usd"] == str(
                    plan.hedge_entry_price
                )
                assert row["planned_hedge_exit_price_usd"] == str(
                    plan.hedge_exit_price
                )
                assert row["entry_execution_pnl_usd"] == str(
                    plan.planned_entry_execution_pnl_usd
                )
                assert row["exit_execution_pnl_usd"] == str(
                    plan.planned_exit_execution_pnl_usd
                )
                assert row["planned_entry_fees_usd"] == str(fee_split[0])
                assert row["planned_exit_fees_usd"] == str(fee_split[1])
                assert row["planned_fees_usd"] == str(plan.planned_fees_usd)
                assert row["source_quality"]["risex_funding"]["source"] == (
                    risex_quote.source
                )
                assert row["source_quality"]["hedge_funding"]["source"] == (
                    hedge_quote.source
                )
                assert baseline_observations[
                    Venue.RISEX, plan.risex_market.venue_symbol
                ].health.stream_connected
                assert baseline_observations[
                    plan.hedge_venue, plan.hedge_market.venue_symbol
                ].health.stream_connected
                assert baseline_observations[
                    Venue.RISEX, plan.risex_market.venue_symbol
                ].trade_stream_ready
                assert baseline_observations[
                    plan.hedge_venue, plan.hedge_market.venue_symbol
                ].trade_stream_ready
            baseline_books = {
                key: runtime.coordinator.stream(*key).book()
                for key in runtime.observations
            }

            clock.advance(1)
            for key, observation in tuple(runtime.observations.items()):
                runtime.observations[key] = replace(
                    observation,
                    funding=replace(
                        observation.funding,
                        observed_at=clock.now(),
                        quality=FundingQuality.APPLIED_RATE,
                    ),
                )
            applied = await runtime.scan(
                refresh=False, scan_kind="FULL", scheduled_at=clock.now()
            )
            applied_quotes = tuple(
                pickle.loads(payload)
                for (payload,) in repository.connection.execute(
                    "SELECT payload FROM funding_quotes WHERE observed_at=?",
                    (clock.now().isoformat(),),
                )
            )
            assert len(applied_quotes) == 15
            applied_by_market = {
                (quote.venue, quote.canonical_market): quote
                for quote in applied_quotes
            }
            assert all(
                quote.quality is FundingQuality.APPLIED_RATE
                and quote.accrual_method
                is FundingAccrualMethod.SNAPSHOT_AT_SETTLEMENT
                and quote.source == baseline_quotes[
                    quote.venue, quote.canonical_market
                ].source
                for quote in applied_quotes
            )
            assert all(
                row["planned_maker_net_pnl_usd"] is not None
                for row in applied["routes"]
            )
            assert all(
                row["source_quality"]["risex_funding"]["source"]
                == applied_by_market[
                    Venue.RISEX, f"{row['canonical_asset']}-RISEX"
                ].source
                and row["source_quality"]["hedge_funding"]["source"]
                == applied_by_market[
                    Venue(row["hedge_venue"]),
                    f"{row['canonical_asset']}-{row['hedge_venue']}",
                ].source
                for row in applied["routes"]
            )
            label_by_blocker = {
                "MARKET_METADATA_STALE": "market metadata stale",
                "PARITY_OR_MULTIPLIER_UNKNOWN": "RISEx parity",
                "BOOK_UNHEALTHY": "book stream",
                "TRADE_STREAM_UNHEALTHY": "trade stream",
                "FUNDING_STALE": "funding",
                "FUNDING_ELIGIBILITY_UNKNOWN": "funding",
                "TARGET_CYCLE_ELAPSED": "public evidence unavailable",
                "INVALID_BBO": "public evidence unavailable",
                "NO_COMMON_EXECUTABLE_QUANTITY": "public evidence unavailable",
                "INSUFFICIENT_EXACT_DEPTH": "book stream",
                "MINIMUM_ORDER": "public evidence unavailable",
            }

            async def reset() -> None:
                runtime.observations.clear()
                runtime.observations.update(baseline_observations)
                for key, book in baseline_books.items():
                    assert book is not None
                    await runtime.recover_snapshot(book, at=clock.now())
                confirm_public_streams(runtime, clock.now())

            async def assert_case(
                blocker: str, venue: Venue, *, risex_leg: bool
            ) -> None:
                result = await runtime.scan(
                    refresh=False, scan_kind="FULL", scheduled_at=clock.now()
                )
                affected = [
                    row for row in result["routes"]
                    if risex_leg or row["hedge_venue"] == venue.value
                ]
                assert len(affected) == (20 if risex_leg else 10)
                assert {row["canonical_asset"] for row in affected} == {
                    f"A{index}" for index in range(5)
                }
                assert {row["hedge_venue"] for row in affected} == (
                    {Venue.EXTENDED.value, Venue.NADO.value}
                    if risex_leg else {venue.value}
                )
                assert {row["direction"] for row in affected} == {
                    "LONG_RISEX_SHORT_HEDGE", "SHORT_RISEX_LONG_HEDGE"
                }
                assert all(row["blockers"] == [blocker] for row in affected)
                assert all(row["planned_maker_net_pnl_usd"] is None for row in affected)
                assert result["routes"] == _latest_routes(repository)
                digest = [
                    row for row in delivery.rows if row.kind == "FULL_SCAN_DIGEST"
                ][-1]
                nado_lines = [
                    line for line in digest.text.splitlines()[1:]
                    if risex_leg or f"/ {venue.value} " in line
                ]
                assert len(nado_lines) == (20 if risex_leg else 10)
                assert all(
                    line.endswith(
                        "Expected PnL: UNKNOWN — " + label_by_blocker[blocker]
                    )
                    for line in nado_lines
                )

            async def apply_case(
                blocker: str, venue: Venue, *, risex_leg: bool
            ) -> None:
                clock.advance(1)
                await reset()
                keys = [
                    (
                        Venue.RISEX if risex_leg else venue,
                        f"A{index}-{'RISEX' if risex_leg else venue.value}",
                    )
                    for index in range(5)
                ]
                for key in keys:
                    observation = runtime.observations[key]
                    if blocker == "MARKET_METADATA_STALE":
                        runtime.observations[key] = replace(
                            observation,
                            market=replace(
                                observation.market,
                                evidence_blockers=(blocker,),
                            ),
                        )
                    elif blocker == "PARITY_OR_MULTIPLIER_UNKNOWN":
                        runtime.observations[key] = replace(
                            observation,
                            market=replace(
                                observation.market, base_multiplier=None
                            ),
                        )
                    elif blocker == "BOOK_UNHEALTHY":
                        runtime.coordinator.stream(*key).gap()
                    elif blocker == "TRADE_STREAM_UNHEALTHY":
                        runtime._trade_stream_ready.discard(key)
                    elif blocker == "FUNDING_STALE":
                        runtime.observations[key] = replace(
                            observation,
                            funding=replace(
                                observation.funding,
                                observed_at=clock.now() - timedelta(seconds=121),
                            ),
                        )
                    elif blocker == "FUNDING_ELIGIBILITY_UNKNOWN":
                        runtime.observations[key] = replace(
                            observation,
                            funding=replace(
                                observation.funding,
                                eligibility_known=False,
                                long_cash_per_canonical_base_usd=None,
                                short_cash_per_canonical_base_usd=None,
                            ),
                        )
                    elif blocker == "TARGET_CYCLE_ELAPSED":
                        runtime.observations[key] = replace(
                            observation,
                            funding=replace(
                                observation.funding, settlement_at=clock.now()
                            ),
                        )
                    elif blocker == "INVALID_BBO":
                        await runtime.recover_snapshot(OrderBook(
                            venue,
                            key[1],
                            (BookLevel(D("99.5"), D("20")),),
                            (BookLevel(D("101"), D("20")),),
                            clock.now(),
                            1,
                        ), at=clock.now())
                    elif blocker == "NO_COMMON_EXECUTABLE_QUANTITY":
                        runtime.observations[key] = replace(
                            observation,
                            market=replace(
                                observation.market, quantity_step_raw=D("1000")
                            ),
                        )
                    elif blocker == "INSUFFICIENT_EXACT_DEPTH":
                        await runtime.recover_snapshot(OrderBook(
                            key[0],
                            key[1],
                            (BookLevel(D("99"), D("0.01")),),
                            (BookLevel(D("101"), D("0.01")),),
                            clock.now(),
                            1,
                        ), at=clock.now())
                    elif blocker == "MINIMUM_ORDER":
                        runtime.observations[key] = replace(
                            observation,
                            market=replace(
                                observation.market,
                                minimum_quantity_raw=D("1000"),
                            ),
                        )
                    else:
                        raise AssertionError(blocker)
                await assert_case(blocker, venue, risex_leg=risex_leg)

            for blocker in label_by_blocker:
                await apply_case(blocker, Venue.RISEX, risex_leg=True)
                for venue in (Venue.EXTENDED, Venue.NADO):
                    await apply_case(blocker, venue, risex_leg=False)

            clock.advance(1)
            await reset()
            for venue in (Venue.EXTENDED, Venue.NADO):
                for index in range(5):
                    key = (venue, f"A{index}-{venue.value}")
                    observation = runtime.observations[key]
                    runtime.observations[key] = replace(
                        observation,
                        funding=replace(
                            observation.funding,
                            observed_at=clock.now() - timedelta(seconds=121),
                            eligibility_known=False,
                            long_cash_per_canonical_base_usd=None,
                            short_cash_per_canonical_base_usd=None,
                            settlement_at=clock.now(),
                        ),
                    )
            combined = await runtime.scan(
                refresh=False, scan_kind="FULL", scheduled_at=clock.now()
            )
            combined_blockers = [
                "FUNDING_STALE",
                "FUNDING_ELIGIBILITY_UNKNOWN",
                "TARGET_CYCLE_ELAPSED",
            ]
            assert len(combined["routes"]) == 20
            assert all(
                row["blockers"] == combined_blockers
                for row in combined["routes"]
            )
            assert combined["routes"] == _latest_routes(repository)
            digest = [
                row for row in delivery.rows if row.kind == "FULL_SCAN_DIGEST"
            ][-1]
            assert len(digest.text.splitlines()[1:]) == 20
            assert all(
                line.endswith("Expected PnL: UNKNOWN — funding")
                for line in digest.text.splitlines()[1:]
            )

            assert all(
                row.book is None or row.book.observed_at <= runtime.last_scan.logical_at
                for row in runtime.observations.values()
            )
            assert all(
                row.funding is None
                or row.funding.observed_at <= runtime.last_scan.logical_at
                for row in runtime.observations.values()
            )


@pytest.mark.asyncio
async def test_stabilization003_i_telegram_relays_only_persisted_full_rows(tmp_path):
    clock = FakeClock(BASE)
    fakes = adapters(clock, settlement_at=BASE + timedelta(hours=1))
    delivery = CaptureNotifications()
    with PaperRepository(tmp_path / "stabilization003-i.db") as repository:
        async with PublicPaperRuntime(
            repository, adapters=fakes, clock=clock,
            notifications=NotificationOutbox(delivery),
        ) as runtime:
            numeric = await runtime.scan(scan_kind="FULL")
            numeric_persisted = _latest_routes(repository)
            clock.advance(1)
            for adapter in fakes.values():
                adapter.funding_unknown = True
            unknown = await runtime.scan(scan_kind="FULL")
            unknown_persisted = _latest_routes(repository)

    assert numeric["routes"] == numeric_persisted
    assert unknown["routes"] == unknown_persisted
    digests = [row for row in delivery.rows if row.kind == "FULL_SCAN_DIGEST"]
    assert len(digests) == 2
    numeric_lines = digests[0].text.splitlines()[1:]
    unknown_lines = digests[1].text.splitlines()[1:]
    assert len(numeric_lines) == len(numeric_persisted)
    assert len(unknown_lines) == len(unknown_persisted)
    for line, row in zip(numeric_lines, numeric_persisted):
        assert line.endswith(
            "Expected PnL: $" + format_telegram_money(
                row["planned_maker_net_pnl_usd"]
            )
        )
    unknown_labels = {
        "FUNDING_STALE": "funding",
        "FUNDING_ELIGIBILITY_UNKNOWN": "funding",
    }
    for line, row in zip(unknown_lines, unknown_persisted):
        blocker = row["blockers"][0]
        assert line.endswith(
            "Expected PnL: UNKNOWN — " + unknown_labels[blocker]
        )


@pytest.mark.asyncio
async def test_stabilization003_run_loop_refresh_completion_wakes_due_full(tmp_path):
    clock = FakeClock(BASE)
    target = BASE + timedelta(hours=1)
    controlled = ControlledFundingAdapter(Venue.RISEX, clock, settlement_at=target)
    with PaperRepository(tmp_path / "run-loop-full-wake.db") as repository:
        runtime, fakes, sleeper, stop, run_task = await _run_loop_ready(
            repository, clock, controlled, target=target
        )
        refresh_before = repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence WHERE event_type='PUBLIC_REFRESH_STARTED'"
        ).fetchone()[0]
        funding_before = {
            venue: adapter.calls.count("funding")
            for venue, adapter in fakes.items()
        }
        controlled.block_funding = True
        scheduled = BASE + timedelta(seconds=120)
        clock.value = scheduled
        sleeper.release_latest()
        assert await _spin_until(controlled.funding_started.is_set)
        assert await _spin_until(lambda: len(sleeper.waits) >= 2)
        refresh_owner = runtime._refresh_task
        assert refresh_owner is not None and not refresh_owner.done()
        ordinary_wait = sleeper.waits[-1]
        clock.value = scheduled + timedelta(seconds=3)
        controlled.funding_gate.set()
        assert await _spin_until(refresh_owner.done)
        woke = await _spin_until(lambda: _full_scan_count(repository) == 1, turns=100)
        full_rows = _scan_details(repository, "FULL")
        refresh_after = repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence WHERE event_type='PUBLIC_REFRESH_STARTED'"
        ).fetchone()[0]
        stop.set()
        result = await asyncio.wait_for(run_task, timeout=1)

    assert ordinary_wait[1].is_set() is False
    assert woke, {
        "terminal_at": clock.now().isoformat(),
        "full_count": len(full_rows),
        "refresh_delta": refresh_after - refresh_before,
        "sleep_waits": len(sleeper.waits),
        "funding_calls": {
            venue.value: adapter.calls.count("funding")
            for venue, adapter in fakes.items()
        },
    }
    assert result == {"status": "STOPPED_SAFE", "forced_close": False}
    assert len(full_rows) == 1
    assert full_rows[0]["scheduled_at"] == scheduled.isoformat()
    assert full_rows[0]["started_at"] == clock.now().isoformat()
    assert runtime.last_scan is not None and runtime.last_scan.logical_at == clock.now()
    assert refresh_after == refresh_before + 1
    assert len(sleeper.waits) <= 3
    assert all(
        adapter.calls.count("funding") == funding_before[venue] + 1
        for venue, adapter in fakes.items()
    )


@pytest.mark.asyncio
async def test_stabilization003_run_loop_deadlines_precede_refresh_completion(tmp_path):
    clock = FakeClock(BASE)
    target = BASE + timedelta(seconds=400)
    controlled = ControlledFundingAdapter(Venue.RISEX, clock, settlement_at=target)
    health: list[datetime] = []
    with PaperRepository(tmp_path / "run-loop-deadline-priority.db") as repository:
        runtime, _, sleeper, stop, run_task = await _run_loop_ready(
            repository, clock, controlled, target=target
        )
        original_health = runtime._check_extended_health

        async def observed_health(at: datetime) -> None:
            health.append(at)
            await original_health(at)

        runtime._check_extended_health = observed_health
        clock.value = BASE + timedelta(seconds=100)
        sleeper.release_latest()
        assert await _spin_until(lambda: bool(_scan_details(repository, "FOCUSED")))
        assert await _spin_until(lambda: len(sleeper.waits) >= 2)
        controlled.block_funding = True
        clock.value = BASE + timedelta(seconds=120)
        sleeper.release_latest()
        assert await _spin_until(controlled.funding_started.is_set)
        assert await _spin_until(lambda: len(sleeper.waits) >= 3)
        refresh_owner = runtime._refresh_task
        assert refresh_owner is not None and not refresh_owner.done()
        for expected_waits, offset in ((4, 280), (5, 395)):
            clock.value = BASE + timedelta(seconds=offset)
            for key, observation in tuple(runtime.observations.items()):
                runtime.observations[key] = replace(
                    observation,
                    funding=replace(observation.funding, observed_at=clock.now()),
                )
            confirm_public_streams(runtime, clock.now())
            sleeper.release_latest()
            assert await _spin_until(lambda: len(sleeper.waits) >= expected_waits)
            assert runtime._refresh_task is refresh_owner and not refresh_owner.done()
        state_at_cutoff = repository.load_runtime()
        clock.value = BASE + timedelta(seconds=398)
        controlled.funding_gate.set()
        assert await _spin_until(refresh_owner.done)
        woke = await _spin_until(lambda: _full_scan_count(repository) == 1, turns=100)
        stop.set()
        await asyncio.wait_for(run_task, timeout=1)
        focused = _scan_details(repository, "FOCUSED")
        activation_rows = repository.connection.execute(
            "SELECT recorded_at FROM runtime_evidence "
            "WHERE event_type='PAPER_ENTRY_ACTIVATED'"
        ).fetchall()

    assert health and health[-1] <= BASE + timedelta(seconds=395)
    assert any(row["scheduled_at"] == (BASE + timedelta(seconds=100)).isoformat() for row in focused)
    assert [row[0] for row in activation_rows] == [
        (target - timedelta(seconds=120)).isoformat()
    ]
    assert isinstance(state_at_cutoff, PaperEntryState)
    assert state_at_cutoff.lifecycle_state is LifecycleState.FLAT
    assert state_at_cutoff.order is not None
    assert state_at_cutoff.order.status is PaperOrderStatus.CANCELLED
    assert state_at_cutoff.order.cancellation_reason is CancellationReason.CUTOFF
    assert woke


@pytest.mark.asyncio
async def test_stabilization003_run_loop_stop_owns_refresh_and_local_wait(tmp_path):
    clock = FakeClock(BASE)
    target = BASE + timedelta(hours=1)
    controlled = ControlledFundingAdapter(Venue.RISEX, clock, settlement_at=target)
    with PaperRepository(tmp_path / "run-loop-stop.db") as repository:
        runtime, _, sleeper, stop, run_task = await _run_loop_ready(
            repository, clock, controlled, target=target
        )
        controlled.block_funding = True
        clock.value = BASE + timedelta(seconds=120)
        sleeper.release_latest()
        assert await _spin_until(controlled.funding_started.is_set)
        assert await _spin_until(lambda: len(sleeper.waits) >= 2)
        writes_before = repository.connection.total_changes
        stop.set()
        result = await asyncio.wait_for(run_task, timeout=1)
        writes_after = repository.connection.total_changes
        await asyncio.sleep(0)
        writes_quiescent = repository.connection.total_changes
        full_count = _full_scan_count(repository)

    assert result == {"status": "STOPPED_SAFE", "forced_close": False}
    assert sleeper.cancelled == 1
    assert controlled.cancelled
    assert runtime._refresh_task is None
    assert runtime._pending_full_scan_at is None
    assert full_count == 0
    assert writes_after > writes_before
    assert writes_quiescent == writes_after


@pytest.mark.asyncio
async def test_stabilization003_run_loop_parent_cancel_cleans_local_waiters(
    tmp_path, monkeypatch,
):
    clock = FakeClock(BASE)
    target = BASE + timedelta(hours=1)
    controlled = ControlledFundingAdapter(Venue.RISEX, clock, settlement_at=target)
    shielded: list[asyncio.Future[object]] = []
    original_shield = asyncio.shield

    def captured_shield(value):
        wrapper = original_shield(value)
        shielded.append(wrapper)
        return wrapper

    monkeypatch.setattr(runtime_module.asyncio, "shield", captured_shield)
    with PaperRepository(tmp_path / "run-loop-parent-cancel.db") as repository:
        runtime, _, sleeper, _, run_task = await _run_loop_ready(
            repository, clock, controlled, target=target
        )
        controlled.block_funding = True
        clock.value = BASE + timedelta(seconds=120)
        sleeper.release_latest()
        assert await _spin_until(controlled.funding_started.is_set)
        assert await _spin_until(lambda: len(sleeper.waits) >= 2 and bool(shielded))
        refresh_owner = runtime._refresh_task
        assert refresh_owner is not None and not refresh_owner.done()
        local_sleep = sleeper.tasks[-1]
        local_shield = shielded[-1]
        writes_before = repository.connection.total_changes
        run_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(run_task, timeout=1)
        terminal_before_test_cleanup = (local_sleep.done(), local_shield.done())
        writes_after = repository.connection.total_changes
        await asyncio.sleep(0)
        writes_quiescent = repository.connection.total_changes
        leaked = [task for task in (local_sleep,) if not task.done()]
        for task in leaked:
            task.cancel()
        await asyncio.gather(*leaked, return_exceptions=True)

    assert terminal_before_test_cleanup == (True, True)
    assert controlled.cancelled
    assert refresh_owner.done()
    assert runtime._refresh_task is None
    assert runtime._pending_full_scan_at is None
    assert writes_after > writes_before
    assert writes_quiescent == writes_after


@pytest.mark.asyncio
@pytest.mark.parametrize("completion", ("success", "expected_failure", "fatal"))
async def test_stabilization003_run_loop_terminal_refresh_wakeup_modes(
    tmp_path, completion,
):
    clock = FakeClock(BASE)
    target = BASE + timedelta(hours=1)
    controlled = ControlledFundingAdapter(Venue.RISEX, clock, settlement_at=target)
    with PaperRepository(tmp_path / f"run-loop-terminal-{completion}.db") as repository:
        runtime, _, sleeper, stop, run_task = await _run_loop_ready(
            repository, clock, controlled, target=target
        )
        controlled.block_funding = True
        if completion == "expected_failure":
            controlled.funding_error = PublicDataUnavailable("expected public outage")
        elif completion == "fatal":
            controlled.funding_error = RuntimeError("programmer defect")
        scheduled = BASE + timedelta(seconds=120)
        clock.value = scheduled
        sleeper.release_latest()
        assert await _spin_until(controlled.funding_started.is_set)
        assert await _spin_until(lambda: len(sleeper.waits) >= 2)
        owner = runtime._refresh_task
        assert owner is not None
        terminal_at = scheduled + timedelta(seconds=3)
        clock.value = terminal_at
        controlled.funding_gate.set()
        assert await _spin_until(owner.done)
        if completion == "fatal":
            with pytest.raises(RuntimeError, match="programmer defect"):
                await asyncio.wait_for(run_task, timeout=1)
            full_count = _full_scan_count(repository)
        else:
            woke = await _spin_until(lambda: _full_scan_count(repository) == 1, turns=100)
            stop.set()
            await asyncio.wait_for(run_task, timeout=1)
            full_count = _full_scan_count(repository)
            assert woke
            assert runtime.last_scan is not None and runtime.last_scan.logical_at == terminal_at
        fatal_rows = repository.connection.execute(
            "SELECT COUNT(*) FROM runtime_evidence WHERE event_type='RUNTIME_FATAL'"
        ).fetchone()[0]

    assert full_count == (0 if completion == "fatal" else 1)
    assert fatal_rows == (1 if completion == "fatal" else 0)
