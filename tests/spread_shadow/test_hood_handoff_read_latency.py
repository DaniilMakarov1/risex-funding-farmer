from __future__ import annotations

import asyncio
from decimal import Decimal
from time import perf_counter

import pytest

from risex_spread_shadow.hood_handoff import (
    ContractError,
    Direction,
    HandoffEngine,
    Outcome,
    run_handoff,
)

from test_hood_handoff_engine import FakeClient, FakeClock, make_config


class TimedReadClient(FakeClient):
    def __init__(self, *, delay: float, delay_all: bool = False, source_fills: bool = True):
        super().__init__(source_fills=source_fills)
        self.delay = delay
        self.delay_all = delay_all
        self.active_reads = 0
        self.max_active_reads = 0
        self.events: list[tuple[str, float]] = []
        self.pre_receiver_read_count = 0
        self.pre_receiver_read_finished = 0
        self.recheck_done = asyncio.Event()

    async def account_snapshot(self, account_index, market_id):
        is_pre_receiver = self.source_order is not None and self.source_order.active
        delayed = self.delay_all or (is_pre_receiver and self.pre_receiver_read_count < 2)
        if not delayed:
            return await super().account_snapshot(account_index, market_id)
        self.pre_receiver_read_count += 1
        label = f"read-start-{account_index}"
        self.events.append((label, perf_counter()))
        self.active_reads += 1
        self.max_active_reads = max(self.max_active_reads, self.active_reads)
        try:
            await asyncio.sleep(self.delay)
        finally:
            self.active_reads -= 1
            self.pre_receiver_read_finished += 1
            self.events.append((f"read-end-{account_index}", perf_counter()))
            if self.pre_receiver_read_finished == 2 and is_pre_receiver:
                self.recheck_done.set()
        return await super().account_snapshot(account_index, market_id)

    async def lookup_order(self, account_index, market_id, *, order_id=None, client_order_index=None):
        if (
            self.recheck_done.is_set()
            and account_index == self.source_account_index
            and order_id == "source-1"
        ):
            self.events.append(("final-source-lookup", perf_counter()))
        return await super().lookup_order(
            account_index,
            market_id,
            order_id=order_id,
            client_order_index=client_order_index,
        )

    async def submit_order(self, plan):
        if not plan.reduce_only:
            self.events.append(("receiver-submit", perf_counter()))
        return await super().submit_order(plan)


@pytest.mark.asyncio
async def test_account_rechecks_overlap_against_controlled_sequential_baseline():
    parallel_client = TimedReadClient(delay=0.03, delay_all=True)
    parallel_engine = HandoffEngine(parallel_client, clock=FakeClock())
    parallel_engine._configured_request_timeout = 1
    await parallel_engine._parallel_account_rechecks(11, 22, 7)

    parallel_times = dict(parallel_client.events)
    assert parallel_client.max_active_reads == 2
    assert parallel_times["read-start-11"] < parallel_times["read-end-22"]
    assert parallel_times["read-start-22"] < parallel_times["read-end-11"]

    sequential_client = TimedReadClient(delay=0.03, delay_all=True)
    await sequential_client.account_snapshot(11, 7)
    await sequential_client.account_snapshot(22, 7)
    sequential_times = dict(sequential_client.events)
    assert sequential_client.max_active_reads == 1
    assert sequential_times["read-end-11"] <= sequential_times["read-start-22"]


@pytest.mark.asyncio
@pytest.mark.parametrize("direction", [Direction.LONG, Direction.SHORT])
async def test_rechecks_finish_before_exact_source_lookup_and_receiver_dispatch(tmp_path, direction):
    client = TimedReadClient(delay=0.02)
    if direction is Direction.SHORT:
        client.source_position = Decimal("-1")
    result = await run_handoff(
        make_config(tmp_path / f"{direction.value}.jsonl", direction=direction),
        client,
        clock=FakeClock(),
    )

    assert result.outcome is Outcome.SUCCESS
    assert client.max_active_reads == 2
    labels = [label for label, _ in client.events]
    end_indexes = [index for index, label in enumerate(labels) if label.startswith("read-end-")]
    final_lookup_index = labels.index("final-source-lookup")
    receiver_submit_index = labels.index("receiver-submit")
    assert len(end_indexes) == 2
    assert max(end_indexes) < final_lookup_index < receiver_submit_index


class FailingReceiverReadClient(FakeClient):
    def __init__(self):
        super().__init__()
        self.source_read_started = asyncio.Event()
        self.source_read_drained = asyncio.Event()
        self.failure_seen = False
        self.lookup_after_drain: list[bool] = []
        self.cancel_after_drain: list[bool] = []

    async def account_snapshot(self, account_index, market_id):
        if self.source_order is not None and self.source_order.active:
            if account_index == self.source_account_index:
                self.source_read_started.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    self.source_read_drained.set()
                    raise
            if account_index == self.receiver_account_index:
                await self.source_read_started.wait()
                self.failure_seen = True
                raise RuntimeError("synthetic receiver recheck failure")
        return await super().account_snapshot(account_index, market_id)

    async def lookup_order(self, account_index, market_id, *, order_id=None, client_order_index=None):
        if self.failure_seen and account_index == self.source_account_index and order_id == "source-1":
            self.lookup_after_drain.append(self.source_read_drained.is_set())
        return await super().lookup_order(
            account_index,
            market_id,
            order_id=order_id,
            client_order_index=client_order_index,
        )

    async def cancel_order(self, account_index, market_id, order_id):
        self.cancel_after_drain.append(self.source_read_drained.is_set())
        return await super().cancel_order(account_index, market_id, order_id)


@pytest.mark.asyncio
async def test_recheck_failure_drains_sibling_before_lookup_or_cancel(tmp_path):
    client = FailingReceiverReadClient()
    result = await run_handoff(
        make_config(tmp_path / "failure.jsonl"),
        client,
        clock=FakeClock(),
    )

    assert result.outcome is Outcome.UNKNOWN
    assert client.source_read_drained.is_set()
    assert not [plan for plan in client.submissions if not plan.reduce_only]
    assert client.lookup_after_drain and all(client.lookup_after_drain)
    assert client.cancel_after_drain and all(client.cancel_after_drain)


class MalformedSourceReadClient(FakeClient):
    def __init__(self):
        super().__init__()
        self.source_yielded = asyncio.Event()
        self.receiver_started = asyncio.Event()
        self.receiver_drained = asyncio.Event()
        self.cancel_after_drain: list[bool] = []

    async def account_snapshot(self, account_index, market_id):
        if self.source_order is not None and self.source_order.active:
            if account_index == self.source_account_index:
                self.source_yielded.set()
                await asyncio.sleep(0)
                return {"account_index": self.source_account_index}
            if account_index == self.receiver_account_index:
                await self.source_yielded.wait()
                self.receiver_started.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    self.receiver_drained.set()
                    raise
        return await super().account_snapshot(account_index, market_id)

    async def cancel_order(self, account_index, market_id, order_id):
        self.cancel_after_drain.append(self.receiver_drained.is_set())
        return await super().cancel_order(account_index, market_id, order_id)


@pytest.mark.asyncio
async def test_malformed_recheck_aborts_before_slow_peer_timeout(tmp_path):
    client = MalformedSourceReadClient()
    result = await run_handoff(
        make_config(tmp_path / "malformed.jsonl", request_timeout_seconds=0.2),
        client,
        clock=FakeClock(),
    )

    assert result.outcome is Outcome.UNKNOWN
    assert client.receiver_started.is_set()
    assert client.receiver_drained.is_set()
    assert client.cancel_after_drain and all(client.cancel_after_drain)
    assert not [plan for plan in client.submissions if not plan.reduce_only]


class TimeoutReadClient:
    source_account_index = 11
    receiver_account_index = 22

    def __init__(self):
        self.started = {11: asyncio.Event(), 22: asyncio.Event()}
        self.drained = {11: asyncio.Event(), 22: asyncio.Event()}

    async def account_snapshot(self, account_index, market_id):
        self.started[account_index].set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.drained[account_index].set()
            raise


@pytest.mark.asyncio
async def test_recheck_timeout_drains_both_bounded_reads():
    client = TimeoutReadClient()
    engine = HandoffEngine(client, clock=FakeClock())
    engine._configured_request_timeout = 0.01

    with pytest.raises(TimeoutError, match="recheck account read exceeded configured request timeout"):
        await engine._parallel_account_rechecks(11, 22, 7)

    assert all(event.is_set() for event in client.started.values())
    assert all(event.is_set() for event in client.drained.values())


@pytest.mark.asyncio
async def test_external_recheck_cancellation_drains_sibling_without_receiver_path():
    client = TimeoutReadClient()
    engine = HandoffEngine(client, clock=FakeClock())
    engine._configured_request_timeout = 1
    task = asyncio.create_task(engine._parallel_account_rechecks(11, 22, 7))
    await asyncio.gather(*(event.wait() for event in client.started.values()))

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert all(event.is_set() for event in client.drained.values())


@pytest.mark.asyncio
async def test_malformed_mapping_is_contract_failure_when_read_helper_is_used():
    class Malformed:
        source_account_index = 11
        receiver_account_index = 22

        async def account_snapshot(self, account_index, market_id):
            if account_index == 11:
                return {"account_index": 11}
            await asyncio.Event().wait()

    engine = HandoffEngine(Malformed(), clock=FakeClock())
    engine._configured_request_timeout = 1
    with pytest.raises(ContractError):
        await engine._parallel_account_rechecks(11, 22, 7)
