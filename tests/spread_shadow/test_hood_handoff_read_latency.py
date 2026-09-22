from __future__ import annotations

import asyncio
from decimal import Decimal
from time import perf_counter

import pytest

from risex_spread_shadow.hood_handoff import (
    AccountSnapshot,
    ContractError,
    DepthLevel,
    Direction,
    HandoffEngine,
    MarketMetadata,
    OrderBookSnapshot,
    Outcome,
    RandomCycleSelection,
    RandomCycleConfig,
    RandomCycleEngine,
    compute_quantity_bounds,
    run_handoff,
)

from test_hood_handoff_engine import FakeClient, FakeClock, make_config


class TimedCycleAccounts(FakeClient):
    def __init__(self, *, delay: float):
        super().__init__()
        self.delay = delay
        self.active_reads = 0
        self.max_active_reads = 0
        self.events: list[tuple[str, float]] = []

    async def account_snapshot(self, account_index, market_id):
        self.active_reads += 1
        self.max_active_reads = max(self.max_active_reads, self.active_reads)
        self.events.append((f"start-{account_index}", perf_counter()))
        try:
            await asyncio.sleep(self.delay)
            return await super().account_snapshot(account_index, market_id)
        finally:
            self.events.append((f"end-{account_index}", perf_counter()))
            self.active_reads -= 1


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


class TimedPreflightClient(FakeClient):
    """Delay every initial preflight read and retain its overlap evidence."""

    def __init__(self, *, delay: float):
        super().__init__(source_fills=True)
        self.delay = delay
        self.active_reads = 0
        self.max_active_reads = 0
        self.events: list[tuple[str, float]] = []

    async def _delayed(self, label: str, operation):
        self.events.append((f"start-{label}", perf_counter()))
        self.active_reads += 1
        self.max_active_reads = max(self.max_active_reads, self.active_reads)
        try:
            await asyncio.sleep(self.delay)
            return await operation()
        finally:
            self.events.append((f"end-{label}", perf_counter()))
            self.active_reads -= 1

    async def market_metadata(self, market_id):
        return await self._delayed(
            "metadata",
            lambda: super(TimedPreflightClient, self).market_metadata(market_id),
        )

    async def account_snapshot(self, account_index, market_id):
        return await self._delayed(
            f"account-{account_index}",
            lambda: super(TimedPreflightClient, self).account_snapshot(account_index, market_id),
        )

    async def submit_order(self, plan):
        self.events.append(("first-submit" if not self.events or not any(label == "first-submit" for label, _ in self.events) else "submit", perf_counter()))
        return await super().submit_order(plan)


class DelayedCycleReads:
    """Synthetic cycle adapter whose read timing is visible at the boundary."""

    source_account_index = 11
    receiver_account_index = 22

    def __init__(self, *, account_delay: float, metadata_delay: float, book_delay: float):
        self.account_delay = account_delay
        self.metadata_delay = metadata_delay
        self.book_delay = book_delay
        self.events: list[tuple[str, float]] = []

    async def market_metadata(self, market_id):
        self.events.append(("metadata-start", perf_counter()))
        await asyncio.sleep(self.metadata_delay)
        self.events.append(("metadata-end", perf_counter()))
        return MarketMetadata(
            market_id=market_id,
            symbol="BTC",
            status="active",
            price_decimals=1,
            size_decimals=2,
            minimum_base_amount=Decimal("0.10"),
            minimum_quote_amount=Decimal("10"),
            source_fee_rate=None,
            receiver_fee_rate=None,
            observed_at=1_000.0,
            margin_evidence="synthetic account limits",
            market_type="perp",
            venue="robinhood",
        )

    async def account_snapshot(self, account_index, market_id):
        self.events.append((f"account-{account_index}-start", perf_counter()))
        await asyncio.sleep(self.account_delay)
        self.events.append((f"account-{account_index}-end", perf_counter()))
        return AccountSnapshot(
            account_index=account_index,
            market_id=market_id,
            signed_position=Decimal("0"),
            active_orders=(),
            observed_at=1_000.0,
            authorized=True,
            ready=True,
            margin_available=Decimal("1000"),
            margin_required=Decimal("1"),
            fee_rate=None,
            source_identity=f"cycle-account-{account_index}",
            incremental_margin_required=Decimal("1"),
            incremental_margin_evidence="synthetic opening margin proof",
            available_balance=Decimal("1000"),
        )

    async def order_book(self, market_id):
        self.events.append(("book-start", perf_counter()))
        await asyncio.sleep(self.book_delay)
        self.events.append(("book-end", perf_counter()))
        return OrderBookSnapshot(
            market_id=market_id,
            symbol="BTC",
            bids=(DepthLevel(Decimal("100.0"), Decimal("100"), "bid-1"),),
            asks=(DepthLevel(Decimal("100.2"), Decimal("100"), "ask-1"),),
            observed_at=1_000.0,
            market_type="perp",
            venue="robinhood",
        )


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
async def test_child_preflight_metadata_and_accounts_overlap_before_source_dispatch(tmp_path):
    client = TimedPreflightClient(delay=0.03)
    result = await run_handoff(
        make_config(tmp_path / "preflight-overlap.jsonl"),
        client,
        clock=FakeClock(),
    )

    assert result.outcome is Outcome.SUCCESS
    assert client.max_active_reads == 3
    preflight_end = next(index for index, (label, _) in enumerate(client.events) if label == "first-submit")
    events: dict[str, float] = {}
    for label, timestamp in client.events[:preflight_end]:
        events.setdefault(label, timestamp)
    assert events["start-metadata"] < events["end-account-11"]
    assert events["start-account-11"] < events["end-account-22"]
    assert events["start-account-22"] < events["end-account-11"]
    assert max(
        events["end-metadata"],
        events["end-account-11"],
        events["end-account-22"],
    ) <= client.events[preflight_end][1]


@pytest.mark.asyncio
async def test_random_cycle_account_reads_overlap_with_maximum_two_concurrent_reads(tmp_path):
    client = TimedCycleAccounts(delay=0.03)
    config = RandomCycleConfig(
        market_id=7,
        market_symbol="BTC",
        direction=Direction.LONG,
        source_account_index=11,
        receiver_account_index=22,
        cycle_dir=tmp_path / "cycle",
    )
    engine = RandomCycleEngine(client, clock=FakeClock())

    source, receiver = await engine._accounts(config)

    assert source.account_index == 11
    assert receiver.account_index == 22
    assert client.max_active_reads == 2
    times = dict(client.events)
    assert times["start-11"] < times["end-22"]
    assert times["start-22"] < times["end-11"]


@pytest.mark.asyncio
async def test_cycle_revalidation_takes_book_quote_after_accounts(tmp_path):
    client = DelayedCycleReads(account_delay=0.03, metadata_delay=0.01, book_delay=0.02)
    config = RandomCycleConfig(
        market_id=7,
        market_symbol="BTC",
        direction=Direction.LONG,
        source_account_index=11,
        receiver_account_index=22,
        cycle_dir=tmp_path / "late-quote",
    )
    engine = RandomCycleEngine(client, clock=FakeClock())

    initial_metadata = await client.market_metadata(config.market_id)
    initial_source = await client.account_snapshot(config.source_account_index, config.market_id)
    initial_receiver = await client.account_snapshot(config.receiver_account_index, config.market_id)
    initial_bounds = compute_quantity_bounds(
        initial_metadata,
        initial_source,
        initial_receiver,
        Decimal("100.1"),
    )
    selection = RandomCycleSelection(
        quantity=Decimal("0.20"),
        quantity_tick=20,
        hold_seconds=20,
        opening_source_price=Decimal("100.1"),
        opening_receiver_bound=Decimal("100.1"),
        bounds=initial_bounds,
        metadata_observed_at=initial_metadata.observed_at,
        book_observed_at=1_000.0,
    )
    refreshed = await engine._revalidate_open(
        config,
        selection,
        initial_metadata=initial_metadata,
        initial_book=None,
        initial_source=initial_source,
        initial_receiver=initial_receiver,
    )
    assert refreshed[0].market_id == 7
    assert refreshed[2].account_index == 11
    assert refreshed[3].account_index == 22
    events = dict(client.events)
    assert events["account-11-end"] <= events["book-start"]
    assert events["account-22-end"] <= events["book-start"]


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
