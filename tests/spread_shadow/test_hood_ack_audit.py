"""ACK/fan-out audit regressions: real receipt shape, persistence and slow storage."""
import asyncio
import json
import os
import threading
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import pytest

from risex_spread_shadow.hood_handoff import Outcome, load_saved_cycle_report, run_random_cycle
from test_hood_fanout import FanoutVenue, fanout_config
from test_hood_handoff_random_cycle import AdvancingClock, FixedRng
from risex_spread_shadow.hood_handoff.journal import DurableJournal, EXECUTION_DIAGNOSTICS


async def test_exhausted_closing_preparation_releases_all_nonces_before_recovery(tmp_path, monkeypatch):
    import risex_spread_shadow.hood_handoff.fanout_cycle as module

    class ReservedVenue(FanoutVenue):
        def __init__(self, clock):
            super().__init__(clock)
            self.reserved = {}

        async def reserve_order_nonce(self, account, *, deadline):
            if account in self.reserved:
                raise RuntimeError("account still has an unsent reservation")
            token = SimpleNamespace(account=account, deadline=deadline)
            self.reserved[account] = token
            return token

        async def invalidate_reserved_nonce(self, token):
            if self.reserved.get(token.account) is token:
                self.reserved.pop(token.account)

        async def prepare_order(self, plan, *, reserved_nonce=None):
            class Prepared(dict):
                diagnostic_timings = {}
            return Prepared(await super().prepare_order(plan, reserved_nonce=reserved_nonce))

    clock = AdvancingClock()
    venue = ReservedVenue(clock)
    original = module._exclusive_source_price_available
    monkeypatch.setattr(module, "_exclusive_source_price_available",
                        lambda *a, **kw: False if any(venue.positions.values()) else original(*a, **kw))
    result = await run_random_cycle(fanout_config(tmp_path / "cycle"), venue,
                                   clock=clock, rng=FixedRng(40, 20, 5))
    assert result.opening.mutual_execution_proven
    assert result.inventory == "CONFIRMED_FLAT", result.reason
    assert {r.account_index for r in result.fallbacks if r.attempted} == {11, 22, 33}
    assert not venue.reserved


@pytest.mark.parametrize("conflict", [False, True])
async def test_ack_without_id_accepts_exact_stream_identity_only(tmp_path, conflict):
    class VisibleSource(FanoutVenue):
        def ws_admission_view(self, anchor, plan, order_id):
            # The initial check has no ACK ID; a later check may use the ID
            # already proved from the exact stream account/client binding.
            order = next(o for o in self.orders.values()
                         if o.account_index == plan.account_index
                         and o.client_order_index == plan.client_order_index)
            if conflict:
                order = replace(order, client_order_index=order.client_order_index + 1)
            return order, self._book()

    clock = AdvancingClock()
    venue = VisibleSource(clock)
    result = await run_random_cycle(fanout_config(tmp_path / "cycle"), venue,
                                   clock=clock, rng=FixedRng(40, 20, 5))
    if conflict:
        assert not any(s[0] == "MARKET" for s in venue.sends)
        assert not result.opening.mutual_execution_proven
    else:
        assert result.outcome is Outcome.SUCCESS, result.reason
        assert result.opening.mutual_execution_proven
        assert result.closing.mutual_execution_proven
        assert all(p == Decimal(0) for p in venue.positions.values())
        report = load_saved_cycle_report(tmp_path / "cycle")
        for phase in ("opening", "closing"):
            measurements = report["latency"][phase][0]["measurements"]
            for name in ("source_to_receiver_send", "source_ack_to_receiver_send", "receiver_intent_durable"):
                assert measurements[name]["status"] == "AVAILABLE"
                assert measurements[name]["seconds"] >= 0


def test_diagnostics_are_bounded_but_every_receiver_intent_is_durable_together(tmp_path, monkeypatch):
    path = tmp_path / "attempt.jsonl"
    journal = DurableJournal(path, deferred_events=EXECUTION_DIAGNOSTICS)
    journal.acquire_attempt()
    try:
        journal.append("SOURCE_DISPATCH_INTENT", {"account_index": 11})
        inode = path.stat().st_ino
        calls = []
        sync = os.fsync

        def counted(fd):
            if os.fstat(fd).st_ino == inode:
                calls.append(path.read_text())
            sync(fd)

        monkeypatch.setattr(os, "fsync", counted)
        journal.append("SOURCE_DISPATCH_RESULT", {"accepted": True})
        journal.append("PRE_RECEIVER_GUARD", {"status": "ACK_ADMITTED"})
        assert len(journal.events) == 3 and len(path.read_text().splitlines()) == 1
        journal.append_many([(f"RECEIVER_{n}_DISPATCH_INTENT", {"account_index": n})
                             for n in range(2, 17)])
        assert len(calls) == 1
        persisted = [json.loads(line) for line in path.read_text().splitlines()]
        assert [r["sequence"] for r in persisted] == list(range(1, 19))
        assert all(f'"event":"RECEIVER_{n}_DISPATCH_INTENT"' in calls[0] for n in range(2, 17))
        assert DurableJournal(path).has_unresolved_mutation()
        for _ in range(64):
            journal.append("ORDER_OBSERVED", {"status": "open"})
        assert len(calls) == 2  # Unbounded buffering cannot hide sustained diagnostics.
        journal.append("ORDER_OBSERVED", {"status": "filled"})
    finally:
        journal.release_attempt()
    assert json.loads(path.read_text().splitlines()[-1])["payload"]["status"] == "filled"


def test_receiver_only_intent_is_not_replayable(tmp_path):
    journal = DurableJournal(tmp_path / "attempt.jsonl")
    journal.append("RECEIVER_16_DISPATCH_INTENT", {})
    with pytest.raises(RuntimeError, match="unresolved"):
        DurableJournal(journal.path).assert_safe_to_start()


@pytest.mark.parametrize("failure", ["write", "fsync"])
async def test_fanout_batch_disk_failure_sends_no_market(tmp_path, monkeypatch, failure):
    original_write, original_sync = os.write, os.fsync
    receiver_fd = None

    def write(fd, value):
        nonlocal receiver_fd
        if b'"event":"RECEIVER_DISPATCH_INTENT"' in value:
            receiver_fd = fd
            if failure == "write":
                original_write(fd, value[:13])  # Preserve a torn suffix, never repair it silently.
                raise OSError("injected write failure")
        return original_write(fd, value)

    def sync(fd):
        if failure == "fsync" and fd == receiver_fd:
            raise OSError("injected fsync failure")
        return original_sync(fd)

    monkeypatch.setattr(os, "write", write)
    monkeypatch.setattr(os, "fsync", sync)
    clock = AdvancingClock()
    venue = FanoutVenue(clock)
    result = await run_random_cycle(fanout_config(tmp_path / "cycle"), venue,
                                   clock=clock, rng=FixedRng(40, 20, 5))
    assert result.outcome is Outcome.UNKNOWN
    assert len(venue.sends) == 1 and venue.sends[0][0] == "LIMIT"
    assert result.inventory != "CONFIRMED_FLAT"


@pytest.mark.parametrize("mode", ["book", "expired", "cancel"])
async def test_slow_durability_keeps_stream_responsive_and_blocks_stale_fanout(tmp_path, monkeypatch, mode):
    entered, release = threading.Event(), threading.Event()
    original = DurableJournal._flush_pending

    def slow(self):
        if self.path.name == "opening.jsonl" and any(e.event == "RECEIVER_DISPATCH_INTENT" for e, _ in self._pending):
            entered.set()
            if not release.wait(3):
                raise TimeoutError("event loop did not stay responsive")
        original(self)

    monkeypatch.setattr(DurableJournal, "_flush_pending", slow)
    clock = AdvancingClock()
    venue = FanoutVenue(clock)
    task = asyncio.create_task(run_random_cycle(fanout_config(tmp_path / "cycle"), venue,
                                               clock=clock, rng=FixedRng(40, 20, 5)))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        assert len(venue.sends) == 1  # Source only; no member sends ahead of the batch barrier.
        if mode == "cancel":
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()  # The disk worker must drain before lock/fd release.
        elif mode == "expired":
            await clock.sleep(20)
        else:
            venue.admission_veto = "better"
    finally:
        release.set()
    if mode == "cancel":
        with pytest.raises(asyncio.CancelledError):
            await task
        assert DurableJournal(tmp_path / "cycle/opening.jsonl").has_unresolved_mutation()
    else:
        result = await task
        assert result.inventory == "CONFIRMED_FLAT", result.reason
        assert all(not leg.dispatched for leg in result.opening.receivers)
        rows = [json.loads(line) for line in (tmp_path / "cycle/opening.jsonl").read_text().splitlines()]
        refusals = [r for r in rows if r["payload"].get("not_sent")]
        assert len(refusals) == 2
        report = load_saved_cycle_report(tmp_path / "cycle")
        assert report["status"] == "COMPLETE", report["issues"]
        latency = report["latency"]["opening"][0]["measurements"]
        assert latency["receiver_dispatch_ack"]["status"] == "UNAVAILABLE"
        assert latency["source_to_receiver_send"]["status"] == "UNAVAILABLE"
        if mode == "book":
            # Intent alone, ambiguous transport, a response with an order ID,
            # and a different account's intent must never become proof of no send.
            for defect in ("missing_marker", "missing_result", "accepted", "order_id", "unknown", "wrong_account"):
                damaged = deepcopy(rows)
                refusal = next(r for r in damaged if r["event"] == "RECEIVER_DISPATCH_RESULT")
                if defect == "missing_marker":
                    refusal["payload"].pop("not_sent")
                elif defect == "missing_result":
                    damaged.remove(refusal)
                elif defect == "accepted":
                    refusal["payload"]["accepted"] = True
                elif defect == "order_id":
                    refusal["payload"]["order_id"] = "possibly-sent"
                elif defect == "unknown":
                    refusal["event"] = "RECEIVER_DISPATCH_UNKNOWN"
                else:
                    intent = next(r for r in damaged if r["event"] == "RECEIVER_DISPATCH_INTENT")
                    intent["payload"]["plan"]["account_index"] = 999
                for sequence, row in enumerate(damaged, 1):
                    row["sequence"] = sequence
                (tmp_path / "cycle/opening.jsonl").write_text("".join(json.dumps(r) + "\n" for r in damaged))
                damaged_report = load_saved_cycle_report(tmp_path / "cycle")
                assert damaged_report["status"] == "INCOMPLETE", defect
                assert any(i["code"] == "UNRESOLVED_INTENT" for i in damaged_report["issues"]), defect
    assert not any(s[0] == "MARKET" for s in venue.sends)


async def test_real_btc_binding_closes_fanout_dust_without_top_up(tmp_path):
    class BtcVenue(FanoutVenue):
        async def market_metadata(self, market):
            return replace(await super().market_metadata(market), market_id=1)

        async def account_snapshot(self, account, market):
            return replace(await super().account_snapshot(account, market), market_id=1)

        def _book(self):
            return replace(super()._book(), market_id=1)

        def _trade(self, *args):
            return replace(super()._trade(*args), market_id=1)

    clock = AdvancingClock()
    venue = BtcVenue(clock)
    venue.external_take = Decimal("0.10")
    result = await run_random_cycle(fanout_config(tmp_path / "cycle", market_id=1), venue,
                                   clock=clock, rng=FixedRng(30, 20, 5))
    assert result.inventory == "CONFIRMED_FLAT", result.reason
    assert ("MARKET", 33, "SELL", Decimal("0.05"), True) in venue.sends
    assert all(s[-1] for s in venue.sends[3:])  # Every recovery order remains reduce-only.
