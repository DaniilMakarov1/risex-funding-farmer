"""HCR-39 regressions: synthetic executions only, never SDK/network access."""
import asyncio
from dataclasses import replace
from decimal import Decimal
import json

import pytest

from risex_spread_shadow.hood_handoff import (
    Direction, HistoryPage, MutationReceipt, Outcome, TradeReceipt,
)
from risex_spread_shadow.hood_handoff import operator_recovery as recovery
from risex_spread_shadow.hood_handoff import random_cycle as cycle
from risex_spread_shadow.hood_handoff.offline_report import load_saved_cycle_report
from risex_spread_shadow.hood_handoff.operator_view import close_result_lines, result_lines
from test_hood_handoff_random_cycle import (
    AdvancingClock, CycleClient, FixedRng, cycle_config, run_random_cycle,
)
from test_hood_operator_recovery import RecoveryClient


def rows(slot, name="close.jsonl"):
    return [json.loads(line) for line in (slot / name).read_text().splitlines()]


class DisappearingSource(CycleClient):
    """ACK has no ID; known source vanishes twice during an external fill."""
    def __init__(self, clock, *, closing=False, adverse=None):
        super().__init__(clock)
        self.closing, self.adverse = closing, adverse
        self.target_reads = 0
        self.target_id = None
        self.history_requests = []

    async def submit_order(self, plan):
        receipt = await super().submit_order(plan)
        if plan.order_type == "LIMIT" and plan.reduce_only == self.closing:
            self.target_id = receipt.order_id
            return replace(receipt, order_id=None)
        return receipt

    async def lookup_order(self, account_index, market_id, **kwargs):
        value = await super().lookup_order(account_index, market_id, **kwargs)
        if value is None or value.order_id != self.target_id:
            return value
        self.target_reads += 1
        if self.target_reads == 2:
            value = self._replace_order(value, status="filled", filled_quantity=value.initial_quantity,
                                        remaining_quantity=Decimal(0))
            self.source_position += value.initial_quantity * (1 if value.side == "BUY" else -1)
            self.trades[value.order_id] = (TradeReceipt(
                "outside-source", value.account_index, value.market_id, value.order_id, value.side,
                value.initial_quantity, value.price, None, 999, self.clock.now(),
                counterparty_order_id="outside", client_order_index=value.client_order_index,
            ),)
        if 2 <= self.target_reads <= 4:
            return None
        if self.adverse == "identifier" and self.target_reads >= 5:
            return replace(value, order_id="different-id")
        return value

    async def list_trades(self, account_index, market_id, **kwargs):
        self.history_requests.append(kwargs.get("order_id"))
        value = await super().list_trades(account_index, market_id, **kwargs)
        if kwargs.get("order_id") == self.target_id:
            if self.adverse == "missing_history":
                return HistoryPage(trades=())
            if self.adverse == "foreign_history":
                return HistoryPage(trades=tuple(replace(t, account_index=777) for t in value.trades))
        return value


@pytest.mark.asyncio
@pytest.mark.parametrize("closing", [False, True])
@pytest.mark.parametrize("direction", [Direction.LONG, Direction.SHORT])
async def test_known_source_id_survives_missing_lookup_and_cleanup_closes_residual(tmp_path, closing, direction):
    clock = AdvancingClock()
    client = DisappearingSource(clock, closing=closing)
    result = await run_random_cycle(cycle_config(tmp_path, direction=direction), client,
                                    clock=clock, rng=FixedRng(20, 20))
    phase = result.closing if closing else result.opening
    assert phase.outcome is Outcome.PARTIAL, result.as_dict()
    assert phase.source.order_id == client.target_id
    assert phase.source.filled_quantity == Decimal(".20")
    assert phase.source.history_complete and not phase.source.unknown_reasons
    assert phase.receiver.dispatched is False
    # Cleanup may resolve publication before history reconciliation starts.
    # Require history for the retained exact ID and its actual external fill,
    # not a redundant second history request from the earlier cleanup path.
    assert client.target_id in client.history_requests
    assert [trade.trade_id for trade in phase.source.trades] == ["outside-source"]
    assert client.source_position == client.receiver_position == 0
    assert result.inventory == "CONFIRMED_FLAT"
    assert result.outcome is Outcome.PARTIAL
    assert len(client.submissions) == (4 if closing else 2)
    assert client.submissions[-1].reduce_only
    assert client.submissions[-1].quantity == Decimal(".20")
    assert client.submissions[-1].account_index == (22 if closing else 11)


@pytest.mark.asyncio
@pytest.mark.parametrize("adverse", ["identifier", "missing_history", "foreign_history"])
async def test_retained_id_does_not_override_conflicting_or_incomplete_execution(tmp_path, adverse):
    clock = AdvancingClock()
    client = DisappearingSource(clock, adverse=adverse)
    result = await run_random_cycle(cycle_config(tmp_path), client, clock=clock, rng=FixedRng(20, 20))
    assert result.outcome is Outcome.UNKNOWN
    assert not result.fallbacks and len(client.submissions) == 1
    assert result.inventory != "CONFIRMED_FLAT"


class BtcRecoveryClient(RecoveryClient):
    """Use actual BTC grid/minima with synthetic balances/orders/receipts."""
    async def market_metadata(self, market_id):
        assert market_id == 1
        m = await super().market_metadata(7)
        return replace(m, market_id=1, size_decimals=5, minimum_base_amount=Decimal(".00020"),
                       minimum_quote_amount=Decimal(10))

    async def order_book(self, market_id):
        assert market_id == 1
        b = await super().order_book(7)
        return replace(b, market_id=1,
                       bids=(replace(b.bids[0], price=Decimal("86178.1")),),
                       asks=(replace(b.asks[0], price=Decimal("86178.3")),))

    async def account_snapshot(self, account_index, market_id):
        assert market_id == 1
        return replace(await super().account_snapshot(account_index, 7), market_id=1)

    async def lookup_order(self, account_index, market_id, **kwargs):
        assert market_id == 1
        return await super().lookup_order(account_index, 7, **kwargs)

    async def submit_order(self, plan):
        receipt = await super().submit_order(plan)
        self.trades = {key: tuple(replace(t, market_id=1) for t in value) for key, value in self.trades.items()}
        return receipt


@pytest.mark.asyncio
@pytest.mark.parametrize("quantity", [".00001", ".00004", ".00007"])
@pytest.mark.parametrize("sign", [-1, 1])
async def test_btc_exact_small_reduce_only_residual_is_submitted_without_rounding(tmp_path, quantity, sign):
    clock = AdvancingClock()
    client = BtcRecoveryClient(clock, str(Decimal(quantity) * sign))
    slot = recovery.allocate_close_slot(tmp_path)
    result = await recovery.close_positions(cycle_config(slot, market_id=1), client, tmp_path, slot, clock=clock)
    assert result["status"] == "CONFIRMED_FLAT", result
    assert len(client.submissions) == 1
    plan = client.submissions[0]
    assert plan.quantity == Decimal(quantity) and plan.quantity_int == int(Decimal(quantity) * 100000)
    assert plan.side == ("SELL" if sign > 0 else "BUY")
    assert plan.reduce_only and plan.order_type == "MARKET" and plan.time_in_force == "IOC"
    exception = next(r["payload"] for r in rows(slot) if r["event"] == "FALLBACK_REDUCE_ONLY_MINIMUM_EXCEPTION")
    assert Decimal(exception["quantity"]) == Decimal(quantity)
    assert exception["minimum_base_amount"] == "0.00020"
    assert result["reason"] is None


@pytest.mark.asyncio
async def test_btc_partial_close_continues_below_opening_minimum(tmp_path):
    clock = AdvancingClock()
    client = BtcRecoveryClient(clock, ".00020", "-.00007", fractions=[Decimal(".8"), Decimal(1), Decimal(1)])
    slot = recovery.allocate_close_slot(tmp_path)
    result = await recovery.close_positions(cycle_config(slot, market_id=1), client, tmp_path, slot, clock=clock)
    assert result["status"] == "CONFIRMED_FLAT", result
    assert [p.quantity for p in client.submissions] == [Decimal(".00020"), Decimal(".00007"), Decimal(".00004")]
    assert [p.account_index for p in client.submissions] == [11, 22, 11]
    assert len({p.client_order_index for p in client.submissions}) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["off_grid", "other_market", "reject", "ambiguous"])
async def test_small_residual_exception_keeps_grid_market_rejection_and_no_replay_guards(tmp_path, kind):
    clock = AdvancingClock()
    client = (RecoveryClient(clock, ".04") if kind == "other_market" else
              BtcRecoveryClient(clock, ".000004" if kind == "off_grid" else ".00004", "-.00007",
                                unknown=kind == "ambiguous"))
    if kind == "reject":
        async def rejected(plan):
            client.submissions.append(plan)
            return MutationReceipt(False, None, None)
        client.submit_order = rejected
    slot = recovery.allocate_close_slot(tmp_path)
    result = await recovery.close_positions(cycle_config(slot, market_id=7 if kind == "other_market" else 1),
                                            client, tmp_path, slot, clock=clock)
    assert result["status"] == ("UNKNOWN" if kind == "ambiguous" else "PARTIAL"), result
    if kind == "other_market":
        assert not client.submissions and "minimum_base=0.10" in result["reason"]
    elif kind == "off_grid":
        assert [p.account_index for p in client.submissions] == [22]
        assert client.source_position == Decimal(".000004")
    elif kind == "reject":
        assert len(client.submissions) == 2  # one known rejection per account, no repeat
    else:
        assert len(client.submissions) == 1 and client.receiver_position == Decimal("-.00007")


class TransientRecoveryClient(RecoveryClient):
    def __init__(self, clock, *, fail_reads, failure="timeout"):
        super().__init__(clock, ".2", "-.2")
        self.fail_reads, self.failure, self.source_reads = set(fail_reads), failure, 0

    async def account_snapshot(self, account_index, market_id):
        value = await super().account_snapshot(account_index, market_id)
        if account_index == 11:
            self.source_reads += 1
            if self.source_reads in self.fail_reads:
                if self.failure == "identity":
                    return replace(value, account_index=777)
                if self.failure == "auth":
                    return replace(value, authorized=False)
                if self.failure == "transport_and_identity":
                    raise TimeoutError("private transport details must be hidden")
                raise TimeoutError("private transport details must be hidden")
        elif self.failure == "transport_and_identity":
            return replace(value, account_index=777)
        return value


@pytest.mark.asyncio
@pytest.mark.parametrize("read_number", [1, 2, 3, 4, 5, 6])
async def test_close_retries_transient_reads_without_repeating_orders(tmp_path, read_number):
    clock = AdvancingClock()
    client = TransientRecoveryClient(clock, fail_reads=[read_number])
    slot = recovery.allocate_close_slot(tmp_path)
    result = await recovery.close_positions(cycle_config(slot), client, tmp_path, slot, clock=clock)
    assert result["status"] == "CONFIRMED_FLAT", result
    assert len(client.submissions) == 2
    assert [p.quantity for p in client.submissions] == [Decimal(".2"), Decimal(".2")]
    assert any(r["event"] == "RECOVERY_READ_RETRY" for r in rows(slot))
    assert "private transport" not in (slot / "close.jsonl").read_text()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["timeout", "identity", "auth", "transport_and_identity"])
async def test_recovery_read_exhaustion_or_invalid_identity_never_dispatches(tmp_path, failure):
    clock = AdvancingClock()
    client = TransientRecoveryClient(clock, fail_reads=[1, 2, 3], failure=failure)
    slot = recovery.allocate_close_slot(tmp_path)
    result = await recovery.close_positions(cycle_config(slot), client, tmp_path, slot, clock=clock)
    assert result["status"] == "UNKNOWN" and not client.submissions
    assert client.source_reads == (2 if failure == "timeout" else 1)
    if failure == "timeout":
        assert "transiently unavailable" in result["reason"]
    assert "private transport" not in json.dumps(result)


@pytest.mark.asyncio
async def test_recovery_read_deadline_and_cancellation_are_bounded(tmp_path):
    clock = AdvancingClock()
    engine = cycle.RandomCycleEngine(CycleClient(clock), clock=clock)
    count = 0
    async def unavailable():
        nonlocal count
        count += 1
        await clock.sleep(.6)
        raise TimeoutError("hidden")
    with pytest.raises(cycle._RetryablePreparationFailure, match="deadline"):
        await engine._recovery_read(cycle_config(tmp_path, max_poll_count=40, poll_interval_seconds=.5),
                                    unavailable, "account read")
    assert count == 1 and clock.now() == pytest.approx(1001)
    async def cancelled():
        raise asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await engine._recovery_read(cycle_config(tmp_path), cancelled, "account read")


@pytest.mark.asyncio
async def test_delayed_recovery_account_must_not_send_against_stale_book(tmp_path):
    clock = AdvancingClock()
    class Delayed(TransientRecoveryClient):
        async def account_snapshot(self, account_index, market_id):
            if account_index == 11 and self.source_reads == 1:
                await clock.sleep(11)
            return await super().account_snapshot(account_index, market_id)
    client = Delayed(clock, fail_reads=[])
    slot = recovery.allocate_close_slot(tmp_path)
    result = await recovery.close_positions(cycle_config(slot, reconcile_timeout_seconds=20),
                                            client, tmp_path, slot, clock=clock)
    assert result["status"] == "PARTIAL", result
    assert all(p.account_index != 11 for p in client.submissions)
    assert "stale" in result["reason"]


@pytest.mark.asyncio
@pytest.mark.parametrize("transient", [False, True])
async def test_cycle_recovery_baseline_retry_and_exact_failure_reason(tmp_path, monkeypatch, transient):
    clock = AdvancingClock()
    client = DisappearingSource(clock)
    original = cycle.RandomCycleEngine._accounts
    calls = 0
    async def accounts(engine, config, now=None):
        nonlocal calls
        if client.target_reads >= 5:
            calls += 1
            if not transient or calls == 1:
                if transient:
                    raise cycle._RetryablePreparationFailure("source account read was transiently unavailable")
                raise cycle.PreflightBlocked("source account has active cycle-market orders")
        return await original(engine, config, now)
    monkeypatch.setattr(cycle.RandomCycleEngine, "_accounts", accounts)
    result = await run_random_cycle(cycle_config(tmp_path), client, clock=clock, rng=FixedRng(20, 20))
    if transient:
        assert result.inventory == "CONFIRMED_FLAT"
        assert len(client.submissions) == 2
    else:
        assert result.outcome is Outcome.UNKNOWN and len(client.submissions) == 1
        assert "source account has active cycle-market orders" in result.reason
        assert "contract_error" not in result.reason
        assert result.remaining_source_position is None
        assert result.remaining_source_position_observed_at is None
        report = load_saved_cycle_report(tmp_path)
        assert "active cycle-market orders" in report["cycle"]["recovery_stop_reason"]
        text = "\n".join(result_lines(report))
        assert "Закрытие остатка остановлено" in text and "есть активные ордера" in text


def test_close_view_explains_historical_opaque_reason_without_inventing_cause():
    text = "\n".join(close_result_lines({"status": "UNKNOWN", "reason": "contract_error"}))
    assert "точная причина не сохранена" in text
    assert "таймаут" not in text


def test_close_view_separates_ambiguous_send_from_read_timeout():
    text = "\n".join(close_result_lines({"status": "UNKNOWN", "reason": "fallback dispatch outcome unknown: timeout"}))
    assert "дошёл ли закрывающий ордер" in text
    assert "данные счёта" not in text
